"""Unit tests for the Gold point-in-time customer join.

The production logic runs in Spark SQL (`databricks/gold/02_build_sales_fact.py`).
These tests reproduce the same join in SQLite against the real generated data and
pin the properties that make the sales fact trustworthy:

* an order line joins to exactly one customer version — never fans out;
* an order placed before a customer change resolves to the *old* version;
* an order placed after it resolves to the *new* version;
* every order line resolves some version;
* joining on `customer_id` alone (the naive version) genuinely does duplicate
  rows — proving the point-in-time predicate is doing real work.
"""
from __future__ import annotations

import hashlib
import sqlite3

import pytest

from retail_lakehouse import generate_source_data as gen

TRACKED = ["email", "customer_name", "segment", "city", "region"]
END_OF_TIME = "9999-12-31 23:59:59"


def _sk(*parts: str) -> str:
    return hashlib.md5("|".join(parts).encode()).hexdigest()


def _build_warehouse() -> sqlite3.Connection:
    """Silver history + cleaned transactions, mirroring commits 3A and 3B."""
    data = gen.generate()
    db = sqlite3.connect(":memory:")
    db.execute(
        """CREATE TABLE customers_history (
            customer_sk TEXT, customer_id TEXT, email TEXT, customer_name TEXT,
            segment TEXT, city TEXT, region TEXT,
            effective_start_timestamp TEXT, effective_end_timestamp TEXT, is_current INT)"""
    )
    db.execute(
        """CREATE TABLE src (customer_id TEXT, email TEXT, customer_name TEXT,
            segment TEXT, city TEXT, region TEXT, updated_at TEXT)"""
    )

    def load(rows):
        db.execute("DELETE FROM src")
        for r in rows:
            db.execute(
                "INSERT INTO src VALUES (?,?,?,?,?,?,?)",
                (
                    r["customer_id"], r["email"] or None, r["customer_name"] or None,
                    r["segment"] or None, r["city"] or None, r["region"] or None,
                    r["updated_at"],
                ),
            )

    def scd2():
        comparison = " AND ".join(f"h.{a} IS s.{a}" for a in TRACKED)
        changed = db.execute(
            f"""SELECT s.* FROM src s
                LEFT JOIN customers_history h
                       ON s.customer_id = h.customer_id AND h.is_current = 1
                WHERE h.customer_id IS NULL OR NOT ({comparison})"""
        ).fetchall()
        for row in changed:
            db.execute(
                """UPDATE customers_history SET is_current = 0, effective_end_timestamp = ?
                   WHERE customer_id = ? AND is_current = 1""",
                (row[6], row[0]),
            )
        for row in changed:
            db.execute(
                "INSERT INTO customers_history VALUES (?,?,?,?,?,?,?,?,?,1)",
                (_sk(row[0], row[6]), *row[:6], row[6], END_OF_TIME),
            )
        db.commit()

    load(data["batch_1"]["customers"])
    scd2()
    load(data["batch_2"]["customers"])
    scd2()

    db.execute(
        """CREATE TABLE orders_clean (
            order_id TEXT, customer_id TEXT, warehouse_id TEXT,
            order_timestamp TEXT, status TEXT)"""
    )
    db.execute(
        """CREATE TABLE order_items_clean (
            order_item_id TEXT, order_id TEXT, product_id TEXT,
            quantity INT, unit_price REAL)"""
    )

    seen: set[str] = set()
    for batch in ("batch_1", "batch_2"):
        for o in data[batch]["orders"]:
            db.execute(
                "INSERT INTO orders_clean VALUES (?,?,?,?,?)",
                (o["order_id"], o["customer_id"], o["warehouse_id"],
                 o["order_timestamp"], o["status"]),
            )
        for i in data[batch]["order_items"]:
            if i["order_item_id"] in seen:      # dedup, as Silver does
                continue
            seen.add(i["order_item_id"])
            if int(i["quantity"]) <= 0:         # quarantined, as Silver does
                continue
            db.execute(
                "INSERT INTO order_items_clean VALUES (?,?,?,?,?)",
                (i["order_item_id"], i["order_id"], i["product_id"],
                 int(i["quantity"]), float(i["unit_price"])),
            )
    db.commit()
    return db


POINT_IN_TIME_SALES = """
SELECT oi.order_item_id, o.order_timestamp, c.customer_sk, c.customer_id, c.email
FROM order_items_clean oi
JOIN orders_clean o ON oi.order_id = o.order_id
LEFT JOIN customers_history c
       ON o.customer_id = c.customer_id
      AND o.order_timestamp >= c.effective_start_timestamp
      AND o.order_timestamp <  c.effective_end_timestamp
"""


@pytest.fixture
def db() -> sqlite3.Connection:
    return _build_warehouse()


def test_grain_is_preserved(db):
    """One fact row per order line — the direct fan-out test."""
    source = db.execute("SELECT COUNT(*) FROM order_items_clean").fetchone()[0]
    fact = len(db.execute(POINT_IN_TIME_SALES).fetchall())
    assert fact == source


def test_no_order_line_is_duplicated(db):
    duplicates = db.execute(
        f"SELECT order_item_id, COUNT(*) c FROM ({POINT_IN_TIME_SALES}) "
        "GROUP BY order_item_id HAVING c > 1"
    ).fetchall()
    assert duplicates == []


def test_every_order_line_resolves_a_customer_version(db):
    orphans = db.execute(
        f"SELECT order_item_id FROM ({POINT_IN_TIME_SALES}) WHERE customer_sk IS NULL"
    ).fetchall()
    assert orphans == []


def _emails_for_order(db: sqlite3.Connection, order_id: str) -> set[str]:
    """Which customer-version emails the lines of one order resolve to."""
    rows = db.execute(
        f"""SELECT DISTINCT f.email
            FROM ({POINT_IN_TIME_SALES}) f
            JOIN orders_clean o ON o.order_timestamp = f.order_timestamp
            WHERE o.order_id = ?""",
        (order_id,),
    ).fetchall()
    return {r[0] for r in rows}


def test_order_before_change_resolves_to_the_old_version(db):
    emails = _emails_for_order(db, gen.EDGE_CASES["order_before_change"])
    assert emails == {"c001@old.example"}


def test_order_after_change_resolves_to_the_new_version(db):
    emails = _emails_for_order(db, gen.EDGE_CASES["order_after_change"])
    assert emails == {"c001@new.example"}


def test_the_same_customer_resolves_to_different_versions_over_time(db):
    """The whole point: history is preserved, so facts see what was true then."""
    customer_id = gen.EDGE_CASES["email_change_customer"]
    rows = db.execute(
        f"SELECT DISTINCT customer_sk, email FROM ({POINT_IN_TIME_SALES}) WHERE customer_id = ?",
        (customer_id,),
    ).fetchall()
    assert len(rows) == 2, "orders should resolve to two different versions of this customer"
    assert {r[1] for r in rows} == {"c001@old.example", "c001@new.example"}


def test_naive_join_on_customer_id_alone_does_fan_out(db):
    """Control test: without the validity window, the fact really is inflated.

    This is what the point-in-time predicate prevents; if this ever stops
    duplicating, the test data no longer exercises the risk.
    """
    naive = db.execute(
        """SELECT oi.order_item_id
           FROM order_items_clean oi
           JOIN orders_clean o ON oi.order_id = o.order_id
           LEFT JOIN customers_history c ON o.customer_id = c.customer_id"""
    ).fetchall()
    source = db.execute("SELECT COUNT(*) FROM order_items_clean").fetchone()[0]
    assert len(naive) > source
