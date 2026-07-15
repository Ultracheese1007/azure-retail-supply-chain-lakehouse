"""Unit tests for the customer SCD Type 2 semantics.

The production logic runs in Spark SQL (see
`databricks/silver/06_build_customer_history.py`). These tests reproduce the
same three-stage algorithm in SQLite — whose `IS` / `IS NOT` operators are
null-safe, exactly like Spark's `<=>` — and assert the invariants against the
real generated source data.

They exist so the rules that make the history correct are pinned down and can
be checked without a Databricks cluster:

* a changed customer is expired *and* gets a new version (the classic
  single-MERGE bug is a customer left with no current version at all);
* a NULL -> value transition is detected (null-safe comparison);
* a brand-new customer gets exactly one version;
* every customer has exactly one current version;
* re-running the same batch creates no extra versions.
"""
from __future__ import annotations

import sqlite3

import pytest

from retail_lakehouse import generate_source_data as gen

TRACKED = ["email", "customer_name", "segment", "city", "region"]
END_OF_TIME = "9999-12-31 23:59:59"


def _connect() -> sqlite3.Connection:
    db = sqlite3.connect(":memory:")
    db.execute(
        """CREATE TABLE hist (
            customer_sk TEXT, customer_id TEXT, email TEXT, customer_name TEXT,
            segment TEXT, city TEXT, region TEXT,
            effective_start_timestamp TEXT, effective_end_timestamp TEXT,
            is_current INT)"""
    )
    db.execute(
        """CREATE TABLE src (
            customer_id TEXT, email TEXT, customer_name TEXT, segment TEXT,
            city TEXT, region TEXT, updated_at TEXT)"""
    )
    return db


def _load_source(db: sqlite3.Connection, rows: list[dict]) -> None:
    db.execute("DELETE FROM src")
    for r in rows:
        db.execute(
            "INSERT INTO src VALUES (?,?,?,?,?,?,?)",
            (
                r["customer_id"],
                r["email"] or None,
                r["customer_name"] or None,
                r["segment"] or None,
                r["city"] or None,
                r["region"] or None,
                r["updated_at"],
            ),
        )


def _run_scd2(db: sqlite3.Connection) -> int:
    """Three stages: detect (null-safe) -> expire -> insert. Returns versions written."""
    comparison = " AND ".join(f"h.{a} IS s.{a}" for a in TRACKED)
    changed = db.execute(
        f"""SELECT s.* FROM src s
            LEFT JOIN hist h
                   ON s.customer_id = h.customer_id AND h.is_current = 1
            WHERE h.customer_id IS NULL OR NOT ({comparison})"""
    ).fetchall()

    for row in changed:
        db.execute(
            """UPDATE hist SET is_current = 0, effective_end_timestamp = ?
               WHERE customer_id = ? AND is_current = 1""",
            (row[6], row[0]),
        )
    for row in changed:
        db.execute(
            "INSERT INTO hist VALUES (?,?,?,?,?,?,?,?,?,1)",
            (f"sk_{row[0]}_{row[6]}", *row[:6], row[6], END_OF_TIME),
        )
    db.commit()
    return len(changed)


@pytest.fixture
def history() -> sqlite3.Connection:
    """History after loading batch 1 then batch 2."""
    data = gen.generate()
    db = _connect()
    _load_source(db, data["batch_1"]["customers"])
    _run_scd2(db)
    _load_source(db, data["batch_2"]["customers"])
    _run_scd2(db)
    return db


def _versions(db: sqlite3.Connection, customer_id: str) -> int:
    return db.execute(
        "SELECT COUNT(*) FROM hist WHERE customer_id = ?", (customer_id,)
    ).fetchone()[0]


def test_email_change_creates_second_version(history):
    assert _versions(history, gen.EDGE_CASES["email_change_customer"]) == 2


def test_null_to_value_is_detected(history):
    """Ordinary `<>` would evaluate NULL <> 'value' to NULL and miss this change."""
    customer = gen.EDGE_CASES["null_to_value_customer"]
    assert _versions(history, customer) == 2

    old_region = history.execute(
        "SELECT region FROM hist WHERE customer_id = ? AND is_current = 0", (customer,)
    ).fetchone()[0]
    new_region = history.execute(
        "SELECT region FROM hist WHERE customer_id = ? AND is_current = 1", (customer,)
    ).fetchone()[0]
    assert old_region is None
    assert new_region is not None


def test_new_customer_has_single_version(history):
    assert _versions(history, gen.EDGE_CASES["new_customer"]) == 1


def test_exactly_one_current_version_per_customer(history):
    offenders = history.execute(
        """SELECT customer_id, COUNT(*) FROM hist WHERE is_current = 1
           GROUP BY customer_id HAVING COUNT(*) <> 1"""
    ).fetchall()
    assert offenders == []


def test_no_customer_left_without_a_current_version(history):
    """Guards the single-MERGE failure mode: expired but never re-inserted."""
    orphans = history.execute(
        """SELECT DISTINCT customer_id FROM hist
           WHERE customer_id NOT IN (SELECT customer_id FROM hist WHERE is_current = 1)"""
    ).fetchall()
    assert orphans == []


def test_expired_version_closes_when_the_new_one_starts(history):
    """Validity windows must be contiguous and non-overlapping."""
    customer = gen.EDGE_CASES["email_change_customer"]
    old_end = history.execute(
        "SELECT effective_end_timestamp FROM hist WHERE customer_id = ? AND is_current = 0",
        (customer,),
    ).fetchone()[0]
    new_start = history.execute(
        "SELECT effective_start_timestamp FROM hist WHERE customer_id = ? AND is_current = 1",
        (customer,),
    ).fetchone()[0]
    assert old_end == new_start


def test_rerunning_the_same_batch_adds_no_versions(history):
    """Idempotency: no tracked attribute changed, so nothing new is written."""
    before = history.execute("SELECT COUNT(*) FROM hist").fetchone()[0]
    _load_source(history, gen.generate()["batch_2"]["customers"])
    written = _run_scd2(history)
    after = history.execute("SELECT COUNT(*) FROM hist").fetchone()[0]
    assert written == 0
    assert before == after
