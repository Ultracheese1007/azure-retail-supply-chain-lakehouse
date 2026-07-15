"""Unit tests for the generated source data.

These pin the invariants that later layers depend on. Most importantly, the
*timeline* must be coherent: a customer version has to start before the orders
that should resolve to it, otherwise the Gold point-in-time join finds no
matching version and silently drops the order.
"""
from __future__ import annotations

from datetime import datetime

from retail_lakehouse import generate_source_data as gen


def _ts(value: str) -> datetime:
    return datetime.fromisoformat(value)


def test_generation_is_deterministic():
    assert gen.generate() == gen.generate()


def test_summary_matches_committed_fixture():
    import json
    import pathlib

    fixture = pathlib.Path(__file__).parents[1] / "fixtures" / "expected_batch_summary.json"
    assert json.loads(fixture.read_text()) == gen.summarize()


def test_customers_are_created_before_their_first_orders():
    """A customer must exist before they can place an order."""
    data = gen.generate()
    created = {c["customer_id"]: _ts(c["updated_at"]) for c in data["batch_1"]["customers"]}
    created.update({c["customer_id"]: _ts(c["updated_at"]) for c in data["batch_2"]["customers"]
                    if c["customer_id"] not in created})

    for batch in ("batch_1", "batch_2"):
        for order in data[batch]["orders"]:
            first_seen = created[order["customer_id"]]
            assert _ts(order["order_timestamp"]) >= first_seen, (
                f"{order['order_id']} predates customer {order['customer_id']}"
            )


def test_change_precedes_the_order_that_should_see_it():
    """The 'after change' order must fall after the change.

    Otherwise the new customer version is never exercised by any fact.
    """
    data = gen.generate()
    changed_id = gen.EDGE_CASES["email_change_customer"]
    change_ts = _ts(
        next(
            c["updated_at"]
            for c in data["batch_2"]["customers"]
            if c["customer_id"] == changed_id
        )
    )

    before_id = gen.EDGE_CASES["order_before_change"]
    after_id = gen.EDGE_CASES["order_after_change"]
    before = next(o for o in data["batch_1"]["orders"] if o["order_id"] == before_id)
    after = next(o for o in data["batch_2"]["orders"] if o["order_id"] == after_id)
    assert _ts(before["order_timestamp"]) < change_ts
    assert _ts(after["order_timestamp"]) >= change_ts


def test_batch_two_plants_its_edge_cases():
    data = gen.generate()
    items = data["batch_2"]["order_items"]
    ec = gen.EDGE_CASES

    duplicated = [i for i in items if i["order_item_id"] == ec["duplicate_order_item"]]
    assert len(duplicated) == 2, "the duplicate row must actually be duplicated"

    illegal = next(i for i in items if i["order_item_id"] == ec["illegal_quantity_order_item"])
    assert int(illegal["quantity"]) <= 0

    changed_ids = {c["customer_id"] for c in data["batch_2"]["customers"]}
    assert ec["email_change_customer"] in changed_ids
    assert ec["null_to_value_customer"] in changed_ids
    assert ec["new_customer"] in changed_ids


def test_null_to_value_customer_starts_null():
    data = gen.generate()
    customer_id = gen.EDGE_CASES["null_to_value_customer"]
    before = next(c for c in data["batch_1"]["customers"] if c["customer_id"] == customer_id)
    after = next(c for c in data["batch_2"]["customers"] if c["customer_id"] == customer_id)
    assert before["region"] == ""
    assert after["region"] != ""
