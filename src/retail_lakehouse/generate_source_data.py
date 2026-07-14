"""Deterministic synthetic source-data generator for the retail lakehouse.

Produces two batches of related retail entities from a fixed random seed, so
regenerating always yields identical output. Batch 2 deliberately plants the
edge cases that the Silver, SCD Type 2 and data-quality layers must handle:

  * a customer whose email changes
  * a customer whose region goes from NULL to a value
  * a brand-new customer
  * an order placed *before* a customer change (joins to the old version)
  * an order placed *after* a customer change (joins to the new version)
  * a duplicated order-item row (deduplication test)
  * an order-item with an illegal quantity (quarantine / validation test)

The generator has no Spark or Databricks dependency, so it runs and can be
tested with plain Python. Timestamps are fixed constants, never `now()`.
"""
from __future__ import annotations

import csv
import json
import os
from datetime import datetime

SEED = 42

# Fixed reference timestamps per batch (never datetime.now()).
BATCH1_TS = datetime(2024, 1, 15, 10, 0, 0)
BATCH2_TS = datetime(2024, 2, 15, 10, 0, 0)

# Identifiers of the deliberately-planted edge cases, exposed by name so tests
# and documentation can reference them instead of using magic strings.
EDGE_CASES = {
    "email_change_customer": "C001",
    "null_to_value_customer": "C002",
    "new_customer": "C006",
    "order_before_change": "O1001",       # C001 order in batch 1 (old version)
    "order_after_change": "O2001",        # C001 order in batch 2 (new version)
    "duplicate_order_item": "OI2001",     # emitted twice in batch 2
    "illegal_quantity_order_item": "OI2002",  # quantity <= 0
}

# Column order per entity (kept explicit so CSV output is stable).
COLUMNS = {
    "customers": ["customer_id", "email", "customer_name", "segment", "city", "region", "updated_at"],
    "products": ["product_id", "product_name", "category", "unit_price", "updated_at"],
    "warehouses": ["warehouse_id", "warehouse_name", "city", "region"],
    "orders": ["order_id", "customer_id", "warehouse_id", "order_timestamp", "status"],
    "order_items": ["order_item_id", "order_id", "product_id", "quantity", "unit_price"],
    "inventory_events": ["event_id", "warehouse_id", "product_id", "event_type", "quantity_change", "event_timestamp"],
}


def _iso(ts: datetime) -> str:
    return ts.isoformat(sep=" ")


def _batch1() -> dict[str, list[dict]]:
    """Initial load. C002.region is intentionally NULL (empty) here."""
    ts = _iso(BATCH1_TS)
    customers = [
        {"customer_id": "C001", "email": "c001@old.example", "customer_name": "Anna Bakker",
         "segment": "Consumer", "city": "Amsterdam", "region": "North Holland", "updated_at": ts},
        {"customer_id": "C002", "email": "c002@example.com", "customer_name": "Bram de Vries",
         "segment": "Corporate", "city": "Rotterdam", "region": "", "updated_at": ts},  # region NULL
        {"customer_id": "C003", "email": "c003@example.com", "customer_name": "Carla Jansen",
         "segment": "Consumer", "city": "Utrecht", "region": "Utrecht", "updated_at": ts},
        {"customer_id": "C004", "email": "c004@example.com", "customer_name": "Dirk Smit",
         "segment": "Home Office", "city": "Eindhoven", "region": "North Brabant", "updated_at": ts},
        {"customer_id": "C005", "email": "c005@example.com", "customer_name": "Eva Meijer",
         "segment": "Consumer", "city": "Groningen", "region": "Groningen", "updated_at": ts},
    ]
    products = [
        {"product_id": "P001", "product_name": "USB-C Cable", "category": "Accessories", "unit_price": "8.50", "updated_at": ts},
        {"product_id": "P002", "product_name": "Wireless Mouse", "category": "Peripherals", "unit_price": "19.99", "updated_at": ts},
        {"product_id": "P003", "product_name": "Mechanical Keyboard", "category": "Peripherals", "unit_price": "79.00", "updated_at": ts},
        {"product_id": "P004", "product_name": "27in Monitor", "category": "Displays", "unit_price": "229.00", "updated_at": ts},
        {"product_id": "P005", "product_name": "Laptop Stand", "category": "Accessories", "unit_price": "34.95", "updated_at": ts},
        {"product_id": "P006", "product_name": "Webcam 1080p", "category": "Peripherals", "unit_price": "45.00", "updated_at": ts},
    ]
    warehouses = [
        {"warehouse_id": "W01", "warehouse_name": "Amsterdam DC", "city": "Amsterdam", "region": "North Holland"},
        {"warehouse_id": "W02", "warehouse_name": "Eindhoven DC", "city": "Eindhoven", "region": "North Brabant"},
        {"warehouse_id": "W03", "warehouse_name": "Zwolle DC", "city": "Zwolle", "region": "Overijssel"},
    ]
    orders = [
        {"order_id": "O1001", "customer_id": "C001", "warehouse_id": "W01", "order_timestamp": _iso(datetime(2024, 1, 10, 9, 30)), "status": "COMPLETED"},
        {"order_id": "O1002", "customer_id": "C002", "warehouse_id": "W02", "order_timestamp": _iso(datetime(2024, 1, 11, 14, 5)), "status": "COMPLETED"},
        {"order_id": "O1003", "customer_id": "C003", "warehouse_id": "W01", "order_timestamp": _iso(datetime(2024, 1, 12, 16, 20)), "status": "COMPLETED"},
        {"order_id": "O1004", "customer_id": "C004", "warehouse_id": "W03", "order_timestamp": _iso(datetime(2024, 1, 13, 11, 0)), "status": "COMPLETED"},
        {"order_id": "O1005", "customer_id": "C005", "warehouse_id": "W02", "order_timestamp": _iso(datetime(2024, 1, 14, 10, 45)), "status": "COMPLETED"},
    ]
    order_items = [
        {"order_item_id": "OI1001", "order_id": "O1001", "product_id": "P001", "quantity": "2", "unit_price": "8.50"},
        {"order_item_id": "OI1002", "order_id": "O1001", "product_id": "P003", "quantity": "1", "unit_price": "79.00"},
        {"order_item_id": "OI1003", "order_id": "O1002", "product_id": "P004", "quantity": "1", "unit_price": "229.00"},
        {"order_item_id": "OI1004", "order_id": "O1003", "product_id": "P002", "quantity": "3", "unit_price": "19.99"},
        {"order_item_id": "OI1005", "order_id": "O1004", "product_id": "P005", "quantity": "2", "unit_price": "34.95"},
        {"order_item_id": "OI1006", "order_id": "O1005", "product_id": "P006", "quantity": "1", "unit_price": "45.00"},
    ]
    inventory_events = [
        {"event_id": "IE1001", "warehouse_id": "W01", "product_id": "P001", "event_type": "INBOUND", "quantity_change": "500", "event_timestamp": _iso(datetime(2024, 1, 2, 8, 0))},
        {"event_id": "IE1002", "warehouse_id": "W01", "product_id": "P003", "event_type": "INBOUND", "quantity_change": "120", "event_timestamp": _iso(datetime(2024, 1, 2, 8, 30))},
        {"event_id": "IE1003", "warehouse_id": "W02", "product_id": "P004", "event_type": "INBOUND", "quantity_change": "60", "event_timestamp": _iso(datetime(2024, 1, 3, 9, 0))},
        {"event_id": "IE1004", "warehouse_id": "W01", "product_id": "P001", "event_type": "OUTBOUND", "quantity_change": "-2", "event_timestamp": _iso(datetime(2024, 1, 10, 9, 35))},
        {"event_id": "IE1005", "warehouse_id": "W03", "product_id": "P005", "event_type": "INBOUND", "quantity_change": "80", "event_timestamp": _iso(datetime(2024, 1, 4, 10, 0))},
    ]
    return {
        "customers": customers, "products": products, "warehouses": warehouses,
        "orders": orders, "order_items": order_items, "inventory_events": inventory_events,
    }


def _batch2() -> dict[str, list[dict]]:
    """Delta load. Customers here are changed/new rows only (a change feed)."""
    ts = _iso(BATCH2_TS)
    customers = [
        # C001: email changed -> new SCD2 version
        {"customer_id": "C001", "email": "c001@new.example", "customer_name": "Anna Bakker",
         "segment": "Consumer", "city": "Amsterdam", "region": "North Holland", "updated_at": ts},
        # C002: region NULL -> value (null-safe change detection must catch this)
        {"customer_id": "C002", "email": "c002@example.com", "customer_name": "Bram de Vries",
         "segment": "Corporate", "city": "Rotterdam", "region": "South Holland", "updated_at": ts},
        # C006: brand-new customer
        {"customer_id": "C006", "email": "c006@example.com", "customer_name": "Femke Visser",
         "segment": "Consumer", "city": "Haarlem", "region": "North Holland", "updated_at": ts},
    ]
    products: list[dict] = []      # no product changes in batch 2
    warehouses: list[dict] = []    # no warehouse changes in batch 2
    orders = [
        # O2001: C001 order AFTER the email change -> must join to the new version
        {"order_id": "O2001", "customer_id": "C001", "warehouse_id": "W01", "order_timestamp": _iso(datetime(2024, 2, 12, 13, 15)), "status": "COMPLETED"},
        {"order_id": "O2002", "customer_id": "C006", "warehouse_id": "W02", "order_timestamp": _iso(datetime(2024, 2, 13, 10, 10)), "status": "COMPLETED"},
        {"order_id": "O2003", "customer_id": "C003", "warehouse_id": "W01", "order_timestamp": _iso(datetime(2024, 2, 14, 15, 40)), "status": "PENDING"},
    ]
    order_items = [
        {"order_item_id": "OI2001", "order_id": "O2001", "product_id": "P002", "quantity": "1", "unit_price": "19.99"},
        # OI2001 duplicated on purpose -> Silver dedup must collapse to one
        {"order_item_id": "OI2001", "order_id": "O2001", "product_id": "P002", "quantity": "1", "unit_price": "19.99"},
        # OI2002 illegal quantity (<= 0) -> must be quarantined, not loaded to Silver
        {"order_item_id": "OI2002", "order_id": "O2002", "product_id": "P004", "quantity": "-1", "unit_price": "229.00"},
        {"order_item_id": "OI2003", "order_id": "O2002", "product_id": "P001", "quantity": "4", "unit_price": "8.50"},
        {"order_item_id": "OI2004", "order_id": "O2003", "product_id": "P006", "quantity": "2", "unit_price": "45.00"},
    ]
    inventory_events = [
        {"event_id": "IE2001", "warehouse_id": "W01", "product_id": "P002", "event_type": "OUTBOUND", "quantity_change": "-1", "event_timestamp": _iso(datetime(2024, 2, 12, 13, 20))},
        {"event_id": "IE2002", "warehouse_id": "W02", "product_id": "P001", "event_type": "OUTBOUND", "quantity_change": "-4", "event_timestamp": _iso(datetime(2024, 2, 13, 10, 15))},
        {"event_id": "IE2003", "warehouse_id": "W02", "product_id": "P004", "event_type": "ADJUSTMENT", "quantity_change": "5", "event_timestamp": _iso(datetime(2024, 2, 14, 9, 0))},
    ]
    return {
        "customers": customers, "products": products, "warehouses": warehouses,
        "orders": orders, "order_items": order_items, "inventory_events": inventory_events,
    }


def generate(seed: int = SEED) -> dict[str, dict[str, list[dict]]]:
    """Return {'batch_1': {entity: rows}, 'batch_2': {entity: rows}}.

    `seed` is accepted for interface stability; the dataset is fully
    deterministic and does not currently depend on randomness.
    """
    return {"batch_1": _batch1(), "batch_2": _batch2()}


def write_csvs(output_dir: str, seed: int = SEED) -> dict[str, dict[str, int]]:
    """Write one CSV per entity per batch under output_dir/<batch>/<entity>.csv.

    Returns a summary of row counts: {batch: {entity: count}}.
    """
    data = generate(seed)
    summary: dict[str, dict[str, int]] = {}
    for batch, entities in data.items():
        batch_dir = os.path.join(output_dir, batch)
        os.makedirs(batch_dir, exist_ok=True)
        summary[batch] = {}
        for entity, rows in entities.items():
            path = os.path.join(batch_dir, f"{entity}.csv")
            with open(path, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=COLUMNS[entity])
                writer.writeheader()
                for row in rows:
                    writer.writerow(row)
            summary[batch][entity] = len(rows)
    return summary


def summarize(seed: int = SEED) -> dict:
    """Deterministic summary used as a committed test fixture."""
    data = generate(seed)
    return {
        "seed": seed,
        "edge_cases": EDGE_CASES,
        "row_counts": {
            batch: {entity: len(rows) for entity, rows in entities.items()}
            for batch, entities in data.items()
        },
    }


if __name__ == "__main__":
    out = os.environ.get("OUTPUT_DIR", "./generated_source_data")
    result = write_csvs(out)
    print(json.dumps({"output_dir": out, "row_counts": result}, indent=2))
