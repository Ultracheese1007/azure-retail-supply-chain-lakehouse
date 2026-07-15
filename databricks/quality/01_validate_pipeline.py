# Databricks notebook source
# MAGIC %md
# MAGIC # Validate the lakehouse
# MAGIC
# MAGIC An executable validation gate. Every check either passes or **fails the
# MAGIC run** — `raise_if_failed()` raises at the end, so a workflow using this as a
# MAGIC task will go red rather than quietly publishing bad data.
# MAGIC
# MAGIC All checks run before anything is raised, so one execution reports every
# MAGIC problem rather than stopping at the first.
# MAGIC
# MAGIC ## What is checked
# MAGIC
# MAGIC | # | Check |
# MAGIC |---|-------|
# MAGIC | 1 | Core tables are not empty |
# MAGIC | 2 | Each customer has exactly one current version |
# MAGIC | 3 | Customer validity windows never overlap |
# MAGIC | 4 | The email change produced two versions |
# MAGIC | 5 | The NULL -> value change was detected |
# MAGIC | 6 | Each order line appears once in `fct_sales` |
# MAGIC | 7 | Fact foreign keys resolve to dimensions |
# MAGIC | 8 | Quantities and amounts obey business rules |
# MAGIC | 9 | Row counts are stable across reruns |
# MAGIC | 10 | Quarantine contains the deliberately invalid record |

# COMMAND ----------

import os
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.getcwd()))
SRC = os.path.join(REPO_ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from retail_lakehouse import generate_source_data as gen  # noqa: E402
from retail_lakehouse.validation import CheckSuite  # noqa: E402

# COMMAND ----------

dbutils.widgets.text("catalog", "retail_lakehouse")
catalog = dbutils.widgets.get("catalog")

suite = CheckSuite(name=f"{catalog} lakehouse validation")
EDGE = gen.EDGE_CASES


def scalar(query: str):
    """Run a query expected to return a single value."""
    return spark.sql(query).collect()[0][0]


def count_of(query: str) -> int:
    return spark.sql(query).count()


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Core tables are not empty

# COMMAND ----------

CORE_TABLES = [
    "silver.customers_history",
    "silver.products_clean",
    "silver.warehouses_clean",
    "silver.orders_clean",
    "silver.order_items_clean",
    "silver.inventory_events_clean",
    "gold.dim_customer",
    "gold.dim_product",
    "gold.dim_warehouse",
    "gold.dim_date",
    "gold.fct_sales",
    "gold.fct_inventory_movements",
]

for table in CORE_TABLES:
    n = spark.table(f"{catalog}.{table}").count()
    suite.record(f"{table} is not empty", n > 0, f"{table} has {n} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Exactly one current version per customer

# COMMAND ----------

suite.expect_zero(
    "each customer has exactly one current version",
    count_of(f"""
        SELECT customer_id
        FROM {catalog}.silver.customers_history
        WHERE is_current = true
        GROUP BY customer_id
        HAVING COUNT(*) <> 1
    """),
)

# A customer must never be left with no current version at all — the failure
# mode of expiring a record without inserting its replacement.
suite.expect_zero(
    "no customer is missing a current version",
    count_of(f"""
        SELECT DISTINCT customer_id
        FROM {catalog}.silver.customers_history
        WHERE customer_id NOT IN (
            SELECT customer_id FROM {catalog}.silver.customers_history WHERE is_current = true
        )
    """),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Validity windows never overlap
# MAGIC
# MAGIC Two versions of one customer must not both be valid at the same instant,
# MAGIC or a point-in-time join could match both.

# COMMAND ----------

suite.expect_zero(
    "customer validity windows do not overlap",
    count_of(f"""
        SELECT a.customer_id
        FROM {catalog}.silver.customers_history a
        JOIN {catalog}.silver.customers_history b
          ON a.customer_id = b.customer_id
         AND a.customer_sk <> b.customer_sk
         AND a.effective_start_timestamp < b.effective_end_timestamp
         AND b.effective_start_timestamp < a.effective_end_timestamp
    """),
)

# Each version must also start where the previous one ended (no gaps).
suite.expect_zero(
    "customer validity windows have no gaps",
    count_of(f"""
        WITH windows AS (
            SELECT
                customer_id,
                effective_end_timestamp,
                LEAD(effective_start_timestamp) OVER (
                    PARTITION BY customer_id ORDER BY effective_start_timestamp
                ) AS next_start
            FROM {catalog}.silver.customers_history
        )
        SELECT customer_id FROM windows
        WHERE next_start IS NOT NULL AND next_start <> effective_end_timestamp
    """),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 & 5. The planted customer changes were detected
# MAGIC
# MAGIC Check 5 is the null-safe one: an ordinary `<>` comparison evaluates
# MAGIC `NULL <> 'value'` to NULL rather than TRUE and would miss the change
# MAGIC entirely, leaving this customer with a single version.

# COMMAND ----------

suite.expect_equal(
    "email change produced two customer versions",
    scalar(f"""
        SELECT COUNT(*) FROM {catalog}.silver.customers_history
        WHERE customer_id = '{EDGE["email_change_customer"]}'
    """),
    2,
)

suite.expect_equal(
    "NULL -> value change was detected (null-safe comparison)",
    scalar(f"""
        SELECT COUNT(*) FROM {catalog}.silver.customers_history
        WHERE customer_id = '{EDGE["null_to_value_customer"]}'
    """),
    2,
)

# The older version must be the one holding the NULL.
suite.expect_equal(
    "the superseded version retains its NULL attribute",
    scalar(f"""
        SELECT COUNT(*) FROM {catalog}.silver.customers_history
        WHERE customer_id = '{EDGE["null_to_value_customer"]}'
          AND is_current = false AND region IS NULL
    """),
    1,
)

suite.expect_equal(
    "new customer has exactly one version",
    scalar(f"""
        SELECT COUNT(*) FROM {catalog}.silver.customers_history
        WHERE customer_id = '{EDGE["new_customer"]}'
    """),
    1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Each order line appears exactly once in fct_sales
# MAGIC
# MAGIC The direct fan-out test: a Type 2 dimension joined without a validity
# MAGIC window would multiply order lines and inflate revenue.

# COMMAND ----------

suite.expect_equal(
    "fct_sales preserves order-line grain",
    spark.table(f"{catalog}.gold.fct_sales").count(),
    spark.table(f"{catalog}.silver.order_items_clean").count(),
    detail="fct_sales row count differs from silver.order_items_clean — the customer join fans out",
)

suite.expect_zero(
    "no order line is duplicated in fct_sales",
    count_of(f"""
        SELECT order_item_id FROM {catalog}.gold.fct_sales
        GROUP BY order_item_id HAVING COUNT(*) > 1
    """),
)

suite.expect_equal(
    "fct_inventory_movements preserves event grain",
    spark.table(f"{catalog}.gold.fct_inventory_movements").count(),
    spark.table(f"{catalog}.silver.inventory_events_clean").count(),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6b. Point-in-time joins resolved the right versions
# MAGIC
# MAGIC The order placed before the change must see the old version, and the one
# MAGIC placed after must see the new one. Without this, the fact table would
# MAGIC still have the right *number* of rows while pointing at the wrong history.

# COMMAND ----------

suite.expect_equal(
    "order before the change resolves to the superseded version",
    scalar(f"""
        SELECT COUNT(*)
        FROM {catalog}.gold.fct_sales f
        JOIN {catalog}.gold.dim_customer c ON f.customer_sk = c.customer_sk
        WHERE f.order_id = '{EDGE["order_before_change"]}' AND c.is_current = false
    """) > 0,
    True,
)

suite.expect_equal(
    "order after the change resolves to the current version",
    scalar(f"""
        SELECT COUNT(*)
        FROM {catalog}.gold.fct_sales f
        JOIN {catalog}.gold.dim_customer c ON f.customer_sk = c.customer_sk
        WHERE f.order_id = '{EDGE["order_after_change"]}' AND c.is_current = true
    """) > 0,
    True,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Fact foreign keys resolve to dimensions

# COMMAND ----------

suite.expect_zero(
    "fct_sales rows all resolve a customer version",
    count_of(f"SELECT order_item_id FROM {catalog}.gold.fct_sales WHERE customer_sk IS NULL"),
)

for fact, key, dim, dim_key in [
    ("fct_sales", "product_sk", "dim_product", "product_sk"),
    ("fct_sales", "warehouse_sk", "dim_warehouse", "warehouse_sk"),
    ("fct_sales", "date_key", "dim_date", "date_key"),
    ("fct_inventory_movements", "warehouse_sk", "dim_warehouse", "warehouse_sk"),
    ("fct_inventory_movements", "product_sk", "dim_product", "product_sk"),
    ("fct_inventory_movements", "date_key", "dim_date", "date_key"),
]:
    suite.expect_zero(
        f"{fact}.{key} resolves to {dim}",
        count_of(f"""
            SELECT f.{key} FROM {catalog}.gold.{fact} f
            LEFT JOIN {catalog}.gold.{dim} d ON f.{key} = d.{dim_key}
            WHERE d.{dim_key} IS NULL
        """),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Business rules on quantities and amounts

# COMMAND ----------

suite.expect_zero(
    "fct_sales quantities are positive",
    count_of(f"SELECT order_item_id FROM {catalog}.gold.fct_sales WHERE quantity <= 0"),
)

suite.expect_zero(
    "fct_sales prices and amounts are non-negative",
    count_of(f"""
        SELECT order_item_id FROM {catalog}.gold.fct_sales
        WHERE unit_price < 0 OR line_amount < 0
    """),
)

suite.expect_zero(
    "fct_sales line_amount equals quantity * unit_price",
    count_of(f"""
        SELECT order_item_id FROM {catalog}.gold.fct_sales
        WHERE ABS(line_amount - ROUND(quantity * unit_price, 2)) > 0.01
    """),
)

suite.expect_zero(
    "inventory movement signs follow their event type",
    count_of(f"""
        SELECT event_id FROM {catalog}.gold.fct_inventory_movements
        WHERE (event_type = 'INBOUND' AND quantity_change <= 0)
           OR (event_type = 'OUTBOUND' AND quantity_change >= 0)
    """),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Row counts are stable across reruns
# MAGIC
# MAGIC Each validation run appends its row counts to a log table. When a previous
# MAGIC run exists, the current counts must match it: re-running the same batch
# MAGIC must not create extra customer versions or duplicate facts. On the very
# MAGIC first run there is nothing to compare against, so the check is skipped
# MAGIC rather than failed — run the pipeline twice to exercise it.

# COMMAND ----------

TRACKED_FOR_STABILITY = [
    "silver.customers_history",
    "silver.order_items_clean",
    "gold.fct_sales",
    "gold.fct_inventory_movements",
]

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.validation_run_log (
    run_timestamp TIMESTAMP,
    table_name    STRING,
    row_count     BIGINT
) USING DELTA
""")

current_counts = {t: spark.table(f"{catalog}.{t}").count() for t in TRACKED_FOR_STABILITY}

previous = spark.sql(f"""
WITH last_run AS (
    SELECT MAX(run_timestamp) AS ts FROM {catalog}.gold.validation_run_log
)
SELECT table_name, row_count
FROM {catalog}.gold.validation_run_log
WHERE run_timestamp = (SELECT ts FROM last_run)
""").collect()

if not previous:
    suite.skip("row counts are stable across reruns", "no previous run recorded yet")
else:
    previous_counts = {row["table_name"]: row["row_count"] for row in previous}
    for table, count in current_counts.items():
        if table in previous_counts:
            suite.expect_equal(
                f"{table} row count is stable across reruns",
                count,
                previous_counts[table],
                detail=f"was {previous_counts[table]}, now {count} — rerun is not idempotent",
            )

# COMMAND ----------

rows = [(t, int(n)) for t, n in current_counts.items()]
log_df = (
    spark.createDataFrame(rows, "table_name STRING, row_count BIGINT")
    .selectExpr("current_timestamp() AS run_timestamp", "table_name", "row_count")
)
log_df.write.mode("append").saveAsTable(f"{catalog}.gold.validation_run_log")
print("recorded this run's row counts")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Quarantine holds the deliberately invalid record
# MAGIC
# MAGIC The generated batch 2 contains an order item with a negative quantity. It
# MAGIC must be in quarantine and must *not* have reached Silver — proving the
# MAGIC validation rules are actually routing bad data rather than passing it
# MAGIC through.

# COMMAND ----------

suite.expect_equal(
    "invalid quantity row is quarantined",
    scalar(f"""
        SELECT COUNT(*) FROM {catalog}.quarantine.order_items
        WHERE order_item_id = '{EDGE["illegal_quantity_order_item"]}'
    """),
    1,
)

suite.expect_equal(
    "invalid quantity row never reached Silver",
    scalar(f"""
        SELECT COUNT(*) FROM {catalog}.silver.order_items_clean
        WHERE order_item_id = '{EDGE["illegal_quantity_order_item"]}'
    """),
    0,
)

# The duplicated row must have collapsed to exactly one Silver row.
suite.expect_equal(
    "duplicated order item collapsed to a single row",
    scalar(f"""
        SELECT COUNT(*) FROM {catalog}.silver.order_items_clean
        WHERE order_item_id = '{EDGE["duplicate_order_item"]}'
    """),
    1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Result
# MAGIC
# MAGIC Raises `ValidationFailed` if anything above failed, so this notebook can be
# MAGIC used as a gate in a workflow.

# COMMAND ----------

suite.raise_if_failed()
