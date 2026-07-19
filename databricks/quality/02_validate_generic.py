# Databricks notebook source
# MAGIC %md
# MAGIC # Generic lakehouse validation (Azure target — statically defined, not executed)
# MAGIC
# MAGIC The demo validation (`databricks/quality/01_validate_pipeline.py`) asserts
# MAGIC facts that are only true for the generated data: specific customer IDs, the
# MAGIC planted NULL→value change, and exact row counts. Those cannot hold against
# MAGIC arbitrary Azure source data.
# MAGIC
# MAGIC This notebook checks the same **structural invariants** without depending on
# MAGIC any particular record: whatever the data, the model must still be internally
# MAGIC consistent. It reuses the `CheckSuite` framework so a failure raises and
# MAGIC fails the run.
# MAGIC
# MAGIC **Status:** statically defined for the Azure target; not executed.

# COMMAND ----------

import os
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.getcwd()))
SRC = os.path.join(REPO_ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from retail_lakehouse.validation import CheckSuite  # noqa: E402

# COMMAND ----------

dbutils.widgets.text("catalog", "retail_lakehouse")
catalog = dbutils.widgets.get("catalog")

suite = CheckSuite(name=f"{catalog} generic lakehouse validation")


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
# MAGIC ## 2. SCD2 structural invariants (data-independent)

# COMMAND ----------

suite.expect_zero(
    "each customer has exactly one current version",
    count_of(f"""
        SELECT customer_id FROM {catalog}.silver.customers_history
        WHERE is_current = true GROUP BY customer_id HAVING COUNT(*) <> 1
    """),
)

suite.expect_zero(
    "no customer is missing a current version",
    count_of(f"""
        SELECT DISTINCT customer_id FROM {catalog}.silver.customers_history
        WHERE customer_id NOT IN (
            SELECT customer_id FROM {catalog}.silver.customers_history WHERE is_current = true
        )
    """),
)

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Fact grain is preserved (data-independent)

# COMMAND ----------

suite.expect_equal(
    "fct_sales preserves order-line grain",
    spark.table(f"{catalog}.gold.fct_sales").count(),
    spark.table(f"{catalog}.silver.order_items_clean").count(),
    detail="fct_sales row count differs from silver.order_items_clean (customer join fan-out)",
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
# MAGIC ## 4. Referential integrity (data-independent)

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
# MAGIC ## 5. Business rules (data-independent)

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

suite.raise_if_failed()
