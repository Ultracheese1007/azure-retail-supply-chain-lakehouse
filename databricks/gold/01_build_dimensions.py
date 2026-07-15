# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: build dimensions
# MAGIC
# MAGIC Builds `dim_customer`, `dim_product`, `dim_warehouse` and `dim_date`.
# MAGIC
# MAGIC ## Design
# MAGIC
# MAGIC **Full deterministic rebuild.** Every Gold table is rebuilt from Silver with
# MAGIC `CREATE OR REPLACE TABLE`. For a portfolio-scale dataset this is simpler and
# MAGIC safer than incremental MERGE into every dimension and fact: reruns are
# MAGIC trivially idempotent, and the model can never drift from Silver. The
# MAGIC trade-off is documented in the README (Gold is not incrementally loaded).
# MAGIC
# MAGIC **Hash surrogate keys.** `dim_product` / `dim_warehouse` keys are
# MAGIC `md5(business_key)`, and `dim_customer` reuses the `customer_sk` already
# MAGIC assigned by the SCD2 build (`md5(customer_id | version start)`). A sequence
# MAGIC or `ROW_NUMBER()` would be reassigned on every full rebuild — and, for a
# MAGIC versioned dimension, would depend on the ordering of versions — so facts
# MAGIC rebuilt at a different time could point at different rows. Hashing the
# MAGIC business key makes the keys stable across rebuilds and reproducible.
# MAGIC
# MAGIC **`dim_customer` keeps every version.** It is a Type 2 dimension: all
# MAGIC historical versions are present, each with its validity window. Facts must
# MAGIC therefore join on the validity window, not on `customer_id` alone — see
# MAGIC `02_build_sales_fact.py`.

# COMMAND ----------

dbutils.widgets.text("catalog", "retail_lakehouse")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_customer — Type 2, all versions

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.gold.dim_customer AS
SELECT
    customer_sk,
    customer_id,
    email,
    customer_name,
    segment,
    city,
    region,
    effective_start_timestamp,
    effective_end_timestamp,
    is_current
FROM {catalog}.silver.customers_history
""")
display(
    spark.sql(
        f"SELECT * FROM {catalog}.gold.dim_customer "
        "ORDER BY customer_id, effective_start_timestamp"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_product

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.gold.dim_product AS
SELECT
    md5(product_id) AS product_sk,
    product_id,
    product_name,
    category,
    unit_price AS list_unit_price
FROM {catalog}.silver.products_clean
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_warehouse

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.gold.dim_warehouse AS
SELECT
    md5(warehouse_id) AS warehouse_sk,
    warehouse_id,
    warehouse_name,
    city,
    region
FROM {catalog}.silver.warehouses_clean
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_date
# MAGIC
# MAGIC Spans every date present in the transactional Silver tables, so both facts
# MAGIC always find a matching date row.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.gold.dim_date AS
WITH bounds AS (
    SELECT
        LEAST(
            (SELECT MIN(CAST(order_timestamp AS DATE)) FROM {catalog}.silver.orders_clean),
            (SELECT MIN(CAST(event_timestamp AS DATE)) FROM {catalog}.silver.inventory_events_clean)
        ) AS min_date,
        GREATEST(
            (SELECT MAX(CAST(order_timestamp AS DATE)) FROM {catalog}.silver.orders_clean),
            (SELECT MAX(CAST(event_timestamp AS DATE)) FROM {catalog}.silver.inventory_events_clean)
        ) AS max_date
),
calendar AS (
    SELECT explode(sequence(min_date, max_date, INTERVAL 1 DAY)) AS date
    FROM bounds
)
SELECT
    CAST(date_format(date, 'yyyyMMdd') AS INT) AS date_key,
    date                                       AS date,
    YEAR(date)                                 AS year,
    QUARTER(date)                              AS quarter,
    MONTH(date)                                AS month,
    DAY(date)                                  AS day_of_month,
    date_format(date, 'EEEE')                  AS day_name,
    DAYOFWEEK(date) IN (1, 7)                  AS is_weekend
FROM calendar
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity checks
# MAGIC
# MAGIC Fail loudly rather than publishing a broken dimension.

# COMMAND ----------

for table, key in [
    ("dim_customer", "customer_sk"),
    ("dim_product", "product_sk"),
    ("dim_warehouse", "warehouse_sk"),
    ("dim_date", "date_key"),
]:
    total = spark.table(f"{catalog}.gold.{table}").count()
    distinct = spark.table(f"{catalog}.gold.{table}").select(key).distinct().count()
    if total == 0:
        raise AssertionError(f"{table} is empty")
    if total != distinct:
        raise AssertionError(f"{table}.{key} is not unique ({total} rows, {distinct} keys)")
    print(f"OK {table}: {total} rows, unique {key}")

# COMMAND ----------

# One current version per customer must survive into the dimension.
bad = spark.sql(f"""
SELECT customer_id, COUNT(*) AS current_versions
FROM {catalog}.gold.dim_customer
WHERE is_current = true
GROUP BY customer_id
HAVING COUNT(*) <> 1
""")
if bad.count() > 0:
    display(bad)
    raise AssertionError("dim_customer: a customer does not have exactly one current version")
print("OK dim_customer: exactly one current version per customer")
