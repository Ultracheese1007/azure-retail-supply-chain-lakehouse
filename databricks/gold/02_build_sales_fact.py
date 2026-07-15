# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: build the sales fact
# MAGIC
# MAGIC Builds `fct_sales` at **order-line grain**: exactly one row per row in
# MAGIC `silver.order_items_clean`.
# MAGIC
# MAGIC ## The point-in-time join
# MAGIC
# MAGIC `dim_customer` is a Type 2 dimension, so one `customer_id` can have several
# MAGIC rows. Joining on `customer_id` alone would match *every* version of that
# MAGIC customer and multiply the order line — a customer with two versions would
# MAGIC turn one sale into two, silently inflating revenue. The join must therefore
# MAGIC also select the version that was in effect when the order was placed:
# MAGIC
# MAGIC ```
# MAGIC ON  o.customer_id = c.customer_id
# MAGIC AND o.order_timestamp >= c.effective_start_timestamp
# MAGIC AND o.order_timestamp <  c.effective_end_timestamp
# MAGIC ```
# MAGIC
# MAGIC Because current versions carry an end-of-time sentinel rather than NULL, the
# MAGIC range works without any COALESCE. The bounds are half-open (`>=` start,
# MAGIC `<` end) so that the instant a version is replaced belongs to exactly one
# MAGIC version — adjacent windows can never both match.
# MAGIC
# MAGIC This is what lets the model answer "what was true *at the time of the
# MAGIC order*" rather than "what is true now": order `O1001` resolves to the
# MAGIC customer's original email, while `O2001` — placed after the change —
# MAGIC resolves to the new one.
# MAGIC
# MAGIC A `LEFT JOIN` is used deliberately: an order that resolves to no customer
# MAGIC version is a real defect, and the assertion at the end of this notebook
# MAGIC catches it. An inner join would hide it by dropping the row.

# COMMAND ----------

dbutils.widgets.text("catalog", "retail_lakehouse")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.gold.fct_sales AS
SELECT
    oi.order_item_id,
    oi.order_id,
    c.customer_sk,
    p.product_sk,
    w.warehouse_sk,
    CAST(date_format(o.order_timestamp, 'yyyyMMdd') AS INT) AS date_key,
    o.order_timestamp,
    o.status                                    AS order_status,
    oi.quantity,
    oi.unit_price,
    ROUND(oi.quantity * oi.unit_price, 2)       AS line_amount
FROM {catalog}.silver.order_items_clean oi
JOIN {catalog}.silver.orders_clean o
    ON oi.order_id = o.order_id
LEFT JOIN {catalog}.gold.dim_customer c
    ON o.customer_id = c.customer_id
   AND o.order_timestamp >= c.effective_start_timestamp
   AND o.order_timestamp <  c.effective_end_timestamp
LEFT JOIN {catalog}.gold.dim_product p
    ON oi.product_id = p.product_id
LEFT JOIN {catalog}.gold.dim_warehouse w
    ON o.warehouse_id = w.warehouse_id
""")

display(spark.sql(f"SELECT * FROM {catalog}.gold.fct_sales ORDER BY order_item_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Assertion: grain is preserved (no fan-out)
# MAGIC
# MAGIC If the point-in-time predicate were wrong, a customer with two versions
# MAGIC would duplicate their order lines. Comparing the row count against the
# MAGIC Silver source is the direct test of that.

# COMMAND ----------

source_rows = spark.table(f"{catalog}.silver.order_items_clean").count()
fact_rows = spark.table(f"{catalog}.gold.fct_sales").count()
print(f"silver.order_items_clean: {source_rows}   gold.fct_sales: {fact_rows}")
if fact_rows != source_rows:
    raise AssertionError(
        f"fan-out: {source_rows} order lines produced {fact_rows} fact rows — "
        "the customer join is matching more than one version per order"
    )

# COMMAND ----------

duplicates = spark.sql(f"""
SELECT order_item_id, COUNT(*) AS n
FROM {catalog}.gold.fct_sales
GROUP BY order_item_id
HAVING COUNT(*) > 1
""")
if duplicates.count() > 0:
    display(duplicates)
    raise AssertionError("fct_sales: order lines are duplicated")
print("OK: one fact row per order line")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Assertion: every order line resolved a customer version

# COMMAND ----------

orphans = spark.sql(f"""
SELECT order_item_id, order_id, order_timestamp
FROM {catalog}.gold.fct_sales
WHERE customer_sk IS NULL
""")
if orphans.count() > 0:
    display(orphans)
    raise AssertionError(
        "fct_sales: order lines with no customer version in effect at order time"
    )
print("OK: every order line resolved to a customer version")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evidence: the same customer, two versions, two orders
# MAGIC
# MAGIC Orders placed before the change resolve to the old version; orders placed
# MAGIC after resolve to the new one.

# COMMAND ----------

display(
    spark.sql(f"""
    SELECT f.order_item_id, f.order_timestamp, c.customer_id, c.email, f.line_amount
    FROM {catalog}.gold.fct_sales f
    JOIN {catalog}.gold.dim_customer c ON f.customer_sk = c.customer_sk
    WHERE c.customer_id IN (
        SELECT customer_id FROM {catalog}.gold.dim_customer
        GROUP BY customer_id HAVING COUNT(*) > 1
    )
    ORDER BY c.customer_id, f.order_timestamp
    """)
)
