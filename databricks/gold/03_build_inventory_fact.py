# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: build the inventory-movement fact
# MAGIC
# MAGIC Builds `fct_inventory_movements` at **event grain**: one row per row in
# MAGIC `silver.inventory_events_clean`.
# MAGIC
# MAGIC Inventory events reference warehouse and product only, so no point-in-time
# MAGIC customer join applies here. The dimensions joined are Type 1 (current state
# MAGIC only), which is why a plain business-key join is correct in this case and
# MAGIC cannot fan out.
# MAGIC
# MAGIC `quantity_change` is signed: INBOUND is positive, OUTBOUND is negative, and
# MAGIC ADJUSTMENT can be either (the sign rules are enforced in Silver). Summing it
# MAGIC per warehouse and product therefore gives the net stock movement directly.

# COMMAND ----------

dbutils.widgets.text("catalog", "retail_lakehouse")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.gold.fct_inventory_movements AS
SELECT
    e.event_id,
    w.warehouse_sk,
    p.product_sk,
    CAST(date_format(e.event_timestamp, 'yyyyMMdd') AS INT) AS date_key,
    e.event_timestamp,
    e.event_type,
    e.quantity_change
FROM {catalog}.silver.inventory_events_clean e
LEFT JOIN {catalog}.gold.dim_warehouse w
    ON e.warehouse_id = w.warehouse_id
LEFT JOIN {catalog}.gold.dim_product p
    ON e.product_id = p.product_id
""")

display(spark.sql(f"SELECT * FROM {catalog}.gold.fct_inventory_movements ORDER BY event_timestamp"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Assertion: grain is preserved

# COMMAND ----------

source_rows = spark.table(f"{catalog}.silver.inventory_events_clean").count()
fact_rows = spark.table(f"{catalog}.gold.fct_inventory_movements").count()
print(f"silver.inventory_events_clean: {source_rows}   gold.fct_inventory_movements: {fact_rows}")
if fact_rows != source_rows:
    raise AssertionError(f"grain broken: {source_rows} events produced {fact_rows} fact rows")

# COMMAND ----------

duplicates = spark.sql(f"""
SELECT event_id, COUNT(*) AS n
FROM {catalog}.gold.fct_inventory_movements
GROUP BY event_id
HAVING COUNT(*) > 1
""")
if duplicates.count() > 0:
    display(duplicates)
    raise AssertionError("fct_inventory_movements: events are duplicated")
print("OK: one fact row per inventory event")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Assertion: every event resolved its dimensions

# COMMAND ----------

orphans = spark.sql(f"""
SELECT event_id, event_type
FROM {catalog}.gold.fct_inventory_movements
WHERE warehouse_sk IS NULL OR product_sk IS NULL
""")
if orphans.count() > 0:
    display(orphans)
    raise AssertionError("fct_inventory_movements: events with unresolved warehouse or product")
print("OK: every event resolved warehouse and product")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Net stock movement per warehouse and product

# COMMAND ----------

display(
    spark.sql(f"""
    SELECT w.warehouse_name, p.product_name, SUM(f.quantity_change) AS net_movement
    FROM {catalog}.gold.fct_inventory_movements f
    JOIN {catalog}.gold.dim_warehouse w ON f.warehouse_sk = w.warehouse_sk
    JOIN {catalog}.gold.dim_product p ON f.product_sk = p.product_sk
    GROUP BY w.warehouse_name, p.product_name
    ORDER BY w.warehouse_name, p.product_name
    """)
)
