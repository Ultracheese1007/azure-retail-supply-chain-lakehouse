# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: clean order items
# MAGIC
# MAGIC Reads `bronze.order_items`, types quantity/price, deduplicates by
# MAGIC `order_item_id`, quarantines rows with non-positive quantity, writes
# MAGIC `silver.order_items_clean`.
# MAGIC
# MAGIC This is where two deliberate batch-2 edge cases are resolved:
# MAGIC the duplicated `OI2001` collapses to one row, and the illegal-quantity
# MAGIC `OI2002` (quantity <= 0) is routed to quarantine.

# COMMAND ----------

import os
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.getcwd()))
SRC = os.path.join(REPO_ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from pyspark.sql import functions as F  # noqa: E402

from retail_lakehouse import silver  # noqa: E402

# COMMAND ----------

dbutils.widgets.text("catalog", "retail_lakehouse")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

df = silver.read_bronze(spark, catalog, "order_items")
df = (
    df.withColumn("quantity", F.col("quantity").cast("int"))
    .withColumn("unit_price", F.col("unit_price").cast("double"))
)
# Dedup collapses the deliberately duplicated OI2001.
df = silver.dedup_latest(df, keys=["order_item_id"])

# COMMAND ----------

valid_condition = (
    F.col("order_item_id").isNotNull()
    & F.col("order_id").isNotNull()
    & F.col("product_id").isNotNull()
    & F.col("quantity").isNotNull()
    & (F.col("quantity") > 0)
    & (F.col("unit_price") >= 0)
)
valid, quarantined = silver.split_quarantine(
    df, valid_condition, reason="null keys / non-positive quantity / negative price"
)

# COMMAND ----------

silver.write_silver(valid, catalog, "order_items_clean")
silver.write_quarantine(quarantined, catalog, "order_items")

# COMMAND ----------

# MAGIC %md
# MAGIC Expect the illegal-quantity row (OI2002) to appear here.

# COMMAND ----------

display(spark.table(f"{catalog}.quarantine.order_items"))
