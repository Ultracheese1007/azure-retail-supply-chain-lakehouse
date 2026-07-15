# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: clean products
# MAGIC
# MAGIC Reads `bronze.products`, types and standardises it, deduplicates by
# MAGIC `product_id`, quarantines rows that fail validation, and writes
# MAGIC `silver.products_clean`.

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

df = silver.read_bronze(spark, catalog, "products")
df = silver.trim_to_null(df, ["product_name", "category"])
df = (
    df.withColumn("unit_price", F.col("unit_price").cast("double"))
    .withColumn("updated_at", F.to_timestamp("updated_at"))
)
df = silver.dedup_latest(df, keys=["product_id"], order_col="updated_at")

# COMMAND ----------

valid_condition = (
    F.col("product_id").isNotNull()
    & F.col("unit_price").isNotNull()
    & (F.col("unit_price") >= 0)
)
valid, quarantined = silver.split_quarantine(
    df, valid_condition, reason="null product_id / missing or negative unit_price"
)

# COMMAND ----------

silver.write_silver(valid, catalog, "products_clean")
silver.write_quarantine(quarantined, catalog, "products")

# COMMAND ----------

display(spark.table(f"{catalog}.silver.products_clean"))
