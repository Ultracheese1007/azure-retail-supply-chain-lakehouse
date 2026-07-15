# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: clean warehouses
# MAGIC
# MAGIC Reads `bronze.warehouses`, standardises it, deduplicates by
# MAGIC `warehouse_id`, quarantines invalid rows, writes `silver.warehouses_clean`.

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

df = silver.read_bronze(spark, catalog, "warehouses")
df = silver.trim_to_null(df, ["warehouse_name", "city", "region"])
df = silver.dedup_latest(df, keys=["warehouse_id"])

# COMMAND ----------

valid_condition = F.col("warehouse_id").isNotNull() & F.col("warehouse_name").isNotNull()
valid, quarantined = silver.split_quarantine(
    df, valid_condition, reason="null warehouse_id or warehouse_name"
)

# COMMAND ----------

silver.write_silver(valid, catalog, "warehouses_clean")
silver.write_quarantine(quarantined, catalog, "warehouses")

# COMMAND ----------

display(spark.table(f"{catalog}.silver.warehouses_clean"))
