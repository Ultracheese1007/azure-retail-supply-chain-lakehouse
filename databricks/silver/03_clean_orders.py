# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: clean orders
# MAGIC
# MAGIC Reads `bronze.orders`, types timestamps, standardises status,
# MAGIC deduplicates by `order_id`, quarantines invalid rows, writes
# MAGIC `silver.orders_clean`.

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

ACCEPTED_STATUSES = ["COMPLETED", "PENDING", "CANCELLED"]

# COMMAND ----------

df = silver.read_bronze(spark, catalog, "orders")
df = (
    df.withColumn("order_timestamp", F.to_timestamp("order_timestamp"))
    .withColumn("status", F.upper(F.trim(F.col("status"))))
)
df = silver.dedup_latest(df, keys=["order_id"])

# COMMAND ----------

valid_condition = (
    F.col("order_id").isNotNull()
    & F.col("customer_id").isNotNull()
    & F.col("order_timestamp").isNotNull()
    & F.col("status").isin(ACCEPTED_STATUSES)
)
valid, quarantined = silver.split_quarantine(
    df, valid_condition, reason="null keys / invalid timestamp / unaccepted status"
)

# COMMAND ----------

silver.write_silver(valid, catalog, "orders_clean")
silver.write_quarantine(quarantined, catalog, "orders")

# COMMAND ----------

display(spark.table(f"{catalog}.silver.orders_clean"))
