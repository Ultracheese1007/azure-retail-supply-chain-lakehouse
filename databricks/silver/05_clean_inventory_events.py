# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: clean inventory events
# MAGIC
# MAGIC Reads `bronze.inventory_events`, types quantities/timestamps, standardises
# MAGIC event type, deduplicates by `event_id`, quarantines invalid rows, writes
# MAGIC `silver.inventory_events_clean`.

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

ACCEPTED_EVENT_TYPES = ["INBOUND", "OUTBOUND", "ADJUSTMENT"]

# COMMAND ----------

df = silver.read_bronze(spark, catalog, "inventory_events")
df = (
    df.withColumn("quantity_change", F.col("quantity_change").cast("int"))
    .withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
    .withColumn("event_type", F.upper(F.trim(F.col("event_type"))))
)
df = silver.dedup_latest(df, keys=["event_id"], order_col="event_timestamp")

# COMMAND ----------

# Sign rules: INBOUND must be positive, OUTBOUND negative; ADJUSTMENT may be either.
sign_ok = (
    ((F.col("event_type") == "INBOUND") & (F.col("quantity_change") > 0))
    | ((F.col("event_type") == "OUTBOUND") & (F.col("quantity_change") < 0))
    | (F.col("event_type") == "ADJUSTMENT")
)
valid_condition = (
    F.col("event_id").isNotNull()
    & F.col("event_timestamp").isNotNull()
    & F.col("event_type").isin(ACCEPTED_EVENT_TYPES)
    & F.col("quantity_change").isNotNull()
    & sign_ok
)
valid, quarantined = silver.split_quarantine(
    df, valid_condition, reason="null keys / invalid type / sign rule violation"
)

# COMMAND ----------

silver.write_silver(valid, catalog, "inventory_events_clean")
silver.write_quarantine(quarantined, catalog, "inventory_events")

# COMMAND ----------

display(spark.table(f"{catalog}.silver.inventory_events_clean"))
