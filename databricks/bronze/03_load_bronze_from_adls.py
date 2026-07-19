# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze ingestion from ADLS Gen2 (Azure target — statically defined, not executed)
# MAGIC
# MAGIC This is the Azure-target counterpart to `databricks/bronze/02_load_bronze.py`.
# MAGIC The demo loader reads generated CSVs from a managed Volume; this one reads
# MAGIC the **Parquet** that Azure Data Factory lands in ADLS Gen2, laid out as:
# MAGIC
# MAGIC ```
# MAGIC <landing_path>/<entity>/load_date=YYYY-MM-DD/*.parquet
# MAGIC ```
# MAGIC
# MAGIC It is not a drop-in replacement for the CSV loader: the format is Parquet,
# MAGIC the directory layout is date-partitioned, and typing comes from the Parquet
# MAGIC schema rather than an explicit CSV schema. It writes the same
# MAGIC `{catalog}.bronze.{entity}` Delta tables, so the existing Silver and Gold
# MAGIC transformations run unchanged on top of it.
# MAGIC
# MAGIC **Status:** statically defined for the Azure target. It has not been
# MAGIC executed, because no ADF ingestion has populated an ADLS landing zone.
# MAGIC
# MAGIC ## Partition handling
# MAGIC
# MAGIC * **Incremental entities** (customers, products, orders, inventory_events)
# MAGIC   accumulate across load dates — each run lands only new/changed rows — so
# MAGIC   Bronze is rebuilt deterministically from **all** available `load_date`
# MAGIC   partitions.
# MAGIC * **Full-load entities** (warehouses, order_items) land a complete snapshot
# MAGIC   each run, so only the **latest** `load_date` partition is read; older
# MAGIC   snapshots are ignored to avoid accumulating duplicates.
# MAGIC
# MAGIC The `load_type` per entity is taken from the same metadata that drives ADF
# MAGIC (`azure/config/ingestion_entities.json`), kept in sync here.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("catalog", "retail_lakehouse")
dbutils.widgets.text("landing_path", "abfss://landing@<storage-account>.dfs.core.windows.net/retail")
catalog = dbutils.widgets.get("catalog")
landing_path = dbutils.widgets.get("landing_path")
print(f"catalog={catalog}  landing_path={landing_path}")

SOURCE_SYSTEM = "adf_ingestion"

# Load type per entity, consistent with azure/config/ingestion_entities.json.
ENTITY_LOAD_TYPE = {
    "customers": "incremental",
    "products": "incremental",
    "warehouses": "full",
    "orders": "incremental",
    "order_items": "full",
    "inventory_events": "incremental",
}

# COMMAND ----------


def list_load_dates(entity: str) -> list[str]:
    """Return the sorted load_date partition values present for an entity."""
    entity_root = f"{landing_path}/{entity}"
    try:
        entries = dbutils.fs.ls(entity_root)
    except Exception as exc:  # noqa: BLE001 - surface a clear message for a missing entity
        raise FileNotFoundError(f"No landing data for entity at {entity_root}") from exc

    dates = []
    for entry in entries:
        name = entry.name.rstrip("/")
        if name.startswith("load_date="):
            dates.append(name.split("=", 1)[1])
    if not dates:
        raise FileNotFoundError(f"No load_date=* partitions under {entity_root}")
    return sorted(dates)


def load_entity(entity: str, load_type: str) -> int:
    """Read the appropriate load_date partitions and overwrite the Bronze table."""
    all_dates = list_load_dates(entity)
    selected = all_dates if load_type == "incremental" else all_dates[-1:]
    print(f"{entity}: load_type={load_type}, partitions {selected} of {all_dates}")

    frames = []
    for load_date in selected:
        path = f"{landing_path}/{entity}/load_date={load_date}"
        df = (
            spark.read.parquet(path)
            .withColumn("batch_id", F.lit(f"load_date={load_date}"))
            .withColumn("source_file", F.col("_metadata.file_path"))
        )
        frames.append(df)

    combined = frames[0]
    for df in frames[1:]:
        combined = combined.unionByName(df, allowMissingColumns=True)

    combined = (
        combined
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source_system", F.lit(SOURCE_SYSTEM))
    )

    target = f"{catalog}.bronze.{entity}"
    combined.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
    n = spark.table(target).count()
    print(f"Loaded {target}: {n} rows")
    return n


# COMMAND ----------

for entity, load_type in ENTITY_LOAD_TYPE.items():
    load_entity(entity, load_type)

# COMMAND ----------

# MAGIC %md
# MAGIC After this notebook, `{catalog}.bronze.*` holds the same tables the demo
# MAGIC loader produces, so the existing Silver and Gold notebooks run unchanged.
