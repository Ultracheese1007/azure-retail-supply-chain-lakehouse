# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze ingestion
# MAGIC
# MAGIC Loads the two generated batches from the landing path into Bronze tables.
# MAGIC
# MAGIC Design choices:
# MAGIC
# MAGIC * **Explicit schema** per entity (all columns read as strings) — no
# MAGIC   uncontrolled schema inference. Bronze preserves the source *as received*;
# MAGIC   typing and cleaning happen in Silver.
# MAGIC * **Audit columns** are added to every row: `ingestion_timestamp`,
# MAGIC   `batch_id`, `source_system`, `source_file`.
# MAGIC * **Raw preservation** — no deduplication, no validation, no SCD2 here.
# MAGIC   The deliberately duplicated and illegal rows from batch 2 are loaded as-is
# MAGIC   and handled later in Silver / quarantine.
# MAGIC * The whole set of batches is loaded with **overwrite**, so re-running this
# MAGIC   notebook is stable and does not accumulate duplicate rows.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

# COMMAND ----------

dbutils.widgets.text("catalog", "retail_lakehouse")
# Default is the managed volume created by databricks/setup/01_create_lakehouse.
# The widget lets a job override it.
dbutils.widgets.text("landing_path", "/Volumes/retail_lakehouse/bronze/landing_files/source")
catalog = dbutils.widgets.get("catalog")
landing_path = dbutils.widgets.get("landing_path")
print(f"catalog={catalog}  landing_path={landing_path}")

SOURCE_SYSTEM = "demo_generator"
BATCHES = ("batch_1", "batch_2")

# COMMAND ----------

# Explicit column names per entity (all read as StringType in Bronze).
ENTITY_COLUMNS = {
    "customers": [
        "customer_id", "email", "customer_name", "segment", "city", "region", "updated_at"
    ],
    "products": ["product_id", "product_name", "category", "unit_price", "updated_at"],
    "warehouses": ["warehouse_id", "warehouse_name", "city", "region"],
    "orders": ["order_id", "customer_id", "warehouse_id", "order_timestamp", "status"],
    "order_items": ["order_item_id", "order_id", "product_id", "quantity", "unit_price"],
    "inventory_events": [
        "event_id", "warehouse_id", "product_id", "event_type", "quantity_change", "event_timestamp"
    ],
}


def schema_for(columns):
    return StructType([StructField(c, StringType(), True) for c in columns])


# COMMAND ----------

def load_entity(entity, columns):
    """Read every available batch for one entity, add audit columns, overwrite Bronze."""
    schema = schema_for(columns)
    frames = []
    for batch in BATCHES:
        path = f"{landing_path}/{batch}/{entity}.csv"
        df = (
            spark.read.option("header", True)
            .schema(schema)
            .csv(path)
            .withColumn("batch_id", F.lit(batch))
            .withColumn("source_file", F.col("_metadata.file_path"))
        )
        frames.append(df)

    combined = frames[0]
    for df in frames[1:]:
        combined = combined.unionByName(df)

    combined = (
        combined
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source_system", F.lit(SOURCE_SYSTEM))
    )

    target = f"{catalog}.bronze.{entity}"
    combined.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
    count = spark.table(target).count()
    print(f"Loaded {target}: {count} rows")
    return count


# COMMAND ----------

for entity, columns in ENTITY_COLUMNS.items():
    load_entity(entity, columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify: rows are present and traceable by batch

# COMMAND ----------

batch_counts_sql = (
    f"SELECT batch_id, COUNT(*) AS n FROM {catalog}.bronze.orders "
    "GROUP BY batch_id ORDER BY batch_id"
)
display(spark.sql(batch_counts_sql))

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {catalog}.bronze.customers ORDER BY batch_id, customer_id"))
