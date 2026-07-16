# Databricks notebook source
# MAGIC %md
# MAGIC # Create the lakehouse (managed storage)
# MAGIC
# MAGIC Creates the catalog, the four schemas used by the pipeline
# MAGIC (`bronze`, `silver`, `gold`, `quarantine`), and the managed volume that
# MAGIC holds incoming source files.
# MAGIC
# MAGIC Everything is **managed** by Unity Catalog — no external location and no
# MAGIC storage account — so the project runs on Databricks Free Edition.
# MAGIC
# MAGIC ## Why a volume rather than `/tmp`
# MAGIC
# MAGIC On serverless compute each task runs on its own machine, and `/tmp` is
# MAGIC local to it and discarded afterwards. Files written to `/tmp` by the
# MAGIC generation task would therefore be invisible to the Bronze ingestion task
# MAGIC running elsewhere. A managed volume is governed, shared storage that every
# MAGIC task can read, which is what makes the multi-task workflow work at all.
# MAGIC
# MAGIC If your workspace does not allow creating catalogs, point the `catalog`
# MAGIC widget at an existing catalog you can write to; the schemas and volume will
# MAGIC be created inside it.

# COMMAND ----------

dbutils.widgets.text("catalog", "retail_lakehouse")
catalog = dbutils.widgets.get("catalog")
print(f"Target catalog: {catalog}")

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")

for schema in ("bronze", "silver", "gold", "quarantine"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    print(f"Ready: {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Landing volume
# MAGIC
# MAGIC Source files land here before Bronze ingestion reads them. The path is
# MAGIC derived from the `catalog` widget, so pointing this notebook at a different
# MAGIC catalog moves the landing area with it.

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`bronze`.`landing_files`")

landing_path = f"/Volumes/{catalog}/bronze/landing_files/source"
dbutils.fs.mkdirs(landing_path)
print(f"Landing path ready: {landing_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC Confirm the schemas and the volume exist.

# COMMAND ----------

display(spark.sql(f"SHOW SCHEMAS IN `{catalog}`"))

# COMMAND ----------

display(spark.sql(f"SHOW VOLUMES IN `{catalog}`.`bronze`"))
