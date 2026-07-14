# Databricks notebook source
# MAGIC %md
# MAGIC # Create the lakehouse (managed storage)
# MAGIC
# MAGIC Creates the catalog and the four schemas used by the pipeline:
# MAGIC `bronze`, `silver`, `gold`, `quarantine`. All use **managed** storage
# MAGIC (no external location / volume), so the project runs on Databricks Free
# MAGIC Edition without any external storage account.
# MAGIC
# MAGIC If your workspace does not allow creating catalogs, point the `catalog`
# MAGIC widget at an existing catalog you can write to; the schemas will be created
# MAGIC inside it.

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
# MAGIC Confirm the schemas exist.

# COMMAND ----------

display(spark.sql(f"SHOW SCHEMAS IN {catalog}"))
