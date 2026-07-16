# Databricks notebook source
# MAGIC %md
# MAGIC # Generate demo source batches
# MAGIC
# MAGIC Writes two deterministic batches of retail source data (CSV) to a landing
# MAGIC path. Bronze ingestion (`databricks/bronze/02_load_bronze.py`, added in the
# MAGIC next commit) reads from this path and tags each row with its `batch_id`.
# MAGIC
# MAGIC The data itself is produced by the pure-Python generator in
# MAGIC `src/retail_lakehouse/generate_source_data.py`, so it can be unit-tested off
# MAGIC Databricks and always yields identical output for the same seed.

# COMMAND ----------

import os
import sys

# Make the repo's src/ importable when running from a Databricks Repo.
# Adjust REPO_ROOT if your workspace path differs.
REPO_ROOT = os.path.dirname(os.path.dirname(os.getcwd()))
SRC = os.path.join(REPO_ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from retail_lakehouse import generate_source_data as gen  # noqa: E402

# COMMAND ----------

# Default is the managed volume created by databricks/setup/01_create_lakehouse.
# It is shared storage: every serverless task can read it, unlike /tmp which is
# local to a single task's compute. The widget lets a job override it.
dbutils.widgets.text("landing_path", "/Volumes/retail_lakehouse/bronze/landing_files/source")
landing_path = dbutils.widgets.get("landing_path")
print(f"Writing demo batches to: {landing_path}")

# COMMAND ----------

# Write the deterministic CSV batches directly to the managed Volume.
# Databricks serverless compute blocks dbutils.fs access to driver-local
# file:/tmp paths, while Unity Catalog Volumes support direct POSIX writes.
dbutils.fs.mkdirs(landing_path)
summary = gen.write_csvs(landing_path)

print("Row counts by batch/entity:")
for batch, entities in summary.items():
    for entity, count in entities.items():
        print(f"  {batch}/{entity}: {count}")

# COMMAND ----------

print("Landing batches:")
display(dbutils.fs.ls(landing_path))

print("Batch 1 files:")
display(dbutils.fs.ls(f"{landing_path}/batch_1"))

print("Batch 2 files:")
display(dbutils.fs.ls(f"{landing_path}/batch_2"))

# COMMAND ----------

# MAGIC %md
# MAGIC Edge cases deliberately planted in batch 2 (used by later Silver / SCD2 /
# MAGIC quality steps):

# COMMAND ----------

print(gen.EDGE_CASES)
