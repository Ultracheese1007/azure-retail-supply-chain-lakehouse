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

dbutils.widgets.text("landing_path", "/tmp/retail_lakehouse/landing")
landing_path = dbutils.widgets.get("landing_path")
print(f"Writing demo batches to: {landing_path}")

# COMMAND ----------

# Write CSVs to the driver-local filesystem first, then copy into the landing
# path (works for both DBFS and Unity Catalog Volume targets).
local_dir = "/tmp/_retail_gen"
summary = gen.write_csvs(local_dir)
print("Row counts by batch/entity:")
for batch, entities in summary.items():
    for entity, count in entities.items():
        print(f"  {batch}/{entity}: {count}")

# COMMAND ----------

dbutils.fs.mkdirs(landing_path)
for batch in ("batch_1", "batch_2"):
    for entity in gen.COLUMNS:
        src = f"file:{local_dir}/{batch}/{entity}.csv"
        dst = f"{landing_path}/{batch}/{entity}.csv"
        dbutils.fs.cp(src, dst)
print("Landing files:")
display(dbutils.fs.ls(landing_path))

# COMMAND ----------

# MAGIC %md
# MAGIC Edge cases deliberately planted in batch 2 (used by later Silver / SCD2 /
# MAGIC quality steps):

# COMMAND ----------

print(gen.EDGE_CASES)
