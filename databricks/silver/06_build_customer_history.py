# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: rebuild customer SCD Type 2 history
# MAGIC
# MAGIC Rebuilds `silver.customers_history` from the complete Bronze customer
# MAGIC change feed.
# MAGIC
# MAGIC Bronze contains multiple source batches. They must be processed in
# MAGIC chronological order; collapsing all batches to the latest customer row
# MAGIC before applying SCD2 would lose the original versions.
# MAGIC
# MAGIC This MVP uses a deterministic full rebuild:
# MAGIC
# MAGIC 1. Clear and recreate the history table.
# MAGIC 2. Process Bronze batches in timestamp order.
# MAGIC 3. For each batch, detect new or changed customers.
# MAGIC 4. Expire the previous current version.
# MAGIC 5. Insert the new current version.
# MAGIC
# MAGIC Re-running the notebook produces the same history and row counts.

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

HISTORY_TABLE = f"{catalog}.silver.customers_history"

TRACKED_ATTRIBUTES = [
    "email",
    "customer_name",
    "segment",
    "city",
    "region",
]

END_OF_TIME = "9999-12-31 23:59:59"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare the complete customer change feed

# COMMAND ----------

src = silver.read_bronze(spark, catalog, "customers")

src = silver.trim_to_null(
    src,
    ["email", "customer_name", "segment", "city", "region"],
)

src = src.withColumn(
    "updated_at",
    F.to_timestamp("updated_at"),
)

valid_condition = (
    F.col("customer_id").isNotNull()
    & F.col("updated_at").isNotNull()
    & F.col("batch_id").isNotNull()
)

src, quarantined = silver.split_quarantine(
    src,
    valid_condition,
    reason="null customer_id, updated_at, or batch_id",
)

silver.write_quarantine(
    quarantined,
    catalog,
    "customers",
)

print(f"valid customer change rows: {src.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recreate the history table
# MAGIC
# MAGIC The Bronze ingestion is a deterministic full refresh, so the Silver
# MAGIC customer history is rebuilt deterministically from all ordered batches.

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {HISTORY_TABLE}")

spark.sql(
    f"""
    CREATE TABLE {HISTORY_TABLE} (
        customer_sk                 STRING,
        customer_id                 STRING,
        email                       STRING,
        customer_name               STRING,
        segment                     STRING,
        city                        STRING,
        region                      STRING,
        effective_start_timestamp   TIMESTAMP,
        effective_end_timestamp     TIMESTAMP,
        is_current                  BOOLEAN
    )
    USING DELTA
    """
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process batches chronologically

# COMMAND ----------

batch_rows = (
    src.groupBy("batch_id")
    .agg(F.min("updated_at").alias("batch_timestamp"))
    .orderBy("batch_timestamp", "batch_id")
    .collect()
)

batch_ids = [row["batch_id"] for row in batch_rows]

print(f"processing batches in order: {batch_ids}")

tracked_comparison = "\n           AND ".join(
    f"h.{attribute} <=> s.{attribute}"
    for attribute in TRACKED_ATTRIBUTES
)

for batch_id in batch_ids:
    print(f"\nProcessing {batch_id}")

    batch_src = src.filter(F.col("batch_id") == batch_id)

    batch_src = silver.dedup_latest(
        batch_src,
        keys=["customer_id"],
        order_col="updated_at",
    )

    batch_src.createOrReplaceTempView("source_customers_batch")

    spark.sql(
        f"""
        CREATE OR REPLACE TEMP VIEW changed_customers AS
        SELECT s.*
        FROM source_customers_batch s
        LEFT JOIN {HISTORY_TABLE} h
               ON s.customer_id = h.customer_id
              AND h.is_current = true
        WHERE h.customer_id IS NULL
           OR NOT (
               {tracked_comparison}
           )
        """
    )

    changed_count = spark.table("changed_customers").count()
    print(f"new customer versions in {batch_id}: {changed_count}")

    spark.sql(
        f"""
        MERGE INTO {HISTORY_TABLE} h
        USING changed_customers s
           ON h.customer_id = s.customer_id
          AND h.is_current = true
        WHEN MATCHED THEN UPDATE SET
            h.is_current = false,
            h.effective_end_timestamp = s.updated_at
        """
    )

    spark.sql(
        f"""
        INSERT INTO {HISTORY_TABLE}
        SELECT
            md5(
                concat_ws(
                    '|',
                    s.customer_id,
                    CAST(s.updated_at AS STRING)
                )
            ) AS customer_sk,
            s.customer_id,
            s.email,
            s.customer_name,
            s.segment,
            s.city,
            s.region,
            s.updated_at AS effective_start_timestamp,
            CAST('{END_OF_TIME}' AS TIMESTAMP)
                AS effective_end_timestamp,
            true AS is_current
        FROM changed_customers s
        """
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Result

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT
            customer_id,
            email,
            region,
            effective_start_timestamp,
            effective_end_timestamp,
            is_current
        FROM {HISTORY_TABLE}
        ORDER BY customer_id, effective_start_timestamp
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD2 validation

# COMMAND ----------

bad_current_versions = spark.sql(
    f"""
    SELECT
        customer_id,
        COUNT(*) AS current_versions
    FROM {HISTORY_TABLE}
    WHERE is_current = true
    GROUP BY customer_id
    HAVING COUNT(*) <> 1
    """
)

if bad_current_versions.count() > 0:
    display(bad_current_versions)
    raise AssertionError(
        "SCD2 invariant violated: customer has != 1 current version"
    )

print("OK: every customer has exactly one current version")

display(
    spark.sql(
        f"""
        SELECT
            customer_id,
            COUNT(*) AS version_count
        FROM {HISTORY_TABLE}
        WHERE customer_id IN ('C001', 'C002', 'C006')
        GROUP BY customer_id
        ORDER BY customer_id
        """
    )
)
