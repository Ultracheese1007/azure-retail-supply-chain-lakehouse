# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: build customer SCD Type 2 history
# MAGIC
# MAGIC Maintains `silver.customers_history`, where every customer version is kept
# MAGIC with a validity window. Downstream facts can then join to the version that
# MAGIC was in effect when an event happened (see the Gold layer).
# MAGIC
# MAGIC ## Design
# MAGIC
# MAGIC **Two-stage expire-and-insert.** A single simple `MERGE` cannot both expire
# MAGIC an old version and insert a new one for the *same* changed source row: that
# MAGIC row matches the existing current record, so it takes the `WHEN MATCHED`
# MAGIC branch (the update) and never reaches `WHEN NOT MATCHED` (the insert). The
# MAGIC result would be a customer with no current version at all. So the work is
# MAGIC split explicitly:
# MAGIC
# MAGIC 1. **Detect** changed and new customers against the current versions.
# MAGIC 2. **Expire** the current version of changed customers (set `is_current` to
# MAGIC    false and close `effective_end_timestamp`).
# MAGIC 3. **Insert** the new version for changed customers, plus first versions for
# MAGIC    brand-new customers.
# MAGIC
# MAGIC (Single-`MERGE` SCD2 *is* possible with a staged-union / null-merge-key
# MAGIC trick; the explicit two-stage form is used here because it is easier to read,
# MAGIC test and reason about.)
# MAGIC
# MAGIC **Null-safe change detection.** Comparisons use `<=>` (null-safe equality)
# MAGIC rather than `=` / `<>`. With ordinary equality, `NULL <> 'South Holland'`
# MAGIC evaluates to NULL — not TRUE — so a value arriving where there was
# MAGIC previously NULL would *not* be detected as a change and the new version
# MAGIC would be silently dropped. Customer `C002` (region NULL -> value) exists in
# MAGIC the generated data specifically to prove this path works.
# MAGIC
# MAGIC **Idempotent.** Re-running the same batch detects no changes (every tracked
# MAGIC attribute is null-safe-equal to the current version), so no extra versions
# MAGIC are created. Each `customer_id` always has exactly one `is_current = true`
# MAGIC row.

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

# Attributes tracked for change detection. A change in any of these opens a new
# version; changes to anything else do not.
TRACKED_ATTRIBUTES = ["email", "customer_name", "segment", "city", "region"]

# Open-ended validity: current rows use this sentinel rather than NULL, so
# point-in-time joins can use a plain BETWEEN-style range without COALESCE.
END_OF_TIME = "9999-12-31 23:59:59"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare the source (latest row per customer in this run)

# COMMAND ----------

src = silver.read_bronze(spark, catalog, "customers")
src = silver.trim_to_null(src, ["email", "customer_name", "segment", "city", "region"])
src = src.withColumn("updated_at", F.to_timestamp("updated_at"))

# Guard: a customer must have an id and a change timestamp to be versioned.
valid_condition = F.col("customer_id").isNotNull() & F.col("updated_at").isNotNull()
src, quarantined = silver.split_quarantine(
    src, valid_condition, reason="null customer_id or updated_at"
)
silver.write_quarantine(quarantined, catalog, "customers")

# If a customer appears more than once in the same run, keep only its latest row.
src = silver.dedup_latest(src, keys=["customer_id"], order_col="updated_at")
src.createOrReplaceTempView("source_customers")
print(f"source customers this run: {src.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the history table if it does not exist

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {HISTORY_TABLE} (
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
) USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 1 — detect changed and new customers
# MAGIC
# MAGIC `<=>` is null-safe equality: `NULL <=> NULL` is TRUE and
# MAGIC `NULL <=> 'x'` is FALSE, so a NULL -> value transition is caught.

# COMMAND ----------

tracked_comparison = "\n       AND ".join(
    f"h.{attr} <=> s.{attr}" for attr in TRACKED_ATTRIBUTES
)

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW changed_customers AS
SELECT s.*
FROM source_customers s
LEFT JOIN {HISTORY_TABLE} h
       ON s.customer_id = h.customer_id
      AND h.is_current = true
WHERE h.customer_id IS NULL          -- brand-new customer
   OR NOT ({tracked_comparison})     -- tracked attribute changed (null-safe)
""")

changed_count = spark.table("changed_customers").count()
print(f"customers with a new version to write: {changed_count}")
display(spark.table("changed_customers").select("customer_id", *TRACKED_ATTRIBUTES, "updated_at"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 2 — expire the current version of changed customers
# MAGIC
# MAGIC Only customers that already have a current version are affected; brand-new
# MAGIC customers match nothing here and are simply inserted in stage 3.

# COMMAND ----------

spark.sql(f"""
MERGE INTO {HISTORY_TABLE} h
USING changed_customers s
   ON h.customer_id = s.customer_id
  AND h.is_current = true
WHEN MATCHED THEN UPDATE SET
    h.is_current = false,
    h.effective_end_timestamp = s.updated_at
""")
print("expired previous current versions for changed customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 3 — insert the new version
# MAGIC
# MAGIC The surrogate key is derived from the business key plus the version start,
# MAGIC so it is stable across full rebuilds and unique per version.

# COMMAND ----------

spark.sql(f"""
INSERT INTO {HISTORY_TABLE}
SELECT
    md5(concat_ws('|', s.customer_id, CAST(s.updated_at AS STRING))) AS customer_sk,
    s.customer_id,
    s.email,
    s.customer_name,
    s.segment,
    s.city,
    s.region,
    s.updated_at                                AS effective_start_timestamp,
    CAST('{END_OF_TIME}' AS TIMESTAMP)          AS effective_end_timestamp,
    true                                        AS is_current
FROM changed_customers s
""")
print("inserted new current versions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Result

# COMMAND ----------

display(
    spark.sql(f"""
    SELECT customer_id, email, region, effective_start_timestamp,
           effective_end_timestamp, is_current
    FROM {HISTORY_TABLE}
    ORDER BY customer_id, effective_start_timestamp
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inline sanity check — exactly one current version per customer
# MAGIC
# MAGIC The full validation suite lives in `databricks/quality/`; this is a fast
# MAGIC guard so the notebook fails loudly rather than writing a broken history.

# COMMAND ----------

bad = spark.sql(f"""
SELECT customer_id, COUNT(*) AS current_versions
FROM {HISTORY_TABLE}
WHERE is_current = true
GROUP BY customer_id
HAVING COUNT(*) <> 1
""")
if bad.count() > 0:
    display(bad)
    raise AssertionError("SCD2 invariant violated: a customer has != 1 current version")
print("OK: every customer has exactly one current version")
