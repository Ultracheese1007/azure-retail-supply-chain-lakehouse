"""Shared Silver-layer helpers.

Small, reusable Spark helpers so each per-entity cleaning notebook stays thin
and focused on its own rules. Kept in the package (not a notebook) so the logic
lives in one place and can be imported and reviewed.

Silver responsibilities handled here: typing support, string standardisation,
business-key deduplication, and splitting valid rows from quarantined rows.
SCD Type 2 history is intentionally *not* here — it is built separately for
customers.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Audit columns carried from Bronze; kept through Silver for lineage.
BRONZE_AUDIT_COLUMNS = ["ingestion_timestamp", "batch_id", "source_system", "source_file"]


def read_bronze(spark, catalog: str, entity: str) -> DataFrame:
    """Read a Bronze table by entity name."""
    return spark.table(f"{catalog}.bronze.{entity}")


def trim_to_null(df: DataFrame, columns: list[str]) -> DataFrame:
    """Trim string columns and convert empty strings to NULL.

    Bronze reads everything as strings, so an absent value arrives as "".
    Converting it to a real NULL lets downstream null-safe logic behave
    correctly (e.g. the region NULL->value change on customer C002).
    """
    for col in columns:
        trimmed = F.trim(F.col(col))
        df = df.withColumn(col, F.when(trimmed == "", None).otherwise(trimmed))
    return df


def dedup_latest(
    df: DataFrame, keys: list[str], order_col: str = "ingestion_timestamp"
) -> DataFrame:
    """Keep one row per business key: the latest by order_col (ties broken deterministically)."""
    order = [F.col(order_col).desc(), *[F.col(k).asc() for k in keys]]
    window = Window.partitionBy(*keys).orderBy(*order)
    return (
        df.withColumn("_rn", F.row_number().over(window))
        .where(F.col("_rn") == 1)
        .drop("_rn")
    )


def split_quarantine(df: DataFrame, valid_condition, reason: str):
    """Split df into (valid, quarantined). Quarantined rows carry a reason and timestamp."""
    valid = df.where(valid_condition)
    quarantined = (
        df.where(~valid_condition | valid_condition.isNull())
        .withColumn("quarantine_reason", F.lit(reason))
        .withColumn("quarantine_timestamp", F.current_timestamp())
    )
    return valid, quarantined


def with_silver_audit(df: DataFrame) -> DataFrame:
    """Add a Silver processing timestamp."""
    return df.withColumn("silver_processed_timestamp", F.current_timestamp())


def write_silver(df: DataFrame, catalog: str, table: str) -> int:
    """Overwrite a Silver table (rerun-stable). Returns row count."""
    target = f"{catalog}.silver.{table}"
    out = with_silver_audit(df)
    out.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
    n = df.sparkSession.table(target).count()
    print(f"Silver {target}: {n} rows")
    return n


def write_quarantine(df: DataFrame, catalog: str, table: str) -> int:
    """Overwrite a quarantine table (rerun-stable). Returns row count."""
    target = f"{catalog}.quarantine.{table}"
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
    n = df.sparkSession.table(target).count()
    print(f"Quarantine {target}: {n} rows")
    return n
