"""DMO Data Warehouse - Shared Utilities

Reusable functions for data quality checks, logging, and common transformations.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from datetime import datetime


# =============================================================================
# DATA QUALITY CHECKS
# =============================================================================

def check_not_null(df: DataFrame, columns: list[str]) -> DataFrame:
    """Assert that specified columns have no nulls. Returns rows with nulls for quarantine."""
    null_condition = F.lit(False)
    for col in columns:
        null_condition = null_condition | F.col(col).isNull()
    return df.filter(null_condition)


def check_unique(df: DataFrame, columns: list[str]) -> bool:
    """Check if the specified columns form a unique key."""
    total = df.count()
    distinct = df.select(columns).distinct().count()
    return total == distinct


def add_null_percentage(df: DataFrame, column: str) -> float:
    """Return the null percentage for a given column."""
    total = df.count()
    if total == 0:
        return 0.0
    nulls = df.filter(F.col(column).isNull()).count()
    return (nulls / total) * 100


# =============================================================================
# METADATA & AUDIT COLUMNS
# =============================================================================

def add_audit_columns(df: DataFrame, source_name: str) -> DataFrame:
    """Add standard audit/metadata columns to a DataFrame."""
    return (
        df
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_name", F.lit(source_name))
        .withColumn("_file_path", F.input_file_name())
    )


def add_scd2_columns(df: DataFrame) -> DataFrame:
    """Add SCD Type 2 tracking columns."""
    return (
        df
        .withColumn("_effective_from", F.current_timestamp())
        .withColumn("_effective_to", F.lit(None).cast("timestamp"))
        .withColumn("_is_current", F.lit(True))
    )


# =============================================================================
# DEDUPLICATION
# =============================================================================

def deduplicate(
    df: DataFrame,
    key_columns: list[str],
    order_column: str,
    ascending: bool = False
) -> DataFrame:
    """Deduplicate by keeping the latest (or earliest) record per key."""
    from pyspark.sql.window import Window

    order_expr = F.col(order_column).asc() if ascending else F.col(order_column).desc()
    window = Window.partitionBy(key_columns).orderBy(order_expr)

    return (
        df
        .withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


# =============================================================================
# LOGGING
# =============================================================================

def log_pipeline_event(
    spark: SparkSession,
    layer: str,
    table: str,
    status: str,
    row_count: int = 0,
    message: str = ""
):
    """Log a pipeline event for observability (prints to driver logs)."""
    timestamp = datetime.utcnow().isoformat()
    print(f"[{timestamp}] [{layer.upper()}] {table} | {status} | rows={row_count} | {message}")
