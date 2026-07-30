# Databricks notebook source
# DBTITLE 1,Silver Layer - Setup
# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Cleansed & Conformed Data
# MAGIC 
# MAGIC This notebook transforms bronze (raw) data into silver (cleansed):
# MAGIC - **Deduplication**: Remove duplicate records
# MAGIC - **Data Quality**: Filter/quarantine bad records
# MAGIC - **Type Casting**: Enforce correct data types
# MAGIC - **Standardisation**: Consistent column naming, date formats
# MAGIC - **MERGE (Upsert)**: Incrementally update silver tables

import sys
sys.path.append("/Workspace/Users/mlakhera@dualasiapacific.com/DMO")

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

from common.config import (
    CATALOG_NAME, BRONZE_SCHEMA, SILVER_SCHEMA,
    get_table_name, MAX_NULL_PERCENTAGE
)
from common.utils import (
    deduplicate, check_not_null, add_null_percentage,
    log_pipeline_event
)

# COMMAND ----------

# DBTITLE 1,Parameters
# Parameters
dbutils.widgets.text("source_table", "", "Bronze Source Table")
dbutils.widgets.text("target_table", "", "Silver Target Table")
dbutils.widgets.text("key_columns", "", "Primary Key Columns (comma-separated)")
dbutils.widgets.text("order_column", "_ingested_at", "Column to determine latest record")

source_table = get_table_name("bronze", dbutils.widgets.get("source_table"))
target_table = get_table_name("silver", dbutils.widgets.get("target_table"))
key_columns = [c.strip() for c in dbutils.widgets.get("key_columns").split(",")]
order_column = dbutils.widgets.get("order_column")

print(f"Source: {source_table}")
print(f"Target: {target_table}")
print(f"Keys: {key_columns}")

# COMMAND ----------

# DBTITLE 1,Read & Deduplicate
# Read bronze data (incremental: only new records since last run)
df_bronze = spark.read.table(source_table)

# Deduplicate: keep latest record per key
df_deduped = deduplicate(
    df=df_bronze,
    key_columns=key_columns,
    order_column=order_column,
    ascending=False  # Keep most recent
)

print(f"Bronze rows: {df_bronze.count()}")
print(f"After dedup: {df_deduped.count()}")

# COMMAND ----------

# DBTITLE 1,Data Quality Checks & Quarantine
# Identify records failing quality checks
df_quarantine = check_not_null(df_deduped, key_columns)

# Good records pass quality checks
df_clean = df_deduped.subtract(df_quarantine)

# Log quarantine stats
quarantine_count = df_quarantine.count()
if quarantine_count > 0:
    log_pipeline_event(spark, "silver", target_table, "QUARANTINE", quarantine_count,
                       f"Null key columns found in {quarantine_count} rows")
    # Optionally write quarantine records
    # df_quarantine.write.mode("append").saveAsTable(f"{target_table}_quarantine")

print(f"Clean rows: {df_clean.count()} | Quarantined: {quarantine_count}")

# COMMAND ----------

# DBTITLE 1,Transformations (Customise per table)
# =============================================================================
# CUSTOMISE TRANSFORMATIONS BELOW PER SOURCE TABLE
# =============================================================================

df_transformed = (
    df_clean
    # Example: Standardise column names (snake_case)
    # .withColumnRenamed("OldName", "new_name")
    
    # Example: Cast data types
    # .withColumn("amount", F.col("amount").cast("decimal(18,2)"))
    # .withColumn("event_date", F.to_date("event_date_str", "yyyy-MM-dd"))
    
    # Example: Trim strings
    # .withColumn("name", F.trim(F.col("name")))
    
    # Drop audit columns from bronze (silver gets its own)
    .drop("_ingested_at", "_source_name", "_file_path")
    
    # Add silver audit columns
    .withColumn("_processed_at", F.current_timestamp())
    .withColumn("_is_valid", F.lit(True))
)

# COMMAND ----------

# DBTITLE 1,MERGE (Upsert) into Silver
# MERGE pattern: Insert new, Update existing
if spark.catalog.tableExists(target_table):
    delta_target = DeltaTable.forName(spark, target_table)
    
    # Build merge condition from key columns
    merge_condition = " AND ".join(
        [f"target.{col} = source.{col}" for col in key_columns]
    )
    
    (
        delta_target.alias("target")
        .merge(df_transformed.alias("source"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    log_pipeline_event(spark, "silver", target_table, "MERGED")
else:
    # First run: create the table
    df_transformed.write.format("delta").saveAsTable(target_table)
    log_pipeline_event(spark, "silver", target_table, "CREATED")

print(f"Silver table {target_table} updated successfully.")