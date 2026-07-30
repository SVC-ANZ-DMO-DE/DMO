# Databricks notebook source
# DBTITLE 1,Bronze Ingestion - Configuration
# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Raw Data Ingestion
# MAGIC 
# MAGIC This notebook ingests raw data from the landing zone into the bronze layer.
# MAGIC - **Pattern**: Auto Loader (cloudFiles) for incremental file ingestion
# MAGIC - **Output**: Raw data with audit columns, no transformations applied
# MAGIC - **Expectations**: Schema enforcement only (no data quality filtering)

import sys
sys.path.append("/Workspace/Users/mlakhera@dualasiapacific.com/DMO")

from common.config import (
    CATALOG_NAME, BRONZE_SCHEMA, LANDING_ZONE_PATH,
    get_table_name, get_checkpoint_path
)
from common.utils import add_audit_columns, log_pipeline_event

# COMMAND ----------

# DBTITLE 1,Parameters (Widgets)
# Define notebook widgets for parameterisation
dbutils.widgets.text("source_name", "", "Source System Name")
dbutils.widgets.text("source_format", "csv", "File Format (csv/json/parquet)")
dbutils.widgets.text("table_name", "", "Target Bronze Table Name")

# Get parameter values
source_name = dbutils.widgets.get("source_name")
source_format = dbutils.widgets.get("source_format")
table_name = dbutils.widgets.get("table_name")

# Derived paths
source_path = f"{LANDING_ZONE_PATH}{source_name}/"
target_table = get_table_name("bronze", table_name)
checkpoint_path = get_checkpoint_path("bronze", table_name)

print(f"Source: {source_path}")
print(f"Target: {target_table}")
print(f"Checkpoint: {checkpoint_path}")

# COMMAND ----------

# DBTITLE 1,Auto Loader Ingestion
# Read raw files using Auto Loader (cloudFiles)
df_raw = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", source_format)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", f"{checkpoint_path}/_schema")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load(source_path)
)

# Add audit/metadata columns
df_bronze = add_audit_columns(df_raw, source_name)

log_pipeline_event(spark, "bronze", table_name, "STARTED")

# COMMAND ----------

# DBTITLE 1,Write to Bronze Table
# Write to bronze Delta table (streaming append)
(
    df_bronze.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(target_table)
)

log_pipeline_event(spark, "bronze", table_name, "COMPLETED")

# COMMAND ----------

# DBTITLE 1,Batch Alternative (non-streaming)
# ALTERNATIVE: Batch ingestion (use when streaming is not needed)
# Uncomment below if you prefer batch mode

# df_raw_batch = (
#     spark.read
#     .format(source_format)
#     .option("header", "true")
#     .option("inferSchema", "true")
#     .load(source_path)
# )
#
# df_bronze_batch = add_audit_columns(df_raw_batch, source_name)
#
# (
#     df_bronze_batch.write
#     .format("delta")
#     .mode("append")
#     .option("mergeSchema", "true")
#     .saveAsTable(target_table)
# )