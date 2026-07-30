# Databricks notebook source
# DBTITLE 1,Pipeline Orchestration - Overview
# Databricks notebook source
# MAGIC %md
# MAGIC # DMO Pipeline Orchestrator
# MAGIC 
# MAGIC This notebook orchestrates the full medallion pipeline:
# MAGIC 1. **Bronze**: Ingest raw data from landing zone
# MAGIC 2. **Silver**: Cleanse, deduplicate, and conform
# MAGIC 3. **Gold**: Build reporting-ready tables
# MAGIC 
# MAGIC ## Execution Options
# MAGIC - Run this notebook directly for ad-hoc execution
# MAGIC - Schedule via Databricks Jobs for automated runs
# MAGIC - Or use Spark Declarative Pipelines (see alternative below)

from datetime import datetime
print(f"Pipeline started: {datetime.utcnow().isoformat()}")

# COMMAND ----------

# DBTITLE 1,Pipeline Configuration
# =============================================================================
# PIPELINE CONFIGURATION
# Define which sources to process through the medallion layers
# =============================================================================

# Each entry represents one source system to ingest and transform
PIPELINE_SOURCES = [
    # {
    #     "source_name": "sales",          # Maps to landing zone subfolder
    #     "source_format": "csv",          # File format
    #     "bronze_table": "raw_sales",     # Bronze table name
    #     "silver_table": "sales",         # Silver table name
    #     "key_columns": "transaction_id", # Primary key(s) - comma separated
    #     "order_column": "_ingested_at",  # Dedup ordering column
    # },
    # {
    #     "source_name": "customers",
    #     "source_format": "parquet",
    #     "bronze_table": "raw_customers",
    #     "silver_table": "customers",
    #     "key_columns": "customer_id",
    #     "order_column": "_ingested_at",
    # },
]

# Notebook paths
BRONZE_NOTEBOOK = "/Users/mlakhera@dualasiapacific.com/DMO/bronze/ingest_raw_to_bronze"
SILVER_NOTEBOOK = "/Users/mlakhera@dualasiapacific.com/DMO/silver/transform_bronze_to_silver"
GOLD_NOTEBOOK = "/Users/mlakhera@dualasiapacific.com/DMO/gold/build_gold_reporting"

# COMMAND ----------

# DBTITLE 1,Step 1 - Bronze Ingestion
# =============================================================================
# STEP 1: BRONZE - Ingest from landing zone
# =============================================================================

for source in PIPELINE_SOURCES:
    print(f"\n{'='*60}")
    print(f"BRONZE: Ingesting {source['source_name']}...")
    print(f"{'='*60}")
    
    result = dbutils.notebook.run(
        BRONZE_NOTEBOOK,
        timeout_seconds=3600,
        arguments={
            "source_name": source["source_name"],
            "source_format": source["source_format"],
            "table_name": source["bronze_table"],
        }
    )
    print(f"Result: {result}")

print("\n✓ Bronze ingestion complete.")

# COMMAND ----------

# DBTITLE 1,Step 2 - Silver Transformation
# =============================================================================
# STEP 2: SILVER - Cleanse and conform
# =============================================================================

for source in PIPELINE_SOURCES:
    print(f"\n{'='*60}")
    print(f"SILVER: Transforming {source['source_name']}...")
    print(f"{'='*60}")
    
    result = dbutils.notebook.run(
        SILVER_NOTEBOOK,
        timeout_seconds=3600,
        arguments={
            "source_table": source["bronze_table"],
            "target_table": source["silver_table"],
            "key_columns": source["key_columns"],
            "order_column": source["order_column"],
        }
    )
    print(f"Result: {result}")

print("\n✓ Silver transformation complete.")

# COMMAND ----------

# DBTITLE 1,Step 3 - Gold Reporting
# =============================================================================
# STEP 3: GOLD - Build reporting tables
# =============================================================================

print(f"\n{'='*60}")
print(f"GOLD: Building reporting tables...")
print(f"{'='*60}")

result = dbutils.notebook.run(
    GOLD_NOTEBOOK,
    timeout_seconds=3600,
    arguments={}
)

print(f"Result: {result}")
print("\n✓ Gold layer complete.")
print(f"\nPipeline finished: {datetime.utcnow().isoformat()}")