# Databricks notebook source
# DBTITLE 1,Gold Layer - Setup
# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Business-Ready Reporting Tables
# MAGIC 
# MAGIC This notebook creates reporting-optimised tables for downstream consumption:
# MAGIC - **Aggregations**: Pre-computed metrics and KPIs
# MAGIC - **Dimensional Models**: Star/snowflake schema for BI tools
# MAGIC - **Materialised Views**: Refreshable reporting datasets
# MAGIC - **Consumers**: Power BI, Tableau, SQL Analytics, Databricks dashboards

import sys
sys.path.append("/Workspace/Users/mlakhera@dualasiapacific.com/DMO")

from pyspark.sql import functions as F
from common.config import CATALOG_NAME, SILVER_SCHEMA, GOLD_SCHEMA, get_table_name
from common.utils import log_pipeline_event

# COMMAND ----------

# DBTITLE 1,Create Gold Schema (if not exists)
# MAGIC %sql
# MAGIC -- Ensure gold schema exists
# MAGIC CREATE SCHEMA IF NOT EXISTS ${catalog}.gold
# MAGIC COMMENT 'Gold layer - business-ready reporting tables';

# COMMAND ----------

# DBTITLE 1,Example: Fact Table - Aggregated Metrics
# =============================================================================
# EXAMPLE: Build a fact table from silver
# Replace with your actual silver tables and business logic
# =============================================================================

# Read silver tables
# df_transactions = spark.read.table(get_table_name("silver", "transactions"))
# df_customers = spark.read.table(get_table_name("silver", "customers"))

# Example aggregation: daily summary fact table
# df_daily_summary = (
#     df_transactions
#     .groupBy(
#         F.col("transaction_date").alias("date_key"),
#         F.col("region"),
#         F.col("product_category")
#     )
#     .agg(
#         F.count("*").alias("transaction_count"),
#         F.sum("amount").alias("total_amount"),
#         F.avg("amount").alias("avg_amount"),
#         F.countDistinct("customer_id").alias("unique_customers")
#     )
#     .withColumn("_refreshed_at", F.current_timestamp())
# )

# Write as gold table (overwrite for full refresh, or merge for incremental)
# (
#     df_daily_summary.write
#     .format("delta")
#     .mode("overwrite")
#     .option("overwriteSchema", "true")
#     .saveAsTable(get_table_name("gold", "fact_daily_summary"))
# )

print("Gold fact table template ready - customise with your silver tables.")

# COMMAND ----------

# DBTITLE 1,Example: Dimension Table
# =============================================================================
# EXAMPLE: Build a dimension table (SCD Type 1 - overwrite)
# =============================================================================

# df_dim_customer = (
#     spark.read.table(get_table_name("silver", "customers"))
#     .select(
#         F.col("customer_id"),
#         F.col("customer_name"),
#         F.col("email"),
#         F.col("region"),
#         F.col("segment"),
#         F.col("created_date"),
#         F.current_timestamp().alias("_refreshed_at")
#     )
# )

# (
#     df_dim_customer.write
#     .format("delta")
#     .mode("overwrite")
#     .saveAsTable(get_table_name("gold", "dim_customer"))
# )

print("Gold dimension table template ready.")

# COMMAND ----------

# DBTITLE 1,Example: SQL-based Gold View (Alternative)
# MAGIC %sql
# MAGIC -- Alternative: Create gold views using SQL for simpler aggregations
# MAGIC -- Uncomment and customise:
# MAGIC
# MAGIC -- CREATE OR REPLACE VIEW ${catalog}.gold.v_sales_summary AS
# MAGIC -- SELECT
# MAGIC --     date_trunc('month', transaction_date) AS month,
# MAGIC --     region,
# MAGIC --     COUNT(*) AS total_transactions,
# MAGIC --     SUM(amount) AS revenue,
# MAGIC --     AVG(amount) AS avg_order_value
# MAGIC -- FROM ${catalog}.silver.transactions
# MAGIC -- GROUP BY 1, 2;