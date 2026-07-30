# Databricks notebook source
# DBTITLE 1,Data Quality Tests - Setup
# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Tests
# MAGIC 
# MAGIC Validates data quality across all layers. Run after pipeline execution.

import sys
sys.path.append("/Workspace/Users/mlakhera@dualasiapacific.com/DMO")

from pyspark.sql import functions as F
from common.config import CATALOG_NAME, get_table_name
from common.utils import add_null_percentage, check_unique

# COMMAND ----------

# DBTITLE 1,Test Framework
# Simple test framework
test_results = []

def run_test(test_name: str, condition: bool, message: str = ""):
    """Run a test assertion and track results."""
    status = "PASS" if condition else "FAIL"
    test_results.append({"test": test_name, "status": status, "message": message})
    icon = "✅" if condition else "❌"
    print(f"{icon} {test_name}: {status} {message}")
    return condition

# COMMAND ----------

# DBTITLE 1,Test: Table Exists & Has Rows
# =============================================================================
# TEST: Verify tables exist and have data
# Update table list with your actual tables
# =============================================================================

tables_to_check = [
    # ("bronze", "raw_sales"),
    # ("silver", "sales"),
    # ("gold", "fact_daily_summary"),
]

for layer, table in tables_to_check:
    fqn = get_table_name(layer, table)
    try:
        df = spark.read.table(fqn)
        count = df.count()
        run_test(
            f"{fqn} - exists & has data",
            count > 0,
            f"({count:,} rows)"
        )
    except Exception as e:
        run_test(f"{fqn} - exists", False, str(e))

# COMMAND ----------

# DBTITLE 1,Test: Null Checks on Key Columns
# =============================================================================
# TEST: Key columns have no nulls
# =============================================================================

null_checks = [
    # ("silver", "sales", ["transaction_id", "customer_id"]),
    # ("gold", "dim_customer", ["customer_id"]),
]

for layer, table, key_cols in null_checks:
    fqn = get_table_name(layer, table)
    try:
        df = spark.read.table(fqn)
        for col in key_cols:
            null_pct = add_null_percentage(df, col)
            run_test(
                f"{fqn}.{col} - no nulls",
                null_pct == 0,
                f"({null_pct:.2f}% null)"
            )
    except Exception as e:
        run_test(f"{fqn} null check", False, str(e))

# COMMAND ----------

# DBTITLE 1,Test: Uniqueness Checks
# =============================================================================
# TEST: Primary key uniqueness
# =============================================================================

uniqueness_checks = [
    # ("silver", "sales", ["transaction_id"]),
    # ("gold", "dim_customer", ["customer_id"]),
]

for layer, table, key_cols in uniqueness_checks:
    fqn = get_table_name(layer, table)
    try:
        df = spark.read.table(fqn)
        is_unique = check_unique(df, key_cols)
        run_test(
            f"{fqn} - unique on {key_cols}",
            is_unique,
            "" if is_unique else "DUPLICATES FOUND"
        )
    except Exception as e:
        run_test(f"{fqn} uniqueness", False, str(e))

# COMMAND ----------

# DBTITLE 1,Test Results Summary
# =============================================================================
# SUMMARY
# =============================================================================
import pandas as pd

if test_results:
    df_results = pd.DataFrame(test_results)
    passed = len(df_results[df_results["status"] == "PASS"])
    failed = len(df_results[df_results["status"] == "FAIL"])
    total = len(df_results)
    
    print(f"\n{'='*60}")
    print(f"TEST SUMMARY: {passed}/{total} passed, {failed} failed")
    print(f"{'='*60}")
    
    if failed > 0:
        print("\nFailed tests:")
        print(df_results[df_results["status"] == "FAIL"].to_string(index=False))
        raise Exception(f"{failed} data quality test(s) failed!")
else:
    print("No tests configured yet. Uncomment the table lists above.")