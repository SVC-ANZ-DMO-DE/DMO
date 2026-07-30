"""DMO Data Warehouse - Configuration

Centralised configuration for the medallion architecture.
Update these values per environment (dev/staging/prod).
"""

# =============================================================================
# CATALOG & SCHEMA CONFIGURATION
# =============================================================================

# Unity Catalog - update per environment
CATALOG_NAME = "dmo"  # Change to dmo_dev / dmo_staging as needed

# Schema names for each layer
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

# =============================================================================
# STORAGE CONFIGURATION
# =============================================================================

# External landing zone for raw files (ADLS / S3)
LANDING_ZONE_PATH = "abfss://landing@<storage_account>.dfs.core.windows.net/"

# Checkpoint base path for streaming
CHECKPOINT_BASE_PATH = "abfss://checkpoints@<storage_account>.dfs.core.windows.net/dmo/"

# =============================================================================
# DATA QUALITY THRESHOLDS
# =============================================================================

# Minimum acceptable row count (fail pipeline if below)
MIN_ROW_COUNT_THRESHOLD = 1

# Maximum acceptable null percentage for key columns
MAX_NULL_PERCENTAGE = 5.0

# =============================================================================
# ENVIRONMENT HELPERS
# =============================================================================

def get_table_name(layer: str, table: str) -> str:
    """Return fully qualified table name: catalog.schema.table"""
    schema_map = {
        "bronze": BRONZE_SCHEMA,
        "silver": SILVER_SCHEMA,
        "gold": GOLD_SCHEMA,
    }
    schema = schema_map.get(layer, layer)
    return f"{CATALOG_NAME}.{schema}.{table}"


def get_checkpoint_path(layer: str, table: str) -> str:
    """Return checkpoint path for a streaming table."""
    return f"{CHECKPOINT_BASE_PATH}{layer}/{table}/_checkpoint"
