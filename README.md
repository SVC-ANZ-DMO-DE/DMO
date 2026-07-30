# DMO Data Warehouse

Medallion architecture (Bronze → Silver → Gold) data engineering project for reporting.

## Project Structure

```
DMO/
├── README.md                           # This file
├── docs/
│   ├── ARCHITECTURE.md                 # Full technical architecture & design decisions
│   └── CONTRIBUTING.md                 # Team coding standards & PR guidelines
├── common/
│   ├── config.py                       # Centralised configuration (catalogs, schemas, paths)
│   └── utils.py                        # Shared utilities (DQ checks, audit columns, dedup)
├── bronze/
│   └── ingest_raw_to_bronze.py         # Auto Loader ingestion from landing zone
├── silver/
│   └── transform_bronze_to_silver.py   # Cleanse, deduplicate, MERGE into silver
├── gold/
│   └── build_gold_reporting.py         # Aggregations, dimensions, reporting views
├── pipelines/
│   └── run_pipeline.py                 # Orchestrator: runs bronze → silver → gold
└── tests/
    └── test_data_quality.py            # Data quality validation tests
```

## Architecture

| Layer  | Purpose                        | Pattern              | Update Mode     |
|--------|--------------------------------|----------------------|-----------------|
| Bronze | Raw ingestion (no transforms)  | Auto Loader / Batch  | Append          |
| Silver | Cleansed & conformed           | MERGE (upsert)       | Incremental     |
| Gold   | Business-ready reporting       | Overwrite / MERGE    | Full / Incr.    |

## Quick Start

1. **Update configuration**: Edit `common/config.py` with your catalog name, storage paths, and schema names.
2. **Add sources**: In `pipelines/run_pipeline.py`, populate the `PIPELINE_SOURCES` list with your source systems.
3. **Customise transformations**: In each silver notebook, add table-specific cleansing logic.
4. **Build gold tables**: In the gold notebook, define your aggregations and dimensional models.
5. **Schedule**: Create a Databricks Job pointing to `pipelines/run_pipeline.py`.

## Conventions

- **Table naming**: `<layer>.<entity>` (e.g., `bronze.raw_sales`, `silver.sales`, `gold.fact_daily_summary`)
- **Audit columns**: All tables include `_ingested_at`, `_processed_at`, or `_refreshed_at`
- **Key columns**: Every silver/gold table must have a defined primary key
- **Data quality**: Null checks on key columns; quarantine bad records
- **Git workflow**: Feature branches → PR review → merge to `main` → deploy

## Environment Setup

| Environment | Catalog      | Notes                        |
|-------------|--------------|------------------------------|
| Dev         | `dmo_dev`    | Developer sandboxes          |
| Staging     | `dmo_staging`| Integration testing          |
| Production  | `dmo`        | Scheduled, monitored         |

## Team

- **Owner**: Data Engineering - Dual Asia Pacific
- **Contact**: mlakhera@dualasiapacific.com
