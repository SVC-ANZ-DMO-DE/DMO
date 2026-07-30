# DMO Data Warehouse - Technical Architecture Document

**Project**: DMO Data Warehouse  
**Team**: Data Engineering - Dual Asia Pacific  
**Last Updated**: 2026-07-29  
**Status**: In Development  

---

## 1. Executive Summary

The DMO Data Warehouse implements a **Medallion Architecture** (Bronze → Silver → Gold) on Databricks Lakehouse to consolidate reporting data from multiple source systems. The solution provides a single source of truth for business reporting through a layered approach that separates raw ingestion, cleansing, and business logic.

---

## 2. Architecture Overview

### 2.1 High-Level Data Flow

```
┌─────────────────┐     ┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│  Source Systems  │────▶│   Landing Zone   │────▶│     Databricks   │────▶│   BI / Reports   │
│                  │     │   (ADLS Gen2)    │     │    Lakehouse     │     │                  │
│ - ERP            │     │                  │     │                  │     │ - Power BI       │
│ - CRM            │     │  Raw files:      │     │  Bronze → Silver │     │ - Tableau        │
│ - Flat files     │     │  CSV, JSON,      │     │  → Gold          │     │ - SQL Analytics  │
│ - APIs           │     │  Parquet         │     │                  │     │ - Dashboards     │
└─────────────────┘     └──────────────────┘     └──────────────────┘     └──────────────────┘
```

### 2.2 Medallion Layers

| Layer | Schema | Purpose | Data Characteristics | Retention |
|-------|--------|---------|---------------------|------------|
| **Bronze** | `bronze` | Raw ingestion | Exact copy of source, append-only, no transforms | 90 days |
| **Silver** | `silver` | Cleansed & conformed | Deduplicated, typed, validated, merged | Indefinite |
| **Gold** | `gold` | Business-ready | Aggregated, denormalised, optimised for queries | Indefinite |

---

## 3. Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|----------|
| Compute | Databricks (Azure) | Spark processing |
| Storage | Azure Data Lake Storage Gen2 | Raw file storage |
| Table Format | Delta Lake | ACID transactions, time travel |
| Catalog | Unity Catalog | Governance, access control |
| Orchestration | Databricks Jobs | Scheduling, monitoring |
| Ingestion | Auto Loader (cloudFiles) | Incremental file processing |
| Version Control | GitHub | Code versioning |
| CI/CD | GitHub Actions (planned) | Automated deployment |

---

## 4. Layer Specifications

### 4.1 Bronze Layer (Raw)

**Objective**: Land raw data exactly as received with minimal processing.

**Design Principles**:
* No business logic or transformations
* Append-only (never update or delete)
* Schema evolution enabled (new columns auto-added)
* Audit columns track provenance

**Ingestion Patterns**:

| Pattern | Use Case | Trigger |
|---------|----------|----------|
| Auto Loader (Streaming) | Continuous file arrival | `availableNow=True` (micro-batch) |
| Batch Read | Scheduled full/delta loads | Triggered by Job |
| COPY INTO | One-time bulk loads | Manual / migration |

**Standard Audit Columns** (added to every bronze table):

| Column | Type | Description |
|--------|------|-------------|
| `_ingested_at` | timestamp | When the record was ingested |
| `_source_name` | string | Source system identifier |
| `_file_path` | string | Original file path (from `input_file_name()`) |

**Naming Convention**: `bronze.raw_<source_entity>`  
**Example**: `dmo.bronze.raw_sales`, `dmo.bronze.raw_customers`

---

### 4.2 Silver Layer (Cleansed)

**Objective**: Provide a cleansed, deduplicated, and conformed version of each entity.

**Design Principles**:
* Single source of truth per entity
* Deduplication on primary key (keep latest)
* Data quality enforcement (null checks, type validation)
* Bad records quarantined, not dropped silently
* MERGE (upsert) pattern for incremental updates

**Transformation Steps**:
1. Read incremental data from bronze
2. Deduplicate by primary key (window function, keep latest `_ingested_at`)
3. Data quality validation (null keys → quarantine table)
4. Type casting and column standardisation
5. MERGE into silver table (update existing, insert new)

**Standard Audit Columns**:

| Column | Type | Description |
|--------|------|-------------|
| `_processed_at` | timestamp | When the record was transformed |
| `_is_valid` | boolean | Whether the record passed all quality checks |

**Naming Convention**: `silver.<entity>`  
**Example**: `dmo.silver.sales`, `dmo.silver.customers`

---

### 4.3 Gold Layer (Business-Ready)

**Objective**: Provide pre-computed, reporting-optimised tables for BI consumers.

**Design Principles**:
* Optimised for query performance (pre-aggregated where appropriate)
* Dimensional modelling (star schema)
* Business-friendly column names
* Documentation via Unity Catalog column comments
* Liquid clustering for optimal file layout

**Table Types**:

| Type | Prefix | Update Strategy | Example |
|------|--------|----------------|---------|
| Fact tables | `fact_` | Incremental MERGE | `gold.fact_daily_sales` |
| Dimension tables | `dim_` | Full overwrite (SCD1) | `gold.dim_customer` |
| Aggregate tables | `agg_` | Full overwrite | `gold.agg_monthly_revenue` |
| Views | `v_` | Live query | `gold.v_sales_ytd` |

**Naming Convention**: `gold.<type>_<business_entity>`  
**Example**: `dmo.gold.fact_daily_sales`, `dmo.gold.dim_customer`

---

## 5. Data Quality Framework

### 5.1 Quality Checks by Layer

| Check | Bronze | Silver | Gold |
|-------|--------|--------|------|
| Schema validation | ✓ (Auto Loader) | ✓ | ✓ |
| Null key columns | — | ✓ (quarantine) | ✓ (fail) |
| Uniqueness on PK | — | ✓ (dedup) | ✓ (assert) |
| Row count > 0 | ✓ | ✓ | ✓ |
| Referential integrity | — | — | ✓ |
| Business rules | — | — | ✓ |

### 5.2 Quarantine Strategy

Records failing silver-layer quality checks are:
1. Written to a `<table>_quarantine` table
2. Logged with failure reason
3. Excluded from downstream processing
4. Reviewed and resolved by data stewards

### 5.3 Monitoring

* **Databricks Data Quality Monitoring**: Enabled on gold tables for drift detection
* **Pipeline alerts**: Notify on job failure, DQ threshold breach, or zero rows
* **Row count tracking**: Logged per run for trend analysis

---

## 6. Security & Governance

### 6.1 Unity Catalog Access Control

| Role | Bronze | Silver | Gold |
|------|--------|--------|------|
| Data Engineers | Read/Write | Read/Write | Read/Write |
| Data Analysts | — | Read | Read |
| BI Service Accounts | — | — | Read |
| Data Stewards | Read | Read | Read |

### 6.2 Service Principal

* **Name**: `SVC_ANZ_DMO_DE`
* **Purpose**: Pipeline execution, scheduled jobs, Git integration
* **Permissions**: Write to all DMO schemas, read from landing zone

### 6.3 Data Classification

| Tag | Description | Handling |
|-----|-------------|----------|
| `PII` | Personally identifiable information | Mask in silver, exclude from gold views |
| `SENSITIVE` | Business-sensitive data | Restrict to authorised groups |
| `PUBLIC` | Non-sensitive data | Available to all analysts |

---

## 7. Operational Runbook

### 7.1 Daily Operations

| Time (AEST) | Action | Notebook |
|-------------|--------|----------|
| 06:00 | Pipeline scheduled run | `pipelines/run_pipeline` |
| 06:30 | DQ tests (automated) | `tests/test_data_quality` |
| 07:00 | Gold tables available for BI refresh | — |

### 7.2 Adding a New Source

1. Create landing zone folder: `<landing_zone>/<source_name>/`
2. Add entry to `PIPELINE_SOURCES` in `pipelines/run_pipeline`
3. (Optional) Create custom silver transformation notebook if complex logic needed
4. Add DQ checks to `tests/test_data_quality`
5. Commit, PR review, merge to `main`
6. Verify in dev environment before promoting to prod

### 7.3 Troubleshooting

| Symptom | Likely Cause | Resolution |
|---------|-------------|------------|
| Bronze table empty | No files in landing zone | Check source system delivery |
| Silver MERGE fails | Schema mismatch (new columns) | Enable `mergeSchema` or update silver DDL |
| Gold aggregation wrong | Duplicate keys in silver | Check dedup logic, verify bronze data |
| Auto Loader stuck | Checkpoint corruption | Delete checkpoint, reprocess |
| Job timeout | Large backlog of files | Increase cluster size, process in batches |

### 7.4 Disaster Recovery

* **Delta Time Travel**: Restore tables to previous version (up to 30 days)
* **Landing Zone**: Files retained for 90 days for reprocessing
* **Git**: All code versioned; redeploy from `main` branch
* **Checkpoints**: Recreatable by reprocessing from landing zone

---

## 8. Development Workflow

### 8.1 Git Branching Strategy

```
main (production)
 ├── develop (integration)
 │   ├── feature/add-source-xyz
 │   ├── feature/silver-transform-customers
 │   └── fix/dedup-logic-sales
 └── hotfix/critical-fix
```

### 8.2 Environments

| Branch | Environment | Catalog | Auto-Deploy |
|--------|-------------|---------|-------------|
| `main` | Production | `dmo` | Yes (after approval) |
| `develop` | Staging | `dmo_staging` | Yes |
| `feature/*` | Dev | `dmo_dev` | Manual |

### 8.3 Code Review Checklist

- [ ] Follows naming conventions
- [ ] DQ checks added for new tables
- [ ] No hardcoded values (use `config.py`)
- [ ] Tested on dev with sample data
- [ ] README/docs updated if architecture changed
- [ ] No PII exposed in gold without masking

---

## 9. Performance Optimisation

### 9.1 Table Optimisation

| Technique | When to Use | Command |
|-----------|------------|----------|
| Liquid Clustering | Frequently filtered columns | `CLUSTER BY (col1, col2)` |
| Z-Ordering | Legacy tables, large scans | `OPTIMIZE table ZORDER BY (col)` |
| Auto-Compaction | Streaming tables with small files | Enabled by default |
| Vacuum | Reclaim storage after deletes | `VACUUM table RETAIN 168 HOURS` |

### 9.2 Compute Sizing Guidelines

| Workload | Cluster Type | Size | Notes |
|----------|-------------|------|-------|
| Bronze ingestion | Jobs cluster | Small-Medium | I/O bound, not compute |
| Silver transforms | Jobs cluster | Medium | Depends on data volume |
| Gold aggregations | Jobs cluster | Medium-Large | Wide shuffles for GROUP BY |
| Ad-hoc / Dev | All-purpose | Small | Interactive, autoscale |

---

## 10. Future Roadmap

| Phase | Scope | Target |
|-------|-------|--------|
| Phase 1 | Core medallion architecture + first sources | Current |
| Phase 2 | CI/CD pipeline, automated testing | Q4 2026 |
| Phase 3 | Real-time streaming (structured streaming) | Q1 2027 |
| Phase 4 | ML feature store integration | Q2 2027 |
| Phase 5 | Data mesh / domain-owned datasets | Q3 2027 |

---

## Appendix A: File & Folder Reference

```
DMO/
├── README.md                           # Quick-start guide
├── docs/
│   └── ARCHITECTURE.md                 # This document
├── common/
│   ├── config.py                       # Environment config (catalogs, paths)
│   └── utils.py                        # Shared functions (DQ, audit, dedup)
├── bronze/
│   └── ingest_raw_to_bronze            # Auto Loader ingestion notebook
├── silver/
│   └── transform_bronze_to_silver      # Cleanse + MERGE notebook
├── gold/
│   └── build_gold_reporting            # Fact/dim/aggregate builders
├── pipelines/
│   └── run_pipeline                    # Orchestrator (bronze → silver → gold)
└── tests/
    └── test_data_quality               # Automated DQ validation
```

---

## Appendix B: Glossary

| Term | Definition |
|------|------------|
| **Medallion Architecture** | Layered data design: raw (Bronze) → cleansed (Silver) → business (Gold) |
| **Auto Loader** | Databricks incremental file ingestion using `cloudFiles` format |
| **MERGE** | SQL operation that combines INSERT and UPDATE (upsert) |
| **SCD** | Slowly Changing Dimension - tracks how dimension attributes change over time |
| **Quarantine** | Holding area for records that fail data quality checks |
| **Unity Catalog** | Databricks governance layer for access control, lineage, and discovery |
| **Delta Lake** | Open table format providing ACID transactions on data lakes |
| **Liquid Clustering** | Automatic data layout optimisation in Delta Lake |

---

*Document maintained by the DMO Data Engineering team.*
