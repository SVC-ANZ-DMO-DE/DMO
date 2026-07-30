# Contributing to DMO Data Warehouse

Guidelines for the data engineering team when contributing to this project.

---

## Getting Started

1. Clone the repository via Databricks Git Folders (Repos)
2. Create a feature branch from `develop`
3. Make changes, test in `dmo_dev` catalog
4. Submit a Pull Request for review

---

## Coding Standards

### Python

* Use **PEP 8** style conventions
* Maximum line length: 120 characters
* Use type hints for function signatures
* Docstrings for all public functions (Google style)
* Import order: stdlib → third-party → local (`common.*`)

### SQL

* Keywords in UPPERCASE (`SELECT`, `FROM`, `WHERE`)
* Indentation: 4 spaces
* One column per line in SELECT statements
* Always qualify column names with table aliases in joins
* Use CTEs (`WITH`) over nested subqueries

### Naming Conventions

| Object | Convention | Example |
|--------|-----------|----------|
| Tables (bronze) | `raw_<source_entity>` | `raw_sales` |
| Tables (silver) | `<entity>` | `sales` |
| Tables (gold - fact) | `fact_<metric>` | `fact_daily_sales` |
| Tables (gold - dim) | `dim_<entity>` | `dim_customer` |
| Tables (gold - agg) | `agg_<aggregation>` | `agg_monthly_revenue` |
| Views | `v_<description>` | `v_sales_ytd` |
| Columns | `snake_case` | `customer_id`, `order_date` |
| Audit columns | `_<name>` (underscore prefix) | `_ingested_at` |
| Notebooks | `<verb>_<object>` | `ingest_raw_to_bronze` |
| Config constants | `UPPER_SNAKE_CASE` | `CATALOG_NAME` |

---

## Adding a New Data Source

### Checklist

- [ ] 1. Confirm source system, delivery format, and frequency
- [ ] 2. Create landing zone folder in ADLS
- [ ] 3. Add source config to `PIPELINE_SOURCES` in `pipelines/run_pipeline`
- [ ] 4. If custom silver logic needed, create dedicated notebook in `silver/`
- [ ] 5. Define gold tables/views for reporting
- [ ] 6. Add DQ tests to `tests/test_data_quality`
- [ ] 7. Update `docs/ARCHITECTURE.md` entity catalogue
- [ ] 8. Test end-to-end in `dmo_dev`
- [ ] 9. Submit PR with description of source and schema

### Template: New Silver Notebook

When a source needs custom transformation beyond the generic template:

1. Copy `silver/transform_bronze_to_silver` as a starting point
2. Name it: `silver/transform_<entity>.py`
3. Customise the "Transformations" cell with source-specific logic
4. Update `run_pipeline` to call the custom notebook for that source

---

## Pull Request Guidelines

### PR Title Format

```
[LAYER] Brief description
```

Examples:
* `[BRONZE] Add SAP invoice ingestion`
* `[SILVER] Fix dedup logic for customer table`
* `[GOLD] Add monthly revenue aggregate`
* `[INFRA] Update compute config for production`

### PR Description Template

```markdown
## Summary
Brief description of what this PR does.

## Changes
- Added/Modified/Removed: ...

## Testing
- [ ] Tested in dmo_dev catalog
- [ ] DQ tests pass
- [ ] No breaking changes to downstream tables

## Affected Tables
- `dmo.silver.xyz`
- `dmo.gold.fact_xyz`
```

### Review Criteria

* Code follows naming conventions
* No hardcoded values (all config in `common/config.py`)
* DQ checks added for new/modified tables
* No secrets or credentials in code
* Pipeline tested end-to-end

---

## Environment Management

### Switching Environments

Update `CATALOG_NAME` in `common/config.py`:

```python
# Development
CATALOG_NAME = "dmo_dev"

# Staging
CATALOG_NAME = "dmo_staging"

# Production (DO NOT change directly - handled by CI/CD)
CATALOG_NAME = "dmo"
```

### Testing in Dev

1. Set `CATALOG_NAME = "dmo_dev"` in config
2. Place sample files in dev landing zone
3. Run `pipelines/run_pipeline` end-to-end
4. Verify results with `tests/test_data_quality`
5. Reset config before committing

---

## Troubleshooting Common Issues

| Issue | Solution |
|-------|----------|
| `AnalysisException: Table not found` | Check catalog/schema in config.py; verify Unity Catalog permissions |
| Auto Loader schema mismatch | Delete `_schema` checkpoint folder and reprocess |
| MERGE conflict (duplicate keys) | Check dedup logic; verify key_columns parameter |
| `Permission denied` on write | Verify service principal has WRITE on target schema |
| Notebook not found in `dbutils.notebook.run` | Use absolute workspace path, not relative |

---

## Contacts

| Role | Name | Responsibility |
|------|------|----------------|
| Lead Engineer | mlakhera@dualasiapacific.com | Architecture, code reviews |
| Data Steward | TBD | DQ issue resolution, quarantine review |
| Platform Admin | TBD | Cluster config, Unity Catalog permissions |

---

*Keep this guide updated as team practices evolve.*
