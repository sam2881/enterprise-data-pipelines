# Pipeline Architecture

## Overview

This repository contains data pipelines for the **data-engineering** team.

## Data Flow

```
SOURCE → RAW → BRONZE → SILVER → [MODELING] → GOLD
```

### Layers

| Layer | Purpose | Storage | Schema |
|-------|---------|---------|--------|
| RAW | Landing zone | `gs://ai-agent-platform-raw/` | As-is |
| BRONZE | Preservation | `gs://ai-agent-platform-bronze/` | All STRING |
| SILVER | Cleansed | `gs://ai-agent-platform-silver/` | Typed |
| GOLD | Analytics | `gs://ai-agent-platform-gold/` | Aggregated |

## DAG Structure

Each DAG follows the canonical 11-phase flow:

1. **File Discovery** - Find files to process
2. **Run Setup** - Generate run IDs, initialize audit
3. **Duplicate Check** - Verify file not already processed
4. **Move to Transient** - Stage files for processing
5. **Schema Validation** - Verify structure matches metadata
6. **Semantic Validation** - Apply data quality rules
7. **Bronze Processing** - Load to bronze (all STRING)
8. **Silver Processing** - Type casting and cleansing
9. **Gold Processing** - Aggregations and modeling
10. **Audit Logging** - Record metrics and lineage
11. **Cleanup** - Archive processed files

## Metadata-Driven Design

All pipeline behavior is driven by metadata stored in PostgreSQL:

- `feed_registry` - Feed configuration
- `feed_columns` - Column definitions per layer
- `feed_validation_rules` - Data quality rules
- `feed_sla` - SLA thresholds
- `file_tracking` - File processing history

## CI/CD

- **develop** → Deploys to DEV
- **main** → Deploys to STAGING
- **Manual** → Deploys to PROD (requires approval)

## Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Validate DAGs
python -c "from airflow.models import DagBag; db = DagBag('dags'); print(db.import_errors or 'OK')"

# Run tests
pytest tests/ -v
```
