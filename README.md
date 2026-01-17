# Enterprise Data Pipelines

Auto-generated data pipelines from Data Agent V2.

## Structure

```
enterprise-data-pipelines/
├── metadata/                    # Metadata SQL files
│   ├── insert_metadata_*.sql    # Initial pipeline registration
│   └── update_metadata_*.sql    # Schema evolution
├── dags/                        # Airflow DAGs
│   └── pipeline_*.py            # Orchestration only
├── spark_jobs/
│   └── common/                  # Reusable Spark processors
│       ├── bronze_processor.py  # Bronze layer (all STRING)
│       ├── silver_processor.py  # Silver layer (typed + validated)
│       └── gold_processor.py    # Gold layer (BigQuery)
└── .github/workflows/
    └── deploy.yml               # CI/CD pipeline

## CI/CD Flow

1. Push to main branch
2. GitHub Actions triggered
3. Metadata SQL executed on PostgreSQL
4. Spark jobs uploaded to GCS
5. DAGs deployed to Airflow
6. DAGs run automatically

## Key Principles

- **Metadata-driven**: All config from PostgreSQL, not code
- **Schema evolution**: Supported from day one
- **No hard-coded logic**: Everything from metadata
- **Reusable processors**: Same Spark jobs for all pipelines
