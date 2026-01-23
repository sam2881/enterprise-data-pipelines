# Enterprise Data Pipelines
Medallion Architecture: Landing → Bronze → Silver → Gold

## Structure
```
dags/           # Airflow DAGs (project-wise)
scripts/        # SQL scripts (project-wise)  
common/         # Shared: spark/, sql/, functions/
```

## GCS Buckets
- Landing: agent-ai-test-461120-raw-data
- Bronze: agent-ai-test-461120-bronze
- Silver: agent-ai-test-461120-silver
- Gold: agent-ai-test-461120-gold
