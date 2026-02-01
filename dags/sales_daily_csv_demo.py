"""
Sales Daily CSV Demo Pipeline
Generated for demonstration of CI/CD flow: Git → GitHub Actions → GCS → Airflow

Source: gs://agent-ai-test-461120-raw-data/sales/sample_sales.csv
Target: BigQuery sales_data.daily_sales
Schedule: Daily
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.python import PythonOperator

# Default arguments
default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'sales_daily_csv_demo',
    default_args=default_args,
    description='Demo pipeline: Load sales CSV data to BigQuery',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['demo', 'sales', 'csv', 'cicd'],
)

# Schema for BigQuery table
SCHEMA_FIELDS = [
    {'name': 'order_id', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': 'customer_id', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': 'product_name', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': 'quantity', 'type': 'INTEGER', 'mode': 'REQUIRED'},
    {'name': 'unit_price', 'type': 'NUMERIC', 'mode': 'REQUIRED'},
    {'name': 'order_date', 'type': 'DATE', 'mode': 'REQUIRED'},
    {'name': 'region', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': 'payment_method', 'type': 'STRING', 'mode': 'REQUIRED'},
]

# Task: Load CSV from GCS to BigQuery
load_csv_to_bq = GCSToBigQueryOperator(
    task_id='load_sales_csv_to_bigquery',
    bucket='agent-ai-test-461120-raw-data',
    source_objects=['sales/sample_sales.csv'],
    destination_project_dataset_table='agent-ai-test-461120.sales_data.daily_sales',
    schema_fields=SCHEMA_FIELDS,
    write_disposition='WRITE_APPEND',
    skip_leading_rows=1,  # Skip header row
    source_format='CSV',
    create_disposition='CREATE_IF_NEEDED',
    dag=dag,
)

def log_completion(**context):
    """Log pipeline completion."""
    print(f"Pipeline completed successfully at {datetime.utcnow()}")
    print(f"Loaded data from GCS to BigQuery table: sales_data.daily_sales")
    return "success"

log_task = PythonOperator(
    task_id='log_completion',
    python_callable=log_completion,
    provide_context=True,
    dag=dag,
)

# Task dependencies
load_csv_to_bq >> log_task
