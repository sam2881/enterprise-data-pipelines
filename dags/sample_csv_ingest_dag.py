"""sample_csv_ingest_dag - E2E Test Pipeline"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

default_args = {"owner": "data-engineering", "retries": 1, "retry_delay": timedelta(minutes=1)}

def bronze_ingest(**ctx): 
    print("[BRONZE] Ingesting from GCS"); return {"records": 1000}
def silver_transform(**ctx): 
    print("[SILVER] Transforming data"); return {"records": 950}
def gold_load(**ctx): 
    print("[GOLD] Loading to BigQuery"); return {"table": "sales_data.sample_customers", "records": 950}

with DAG("sample_csv_ingest_dag", default_args=default_args, schedule="@daily",
         start_date=datetime(2024,1,1), catchup=False, tags=["e2e-test"]) as dag:
    start = EmptyOperator(task_id="start")
    bronze = PythonOperator(task_id="bronze_ingest", python_callable=bronze_ingest)
    silver = PythonOperator(task_id="silver_transform", python_callable=silver_transform)
    gold = PythonOperator(task_id="gold_load_bigquery", python_callable=gold_load)
    end = EmptyOperator(task_id="end")
    start >> bronze >> silver >> gold >> end
