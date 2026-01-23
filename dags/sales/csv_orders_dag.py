"""CSV Orders Ingest DAG - Medallion Architecture"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

PROJECT_ID = "agent-ai-test-461120"
DOMAIN = "sales"
PRODUCT = "orders"

default_args = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="csv_orders_medallion_dag",
    default_args=default_args,
    description="CSV Orders: Landing → Bronze → Silver → Gold",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["medallion", "csv", "sales"],
) as dag:

    start = EmptyOperator(task_id="start")
    
    @task
    def bronze_ingest():
        """Ingest raw CSV to Bronze zone"""
        print(f"Bronze: Reading from gs://{PROJECT_ID}-raw-data/{DOMAIN}/{PRODUCT}/")
        return {"status": "success", "records": 1000}
    
    @task
    def silver_transform(bronze_result):
        """Transform Bronze to Silver"""
        print(f"Silver: Cleansing {bronze_result['records']} records")
        return {"status": "success", "records": 950}
    
    @task
    def gold_load(silver_result):
        """Load Silver to Gold/BigQuery"""
        print(f"Gold: Loading {silver_result['records']} records to BigQuery")
        return {"status": "success", "table": f"{PROJECT_ID}.sales_raw.orders"}
    
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)
    
    bronze = bronze_ingest()
    silver = silver_transform(bronze)
    gold = gold_load(silver)
    
    start >> bronze >> silver >> gold >> end
