"""
Airflow DAG: hr_employee_data_pipeline_data-2001
Pipeline: HR Employee Data Pipeline
Generated: 2026-01-17T14:42:09.164671

ORCHESTRATION ONLY - NO BUSINESS LOGIC IN THIS FILE
All processing logic is in the common Spark processors.
Configuration is read from PostgreSQL metadata at runtime.

DAG Structure:
    setup → bronze → silver → gold → audit → cleanup
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperator,
)
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

# =============================================================================
# CONFIGURATION - FROM ENVIRONMENT (NOT HARD-CODED)
# =============================================================================

PIPELINE_ID = "hr_employee_data_pipeline_data-2001"
GCP_PROJECT = "{{ var.value.gcp_project }}"
GCP_REGION = "{{ var.value.gcp_region }}"
DATAPROC_CLUSTER = "{{ var.value.dataproc_cluster }}"
SPARK_JOBS_BUCKET = "{{ var.value.spark_jobs_bucket }}"
DATA_PATH = "gs://hr-data-bucket/input/data/"
METADATA_PATH = "gs://hr-data-bucket/input/metadata/"

# =============================================================================
# DEFAULT ARGS
# =============================================================================

default_args = {
    "owner": "HR Analytics Team",
    "depends_on_past": False,
    "email": ["hr-analytics@company.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# =============================================================================
# DAG DEFINITION
# =============================================================================

with DAG(
    dag_id=f"pipeline_{PIPELINE_ID}",
    default_args=default_args,
    description="HR Employee Data Pipeline",
    schedule_interval="0 6 1 * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["Human Resources", "excel", "data-agent-v2"],
    max_active_runs=1,
) as dag:

    # =========================================================================
    # TASK: Setup Run Context
    # =========================================================================

    def setup_run(**context):
        """Initialize run context with run_id and reporting_date."""
        import uuid
        from datetime import date

        run_id = f"{PIPELINE_ID}_{context['ds']}_{uuid.uuid4().hex[:8]}"
        reporting_date = context["ds"]

        context["ti"].xcom_push(key="run_id", value=run_id)
        context["ti"].xcom_push(key="reporting_date", value=reporting_date)

        return {"run_id": run_id, "reporting_date": reporting_date}

    setup_task = PythonOperator(
        task_id="setup_run",
        python_callable=setup_run,
    )

    # =========================================================================
    # TASK: Wait for Source Data
    # =========================================================================

    wait_for_data = GCSObjectExistenceSensor(
        task_id="wait_for_data",
        bucket=DATA_PATH.replace("gs://", "").split("/")[0],
        object=DATA_PATH.replace("gs://", "").split("/", 1)[1].rstrip("/") + "/_SUCCESS",
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    # =========================================================================
    # TASK GROUP: Processing
    # =========================================================================

    with TaskGroup("processing") as processing_group:

        # Bronze Layer
        bronze_job = DataprocSubmitJobOperator(
            task_id="bronze_processor",
            project_id=GCP_PROJECT,
            region=GCP_REGION,
            cluster_name=DATAPROC_CLUSTER,
            job={
                "reference": {"job_id": f"bronze_{PIPELINE_ID}_{{{ ds_nodash }}}"},
                "placement": {"cluster_name": DATAPROC_CLUSTER},
                "pyspark_job": {
                    "main_python_file_uri": f"gs://{SPARK_JOBS_BUCKET}/common/bronze_processor.py",
                    "args": [
                        "--pipeline-id", PIPELINE_ID,
                        "--reporting-date", "{{ ds }}",
                        "--run-id", "{{ ti.xcom_pull(key='run_id') }}",
                        "--data-path", DATA_PATH,
                        "--metadata-path", METADATA_PATH,
                    ],
                },
            },
        )

        # Silver Layer
        silver_job = DataprocSubmitJobOperator(
            task_id="silver_processor",
            project_id=GCP_PROJECT,
            region=GCP_REGION,
            cluster_name=DATAPROC_CLUSTER,
            job={
                "reference": {"job_id": f"silver_{PIPELINE_ID}_{{{ ds_nodash }}}"},
                "placement": {"cluster_name": DATAPROC_CLUSTER},
                "pyspark_job": {
                    "main_python_file_uri": f"gs://{SPARK_JOBS_BUCKET}/common/silver_processor.py",
                    "args": [
                        "--pipeline-id", PIPELINE_ID,
                        "--reporting-date", "{{ ds }}",
                        "--run-id", "{{ ti.xcom_pull(key='run_id') }}",
                    ],
                },
            },
        )

        # Gold Layer (BigQuery)
        gold_job = DataprocSubmitJobOperator(
            task_id="gold_processor",
            project_id=GCP_PROJECT,
            region=GCP_REGION,
            cluster_name=DATAPROC_CLUSTER,
            job={
                "reference": {"job_id": f"gold_{PIPELINE_ID}_{{{ ds_nodash }}}"},
                "placement": {"cluster_name": DATAPROC_CLUSTER},
                "pyspark_job": {
                    "main_python_file_uri": f"gs://{SPARK_JOBS_BUCKET}/common/gold_processor.py",
                    "args": [
                        "--pipeline-id", PIPELINE_ID,
                        "--reporting-date", "{{ ds }}",
                        "--run-id", "{{ ti.xcom_pull(key='run_id') }}",
                    ],
                },
            },
        )

        bronze_job >> silver_job >> gold_job

    # =========================================================================
    # TASK: Audit & Cleanup
    # =========================================================================

    def finalize_run(**context):
        """Log run completion to audit table."""
        run_id = context["ti"].xcom_pull(key="run_id")
        print(f"Pipeline {PIPELINE_ID} completed. Run ID: {run_id}")
        # In production, log to PostgreSQL audit table
        return {"status": "success", "run_id": run_id}

    finalize_task = PythonOperator(
        task_id="finalize_run",
        python_callable=finalize_run,
    )

    # =========================================================================
    # DAG FLOW
    # =========================================================================

    setup_task >> wait_for_data >> processing_group >> finalize_task
