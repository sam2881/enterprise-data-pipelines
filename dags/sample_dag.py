"""
Sample DAG - Template for new pipelines.

WHY: Demonstrates the canonical 11-phase enterprise DAG structure.

HOW: Copy this file and update FEED_ID and configuration.

GENERATED: 2026-01-17T03:54:03.695868
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

# ===========================================================================
# DAG CONFIGURATION (from metadata in production)
# ===========================================================================
FEED_ID = "sample_feed"
DAG_ID = f"pipeline_{FEED_ID}"
SCHEDULE = "@daily"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ===========================================================================
# TASK DEFINITIONS
# ===========================================================================

def discover_files(**context):
    """Phase 1: Discover files to process."""
    # In production: file_validator.discover_files(FEED_ID, pattern, posting_date)
    return [{"file_path": "gs://bucket/sample.csv", "file_name": "sample.csv"}]


def setup_run(**context):
    """Phase 2: Initialize run context."""
    import uuid
    run_id = f"{FEED_ID}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    context["ti"].xcom_push(key="run_id", value=run_id)
    return run_id


def check_for_files(**context):
    """Branch: Check if files were discovered."""
    files = context["ti"].xcom_pull(task_ids="file_discovery.discover")
    if files and len(files) > 0:
        return "process_files"
    return "no_files_found"


# ===========================================================================
# DAG DEFINITION
# ===========================================================================

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description=f"Enterprise pipeline for {FEED_ID}",
    schedule=SCHEDULE,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["enterprise", "sample", "data-engineering"],
) as dag:

    start = EmptyOperator(task_id="start")

    # Phase 1: File Discovery
    with TaskGroup("file_discovery") as tg_discovery:
        discover = PythonOperator(
            task_id="discover",
            python_callable=discover_files,
        )

    # Phase 2: Run Setup
    with TaskGroup("run_setup") as tg_setup:
        setup = PythonOperator(
            task_id="initialize",
            python_callable=setup_run,
        )

    # Branch: Check for files
    check_files = BranchPythonOperator(
        task_id="check_files",
        python_callable=check_for_files,
    )

    # Success path
    process_files = EmptyOperator(task_id="process_files")

    # No files path
    no_files = EmptyOperator(task_id="no_files_found")

    # End
    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    # Define dependencies
    start >> tg_discovery >> tg_setup >> check_files
    check_files >> process_files >> end
    check_files >> no_files >> end
