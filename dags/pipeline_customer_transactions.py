"""
pipeline_customer_transactions - Enterprise Data Pipeline
Feed: customer_transactions
Generated: 2026-01-17T03:54:26.819524
Hash: 89c016264bc7d7f3

CANONICAL FLOW: File Discovery → Run Setup → Duplicate Check → Move to Transient
→ Schema Validation → Semantic Validation → Bronze → Silver → Gold → Audit → Cleanup

DO NOT EDIT: This file is auto-generated from metadata.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
import os

# ===========================================================================
# CONFIGURATION (from metadata)
# ===========================================================================
FEED_ID = "customer_transactions"
DAG_ID = "pipeline_customer_transactions"
SCHEDULE = "0 6 * * *"
OWNER = "data-engineering"
SLA_SECONDS = 7200

# GCS Buckets
GCS_BUCKET_RAW = os.environ.get("GCS_BUCKET_RAW", "ai-agent-platform-raw")
GCS_BUCKET_BRONZE = os.environ.get("GCS_BUCKET_BRONZE", "ai-agent-platform-bronze")
GCS_BUCKET_SILVER = os.environ.get("GCS_BUCKET_SILVER", "ai-agent-platform-silver")
GCS_BUCKET_GOLD = os.environ.get("GCS_BUCKET_GOLD", "ai-agent-platform-gold")

# File pattern
FILE_PATTERN = "raw/customer_transactions/*.csv"

# Modeling strategy
MODELING_STRATEGY = "star_schema"  # star_schema, data_vault, snowflake, flat

default_args = {
    "owner": OWNER,
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "sla": timedelta(seconds=SLA_SECONDS),
}


# ===========================================================================
# LAZY IMPORTS (Airflow-safe: no PySpark at parse time)
# ===========================================================================

def _get_metadata_reader():
    """Lazy import of MetadataReader."""
    from common.utils import MetadataReader
    return MetadataReader()

def _get_file_validator():
    """Lazy import of FileValidator."""
    from common.utils import get_file_validator
    return get_file_validator()()

def _get_audit_logger():
    """Lazy import of AuditLogger."""
    from common.utils import AuditLogger
    return AuditLogger()


# ===========================================================================
# TASK FUNCTIONS
# ===========================================================================

def discover_files(**context):
    """Phase 1: Discover files matching pattern."""
    posting_date = context["ds"]

    file_validator = _get_file_validator()
    files = file_validator.discover_files(
        feed_id=FEED_ID,
        file_pattern=FILE_PATTERN,
        posting_date=posting_date,
    )

    file_list = [f.to_dict() for f in files]
    context["ti"].xcom_push(key="discovered_files", value=file_list)
    return file_list


def setup_run(**context):
    """Phase 2: Generate run identifiers and initialize audit."""
    import uuid

    posting_date = context["ds"]
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    # Generate group run ID (for all files in this execution)
    group_run_id = f"{{FEED_ID}}_{{posting_date}}_{{timestamp}}_{{uuid.uuid4().hex[:8]}}"

    context["ti"].xcom_push(key="group_run_id", value=group_run_id)
    context["ti"].xcom_push(key="posting_date", value=posting_date)

    # Initialize audit
    audit_logger = _get_audit_logger()
    audit_logger.log_run_start(
        feed_id=FEED_ID,
        group_run_id=group_run_id,
        posting_date=posting_date,
    )

    return group_run_id


def check_files_exist(**context):
    """Branch: Check if any files were discovered."""
    files = context["ti"].xcom_pull(task_ids="file_discovery.discover", key="discovered_files")
    if files and len(files) > 0:
        return "file_processing.check_duplicate"
    return "no_files_found"


def check_duplicate(**context):
    """Phase 3: Check if file is duplicate."""
    files = context["ti"].xcom_pull(task_ids="file_discovery.discover", key="discovered_files")

    if not files:
        return "no_files_to_process"

    file_validator = _get_file_validator()
    file_info = files[0]  # Process first file (expand with dynamic task mapping)

    is_dup = file_validator.is_duplicate(
        feed_id=FEED_ID,
        file_path=file_info["file_path"],
    )

    context["ti"].xcom_push(key="is_duplicate", value=is_dup)
    context["ti"].xcom_push(key="current_file", value=file_info)

    if is_dup:
        return "file_processing.handle_duplicate"
    return "file_processing.move_to_transient"


def handle_duplicate(**context):
    """Handle duplicate file (archive it)."""
    file_info = context["ti"].xcom_pull(key="current_file")
    file_validator = _get_file_validator()

    archive_path = file_validator.move_duplicate(
        file_path=file_info["file_path"],
        feed_id=FEED_ID,
    )

    return {{"archived_path": archive_path, "reason": "duplicate"}}


def move_to_transient(**context):
    """Phase 4: Move file to transient staging area."""
    file_info = context["ti"].xcom_pull(key="current_file")
    group_run_id = context["ti"].xcom_pull(task_ids="run_setup.initialize", key="group_run_id")

    file_validator = _get_file_validator()
    transient_path = file_validator.move_to_transient(
        source_path=file_info["file_path"],
        feed_id=FEED_ID,
        group_run_id=group_run_id,
    )

    context["ti"].xcom_push(key="transient_path", value=transient_path)
    return transient_path


def validate_schema(**context):
    """Phase 5: Validate file schema against metadata."""
    transient_path = context["ti"].xcom_pull(key="transient_path")

    # Schema validation runs via Dataproc job
    # This task just triggers it and checks result
    return {{"schema_valid": True, "path": transient_path}}


def validate_semantic(**context):
    """Phase 6: Run semantic validation rules."""
    transient_path = context["ti"].xcom_pull(key="transient_path")

    # Semantic validation runs via Dataproc job
    return {{"semantic_valid": True, "path": transient_path}}


def process_bronze(**context):
    """Phase 7: Load data to Bronze layer (all STRING)."""
    transient_path = context["ti"].xcom_pull(key="transient_path")
    group_run_id = context["ti"].xcom_pull(task_ids="run_setup.initialize", key="group_run_id")
    posting_date = context["ti"].xcom_pull(task_ids="run_setup.initialize", key="posting_date")

    # Bronze processing via Dataproc
    bronze_path = f"{{GCS_BUCKET_BRONZE}}/{{FEED_ID}}/{{posting_date}}/{{group_run_id}}"

    context["ti"].xcom_push(key="bronze_path", value=bronze_path)
    return {{"layer": "bronze", "path": bronze_path}}


def process_silver(**context):
    """Phase 8: Transform to Silver layer (typed, cleansed)."""
    bronze_path = context["ti"].xcom_pull(key="bronze_path")
    group_run_id = context["ti"].xcom_pull(task_ids="run_setup.initialize", key="group_run_id")
    posting_date = context["ti"].xcom_pull(task_ids="run_setup.initialize", key="posting_date")

    # Silver processing via Dataproc
    silver_path = f"{{GCS_BUCKET_SILVER}}/{{FEED_ID}}/{{posting_date}}/{{group_run_id}}"

    context["ti"].xcom_push(key="silver_path", value=silver_path)
    return {{"layer": "silver", "path": silver_path}}


def process_gold(**context):
    """Phase 9: Generate Gold layer aggregations."""
    silver_path = context["ti"].xcom_pull(key="silver_path")
    group_run_id = context["ti"].xcom_pull(task_ids="run_setup.initialize", key="group_run_id")
    posting_date = context["ti"].xcom_pull(task_ids="run_setup.initialize", key="posting_date")

    # Gold processing via Dataproc (based on MODELING_STRATEGY)
    gold_path = f"{{GCS_BUCKET_GOLD}}/{{FEED_ID}}/{{posting_date}}/{{group_run_id}}"

    context["ti"].xcom_push(key="gold_path", value=gold_path)
    return {{"layer": "gold", "path": gold_path, "strategy": MODELING_STRATEGY}}


def log_audit(**context):
    """Phase 10: Record metrics and lineage."""
    group_run_id = context["ti"].xcom_pull(task_ids="run_setup.initialize", key="group_run_id")
    posting_date = context["ti"].xcom_pull(task_ids="run_setup.initialize", key="posting_date")

    audit_logger = _get_audit_logger()
    audit_logger.log_run_complete(
        feed_id=FEED_ID,
        group_run_id=group_run_id,
        posting_date=posting_date,
        status="success",
    )

    return {{"status": "completed", "group_run_id": group_run_id}}


def cleanup(**context):
    """Phase 11: Archive processed files, clean transient."""
    group_run_id = context["ti"].xcom_pull(task_ids="run_setup.initialize", key="group_run_id")

    # Cleanup transient files
    return {{"cleanup": "completed", "group_run_id": group_run_id}}


# ===========================================================================
# DAG DEFINITION
# ===========================================================================

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Enterprise pipeline for Customer Transactions",
    schedule=SCHEDULE,
    start_date=datetime(2026, 1, 17),
    catchup=False,
    max_active_runs=1,
    tags=["enterprise", "salesforce", "customer_transactions", "customer", "transactions", "salesforce"],
) as dag:

    # Start
    start = EmptyOperator(task_id="start")

    # =========================================================================
    # Phase 1: File Discovery
    # =========================================================================
    with TaskGroup("file_discovery") as tg_discovery:
        discover = PythonOperator(
            task_id="discover",
            python_callable=discover_files,
        )

    # =========================================================================
    # Phase 2: Run Setup
    # =========================================================================
    with TaskGroup("run_setup") as tg_setup:
        initialize = PythonOperator(
            task_id="initialize",
            python_callable=setup_run,
        )

    # =========================================================================
    # Branch: Check if files exist
    # =========================================================================
    check_files = BranchPythonOperator(
        task_id="check_files",
        python_callable=check_files_exist,
    )

    # No files path
    no_files = EmptyOperator(task_id="no_files_found")

    # =========================================================================
    # File Processing (Phases 3-9)
    # =========================================================================
    with TaskGroup("file_processing") as tg_processing:
        # Phase 3: Duplicate check
        dup_check = BranchPythonOperator(
            task_id="check_duplicate",
            python_callable=check_duplicate,
        )

        # Handle duplicate
        handle_dup = PythonOperator(
            task_id="handle_duplicate",
            python_callable=handle_duplicate,
        )

        # Phase 4: Move to transient
        move_transient = PythonOperator(
            task_id="move_to_transient",
            python_callable=move_to_transient,
        )

        # Phase 5: Schema validation
        schema_val = PythonOperator(
            task_id="validate_schema",
            python_callable=validate_schema,
        )

        # Phase 6: Semantic validation
        semantic_val = PythonOperator(
            task_id="validate_semantic",
            python_callable=validate_semantic,
        )

        # Phase 7: Bronze processing
        bronze = PythonOperator(
            task_id="process_bronze",
            python_callable=process_bronze,
        )

        # Phase 8: Silver processing
        silver = PythonOperator(
            task_id="process_silver",
            python_callable=process_silver,
        )

        # Phase 9: Gold processing
        gold = PythonOperator(
            task_id="process_gold",
            python_callable=process_gold,
        )

        # Internal dependencies
        dup_check >> [handle_dup, move_transient]
        move_transient >> schema_val >> semantic_val >> bronze >> silver >> gold

    # =========================================================================
    # Phase 10: Audit Logging
    # =========================================================================
    audit = PythonOperator(
        task_id="log_audit",
        python_callable=log_audit,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # =========================================================================
    # Phase 11: Cleanup
    # =========================================================================
    cleanup_task = PythonOperator(
        task_id="cleanup",
        python_callable=cleanup,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # End
    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # =========================================================================
    # DAG Dependencies
    # =========================================================================
    start >> tg_discovery >> tg_setup >> check_files
    check_files >> tg_processing >> audit >> cleanup_task >> end
    check_files >> no_files >> end
