"""
APEX Common Pipeline Task Functions.

Shared across ALL 9 pipeline patterns. Each function is designed to be
used as a PythonOperator callable via op_kwargs binding.

Usage in Jinja2 templates:
    from dag_utilities.pipeline import pipeline_tasks

    init_exec = PythonOperator(
        task_id="initialize_execution",
        python_callable=pipeline_tasks.initialize_execution,
        op_kwargs={
            "metadata": metadata,
            "feed_id": FEED_ID,
            "contract_id": CONTRACT_ID,
            "execution_mode": "FILE_MEDALLION",
        },
    )
"""

import os
from pathlib import Path

from dag_utilities.logging import AuditLogger, MetricsCollector, LineageTracker
from dag_utilities.remediation import SelfHealer
from dag_utilities.spark import SparkJobSubmitter
from dag_utilities.storage import FileOperations
from dag_utilities.validation import QualityChecker

# Spark jobs base path — resolved from env var or default
SPARK_JOBS_PATH = os.getenv("APEX_SPARK_JOBS_PATH", "/opt/airflow/spark_jobs")


def _resolve_job_path(job_path: str) -> str:
    """
    Resolve a spark job path to an absolute path.

    Templates pass relative paths like "spark_jobs/raw_to_bronze.py".
    This resolves to the absolute path using APEX_SPARK_JOBS_PATH env var.

    Args:
        job_path: Relative or absolute path to spark job script

    Returns:
        Absolute path string
    """
    if os.path.isabs(job_path):
        return job_path

    # Strip "spark_jobs/" prefix if present
    basename = job_path
    if basename.startswith("spark_jobs/"):
        basename = basename[len("spark_jobs/"):]

    return str(Path(SPARK_JOBS_PATH) / basename)


# ═══════════════════════════════════════════════════════════════════════════════
# INITIALIZATION & FINALIZATION (All patterns)
# ═══════════════════════════════════════════════════════════════════════════════


def initialize_execution(metadata, feed_id, contract_id, execution_mode,
                         event_type=None, extra_event_data=None, **context):
    """
    Initialize pipeline execution with metadata.

    Creates an execution record, pushes execution_id to XCom,
    and logs the pipeline start event.

    Args:
        metadata: MetadataClient instance
        feed_id: Feed identifier
        contract_id: Contract identifier
        execution_mode: Execution mode string (e.g. "FILE_MEDALLION", "SCD2")
        event_type: Override audit event type (default: "{execution_mode}_PIPELINE_START")
        extra_event_data: Additional data to include in audit event
    """
    execution_id = metadata.create_execution(
        feed_id=feed_id,
        contract_id=contract_id,
        execution_date=str(context["ds"]),
        triggered_by="airflow_scheduler",
        execution_mode=execution_mode,
    )
    context["ti"].xcom_push(key="execution_id", value=execution_id)

    event_data = {"feed_id": feed_id}
    if extra_event_data:
        event_data.update(extra_event_data)

    AuditLogger().log_event(
        event_type=event_type or f"{execution_mode}_PIPELINE_START",
        execution_id=execution_id,
        event_data=event_data,
    )
    return execution_id


def finalize_execution(metadata, metric_keys=None, lineage_fn=None,
                       extra_metrics=None, **context):
    """
    Finalize pipeline execution.

    Collects metrics from XCom, updates execution status, and tracks lineage.

    Args:
        metadata: MetadataClient instance
        metric_keys: List of XCom keys to collect as metrics
        lineage_fn: Callable(execution_id, **context) to track lineage
        extra_metrics: Static dict to merge into metrics
    """
    execution_id = context["ti"].xcom_pull(key="execution_id")

    metrics = {}
    for key in (metric_keys or []):
        value = context["ti"].xcom_pull(key=key)
        if value is not None:
            metrics[key] = value

    if extra_metrics:
        metrics.update(extra_metrics)

    metadata.update_execution_status(
        execution_id=execution_id,
        status="COMPLETED",
        metrics=metrics,
    )

    if lineage_fn:
        lineage_fn(execution_id=execution_id, context=context)


def handle_validation_failure(**context):
    """Handle validation failures with self-healing."""
    SelfHealer().on_task_failure(context)


# ═══════════════════════════════════════════════════════════════════════════════
# ZONE TRANSITION JOBS (Most patterns)
# ═══════════════════════════════════════════════════════════════════════════════


def submit_zone_job(config, job_name, job_path, feed_id, contract_id,
                    extra_args=None, spark_config=None,
                    metrics_xcom_key=None, **context):
    """
    Submit a Spark zone transition job.

    Generic wrapper for raw→bronze, bronze→silver, silver→gold transitions.

    Args:
        config: APEXConfig instance
        job_name: Spark job name for logging
        job_path: Path to Spark job script
        feed_id: Feed identifier
        contract_id: Contract identifier
        extra_args: Additional arguments dict to merge
        spark_config: Optional SparkConfig override
        metrics_xcom_key: XCom key to store result metrics (if provided)

    Returns:
        Result metrics dict

    Raises:
        Exception: If Spark job fails
    """
    execution_id = context["ti"].xcom_pull(key="execution_id")

    arguments = {
        "--feed-id": feed_id,
        "--contract-id": contract_id,
        "--execution-id": execution_id,
        "--execution-date": context["ds"],
        "--metadata-db": config.metadata_db_url,
    }
    if extra_args:
        arguments.update(extra_args)

    resolved_path = _resolve_job_path(job_path)

    submitter = SparkJobSubmitter(config)
    submit_kwargs = {
        "job_name": job_name,
        "job_path": resolved_path,
        "arguments": arguments,
    }
    if spark_config:
        submit_kwargs["spark_config"] = spark_config

    result = submitter.submit_job(**submit_kwargs)

    if not result.success:
        raise Exception(f"{job_name} failed: {result.error}")

    if metrics_xcom_key:
        context["ti"].xcom_push(key=metrics_xcom_key, value=result.metrics)

    return result.metrics


# ═══════════════════════════════════════════════════════════════════════════════
# VALIDATION BRANCHING (Most patterns)
# ═══════════════════════════════════════════════════════════════════════════════


def run_schema_validation(config, feed_id, contract_id,
                          pass_task="bronze_to_silver",
                          fail_task="handle_validation_failure",
                          skip_task=None, skip_check_xcom_key=None,
                          skip_check_field=None, **context):
    """
    Execute Bronze schema validation as a BranchPythonOperator callable.

    Args:
        config: APEXConfig instance
        feed_id: Feed identifier
        contract_id: Contract identifier
        pass_task: Task ID to branch to on success
        fail_task: Task ID to branch to on failure
        skip_task: Task ID to branch to when skipping (e.g. zero records)
        skip_check_xcom_key: XCom key to check for skip condition
        skip_check_field: Field in XCom value to check (e.g. "records_written")

    Returns:
        Task ID string for branching
    """
    execution_id = context["ti"].xcom_pull(key="execution_id")

    # Optional skip check (e.g. streaming/API with zero records)
    if skip_task and skip_check_xcom_key:
        check_value = context["ti"].xcom_pull(key=skip_check_xcom_key) or {}
        if isinstance(check_value, dict):
            if check_value.get(skip_check_field, 1) == 0:
                return skip_task
        elif check_value == 0:
            return skip_task

    submitter = SparkJobSubmitter(config)
    result = submitter.submit_job(
        job_name="bronze_schema_validation",
        job_path=_resolve_job_path("spark_jobs/bronze_schema_validation.py"),
        arguments={
            "--feed-id": feed_id,
            "--contract-id": contract_id,
            "--execution-id": execution_id,
            "--metadata-db": config.metadata_db_url,
        },
    )

    if not result.success or not result.metrics.get("is_valid", False):
        return fail_task

    return pass_task


def run_semantic_validation(config, feed_id, contract_id,
                            pass_task="silver_to_gold",
                            fail_task="handle_validation_failure",
                            validation_type=None, **context):
    """
    Execute Silver semantic validation as a BranchPythonOperator callable.

    Args:
        config: APEXConfig instance
        feed_id: Feed identifier
        contract_id: Contract identifier
        pass_task: Task ID to branch to on success
        fail_task: Task ID to branch to on failure
        validation_type: Optional validation type override (e.g. "SCD2", "DATA_VAULT")

    Returns:
        Task ID string for branching
    """
    execution_id = context["ti"].xcom_pull(key="execution_id")

    arguments = {
        "--feed-id": feed_id,
        "--contract-id": contract_id,
        "--execution-id": execution_id,
        "--metadata-db": config.metadata_db_url,
    }
    if validation_type:
        arguments["--validation-type"] = validation_type

    submitter = SparkJobSubmitter(config)
    result = submitter.submit_job(
        job_name="silver_semantic_validation",
        job_path=_resolve_job_path("spark_jobs/silver_semantic_validation.py"),
        arguments=arguments,
    )

    if not result.success or not result.metrics.get("is_valid", False):
        return fail_task

    return pass_task


# ═══════════════════════════════════════════════════════════════════════════════
# WATERMARK MANAGEMENT (P03, P06)
# ═══════════════════════════════════════════════════════════════════════════════


def get_watermark(metadata, feed_id, contract_id, load_type,
                  event_type="WATERMARK_RETRIEVED", **context):
    """
    Get high watermark from previous execution.

    Args:
        metadata: MetadataClient instance
        feed_id: Feed identifier
        contract_id: Contract identifier
        load_type: Load type (FULL returns None)
        event_type: Audit event type
    """
    execution_id = context["ti"].xcom_pull(key="execution_id")

    if load_type == "FULL":
        context["ti"].xcom_push(key="low_watermark", value=None)
        return None

    last_execution = metadata.get_last_successful_execution(
        feed_id=feed_id,
        contract_id=contract_id,
    )

    low_watermark = None
    if last_execution:
        low_watermark = last_execution.get("high_watermark")

    context["ti"].xcom_push(key="low_watermark", value=low_watermark)

    AuditLogger().log_event(
        event_type=event_type,
        execution_id=execution_id,
        event_data={"low_watermark": str(low_watermark)},
    )
    return low_watermark


def update_watermark(metadata, **context):
    """
    Update high watermark in metadata.

    Reads high_watermark from XCom and persists it.
    """
    execution_id = context["ti"].xcom_pull(key="execution_id")
    high_watermark = context["ti"].xcom_pull(key="high_watermark")

    if high_watermark:
        metadata.update_execution_watermark(
            execution_id=execution_id,
            high_watermark=high_watermark,
        )


# ═══════════════════════════════════════════════════════════════════════════════
# SOURCE FILE CHECKS (P01, P07)
# ═══════════════════════════════════════════════════════════════════════════════


def check_source_file(metadata, feed_id, **context):
    """
    Verify source file exists and push path to XCom.

    Args:
        metadata: MetadataClient instance
        feed_id: Feed identifier

    Returns:
        source_path string

    Raises:
        FileNotFoundError: If source file doesn't exist
    """
    feed_config = metadata.get_feed_config(feed_id)
    file_ops = FileOperations()
    source_path = feed_config["source_path"].replace(
        "{{ ds }}", context["ds"]
    )

    file_info = file_ops.get_file_info(source_path)
    if not file_info.exists:
        raise FileNotFoundError(f"Source file not found: {source_path}")

    context["ti"].xcom_push(key="source_path", value=source_path)
    context["ti"].xcom_push(key="file_size", value=file_info.size)
    return source_path


# ═══════════════════════════════════════════════════════════════════════════════
# QUALITY CHECKS (P01, P09)
# ═══════════════════════════════════════════════════════════════════════════════


def run_quality_checks(metadata, contract_id, check_type="STANDARD",
                       pass_task="finalize_execution",
                       fail_task="handle_validation_failure",
                       custom_checks=None, **context):
    """
    Run data quality checks as a BranchPythonOperator callable.

    Args:
        metadata: MetadataClient instance
        contract_id: Contract identifier
        check_type: Quality check type (STANDARD, STAR_SCHEMA, etc.)
        pass_task: Task ID on success
        fail_task: Task ID on failure
        custom_checks: Dict of check_name → results_key for pattern-specific checks

    Returns:
        Task ID string for branching
    """
    execution_id = context["ti"].xcom_pull(key="execution_id")

    checker = QualityChecker(metadata)
    results = checker.run_all_checks(
        contract_id=contract_id,
        execution_id=execution_id,
        check_type=check_type,
    )

    context["ti"].xcom_push(key="quality_results", value=results)

    if custom_checks:
        checks_passed = all(results.get(key, True) for key in custom_checks)
    else:
        checks_passed = results.get("all_passed", True)

    if not checks_passed:
        return fail_task
    return pass_task


# ═══════════════════════════════════════════════════════════════════════════════
# SKIP PROCESSING (P05, P06)
# ═══════════════════════════════════════════════════════════════════════════════


def skip_processing(reason="No records to process", event_type="EMPTY_BATCH_SKIPPED",
                    **context):
    """
    Log skip event when there are no records to process.

    Args:
        reason: Human-readable reason for skipping
        event_type: Audit event type
    """
    execution_id = context["ti"].xcom_pull(key="execution_id")

    AuditLogger().log_event(
        event_type=event_type,
        execution_id=execution_id,
        event_data={"reason": reason},
    )


# ═══════════════════════════════════════════════════════════════════════════════
# NOTIFICATIONS (P01)
# ═══════════════════════════════════════════════════════════════════════════════


def run_ge_validation(config, feed_id, contract_id,
                      zone_level="BRONZE", validation_type="SCHEMA",
                      pass_task="next_task", fail_task="handle_validation_failure",
                      skip_task=None, skip_check_xcom_key=None,
                      skip_check_field=None, **context):
    """
    Execute GE-backed validation as a BranchPythonOperator callable.

    Submits the appropriate Spark validation job (bronze_schema_validation
    or silver_semantic_validation) with GE persistence args.

    Args:
        config: APEXConfig instance
        feed_id: Feed identifier
        contract_id: Contract identifier
        zone_level: BRONZE or SILVER
        validation_type: SCHEMA, SEMANTIC, QUALITY
        pass_task: Task ID to branch to on success
        fail_task: Task ID to branch to on failure
        skip_task: Task ID when skipping (zero records)
        skip_check_xcom_key: XCom key to check for skip condition
        skip_check_field: Field in XCom value to check

    Returns:
        Task ID string for branching
    """
    execution_id = context["ti"].xcom_pull(key="execution_id")

    # Optional skip check
    if skip_task and skip_check_xcom_key:
        check_value = context["ti"].xcom_pull(key=skip_check_xcom_key) or {}
        if isinstance(check_value, dict):
            if check_value.get(skip_check_field, 1) == 0:
                return skip_task
        elif check_value == 0:
            return skip_task

    # Select validation job based on zone
    if zone_level.upper() == "SILVER":
        job_script = "spark_jobs/silver_semantic_validation.py"
        job_name = "silver_semantic_validation"
    else:
        job_script = "spark_jobs/bronze_schema_validation.py"
        job_name = "bronze_schema_validation"

    arguments = {
        "--feed-id": feed_id,
        "--contract-id": contract_id,
        "--execution-id": execution_id,
        "--metadata-db": config.metadata_db_url,
        "--batch-identifier": f"{feed_id}_{context['ds_nodash']}",
    }

    # Add pg connection string if available
    pg_conn = getattr(config, "pg_connection_string", None)
    if pg_conn:
        arguments["--pg-connection-string"] = pg_conn

    submitter = SparkJobSubmitter(config)
    result = submitter.submit_job(
        job_name=job_name,
        job_path=_resolve_job_path(job_script),
        arguments=arguments,
    )

    if not result.success:
        return fail_task

    # Push validation score to XCom for downstream use
    quality_score = result.metrics.get("quality_score")
    if quality_score is not None:
        context["ti"].xcom_push(key=f"{zone_level.lower()}_quality_score", value=quality_score)

    return pass_task


def check_duplicate_files(metadata, feed_group_id, **context):
    """
    Check for duplicate files already processed.

    Returns task ID for branching - skips to finalize if all duplicates.
    """
    execution_id = context["ti"].xcom_pull(key="execution_id")
    source_files = context["ti"].xcom_pull(key="source_files") or []

    if not source_files:
        return "finalize"

    processed = metadata.get_processed_files(feed_group_id=feed_group_id)
    new_files = [f for f in source_files if f not in processed]

    context["ti"].xcom_push(key="new_files", value=new_files)

    if not new_files:
        AuditLogger().log_event(
            event_type="DUPLICATE_FILES_SKIPPED",
            execution_id=execution_id,
            event_data={"skipped_count": len(source_files)},
        )
        return "finalize"

    return "finalize"


def check_remaining_files(source_config, retrigger_task="retrigger_dag",
                          done_task="finalize", **context):
    """
    Check if there are remaining files to process after current batch.

    Returns retrigger_task if more files exist, done_task otherwise.
    """
    from dag_utilities.storage import FileOperations

    file_ops = FileOperations()
    remaining = file_ops.list_files(
        bucket=source_config.get("bucket", ""),
        prefix=source_config.get("prefix", ""),
    )

    processed_count = context["ti"].xcom_pull(key="source_file_count") or 0

    if len(remaining) > processed_count:
        return retrigger_task
    return done_task


def audit_execution(metadata, feed_id, **context):
    """Log audit record for feed execution."""
    execution_id = context["ti"].xcom_pull(key="execution_id")

    AuditLogger().log_event(
        event_type="FEED_EXECUTION_COMPLETE",
        execution_id=execution_id,
        event_data={"feed_id": feed_id},
    )


def send_notifications(pipeline_name=None, feed_id=None, feed_group_id=None,
                       dag_id=None, emails=None, **context):
    """
    Send completion notifications.

    Args:
        pipeline_name: Human-readable pipeline name
        feed_id: Feed identifier
        feed_group_id: Feed group identifier
        dag_id: DAG identifier
        emails: List of email addresses
    """
    execution_id = context["ti"].xcom_pull(key="execution_id")

    if emails:
        from dag_utilities.notification import EmailNotifier
        EmailNotifier().send(
            to=emails,
            subject=f"Pipeline {dag_id or pipeline_name} completed",
            body=f"Execution {execution_id} completed successfully.",
        )

    try:
        from dag_utilities.notification import SlackNotifier
        SlackNotifier().send_success_alert(
            execution_id=execution_id,
            pipeline_name=pipeline_name or dag_id,
            feed_id=feed_id or feed_group_id,
        )
    except Exception:
        pass  # Slack not configured
