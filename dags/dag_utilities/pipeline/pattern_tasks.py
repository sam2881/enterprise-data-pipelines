"""
APEX Pattern-Specific Pipeline Task Functions.

Functions unique to specific pipeline patterns (P02-P09).
Each function is designed to be used as a PythonOperator callable
via op_kwargs binding.

Usage in Jinja2 templates:
    from dag_utilities.pipeline import pattern_tasks

    extract = PythonOperator(
        task_id="extract_from_database",
        python_callable=pattern_tasks.extract_from_database,
        op_kwargs={...},
    )
"""

from datetime import datetime, timedelta

from dag_utilities.logging import AuditLogger, LineageTracker
from dag_utilities.spark import SparkJobSubmitter, SparkConfigBuilder, ClusterManager
from dag_utilities.storage import FileOperations
from dag_utilities.validation import QualityChecker


# ═══════════════════════════════════════════════════════════════════════════════
# P02: BIG DATA FILE
# ═══════════════════════════════════════════════════════════════════════════════


def analyze_source_file(metadata, feed_id, partition_size_mb, file_size_threshold_gb,
                        **context):
    """
    Analyze source file for size-based processing decisions (P02).

    Args:
        metadata: MetadataClient instance
        feed_id: Feed identifier
        partition_size_mb: Target partition size in MB
        file_size_threshold_gb: Threshold for bigdata classification
    """
    execution_id = context["ti"].xcom_pull(key="execution_id")
    feed_config = metadata.get_feed_config(feed_id)

    file_ops = FileOperations()
    source_path = feed_config["source_path"].replace("{{ ds }}", context["ds"])

    file_info = file_ops.get_file_info(source_path)
    if not file_info.exists:
        raise FileNotFoundError(f"Source file not found: {source_path}")

    file_size_gb = file_info.size / (1024 ** 3)
    num_partitions = max(1, int(file_size_gb * 1024 / partition_size_mb))

    context["ti"].xcom_push(key="source_path", value=source_path)
    context["ti"].xcom_push(key="file_size_gb", value=file_size_gb)
    context["ti"].xcom_push(key="num_partitions", value=num_partitions)

    AuditLogger().log_event(
        event_type="FILE_ANALYSIS",
        execution_id=execution_id,
        event_data={
            "file_size_gb": file_size_gb,
            "num_partitions": num_partitions,
            "is_bigdata": file_size_gb >= file_size_threshold_gb,
        },
    )
    return {"size_gb": file_size_gb, "partitions": num_partitions}


def scale_cluster(config, num_executors, executor_memory, executor_cores, **context):
    """
    Scale Spark cluster based on file size (P02).

    Args:
        config: APEXConfig instance
        num_executors: Base number of executors
        executor_memory: Executor memory string (e.g. "8g")
        executor_cores: Executor core count
    """
    file_size_gb = context["ti"].xcom_pull(key="file_size_gb")
    optimal_executors = max(num_executors, int(file_size_gb / 2))

    cluster_manager = ClusterManager(config)
    cluster_config = cluster_manager.scale_cluster(
        num_executors=optimal_executors,
        executor_memory=executor_memory,
        executor_cores=executor_cores,
    )

    context["ti"].xcom_push(key="cluster_config", value=cluster_config)
    return cluster_config


def scale_down_cluster(config, **context):
    """Scale down Spark cluster after processing (P02)."""
    cluster_manager = ClusterManager(config)
    cluster_manager.scale_down()


# ═══════════════════════════════════════════════════════════════════════════════
# P03: DATABASE LAKEHOUSE
# ═══════════════════════════════════════════════════════════════════════════════


def extract_from_database(config, feed_id, contract_id, source_connection,
                          load_type, watermark_column="updated_at", **context):
    """
    Extract data from source database via JDBC (P03).

    Args:
        config: APEXConfig instance
        feed_id: Feed identifier
        contract_id: Contract identifier
        source_connection: Airflow connection ID for source database
        load_type: FULL, INCREMENTAL, or CDC
        watermark_column: Column for incremental extraction
    """
    execution_id = context["ti"].xcom_pull(key="execution_id")
    low_watermark = context["ti"].xcom_pull(key="low_watermark")

    arguments = {
        "--feed-id": feed_id,
        "--contract-id": contract_id,
        "--execution-id": execution_id,
        "--execution-date": context["ds"],
        "--metadata-db": config.metadata_db_url,
        "--connection-id": source_connection,
        "--load-type": load_type,
    }

    if low_watermark and load_type in ("INCREMENTAL", "CDC"):
        arguments["--low-watermark"] = str(low_watermark)
        arguments["--watermark-column"] = watermark_column

    submitter = SparkJobSubmitter(config)
    result = submitter.submit_job(
        job_name="database_extract",
        job_path="spark_jobs/raw_to_bronze.py",
        arguments=arguments,
    )

    if not result.success:
        raise Exception(f"Database extraction failed: {result.error}")

    high_watermark = result.metrics.get("high_watermark")
    context["ti"].xcom_push(key="high_watermark", value=high_watermark)
    context["ti"].xcom_push(key="extract_metrics", value=result.metrics)
    return result.metrics


def apply_delta_merge(config, feed_id, contract_id, primary_keys, load_type,
                      **context):
    """
    Apply Delta Lake merge for upserts (P03).

    Args:
        config: APEXConfig instance
        feed_id: Feed identifier
        contract_id: Contract identifier
        primary_keys: List of primary key column names
        load_type: FULL, INCREMENTAL, or CDC
    """
    execution_id = context["ti"].xcom_pull(key="execution_id")

    if load_type not in ("INCREMENTAL", "CDC"):
        return {"merge_type": "OVERWRITE"}

    submitter = SparkJobSubmitter(config)
    result = submitter.submit_job(
        job_name="delta_merge",
        job_path="spark_jobs/v2/build_gold_layer.py",
        arguments={
            "--feed-id": feed_id,
            "--contract-id": contract_id,
            "--execution-id": execution_id,
            "--execution-date": context["ds"],
            "--metadata-db": config.metadata_db_url,
            "--merge-keys": ",".join(primary_keys),
            "--operation": "MERGE",
        },
    )

    if not result.success:
        raise Exception(f"Delta merge failed: {result.error}")

    context["ti"].xcom_push(key="gold_metrics", value=result.metrics)
    return result.metrics


# ═══════════════════════════════════════════════════════════════════════════════
# P04: LEGACY MIGRATION
# ═══════════════════════════════════════════════════════════════════════════════


def validate_legacy_source(metadata, feed_id, legacy_source_type, copybook_path="",
                           **context):
    """
    Validate legacy source configuration (P04).

    Args:
        metadata: MetadataClient instance
        feed_id: Feed identifier
        legacy_source_type: DTSX, COBOL, AS400, or EBCDIC
        copybook_path: Path to COBOL copybook file
    """
    feed_config = metadata.get_feed_config(feed_id)

    file_ops = FileOperations()
    source_path = feed_config["source_path"].replace("{{ ds }}", context["ds"])

    file_info = file_ops.get_file_info(source_path)
    if not file_info.exists:
        raise FileNotFoundError(f"Legacy source not found: {source_path}")

    if legacy_source_type in ("COBOL", "EBCDIC") and copybook_path:
        copybook_info = file_ops.get_file_info(copybook_path)
        if not copybook_info.exists:
            raise FileNotFoundError(f"Copybook not found: {copybook_path}")

    context["ti"].xcom_push(key="source_path", value=source_path)
    context["ti"].xcom_push(key="file_size", value=file_info.size)
    return {"source_path": source_path, "size": file_info.size}


def parse_legacy_schema(metadata, feed_id, contract_id, legacy_source_type,
                        copybook_path="", **context):
    """
    Parse schema from legacy source (P04).

    Args:
        metadata: MetadataClient instance
        feed_id: Feed identifier
        contract_id: Contract identifier
        legacy_source_type: DTSX, COBOL, AS400, or EBCDIC
        copybook_path: Path to COBOL copybook file
    """
    execution_id = context["ti"].xcom_pull(key="execution_id")

    if legacy_source_type == "COBOL":
        from dag_utilities.parsers.copybook_parser import CopybookParser
        parser = CopybookParser()
        schema = parser.parse(copybook_path)
    elif legacy_source_type == "DTSX":
        from dag_utilities.parsers.dtsx_parser import DTSXParser
        parser = DTSXParser()
        schema = parser.extract_schema(context["ti"].xcom_pull(key="source_path"))
    else:
        contract = metadata.get_contract(contract_id)
        schema = contract.get("schema_definition", {})

    context["ti"].xcom_push(key="parsed_schema", value=schema)

    AuditLogger().log_event(
        event_type="LEGACY_SCHEMA_PARSED",
        execution_id=execution_id,
        event_data={
            "legacy_type": legacy_source_type,
            "column_count": len(schema.get("columns", [])),
        },
    )
    return schema


def legacy_to_bronze(config, feed_id, contract_id, legacy_source_type,
                     copybook_path="", encoding="EBCDIC", record_length=0,
                     **context):
    """
    Convert legacy format to Bronze with Cobrix for EBCDIC (P04).

    Args:
        config: APEXConfig instance
        feed_id: Feed identifier
        contract_id: Contract identifier
        legacy_source_type: DTSX, COBOL, AS400, or EBCDIC
        copybook_path: Path to COBOL copybook file
        encoding: File encoding
        record_length: Fixed record length (0 for variable)
    """
    execution_id = context["ti"].xcom_pull(key="execution_id")

    spark_config_builder = SparkConfigBuilder()
    if legacy_source_type in ("COBOL", "EBCDIC"):
        spark_config_builder.with_package("za.co.absa:cobrix_2.12:2.6.3")
    platform_spark_config = spark_config_builder.build()

    arguments = {
        "--feed-id": feed_id,
        "--contract-id": contract_id,
        "--execution-id": execution_id,
        "--execution-date": context["ds"],
        "--metadata-db": config.metadata_db_url,
        "--source-type": legacy_source_type,
    }

    if legacy_source_type in ("COBOL", "EBCDIC"):
        arguments["--copybook-path"] = copybook_path
        arguments["--encoding"] = encoding
        if record_length > 0:
            arguments["--record-length"] = str(record_length)

    submitter = SparkJobSubmitter(config)
    result = submitter.submit_job(
        job_name="legacy_to_bronze",
        job_path="spark_jobs/raw_to_bronze.py",
        arguments=arguments,
        platform_spark_config=platform_spark_config,
    )

    if not result.success:
        raise Exception(f"Legacy to Bronze failed: {result.error}")

    context["ti"].xcom_push(key="bronze_metrics", value=result.metrics)
    return result.metrics


def generate_migration_report(metadata, legacy_source_type, **context):
    """
    Generate legacy migration report (P04).

    Args:
        metadata: MetadataClient instance
        legacy_source_type: Legacy source type string
    """
    execution_id = context["ti"].xcom_pull(key="execution_id")
    bronze_metrics = context["ti"].xcom_pull(key="bronze_metrics") or {}
    silver_metrics = context["ti"].xcom_pull(key="silver_metrics") or {}
    gold_metrics = context["ti"].xcom_pull(key="gold_metrics") or {}

    report = {
        "execution_id": execution_id,
        "legacy_type": legacy_source_type,
        "records_ingested": bronze_metrics.get("records_written", 0),
        "records_transformed": silver_metrics.get("records_written", 0),
        "records_loaded": gold_metrics.get("records_written", 0),
        "encoding_conversions": bronze_metrics.get("encoding_conversions", 0),
        "schema_mappings": bronze_metrics.get("schema_mappings", 0),
    }

    context["ti"].xcom_push(key="migration_report", value=report)

    AuditLogger().log_event(
        event_type="LEGACY_MIGRATION_REPORT",
        execution_id=execution_id,
        event_data=report,
    )
    return report


# ═══════════════════════════════════════════════════════════════════════════════
# P05: STREAMING BATCH
# ═══════════════════════════════════════════════════════════════════════════════


def get_checkpoint_offsets(checkpoint_location, **context):
    """
    Get last committed offsets from checkpoint (P05).

    Args:
        checkpoint_location: GCS/S3 path to checkpoint directory
    """
    file_ops = FileOperations()
    checkpoint_info = file_ops.get_file_info(f"{checkpoint_location}/offsets")

    if checkpoint_info.exists:
        offsets = file_ops.read_json(f"{checkpoint_location}/offsets/latest")
        context["ti"].xcom_push(key="start_offsets", value=offsets)
    else:
        context["ti"].xcom_push(key="start_offsets", value="earliest")

    return context["ti"].xcom_pull(key="start_offsets")


def consume_streaming_batch(config, feed_id, contract_id, streaming_source,
                            topic_name, consumer_group, checkpoint_location,
                            batch_interval_minutes=5, **context):
    """
    Consume micro-batch from streaming source (P05).

    Args:
        config: APEXConfig instance
        feed_id: Feed identifier
        contract_id: Contract identifier
        streaming_source: KAFKA, PUBSUB, KINESIS, or EVENTHUB
        topic_name: Topic/subscription name
        consumer_group: Consumer group ID
        checkpoint_location: Checkpoint path
        batch_interval_minutes: Batch window size
    """
    execution_id = context["ti"].xcom_pull(key="execution_id")
    batch_start = context["ti"].xcom_pull(key="batch_start")
    batch_end = context["ti"].xcom_pull(key="batch_end")
    start_offsets = context["ti"].xcom_pull(key="start_offsets")

    spark_config_builder = SparkConfigBuilder()
    if streaming_source == "KAFKA":
        spark_config_builder.with_package("org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    elif streaming_source == "PUBSUB":
        spark_config_builder.with_package("com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.0")
    platform_spark_config = spark_config_builder.build()

    arguments = {
        "--feed-id": feed_id,
        "--contract-id": contract_id,
        "--execution-id": execution_id,
        "--execution-date": context["ds"],
        "--metadata-db": config.metadata_db_url,
        "--streaming-source": streaming_source,
        "--topic": topic_name,
        "--consumer-group": consumer_group,
        "--batch-start": batch_start,
        "--batch-end": batch_end,
        "--checkpoint-location": checkpoint_location,
        "--processing-mode": "MICRO_BATCH",
    }

    if start_offsets != "earliest":
        arguments["--start-offsets"] = str(start_offsets)

    submitter = SparkJobSubmitter(config)
    result = submitter.submit_job(
        job_name="streaming_to_bronze",
        job_path="spark_jobs/raw_to_bronze.py",
        arguments=arguments,
        platform_spark_config=platform_spark_config,
    )

    if not result.success:
        raise Exception(f"Streaming consumption failed: {result.error}")

    context["ti"].xcom_push(key="end_offsets", value=result.metrics.get("end_offsets"))
    context["ti"].xcom_push(key="bronze_metrics", value=result.metrics)
    return result.metrics


def commit_offsets(checkpoint_location, **context):
    """
    Commit consumer offsets after successful processing (P05).

    Args:
        checkpoint_location: Checkpoint path
    """
    execution_id = context["ti"].xcom_pull(key="execution_id")
    end_offsets = context["ti"].xcom_pull(key="end_offsets")

    if end_offsets:
        file_ops = FileOperations()
        file_ops.write_json(
            path=f"{checkpoint_location}/offsets/latest",
            data=end_offsets,
        )

        AuditLogger().log_event(
            event_type="OFFSETS_COMMITTED",
            execution_id=execution_id,
            event_data={"offsets": end_offsets},
        )


def process_late_data(config, feed_id, contract_id, late_data_tolerance_hours,
                      **context):
    """
    Process late arriving data (P05).

    Args:
        config: APEXConfig instance
        feed_id: Feed identifier
        contract_id: Contract identifier
        late_data_tolerance_hours: Hours of late data tolerance
    """
    execution_id = context["ti"].xcom_pull(key="execution_id")

    late_data_cutoff = datetime.fromisoformat(
        context["ti"].xcom_pull(key="batch_start")
    ) - timedelta(hours=late_data_tolerance_hours)

    submitter = SparkJobSubmitter(config)
    result = submitter.submit_job(
        job_name="process_late_data",
        job_path="spark_jobs/v2/promote_bronze_to_silver.py",
        arguments={
            "--feed-id": feed_id,
            "--contract-id": contract_id,
            "--execution-id": execution_id,
            "--execution-date": context["ds"],
            "--metadata-db": config.metadata_db_url,
            "--late-data-cutoff": str(late_data_cutoff),
            "--processing-mode": "LATE_DATA",
        },
    )

    context["ti"].xcom_push(key="late_data_metrics", value=result.metrics)
    return result.metrics


# ═══════════════════════════════════════════════════════════════════════════════
# P06: API / SAAS
# ═══════════════════════════════════════════════════════════════════════════════


def extract_from_api(api_connection_id, api_endpoint, http_method, feed_id,
                     landing_bucket, pagination_type="OFFSET", page_size=1000,
                     rate_limit_calls=100, rate_limit_period=60,
                     incremental_field="modified_at", load_type="INCREMENTAL",
                     **context):
    """
    Extract data from API with pagination and rate limiting (P06).

    Args:
        api_connection_id: Airflow HTTP connection ID
        api_endpoint: API endpoint path
        http_method: HTTP method (GET/POST)
        feed_id: Feed identifier
        landing_bucket: GCS landing bucket
        pagination_type: OFFSET, CURSOR, or LINK
        page_size: Records per page
        rate_limit_calls: Max calls before rate limit pause
        rate_limit_period: Rate limit pause duration in seconds
        incremental_field: Field name for watermark tracking
        load_type: FULL or INCREMENTAL
    """
    import time
    import json
    from airflow.providers.http.hooks.http import HttpHook

    execution_id = context["ti"].xcom_pull(key="execution_id")
    low_watermark = context["ti"].xcom_pull(key="low_watermark")

    http_hook = HttpHook(http_conn_id=api_connection_id, method=http_method)
    file_ops = FileOperations()

    all_records = []
    page = 0
    has_more = True
    api_calls = 0
    high_watermark = low_watermark

    base_params = {}
    if low_watermark and load_type == "INCREMENTAL":
        base_params[f"{incremental_field}_gt"] = low_watermark

    while has_more:
        if api_calls >= rate_limit_calls:
            time.sleep(rate_limit_period)
            api_calls = 0

        if pagination_type == "OFFSET":
            params = {**base_params, "limit": page_size, "offset": page * page_size}
        elif pagination_type == "CURSOR":
            cursor = context["ti"].xcom_pull(key="next_cursor")
            params = {**base_params, "limit": page_size}
            if cursor:
                params["cursor"] = cursor
        else:
            params = {**base_params, "limit": page_size}

        response = http_hook.run(
            endpoint=api_endpoint,
            data=params if http_method == "GET" else json.dumps(params),
            headers={"Content-Type": "application/json"},
        )

        api_calls += 1
        data = response.json()

        if isinstance(data, list):
            records = data
        elif isinstance(data, dict):
            records = data.get("data", data.get("results", data.get("items", [])))

        all_records.extend(records)

        for record in records:
            record_watermark = record.get(incremental_field)
            if record_watermark and (not high_watermark or record_watermark > high_watermark):
                high_watermark = record_watermark

        if pagination_type == "CURSOR":
            next_cursor = data.get("next_cursor", data.get("cursor"))
            context["ti"].xcom_push(key="next_cursor", value=next_cursor)
            has_more = bool(next_cursor)
        elif pagination_type == "LINK":
            next_link = data.get("next", data.get("_links", {}).get("next"))
            has_more = bool(next_link)
        else:
            has_more = len(records) == page_size

        page += 1

        AuditLogger().log_event(
            event_type="API_PAGE_EXTRACTED",
            execution_id=execution_id,
            event_data={
                "page": page,
                "records_in_page": len(records),
                "total_records": len(all_records),
            },
        )

    landing_path = f"gs://{landing_bucket}/api/{feed_id}/{context['ds']}/data.json"
    file_ops.write_json(landing_path, all_records)

    context["ti"].xcom_push(key="landing_path", value=landing_path)
    context["ti"].xcom_push(key="record_count", value=len(all_records))
    context["ti"].xcom_push(key="high_watermark", value=high_watermark)

    return {"records": len(all_records), "high_watermark": high_watermark}


# ═══════════════════════════════════════════════════════════════════════════════
# P07: SCD TYPE 2
# ═══════════════════════════════════════════════════════════════════════════════


def detect_changes(config, feed_id, contract_id, business_keys, hash_col,
                   pass_task="apply_scd2", no_changes_task="no_changes",
                   **context):
    """
    Detect changes between Silver (new) and current dimension (P07).

    Args:
        config: APEXConfig instance
        feed_id: Feed identifier
        contract_id: Contract identifier
        business_keys: List of business key column names
        hash_col: Hash column name for comparison
        pass_task: Task to branch to when changes exist
        no_changes_task: Task to branch to when no changes

    Returns:
        Task ID string for branching
    """
    execution_id = context["ti"].xcom_pull(key="execution_id")

    submitter = SparkJobSubmitter(config)
    result = submitter.submit_job(
        job_name="scd2_change_detection",
        job_path="spark_jobs/v2/build_gold_layer.py",
        arguments={
            "--feed-id": feed_id,
            "--contract-id": contract_id,
            "--execution-id": execution_id,
            "--execution-date": context["ds"],
            "--metadata-db": config.metadata_db_url,
            "--operation": "DETECT_CHANGES",
            "--business-keys": ",".join(business_keys),
            "--hash-column": hash_col,
        },
    )

    if not result.success:
        raise Exception(f"Change detection failed: {result.error}")

    new_records = result.metrics.get("new_records", 0)
    changed_records = result.metrics.get("changed_records", 0)
    unchanged_records = result.metrics.get("unchanged_records", 0)

    context["ti"].xcom_push(key="change_detection", value={
        "new": new_records,
        "changed": changed_records,
        "unchanged": unchanged_records,
    })

    AuditLogger().log_event(
        event_type="SCD2_CHANGES_DETECTED",
        execution_id=execution_id,
        event_data={
            "new_records": new_records,
            "changed_records": changed_records,
            "unchanged_records": unchanged_records,
        },
    )

    if new_records == 0 and changed_records == 0:
        return no_changes_task
    return pass_task


def apply_scd2(config, feed_id, contract_id, business_keys, tracked_columns,
               surrogate_key_name, effective_date_col, end_date_col,
               current_flag_col, end_date_infinity, **context):
    """
    Apply SCD Type 2 logic to dimension table (P07).

    Args:
        config: APEXConfig instance
        feed_id: Feed identifier
        contract_id: Contract identifier
        business_keys: List of business key columns
        tracked_columns: List of tracked attribute columns
        surrogate_key_name: Name for surrogate key column
        effective_date_col: Effective date column name
        end_date_col: End date column name
        current_flag_col: Current record flag column name
        end_date_infinity: Far-future date string for current records
    """
    execution_id = context["ti"].xcom_pull(key="execution_id")

    submitter = SparkJobSubmitter(config)
    result = submitter.submit_job(
        job_name="scd2_apply",
        job_path="spark_jobs/v2/build_gold_layer.py",
        arguments={
            "--feed-id": feed_id,
            "--contract-id": contract_id,
            "--execution-id": execution_id,
            "--execution-date": context["ds"],
            "--metadata-db": config.metadata_db_url,
            "--operation": "APPLY_SCD2",
            "--business-keys": ",".join(business_keys),
            "--tracked-columns": ",".join(tracked_columns),
            "--surrogate-key": surrogate_key_name,
            "--effective-date-col": effective_date_col,
            "--end-date-col": end_date_col,
            "--current-flag-col": current_flag_col,
            "--end-date-infinity": end_date_infinity,
        },
    )

    if not result.success:
        raise Exception(f"SCD2 apply failed: {result.error}")

    scd2_metrics = {
        "new_versions_created": result.metrics.get("new_versions", 0),
        "records_expired": result.metrics.get("expired_records", 0),
        "total_dimension_records": result.metrics.get("total_records", 0),
        "current_records": result.metrics.get("current_records", 0),
    }

    context["ti"].xcom_push(key="scd2_metrics", value=scd2_metrics)

    AuditLogger().log_event(
        event_type="SCD2_APPLIED",
        execution_id=execution_id,
        event_data=scd2_metrics,
    )
    return scd2_metrics


def validate_dimension(config, feed_id, contract_id,
                       pass_task="finalize_execution",
                       fail_task="handle_validation_failure", **context):
    """
    Validate SCD2 dimension table integrity (P07).

    Returns:
        Task ID string for branching
    """
    execution_id = context["ti"].xcom_pull(key="execution_id")

    submitter = SparkJobSubmitter(config)
    result = submitter.submit_job(
        job_name="scd2_validation",
        job_path="spark_jobs/silver_semantic_validation.py",
        arguments={
            "--feed-id": feed_id,
            "--contract-id": contract_id,
            "--execution-id": execution_id,
            "--metadata-db": config.metadata_db_url,
            "--validation-type": "SCD2",
        },
    )

    validation_results = result.metrics
    checks_passed = all([
        validation_results.get("unique_surrogate_keys", False),
        validation_results.get("valid_date_ranges", False),
        validation_results.get("single_current_per_business_key", False),
    ])

    context["ti"].xcom_push(key="validation_results", value=validation_results)

    if not checks_passed:
        return fail_task
    return pass_task


def no_changes(metadata, event_type="SCD2_NO_CHANGES", **context):
    """
    Handle case when no changes detected (P07).

    Args:
        metadata: MetadataClient instance
        event_type: Audit event type
    """
    execution_id = context["ti"].xcom_pull(key="execution_id")

    AuditLogger().log_event(
        event_type=event_type,
        execution_id=execution_id,
        event_data={"reason": "No new or changed records detected"},
    )

    metadata.update_execution_status(
        execution_id=execution_id,
        status="COMPLETED",
        metrics={"changes_detected": False},
    )


# ═══════════════════════════════════════════════════════════════════════════════
# P08: DATA VAULT
# ═══════════════════════════════════════════════════════════════════════════════


def generate_hash_keys(config, feed_id, contract_id, hubs, links, satellites,
                       hash_algorithm="MD5", hash_concatenation="|", **context):
    """
    Generate hash keys for all Data Vault entities (P08).

    Args:
        config: APEXConfig instance
        feed_id: Feed identifier
        contract_id: Contract identifier
        hubs: List of hub definitions
        links: List of link definitions
        satellites: List of satellite definitions
        hash_algorithm: Hash algorithm (MD5, SHA256)
        hash_concatenation: Separator for hash input concatenation
    """
    execution_id = context["ti"].xcom_pull(key="execution_id")

    hash_definitions = []
    for hub in hubs:
        hash_definitions.append({
            "entity_type": "HUB",
            "entity_name": hub["name"],
            "business_keys": hub["business_keys"],
            "hash_key_name": f"{hub['name']}_hk",
        })
    for link in links:
        hash_definitions.append({
            "entity_type": "LINK",
            "entity_name": link["name"],
            "business_keys": link["business_keys"],
            "hash_key_name": f"{link['name']}_hk",
        })
    for sat in satellites:
        hash_definitions.append({
            "entity_type": "SATELLITE",
            "entity_name": sat["name"],
            "attributes": sat["attributes"],
            "hash_diff_name": f"{sat['name']}_hdiff",
        })

    submitter = SparkJobSubmitter(config)
    result = submitter.submit_job(
        job_name="generate_hash_keys",
        job_path="spark_jobs/v2/promote_bronze_to_silver.py",
        arguments={
            "--feed-id": feed_id,
            "--contract-id": contract_id,
            "--execution-id": execution_id,
            "--execution-date": context["ds"],
            "--metadata-db": config.metadata_db_url,
            "--operation": "GENERATE_HASH_KEYS",
            "--hash-definitions": str(hash_definitions),
            "--hash-algorithm": hash_algorithm,
            "--hash-concatenation": hash_concatenation,
        },
    )

    if not result.success:
        raise Exception(f"Hash key generation failed: {result.error}")

    context["ti"].xcom_push(key="hash_metrics", value=result.metrics)
    return result.metrics


def load_hubs(config, feed_id, contract_id, hubs, record_source, **context):
    """Load Hub entities (P08)."""
    execution_id = context["ti"].xcom_pull(key="execution_id")
    hub_metrics = {}
    submitter = SparkJobSubmitter(config)

    for hub in hubs:
        result = submitter.submit_job(
            job_name=f"load_{hub['name']}",
            job_path="spark_jobs/v2/build_gold_layer.py",
            arguments={
                "--feed-id": feed_id,
                "--contract-id": contract_id,
                "--execution-id": execution_id,
                "--execution-date": context["ds"],
                "--metadata-db": config.metadata_db_url,
                "--operation": "LOAD_HUB",
                "--entity-name": hub["name"],
                "--business-keys": ",".join(hub["business_keys"]),
                "--hash-key": f"{hub['name']}_hk",
                "--record-source": record_source,
            },
        )
        if not result.success:
            raise Exception(f"Hub {hub['name']} load failed: {result.error}")
        hub_metrics[hub["name"]] = result.metrics

    context["ti"].xcom_push(key="hub_metrics", value=hub_metrics)

    AuditLogger().log_event(
        event_type="DATA_VAULT_HUBS_LOADED",
        execution_id=execution_id,
        event_data=hub_metrics,
    )
    return hub_metrics


def load_links(config, feed_id, contract_id, links, record_source, **context):
    """Load Link entities (P08)."""
    execution_id = context["ti"].xcom_pull(key="execution_id")
    link_metrics = {}
    submitter = SparkJobSubmitter(config)

    for link in links:
        result = submitter.submit_job(
            job_name=f"load_{link['name']}",
            job_path="spark_jobs/v2/build_gold_layer.py",
            arguments={
                "--feed-id": feed_id,
                "--contract-id": contract_id,
                "--execution-id": execution_id,
                "--execution-date": context["ds"],
                "--metadata-db": config.metadata_db_url,
                "--operation": "LOAD_LINK",
                "--entity-name": link["name"],
                "--business-keys": ",".join(link["business_keys"]),
                "--hub-refs": ",".join(link["hub_refs"]),
                "--hash-key": f"{link['name']}_hk",
                "--record-source": record_source,
            },
        )
        if not result.success:
            raise Exception(f"Link {link['name']} load failed: {result.error}")
        link_metrics[link["name"]] = result.metrics

    context["ti"].xcom_push(key="link_metrics", value=link_metrics)

    AuditLogger().log_event(
        event_type="DATA_VAULT_LINKS_LOADED",
        execution_id=execution_id,
        event_data=link_metrics,
    )
    return link_metrics


def load_satellites(config, feed_id, contract_id, satellites, record_source,
                    **context):
    """Load Satellite entities with point-in-time loading (P08)."""
    execution_id = context["ti"].xcom_pull(key="execution_id")
    satellite_metrics = {}
    submitter = SparkJobSubmitter(config)

    for sat in satellites:
        result = submitter.submit_job(
            job_name=f"load_{sat['name']}",
            job_path="spark_jobs/v2/build_gold_layer.py",
            arguments={
                "--feed-id": feed_id,
                "--contract-id": contract_id,
                "--execution-id": execution_id,
                "--execution-date": context["ds"],
                "--metadata-db": config.metadata_db_url,
                "--operation": "LOAD_SATELLITE",
                "--entity-name": sat["name"],
                "--parent-hub": sat["hub"],
                "--attributes": ",".join(sat["attributes"]),
                "--hash-diff": f"{sat['name']}_hdiff",
                "--record-source": record_source,
            },
        )
        if not result.success:
            raise Exception(f"Satellite {sat['name']} load failed: {result.error}")
        satellite_metrics[sat["name"]] = result.metrics

    context["ti"].xcom_push(key="satellite_metrics", value=satellite_metrics)

    AuditLogger().log_event(
        event_type="DATA_VAULT_SATELLITES_LOADED",
        execution_id=execution_id,
        event_data=satellite_metrics,
    )
    return satellite_metrics


def validate_data_vault(config, feed_id, contract_id,
                        pass_task="finalize_execution",
                        fail_task="handle_validation_failure", **context):
    """
    Validate Data Vault integrity (P08).

    Returns:
        Task ID string for branching
    """
    execution_id = context["ti"].xcom_pull(key="execution_id")

    submitter = SparkJobSubmitter(config)
    result = submitter.submit_job(
        job_name="data_vault_validation",
        job_path="spark_jobs/silver_semantic_validation.py",
        arguments={
            "--feed-id": feed_id,
            "--contract-id": contract_id,
            "--execution-id": execution_id,
            "--metadata-db": config.metadata_db_url,
            "--validation-type": "DATA_VAULT",
        },
    )

    validation_results = {
        "hub_uniqueness": result.metrics.get("hub_uniqueness", False),
        "link_referential_integrity": result.metrics.get("link_ri", False),
        "satellite_hash_diff_uniqueness": result.metrics.get("sat_hdiff_unique", False),
        "record_source_present": result.metrics.get("record_source", False),
    }

    context["ti"].xcom_push(key="validation_results", value=validation_results)

    if not all(validation_results.values()):
        return fail_task
    return pass_task


# ═══════════════════════════════════════════════════════════════════════════════
# P09: STAR SCHEMA
# ═══════════════════════════════════════════════════════════════════════════════


def load_date_dimension(config, feed_id, contract_id, dimensions, **context):
    """
    Load date dimension if defined (P09).

    Args:
        config: APEXConfig instance
        feed_id: Feed identifier
        contract_id: Contract identifier
        dimensions: Full list of dimension definitions
    """
    execution_id = context["ti"].xcom_pull(key="execution_id")
    date_dims = [d for d in dimensions if d.get("type") == "DATE"]

    if not date_dims:
        return {"skipped": True}

    submitter = SparkJobSubmitter(config)
    result = submitter.submit_job(
        job_name="load_date_dimension",
        job_path="spark_jobs/v2/build_gold_layer.py",
        arguments={
            "--feed-id": feed_id,
            "--contract-id": contract_id,
            "--execution-id": execution_id,
            "--execution-date": context["ds"],
            "--metadata-db": config.metadata_db_url,
            "--operation": "LOAD_DATE_DIMENSION",
            "--dimension-name": date_dims[0]["name"],
            "--grain": date_dims[0].get("grain", "DAY"),
        },
    )

    context["ti"].xcom_push(key="date_dim_metrics", value=result.metrics)
    return result.metrics


def load_dimensions(config, feed_id, contract_id, dimensions, **context):
    """
    Load all non-date dimensions with SCD logic (P09).

    Args:
        config: APEXConfig instance
        feed_id: Feed identifier
        contract_id: Contract identifier
        dimensions: Full list of dimension definitions
    """
    execution_id = context["ti"].xcom_pull(key="execution_id")
    dimension_metrics = {}
    submitter = SparkJobSubmitter(config)

    regular_dimensions = [d for d in dimensions if d.get("type") != "DATE"]

    for dim in regular_dimensions:
        scd_type = dim.get("scd_type", 1)
        result = submitter.submit_job(
            job_name=f"load_{dim['name']}",
            job_path="spark_jobs/v2/build_gold_layer.py",
            arguments={
                "--feed-id": feed_id,
                "--contract-id": contract_id,
                "--execution-id": execution_id,
                "--execution-date": context["ds"],
                "--metadata-db": config.metadata_db_url,
                "--operation": f"LOAD_DIMENSION_SCD{scd_type}",
                "--dimension-name": dim["name"],
                "--business-key": dim["business_key"],
                "--attributes": ",".join(dim.get("attributes", [])),
            },
        )

        if not result.success:
            raise Exception(f"Dimension {dim['name']} load failed: {result.error}")

        dimension_metrics[dim["name"]] = {
            "records_inserted": result.metrics.get("records_inserted", 0),
            "records_updated": result.metrics.get("records_updated", 0),
            "scd_type": scd_type,
        }

    context["ti"].xcom_push(key="dimension_metrics", value=dimension_metrics)

    AuditLogger().log_event(
        event_type="DIMENSIONS_LOADED",
        execution_id=execution_id,
        event_data=dimension_metrics,
    )
    return dimension_metrics


def load_fact_table(config, feed_id, contract_id, fact_table, **context):
    """
    Load fact table with dimension surrogate key lookups (P09).

    Args:
        config: APEXConfig instance
        feed_id: Feed identifier
        contract_id: Contract identifier
        fact_table: Fact table definition dict
    """
    execution_id = context["ti"].xcom_pull(key="execution_id")

    dim_key_lookups = [
        {
            "dimension": dk["dimension"],
            "natural_key": dk["natural_key"],
            "surrogate_key": dk["surrogate_key"],
        }
        for dk in fact_table.get("dimension_keys", [])
    ]

    submitter = SparkJobSubmitter(config)
    result = submitter.submit_job(
        job_name=f"load_{fact_table['name']}",
        job_path="spark_jobs/v2/build_gold_layer.py",
        arguments={
            "--feed-id": feed_id,
            "--contract-id": contract_id,
            "--execution-id": execution_id,
            "--execution-date": context["ds"],
            "--metadata-db": config.metadata_db_url,
            "--operation": "LOAD_FACT",
            "--fact-name": fact_table["name"],
            "--grain": ",".join(fact_table.get("grain", [])),
            "--measures": ",".join(fact_table.get("measures", [])),
            "--dimension-lookups": str(dim_key_lookups),
        },
    )

    if not result.success:
        raise Exception(f"Fact table load failed: {result.error}")

    fact_metrics = {
        "records_inserted": result.metrics.get("records_inserted", 0),
        "late_arriving_facts": result.metrics.get("late_arriving", 0),
        "orphan_facts": result.metrics.get("orphan_facts", 0),
    }

    context["ti"].xcom_push(key="fact_metrics", value=fact_metrics)

    AuditLogger().log_event(
        event_type="FACT_TABLE_LOADED",
        execution_id=execution_id,
        event_data=fact_metrics,
    )
    return fact_metrics


def handle_late_arriving_dimensions(config, feed_id, contract_id, fact_table,
                                    **context):
    """
    Handle facts with late-arriving dimensions (P09).

    Args:
        config: APEXConfig instance
        feed_id: Feed identifier
        contract_id: Contract identifier
        fact_table: Fact table definition dict
    """
    execution_id = context["ti"].xcom_pull(key="execution_id")
    fact_metrics = context["ti"].xcom_pull(key="fact_metrics") or {}

    orphan_facts = fact_metrics.get("orphan_facts", 0)
    if orphan_facts == 0:
        return {"skipped": True, "reason": "No orphan facts"}

    submitter = SparkJobSubmitter(config)
    result = submitter.submit_job(
        job_name="handle_late_arriving_dims",
        job_path="spark_jobs/v2/build_gold_layer.py",
        arguments={
            "--feed-id": feed_id,
            "--contract-id": contract_id,
            "--execution-id": execution_id,
            "--execution-date": context["ds"],
            "--metadata-db": config.metadata_db_url,
            "--operation": "CREATE_PLACEHOLDER_DIMS",
            "--fact-name": fact_table["name"],
        },
    )

    context["ti"].xcom_push(key="late_arriving_metrics", value=result.metrics)
    return result.metrics


def load_aggregates(config, feed_id, contract_id, fact_table, aggregates,
                    **context):
    """
    Load aggregate fact tables (P09).

    Args:
        config: APEXConfig instance
        feed_id: Feed identifier
        contract_id: Contract identifier
        fact_table: Source fact table definition
        aggregates: List of aggregate table definitions
    """
    execution_id = context["ti"].xcom_pull(key="execution_id")

    if not aggregates:
        return {"skipped": True, "reason": "No aggregates defined"}

    aggregate_metrics = {}
    submitter = SparkJobSubmitter(config)

    for agg in aggregates:
        result = submitter.submit_job(
            job_name=f"load_{agg['name']}",
            job_path="spark_jobs/v2/build_gold_layer.py",
            arguments={
                "--feed-id": feed_id,
                "--contract-id": contract_id,
                "--execution-id": execution_id,
                "--execution-date": context["ds"],
                "--metadata-db": config.metadata_db_url,
                "--operation": "LOAD_AGGREGATE",
                "--aggregate-name": agg["name"],
                "--source-fact": fact_table["name"],
                "--grain": ",".join(agg.get("grain", [])),
                "--measures": ",".join(agg.get("measures", [])),
            },
        )

        if not result.success:
            raise Exception(f"Aggregate {agg['name']} load failed: {result.error}")

        aggregate_metrics[agg["name"]] = result.metrics

    context["ti"].xcom_push(key="aggregate_metrics", value=aggregate_metrics)
    return aggregate_metrics
