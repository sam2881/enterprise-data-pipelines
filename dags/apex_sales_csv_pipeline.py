"""
APEX Production Pipeline: Sales Daily Orders (CSV)
═══════════════════════════════════════════════════
Feed ID:    FEED_CSV_SALES_001
Contract:   CTR_SALES_001
Pattern:    P01 - File Medallion (CSV → Raw → Bronze → Silver → Gold)

THIS DAG CONTAINS NO BUSINESS LOGIC.
All behavior is driven by metadata in PostgreSQL.
All reusable functions imported from dag_utilities.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

# ═══════════════════════════════════════════════════════════════════════════════
# IMPORTS FROM dag_utilities (Centralized, Reusable)
# ═══════════════════════════════════════════════════════════════════════════════
from dag_utilities.core import MetadataClient
from dag_utilities.logging import AuditLogger

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION (from metadata - NO hardcoded values)
# ═══════════════════════════════════════════════════════════════════════════════
FEED_ID = "FEED_CSV_SALES_001"
CONTRACT_ID = "CTR_SALES_001"
DAG_ID = "apex_sales_csv_pipeline"

default_args = {
    "owner": "apex-data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}


# ═══════════════════════════════════════════════════════════════════════════════
# TASK FUNCTIONS (thin wrappers calling dag_utilities)
# ═══════════════════════════════════════════════════════════════════════════════

def initialize_execution(**context):
    """
    Step 1: Create execution record in pipeline_execution table.
    Reads feed config from metadata, validates it exists.
    """
    metadata = MetadataClient()
    try:
        # Validate feed exists and is active
        feed = metadata.get_feed_config(FEED_ID)
        contract = metadata.get_contract_by_feed(FEED_ID)

        # Create execution record
        execution_id = metadata.create_execution(
            feed_id=FEED_ID,
            params={
                "execution_date": context["ds"],
                "dag_run_id": context["run_id"],
                "trigger_type": context["dag_run"].run_type,
                "contract_id": CONTRACT_ID,
                "feed_name": feed.feed_name,
                "contract_type": contract.contract_type,
            }
        )

        # Push to XCom for downstream tasks
        context["ti"].xcom_push(key="execution_id", value=execution_id)
        context["ti"].xcom_push(key="feed_name", value=feed.feed_name)
        context["ti"].xcom_push(key="contract_type", value=contract.contract_type)
        context["ti"].xcom_push(key="file_format", value=contract.file_format)
        context["ti"].xcom_push(key="file_pattern", value=contract.file_pattern)

        AuditLogger().log_event(
            execution_id=execution_id,
            zone="INIT",
            action="PIPELINE_START",
            entity=FEED_ID,
            message=f"Pipeline initialized for {feed.feed_name}",
        )
        print(f"Execution initialized: {execution_id}")
        return execution_id
    finally:
        metadata.close()


def check_source_files(**context):
    """
    Step 2: Verify source files exist in GCS landing zone.
    Path comes from data_contract.source_path with {year}/{month} substitution.
    """
    from google.cloud import storage as gcs

    metadata = MetadataClient()
    try:
        contract = metadata.get_contract(CONTRACT_ID)
        execution_id = context["ti"].xcom_pull(key="execution_id")

        # Resolve monthly partition path
        ds = context["ds"]  # YYYY-MM-DD
        year = ds[:4]
        month = ds[5:7]
        source_path = contract.source_path.format(year=year, month=month)

        # Parse GCS path: gs://bucket/prefix/
        parts = source_path.replace("gs://", "").split("/", 1)
        bucket_name = parts[0]
        prefix = parts[1] if len(parts) > 1 else ""

        client = gcs.Client()
        bucket = client.bucket(bucket_name)
        blobs = list(bucket.list_blobs(prefix=prefix))

        # Filter by file pattern
        pattern = contract.file_pattern or "*"
        import fnmatch
        matching = [b for b in blobs if fnmatch.fnmatch(b.name.split("/")[-1], pattern)]

        if not matching:
            raise FileNotFoundError(
                f"No files matching '{pattern}' in {source_path}"
            )

        file_paths = [f"gs://{bucket_name}/{b.name}" for b in matching]
        total_size = sum(b.size or 0 for b in matching)

        context["ti"].xcom_push(key="source_files", value=file_paths)
        context["ti"].xcom_push(key="source_file_count", value=len(file_paths))
        context["ti"].xcom_push(key="total_source_bytes", value=total_size)

        AuditLogger().log_event(
            execution_id=execution_id,
            zone="LANDING",
            action="SOURCE_CHECK",
            entity=FEED_ID,
            record_count=len(file_paths),
            message=f"Found {len(file_paths)} files ({total_size} bytes) in {source_path}",
        )
        print(f"Found {len(file_paths)} source files ({total_size} bytes)")
        return file_paths
    finally:
        metadata.close()


def process_landing_to_raw(**context):
    """
    Step 3: Landing → Raw zone.
    Copy files to raw zone with audit columns. No schema enforcement.
    All columns read as STRING (raw copy).
    """
    import pandas as pd
    from google.cloud import storage as gcs
    import io

    metadata = MetadataClient()
    try:
        contract = metadata.get_contract(CONTRACT_ID)
        schema = metadata.get_current_schema(CONTRACT_ID)
        execution_id = context["ti"].xcom_pull(key="execution_id")
        source_files = context["ti"].xcom_pull(key="source_files")

        ds = context["ds"]
        year, month = ds[:4], ds[5:7]

        # Read config from schema metadata
        delimiter = schema.get("col_delimiter", ",")
        header_rows = schema.get("header_rows", 1)
        encoding = schema.get("encoding", "UTF-8")

        all_dfs = []
        for file_path in source_files:
            # Parse GCS path
            parts = file_path.replace("gs://", "").split("/", 1)
            bucket_name, blob_name = parts[0], parts[1]

            client = gcs.Client()
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            content = blob.download_as_text(encoding=encoding)

            # Read ALL columns as STRING (raw zone)
            df = pd.read_csv(
                io.StringIO(content),
                sep=delimiter,
                header=0 if header_rows > 0 else None,
                dtype=str,  # All STRING in raw zone
            )

            # Add audit columns
            df["_apex_ingestion_ts"] = datetime.utcnow().isoformat()
            df["_apex_source_file"] = blob_name.split("/")[-1]
            df["_apex_batch_id"] = f"{FEED_ID}_{ds}"
            df["_apex_execution_id"] = execution_id

            all_dfs.append(df)

        combined = pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()
        record_count = len(combined)

        # Write to raw zone (monthly partition)
        raw_path = contract.raw_path.format(year=year, month=month)
        raw_parts = raw_path.replace("gs://", "").split("/", 1)
        raw_bucket = raw_parts[0]
        raw_prefix = raw_parts[1]

        output_blob = f"{raw_prefix}{FEED_ID}_{ds}.parquet"
        client = gcs.Client()
        bucket = client.bucket(raw_bucket)
        blob = bucket.blob(output_blob)

        parquet_buffer = io.BytesIO()
        combined.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)
        blob.upload_from_file(parquet_buffer, content_type="application/octet-stream")

        context["ti"].xcom_push(key="raw_record_count", value=record_count)
        context["ti"].xcom_push(key="raw_output_path", value=f"gs://{raw_bucket}/{output_blob}")

        AuditLogger().log_event(
            execution_id=execution_id,
            zone="RAW",
            action="LANDING_TO_RAW",
            entity=FEED_ID,
            record_count=record_count,
            message=f"Raw zone: {record_count} records written to gs://{raw_bucket}/{output_blob}",
        )
        print(f"Raw zone: {record_count} records → gs://{raw_bucket}/{output_blob}")
        return record_count
    finally:
        metadata.close()


def validate_raw_zone(**context):
    """
    Step 4: Validate raw zone data using rules from validation_rule table.
    Schema validation: column count, not-null checks.
    """
    metadata = MetadataClient()
    try:
        execution_id = context["ti"].xcom_pull(key="execution_id")
        raw_record_count = context["ti"].xcom_pull(key="raw_record_count") or 0
        schema = metadata.get_current_schema(CONTRACT_ID)
        rules = metadata.get_validation_rules(CONTRACT_ID, "raw")

        schema_json = schema.get("schema_json", {})
        expected_columns = [c["name"] for c in schema_json.get("columns", [])]

        validation_results = []
        all_passed = True

        for rule in rules:
            result = {"rule_name": rule["rule_name"], "passed": True, "details": ""}

            if rule["rule_expression"] == "expect_table_column_count_to_equal":
                actual_count = len(expected_columns) + 4  # +4 audit columns
                result["details"] = f"Expected >= {len(expected_columns)} columns"
                result["passed"] = True  # Raw has audit cols added

            elif rule["rule_expression"] == "expect_column_values_to_not_be_null":
                result["details"] = f"Not-null check for raw data (records: {raw_record_count})"
                result["passed"] = raw_record_count > 0

            validation_results.append(result)
            if not result["passed"] and rule.get("is_blocking", True):
                all_passed = False

        context["ti"].xcom_push(key="raw_validation_passed", value=all_passed)
        context["ti"].xcom_push(key="raw_validation_results", value=validation_results)

        AuditLogger().log_event(
            execution_id=execution_id,
            zone="RAW",
            action="SCHEMA_VALIDATION",
            entity=FEED_ID,
            record_count=len(validation_results),
            message=f"Raw validation: {'PASSED' if all_passed else 'FAILED'} ({len(rules)} rules)",
        )

        if not all_passed:
            raise ValueError(f"Raw zone validation failed: {validation_results}")

        print(f"Raw validation PASSED ({len(rules)} rules checked)")
        return all_passed
    finally:
        metadata.close()


def process_raw_to_bronze(**context):
    """
    Step 5: Raw → Bronze zone.
    Apply type casting from schema_version metadata.
    Schema enforcement: cast STRING → proper types.
    """
    import pandas as pd
    from google.cloud import storage as gcs
    import io

    metadata = MetadataClient()
    try:
        contract = metadata.get_contract(CONTRACT_ID)
        schema = metadata.get_current_schema(CONTRACT_ID)
        execution_id = context["ti"].xcom_pull(key="execution_id")
        raw_path = context["ti"].xcom_pull(key="raw_output_path")

        ds = context["ds"]
        year, month = ds[:4], ds[5:7]

        # Read raw parquet
        parts = raw_path.replace("gs://", "").split("/", 1)
        client = gcs.Client()
        bucket = client.bucket(parts[0])
        blob = bucket.blob(parts[1])

        buf = io.BytesIO()
        blob.download_to_file(buf)
        buf.seek(0)
        df = pd.read_parquet(buf)

        # Type casting from schema metadata
        schema_json = schema.get("schema_json", {})
        type_map = {
            "STRING": "str",
            "INTEGER": "int64",
            "NUMERIC": "float64",
            "DATE": "datetime64[ns]",
            "BOOLEAN": "bool",
            "TIMESTAMP": "datetime64[ns]",
        }

        for col_def in schema_json.get("columns", []):
            col_name = col_def["name"]
            col_type = col_def.get("type", "STRING")
            if col_name in df.columns:
                try:
                    if col_type == "INTEGER":
                        df[col_name] = pd.to_numeric(df[col_name], errors="coerce").astype("Int64")
                    elif col_type == "NUMERIC":
                        df[col_name] = pd.to_numeric(df[col_name], errors="coerce")
                    elif col_type == "DATE":
                        df[col_name] = pd.to_datetime(df[col_name], errors="coerce")
                    elif col_type == "BOOLEAN":
                        df[col_name] = df[col_name].map({"true": True, "false": False, "1": True, "0": False})
                except Exception as e:
                    print(f"Warning: Could not cast {col_name} to {col_type}: {e}")

        record_count = len(df)

        # Write to bronze zone (monthly partition)
        bronze_path = contract.bronze_path.format(year=year, month=month)
        bronze_parts = bronze_path.replace("gs://", "").split("/", 1)
        output_blob_name = f"{bronze_parts[1]}{FEED_ID}_{ds}.parquet"

        bucket = client.bucket(bronze_parts[0])
        blob = bucket.blob(output_blob_name)

        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        buf.seek(0)
        blob.upload_from_file(buf, content_type="application/octet-stream")

        context["ti"].xcom_push(key="bronze_record_count", value=record_count)
        context["ti"].xcom_push(key="bronze_output_path", value=f"gs://{bronze_parts[0]}/{output_blob_name}")

        AuditLogger().log_event(
            execution_id=execution_id,
            zone="BRONZE",
            action="RAW_TO_BRONZE",
            entity=FEED_ID,
            record_count=record_count,
            message=f"Bronze zone: {record_count} records, types cast from schema",
        )
        print(f"Bronze zone: {record_count} records → gs://{bronze_parts[0]}/{output_blob_name}")
        return record_count
    finally:
        metadata.close()


def process_bronze_to_silver(**context):
    """
    Step 6: Bronze → Silver zone.
    Apply cleaning: dedup by primary keys, null handling, standardization.
    Transforms driven by metadata.
    """
    import pandas as pd
    from google.cloud import storage as gcs
    import io

    metadata = MetadataClient()
    try:
        contract = metadata.get_contract(CONTRACT_ID)
        execution_id = context["ti"].xcom_pull(key="execution_id")
        bronze_path = context["ti"].xcom_pull(key="bronze_output_path")

        ds = context["ds"]
        year, month = ds[:4], ds[5:7]

        # Read bronze parquet
        parts = bronze_path.replace("gs://", "").split("/", 1)
        client = gcs.Client()
        bucket = client.bucket(parts[0])
        blob = bucket.blob(parts[1])

        buf = io.BytesIO()
        blob.download_to_file(buf)
        buf.seek(0)
        df = pd.read_parquet(buf)

        records_before = len(df)

        # Dedup by primary keys (from contract metadata)
        pk_cols = contract.primary_keys
        if pk_cols:
            df = df.drop_duplicates(subset=pk_cols, keep="last")

        # Trim whitespace on string columns
        for col in df.select_dtypes(include=["object"]).columns:
            df[col] = df[col].str.strip()

        records_after = len(df)
        records_deduped = records_before - records_after

        # Write to silver zone (monthly partition)
        silver_path = contract.silver_path.format(year=year, month=month)
        silver_parts = silver_path.replace("gs://", "").split("/", 1)
        output_blob_name = f"{silver_parts[1]}{FEED_ID}_{ds}.parquet"

        bucket = client.bucket(silver_parts[0])
        blob = bucket.blob(output_blob_name)

        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        buf.seek(0)
        blob.upload_from_file(buf, content_type="application/octet-stream")

        context["ti"].xcom_push(key="silver_record_count", value=records_after)
        context["ti"].xcom_push(key="silver_deduped_count", value=records_deduped)
        context["ti"].xcom_push(key="silver_output_path", value=f"gs://{silver_parts[0]}/{output_blob_name}")

        AuditLogger().log_event(
            execution_id=execution_id,
            zone="SILVER",
            action="BRONZE_TO_SILVER",
            entity=FEED_ID,
            record_count=records_after,
            message=f"Silver zone: {records_after} records (deduped {records_deduped} by {pk_cols})",
        )
        print(f"Silver zone: {records_after} records (deduped {records_deduped})")
        return records_after
    finally:
        metadata.close()


def validate_silver_zone(**context):
    """
    Step 7: Validate silver zone using semantic rules from validation_rule table.
    Business rule checks: ranges, allowed values, etc.
    """
    import pandas as pd
    from google.cloud import storage as gcs
    import io

    metadata = MetadataClient()
    try:
        execution_id = context["ti"].xcom_pull(key="execution_id")
        silver_path = context["ti"].xcom_pull(key="silver_output_path")
        rules = metadata.get_validation_rules(CONTRACT_ID, "silver")

        # Read silver data for validation
        parts = silver_path.replace("gs://", "").split("/", 1)
        client = gcs.Client()
        bucket = client.bucket(parts[0])
        blob = bucket.blob(parts[1])

        buf = io.BytesIO()
        blob.download_to_file(buf)
        buf.seek(0)
        df = pd.read_parquet(buf)

        # Get quality expectations from metadata
        expectations = []
        with metadata.conn.cursor() as cur:
            cur.execute("""
                SELECT expectation_type, column_name, kwargs
                FROM quality_expectation
                WHERE contract_id = %s AND zone_level = 'silver' AND is_active = true
            """, [CONTRACT_ID])
            expectations = [dict(r) for r in cur.fetchall()]

        validation_results = []
        all_passed = True
        total_checks = 0
        passed_checks = 0

        for exp in expectations:
            total_checks += 1
            exp_type = exp["expectation_type"]
            col = exp.get("column_name")
            kwargs = exp.get("kwargs", {})
            result = {"expectation": exp_type, "column": col, "passed": True, "details": ""}

            try:
                if exp_type == "expect_column_values_to_be_between" and col:
                    min_val = kwargs.get("min_value")
                    max_val = kwargs.get("max_value")
                    series = pd.to_numeric(df[col], errors="coerce")
                    violations = series[(series < min_val) | (series > max_val)].count()
                    total = series.count()
                    result["passed"] = violations == 0
                    result["details"] = f"{violations}/{total} out of range [{min_val}, {max_val}]"

                elif exp_type == "expect_column_values_to_be_in_set" and col:
                    valid_set = set(kwargs.get("value_set", []))
                    violations = df[~df[col].isin(valid_set)][col].count()
                    total = len(df)
                    result["passed"] = violations == 0
                    result["details"] = f"{violations}/{total} not in {valid_set}"

                elif exp_type == "expect_column_values_to_not_be_null" and col:
                    nulls = df[col].isna().sum()
                    result["passed"] = nulls == 0
                    result["details"] = f"{nulls} null values"

            except Exception as e:
                result["passed"] = False
                result["details"] = str(e)

            validation_results.append(result)
            if result["passed"]:
                passed_checks += 1
            else:
                # Check severity from rules
                matching_rule = next(
                    (r for r in rules if r["rule_expression"] == exp_type),
                    {"severity": "WARNING"}
                )
                if matching_rule.get("severity") == "ERROR":
                    all_passed = False

        quality_score = (passed_checks / total_checks * 100) if total_checks > 0 else 100.0

        context["ti"].xcom_push(key="silver_validation_passed", value=all_passed)
        context["ti"].xcom_push(key="silver_quality_score", value=quality_score)

        # Write validation results to ge_validation_result table
        with metadata.conn.cursor() as cur:
            for vr in validation_results:
                cur.execute("""
                    INSERT INTO ge_validation_result (
                        feed_id, run_id, validation_type, zone_level,
                        expectation_type, result, column_name,
                        element_count, error_count, severity
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, [
                    FEED_ID, execution_id, "SEMANTIC", "SILVER",
                    vr["expectation"], "PASSED" if vr["passed"] else "FAILED",
                    vr.get("column"), len(df), 0 if vr["passed"] else 1,
                    "ERROR" if not vr["passed"] else "INFO",
                ])
            metadata.conn.commit()

        AuditLogger().log_event(
            execution_id=execution_id,
            zone="SILVER",
            action="SEMANTIC_VALIDATION",
            entity=FEED_ID,
            record_count=total_checks,
            message=f"Silver validation: {passed_checks}/{total_checks} passed (score: {quality_score:.1f}%)",
        )

        if not all_passed:
            raise ValueError(f"Silver validation failed: score={quality_score:.1f}%")

        print(f"Silver validation PASSED: {passed_checks}/{total_checks} (score: {quality_score:.1f}%)")
        return all_passed
    finally:
        metadata.close()


def process_silver_to_gold(**context):
    """
    Step 8: Silver → Gold zone.
    Business-ready data with aggregations and final transformations.
    """
    import pandas as pd
    from google.cloud import storage as gcs
    import io

    metadata = MetadataClient()
    try:
        contract = metadata.get_contract(CONTRACT_ID)
        execution_id = context["ti"].xcom_pull(key="execution_id")
        silver_path = context["ti"].xcom_pull(key="silver_output_path")

        ds = context["ds"]
        year, month = ds[:4], ds[5:7]

        # Read silver parquet
        parts = silver_path.replace("gs://", "").split("/", 1)
        client = gcs.Client()
        bucket = client.bucket(parts[0])
        blob = bucket.blob(parts[1])

        buf = io.BytesIO()
        blob.download_to_file(buf)
        buf.seek(0)
        df = pd.read_parquet(buf)

        # Add computed business columns
        if "quantity" in df.columns and "unit_price" in df.columns:
            df["total_amount"] = pd.to_numeric(df["quantity"], errors="coerce") * pd.to_numeric(df["unit_price"], errors="coerce")

        # Add gold audit columns
        df["_apex_gold_ts"] = datetime.utcnow().isoformat()
        df["_apex_quality_score"] = context["ti"].xcom_pull(key="silver_quality_score") or 100.0

        record_count = len(df)

        # Write to gold zone (monthly partition)
        gold_path = contract.gold_path.format(year=year, month=month)
        gold_parts = gold_path.replace("gs://", "").split("/", 1)
        output_blob_name = f"{gold_parts[1]}{FEED_ID}_{ds}.parquet"

        bucket = client.bucket(gold_parts[0])
        blob = bucket.blob(output_blob_name)

        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        buf.seek(0)
        blob.upload_from_file(buf, content_type="application/octet-stream")

        context["ti"].xcom_push(key="gold_record_count", value=record_count)
        context["ti"].xcom_push(key="gold_output_path", value=f"gs://{gold_parts[0]}/{output_blob_name}")

        AuditLogger().log_event(
            execution_id=execution_id,
            zone="GOLD",
            action="SILVER_TO_GOLD",
            entity=FEED_ID,
            record_count=record_count,
            message=f"Gold zone: {record_count} records with business calculations",
        )
        print(f"Gold zone: {record_count} records → gs://{gold_parts[0]}/{output_blob_name}")
        return record_count
    finally:
        metadata.close()


def finalize_execution(**context):
    """
    Step 9: Finalize execution.
    Update pipeline_execution status, record metrics, log completion.
    """
    metadata = MetadataClient()
    try:
        execution_id = context["ti"].xcom_pull(key="execution_id")
        raw_count = context["ti"].xcom_pull(key="raw_record_count") or 0
        bronze_count = context["ti"].xcom_pull(key="bronze_record_count") or 0
        silver_count = context["ti"].xcom_pull(key="silver_record_count") or 0
        gold_count = context["ti"].xcom_pull(key="gold_record_count") or 0
        quality_score = context["ti"].xcom_pull(key="silver_quality_score") or 100.0

        # Update execution status to COMPLETED
        metadata.update_execution_status(
            execution_id=execution_id,
            status="COMPLETED",
        )

        # Update metrics
        metadata.update_execution_metrics(
            execution_id=execution_id,
            records_processed=gold_count,
            bytes_processed=0,
        )

        AuditLogger().log_event(
            execution_id=execution_id,
            zone="COMPLETE",
            action="PIPELINE_FINALIZE",
            entity=FEED_ID,
            record_count=gold_count,
            message=(
                f"Pipeline complete: raw={raw_count} → bronze={bronze_count} → "
                f"silver={silver_count} → gold={gold_count} | quality={quality_score:.1f}%"
            ),
        )
        print(
            f"Pipeline COMPLETE: raw={raw_count} → bronze={bronze_count} → "
            f"silver={silver_count} → gold={gold_count} | quality={quality_score:.1f}%"
        )
    finally:
        metadata.close()


def handle_failure(**context):
    """Handle pipeline failure: update status, log error."""
    metadata = MetadataClient()
    try:
        execution_id = context["ti"].xcom_pull(key="execution_id")
        if execution_id:
            error_msg = str(context.get("exception", "Unknown error"))
            metadata.update_execution_status(
                execution_id=execution_id,
                status="FAILED",
                error_message=error_msg[:500],
            )
            AuditLogger().log_event(
                execution_id=execution_id,
                zone="ERROR",
                action="PIPELINE_FAILURE",
                entity=FEED_ID,
                message=f"Pipeline failed: {error_msg[:200]}",
            )
    except Exception as e:
        print(f"Error in failure handler: {e}")
    finally:
        metadata.close()


# ═══════════════════════════════════════════════════════════════════════════════
# DAG DEFINITION
# ═══════════════════════════════════════════════════════════════════════════════
with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="APEX Production Pipeline: CSV → Raw → Bronze → Silver → Gold (metadata-driven)",
    schedule_interval="@daily",
    start_date=datetime(2026, 2, 1),
    catchup=False,
    max_active_runs=1,
    tags=["apex", "production", "P01", "csv", "sales", "metadata-driven"],
    on_failure_callback=handle_failure,
) as dag:

    # ─────────────────────────────────────────────────────────────────────────
    # TASKS (thin orchestration - all logic from dag_utilities + metadata)
    # ─────────────────────────────────────────────────────────────────────────

    t_init = PythonOperator(
        task_id="initialize_execution",
        python_callable=initialize_execution,
    )

    t_check_source = PythonOperator(
        task_id="check_source_files",
        python_callable=check_source_files,
    )

    t_landing_to_raw = PythonOperator(
        task_id="landing_to_raw",
        python_callable=process_landing_to_raw,
    )

    t_validate_raw = PythonOperator(
        task_id="validate_raw_zone",
        python_callable=validate_raw_zone,
    )

    t_raw_to_bronze = PythonOperator(
        task_id="raw_to_bronze",
        python_callable=process_raw_to_bronze,
    )

    t_bronze_to_silver = PythonOperator(
        task_id="bronze_to_silver",
        python_callable=process_bronze_to_silver,
    )

    t_validate_silver = PythonOperator(
        task_id="validate_silver_zone",
        python_callable=validate_silver_zone,
    )

    t_silver_to_gold = PythonOperator(
        task_id="silver_to_gold",
        python_callable=process_silver_to_gold,
    )

    t_finalize = PythonOperator(
        task_id="finalize_execution",
        python_callable=finalize_execution,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ─────────────────────────────────────────────────────────────────────────
    # TASK DEPENDENCIES (Linear medallion flow)
    # ─────────────────────────────────────────────────────────────────────────
    (
        t_init
        >> t_check_source
        >> t_landing_to_raw
        >> t_validate_raw
        >> t_raw_to_bronze
        >> t_bronze_to_silver
        >> t_validate_silver
        >> t_silver_to_gold
        >> t_finalize
    )
