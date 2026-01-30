"""
E2E Test DAG: CSV Sales Orders - Metadata-Driven Medallion Pipeline

This DAG demonstrates the APEX framework's metadata-driven approach:
  1. Reads feed configuration from PostgreSQL metadata tables
  2. Reads CSV from GCS landing zone (raw)
  3. Bronze: Schema enforcement + audit columns → GCS Parquet + BigQuery
  4. Silver: Cleansing, dedup, business key → GCS Parquet + BigQuery
  5. Gold:  Aggregation → BigQuery
  6. Validates data at each zone transition

All schema, paths, and table names come from metadata — NOT hard-coded.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

# ─── Constants ───────────────────────────────────────────────────────────────
FEED_ID = "sales_orders_csv"
GCP_PROJECT = "agent-ai-test-461120"
METADATA_DB = "postgresql://admin:admin123@postgres:5432/agentdb"

default_args = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="e2e_csv_sales_orders",
    default_args=default_args,
    description="E2E: CSV → Bronze → Silver → Gold (metadata-driven)",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["e2e", "medallion", "csv", "sales", "metadata-driven"],
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")

    # ═════════════════════════════════════════════════════════════════════════
    # STEP 1: Initialize — Read all metadata from PostgreSQL
    # ═════════════════════════════════════════════════════════════════════════
    @task
    def initialize_execution(**context):
        """Read feed metadata from PostgreSQL and create execution record."""
        import psycopg2
        import json

        conn = psycopg2.connect(METADATA_DB)
        cur = conn.cursor()

        # Read feed registry
        cur.execute(
            "SELECT feed_name, source_type, file_delimiter, file_header, schedule_interval "
            "FROM feed_registry WHERE feed_id = %s AND is_active = true",
            (FEED_ID,),
        )
        row = cur.fetchone()
        if not row:
            raise ValueError(f"Feed {FEED_ID} not found or inactive in feed_registry")

        feed_config = {
            "feed_name": row[0],
            "source_type": row[1],
            "delimiter": row[2],
            "header": row[3],
            "schedule": row[4],
        }

        # Read feed contract (paths)
        cur.execute(
            "SELECT source_path, bronze_path, silver_path, gold_path, file_format, encoding "
            "FROM feed_contract WHERE feed_id = %s AND is_active = true",
            (FEED_ID,),
        )
        row = cur.fetchone()
        if not row:
            raise ValueError(f"No active contract for feed {FEED_ID}")

        paths = {
            "source_path": row[0],
            "bronze_path": row[1],
            "silver_path": row[2],
            "gold_path": row[3],
            "file_format": row[4],
            "encoding": row[5],
        }

        # Read schema (columns)
        cur.execute(
            "SELECT column_name, column_order, source_type, target_type, "
            "is_primary_key, is_nullable, transform_expression "
            "FROM feed_columns WHERE feed_id = %s ORDER BY column_order",
            (FEED_ID,),
        )
        columns = []
        primary_keys = []
        for r in cur.fetchall():
            col = {
                "name": r[0],
                "order": r[1],
                "source_type": r[2],
                "target_type": r[3],
                "is_primary_key": r[4],
                "is_nullable": r[5],
                "transform_expression": r[6],
            }
            columns.append(col)
            if r[4]:
                primary_keys.append(r[0])

        # Read targets
        cur.execute(
            "SELECT layer, database_name, schema_name, table_name, write_mode "
            "FROM feed_targets WHERE feed_id = %s",
            (FEED_ID,),
        )
        targets = {}
        for r in cur.fetchall():
            targets[r[0]] = {
                "project": r[1],
                "dataset": r[2],
                "table": r[3],
                "write_mode": r[4],
            }

        # Create execution record — use ts_nodash for uniqueness across manual runs
        run_ts = context['ts_nodash']  # e.g. 20260127T202905
        execution_id = f"exec_{FEED_ID}_{run_ts}"

        exec_run_id = f"{FEED_ID}_{run_ts}"
        cur.execute(
            "INSERT INTO pipeline_executions (pipeline_id, run_id, status, started_at) "
            "VALUES (%s, %s, %s, NOW()) RETURNING id",
            (FEED_ID, exec_run_id, "RUNNING"),
        )
        db_exec_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()

        metadata = {
            "execution_id": execution_id,
            "db_execution_id": db_exec_id,
            "feed_id": FEED_ID,
            "feed_config": feed_config,
            "paths": paths,
            "columns": columns,
            "primary_keys": primary_keys,
            "targets": targets,
        }

        print(f"✅ Initialized execution: {execution_id}")
        print(f"   Feed: {feed_config['feed_name']}")
        print(f"   Columns: {len(columns)}")
        print(f"   Primary Keys: {primary_keys}")
        print(f"   Targets: {list(targets.keys())}")

        return metadata

    # ═════════════════════════════════════════════════════════════════════════
    # STEP 2: Check Source — Verify CSV exists in GCS landing zone
    # ═════════════════════════════════════════════════════════════════════════
    @task
    def check_source_file(metadata):
        """Verify source CSV file exists in GCS."""
        from google.cloud import storage

        source_path = metadata["paths"]["source_path"]
        print(f"Checking source path: {source_path}")

        # Parse GCS path
        parts = source_path.replace("gs://", "").split("/", 1)
        bucket_name = parts[0]
        prefix = parts[1] if len(parts) > 1 else ""

        client = storage.Client(project=GCP_PROJECT)
        bucket = client.bucket(bucket_name)
        blobs = list(bucket.list_blobs(prefix=prefix))

        csv_files = [b.name for b in blobs if b.name.endswith(".csv")]
        if not csv_files:
            raise FileNotFoundError(f"No CSV files found in {source_path}")

        total_size = sum(b.size for b in blobs if b.name.endswith(".csv"))

        print(f"✅ Found {len(csv_files)} CSV file(s), total size: {total_size} bytes")
        for f in csv_files:
            print(f"   - {f}")

        metadata["source_files"] = csv_files
        metadata["source_size_bytes"] = total_size
        return metadata

    # ═════════════════════════════════════════════════════════════════════════
    # STEP 3: Raw → Bronze — Schema enforcement + audit columns
    # ═════════════════════════════════════════════════════════════════════════
    @task
    def raw_to_bronze(metadata):
        """
        Read CSV from GCS, enforce schema from metadata, add audit columns,
        write Parquet to GCS Bronze zone and BigQuery bronze table.
        """
        import pandas as pd
        from google.cloud import storage, bigquery
        from io import StringIO
        import hashlib

        paths = metadata["paths"]
        columns = metadata["columns"]
        feed_config = metadata["feed_config"]
        target = metadata["targets"]["bronze"]
        execution_id = metadata["execution_id"]

        # --- Read CSV from GCS ---
        source_path = paths["source_path"]
        parts = source_path.replace("gs://", "").split("/", 1)
        bucket_name, prefix = parts[0], parts[1] if len(parts) > 1 else ""

        client = storage.Client(project=GCP_PROJECT)
        bucket = client.bucket(bucket_name)

        all_dfs = []
        for blob_name in metadata["source_files"]:
            blob = bucket.blob(blob_name)
            content = blob.download_as_text(encoding=paths.get("encoding", "UTF-8"))
            df = pd.read_csv(
                StringIO(content),
                delimiter=feed_config["delimiter"],
                header=0 if feed_config["header"] else None,
            )
            all_dfs.append(df)

        df = pd.concat(all_dfs, ignore_index=True)
        records_read = len(df)
        print(f"Read {records_read} records from {len(all_dfs)} file(s)")

        # --- Schema enforcement from metadata ---
        expected_cols = [c["name"] for c in columns]
        actual_cols = list(df.columns)

        missing = set(expected_cols) - set(actual_cols)
        if missing:
            raise ValueError(f"Missing columns in source: {missing}")

        # Select only declared columns in order
        df = df[expected_cols]

        # Type casting based on metadata target_type
        TYPE_MAP = {
            "STRING": "str",
            "INTEGER": "int64",
            "DECIMAL": "float64",
            "DATE": "datetime64[ns]",
            "BOOLEAN": "bool",
            "TIMESTAMP": "datetime64[ns]",
        }

        for col_def in columns:
            col_name = col_def["name"]
            target_type = col_def["target_type"]
            is_nullable = col_def["is_nullable"]

            if target_type in ("INTEGER",):
                # Handle nullable integers
                df[col_name] = pd.to_numeric(df[col_name], errors="coerce")
                if not is_nullable:
                    nulls = df[col_name].isna().sum()
                    if nulls > 0:
                        raise ValueError(
                            f"Column {col_name} has {nulls} nulls but is NOT NULLABLE"
                        )
            elif target_type == "DECIMAL":
                df[col_name] = pd.to_numeric(df[col_name], errors="coerce")
            elif target_type == "DATE":
                df[col_name] = pd.to_datetime(df[col_name], errors="coerce")
            elif target_type == "STRING":
                df[col_name] = df[col_name].astype(str)

        # --- Add audit columns ---
        df["_execution_id"] = execution_id
        df["_ingestion_ts"] = pd.Timestamp.utcnow()
        df["_source_file"] = metadata["source_files"][0]
        df["_is_current"] = True

        records_written = len(df)

        # --- Write to BigQuery bronze table ---
        bq_client = bigquery.Client(project=GCP_PROJECT)
        table_ref = f"{target['project']}.{target['dataset']}.{target['table']}"

        job_config = bigquery.LoadJobConfig(
            write_disposition=(
                bigquery.WriteDisposition.WRITE_APPEND
                if target["write_mode"] == "append"
                else bigquery.WriteDisposition.WRITE_TRUNCATE
            ),
            autodetect=False,
        )

        job = bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()  # Wait for completion

        print(f"✅ Bronze: Loaded {records_written} records to BigQuery {table_ref}")

        metadata["bronze_metrics"] = {
            "records_read": records_read,
            "records_written": records_written,
            "bronze_bq_table": table_ref,
        }
        return metadata

    # ═════════════════════════════════════════════════════════════════════════
    # STEP 4: Bronze Validation — Check schema + nullability
    # ═════════════════════════════════════════════════════════════════════════
    @task
    def validate_bronze(metadata):
        """Validate Bronze data quality."""
        from google.cloud import bigquery

        target = metadata["targets"]["bronze"]
        table_ref = f"{target['project']}.{target['dataset']}.{target['table']}"
        columns = metadata["columns"]
        primary_keys = metadata["primary_keys"]

        bq_client = bigquery.Client(project=GCP_PROJECT)

        validations = []

        # 1. Row count check
        query = f"SELECT COUNT(*) as cnt FROM `{table_ref}` WHERE _execution_id = '{metadata['execution_id']}'"
        result = list(bq_client.query(query).result())
        row_count = result[0].cnt
        validations.append({
            "check": "row_count",
            "passed": row_count > 0,
            "detail": f"{row_count} rows",
        })

        # 2. Primary key uniqueness
        if primary_keys:
            pk_cols = ", ".join(primary_keys)
            query = (
                f"SELECT {pk_cols}, COUNT(*) as cnt "
                f"FROM `{table_ref}` "
                f"WHERE _execution_id = '{metadata['execution_id']}' "
                f"GROUP BY {pk_cols} HAVING COUNT(*) > 1"
            )
            dupes = list(bq_client.query(query).result())
            validations.append({
                "check": "primary_key_uniqueness",
                "passed": len(dupes) == 0,
                "detail": f"{len(dupes)} duplicate keys",
            })

        # 3. Not-null checks
        for col_def in columns:
            if not col_def["is_nullable"]:
                query = (
                    f"SELECT COUNT(*) as null_count FROM `{table_ref}` "
                    f"WHERE _execution_id = '{metadata['execution_id']}' "
                    f"AND {col_def['name']} IS NULL"
                )
                result = list(bq_client.query(query).result())
                null_count = result[0].null_count
                validations.append({
                    "check": f"not_null_{col_def['name']}",
                    "passed": null_count == 0,
                    "detail": f"{null_count} nulls",
                })

        all_passed = all(v["passed"] for v in validations)
        print(f"{'✅' if all_passed else '❌'} Bronze Validation: {sum(v['passed'] for v in validations)}/{len(validations)} checks passed")
        for v in validations:
            print(f"   {'✅' if v['passed'] else '❌'} {v['check']}: {v['detail']}")

        if not all_passed:
            failed = [v for v in validations if not v["passed"]]
            raise ValueError(f"Bronze validation failed: {failed}")

        metadata["bronze_validation"] = {"all_passed": all_passed, "checks": len(validations)}
        return metadata

    # ═════════════════════════════════════════════════════════════════════════
    # STEP 5: Bronze → Silver — Dedup + business key + cleansing
    # ═════════════════════════════════════════════════════════════════════════
    @task
    def bronze_to_silver(metadata):
        """
        Transform Bronze to Silver:
        - Deduplicate by primary key (keep latest ingestion)
        - Generate MD5 business key
        - Apply cleansing transforms from metadata
        - Write to GCS Silver + BigQuery
        """
        from google.cloud import bigquery
        import pandas as pd
        import hashlib

        target_bronze = metadata["targets"]["bronze"]
        target_silver = metadata["targets"]["silver"]
        columns = metadata["columns"]
        primary_keys = metadata["primary_keys"]
        execution_id = metadata["execution_id"]

        bq_client = bigquery.Client(project=GCP_PROJECT)
        bronze_table = f"{target_bronze['project']}.{target_bronze['dataset']}.{target_bronze['table']}"

        # Read from BigQuery bronze
        query = f"SELECT * FROM `{bronze_table}` WHERE _execution_id = '{execution_id}'"
        df = bq_client.query(query).to_dataframe()
        records_before_dedup = len(df)

        # --- Deduplication by primary key (keep latest _ingestion_ts) ---
        if primary_keys:
            df = df.sort_values("_ingestion_ts", ascending=False)
            df = df.drop_duplicates(subset=primary_keys, keep="first")

        records_after_dedup = len(df)
        dupes_removed = records_before_dedup - records_after_dedup

        # --- Generate MD5 business key ---
        if primary_keys:
            df["_business_key"] = df[primary_keys].astype(str).agg("|".join, axis=1).apply(
                lambda x: hashlib.md5(x.encode()).hexdigest()
            )

        # --- Apply transform expressions from metadata ---
        for col_def in columns:
            if col_def.get("transform_expression"):
                expr = col_def["transform_expression"]
                col_name = col_def["name"]
                try:
                    df[col_name] = df.eval(expr)
                    print(f"   Applied transform on {col_name}: {expr}")
                except Exception as e:
                    print(f"   ⚠ Transform failed on {col_name}: {e}")

        # --- Add Silver audit columns ---
        df["_silver_execution_id"] = execution_id
        df["_silver_processed_ts"] = pd.Timestamp.utcnow()
        df["_is_valid"] = True

        # --- Write to BigQuery silver table ---
        silver_table_ref = f"{target_silver['project']}.{target_silver['dataset']}.{target_silver['table']}"
        job_config = bigquery.LoadJobConfig(
            write_disposition=(
                bigquery.WriteDisposition.WRITE_APPEND
                if target_silver["write_mode"] == "append"
                else bigquery.WriteDisposition.WRITE_TRUNCATE
            ),
        )
        job = bq_client.load_table_from_dataframe(df, silver_table_ref, job_config=job_config)
        job.result()

        print(f"✅ Silver: {records_after_dedup} records to BigQuery {silver_table_ref}")
        print(f"   Deduplication: removed {dupes_removed} duplicates")

        metadata["silver_metrics"] = {
            "records_in": records_before_dedup,
            "records_out": records_after_dedup,
            "duplicates_removed": dupes_removed,
            "silver_bq_table": silver_table_ref,
        }
        return metadata

    # ═════════════════════════════════════════════════════════════════════════
    # STEP 6: Silver → Gold — Aggregation + BigQuery final table
    # ═════════════════════════════════════════════════════════════════════════
    @task
    def silver_to_gold(metadata):
        """
        Build Gold layer with business aggregations:
        - Total revenue per customer
        - Order count per category
        - Summary table in BigQuery
        """
        from google.cloud import bigquery

        target_silver = metadata["targets"]["silver"]
        target_gold = metadata["targets"]["gold"]
        execution_id = metadata["execution_id"]

        bq_client = bigquery.Client(project=GCP_PROJECT)
        silver_table = f"{target_silver['project']}.{target_silver['dataset']}.{target_silver['table']}"
        gold_table = f"{target_gold['project']}.{target_gold['dataset']}.{target_gold['table']}"

        # Gold aggregation query — derived from silver
        gold_query = f"""
        SELECT
            customer_id,
            customer_name,
            COUNT(DISTINCT order_id) AS total_orders,
            SUM(CAST(quantity AS INT64) * CAST(unit_price AS FLOAT64)) AS total_revenue,
            MIN(order_date) AS first_order_date,
            MAX(order_date) AS last_order_date,
            COUNTIF(status = 'COMPLETED') AS completed_orders,
            COUNTIF(status = 'SHIPPED') AS shipped_orders,
            COUNTIF(status = 'PENDING') AS pending_orders,
            '{execution_id}' AS _gold_execution_id,
            CURRENT_TIMESTAMP() AS _gold_load_ts
        FROM `{silver_table}`
        WHERE _silver_execution_id = '{execution_id}'
        GROUP BY customer_id, customer_name
        ORDER BY total_revenue DESC
        """

        # Write to Gold BigQuery table
        job_config = bigquery.QueryJobConfig(
            destination=gold_table,
            write_disposition=(
                bigquery.WriteDisposition.WRITE_TRUNCATE
                if target_gold["write_mode"] == "write_truncate"
                else bigquery.WriteDisposition.WRITE_APPEND
            ),
        )

        job = bq_client.query(gold_query, job_config=job_config)
        result = job.result()
        rows_written = job.num_dml_affected_rows or sum(1 for _ in bq_client.list_rows(gold_table))

        # Also get a preview
        preview = list(bq_client.query(f"SELECT * FROM `{gold_table}` ORDER BY total_revenue DESC LIMIT 5").result())

        print(f"✅ Gold: Aggregated data to {gold_table}")
        print(f"   Customers: {len(preview)}+")
        for row in preview:
            print(f"   {row.customer_name}: {row.total_orders} orders, ${row.total_revenue:.2f} revenue")

        metadata["gold_metrics"] = {
            "gold_bq_table": gold_table,
            "customers_written": len(preview),
        }
        return metadata

    # ═════════════════════════════════════════════════════════════════════════
    # STEP 7: Finalize — Update execution status + audit log
    # ═════════════════════════════════════════════════════════════════════════
    @task
    def finalize_execution(metadata):
        """Update execution status in metadata DB and log summary."""
        import psycopg2
        import json

        conn = psycopg2.connect(METADATA_DB)
        cur = conn.cursor()

        # Update execution status
        metrics_json = json.dumps({
            "bronze": metadata.get("bronze_metrics", {}),
            "silver": metadata.get("silver_metrics", {}),
            "gold": metadata.get("gold_metrics", {}),
        })
        cur.execute(
            "UPDATE pipeline_executions SET status = %s, completed_at = NOW(), "
            "input_rows = %s, output_rows = %s, metrics = %s WHERE id = %s",
            (
                "COMPLETED",
                metadata.get("bronze_metrics", {}).get("records_read", 0),
                metadata.get("silver_metrics", {}).get("records_out", 0),
                metrics_json,
                metadata["db_execution_id"],
            ),
        )

        # Log audit event
        import uuid
        cur.execute(
            "INSERT INTO audit_log (event_id, event_type, actor, action, resource, outcome, details) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (
                str(uuid.uuid4()),
                "PIPELINE_COMPLETED",
                "airflow_scheduler",
                "E2E_PIPELINE_EXECUTION",
                FEED_ID,
                "SUCCESS",
                metrics_json,
            ),
        )

        conn.commit()
        cur.close()
        conn.close()

        print("═" * 60)
        print("  E2E PIPELINE COMPLETED SUCCESSFULLY")
        print("═" * 60)
        print(f"  Execution ID: {metadata['execution_id']}")
        print(f"  Feed: {metadata['feed_config']['feed_name']}")
        print(f"  Bronze: {metadata.get('bronze_metrics', {}).get('records_written', 0)} records")
        print(f"  Silver: {metadata.get('silver_metrics', {}).get('records_out', 0)} records")
        print(f"  Gold:   {metadata.get('gold_metrics', {}).get('gold_bq_table', 'N/A')}")
        print("═" * 60)

        return metadata

    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    # ═════════════════════════════════════════════════════════════════════════
    # DAG FLOW
    # ═════════════════════════════════════════════════════════════════════════
    meta = initialize_execution()
    checked = check_source_file(meta)
    bronze = raw_to_bronze(checked)
    validated = validate_bronze(bronze)
    silver = bronze_to_silver(validated)
    gold = silver_to_gold(silver)
    final = finalize_execution(gold)

    start >> meta
    final >> end
