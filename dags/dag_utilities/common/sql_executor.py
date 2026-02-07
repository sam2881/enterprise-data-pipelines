"""
APEX Data Agent - SQL Executor Utilities

Execute SQL scripts against BigQuery and Snowflake.
Loads SQL templates from deployed scripts directory.
"""

import os
from typing import Any, Dict, Optional


def run_bq_query(
    sql_path: str,
    params: Optional[Dict[str, str]] = None,
    project: Optional[str] = None,
    **context,
) -> Dict[str, Any]:
    """
    Execute BigQuery SQL from a deployed script file.
    Supports Jinja-style parameter substitution (@param_name).
    """
    params = params or {}
    project = project or os.getenv("GCP_PROJECT", "")

    # Resolve execution context params
    ti = context.get("ti")
    ds = context.get("ds", "")
    execution_id = ti.xcom_pull(key="execution_id") if ti else ""

    params.setdefault("execution_date", ds)
    params.setdefault("run_id", execution_id)

    # Load SQL template
    sql = load_sql_template(sql_path)

    # Substitute parameters
    for key, value in params.items():
        sql = sql.replace(f"@{key}", f"'{value}'" if isinstance(value, str) else str(value))

    print(f"[sql_executor] Executing BigQuery SQL: {sql_path}")

    try:
        from google.cloud import bigquery

        client = bigquery.Client(project=project)
        query_job = client.query(sql)
        result = query_job.result()

        rows_affected = query_job.num_dml_affected_rows or 0
        print(f"[sql_executor] Rows affected: {rows_affected}")

        return {
            "sql_path": sql_path,
            "rows_affected": rows_affected,
            "status": "SUCCESS",
        }
    except ImportError:
        print("[sql_executor] google-cloud-bigquery not available. SQL not executed.")
        return {"sql_path": sql_path, "status": "SKIPPED", "reason": "bigquery not installed"}
    except Exception as e:
        print(f"[sql_executor] BigQuery error: {e}")
        return {"sql_path": sql_path, "status": "FAILED", "error": str(e)}


def run_snowflake_query(
    sql_path: str,
    params: Optional[Dict[str, str]] = None,
    connection_id: str = "snowflake_default",
    **context,
) -> Dict[str, Any]:
    """Execute SQL against Snowflake (optional target)."""
    params = params or {}
    sql = load_sql_template(sql_path)

    for key, value in params.items():
        sql = sql.replace(f"@{key}", f"'{value}'" if isinstance(value, str) else str(value))

    print(f"[sql_executor] Executing Snowflake SQL: {sql_path}")

    try:
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        hook = SnowflakeHook(snowflake_conn_id=connection_id)
        result = hook.run(sql)

        return {"sql_path": sql_path, "status": "SUCCESS"}
    except ImportError:
        print("[sql_executor] Snowflake provider not available. SQL not executed.")
        return {"sql_path": sql_path, "status": "SKIPPED", "reason": "snowflake not installed"}
    except Exception as e:
        print(f"[sql_executor] Snowflake error: {e}")
        return {"sql_path": sql_path, "status": "FAILED", "error": str(e)}


def load_sql_template(sql_path: str) -> str:
    """
    Load SQL template from deployed scripts directory.
    Searches in: /opt/airflow/dags/scripts/, then relative path.
    """
    search_paths = [
        os.path.join("/opt/airflow/dags/scripts", sql_path),
        os.path.join("/opt/airflow/dags", sql_path),
        sql_path,
    ]

    for path in search_paths:
        if os.path.exists(path):
            with open(path, "r") as f:
                return f.read()

    # Try GCS
    if sql_path.startswith("gs://"):
        try:
            from google.cloud import storage

            bucket_name = sql_path.replace("gs://", "").split("/")[0]
            blob_path = "/".join(sql_path.replace("gs://", "").split("/")[1:])
            client = storage.Client()
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(blob_path)
            return blob.download_as_text()
        except Exception as e:
            print(f"[sql_executor] Could not load from GCS: {e}")

    raise FileNotFoundError(f"SQL template not found: {sql_path}")


def run_pg_query(
    sql: str,
    connection_string: str,
    params: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """Execute SQL against PostgreSQL metadata database."""
    import psycopg2

    params = params or {}
    for key, value in params.items():
        sql = sql.replace(f"@{key}", f"'{value}'" if isinstance(value, str) else str(value))

    try:
        conn = psycopg2.connect(connection_string)
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()

        rows_affected = cur.rowcount
        conn.close()

        return {"rows_affected": rows_affected, "status": "SUCCESS"}
    except Exception as e:
        print(f"[sql_executor] PostgreSQL error: {e}")
        return {"status": "FAILED", "error": str(e)}
