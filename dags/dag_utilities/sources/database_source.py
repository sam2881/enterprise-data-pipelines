"""
APEX Data Agent - Database Source Utilities

JDBC connection management, watermark extraction, and CDC support.
"""

from typing import Any, Dict, Optional


def get_jdbc_hook(connection_id: str):
    """Get Airflow JDBC hook for database connections."""
    from airflow.providers.jdbc.hooks.jdbc import JdbcHook
    return JdbcHook(jdbc_conn_id=connection_id)


def check_jdbc_connectivity(connection_id: str, **context) -> bool:
    """Check JDBC source is reachable. Returns True if connected."""
    try:
        hook = get_jdbc_hook(connection_id)
        hook.get_conn().close()
        print(f"[database_source] Connection {connection_id} OK")
        return True
    except Exception as e:
        print(f"[database_source] Connection {connection_id} failed: {e}")
        return False


def get_watermark(metadata_db_url: str, feed_id: str, watermark_column: str, **context) -> Optional[str]:
    """Get last watermark value from metadata for incremental extraction."""
    import psycopg2

    try:
        conn = psycopg2.connect(metadata_db_url)
        cur = conn.cursor()
        cur.execute(
            "SELECT watermark_value FROM watermarks WHERE feed_id = %s AND column_name = %s",
            (feed_id, watermark_column),
        )
        row = cur.fetchone()
        conn.close()

        value = row[0] if row else None
        context["ti"].xcom_push(key="last_watermark", value=value)
        print(f"[database_source] Watermark for {feed_id}.{watermark_column}: {value}")
        return value
    except Exception as e:
        print(f"[database_source] Could not get watermark: {e}")
        return None


def update_watermark(metadata_db_url: str, feed_id: str, watermark_column: str, value: str, **context):
    """Update watermark value after successful extraction."""
    import psycopg2

    try:
        conn = psycopg2.connect(metadata_db_url)
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO watermarks (feed_id, column_name, watermark_value, updated_at)
               VALUES (%s, %s, %s, NOW())
               ON CONFLICT (feed_id, column_name) DO UPDATE SET
                 watermark_value = EXCLUDED.watermark_value,
                 updated_at = NOW()""",
            (feed_id, watermark_column, value),
        )
        conn.commit()
        conn.close()
        print(f"[database_source] Updated watermark {feed_id}.{watermark_column} = {value}")
    except Exception as e:
        print(f"[database_source] Could not update watermark: {e}")


def extract_incremental(
    connection_id: str,
    table: str,
    watermark_column: str,
    last_value: Optional[str] = None,
    target_path: str = "",
    **context,
) -> Dict[str, Any]:
    """Extract incremental data from JDBC source using watermark."""
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName(f"extract-{table}").getOrCreate()
    hook = get_jdbc_hook(connection_id)
    jdbc_url = hook.get_uri()

    query = f"SELECT * FROM {table}"
    if last_value:
        query += f" WHERE {watermark_column} > '{last_value}'"

    df = spark.read.jdbc(url=jdbc_url, table=f"({query}) AS src", properties={"driver": "org.postgresql.Driver"})
    row_count = df.count()

    if target_path and row_count > 0:
        df.write.mode("overwrite").parquet(target_path)

    # Get new watermark
    new_watermark = None
    if row_count > 0 and watermark_column:
        new_watermark = str(df.agg({watermark_column: "max"}).collect()[0][0])

    spark.stop()

    result = {"row_count": row_count, "new_watermark": new_watermark}
    context["ti"].xcom_push(key="extract_result", value=result)
    return result
