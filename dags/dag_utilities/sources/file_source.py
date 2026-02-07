"""
APEX Data Agent - File Source Utilities

GCS file sensing, listing, and schema detection for file-based sources.
"""

import os
from typing import Any, Dict, List, Optional


def get_gcs_file_sensor(
    task_id: str,
    bucket: str,
    prefix: str,
    timeout: int = 3600,
    poke_interval: int = 300,
    mode: str = "reschedule",
    soft_fail: bool = False,
):
    """Create a GCS file existence sensor."""
    from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
    return GCSObjectsWithPrefixExistenceSensor(
        task_id=task_id,
        bucket=bucket,
        prefix=prefix,
        timeout=timeout,
        poke_interval=poke_interval,
        mode=mode,
        soft_fail=soft_fail,
    )


def list_source_files(source_config: Dict[str, Any], file_format: str = "csv", **context) -> List[str]:
    """
    List files matching the source configuration pattern.
    Pushes file list to XCom.
    """
    from google.cloud import storage

    bucket_name = source_config.get("bucket", "")
    prefix = source_config.get("prefix", "")
    pattern = source_config.get("pattern", f"*.{file_format}")

    # Resolve date templates
    ds = context.get("ds", "")
    if ds:
        prefix = prefix.replace("{date}", ds).replace("{ds}", ds)

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)

    files = []
    for blob in blobs:
        if blob.name.endswith(f".{file_format}") or pattern == "*":
            files.append(f"gs://{bucket_name}/{blob.name}")

    context["ti"].xcom_push(key="source_files", value=files)
    context["ti"].xcom_push(key="source_file_count", value=len(files))

    print(f"[file_source] Found {len(files)} files in gs://{bucket_name}/{prefix}")
    return files


def detect_schema(file_path: str, file_format: str = "csv", sample_rows: int = 100) -> Dict[str, str]:
    """
    Auto-detect schema from a sample file.
    Returns dict of column_name → inferred_type.
    """
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("schema-detect").getOrCreate()

    if file_format == "csv":
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
    elif file_format == "json":
        df = spark.read.option("multiline", "true").json(file_path)
    elif file_format == "parquet":
        df = spark.read.parquet(file_path)
    else:
        df = spark.read.text(file_path)

    schema = {field.name: field.dataType.simpleString() for field in df.schema.fields}
    spark.stop()
    return schema
