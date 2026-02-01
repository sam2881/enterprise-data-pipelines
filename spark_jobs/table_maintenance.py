"""
APEX Data Agent — Table Maintenance

Provides maintenance operations for open table formats:
- Delta Lake: VACUUM, OPTIMIZE, Z-ORDER
- Apache Iceberg: expire_snapshots, rewrite_data_files
- Small file compaction

Called as a post-Gold task in DAG templates to keep tables performant.
"""

import os
import json
import argparse
from datetime import datetime
from typing import Any, Dict, List, Optional

from pyspark.sql import SparkSession


# ═══════════════════════════════════════════════════════════════════════════
# DELTA LAKE MAINTENANCE
# ═══════════════════════════════════════════════════════════════════════════

def vacuum_delta_table(
    spark: SparkSession,
    table_path: str,
    retention_hours: int = 168,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    Run Delta Lake VACUUM to remove old files.

    Args:
        spark: SparkSession
        table_path: Path to Delta table (GCS or local)
        retention_hours: Keep files newer than this (default 7 days)
        dry_run: If True, only list files to delete

    Returns:
        Dict with deleted_files count and bytes_freed
    """
    from delta.tables import DeltaTable

    dt = DeltaTable.forPath(spark, table_path)

    # Disable retention check for custom retention
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

    if dry_run:
        result = dt.vacuum(retention_hours)
        return {"action": "VACUUM_DRY_RUN", "table_path": table_path, "retention_hours": retention_hours}
    else:
        dt.vacuum(retention_hours)
        return {"action": "VACUUM", "table_path": table_path, "retention_hours": retention_hours}


def optimize_delta_table(
    spark: SparkSession,
    table_path: str,
    z_order_columns: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Run Delta Lake OPTIMIZE to compact small files.

    Args:
        spark: SparkSession
        table_path: Path to Delta table
        z_order_columns: Columns to Z-ORDER by for query optimization

    Returns:
        Dict with optimization results
    """
    from delta.tables import DeltaTable

    dt = DeltaTable.forPath(spark, table_path)

    if z_order_columns:
        result = dt.optimize().zOrderBy(*z_order_columns).executeCompaction()
    else:
        result = dt.optimize().executeCompaction()

    # Extract metrics from result
    metrics = {}
    if result:
        try:
            metrics_row = result.collect()
            if metrics_row:
                metrics = metrics_row[0].asDict()
        except Exception:
            pass

    return {
        "action": "OPTIMIZE",
        "table_path": table_path,
        "z_order_columns": z_order_columns,
        "metrics": metrics,
    }


# ═══════════════════════════════════════════════════════════════════════════
# APACHE ICEBERG MAINTENANCE
# ═══════════════════════════════════════════════════════════════════════════

def expire_iceberg_snapshots(
    spark: SparkSession,
    table_name: str,
    older_than_days: int = 7,
) -> Dict[str, Any]:
    """
    Expire old Iceberg snapshots to free storage.

    Args:
        spark: SparkSession
        table_name: Fully qualified Iceberg table name
        older_than_days: Remove snapshots older than this

    Returns:
        Dict with expiration results
    """
    from datetime import timedelta

    cutoff = datetime.utcnow() - timedelta(days=older_than_days)
    cutoff_ms = int(cutoff.timestamp() * 1000)

    spark.sql(f"""
        CALL system.expire_snapshots(
            table => '{table_name}',
            older_than => TIMESTAMP '{cutoff.strftime("%Y-%m-%d %H:%M:%S")}'
        )
    """)

    return {
        "action": "EXPIRE_SNAPSHOTS",
        "table_name": table_name,
        "older_than_days": older_than_days,
        "cutoff": cutoff.isoformat(),
    }


def rewrite_iceberg_data_files(
    spark: SparkSession,
    table_name: str,
    target_file_size_mb: int = 512,
) -> Dict[str, Any]:
    """
    Rewrite Iceberg data files to target size (compaction).

    Args:
        spark: SparkSession
        table_name: Fully qualified Iceberg table name
        target_file_size_mb: Target file size in MB

    Returns:
        Dict with rewrite results
    """
    target_bytes = target_file_size_mb * 1024 * 1024

    spark.sql(f"""
        CALL system.rewrite_data_files(
            table => '{table_name}',
            options => map('target-file-size-bytes', '{target_bytes}')
        )
    """)

    return {
        "action": "REWRITE_DATA_FILES",
        "table_name": table_name,
        "target_file_size_mb": target_file_size_mb,
    }


# ═══════════════════════════════════════════════════════════════════════════
# GENERIC COMPACTION
# ═══════════════════════════════════════════════════════════════════════════

def compact_small_files(
    spark: SparkSession,
    source_path: str,
    target_path: Optional[str] = None,
    target_file_size_mb: int = 256,
    file_format: str = "parquet",
) -> Dict[str, Any]:
    """
    Compact small files by repartitioning and rewriting.

    Works with any format (Parquet, Avro, ORC).
    """
    target_path = target_path or source_path

    df = spark.read.format(file_format).load(source_path)
    total_bytes = sum(f.length for f in spark._jvm.org.apache.hadoop.fs.FileSystem
                      .get(spark._jsc.hadoopConfiguration())
                      .listStatus(spark._jvm.org.apache.hadoop.fs.Path(source_path)))

    target_bytes = target_file_size_mb * 1024 * 1024
    num_partitions = max(1, int(total_bytes / target_bytes))

    df.repartition(num_partitions).write.mode("overwrite").format(file_format).save(target_path)

    return {
        "action": "COMPACT",
        "source_path": source_path,
        "target_path": target_path,
        "original_bytes": total_bytes,
        "num_partitions": num_partitions,
        "file_format": file_format,
    }


# ═══════════════════════════════════════════════════════════════════════════
# MAINTENANCE ORCHESTRATOR
# ═══════════════════════════════════════════════════════════════════════════

def run_maintenance(
    spark: SparkSession,
    table_path: str,
    table_format: str = "delta",
    table_name: Optional[str] = None,
    vacuum_hours: int = 168,
    z_order_columns: Optional[List[str]] = None,
    expire_snapshot_days: int = 7,
) -> List[Dict[str, Any]]:
    """
    Run all applicable maintenance tasks for a table.

    Args:
        spark: SparkSession
        table_path: Path to table
        table_format: "delta", "iceberg", or "parquet"
        table_name: Iceberg table name (required for iceberg)
        vacuum_hours: Delta VACUUM retention
        z_order_columns: Delta Z-ORDER columns
        expire_snapshot_days: Iceberg snapshot expiration

    Returns:
        List of maintenance task results
    """
    results = []

    if table_format == "delta":
        results.append(vacuum_delta_table(spark, table_path, vacuum_hours))
        results.append(optimize_delta_table(spark, table_path, z_order_columns))

    elif table_format == "iceberg" and table_name:
        results.append(expire_iceberg_snapshots(spark, table_name, expire_snapshot_days))
        results.append(rewrite_iceberg_data_files(spark, table_name))

    elif table_format == "parquet":
        results.append(compact_small_files(spark, table_path))

    return results


# ═══════════════════════════════════════════════════════════════════════════
# CLI ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════

def main():
    """CLI entry point for table maintenance."""
    parser = argparse.ArgumentParser(description="APEX Table Maintenance")
    parser.add_argument("--table-path", required=True)
    parser.add_argument("--table-format", default="delta", choices=["delta", "iceberg", "parquet"])
    parser.add_argument("--table-name", help="Iceberg table name")
    parser.add_argument("--vacuum-hours", type=int, default=168)
    parser.add_argument("--z-order-columns", nargs="*")
    parser.add_argument("--expire-snapshot-days", type=int, default=7)
    parser.add_argument("--metadata-db-url", default=os.getenv("APEX_METADATA_DB_URL"))
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName(f"apex_table_maintenance_{args.table_format}") \
        .getOrCreate()

    try:
        results = run_maintenance(
            spark=spark,
            table_path=args.table_path,
            table_format=args.table_format,
            table_name=args.table_name,
            vacuum_hours=args.vacuum_hours,
            z_order_columns=args.z_order_columns,
            expire_snapshot_days=args.expire_snapshot_days,
        )
        for r in results:
            print(json.dumps(r, default=str))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
