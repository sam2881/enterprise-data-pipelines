"""
APEX Data Agent - Landing to Bronze Zone Spark Job (v2)

Transforms landing zone data (all STRING) into typed Bronze zone data.
Loads schema from PostgreSQL metadata DB, casts columns, adds audit columns.

Arguments (argparse):
    --feed-id           Feed identifier
    --contract-id       Data contract identifier
    --execution-id      Execution run ID
    --execution-date    Business date (YYYY-MM-DD)
    --metadata-db       PostgreSQL JDBC URL
    --landing-path      Landing zone Parquet path (GCS)
    --bronze-path       Bronze zone output path (GCS)
    --bq-table          BigQuery bronze table (project.dataset.table)
    --validate          Run row count validation (true/false)

Output:
    - Typed Parquet in bronze GCS path
    - BigQuery bronze table (optional)
    - Audit columns: _record_uuid, _reporting_date, _run_id, _load_timestamp
"""

import argparse
import json
import sys
from datetime import datetime
from uuid import uuid4

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import col, current_timestamp, lit, udf
from pyspark.sql.types import StringType, StructType, StructField


CAST_MAP = {
    "STRING": "string",
    "INTEGER": "int",
    "BIGINT": "long",
    "FLOAT": "float",
    "DOUBLE": "double",
    "DECIMAL": "decimal(38,10)",
    "BOOLEAN": "boolean",
    "DATE": "date",
    "TIMESTAMP": "timestamp",
    "BINARY": "binary",
}


def parse_args():
    parser = argparse.ArgumentParser(description="Landing to Bronze")
    parser.add_argument("--feed-id", required=True)
    parser.add_argument("--contract-id", required=True)
    parser.add_argument("--execution-id", required=True)
    parser.add_argument("--execution-date", required=True)
    parser.add_argument("--metadata-db", required=True)
    parser.add_argument("--landing-path", required=True)
    parser.add_argument("--bronze-path", required=True)
    parser.add_argument("--bq-table", default="")
    parser.add_argument("--validate", default="true")
    return parser.parse_args()


def get_spark(feed_id: str, execution_id: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(f"{feed_id}-landing-to-bronze-{execution_id}")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .getOrCreate()
    )


def load_schema_from_metadata(spark, metadata_db: str, feed_id: str):
    """Load column definitions from PostgreSQL metadata."""
    props = {"driver": "org.postgresql.Driver"}
    query = f"""(
        SELECT columns
        FROM schema_details
        WHERE feed_id = '{feed_id}'
        ORDER BY platform_schema_version DESC
        LIMIT 1
    ) AS schema_q"""

    try:
        df = spark.read.jdbc(url=metadata_db, table=query, properties=props)
        if df.count() > 0:
            cols_json = df.first()["columns"]
            if isinstance(cols_json, str):
                return json.loads(cols_json)
            return cols_json
    except Exception as e:
        print(f"[landing_to_bronze] Warning: Could not load schema from metadata: {e}")

    return None


def cast_columns(df, schema_cols):
    """Cast STRING columns to target types from metadata schema."""
    for col_def in schema_cols:
        col_name = col_def["name"]
        col_type = col_def.get("type", "STRING").upper()
        spark_type = CAST_MAP.get(col_type, "string")

        if col_name in df.columns:
            try:
                df = df.withColumn(col_name, col(col_name).cast(spark_type))
            except Exception as e:
                print(f"[landing_to_bronze] Warning: Cast failed for {col_name} to {spark_type}: {e}")

    return df


def add_audit_columns(df, execution_id: str, execution_date: str):
    """Add bronze zone audit columns."""
    generate_uuid = udf(lambda: str(uuid4()), StringType())

    df = df.withColumn("_record_uuid", generate_uuid())
    df = df.withColumn("_reporting_date", lit(execution_date).cast("date"))
    df = df.withColumn("_run_id", lit(execution_id))
    df = df.withColumn("_load_timestamp", current_timestamp())

    return df


def validate_row_counts(landing_count: int, bronze_count: int):
    """Validate no data loss between landing and bronze."""
    if bronze_count < landing_count:
        loss_pct = ((landing_count - bronze_count) / landing_count) * 100
        if loss_pct > 5:
            raise RuntimeError(
                f"Data loss detected: landing={landing_count}, bronze={bronze_count} "
                f"(loss={loss_pct:.1f}%)"
            )
        print(f"[landing_to_bronze] Warning: Minor row difference: landing={landing_count}, bronze={bronze_count}")


def main():
    args = parse_args()
    spark = get_spark(args.feed_id, args.execution_id)

    print(f"[landing_to_bronze] Feed: {args.feed_id}, Execution: {args.execution_id}")
    print(f"[landing_to_bronze] {args.landing_path} → {args.bronze_path}")

    try:
        # Read landing data
        df = spark.read.parquet(args.landing_path)
        landing_count = df.count()
        print(f"[landing_to_bronze] Landing rows: {landing_count}")

        if landing_count == 0:
            print("[landing_to_bronze] No data in landing zone. Skipping.")
            spark.stop()
            return

        # Load schema from metadata and cast types
        schema_cols = load_schema_from_metadata(spark, args.metadata_db, args.feed_id)
        if schema_cols:
            # Drop landing audit columns before casting
            data_cols = [c for c in df.columns if not c.startswith("_")]
            df = df.select(*data_cols)
            df = cast_columns(df, schema_cols)
            print(f"[landing_to_bronze] Schema applied: {len(schema_cols)} columns typed")
        else:
            print("[landing_to_bronze] No schema metadata found. Keeping STRING types.")

        # Add audit columns
        df = add_audit_columns(df, args.execution_id, args.execution_date)

        # Write to bronze GCS (idempotent: overwrite partition)
        (
            df.write
            .mode("overwrite")
            .partitionBy("_reporting_date")
            .parquet(args.bronze_path)
        )

        bronze_count = df.count()
        print(f"[landing_to_bronze] Bronze rows: {bronze_count}")

        # Validate
        if args.validate.lower() == "true":
            validate_row_counts(landing_count, bronze_count)

        # Write to BigQuery if configured
        if args.bq_table:
            print(f"[landing_to_bronze] Writing to BigQuery: {args.bq_table}")
            (
                df.write
                .format("bigquery")
                .option("table", args.bq_table)
                .option("temporaryGcsBucket", f"{args.bronze_path.split('/')[2]}-temp")
                .option("createDisposition", "CREATE_IF_NEEDED")
                .option("writeDisposition", "WRITE_APPEND")
                .mode("append")
                .save()
            )

        # Output metrics
        metrics = {
            "feed_id": args.feed_id,
            "execution_id": args.execution_id,
            "landing_count": landing_count,
            "bronze_count": bronze_count,
            "columns_typed": len(schema_cols) if schema_cols else 0,
            "bronze_path": args.bronze_path,
            "status": "SUCCESS",
            "timestamp": datetime.utcnow().isoformat(),
        }
        print(f"[landing_to_bronze] METRICS: {json.dumps(metrics)}")

    except Exception as e:
        print(f"[landing_to_bronze] ERROR: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
