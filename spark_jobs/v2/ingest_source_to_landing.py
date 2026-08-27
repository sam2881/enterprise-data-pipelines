"""
APEX Data Agent - Source to Landing Zone Spark Job (v2)

Ingests data from any source type into the landing zone as raw STRING columns.
No schema enforcement at this stage — all columns stored as STRING in Parquet.

Supports: GCS files (CSV/JSON/Parquet/Avro/EBCDIC/Fixed-Width), JDBC, API staged files.

Arguments (argparse):
    --feed-id           Feed identifier
    --batch-id          Batch/execution identifier
    --source-type       gcs_file | jdbc | api | streaming | legacy_ebcdic
    --source-path       Source file path or JDBC URL
    --file-format       csv | json | parquet | avro | ebcdic | fixed_width | orc | xml
    --target-path       Landing zone GCS path
    --metadata-db       PostgreSQL JDBC URL
    --delimiter         Column delimiter (default: ,)
    --header-rows       Number of header rows to skip (default: 0)
    --footer-rows       Number of footer rows to skip (default: 0)
    --encoding          File encoding (default: UTF-8)
    --copybook-path     COBOL copybook path (for EBCDIC)
    --connection-id     Airflow connection ID (for JDBC/API)
    --watermark-column  Watermark column for incremental (optional)
    --watermark-value   Last watermark value (optional)

Output:
    - Parquet files in landing zone with all columns as STRING
    - Audit columns: _feed_id, _batch_id, _load_timestamp, _source_file
    - Metrics written to XCom-compatible output
"""

import argparse
import json
import os
import sys
from datetime import datetime

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as f
from pyspark.sql.functions import (
    col,
    current_timestamp,
    input_file_name,
    lit,
    monotonically_increasing_id,
    row_number,
)
from pyspark.sql.types import StringType


def parse_args():
    parser = argparse.ArgumentParser(description="Source to Landing Zone")
    parser.add_argument("--feed-id", required=True)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--source-type", required=True)
    parser.add_argument("--source-path", required=True)
    parser.add_argument("--file-format", default="csv")
    parser.add_argument("--target-path", required=True)
    parser.add_argument("--metadata-db", default="")
    parser.add_argument("--delimiter", default=",")
    parser.add_argument("--header-rows", type=int, default=0)
    parser.add_argument("--footer-rows", type=int, default=0)
    parser.add_argument("--encoding", default="UTF-8")
    parser.add_argument("--copybook-path", default="")
    parser.add_argument("--connection-id", default="")
    parser.add_argument("--watermark-column", default="")
    parser.add_argument("--watermark-value", default="")
    return parser.parse_args()


def get_spark(feed_id: str, batch_id: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(f"{feed_id}-source-to-landing-{batch_id}")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .getOrCreate()
    )


def read_gcs_file(spark, args):
    """Read file from GCS based on format."""
    path = args.source_path
    fmt = args.file_format.lower()

    if fmt == "csv":
        df = (
            spark.read
            .option("header", str(args.header_rows > 0).lower())
            .option("delimiter", args.delimiter)
            .option("quote", '"')
            .option("escape", '"')
            .option("multiline", "true")
            .option("encoding", args.encoding)
            .option("ignoreLeadingWhiteSpace", "true")
            .option("ignoreTrailingWhiteSpace", "true")
            .csv(path)
        )

    elif fmt in ("fixed_width", "delimiter"):
        df = (
            spark.read
            .option("header", "false")
            .text(path)
            .toDF("raw_line")
        )

    elif fmt == "json":
        df = (
            spark.read
            .option("multiline", "true")
            .json(path)
        )

    elif fmt == "parquet":
        df = spark.read.parquet(path)

    elif fmt == "avro":
        df = spark.read.format("avro").load(path)

    elif fmt == "orc":
        df = spark.read.orc(path)

    elif fmt == "xml":
        df = spark.read.format("xml").option("rowTag", "row").load(path)

    elif fmt == "ebcdic":
        if args.copybook_path:
            # Cobrix integration
            df = (
                spark.read
                .format("cobol")
                .option("copybook", args.copybook_path)
                .option("encoding", args.encoding)
                .option("is_text", "false")
                .load(path)
            )
        else:
            # Fallback: read as pipe-delimited CSV (post-Cobrix conversion)
            df = (
                spark.read
                .option("header", "true")
                .option("delimiter", "|")
                .option("multiLine", "true")
                .csv(path)
            )

    else:
        raise ValueError(f"Unsupported file format: {fmt}")

    return df


def read_jdbc(spark, args):
    """Read from JDBC source."""
    props = {}
    if args.metadata_db:
        # Load connection details from metadata DB
        conn_df = spark.read.jdbc(
            url=args.metadata_db,
            table=f"(SELECT connection_uri, username, password FROM connections WHERE connection_id = '{args.connection_id}') AS conn",
            properties={"driver": "org.postgresql.Driver"},
        )
        if conn_df.count() > 0:
            row = conn_df.first()
            props = {
                "url": row["connection_uri"],
                "user": row["username"],
                "password": row["password"],
                "driver": "org.postgresql.Driver",
            }

    if not props:
        props = {
            "url": args.source_path,
            "driver": "org.postgresql.Driver",
        }

    query = f"(SELECT * FROM {args.source_path.split('/')[-1]}) AS src"
    if args.watermark_column and args.watermark_value:
        table = args.source_path.split("/")[-1]
        query = f"(SELECT * FROM {table} WHERE {args.watermark_column} > '{args.watermark_value}') AS src"

    return spark.read.jdbc(url=props.get("url", args.source_path), table=query, properties=props)


def read_api_staged(spark, args):
    """Read pre-staged API response files (JSON)."""
    return spark.read.option("multiline", "true").json(args.source_path)


def handle_header_footer(df, args):
    """Skip header and footer rows if configured."""
    if args.header_rows > 0 or args.footer_rows > 0:
        df = df.withColumn("_row_idx", row_number().over(Window.orderBy(monotonically_increasing_id())))
        total = df.count()

        if args.header_rows > 0:
            df = df.filter(col("_row_idx") > args.header_rows)

        if args.footer_rows > 0:
            df = df.filter(col("_row_idx") <= total - args.footer_rows)

        df = df.drop("_row_idx")

    return df


def cast_all_to_string(df):
    """Cast all columns to STRING for landing zone (no schema enforcement)."""
    for c in df.columns:
        if c.startswith("_"):
            continue
        df = df.withColumn(c, col(c).cast(StringType()))
    return df


def add_audit_columns(df, feed_id: str, batch_id: str):
    """Add standard audit columns."""
    df = df.withColumn("_feed_id", lit(feed_id))
    df = df.withColumn("_batch_id", lit(batch_id))
    df = df.withColumn("_load_timestamp", current_timestamp())

    # Try to add source file name (works for file-based sources)
    try:
        df = df.withColumn("_source_file", input_file_name())
    except Exception:
        df = df.withColumn("_source_file", lit("unknown"))

    return df


def main():
    args = parse_args()
    spark = get_spark(args.feed_id, args.batch_id)

    print(f"[source_to_landing] Feed: {args.feed_id}, Source: {args.source_type}, Format: {args.file_format}")
    print(f"[source_to_landing] Path: {args.source_path} → {args.target_path}")

    try:
        # Read based on source type
        if args.source_type == "gcs_file":
            df = read_gcs_file(spark, args)
        elif args.source_type == "jdbc":
            df = read_jdbc(spark, args)
        elif args.source_type == "api":
            df = read_api_staged(spark, args)
        elif args.source_type == "legacy_ebcdic":
            args.file_format = "ebcdic"
            df = read_gcs_file(spark, args)
        elif args.source_type == "streaming":
            # Streaming data is pre-staged as Parquet by the consumer
            df = spark.read.parquet(args.source_path)
        else:
            raise ValueError(f"Unsupported source_type: {args.source_type}")

        # Handle header/footer
        df = handle_header_footer(df, args)

        # Cast all to STRING
        df = cast_all_to_string(df)

        # Add audit columns
        df = add_audit_columns(df, args.feed_id, args.batch_id)

        # Count
        row_count = df.count()
        col_count = len([c for c in df.columns if not c.startswith("_")])

        print(f"[source_to_landing] Rows: {row_count}, Columns: {col_count}")

        # Write to landing zone as Parquet (overwrite for idempotency)
        (
            df.write
            .mode("overwrite")
            .parquet(args.target_path)
        )

        # Output metrics
        metrics = {
            "feed_id": args.feed_id,
            "batch_id": args.batch_id,
            "source_type": args.source_type,
            "file_format": args.file_format,
            "row_count": row_count,
            "column_count": col_count,
            "target_path": args.target_path,
            "status": "SUCCESS",
            "timestamp": datetime.utcnow().isoformat(),
        }
        print(f"[source_to_landing] METRICS: {json.dumps(metrics)}")

    except Exception as e:
        print(f"[source_to_landing] ERROR: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
