#!/usr/bin/env python3
"""
Common Bronze Processor

Reads source data and loads to Bronze layer (ALL columns as STRING).
Configuration is read from PostgreSQL metadata - NO HARD-CODED SCHEMA.

Usage:
    spark-submit bronze_processor.py \
        --pipeline-id <id> \
        --reporting-date <date> \
        --run-id <run_id> \
        --data-path gs://bucket/data/ \
        --metadata-path gs://bucket/metadata/
"""

import argparse
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


def get_pipeline_config(spark, pipeline_id: str) -> dict:
    """Read pipeline configuration from PostgreSQL metadata."""
    jdbc_url = spark.conf.get("spark.pipeline.metadata.jdbc.url")

    config_df = spark.read.jdbc(
        url=jdbc_url,
        table=f"(SELECT * FROM pipeline_metadata.pipeline_registry WHERE pipeline_id = '{pipeline_id}') as config",
        properties={"driver": "org.postgresql.Driver"}
    )

    return config_df.first().asDict()


def get_schema_definitions(spark, pipeline_id: str) -> list:
    """Read schema definitions from PostgreSQL metadata."""
    jdbc_url = spark.conf.get("spark.pipeline.metadata.jdbc.url")

    schema_df = spark.read.jdbc(
        url=jdbc_url,
        table=f"(SELECT * FROM pipeline_metadata.schema_definitions WHERE pipeline_id = '{pipeline_id}' AND is_active = true ORDER BY column_order) as schema",
        properties={"driver": "org.postgresql.Driver"}
    )

    return [row.asDict() for row in schema_df.collect()]


def read_source_data(spark, config: dict, data_path: str):
    """Read source data based on source_type from metadata."""
    source_type = config.get("source_type", "csv")

    if source_type == "excel":
        df = (
            spark.read
            .format("com.crealytics.spark.excel")
            .option("header", "true")
            .option("inferSchema", "false")
            .load(data_path)
        )
    elif source_type == "csv":
        df = (
            spark.read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "false")
            .load(data_path)
        )
    elif source_type == "json":
        df = (
            spark.read
            .format("json")
            .option("primitivesAsString", "true")
            .load(data_path)
        )
    elif source_type == "parquet":
        df = spark.read.parquet(data_path)
    else:
        df = spark.read.format("csv").option("header", "true").load(data_path)

    # Cast ALL columns to STRING (Bronze layer requirement)
    for col_name in df.columns:
        df = df.withColumn(col_name, F.col(col_name).cast(StringType()))

    return df


def add_system_columns(df, reporting_date: str, run_id: str, source_system: str, source_file: str):
    """Add standard system columns."""
    return (
        df
        .withColumn("reporting_date", F.lit(reporting_date))
        .withColumn("run_id", F.lit(run_id))
        .withColumn("record_uuid", F.expr("uuid()"))
        .withColumn("ingestion_ts", F.current_timestamp().cast(StringType()))
        .withColumn("source_system", F.lit(source_system))
        .withColumn("source_file", F.lit(source_file))
    )


def write_to_bronze(df, pipeline_id: str, reporting_date: str):
    """Write to Bronze layer (Iceberg)."""
    bronze_table = f"bronze.{pipeline_id.replace('-', '_')}"

    (
        df.write
        .format("iceberg")
        .mode("append")
        .partitionBy("reporting_date")
        .saveAsTable(bronze_table)
    )

    return df.count()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pipeline-id", required=True)
    parser.add_argument("--reporting-date", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--data-path", required=True)
    parser.add_argument("--metadata-path", required=True)
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName(f"Bronze_{args.pipeline_id}")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate()
    )

    try:
        # Read config from metadata
        config = get_pipeline_config(spark, args.pipeline_id)
        print(f"Pipeline Config: {json.dumps(config, default=str)}")

        # Read source data
        df = read_source_data(spark, config, args.data_path)
        print(f"Read {df.count()} records from source")

        # Add system columns
        df = add_system_columns(
            df,
            args.reporting_date,
            args.run_id,
            config.get("source_system", "unknown"),
            args.data_path
        )

        # Write to Bronze
        count = write_to_bronze(df, args.pipeline_id, args.reporting_date)
        print(f"Wrote {count} records to Bronze layer")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
