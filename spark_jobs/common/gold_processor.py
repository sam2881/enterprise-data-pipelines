#!/usr/bin/env python3
"""
Common Gold Processor - BigQuery Loader

Loads Silver data to BigQuery Gold layer.
Configuration is read from PostgreSQL metadata.

Usage:
    spark-submit gold_processor.py \
        --pipeline-id <id> \
        --reporting-date <date> \
        --run-id <run_id>
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def get_bigquery_config(spark, pipeline_id: str) -> dict:
    """Read BigQuery configuration from metadata."""
    jdbc_url = spark.conf.get("spark.pipeline.metadata.jdbc.url")

    config_df = spark.read.jdbc(
        url=jdbc_url,
        table=f"(SELECT * FROM pipeline_metadata.bigquery_configuration WHERE pipeline_id = '{pipeline_id}' AND is_active = true) as config",
        properties={"driver": "org.postgresql.Driver"}
    )

    row = config_df.first()
    return row.asDict() if row else {}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pipeline-id", required=True)
    parser.add_argument("--reporting-date", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName(f"Gold_{args.pipeline_id}")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate()
    )

    try:
        # Read from Silver
        silver_table = f"silver.{args.pipeline_id.replace('-', '_')}"
        df = spark.table(silver_table).filter(F.col("reporting_date") == args.reporting_date)
        print(f"Read {df.count()} records from Silver")

        # Get BigQuery config
        bq_config = get_bigquery_config(spark, args.pipeline_id)
        if not bq_config:
            raise ValueError(f"BigQuery config not found for {args.pipeline_id}")

        bq_table = f"{bq_config['project_id']}.{bq_config['dataset_id']}.{bq_config['table_name']}"
        print(f"Loading to BigQuery: {bq_table}")

        # Drop internal columns
        columns_to_drop = ["run_id", "record_uuid", "ingestion_ts", "source_file"]
        df = df.drop(*[c for c in columns_to_drop if c in df.columns])

        # Write to BigQuery
        write_mode = "overwrite" if bq_config.get("load_strategy") == "full" else "append"

        (
            df.write
            .format("bigquery")
            .option("table", bq_table)
            .option("temporaryGcsBucket", spark.conf.get("spark.bigquery.temp.bucket"))
            .mode(write_mode)
            .save()
        )

        print(f"Loaded {df.count()} records to BigQuery")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
