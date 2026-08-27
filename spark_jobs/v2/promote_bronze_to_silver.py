"""
APEX Data Agent - Bronze to Silver Zone Spark Job (v2)

Transforms bronze data into cleaned, deduplicated silver zone data.
Loads transform rules from PostgreSQL metadata.

Operations:
    - Deduplication on business_keys (keep latest by watermark)
    - Null handling (fill, drop, default)
    - String trimming and standardization
    - Date standardization
    - Apply silver transforms from metadata

Arguments (argparse):
    --feed-id           Feed identifier
    --contract-id       Data contract identifier
    --execution-id      Execution run ID
    --execution-date    Business date (YYYY-MM-DD)
    --metadata-db       PostgreSQL JDBC URL
    --bronze-path       Bronze zone Parquet path (GCS)
    --silver-path       Silver zone output path (GCS)
    --bq-table          BigQuery silver table (project.dataset.table)
    --business-keys     Comma-separated business key columns
    --watermark-column  Column for ordering in dedup (optional)
    --transforms-json   JSON array of transform rules (optional)
"""

import argparse
import json
import sys
from datetime import datetime

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as f
from pyspark.sql.functions import (
    col,
    coalesce,
    current_timestamp,
    lit,
    lower,
    regexp_replace,
    row_number,
    trim,
    when,
)


def parse_args():
    parser = argparse.ArgumentParser(description="Bronze to Silver")
    parser.add_argument("--feed-id", required=True)
    parser.add_argument("--contract-id", required=True)
    parser.add_argument("--execution-id", required=True)
    parser.add_argument("--execution-date", required=True)
    parser.add_argument("--metadata-db", required=True)
    parser.add_argument("--bronze-path", required=True)
    parser.add_argument("--silver-path", required=True)
    parser.add_argument("--bq-table", default="")
    parser.add_argument("--business-keys", default="")
    parser.add_argument("--watermark-column", default="")
    parser.add_argument("--transforms-json", default="[]")
    return parser.parse_args()


def get_spark(feed_id: str, execution_id: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(f"{feed_id}-bronze-to-silver-{execution_id}")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .getOrCreate()
    )


def load_transforms_from_metadata(spark, metadata_db: str, feed_id: str):
    """Load silver transform rules from PostgreSQL metadata."""
    props = {"driver": "org.postgresql.Driver"}
    query = f"""(
        SELECT zone_transforms
        FROM feed_configs
        WHERE feed_id = '{feed_id}'
    ) AS transforms_q"""

    try:
        df = spark.read.jdbc(url=metadata_db, table=query, properties=props)
        if df.count() > 0:
            transforms_json = df.first()["zone_transforms"]
            if isinstance(transforms_json, str):
                all_transforms = json.loads(transforms_json)
                return all_transforms.get("silver", [])
    except Exception as e:
        print(f"[bronze_to_silver] Warning: Could not load transforms from metadata: {e}")

    return []


def deduplicate(df, business_keys, watermark_column):
    """Deduplicate on business keys, keeping the latest record by watermark."""
    if not business_keys:
        return df

    order_col = watermark_column if watermark_column else "_load_timestamp"

    window = Window.partitionBy(*business_keys).orderBy(col(order_col).desc())
    df = df.withColumn("_dedup_rank", row_number().over(window))
    df = df.filter(col("_dedup_rank") == 1).drop("_dedup_rank")

    return df


def apply_transforms(df, transforms):
    """Apply transform rules from metadata."""
    for rule in transforms:
        rule_type = rule.get("type", "")
        config = rule.get("config", {})

        if rule_type == "rename":
            old_name = config.get("from")
            new_name = config.get("to")
            if old_name and new_name and old_name in df.columns:
                df = df.withColumnRenamed(old_name, new_name)

        elif rule_type == "cast":
            column = config.get("column")
            target_type = config.get("type", "string")
            if column and column in df.columns:
                df = df.withColumn(column, col(column).cast(target_type))

        elif rule_type == "derive":
            new_col = config.get("name")
            expression = config.get("expression")
            if new_col and expression:
                df = df.withColumn(new_col, f.expr(expression))

        elif rule_type == "filter":
            condition = config.get("condition")
            if condition:
                df = df.filter(f.expr(condition))

        elif rule_type == "null_fill":
            column = config.get("column")
            value = config.get("value", "")
            if column and column in df.columns:
                df = df.withColumn(column, coalesce(col(column), lit(value)))

        elif rule_type == "trim":
            columns = config.get("columns", [])
            for c in columns:
                if c in df.columns:
                    df = df.withColumn(c, trim(col(c)))

        elif rule_type == "standardize_date":
            column = config.get("column")
            input_format = config.get("input_format", "yyyy-MM-dd")
            if column and column in df.columns:
                df = df.withColumn(column, f.to_date(col(column), input_format))

        elif rule_type == "regex_replace":
            column = config.get("column")
            pattern = config.get("pattern", "")
            replacement = config.get("replacement", "")
            if column and column in df.columns:
                df = df.withColumn(column, regexp_replace(col(column), pattern, replacement))

        elif rule_type == "mask":
            column = config.get("column")
            mask_char = config.get("char", "*")
            keep_last = config.get("keep_last", 4)
            if column and column in df.columns:
                df = df.withColumn(
                    column,
                    when(
                        f.length(col(column)) > keep_last,
                        f.concat(
                            f.lpad(lit(""), f.length(col(column)) - keep_last, mask_char),
                            f.substring(col(column), f.length(col(column)) - keep_last + 1, keep_last),
                        ),
                    ).otherwise(col(column)),
                )

        elif rule_type == "hash":
            column = config.get("column")
            algorithm = config.get("algorithm", "sha2")
            if column and column in df.columns:
                if algorithm == "md5":
                    df = df.withColumn(column, f.md5(col(column).cast("string")))
                else:
                    df = df.withColumn(column, f.sha2(col(column).cast("string"), 256))

    return df


def standard_cleaning(df):
    """Apply standard data cleaning operations."""
    string_cols = [c.name for c in df.schema.fields if c.dataType.simpleString() == "string" and not c.name.startswith("_")]

    for c in string_cols:
        # Trim whitespace
        df = df.withColumn(c, trim(col(c)))
        # Replace empty strings with null
        df = df.withColumn(c, when(col(c) == "", None).otherwise(col(c)))

    return df


def main():
    args = parse_args()
    spark = get_spark(args.feed_id, args.execution_id)

    print(f"[bronze_to_silver] Feed: {args.feed_id}, Execution: {args.execution_id}")
    print(f"[bronze_to_silver] {args.bronze_path} → {args.silver_path}")

    try:
        # Read bronze data
        df = spark.read.parquet(args.bronze_path)
        bronze_count = df.count()
        print(f"[bronze_to_silver] Bronze rows: {bronze_count}")

        if bronze_count == 0:
            print("[bronze_to_silver] No data in bronze zone. Skipping.")
            spark.stop()
            return

        # Standard cleaning
        df = standard_cleaning(df)

        # Deduplicate
        business_keys = [k.strip() for k in args.business_keys.split(",") if k.strip()]
        if business_keys:
            before_dedup = df.count()
            df = deduplicate(df, business_keys, args.watermark_column)
            after_dedup = df.count()
            print(f"[bronze_to_silver] Dedup: {before_dedup} → {after_dedup} ({before_dedup - after_dedup} duplicates removed)")

        # Load and apply transforms
        transforms = json.loads(args.transforms_json) if args.transforms_json != "[]" else []
        if not transforms:
            transforms = load_transforms_from_metadata(spark, args.metadata_db, args.feed_id)

        if transforms:
            df = apply_transforms(df, transforms)
            print(f"[bronze_to_silver] Applied {len(transforms)} transform rules")

        # Update audit columns
        df = df.withColumn("_load_timestamp", current_timestamp())

        silver_count = df.count()

        # Write to silver GCS
        (
            df.write
            .mode("overwrite")
            .partitionBy("_reporting_date")
            .parquet(args.silver_path)
        )

        # Write to BigQuery if configured
        if args.bq_table:
            print(f"[bronze_to_silver] Writing to BigQuery: {args.bq_table}")
            (
                df.write
                .format("bigquery")
                .option("table", args.bq_table)
                .option("temporaryGcsBucket", f"{args.silver_path.split('/')[2]}-temp")
                .option("writeDisposition", "WRITE_APPEND")
                .mode("append")
                .save()
            )

        # Output metrics
        metrics = {
            "feed_id": args.feed_id,
            "execution_id": args.execution_id,
            "bronze_count": bronze_count,
            "silver_count": silver_count,
            "duplicates_removed": bronze_count - silver_count,
            "transforms_applied": len(transforms),
            "silver_path": args.silver_path,
            "status": "SUCCESS",
            "timestamp": datetime.utcnow().isoformat(),
        }
        print(f"[bronze_to_silver] METRICS: {json.dumps(metrics)}")

    except Exception as e:
        print(f"[bronze_to_silver] ERROR: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
