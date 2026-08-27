"""
APEX Data Agent - Data Vault 2.0 Satellite Loader Spark Job (v2)

Loads descriptive attributes into a Data Vault Satellite table.
Detects changes via hash_diff comparison and applies insert/close pattern.

Arguments (argparse):
    --feed-id               Feed identifier
    --execution-id          Execution run ID
    --metadata-db           PostgreSQL JDBC URL
    --source-path           Silver zone Parquet path (GCS)
    --satellite-name        Satellite entity name (e.g., sat_customer_details)
    --hub-hash-key-col      Column name containing the hub hash key
    --descriptive-columns   Comma-separated descriptive attribute columns
    --target-table          BigQuery target table (project.dataset.table)
    --bq-project            BigQuery project ID
"""

import argparse
import json
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import (
    col,
    concat_ws,
    current_timestamp,
    lit,
    md5,
    when,
)


def parse_args():
    parser = argparse.ArgumentParser(description="Data Vault 2.0 Satellite Loader")
    parser.add_argument("--feed-id", required=True)
    parser.add_argument("--execution-id", required=True)
    parser.add_argument("--metadata-db", required=True)
    parser.add_argument("--source-path", required=True)
    parser.add_argument("--satellite-name", required=True)
    parser.add_argument("--hub-hash-key-col", required=True)
    parser.add_argument("--descriptive-columns", required=True)
    parser.add_argument("--target-table", required=True)
    parser.add_argument("--bq-project", default="")
    return parser.parse_args()


def get_spark(feed_id: str, execution_id: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(f"{feed_id}-load-satellite-{execution_id}")
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .getOrCreate()
    )


def load_satellite_metadata(spark, metadata_db: str, satellite_name: str):
    """Load satellite configuration from PostgreSQL metadata."""
    props = {"driver": "org.postgresql.Driver"}
    query = f"""(
        SELECT satellite_config
        FROM data_vault_satellites
        WHERE satellite_name = '{satellite_name}'
    ) AS sat_q"""

    try:
        df = spark.read.jdbc(url=metadata_db, table=query, properties=props)
        if df.count() > 0:
            config_json = df.first()["satellite_config"]
            if isinstance(config_json, str):
                return json.loads(config_json)
    except Exception as e:
        print(f"[load_satellite] Warning: Could not load satellite metadata: {e}")

    return {}


def generate_hash_diff(df, descriptive_columns):
    """Generate hash_diff from descriptive columns for change detection."""
    return df.withColumn(
        "hash_diff",
        md5(concat_ws("||", *[col(c).cast("string") for c in descriptive_columns]))
    )


def main():
    args = parse_args()
    spark = get_spark(args.feed_id, args.execution_id)

    hub_key_col = args.hub_hash_key_col
    desc_columns = [c.strip() for c in args.descriptive_columns.split(",") if c.strip()]

    print(f"[load_satellite] Satellite: {args.satellite_name}, Feed: {args.feed_id}")
    print(f"[load_satellite] Hub key column: {hub_key_col}")
    print(f"[load_satellite] Descriptive columns: {desc_columns}")

    if not desc_columns:
        raise ValueError("--descriptive-columns is required for satellite loading")

    try:
        # Load satellite metadata from PostgreSQL
        sat_meta = load_satellite_metadata(spark, args.metadata_db, args.satellite_name)
        if sat_meta:
            print(f"[load_satellite] Loaded satellite metadata: {list(sat_meta.keys())}")

        # Read source data
        source_df = spark.read.parquet(args.source_path)
        source_count = source_df.count()
        print(f"[load_satellite] Source rows: {source_count}")

        if source_count == 0:
            print("[load_satellite] No source data. Skipping.")
            spark.stop()
            return

        # Select hub key + descriptive columns
        select_cols = [hub_key_col] + desc_columns
        incoming = source_df.select(*select_cols).distinct()

        # Generate hash_diff for change detection
        incoming = generate_hash_diff(incoming, desc_columns)

        # Add satellite metadata columns
        incoming = (
            incoming
            .withColumn("load_date", current_timestamp())
            .withColumn("load_end_date", lit(None).cast("timestamp"))
            .withColumn("record_source", lit(args.feed_id))
        )

        incoming_count = incoming.count()
        print(f"[load_satellite] Incoming distinct records: {incoming_count}")

        # Try to read existing satellite
        gold_path = f"gs://{args.bq_project}-gold/{args.satellite_name}"
        has_existing = False
        existing = None
        existing_count = 0
        closed_count = 0
        new_count = 0

        try:
            existing = spark.read.parquet(gold_path)
            existing_count = existing.count()
            has_existing = existing_count > 0
            print(f"[load_satellite] Existing satellite rows: {existing_count}")
        except Exception as e:
            print(f"[load_satellite] No existing satellite found: {e}")

        if not has_existing:
            # First load: all records are new
            result = incoming
            new_count = incoming_count
        else:
            # Get current records (load_end_date IS NULL)
            current_records = existing.filter(col("load_end_date").isNull())
            historical_records = existing.filter(col("load_end_date").isNotNull())

            # Detect changes: join incoming with current on hub key, compare hash_diff
            matched = incoming.alias("inc").join(
                current_records.alias("cur"),
                col(f"inc.{hub_key_col}") == col(f"cur.{hub_key_col}"),
                "inner"
            )

            # Changed records: same hub key, different hash_diff
            changed_keys = matched.filter(
                col("inc.hash_diff") != col("cur.hash_diff")
            ).select(col(f"inc.{hub_key_col}").alias(hub_key_col)).distinct()

            changed_key_count = changed_keys.count()
            print(f"[load_satellite] Changed records detected: {changed_key_count}")

            # Close changed current records (set load_end_date)
            if changed_key_count > 0:
                records_to_close = current_records.join(changed_keys, hub_key_col, "left_semi")
                closed_records = records_to_close.withColumn("load_end_date", current_timestamp())
                unchanged_current = current_records.join(changed_keys, hub_key_col, "left_anti")
                closed_count = changed_key_count
            else:
                closed_records = spark.createDataFrame([], current_records.schema)
                unchanged_current = current_records

            # New records: hub keys not in existing at all
            all_existing_keys = existing.select(hub_key_col).distinct()
            truly_new = incoming.join(all_existing_keys, hub_key_col, "left_anti")

            # Changed records get new satellite rows
            changed_inserts = incoming.join(changed_keys, hub_key_col, "left_semi")

            # Combine all inserts
            inserts = truly_new.unionByName(changed_inserts, allowMissingColumns=True)
            new_count = inserts.count()

            # Assemble final dataset
            result = (
                historical_records
                .unionByName(closed_records, allowMissingColumns=True)
                .unionByName(unchanged_current, allowMissingColumns=True)
                .unionByName(inserts, allowMissingColumns=True)
            )

        result_count = result.count()

        # Write to GCS
        result.write.mode("overwrite").parquet(gold_path)
        print(f"[load_satellite] Wrote {result_count} rows to {gold_path}")

        # Write to BigQuery
        if args.target_table:
            print(f"[load_satellite] Writing to BigQuery: {args.target_table}")
            (
                result.write
                .format("bigquery")
                .option("table", args.target_table)
                .option("temporaryGcsBucket", f"{args.bq_project}-temp")
                .option("writeDisposition", "WRITE_TRUNCATE")
                .mode("overwrite")
                .save()
            )

        # Output metrics
        metrics = {
            "feed_id": args.feed_id,
            "execution_id": args.execution_id,
            "satellite_name": args.satellite_name,
            "source_rows": source_count,
            "incoming_distinct": incoming_count,
            "existing_rows": existing_count,
            "records_closed": closed_count,
            "new_records_inserted": new_count,
            "total_output_rows": result_count,
            "gold_path": gold_path,
            "status": "SUCCESS",
            "timestamp": datetime.utcnow().isoformat(),
        }
        print(f"[load_satellite] METRICS: {json.dumps(metrics)}")

    except Exception as e:
        print(f"[load_satellite] ERROR: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
