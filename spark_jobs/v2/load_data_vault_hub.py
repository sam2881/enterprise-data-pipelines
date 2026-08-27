"""
APEX Data Agent - Data Vault 2.0 Hub Loader Spark Job (v2)

Loads business keys into a Data Vault Hub table.
Only inserts new keys not already present in the hub (INSERT-only pattern).

Arguments (argparse):
    --feed-id           Feed identifier
    --execution-id      Execution run ID
    --metadata-db       PostgreSQL JDBC URL
    --source-path       Silver zone Parquet path (GCS)
    --hub-name          Hub entity name (e.g., hub_customer)
    --business-keys     Comma-separated business key columns
    --target-table      BigQuery target table (project.dataset.table)
    --bq-project        BigQuery project ID
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
)


def parse_args():
    parser = argparse.ArgumentParser(description="Data Vault 2.0 Hub Loader")
    parser.add_argument("--feed-id", required=True)
    parser.add_argument("--execution-id", required=True)
    parser.add_argument("--metadata-db", required=True)
    parser.add_argument("--source-path", required=True)
    parser.add_argument("--hub-name", required=True)
    parser.add_argument("--business-keys", required=True)
    parser.add_argument("--target-table", required=True)
    parser.add_argument("--bq-project", default="")
    return parser.parse_args()


def get_spark(feed_id: str, execution_id: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(f"{feed_id}-load-hub-{execution_id}")
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .getOrCreate()
    )


def load_hub_metadata(spark, metadata_db: str, hub_name: str):
    """Load hub configuration from PostgreSQL metadata."""
    props = {"driver": "org.postgresql.Driver"}
    query = f"""(
        SELECT hub_config
        FROM data_vault_hubs
        WHERE hub_name = '{hub_name}'
    ) AS hub_q"""

    try:
        df = spark.read.jdbc(url=metadata_db, table=query, properties=props)
        if df.count() > 0:
            config_json = df.first()["hub_config"]
            if isinstance(config_json, str):
                return json.loads(config_json)
    except Exception as e:
        print(f"[load_hub] Warning: Could not load hub metadata: {e}")

    return {}


def generate_hub_hash_key(df, business_keys):
    """Generate hub_hash_key as MD5 of concatenated business keys."""
    return df.withColumn(
        "hub_hash_key",
        md5(concat_ws("||", *[col(k).cast("string") for k in business_keys]))
    )


def main():
    args = parse_args()
    spark = get_spark(args.feed_id, args.execution_id)

    business_keys = [k.strip() for k in args.business_keys.split(",") if k.strip()]

    print(f"[load_hub] Hub: {args.hub_name}, Feed: {args.feed_id}")
    print(f"[load_hub] Business keys: {business_keys}")
    print(f"[load_hub] Source: {args.source_path}")

    if not business_keys:
        raise ValueError("--business-keys is required for hub loading")

    try:
        # Load hub metadata from PostgreSQL
        hub_meta = load_hub_metadata(spark, args.metadata_db, args.hub_name)
        if hub_meta:
            print(f"[load_hub] Loaded hub metadata: {list(hub_meta.keys())}")

        # Read source data
        source_df = spark.read.parquet(args.source_path)
        source_count = source_df.count()
        print(f"[load_hub] Source rows: {source_count}")

        if source_count == 0:
            print("[load_hub] No source data. Skipping.")
            spark.stop()
            return

        # Select only business key columns and deduplicate
        hub_candidates = source_df.select(*business_keys).distinct()

        # Generate hub_hash_key
        hub_candidates = generate_hub_hash_key(hub_candidates, business_keys)

        # Add hub metadata columns
        hub_candidates = (
            hub_candidates
            .withColumn("load_date", current_timestamp())
            .withColumn("record_source", lit(args.feed_id))
        )

        candidate_count = hub_candidates.count()
        print(f"[load_hub] Distinct business key combinations: {candidate_count}")

        # Try to read existing hub to find new-only keys
        new_keys = hub_candidates
        existing_count = 0
        try:
            # Attempt to read existing hub from GCS (gold zone)
            target_path = args.target_table.replace(".", "/")
            existing_hub = spark.read.parquet(f"gs://{args.bq_project}-gold/{target_path}")
            existing_count = existing_hub.count()

            if existing_count > 0:
                # LEFT ANTI JOIN: only keys not already in the hub
                new_keys = hub_candidates.join(
                    existing_hub.select("hub_hash_key"),
                    on="hub_hash_key",
                    how="left_anti"
                )
                print(f"[load_hub] Existing hub rows: {existing_count}")
        except Exception as e:
            print(f"[load_hub] No existing hub found, all keys are new: {e}")

        new_count = new_keys.count()
        print(f"[load_hub] New keys to insert: {new_count}")

        if new_count == 0:
            print("[load_hub] No new keys to insert. Skipping write.")
            metrics = {
                "feed_id": args.feed_id,
                "execution_id": args.execution_id,
                "hub_name": args.hub_name,
                "source_rows": source_count,
                "candidate_keys": candidate_count,
                "existing_keys": existing_count,
                "new_keys_inserted": 0,
                "status": "SUCCESS_NO_NEW_KEYS",
                "timestamp": datetime.utcnow().isoformat(),
            }
            print(f"[load_hub] METRICS: {json.dumps(metrics)}")
            spark.stop()
            return

        # Reorder columns: hub_hash_key first, then business keys, then metadata
        output_columns = ["hub_hash_key"] + business_keys + ["load_date", "record_source"]
        new_keys = new_keys.select(*output_columns)

        # Write to GCS gold zone
        gold_path = f"gs://{args.bq_project}-gold/{args.hub_name}"
        new_keys.write.mode("append").parquet(gold_path)
        print(f"[load_hub] Wrote {new_count} new hub rows to {gold_path}")

        # Write to BigQuery
        if args.target_table:
            print(f"[load_hub] Writing to BigQuery: {args.target_table}")
            (
                new_keys.write
                .format("bigquery")
                .option("table", args.target_table)
                .option("temporaryGcsBucket", f"{args.bq_project}-temp")
                .option("writeDisposition", "WRITE_APPEND")
                .mode("append")
                .save()
            )

        # Output metrics
        metrics = {
            "feed_id": args.feed_id,
            "execution_id": args.execution_id,
            "hub_name": args.hub_name,
            "source_rows": source_count,
            "candidate_keys": candidate_count,
            "existing_keys": existing_count,
            "new_keys_inserted": new_count,
            "gold_path": gold_path,
            "status": "SUCCESS",
            "timestamp": datetime.utcnow().isoformat(),
        }
        print(f"[load_hub] METRICS: {json.dumps(metrics)}")

    except Exception as e:
        print(f"[load_hub] ERROR: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
