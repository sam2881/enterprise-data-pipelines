"""
APEX Data Agent - Star Schema Dimension Loader Spark Job (v2)

Loads dimension data with SCD support (Type 0, 1, 2, 3).
Generates surrogate keys and manages versioning.

Arguments (argparse):
    --feed-id           Feed identifier
    --execution-id      Execution run ID
    --metadata-db       PostgreSQL JDBC URL
    --source-path       Silver zone Parquet path (GCS)
    --dimension-name    Dimension entity name (e.g., dim_customer)
    --natural-key       Natural key column name
    --scd-type          SCD type: 0, 1, 2, or 3
    --columns           Comma-separated dimension attribute columns
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
    monotonically_increasing_id,
    when,
)


def parse_args():
    parser = argparse.ArgumentParser(description="Star Schema Dimension Loader")
    parser.add_argument("--feed-id", required=True)
    parser.add_argument("--execution-id", required=True)
    parser.add_argument("--metadata-db", required=True)
    parser.add_argument("--source-path", required=True)
    parser.add_argument("--dimension-name", required=True)
    parser.add_argument("--natural-key", required=True)
    parser.add_argument("--scd-type", required=True, choices=["0", "1", "2", "3"])
    parser.add_argument("--columns", required=True)
    parser.add_argument("--target-table", required=True)
    parser.add_argument("--bq-project", default="")
    return parser.parse_args()


def get_spark(feed_id: str, execution_id: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(f"{feed_id}-load-dimension-{execution_id}")
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .getOrCreate()
    )


def load_dimension_metadata(spark, metadata_db: str, dimension_name: str):
    """Load dimension configuration from PostgreSQL metadata."""
    props = {"driver": "org.postgresql.Driver"}
    query = f"""(
        SELECT dimension_config
        FROM dimension_configs
        WHERE dimension_name = '{dimension_name}'
    ) AS dim_q"""

    try:
        df = spark.read.jdbc(url=metadata_db, table=query, properties=props)
        if df.count() > 0:
            config_json = df.first()["dimension_config"]
            if isinstance(config_json, str):
                return json.loads(config_json)
    except Exception as e:
        print(f"[load_dimension] Warning: Could not load dimension metadata: {e}")

    return {}


def read_existing(spark, gold_path):
    """Read existing dimension table, return None if not found."""
    try:
        existing = spark.read.parquet(gold_path)
        if existing.count() > 0:
            return existing
    except Exception as e:
        print(f"[load_dimension] No existing dimension found: {e}")
    return None


def run_scd0(incoming, existing, natural_key):
    """SCD Type 0: Insert only new records. Never update existing."""
    if existing is None:
        result = incoming.withColumn("surrogate_key", monotonically_increasing_id())
        return result, incoming.count(), 0

    existing_keys = existing.select(natural_key).distinct()
    new_records = incoming.join(existing_keys, natural_key, "left_anti")
    new_count = new_records.count()

    if new_count == 0:
        return existing, 0, 0

    new_records = new_records.withColumn("surrogate_key", monotonically_increasing_id())
    result = existing.unionByName(new_records, allowMissingColumns=True)
    return result, new_count, 0


def run_scd1(incoming, existing, natural_key, attribute_columns):
    """SCD Type 1: Overwrite existing records, insert new ones."""
    if existing is None:
        result = incoming.withColumn("surrogate_key", monotonically_increasing_id())
        return result, incoming.count(), 0

    # Split existing into matched and unmatched
    existing_keys = existing.select(natural_key).distinct()

    # Records in existing that have updates incoming
    incoming_keys = incoming.select(natural_key).distinct()
    keys_to_update = existing.join(incoming_keys, natural_key, "left_semi")
    keys_unchanged = existing.join(incoming_keys, natural_key, "left_anti")

    # For updated keys, take the incoming version but preserve surrogate_key
    updated = incoming.join(
        keys_to_update.select(natural_key, "surrogate_key"),
        natural_key,
        "inner"
    )
    update_count = updated.count()

    # Truly new records
    new_records = incoming.join(existing_keys, natural_key, "left_anti")
    new_records = new_records.withColumn("surrogate_key", monotonically_increasing_id())
    new_count = new_records.count()

    result = keys_unchanged.unionByName(updated, allowMissingColumns=True)
    result = result.unionByName(new_records, allowMissingColumns=True)

    return result, new_count, update_count


def run_scd2(incoming, existing, natural_key, attribute_columns, execution_date):
    """SCD Type 2: Version records. Close old, insert new version."""
    if existing is None:
        result = (
            incoming
            .withColumn("surrogate_key", monotonically_increasing_id())
            .withColumn("effective_start_date", lit(execution_date))
            .withColumn("effective_end_date", lit(None).cast("string"))
            .withColumn("is_current", lit(True))
        )
        return result, incoming.count(), 0

    # Hash attributes for change detection
    hash_cols = [col(c).cast("string") for c in attribute_columns]
    incoming_hashed = incoming.withColumn("_row_hash", md5(concat_ws("||", *hash_cols)))

    current_records = existing.filter(col("is_current") == True)
    historical_records = existing.filter(col("is_current") == False)

    current_hashed = current_records.withColumn("_row_hash", md5(concat_ws("||", *hash_cols)))

    # Find changed records
    matched = incoming_hashed.alias("inc").join(
        current_hashed.alias("cur"),
        col(f"inc.{natural_key}") == col(f"cur.{natural_key}"),
        "inner"
    ).filter(col("inc._row_hash") != col("cur._row_hash"))

    changed_keys = matched.select(col(f"inc.{natural_key}").alias(natural_key)).distinct()
    changed_count = changed_keys.count()

    # Close changed current records
    if changed_count > 0:
        records_to_close = current_records.join(changed_keys, natural_key, "left_semi")
        closed = (
            records_to_close
            .withColumn("effective_end_date", lit(execution_date))
            .withColumn("is_current", lit(False))
        )
        unchanged_current = current_records.join(changed_keys, natural_key, "left_anti")
    else:
        closed = None
        unchanged_current = current_records

    # New versions of changed records
    changed_inserts = incoming.join(changed_keys, natural_key, "left_semi")

    # Truly new records
    all_existing_keys = existing.select(natural_key).distinct()
    truly_new = incoming.join(all_existing_keys, natural_key, "left_anti")

    inserts = changed_inserts.unionByName(truly_new, allowMissingColumns=True)
    inserts = (
        inserts
        .withColumn("surrogate_key", monotonically_increasing_id())
        .withColumn("effective_start_date", lit(execution_date))
        .withColumn("effective_end_date", lit(None).cast("string"))
        .withColumn("is_current", lit(True))
    )
    new_count = inserts.count()

    # Assemble final dataset
    result = historical_records.unionByName(unchanged_current, allowMissingColumns=True)
    if closed is not None:
        result = result.unionByName(closed, allowMissingColumns=True)
    result = result.unionByName(inserts, allowMissingColumns=True)

    return result, new_count, changed_count


def run_scd3(incoming, existing, natural_key, attribute_columns):
    """SCD Type 3: Track previous value in additional columns."""
    if existing is None:
        # Create prev_ columns initialized to NULL
        result = incoming
        for attr_col in attribute_columns:
            result = result.withColumn(f"prev_{attr_col}", lit(None).cast("string"))
        result = result.withColumn("surrogate_key", monotonically_increasing_id())
        return result, incoming.count(), 0

    existing_keys = existing.select(natural_key).distinct()

    # For existing records: shift current values to prev_ columns, then update
    matched_existing = existing.join(
        incoming.select(natural_key).distinct(), natural_key, "left_semi"
    )
    unmatched_existing = existing.join(
        incoming.select(natural_key).distinct(), natural_key, "left_anti"
    )

    if matched_existing.count() > 0:
        # Shift current attribute values to prev_ columns
        updated = matched_existing
        for attr_col in attribute_columns:
            updated = updated.withColumn(f"prev_{attr_col}", col(attr_col))

        # Drop attribute columns and join with incoming to get new values
        drop_cols = attribute_columns
        updated_keys = updated.drop(*drop_cols)

        updated = updated_keys.join(
            incoming.select(natural_key, *attribute_columns),
            natural_key,
            "inner"
        )
        update_count = updated.count()
    else:
        updated = None
        update_count = 0

    # Truly new records
    new_records = incoming.join(existing_keys, natural_key, "left_anti")
    for attr_col in attribute_columns:
        new_records = new_records.withColumn(f"prev_{attr_col}", lit(None).cast("string"))
    new_records = new_records.withColumn("surrogate_key", monotonically_increasing_id())
    new_count = new_records.count()

    # Assemble
    result = unmatched_existing
    if updated is not None:
        result = result.unionByName(updated, allowMissingColumns=True)
    result = result.unionByName(new_records, allowMissingColumns=True)

    return result, new_count, update_count


def main():
    args = parse_args()
    spark = get_spark(args.feed_id, args.execution_id)

    attribute_columns = [c.strip() for c in args.columns.split(",") if c.strip()]
    scd_type = int(args.scd_type)

    print(f"[load_dimension] Dimension: {args.dimension_name}, SCD Type: {scd_type}")
    print(f"[load_dimension] Natural key: {args.natural_key}, Columns: {attribute_columns}")

    if not attribute_columns:
        raise ValueError("--columns is required for dimension loading")

    try:
        # Load dimension metadata from PostgreSQL
        dim_meta = load_dimension_metadata(spark, args.metadata_db, args.dimension_name)
        if dim_meta:
            print(f"[load_dimension] Loaded dimension metadata: {list(dim_meta.keys())}")

        # Read source data
        source_df = spark.read.parquet(args.source_path)
        source_count = source_df.count()
        print(f"[load_dimension] Source rows: {source_count}")

        if source_count == 0:
            print("[load_dimension] No source data. Skipping.")
            spark.stop()
            return

        # Select relevant columns
        select_cols = [args.natural_key] + attribute_columns
        incoming = source_df.select(*select_cols).distinct()

        # Read existing dimension
        gold_path = f"gs://{args.bq_project}-gold/{args.dimension_name}"
        existing = read_existing(spark, gold_path)

        # Add load timestamp to incoming
        incoming = incoming.withColumn("_load_timestamp", current_timestamp())

        # Route to SCD handler
        if scd_type == 0:
            result, new_count, update_count = run_scd0(incoming, existing, args.natural_key)
        elif scd_type == 1:
            result, new_count, update_count = run_scd1(incoming, existing, args.natural_key, attribute_columns)
        elif scd_type == 2:
            result, new_count, update_count = run_scd2(
                incoming, existing, args.natural_key, attribute_columns, args.execution_id
            )
        elif scd_type == 3:
            result, new_count, update_count = run_scd3(incoming, existing, args.natural_key, attribute_columns)
        else:
            raise ValueError(f"Unsupported SCD type: {scd_type}")

        result_count = result.count()
        print(f"[load_dimension] New: {new_count}, Updated: {update_count}, Total: {result_count}")

        # Write to GCS
        result.write.mode("overwrite").parquet(gold_path)
        print(f"[load_dimension] Wrote {result_count} rows to {gold_path}")

        # Write to BigQuery
        if args.target_table:
            print(f"[load_dimension] Writing to BigQuery: {args.target_table}")
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
            "dimension_name": args.dimension_name,
            "scd_type": scd_type,
            "source_rows": source_count,
            "new_records": new_count,
            "updated_records": update_count,
            "total_output_rows": result_count,
            "gold_path": gold_path,
            "status": "SUCCESS",
            "timestamp": datetime.utcnow().isoformat(),
        }
        print(f"[load_dimension] METRICS: {json.dumps(metrics)}")

    except Exception as e:
        print(f"[load_dimension] ERROR: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
