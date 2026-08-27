"""
APEX Data Agent - Silver to Gold Zone Spark Job (v2)

Transforms silver data into gold zone modeled data.
Supports multiple modeling strategies: flat, SCD2, Data Vault, Star Schema.

Arguments (argparse):
    --feed-id           Feed identifier
    --contract-id       Data contract identifier
    --execution-id      Execution run ID
    --execution-date    Business date (YYYY-MM-DD)
    --metadata-db       PostgreSQL JDBC URL
    --silver-path       Silver zone Parquet path (GCS)
    --gold-path         Gold zone output path (GCS)
    --bq-table          BigQuery gold table (project.dataset.table)
    --model-type        Modeling strategy: flat, scd2, data_vault, star_schema
    --business-keys     Comma-separated business key columns
    --tracked-columns   Comma-separated columns tracked for SCD2 changes
    --transforms-json   JSON array of business logic transform rules
"""

import argparse
import json
import sys
from datetime import datetime

from pyspark.sql import SparkSession, Window
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
    parser = argparse.ArgumentParser(description="Silver to Gold")
    parser.add_argument("--feed-id", required=True)
    parser.add_argument("--contract-id", required=True)
    parser.add_argument("--execution-id", required=True)
    parser.add_argument("--execution-date", required=True)
    parser.add_argument("--metadata-db", required=True)
    parser.add_argument("--silver-path", required=True)
    parser.add_argument("--gold-path", required=True)
    parser.add_argument("--bq-table", default="")
    parser.add_argument("--model-type", required=True, choices=["flat", "scd2", "data_vault", "star_schema"])
    parser.add_argument("--business-keys", default="")
    parser.add_argument("--tracked-columns", default="")
    parser.add_argument("--transforms-json", default="[]")
    return parser.parse_args()


def get_spark(feed_id: str, execution_id: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(f"{feed_id}-silver-to-gold-{execution_id}")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .getOrCreate()
    )


def load_transforms_from_metadata(spark, metadata_db: str, feed_id: str):
    """Load gold transform rules from PostgreSQL metadata."""
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
                return all_transforms.get("gold", [])
    except Exception as e:
        print(f"[silver_to_gold] Warning: Could not load transforms from metadata: {e}")

    return []


def apply_business_transforms(df, transforms):
    """Apply business logic transform rules from metadata."""
    for rule in transforms:
        rule_type = rule.get("type", "")
        config = rule.get("config", {})

        if rule_type == "derive":
            new_col = config.get("name")
            expression = config.get("expression")
            if new_col and expression:
                df = df.withColumn(new_col, f.expr(expression))

        elif rule_type == "aggregate":
            group_cols = config.get("group_by", [])
            agg_exprs = config.get("aggregations", [])
            if group_cols and agg_exprs:
                agg_funcs = []
                for agg in agg_exprs:
                    agg_func = getattr(f, agg["func"])(col(agg["column"])).alias(agg.get("alias", agg["column"]))
                    agg_funcs.append(agg_func)
                df = df.groupBy(*group_cols).agg(*agg_funcs)

        elif rule_type == "filter":
            condition = config.get("condition")
            if condition:
                df = df.filter(f.expr(condition))

        elif rule_type == "cast":
            column = config.get("column")
            target_type = config.get("type", "string")
            if column and column in df.columns:
                df = df.withColumn(column, col(column).cast(target_type))

        elif rule_type == "rename":
            old_name = config.get("from")
            new_name = config.get("to")
            if old_name and new_name and old_name in df.columns:
                df = df.withColumnRenamed(old_name, new_name)

        elif rule_type == "window":
            new_col = config.get("name")
            func = config.get("func")
            partition = config.get("partition_by", [])
            order = config.get("order_by", [])
            if new_col and func and partition:
                w = Window.partitionBy(*partition)
                if order:
                    w = w.orderBy(*[col(c) for c in order])
                df = df.withColumn(new_col, f.expr(f"{func}() OVER (PARTITION BY {', '.join(partition)} ORDER BY {', '.join(order)})"))

    return df


def run_flat(spark, args, df, transforms):
    """Flat model: apply business logic transforms and write to gold."""
    print("[silver_to_gold] Mode: FLAT - applying business logic transforms")

    if transforms:
        df = apply_business_transforms(df, transforms)
        print(f"[silver_to_gold] Applied {len(transforms)} business transforms")

    df = (
        df.withColumn("_gold_load_timestamp", current_timestamp())
        .withColumn("_execution_id", lit(args.execution_id))
    )
    return df


def run_scd2(spark, args, df):
    """SCD Type 2: close changed records, insert new versions."""
    print("[silver_to_gold] Mode: SCD2 - applying Slowly Changing Dimension Type 2")

    business_keys = [k.strip() for k in args.business_keys.split(",") if k.strip()]
    tracked_columns = [c.strip() for c in args.tracked_columns.split(",") if c.strip()]

    if not business_keys:
        raise ValueError("SCD2 requires --business-keys")
    if not tracked_columns:
        raise ValueError("SCD2 requires --tracked-columns")

    execution_date = args.execution_date

    # Hash tracked columns for change detection
    incoming = df.withColumn(
        "_row_hash", md5(concat_ws("||", *[col(c).cast("string") for c in tracked_columns]))
    )

    # Try to read existing gold table
    try:
        existing = spark.read.parquet(args.gold_path)
        has_existing = existing.count() > 0
    except Exception:
        has_existing = False
        existing = None

    if not has_existing:
        # First load: all records are new
        result = (
            incoming
            .withColumn("surrogate_key", monotonically_increasing_id())
            .withColumn("effective_start_date", lit(execution_date))
            .withColumn("effective_end_date", lit(None).cast("string"))
            .withColumn("is_current", lit(True))
            .withColumn("_gold_load_timestamp", current_timestamp())
        )
        return result

    # Compare incoming vs existing current records
    existing_current = existing.filter(col("is_current") == True)

    # Join on business keys
    join_cond = [incoming[k] == existing_current[k] for k in business_keys]

    # Find changed records (hash differs)
    matched = incoming.alias("inc").join(
        existing_current.alias("cur"), join_cond, "inner"
    ).filter(col("inc._row_hash") != col("cur._row_hash"))

    changed_keys = matched.select(*[col(f"inc.{k}") for k in business_keys])

    # Close old records: set effective_end_date and is_current=FALSE
    close_cond = [existing[k] == changed_keys[k] for k in business_keys]
    records_to_close = existing.join(changed_keys, business_keys, "left_semi").filter(col("is_current") == True)
    closed_records = (
        records_to_close
        .withColumn("effective_end_date", lit(execution_date))
        .withColumn("is_current", lit(False))
    )

    # Unchanged existing records stay as-is
    unchanged_existing = existing.join(changed_keys, business_keys, "left_anti")
    # Also keep already-closed historical records that matched
    historical_closed = existing.join(changed_keys, business_keys, "left_semi").filter(col("is_current") == False)

    # New versions of changed records
    new_versions = incoming.join(changed_keys, business_keys, "left_semi")

    # Truly new records (not in existing at all)
    all_existing_keys = existing.select(*business_keys).distinct()
    new_records = incoming.join(all_existing_keys, business_keys, "left_anti")

    # Combine new + changed
    inserts = new_versions.unionByName(new_records, allowMissingColumns=True)
    inserts = (
        inserts
        .withColumn("surrogate_key", monotonically_increasing_id())
        .withColumn("effective_start_date", lit(execution_date))
        .withColumn("effective_end_date", lit(None).cast("string"))
        .withColumn("is_current", lit(True))
        .withColumn("_gold_load_timestamp", current_timestamp())
    )

    # Assemble final dataset
    result = unchanged_existing.unionByName(historical_closed, allowMissingColumns=True)
    result = result.unionByName(closed_records, allowMissingColumns=True)
    result = result.unionByName(inserts, allowMissingColumns=True)

    return result


def run_data_vault(args):
    """Data Vault 2.0: delegates to load_hub and load_satellite sub-jobs."""
    print("[silver_to_gold] Mode: DATA_VAULT - orchestrating Hub + Satellite loads")
    print(f"[silver_to_gold] Step 1: Run load_data_vault_hub.py with:")
    print(f"    --feed-id {args.feed_id}")
    print(f"    --execution-id {args.execution_id}")
    print(f"    --metadata-db {args.metadata_db}")
    print(f"    --source-path {args.silver_path}")
    print(f"    --business-keys {args.business_keys}")
    print(f"    --target-table <hub_table>")
    print(f"[silver_to_gold] Step 2: Run load_data_vault_satellite.py with:")
    print(f"    --feed-id {args.feed_id}")
    print(f"    --execution-id {args.execution_id}")
    print(f"    --metadata-db {args.metadata_db}")
    print(f"    --source-path {args.silver_path}")
    print(f"    --hub-hash-key-col hub_hash_key")
    print(f"    --descriptive-columns {args.tracked_columns}")
    print(f"    --target-table <sat_table>")
    return None


def run_star_schema(args):
    """Star Schema: delegates to load_dimension and load_fact sub-jobs."""
    print("[silver_to_gold] Mode: STAR_SCHEMA - orchestrating Dimension + Fact loads")
    print(f"[silver_to_gold] Step 1: Run load_dimension_tables.py with:")
    print(f"    --feed-id {args.feed_id}")
    print(f"    --execution-id {args.execution_id}")
    print(f"    --metadata-db {args.metadata_db}")
    print(f"    --source-path {args.silver_path}")
    print(f"    --natural-key <natural_key_column>")
    print(f"    --scd-type 2")
    print(f"    --target-table <dim_table>")
    print(f"[silver_to_gold] Step 2: Run load_dimension_tables.py for each dimension table")
    print(f"[silver_to_gold] Step 3: Run a fact loader with:")
    print(f"    --source-path {args.silver_path}")
    print(f"    --target-table <fact_table>")
    print(f"    --dimension-lookups <dim_table_1>,<dim_table_2>")
    return None


def write_output(df, args):
    """Write gold DataFrame to GCS and BigQuery."""
    df.write.mode("overwrite").parquet(args.gold_path)
    print(f"[silver_to_gold] Wrote {df.count()} rows to {args.gold_path}")

    if args.bq_table:
        print(f"[silver_to_gold] Writing to BigQuery: {args.bq_table}")
        (
            df.write
            .format("bigquery")
            .option("table", args.bq_table)
            .option("temporaryGcsBucket", f"{args.gold_path.split('/')[2]}-temp")
            .option("writeDisposition", "WRITE_TRUNCATE")
            .mode("overwrite")
            .save()
        )


def main():
    args = parse_args()
    spark = get_spark(args.feed_id, args.execution_id)

    print(f"[silver_to_gold] Feed: {args.feed_id}, Execution: {args.execution_id}")
    print(f"[silver_to_gold] Model: {args.model_type}, {args.silver_path} -> {args.gold_path}")

    result_df = None
    rows_in = 0
    rows_out = 0

    try:
        # Data Vault and Star Schema delegate to sub-jobs
        if args.model_type == "data_vault":
            run_data_vault(args)
            metrics = {
                "feed_id": args.feed_id,
                "execution_id": args.execution_id,
                "model_type": "data_vault",
                "status": "DELEGATED",
                "message": "Hub and Satellite sub-jobs must be run separately",
                "timestamp": datetime.utcnow().isoformat(),
            }
            print(f"[silver_to_gold] METRICS: {json.dumps(metrics)}")
            spark.stop()
            return

        if args.model_type == "star_schema":
            run_star_schema(args)
            metrics = {
                "feed_id": args.feed_id,
                "execution_id": args.execution_id,
                "model_type": "star_schema",
                "status": "DELEGATED",
                "message": "Dimension and Fact sub-jobs must be run separately",
                "timestamp": datetime.utcnow().isoformat(),
            }
            print(f"[silver_to_gold] METRICS: {json.dumps(metrics)}")
            spark.stop()
            return

        # Read silver data
        df = spark.read.parquet(args.silver_path)
        rows_in = df.count()
        print(f"[silver_to_gold] Silver rows: {rows_in}")

        if rows_in == 0:
            print("[silver_to_gold] No data in silver zone. Skipping.")
            spark.stop()
            return

        # Load transforms from metadata or args
        transforms = json.loads(args.transforms_json) if args.transforms_json != "[]" else []
        if not transforms:
            transforms = load_transforms_from_metadata(spark, args.metadata_db, args.feed_id)

        if args.model_type == "flat":
            result_df = run_flat(spark, args, df, transforms)
        elif args.model_type == "scd2":
            result_df = run_scd2(spark, args, df)

        if result_df is not None:
            rows_out = result_df.count()
            write_output(result_df, args)

        metrics = {
            "feed_id": args.feed_id,
            "execution_id": args.execution_id,
            "model_type": args.model_type,
            "rows_in": rows_in,
            "rows_out": rows_out,
            "transforms_applied": len(transforms),
            "gold_path": args.gold_path,
            "status": "SUCCESS",
            "timestamp": datetime.utcnow().isoformat(),
        }
        print(f"[silver_to_gold] METRICS: {json.dumps(metrics)}")

    except Exception as e:
        print(f"[silver_to_gold] ERROR: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
