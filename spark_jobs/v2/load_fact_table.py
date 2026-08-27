"""
APEX Data Agent - Star Schema Fact Table Loader (v2)

Loads fact table with dimension surrogate key lookups.
Handles late-arriving dimensions by inserting placeholder rows.

Arguments (argparse):
    --feed-id               Feed identifier
    --execution-id          Execution run ID
    --execution-date        Business date (YYYY-MM-DD)
    --metadata-db           PostgreSQL JDBC URL
    --source-path           Silver zone source path (GCS)
    --fact-name             Fact table name
    --grain-columns         Comma-separated grain columns
    --measures-json         JSON array: [{"name": "amount", "source_column": "amt", "agg": "sum"}]
    --dimension-lookups-json JSON array: [{"dimension_name": "customer", "join_key": "cust_id", "surrogate_key": "sk_id"}]
    --target-table          Target BigQuery table name
    --bq-project            BigQuery project ID
"""

import argparse
import json
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import col, current_timestamp, lit, when


def parse_args():
    parser = argparse.ArgumentParser(description="Load Fact Table")
    parser.add_argument("--feed-id", required=True)
    parser.add_argument("--execution-id", required=True)
    parser.add_argument("--execution-date", required=True)
    parser.add_argument("--metadata-db", default="")
    parser.add_argument("--source-path", required=True)
    parser.add_argument("--fact-name", required=True)
    parser.add_argument("--grain-columns", required=True)
    parser.add_argument("--measures-json", required=True)
    parser.add_argument("--dimension-lookups-json", default="[]")
    parser.add_argument("--target-table", required=True)
    parser.add_argument("--bq-project", default="")
    return parser.parse_args()


def get_spark(feed_id: str, execution_id: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(f"{feed_id}-load-fact-{execution_id}")
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .getOrCreate()
    )


def lookup_dimension_keys(spark, df, lookups, bq_project):
    """Join with dimension tables to resolve surrogate keys."""
    for lookup in lookups:
        dim_name = lookup["dimension_name"]
        join_key = lookup["join_key"]
        sk = lookup.get("surrogate_key", "sk_id")
        dim_table = f"dim_{dim_name}"

        print(f"[load_fact] Joining with dimension: {dim_table} on {join_key}")

        try:
            # Read dimension table from BigQuery
            dim_df = (
                spark.read
                .format("bigquery")
                .option("table", f"{bq_project}.gold.{dim_table}")
                .load()
                .filter(col("is_current") == True)
                .select(col(join_key), col(sk).alias(f"{dim_name}_{sk}"))
            )

            # Left join
            df = df.join(dim_df, on=join_key, how="left")

            # Handle late-arriving dimensions (NULL surrogate key)
            null_count = df.filter(col(f"{dim_name}_{sk}").isNull()).count()
            if null_count > 0:
                print(f"[load_fact] Warning: {null_count} rows with missing {dim_name} dimension (late-arriving)")
                df = df.withColumn(
                    f"{dim_name}_{sk}",
                    when(col(f"{dim_name}_{sk}").isNull(), lit(-1)).otherwise(col(f"{dim_name}_{sk}")),
                )

        except Exception as e:
            print(f"[load_fact] Warning: Could not join {dim_table}: {e}. Using -1 as placeholder.")
            df = df.withColumn(f"{dim_name}_{sk}", lit(-1))

    return df


def main():
    args = parse_args()
    spark = get_spark(args.feed_id, args.execution_id)

    print(f"[load_fact] Feed: {args.feed_id}, Fact: {args.fact_name}")
    print(f"[load_fact] Source: {args.source_path}")

    try:
        # Parse config
        grain_columns = [c.strip() for c in args.grain_columns.split(",") if c.strip()]
        measures = json.loads(args.measures_json)
        lookups = json.loads(args.dimension_lookups_json)

        # Read source data
        df = spark.read.parquet(args.source_path)
        source_count = df.count()
        print(f"[load_fact] Source rows: {source_count}")

        if source_count == 0:
            print("[load_fact] No data. Skipping.")
            spark.stop()
            return

        # Resolve dimension surrogate keys
        if lookups and args.bq_project:
            df = lookup_dimension_keys(spark, df, lookups, args.bq_project)

        # Select grain + SK + measures + audit
        select_cols = list(grain_columns)

        for lookup in lookups:
            sk_col = f"{lookup['dimension_name']}_{lookup.get('surrogate_key', 'sk_id')}"
            if sk_col in df.columns:
                select_cols.append(sk_col)

        for m in measures:
            src = m.get("source_column", m["name"])
            name = m["name"]
            if src in df.columns:
                if src != name:
                    df = df.withColumnRenamed(src, name)
                select_cols.append(name)

        # Add audit columns
        df = df.withColumn("_execution_date", lit(args.execution_date).cast("date"))
        df = df.withColumn("_run_id", lit(args.execution_id))
        df = df.withColumn("_load_timestamp", current_timestamp())
        select_cols.extend(["_execution_date", "_run_id", "_load_timestamp"])

        df = df.select(*[c for c in select_cols if c in df.columns])

        fact_count = df.count()

        # Write to BigQuery
        if args.bq_project:
            target = f"{args.bq_project}.gold.{args.target_table}"
            print(f"[load_fact] Writing to BigQuery: {target}")
            (
                df.write
                .format("bigquery")
                .option("table", target)
                .option("createDisposition", "CREATE_IF_NEEDED")
                .option("writeDisposition", "WRITE_APPEND")
                .mode("append")
                .save()
            )

        # Also write to GCS gold path
        gold_path = f"gs://datalake/gold/{args.fact_name}/{args.execution_date}/"
        df.write.mode("overwrite").parquet(gold_path)

        metrics = {
            "feed_id": args.feed_id,
            "execution_id": args.execution_id,
            "fact_name": args.fact_name,
            "source_count": source_count,
            "fact_count": fact_count,
            "dimensions_joined": len(lookups),
            "measures": len(measures),
            "status": "SUCCESS",
            "timestamp": datetime.utcnow().isoformat(),
        }
        print(f"[load_fact] METRICS: {json.dumps(metrics)}")

    except Exception as e:
        print(f"[load_fact] ERROR: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
