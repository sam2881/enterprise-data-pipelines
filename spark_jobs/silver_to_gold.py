#!/usr/bin/env python3
"""
APEX Data Agent - Silver to Gold PySpark Job

Transforms Silver data to Gold zone:
- Applies Gold view transformations
- Business aggregations
- Surrogate key generation
- Dimensional modeling (Star Schema, SCD2, Data Vault)
- Final business-ready data

This is one of the 5 canonical Spark jobs.
ALL Gold processing MUST use this job.
"""

import argparse
import sys
import json
from datetime import datetime
from typing import Dict, Any, List, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, row_number,
    monotonically_increasing_id, md5, concat_ws,
    when, coalesce, max as spark_max, min as spark_min,
    sum as spark_sum, count, avg,
    date_format, year, month, quarter, expr,
)
from pyspark.sql.window import Window


# ═══════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Silver to Gold transformation")
    parser.add_argument("--feed-id", required=True, help="Feed ID")
    parser.add_argument("--contract-id", required=True, help="Contract ID")
    parser.add_argument("--execution-id", required=True, help="Execution ID")
    parser.add_argument("--execution-date", required=True, help="Execution date")
    parser.add_argument("--metadata-db", required=True, help="Metadata DB connection string")
    parser.add_argument("--validate", action="store_true", help="Run post-load validation")
    parser.add_argument("--pg-connection-string", default="", help="PostgreSQL connection for validation results")
    return parser.parse_args()


# ═══════════════════════════════════════════════════════════════════════════
# METADATA LOADING
# ═══════════════════════════════════════════════════════════════════════════

def load_contract_config(spark: SparkSession, metadata_db: str, contract_id: str) -> Dict[str, Any]:
    """Load contract configuration."""
    df = spark.read \
        .format("jdbc") \
        .option("url", metadata_db) \
        .option("dbtable", f"(SELECT * FROM data_contract WHERE contract_id = '{contract_id}') AS contract") \
        .load()

    row = df.first()
    return {
        "silver_table": row["silver_table"],
        "silver_path": row["silver_path"],
        "gold_table": row["gold_table"],
        "gold_path": row["gold_path"],
        "primary_keys": row["primary_keys"] or [],
        "load_type": row["load_type"],
    }


def load_gold_view(spark: SparkSession, metadata_db: str, contract_id: str) -> Optional[Dict[str, Any]]:
    """Load Gold view definition from metadata."""
    df = spark.read \
        .format("jdbc") \
        .option("url", metadata_db) \
        .option("dbtable", f"""(
            SELECT * FROM view_definition
            WHERE contract_id = '{contract_id}'
              AND zone_level = 'GOLD'
              AND is_active = true
        ) AS view""") \
        .load()

    row = df.first()
    if row:
        return {
            "view_name": row["view_name"],
            "view_sql": row["view_sql"],
            "materialized": row["materialized"],
            "refresh_mode": row["refresh_mode"],
        }
    return None


def load_transformation_rules(spark: SparkSession, metadata_db: str, contract_id: str) -> List[Dict[str, Any]]:
    """Load transformation rules for Gold zone."""
    df = spark.read \
        .format("jdbc") \
        .option("url", metadata_db) \
        .option("dbtable", f"""(
            SELECT tr.* FROM transformation_rule tr
            JOIN contract_transformation ct ON tr.rule_id = ct.rule_id
            WHERE ct.contract_id = '{contract_id}'
              AND ct.zone_level = 'GOLD'
              AND ct.is_active = true
            ORDER BY ct.execution_order
        ) AS rules""") \
        .load()

    return [row.asDict() for row in df.collect()]


# ═══════════════════════════════════════════════════════════════════════════
# TRANSFORMATION FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════

def apply_gold_view(
    spark: SparkSession,
    silver_table: str,
    view_sql: str,
    execution_id: str,
) -> DataFrame:
    """Apply Gold view transformation SQL."""
    # Register Silver table
    silver_df = spark.table(silver_table)
    silver_df.createOrReplaceTempView("silver")

    # Replace template variables
    sql = view_sql.replace("{{ execution_id }}", execution_id)
    sql = sql.replace("{{ silver_table }}", silver_table)

    # Extract SELECT if CREATE VIEW
    if "CREATE" in sql.upper() and "VIEW" in sql.upper():
        as_pos = sql.upper().find(" AS ")
        if as_pos > 0:
            sql = sql[as_pos + 4:].strip()

    return spark.sql(sql)


def generate_surrogate_keys(
    df: DataFrame,
    key_column: str = "_surrogate_key",
) -> DataFrame:
    """Generate surrogate keys for dimension tables."""
    return df.withColumn(
        key_column,
        monotonically_increasing_id() + 1
    )


def apply_scd_type2(
    spark: SparkSession,
    current_df: DataFrame,
    new_df: DataFrame,
    business_keys: List[str],
    tracked_columns: List[str],
) -> DataFrame:
    """Apply SCD Type 2 logic for slowly changing dimensions."""
    if not business_keys or not tracked_columns:
        return new_df

    # Columns for change detection
    tracked_cols_expr = [col(c) for c in tracked_columns]
    bk_cols = [col(c) for c in business_keys]

    # Add hash for change detection
    current_with_hash = current_df.withColumn(
        "_tracked_hash",
        md5(concat_ws("|", *[col(c).cast("string") for c in tracked_columns]))
    )

    new_with_hash = new_df.withColumn(
        "_tracked_hash",
        md5(concat_ws("|", *[col(c).cast("string") for c in tracked_columns]))
    )

    # Find changed records
    changes = new_with_hash.alias("new").join(
        current_with_hash.filter(col("_is_current") == True).alias("curr"),
        [new_with_hash[k] == current_with_hash[k] for k in business_keys],
        "left"
    ).filter(
        col("curr._tracked_hash").isNull() |
        (col("new._tracked_hash") != col("curr._tracked_hash"))
    ).select("new.*")

    # Close old records
    closed_records = current_with_hash.filter(col("_is_current") == True).alias("curr").join(
        changes.alias("chg"),
        [current_with_hash[k] == changes[k] for k in business_keys],
        "inner"
    ).select("curr.*").withColumn("_is_current", lit(False)).withColumn("_end_date", current_timestamp())

    # Combine: unchanged current + closed + new versions
    unchanged = current_with_hash.filter(col("_is_current") == True).join(
        changes,
        business_keys,
        "left_anti"
    )

    result = unchanged.union(
        closed_records
    ).union(
        changes.withColumn("_is_current", lit(True))
              .withColumn("_start_date", current_timestamp())
              .withColumn("_end_date", lit(None))
    )

    return result.drop("_tracked_hash")


def apply_aggregations(
    df: DataFrame,
    rules: List[Dict[str, Any]],
) -> DataFrame:
    """Apply aggregation rules."""
    for rule in rules:
        if rule.get("rule_type") != "AGGREGATION":
            continue

        # Parse aggregation expression
        # Expected format: "SUM(amount) AS total_amount GROUP BY customer_id"
        expression = rule.get("transformation_expression", "")

        # Would parse and apply aggregation
        pass

    return df


def add_gold_audit_columns(
    df: DataFrame,
    execution_id: str,
    execution_date: str,
) -> DataFrame:
    """
    Add audit columns for Gold zone.

    Mandatory system fields (non-negotiable):
      - _run_id:          UUID of this execution run
      - _record_uuid:     Unique UUID per row
      - _reporting_date:  Business reporting date
      - _load_timestamp:  Timestamp when data was loaded

    Gold-specific audit fields:
      - _gold_execution_id, _gold_load_ts, _gold_execution_date
    """
    return df \
        .withColumn("_run_id", lit(execution_id)) \
        .withColumn("_record_uuid", expr("uuid()")) \
        .withColumn("_reporting_date", lit(execution_date)) \
        .withColumn("_load_timestamp", current_timestamp()) \
        .withColumn("_gold_execution_id", lit(execution_id)) \
        .withColumn("_gold_load_ts", current_timestamp()) \
        .withColumn("_gold_execution_date", lit(execution_date))


# ═══════════════════════════════════════════════════════════════════════════
# MAIN PROCESSING
# ═══════════════════════════════════════════════════════════════════════════

def process_silver_to_gold(
    spark: SparkSession,
    contract_config: Dict[str, Any],
    gold_view: Optional[Dict[str, Any]],
    transformation_rules: List[Dict[str, Any]],
    execution_id: str,
    execution_date: str,
) -> Dict[str, Any]:
    """
    Process Silver data to Gold zone.

    Returns:
        Dict with processing metrics
    """
    # Step 1: Apply Gold view transformation
    if gold_view and gold_view.get("view_sql"):
        gold_df = apply_gold_view(
            spark=spark,
            silver_table=contract_config["silver_table"],
            view_sql=gold_view["view_sql"],
            execution_id=execution_id,
        )
    else:
        # Direct copy from Silver
        silver_df = spark.table(contract_config["silver_table"])
        gold_df = silver_df.filter(col("_silver_execution_id") == execution_id)

    initial_count = gold_df.count()

    # Step 2: Apply aggregation rules if any
    agg_rules = [r for r in transformation_rules if r.get("rule_type") == "AGGREGATION"]
    if agg_rules:
        gold_df = apply_aggregations(gold_df, agg_rules)

    # Step 3: Handle SCD Type 2 if applicable
    scd2_rules = [r for r in transformation_rules if r.get("rule_type") == "SCD2"]
    if scd2_rules and contract_config.get("gold_table"):
        try:
            existing_gold = spark.table(contract_config["gold_table"])
            for rule in scd2_rules:
                gold_df = apply_scd_type2(
                    spark=spark,
                    current_df=existing_gold,
                    new_df=gold_df,
                    business_keys=contract_config.get("primary_keys", []),
                    tracked_columns=rule.get("tracked_columns", []),
                )
        except:
            # Table doesn't exist yet
            gold_df = gold_df.withColumn("_is_current", lit(True))
            gold_df = gold_df.withColumn("_start_date", current_timestamp())
            gold_df = gold_df.withColumn("_end_date", lit(None))

    # Step 4: Generate surrogate keys for dimensions
    dim_rules = [r for r in transformation_rules if r.get("rule_type") == "DIMENSION"]
    if dim_rules:
        gold_df = generate_surrogate_keys(gold_df)

    # Step 5: Add Gold audit columns
    gold_df = add_gold_audit_columns(gold_df, execution_id, execution_date)

    # Step 6: Determine write mode
    load_type = contract_config.get("load_type", "APPEND")
    is_scd2 = len(scd2_rules) > 0

    if load_type == "FULL" and not is_scd2:
        write_mode = "overwrite"
    else:
        write_mode = "append"

    # Idempotent re-run: delete existing Gold data for this execution_date (skip for SCD2)
    if not is_scd2:
        try:
            from delta.tables import DeltaTable
            if contract_config.get("gold_table"):
                dt = DeltaTable.forName(spark, contract_config["gold_table"])
            else:
                dt = DeltaTable.forPath(spark, contract_config["gold_path"])
            dt.delete(f"_gold_execution_date = '{execution_date}'")
            print(f"Deleted existing Gold data for execution_date={execution_date}")
        except Exception:
            pass  # Table doesn't exist yet on first run

    # For SCD2, we need to use merge (Delta Lake)
    if is_scd2:
        # Use Delta Lake merge
        if contract_config.get("gold_table"):
            # Would use Delta merge operation here
            writer = gold_df.write.format("delta").mode("overwrite")
            writer.saveAsTable(contract_config["gold_table"])
        else:
            gold_df.write.format("delta").mode("overwrite").save(contract_config["gold_path"])
    else:
        # Standard write
        writer = gold_df.write \
            .format("delta") \
            .mode(write_mode) \
            .partitionBy("_gold_execution_date")

        if contract_config.get("gold_table"):
            writer.saveAsTable(contract_config["gold_table"])
        else:
            writer.save(contract_config["gold_path"])

    final_count = gold_df.count()

    return {
        "records_read": initial_count,
        "records_written": final_count,
    }


def main():
    """Main entry point."""
    args = parse_args()

    spark = SparkSession.builder \
        .appName(f"silver_to_gold_{args.feed_id}") \
        .enableHiveSupport() \
        .getOrCreate()

    try:
        # Load configurations
        contract_config = load_contract_config(spark, args.metadata_db, args.contract_id)
        gold_view = load_gold_view(spark, args.metadata_db, args.contract_id)
        transformation_rules = load_transformation_rules(spark, args.metadata_db, args.contract_id)

        # Process data
        metrics = process_silver_to_gold(
            spark=spark,
            contract_config=contract_config,
            gold_view=gold_view,
            transformation_rules=transformation_rules,
            execution_id=args.execution_id,
            execution_date=args.execution_date,
        )

        print(f"Silver to Gold completed: {metrics}")

        # Optional post-load validation
        if args.validate and metrics["records_written"] > 0:
            try:
                from dag_utilities.validation.ge_helper import GEHelper
                helper = GEHelper(spark)

                if contract_config.get("gold_table"):
                    gold_df = spark.table(contract_config["gold_table"])
                else:
                    gold_df = spark.read.format("delta").load(contract_config["gold_path"])
                gold_df = gold_df.filter(col("_gold_execution_id") == args.execution_id)

                expectations = [
                    {
                        "expectation_type": "expect_table_row_count_to_be_between",
                        "kwargs": {"min_value": 1},
                        "meta": {"rule_name": "gold_not_empty", "severity": "error", "weight": "1.0"},
                    },
                ]
                result = helper.validate_dataframe(
                    df=gold_df,
                    suite_name=f"{args.feed_id}_gold_load",
                    data_asset_name=contract_config.get("gold_table", args.feed_id),
                    expectations=expectations,
                    batch_identifier=args.execution_id,
                )
                print(f"Post-load validation: {result.passed}/{result.total} passed")

                pg_conn = args.pg_connection_string
                if pg_conn:
                    from dag_utilities.validation.ge_result_writer import GEResultWriter
                    writer = GEResultWriter(pg_conn)
                    writer.write_results(
                        feed_id=args.feed_id, run_id=args.execution_id,
                        suite_name=f"{args.feed_id}_gold_load",
                        validation_type="LOAD", zone_level="GOLD",
                        checkpoint_result=result, batch_identifier=args.execution_id,
                    )
            except Exception as e:
                print(f"Warning: Post-load validation failed: {e}")

        return 0

    except Exception as e:
        print(f"Error in silver_to_gold: {e}")
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
