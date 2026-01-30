#!/usr/bin/env python3
"""
APEX Data Agent - Bronze to Silver PySpark Job

Transforms Bronze data to Silver zone using view definitions:
- Applies view SQL from metadata
- Data cleansing and standardization
- Business key generation
- Deduplication
- Join with reference data

This is one of the 5 canonical Spark jobs.
ALL Silver processing MUST use this job.
"""

import argparse
import sys
import json
from datetime import datetime
from typing import Dict, Any, List, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, md5, concat_ws,
    row_number, monotonically_increasing_id,
    when, coalesce, trim, upper, lower, expr,
)
from pyspark.sql.window import Window


# ═══════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Bronze to Silver transformation")
    parser.add_argument("--feed-id", required=True, help="Feed ID")
    parser.add_argument("--contract-id", required=True, help="Contract ID")
    parser.add_argument("--execution-id", required=True, help="Execution ID")
    parser.add_argument("--execution-date", required=True, help="Execution date")
    parser.add_argument("--metadata-db", required=True, help="Metadata DB connection string")
    parser.add_argument("--validate", action="store_true", help="Run post-transform validation")
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
        "bronze_table": row["bronze_table"],
        "bronze_path": row["bronze_path"],
        "silver_table": row["silver_table"],
        "silver_path": row["silver_path"],
        "primary_keys": row["primary_keys"] or [],
        "load_type": row["load_type"],
    }


def load_silver_view(spark: SparkSession, metadata_db: str, contract_id: str) -> Dict[str, Any]:
    """Load Silver view definition from metadata."""
    df = spark.read \
        .format("jdbc") \
        .option("url", metadata_db) \
        .option("dbtable", f"""(
            SELECT * FROM view_definition
            WHERE contract_id = '{contract_id}'
              AND zone_level = 'SILVER'
              AND is_active = true
        ) AS view""") \
        .load()

    row = df.first()
    if row:
        return {
            "view_name": row["view_name"],
            "view_sql": row["view_sql"],
            "refresh_mode": row["refresh_mode"],
            "dependencies": row["dependencies"],
        }
    return None


def load_transformation_rules(spark: SparkSession, metadata_db: str, contract_id: str) -> List[Dict[str, Any]]:
    """Load transformation rules for Silver zone."""
    df = spark.read \
        .format("jdbc") \
        .option("url", metadata_db) \
        .option("dbtable", f"""(
            SELECT tr.* FROM transformation_rule tr
            JOIN contract_transformation ct ON tr.rule_id = ct.rule_id
            WHERE ct.contract_id = '{contract_id}'
              AND ct.zone_level = 'SILVER'
              AND ct.is_active = true
            ORDER BY ct.execution_order
        ) AS rules""") \
        .load()

    return [row.asDict() for row in df.collect()]


# ═══════════════════════════════════════════════════════════════════════════
# TRANSFORMATION FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════

def apply_view_transformation(
    spark: SparkSession,
    bronze_table: str,
    view_sql: str,
    execution_id: str,
) -> DataFrame:
    """Apply Silver view transformation SQL."""
    # Register Bronze table for SQL
    bronze_df = spark.table(bronze_table)
    bronze_df.createOrReplaceTempView("bronze")

    # Replace template variables in view SQL
    sql = view_sql.replace("{{ execution_id }}", execution_id)
    sql = sql.replace("{{ bronze_table }}", bronze_table)

    # Extract just the SELECT part if it's a CREATE VIEW statement
    if "CREATE" in sql.upper() and "VIEW" in sql.upper():
        # Find the AS keyword and get everything after
        as_pos = sql.upper().find(" AS ")
        if as_pos > 0:
            sql = sql[as_pos + 4:].strip()

    return spark.sql(sql)


def apply_transformation_rules(
    df: DataFrame,
    rules: List[Dict[str, Any]],
) -> DataFrame:
    """Apply transformation rules to DataFrame."""
    for rule in rules:
        rule_type = rule.get("rule_type")

        if rule_type == "CLEANSING":
            # Apply cleansing expression
            target_col = rule.get("target_column")
            expression = rule.get("transformation_expression")
            df = df.withColumn(target_col, expr(expression))

        elif rule_type == "DERIVATION":
            # Derive new column
            target_col = rule.get("target_column")
            expression = rule.get("transformation_expression")
            df = df.withColumn(target_col, expr(expression))

        elif rule_type == "LOOKUP":
            # Join with lookup table
            lookup_table = rule.get("lookup_table")
            join_condition = rule.get("lookup_join_condition")
            # Would need to load lookup table and join

        elif rule_type == "AGGREGATION":
            # Apply aggregation
            pass

    return df


def deduplicate_data(
    df: DataFrame,
    primary_keys: List[str],
    order_by_col: str = "_ingestion_ts",
) -> DataFrame:
    """Deduplicate data keeping latest record."""
    if not primary_keys:
        return df

    window = Window.partitionBy(primary_keys).orderBy(col(order_by_col).desc())

    return df \
        .withColumn("_row_num", row_number().over(window)) \
        .filter(col("_row_num") == 1) \
        .drop("_row_num")


def generate_business_keys(
    df: DataFrame,
    key_columns: List[str],
    key_name: str = "_business_key",
) -> DataFrame:
    """Generate MD5 hash business key."""
    if not key_columns:
        return df

    return df.withColumn(
        key_name,
        md5(concat_ws("|", *[col(c).cast("string") for c in key_columns]))
    )


def add_silver_audit_columns(
    df: DataFrame,
    execution_id: str,
    execution_date: str,
) -> DataFrame:
    """
    Add audit columns for Silver zone.

    Mandatory system fields (non-negotiable):
      - _run_id:          UUID of this execution run
      - _record_uuid:     Unique UUID per row
      - _reporting_date:  Business reporting date
      - _load_timestamp:  Timestamp when data was loaded

    Silver-specific audit fields:
      - _silver_execution_id, _silver_processed_ts, _silver_execution_date, _is_valid
    """
    return df \
        .withColumn("_run_id", lit(execution_id)) \
        .withColumn("_record_uuid", expr("uuid()")) \
        .withColumn("_reporting_date", lit(execution_date)) \
        .withColumn("_load_timestamp", current_timestamp()) \
        .withColumn("_silver_execution_id", lit(execution_id)) \
        .withColumn("_silver_processed_ts", current_timestamp()) \
        .withColumn("_silver_execution_date", lit(execution_date)) \
        .withColumn("_is_valid", lit(True))


# ═══════════════════════════════════════════════════════════════════════════
# MAIN PROCESSING
# ═══════════════════════════════════════════════════════════════════════════

def process_bronze_to_silver(
    spark: SparkSession,
    contract_config: Dict[str, Any],
    silver_view: Optional[Dict[str, Any]],
    transformation_rules: List[Dict[str, Any]],
    execution_id: str,
    execution_date: str,
) -> Dict[str, Any]:
    """
    Process Bronze data to Silver zone.

    Returns:
        Dict with processing metrics
    """
    # Step 1: Apply view transformation if defined
    if silver_view and silver_view.get("view_sql"):
        silver_df = apply_view_transformation(
            spark=spark,
            bronze_table=contract_config["bronze_table"],
            view_sql=silver_view["view_sql"],
            execution_id=execution_id,
        )
    else:
        # Direct copy from Bronze with basic transformations
        bronze_df = spark.table(contract_config["bronze_table"])
        silver_df = bronze_df.filter(col("_execution_id") == execution_id)

    initial_count = silver_df.count()

    # Step 2: Apply transformation rules
    if transformation_rules:
        silver_df = apply_transformation_rules(silver_df, transformation_rules)

    # Step 3: Deduplicate
    primary_keys = contract_config.get("primary_keys", [])
    if primary_keys:
        silver_df = deduplicate_data(silver_df, primary_keys)

    # Step 4: Generate business keys
    if primary_keys:
        silver_df = generate_business_keys(silver_df, primary_keys)

    # Step 5: Add Silver audit columns
    silver_df = add_silver_audit_columns(silver_df, execution_id, execution_date)

    # Step 6: Determine write mode
    load_type = contract_config.get("load_type", "APPEND")
    if load_type == "FULL":
        write_mode = "overwrite"
    elif load_type == "MERGE":
        # Would use Delta Lake merge
        write_mode = "append"
    else:
        write_mode = "append"

    # Step 7: Write to Silver
    writer = silver_df.write \
        .format("delta") \
        .mode(write_mode) \
        .partitionBy("_silver_execution_date")

    if contract_config.get("silver_table"):
        writer.saveAsTable(contract_config["silver_table"])
    else:
        writer.save(contract_config["silver_path"])

    final_count = silver_df.count()

    return {
        "records_read": initial_count,
        "records_written": final_count,
        "records_deduplicated": initial_count - final_count,
    }


def main():
    """Main entry point."""
    args = parse_args()

    spark = SparkSession.builder \
        .appName(f"bronze_to_silver_{args.feed_id}") \
        .enableHiveSupport() \
        .getOrCreate()

    try:
        # Load configurations
        contract_config = load_contract_config(spark, args.metadata_db, args.contract_id)
        silver_view = load_silver_view(spark, args.metadata_db, args.contract_id)
        transformation_rules = load_transformation_rules(spark, args.metadata_db, args.contract_id)

        # Process data
        metrics = process_bronze_to_silver(
            spark=spark,
            contract_config=contract_config,
            silver_view=silver_view,
            transformation_rules=transformation_rules,
            execution_id=args.execution_id,
            execution_date=args.execution_date,
        )

        print(f"Bronze to Silver completed: {metrics}")

        # Optional post-transform validation
        if args.validate and metrics["records_written"] > 0:
            try:
                from dag_utilities.validation.ge_helper import GEHelper
                helper = GEHelper(spark)

                if contract_config.get("silver_table"):
                    silver_df = spark.table(contract_config["silver_table"])
                else:
                    silver_df = spark.read.format("delta").load(contract_config["silver_path"])
                silver_df = silver_df.filter(col("_silver_execution_id") == args.execution_id)

                expectations = [
                    {
                        "expectation_type": "expect_table_row_count_to_be_between",
                        "kwargs": {"min_value": 1},
                        "meta": {"rule_name": "silver_not_empty", "severity": "error", "weight": "1.0"},
                    },
                ]
                result = helper.validate_dataframe(
                    df=silver_df,
                    suite_name=f"{args.feed_id}_silver_transform",
                    data_asset_name=contract_config.get("silver_table", args.feed_id),
                    expectations=expectations,
                    batch_identifier=args.execution_id,
                )
                print(f"Post-transform validation: {result.passed}/{result.total} passed")

                pg_conn = args.pg_connection_string
                if pg_conn:
                    from dag_utilities.validation.ge_result_writer import GEResultWriter
                    writer = GEResultWriter(pg_conn)
                    writer.write_results(
                        feed_id=args.feed_id, run_id=args.execution_id,
                        suite_name=f"{args.feed_id}_silver_transform",
                        validation_type="TRANSFORM", zone_level="SILVER",
                        checkpoint_result=result, batch_identifier=args.execution_id,
                    )
            except Exception as e:
                print(f"Warning: Post-transform validation failed: {e}")

        return 0

    except Exception as e:
        print(f"Error in bronze_to_silver: {e}")
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
