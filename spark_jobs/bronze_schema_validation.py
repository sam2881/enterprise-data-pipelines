#!/usr/bin/env python3
"""
APEX Data Agent - Bronze Schema Validation PySpark Job

Validates Bronze zone data against schema definition using
Great Expectations (with PySpark fallback). Results are persisted
to ge_validation_result and ge_validation_summary tables.

Validation checks:
- Column presence
- Data type compatibility
- Not null constraints
- Primary key uniqueness
- Custom rules from metadata

This is one of the 5 canonical Spark jobs.
"""

import argparse
import sys
import json
from datetime import datetime
from typing import Dict, Any, List, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col


# ═══════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Bronze Schema Validation")
    parser.add_argument("--feed-id", required=True, help="Feed ID")
    parser.add_argument("--contract-id", required=True, help="Contract ID")
    parser.add_argument("--execution-id", required=True, help="Execution ID")
    parser.add_argument("--metadata-db", required=True, help="Metadata DB JDBC URL")
    parser.add_argument("--batch-identifier", default="", help="Batch/partition identifier")
    parser.add_argument("--pg-connection-string", default="", help="PostgreSQL connection for result persistence")
    parser.add_argument("--header-rows-skip", type=int, default=0, help="Header rows to skip")
    parser.add_argument("--footer-rows-skip", type=int, default=0, help="Footer rows to skip")
    return parser.parse_args()


# ═══════════════════════════════════════════════════════════════════════════
# METADATA LOADING
# ═══════════════════════════════════════════════════════════════════════════

def load_validation_rules(spark: SparkSession, metadata_db: str, contract_id: str) -> List[Dict[str, Any]]:
    """Load validation rules for Bronze zone."""
    try:
        df = spark.read \
            .format("jdbc") \
            .option("url", metadata_db) \
            .option("dbtable", f"""(
                SELECT * FROM validation_rule
                WHERE contract_id = '{contract_id}'
                  AND zone_level = 'BRONZE'
                  AND is_active = true
            ) AS rules""") \
            .load()
        return [row.asDict() for row in df.collect()]
    except Exception as e:
        print(f"Warning: Could not load validation rules: {e}")
        return []


def load_quality_rules(spark: SparkSession, metadata_db: str, contract_id: str) -> List[Dict[str, Any]]:
    """Load quality expectations for Bronze zone."""
    try:
        df = spark.read \
            .format("jdbc") \
            .option("url", metadata_db) \
            .option("dbtable", f"""(
                SELECT * FROM quality_expectation
                WHERE contract_id = '{contract_id}'
                  AND zone_level = 'BRONZE'
                  AND is_active = true
            ) AS rules""") \
            .load()
        return [row.asDict() for row in df.collect()]
    except Exception as e:
        print(f"Warning: Could not load quality expectations: {e}")
        return []


def load_schema_columns(spark: SparkSession, metadata_db: str, contract_id: str) -> List[Dict[str, Any]]:
    """Load schema column definitions."""
    try:
        df = spark.read \
            .format("jdbc") \
            .option("url", metadata_db) \
            .option("dbtable", f"""(
                SELECT schema_json FROM schema_version
                WHERE contract_id = '{contract_id}' AND is_current = true
            ) AS schema""") \
            .load()

        row = df.first()
        if row:
            schema_json = row["schema_json"]
            if isinstance(schema_json, str):
                return json.loads(schema_json)
            return schema_json
    except Exception as e:
        print(f"Warning: Could not load schema columns: {e}")
    return []


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
        "primary_keys": row["primary_keys"] or [],
    }


# ═══════════════════════════════════════════════════════════════════════════
# BUILD GE EXPECTATIONS FROM METADATA
# ═══════════════════════════════════════════════════════════════════════════

def build_expectations(
    schema_columns: List[Dict[str, Any]],
    primary_keys: List[str],
    validation_rules: List[Dict[str, Any]],
    quality_rules: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Build a complete list of GE expectations from metadata.

    Combines:
    1. Schema-derived expectations (column exists, not null, type checks)
    2. Primary key uniqueness
    3. Metadata validation_rule rows
    4. Metadata quality_expectation rows
    """
    from dag_utilities.validation.ge_configs import GEConfigBuilder

    builder = GEConfigBuilder()

    # 1. Schema expectations (column exists + not null)
    expectations = builder.build_schema_expectations(schema_columns)

    # 2. Primary key uniqueness
    if primary_keys:
        if len(primary_keys) == 1:
            expectations.append({
                "expectation_type": "expect_column_values_to_be_unique",
                "kwargs": {"column": primary_keys[0], "mostly": 1.0},
                "meta": {
                    "rule_name": f"pk_unique_{primary_keys[0]}",
                    "severity": "error",
                    "weight": "2.0",
                    "original_rule_type": "unique",
                },
            })
        else:
            expectations.append({
                "expectation_type": "expect_compound_columns_to_be_unique",
                "kwargs": {"column_list": primary_keys},
                "meta": {
                    "rule_name": f"pk_unique_{'+'.join(primary_keys)}",
                    "severity": "error",
                    "weight": "2.0",
                    "original_rule_type": "compound_unique",
                },
            })

    # 3. Validation rules from metadata
    expectations.extend(builder.build_expectations_from_rules(validation_rules))

    # 4. Quality rules from metadata
    expectations.extend(builder.build_expectations_from_rules(quality_rules))

    return expectations


# ═══════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════

def main():
    """Main entry point."""
    args = parse_args()

    spark = SparkSession.builder \
        .appName(f"bronze_validation_{args.feed_id}") \
        .enableHiveSupport() \
        .getOrCreate()

    try:
        # Load configurations
        contract_config = load_contract_config(spark, args.metadata_db, args.contract_id)
        schema_columns = load_schema_columns(spark, args.metadata_db, args.contract_id)
        validation_rules = load_validation_rules(spark, args.metadata_db, args.contract_id)
        quality_rules = load_quality_rules(spark, args.metadata_db, args.contract_id)

        # Load Bronze data
        if contract_config.get("bronze_table"):
            bronze_df = spark.table(contract_config["bronze_table"])
        else:
            bronze_df = spark.read.format("delta").load(contract_config["bronze_path"])

        # Filter to current execution
        if "_execution_id" in bronze_df.columns:
            bronze_df = bronze_df.filter(col("_execution_id") == args.execution_id)

        # Build GE expectations from all metadata sources
        expectations = build_expectations(
            schema_columns=schema_columns,
            primary_keys=contract_config.get("primary_keys", []),
            validation_rules=validation_rules,
            quality_rules=quality_rules,
        )

        # Run validation via GEHelper (GE or PySpark fallback)
        from dag_utilities.validation.ge_helper import GEHelper

        helper = GEHelper(spark)
        result = helper.validate_dataframe(
            df=bronze_df,
            suite_name=f"{args.feed_id}_bronze_schema",
            data_asset_name=contract_config.get("bronze_table", args.feed_id),
            expectations=expectations,
            batch_identifier=args.batch_identifier or args.execution_id,
            header_rows_to_skip=args.header_rows_skip,
            footer_rows_to_skip=args.footer_rows_skip,
        )

        # Persist results to PostgreSQL
        pg_conn = args.pg_connection_string
        if pg_conn:
            try:
                from dag_utilities.validation.ge_result_writer import GEResultWriter
                writer = GEResultWriter(pg_conn)
                writer.write_results(
                    feed_id=args.feed_id,
                    run_id=args.execution_id,
                    suite_name=f"{args.feed_id}_bronze_schema",
                    validation_type="SCHEMA",
                    zone_level="BRONZE",
                    checkpoint_result=result,
                    batch_identifier=args.batch_identifier,
                )
            except Exception as e:
                print(f"Warning: Could not persist results: {e}")
        else:
            # Fallback: persist via Spark JDBC
            try:
                jdbc_url = args.metadata_db
                jdbc_props = {"driver": "org.postgresql.Driver"}
                from dag_utilities.validation.ge_result_writer import GEResultWriter
                writer = GEResultWriter.__new__(GEResultWriter)
                writer.write_results_spark(
                    spark=spark,
                    feed_id=args.feed_id,
                    run_id=args.execution_id,
                    suite_name=f"{args.feed_id}_bronze_schema",
                    validation_type="SCHEMA",
                    zone_level="BRONZE",
                    checkpoint_result=result,
                    jdbc_url=jdbc_url,
                    jdbc_properties=jdbc_props,
                    batch_identifier=args.batch_identifier,
                )
            except Exception as e:
                print(f"Warning: Could not persist results via JDBC: {e}")

        # Report
        print(f"Bronze validation: {result.passed}/{result.total} passed "
              f"(score: {result.quality_score:.1f}/100)")

        blocking = result.get_blocking_failures()
        if blocking:
            print(f"BLOCKING FAILURES ({len(blocking)}):")
            for b in blocking:
                print(f"  - {b.rule_name}: {b.expectation_type} on {b.column}")
            sys.exit(1)

        return 0

    except Exception as e:
        print(f"Error in bronze_validation: {e}")
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
