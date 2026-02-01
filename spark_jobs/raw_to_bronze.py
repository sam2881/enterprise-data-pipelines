#!/usr/bin/env python3
"""
APEX Data Agent - Raw to Bronze PySpark Job

Transforms raw data files into Bronze zone with:
- Schema enforcement (from metadata)
- Data type casting
- Basic cleansing (trim, null handling)
- Audit columns addition
- Partitioning

This is one of the 5 canonical Spark jobs.
ALL Bronze processing MUST use this job.
"""

import argparse
import sys
from datetime import datetime
from typing import Dict, Any, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, input_file_name,
    trim, when, to_date, to_timestamp, substring,
    expr,
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    LongType, DoubleType, DateType, TimestampType, DecimalType, BooleanType,
)


# ═══════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Raw to Bronze transformation")
    parser.add_argument("--feed-id", required=True, help="Feed ID")
    parser.add_argument("--contract-id", required=True, help="Contract ID")
    parser.add_argument("--execution-id", required=True, help="Execution ID")
    parser.add_argument("--execution-date", required=True, help="Execution date")
    parser.add_argument("--metadata-db", required=True, help="Metadata DB connection string")
    parser.add_argument("--validate", action="store_true", help="Run post-ingest row-count validation")
    parser.add_argument("--pg-connection-string", default="", help="PostgreSQL connection for validation results")
    return parser.parse_args()


# ═══════════════════════════════════════════════════════════════════════════
# METADATA LOADING
# ═══════════════════════════════════════════════════════════════════════════

def load_contract_config(spark: SparkSession, metadata_db: str, contract_id: str) -> Dict[str, Any]:
    """Load contract configuration from metadata."""
    df = spark.read \
        .format("jdbc") \
        .option("url", metadata_db) \
        .option("dbtable", f"(SELECT * FROM data_contract WHERE contract_id = '{contract_id}') AS contract") \
        .load()

    row = df.first()
    return {
        "raw_path": row["raw_path"],
        "bronze_path": row["bronze_path"],
        "bronze_table": row["bronze_table"],
        "file_format": row["file_format"],
        "primary_keys": row["primary_keys"],
        "partition_columns": row["partition_columns"],
        "load_type": row["load_type"],
    }


def load_schema_config(spark: SparkSession, metadata_db: str, contract_id: str) -> Dict[str, Any]:
    """Load current schema configuration from metadata."""
    df = spark.read \
        .format("jdbc") \
        .option("url", metadata_db) \
        .option("dbtable", f"""(
            SELECT * FROM schema_version
            WHERE contract_id = '{contract_id}' AND is_current = true
        ) AS schema""") \
        .load()

    row = df.first()
    return {
        "schema_json": row["schema_json"],
        "col_delimiter": row["col_delimiter"],
        "header_rows": row["header_rows"],
        "encoding": row["encoding"],
        "copybook_content": row["copybook_content"],
    }


# ═══════════════════════════════════════════════════════════════════════════
# SCHEMA MAPPING
# ═══════════════════════════════════════════════════════════════════════════

TYPE_MAPPING = {
    "STRING": StringType(),
    "VARCHAR": StringType(),
    "CHAR": StringType(),
    "INTEGER": IntegerType(),
    "INT": IntegerType(),
    "BIGINT": LongType(),
    "LONG": LongType(),
    "DOUBLE": DoubleType(),
    "FLOAT": DoubleType(),
    "DECIMAL": DecimalType(38, 10),
    "DATE": DateType(),
    "TIMESTAMP": TimestampType(),
    "BOOLEAN": BooleanType(),
    "BOOL": BooleanType(),
}


def build_spark_schema(schema_json: list) -> StructType:
    """Build Spark schema from metadata JSON."""
    fields = []
    for col_def in schema_json:
        col_name = col_def["column"]
        col_type_str = col_def["type"].upper().split("(")[0]  # Remove precision
        nullable = col_def.get("nullable", True)

        spark_type = TYPE_MAPPING.get(col_type_str, StringType())
        fields.append(StructField(col_name, spark_type, nullable))

    return StructType(fields)


def cast_columns(df: DataFrame, schema_json: list) -> DataFrame:
    """Cast columns to target types."""
    for col_def in schema_json:
        col_name = col_def["column"]
        col_type_str = col_def["type"].upper().split("(")[0]

        if col_name not in df.columns:
            continue

        if col_type_str in ["INTEGER", "INT"]:
            df = df.withColumn(col_name, col(col_name).cast(IntegerType()))
        elif col_type_str in ["BIGINT", "LONG"]:
            df = df.withColumn(col_name, col(col_name).cast(LongType()))
        elif col_type_str in ["DOUBLE", "FLOAT"]:
            df = df.withColumn(col_name, col(col_name).cast(DoubleType()))
        elif col_type_str == "DECIMAL":
            df = df.withColumn(col_name, col(col_name).cast(DecimalType(38, 10)))
        elif col_type_str == "DATE":
            df = df.withColumn(col_name, to_date(col(col_name)))
        elif col_type_str == "TIMESTAMP":
            df = df.withColumn(col_name, to_timestamp(col(col_name)))
        elif col_type_str in ["BOOLEAN", "BOOL"]:
            df = df.withColumn(col_name, col(col_name).cast(BooleanType()))
        else:
            # String type - trim whitespace
            df = df.withColumn(col_name, trim(col(col_name)))

    return df


# ═══════════════════════════════════════════════════════════════════════════
# DATA READING
# ═══════════════════════════════════════════════════════════════════════════

def read_raw_data(
    spark: SparkSession,
    raw_path: str,
    file_format: str,
    schema_config: Dict[str, Any],
    execution_id: str,
) -> DataFrame:
    """Read raw data files."""
    if file_format.upper() == "CSV":
        df = spark.read \
            .option("delimiter", schema_config.get("col_delimiter", ",")) \
            .option("header", schema_config.get("header_rows", 1) > 0) \
            .option("encoding", schema_config.get("encoding", "UTF-8")) \
            .option("mode", "PERMISSIVE") \
            .option("columnNameOfCorruptRecord", "_corrupt_record") \
            .csv(f"{raw_path}/{execution_id}/*")

    elif file_format.upper() == "JSON":
        df = spark.read \
            .option("mode", "PERMISSIVE") \
            .option("columnNameOfCorruptRecord", "_corrupt_record") \
            .json(f"{raw_path}/{execution_id}/*")

    elif file_format.upper() == "PARQUET":
        df = spark.read.parquet(f"{raw_path}/{execution_id}/*")

    elif file_format.upper() == "AVRO":
        df = spark.read.format("avro").load(f"{raw_path}/{execution_id}/*")

    elif file_format.upper() in ["EBCDIC", "COBOL"]:
        # Use Cobrix for EBCDIC files
        copybook = schema_config.get("copybook_content", "")
        df = spark.read \
            .format("cobol") \
            .option("copybook_contents", copybook) \
            .option("encoding", "cp037") \
            .option("schema_retention_policy", "collapse_root") \
            .load(f"{raw_path}/{execution_id}/*")

    elif file_format.upper() == "ORC":
        df = spark.read.orc(f"{raw_path}/{execution_id}/*")

    elif file_format.upper() == "XML":
        # Requires spark-xml package (com.databricks:spark-xml)
        row_tag = schema_config.get("xml_row_tag", "row")
        df = spark.read \
            .format("xml") \
            .option("rowTag", row_tag) \
            .option("encoding", schema_config.get("encoding", "UTF-8")) \
            .load(f"{raw_path}/{execution_id}/*")

    elif file_format.upper() == "EXCEL":
        # Requires spark-excel package (com.crealytics:spark-excel)
        header = schema_config.get("header_rows", 1) > 0
        sheet_name = schema_config.get("sheet_name", "Sheet1")
        df = spark.read \
            .format("com.crealytics.spark.excel") \
            .option("header", str(header).lower()) \
            .option("dataAddress", f"'{sheet_name}'!A1") \
            .option("inferSchema", "false") \
            .load(f"{raw_path}/{execution_id}/*")

    elif file_format.upper() == "FIXED_WIDTH":
        # Read as single-column text, then split by positional widths from metadata
        import json
        raw_df = spark.read.text(f"{raw_path}/{execution_id}/*")
        schema_json = json.loads(schema_config["schema_json"]) if isinstance(
            schema_config["schema_json"], str
        ) else schema_config["schema_json"]

        # Build positional substring expressions from column widths
        pos = 1
        select_exprs = []
        for col_def in schema_json:
            width = col_def.get("width", col_def.get("length", 10))
            select_exprs.append(
                trim(substring(col("value"), pos, width)).alias(col_def["column"])
            )
            pos += width

        df = raw_df.select(*select_exprs)

    else:
        raise ValueError(f"Unsupported file format: {file_format}")

    return df


# ═══════════════════════════════════════════════════════════════════════════
# AUDIT COLUMNS
# ═══════════════════════════════════════════════════════════════════════════

def add_audit_columns(
    df: DataFrame,
    execution_id: str,
    execution_date: str,
) -> DataFrame:
    """
    Add audit columns for Bronze zone.

    Mandatory system fields (non-negotiable):
      - _run_id:          UUID of this execution run (same for all rows in batch)
      - _record_uuid:     Unique UUID per row (generated via Spark uuid())
      - _reporting_date:  Business reporting date (= execution_date)
      - _load_timestamp:  Timestamp when data was loaded (= current_timestamp)

    Legacy audit fields (kept for backward compatibility):
      - _execution_id, _ingestion_ts, _execution_date, _source_file, _is_current
    """
    return df \
        .withColumn("_run_id", lit(execution_id)) \
        .withColumn("_record_uuid", expr("uuid()")) \
        .withColumn("_reporting_date", lit(execution_date)) \
        .withColumn("_load_timestamp", current_timestamp()) \
        .withColumn("_execution_id", lit(execution_id)) \
        .withColumn("_ingestion_ts", current_timestamp()) \
        .withColumn("_execution_date", lit(execution_date)) \
        .withColumn("_source_file", input_file_name()) \
        .withColumn("_is_current", lit(True))


# ═══════════════════════════════════════════════════════════════════════════
# MAIN PROCESSING
# ═══════════════════════════════════════════════════════════════════════════

def process_raw_to_bronze(
    spark: SparkSession,
    contract_config: Dict[str, Any],
    schema_config: Dict[str, Any],
    execution_id: str,
    execution_date: str,
) -> Dict[str, Any]:
    """
    Process raw data to Bronze zone.

    Returns:
        Dict with processing metrics
    """
    # Read raw data
    raw_df = read_raw_data(
        spark=spark,
        raw_path=contract_config["raw_path"],
        file_format=contract_config["file_format"],
        schema_config=schema_config,
        execution_id=execution_id,
    )

    initial_count = raw_df.count()

    # Parse schema from metadata
    import json
    schema_json = json.loads(schema_config["schema_json"]) if isinstance(
        schema_config["schema_json"], str
    ) else schema_config["schema_json"]

    # Separate corrupt/rejected records from valid records
    rejected_count = 0
    if "_corrupt_record" in raw_df.columns:
        rejected_df = raw_df.filter(col("_corrupt_record").isNotNull())
        rejected_count = rejected_df.count()

        if rejected_count > 0:
            # Write rejected records to reject path
            reject_path = contract_config.get("reject_path") or (
                contract_config["bronze_path"].rstrip("/") + "_rejects"
            )
            rejected_df.withColumn("_reject_reason", lit("corrupt_record")) \
                .withColumn("_reject_execution_id", lit(execution_id)) \
                .withColumn("_reject_ts", current_timestamp()) \
                .write \
                .format("delta") \
                .mode("append") \
                .partitionBy("_execution_date" if "_execution_date" in rejected_df.columns else []) \
                .save(f"{reject_path}/{execution_id}")

        # Keep only valid records
        raw_df = raw_df.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")

    # Schema evolution check
    try:
        from quality.schema_evolution import validate_schema_evolution
        registered_cols = {
            col_def["name"]: col_def.get("type", "string")
            for col_def in schema_json.get("columns", schema_json) if isinstance(col_def, dict)
        }
        incoming_cols = {f.name: f.dataType.simpleString() for f in raw_df.schema.fields
                        if not f.name.startswith("_")}
        policy = contract_config.get("schema_evolution_policy", "ADDITIVE")
        evolution_result = validate_schema_evolution(registered_cols, incoming_cols, policy)
        if not evolution_result.is_valid:
            raise ValueError(
                f"Schema evolution failed ({policy}): "
                + "; ".join(evolution_result.errors)
            )
        if evolution_result.warnings:
            for w in evolution_result.warnings:
                print(f"Schema warning: {w}")
    except ImportError:
        pass  # Module not available in this environment

    # Cast columns to target types
    typed_df = cast_columns(raw_df, schema_json)

    # Add audit columns
    bronze_df = add_audit_columns(typed_df, execution_id, execution_date)

    # Determine write mode based on load type
    write_mode = "append" if contract_config["load_type"] == "APPEND" else "overwrite"

    # Determine table format (default: delta, supports iceberg/parquet)
    table_format = contract_config.get("table_format", "delta")

    # Idempotent re-run: delete existing data for this execution_date before writing
    if table_format == "delta":
        try:
            from delta.tables import DeltaTable
            if contract_config.get("bronze_table"):
                dt = DeltaTable.forName(spark, contract_config["bronze_table"])
            else:
                dt = DeltaTable.forPath(spark, contract_config["bronze_path"])
            dt.delete(f"_execution_date = '{execution_date}'")
            print(f"Deleted existing Bronze data for execution_date={execution_date}")
        except Exception:
            pass  # Table doesn't exist yet on first run
    elif table_format == "iceberg":
        try:
            spark.sql(f"""
                DELETE FROM {contract_config['bronze_table']}
                WHERE _execution_date = '{execution_date}'
            """)
            print(f"Deleted existing Bronze (Iceberg) data for execution_date={execution_date}")
        except Exception:
            pass

    # Route rejected records to DLQ path if configured
    dlq_path = contract_config.get("dlq_path")
    if dlq_path and rejected_count > 0:
        try:
            rejected_df.write.format("json").mode("append").save(
                f"{dlq_path}/{execution_date}/{execution_id}"
            )
            print(f"Wrote {rejected_count} rejected records to DLQ: {dlq_path}")
        except Exception as e:
            print(f"Warning: DLQ write failed: {e}")

    # Write to Bronze
    partition_cols = contract_config.get("partition_columns") or ["_execution_date"]

    writer = bronze_df.write \
        .format(table_format) \
        .mode(write_mode) \
        .partitionBy(*partition_cols)

    # Write to path or table
    if contract_config.get("bronze_table"):
        writer.saveAsTable(contract_config["bronze_table"])
    else:
        writer.save(contract_config["bronze_path"])

    final_count = bronze_df.count()

    return {
        "records_read": initial_count,
        "records_written": final_count,
        "records_rejected": rejected_count,
    }


def main():
    """Main entry point."""
    args = parse_args()

    # Create Spark session
    spark = SparkSession.builder \
        .appName(f"raw_to_bronze_{args.feed_id}") \
        .enableHiveSupport() \
        .getOrCreate()

    try:
        # Load configurations from metadata
        contract_config = load_contract_config(
            spark, args.metadata_db, args.contract_id
        )
        schema_config = load_schema_config(
            spark, args.metadata_db, args.contract_id
        )

        # Process data
        metrics = process_raw_to_bronze(
            spark=spark,
            contract_config=contract_config,
            schema_config=schema_config,
            execution_id=args.execution_id,
            execution_date=args.execution_date,
        )

        print(f"Raw to Bronze completed: {metrics}")

        # Optional post-ingest validation
        if args.validate and metrics["records_written"] > 0:
            try:
                from dag_utilities.validation.ge_helper import GEHelper
                helper = GEHelper(spark)

                # Load bronze data just written
                if contract_config.get("bronze_table"):
                    bronze_df = spark.table(contract_config["bronze_table"])
                else:
                    bronze_df = spark.read.format("delta").load(contract_config["bronze_path"])
                bronze_df = bronze_df.filter(col("_execution_id") == args.execution_id)

                expectations = [
                    {
                        "expectation_type": "expect_table_row_count_to_be_between",
                        "kwargs": {"min_value": 1},
                        "meta": {"rule_name": "bronze_not_empty", "severity": "error", "weight": "1.0"},
                    },
                ]
                result = helper.validate_dataframe(
                    df=bronze_df,
                    suite_name=f"{args.feed_id}_bronze_ingest",
                    data_asset_name=contract_config.get("bronze_table", args.feed_id),
                    expectations=expectations,
                    batch_identifier=args.execution_id,
                )
                print(f"Post-ingest validation: {result.passed}/{result.total} passed")

                pg_conn = args.pg_connection_string
                if pg_conn:
                    from dag_utilities.validation.ge_result_writer import GEResultWriter
                    writer = GEResultWriter(pg_conn)
                    writer.write_results(
                        feed_id=args.feed_id, run_id=args.execution_id,
                        suite_name=f"{args.feed_id}_bronze_ingest",
                        validation_type="INGEST", zone_level="BRONZE",
                        checkpoint_result=result, batch_identifier=args.execution_id,
                    )
            except Exception as e:
                print(f"Warning: Post-ingest validation failed: {e}")

        return 0

    except Exception as e:
        print(f"Error in raw_to_bronze: {e}")
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
