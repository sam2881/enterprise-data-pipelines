"""
Great Expectations Schema Validation Spark Job (v2)

Production-grade schema validation using Great Expectations integrated with
Apache Spark. Reads data from GCS in multiple formats, runs GE checkpoint
validations, and persists results to PostgreSQL.

Usage:
    spark-submit validate_schema_with_great_expectations.py <feed_id> <batch_identifier> <data_asset_name> \
        <pg_host> <pg_schema> <pg_user> <pg_pw> <pg_port> <pg_extra> \
        <run_id_seq> <validations_store_prefix> <data_docs_prefix> \
        <file_format> <header_details> <validation_type> <checkpoint_name> \
        <transient_path> <dag_start_date> <local_tz> <footer_details> \
        <encoding_details> [read_as_csv] [column_delimiter]
"""

import os
import sys
import json
import logging
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    FloatType,
)

from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    DatasourceConfig,
    FilesystemStoreBackendDefaults,
)
from great_expectations.checkpoint import SimpleCheckpoint

from ge_helper import (
    build_expectation_suite_name,
    build_run_name,
    build_checkpoint_config,
    get_pg_jdbc_url,
    get_pg_properties,
)
from ge_configs import (
    build_datasource_config,
    build_stores_config,
    build_data_docs_config,
    build_evaluation_parameter_store_config,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("ge_schema_validator")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
VALIDATION_RESULT_TABLE = "ge_validation_result"
VALIDATION_SUMMARY_TABLE = "ge_validation_summary"

RESULT_SCHEMA = StructType(
    [
        StructField("feed_id", IntegerType(), False),
        StructField("run_id", StringType(), False),
        StructField("sequence", IntegerType(), False),
        StructField("validation_type", StringType(), False),
        StructField("batch_identifier", StringType(), False),
        StructField("expectation_suite_name", StringType(), True),
        StructField("column_name", StringType(), True),
        StructField("expectation_type", StringType(), True),
        StructField("result", StringType(), True),
        StructField("data_asset_name", StringType(), False),
        StructField("run_name", StringType(), True),
        StructField("run_time", StringType(), True),
        StructField("error_count", IntegerType(), True),
        StructField("error_rate", FloatType(), True),
        StructField("partial_unexpected_list", StringType(), True),
    ]
)

SUMMARY_SCHEMA = StructType(
    [
        StructField("feed_id", IntegerType(), False),
        StructField("run_id", StringType(), False),
        StructField("sequence", IntegerType(), False),
        StructField("validation_type", StringType(), False),
        StructField("batch_identifier", StringType(), False),
        StructField("data_asset_name", StringType(), False),
        StructField("row_count", IntegerType(), True),
        StructField("error_count", IntegerType(), True),
        StructField("result", StringType(), True),
        StructField("created_date", StringType(), True),
    ]
)


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
def parse_arguments() -> dict:
    """Parse positional command-line arguments into a configuration dict."""
    args = sys.argv[1:]
    if len(args) < 21:
        raise ValueError(
            f"Expected at least 21 arguments, received {len(args)}. "
            "See module docstring for usage."
        )

    config = {
        "feed_id": int(args[0]),
        "batch_identifier": args[1],
        "data_asset_name": args[2],
        "pg_host": args[3],
        "pg_schema": args[4],
        "pg_user": args[5],
        "pg_pw": args[6],
        "pg_port": args[7],
        "pg_extra": args[8],
        "run_id_seq": args[9],
        "validations_store_prefix": args[10],
        "data_docs_prefix": args[11],
        "file_format": args[12].lower(),
        "header_details": args[13].upper(),
        "validation_type": args[14],
        "checkpoint_name": args[15],
        "transient_path": args[16],
        "dag_start_date": args[17],
        "local_tz": args[18],
        "footer_details": args[19].upper(),
        "encoding_details": args[20],
        "read_as_csv": args[21].upper() if len(args) > 21 else "N",
        "column_delimiter": args[22] if len(args) > 22 else ",",
    }

    # Derive run_id and sequence from run_id_seq  (format: "{run_id}_{sequence}")
    parts = config["run_id_seq"].rsplit("_", 1)
    config["run_id"] = parts[0]
    config["sequence"] = int(parts[1]) if len(parts) > 1 else 0

    return config


# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------
def get_spark_session(app_name: str = "ge_schema_validator") -> SparkSession:
    """Return or create a SparkSession."""
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Data readers
# ---------------------------------------------------------------------------
def _bucket_path() -> str:
    bucket = os.environ.get("BUCKET_NAME", "")
    if not bucket:
        raise EnvironmentError("BUCKET_NAME environment variable is not set.")
    return f"gs://{bucket}" if not bucket.startswith("gs://") else bucket


def _strip_header(df: DataFrame) -> DataFrame:
    """Remove the first row (header row embedded in data)."""
    first_row = df.first()
    if first_row is None:
        return df
    return df.subtract(df.limit(1))


def _strip_footer(df: DataFrame) -> DataFrame:
    """Remove the last row (footer/trailer row)."""
    row_count = df.count()
    if row_count <= 1:
        return df
    df_with_idx = df.withColumn("__row_idx__", F.monotonically_increasing_id())
    max_idx = df_with_idx.agg(F.max("__row_idx__")).collect()[0][0]
    return df_with_idx.filter(F.col("__row_idx__") != max_idx).drop("__row_idx__")


def _apply_encoding(df: DataFrame, encoding: str) -> DataFrame:
    """Encode the first column value using the specified encoding."""
    if not encoding or encoding.upper() == "NONE":
        return df
    first_col = df.columns[0]
    return df.withColumn(
        first_col,
        F.encode(F.col(first_col), encoding),
    )


def read_data(spark: SparkSession, config: dict) -> DataFrame:
    """Read source data from GCS based on file_format and flags."""
    file_format = config["file_format"]
    transient_path = config["transient_path"]
    header_details = config["header_details"]
    footer_details = config["footer_details"]
    encoding_details = config["encoding_details"]
    read_as_csv = config["read_as_csv"]
    delimiter = config["column_delimiter"]

    logger.info(
        "Reading data: format=%s, path=%s, header=%s, footer=%s, encoding=%s",
        file_format,
        transient_path,
        header_details,
        footer_details,
        encoding_details,
    )

    df: DataFrame

    if file_format == "csv":
        df = (
            spark.read.option("header", header_details == "Y")
            .option("delimiter", delimiter)
            .option("inferSchema", "false")
            .csv(transient_path)
        )
        # When header is embedded, Spark handles it with the header option.
        # Footer still needs manual removal.
        if footer_details == "Y":
            df = _strip_footer(df)

    elif file_format in ("fixed", "delimiter"):
        if read_as_csv == "Y":
            df = (
                spark.read.option("header", "false")
                .option("delimiter", delimiter)
                .option("inferSchema", "false")
                .csv(transient_path)
            )
        else:
            df = spark.read.text(transient_path)

        if header_details == "Y":
            df = _strip_header(df)
        if footer_details == "Y":
            df = _strip_footer(df)

    elif file_format == "cobrix":
        # Cobrix EBCDIC files are pre-converted to pipe-delimited CSV
        df = (
            spark.read.option("header", "false")
            .option("delimiter", "|")
            .option("inferSchema", "false")
            .csv(transient_path)
        )
        if header_details == "Y":
            df = _strip_header(df)
        if footer_details == "Y":
            df = _strip_footer(df)

    elif file_format == "parquet":
        df = spark.read.parquet(transient_path)

    elif file_format == "json":
        df = spark.read.option("multiLine", "true").json(transient_path)
        # Cast all columns to string for schema validation uniformity
        for col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(StringType()))

    else:
        raise ValueError(f"Unsupported file format: {file_format}")

    # Apply encoding if specified
    df = _apply_encoding(df, encoding_details)

    row_count = df.count()
    logger.info("Loaded %d rows from %s", row_count, transient_path)
    return df


# ---------------------------------------------------------------------------
# Great Expectations context
# ---------------------------------------------------------------------------
def build_data_context(config: dict) -> BaseDataContext:
    """Build a GE BaseDataContext with GCS stores and PostgreSQL evaluation
    parameter store."""

    pg_jdbc_url = get_pg_jdbc_url(
        host=config["pg_host"],
        port=config["pg_port"],
        schema=config["pg_schema"],
        extra=config["pg_extra"],
    )

    stores_config = build_stores_config(
        validations_store_prefix=config["validations_store_prefix"],
    )

    data_docs_config = build_data_docs_config(
        data_docs_prefix=config["data_docs_prefix"],
    )

    evaluation_parameter_store = build_evaluation_parameter_store_config(
        pg_host=config["pg_host"],
        pg_port=config["pg_port"],
        pg_schema=config["pg_schema"],
        pg_user=config["pg_user"],
        pg_pw=config["pg_pw"],
    )

    datasource_config = build_datasource_config()

    data_context_config = DataContextConfig(
        datasources={"spark_datasource": datasource_config},
        stores=stores_config,
        data_docs_sites=data_docs_config,
        evaluation_parameter_store_name="evaluation_parameter_store",
        validations_store_name="validations_GCS_store",
        expectations_store_name="expectations_store",
    )

    context = BaseDataContext(project_config=data_context_config)
    return context


# ---------------------------------------------------------------------------
# Validation execution
# ---------------------------------------------------------------------------
def run_validation(
    context: BaseDataContext,
    df: DataFrame,
    config: dict,
) -> dict:
    """Create a RuntimeBatchRequest, build a checkpoint, and run validation."""

    expectation_suite_name = build_expectation_suite_name(
        feed_id=config["feed_id"],
        data_asset_name=config["data_asset_name"],
        validation_type=config["validation_type"],
    )

    run_name = build_run_name(
        feed_id=config["feed_id"],
        run_id=config["run_id"],
        dag_start_date=config["dag_start_date"],
    )

    batch_request = RuntimeBatchRequest(
        datasource_name="spark_datasource",
        data_connector_name="runtime_data_connector",
        data_asset_name=config["data_asset_name"],
        runtime_parameters={"batch_data": df},
        batch_identifiers={"batch_identifier": config["batch_identifier"]},
    )

    checkpoint_config = build_checkpoint_config(
        checkpoint_name=config["checkpoint_name"],
        expectation_suite_name=expectation_suite_name,
        batch_request=batch_request,
        run_name=run_name,
    )

    context.add_checkpoint(**checkpoint_config)

    logger.info(
        "Running checkpoint '%s' with suite '%s'",
        config["checkpoint_name"],
        expectation_suite_name,
    )

    result = context.run_checkpoint(
        checkpoint_name=config["checkpoint_name"],
    )

    return result


# ---------------------------------------------------------------------------
# Result processing
# ---------------------------------------------------------------------------
def build_result_rows(validation_result: dict, config: dict) -> list:
    """Extract individual expectation results into flat row dicts."""
    rows = []

    run_results = validation_result.get("run_results", {})
    for _key, run_result in run_results.items():
        validation = run_result.get("validation_result", {})
        suite_name = validation.get("meta", {}).get("expectation_suite_name", "")
        run_name = validation.get("meta", {}).get("run_id", {}).get("run_name", "")
        run_time = validation.get("meta", {}).get("run_id", {}).get("run_time", "")

        results_list = validation.get("results", [])
        for exp_result in results_list:
            expectation_config = exp_result.get("expectation_config", {})
            result_block = exp_result.get("result", {})

            expectation_type = expectation_config.get("expectation_type", "")
            column_name = expectation_config.get("kwargs", {}).get("column", "N/A")

            success = exp_result.get("success", False)
            result_str = "PASS" if success else "FAIL"

            error_count = int(result_block.get("unexpected_count", 0))
            element_count = result_block.get("element_count", 0)
            error_rate = (
                float(error_count) / float(element_count)
                if element_count and element_count > 0
                else 0.0
            )

            partial_unexpected = result_block.get(
                "partial_unexpected_list", []
            )
            partial_unexpected_str = (
                json.dumps(partial_unexpected) if partial_unexpected else "[]"
            )

            rows.append(
                {
                    "feed_id": config["feed_id"],
                    "run_id": config["run_id"],
                    "sequence": config["sequence"],
                    "validation_type": config["validation_type"],
                    "batch_identifier": config["batch_identifier"],
                    "expectation_suite_name": suite_name,
                    "column_name": str(column_name),
                    "expectation_type": expectation_type,
                    "result": result_str,
                    "data_asset_name": config["data_asset_name"],
                    "run_name": run_name,
                    "run_time": str(run_time),
                    "error_count": error_count,
                    "error_rate": round(error_rate, 6),
                    "partial_unexpected_list": partial_unexpected_str,
                }
            )

    return rows


def build_summary_row(
    validation_result: dict,
    config: dict,
    row_count: int,
) -> dict:
    """Build a single summary row from the overall validation result."""
    total_errors = 0
    overall_success = True

    run_results = validation_result.get("run_results", {})
    for _key, run_result in run_results.items():
        validation = run_result.get("validation_result", {})
        if not validation.get("success", True):
            overall_success = False
        for exp_result in validation.get("results", []):
            total_errors += int(
                exp_result.get("result", {}).get("unexpected_count", 0)
            )

    return {
        "feed_id": config["feed_id"],
        "run_id": config["run_id"],
        "sequence": config["sequence"],
        "validation_type": config["validation_type"],
        "batch_identifier": config["batch_identifier"],
        "data_asset_name": config["data_asset_name"],
        "row_count": row_count,
        "error_count": total_errors,
        "result": "PASS" if overall_success else "FAIL",
        "created_date": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
    }


# ---------------------------------------------------------------------------
# PostgreSQL writers
# ---------------------------------------------------------------------------
def write_to_postgres(
    spark: SparkSession,
    data: list,
    schema: StructType,
    table: str,
    config: dict,
) -> None:
    """Write a list of row dicts to a PostgreSQL table via JDBC."""
    if not data:
        logger.warning("No data to write to %s", table)
        return

    jdbc_url = get_pg_jdbc_url(
        host=config["pg_host"],
        port=config["pg_port"],
        schema=config["pg_schema"],
        extra=config["pg_extra"],
    )

    properties = get_pg_properties(
        user=config["pg_user"],
        password=config["pg_pw"],
    )

    df = spark.createDataFrame(data, schema=schema)

    logger.info("Writing %d rows to %s.%s", df.count(), config["pg_schema"], table)

    df.write.jdbc(
        url=jdbc_url,
        table=f"{config['pg_schema']}.{table}",
        mode="append",
        properties=properties,
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    """Orchestrate the full schema validation pipeline."""
    logger.info("Starting GE Schema Validator v2")

    # Parse arguments
    config = parse_arguments()
    logger.info(
        "Config: feed_id=%s, data_asset=%s, format=%s, validation=%s, run_id=%s, seq=%s",
        config["feed_id"],
        config["data_asset_name"],
        config["file_format"],
        config["validation_type"],
        config["run_id"],
        config["sequence"],
    )

    # Spark session
    spark = get_spark_session(
        app_name=f"ge_schema_validator_{config['feed_id']}_{config['run_id']}"
    )

    try:
        # Read source data
        df = read_data(spark, config)
        row_count = df.count()

        if row_count == 0:
            logger.warning("Source data is empty. Skipping validation.")
            summary = {
                "feed_id": config["feed_id"],
                "run_id": config["run_id"],
                "sequence": config["sequence"],
                "validation_type": config["validation_type"],
                "batch_identifier": config["batch_identifier"],
                "data_asset_name": config["data_asset_name"],
                "row_count": 0,
                "error_count": 0,
                "result": "SKIP",
                "created_date": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            }
            write_to_postgres(
                spark, [summary], SUMMARY_SCHEMA, VALIDATION_SUMMARY_TABLE, config
            )
            return

        # Build GE data context
        context = build_data_context(config)

        # Run checkpoint validation
        validation_result = run_validation(context, df, config)

        # Process results
        result_rows = build_result_rows(validation_result, config)
        summary_row = build_summary_row(validation_result, config, row_count)

        logger.info(
            "Validation complete: %s — %d expectations evaluated, overall=%s",
            config["data_asset_name"],
            len(result_rows),
            summary_row["result"],
        )

        # Persist to PostgreSQL
        write_to_postgres(
            spark, result_rows, RESULT_SCHEMA, VALIDATION_RESULT_TABLE, config
        )
        write_to_postgres(
            spark, [summary_row], SUMMARY_SCHEMA, VALIDATION_SUMMARY_TABLE, config
        )

        logger.info("GE Schema Validator v2 completed successfully.")

    except Exception:
        logger.exception("GE Schema Validator failed.")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
