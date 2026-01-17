#!/usr/bin/env python3
"""
Common Silver Processor

Reads Bronze data, applies type casting and validations from metadata.
Configuration is read from PostgreSQL - NO HARD-CODED LOGIC.

Usage:
    spark-submit silver_processor.py \
        --pipeline-id <id> \
        --reporting-date <date> \
        --run-id <run_id>
"""

import argparse
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *


def get_schema_definitions(spark, pipeline_id: str) -> list:
    """Read schema definitions from metadata."""
    jdbc_url = spark.conf.get("spark.pipeline.metadata.jdbc.url")

    schema_df = spark.read.jdbc(
        url=jdbc_url,
        table=f"(SELECT * FROM pipeline_metadata.schema_definitions WHERE pipeline_id = '{pipeline_id}' AND is_active = true ORDER BY column_order) as schema",
        properties={"driver": "org.postgresql.Driver"}
    )

    return [row.asDict() for row in schema_df.collect()]


def get_validation_rules(spark, pipeline_id: str) -> list:
    """Read validation rules from metadata."""
    jdbc_url = spark.conf.get("spark.pipeline.metadata.jdbc.url")

    rules_df = spark.read.jdbc(
        url=jdbc_url,
        table=f"(SELECT * FROM pipeline_metadata.validation_rules WHERE pipeline_id = '{pipeline_id}' AND is_active = true) as rules",
        properties={"driver": "org.postgresql.Driver"}
    )

    return [row.asDict() for row in rules_df.collect()]


def cast_to_silver_types(df, schema_defs: list):
    """Cast columns from STRING to target types based on metadata."""
    type_mapping = {
        "STRING": StringType(),
        "INTEGER": IntegerType(),
        "BIGINT": LongType(),
        "DOUBLE": DoubleType(),
        "FLOAT": FloatType(),
        "BOOLEAN": BooleanType(),
        "DATE": DateType(),
        "TIMESTAMP": TimestampType(),
    }

    for col_def in schema_defs:
        col_name = col_def["column_name"]
        silver_type = col_def["silver_type"]

        if col_name not in df.columns:
            continue

        if silver_type.startswith("DECIMAL"):
            import re
            match = re.match(r"DECIMAL\((\d+),(\d+)\)", silver_type)
            if match:
                p, s = int(match.group(1)), int(match.group(2))
                df = df.withColumn(col_name, F.col(col_name).cast(DecimalType(p, s)))
        elif silver_type in type_mapping:
            df = df.withColumn(col_name, F.col(col_name).cast(type_mapping[silver_type]))

    return df


def apply_validations(df, rules: list):
    """Apply validation rules and separate valid/invalid records."""
    conditions = []

    for rule in rules:
        col_name = rule["column_name"]
        rule_type = rule["rule_type"]

        if col_name not in df.columns:
            continue

        if rule_type == "not_null":
            conditions.append(F.col(col_name).isNotNull())
        elif rule_type == "regex" and rule.get("rule_expression"):
            conditions.append(F.col(col_name).rlike(rule["rule_expression"]))
        elif rule_type == "range":
            params = json.loads(rule.get("parameters", "{}"))
            if "min" in params:
                conditions.append(F.col(col_name) >= params["min"])
            if "max" in params:
                conditions.append(F.col(col_name) <= params["max"])
        elif rule_type == "in_list":
            params = json.loads(rule.get("parameters", "{}"))
            if "values" in params:
                conditions.append(F.col(col_name).isin(params["values"]))

    if conditions:
        is_valid = conditions[0]
        for cond in conditions[1:]:
            is_valid = is_valid & cond
        df = df.withColumn("_is_valid", is_valid)
    else:
        df = df.withColumn("_is_valid", F.lit(True))

    valid_df = df.filter(F.col("_is_valid")).drop("_is_valid")
    invalid_df = df.filter(~F.col("_is_valid")).drop("_is_valid")

    return valid_df, invalid_df


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pipeline-id", required=True)
    parser.add_argument("--reporting-date", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName(f"Silver_{args.pipeline_id}")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate()
    )

    try:
        # Read from Bronze
        bronze_table = f"bronze.{args.pipeline_id.replace('-', '_')}"
        df = spark.table(bronze_table).filter(F.col("reporting_date") == args.reporting_date)
        print(f"Read {df.count()} records from Bronze")

        # Get metadata
        schema_defs = get_schema_definitions(spark, args.pipeline_id)
        validation_rules = get_validation_rules(spark, args.pipeline_id)

        # Apply type casting
        df = cast_to_silver_types(df, schema_defs)

        # Apply validations
        valid_df, invalid_df = apply_validations(df, validation_rules)

        # Write valid to Silver
        silver_table = f"silver.{args.pipeline_id.replace('-', '_')}"
        valid_df.write.format("iceberg").mode("append").partitionBy("reporting_date").saveAsTable(silver_table)
        print(f"Wrote {valid_df.count()} valid records to Silver")

        # Write rejected
        if invalid_df.count() > 0:
            rejected_table = f"silver.{args.pipeline_id.replace('-', '_')}_rejected"
            invalid_df.write.format("iceberg").mode("append").partitionBy("reporting_date").saveAsTable(rejected_table)
            print(f"Wrote {invalid_df.count()} rejected records")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
