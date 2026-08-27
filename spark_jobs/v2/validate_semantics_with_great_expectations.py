#!/usr/bin/env python3
"""
APEX Data Agent v2 - Great Expectations Semantic / Business Rule Validator

Production-grade PySpark job for GCP Dataproc that validates data against
semantic and business rules using Great Expectations (with PySpark fallback).

Validation categories:
- Referential integrity   (column values exist in reference tables)
- Cross-field validation   (column A > column B, date range validity)
- Business constraints     (status enums, conditional nullability)
- Custom SQL conditions    (arbitrary SQL WHERE predicates)
- Statistical checks       (mean / stddev within bounds)
- Multi-column uniqueness  (composite business keys)

Results are persisted to:
- platform_validation_result   (one row per expectation)
- platform_validation_summary  (one row per run)
with validation_type = 'SEMANTIC'.

Usage (Dataproc submit):
    spark-submit validate_semantics_with_great_expectations.py \
        --feed-id <feed> \
        --contract-id <contract> \
        --execution-id <exec> \
        --metadata-db <jdbc_url> \
        --transient-path gs://bucket/transient/feed/exec/ \
        [--validation-rules-json '<json_string>'] \
        [--batch-identifier <batch>] \
        [--pg-connection-string <pg_url>] \
        [--reference-db <jdbc_url>] \
        [--fail-on-warning]
"""

import argparse
import json
import os
import sys
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

VALIDATION_TYPE = "SEMANTIC"
BUCKET_NAME = os.environ.get("BUCKET_NAME", "")
BUCKET_PATH = f"gs://{BUCKET_NAME}" if BUCKET_NAME else ""

_GE_AVAILABLE: Optional[bool] = None


def _check_ge_available() -> bool:
    """Lazily check whether Great Expectations is importable."""
    global _GE_AVAILABLE
    if _GE_AVAILABLE is None:
        try:
            import great_expectations  # noqa: F401
            _GE_AVAILABLE = True
        except ImportError:
            _GE_AVAILABLE = False
    return _GE_AVAILABLE


# ═══════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════

def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="GE Semantic / Business Rule Validator (v2)"
    )
    parser.add_argument("--feed-id", required=True, help="Feed ID")
    parser.add_argument("--contract-id", required=True, help="Contract ID")
    parser.add_argument("--execution-id", required=True, help="Execution ID")
    parser.add_argument("--metadata-db", required=True, help="Metadata DB JDBC URL")
    parser.add_argument(
        "--transient-path",
        default="",
        help="GCS transient path (gs://) for the data to validate",
    )
    parser.add_argument(
        "--validation-rules-json",
        default="",
        help="JSON string of business rules; overrides metadata DB lookup",
    )
    parser.add_argument(
        "--batch-identifier", default="", help="Batch/partition identifier"
    )
    parser.add_argument(
        "--pg-connection-string",
        default="",
        help="PostgreSQL connection string for result persistence",
    )
    parser.add_argument(
        "--reference-db",
        default="",
        help="JDBC URL for reference-data lookups (referential integrity)",
    )
    parser.add_argument(
        "--fail-on-warning",
        action="store_true",
        default=False,
        help="Treat warning-severity failures as blocking",
    )
    # Legacy positional support: sys.argv[22] as validation_rules_json
    return parser.parse_args()


# ═══════════════════════════════════════════════════════════════════════════
# METADATA LOADING
# ═══════════════════════════════════════════════════════════════════════════

def load_contract_config(
    spark: SparkSession, metadata_db: str, contract_id: str
) -> Dict[str, Any]:
    """Load contract configuration from metadata DB."""
    df = (
        spark.read.format("jdbc")
        .option("url", metadata_db)
        .option(
            "dbtable",
            f"(SELECT * FROM platform_data_contract WHERE contract_id = '{contract_id}') AS contract",
        )
        .load()
    )
    row = df.first()
    if row is None:
        raise ValueError(f"No contract found for contract_id={contract_id}")
    return {
        "silver_table": row["silver_table"] if "silver_table" in row else None,
        "silver_path": row["silver_path"] if "silver_path" in row else None,
        "primary_keys": row["primary_keys"] or [],
        "domain": row["domain"] if "domain" in row else "",
    }


def load_semantic_rules_from_db(
    spark: SparkSession, metadata_db: str, contract_id: str
) -> List[Dict[str, Any]]:
    """Load semantic / business validation rules from metadata DB."""
    try:
        df = (
            spark.read.format("jdbc")
            .option("url", metadata_db)
            .option(
                "dbtable",
                f"""(
                    SELECT * FROM platform_validation_rule
                    WHERE contract_id = '{contract_id}'
                      AND zone_level IN ('SILVER', 'GOLD')
                      AND rule_category IN (
                          'referential_integrity',
                          'cross_field',
                          'business_constraint',
                          'sql_condition',
                          'statistical',
                          'multi_column_unique'
                      )
                      AND is_active = true
                    ORDER BY severity DESC, rule_name ASC
                ) AS rules""",
            )
            .load()
        )
        return [row.asDict() for row in df.collect()]
    except Exception as e:
        print(f"Warning: Could not load semantic rules from DB: {e}")
        return []


def load_quality_rules(
    spark: SparkSession, metadata_db: str, contract_id: str
) -> List[Dict[str, Any]]:
    """Load quality expectations tagged as SEMANTIC."""
    try:
        df = (
            spark.read.format("jdbc")
            .option("url", metadata_db)
            .option(
                "dbtable",
                f"""(
                    SELECT * FROM platform_quality_expectation
                    WHERE contract_id = '{contract_id}'
                      AND zone_level IN ('SILVER', 'GOLD')
                      AND expectation_category = 'SEMANTIC'
                      AND is_active = true
                ) AS rules""",
            )
            .load()
        )
        return [row.asDict() for row in df.collect()]
    except Exception as e:
        print(f"Warning: Could not load quality expectations: {e}")
        return []


def load_reference_data(
    spark: SparkSession,
    reference_db: str,
    table_name: str,
    column_name: str,
) -> List[Any]:
    """Load distinct reference values for referential integrity checks."""
    try:
        df = (
            spark.read.format("jdbc")
            .option("url", reference_db)
            .option(
                "dbtable",
                f"(SELECT DISTINCT {column_name} FROM {table_name}) AS ref",
            )
            .load()
        )
        return [row[0] for row in df.collect()]
    except Exception as e:
        print(f"Warning: Could not load reference data from {table_name}.{column_name}: {e}")
        return []


# ═══════════════════════════════════════════════════════════════════════════
# BUILD GE EXPECTATIONS FROM RULES
# ═══════════════════════════════════════════════════════════════════════════

def _parse_rules_json(raw: str) -> List[Dict[str, Any]]:
    """Safely parse the validation_rules_json argument."""
    if not raw or not raw.strip():
        return []
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return parsed
        if isinstance(parsed, dict):
            return [parsed]
    except json.JSONDecodeError as e:
        print(f"Warning: Could not parse validation_rules_json: {e}")
    return []


def build_semantic_expectations(
    rules: List[Dict[str, Any]],
    quality_rules: List[Dict[str, Any]],
    primary_keys: List[str],
    spark: Optional[SparkSession] = None,
    reference_db: str = "",
) -> List[Dict[str, Any]]:
    """
    Convert business rules into GE expectation dicts.

    Supported rule_category values:
    - referential_integrity
    - cross_field
    - business_constraint
    - sql_condition
    - statistical
    - multi_column_unique
    """
    expectations: List[Dict[str, Any]] = []

    for rule in rules:
        category = rule.get("rule_category", "").lower()
        severity = rule.get("severity", "warning").lower()
        rule_name = rule.get("rule_name", f"rule_{len(expectations)}")
        config = rule.get("rule_config", {})
        if isinstance(config, str):
            try:
                config = json.loads(config)
            except json.JSONDecodeError:
                config = {}

        meta = {
            "rule_name": rule_name,
            "severity": severity,
            "weight": str(rule.get("weight", "1.0")),
            "original_rule_type": category,
            "rule_category": category,
        }

        # ----- Referential integrity -----
        if category == "referential_integrity":
            column = config.get("column")
            ref_table = config.get("reference_table")
            ref_column = config.get("reference_column", column)
            value_set = config.get("value_set")

            if not column:
                continue

            # If an explicit value_set is provided, use it directly
            if value_set and isinstance(value_set, list):
                expectations.append({
                    "expectation_type": "expect_column_values_to_be_in_set",
                    "kwargs": {
                        "column": column,
                        "value_set": value_set,
                        "mostly": config.get("mostly", 1.0),
                    },
                    "meta": meta,
                })
            elif ref_table and spark and reference_db:
                # Load values from reference table at runtime
                ref_values = load_reference_data(
                    spark, reference_db, ref_table, ref_column
                )
                if ref_values:
                    expectations.append({
                        "expectation_type": "expect_column_values_to_be_in_set",
                        "kwargs": {
                            "column": column,
                            "value_set": ref_values,
                            "mostly": config.get("mostly", 1.0),
                        },
                        "meta": meta,
                    })
                else:
                    print(
                        f"Warning: Empty reference set for {rule_name}, skipping"
                    )
            else:
                print(
                    f"Warning: Referential integrity rule '{rule_name}' missing "
                    f"value_set or reference_table+reference_db, skipping"
                )

        # ----- Cross-field validation -----
        elif category == "cross_field":
            column_a = config.get("column_a") or config.get("column_A")
            column_b = config.get("column_b") or config.get("column_B")
            operator = config.get("operator", "greater_than")

            if not column_a or not column_b:
                continue

            if operator in ("greater_than", "gt", ">"):
                expectations.append({
                    "expectation_type": "expect_column_pair_values_A_to_be_greater_than_B",
                    "kwargs": {
                        "column_A": column_a,
                        "column_B": column_b,
                        "or_equal": config.get("or_equal", False),
                        "mostly": config.get("mostly", 1.0),
                    },
                    "meta": meta,
                })
            elif operator in ("greater_than_or_equal", "gte", ">="):
                expectations.append({
                    "expectation_type": "expect_column_pair_values_A_to_be_greater_than_B",
                    "kwargs": {
                        "column_A": column_a,
                        "column_B": column_b,
                        "or_equal": True,
                        "mostly": config.get("mostly", 1.0),
                    },
                    "meta": meta,
                })
            elif operator in ("equal", "eq", "=="):
                # Use SQL condition for equality
                sql_cond = f"{column_a} = {column_b}"
                expectations.append({
                    "expectation_type": "expect_column_values_to_match_sql_condition",
                    "kwargs": {
                        "column": column_a,
                        "sql_condition": sql_cond,
                        "mostly": config.get("mostly", 1.0),
                    },
                    "meta": meta,
                })
            elif operator in ("not_equal", "ne", "!="):
                sql_cond = f"{column_a} != {column_b}"
                expectations.append({
                    "expectation_type": "expect_column_values_to_match_sql_condition",
                    "kwargs": {
                        "column": column_a,
                        "sql_condition": sql_cond,
                        "mostly": config.get("mostly", 1.0),
                    },
                    "meta": meta,
                })

        # ----- Business constraints (enum / conditional nullability) -----
        elif category == "business_constraint":
            constraint_type = config.get("constraint_type", "enum")

            if constraint_type == "enum":
                column = config.get("column")
                allowed = config.get("allowed_values", [])
                if column and allowed:
                    expectations.append({
                        "expectation_type": "expect_column_values_to_be_in_set",
                        "kwargs": {
                            "column": column,
                            "value_set": allowed,
                            "mostly": config.get("mostly", 1.0),
                        },
                        "meta": meta,
                    })

            elif constraint_type == "conditional_not_null":
                # When condition_column == condition_value, then target_column must not be null
                # Expressed as SQL condition
                target_col = config.get("target_column")
                cond_col = config.get("condition_column")
                cond_val = config.get("condition_value")
                if target_col and cond_col and cond_val is not None:
                    if isinstance(cond_val, str):
                        sql_cond = (
                            f"CASE WHEN {cond_col} = '{cond_val}' "
                            f"THEN {target_col} IS NOT NULL ELSE TRUE END"
                        )
                    else:
                        sql_cond = (
                            f"CASE WHEN {cond_col} = {cond_val} "
                            f"THEN {target_col} IS NOT NULL ELSE TRUE END"
                        )
                    expectations.append({
                        "expectation_type": "expect_column_values_to_match_sql_condition",
                        "kwargs": {
                            "column": target_col,
                            "sql_condition": sql_cond,
                            "mostly": config.get("mostly", 1.0),
                        },
                        "meta": meta,
                    })

            elif constraint_type == "range":
                column = config.get("column")
                min_val = config.get("min_value")
                max_val = config.get("max_value")
                if column:
                    kwargs: Dict[str, Any] = {
                        "column": column,
                        "mostly": config.get("mostly", 1.0),
                    }
                    if min_val is not None:
                        kwargs["min_value"] = min_val
                    if max_val is not None:
                        kwargs["max_value"] = max_val
                    expectations.append({
                        "expectation_type": "expect_column_values_to_be_between",
                        "kwargs": kwargs,
                        "meta": meta,
                    })

        # ----- Custom SQL conditions -----
        elif category == "sql_condition":
            column = config.get("column")
            sql_condition = config.get("sql_condition") or config.get("condition")
            if column and sql_condition:
                expectations.append({
                    "expectation_type": "expect_column_values_to_match_sql_condition",
                    "kwargs": {
                        "column": column,
                        "sql_condition": sql_condition,
                        "mostly": config.get("mostly", 1.0),
                    },
                    "meta": meta,
                })

        # ----- Statistical checks -----
        elif category == "statistical":
            stat_type = config.get("stat_type", "mean")
            column = config.get("column")
            if not column:
                continue

            if stat_type == "mean":
                expectations.append({
                    "expectation_type": "expect_column_mean_to_be_between",
                    "kwargs": {
                        "column": column,
                        "min_value": config.get("min_value"),
                        "max_value": config.get("max_value"),
                    },
                    "meta": meta,
                })
            elif stat_type == "stdev":
                expectations.append({
                    "expectation_type": "expect_column_stdev_to_be_between",
                    "kwargs": {
                        "column": column,
                        "min_value": config.get("min_value"),
                        "max_value": config.get("max_value"),
                    },
                    "meta": meta,
                })
            elif stat_type == "median":
                expectations.append({
                    "expectation_type": "expect_column_median_to_be_between",
                    "kwargs": {
                        "column": column,
                        "min_value": config.get("min_value"),
                        "max_value": config.get("max_value"),
                    },
                    "meta": meta,
                })

        # ----- Multi-column uniqueness -----
        elif category == "multi_column_unique":
            column_list = config.get("column_list", [])
            if column_list and len(column_list) >= 2:
                expectations.append({
                    "expectation_type": "expect_multicolumn_values_to_be_unique",
                    "kwargs": {"column_list": column_list},
                    "meta": meta,
                })

    # --- Quality rules (pre-formatted expectations from metadata) ---
    for qr in quality_rules:
        exp_type = qr.get("expectation_type")
        exp_kwargs = qr.get("expectation_kwargs", {})
        if isinstance(exp_kwargs, str):
            try:
                exp_kwargs = json.loads(exp_kwargs)
            except json.JSONDecodeError:
                exp_kwargs = {}
        if exp_type:
            expectations.append({
                "expectation_type": exp_type,
                "kwargs": exp_kwargs,
                "meta": {
                    "rule_name": qr.get("rule_name", f"quality_{len(expectations)}"),
                    "severity": qr.get("severity", "warning"),
                    "weight": str(qr.get("weight", "1.0")),
                    "original_rule_type": "platform_quality_expectation",
                },
            })

    # --- Composite primary key uniqueness (always added) ---
    if primary_keys and len(primary_keys) >= 2:
        expectations.append({
            "expectation_type": "expect_multicolumn_values_to_be_unique",
            "kwargs": {"column_list": primary_keys},
            "meta": {
                "rule_name": f"pk_unique_{'+'.join(primary_keys)}",
                "severity": "error",
                "weight": "2.0",
                "original_rule_type": "primary_key",
            },
        })
    elif primary_keys and len(primary_keys) == 1:
        expectations.append({
            "expectation_type": "expect_column_values_to_be_unique",
            "kwargs": {"column": primary_keys[0], "mostly": 1.0},
            "meta": {
                "rule_name": f"pk_unique_{primary_keys[0]}",
                "severity": "error",
                "weight": "2.0",
                "original_rule_type": "primary_key",
            },
        })

    return expectations


# ═══════════════════════════════════════════════════════════════════════════
# PYSPARK FALLBACK VALIDATION
# ═══════════════════════════════════════════════════════════════════════════

class _FallbackResult:
    """Lightweight result object mimicking GEHelper result when GE is unavailable."""

    def __init__(self) -> None:
        self.passed: int = 0
        self.failed: int = 0
        self.total: int = 0
        self.quality_score: float = 100.0
        self.results: List[Dict[str, Any]] = []
        self._blocking: List[Any] = []

    def get_blocking_failures(self) -> List[Any]:
        return self._blocking


class _BlockingFailure:
    def __init__(self, rule_name: str, expectation_type: str, column: str) -> None:
        self.rule_name = rule_name
        self.expectation_type = expectation_type
        self.column = column


def _fallback_validate(
    spark: SparkSession,
    df: DataFrame,
    expectations: List[Dict[str, Any]],
    fail_on_warning: bool = False,
) -> _FallbackResult:
    """Run expectations manually using PySpark when GE is unavailable."""
    result = _FallbackResult()

    if df.head(1) is None or df.count() == 0:
        print("Warning: DataFrame is empty, skipping fallback validation")
        return result

    total_rows = df.count()
    df_columns = set(df.columns)

    for exp in expectations:
        exp_type = exp.get("expectation_type", "")
        kwargs = exp.get("kwargs", {})
        meta = exp.get("meta", {})
        rule_name = meta.get("rule_name", exp_type)
        severity = meta.get("severity", "warning")
        result.total += 1
        success = False
        observed = ""

        try:
            # --- Column pair A > B ---
            if exp_type == "expect_column_pair_values_A_to_be_greater_than_B":
                col_a = kwargs.get("column_A", "")
                col_b = kwargs.get("column_B", "")
                or_equal = kwargs.get("or_equal", False)
                mostly = kwargs.get("mostly", 1.0)
                if col_a not in df_columns or col_b not in df_columns:
                    observed = f"Missing column(s): {col_a}, {col_b}"
                else:
                    if or_equal:
                        bad = df.filter(F.col(col_a) < F.col(col_b)).count()
                    else:
                        bad = df.filter(F.col(col_a) <= F.col(col_b)).count()
                    pct = 1.0 - (bad / total_rows) if total_rows else 1.0
                    success = pct >= mostly
                    observed = f"{bad}/{total_rows} rows failed ({pct:.4f} pass rate)"

            # --- Column values in set ---
            elif exp_type == "expect_column_values_to_be_in_set":
                column = kwargs.get("column", "")
                value_set = kwargs.get("value_set", [])
                mostly = kwargs.get("mostly", 1.0)
                if column not in df_columns:
                    observed = f"Missing column: {column}"
                else:
                    bad = df.filter(~F.col(column).isin(value_set)).count()
                    pct = 1.0 - (bad / total_rows) if total_rows else 1.0
                    success = pct >= mostly
                    observed = f"{bad}/{total_rows} rows not in set ({pct:.4f})"

            # --- SQL condition ---
            elif exp_type == "expect_column_values_to_match_sql_condition":
                column = kwargs.get("column", "")
                sql_condition = kwargs.get("sql_condition", "TRUE")
                mostly = kwargs.get("mostly", 1.0)
                if column not in df_columns:
                    observed = f"Missing column: {column}"
                else:
                    df.createOrReplaceTempView("_semantic_validation_tmp")
                    bad_count = spark.sql(
                        f"SELECT COUNT(*) AS cnt FROM _semantic_validation_tmp "
                        f"WHERE NOT ({sql_condition})"
                    ).first()[0]
                    pct = 1.0 - (bad_count / total_rows) if total_rows else 1.0
                    success = pct >= mostly
                    observed = f"{bad_count}/{total_rows} rows failed SQL ({pct:.4f})"

            # --- Column mean between ---
            elif exp_type == "expect_column_mean_to_be_between":
                column = kwargs.get("column", "")
                if column not in df_columns:
                    observed = f"Missing column: {column}"
                else:
                    mean_val = df.agg(F.mean(F.col(column))).first()[0]
                    min_v = kwargs.get("min_value")
                    max_v = kwargs.get("max_value")
                    success = True
                    if min_v is not None and mean_val is not None and mean_val < min_v:
                        success = False
                    if max_v is not None and mean_val is not None and mean_val > max_v:
                        success = False
                    observed = f"mean={mean_val}, bounds=[{min_v}, {max_v}]"

            # --- Column stdev between ---
            elif exp_type == "expect_column_stdev_to_be_between":
                column = kwargs.get("column", "")
                if column not in df_columns:
                    observed = f"Missing column: {column}"
                else:
                    std_val = df.agg(F.stddev(F.col(column))).first()[0]
                    min_v = kwargs.get("min_value")
                    max_v = kwargs.get("max_value")
                    success = True
                    if min_v is not None and std_val is not None and std_val < min_v:
                        success = False
                    if max_v is not None and std_val is not None and std_val > max_v:
                        success = False
                    observed = f"stdev={std_val}, bounds=[{min_v}, {max_v}]"

            # --- Column median between ---
            elif exp_type == "expect_column_median_to_be_between":
                column = kwargs.get("column", "")
                if column not in df_columns:
                    observed = f"Missing column: {column}"
                else:
                    median_val = df.approxQuantile(column, [0.5], 0.01)[0]
                    min_v = kwargs.get("min_value")
                    max_v = kwargs.get("max_value")
                    success = True
                    if min_v is not None and median_val < min_v:
                        success = False
                    if max_v is not None and median_val > max_v:
                        success = False
                    observed = f"median~={median_val}, bounds=[{min_v}, {max_v}]"

            # --- Multi-column uniqueness ---
            elif exp_type in (
                "expect_multicolumn_values_to_be_unique",
                "expect_compound_columns_to_be_unique",
            ):
                col_list = kwargs.get("column_list", [])
                missing = [c for c in col_list if c not in df_columns]
                if missing:
                    observed = f"Missing columns: {missing}"
                else:
                    dup_count = (
                        df.groupBy(*col_list)
                        .count()
                        .filter(F.col("count") > 1)
                        .count()
                    )
                    success = dup_count == 0
                    observed = f"{dup_count} duplicate key combinations"

            # --- Column values to be unique ---
            elif exp_type == "expect_column_values_to_be_unique":
                column = kwargs.get("column", "")
                mostly = kwargs.get("mostly", 1.0)
                if column not in df_columns:
                    observed = f"Missing column: {column}"
                else:
                    dup_count = (
                        df.groupBy(column)
                        .count()
                        .filter(F.col("count") > 1)
                        .agg(F.sum("count"))
                        .first()[0]
                    ) or 0
                    pct = 1.0 - (dup_count / total_rows) if total_rows else 1.0
                    success = pct >= mostly
                    observed = f"{dup_count} duplicates ({pct:.4f} unique rate)"

            # --- Column values between ---
            elif exp_type == "expect_column_values_to_be_between":
                column = kwargs.get("column", "")
                mostly = kwargs.get("mostly", 1.0)
                if column not in df_columns:
                    observed = f"Missing column: {column}"
                else:
                    min_v = kwargs.get("min_value")
                    max_v = kwargs.get("max_value")
                    cond = F.lit(True)
                    if min_v is not None:
                        cond = cond & (F.col(column) >= min_v)
                    if max_v is not None:
                        cond = cond & (F.col(column) <= max_v)
                    bad = df.filter(~cond).count()
                    pct = 1.0 - (bad / total_rows) if total_rows else 1.0
                    success = pct >= mostly
                    observed = f"{bad}/{total_rows} out of range ({pct:.4f})"

            # --- Unknown expectation type ---
            else:
                observed = f"Unsupported expectation_type: {exp_type}"
                success = True  # Don't block on unknown types

        except Exception as e:
            observed = f"Error: {e}"
            success = False

        if success:
            result.passed += 1
        else:
            result.failed += 1

        result.results.append({
            "rule_name": rule_name,
            "expectation_type": exp_type,
            "column": kwargs.get("column", kwargs.get("column_A", "")),
            "success": success,
            "observed_value": observed,
            "severity": severity,
        })

        # Track blocking failures
        if not success and (severity == "error" or (fail_on_warning and severity == "warning")):
            result._blocking.append(
                _BlockingFailure(
                    rule_name=rule_name,
                    expectation_type=exp_type,
                    column=kwargs.get("column", kwargs.get("column_A", "")),
                )
            )

    # Compute quality score
    if result.total > 0:
        result.quality_score = (result.passed / result.total) * 100.0

    return result


# ═══════════════════════════════════════════════════════════════════════════
# RESULT PERSISTENCE
# ═══════════════════════════════════════════════════════════════════════════

def persist_results_jdbc(
    spark: SparkSession,
    feed_id: str,
    execution_id: str,
    suite_name: str,
    result: Any,
    jdbc_url: str,
    batch_identifier: str = "",
) -> None:
    """Persist validation results via PySpark JDBC (fallback path)."""
    jdbc_props = {"driver": "org.postgresql.Driver"}
    now = datetime.now(timezone.utc).isoformat()

    # -- platform_validation_result rows --
    result_rows = []
    for r in getattr(result, "results", []):
        result_rows.append((
            feed_id,
            execution_id,
            suite_name,
            VALIDATION_TYPE,
            r.get("rule_name", ""),
            r.get("expectation_type", ""),
            r.get("column", ""),
            r.get("success", False),
            str(r.get("observed_value", "")),
            r.get("severity", "warning"),
            batch_identifier,
            now,
        ))

    if result_rows:
        schema = StructType([
            StructField("feed_id", StringType()),
            StructField("execution_id", StringType()),
            StructField("suite_name", StringType()),
            StructField("validation_type", StringType()),
            StructField("rule_name", StringType()),
            StructField("expectation_type", StringType()),
            StructField("column_name", StringType()),
            StructField("success", StringType()),
            StructField("observed_value", StringType()),
            StructField("severity", StringType()),
            StructField("batch_identifier", StringType()),
            StructField("created_at", StringType()),
        ])
        result_df = spark.createDataFrame(result_rows, schema)
        result_df.write.jdbc(
            url=jdbc_url,
            table="platform_validation_result",
            mode="append",
            properties=jdbc_props,
        )

    # -- platform_validation_summary row --
    summary_rows = [(
        feed_id,
        execution_id,
        suite_name,
        VALIDATION_TYPE,
        getattr(result, "total", 0),
        getattr(result, "passed", 0),
        getattr(result, "failed", 0),
        round(getattr(result, "quality_score", 0.0), 2),
        batch_identifier,
        now,
    )]
    summary_schema = StructType([
        StructField("feed_id", StringType()),
        StructField("execution_id", StringType()),
        StructField("suite_name", StringType()),
        StructField("validation_type", StringType()),
        StructField("total_expectations", IntegerType()),
        StructField("passed", IntegerType()),
        StructField("failed", IntegerType()),
        StructField("quality_score", DoubleType()),
        StructField("batch_identifier", StringType()),
        StructField("created_at", StringType()),
    ])
    summary_df = spark.createDataFrame(summary_rows, summary_schema)
    summary_df.write.jdbc(
        url=jdbc_url,
        table="platform_validation_summary",
        mode="append",
        properties=jdbc_props,
    )


def persist_results(
    spark: SparkSession,
    args: argparse.Namespace,
    suite_name: str,
    result: Any,
) -> None:
    """Persist results using GEResultWriter or JDBC fallback."""
    pg_conn = args.pg_connection_string

    if pg_conn:
        try:
            from dag_utilities.validation.ge_result_writer import GEResultWriter

            writer = GEResultWriter(pg_conn)
            writer.write_results(
                feed_id=args.feed_id,
                run_id=args.execution_id,
                suite_name=suite_name,
                validation_type=VALIDATION_TYPE,
                zone_level="SILVER",
                checkpoint_result=result,
                batch_identifier=args.batch_identifier,
            )
            return
        except Exception as e:
            print(f"Warning: GEResultWriter failed, trying JDBC fallback: {e}")

    # Fallback: JDBC via PySpark
    try:
        persist_results_jdbc(
            spark=spark,
            feed_id=args.feed_id,
            execution_id=args.execution_id,
            suite_name=suite_name,
            result=result,
            jdbc_url=args.metadata_db,
            batch_identifier=args.batch_identifier,
        )
    except Exception as e:
        print(f"Warning: Could not persist results via JDBC: {e}")


# ═══════════════════════════════════════════════════════════════════════════
# DATA LOADING
# ═══════════════════════════════════════════════════════════════════════════

def load_dataframe(
    spark: SparkSession,
    transient_path: str,
    contract_config: Dict[str, Any],
    execution_id: str,
) -> DataFrame:
    """
    Load the DataFrame to validate.

    Priority:
    1. transient_path (GCS gs:// parquet/delta)
    2. silver_table from contract config
    3. silver_path from contract config
    """
    df: Optional[DataFrame] = None

    if transient_path:
        print(f"Loading data from transient_path: {transient_path}")
        if transient_path.endswith(".parquet") or "/parquet/" in transient_path:
            df = spark.read.parquet(transient_path)
        else:
            try:
                df = spark.read.format("delta").load(transient_path)
            except Exception:
                df = spark.read.parquet(transient_path)
    elif contract_config.get("silver_table"):
        table = contract_config["silver_table"]
        print(f"Loading data from table: {table}")
        df = spark.table(table)
    elif contract_config.get("silver_path"):
        path = contract_config["silver_path"]
        print(f"Loading data from silver_path: {path}")
        df = spark.read.format("delta").load(path)
    else:
        raise ValueError(
            "No data source: provide --transient-path, or ensure contract "
            "has silver_table / silver_path"
        )

    # Filter to execution if column present
    for exec_col in ("_execution_id", "_silver_execution_id"):
        if exec_col in df.columns:
            df = df.filter(F.col(exec_col) == execution_id)
            break

    return df


# ═══════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════

def main() -> int:
    """Main entry point."""
    args = parse_args()

    # Legacy positional arg support
    if not args.validation_rules_json and len(sys.argv) > 22:
        args.validation_rules_json = sys.argv[22]

    spark = (
        SparkSession.builder.appName(f"ge_semantic_validator_{args.feed_id}")
        .enableHiveSupport()
        .getOrCreate()
    )

    suite_name = f"{args.feed_id}_semantic"

    try:
        # ---- Load contract config ----
        contract_config = load_contract_config(
            spark, args.metadata_db, args.contract_id
        )

        # ---- Resolve transient_path ----
        transient_path = args.transient_path
        if not transient_path and BUCKET_PATH:
            transient_path = (
                f"{BUCKET_PATH}/transient/{args.feed_id}/{args.execution_id}"
            )

        # ---- Load DataFrame ----
        df = load_dataframe(spark, transient_path, contract_config, args.execution_id)

        row_count = df.count()
        print(f"Loaded {row_count} rows for semantic validation")

        if row_count == 0:
            print("Warning: DataFrame is empty. Writing empty result and exiting.")
            empty_result = _FallbackResult()
            persist_results(spark, args, suite_name, empty_result)
            return 0

        # ---- Build expectations ----
        # Source 1: CLI-provided JSON rules
        cli_rules = _parse_rules_json(args.validation_rules_json)

        # Source 2: Metadata DB rules
        db_rules = load_semantic_rules_from_db(
            spark, args.metadata_db, args.contract_id
        )

        # Source 3: Quality expectations
        quality_rules = load_quality_rules(
            spark, args.metadata_db, args.contract_id
        )

        # Merge: CLI rules take precedence (appended after DB rules)
        all_rules = db_rules + cli_rules

        if not all_rules and not quality_rules:
            print("Warning: No semantic validation rules found. Skipping.")
            empty_result = _FallbackResult()
            persist_results(spark, args, suite_name, empty_result)
            return 0

        expectations = build_semantic_expectations(
            rules=all_rules,
            quality_rules=quality_rules,
            primary_keys=contract_config.get("primary_keys", []),
            spark=spark,
            reference_db=args.reference_db or args.metadata_db,
        )

        if not expectations:
            print("Warning: No expectations generated from rules. Skipping.")
            empty_result = _FallbackResult()
            persist_results(spark, args, suite_name, empty_result)
            return 0

        print(f"Running {len(expectations)} semantic expectations")

        # ---- Validate ----
        result: Any = None

        if _check_ge_available():
            try:
                from dag_utilities.validation.ge_helper import GEHelper

                helper = GEHelper(spark)
                result = helper.validate_dataframe(
                    df=df,
                    suite_name=suite_name,
                    data_asset_name=contract_config.get("silver_table", args.feed_id),
                    expectations=expectations,
                    batch_identifier=args.batch_identifier or args.execution_id,
                )
            except Exception as e:
                print(f"Warning: GE validation failed, falling back to PySpark: {e}")
                traceback.print_exc()
                result = None

        if result is None:
            print("Using PySpark fallback validation engine")
            result = _fallback_validate(
                spark, df, expectations, fail_on_warning=args.fail_on_warning
            )

        # ---- Persist ----
        persist_results(spark, args, suite_name, result)

        # ---- Report ----
        passed = getattr(result, "passed", 0)
        total = getattr(result, "total", 0)
        score = getattr(result, "quality_score", 0.0)
        print(
            f"Semantic validation: {passed}/{total} passed "
            f"(score: {score:.1f}/100)"
        )

        blocking = result.get_blocking_failures()
        if blocking:
            print(f"BLOCKING FAILURES ({len(blocking)}):")
            for b in blocking:
                print(f"  - {b.rule_name}: {b.expectation_type} on {b.column}")
            return 1

        return 0

    except Exception as e:
        print(f"Error in ge_semantic_validator: {e}")
        traceback.print_exc()
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
