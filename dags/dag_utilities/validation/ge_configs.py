"""
GE Config Builder — Converts metadata quality rules to GE expectation dicts.

Maps rule_type from PostgreSQL quality_rules/quality_expectations tables
to Great Expectations expectation types with proper kwargs.

Usage:
    from dag_utilities.validation.ge_configs import GEConfigBuilder

    builder = GEConfigBuilder()
    expectations = builder.build_expectations_from_rules(rules)
    expectations += builder.build_schema_expectations(columns)
"""

import logging
from decimal import Decimal
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Mapping: metadata rule_type → GE expectation method name
RULE_TYPE_MAP = {
    "not_null": "expect_column_values_to_not_be_null",
    "unique": "expect_column_values_to_be_unique",
    "range": "expect_column_values_to_be_between",
    "regex": "expect_column_values_to_match_regex",
    "in_set": "expect_column_values_to_be_in_set",
    "length": "expect_column_value_lengths_to_be_between",
    "row_count": "expect_table_row_count_to_be_between",
    "custom_sql": "expect_column_values_to_match_sql_condition",
    "freshness": "expect_column_max_to_be_between",
    "mean": "expect_column_mean_to_be_between",
    "null_rate": "expect_column_null_value_proportion_to_be_between",
    "compound_unique": "expect_compound_columns_to_be_unique",
    "not_in_set": "expect_column_values_to_not_be_in_set",
    "stdev": "expect_column_stdev_to_be_between",
    "median": "expect_column_median_to_be_between",
    "increasing": "expect_column_values_to_be_increasing",
    "column_exists": "expect_column_to_exist",
}

# Spark type mapping for schema expectations
SPARK_TYPE_MAP = {
    "string": "StringType",
    "integer": "IntegerType",
    "int": "IntegerType",
    "long": "LongType",
    "bigint": "LongType",
    "float": "FloatType",
    "double": "DoubleType",
    "decimal": "DecimalType",
    "boolean": "BooleanType",
    "date": "DateType",
    "timestamp": "TimestampType",
    "binary": "BinaryType",
}


class GEConfigBuilder:
    """Builds GE expectation configs from metadata rules and schema columns."""

    def build_expectations_from_rules(
        self, rules: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Convert metadata quality rules to GE expectation dicts.

        Each rule dict should have:
            rule_type, column_name, rule_expression, parameters,
            rule_name, severity, weight (all optional except rule_type)

        Returns list of dicts: {expectation_type, kwargs, meta}
        """
        expectations = []
        for i, rule in enumerate(rules):
            try:
                exp = self._build_one(rule, i)
                if exp:
                    expectations.append(exp)
            except Exception as e:
                logger.warning(f"Skipping rule {rule.get('rule_name', i)}: {e}")
        return expectations

    def build_schema_expectations(
        self, columns: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Auto-generate expectations from schema column definitions.

        For each column:
        - expect_column_to_exist
        - expect_column_values_to_not_be_null (if nullable=False)
        """
        expectations = []
        for col in columns:
            name = col.get("name") or col.get("column_name")
            if not name:
                continue

            # Column must exist
            expectations.append({
                "expectation_type": "expect_column_to_exist",
                "kwargs": {"column": name},
                "meta": {
                    "rule_name": f"schema_exists_{name}",
                    "severity": "error",
                    "weight": "1.0",
                    "original_rule_type": "column_exists",
                },
            })

            # Not-null if required
            nullable = col.get("nullable", True)
            if not nullable:
                expectations.append({
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": name, "mostly": 1.0},
                    "meta": {
                        "rule_name": f"schema_not_null_{name}",
                        "severity": "error",
                        "weight": "1.0",
                        "original_rule_type": "not_null",
                    },
                })

        return expectations

    def _build_one(self, rule: Dict[str, Any], index: int) -> Optional[Dict[str, Any]]:
        """Build a single expectation dict from a rule."""
        rule_type = rule.get("rule_type", "custom_sql")
        column_name = rule.get("column_name")
        expression = rule.get("rule_expression", "")
        params = rule.get("parameters", {})
        if isinstance(params, str):
            import json
            try:
                params = json.loads(params)
            except Exception:
                params = {}

        rule_name = rule.get("rule_name", f"rule_{index}")
        severity = rule.get("severity", "error")
        weight = str(rule.get("weight", "1.0"))
        threshold = params.get("threshold", 1.0)

        ge_type = RULE_TYPE_MAP.get(rule_type)
        if not ge_type:
            logger.warning(f"Unknown rule_type '{rule_type}', using custom_sql")
            ge_type = "expect_column_values_to_match_sql_condition"

        kwargs: Dict[str, Any] = {}

        if ge_type == "expect_column_values_to_not_be_null":
            kwargs = {"column": column_name, "mostly": threshold}

        elif ge_type == "expect_column_values_to_be_unique":
            kwargs = {"column": column_name, "mostly": threshold}

        elif ge_type == "expect_compound_columns_to_be_unique":
            kwargs = {"column_list": params.get("columns", [column_name])}
            column_name = None

        elif ge_type == "expect_column_values_to_be_between":
            kwargs = {"column": column_name, "mostly": threshold}
            if params.get("min") is not None:
                kwargs["min_value"] = params["min"]
            if params.get("max") is not None:
                kwargs["max_value"] = params["max"]

        elif ge_type == "expect_column_values_to_match_regex":
            kwargs = {
                "column": column_name,
                "regex": expression or params.get("pattern", ".*"),
                "mostly": threshold,
            }

        elif ge_type == "expect_column_values_to_be_in_set":
            kwargs = {
                "column": column_name,
                "value_set": params.get("values", []),
                "mostly": threshold,
            }

        elif ge_type == "expect_column_values_to_not_be_in_set":
            kwargs = {
                "column": column_name,
                "value_set": params.get("values", []),
                "mostly": threshold,
            }

        elif ge_type == "expect_column_value_lengths_to_be_between":
            kwargs = {"column": column_name, "mostly": threshold}
            if params.get("min_length") is not None or params.get("min") is not None:
                kwargs["min_value"] = params.get("min_length", params.get("min"))
            if params.get("max_length") is not None or params.get("max") is not None:
                kwargs["max_value"] = params.get("max_length", params.get("max"))

        elif ge_type == "expect_table_row_count_to_be_between":
            kwargs = {}
            if params.get("min_rows") is not None or params.get("min") is not None:
                kwargs["min_value"] = params.get("min_rows", params.get("min"))
            if params.get("max_rows") is not None or params.get("max") is not None:
                kwargs["max_value"] = params.get("max_rows", params.get("max"))
            column_name = None

        elif ge_type == "expect_column_max_to_be_between":
            kwargs = {"column": column_name}
            if params.get("min_value") is not None:
                kwargs["min_value"] = params["min_value"]
            if params.get("max_value") is not None:
                kwargs["max_value"] = params["max_value"]

        elif ge_type == "expect_column_mean_to_be_between":
            kwargs = {"column": column_name}
            if params.get("min") is not None:
                kwargs["min_value"] = params["min"]
            if params.get("max") is not None:
                kwargs["max_value"] = params["max"]

        elif ge_type == "expect_column_null_value_proportion_to_be_between":
            kwargs = {
                "column": column_name,
                "min_value": params.get("min_null_rate", 0.0),
                "max_value": params.get("max_null_rate", threshold),
            }

        elif ge_type == "expect_column_values_to_match_sql_condition":
            kwargs = {
                "column": column_name,
                "sql_condition": expression,
                "mostly": threshold,
            }

        elif ge_type == "expect_column_to_exist":
            kwargs = {"column": column_name}

        elif ge_type == "expect_column_stdev_to_be_between":
            kwargs = {"column": column_name}
            if params.get("min") is not None:
                kwargs["min_value"] = params["min"]
            if params.get("max") is not None:
                kwargs["max_value"] = params["max"]

        elif ge_type == "expect_column_median_to_be_between":
            kwargs = {"column": column_name}
            if params.get("min") is not None:
                kwargs["min_value"] = params["min"]
            if params.get("max") is not None:
                kwargs["max_value"] = params["max"]

        elif ge_type == "expect_column_values_to_be_increasing":
            kwargs = {"column": column_name, "mostly": threshold}

        # Clean None values from kwargs
        kwargs = {k: v for k, v in kwargs.items() if v is not None}

        meta = {
            "rule_name": rule_name,
            "severity": severity,
            "weight": weight,
            "original_rule_type": rule_type,
        }

        return {
            "expectation_type": ge_type,
            "kwargs": kwargs,
            "meta": meta,
        }
