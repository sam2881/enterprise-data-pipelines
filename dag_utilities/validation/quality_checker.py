"""
APEX Data Agent - Quality Checker

Data quality checks across all zones.
Implements Great Expectations style quality rules.
"""

from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field


@dataclass
class QualityCheckResult:
    """Result of a quality check."""

    expectation_type: str
    column_name: Optional[str]
    success: bool
    result: Dict[str, Any] = field(default_factory=dict)
    exception_info: Optional[Dict[str, Any]] = None


@dataclass
class QualityReport:
    """Complete data quality report."""

    dataset_name: str
    execution_id: str
    success_percent: float
    successful_expectations: int
    unsuccessful_expectations: int
    results: List[QualityCheckResult] = field(default_factory=list)


class QualityChecker:
    """
    Data Quality Checker.

    Implements common data quality expectations:
    - Completeness (not null, column presence)
    - Uniqueness (unique values, primary keys)
    - Validity (value ranges, patterns, valid codes)
    - Consistency (cross-field checks)
    - Freshness (data recency)
    """

    # Supported expectation types
    EXPECTATION_TYPES = [
        "expect_column_to_exist",
        "expect_column_values_to_not_be_null",
        "expect_column_values_to_be_unique",
        "expect_column_values_to_be_in_set",
        "expect_column_values_to_be_between",
        "expect_column_value_lengths_to_be_between",
        "expect_column_values_to_match_regex",
        "expect_table_row_count_to_be_between",
        "expect_compound_columns_to_be_unique",
    ]

    def __init__(self):
        """Initialize quality checker."""
        pass

    def run_expectations(
        self,
        expectations: List[Dict[str, Any]],
        spark_session,
        table_name: str,
        execution_id: str,
    ) -> QualityReport:
        """
        Run quality expectations against a dataset.

        Args:
            expectations: List of expectations to check
            spark_session: Spark session
            table_name: Table to check
            execution_id: Pipeline execution ID

        Returns:
            QualityReport with results
        """
        results = []

        for expectation in expectations:
            result = self._run_expectation(
                expectation, spark_session, table_name
            )
            results.append(result)

        successful = len([r for r in results if r.success])
        total = len(results)

        return QualityReport(
            dataset_name=table_name,
            execution_id=execution_id,
            success_percent=(successful / total * 100) if total > 0 else 100.0,
            successful_expectations=successful,
            unsuccessful_expectations=total - successful,
            results=results,
        )

    def _run_expectation(
        self,
        expectation: Dict[str, Any],
        spark_session,
        table_name: str,
    ) -> QualityCheckResult:
        """Run a single expectation."""
        exp_type = expectation["expectation_type"]
        column = expectation.get("column_name")
        kwargs = expectation.get("kwargs", {})
        mostly = expectation.get("mostly", 1.0)

        try:
            if exp_type == "expect_column_to_exist":
                return self._expect_column_to_exist(
                    spark_session, table_name, column
                )
            elif exp_type == "expect_column_values_to_not_be_null":
                return self._expect_column_values_to_not_be_null(
                    spark_session, table_name, column, mostly
                )
            elif exp_type == "expect_column_values_to_be_unique":
                return self._expect_column_values_to_be_unique(
                    spark_session, table_name, column, mostly
                )
            elif exp_type == "expect_column_values_to_be_in_set":
                return self._expect_column_values_to_be_in_set(
                    spark_session, table_name, column, kwargs.get("value_set", []), mostly
                )
            elif exp_type == "expect_column_values_to_be_between":
                return self._expect_column_values_to_be_between(
                    spark_session, table_name, column,
                    kwargs.get("min_value"), kwargs.get("max_value"), mostly
                )
            elif exp_type == "expect_table_row_count_to_be_between":
                return self._expect_table_row_count_to_be_between(
                    spark_session, table_name,
                    kwargs.get("min_value", 0), kwargs.get("max_value")
                )
            else:
                return QualityCheckResult(
                    expectation_type=exp_type,
                    column_name=column,
                    success=False,
                    exception_info={"message": f"Unknown expectation type: {exp_type}"},
                )
        except Exception as e:
            return QualityCheckResult(
                expectation_type=exp_type,
                column_name=column,
                success=False,
                exception_info={"message": str(e)},
            )

    def _expect_column_to_exist(
        self,
        spark_session,
        table_name: str,
        column: str,
    ) -> QualityCheckResult:
        """Check if column exists."""
        df = spark_session.table(table_name)
        exists = column in df.columns

        return QualityCheckResult(
            expectation_type="expect_column_to_exist",
            column_name=column,
            success=exists,
            result={"column_exists": exists},
        )

    def _expect_column_values_to_not_be_null(
        self,
        spark_session,
        table_name: str,
        column: str,
        mostly: float = 1.0,
    ) -> QualityCheckResult:
        """Check that column values are not null."""
        result = spark_session.sql(f"""
            SELECT
                COUNT(*) as total,
                COUNT({column}) as non_null
            FROM {table_name}
        """).collect()[0]

        total = result["total"]
        non_null = result["non_null"]
        success_ratio = non_null / total if total > 0 else 1.0

        return QualityCheckResult(
            expectation_type="expect_column_values_to_not_be_null",
            column_name=column,
            success=success_ratio >= mostly,
            result={
                "total_count": total,
                "non_null_count": non_null,
                "null_count": total - non_null,
                "success_ratio": success_ratio,
            },
        )

    def _expect_column_values_to_be_unique(
        self,
        spark_session,
        table_name: str,
        column: str,
        mostly: float = 1.0,
    ) -> QualityCheckResult:
        """Check that column values are unique."""
        result = spark_session.sql(f"""
            SELECT
                COUNT(*) as total,
                COUNT(DISTINCT {column}) as distinct_count
            FROM {table_name}
        """).collect()[0]

        total = result["total"]
        distinct = result["distinct_count"]
        success_ratio = distinct / total if total > 0 else 1.0

        return QualityCheckResult(
            expectation_type="expect_column_values_to_be_unique",
            column_name=column,
            success=success_ratio >= mostly,
            result={
                "total_count": total,
                "distinct_count": distinct,
                "duplicate_count": total - distinct,
                "success_ratio": success_ratio,
            },
        )

    def _expect_column_values_to_be_in_set(
        self,
        spark_session,
        table_name: str,
        column: str,
        value_set: List[Any],
        mostly: float = 1.0,
    ) -> QualityCheckResult:
        """Check that column values are in allowed set."""
        values_str = ", ".join([f"'{v}'" if isinstance(v, str) else str(v) for v in value_set])

        result = spark_session.sql(f"""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN {column} IN ({values_str}) THEN 1 ELSE 0 END) as in_set
            FROM {table_name}
        """).collect()[0]

        total = result["total"]
        in_set = result["in_set"]
        success_ratio = in_set / total if total > 0 else 1.0

        return QualityCheckResult(
            expectation_type="expect_column_values_to_be_in_set",
            column_name=column,
            success=success_ratio >= mostly,
            result={
                "total_count": total,
                "in_set_count": in_set,
                "out_of_set_count": total - in_set,
                "success_ratio": success_ratio,
            },
        )

    def _expect_column_values_to_be_between(
        self,
        spark_session,
        table_name: str,
        column: str,
        min_value: Optional[float],
        max_value: Optional[float],
        mostly: float = 1.0,
    ) -> QualityCheckResult:
        """Check that column values are within range."""
        conditions = []
        if min_value is not None:
            conditions.append(f"{column} >= {min_value}")
        if max_value is not None:
            conditions.append(f"{column} <= {max_value}")

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        result = spark_session.sql(f"""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN {where_clause} THEN 1 ELSE 0 END) as in_range
            FROM {table_name}
            WHERE {column} IS NOT NULL
        """).collect()[0]

        total = result["total"]
        in_range = result["in_range"]
        success_ratio = in_range / total if total > 0 else 1.0

        return QualityCheckResult(
            expectation_type="expect_column_values_to_be_between",
            column_name=column,
            success=success_ratio >= mostly,
            result={
                "total_count": total,
                "in_range_count": in_range,
                "out_of_range_count": total - in_range,
                "success_ratio": success_ratio,
                "min_value": min_value,
                "max_value": max_value,
            },
        )

    def _expect_table_row_count_to_be_between(
        self,
        spark_session,
        table_name: str,
        min_value: int = 0,
        max_value: Optional[int] = None,
    ) -> QualityCheckResult:
        """Check that table row count is within range."""
        row_count = spark_session.sql(
            f"SELECT COUNT(*) FROM {table_name}"
        ).collect()[0][0]

        success = row_count >= min_value
        if max_value is not None:
            success = success and row_count <= max_value

        return QualityCheckResult(
            expectation_type="expect_table_row_count_to_be_between",
            column_name=None,
            success=success,
            result={
                "row_count": row_count,
                "min_value": min_value,
                "max_value": max_value,
            },
        )
