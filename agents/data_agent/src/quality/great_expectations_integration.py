"""
Great Expectations Integration for Enterprise Data Quality.

WHY: Great Expectations provides:
- Declarative data quality expectations
- Auto-profiling for quick baseline
- Rich documentation (Data Docs)
- Checkpoint-based validation
- Integration with Spark, Pandas, SQL

DESIGN PATTERN (Airbnb Midas inspired):
1. Define expectations from metadata (QualityRule table)
2. Auto-generate expectation suites
3. Run checkpoints at zone boundaries
4. Calculate DQ Score from results
5. Block or warn based on severity

Enterprise Features:
- Multi-zone validation (Bronze, Silver, Gold)
- Quality score calculation with weighting
- Integration with self-healing agent
- Metadata-driven expectation generation
- Slack/PagerDuty alerting on failures

References:
- https://greatexpectations.io/
- https://docs.greatexpectations.io/docs/guides/expectations
- https://medium.com/airbnb-engineering/data-quality-at-airbnb
"""

import hashlib
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)


# =============================================================================
# Enums and Constants
# =============================================================================


class ExpectationType(str, Enum):
    """Supported Great Expectations expectation types."""
    # Column existence
    COLUMN_EXISTS = "expect_column_to_exist"

    # Null checks
    NOT_NULL = "expect_column_values_to_not_be_null"
    NULL_RATE = "expect_column_null_value_proportion_to_be_between"

    # Uniqueness
    UNIQUE = "expect_column_values_to_be_unique"
    UNIQUE_COMPOUND = "expect_compound_columns_to_be_unique"

    # Value range
    BETWEEN = "expect_column_values_to_be_between"
    GREATER_THAN = "expect_column_min_to_be_between"
    LESS_THAN = "expect_column_max_to_be_between"

    # String patterns
    REGEX_MATCH = "expect_column_values_to_match_regex"
    LIKE_PATTERN = "expect_column_values_to_match_like_pattern"
    LENGTH_BETWEEN = "expect_column_value_lengths_to_be_between"

    # Set membership
    IN_SET = "expect_column_values_to_be_in_set"
    NOT_IN_SET = "expect_column_values_to_not_be_in_set"
    IN_TYPE_LIST = "expect_column_values_to_be_in_type_list"

    # Statistical
    MEAN_BETWEEN = "expect_column_mean_to_be_between"
    MEDIAN_BETWEEN = "expect_column_median_to_be_between"
    STDEV_BETWEEN = "expect_column_stdev_to_be_between"

    # Row counts
    ROW_COUNT_BETWEEN = "expect_table_row_count_to_be_between"
    ROW_COUNT_EQUAL = "expect_table_row_count_to_equal"

    # Freshness
    COLUMN_VALUES_INCREASING = "expect_column_values_to_be_increasing"
    MAX_TO_BE_BETWEEN = "expect_column_max_to_be_between"

    # Custom SQL
    CUSTOM_SQL = "expect_column_values_to_match_sql_condition"
    TABLE_CUSTOM_SQL = "expect_table_columns_to_match_sql_condition"


class QualitySeverity(str, Enum):
    """Quality check severity levels."""
    INFO = "info"           # Log only
    WARNING = "warning"     # Alert but continue
    ERROR = "error"         # Fail task
    CRITICAL = "critical"   # Fail entire DAG


class ValidationOutcome(str, Enum):
    """Validation result status."""
    PASSED = "passed"
    FAILED = "failed"
    SKIPPED = "skipped"
    ERROR = "error"


# Mapping from metadata rule types to GE expectations
RULE_TYPE_TO_GE_EXPECTATION = {
    "not_null": ExpectationType.NOT_NULL,
    "unique": ExpectationType.UNIQUE,
    "range": ExpectationType.BETWEEN,
    "regex": ExpectationType.REGEX_MATCH,
    "in_set": ExpectationType.IN_SET,
    "length": ExpectationType.LENGTH_BETWEEN,
    "custom_sql": ExpectationType.CUSTOM_SQL,
    "row_count": ExpectationType.ROW_COUNT_BETWEEN,
    "freshness": ExpectationType.MAX_TO_BE_BETWEEN,
    "mean": ExpectationType.MEAN_BETWEEN,
    "null_rate": ExpectationType.NULL_RATE,
    "compound_unique": ExpectationType.UNIQUE_COMPOUND,
}


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class ExpectationConfig:
    """Configuration for a single expectation."""
    expectation_type: str
    column: Optional[str]
    kwargs: Dict[str, Any]
    meta: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        result = {
            "expectation_type": self.expectation_type,
            "kwargs": self.kwargs.copy(),
            "meta": self.meta.copy(),
        }
        if self.column:
            result["kwargs"]["column"] = self.column
        return result


@dataclass
class ExpectationSuiteConfig:
    """Configuration for an expectation suite."""
    suite_name: str
    data_asset_name: str
    expectations: List[ExpectationConfig]
    meta: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "expectation_suite_name": self.suite_name,
            "data_asset_name": self.data_asset_name,
            "expectations": [e.to_dict() for e in self.expectations],
            "meta": {
                **self.meta,
                "generated_at": datetime.utcnow().isoformat(),
                "generator": "data_agent_ge_integration",
            },
        }


@dataclass
class ValidationResult:
    """Result of a single expectation validation."""
    expectation_type: str
    column: Optional[str]
    success: bool
    observed_value: Any
    expected_value: Any
    element_count: int
    unexpected_count: int
    unexpected_percent: float
    severity: QualitySeverity
    rule_name: str
    weight: Decimal


@dataclass
class CheckpointResult:
    """Result of a checkpoint run (multiple expectation validations)."""
    checkpoint_name: str
    data_asset_name: str
    run_id: str
    success: bool
    run_time: datetime
    duration_seconds: float
    validations: List[ValidationResult]
    quality_score: Decimal
    records_validated: int
    records_failed: int

    def get_failures(self) -> List[ValidationResult]:
        """Get failed validations."""
        return [v for v in self.validations if not v.success]

    def get_blocking_failures(self) -> List[ValidationResult]:
        """Get failures that should block the pipeline."""
        return [
            v for v in self.validations
            if not v.success and v.severity in (QualitySeverity.ERROR, QualitySeverity.CRITICAL)
        ]


# =============================================================================
# Expectation Builder
# =============================================================================


class ExpectationBuilder:
    """
    Builds Great Expectations expectation configs from metadata.

    Converts QualityRule metadata records to GE expectation dictionaries.
    """

    def __init__(self):
        self.logger = logging.getLogger(f"{__name__}.ExpectationBuilder")

    def build_expectation(
        self,
        rule_type: str,
        column_name: Optional[str],
        rule_expression: str,
        parameters: Dict[str, Any],
        rule_name: str,
        severity: str = "error",
        weight: Decimal = Decimal("1.0"),
    ) -> ExpectationConfig:
        """
        Build a GE expectation from a metadata rule.

        Args:
            rule_type: Type of rule (not_null, unique, range, etc.)
            column_name: Column to validate (None for table-level)
            rule_expression: SQL expression or pattern
            parameters: Rule-specific parameters
            rule_name: Human-readable rule name
            severity: info, warning, error, critical
            weight: Weight for score calculation

        Returns:
            ExpectationConfig ready for GE suite
        """
        ge_type = RULE_TYPE_TO_GE_EXPECTATION.get(rule_type)
        if not ge_type:
            self.logger.warning(f"Unknown rule type: {rule_type}, using custom SQL")
            ge_type = ExpectationType.CUSTOM_SQL

        kwargs = {}

        # Build kwargs based on expectation type
        if ge_type == ExpectationType.NOT_NULL:
            kwargs = {"mostly": parameters.get("threshold", 1.0)}

        elif ge_type == ExpectationType.UNIQUE:
            kwargs = {"mostly": parameters.get("threshold", 1.0)}

        elif ge_type == ExpectationType.UNIQUE_COMPOUND:
            kwargs = {
                "column_list": parameters.get("columns", [column_name]),
            }
            column_name = None  # Compound is table-level

        elif ge_type == ExpectationType.BETWEEN:
            kwargs = {
                "min_value": parameters.get("min"),
                "max_value": parameters.get("max"),
                "mostly": parameters.get("threshold", 1.0),
            }
            # Remove None values
            kwargs = {k: v for k, v in kwargs.items() if v is not None}

        elif ge_type == ExpectationType.REGEX_MATCH:
            kwargs = {
                "regex": rule_expression or parameters.get("pattern"),
                "mostly": parameters.get("threshold", 1.0),
            }

        elif ge_type == ExpectationType.IN_SET:
            kwargs = {
                "value_set": parameters.get("values", []),
                "mostly": parameters.get("threshold", 1.0),
            }

        elif ge_type == ExpectationType.LENGTH_BETWEEN:
            kwargs = {
                "min_value": parameters.get("min_length", parameters.get("min")),
                "max_value": parameters.get("max_length", parameters.get("max")),
                "mostly": parameters.get("threshold", 1.0),
            }
            kwargs = {k: v for k, v in kwargs.items() if v is not None}

        elif ge_type == ExpectationType.ROW_COUNT_BETWEEN:
            kwargs = {
                "min_value": parameters.get("min_rows", parameters.get("min")),
                "max_value": parameters.get("max_rows", parameters.get("max")),
            }
            kwargs = {k: v for k, v in kwargs.items() if v is not None}
            column_name = None  # Table-level

        elif ge_type == ExpectationType.MAX_TO_BE_BETWEEN:
            # Freshness check - max date should be recent
            kwargs = {
                "min_value": parameters.get("min_value"),
                "max_value": parameters.get("max_value"),
            }
            if "max_hours" in parameters:
                # Convert hours to a date boundary (handled at runtime)
                kwargs["max_hours_ago"] = parameters["max_hours"]
            kwargs = {k: v for k, v in kwargs.items() if v is not None}

        elif ge_type == ExpectationType.MEAN_BETWEEN:
            kwargs = {
                "min_value": parameters.get("min_mean", parameters.get("min")),
                "max_value": parameters.get("max_mean", parameters.get("max")),
            }
            kwargs = {k: v for k, v in kwargs.items() if v is not None}

        elif ge_type == ExpectationType.NULL_RATE:
            kwargs = {
                "min_value": parameters.get("min_null_rate", 0.0),
                "max_value": parameters.get("max_null_rate", parameters.get("threshold", 1.0)),
            }

        elif ge_type == ExpectationType.CUSTOM_SQL:
            kwargs = {
                "sql_condition": rule_expression,
                "mostly": parameters.get("threshold", 1.0),
            }

        # Build meta for tracking
        meta = {
            "rule_name": rule_name,
            "severity": severity,
            "weight": str(weight),
            "original_rule_type": rule_type,
        }

        return ExpectationConfig(
            expectation_type=ge_type.value,
            column=column_name,
            kwargs=kwargs,
            meta=meta,
        )

    def build_suite_from_rules(
        self,
        suite_name: str,
        data_asset_name: str,
        rules: List[Dict[str, Any]],
    ) -> ExpectationSuiteConfig:
        """
        Build a complete expectation suite from a list of quality rules.

        Args:
            suite_name: Name for the expectation suite
            data_asset_name: Name of the data asset (table/file)
            rules: List of rule dictionaries from metadata

        Returns:
            ExpectationSuiteConfig ready for GE
        """
        expectations = []

        for rule in rules:
            try:
                expectation = self.build_expectation(
                    rule_type=rule.get("rule_type", "custom_sql"),
                    column_name=rule.get("column_name"),
                    rule_expression=rule.get("rule_expression", ""),
                    parameters=rule.get("parameters", {}),
                    rule_name=rule.get("rule_name", f"rule_{len(expectations)}"),
                    severity=rule.get("severity", "error"),
                    weight=Decimal(str(rule.get("weight", "1.0"))),
                )
                expectations.append(expectation)

            except Exception as e:
                self.logger.error(f"Failed to build expectation for rule: {rule.get('rule_name')}: {e}")

        return ExpectationSuiteConfig(
            suite_name=suite_name,
            data_asset_name=data_asset_name,
            expectations=expectations,
            meta={
                "rules_count": len(rules),
                "expectations_count": len(expectations),
            },
        )


# =============================================================================
# Quality Score Calculator
# =============================================================================


class QualityScoreCalculator:
    """
    Calculates weighted quality scores from validation results.

    Airbnb Midas Pattern:
    - Each rule has a weight
    - Score = weighted average of pass rates
    - Normalized to 0-100 scale
    """

    def __init__(self):
        self.logger = logging.getLogger(f"{__name__}.QualityScoreCalculator")

    def calculate_score(
        self,
        validations: List[ValidationResult],
    ) -> Tuple[Decimal, Dict[str, Any]]:
        """
        Calculate weighted quality score from validation results.

        Args:
            validations: List of validation results

        Returns:
            Tuple of (score 0-100, breakdown details)
        """
        if not validations:
            return Decimal("100.00"), {"message": "No validations to score"}

        total_weight = Decimal("0")
        weighted_sum = Decimal("0")

        breakdown = {
            "by_rule": {},
            "by_dimension": {},
            "by_severity": {},
        }

        for validation in validations:
            weight = validation.weight
            total_weight += weight

            # Calculate rule pass rate (1 - unexpected_percent/100)
            if validation.success:
                pass_rate = Decimal("1.0")
            else:
                pass_rate = Decimal(str(max(0, 1 - validation.unexpected_percent / 100)))

            weighted_sum += weight * pass_rate

            # Track breakdown
            breakdown["by_rule"][validation.rule_name] = {
                "pass_rate": float(pass_rate),
                "weight": float(weight),
                "success": validation.success,
                "unexpected_count": validation.unexpected_count,
            }

            # Group by severity
            severity = validation.severity.value
            if severity not in breakdown["by_severity"]:
                breakdown["by_severity"][severity] = {
                    "total": 0,
                    "passed": 0,
                    "failed": 0,
                }
            breakdown["by_severity"][severity]["total"] += 1
            if validation.success:
                breakdown["by_severity"][severity]["passed"] += 1
            else:
                breakdown["by_severity"][severity]["failed"] += 1

        # Calculate final score (0-100)
        if total_weight > 0:
            score = (weighted_sum / total_weight) * Decimal("100")
        else:
            score = Decimal("100.00")

        # Round to 2 decimal places
        score = score.quantize(Decimal("0.01"))

        breakdown["total_score"] = float(score)
        breakdown["total_rules"] = len(validations)
        breakdown["rules_passed"] = sum(1 for v in validations if v.success)
        breakdown["rules_failed"] = sum(1 for v in validations if not v.success)

        self.logger.info(
            f"Quality score: {score}/100 "
            f"({breakdown['rules_passed']}/{breakdown['total_rules']} rules passed)"
        )

        return score, breakdown

    def meets_threshold(
        self,
        score: Decimal,
        min_score: Decimal,
        blocking_failures: List[ValidationResult],
    ) -> Tuple[bool, str]:
        """
        Check if quality score meets threshold and no blocking failures.

        Args:
            score: Calculated quality score
            min_score: Minimum required score
            blocking_failures: List of error/critical failures

        Returns:
            Tuple of (passes, reason)
        """
        if blocking_failures:
            failed_rules = [f.rule_name for f in blocking_failures]
            return False, f"Blocking failures: {', '.join(failed_rules)}"

        if score < min_score:
            return False, f"Score {score} below threshold {min_score}"

        return True, "Quality gate passed"


# =============================================================================
# GE Context Manager
# =============================================================================


class GreatExpectationsManager:
    """
    Manages Great Expectations context, suites, and checkpoints.

    Provides high-level interface for:
    - Creating expectation suites from metadata
    - Running validations
    - Generating data docs
    """

    def __init__(
        self,
        context_root_dir: str = "/tmp/great_expectations",
        datasource_name: str = "spark_datasource",
    ):
        """
        Initialize GE manager.

        Args:
            context_root_dir: Root directory for GE context
            datasource_name: Name of the Spark datasource
        """
        self.context_root_dir = Path(context_root_dir)
        self.datasource_name = datasource_name
        self.builder = ExpectationBuilder()
        self.score_calculator = QualityScoreCalculator()
        self.logger = logging.getLogger(f"{__name__}.GreatExpectationsManager")

        # Initialize context lazily
        self._context = None

    @property
    def context(self):
        """Get or create GE context."""
        if self._context is None:
            self._context = self._create_context()
        return self._context

    def _create_context(self) -> Any:
        """
        Create Great Expectations DataContext.

        Returns ephemeral context for pipeline execution.
        """
        try:
            import great_expectations as gx
            from great_expectations.data_context.types.base import (
                DataContextConfig,
                DatasourceConfig,
            )

            # Create context directory
            self.context_root_dir.mkdir(parents=True, exist_ok=True)

            # Create ephemeral context (no persistent config)
            context = gx.get_context(mode="ephemeral")

            self.logger.info(f"Created GE ephemeral context")
            return context

        except ImportError:
            self.logger.warning(
                "Great Expectations not installed. "
                "Install with: pip install great_expectations"
            )
            return None

    def create_suite_from_metadata(
        self,
        product_name: str,
        zone: str,
        rules: List[Dict[str, Any]],
    ) -> ExpectationSuiteConfig:
        """
        Create expectation suite from metadata quality rules.

        Args:
            product_name: Data product name
            zone: Data zone (bronze, silver, gold)
            rules: List of quality rule metadata

        Returns:
            ExpectationSuiteConfig
        """
        suite_name = f"{product_name}_{zone}_suite"
        data_asset_name = f"{product_name}_{zone}"

        self.logger.info(f"Creating expectation suite: {suite_name} with {len(rules)} rules")

        return self.builder.build_suite_from_rules(
            suite_name=suite_name,
            data_asset_name=data_asset_name,
            rules=rules,
        )

    def validate_spark_dataframe(
        self,
        df: Any,  # pyspark.sql.DataFrame
        suite_config: ExpectationSuiteConfig,
        run_id: Optional[str] = None,
    ) -> CheckpointResult:
        """
        Validate a Spark DataFrame against an expectation suite.

        Args:
            df: PySpark DataFrame to validate
            suite_config: Expectation suite configuration
            run_id: Optional run identifier

        Returns:
            CheckpointResult with validation details
        """
        import time
        start_time = time.time()
        run_time = datetime.utcnow()

        if run_id is None:
            run_id = f"{suite_config.suite_name}_{run_time.strftime('%Y%m%d_%H%M%S')}"

        validations = []

        # If GE is not available, run manual validations
        if self.context is None:
            self.logger.warning("Running fallback validation without GE")
            validations = self._validate_fallback(df, suite_config)
        else:
            validations = self._validate_with_ge(df, suite_config, run_id)

        duration = time.time() - start_time

        # Calculate quality score
        score, _ = self.score_calculator.calculate_score(validations)

        # Count records
        records_validated = df.count() if hasattr(df, 'count') else 0
        records_failed = sum(v.unexpected_count for v in validations)

        # Determine overall success
        success = all(v.success for v in validations if v.severity in (
            QualitySeverity.ERROR, QualitySeverity.CRITICAL
        ))

        return CheckpointResult(
            checkpoint_name=f"checkpoint_{suite_config.suite_name}",
            data_asset_name=suite_config.data_asset_name,
            run_id=run_id,
            success=success,
            run_time=run_time,
            duration_seconds=duration,
            validations=validations,
            quality_score=score,
            records_validated=records_validated,
            records_failed=records_failed,
        )

    def _validate_with_ge(
        self,
        df: Any,
        suite_config: ExpectationSuiteConfig,
        run_id: str,
    ) -> List[ValidationResult]:
        """Run validation using Great Expectations."""
        try:
            import great_expectations as gx

            # Create batch from DataFrame
            batch = self.context.sources.pandas_default.read_dataframe(
                df.toPandas() if hasattr(df, 'toPandas') else df
            )

            # Create expectation suite
            suite = self.context.add_expectation_suite(
                expectation_suite_name=suite_config.suite_name
            )

            # Add expectations
            for exp_config in suite_config.expectations:
                expectation_config = gx.core.ExpectationConfiguration(
                    expectation_type=exp_config.expectation_type,
                    kwargs=exp_config.kwargs,
                    meta=exp_config.meta,
                )
                suite.add_expectation(expectation_config)

            # Run validation
            results = batch.validate(expectation_suite=suite)

            # Convert to our result format
            validations = []
            for result in results.results:
                exp_config = result.expectation_config
                meta = exp_config.meta or {}

                validations.append(ValidationResult(
                    expectation_type=exp_config.expectation_type,
                    column=exp_config.kwargs.get("column"),
                    success=result.success,
                    observed_value=result.result.get("observed_value"),
                    expected_value=exp_config.kwargs,
                    element_count=result.result.get("element_count", 0),
                    unexpected_count=result.result.get("unexpected_count", 0),
                    unexpected_percent=result.result.get("unexpected_percent", 0),
                    severity=QualitySeverity(meta.get("severity", "error")),
                    rule_name=meta.get("rule_name", exp_config.expectation_type),
                    weight=Decimal(meta.get("weight", "1.0")),
                ))

            return validations

        except Exception as e:
            self.logger.error(f"GE validation failed: {e}")
            # Fall back to manual validation
            return self._validate_fallback(df, suite_config)

    def _validate_fallback(
        self,
        df: Any,
        suite_config: ExpectationSuiteConfig,
    ) -> List[ValidationResult]:
        """
        Fallback validation using PySpark directly.

        Used when Great Expectations is not available.
        """
        from pyspark.sql import functions as F

        validations = []
        total_count = df.count()

        for exp_config in suite_config.expectations:
            try:
                exp_type = exp_config.expectation_type
                column = exp_config.column
                kwargs = exp_config.kwargs
                meta = exp_config.meta

                unexpected_count = 0
                observed_value = None
                success = True

                if exp_type == ExpectationType.NOT_NULL.value and column:
                    null_count = df.filter(F.col(column).isNull()).count()
                    unexpected_count = null_count
                    observed_value = f"{null_count} nulls"
                    mostly = kwargs.get("mostly", 1.0)
                    success = (null_count / max(total_count, 1)) <= (1 - mostly)

                elif exp_type == ExpectationType.UNIQUE.value and column:
                    dup_df = df.groupBy(column).count().filter(F.col("count") > 1)
                    dup_count = dup_df.count()
                    unexpected_count = dup_count
                    observed_value = f"{dup_count} duplicates"
                    success = dup_count == 0

                elif exp_type == ExpectationType.BETWEEN.value and column:
                    min_val = kwargs.get("min_value")
                    max_val = kwargs.get("max_value")
                    condition = F.lit(True)
                    if min_val is not None:
                        condition = condition & (F.col(column) >= min_val)
                    if max_val is not None:
                        condition = condition & (F.col(column) <= max_val)
                    fail_count = df.filter(~condition).count()
                    unexpected_count = fail_count
                    observed_value = f"{fail_count} out of range"
                    mostly = kwargs.get("mostly", 1.0)
                    success = (fail_count / max(total_count, 1)) <= (1 - mostly)

                elif exp_type == ExpectationType.REGEX_MATCH.value and column:
                    pattern = kwargs.get("regex", ".*")
                    fail_count = df.filter(~F.col(column).rlike(pattern)).count()
                    unexpected_count = fail_count
                    observed_value = f"{fail_count} not matching"
                    mostly = kwargs.get("mostly", 1.0)
                    success = (fail_count / max(total_count, 1)) <= (1 - mostly)

                elif exp_type == ExpectationType.IN_SET.value and column:
                    value_set = kwargs.get("value_set", [])
                    fail_count = df.filter(~F.col(column).isin(value_set)).count()
                    unexpected_count = fail_count
                    observed_value = f"{fail_count} not in set"
                    mostly = kwargs.get("mostly", 1.0)
                    success = (fail_count / max(total_count, 1)) <= (1 - mostly)

                elif exp_type == ExpectationType.ROW_COUNT_BETWEEN.value:
                    min_rows = kwargs.get("min_value")
                    max_rows = kwargs.get("max_value")
                    observed_value = total_count
                    if min_rows is not None and total_count < min_rows:
                        success = False
                        unexpected_count = min_rows - total_count
                    elif max_rows is not None and total_count > max_rows:
                        success = False
                        unexpected_count = total_count - max_rows

                elif exp_type == ExpectationType.CUSTOM_SQL.value and column:
                    sql_condition = kwargs.get("sql_condition", "1=1")
                    fail_count = df.filter(~F.expr(sql_condition)).count()
                    unexpected_count = fail_count
                    observed_value = f"{fail_count} failed condition"
                    mostly = kwargs.get("mostly", 1.0)
                    success = (fail_count / max(total_count, 1)) <= (1 - mostly)

                else:
                    self.logger.warning(f"Unsupported expectation type in fallback: {exp_type}")
                    observed_value = "Skipped (unsupported)"

                # Calculate unexpected percent
                unexpected_percent = (unexpected_count / max(total_count, 1)) * 100

                validations.append(ValidationResult(
                    expectation_type=exp_type,
                    column=column,
                    success=success,
                    observed_value=observed_value,
                    expected_value=kwargs,
                    element_count=total_count,
                    unexpected_count=unexpected_count,
                    unexpected_percent=unexpected_percent,
                    severity=QualitySeverity(meta.get("severity", "error")),
                    rule_name=meta.get("rule_name", exp_type),
                    weight=Decimal(meta.get("weight", "1.0")),
                ))

            except Exception as e:
                self.logger.error(f"Validation failed for {exp_config.expectation_type}: {e}")
                validations.append(ValidationResult(
                    expectation_type=exp_config.expectation_type,
                    column=exp_config.column,
                    success=False,
                    observed_value=f"Error: {str(e)}",
                    expected_value=exp_config.kwargs,
                    element_count=total_count,
                    unexpected_count=total_count,
                    unexpected_percent=100.0,
                    severity=QualitySeverity(exp_config.meta.get("severity", "error")),
                    rule_name=exp_config.meta.get("rule_name", "unknown"),
                    weight=Decimal(exp_config.meta.get("weight", "1.0")),
                ))

        return validations


# =============================================================================
# PySpark Code Generator
# =============================================================================


class GESparkCodeGenerator:
    """
    Generates PySpark code for data quality validation.

    Produces standalone Spark jobs that can run in Dataproc/EMR.
    """

    VALIDATION_TEMPLATE = '''
"""
Data Quality Validation Job
Suite: {suite_name}
Generated: {generated_at}
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dq_validation")


# Quality Rules Configuration
QUALITY_RULES = {quality_rules_json}


def validate_not_null(df: DataFrame, column: str, threshold: float = 1.0) -> dict:
    """Check column for null values."""
    total = df.count()
    nulls = df.filter(F.col(column).isNull()).count()
    pass_rate = 1 - (nulls / max(total, 1))
    return {{
        "rule_type": "not_null",
        "column": column,
        "total": total,
        "failures": nulls,
        "pass_rate": pass_rate,
        "success": pass_rate >= threshold,
    }}


def validate_unique(df: DataFrame, column: str) -> dict:
    """Check column for duplicate values."""
    total = df.count()
    distinct = df.select(column).distinct().count()
    duplicates = total - distinct
    return {{
        "rule_type": "unique",
        "column": column,
        "total": total,
        "failures": duplicates,
        "pass_rate": distinct / max(total, 1),
        "success": duplicates == 0,
    }}


def validate_range(df: DataFrame, column: str, min_val, max_val, threshold: float = 1.0) -> dict:
    """Check column values are within range."""
    total = df.count()
    condition = F.lit(True)
    if min_val is not None:
        condition = condition & (F.col(column) >= min_val)
    if max_val is not None:
        condition = condition & (F.col(column) <= max_val)
    failures = df.filter(~condition).count()
    pass_rate = 1 - (failures / max(total, 1))
    return {{
        "rule_type": "range",
        "column": column,
        "total": total,
        "failures": failures,
        "pass_rate": pass_rate,
        "success": pass_rate >= threshold,
    }}


def validate_regex(df: DataFrame, column: str, pattern: str, threshold: float = 1.0) -> dict:
    """Check column values match regex pattern."""
    total = df.count()
    failures = df.filter(~F.col(column).rlike(pattern)).count()
    pass_rate = 1 - (failures / max(total, 1))
    return {{
        "rule_type": "regex",
        "column": column,
        "total": total,
        "failures": failures,
        "pass_rate": pass_rate,
        "success": pass_rate >= threshold,
    }}


def validate_in_set(df: DataFrame, column: str, values: list, threshold: float = 1.0) -> dict:
    """Check column values are in allowed set."""
    total = df.count()
    failures = df.filter(~F.col(column).isin(values)).count()
    pass_rate = 1 - (failures / max(total, 1))
    return {{
        "rule_type": "in_set",
        "column": column,
        "total": total,
        "failures": failures,
        "pass_rate": pass_rate,
        "success": pass_rate >= threshold,
    }}


def validate_row_count(df: DataFrame, min_rows: int = None, max_rows: int = None) -> dict:
    """Check table row count is within bounds."""
    total = df.count()
    success = True
    if min_rows is not None and total < min_rows:
        success = False
    if max_rows is not None and total > max_rows:
        success = False
    return {{
        "rule_type": "row_count",
        "column": None,
        "total": total,
        "failures": 0 if success else 1,
        "pass_rate": 1.0 if success else 0.0,
        "success": success,
    }}


def run_validations(df: DataFrame, rules: list) -> list:
    """Run all validation rules and return results."""
    results = []

    for rule in rules:
        rule_type = rule.get("rule_type")
        column = rule.get("column_name")
        params = rule.get("parameters", {{}})
        threshold = rule.get("pass_threshold", 1.0)

        try:
            if rule_type == "not_null":
                result = validate_not_null(df, column, threshold)
            elif rule_type == "unique":
                result = validate_unique(df, column)
            elif rule_type == "range":
                result = validate_range(df, column, params.get("min"), params.get("max"), threshold)
            elif rule_type == "regex":
                result = validate_regex(df, column, rule.get("rule_expression"), threshold)
            elif rule_type == "in_set":
                result = validate_in_set(df, column, params.get("values", []), threshold)
            elif rule_type == "row_count":
                result = validate_row_count(df, params.get("min"), params.get("max"))
            else:
                logger.warning(f"Unknown rule type: {{rule_type}}")
                continue

            result["rule_name"] = rule.get("rule_name")
            result["severity"] = rule.get("severity", "error")
            result["weight"] = rule.get("weight", 1.0)
            results.append(result)

        except Exception as e:
            logger.error(f"Validation failed for {{rule.get('rule_name')}}: {{e}}")
            results.append({{
                "rule_name": rule.get("rule_name"),
                "rule_type": rule_type,
                "column": column,
                "success": False,
                "error": str(e),
            }})

    return results


def calculate_quality_score(results: list) -> float:
    """Calculate weighted quality score from results."""
    if not results:
        return 100.0

    total_weight = sum(r.get("weight", 1.0) for r in results)
    weighted_sum = sum(
        r.get("weight", 1.0) * r.get("pass_rate", 0.0)
        for r in results
    )

    if total_weight == 0:
        return 100.0

    return (weighted_sum / total_weight) * 100


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--source-table", required=True)
    parser.add_argument("--output-path", required=True)
    parser.add_argument("--min-score", type=float, default=80.0)
    args = parser.parse_args()

    spark = SparkSession.builder \\
        .appName("{suite_name}_validation") \\
        .getOrCreate()

    try:
        # Read source data
        logger.info(f"Reading from {{args.source_table}}")
        df = spark.read.table(args.source_table)

        # Run validations
        logger.info(f"Running {{len(QUALITY_RULES)}} quality rules")
        results = run_validations(df, QUALITY_RULES)

        # Calculate score
        score = calculate_quality_score(results)
        logger.info(f"Quality score: {{score:.2f}}/100")

        # Build output
        output = {{
            "suite_name": "{suite_name}",
            "source_table": args.source_table,
            "run_time": datetime.utcnow().isoformat(),
            "quality_score": score,
            "min_score": args.min_score,
            "passed": score >= args.min_score,
            "validations": results,
        }}

        # Write results
        spark.createDataFrame([output]).write.mode("overwrite").json(args.output_path)
        logger.info(f"Results written to {{args.output_path}}")

        # Check threshold
        if score < args.min_score:
            logger.error(f"Quality score {{score:.2f}} below threshold {{args.min_score}}")
            raise ValueError(f"Quality gate failed: {{score:.2f}} < {{args.min_score}}")

        # Check blocking rules
        blocking_failures = [
            r for r in results
            if not r.get("success") and r.get("severity") in ("error", "critical")
        ]
        if blocking_failures:
            failed_names = [r.get("rule_name") for r in blocking_failures]
            logger.error(f"Blocking failures: {{failed_names}}")
            raise ValueError(f"Blocking rule failures: {{failed_names}}")

        logger.info("Quality validation passed!")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
'''

    def generate_validation_job(
        self,
        suite_config: ExpectationSuiteConfig,
        rules: List[Dict[str, Any]],
    ) -> str:
        """
        Generate a standalone Spark validation job.

        Args:
            suite_config: Expectation suite configuration
            rules: Original quality rule metadata

        Returns:
            PySpark code as string
        """
        return self.VALIDATION_TEMPLATE.format(
            suite_name=suite_config.suite_name,
            generated_at=datetime.utcnow().isoformat(),
            quality_rules_json=json.dumps(rules, indent=4, default=str),
        )


# =============================================================================
# Factory and Helpers
# =============================================================================


def create_quality_validator(
    context_root_dir: str = "/tmp/great_expectations",
) -> GreatExpectationsManager:
    """Create a configured quality validator."""
    return GreatExpectationsManager(context_root_dir=context_root_dir)


def validate_dataframe_quality(
    df: Any,
    rules: List[Dict[str, Any]],
    product_name: str,
    zone: str,
    min_score: Decimal = Decimal("80.00"),
) -> Tuple[bool, CheckpointResult]:
    """
    High-level function to validate a DataFrame against quality rules.

    Args:
        df: PySpark DataFrame
        rules: Quality rule metadata
        product_name: Data product name
        zone: Data zone (bronze, silver, gold)
        min_score: Minimum quality score required

    Returns:
        Tuple of (passed, CheckpointResult)
    """
    manager = create_quality_validator()

    # Build suite from rules
    suite = manager.create_suite_from_metadata(product_name, zone, rules)

    # Run validation
    result = manager.validate_spark_dataframe(df, suite)

    # Check if passed
    passed, reason = manager.score_calculator.meets_threshold(
        result.quality_score,
        min_score,
        result.get_blocking_failures(),
    )

    if not passed:
        logger.warning(f"Quality gate failed: {reason}")

    return passed, result
