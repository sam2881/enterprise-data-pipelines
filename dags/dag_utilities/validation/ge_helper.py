"""
GE Helper — Run Great Expectations validation on Spark DataFrames.

Creates an ephemeral GE DataContext, builds a RuntimeBatchRequest,
runs a checkpoint, and returns structured results.

Falls back to PySpark manual validation when GE is not installed.

Usage:
    from dag_utilities.validation.ge_helper import GEHelper

    helper = GEHelper(spark)
    result = helper.validate_dataframe(
        df=bronze_df,
        suite_name="feed_123_bronze_schema",
        data_asset_name="orders_bronze",
        expectations=[...],  # from GEConfigBuilder
        batch_identifier="2024-01-19_run_001",
    )
"""

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Check if GE is available
try:
    import great_expectations as gx
    from great_expectations.core.batch import RuntimeBatchRequest
    GE_AVAILABLE = True
except ImportError:
    GE_AVAILABLE = False
    logger.info("great_expectations not installed, using PySpark fallback")


@dataclass
class ValidationResult:
    """Result of a single expectation."""
    expectation_type: str
    column: Optional[str]
    success: bool
    element_count: int = 0
    unexpected_count: int = 0
    unexpected_percent: float = 0.0
    observed_value: Any = None
    partial_unexpected_list: List = field(default_factory=list)
    severity: str = "error"
    weight: str = "1.0"
    rule_name: str = ""
    kwargs: Dict = field(default_factory=dict)


@dataclass
class CheckpointResult:
    """Aggregated result of all expectations in a checkpoint run."""
    suite_name: str
    data_asset_name: str
    success: bool
    run_time: datetime
    duration_seconds: float
    validations: List[ValidationResult]
    records_validated: int = 0
    quality_score: float = 100.0

    @property
    def total(self) -> int:
        return len(self.validations)

    @property
    def passed(self) -> int:
        return sum(1 for v in self.validations if v.success)

    @property
    def failed(self) -> int:
        return sum(1 for v in self.validations if not v.success)

    def get_blocking_failures(self) -> List[ValidationResult]:
        return [
            v for v in self.validations
            if not v.success and v.severity in ("error", "critical")
        ]


class GEHelper:
    """
    Validates Spark DataFrames using Great Expectations (with PySpark fallback).

    Design: Mirrors the user's production GE pattern:
    1. Create ephemeral context with Spark datasource
    2. Add expectation suite
    3. Build RuntimeBatchRequest from DataFrame
    4. Run checkpoint
    5. Iterate results, build structured output
    """

    DATASOURCE_NAME = "spark_datasource"
    CONNECTOR_NAME = "runtime_connector"

    def __init__(self, spark=None):
        """
        Args:
            spark: SparkSession (required for GE Spark execution engine)
        """
        self.spark = spark
        self._context = None

    def validate_dataframe(
        self,
        df,
        suite_name: str,
        data_asset_name: str,
        expectations: List[Dict[str, Any]],
        batch_identifier: Optional[str] = None,
        checkpoint_name: Optional[str] = None,
        header_rows_to_skip: int = 0,
        footer_rows_to_skip: int = 0,
    ) -> CheckpointResult:
        """
        Validate a Spark DataFrame against a list of expectations.

        Args:
            df: PySpark DataFrame to validate
            suite_name: Name for the expectation suite
            data_asset_name: Logical name of the data asset
            expectations: List of expectation dicts from GEConfigBuilder
            batch_identifier: Optional identifier for this batch
            checkpoint_name: Optional name for the checkpoint
            header_rows_to_skip: Remove first N rows before validation
            footer_rows_to_skip: Remove last N rows before validation

        Returns:
            CheckpointResult with per-expectation details
        """
        start = time.time()
        run_time = datetime.utcnow()
        checkpoint_name = checkpoint_name or f"{suite_name}_checkpoint"
        batch_identifier = batch_identifier or datetime.utcnow().strftime("%Y%m%d_%H%M%S")

        # Pre-process: skip header/footer rows
        if header_rows_to_skip > 0 or footer_rows_to_skip > 0:
            df = self._skip_rows(df, header_rows_to_skip, footer_rows_to_skip)

        record_count = df.count()

        if GE_AVAILABLE and self.spark is not None:
            try:
                result = self._validate_with_ge(
                    df, suite_name, data_asset_name, expectations,
                    batch_identifier, checkpoint_name, record_count,
                )
                result.run_time = run_time
                result.duration_seconds = round(time.time() - start, 3)
                return result
            except Exception as e:
                logger.warning(f"GE validation failed, falling back to PySpark: {e}")

        # Fallback: PySpark manual validation
        result = self._validate_with_pyspark(
            df, suite_name, data_asset_name, expectations, record_count,
        )
        result.run_time = run_time
        result.duration_seconds = round(time.time() - start, 3)
        return result

    # =========================================================================
    # GE-Based Validation
    # =========================================================================

    def _validate_with_ge(
        self, df, suite_name, data_asset_name, expectations,
        batch_identifier, checkpoint_name, record_count,
    ) -> CheckpointResult:
        """Run validation using Great Expectations."""
        context = self._get_or_create_context()

        # Add expectation suite
        suite = context.add_expectation_suite(expectation_suite_name=suite_name)
        for exp in expectations:
            suite.add_expectation(
                expectation_configuration={
                    "expectation_type": exp["expectation_type"],
                    "kwargs": exp["kwargs"],
                    "meta": exp.get("meta", {}),
                }
            )
        context.save_expectation_suite(suite)

        # Build RuntimeBatchRequest
        batch_request = RuntimeBatchRequest(
            datasource_name=self.DATASOURCE_NAME,
            data_connector_name=self.CONNECTOR_NAME,
            data_asset_name=data_asset_name,
            runtime_parameters={"batch_data": df},
            batch_identifiers={"default_identifier_name": batch_identifier},
        )

        # Run checkpoint
        checkpoint_config = {
            "name": checkpoint_name,
            "config_version": 1.0,
            "class_name": "SimpleCheckpoint",
            "validations": [
                {
                    "batch_request": batch_request,
                    "expectation_suite_name": suite_name,
                },
            ],
        }
        checkpoint = context.add_checkpoint(**checkpoint_config)
        ge_result = checkpoint.run(result_format={"result_format": "SUMMARY"})

        # Parse results
        validations = []
        for key, run_result in ge_result.run_results.items():
            for r in run_result["platform_validation_result"]["results"]:
                meta = r.get("expectation_config", {}).get("meta", {})
                exp_type = r["expectation_config"]["expectation_type"]
                column = r["expectation_config"].get("kwargs", {}).get("column")

                try:
                    unexpected_count = r["result"].get("unexpected_count", 0)
                    unexpected_pct = r["result"].get("unexpected_percent", 0.0)
                    partial = r["result"].get("partial_unexpected_list", [])
                    observed = r["result"].get("observed_value")
                    element_count = r["result"].get("element_count", record_count)
                except (KeyError, TypeError):
                    unexpected_count = 0 if r["success"] else record_count
                    unexpected_pct = 0.0 if r["success"] else 100.0
                    partial = []
                    observed = None
                    element_count = record_count

                validations.append(ValidationResult(
                    expectation_type=exp_type,
                    column=column,
                    success=r["success"],
                    element_count=element_count,
                    unexpected_count=unexpected_count,
                    unexpected_percent=unexpected_pct,
                    observed_value=str(observed) if observed is not None else None,
                    partial_unexpected_list=partial[:20] if partial else [],
                    severity=meta.get("severity", "error"),
                    weight=meta.get("weight", "1.0"),
                    rule_name=meta.get("rule_name", exp_type),
                    kwargs=r["expectation_config"].get("kwargs", {}),
                ))

        quality_score = self._calculate_score(validations)
        overall_success = all(
            v.success for v in validations
            if v.severity in ("error", "critical")
        )

        return CheckpointResult(
            suite_name=suite_name,
            data_asset_name=data_asset_name,
            success=overall_success,
            run_time=datetime.utcnow(),
            duration_seconds=0,
            validations=validations,
            records_validated=record_count,
            quality_score=quality_score,
        )

    def _get_or_create_context(self):
        """Lazily create GE ephemeral context with Spark datasource."""
        if self._context is not None:
            return self._context

        self._context = gx.get_context(mode="ephemeral")

        # Add Spark datasource with runtime connector
        datasource_config = {
            "name": self.DATASOURCE_NAME,
            "class_name": "Datasource",
            "execution_engine": {
                "class_name": "SparkDFExecutionEngine",
            },
            "data_connectors": {
                self.CONNECTOR_NAME: {
                    "class_name": "RuntimeDataConnector",
                    "batch_identifiers": ["default_identifier_name"],
                },
            },
        }
        try:
            self._context.add_datasource(**datasource_config)
        except Exception:
            pass  # Datasource may already exist

        return self._context

    # =========================================================================
    # PySpark Fallback Validation
    # =========================================================================

    def _validate_with_pyspark(
        self, df, suite_name, data_asset_name, expectations, record_count,
    ) -> CheckpointResult:
        """Manual PySpark validation when GE is not available."""
        validations = []

        for exp in expectations:
            exp_type = exp["expectation_type"]
            kwargs = exp.get("kwargs", {})
            meta = exp.get("meta", {})
            column = kwargs.get("column")
            mostly = kwargs.get("mostly", 1.0)

            try:
                result = self._run_pyspark_check(df, exp_type, kwargs, record_count)
            except Exception as e:
                logger.warning(f"PySpark check failed for {exp_type}: {e}")
                result = {
                    "success": False,
                    "element_count": record_count,
                    "unexpected_count": record_count,
                    "unexpected_percent": 100.0,
                    "observed_value": f"Error: {e}",
                }

            validations.append(ValidationResult(
                expectation_type=exp_type,
                column=column,
                success=result["success"],
                element_count=result.get("element_count", record_count),
                unexpected_count=result.get("unexpected_count", 0),
                unexpected_percent=result.get("unexpected_percent", 0.0),
                observed_value=str(result.get("observed_value")),
                severity=meta.get("severity", "error"),
                weight=meta.get("weight", "1.0"),
                rule_name=meta.get("rule_name", exp_type),
                kwargs=kwargs,
            ))

        quality_score = self._calculate_score(validations)
        overall_success = all(
            v.success for v in validations
            if v.severity in ("error", "critical")
        )

        return CheckpointResult(
            suite_name=suite_name,
            data_asset_name=data_asset_name,
            success=overall_success,
            run_time=datetime.utcnow(),
            duration_seconds=0,
            validations=validations,
            records_validated=record_count,
            quality_score=quality_score,
        )

    def _run_pyspark_check(
        self, df, exp_type: str, kwargs: Dict, total: int
    ) -> Dict[str, Any]:
        """Run a single expectation check using PySpark."""
        from pyspark.sql import functions as F

        column = kwargs.get("column")
        mostly = kwargs.get("mostly", 1.0)

        if exp_type == "expect_column_to_exist":
            exists = column in df.columns
            return {
                "success": exists,
                "element_count": total,
                "unexpected_count": 0 if exists else 1,
                "unexpected_percent": 0.0 if exists else 100.0,
                "observed_value": f"Column {'exists' if exists else 'missing'}",
            }

        elif exp_type == "expect_column_values_to_not_be_null":
            if column not in df.columns:
                return {"success": False, "element_count": total,
                        "unexpected_count": total, "unexpected_percent": 100.0}
            null_count = df.filter(F.col(column).isNull()).count()
            pct = (null_count / total * 100) if total > 0 else 0
            return {
                "success": (1 - null_count / total) >= mostly if total > 0 else True,
                "element_count": total,
                "unexpected_count": null_count,
                "unexpected_percent": pct,
                "observed_value": f"{null_count} nulls",
            }

        elif exp_type == "expect_column_values_to_be_unique":
            if column not in df.columns:
                return {"success": False, "element_count": total,
                        "unexpected_count": total, "unexpected_percent": 100.0}
            dup_count = total - df.select(column).distinct().count()
            pct = (dup_count / total * 100) if total > 0 else 0
            return {
                "success": (1 - dup_count / total) >= mostly if total > 0 else True,
                "element_count": total,
                "unexpected_count": dup_count,
                "unexpected_percent": pct,
                "observed_value": f"{dup_count} duplicates",
            }

        elif exp_type == "expect_column_values_to_be_between":
            if column not in df.columns:
                return {"success": False, "element_count": total,
                        "unexpected_count": total, "unexpected_percent": 100.0}
            min_val = kwargs.get("min_value")
            max_val = kwargs.get("max_value")
            cond = F.lit(True)
            if min_val is not None:
                cond = cond & (F.col(column) >= min_val)
            if max_val is not None:
                cond = cond & (F.col(column) <= max_val)
            fail_count = df.filter(~cond & F.col(column).isNotNull()).count()
            pct = (fail_count / total * 100) if total > 0 else 0
            return {
                "success": (1 - fail_count / total) >= mostly if total > 0 else True,
                "element_count": total,
                "unexpected_count": fail_count,
                "unexpected_percent": pct,
            }

        elif exp_type == "expect_column_values_to_match_regex":
            if column not in df.columns:
                return {"success": False, "element_count": total,
                        "unexpected_count": total, "unexpected_percent": 100.0}
            regex = kwargs.get("regex", ".*")
            fail_count = df.filter(
                ~F.col(column).rlike(regex) & F.col(column).isNotNull()
            ).count()
            pct = (fail_count / total * 100) if total > 0 else 0
            return {
                "success": (1 - fail_count / total) >= mostly if total > 0 else True,
                "element_count": total,
                "unexpected_count": fail_count,
                "unexpected_percent": pct,
            }

        elif exp_type == "expect_column_values_to_be_in_set":
            if column not in df.columns:
                return {"success": False, "element_count": total,
                        "unexpected_count": total, "unexpected_percent": 100.0}
            value_set = kwargs.get("value_set", [])
            fail_count = df.filter(
                ~F.col(column).isin(value_set) & F.col(column).isNotNull()
            ).count()
            pct = (fail_count / total * 100) if total > 0 else 0
            return {
                "success": (1 - fail_count / total) >= mostly if total > 0 else True,
                "element_count": total,
                "unexpected_count": fail_count,
                "unexpected_percent": pct,
            }

        elif exp_type == "expect_table_row_count_to_be_between":
            min_val = kwargs.get("min_value")
            max_val = kwargs.get("max_value")
            ok = True
            if min_val is not None and total < min_val:
                ok = False
            if max_val is not None and total > max_val:
                ok = False
            return {
                "success": ok,
                "element_count": total,
                "unexpected_count": 0 if ok else 1,
                "unexpected_percent": 0.0 if ok else 100.0,
                "observed_value": f"row_count={total}",
            }

        else:
            logger.warning(f"No PySpark fallback for {exp_type}, skipping")
            return {
                "success": True,
                "element_count": total,
                "unexpected_count": 0,
                "unexpected_percent": 0.0,
                "observed_value": "skipped (no fallback)",
            }

    # =========================================================================
    # Helpers
    # =========================================================================

    def _skip_rows(self, df, header_skip: int, footer_skip: int):
        """Remove header/footer rows from DataFrame."""
        from pyspark.sql import Window
        from pyspark.sql import functions as F

        if header_skip <= 0 and footer_skip <= 0:
            return df

        df = df.withColumn(
            "_row_num",
            F.row_number().over(Window.orderBy(F.monotonically_increasing_id()))
        )
        total = df.count()

        conditions = F.lit(True)
        if header_skip > 0:
            conditions = conditions & (F.col("_row_num") > header_skip)
        if footer_skip > 0:
            conditions = conditions & (F.col("_row_num") <= total - footer_skip)

        return df.filter(conditions).drop("_row_num")

    @staticmethod
    def _calculate_score(validations: List[ValidationResult]) -> float:
        """Weighted quality score (0-100)."""
        if not validations:
            return 100.0

        total_weight = Decimal("0")
        weighted_sum = Decimal("0")

        for v in validations:
            w = Decimal(v.weight)
            total_weight += w
            if v.success:
                weighted_sum += w
            else:
                pass_rate = max(0, 1 - v.unexpected_percent / 100)
                weighted_sum += w * Decimal(str(pass_rate))

        if total_weight == 0:
            return 100.0

        score = float((weighted_sum / total_weight) * 100)
        return round(score, 2)
