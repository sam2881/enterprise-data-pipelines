"""
APEX Data Agent - Semantic Validator

Validates business rules and semantic constraints in Silver zone.
"""

from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field

from dag_utilities.core.metadata_client import MetadataClient
from dag_utilities.validation.schema_validator import ValidationResult


@dataclass
class SemanticValidationReport:
    """Complete semantic validation report."""

    contract_id: str
    zone: str
    execution_id: str
    total_rules: int
    passed_rules: int
    failed_rules: int
    is_valid: bool
    results: List[ValidationResult] = field(default_factory=list)
    referential_failures: List[ValidationResult] = field(default_factory=list)


class SemanticValidator:
    """
    Semantic Validator for Silver zone.

    Validates:
    - Business rules (value ranges, valid codes)
    - Referential integrity (foreign key relationships)
    - Cross-field validations
    - Temporal consistency
    """

    def __init__(self):
        """Initialize semantic validator."""
        self.metadata_client = MetadataClient()

    def validate(
        self,
        contract_id: str,
        zone: str,
        execution_id: str,
        spark_session=None,
    ) -> SemanticValidationReport:
        """
        Validate data against semantic rules.

        Args:
            contract_id: Data contract ID
            zone: Data zone (typically SILVER)
            execution_id: Pipeline execution ID
            spark_session: Spark session for validation

        Returns:
            SemanticValidationReport with validation results
        """
        # Get semantic validation rules
        rules = self.metadata_client.get_validation_rules(contract_id, zone)
        semantic_rules = [r for r in rules if r["validation_type"] in ["SEMANTIC", "REFERENTIAL"]]

        # Execute validations
        results = []
        referential_failures = []

        for rule in semantic_rules:
            result = self._execute_rule(rule, contract_id, execution_id, spark_session)
            results.append(result)

            if not result.is_passed and rule["validation_type"] == "REFERENTIAL":
                referential_failures.append(result)

        # Determine overall validity
        is_valid = all(r.is_passed or not r.is_blocking for r in results)

        return SemanticValidationReport(
            contract_id=contract_id,
            zone=zone,
            execution_id=execution_id,
            total_rules=len(results),
            passed_rules=len([r for r in results if r.is_passed]),
            failed_rules=len([r for r in results if not r.is_passed]),
            is_valid=is_valid,
            results=results,
            referential_failures=referential_failures,
        )

    def _execute_rule(
        self,
        rule: Dict[str, Any],
        contract_id: str,
        execution_id: str,
        spark_session=None,
    ) -> ValidationResult:
        """Execute a semantic validation rule."""
        result = ValidationResult(
            rule_name=rule["rule_name"],
            rule_type=rule["validation_type"],
            is_passed=True,
            total_records=0,
            passed_records=0,
            failed_records=0,
            pass_percentage=100.0,
            threshold_percentage=rule["threshold_pct"],
            is_blocking=rule["is_blocking"],
        )

        if spark_session is None:
            return result

        try:
            contract = self.metadata_client.get_contract(contract_id)
            table_name = contract.silver_table
            expression = rule["rule_expression"]

            # Count total and passing records
            total_count = spark_session.sql(
                f"SELECT COUNT(*) FROM {table_name}"
            ).collect()[0][0]
            result.total_records = total_count

            if total_count == 0:
                return result

            passed_count = spark_session.sql(f"""
                SELECT COUNT(*) FROM {table_name}
                WHERE {expression}
            """).collect()[0][0]

            result.passed_records = passed_count
            result.failed_records = total_count - passed_count
            result.pass_percentage = (passed_count / total_count * 100)
            result.is_passed = result.pass_percentage >= (100 - result.threshold_percentage)

        except Exception as e:
            result.is_passed = False
            result.error_message = str(e)

        return result

    def validate_referential_integrity(
        self,
        child_table: str,
        child_column: str,
        parent_table: str,
        parent_column: str,
        spark_session,
    ) -> ValidationResult:
        """
        Validate referential integrity between tables.

        Returns:
            ValidationResult with orphan record details
        """
        result = ValidationResult(
            rule_name=f"ri_{child_table}_{child_column}",
            rule_type="REFERENTIAL",
            is_passed=True,
            total_records=0,
            passed_records=0,
            failed_records=0,
            pass_percentage=100.0,
            threshold_percentage=0.0,
            is_blocking=True,
        )

        try:
            # Find orphan records
            orphan_df = spark_session.sql(f"""
                SELECT c.* FROM {child_table} c
                LEFT JOIN {parent_table} p ON c.{child_column} = p.{parent_column}
                WHERE p.{parent_column} IS NULL AND c.{child_column} IS NOT NULL
            """)

            total_df = spark_session.sql(f"SELECT COUNT(*) FROM {child_table}")

            result.failed_records = orphan_df.count()
            result.total_records = total_df.collect()[0][0]
            result.passed_records = result.total_records - result.failed_records

            if result.total_records > 0:
                result.pass_percentage = (
                    result.passed_records / result.total_records * 100
                )

            result.is_passed = result.failed_records == 0

            if not result.is_passed:
                result.sample_failures = [
                    row.asDict() for row in orphan_df.limit(10).collect()
                ]

        except Exception as e:
            result.is_passed = False
            result.error_message = str(e)

        return result
