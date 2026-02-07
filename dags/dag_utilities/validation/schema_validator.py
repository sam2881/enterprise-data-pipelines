"""
APEX Data Agent - Schema Validator

Validates data against schema definitions in Bronze zone.
Checks column presence, data types, nullability, and constraints.
"""

from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field

from dag_utilities.core.exceptions import ValidationError
from dag_utilities.core.metadata_client import MetadataClient


@dataclass
class ValidationResult:
    """Result of a validation rule execution."""

    rule_name: str
    rule_type: str
    is_passed: bool
    total_records: int
    passed_records: int
    failed_records: int
    pass_percentage: float
    threshold_percentage: float
    is_blocking: bool
    sample_failures: List[Dict[str, Any]] = field(default_factory=list)
    error_message: Optional[str] = None


@dataclass
class SchemaValidationReport:
    """Complete schema validation report."""

    contract_id: str
    zone: str
    execution_id: str
    total_rules: int
    passed_rules: int
    failed_rules: int
    is_valid: bool
    results: List[ValidationResult] = field(default_factory=list)
    blocking_failures: List[ValidationResult] = field(default_factory=list)


class SchemaValidator:
    """
    Schema Validator for Bronze zone.

    Validates:
    - Column presence (required columns exist)
    - Data type conformance
    - Not null constraints
    - Primary key uniqueness
    - Value range constraints
    """

    def __init__(self):
        """Initialize schema validator."""
        self.metadata_client = MetadataClient()

    def validate(
        self,
        contract_id: str,
        zone: str,
        execution_id: str,
        spark_session=None,
    ) -> SchemaValidationReport:
        """
        Validate data against schema definition.

        Args:
            contract_id: Data contract ID
            zone: Data zone (typically BRONZE)
            execution_id: Pipeline execution ID
            spark_session: Optional Spark session for validation

        Returns:
            SchemaValidationReport with validation results
        """
        # Get validation rules from metadata
        rules = self.metadata_client.get_validation_rules(contract_id, zone)

        # Filter to schema validation rules only
        schema_rules = [r for r in rules if r["validation_type"] == "SCHEMA"]

        # Execute validations
        results = []
        for rule in schema_rules:
            result = self._execute_rule(rule, contract_id, execution_id, spark_session)
            results.append(result)

        # Determine overall validity
        blocking_failures = [r for r in results if not r.is_passed and r.is_blocking]
        is_valid = len(blocking_failures) == 0

        return SchemaValidationReport(
            contract_id=contract_id,
            zone=zone,
            execution_id=execution_id,
            total_rules=len(results),
            passed_rules=len([r for r in results if r.is_passed]),
            failed_rules=len([r for r in results if not r.is_passed]),
            is_valid=is_valid,
            results=results,
            blocking_failures=blocking_failures,
        )

    def _execute_rule(
        self,
        rule: Dict[str, Any],
        contract_id: str,
        execution_id: str,
        spark_session=None,
    ) -> ValidationResult:
        """Execute a single validation rule."""
        # Get contract to find table name
        contract = self.metadata_client.get_contract(contract_id)

        # Default result
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
            # Return placeholder result if no Spark session
            return result

        try:
            # Build validation SQL
            table_name = contract.bronze_table
            expression = rule["rule_expression"]

            # Count total records
            total_df = spark_session.sql(f"SELECT COUNT(*) as cnt FROM {table_name}")
            result.total_records = total_df.collect()[0]["cnt"]

            if result.total_records == 0:
                return result

            # Count passing records
            passed_df = spark_session.sql(f"""
                SELECT COUNT(*) as cnt FROM {table_name}
                WHERE {expression}
            """)
            result.passed_records = passed_df.collect()[0]["cnt"]
            result.failed_records = result.total_records - result.passed_records

            # Calculate pass percentage
            result.pass_percentage = (
                result.passed_records / result.total_records * 100
            )

            # Determine if passed based on threshold
            result.is_passed = (
                result.pass_percentage >= (100 - result.threshold_percentage)
            )

            # Get sample failures if not passed
            if not result.is_passed:
                sample_df = spark_session.sql(f"""
                    SELECT * FROM {table_name}
                    WHERE NOT ({expression})
                    LIMIT 10
                """)
                result.sample_failures = [
                    row.asDict() for row in sample_df.collect()
                ]

        except Exception as e:
            result.is_passed = False
            result.error_message = str(e)

        return result

    def validate_schema_compatibility(
        self,
        existing_schema: Dict[str, Any],
        new_schema: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Check if schema change is backward compatible.

        Returns compatibility assessment.
        """
        existing_columns = {c["column"]: c for c in existing_schema.get("schema_json", [])}
        new_columns = {c["column"]: c for c in new_schema.get("schema_json", [])}

        # Check for breaking changes
        breaking_changes = []
        additive_changes = []

        # Removed columns
        for col_name in existing_columns:
            if col_name not in new_columns:
                breaking_changes.append({
                    "type": "COLUMN_REMOVED",
                    "column": col_name,
                })

        # Type changes
        for col_name, new_col in new_columns.items():
            if col_name in existing_columns:
                old_col = existing_columns[col_name]
                if old_col["type"] != new_col["type"]:
                    breaking_changes.append({
                        "type": "TYPE_CHANGED",
                        "column": col_name,
                        "old_type": old_col["type"],
                        "new_type": new_col["type"],
                    })
                # Nullable changed from true to false
                if old_col.get("nullable", True) and not new_col.get("nullable", True):
                    breaking_changes.append({
                        "type": "NULLABLE_CHANGED",
                        "column": col_name,
                    })
            else:
                additive_changes.append({
                    "type": "COLUMN_ADDED",
                    "column": col_name,
                })

        return {
            "is_compatible": len(breaking_changes) == 0,
            "breaking_changes": breaking_changes,
            "additive_changes": additive_changes,
        }
