"""
Validator Agent - Artifact Validation

WHY: Validates all generated artifacts before deployment.
     Catches errors early to prevent failed deployments.

HOW:
1. Validate DAG Python syntax and import test
2. Validate Spark job Python syntax
3. Validate SQL syntax
4. Check schema compatibility
5. Perform security scan for secrets/PII

VALIDATION RULES:
- Python code must be syntactically valid
- DAG must be importable by Airflow
- SQL must be syntactically valid
- Schema changes must not break existing data
- No secrets or credentials in generated code
"""

import ast
from typing import Any, Dict, List

from src.agents.base_agent import BaseAgent, requires_state_keys
from src.utils.exceptions import ValidationError


class ValidatorAgent(BaseAgent):
    """
    Validator Agent for artifact validation.

    RESPONSIBILITIES:
    1. Validate DAG syntax and importability
    2. Validate Spark job syntax
    3. Validate SQL syntax
    4. Check schema compatibility
    5. Scan for security issues

    RULES:
    - NEVER skip validation
    - ALWAYS fail fast on errors
    - Report ALL errors, not just first
    """

    def __init__(self) -> None:
        super().__init__("validator")
        self._dag_validator = None
        self._sql_validator = None
        self._schema_validator = None
        self._security_validator = None

    @property
    def dag_validator(self):
        """Lazy-load DAG validator."""
        if self._dag_validator is None:
            from src.validators.dag_validator import DAGValidator
            self._dag_validator = DAGValidator()
        return self._dag_validator

    @property
    def sql_validator(self):
        """Lazy-load SQL validator."""
        if self._sql_validator is None:
            from src.validators.sql_validator import SQLValidator
            self._sql_validator = SQLValidator()
        return self._sql_validator

    @property
    def schema_validator(self):
        """Lazy-load schema validator."""
        if self._schema_validator is None:
            from src.validators.schema_validator import SchemaValidator
            self._schema_validator = SchemaValidator()
        return self._schema_validator

    @property
    def security_validator(self):
        """Lazy-load security validator."""
        if self._security_validator is None:
            from src.validators.security_validator import SecurityValidator
            self._security_validator = SecurityValidator()
        return self._security_validator

    @requires_state_keys("intent_json", "request_id", "generator_output")
    def execute(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute validation of all generated artifacts.

        Steps:
        1. Validate DAG Python syntax and import
        2. Validate Spark job syntax
        3. Validate SQL syntax
        4. Check schema compatibility
        5. Perform security scan
        6. Return ValidatorOutput with results

        Args:
            state: Current workflow state with generator_output

        Returns:
            Partial state update with validator_output
        """
        self.logger.info(
            "Starting artifact validation",
            request_id=state["request_id"],
        )

        errors: List[str] = []
        warnings: List[str] = []

        try:
            generator_output = state["generator_output"]
            intent = state["intent_json"]
            planner_output = state.get("planner_output", {})

            # Check if generation was skipped
            if generator_output.get("skipped"):
                self.logger.info("Validation skipped - no artifacts generated")
                return {
                    "current_phase": "validating",
                    "validator_output": {
                        "is_valid": True,
                        "dag_import_success": True,
                        "sql_syntax_valid": True,
                        "schema_compatible": True,
                        "security_passed": True,
                        "validation_skipped": True,
                        "skip_reason": "No artifacts to validate",
                        "errors": [],
                        "warnings": [],
                    },
                }

            # Step 1: Validate DAG
            dag_code = generator_output.get("dag_code", "")
            dag_result = self._validate_dag(dag_code)
            errors.extend(dag_result.get("errors", []))
            warnings.extend(dag_result.get("warnings", []))
            dag_import_success = dag_result.get("success", False)

            # Step 2: Validate Spark jobs
            spark_jobs = generator_output.get("spark_jobs", {})
            spark_results = self._validate_spark_jobs(spark_jobs)
            errors.extend(spark_results.get("errors", []))
            warnings.extend(spark_results.get("warnings", []))
            spark_syntax_valid = spark_results.get("success", False)

            # Step 3: Validate SQL
            metadata_sql = generator_output.get("metadata_sql", [])
            sql_result = self._validate_sql(metadata_sql)
            errors.extend(sql_result.get("errors", []))
            warnings.extend(sql_result.get("warnings", []))
            sql_syntax_valid = sql_result.get("success", False)

            # Step 4: Check schema compatibility
            schema_plan = planner_output.get("schema_plan", {})
            schema_result = self._validate_schema_compatibility(
                intent.get("schema_definition", {}),
                schema_plan,
            )
            errors.extend(schema_result.get("errors", []))
            warnings.extend(schema_result.get("warnings", []))
            schema_compatible = schema_result.get("success", False)

            # Step 5: Security scan
            all_code = self._collect_all_code(generator_output)
            security_result = self._validate_security(all_code, intent)
            errors.extend(security_result.get("errors", []))
            warnings.extend(security_result.get("warnings", []))
            security_passed = security_result.get("success", False)

            # Determine overall validity
            is_valid = (
                dag_import_success and
                spark_syntax_valid and
                sql_syntax_valid and
                schema_compatible and
                security_passed
            )

            # Build validator output
            validator_output = {
                "is_valid": is_valid,
                "dag_import_success": dag_import_success,
                "sql_syntax_valid": sql_syntax_valid,
                "schema_compatible": schema_compatible,
                "security_passed": security_passed,
                "errors": errors,
                "warnings": warnings,
            }

            self.logger.info(
                "Validation complete",
                request_id=state["request_id"],
                is_valid=is_valid,
                error_count=len(errors),
                warning_count=len(warnings),
            )

            # Log decision
            if is_valid:
                self.log_decision(
                    decision="Artifacts validated successfully",
                    reasoning="All validation checks passed",
                    context={"warning_count": len(warnings)},
                )
            else:
                self.log_decision(
                    decision="Validation failed",
                    reasoning=f"{len(errors)} validation errors found",
                    context={"errors": errors[:5]},
                )

            result = {
                "current_phase": "validating",
                "validator_output": validator_output,
            }

            # If validation failed, add error state
            if not is_valid:
                result["error_message"] = f"Validation failed: {len(errors)} errors"
                result["error_agent"] = self.agent_name

            return result

        except Exception as e:
            self.logger.error("Unexpected validation error", error=str(e))
            return self.create_error_state(f"Validation failed: {str(e)}", e)

    def _validate_dag(self, dag_code: str) -> Dict[str, Any]:
        """
        Validate DAG Python syntax and importability.

        Checks:
        1. Python syntax is valid (ast.parse)
        2. DAG object is created
        3. Required imports present
        """
        if not dag_code:
            return {"success": True, "errors": [], "warnings": []}

        return self.dag_validator.validate(dag_code)

    def _validate_spark_jobs(self, spark_jobs: Dict[str, str]) -> Dict[str, Any]:
        """
        Validate all Spark job Python syntax.

        Checks each job for valid Python syntax.
        """
        all_errors = []
        all_warnings = []
        all_success = True

        for job_name, job_code in spark_jobs.items():
            if not job_code:
                continue

            try:
                # Basic Python syntax check
                ast.parse(job_code)

                # Check for required Spark patterns
                if "SparkSession" not in job_code:
                    all_warnings.append(f"{job_name}: Missing SparkSession builder")

            except SyntaxError as e:
                all_errors.append(f"{job_name}: Syntax error at line {e.lineno}: {e.msg}")
                all_success = False

        return {
            "success": all_success,
            "errors": all_errors,
            "warnings": all_warnings,
        }

    def _validate_sql(self, sql_statements: List[str]) -> Dict[str, Any]:
        """
        Validate SQL syntax.

        Uses sqlparse for basic syntax validation.
        """
        if not sql_statements:
            return {"success": True, "errors": [], "warnings": []}

        return self.sql_validator.validate_statements(sql_statements)

    def _validate_schema_compatibility(
        self,
        schema_definition: Dict[str, Any],
        schema_plan: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Validate schema changes are compatible.

        Checks for breaking changes:
        - Removed required columns
        - Type changes that lose data
        - Constraint changes that invalidate existing data
        """
        errors = []
        warnings = []

        breaking_changes = schema_plan.get("breaking_changes", [])

        for change in breaking_changes:
            change_type = change.get("change_type")
            column_name = change.get("column_name")

            if change_type == "removed":
                errors.append(
                    f"Breaking change: Column '{column_name}' will be removed. "
                    "This may cause downstream failures."
                )
            elif change_type == "type_changed":
                old_type = change.get("old_value")
                new_type = change.get("new_value")
                warnings.append(
                    f"Type change: Column '{column_name}' changed from "
                    f"'{old_type}' to '{new_type}'. Verify data compatibility."
                )

        return {
            "success": len(errors) == 0,
            "errors": errors,
            "warnings": warnings,
        }

    def _validate_security(
        self,
        all_code: str,
        intent: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Perform security validation.

        Checks:
        1. No secrets/credentials in code
        2. PII columns properly flagged
        3. No dangerous patterns
        """
        return self.security_validator.validate(all_code, intent)

    def _collect_all_code(self, generator_output: Dict[str, Any]) -> str:
        """Collect all generated code into single string for scanning."""
        code_parts = []

        # Add DAG code
        if generator_output.get("dag_code"):
            code_parts.append(generator_output["dag_code"])

        # Add Spark jobs
        for job_code in generator_output.get("spark_jobs", {}).values():
            if job_code:
                code_parts.append(job_code)

        # Add SQL
        for sql in generator_output.get("metadata_sql", []):
            if sql:
                code_parts.append(sql)

        return "\n\n".join(code_parts)
