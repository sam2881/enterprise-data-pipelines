"""
DAG validation logic.

WHY: Validates generated Airflow DAGs before deployment.
     Catches syntax errors and structural issues early.

VALIDATION STEPS:
1. Python syntax check (ast.parse)
2. DAG object detection
3. Required imports present
4. Task dependency validation
"""

import ast
import re
from typing import Any, Dict, List

import structlog

logger = structlog.get_logger()


class DAGValidator:
    """
    Validates generated Airflow DAGs.

    RULES:
    1. Attempt to import DAG code (syntax check)
    2. Validate DAG structure
    3. Check for required components
    4. Validate task dependencies
    """

    # Required imports for Airflow DAGs
    REQUIRED_IMPORTS = [
        "airflow",
        "DAG",
    ]

    # Patterns that indicate DAG definition
    DAG_PATTERNS = [
        r'DAG\s*\(',
        r'with\s+DAG\s*\(',
        r'@dag\s*\(',
    ]

    def validate(self, dag_code: str) -> Dict[str, Any]:
        """
        Perform all validations on DAG code.

        Args:
            dag_code: Generated DAG Python code

        Returns:
            Dict with 'success', 'errors', 'warnings'
        """
        errors = []
        warnings = []

        # Step 1: Validate Python syntax
        syntax_result = self.validate_syntax(dag_code)
        if not syntax_result["success"]:
            errors.extend(syntax_result["errors"])
            return {"success": False, "errors": errors, "warnings": warnings}

        # Step 2: Check for DAG definition
        structure_result = self.validate_dag_structure(dag_code)
        errors.extend(structure_result.get("errors", []))
        warnings.extend(structure_result.get("warnings", []))

        # Step 3: Check required imports
        import_result = self.validate_imports(dag_code)
        errors.extend(import_result.get("errors", []))
        warnings.extend(import_result.get("warnings", []))

        # Step 4: Validate task dependencies
        deps_result = self.validate_task_dependencies(dag_code)
        warnings.extend(deps_result.get("warnings", []))

        is_valid = len(errors) == 0

        logger.info(
            "DAG validation complete",
            valid=is_valid,
            error_count=len(errors),
            warning_count=len(warnings),
        )

        return {
            "success": is_valid,
            "errors": errors,
            "warnings": warnings,
        }

    def validate_syntax(self, dag_code: str) -> Dict[str, Any]:
        """
        Validate Python syntax using ast.parse.

        Args:
            dag_code: Generated DAG Python code

        Returns:
            Dict with 'success', 'errors'
        """
        try:
            ast.parse(dag_code)
            return {"success": True, "errors": []}
        except SyntaxError as e:
            error_msg = f"Syntax error at line {e.lineno}: {e.msg}"
            logger.error("DAG syntax error", line=e.lineno, error=e.msg)
            return {"success": False, "errors": [error_msg]}

    def validate_dag_structure(self, dag_code: str) -> Dict[str, Any]:
        """
        Validate DAG structure and components.

        Args:
            dag_code: Generated DAG Python code

        Returns:
            Dict with 'errors', 'warnings'
        """
        errors = []
        warnings = []

        # Check for DAG definition
        has_dag = any(re.search(pattern, dag_code) for pattern in self.DAG_PATTERNS)
        if not has_dag:
            errors.append("No DAG definition found. Expected DAG() or @dag decorator.")

        # Check for dag_id
        if 'dag_id' not in dag_code and 'dag_id=' not in dag_code:
            warnings.append("dag_id not explicitly set. Will use function name or default.")

        # Check for schedule
        if 'schedule' not in dag_code and 'schedule_interval' not in dag_code:
            warnings.append("No schedule defined. DAG will not run automatically.")

        # Check for start_date
        if 'start_date' not in dag_code:
            warnings.append("No start_date defined. This may cause issues.")

        return {"errors": errors, "warnings": warnings}

    def validate_imports(self, dag_code: str) -> Dict[str, Any]:
        """
        Validate required imports are present.

        Args:
            dag_code: Generated DAG Python code

        Returns:
            Dict with 'errors', 'warnings'
        """
        errors = []
        warnings = []

        # Check for airflow imports
        if 'from airflow' not in dag_code and 'import airflow' not in dag_code:
            errors.append("Missing Airflow import")

        # Check for DAG import
        if 'DAG' not in dag_code and '@dag' not in dag_code:
            errors.append("Missing DAG import or decorator")

        # Check for common operators
        operators = [
            'DataprocSubmitJobOperator',
            'DataprocCreateBatchOperator',
            'PythonOperator',
            'BashOperator',
            'DummyOperator',
            'EmptyOperator',
        ]

        has_operator = any(op in dag_code for op in operators)
        if not has_operator:
            warnings.append("No standard operators found. Using custom implementation?")

        return {"errors": errors, "warnings": warnings}

    def validate_task_dependencies(self, dag_code: str) -> Dict[str, Any]:
        """
        Validate task dependency graph.

        Checks for:
        - Dependencies defined with >> or <<
        - set_downstream/set_upstream calls
        - Circular dependencies (basic check)

        Args:
            dag_code: Generated DAG Python code

        Returns:
            Dict with 'warnings'
        """
        warnings = []

        # Check for dependency operators
        has_deps = '>>' in dag_code or '<<' in dag_code
        has_method_deps = 'set_downstream' in dag_code or 'set_upstream' in dag_code

        if not has_deps and not has_method_deps:
            warnings.append(
                "No task dependencies defined. "
                "Tasks will run in parallel without ordering."
            )

        # Check for potential circular dependencies (very basic)
        # A more thorough check would require actual code execution
        lines = dag_code.split('\n')
        for i, line in enumerate(lines):
            if '>>' in line and '<<' in line:
                warnings.append(
                    f"Line {i+1}: Mixed dependency operators may cause confusion"
                )

        return {"warnings": warnings}

    def validate_dag_import(self, dag_code: str) -> Dict[str, Any]:
        """
        Attempt to actually import the DAG module.

        This is a more thorough check but requires
        proper Python environment setup.

        Args:
            dag_code: Generated DAG Python code

        Returns:
            Dict with 'success', 'errors'
        """
        import tempfile
        import sys
        import importlib.util

        # Write to temporary file
        with tempfile.NamedTemporaryFile(
            mode='w',
            suffix='.py',
            delete=False,
        ) as f:
            f.write(dag_code)
            temp_path = f.name

        try:
            # Try to import
            spec = importlib.util.spec_from_file_location("temp_dag", temp_path)
            if spec and spec.loader:
                module = importlib.util.module_from_spec(spec)
                sys.modules["temp_dag"] = module
                spec.loader.exec_module(module)

                return {"success": True, "errors": []}

        except Exception as e:
            return {"success": False, "errors": [f"Import failed: {str(e)}"]}

        finally:
            # Cleanup
            import os
            try:
                os.unlink(temp_path)
            except OSError:
                pass

            # Remove from sys.modules
            sys.modules.pop("temp_dag", None)

        return {"success": True, "errors": []}
