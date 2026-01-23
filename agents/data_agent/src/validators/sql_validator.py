"""
SQL Syntax and Security Validation.

WHY: Validates generated SQL statements before execution.
     Catches syntax errors, injection risks, and structural issues.

VALIDATION STEPS:
1. Basic syntax parsing with sqlparse
2. Security checks for injection patterns
3. Parameterized query validation
4. Transaction boundary validation
"""

import re
from typing import Any, Dict, List, Tuple

import structlog

logger = structlog.get_logger()


class SQLValidator:
    """
    Validates generated SQL statements.

    RULES:
    1. Check SQL syntax with sqlparse
    2. Validate against security patterns
    3. Check for SQL injection risks
    4. Validate transaction boundaries
    5. Ensure parameterization where needed
    """

    # Dangerous patterns that should never appear in generated SQL
    DANGEROUS_PATTERNS = [
        r"--\s*$",  # SQL comments at end (potential injection)
        r";\s*DROP\s+",  # DROP after semicolon
        r";\s*DELETE\s+",  # DELETE after semicolon
        r";\s*TRUNCATE\s+",  # TRUNCATE after semicolon
        r"EXECUTE\s+IMMEDIATE",  # Dynamic SQL execution
        r"EXEC\s*\(",  # Stored proc exec
        r"xp_cmdshell",  # Command shell access
        r"WAITFOR\s+DELAY",  # Time-based injection
        r"BENCHMARK\s*\(",  # MySQL benchmark attack
        r"SLEEP\s*\(",  # Sleep-based injection
    ]

    # Patterns that suggest string concatenation (potential injection)
    CONCAT_PATTERNS = [
        r"\+\s*['\"]",  # + 'string'
        r"['\"]\s*\+",  # 'string' +
        r"\|\|\s*['\"]",  # || 'string'
        r"['\"]\s*\|\|",  # 'string' ||
        r"CONCAT\s*\(",  # CONCAT function
    ]

    # Required patterns for metadata SQL
    REQUIRED_METADATA_PATTERNS = {
        "INSERT": r"INSERT\s+INTO\s+\w+",
        "UPDATE": r"UPDATE\s+\w+\s+SET",
        "SELECT": r"SELECT\s+.+\s+FROM",
    }

    def validate_statements(self, statements: List[str]) -> Dict[str, Any]:
        """
        Validate a list of SQL statements.

        Args:
            statements: List of SQL statements to validate

        Returns:
            Dict with 'success', 'errors', 'warnings'
        """
        errors = []
        warnings = []

        for i, sql in enumerate(statements):
            if not sql or not sql.strip():
                continue

            # Validate syntax
            syntax_result = self.validate_syntax(sql)
            if not syntax_result[0]:
                for error in syntax_result[1]:
                    errors.append(f"Statement {i+1}: {error}")

            # Validate security
            security_result = self.validate_security(sql)
            if not security_result[0]:
                for error in security_result[1]:
                    errors.append(f"Statement {i+1}: {error}")

            # Check for parameterization warnings
            param_result = self.validate_parameterized_queries(sql)
            if not param_result[0]:
                for warning in param_result[1]:
                    warnings.append(f"Statement {i+1}: {warning}")

        return {
            "success": len(errors) == 0,
            "errors": errors,
            "warnings": warnings,
        }

    def validate_syntax(self, sql: str) -> Tuple[bool, List[str]]:
        """
        Validate SQL syntax.

        Uses sqlparse for parsing if available, falls back to
        basic structural validation.

        Args:
            sql: SQL statement to validate

        Returns:
            Tuple of (is_valid, list of error messages)
        """
        errors = []

        if not sql or not sql.strip():
            return True, []

        try:
            # Try to use sqlparse for proper parsing
            import sqlparse
            parsed = sqlparse.parse(sql)

            if not parsed:
                errors.append("Failed to parse SQL statement")
                return False, errors

            # Check for basic validity
            for statement in parsed:
                if statement.get_type() == "UNKNOWN":
                    # Unknown type might still be valid, just warning
                    logger.warning("SQL statement type unknown", sql=sql[:100])

            return True, errors

        except ImportError:
            # Fallback to basic validation without sqlparse
            return self._basic_syntax_check(sql)
        except Exception as e:
            errors.append(f"SQL parse error: {str(e)}")
            return False, errors

    def _basic_syntax_check(self, sql: str) -> Tuple[bool, List[str]]:
        """
        Basic SQL syntax validation without sqlparse.

        Checks for:
        - Balanced parentheses
        - Balanced quotes
        - Basic statement structure
        """
        errors = []

        # Check balanced parentheses
        paren_count = 0
        for char in sql:
            if char == "(":
                paren_count += 1
            elif char == ")":
                paren_count -= 1
            if paren_count < 0:
                errors.append("Unbalanced parentheses: extra closing paren")
                break

        if paren_count > 0:
            errors.append("Unbalanced parentheses: missing closing paren")

        # Check balanced single quotes (ignoring escaped)
        in_string = False
        prev_char = ""
        for char in sql:
            if char == "'" and prev_char != "\\":
                in_string = not in_string
            prev_char = char

        if in_string:
            errors.append("Unbalanced quotes: string not closed")

        # Check for basic SQL keywords
        # Strip comment lines at the beginning before checking keywords
        sql_lines = sql.strip().split('\n')
        sql_without_comments = '\n'.join(
            line for line in sql_lines
            if not line.strip().startswith('--')
        ).strip()
        sql_upper = sql_without_comments.upper().strip()

        valid_starts = [
            "SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "ALTER",
            "DROP", "TRUNCATE", "WITH", "MERGE", "UPSERT", "BEGIN",
            "COMMIT", "ROLLBACK", "SET", "GRANT", "REVOKE", "COMMENT",
        ]

        has_valid_start = any(sql_upper.startswith(kw) for kw in valid_starts)
        if not has_valid_start:
            errors.append(f"SQL does not start with valid keyword")

        return len(errors) == 0, errors

    def validate_security(self, sql: str) -> Tuple[bool, List[str]]:
        """
        Validate SQL for security issues.

        Checks for:
        - SQL injection patterns
        - Dangerous commands
        - Suspicious concatenation

        Args:
            sql: SQL statement to validate

        Returns:
            Tuple of (is_valid, list of error messages)
        """
        errors = []

        if not sql:
            return True, []

        sql_upper = sql.upper()

        # Check for dangerous patterns
        for pattern in self.DANGEROUS_PATTERNS:
            if re.search(pattern, sql_upper, re.IGNORECASE):
                errors.append(f"Dangerous SQL pattern detected: {pattern}")

        # Check for string concatenation (potential injection)
        for pattern in self.CONCAT_PATTERNS:
            if re.search(pattern, sql):
                # This is a warning, not error - might be legitimate
                logger.warning(
                    "String concatenation in SQL",
                    pattern=pattern,
                    sql=sql[:100],
                )

        return len(errors) == 0, errors

    def validate_parameterized_queries(self, sql: str) -> Tuple[bool, List[str]]:
        """
        Validate that queries use parameterization where needed.

        For metadata SQL (generated code), we use template values
        rather than runtime parameters, so this checks for
        suspicious patterns that might indicate injection risk.

        Args:
            sql: SQL statement to validate

        Returns:
            Tuple of (is_valid, list of warnings)
        """
        warnings = []

        if not sql:
            return True, []

        # Check for literal values that look like user input
        # These patterns suggest hard-coded values that should be parameters
        suspicious_patterns = [
            (r"WHERE\s+\w+\s*=\s*'[^']{50,}'", "Long string literal in WHERE clause"),
            (r"VALUES\s*\([^)]*'[^']{100,}'", "Very long string in VALUES"),
            (r"--\s*\w+", "SQL comment that might hide injection"),
        ]

        for pattern, message in suspicious_patterns:
            if re.search(pattern, sql, re.IGNORECASE):
                warnings.append(message)

        return True, warnings  # Warnings don't fail validation

    def validate_transaction_boundaries(
        self, sql_statements: List[str]
    ) -> Tuple[bool, List[str]]:
        """
        Validate transaction BEGIN/COMMIT structure.

        Checks that:
        - BEGIN has matching COMMIT or ROLLBACK
        - No nested transactions
        - Proper ordering

        Args:
            sql_statements: List of SQL statements to validate

        Returns:
            Tuple of (is_valid, list of error messages)
        """
        errors = []

        if not sql_statements:
            return True, []

        in_transaction = False

        for i, sql in enumerate(sql_statements):
            if not sql:
                continue

            sql_upper = sql.upper().strip()

            if sql_upper.startswith("BEGIN"):
                if in_transaction:
                    errors.append(f"Statement {i+1}: Nested BEGIN not allowed")
                in_transaction = True

            elif sql_upper.startswith("COMMIT") or sql_upper.startswith("ROLLBACK"):
                if not in_transaction:
                    errors.append(
                        f"Statement {i+1}: COMMIT/ROLLBACK without BEGIN"
                    )
                in_transaction = False

        if in_transaction:
            errors.append("Transaction not closed: missing COMMIT or ROLLBACK")

        return len(errors) == 0, errors

    def validate_metadata_sql(
        self,
        sql: str,
        expected_type: str = "INSERT",
    ) -> Dict[str, Any]:
        """
        Validate metadata SQL for pipeline registration.

        Specific validation for the metadata SQL generated
        by the metadata_generator.

        Args:
            sql: SQL statement to validate
            expected_type: Expected statement type (INSERT, UPDATE, etc.)

        Returns:
            Dict with 'success', 'errors', 'warnings'
        """
        errors = []
        warnings = []

        if not sql or not sql.strip():
            errors.append("Empty SQL statement")
            return {"success": False, "errors": errors, "warnings": warnings}

        # Check expected type
        pattern = self.REQUIRED_METADATA_PATTERNS.get(expected_type.upper())
        if pattern and not re.search(pattern, sql, re.IGNORECASE):
            errors.append(f"SQL does not match expected {expected_type} pattern")

        # Validate syntax
        syntax_valid, syntax_errors = self.validate_syntax(sql)
        errors.extend(syntax_errors)

        # Validate security
        security_valid, security_errors = self.validate_security(sql)
        errors.extend(security_errors)

        # Check for required metadata table names
        metadata_tables = [
            "pipeline", "schema_version", "transformation_rule",
            "data_quality_rule", "execution_log", "audit_log",
        ]

        has_metadata_table = any(
            table in sql.lower() for table in metadata_tables
        )
        if not has_metadata_table:
            warnings.append(
                "SQL does not reference standard metadata tables"
            )

        return {
            "success": len(errors) == 0,
            "errors": errors,
            "warnings": warnings,
        }
