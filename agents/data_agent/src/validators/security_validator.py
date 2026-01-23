"""
Security Validation for Generated Artifacts.

WHY: Scans generated code for security issues before deployment.
     Prevents secrets leakage, validates PII handling, catches dangerous patterns.

VALIDATION STEPS:
1. Scan for hardcoded secrets (API keys, passwords, tokens)
2. Validate PII columns are properly flagged
3. Check for dangerous code patterns
4. Validate data sensitivity classification
"""

import re
from typing import Any, Dict, List, Tuple

import structlog

logger = structlog.get_logger()


class SecurityValidator:
    """
    Validates security rules for generated artifacts.

    RULES:
    1. NO hardcoded secrets in generated code
    2. PII columns must be flagged in schema
    3. Dangerous patterns must be blocked
    4. Data sensitivity must be properly classified
    """

    # Patterns that indicate hardcoded secrets
    SECRET_PATTERNS = [
        # API Keys
        (r"api[_-]?key\s*[=:]\s*['\"][a-zA-Z0-9_-]{20,}['\"]", "API key"),
        (r"apikey\s*[=:]\s*['\"][a-zA-Z0-9_-]{20,}['\"]", "API key"),

        # AWS patterns
        (r"AKIA[0-9A-Z]{16}", "AWS Access Key ID"),
        (r"aws[_-]?secret[_-]?access[_-]?key\s*[=:]\s*['\"][A-Za-z0-9/+=]{40}['\"]", "AWS Secret Key"),

        # Google Cloud patterns
        (r"AIza[0-9A-Za-z_-]{35}", "Google API Key"),
        (r'"type"\s*:\s*"service_account"', "GCP Service Account JSON"),

        # Generic secrets
        (r"password\s*[=:]\s*['\"][^'\"]{8,}['\"]", "Password"),
        (r"secret\s*[=:]\s*['\"][^'\"]{8,}['\"]", "Secret"),
        (r"token\s*[=:]\s*['\"][a-zA-Z0-9_-]{20,}['\"]", "Token"),
        (r"bearer\s+[a-zA-Z0-9_-]{20,}", "Bearer token"),

        # Connection strings
        (r"mongodb(\+srv)?://[^:]+:[^@]+@", "MongoDB connection with password"),
        (r"postgres(ql)?://[^:]+:[^@]+@", "PostgreSQL connection with password"),
        (r"mysql://[^:]+:[^@]+@", "MySQL connection with password"),
        (r"redis://:[^@]+@", "Redis connection with password"),

        # Private keys
        (r"-----BEGIN (RSA |EC |DSA |OPENSSH )?PRIVATE KEY-----", "Private key"),
        (r"-----BEGIN PGP PRIVATE KEY BLOCK-----", "PGP private key"),

        # JWT tokens
        (r"eyJ[a-zA-Z0-9_-]*\.eyJ[a-zA-Z0-9_-]*\.[a-zA-Z0-9_-]*", "JWT token"),

        # Slack tokens
        (r"xox[baprs]-[0-9]{10,12}-[0-9]{10,12}-[a-zA-Z0-9]{24}", "Slack token"),

        # GitHub tokens
        (r"gh[pousr]_[A-Za-z0-9_]{36,}", "GitHub token"),
        (r"github_pat_[a-zA-Z0-9]{22}_[a-zA-Z0-9]{59}", "GitHub PAT"),
    ]

    # PII column patterns
    PII_PATTERNS = [
        (r"ssn|social_security", "SSN"),
        (r"credit_card|cc_number|card_number", "Credit Card"),
        (r"email|e_mail", "Email"),
        (r"phone|mobile|cell", "Phone"),
        (r"address|street|city|zip|postal", "Address"),
        (r"birth_date|dob|date_of_birth", "Date of Birth"),
        (r"passport", "Passport"),
        (r"driver_license|drivers_license", "Driver's License"),
        (r"salary|wage|income|compensation", "Financial"),
        (r"medical|diagnosis|prescription|health", "Medical"),
        (r"first_name|last_name|full_name|name", "Name"),
        (r"ip_address|user_agent", "Technical PII"),
    ]

    # Dangerous code patterns
    DANGEROUS_PATTERNS = [
        (r"eval\s*\(", "eval() function - code injection risk"),
        (r"exec\s*\(", "exec() function - code injection risk"),
        (r"subprocess\.call\s*\([^)]*shell\s*=\s*True", "Shell injection risk"),
        (r"os\.system\s*\(", "os.system - command injection risk"),
        (r"pickle\.loads?\s*\(", "Pickle - deserialization risk"),
        (r"yaml\.load\s*\([^)]*Loader\s*=\s*None", "Unsafe YAML loading"),
        (r"__import__\s*\(", "Dynamic import - code injection risk"),
        (r"compile\s*\([^)]*exec", "Dynamic code compilation"),
        (r"open\s*\([^)]*,\s*['\"]w", "File write - verify path sanitization"),
    ]

    def validate(
        self,
        code: str,
        intent: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Perform full security validation.

        Args:
            code: All generated code concatenated
            intent: Pipeline intent for PII validation

        Returns:
            Dict with 'success', 'errors', 'warnings'
        """
        errors = []
        warnings = []

        # Step 1: Scan for hardcoded secrets
        secret_result = self.scan_for_secrets(code)
        errors.extend(secret_result.get("errors", []))
        warnings.extend(secret_result.get("warnings", []))

        # Step 2: Validate PII handling
        schema_def = intent.get("schema_definition", {})
        pii_result = self.validate_pii_handling(code, schema_def)
        errors.extend(pii_result.get("errors", []))
        warnings.extend(pii_result.get("warnings", []))

        # Step 3: Check for dangerous patterns
        pattern_result = self.check_dangerous_patterns(code)
        errors.extend(pattern_result.get("errors", []))
        warnings.extend(pattern_result.get("warnings", []))

        # Step 4: Validate data sensitivity
        sensitivity = intent.get("pipeline_identity", {}).get(
            "data_sensitivity", "internal"
        )
        sensitivity_result = self.validate_data_sensitivity(schema_def, sensitivity)
        errors.extend(sensitivity_result.get("errors", []))
        warnings.extend(sensitivity_result.get("warnings", []))

        is_valid = len(errors) == 0

        logger.info(
            "Security validation complete",
            valid=is_valid,
            error_count=len(errors),
            warning_count=len(warnings),
        )

        return {
            "success": is_valid,
            "errors": errors,
            "warnings": warnings,
        }

    def scan_for_secrets(self, code: str) -> Dict[str, Any]:
        """
        Scan code for hardcoded secrets.

        Args:
            code: Code to scan

        Returns:
            Dict with 'errors', 'warnings'
        """
        errors = []
        warnings = []

        if not code:
            return {"errors": errors, "warnings": warnings}

        for pattern, secret_type in self.SECRET_PATTERNS:
            matches = re.findall(pattern, code, re.IGNORECASE)
            if matches:
                # Mask the actual secret in the error message
                errors.append(
                    f"Hardcoded {secret_type} detected. "
                    "Use environment variables or secret manager instead."
                )

        # Check for common secret variable names without values
        # These are warnings as they might be legitimate references
        secret_vars = re.findall(
            r"(password|secret|api_key|token|credential)\s*=\s*\w+",
            code,
            re.IGNORECASE,
        )
        for match in secret_vars:
            # Check if it's referencing an env var or config
            if not re.search(r"os\.environ|config\.|settings\.|getenv", code):
                warnings.append(
                    f"Variable '{match}' may contain secrets. "
                    "Ensure it uses proper secret management."
                )

        return {"errors": errors, "warnings": warnings}

    def validate_pii_handling(
        self,
        code: str,
        schema_def: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Validate PII columns are properly flagged.

        Checks that columns matching PII patterns are:
        1. Flagged as PII in schema definition
        2. Have appropriate handling (masking, encryption)

        Args:
            code: Generated code
            schema_def: Schema definition with columns

        Returns:
            Dict with 'errors', 'warnings'
        """
        errors = []
        warnings = []

        columns = schema_def.get("columns", [])
        pii_flagged_columns = {
            col.get("name"): col
            for col in columns
            if col.get("is_pii") or col.get("pii") or col.get("sensitive")
        }

        # Check all columns for PII patterns
        for col in columns:
            col_name = col.get("name", "").lower()

            for pattern, pii_type in self.PII_PATTERNS:
                if re.search(pattern, col_name, re.IGNORECASE):
                    if col.get("name") not in pii_flagged_columns:
                        warnings.append(
                            f"Column '{col.get('name')}' appears to contain "
                            f"{pii_type} data but is not flagged as PII. "
                            "Consider adding is_pii: true to schema."
                        )
                    break

        # Check that PII columns have proper handling defined
        for col_name, col_def in pii_flagged_columns.items():
            handling = col_def.get("pii_handling") or col_def.get("handling")
            if not handling:
                warnings.append(
                    f"PII column '{col_name}' has no handling strategy defined. "
                    "Consider adding masking, hashing, or encryption."
                )

        # Check code for unmasked PII column references
        for col_name in pii_flagged_columns.keys():
            # Check if column is referenced without masking
            if col_name in code:
                # Check for masking/hashing functions
                has_protection = any(
                    func in code.lower()
                    for func in ["mask", "hash", "encrypt", "sha256", "md5", "anonymize"]
                )
                if not has_protection:
                    warnings.append(
                        f"PII column '{col_name}' is referenced in code. "
                        "Verify proper masking/encryption is applied."
                    )

        return {"errors": errors, "warnings": warnings}

    def check_dangerous_patterns(self, code: str) -> Dict[str, Any]:
        """
        Check code for dangerous patterns.

        Args:
            code: Code to check

        Returns:
            Dict with 'errors', 'warnings'
        """
        errors = []
        warnings = []

        if not code:
            return {"errors": errors, "warnings": warnings}

        for pattern, description in self.DANGEROUS_PATTERNS:
            if re.search(pattern, code, re.IGNORECASE):
                # Some patterns are errors, some are warnings
                if "injection" in description.lower():
                    errors.append(f"Dangerous pattern: {description}")
                else:
                    warnings.append(f"Potential risk: {description}")

        return {"errors": errors, "warnings": warnings}

    def validate_data_sensitivity(
        self,
        schema_def: Dict[str, Any],
        sensitivity_level: str,
    ) -> Dict[str, Any]:
        """
        Validate data sensitivity classification.

        Sensitivity levels:
        - public: No restrictions
        - internal: Standard access controls
        - confidential: Encryption required, audit logging
        - restricted: Encryption + approval + audit + retention limits

        Args:
            schema_def: Schema definition
            sensitivity_level: Required sensitivity level

        Returns:
            Dict with 'errors', 'warnings'
        """
        errors = []
        warnings = []

        columns = schema_def.get("columns", [])
        pii_columns = [
            col for col in columns
            if col.get("is_pii") or col.get("pii") or col.get("sensitive")
        ]

        # Check sensitivity level requirements
        if sensitivity_level in ["confidential", "restricted"]:
            # Must have encryption for PII
            for col in pii_columns:
                if not col.get("encrypted") and not col.get("pii_handling"):
                    errors.append(
                        f"Column '{col.get('name')}' is PII in {sensitivity_level} "
                        "pipeline but has no encryption or handling defined."
                    )

        if sensitivity_level == "restricted":
            # Additional checks for restricted data
            if not schema_def.get("retention_policy"):
                warnings.append(
                    "Restricted data should have explicit retention policy."
                )
            if not schema_def.get("access_control"):
                warnings.append(
                    "Restricted data should have explicit access control rules."
                )

        # Check for sensitivity mismatches
        if sensitivity_level in ["public", "internal"] and len(pii_columns) > 0:
            warnings.append(
                f"Pipeline marked as '{sensitivity_level}' but contains "
                f"{len(pii_columns)} PII columns. Consider higher sensitivity."
            )

        return {"errors": errors, "warnings": warnings}

    def validate_no_hardcoded_secrets(self, code: str) -> Tuple[bool, List[str]]:
        """
        Check code for hardcoded secrets.

        Convenience method that returns tuple format.

        Args:
            code: Code to check

        Returns:
            Tuple of (is_valid, list of findings)
        """
        result = self.scan_for_secrets(code)
        errors = result.get("errors", [])
        return len(errors) == 0, errors

    def validate_access_patterns(
        self,
        config: Dict[str, Any],
    ) -> Tuple[bool, List[str]]:
        """
        Validate access control patterns.

        Checks that proper access controls are in place
        based on data sensitivity.

        Args:
            config: Pipeline configuration

        Returns:
            Tuple of (is_valid, list of findings)
        """
        errors = []

        # Check IAM roles are specified
        if not config.get("service_account") and not config.get("iam_role"):
            errors.append("No service account or IAM role specified")

        # Check data access is properly scoped
        target_config = config.get("target_config", {})
        if target_config.get("bigquery_config"):
            bq_config = target_config["bigquery_config"]
            if not bq_config.get("dataset"):
                errors.append("BigQuery dataset not specified")

        return len(errors) == 0, errors
