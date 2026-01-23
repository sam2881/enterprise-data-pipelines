"""
Schema Validation for Pipeline Configurations.

WHY: Validates pipeline intent JSON and schema definitions.
     Ensures all required fields are present and data types are valid.

VALIDATION STEPS:
1. Validate required fields in intent JSON
2. Validate column definitions in schema
3. Check data type compatibility
4. Compare schemas for breaking changes
"""

import re
from typing import Any, Dict, List, Tuple

import structlog

logger = structlog.get_logger()


class SchemaValidator:
    """
    Validates JSON schemas for pipeline configurations.

    RULES:
    1. Validate against predefined schema definitions
    2. Check for required fields
    3. Validate data types
    4. Report all errors (don't stop at first error)
    """

    # Required fields in pipeline intent
    REQUIRED_INTENT_FIELDS = {
        "pipeline_identity": ["pipeline_name", "domain", "environment"],
        "source_config": ["source_type"],
        "target_config": [],  # Optional but validated if present
        "schema_definition": ["columns"],
    }

    # Supported data types (maps to Spark/BigQuery types)
    SUPPORTED_DATA_TYPES = {
        # String types
        "string", "varchar", "char", "text",
        # Numeric types
        "int", "integer", "long", "bigint", "smallint", "tinyint",
        "float", "double", "decimal", "numeric",
        # Boolean
        "boolean", "bool",
        # Date/Time
        "date", "timestamp", "datetime", "time",
        # Binary
        "binary", "bytes",
        # Complex types
        "array", "map", "struct", "json",
    }

    # Valid source types
    VALID_SOURCE_TYPES = {"file", "database", "streaming", "api"}

    # Valid environments
    VALID_ENVIRONMENTS = {"dev", "staging", "prod", "test"}

    # Valid processing modes
    VALID_PROCESSING_MODES = {"batch", "micro_batch", "streaming"}

    def validate_intent_schema(
        self, intent: Dict[str, Any]
    ) -> Tuple[bool, List[str]]:
        """
        Validate intent JSON against schema.

        Args:
            intent: Intent JSON to validate

        Returns:
            Tuple of (is_valid, list of error messages)
        """
        errors = []

        if not intent:
            return False, ["Intent JSON is empty"]

        # Check required top-level sections
        for section, required_fields in self.REQUIRED_INTENT_FIELDS.items():
            if section not in intent:
                errors.append(f"Missing required section: {section}")
                continue

            section_data = intent[section]
            if not isinstance(section_data, dict):
                errors.append(f"Section '{section}' must be a dictionary")
                continue

            for field in required_fields:
                if field not in section_data:
                    errors.append(f"Missing required field: {section}.{field}")

        # Validate pipeline_identity
        if "pipeline_identity" in intent:
            errors.extend(self._validate_pipeline_identity(intent["pipeline_identity"]))

        # Validate source_config
        if "source_config" in intent:
            errors.extend(self._validate_source_config(intent["source_config"]))

        # Validate schema_definition
        if "schema_definition" in intent:
            schema_valid, schema_errors = self.validate_schema_definition(
                intent["schema_definition"]
            )
            errors.extend(schema_errors)

        return len(errors) == 0, errors

    def _validate_pipeline_identity(
        self, identity: Dict[str, Any]
    ) -> List[str]:
        """Validate pipeline identity section."""
        errors = []

        # Validate pipeline_name format
        pipeline_name = identity.get("pipeline_name", "")
        if pipeline_name:
            if not re.match(r"^[a-z][a-z0-9_]*$", pipeline_name):
                errors.append(
                    "pipeline_name must be lowercase alphanumeric with underscores, "
                    f"starting with a letter. Got: '{pipeline_name}'"
                )
            if len(pipeline_name) > 64:
                errors.append("pipeline_name must be 64 characters or less")

        # Validate domain format
        domain = identity.get("domain", "")
        if domain:
            if not re.match(r"^[a-z][a-z0-9_]*$", domain):
                errors.append(
                    f"domain must be lowercase alphanumeric with underscores. Got: '{domain}'"
                )

        # Validate environment
        environment = identity.get("environment", "")
        if environment and environment not in self.VALID_ENVIRONMENTS:
            errors.append(
                f"environment must be one of {self.VALID_ENVIRONMENTS}. Got: '{environment}'"
            )

        return errors

    def _validate_source_config(
        self, source: Dict[str, Any]
    ) -> List[str]:
        """Validate source configuration section."""
        errors = []

        # Validate source_type
        source_type = source.get("source_type", "")
        if source_type and source_type not in self.VALID_SOURCE_TYPES:
            errors.append(
                f"source_type must be one of {self.VALID_SOURCE_TYPES}. Got: '{source_type}'"
            )

        # Validate processing_mode if present
        processing_mode = source.get("processing_mode", "batch")
        if processing_mode not in self.VALID_PROCESSING_MODES:
            errors.append(
                f"processing_mode must be one of {self.VALID_PROCESSING_MODES}. "
                f"Got: '{processing_mode}'"
            )

        # Validate source-specific config
        if source_type == "file":
            file_config = source.get("file_config", {})
            if not file_config.get("landing_path"):
                errors.append("file source requires file_config.landing_path")

        elif source_type == "database":
            db_config = source.get("database_config", {})
            if not db_config.get("jdbc_url") and not db_config.get("connection_name"):
                errors.append(
                    "database source requires database_config.jdbc_url or connection_name"
                )

        elif source_type == "streaming":
            stream_config = source.get("streaming_config", {})
            if not stream_config.get("topic"):
                errors.append("streaming source requires streaming_config.topic")

        elif source_type == "api":
            api_config = source.get("api_config", {})
            if not api_config.get("endpoint"):
                errors.append("api source requires api_config.endpoint")

        return errors

    def validate_schema_definition(
        self, schema_def: Dict[str, Any]
    ) -> Tuple[bool, List[str]]:
        """
        Validate schema definition structure.

        Args:
            schema_def: Schema definition to validate

        Returns:
            Tuple of (is_valid, list of error messages)
        """
        errors = []

        if not schema_def:
            return False, ["Schema definition is empty"]

        columns = schema_def.get("columns", [])
        if not columns:
            errors.append("Schema must have at least one column")
            return False, errors

        if not isinstance(columns, list):
            errors.append("columns must be a list")
            return False, errors

        # Track column names for duplicate detection
        seen_names = set()

        for i, col in enumerate(columns):
            if not isinstance(col, dict):
                errors.append(f"Column {i} must be a dictionary")
                continue

            # Validate column name
            col_name = col.get("name")
            if not col_name:
                errors.append(f"Column {i} is missing 'name' field")
            else:
                # Check format
                if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", col_name):
                    errors.append(
                        f"Column name '{col_name}' invalid. "
                        "Must start with letter/underscore, contain only alphanumeric/underscore"
                    )

                # Check for duplicates
                if col_name.lower() in seen_names:
                    errors.append(f"Duplicate column name: '{col_name}'")
                seen_names.add(col_name.lower())

            # Validate data type
            data_type = col.get("data_type")
            if not data_type:
                errors.append(f"Column '{col_name}' is missing 'data_type' field")
            else:
                # Extract base type (before parentheses for decimal(10,2) etc)
                base_type = data_type.lower().split("(")[0].strip()
                if base_type not in self.SUPPORTED_DATA_TYPES:
                    errors.append(
                        f"Column '{col_name}' has unsupported type '{data_type}'. "
                        f"Supported: {sorted(self.SUPPORTED_DATA_TYPES)}"
                    )

        # Validate primary keys if specified
        primary_keys = schema_def.get("primary_keys", [])
        column_names = {col.get("name") for col in columns if col.get("name")}
        for pk in primary_keys:
            if pk not in column_names:
                errors.append(f"Primary key '{pk}' not found in columns")

        # Validate partition columns if specified
        partition_columns = schema_def.get("partition_columns", [])
        for pc in partition_columns:
            if pc not in column_names:
                errors.append(f"Partition column '{pc}' not found in columns")

        return len(errors) == 0, errors

    def compare_schemas(
        self,
        old_schema: Dict[str, Any],
        new_schema: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """
        Compare two schemas and return list of changes.

        Args:
            old_schema: Previous schema version
            new_schema: New schema version

        Returns:
            List of change dictionaries with:
            - change_type: added, removed, modified, type_changed, constraint_changed
            - column_name: affected column
            - old_value: previous value (if applicable)
            - new_value: new value (if applicable)
        """
        changes = []

        old_columns = {
            col.get("name"): col
            for col in old_schema.get("columns", [])
        }
        new_columns = {
            col.get("name"): col
            for col in new_schema.get("columns", [])
        }

        # Find added columns
        for name, col in new_columns.items():
            if name not in old_columns:
                changes.append({
                    "change_type": "added",
                    "column_name": name,
                    "old_value": None,
                    "new_value": col.get("data_type"),
                })

        # Find removed columns
        for name, col in old_columns.items():
            if name not in new_columns:
                changes.append({
                    "change_type": "removed",
                    "column_name": name,
                    "old_value": col.get("data_type"),
                    "new_value": None,
                })

        # Find modified columns
        for name in set(old_columns.keys()) & set(new_columns.keys()):
            old_col = old_columns[name]
            new_col = new_columns[name]

            # Check data type change
            if old_col.get("data_type") != new_col.get("data_type"):
                changes.append({
                    "change_type": "type_changed",
                    "column_name": name,
                    "old_value": old_col.get("data_type"),
                    "new_value": new_col.get("data_type"),
                })

            # Check nullable change
            elif old_col.get("nullable") != new_col.get("nullable"):
                changes.append({
                    "change_type": "constraint_changed",
                    "column_name": name,
                    "old_value": f"nullable={old_col.get('nullable')}",
                    "new_value": f"nullable={new_col.get('nullable')}",
                })

            # Check description change
            elif old_col.get("description") != new_col.get("description"):
                changes.append({
                    "change_type": "description_changed",
                    "column_name": name,
                    "old_value": old_col.get("description", ""),
                    "new_value": new_col.get("description", ""),
                })

        # Check primary key changes
        old_pks = set(old_schema.get("primary_keys", []))
        new_pks = set(new_schema.get("primary_keys", []))
        if old_pks != new_pks:
            changes.append({
                "change_type": "primary_key_changed",
                "column_name": "_primary_keys",
                "old_value": list(old_pks),
                "new_value": list(new_pks),
            })

        # Check partition column changes
        old_partitions = set(old_schema.get("partition_columns", []))
        new_partitions = set(new_schema.get("partition_columns", []))
        if old_partitions != new_partitions:
            changes.append({
                "change_type": "partition_changed",
                "column_name": "_partition_columns",
                "old_value": list(old_partitions),
                "new_value": list(new_partitions),
            })

        return changes

    def get_breaking_changes(
        self,
        changes: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Filter changes to only breaking changes.

        Breaking changes are:
        - Column removed
        - Type changed (potential data loss)
        - Primary key changed
        - Nullable changed from True to False

        Args:
            changes: List of all changes

        Returns:
            List of breaking changes only
        """
        breaking_types = {
            "removed", "type_changed", "primary_key_changed",
        }

        breaking = []
        for change in changes:
            if change.get("change_type") in breaking_types:
                breaking.append(change)
            elif change.get("change_type") == "constraint_changed":
                # Check if nullable changed from True to False
                if "nullable=True" in str(change.get("old_value", "")):
                    if "nullable=False" in str(change.get("new_value", "")):
                        breaking.append(change)

        return breaking

    def validate_compatibility(
        self,
        old_schema: Dict[str, Any],
        new_schema: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Validate schema compatibility for upgrade.

        Returns:
            Dict with 'compatible', 'changes', 'breaking_changes', 'warnings'
        """
        changes = self.compare_schemas(old_schema, new_schema)
        breaking_changes = self.get_breaking_changes(changes)

        warnings = []

        # Warn about removed columns
        for change in breaking_changes:
            if change.get("change_type") == "removed":
                warnings.append(
                    f"Column '{change.get('column_name')}' will be removed. "
                    "This may break downstream consumers."
                )

        # Warn about type changes
        for change in breaking_changes:
            if change.get("change_type") == "type_changed":
                warnings.append(
                    f"Column '{change.get('column_name')}' type changed from "
                    f"'{change.get('old_value')}' to '{change.get('new_value')}'. "
                    "Verify data compatibility."
                )

        return {
            "compatible": len(breaking_changes) == 0,
            "changes": changes,
            "breaking_changes": breaking_changes,
            "warnings": warnings,
        }
