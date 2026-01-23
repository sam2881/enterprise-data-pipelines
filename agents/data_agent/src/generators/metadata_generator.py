"""
Metadata SQL Generation from Jinja2 Templates.

WHY: Generates SQL statements for pipeline metadata registration.
     All SQL is deterministic and parameterized for safety.

HOW:
1. Load appropriate SQL template based on operation
2. Inject context variables from intent and state
3. Render template to produce valid SQL
4. Return SQL for validation and execution

SQL OPERATIONS:
- insert_pipeline: Register new pipeline in metadata
- insert_schema: Record new schema version
- update_schema: Upgrade schema version
- insert_transformation_rules: Register transformation rules
- insert_dq_rules: Register data quality rules
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import structlog
from jinja2 import Environment, FileSystemLoader, StrictUndefined, TemplateNotFound

from src.utils.exceptions import TemplateNotFoundError, TemplateRenderError

logger = structlog.get_logger()


class MetadataGenerator:
    """
    Generate metadata SQL statements from Jinja2 templates.

    RULES:
    1. NEVER hard-code business logic - all config from metadata
    2. Use Jinja2 templates ONLY
    3. Templates are FROZEN - no runtime modifications
    4. All generated SQL must be safe (no injection)
    """

    TEMPLATE_DIR = Path(__file__).parent.parent / "templates" / "sql"

    def __init__(self) -> None:
        """Initialize generator with Jinja2 environment."""
        self._env: Optional[Environment] = None

    @property
    def env(self) -> Environment:
        """Lazy-load Jinja2 environment."""
        if self._env is None:
            # Create template directory if it doesn't exist
            self.TEMPLATE_DIR.mkdir(parents=True, exist_ok=True)

            self._env = Environment(
                loader=FileSystemLoader(str(self.TEMPLATE_DIR)),
                undefined=StrictUndefined,
                trim_blocks=True,
                lstrip_blocks=True,
                keep_trailing_newline=True,
                autoescape=False,  # SQL templates handle their own escaping
            )

            # Add custom filters
            self._env.filters["to_json"] = self._to_json
            self._env.filters["sql_string"] = self._sql_string
            self._env.filters["sql_array"] = self._sql_array

        return self._env

    def generate(self, template_name: str, context: Dict[str, Any]) -> str:
        """
        Generate SQL from template.

        Args:
            template_name: Name of template (without .sql.jinja2)
            context: Template variables

        Returns:
            Generated SQL as string

        Raises:
            TemplateNotFoundError: If template doesn't exist
            TemplateRenderError: If rendering fails
        """
        logger.debug("Generating SQL", template=template_name)

        try:
            # Try with .sql.jinja2 extension first
            try:
                template = self.env.get_template(f"{template_name}.sql.jinja2")
            except TemplateNotFound:
                # Try with just .jinja2
                try:
                    template = self.env.get_template(f"{template_name}.jinja2")
                except TemplateNotFound:
                    raise TemplateNotFoundError(
                        f"Template '{template_name}' not found in {self.TEMPLATE_DIR}"
                    )

            rendered = template.render(**context)

            logger.debug(
                "SQL generated successfully",
                template=template_name,
                length=len(rendered),
            )

            return rendered

        except TemplateNotFoundError:
            raise
        except Exception as e:
            logger.error(
                "SQL template render failed",
                template=template_name,
                error=str(e),
            )
            raise TemplateRenderError(
                message=f"Failed to render SQL template '{template_name}': {str(e)}",
                template_name=template_name,
            )

    def generate_pipeline_insert(self, context: Dict[str, Any]) -> str:
        """
        Generate INSERT statement for new pipeline.

        Args:
            context: Pipeline context with identity, source, target config

        Returns:
            SQL INSERT statement
        """
        # Build insert context
        insert_context = {
            "pipeline_name": context.get("pipeline_name"),
            "domain": context.get("domain"),
            "environment": context.get("environment", "dev"),
            "source_type": context.get("source_type", "file"),
            "source_system": context.get("source_system", "unknown"),
            "data_sensitivity": context.get("data_sensitivity", "internal"),
            "processing_mode": context.get("processing_mode", "batch"),
            "schedule": context.get("schedule", "@daily"),
            "owner": context.get("owner", "data-platform"),
            "technical_owner": context.get("owner", "data-platform"),
            "business_owner": context.get("business_owner", ""),
            "status": "active",
            "jira_ticket": context.get("jira_ticket", ""),
            "created_by": context.get("created_by", "data-agent"),
            "created_at": datetime.utcnow().isoformat(),
            "generated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "config_json": self._to_json({
                "landing_path": context.get("landing_path"),
                "bronze_table": context.get("bronze_table"),
                "silver_table": context.get("silver_table"),
                "gold_table": context.get("gold_table"),
                "bigquery_dataset": context.get("bigquery_dataset"),
                "bigquery_table": context.get("bigquery_table"),
            }),
        }

        try:
            return self.generate("insert_pipeline", insert_context)
        except TemplateNotFoundError:
            # Generate inline if template doesn't exist
            return self._generate_pipeline_insert_inline(insert_context)

    def _generate_pipeline_insert_inline(self, ctx: Dict[str, Any]) -> str:
        """Generate pipeline INSERT SQL inline (fallback)."""
        return f"""
INSERT INTO pipeline (
    pipeline_name,
    domain,
    environment,
    source_type,
    processing_mode,
    schedule_cron,
    technical_owner,
    business_owner,
    status,
    jira_ticket,
    config_json,
    created_by,
    created_at
) VALUES (
    {self._sql_string(ctx.get('pipeline_name'))},
    {self._sql_string(ctx.get('domain'))},
    {self._sql_string(ctx.get('environment'))},
    {self._sql_string(ctx.get('source_type'))},
    {self._sql_string(ctx.get('processing_mode'))},
    {self._sql_string(ctx.get('schedule'))},
    {self._sql_string(ctx.get('owner'))},
    {self._sql_string(ctx.get('business_owner'))},
    {self._sql_string(ctx.get('status'))},
    {self._sql_string(ctx.get('jira_ticket'))},
    {self._sql_string(ctx.get('config_json'))}::jsonb,
    {self._sql_string(ctx.get('created_by'))},
    NOW()
);
""".strip()

    def generate_schema_insert(self, context: Dict[str, Any]) -> str:
        """
        Generate INSERT statement for new schema version.

        Args:
            context: Schema context with columns, version info

        Returns:
            SQL INSERT statement
        """
        schema_context = {
            "pipeline_id": context.get("pipeline_id"),
            "version": context.get("schema_version", 1),
            "schema_json": self._to_json({
                "columns": context.get("columns", []),
                "primary_keys": context.get("primary_keys", []),
                "partition_columns": context.get("partition_columns", []),
            }),
            "created_by": context.get("created_by", "data-agent"),
            "created_at": datetime.utcnow().isoformat(),
        }

        try:
            return self.generate("insert_schema", schema_context)
        except TemplateNotFoundError:
            return self._generate_schema_insert_inline(schema_context)

    def _generate_schema_insert_inline(self, ctx: Dict[str, Any]) -> str:
        """Generate schema INSERT SQL inline (fallback)."""
        return f"""
INSERT INTO schema_version (
    pipeline_id,
    version,
    schema_json,
    is_current,
    created_by,
    created_at
) VALUES (
    {ctx.get('pipeline_id')},
    {ctx.get('version')},
    {self._sql_string(ctx.get('schema_json'))}::jsonb,
    TRUE,
    {self._sql_string(ctx.get('created_by'))},
    NOW()
);
""".strip()

    def generate_schema_upgrade(
        self,
        context: Dict[str, Any],
        schema_plan: Dict[str, Any],
    ) -> str:
        """
        Generate UPDATE + INSERT for schema upgrade.

        Args:
            context: Current pipeline context
            schema_plan: Schema change plan from planner

        Returns:
            SQL statements for schema upgrade
        """
        pipeline_id = context.get("pipeline_id")
        new_version = schema_plan.get("new_version", 1)

        statements = []

        # First, mark current version as not current
        statements.append(f"""
UPDATE schema_version
SET is_current = FALSE
WHERE pipeline_id = {pipeline_id}
  AND is_current = TRUE;
""".strip())

        # Then insert new version
        schema_context = {
            "pipeline_id": pipeline_id,
            "version": new_version,
            "schema_json": self._to_json({
                "columns": context.get("columns", []),
                "primary_keys": context.get("primary_keys", []),
                "partition_columns": context.get("partition_columns", []),
                "changes": schema_plan.get("changes", []),
            }),
            "created_by": context.get("created_by", "data-agent"),
        }

        statements.append(self._generate_schema_insert_inline(schema_context))

        return "\n\n".join(statements)

    def generate_transformation_rules(self, context: Dict[str, Any]) -> str:
        """
        Generate INSERT statements for transformation rules.

        Args:
            context: Context with transformation_rules list

        Returns:
            SQL INSERT statements
        """
        rules = context.get("transformation_rules", [])
        if not rules:
            return "-- No transformation rules to insert"

        statements = []
        for i, rule in enumerate(rules):
            statements.append(f"""
INSERT INTO transformation_rule (
    pipeline_id,
    rule_order,
    rule_type,
    source_column,
    target_column,
    expression,
    description,
    created_by,
    created_at
) VALUES (
    {context.get('pipeline_id')},
    {i + 1},
    {self._sql_string(rule.get('rule_type', 'transform'))},
    {self._sql_string(rule.get('source_column'))},
    {self._sql_string(rule.get('target_column'))},
    {self._sql_string(rule.get('expression'))},
    {self._sql_string(rule.get('description', ''))},
    {self._sql_string(context.get('created_by', 'data-agent'))},
    NOW()
);
""".strip())

        return "\n\n".join(statements)

    def generate_dq_rules(self, context: Dict[str, Any]) -> str:
        """
        Generate INSERT statements for data quality rules.

        Args:
            context: Context with data_quality_rules list

        Returns:
            SQL INSERT statements
        """
        rules = context.get("data_quality_rules", [])
        if not rules:
            return "-- No data quality rules to insert"

        statements = []
        for rule in rules:
            statements.append(f"""
INSERT INTO data_quality_rule (
    pipeline_id,
    rule_name,
    rule_type,
    column_name,
    expression,
    threshold,
    severity,
    enabled,
    created_by,
    created_at
) VALUES (
    {context.get('pipeline_id')},
    {self._sql_string(rule.get('rule_name'))},
    {self._sql_string(rule.get('rule_type', 'not_null'))},
    {self._sql_string(rule.get('column_name'))},
    {self._sql_string(rule.get('expression', ''))},
    {rule.get('threshold', 100.0)},
    {self._sql_string(rule.get('severity', 'error'))},
    TRUE,
    {self._sql_string(context.get('created_by', 'data-agent'))},
    NOW()
);
""".strip())

        return "\n\n".join(statements)

    def generate_pipeline_update(self, pipeline_data: Dict[str, Any]) -> str:
        """Generate UPDATE statement for pipeline modification."""
        try:
            return self.generate("update_pipeline", pipeline_data)
        except TemplateNotFoundError:
            return f"""
UPDATE pipeline
SET schedule_cron = {self._sql_string(pipeline_data.get('schedule'))},
    config_json = {self._sql_string(self._to_json(pipeline_data.get('config', {})))}::jsonb,
    updated_by = {self._sql_string(pipeline_data.get('updated_by', 'data-agent'))},
    updated_at = NOW()
WHERE pipeline_id = {pipeline_data.get('pipeline_id')};
""".strip()

    def generate_schema_update(self, schema_data: Dict[str, Any]) -> str:
        """Generate UPDATE/INSERT statements for schema version."""
        return self.generate_schema_upgrade(schema_data, schema_data.get("schema_plan", {}))

    def generate_execution_insert(self, execution_data: Dict[str, Any]) -> str:
        """Generate INSERT statement for execution record."""
        try:
            return self.generate("insert_execution", execution_data)
        except TemplateNotFoundError:
            return f"""
INSERT INTO execution_log (
    pipeline_id,
    execution_id,
    dag_run_id,
    status,
    started_at,
    context_json,
    created_at
) VALUES (
    {execution_data.get('pipeline_id')},
    {self._sql_string(execution_data.get('execution_id'))},
    {self._sql_string(execution_data.get('dag_run_id', ''))},
    {self._sql_string(execution_data.get('status', 'pending'))},
    NOW(),
    {self._sql_string(self._to_json(execution_data.get('context', {})))}::jsonb,
    NOW()
);
""".strip()

    def generate_all_metadata_sql(
        self,
        pipeline_data: Dict[str, Any],
        schema_data: Dict[str, Any],
        execution_data: Dict[str, Any],
    ) -> List[str]:
        """
        Generate all metadata SQL for a pipeline execution.

        Args:
            pipeline_data: Pipeline metadata
            schema_data: Schema version data
            execution_data: Execution record data

        Returns:
            List of SQL statements
        """
        statements: List[str] = []

        if pipeline_data.get("is_new"):
            statements.append(self.generate_pipeline_insert(pipeline_data))

        if schema_data.get("has_changes"):
            statements.append(self.generate_schema_update(schema_data))

        statements.append(self.generate_execution_insert(execution_data))

        return statements

    @staticmethod
    def _to_json(value: Any) -> str:
        """Convert value to JSON string."""
        if value is None:
            return "{}"
        return json.dumps(value, default=str)

    @staticmethod
    def _sql_string(value: Optional[str]) -> str:
        """
        Safely quote a string for SQL.

        Escapes single quotes and wraps in quotes.
        """
        if value is None:
            return "NULL"
        # Escape single quotes by doubling them
        escaped = str(value).replace("'", "''")
        return f"'{escaped}'"

    @staticmethod
    def _sql_array(values: List[str]) -> str:
        """Convert list to SQL array literal."""
        if not values:
            return "ARRAY[]::TEXT[]"
        items = [MetadataGenerator._sql_string(v) for v in values]
        return "ARRAY[" + ", ".join(items) + "]"
