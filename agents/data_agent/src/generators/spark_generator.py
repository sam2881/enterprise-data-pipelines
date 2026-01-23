"""
Spark Job Code Generation from Jinja2 Templates.

WHY: Generates PySpark job code from templates for each data layer.
     All Spark code is deterministic - same input = same output.

HOW:
1. Load appropriate template based on layer and processing type
2. Inject context variables from intent and planner output
3. Render template to produce valid PySpark code
4. Return code for validation and deployment

TEMPLATES:
- bronze_ingest: Raw data ingestion to bronze layer
- silver_transform: Data cleansing and transformation to silver
- gold_load_bq: Load curated data to BigQuery
- cdc_merge: Merge CDC events into target table
- scd2_apply: Apply SCD Type 2 history tracking
- streaming_bronze: Streaming ingestion to bronze layer
"""

from pathlib import Path
from typing import Any, Dict, List, Optional

import structlog
from jinja2 import Environment, FileSystemLoader, StrictUndefined, TemplateNotFound

from src.utils.exceptions import TemplateNotFoundError, TemplateRenderError

logger = structlog.get_logger()


class SparkGenerator:
    """
    Generate PySpark jobs from Jinja2 templates.

    RULES:
    1. NEVER hard-code business logic - all config from metadata
    2. Use Jinja2 templates ONLY
    3. Templates are FROZEN - no runtime modifications
    4. All generated code must be deterministic
    """

    TEMPLATE_DIR = Path(__file__).parent.parent / "templates" / "spark"

    # Template requirements - required context variables per template
    TEMPLATE_REQUIREMENTS: Dict[str, List[str]] = {
        "bronze_ingest": [
            "pipeline_name",
            "landing_path",
            "bronze_table",
            "file_format",
        ],
        "silver_transform": [
            "pipeline_name",
            "bronze_table",
            "silver_table",
            "columns",
        ],
        "gold_load_bq": [
            "pipeline_name",
            "silver_table",
            "bigquery_dataset",
            "bigquery_table",
        ],
        "cdc_merge": [
            "pipeline_name",
            "cdc_topic",
            "bronze_table",
            "merge_keys",
        ],
        "scd2_apply": [
            "pipeline_name",
            "bronze_table",
            "silver_table",
            "primary_keys",
        ],
        "streaming_bronze": [
            "pipeline_name",
            "kafka_topic",
            "kafka_bootstrap",
            "bronze_table",
        ],
        "dv2_hub": [
            "pipeline_name",
            "silver_table",
            "hub_table",
            "business_keys",
        ],
        "dv2_satellite": [
            "pipeline_name",
            "silver_table",
            "satellite_table",
            "hub_key",
        ],
        "dv2_link": [
            "pipeline_name",
            "silver_table",
            "link_table",
            "hub_keys",
        ],
        "star_fact": [
            "pipeline_name",
            "silver_table",
            "fact_table",
            "dimension_keys",
        ],
        "star_dimension": [
            "pipeline_name",
            "silver_table",
            "dimension_table",
            "natural_keys",
        ],
    }

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
            )

            # Add custom filters
            self._env.filters["to_spark_schema"] = self._to_spark_schema
            self._env.filters["to_column_list"] = self._to_column_list
            self._env.filters["quote"] = self._quote_string

        return self._env

    def generate(self, template_name: str, context: Dict[str, Any]) -> str:
        """
        Generate Spark job code from template.

        Args:
            template_name: Name of template (without .py.jinja2)
            context: Template variables

        Returns:
            Generated Python code as string

        Raises:
            TemplateNotFoundError: If template doesn't exist
            TemplateRenderError: If rendering fails
        """
        logger.debug(
            "Generating Spark job",
            template=template_name,
            pipeline=context.get("pipeline_name"),
        )

        # Validate required context
        self._validate_context(template_name, context)

        # Add default values for optional context
        context = self._apply_defaults(context)

        try:
            # Try with .py.jinja2 extension first
            try:
                template = self.env.get_template(f"{template_name}.py.jinja2")
            except TemplateNotFound:
                # Try with just .jinja2
                try:
                    template = self.env.get_template(f"{template_name}.jinja2")
                except TemplateNotFound:
                    raise TemplateNotFoundError(
                        f"Template '{template_name}' not found in {self.TEMPLATE_DIR}"
                    )

            rendered = template.render(**context)

            logger.info(
                "Spark job generated successfully",
                template=template_name,
                pipeline=context.get("pipeline_name"),
                lines=len(rendered.splitlines()),
            )

            return rendered

        except TemplateNotFoundError:
            raise
        except Exception as e:
            logger.error(
                "Template render failed",
                template=template_name,
                error=str(e),
            )
            raise TemplateRenderError(
                message=f"Failed to render template '{template_name}': {str(e)}",
                template_name=template_name,
            )

    def generate_all_layers(
        self,
        pipeline_name: str,
        context: Dict[str, Any],
        templates: List[str],
    ) -> Dict[str, str]:
        """
        Generate all Spark jobs for a pipeline.

        Args:
            pipeline_name: Name of the pipeline
            context: Template variables
            templates: List of template names to generate

        Returns:
            Dict mapping job names to generated code
        """
        jobs: Dict[str, str] = {}

        for template in templates:
            job_name = f"{pipeline_name}_{template}"
            try:
                jobs[job_name] = self.generate(template, context)
            except TemplateNotFoundError:
                logger.warning(
                    "Template not found, skipping",
                    template=template,
                    pipeline=pipeline_name,
                )

        return jobs

    def _validate_context(
        self,
        template_name: str,
        context: Dict[str, Any],
    ) -> None:
        """
        Validate that required context variables are present.

        Raises:
            ValueError: If required variables are missing
        """
        required_vars = self.TEMPLATE_REQUIREMENTS.get(template_name, [])
        missing = [v for v in required_vars if v not in context or context[v] is None]

        if missing:
            raise ValueError(
                f"Missing required template variables for '{template_name}': {missing}"
            )

    def _apply_defaults(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Apply default values for optional context variables."""
        defaults = {
            "environment": "dev",
            "spark_config": {
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true",
            },
            "write_mode": "append",
            "partition_columns": [],
            "file_format": "parquet",
        }

        # Merge defaults with provided context (provided takes precedence)
        merged = {**defaults, **context}

        # Merge spark_config separately
        if "spark_config" in context:
            merged["spark_config"] = {
                **defaults["spark_config"],
                **context["spark_config"],
            }

        return merged

    def _get_required_vars(self, template_name: str) -> List[str]:
        """Get required variables for a template."""
        return self.TEMPLATE_REQUIREMENTS.get(template_name, [])

    def get_available_templates(self) -> List[str]:
        """Get list of available Spark templates."""
        templates = []
        if self.TEMPLATE_DIR.exists():
            for path in self.TEMPLATE_DIR.glob("*.jinja2"):
                name = path.name
                if name.endswith(".py.jinja2"):
                    name = name[:-10]
                elif name.endswith(".jinja2"):
                    name = name[:-7]
                templates.append(name)
        return sorted(templates)

    @staticmethod
    def _to_spark_schema(columns: List[Dict[str, Any]]) -> str:
        """
        Convert column definitions to Spark StructType schema.

        Args:
            columns: List of column definitions with name and data_type

        Returns:
            Python code string for StructType schema
        """
        if not columns:
            return "StructType([])"

        type_mapping = {
            "string": "StringType()",
            "varchar": "StringType()",
            "text": "StringType()",
            "int": "IntegerType()",
            "integer": "IntegerType()",
            "long": "LongType()",
            "bigint": "LongType()",
            "float": "FloatType()",
            "double": "DoubleType()",
            "boolean": "BooleanType()",
            "bool": "BooleanType()",
            "date": "DateType()",
            "timestamp": "TimestampType()",
            "datetime": "TimestampType()",
            "binary": "BinaryType()",
            "bytes": "BinaryType()",
        }

        fields = []
        for col in columns:
            name = col.get("name", "unknown")
            data_type = col.get("data_type", "string").lower()
            nullable = col.get("nullable", True)

            # Handle decimal type specially
            if data_type.startswith("decimal"):
                spark_type = f"DecimalType({data_type[7:]})" if "(" in data_type else "DecimalType(10, 2)"
            else:
                spark_type = type_mapping.get(data_type.split("(")[0], "StringType()")

            fields.append(f'    StructField("{name}", {spark_type}, {nullable})')

        return "StructType([\n" + ",\n".join(fields) + "\n])"

    @staticmethod
    def _to_column_list(columns: List[Dict[str, Any]]) -> str:
        """Convert column definitions to Python list of column names."""
        if not columns:
            return "[]"
        names = [f'"{col.get("name", "")}"' for col in columns]
        return "[" + ", ".join(names) + "]"

    @staticmethod
    def _quote_string(value: str) -> str:
        """Quote a string for Python code."""
        if value is None:
            return '""'
        escaped = value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
        return f'"{escaped}"'
