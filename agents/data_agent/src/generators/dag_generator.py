"""
DAG Code Generation from Jinja2 Templates.

WHY: Generates Airflow DAGs from templates based on pipeline configuration.
     All DAG code is deterministic - same input = same output.

HOW:
1. Load appropriate template based on source type and processing mode
2. Inject context variables from intent and planner output
3. Render template to produce valid Python DAG code
4. Return code for validation and deployment

TEMPLATES (Enterprise):
- enterprise_pipeline_dag: Full medallion architecture with task groups
- database_cdc_dag: Database change data capture (timestamp, log-based)

TEMPLATES (Simple):
- file_ingest_dag: Batch file ingestion from GCS/S3
- cdc_ingest_dag: CDC processing with merge operations
- streaming_dag: Kafka/Pub/Sub streaming ingestion
- api_ingest_dag: REST API polling ingestion

TEMPLATE SELECTION:
- Enterprise template used for production and complex pipelines
- Simple templates for development and quick prototypes
"""

from pathlib import Path
from typing import Any, Dict, List, Optional

import structlog
from jinja2 import Environment, FileSystemLoader, StrictUndefined, TemplateNotFound

from src.utils.exceptions import TemplateNotFoundError, TemplateRenderError

logger = structlog.get_logger()


class DAGGenerator:
    """
    Generate Airflow DAGs from Jinja2 templates.

    RULES:
    1. NEVER hard-code business logic - all config from metadata
    2. Use Jinja2 templates ONLY
    3. Templates are FROZEN - no runtime modifications
    4. All generated code must be deterministic
    """

    TEMPLATE_DIR = Path(__file__).parent.parent / "templates" / "dag"

    # Template requirements - required context variables per template
    TEMPLATE_REQUIREMENTS: Dict[str, List[str]] = {
        # Enterprise templates
        "enterprise_pipeline_dag": [
            "dag_id",
            "product_name",
            "product_code",
            "domain_name",
            "environment",
            "source_type",
            "source_format",
            "landing_path",
        ],
        "database_cdc_dag": [
            "dag_id",
            "product_code",
            "domain_name",
            "source_database",
            "jdbc_url_secret",
            "source_schema",
            "source_table",
            "primary_keys",
        ],
        # Simple templates (legacy)
        "file_ingest_dag": [
            "dag_id",
            "pipeline_name",
            "domain",
            "schedule",
            "landing_path",
            "bronze_table",
            "silver_table",
            "spark_job_paths",
        ],
        "cdc_ingest_dag": [
            "dag_id",
            "pipeline_name",
            "domain",
            "schedule",
            "cdc_topic",
            "bronze_table",
            "silver_table",
            "merge_keys",
            "spark_job_paths",
        ],
        "streaming_dag": [
            "dag_id",
            "pipeline_name",
            "domain",
            "kafka_topic",
            "bronze_table",
            "spark_job_paths",
        ],
        "api_ingest_dag": [
            "dag_id",
            "pipeline_name",
            "domain",
            "schedule",
            "api_endpoint",
            "bronze_table",
            "silver_table",
            "spark_job_paths",
        ],
    }

    # Template selection based on source type and processing mode
    TEMPLATE_SELECTION_MAP: Dict[str, Dict[str, str]] = {
        # File-based sources
        "file_gcs": {
            "default": "enterprise_pipeline_dag",
            "simple": "file_ingest_dag",
        },
        "file_s3": {
            "default": "enterprise_pipeline_dag",
            "simple": "file_ingest_dag",
        },
        "file_sftp": {
            "default": "enterprise_pipeline_dag",
            "simple": "file_ingest_dag",
        },
        # Database sources
        "database_postgres": {
            "default": "database_cdc_dag",
            "full_snapshot": "enterprise_pipeline_dag",
        },
        "database_mysql": {
            "default": "database_cdc_dag",
            "full_snapshot": "enterprise_pipeline_dag",
        },
        "database_oracle": {
            "default": "database_cdc_dag",
            "full_snapshot": "enterprise_pipeline_dag",
        },
        "database_sqlserver": {
            "default": "database_cdc_dag",
            "full_snapshot": "enterprise_pipeline_dag",
        },
        # Streaming sources
        "streaming_kafka": {
            "default": "streaming_dag",
        },
        "streaming_pubsub": {
            "default": "streaming_dag",
        },
        # API sources
        "api_rest": {
            "default": "api_ingest_dag",
        },
        # Mainframe sources
        "mainframe_ftp": {
            "default": "enterprise_pipeline_dag",
        },
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
                undefined=StrictUndefined,  # Fail on undefined variables
                trim_blocks=True,
                lstrip_blocks=True,
                keep_trailing_newline=True,
            )

            # Add custom filters
            self._env.filters["to_python_list"] = self._to_python_list
            self._env.filters["quote"] = self._quote_string

        return self._env

    def generate(self, template_name: str, context: Dict[str, Any]) -> str:
        """
        Generate DAG code from template.

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
            "Generating DAG",
            template=template_name,
            dag_id=context.get("dag_id"),
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
                "DAG generated successfully",
                template=template_name,
                dag_id=context.get("dag_id"),
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
            "start_date": "2024-01-01",
            "catchup": False,
            "max_active_runs": 1,
            "default_args": {
                "owner": context.get("owner", "data-platform"),
                "depends_on_past": False,
                "email_on_failure": True,
                "email_on_retry": False,
                "retries": 3,
                "retry_delay_minutes": 5,
            },
            "tags": [
                context.get("domain", "unknown"),
                context.get("environment", "dev"),
                "data-platform",
            ],
            "description": f"Pipeline for {context.get('pipeline_name', 'unknown')}",
        }

        # Merge defaults with provided context (provided takes precedence)
        merged = {**defaults, **context}

        # Merge default_args separately
        if "default_args" in context:
            merged["default_args"] = {
                **defaults["default_args"],
                **context["default_args"],
            }

        return merged

    def _get_required_vars(self, template_name: str) -> List[str]:
        """Get required variables for a template."""
        return self.TEMPLATE_REQUIREMENTS.get(template_name, [])

    def get_available_templates(self) -> List[str]:
        """Get list of available DAG templates."""
        templates = []
        if self.TEMPLATE_DIR.exists():
            for path in self.TEMPLATE_DIR.glob("*.jinja2"):
                # Remove extension(s) to get template name
                name = path.name
                if name.endswith(".py.jinja2"):
                    name = name[:-10]
                elif name.endswith(".jinja2"):
                    name = name[:-7]
                templates.append(name)
        return sorted(templates)

    @staticmethod
    def _to_python_list(value: List[Any]) -> str:
        """Convert list to Python list literal."""
        if not value:
            return "[]"
        items = [f'"{v}"' if isinstance(v, str) else str(v) for v in value]
        return "[" + ", ".join(items) + "]"

    @staticmethod
    def _quote_string(value: str) -> str:
        """Quote a string for Python code."""
        if value is None:
            return '""'
        # Escape quotes and newlines
        escaped = value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
        return f'"{escaped}"'

    def select_template(
        self,
        source_type: str,
        processing_mode: str = "default",
        use_enterprise: bool = True,
    ) -> str:
        """
        Select appropriate template based on source type and processing mode.

        Args:
            source_type: Type of data source (file_gcs, database_postgres, etc.)
            processing_mode: Processing mode (default, simple, full_snapshot, etc.)
            use_enterprise: Whether to prefer enterprise templates

        Returns:
            Template name to use
        """
        # Get template options for source type
        template_options = self.TEMPLATE_SELECTION_MAP.get(source_type, {})

        if not template_options:
            # Default to enterprise template for unknown sources
            logger.warning(
                f"Unknown source type: {source_type}, defaulting to enterprise_pipeline_dag"
            )
            return "enterprise_pipeline_dag"

        # Select based on processing mode
        if processing_mode in template_options:
            template = template_options[processing_mode]
        elif "default" in template_options:
            template = template_options["default"]
        else:
            template = list(template_options.values())[0]

        # Override to simple if not using enterprise
        if not use_enterprise and "simple" in template_options:
            template = template_options["simple"]

        logger.debug(
            f"Selected template: {template} for source_type={source_type}, "
            f"processing_mode={processing_mode}"
        )

        return template

    def generate_from_config(
        self,
        config: Dict[str, Any],
        use_enterprise: bool = True,
    ) -> str:
        """
        Generate DAG from a configuration dictionary.

        Automatically selects the appropriate template based on config.

        Args:
            config: Pipeline configuration dictionary
            use_enterprise: Whether to prefer enterprise templates

        Returns:
            Generated DAG code
        """
        # Extract source type and processing mode
        source_type = config.get("source_type", "file_gcs")
        processing_mode = config.get("processing_mode", "default")

        # Select template
        template_name = self.select_template(
            source_type=source_type,
            processing_mode=processing_mode,
            use_enterprise=use_enterprise,
        )

        logger.info(
            f"Generating DAG using template: {template_name}",
            dag_id=config.get("dag_id"),
            source_type=source_type,
        )

        return self.generate(template_name, config)

    def generate_enterprise_dag(
        self,
        product_config: Dict[str, Any],
    ) -> str:
        """
        Generate enterprise DAG with full medallion architecture.

        Args:
            product_config: Data product configuration from metadata

        Returns:
            Generated enterprise DAG code
        """
        # Build context for enterprise template
        context = {
            # Core identification
            "dag_id": product_config.get("dag_id", f"{product_config['product_code']}_dag"),
            "product_name": product_config["product_name"],
            "product_code": product_config["product_code"],
            "domain_name": product_config["domain_name"],
            "environment": product_config.get("environment", "dev"),

            # Source configuration
            "source_type": product_config.get("source_type", "file_gcs"),
            "source_format": product_config.get("source_format", "parquet"),
            "landing_path": product_config["landing_path"],
            "source_config": product_config.get("source_config", {}),

            # Zone paths
            "bronze_path": product_config.get("bronze_path"),
            "silver_path": product_config.get("silver_path"),
            "gold_path": product_config.get("gold_path"),
            "trusted_path": product_config.get("trusted_path"),
            "rejected_path": product_config.get("rejected_path"),
            "archive_path": product_config.get("archive_path"),

            # Processing
            "processing_mode": product_config.get("processing_mode", "batch_incremental"),
            "load_strategy": product_config.get("load_strategy", "append"),
            "primary_keys": product_config.get("primary_keys", []),
            "partition_columns": product_config.get("partition_columns", []),
            "watermark_column": product_config.get("watermark_column", ""),

            # Scheduling
            "schedule_interval": product_config.get("schedule_interval"),
            "start_date": product_config.get("start_date", "2024-01-01"),
            "catchup": product_config.get("catchup", False),
            "max_active_runs": product_config.get("max_active_runs", 1),

            # Quality
            "quality_rules": product_config.get("quality_rules", []),
            "min_quality_score": product_config.get("min_quality_score", 80.0),
            "block_on_quality_failure": product_config.get("block_on_quality_failure", True),

            # Self-healing
            "self_healing_enabled": product_config.get("self_healing_enabled", True),
            "max_healing_attempts": product_config.get("max_healing_attempts", 3),

            # Spark configurations
            "spark_bronze_config": product_config.get("spark_bronze_config", {}),
            "spark_silver_config": product_config.get("spark_silver_config", {}),
            "spark_gold_config": product_config.get("spark_gold_config", {}),
            "spark_jobs": product_config.get("spark_jobs", {}),

            # Targets
            "targets": product_config.get("targets", []),

            # Notifications
            "alert_emails": product_config.get("alert_emails", []),
            "slack_webhook": product_config.get("slack_webhook", ""),
            "pagerduty_key": product_config.get("pagerduty_key", ""),

            # Ownership
            "owner": product_config.get("owner", "data-platform"),
            "jira_ticket": product_config.get("jira_ticket"),
            "tags": product_config.get("tags", []),
        }

        return self.generate("enterprise_pipeline_dag", context)
