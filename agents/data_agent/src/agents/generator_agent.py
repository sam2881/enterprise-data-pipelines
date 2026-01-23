"""
Generator Agent - Code Generation from Templates

WHY: Generates Airflow DAGs, Spark jobs, and metadata SQL from Jinja2 templates.
     All code generation is DETERMINISTIC - same input = same output.

HOW:
1. Read planner_output for template selection
2. Build context from intent and metadata
3. Render templates using Jinja2
4. Return GeneratorOutput with all generated artifacts

RULES:
- NEVER hard-code business logic - all config from metadata
- ALWAYS use Jinja2 templates
- Templates are FROZEN - no runtime modifications
- All generated code must be deterministic
"""

from typing import Any, Dict, List
import uuid

from src.agents.base_agent import BaseAgent, requires_state_keys
from src.utils.exceptions import GenerationError, TemplateNotFoundError, TemplateRenderError


class GeneratorAgent(BaseAgent):
    """
    Generator Agent for code artifact generation.

    RESPONSIBILITIES:
    1. Generate Airflow DAG code from templates
    2. Generate PySpark job code for each layer
    3. Generate metadata SQL for database updates
    4. Return all artifacts with paths

    RULES:
    - ALL code comes from templates
    - NO hard-coded business logic
    - Deterministic output for same input
    """

    def __init__(self) -> None:
        super().__init__("generator")
        self._dag_generator = None
        self._spark_generator = None
        self._metadata_generator = None

    @property
    def dag_generator(self):
        """Lazy-load DAG generator."""
        if self._dag_generator is None:
            from src.generators.dag_generator import DAGGenerator
            self._dag_generator = DAGGenerator()
        return self._dag_generator

    @property
    def spark_generator(self):
        """Lazy-load Spark generator."""
        if self._spark_generator is None:
            from src.generators.spark_generator import SparkGenerator
            self._spark_generator = SparkGenerator()
        return self._spark_generator

    @property
    def metadata_generator(self):
        """Lazy-load metadata generator."""
        if self._metadata_generator is None:
            from src.generators.metadata_generator import MetadataGenerator
            self._metadata_generator = MetadataGenerator()
        return self._metadata_generator

    @requires_state_keys("intent_json", "request_id", "planner_output")
    def execute(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute code generation.

        Steps:
        1. Extract template selection from planner_output
        2. Build template context from intent
        3. Generate DAG code
        4. Generate Spark job code for each template
        5. Generate metadata SQL
        6. Return GeneratorOutput with all artifacts

        Args:
            state: Current workflow state with intent and planner_output

        Returns:
            Partial state update with generator_output
        """
        self.logger.info(
            "Starting code generation",
            request_id=state["request_id"],
        )

        try:
            intent = state["intent_json"]
            planner_output = state["planner_output"]
            metadata_context = state.get("metadata_context", {})

            # Check if no generation needed
            if planner_output.get("pipeline_action") == "no_change":
                self.logger.info("No generation needed - pipeline unchanged")
                return {
                    "current_phase": "generating",
                    "generator_output": {
                        "generation_id": str(uuid.uuid4()),
                        "dag_code": "",
                        "spark_jobs": {},
                        "metadata_sql": [],
                        "artifact_paths": {},
                        "skipped": True,
                        "skip_reason": "Pipeline unchanged - no generation needed",
                    },
                }

            template_selection = planner_output.get("template_selection", {})

            # Build generation context
            context = self._build_context(intent, planner_output, metadata_context)

            # Generate DAG code
            dag_template = template_selection.get("dag_template", "file_ingest_dag")
            dag_code = self._generate_dag(dag_template, context)

            # Generate Spark jobs
            spark_templates = template_selection.get("spark_templates", [])
            spark_jobs = self._generate_spark_jobs(spark_templates, context)

            # Generate metadata SQL
            metadata_sql = self._generate_metadata_sql(context, planner_output)

            # Build artifact paths
            artifact_paths = self._build_artifact_paths(context, template_selection)

            # Build generator output
            generator_output = {
                "generation_id": str(uuid.uuid4()),
                "dag_code": dag_code,
                "spark_jobs": spark_jobs,
                "metadata_sql": metadata_sql,
                "artifact_paths": artifact_paths,
            }

            self.logger.info(
                "Code generation complete",
                request_id=state["request_id"],
                dag_lines=len(dag_code.splitlines()),
                spark_job_count=len(spark_jobs),
                sql_statement_count=len(metadata_sql),
            )

            return {
                "current_phase": "generating",
                "generator_output": generator_output,
                "decision_reasoning": f"Generated {len(spark_jobs)} Spark jobs and 1 DAG "
                    f"using {dag_template} template.",
            }

        except TemplateNotFoundError as e:
            self.logger.error("Template not found", error=str(e))
            return self.create_error_state(str(e), e)
        except TemplateRenderError as e:
            self.logger.error("Template rendering failed", error=str(e))
            return self.create_error_state(str(e), e)
        except Exception as e:
            self.logger.error("Unexpected generation error", error=str(e))
            return self.create_error_state(f"Generation failed: {str(e)}", e)

    def _build_context(
        self,
        intent: Dict[str, Any],
        planner_output: Dict[str, Any],
        metadata_context: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Build template context from intent and metadata.

        Context includes all variables needed by templates.
        """
        pipeline_identity = intent.get("pipeline_identity", {})
        source_config = intent.get("source_config", {})
        target_config = intent.get("target_config", {})
        schema_definition = intent.get("schema_definition", {})
        execution_policy = intent.get("execution_policy", {})

        # Extract common values
        pipeline_name = pipeline_identity.get("pipeline_name", "unknown")
        domain = pipeline_identity.get("domain", "default")
        environment = pipeline_identity.get("environment", "dev")

        # Build DAG ID
        dag_id = f"{domain}_{pipeline_name}"

        # Get table names
        bronze_table = f"{domain}.bronze_{pipeline_name}"
        silver_table = f"{domain}.silver_{pipeline_name}"
        gold_table = f"{domain}.gold_{pipeline_name}"

        # Build context
        context = {
            # Pipeline identity
            "dag_id": dag_id,
            "pipeline_name": pipeline_name,
            "domain": domain,
            "environment": environment,
            "pipeline_id": metadata_context.get("pipeline_id"),

            # Schedule
            "schedule": execution_policy.get("schedule_cron", "@daily"),
            "start_date": execution_policy.get("start_date", "2024-01-01"),
            "catchup": execution_policy.get("catchup", False),

            # Source configuration
            "source_type": source_config.get("source_type", "file"),
            "source_system": source_config.get("source_system", "unknown"),
            "processing_mode": source_config.get("processing_mode", "batch"),
            "cdc_enabled": source_config.get("cdc_enabled", False),

            # File source specific - support both file_config (structured) and direct file_path (API)
            "landing_path": source_config.get("file_config", {}).get("landing_path", "") or source_config.get("file_path", ""),
            "source_path": source_config.get("file_config", {}).get("landing_path", "") or source_config.get("file_path", ""),
            "file_pattern": source_config.get("file_config", {}).get("file_pattern", "*.parquet"),
            "file_format": source_config.get("file_config", {}).get("file_format", "parquet"),

            # Database source specific
            "jdbc_url": source_config.get("database_config", {}).get("jdbc_url", ""),
            "source_table": source_config.get("database_config", {}).get("table_name", ""),
            "incremental_column": source_config.get("database_config", {}).get("incremental_column"),

            # Streaming source specific
            "kafka_topic": source_config.get("streaming_config", {}).get("topic", ""),
            "kafka_bootstrap": source_config.get("streaming_config", {}).get("bootstrap_servers", ""),

            # API source specific
            "api_endpoint": source_config.get("api_config", {}).get("endpoint", ""),
            "api_method": source_config.get("api_config", {}).get("method", "GET"),

            # CDC specific
            "cdc_topic": source_config.get("database_config", {}).get("cdc_topic", ""),
            "merge_keys": source_config.get("database_config", {}).get("merge_keys", []),

            # Target configuration
            "bronze_table": bronze_table,
            "silver_table": silver_table,
            "gold_table": gold_table,
            "bigquery_dataset": target_config.get("bigquery_config", {}).get("dataset", domain),
            "bigquery_table": target_config.get("bigquery_config", {}).get("table_name", pipeline_name),

            # GCS paths
            "gcs_bronze_path": f"gs://{domain}-bronze/{pipeline_name}",
            "gcs_silver_path": f"gs://{domain}-silver/{pipeline_name}",
            "gcs_gold_path": f"gs://{domain}-gold/{pipeline_name}",

            # Schema
            "columns": schema_definition.get("columns", []),
            "primary_keys": schema_definition.get("primary_keys", []),
            "partition_columns": schema_definition.get("partition_columns", []),

            # Transformation rules
            "transformation_rules": intent.get("transformation_rules", []),

            # Data quality rules
            "data_quality_rules": intent.get("data_quality_rules", []),

            # Spark job paths (will be filled in)
            "spark_job_paths": {},

            # Owners
            "owner": pipeline_identity.get("technical_owner", "data-platform"),
            "business_owner": pipeline_identity.get("business_owner", ""),

            # Metadata
            "jira_ticket": intent.get("jira_ticket", ""),
            "created_by": intent.get("created_by", "data-agent"),
            "schema_version": planner_output.get("schema_plan", {}).get("new_version", 1),
        }

        return context

    def _generate_dag(self, template_name: str, context: Dict[str, Any]) -> str:
        """Generate Airflow DAG code from template."""
        self.logger.debug(f"Generating DAG from template: {template_name}")

        # Add Spark job paths to context
        spark_job_paths = self._get_spark_job_paths(context)
        context["spark_job_paths"] = spark_job_paths

        return self.dag_generator.generate(template_name, context)

    def _generate_spark_jobs(
        self,
        template_names: List[str],
        context: Dict[str, Any],
    ) -> Dict[str, str]:
        """Generate PySpark job code for each template."""
        spark_jobs = {}

        for template_name in template_names:
            self.logger.debug(f"Generating Spark job from template: {template_name}")

            job_name = f"{context['pipeline_name']}_{template_name}"
            spark_jobs[job_name] = self.spark_generator.generate(template_name, context)

        return spark_jobs

    def _generate_metadata_sql(
        self,
        context: Dict[str, Any],
        planner_output: Dict[str, Any],
    ) -> List[str]:
        """Generate metadata SQL statements."""
        sql_statements = []

        schema_plan = planner_output.get("schema_plan", {})
        action = schema_plan.get("action", "none")

        if action == "create":
            # Generate INSERT for new pipeline
            sql_statements.append(
                self.metadata_generator.generate_pipeline_insert(context)
            )
            # Generate schema version INSERT
            sql_statements.append(
                self.metadata_generator.generate_schema_insert(context)
            )
        elif action == "upgrade":
            # Generate schema version UPDATE and INSERT
            sql_statements.append(
                self.metadata_generator.generate_schema_upgrade(context, schema_plan)
            )

        # Generate transformation rules SQL if present
        if context.get("transformation_rules"):
            sql_statements.append(
                self.metadata_generator.generate_transformation_rules(context)
            )

        # Generate data quality rules SQL if present
        if context.get("data_quality_rules"):
            sql_statements.append(
                self.metadata_generator.generate_dq_rules(context)
            )

        return sql_statements

    def _build_artifact_paths(
        self,
        context: Dict[str, Any],
        template_selection: Dict[str, Any],
    ) -> Dict[str, str]:
        """Build paths for all generated artifacts."""
        pipeline_name = context["pipeline_name"]
        domain = context["domain"]

        paths = {
            "dag": f"dags/{domain}_{pipeline_name}_dag.py",
        }

        # Add Spark job paths
        for template in template_selection.get("spark_templates", []):
            job_name = f"{pipeline_name}_{template}"
            paths[f"spark_{template}"] = f"spark_jobs/{domain}/{job_name}.py"

        # Add metadata SQL path
        paths["metadata_sql"] = f"metadata_sql/{domain}_{pipeline_name}.sql"

        return paths

    def _get_spark_job_paths(self, context: Dict[str, Any]) -> Dict[str, str]:
        """Get GCS paths for Spark jobs."""
        pipeline_name = context["pipeline_name"]
        domain = context["domain"]

        return {
            "bronze": f"gs://spark-jobs/{domain}/{pipeline_name}_bronze_ingest.py",
            "silver": f"gs://spark-jobs/{domain}/{pipeline_name}_silver_transform.py",
            "gold": f"gs://spark-jobs/{domain}/{pipeline_name}_gold_load_bq.py",
            "cdc_merge": f"gs://spark-jobs/{domain}/{pipeline_name}_cdc_merge.py",
            "scd2": f"gs://spark-jobs/{domain}/{pipeline_name}_scd2_apply.py",
        }
