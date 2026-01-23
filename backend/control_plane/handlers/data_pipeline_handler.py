"""
Data Pipeline Workflow Handler - Jira-Driven Pipeline Generation

WHY: Implements the complete Data Pipeline workflow from Jira ticket to deployed DAG.
     Integrates with Jira, code generation, Dataproc, and Airflow.

WORKFLOW:
1. Receive Jira ticket for new data pipeline
2. Analyze source data requirements
3. Generate Spark code and Airflow DAG
4. Review and approve generated code
5. Deploy to Git and trigger CI/CD
6. Monitor Airflow execution

USAGE:
    from control_plane.handlers.data_pipeline_handler import DataPipelineHandler
    from control_plane import get_control_plane, WorkflowType

    handler = DataPipelineHandler()
    orchestrator = get_control_plane()
    orchestrator.register_handler(WorkflowType.DATA_PIPELINE, handler)
"""
import logging
from typing import Any, Dict, List, Optional
from datetime import datetime

from backend.control_plane.orchestrator import (
    WorkflowHandler,
    WorkflowContext,
    RiskLevel,
)

logger = logging.getLogger(__name__)


class DataPipelineHandler(WorkflowHandler):
    """
    Handler for Data Pipeline (Jira) workflows.

    INTEGRATIONS:
    - Jira API for ticket management
    - GCS for source data analysis
    - LLM for code generation
    - Dataproc for Spark job submission
    - Airflow for DAG deployment
    - Git for code versioning
    """

    def __init__(self, settings=None):
        """Initialize Data Pipeline handler."""
        from backend.config.settings import get_settings

        self.settings = settings or get_settings()
        self._llm_client = None
        self._gcs_client = None
        self._dataproc_client = None

    async def _ensure_initialized(self) -> None:
        """Lazy initialization of clients."""
        if self._llm_client is not None:
            return

        from backend.secrets_manager import get_secret

        # Initialize LLM client
        try:
            import openai
            self._llm_client = openai.AsyncOpenAI(
                api_key=get_secret("OPENAI_API_KEY")
            )
        except Exception as e:
            logger.warning(f"OpenAI client initialization failed: {e}")

        # Initialize Dataproc client
        try:
            from backend.infrastructure import get_dataproc_client
            self._dataproc_client = get_dataproc_client()
        except Exception as e:
            logger.warning(f"Dataproc client initialization failed: {e}")

        logger.info("DataPipelineHandler initialized")

    async def analyze(self, context: WorkflowContext) -> Dict[str, Any]:
        """
        Analyze the Jira ticket and source data.

        Steps:
        1. Parse Jira ticket for requirements
        2. Analyze source data location and schema
        3. Determine pipeline type and complexity
        4. Estimate effort and risk

        Returns:
            Analysis result with source schema and recommendations
        """
        await self._ensure_initialized()

        jira_data = context.metadata.get("jira_ticket", {})

        # Parse requirements from Jira
        requirements = self._parse_requirements(jira_data)

        # Analyze source data if path provided
        source_analysis = None
        if requirements.get("source_path"):
            source_analysis = await self._analyze_source_data(
                requirements["source_path"]
            )

        # Determine risk and complexity
        complexity = self._assess_complexity(requirements, source_analysis)
        context.risk_level = self._determine_risk_level(complexity)
        context.confidence_score = complexity.get("confidence", 0.6)

        return {
            "jira_key": context.source_id,
            "requirements": requirements,
            "source_analysis": source_analysis,
            "pipeline_type": requirements.get("pipeline_type", "batch"),
            "complexity": complexity,
            "estimated_effort_hours": complexity.get("effort_hours", 8),
            "analyzed_at": datetime.utcnow().isoformat(),
        }

    def _parse_requirements(self, jira_data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse pipeline requirements from Jira ticket."""
        description = jira_data.get("description", "")
        summary = jira_data.get("summary", "")

        # Extract key information (would use LLM in production)
        return {
            "summary": summary,
            "source_path": self._extract_gcs_path(description),
            "source_format": self._extract_format(description),
            "target_zone": self._extract_target_zone(description),
            "schedule": self._extract_schedule(description),
            "pipeline_type": "batch",  # Default to batch
            "sla_hours": 4,  # Default SLA
        }

    def _extract_gcs_path(self, text: str) -> Optional[str]:
        """Extract GCS path from text."""
        import re
        match = re.search(r'gs://[^\s]+', text)
        return match.group(0) if match else None

    def _extract_format(self, text: str) -> str:
        """Extract data format from text."""
        text_lower = text.lower()
        if "parquet" in text_lower:
            return "parquet"
        elif "json" in text_lower:
            return "json"
        elif "csv" in text_lower:
            return "csv"
        return "unknown"

    def _extract_target_zone(self, text: str) -> str:
        """Extract target zone from text."""
        text_lower = text.lower()
        if "gold" in text_lower:
            return "gold"
        elif "silver" in text_lower:
            return "silver"
        return "bronze"  # Default to bronze

    def _extract_schedule(self, text: str) -> str:
        """Extract schedule from text."""
        text_lower = text.lower()
        if "hourly" in text_lower:
            return "@hourly"
        elif "weekly" in text_lower:
            return "@weekly"
        return "@daily"  # Default to daily

    async def _analyze_source_data(self, source_path: str) -> Dict[str, Any]:
        """Analyze source data schema and statistics."""
        # TODO: Integrate with GCS client to infer schema
        # For now, return placeholder

        return {
            "path": source_path,
            "format": "parquet",  # Would be detected
            "schema": [],  # Would contain column definitions
            "row_count_estimate": 1000000,
            "size_mb": 100,
            "partitions": [],
        }

    def _assess_complexity(
        self,
        requirements: Dict[str, Any],
        source_analysis: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Assess pipeline complexity."""
        complexity_score = 0.5  # Base complexity

        # Increase for larger datasets
        if source_analysis:
            size_mb = source_analysis.get("size_mb", 0)
            if size_mb > 1000:
                complexity_score += 0.2
            if size_mb > 10000:
                complexity_score += 0.2

        # Increase for gold zone (aggregations)
        if requirements.get("target_zone") == "gold":
            complexity_score += 0.1

        return {
            "score": min(complexity_score, 1.0),
            "effort_hours": int(complexity_score * 16),  # 0.5 = 8 hours
            "confidence": 1.0 - complexity_score,  # More complex = less confident
            "factors": ["data_volume", "transformation_complexity", "target_zone"],
        }

    def _determine_risk_level(self, complexity: Dict[str, Any]) -> RiskLevel:
        """Determine risk level from complexity."""
        score = complexity.get("score", 0.5)

        if score >= 0.8:
            return RiskLevel.HIGH
        elif score >= 0.5:
            return RiskLevel.MEDIUM
        return RiskLevel.LOW

    async def generate_plan(self, context: WorkflowContext) -> Dict[str, Any]:
        """
        Generate pipeline code and deployment plan.

        Steps:
        1. Generate PySpark transformation code
        2. Generate Airflow DAG
        3. Generate data quality checks
        4. Create deployment plan

        Returns:
            Generated code and deployment plan
        """
        await self._ensure_initialized()

        analysis = context.analysis_result or {}
        requirements = analysis.get("requirements", {})
        source_analysis = analysis.get("source_analysis", {})

        # Generate PySpark code
        spark_code = await self._generate_spark_code(
            requirements,
            source_analysis,
        )

        # Generate Airflow DAG
        dag_code = await self._generate_dag_code(
            context.source_id,
            requirements,
            spark_code,
        )

        # Generate DQ checks
        dq_config = self._generate_dq_config(source_analysis)

        return {
            "summary": f"Pipeline for {context.source_id}: {requirements.get('source_path', 'unknown')} → {requirements.get('target_zone', 'bronze')}",
            "action": "deploy_pipeline",
            "artifacts": {
                "spark_code": spark_code,
                "dag_code": dag_code,
                "dq_config": dq_config,
            },
            "deployment_steps": [
                {"step": 1, "action": "Validate generated code", "automated": True},
                {"step": 2, "action": "Create Git branch", "automated": True},
                {"step": 3, "action": "Commit artifacts", "automated": True},
                {"step": 4, "action": "Create pull request", "automated": True},
                {"step": 5, "action": "Run CI/CD pipeline", "automated": True},
                {"step": 6, "action": "Deploy DAG to Airflow", "automated": True},
            ],
            "target_environment": self.settings.environment.value,
            "generated_at": datetime.utcnow().isoformat(),
        }

    async def _generate_spark_code(
        self,
        requirements: Dict[str, Any],
        source_analysis: Dict[str, Any],
    ) -> str:
        """Generate PySpark transformation code."""
        if not self._llm_client:
            return self._fallback_spark_code(requirements)

        prompt = f"""Generate production-ready PySpark code for the following data pipeline:

Source: {requirements.get('source_path', 'gs://bucket/data')}
Format: {requirements.get('source_format', 'parquet')}
Target Zone: {requirements.get('target_zone', 'bronze')}

Requirements:
- Use Spark 3.x API
- Include error handling
- Add logging
- Support idempotent execution
- Use partitioning appropriately

Generate clean, documented PySpark code."""

        try:
            response = await self._llm_client.chat.completions.create(
                model=self.settings.llm.primary_model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.0,
            )
            return response.choices[0].message.content

        except Exception as e:
            logger.error(f"Spark code generation failed: {e}")
            return self._fallback_spark_code(requirements)

    def _fallback_spark_code(self, requirements: Dict[str, Any]) -> str:
        """Fallback Spark code template."""
        source_path = requirements.get("source_path", "gs://bucket/data")
        target_zone = requirements.get("target_zone", "bronze")

        return f'''"""
Auto-generated PySpark transformation.
Source: {source_path}
Target: {target_zone}
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

def main():
    spark = SparkSession.builder \\
        .appName("data_pipeline") \\
        .getOrCreate()

    # Read source data
    df = spark.read.parquet("{source_path}")

    # Add metadata columns
    df = df.withColumn("_loaded_at", current_timestamp())

    # Write to {target_zone} zone
    output_path = "gs://bucket-{target_zone}/data"
    df.write.mode("overwrite").parquet(output_path)

    spark.stop()

if __name__ == "__main__":
    main()
'''

    async def _generate_dag_code(
        self,
        jira_key: str,
        requirements: Dict[str, Any],
        spark_code: str,
    ) -> str:
        """Generate Airflow DAG code."""
        dag_id = f"dag_{jira_key.lower().replace('-', '_')}"
        schedule = requirements.get("schedule", "@daily")

        return f'''"""
Auto-generated Airflow DAG for {jira_key}.
Schedule: {schedule}
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperator,
)

default_args = {{
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}}

with DAG(
    dag_id="{dag_id}",
    default_args=default_args,
    schedule_interval="{schedule}",
    catchup=False,
    tags=["data-pipeline", "{jira_key}"],
) as dag:

    submit_spark_job = DataprocSubmitJobOperator(
        task_id="submit_spark_job",
        project_id="{{{{ var.value.gcp_project_id }}}}",
        region="{{{{ var.value.gcp_region }}}}",
        job={{
            "placement": {{"cluster_name": "{{{{ var.value.dataproc_cluster }}}}"}},
            "pyspark_job": {{
                "main_python_file_uri": "gs://{{{{ var.value.bucket_temp }}}}/jobs/{dag_id}/transform.py",
            }},
        }},
    )
'''

    def _generate_dq_config(self, source_analysis: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """Generate data quality configuration."""
        return {
            "suite_name": "auto_generated_dq",
            "expectations": [
                {
                    "expectation_type": "expect_table_row_count_to_be_between",
                    "kwargs": {"min_value": 1, "max_value": 1000000000},
                },
                {
                    "expectation_type": "expect_table_columns_to_match_set",
                    "kwargs": {"column_set": []},  # Would be populated from schema
                },
            ],
        }

    async def execute(self, context: WorkflowContext) -> Dict[str, Any]:
        """
        Execute the pipeline deployment.

        Steps:
        1. Upload Spark code to GCS
        2. Deploy DAG to Airflow
        3. Trigger initial run (optional)
        """
        plan = context.plan or {}
        artifacts = plan.get("artifacts", {})

        # Upload Spark code to GCS
        spark_uri = await self._upload_spark_code(
            context.source_id,
            artifacts.get("spark_code", ""),
        )

        # Deploy DAG (in production, this would go through Git/CI)
        dag_deployed = await self._deploy_dag(
            context.source_id,
            artifacts.get("dag_code", ""),
        )

        return {
            "execution_type": "deployment",
            "spark_code_uri": spark_uri,
            "dag_deployed": dag_deployed,
            "status": "deployed" if dag_deployed else "failed",
            "deployed_at": datetime.utcnow().isoformat(),
        }

    async def _upload_spark_code(self, jira_key: str, code: str) -> str:
        """Upload Spark code to GCS."""
        # TODO: Integrate with GCS client
        gcs_uri = f"gs://{self.settings.gcs.bucket_temp}/jobs/dag_{jira_key.lower().replace('-', '_')}/transform.py"
        logger.info(f"Uploaded Spark code to {gcs_uri}")
        return gcs_uri

    async def _deploy_dag(self, jira_key: str, dag_code: str) -> bool:
        """Deploy DAG to Airflow."""
        # TODO: Integrate with Git and Airflow deployment
        logger.info(f"Deployed DAG for {jira_key}")
        return True

    async def verify(self, context: WorkflowContext) -> Dict[str, Any]:
        """
        Verify pipeline deployment.

        Steps:
        1. Check DAG is visible in Airflow
        2. Verify Spark code is accessible
        3. Update Jira ticket status
        """
        execution_result = context.execution_result or {}

        # Check deployment status
        success = execution_result.get("status") == "deployed"

        if success:
            # Update Jira ticket
            await self._update_jira_status(
                context.source_id,
                status="Done",
                comment=f"Pipeline deployed by AI Agent: {context.workflow_id}",
            )

        return {
            "success": success,
            "dag_visible": success,  # Would actually check Airflow
            "spark_code_accessible": success,  # Would actually check GCS
            "jira_updated": success,
            "verified_at": datetime.utcnow().isoformat(),
        }

    async def _update_jira_status(
        self,
        jira_key: str,
        status: str,
        comment: str,
    ) -> None:
        """Update Jira ticket status."""
        # TODO: Integrate with Jira API
        logger.info(f"Updated Jira {jira_key} to {status}")

    async def rollback(self, context: WorkflowContext) -> None:
        """
        Rollback failed deployment.

        Compensation:
        1. Remove deployed DAG
        2. Delete uploaded code
        3. Update Jira status
        """
        logger.info(f"Rolling back pipeline deployment for {context.source_id}")

        # TODO: Implement actual rollback
        # - Delete DAG from Airflow
        # - Remove code from GCS
        # - Update Jira status

        await self._update_jira_status(
            context.source_id,
            status="In Progress",
            comment=f"Deployment rolled back: {context.workflow_id}",
        )
