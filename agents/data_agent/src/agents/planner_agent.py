"""
Planner Agent - Pipeline Strategy and Template Selection

WHY: Determines the execution strategy for pipeline creation/modification.
     Queries metadata repository for existing pipelines, detects schema changes,
     and selects appropriate templates based on source configuration.

HOW:
1. Check if pipeline exists in metadata repository
2. Compare schemas to detect changes (added, removed, modified columns)
3. Select templates using Template Selection Matrix
4. Return PlannerOutput with pipeline_action, schema_plan, template_selection

TEMPLATE SELECTION MATRIX:
| Source   | Mode        | CDC | DAG Template         | Spark Templates              |
|----------|-------------|-----|----------------------|------------------------------|
| file     | batch       | No  | file_ingest_dag      | bronze, silver, gold_bq      |
| file     | micro_batch | No  | streaming_ingest_dag | bronze, silver, gold_bq      |
| database | batch       | No  | db_snapshot_dag      | bronze, silver, gold_bq      |
| database | batch       | Yes | cdc_ingest_dag       | cdc_merge, scd2, gold_bq     |
| streaming| streaming   | N/A | streaming_ingest_dag | streaming_bronze, gold_bq    |
| api      | batch       | No  | api_ingest_dag       | bronze, silver, gold_bq      |
"""

from typing import Any, Dict, List, Optional
import uuid

from src.agents.base_agent import BaseAgent, requires_state_keys
from src.utils.exceptions import PlanningError, TemplateSelectionError


class PlannerAgent(BaseAgent):
    """
    Planner Agent for pipeline strategy determination.

    RESPONSIBILITIES:
    1. Query metadata for existing pipeline
    2. Detect schema changes by comparison
    3. Select templates based on source configuration
    4. Generate execution plan

    RULES:
    - NEVER guess missing configuration values
    - NEVER auto-fix schema conflicts
    - ALWAYS query metadata first
    - ALWAYS return explicit pipeline_action
    """

    def __init__(self) -> None:
        super().__init__("planner")

    @requires_state_keys("intent_json", "request_id")
    def execute(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute planning logic.

        Steps:
        1. Parse intent and extract pipeline identity
        2. Query metadata for existing pipeline
        3. If exists, detect schema changes
        4. Select templates based on source config
        5. Build and return PlannerOutput

        Args:
            state: Current workflow state with intent_json

        Returns:
            Partial state update with planner_output
        """
        self.logger.info(
            "Starting pipeline planning",
            request_id=state["request_id"],
        )

        try:
            intent = state["intent_json"]

            # Step 1: Extract pipeline identity
            pipeline_identity = intent.get("pipeline_identity", {})
            pipeline_name = pipeline_identity.get("pipeline_name")
            environment = pipeline_identity.get("environment", "dev")
            domain = pipeline_identity.get("domain", "unknown")

            if not pipeline_name:
                return self.create_error_state("Missing pipeline_name in intent")

            # Step 2: Check if pipeline exists
            existing_pipeline = self.repo.get_pipeline_by_name(
                pipeline_name=pipeline_name,
                environment=environment,
            )

            # Step 3: Determine pipeline action
            if existing_pipeline is None:
                pipeline_action = "create"
                is_new = True
                pipeline_id = None
                schema_changes = []
                current_schema_version = None

                self.log_decision(
                    decision="Create new pipeline",
                    reasoning=f"No existing pipeline found with name '{pipeline_name}' in {environment}",
                    alternatives=["Modify existing", "Upgrade schema"],
                )
            else:
                pipeline_id = existing_pipeline.pipeline_id
                is_new = False

                # Get current schema for comparison
                current_schema = self.repo.get_current_schema(pipeline_id)
                current_schema_version = current_schema.version if current_schema else None

                # Detect schema changes
                new_schema = intent.get("schema_definition", {})
                schema_changes = self._detect_schema_changes(
                    current_schema.schema_json if current_schema else {},
                    new_schema,
                )

                # Determine action based on changes
                if schema_changes:
                    pipeline_action = "upgrade_schema"
                    self.log_decision(
                        decision="Upgrade schema",
                        reasoning=f"Detected {len(schema_changes)} schema changes",
                        alternatives=["No change", "Modify config only"],
                        context={"changes": schema_changes[:5]},
                    )
                elif self._config_changed(existing_pipeline, intent):
                    pipeline_action = "modify"
                    self.log_decision(
                        decision="Modify pipeline configuration",
                        reasoning="Configuration changed but schema unchanged",
                        alternatives=["No change", "Upgrade schema"],
                    )
                else:
                    pipeline_action = "no_change"
                    self.log_decision(
                        decision="No changes needed",
                        reasoning="Pipeline and schema match current state",
                        alternatives=["Force update"],
                    )

            # Step 4: Select templates
            source_config = intent.get("source_config", {})
            template_selection = self._select_templates(
                source_type=source_config.get("source_type", "file"),
                processing_mode=source_config.get("processing_mode", "batch"),
                cdc_enabled=source_config.get("cdc_enabled", False),
                modeling_strategy=intent.get("modeling_strategy", "none"),
            )

            # Step 5: Build schema plan
            schema_plan = {
                "action": "create" if is_new else (
                    "upgrade" if pipeline_action == "upgrade_schema" else "none"
                ),
                "new_version": (current_schema_version or 0) + 1 if pipeline_action in [
                    "create", "upgrade_schema"
                ] else current_schema_version,
                "changes": schema_changes,
                "breaking_changes": [
                    c for c in schema_changes
                    if c.get("change_type") in ["removed", "type_changed"]
                ],
            }

            # Step 6: Determine estimated tasks
            estimated_tasks = self._get_estimated_tasks(pipeline_action, template_selection)

            # Build planner output
            planner_output = {
                "plan_id": str(uuid.uuid4()),
                "pipeline_action": pipeline_action,
                "pipeline_id": pipeline_id,
                "is_new_pipeline": is_new,
                "schema_plan": schema_plan,
                "template_selection": template_selection,
                "estimated_tasks": estimated_tasks,
            }

            self.logger.info(
                "Planning complete",
                request_id=state["request_id"],
                pipeline_action=pipeline_action,
                is_new=is_new,
                template_count=len(template_selection.get("spark_templates", [])),
            )

            return {
                "current_phase": "planning",
                "planner_output": planner_output,
                "metadata_context": {
                    "pipeline_id": pipeline_id,
                    "is_new": is_new,
                    "existing_schema_version": current_schema_version,
                    "domain": domain,
                    "environment": environment,
                },
                "decision_reasoning": f"Action: {pipeline_action}. "
                    f"{'New pipeline' if is_new else f'Existing pipeline #{pipeline_id}'} "
                    f"with {len(schema_changes)} schema changes.",
            }

        except PlanningError as e:
            self.logger.error("Planning failed", error=str(e))
            return self.create_error_state(str(e), e)
        except Exception as e:
            self.logger.error("Unexpected planning error", error=str(e))
            return self.create_error_state(f"Planning failed: {str(e)}", e)

    def _detect_schema_changes(
        self,
        current_schema: Dict[str, Any],
        new_schema: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """
        Detect changes between current and new schema.

        Compares columns to find:
        - Added columns
        - Removed columns
        - Modified columns (type, nullable, etc.)

        Returns:
            List of schema change dictionaries
        """
        changes = []

        current_columns = {
            col.get("name"): col
            for col in current_schema.get("columns", [])
        }
        new_columns = {
            col.get("name"): col
            for col in new_schema.get("columns", [])
        }

        # Find added columns
        for name, col in new_columns.items():
            if name not in current_columns:
                changes.append({
                    "change_type": "added",
                    "column_name": name,
                    "old_value": None,
                    "new_value": col.get("data_type"),
                })

        # Find removed columns
        for name, col in current_columns.items():
            if name not in new_columns:
                changes.append({
                    "change_type": "removed",
                    "column_name": name,
                    "old_value": col.get("data_type"),
                    "new_value": None,
                })

        # Find modified columns
        for name in set(current_columns.keys()) & set(new_columns.keys()):
            old_col = current_columns[name]
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

        return changes

    def _config_changed(
        self,
        existing_pipeline,
        intent: Dict[str, Any],
    ) -> bool:
        """
        Check if pipeline configuration has changed.

        Compares source config, target config, etc.
        """
        if not existing_pipeline:
            return False

        # Compare key configuration fields
        source_config = intent.get("source_config", {})

        # Check source type
        if existing_pipeline.source_type != source_config.get("source_type"):
            return True

        return False

    def _select_templates(
        self,
        source_type: str,
        processing_mode: str,
        cdc_enabled: bool,
        modeling_strategy: str,
    ) -> Dict[str, Any]:
        """
        Select templates based on source configuration.

        Uses Template Selection Matrix (AUTHORITATIVE):

        | Source   | Mode        | CDC | DAG Template         | Spark Templates              |
        |----------|-------------|-----|----------------------|------------------------------|
        | file     | batch       | No  | file_ingest_dag      | bronze, silver, gold_bq      |
        | file     | micro_batch | No  | streaming_ingest_dag | bronze, silver, gold_bq      |
        | database | batch       | No  | db_snapshot_dag      | bronze, silver, gold_bq      |
        | database | batch       | Yes | cdc_ingest_dag       | cdc_merge, scd2, gold_bq     |
        | streaming| streaming   | N/A | streaming_ingest_dag | streaming_bronze, gold_bq    |
        | api      | batch       | No  | api_ingest_dag       | bronze, silver, gold_bq      |
        """
        # DAG template selection
        if source_type == "file":
            dag_template = "streaming_ingest_dag" if processing_mode == "micro_batch" else "file_ingest_dag"
        elif source_type == "database":
            dag_template = "cdc_ingest_dag" if cdc_enabled else "db_snapshot_dag"
        elif source_type == "streaming":
            dag_template = "streaming_ingest_dag"
        elif source_type == "api":
            dag_template = "api_ingest_dag"
        else:
            raise TemplateSelectionError(f"Unknown source_type: {source_type}")

        # Spark templates selection
        if cdc_enabled:
            spark_templates = ["cdc_merge", "scd2_apply"]
        elif source_type == "streaming":
            spark_templates = ["streaming_bronze"]
        else:
            spark_templates = ["bronze_ingest", "silver_transform"]

        # Add gold layer based on modeling strategy
        if modeling_strategy == "dv2":
            spark_templates.extend(["dv2_hub", "dv2_satellite", "dv2_link"])
        elif modeling_strategy == "star":
            spark_templates.extend(["star_fact", "star_dimension"])

        # Always add BigQuery loader
        spark_templates.append("gold_load_bq")

        return {
            "dag_template": dag_template,
            "spark_templates": spark_templates,
            "source_type": source_type,
            "processing_mode": processing_mode,
            "cdc_enabled": cdc_enabled,
        }

    def _get_estimated_tasks(
        self,
        pipeline_action: str,
        template_selection: Dict[str, Any],
    ) -> List[str]:
        """Get list of estimated tasks based on action and templates."""
        if pipeline_action == "no_change":
            return ["verify_no_changes"]

        tasks = []

        # Generation tasks
        tasks.append("generate_dag")
        for template in template_selection.get("spark_templates", []):
            tasks.append(f"generate_{template}")
        tasks.append("generate_metadata_sql")

        # Validation tasks
        tasks.append("validate_dag_syntax")
        tasks.append("validate_spark_syntax")
        tasks.append("validate_schema_compatibility")
        tasks.append("validate_security")

        # Deployment tasks
        tasks.append("deploy_artifacts")

        return tasks
