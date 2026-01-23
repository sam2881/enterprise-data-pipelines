# Enterprise Agentic Data Engineering Platform - Claude Code Context

## 🎯 CLAUDE CODE DIRECTIVE: READ THIS FIRST

This document is the **AUTHORITATIVE CONTEXT** for implementing the Enterprise Agentic Data Engineering Platform. When generating code, creating files, or making architectural decisions, Claude Code MUST follow these specifications exactly.

---

## 📂 PROJECT FILE STRUCTURE (MANDATORY)

```
enterprise-agentic-data-platform/
├── README.md                           # Main documentation (full spec)
├── pyproject.toml                      # Python project configuration
├── requirements.txt                    # Python dependencies
│
├── src/
│   ├── __init__.py
│   │
│   ├── agents/                         # LangGraph Agent Implementations
│   │   ├── __init__.py
│   │   ├── base_agent.py              # Abstract base class for all agents
│   │   ├── supervisor_agent.py        # Orchestration agent
│   │   ├── planner_agent.py           # Intent parsing & strategy
│   │   ├── generator_agent.py         # Code generation agent
│   │   ├── validator_agent.py         # Validation agent
│   │   └── deployer_agent.py          # Git & CI/CD agent
│   │
│   ├── graphs/                         # LangGraph Graph Definitions
│   │   ├── __init__.py
│   │   ├── main_graph.py              # Primary workflow graph
│   │   ├── nodes.py                   # Node function implementations
│   │   └── edges.py                   # Conditional edge functions
│   │
│   ├── state/                          # State Management
│   │   ├── __init__.py
│   │   ├── pipeline_state.py          # Pydantic state models
│   │   ├── agent_state.py             # LangGraph state definitions
│   │   └── execution_context.py       # Runtime context
│   │
│   ├── templates/                      # Code Generation Templates
│   │   ├── __init__.py
│   │   ├── dag/                        # Airflow DAG templates
│   │   │   ├── base_dag.py.jinja2
│   │   │   ├── file_ingest_dag.py.jinja2
│   │   │   ├── cdc_ingest_dag.py.jinja2
│   │   │   ├── streaming_dag.py.jinja2
│   │   │   └── api_ingest_dag.py.jinja2
│   │   ├── spark/                      # PySpark job templates
│   │   │   ├── bronze_ingest.py.jinja2
│   │   │   ├── silver_transform.py.jinja2
│   │   │   ├── gold_load_bq.py.jinja2
│   │   │   ├── cdc_merge.py.jinja2
│   │   │   └── scd2_apply.py.jinja2
│   │   └── sql/                        # Metadata SQL templates
│   │       ├── insert_pipeline.sql.jinja2
│   │       ├── update_schema.sql.jinja2
│   │       └── insert_execution.sql.jinja2
│   │
│   ├── metadata/                       # Metadata Database Layer
│   │   ├── __init__.py
│   │   ├── repository.py              # PostgreSQL repository
│   │   ├── models.py                  # SQLAlchemy models
│   │   └── queries.py                 # Named queries
│   │
│   ├── validators/                     # Validation Logic
│   │   ├── __init__.py
│   │   ├── schema_validator.py        # JSON schema validation
│   │   ├── dag_validator.py           # DAG import testing
│   │   ├── sql_validator.py           # SQL syntax validation
│   │   └── security_validator.py      # Security rule checks
│   │
│   ├── generators/                     # Code Generators
│   │   ├── __init__.py
│   │   ├── dag_generator.py           # DAG code generation
│   │   ├── spark_generator.py         # PySpark code generation
│   │   └── metadata_generator.py      # SQL generation
│   │
│   ├── deployers/                      # Deployment Logic
│   │   ├── __init__.py
│   │   ├── git_client.py              # Git operations
│   │   ├── cicd_trigger.py            # Cloud Build trigger
│   │   └── composer_client.py         # Composer DAG sync
│   │
│   ├── integrations/                   # External Integrations
│   │   ├── __init__.py
│   │   ├── jira_client.py             # Jira API client
│   │   ├── pubsub_client.py           # Pub/Sub consumer/producer
│   │   └── secret_manager.py          # Secret Manager client
│   │
│   ├── config/                         # Configuration
│   │   ├── __init__.py
│   │   ├── settings.py                # Pydantic settings
│   │   └── environments/
│   │       ├── dev.yaml
│   │       ├── qa.yaml
│   │       └── prod.yaml
│   │
│   └── utils/                          # Utilities
│       ├── __init__.py
│       ├── logging.py                 # Structured logging
│       ├── exceptions.py              # Custom exceptions
│       └── helpers.py                 # Helper functions
│
├── prompts/                            # Agent System Prompts (Markdown)
│   ├── base.prompt.md
│   ├── supervisor.prompt.md
│   ├── planner.prompt.md
│   ├── generator.prompt.md
│   ├── validator.prompt.md
│   └── deployer.prompt.md
│
├── ddl/                                # Database DDL (PostgreSQL)
│   ├── 01_extensions.sql
│   ├── 02_pipeline.sql
│   ├── 03_schema_version.sql
│   ├── 04_transformation.sql
│   ├── 05_data_quality.sql
│   ├── 06_execution.sql
│   └── 07_audit.sql
│
├── tests/
│   ├── __init__.py
│   ├── unit/
│   ├── integration/
│   └── fixtures/
│
├── terraform/                          # Infrastructure as Code
│   ├── main.tf
│   ├── variables.tf
│   ├── modules/
│   │   ├── composer/
│   │   ├── cloudsql/
│   │   ├── bigquery/
│   │   └── pubsub/
│   └── environments/
│       ├── dev.tfvars
│       ├── qa.tfvars
│       └── prod.tfvars
│
└── deployment/                         # Generated Artifacts (Git-tracked)
    ├── dags/
    ├── spark_jobs/
    └── metadata_sql/
```

---

## 🏗️ ARCHITECTURE PRINCIPLES (NON-NEGOTIABLE)

### This Platform is a COMPILER, Not an ETL Tool

```
┌─────────────────────────────────────────────────────────────────────────┐
│  ❌ THIS PLATFORM DOES NOT:                                             │
│     • Parse free-text Jira stories                                     │
│     • Guess missing configuration values                               │
│     • Auto-fix validation errors                                       │
│     • Allow manual DAG creation                                        │
│                                                                         │
│  ✅ THIS PLATFORM DOES:                                                 │
│     • Accept ONLY validated JSON from UI                               │
│     • Generate code deterministically from metadata                    │
│     • Stop immediately on any validation failure                       │
│     • Version all changes immutably                                    │
└─────────────────────────────────────────────────────────────────────────┘
```

### Agent Framework: LangGraph (REQUIRED)

**DO NOT USE:**
- ❌ Plain LangChain Agents
- ❌ ReAct pattern
- ❌ Tool-reactive agents
- ❌ Chat-based memory

**MUST USE:**
- ✅ LangGraph StateGraph
- ✅ Explicit state objects (Pydantic)
- ✅ Conditional edges
- ✅ Human-in-the-loop checkpoints

---

## 🔧 CODE TEMPLATES AND PATTERNS

### 1. LangGraph State Definition (ALWAYS USE THIS PATTERN)

```python
# src/state/pipeline_state.py
from typing import TypedDict, Annotated, List, Optional, Literal
from pydantic import BaseModel, Field
from datetime import datetime
import uuid

class IntentSchema(BaseModel):
    """Validated intent from UI - IMMUTABLE after creation."""
    intent_version: str = "1.0.0"
    created_at: datetime
    created_by: str
    jira_ticket: str
    
    pipeline_identity: dict
    source_config: dict
    schema_definition: dict
    transformation_rules: Optional[dict] = None
    data_quality_rules: Optional[List[dict]] = None
    target_config: dict
    execution_policy: dict

class SchemaChange(BaseModel):
    """Detected schema change."""
    change_type: Literal["added", "removed", "modified"]
    column_name: str
    old_value: Optional[str] = None
    new_value: Optional[str] = None

class PlannerOutput(BaseModel):
    """Output from Planner Agent."""
    plan_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    pipeline_action: Literal["create", "modify", "upgrade_schema", "no_change"]
    pipeline_id: Optional[int] = None
    is_new_pipeline: bool
    
    schema_plan: dict
    template_selection: dict
    estimated_tasks: List[str]

class GeneratorOutput(BaseModel):
    """Output from Generator Agent."""
    dag_code: str
    spark_jobs: dict  # {job_name: code}
    metadata_sql: List[str]
    artifact_paths: dict

class ValidatorOutput(BaseModel):
    """Output from Validator Agent."""
    is_valid: bool
    dag_import_success: bool
    sql_syntax_valid: bool
    schema_compatible: bool
    security_passed: bool
    errors: List[str] = []
    warnings: List[str] = []

class DeployerOutput(BaseModel):
    """Output from Deployer Agent."""
    branch_name: str
    commit_sha: str
    pr_url: Optional[str] = None
    cicd_build_id: str
    deployment_status: Literal["pending", "in_progress", "success", "failed"]

# LangGraph State - ALWAYS use TypedDict for LangGraph
class AgentState(TypedDict):
    """Main state object passed through LangGraph nodes."""
    # Request identification
    request_id: str
    
    # Input
    intent_json: dict
    
    # Workflow tracking
    current_phase: Literal[
        "init", 
        "planning", 
        "generating", 
        "validating", 
        "awaiting_approval",
        "deploying", 
        "complete", 
        "failed"
    ]
    
    # Agent outputs
    planner_output: Optional[dict]
    generator_output: Optional[dict]
    validator_output: Optional[dict]
    deployer_output: Optional[dict]
    
    # Control flow
    human_approval_required: bool
    human_approval_received: bool
    
    # Error handling
    error_message: Optional[str]
    error_agent: Optional[str]
    
    # Metadata context (from PostgreSQL)
    metadata_context: dict
    
    # Timestamps
    started_at: str
    completed_at: Optional[str]
```

### 2. LangGraph Graph Definition (MANDATORY PATTERN)

```python
# src/graphs/main_graph.py
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.postgres import PostgresSaver
from src.state.pipeline_state import AgentState
from src.graphs.nodes import (
    validate_intent_node,
    plan_pipeline_node,
    generate_artifacts_node,
    validate_artifacts_node,
    wait_for_approval_node,
    deploy_artifacts_node,
    handle_error_node
)
from src.graphs.edges import (
    should_continue_after_planning,
    should_continue_after_generation,
    should_continue_after_validation,
    needs_human_approval,
    should_continue_after_approval
)

def create_pipeline_graph() -> StateGraph:
    """
    Create the main pipeline generation workflow graph.
    
    Flow:
    START → VALIDATE_INTENT → PLAN → GENERATE → VALIDATE → [APPROVAL] → DEPLOY → END
    
    All failures route to ERROR handler.
    """
    # Initialize graph with state schema
    workflow = StateGraph(AgentState)
    
    # Add nodes (each node is a function that takes state, returns partial state)
    workflow.add_node("validate_intent", validate_intent_node)
    workflow.add_node("plan_pipeline", plan_pipeline_node)
    workflow.add_node("generate_artifacts", generate_artifacts_node)
    workflow.add_node("validate_artifacts", validate_artifacts_node)
    workflow.add_node("wait_for_approval", wait_for_approval_node)
    workflow.add_node("deploy_artifacts", deploy_artifacts_node)
    workflow.add_node("handle_error", handle_error_node)
    
    # Set entry point
    workflow.set_entry_point("validate_intent")
    
    # Add edges
    workflow.add_conditional_edges(
        "validate_intent",
        lambda state: "plan_pipeline" if not state.get("error_message") else "handle_error",
        {
            "plan_pipeline": "plan_pipeline",
            "handle_error": "handle_error"
        }
    )
    
    workflow.add_conditional_edges(
        "plan_pipeline",
        should_continue_after_planning,
        {
            "generate": "generate_artifacts",
            "no_change": END,
            "error": "handle_error"
        }
    )
    
    workflow.add_conditional_edges(
        "generate_artifacts",
        should_continue_after_generation,
        {
            "validate": "validate_artifacts",
            "error": "handle_error"
        }
    )
    
    workflow.add_conditional_edges(
        "validate_artifacts",
        should_continue_after_validation,
        {
            "check_approval": "wait_for_approval",
            "deploy": "deploy_artifacts",
            "error": "handle_error"
        }
    )
    
    workflow.add_conditional_edges(
        "wait_for_approval",
        should_continue_after_approval,
        {
            "deploy": "deploy_artifacts",
            "timeout": "handle_error",
            "rejected": "handle_error"
        }
    )
    
    workflow.add_edge("deploy_artifacts", END)
    workflow.add_edge("handle_error", END)
    
    return workflow.compile()


def create_graph_with_checkpointing(connection_string: str) -> StateGraph:
    """Create graph with PostgreSQL checkpointing for durability."""
    workflow = create_pipeline_graph()
    
    # Add checkpointer for state persistence
    checkpointer = PostgresSaver.from_conn_string(connection_string)
    
    return workflow.compile(checkpointer=checkpointer)
```

### 3. Node Implementation Pattern (ALWAYS FOLLOW)

```python
# src/graphs/nodes.py
from typing import Dict, Any
from src.state.pipeline_state import AgentState, PlannerOutput
from src.metadata.repository import MetadataRepository
from src.utils.logging import get_logger
from src.utils.exceptions import PlanningError
import traceback

logger = get_logger(__name__)

def plan_pipeline_node(state: AgentState) -> Dict[str, Any]:
    """
    Planner Agent Node: Analyze intent and determine execution strategy.
    
    RULES:
    1. Query metadata FIRST - always check existing pipelines
    2. Detect schema changes by comparison
    3. Select templates based on source_type + processing_mode
    4. NEVER guess or infer missing values
    5. Return partial state update only
    
    Args:
        state: Current workflow state
        
    Returns:
        Partial state update dict
    """
    logger.info(f"Planning pipeline for request: {state['request_id']}")
    
    try:
        intent = state["intent_json"]
        repo = MetadataRepository()
        
        # Step 1: Check if pipeline exists
        pipeline_name = intent["pipeline_identity"]["pipeline_name"]
        environment = intent["pipeline_identity"]["environment"]
        
        existing_pipeline = repo.get_pipeline_by_name(
            pipeline_name=pipeline_name,
            environment=environment
        )
        
        # Step 2: Determine action
        if existing_pipeline is None:
            pipeline_action = "create"
            is_new = True
            pipeline_id = None
        else:
            pipeline_id = existing_pipeline.pipeline_id
            is_new = False
            
            # Check for schema changes
            current_schema = repo.get_current_schema(pipeline_id)
            new_schema = intent["schema_definition"]
            
            schema_changes = _compare_schemas(current_schema, new_schema)
            
            if schema_changes:
                pipeline_action = "upgrade_schema"
            elif _config_changed(existing_pipeline, intent):
                pipeline_action = "modify"
            else:
                pipeline_action = "no_change"
        
        # Step 3: Select templates
        template_selection = _select_templates(
            source_type=intent["source_config"]["source_type"],
            processing_mode=intent["source_config"].get("processing_mode", "batch"),
            cdc_enabled=intent["source_config"].get("cdc_enabled", False),
            modeling_strategy=intent.get("modeling_strategy", "none")
        )
        
        # Step 4: Build plan output
        planner_output = PlannerOutput(
            pipeline_action=pipeline_action,
            pipeline_id=pipeline_id,
            is_new_pipeline=is_new,
            schema_plan={
                "action": "create" if is_new else ("upgrade" if pipeline_action == "upgrade_schema" else "none"),
                "new_version": (current_schema.version + 1) if not is_new and pipeline_action == "upgrade_schema" else 1,
                "changes": schema_changes if not is_new else []
            },
            template_selection=template_selection,
            estimated_tasks=[
                "generate_dag",
                "generate_bronze_spark",
                "generate_silver_spark",
                "generate_metadata_sql",
                "validate_all",
                "deploy"
            ]
        )
        
        logger.info(f"Planning complete: action={pipeline_action}")
        
        return {
            "current_phase": "planning",
            "planner_output": planner_output.model_dump(),
            "metadata_context": {
                "pipeline_id": pipeline_id,
                "is_new": is_new,
                "existing_schema_version": current_schema.version if not is_new else None
            }
        }
        
    except Exception as e:
        logger.error(f"Planning failed: {str(e)}\n{traceback.format_exc()}")
        return {
            "current_phase": "failed",
            "error_message": f"Planning failed: {str(e)}",
            "error_agent": "planner"
        }


def _select_templates(
    source_type: str,
    processing_mode: str,
    cdc_enabled: bool,
    modeling_strategy: str
) -> dict:
    """
    Select templates based on source configuration.
    
    Template Selection Matrix (AUTHORITATIVE):
    
    | Source Type | Processing Mode | CDC | DAG Template          | Spark Templates                    |
    |-------------|-----------------|-----|----------------------|-----------------------------------|
    | file        | batch           | No  | file_ingest_dag      | bronze_ingest, silver_transform   |
    | file        | micro_batch     | No  | streaming_ingest_dag | bronze_ingest, silver_transform   |
    | database    | batch           | No  | db_snapshot_dag      | bronze_ingest, silver_transform   |
    | database    | batch           | Yes | cdc_ingest_dag       | cdc_merge, scd2_apply             |
    | streaming   | streaming       | N/A | streaming_ingest_dag | streaming_bronze                  |
    | api         | batch           | No  | api_ingest_dag       | bronze_ingest, silver_transform   |
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
        raise ValueError(f"Unknown source_type: {source_type}")
    
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
        "spark_templates": spark_templates
    }
```

### 4. Conditional Edge Functions (MANDATORY PATTERN)

```python
# src/graphs/edges.py
from typing import Literal
from src.state.pipeline_state import AgentState

def should_continue_after_planning(state: AgentState) -> Literal["generate", "no_change", "error"]:
    """
    Determine next step after planning.
    
    RULES:
    - If error occurred, route to error handler
    - If no changes needed, end workflow
    - Otherwise, proceed to generation
    """
    if state.get("error_message"):
        return "error"
    
    planner_output = state.get("planner_output", {})
    if planner_output.get("pipeline_action") == "no_change":
        return "no_change"
    
    return "generate"


def should_continue_after_validation(state: AgentState) -> Literal["check_approval", "deploy", "error"]:
    """
    Determine next step after validation.
    
    RULES:
    - If validation failed, route to error handler
    - If PROD environment OR schema change, require approval
    - Otherwise, proceed directly to deployment
    """
    if state.get("error_message"):
        return "error"
    
    validator_output = state.get("validator_output", {})
    if not validator_output.get("is_valid", False):
        return "error"
    
    # Check if approval is required
    intent = state["intent_json"]
    environment = intent["pipeline_identity"]["environment"]
    planner_output = state.get("planner_output", {})
    
    needs_approval = (
        environment == "prod" or
        planner_output.get("pipeline_action") == "upgrade_schema" or
        intent.get("execution_policy", {}).get("human_approval_required", False)
    )
    
    if needs_approval:
        return "check_approval"
    
    return "deploy"


def should_continue_after_approval(state: AgentState) -> Literal["deploy", "timeout", "rejected"]:
    """
    Determine next step after human approval checkpoint.
    
    RULES:
    - If approved, proceed to deployment
    - If rejected, stop with rejection error
    - If timeout, stop with timeout error
    """
    if state.get("human_approval_received"):
        return "deploy"
    
    # Check for timeout or rejection
    if state.get("error_message", "").startswith("Approval timeout"):
        return "timeout"
    
    if state.get("error_message", "").startswith("Approval rejected"):
        return "rejected"
    
    # Default to waiting (shouldn't reach here in normal flow)
    return "timeout"
```

### 5. Agent Base Class (USE FOR ALL AGENTS)

```python
# src/agents/base_agent.py
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from pydantic import BaseModel
from src.utils.logging import get_logger
from src.metadata.repository import MetadataRepository
import uuid
from datetime import datetime

class AgentAuditLog(BaseModel):
    """Audit log entry for agent actions."""
    log_id: str
    agent_name: str
    action: str
    pipeline_id: Optional[int]
    input_state: dict
    output_state: dict
    decision_reasoning: str
    duration_ms: int
    status: str

class BaseAgent(ABC):
    """
    Abstract base class for all agents.
    
    ALL agents MUST:
    1. Inherit from this class
    2. Implement execute() method
    3. Log all decisions to audit table
    4. Handle errors gracefully
    5. Return partial state updates only
    """
    
    def __init__(self, agent_name: str):
        self.agent_name = agent_name
        self.logger = get_logger(f"agent.{agent_name}")
        self.repo = MetadataRepository()
    
    @abstractmethod
    def execute(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute agent logic.
        
        Args:
            state: Current workflow state
            
        Returns:
            Partial state update dict
        """
        pass
    
    def log_audit(
        self,
        action: str,
        pipeline_id: Optional[int],
        input_state: dict,
        output_state: dict,
        reasoning: str,
        duration_ms: int,
        status: str
    ) -> None:
        """Log agent action to audit table."""
        audit_entry = AgentAuditLog(
            log_id=str(uuid.uuid4()),
            agent_name=self.agent_name,
            action=action,
            pipeline_id=pipeline_id,
            input_state=self._sanitize_state(input_state),
            output_state=self._sanitize_state(output_state),
            decision_reasoning=reasoning,
            duration_ms=duration_ms,
            status=status
        )
        
        self.repo.insert_audit_log(audit_entry)
        self.logger.info(f"Audit logged: {action} - {status}")
    
    def _sanitize_state(self, state: dict) -> dict:
        """Remove sensitive data before logging."""
        # Remove any secrets or credentials
        sanitized = state.copy()
        sensitive_keys = ["password", "secret", "token", "key", "credential"]
        
        def remove_sensitive(obj, path=""):
            if isinstance(obj, dict):
                return {
                    k: "***REDACTED***" if any(s in k.lower() for s in sensitive_keys) 
                    else remove_sensitive(v, f"{path}.{k}")
                    for k, v in obj.items()
                }
            elif isinstance(obj, list):
                return [remove_sensitive(item, path) for item in obj]
            return obj
        
        return remove_sensitive(sanitized)
```

### 6. Code Generator Pattern (FOLLOW FOR ALL GENERATORS)

```python
# src/generators/dag_generator.py
from typing import Dict, Any
from jinja2 import Environment, FileSystemLoader, StrictUndefined
from pathlib import Path
from src.utils.logging import get_logger

logger = get_logger(__name__)

class DAGGenerator:
    """
    Generate Airflow DAGs from templates.
    
    RULES:
    1. NEVER hard-code business logic - all config from metadata
    2. Use Jinja2 templates ONLY
    3. Templates are FROZEN - no runtime modifications
    4. All generated code must be deterministic
    """
    
    TEMPLATE_DIR = Path(__file__).parent.parent / "templates" / "dag"
    
    def __init__(self):
        self.env = Environment(
            loader=FileSystemLoader(self.TEMPLATE_DIR),
            undefined=StrictUndefined,  # Fail on undefined variables
            trim_blocks=True,
            lstrip_blocks=True
        )
    
    def generate(
        self,
        template_name: str,
        context: Dict[str, Any]
    ) -> str:
        """
        Generate DAG code from template.
        
        Args:
            template_name: Name of template (without .jinja2)
            context: Template variables
            
        Returns:
            Generated Python code as string
            
        Raises:
            ValueError: If required context variables are missing
        """
        # Validate required context
        required_vars = self._get_required_vars(template_name)
        missing = [v for v in required_vars if v not in context]
        if missing:
            raise ValueError(f"Missing required template variables: {missing}")
        
        # Load and render template
        template = self.env.get_template(f"{template_name}.py.jinja2")
        code = template.render(**context)
        
        logger.info(f"Generated DAG: {context.get('dag_id', 'unknown')}")
        return code
    
    def _get_required_vars(self, template_name: str) -> list:
        """Get required variables for a template."""
        # Template-specific requirements
        requirements = {
            "file_ingest_dag": [
                "dag_id", "pipeline_name", "domain", "schedule",
                "landing_path", "file_pattern", "bronze_table",
                "silver_table", "spark_job_paths"
            ],
            "cdc_ingest_dag": [
                "dag_id", "pipeline_name", "domain", "schedule",
                "cdc_topic", "bronze_table", "silver_table",
                "merge_keys", "spark_job_paths"
            ],
            "streaming_ingest_dag": [
                "dag_id", "pipeline_name", "domain",
                "kafka_topic", "bronze_table", "spark_job_paths"
            ],
            "api_ingest_dag": [
                "dag_id", "pipeline_name", "domain", "schedule",
                "api_endpoint", "bronze_table", "silver_table",
                "spark_job_paths"
            ]
        }
        return requirements.get(template_name, [])
```

### 7. Metadata Repository Pattern (ALWAYS USE)

```python
# src/metadata/repository.py
from typing import Optional, List
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from contextlib import contextmanager
from src.config.settings import get_settings
from src.metadata.models import Pipeline, SchemaVersion, ExecutionPolicy
from src.utils.logging import get_logger

logger = get_logger(__name__)

class MetadataRepository:
    """
    PostgreSQL metadata repository.
    
    RULES:
    1. All database access goes through this class
    2. Use context managers for sessions
    3. All queries must be parameterized (no string concatenation)
    4. Log all database operations
    """
    
    def __init__(self):
        settings = get_settings()
        self.engine = create_engine(
            settings.database_url,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10
        )
        self.SessionLocal = sessionmaker(bind=self.engine)
    
    @contextmanager
    def get_session(self) -> Session:
        """Get database session with automatic cleanup."""
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
    
    def get_pipeline_by_name(
        self,
        pipeline_name: str,
        environment: str
    ) -> Optional[Pipeline]:
        """
        Get pipeline by name and environment.
        
        Args:
            pipeline_name: Unique pipeline identifier
            environment: Target environment (dev/qa/prod)
            
        Returns:
            Pipeline object or None if not found
        """
        with self.get_session() as session:
            result = session.execute(
                text("""
                    SELECT * FROM pipeline 
                    WHERE pipeline_name = :name 
                    AND environment = :env
                    AND is_active = TRUE
                """),
                {"name": pipeline_name, "env": environment}
            )
            row = result.fetchone()
            
            if row:
                logger.debug(f"Found pipeline: {pipeline_name}")
                return Pipeline(**dict(row._mapping))
            
            logger.debug(f"Pipeline not found: {pipeline_name}")
            return None
    
    def get_current_schema(self, pipeline_id: int) -> Optional[SchemaVersion]:
        """Get current schema version for a pipeline."""
        with self.get_session() as session:
            result = session.execute(
                text("""
                    SELECT * FROM schema_version
                    WHERE pipeline_id = :pid
                    AND is_current = TRUE
                """),
                {"pid": pipeline_id}
            )
            row = result.fetchone()
            
            if row:
                return SchemaVersion(**dict(row._mapping))
            return None
    
    def insert_pipeline(self, pipeline_data: dict) -> int:
        """
        Insert new pipeline and return ID.
        
        IMPORTANT: This creates the initial record only.
        Schema, transforms, and policies are inserted separately.
        """
        with self.get_session() as session:
            result = session.execute(
                text("""
                    INSERT INTO pipeline (
                        pipeline_name, domain, source_type, source_system,
                        environment, data_sensitivity, business_owner,
                        technical_owner, is_active, created_by
                    ) VALUES (
                        :pipeline_name, :domain, :source_type, :source_system,
                        :environment, :data_sensitivity, :business_owner,
                        :technical_owner, TRUE, :created_by
                    )
                    RETURNING pipeline_id
                """),
                pipeline_data
            )
            pipeline_id = result.fetchone()[0]
            logger.info(f"Created pipeline: {pipeline_id}")
            return pipeline_id
```

---

## 📝 NAMING CONVENTIONS (MANDATORY)

### Python Files
```
# Modules: snake_case
pipeline_state.py
dag_generator.py
metadata_repository.py

# Classes: PascalCase
class PipelineState:
class DAGGenerator:
class MetadataRepository:

# Functions: snake_case
def generate_dag():
def validate_schema():
def deploy_artifacts():

# Constants: UPPER_SNAKE_CASE
MAX_RETRY_COUNT = 3
DEFAULT_TIMEOUT_SECONDS = 3600
```

### Database
```sql
-- Tables: snake_case, singular
pipeline
schema_version
execution_policy

-- Columns: snake_case
pipeline_id
created_at
is_active

-- Indexes: idx_{table}_{columns}
idx_pipeline_name_env
idx_execution_status

-- Foreign keys: fk_{table}_{ref_table}
fk_schema_version_pipeline
```

### Generated Artifacts
```
# DAGs: {domain}_{pipeline_name}_dag.py
sales_customer_orders_dag.py
finance_gl_transactions_dag.py

# Spark jobs: {pipeline_name}_{layer}.py
customer_orders_bronze.py
customer_orders_silver.py
customer_orders_gold_bq.py

# Metadata SQL: {pipeline_name}_{operation}.sql
customer_orders_insert.sql
customer_orders_schema_v2.sql
```

---

## ⚠️ WHEN GENERATING X, ALWAYS INCLUDE Y

### When generating a LangGraph node:
```python
# ALWAYS include:
1. Type hints for state parameter and return
2. Try/except with proper error state return
3. Logging at start and end
4. Audit log entry
5. Return ONLY partial state update (not full state)
```

### When generating a DAG:
```python
# ALWAYS include:
1. dag_id matching pattern: {domain}_{pipeline_name}
2. default_args with owner, retries, retry_delay
3. tags for filtering
4. doc_md for documentation
5. on_failure_callback for alerting
6. sla parameter if defined in metadata
```

### When generating a Spark job:
```python
# ALWAYS include:
1. SparkSession builder with app name
2. Logging configuration
3. Schema definition (never infer)
4. Write mode explicitly set
5. Checkpointing for streaming
6. Graceful shutdown handling
```

### When generating metadata SQL:
```sql
-- ALWAYS include:
1. Transaction wrapper (BEGIN/COMMIT)
2. created_at and created_by
3. version number increment
4. is_current flag management
5. effective_from/effective_to for SCD
```

### When adding a new agent:
```python
# ALWAYS include:
1. Inherit from BaseAgent
2. Define clear input/output contracts
3. Add to graph with proper edges
4. Add to supervisor routing
5. Create system prompt in prompts/
6. Add unit tests
```

---

## 🚫 ANTI-PATTERNS (NEVER DO THESE)

```python
# ❌ NEVER: Parse free-text from Jira
intent = jira_client.get_issue_description()  # WRONG
llm.parse(intent)  # WRONG

# ✅ ALWAYS: Accept validated JSON only
intent = pubsub_client.receive_intent()  # Already validated by UI

# ❌ NEVER: Use implicit LLM memory
agent.remember(previous_conversation)  # WRONG

# ✅ ALWAYS: Use explicit state
state["metadata_context"] = repo.get_pipeline(id)  # Queryable, versioned

# ❌ NEVER: Auto-fix validation errors
if not valid:
    auto_fix(schema)  # WRONG

# ✅ ALWAYS: Fail fast and report
if not valid:
    return {"error_message": "Schema validation failed", "errors": errors}

# ❌ NEVER: Hard-code business logic
if domain == "finance":
    apply_finance_rules()  # WRONG

# ✅ ALWAYS: Read from metadata
rules = metadata["transformation_rules"]
apply_rules(rules)

# ❌ NEVER: Skip human approval for PROD
if env == "prod":
    deploy()  # WRONG without approval

# ✅ ALWAYS: Enforce approval gates
if env == "prod":
    wait_for_human_approval()
    if approved:
        deploy()

# ❌ NEVER: Use ReAct pattern
agent = ReActAgent(tools=[...])  # WRONG for this platform

# ✅ ALWAYS: Use LangGraph compiler pattern
graph = StateGraph(AgentState)
graph.add_node("planner", plan_node)
graph.add_node("generator", generate_node)
```

---

## 🔐 MEMORY STRATEGY (CRITICAL)

### Types of Memory in This Platform

| Memory Type | Storage | Purpose | Lifetime |
|------------|---------|---------|----------|
| **Execution State** | LangGraph State | Control flow, decisions | Single request |
| **System of Record** | PostgreSQL | Pipeline metadata, schema versions | Permanent |
| **Artifact History** | Git | DAGs, Spark jobs, SQL | Permanent |
| **Semantic Memory** | Vector DB (optional) | Similar pipeline detection | Optional |

### What Memory to Use When

```python
# For control flow decisions:
state["current_phase"]  # LangGraph state

# For pipeline metadata:
repo.get_pipeline(id)  # PostgreSQL

# For historical artifacts:
git.get_file_history(path)  # Git

# For similarity search (OPTIONAL, never for decisions):
vector_store.search_similar(embedding)  # Only for suggestions
```

### What Memory to NEVER Use

```python
# ❌ Chat history as memory
# ❌ Free-text summaries
# ❌ Implicit LLM recall
# ❌ Hidden tool state
# ❌ Session cookies
# ❌ In-memory caches for decisions
```

---

## 📊 TEMPLATE SELECTION MATRIX (AUTHORITATIVE)

| Source Type | Processing Mode | CDC | DAG Template | Spark Templates |
|------------|-----------------|-----|--------------|-----------------|
| file | batch | No | file_ingest_dag | bronze_ingest, silver_transform, gold_load_bq |
| file | micro_batch | No | streaming_ingest_dag | bronze_ingest, silver_transform, gold_load_bq |
| database | batch | No | db_snapshot_dag | bronze_ingest, silver_transform, gold_load_bq |
| database | batch | Yes | cdc_ingest_dag | cdc_merge, scd2_apply, gold_load_bq |
| streaming | streaming | N/A | streaming_ingest_dag | streaming_bronze, gold_load_bq |
| api | batch | No | api_ingest_dag | bronze_ingest, silver_transform, gold_load_bq |

---

## 🧪 TESTING REQUIREMENTS

### Every Agent Must Have:
```python
# tests/unit/agents/test_planner_agent.py

def test_new_pipeline_detection():
    """New pipeline should return action='create'."""
    
def test_schema_change_detection():
    """Schema change should return action='upgrade_schema'."""
    
def test_template_selection_file_batch():
    """File + batch should select file_ingest_dag."""
    
def test_template_selection_cdc():
    """Database + CDC should select cdc_ingest_dag."""
    
def test_error_handling():
    """Database errors should return error state."""
```

### Every Generator Must Have:
```python
# tests/unit/generators/test_dag_generator.py

def test_generates_valid_python():
    """Generated code must be valid Python."""
    
def test_all_required_vars_present():
    """All template variables must be provided."""
    
def test_deterministic_output():
    """Same input must produce same output."""
```

---

## 🚀 QUICK START FOR CLAUDE CODE

When starting implementation, follow this order:

1. **Create state models first** (`src/state/`)
2. **Create metadata repository** (`src/metadata/`)
3. **Create base agent class** (`src/agents/base_agent.py`)
4. **Create individual agents** (`src/agents/`)
5. **Create graph definition** (`src/graphs/main_graph.py`)
6. **Create templates** (`src/templates/`)
7. **Create generators** (`src/generators/`)
8. **Wire everything together**

---

## 📋 CHECKLIST FOR CODE REVIEW

Before committing any code, verify:

- [ ] Uses LangGraph (not plain LangChain)
- [ ] State is explicit Pydantic/TypedDict
- [ ] No free-text parsing
- [ ] No auto-fixing errors
- [ ] Audit logging present
- [ ] Error handling returns proper state
- [ ] Templates used (no hard-coded logic)
- [ ] Naming conventions followed
- [ ] Tests written
- [ ] PROD requires human approval

---

**END OF CLAUDE CODE CONTEXT**

*This document is the authoritative reference for Claude Code when implementing the Enterprise Agentic Data Engineering Platform. Follow it exactly.*