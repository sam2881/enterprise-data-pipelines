"""
Kafka Event Schemas v5.0

WHY: Defines all Kafka topics and message schemas for the platform:
- Incident lifecycle events (created → closed)
- Plan and approval events
- Execution events
- Audit trail for compliance

HOW: Uses Pydantic for schema validation and JSON serialization.

Topics:
    incident.created    - New incident from ServiceNow
    incident.enriched   - After RAG and classification
    plan.generated      - Remediation plan created
    plan.judged         - After LLM-as-Judge evaluation
    incident.approved   - Plan approved for execution
    incident.rejected   - Plan rejected
    incident.executed   - Execution completed
    incident.verified   - Fix verified
    incident.closed     - Ticket closed
    incident.failed     - Workflow failed
"""
from typing import Any, Dict, List, Optional
from datetime import datetime
from enum import Enum
from pydantic import BaseModel, Field
import json
import uuid


# =============================================================================
# TOPIC DEFINITIONS
# =============================================================================

class Topics:
    """
    Kafka topic names - SINGLE SOURCE OF TRUTH

    Architecture: Kafka is the system of record. All state transitions
    flow through these topics. MCPs poll external systems and publish here.
    LangGraph workflows consume and publish here.

    Naming: {domain}.{event_type} or {domain}.{action}
    """
    # =========================================================================
    # INCIDENT LIFECYCLE (ServiceNow → LangGraph → GitHub Actions → Close)
    # =========================================================================

    # State transition events (LangGraph publishes these)
    INCIDENT_CREATED = "incident.created"           # MCP detects new incident
    INCIDENT_RECEIVED = "incident.received"         # LangGraph started processing
    INCIDENT_ENRICHED = "incident.enriched"         # After RAG enrichment
    INCIDENT_PLAN_GENERATED = "incident.plan_generated"  # After LLM planning
    INCIDENT_REQUIRES_APPROVAL = "incident.requires_approval"  # Pending human
    INCIDENT_APPROVED = "incident.approved"         # Human approved
    INCIDENT_REJECTED = "incident.rejected"         # Human rejected
    INCIDENT_EXECUTED = "incident.executed"         # GitHub Actions complete
    INCIDENT_VERIFIED = "incident.verified"         # Post-execution verified
    INCIDENT_CLOSE_REQUESTED = "incident.close_requested"  # Human clicked close
    INCIDENT_CLOSE_EXECUTE = "incident.close_execute"  # Orchestrator approved
    INCIDENT_CLOSED = "incident.closed"             # ServiceNow ticket closed
    INCIDENT_FAILED = "incident.failed"             # Workflow failed

    # Legacy alias
    INCIDENT_ENRICHED_LEGACY = "incident.enriched"

    # Plan events (separate namespace for clarity)
    PLAN_GENERATED = "plan.generated"
    PLAN_JUDGED = "plan.judged"

    # Remediation events
    REMEDIATION_STARTED = "remediation.started"
    REMEDIATION_EXECUTED = "remediation.executed"
    REMEDIATION_FAILED = "remediation.failed"
    REMEDIATION_ROLLBACK = "remediation.rollback"

    # =========================================================================
    # DATA PIPELINE LIFECYCLE (Jira/UI → LangGraph → Airflow → Complete)
    # =========================================================================

    # State transition events (Data Agent LangGraph publishes these)
    PIPELINE_REQUESTED = "pipeline.requested"       # New request from Jira/UI
    PIPELINE_PLANNED = "pipeline.planned"           # Planner agent complete
    PIPELINE_GENERATED = "pipeline.generated"       # Generator agent complete
    PIPELINE_VALIDATED = "pipeline.validated"       # Validator agent passed
    PIPELINE_REQUIRES_APPROVAL = "pipeline.requires_approval"  # Pending human
    PIPELINE_APPROVED = "pipeline.approved"         # Human approved
    PIPELINE_REJECTED = "pipeline.rejected"         # Human rejected
    PIPELINE_DEPLOY_EXECUTE = "pipeline.deploy_execute"  # Ready for deployment
    PIPELINE_DEPLOYED = "pipeline.deployed"         # Deployment completed
    PIPELINE_FAILED = "pipeline.failed"             # Any stage failed

    # Legacy aliases for backward compatibility
    PIPELINE_INTENTS = "pipeline.intents"
    PIPELINE_STATUS = "pipeline.status"
    PIPELINE_PLANNING = "pipeline.planning"
    PIPELINE_GENERATING = "pipeline.generating"
    PIPELINE_VALIDATING = "pipeline.validating"
    PIPELINE_AWAITING_APPROVAL = "pipeline.awaiting_approval"
    PIPELINE_DEPLOYING = "pipeline.deploying"
    PIPELINE_COMPLETED = "pipeline.completed"
    PIPELINE_MR_CREATED = "pipeline.mr.created"

    # =========================================================================
    # EXTERNAL SYSTEM INTEGRATION (MCP servers publish to these)
    # =========================================================================

    # ServiceNow MCP publishes normalized incidents here
    SERVICENOW_INCIDENTS = "servicenow.incidents"
    SERVICENOW_UPDATES = "servicenow.updates"

    # Jira MCP publishes pipeline requests here
    JIRA_TICKETS = "jira.tickets"
    JIRA_UPDATES = "jira.updates"

    # GCP monitoring alerts
    GCP_ALERTS = "gcp.alerts"

    # Agent coordination events
    AGENT_EVENTS = "agent.events"

    # =========================================================================
    # MCP COMMAND TOPICS (FastAPI/Orchestrator publish commands here)
    # =========================================================================

    # Commands for MCPs to execute (consumer-side)
    MCP_SERVICENOW_COMMANDS = "mcp.servicenow.commands"  # close ticket, update
    MCP_GITHUB_COMMANDS = "mcp.github.commands"          # trigger workflow
    MCP_AIRFLOW_COMMANDS = "mcp.airflow.commands"        # trigger DAG

    # Legacy MCP topics (kept for backward compatibility)
    MCP_SERVICENOW_REQUESTS = "mcp.servicenow.requests"
    MCP_SERVICENOW_RESPONSES = "mcp.servicenow.responses"
    MCP_GITHUB_REQUESTS = "mcp.github.requests"
    MCP_GITHUB_RESPONSES = "mcp.github.responses"

    # =========================================================================
    # AIRFLOW MCP TOPICS (DAG Orchestration via MCP - not direct REST API)
    # =========================================================================

    # LangGraph publishes to trigger DAG operations
    AIRFLOW_TRIGGER_DAG = "airflow.trigger_dag"
    AIRFLOW_RETRY_DAG = "airflow.retry_dag"
    AIRFLOW_GET_STATUS = "airflow.get_dag_status"

    # Airflow MCP publishes results
    AIRFLOW_DAG_COMPLETED = "airflow.dag_completed"
    AIRFLOW_DAG_FAILED = "airflow.dag_failed"

    # =========================================================================
    # GITHUB MCP TOPICS (Workflow Dispatch for Remediation Scripts)
    # =========================================================================
    # Used to trigger GitHub Actions workflows in test_01 repo for:
    # - Terraform scripts (GCP VM operations)
    # - Ansible playbooks (infrastructure fixes)
    # - Shell scripts (remediation commands)

    # LangGraph publishes to trigger workflow operations
    GITHUB_TRIGGER_WORKFLOW = "github.trigger_workflow"
    GITHUB_GET_WORKFLOW_STATUS = "github.get_workflow_status"

    # GitHub MCP publishes results
    GITHUB_WORKFLOW_COMPLETED = "github.workflow_completed"
    GITHUB_WORKFLOW_FAILED = "github.workflow_failed"

    # Pipeline deployment (enterprise-data-pipelines repo)
    GITHUB_COMMIT_FILE = "github.commit_file"
    GITHUB_CREATE_PR = "github.create_pr"

    @classmethod
    def all(cls) -> List[str]:
        """Get all active topic names"""
        return [
            # Incident lifecycle
            cls.INCIDENT_CREATED,
            cls.INCIDENT_ENRICHED,
            cls.INCIDENT_PLAN_GENERATED,
            cls.INCIDENT_REQUIRES_APPROVAL,
            cls.INCIDENT_APPROVED,
            cls.INCIDENT_REJECTED,
            cls.INCIDENT_EXECUTED,
            cls.INCIDENT_VERIFIED,
            cls.INCIDENT_CLOSE_REQUESTED,
            cls.INCIDENT_CLOSE_EXECUTE,
            cls.INCIDENT_CLOSED,
            cls.INCIDENT_FAILED,
            # Plan events
            cls.PLAN_GENERATED,
            cls.PLAN_JUDGED,
            # Remediation
            cls.REMEDIATION_STARTED,
            cls.REMEDIATION_EXECUTED,
            cls.REMEDIATION_FAILED,
            # Pipeline lifecycle
            cls.PIPELINE_REQUESTED,
            cls.PIPELINE_PLANNED,
            cls.PIPELINE_GENERATED,
            cls.PIPELINE_VALIDATED,
            cls.PIPELINE_REQUIRES_APPROVAL,
            cls.PIPELINE_APPROVED,
            cls.PIPELINE_REJECTED,
            cls.PIPELINE_DEPLOY_EXECUTE,
            cls.PIPELINE_DEPLOYED,
            cls.PIPELINE_FAILED,
            # External integrations
            cls.SERVICENOW_INCIDENTS,
            cls.SERVICENOW_UPDATES,
            cls.JIRA_TICKETS,
            cls.JIRA_UPDATES,
            cls.GCP_ALERTS,
            cls.AGENT_EVENTS,
            # MCP commands
            cls.MCP_SERVICENOW_COMMANDS,
            cls.MCP_GITHUB_COMMANDS,
            cls.MCP_AIRFLOW_COMMANDS,
        ]

    @classmethod
    def incident_topics(cls) -> List[str]:
        """Get incident-related topics"""
        return [
            cls.INCIDENT_CREATED,
            cls.INCIDENT_ENRICHED,
            cls.INCIDENT_PLAN_GENERATED,
            cls.INCIDENT_REQUIRES_APPROVAL,
            cls.INCIDENT_APPROVED,
            cls.INCIDENT_REJECTED,
            cls.INCIDENT_EXECUTED,
            cls.INCIDENT_VERIFIED,
            cls.INCIDENT_CLOSE_REQUESTED,
            cls.INCIDENT_CLOSE_EXECUTE,
            cls.INCIDENT_CLOSED,
            cls.INCIDENT_FAILED,
        ]

    @classmethod
    def pipeline_topics(cls) -> List[str]:
        """Get pipeline-related topics"""
        return [
            cls.PIPELINE_REQUESTED,
            cls.PIPELINE_PLANNED,
            cls.PIPELINE_GENERATED,
            cls.PIPELINE_VALIDATED,
            cls.PIPELINE_REQUIRES_APPROVAL,
            cls.PIPELINE_APPROVED,
            cls.PIPELINE_REJECTED,
            cls.PIPELINE_DEPLOY_EXECUTE,
            cls.PIPELINE_DEPLOYED,
            cls.PIPELINE_FAILED,
        ]


# =============================================================================
# EVENT SCHEMAS
# =============================================================================

class EventBase(BaseModel):
    """Base event with common fields"""
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    correlation_id: str
    source: str = "ai_agent_platform"
    version: str = "5.0"


class IncidentCreatedEvent(EventBase):
    """Event when incident is created/received"""
    event_type: str = "incident.created"
    incident_id: str
    short_description: str
    description: str
    priority: str
    service: str
    source_system: str = "servicenow"
    raw_data: Dict[str, Any] = Field(default_factory=dict)


class IncidentEnrichedEvent(EventBase):
    """Event after classification and RAG enrichment"""
    event_type: str = "incident.enriched"
    incident_id: str
    classification: str
    severity: str
    service: str
    rag_results_count: int
    rag_confidence: float
    parsed_context: Dict[str, Any]


class PlanGeneratedEvent(EventBase):
    """Event when remediation plan is generated"""
    event_type: str = "plan.generated"
    incident_id: str
    plan_id: str
    action_type: str
    script_id: str
    script_path: str
    steps_count: int
    has_rollback: bool
    confidence: float
    plan: Dict[str, Any]


class PlanJudgedEvent(EventBase):
    """Event after LLM-as-Judge evaluation"""
    event_type: str = "plan.judged"
    incident_id: str
    plan_id: str
    quality_score: float
    safety_passed: bool
    factual_score: float
    feasibility_score: float
    risk_level: str
    verdict: str  # approve, revise, reject
    reasoning: str


class IncidentApprovedEvent(EventBase):
    """Event when plan is approved for execution"""
    event_type: str = "incident.approved"
    incident_id: str
    plan_id: str
    approval_route: str  # auto, async, manual
    approved_by: str
    conditions: List[str] = Field(default_factory=list)
    risk_level: str


class IncidentRejectedEvent(EventBase):
    """Event when plan is rejected"""
    event_type: str = "incident.rejected"
    incident_id: str
    plan_id: str
    reason: str
    rejected_by: str
    recommendations: List[str] = Field(default_factory=list)


class IncidentExecutedEvent(EventBase):
    """Event after execution completes"""
    event_type: str = "incident.executed"
    incident_id: str
    execution_plan_id: str
    status: str  # success, failed, partial
    execution_time_seconds: float
    github_run_id: Optional[int] = None
    output: Dict[str, Any] = Field(default_factory=dict)
    error: Optional[str] = None


class IncidentVerifiedEvent(EventBase):
    """Event after fix verification"""
    event_type: str = "incident.verified"
    incident_id: str
    fix_verified: bool
    verification_method: str
    verification_reason: str
    confidence: float


class IncidentClosedEvent(EventBase):
    """Event when incident is closed"""
    event_type: str = "incident.closed"
    incident_id: str
    resolution_summary: str
    total_duration_seconds: float
    automation_level: str  # full, partial, manual
    script_used: Optional[str] = None
    feedback_recorded: bool


class IncidentFailedEvent(EventBase):
    """Event when workflow fails"""
    event_type: str = "incident.failed"
    incident_id: str
    failed_at_step: str
    error_message: str
    error_type: str
    recoverable: bool
    recommended_action: str


class IncidentRequiresApprovalEvent(EventBase):
    """Event when incident requires human approval before execution"""
    event_type: str = "incident.requires_approval"
    incident_id: str
    plan_id: str
    approval_route: str  # auto, async, manual
    risk_level: str
    script_id: str
    script_path: str
    estimated_duration_seconds: Optional[int] = None
    rollback_available: bool = True
    approval_timeout_seconds: int = 3600  # 1 hour default
    judge_score: Optional[Dict[str, Any]] = None


class IncidentCloseRequestedEvent(EventBase):
    """Event when human clicks close button in UI"""
    event_type: str = "incident.close_requested"
    incident_id: str
    requested_by: str
    resolution: str
    close_notes: Optional[str] = None


class IncidentCloseExecuteEvent(EventBase):
    """Event when orchestrator approves closure - MCP consumes this"""
    event_type: str = "incident.close_execute"
    incident_id: str
    servicenow_sys_id: str
    resolution_code: str
    resolution_notes: str
    close_code: str = "Resolved"


# =============================================================================
# REMEDIATION EVENTS
# =============================================================================

class RemediationStartedEvent(EventBase):
    """Event when remediation execution begins"""
    event_type: str = "remediation.started"
    incident_id: str
    plan_id: str
    script_id: str
    github_workflow_id: Optional[str] = None
    expected_duration_seconds: Optional[int] = None


class RemediationExecutedEvent(EventBase):
    """Event when remediation completes (success or failure)"""
    event_type: str = "remediation.executed"
    incident_id: str
    plan_id: str
    success: bool
    execution_time_seconds: float
    github_run_id: Optional[int] = None
    github_run_url: Optional[str] = None
    output: Dict[str, Any] = Field(default_factory=dict)
    error: Optional[str] = None


class RemediationRollbackEvent(EventBase):
    """Event when rollback is triggered"""
    event_type: str = "remediation.rollback"
    incident_id: str
    plan_id: str
    original_execution_id: str
    rollback_reason: str
    rollback_script_path: Optional[str] = None


# =============================================================================
# DATA PIPELINE EVENTS (Data Agent)
# =============================================================================

class PipelineIntentEvent(EventBase):
    """Event when pipeline intent is received from Jira"""
    event_type: str = "pipeline.intent"
    request_id: str
    jira_key: str
    intent_json: Dict[str, Any]
    source_type: str
    environment: str


class PipelineStatusEvent(EventBase):
    """Event for pipeline workflow status updates"""
    event_type: str = "pipeline.status"
    request_id: str
    jira_key: str
    phase: str  # planning, generating, validating, awaiting_approval, deploying, complete
    status: str  # in_progress, completed, failed
    message: str
    details: Dict[str, Any] = Field(default_factory=dict)


class PipelineRequestedEvent(EventBase):
    """Event when pipeline generation is requested"""
    event_type: str = "pipeline.requested"
    request_id: str
    jira_key: str
    pipeline_identity: Dict[str, Any]
    source_config: Dict[str, Any]
    target_config: Dict[str, Any]
    schema_definition: Dict[str, Any]
    execution_policy: Dict[str, Any] = Field(default_factory=dict)


class PipelinePlanningEvent(EventBase):
    """Event when pipeline planning is complete"""
    event_type: str = "pipeline.planning"
    request_id: str
    jira_key: str
    pipeline_action: str  # create, modify, upgrade_schema, no_change
    is_new_pipeline: bool
    template_selection: Dict[str, Any]
    schema_changes: List[Dict[str, Any]] = Field(default_factory=list)


class PipelineGeneratingEvent(EventBase):
    """Event when code generation is complete"""
    event_type: str = "pipeline.generating"
    request_id: str
    jira_key: str
    dag_template: str
    spark_templates: List[str]
    artifact_count: int


class PipelineValidatingEvent(EventBase):
    """Event when validation is complete"""
    event_type: str = "pipeline.validating"
    request_id: str
    jira_key: str
    is_valid: bool
    error_count: int
    warning_count: int
    errors: List[str] = Field(default_factory=list)


class PipelineAwaitingApprovalEvent(EventBase):
    """Event when human approval is required"""
    event_type: str = "pipeline.awaiting_approval"
    request_id: str
    jira_key: str
    environment: str
    approval_reason: str
    risk_level: str
    artifacts_url: Optional[str] = None


class PipelineDeployingEvent(EventBase):
    """Event when deployment is in progress"""
    event_type: str = "pipeline.deploying"
    request_id: str
    jira_key: str
    branch_name: str
    commit_sha: str
    pr_url: Optional[str] = None
    build_id: Optional[str] = None


class PipelineCompletedEvent(EventBase):
    """Event when pipeline workflow completes successfully"""
    event_type: str = "pipeline.completed"
    request_id: str
    jira_key: str
    pipeline_id: Optional[int] = None
    pr_url: Optional[str] = None
    build_status: str
    dag_path: str
    spark_job_paths: Dict[str, str] = Field(default_factory=dict)
    total_duration_seconds: float


class PipelineFailedEvent(EventBase):
    """Event when pipeline workflow fails"""
    event_type: str = "pipeline.failed"
    request_id: str
    jira_key: str
    failed_at_phase: str
    error_message: str
    error_type: str
    recoverable: bool
    recommended_action: str


class PipelineMRCreatedEvent(EventBase):
    """Event when GitHub MR/PR is created"""
    event_type: str = "pipeline.mr.created"
    request_id: str
    jira_key: str
    mr_url: str
    mr_number: int
    branch_name: str
    files_changed: List[str] = Field(default_factory=list)


class PipelineRequiresApprovalEvent(EventBase):
    """Event when pipeline requires human approval (PROD or schema change)"""
    event_type: str = "pipeline.requires_approval"
    request_id: str
    jira_key: str
    environment: str
    approval_reason: str  # "prod_deployment", "schema_change", "policy_required"
    risk_level: str
    artifacts_preview: Dict[str, Any] = Field(default_factory=dict)
    approval_timeout_seconds: int = 86400  # 24 hours default


class PipelineApprovedEvent(EventBase):
    """Event when pipeline is approved by human"""
    event_type: str = "pipeline.approved"
    request_id: str
    jira_key: str
    approved_by: str
    approval_notes: Optional[str] = None
    conditions: List[str] = Field(default_factory=list)


class PipelineRejectedEvent(EventBase):
    """Event when pipeline is rejected by human"""
    event_type: str = "pipeline.rejected"
    request_id: str
    jira_key: str
    rejected_by: str
    rejection_reason: str
    recommendations: List[str] = Field(default_factory=list)


class PipelineDeployExecuteEvent(EventBase):
    """Event when deployment should be executed - Airflow MCP consumes this"""
    event_type: str = "pipeline.deploy_execute"
    request_id: str
    jira_key: str
    dag_path: str
    spark_job_paths: Dict[str, str] = Field(default_factory=dict)
    environment: str
    pr_merged: bool = False
    pr_url: Optional[str] = None


class PipelineDeployedEvent(EventBase):
    """Event when deployment completes successfully"""
    event_type: str = "pipeline.deployed"
    request_id: str
    jira_key: str
    pipeline_id: Optional[int] = None
    environment: str
    dag_deployed: bool
    airflow_dag_id: Optional[str] = None
    deployment_timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())


# =============================================================================
# MCP COMMAND EVENTS (FastAPI publishes, MCPs consume)
# =============================================================================

class MCPServiceNowCommand(EventBase):
    """Command for ServiceNow MCP to execute"""
    event_type: str = "mcp.servicenow.command"
    command: str  # "close_ticket", "update_ticket", "add_work_notes"
    incident_id: str
    servicenow_sys_id: str
    payload: Dict[str, Any] = Field(default_factory=dict)


class MCPGitHubCommand(EventBase):
    """Command for GitHub MCP to execute"""
    event_type: str = "mcp.github.command"
    command: str  # "trigger_workflow", "create_pr", "merge_pr"
    incident_id: Optional[str] = None
    request_id: Optional[str] = None
    payload: Dict[str, Any] = Field(default_factory=dict)


class MCPAirflowCommand(EventBase):
    """Command for Airflow MCP to execute"""
    event_type: str = "mcp.airflow.command"
    command: str  # "trigger_dag", "pause_dag", "unpause_dag"
    request_id: Optional[str] = None
    dag_id: str
    payload: Dict[str, Any] = Field(default_factory=dict)


# =============================================================================
# EVENT FACTORY
# =============================================================================

class EventFactory:
    """Factory for creating Kafka events"""

    @staticmethod
    def incident_created(
        incident_id: str,
        correlation_id: str,
        short_description: str,
        description: str,
        priority: str,
        service: str,
        raw_data: Dict[str, Any]
    ) -> IncidentCreatedEvent:
        """Create incident.created event"""
        return IncidentCreatedEvent(
            incident_id=incident_id,
            correlation_id=correlation_id,
            short_description=short_description,
            description=description,
            priority=priority,
            service=service,
            raw_data=raw_data
        )

    @staticmethod
    def incident_enriched(
        incident_id: str,
        correlation_id: str,
        classification: str,
        severity: str,
        service: str,
        rag_results_count: int,
        rag_confidence: float,
        parsed_context: Dict[str, Any]
    ) -> IncidentEnrichedEvent:
        """Create incident.enriched event"""
        return IncidentEnrichedEvent(
            incident_id=incident_id,
            correlation_id=correlation_id,
            classification=classification,
            severity=severity,
            service=service,
            rag_results_count=rag_results_count,
            rag_confidence=rag_confidence,
            parsed_context=parsed_context
        )

    @staticmethod
    def plan_generated(
        incident_id: str,
        correlation_id: str,
        plan_id: str,
        plan: Dict[str, Any]
    ) -> PlanGeneratedEvent:
        """Create plan.generated event"""
        return PlanGeneratedEvent(
            incident_id=incident_id,
            correlation_id=correlation_id,
            plan_id=plan_id,
            action_type=plan.get("action_type", "script"),
            script_id=plan.get("script_id", ""),
            script_path=plan.get("script_path", ""),
            steps_count=len(plan.get("steps", [])),
            has_rollback="rollback_plan" in plan,
            confidence=plan.get("confidence", 0.0),
            plan=plan
        )

    @staticmethod
    def plan_judged(
        incident_id: str,
        correlation_id: str,
        plan_id: str,
        judge_score: Dict[str, Any],
        verdict: str
    ) -> PlanJudgedEvent:
        """Create plan.judged event"""
        return PlanJudgedEvent(
            incident_id=incident_id,
            correlation_id=correlation_id,
            plan_id=plan_id,
            quality_score=judge_score.get("quality_score", 0.0),
            safety_passed=judge_score.get("safety_passed", False),
            factual_score=judge_score.get("factual_score", 0.0),
            feasibility_score=judge_score.get("feasibility_score", 0.0),
            risk_level=judge_score.get("risk_level", "unknown"),
            verdict=verdict,
            reasoning=judge_score.get("reasoning", "")
        )

    @staticmethod
    def incident_approved(
        incident_id: str,
        correlation_id: str,
        plan_id: str,
        approval_route: str,
        approved_by: str,
        conditions: List[str],
        risk_level: str
    ) -> IncidentApprovedEvent:
        """Create incident.approved event"""
        return IncidentApprovedEvent(
            incident_id=incident_id,
            correlation_id=correlation_id,
            plan_id=plan_id,
            approval_route=approval_route,
            approved_by=approved_by,
            conditions=conditions,
            risk_level=risk_level
        )

    @staticmethod
    def incident_executed(
        incident_id: str,
        correlation_id: str,
        execution_plan_id: str,
        status: str,
        execution_time: float,
        output: Dict[str, Any],
        github_run_id: Optional[int] = None,
        error: Optional[str] = None
    ) -> IncidentExecutedEvent:
        """Create incident.executed event"""
        return IncidentExecutedEvent(
            incident_id=incident_id,
            correlation_id=correlation_id,
            execution_plan_id=execution_plan_id,
            status=status,
            execution_time_seconds=execution_time,
            github_run_id=github_run_id,
            output=output,
            error=error
        )

    @staticmethod
    def incident_closed(
        incident_id: str,
        correlation_id: str,
        resolution_summary: str,
        total_duration: float,
        automation_level: str,
        script_used: Optional[str] = None
    ) -> IncidentClosedEvent:
        """Create incident.closed event"""
        return IncidentClosedEvent(
            incident_id=incident_id,
            correlation_id=correlation_id,
            resolution_summary=resolution_summary,
            total_duration_seconds=total_duration,
            automation_level=automation_level,
            script_used=script_used,
            feedback_recorded=True
        )

    # Pipeline Events
    @staticmethod
    def pipeline_requested(
        request_id: str,
        correlation_id: str,
        jira_key: str,
        intent: Dict[str, Any],
    ) -> PipelineRequestedEvent:
        """Create pipeline.requested event"""
        return PipelineRequestedEvent(
            request_id=request_id,
            correlation_id=correlation_id,
            jira_key=jira_key,
            pipeline_identity=intent.get("pipeline_identity", {}),
            source_config=intent.get("source_config", {}),
            target_config=intent.get("target_config", {}),
            schema_definition=intent.get("schema_definition", {}),
            execution_policy=intent.get("execution_policy", {}),
        )

    @staticmethod
    def pipeline_status(
        request_id: str,
        correlation_id: str,
        jira_key: str,
        phase: str,
        status: str,
        message: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> PipelineStatusEvent:
        """Create pipeline.status event"""
        return PipelineStatusEvent(
            request_id=request_id,
            correlation_id=correlation_id,
            jira_key=jira_key,
            phase=phase,
            status=status,
            message=message,
            details=details or {},
        )

    @staticmethod
    def pipeline_completed(
        request_id: str,
        correlation_id: str,
        jira_key: str,
        pr_url: str,
        build_status: str,
        dag_path: str,
        spark_job_paths: Dict[str, str],
        total_duration: float,
        pipeline_id: Optional[int] = None,
    ) -> PipelineCompletedEvent:
        """Create pipeline.completed event"""
        return PipelineCompletedEvent(
            request_id=request_id,
            correlation_id=correlation_id,
            jira_key=jira_key,
            pipeline_id=pipeline_id,
            pr_url=pr_url,
            build_status=build_status,
            dag_path=dag_path,
            spark_job_paths=spark_job_paths,
            total_duration_seconds=total_duration,
        )

    @staticmethod
    def pipeline_failed(
        request_id: str,
        correlation_id: str,
        jira_key: str,
        failed_at_phase: str,
        error_message: str,
        error_type: str = "unknown",
        recoverable: bool = False,
        recommended_action: str = "Review error and retry",
    ) -> PipelineFailedEvent:
        """Create pipeline.failed event"""
        return PipelineFailedEvent(
            request_id=request_id,
            correlation_id=correlation_id,
            jira_key=jira_key,
            failed_at_phase=failed_at_phase,
            error_message=error_message,
            error_type=error_type,
            recoverable=recoverable,
            recommended_action=recommended_action,
        )

    @staticmethod
    def pipeline_mr_created(
        request_id: str,
        correlation_id: str,
        jira_key: str,
        mr_url: str,
        mr_number: int,
        branch_name: str,
        files_changed: Optional[List[str]] = None,
    ) -> PipelineMRCreatedEvent:
        """Create pipeline.mr.created event"""
        return PipelineMRCreatedEvent(
            request_id=request_id,
            correlation_id=correlation_id,
            jira_key=jira_key,
            mr_url=mr_url,
            mr_number=mr_number,
            branch_name=branch_name,
            files_changed=files_changed or [],
        )


# =============================================================================
# UTILITIES
# =============================================================================

def event_to_json(event: EventBase) -> str:
    """Serialize event to JSON string"""
    return event.model_dump_json()


def event_from_json(json_str: str, event_type: str) -> EventBase:
    """Deserialize event from JSON string"""
    data = json.loads(json_str)

    event_classes = {
        # Incident lifecycle events
        "incident.created": IncidentCreatedEvent,
        "incident.enriched": IncidentEnrichedEvent,
        "incident.requires_approval": IncidentRequiresApprovalEvent,
        "incident.approved": IncidentApprovedEvent,
        "incident.rejected": IncidentRejectedEvent,
        "incident.executed": IncidentExecutedEvent,
        "incident.verified": IncidentVerifiedEvent,
        "incident.close_requested": IncidentCloseRequestedEvent,
        "incident.close_execute": IncidentCloseExecuteEvent,
        "incident.closed": IncidentClosedEvent,
        "incident.failed": IncidentFailedEvent,
        # Plan events
        "plan.generated": PlanGeneratedEvent,
        "plan.judged": PlanJudgedEvent,
        # Remediation events
        "remediation.started": RemediationStartedEvent,
        "remediation.executed": RemediationExecutedEvent,
        "remediation.rollback": RemediationRollbackEvent,
        # Pipeline lifecycle events
        "pipeline.intent": PipelineIntentEvent,
        "pipeline.status": PipelineStatusEvent,
        "pipeline.requested": PipelineRequestedEvent,
        "pipeline.planning": PipelinePlanningEvent,
        "pipeline.planned": PipelinePlanningEvent,  # Alias
        "pipeline.generating": PipelineGeneratingEvent,
        "pipeline.generated": PipelineGeneratingEvent,  # Alias
        "pipeline.validating": PipelineValidatingEvent,
        "pipeline.validated": PipelineValidatingEvent,  # Alias
        "pipeline.requires_approval": PipelineRequiresApprovalEvent,
        "pipeline.awaiting_approval": PipelineAwaitingApprovalEvent,  # Legacy
        "pipeline.approved": PipelineApprovedEvent,
        "pipeline.rejected": PipelineRejectedEvent,
        "pipeline.deploy_execute": PipelineDeployExecuteEvent,
        "pipeline.deploying": PipelineDeployingEvent,
        "pipeline.deployed": PipelineDeployedEvent,
        "pipeline.completed": PipelineCompletedEvent,
        "pipeline.failed": PipelineFailedEvent,
        "pipeline.mr.created": PipelineMRCreatedEvent,
        # MCP command events
        "mcp.servicenow.command": MCPServiceNowCommand,
        "mcp.github.command": MCPGitHubCommand,
        "mcp.airflow.command": MCPAirflowCommand,
    }

    event_class = event_classes.get(event_type, EventBase)
    return event_class(**data)
