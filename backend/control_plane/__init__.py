"""
Control Plane Module - Central Workflow Orchestration

WHY: Provides unified orchestration for all agent workflows:
- IT Service (ServiceNow incident resolution)
- Data Pipeline (Jira-driven pipeline generation)
- Infrastructure (GCP resource management)

HOW: Uses a saga pattern with compensation logic for workflow management.
     Policy engine evaluates risk and routes to appropriate approval workflow.

Key Components:
- ControlPlaneOrchestrator: Main workflow coordinator (saga orchestration)
- PolicyEngine: Comprehensive plan-level risk assessment and approval routing
- WorkflowPolicyEngine: Lightweight workflow-level routing (used by orchestrator)
- WorkflowHandler: Interface for workflow-specific logic
- Handlers: IT Service, Data Pipeline, Infrastructure implementations

USAGE:
    from backend.control_plane import get_control_plane, WorkflowType

    orchestrator = get_control_plane()
    workflow_id = await orchestrator.start_workflow(
        workflow_type=WorkflowType.IT_SERVICE,
        source_id="INC0001234",
        source_system="servicenow"
    )
"""
# Orchestrator
from .orchestrator import (
    ControlPlaneOrchestrator,
    get_control_plane,
    WorkflowType,
    WorkflowState,
    WorkflowContext,
    WorkflowHandler,
    WorkflowPolicyEngine,
    ApprovalRequest,
    PolicyDecision,
)

# Policy Engine (comprehensive risk assessment, plan approval routing)
from .policy_engine import (
    PolicyEngine,
    get_policy_engine,
    Policy,
    RiskLevel,
    RiskAssessment,
    ApprovalRoute,
    ApprovalDecision,
)

__all__ = [
    # Orchestrator
    "ControlPlaneOrchestrator",
    "get_control_plane",
    "WorkflowType",
    "WorkflowState",
    "WorkflowContext",
    "WorkflowHandler",
    "WorkflowPolicyEngine",
    "ApprovalRequest",
    "PolicyDecision",
    # Policy Engine
    "PolicyEngine",
    "get_policy_engine",
    "Policy",
    "RiskLevel",
    "RiskAssessment",
    "ApprovalRoute",
    "ApprovalDecision",
]
