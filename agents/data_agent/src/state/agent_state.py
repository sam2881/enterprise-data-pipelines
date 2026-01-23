"""
LangGraph state definitions using TypedDict.

This module defines the main state object that flows through the LangGraph
workflow. LangGraph requires TypedDict for state definitions to enable
proper type checking and state updates.

RULES:
1. ALWAYS use TypedDict for LangGraph state (not Pydantic)
2. State updates are PARTIAL - return only changed keys
3. Use Annotated with reducer functions for list accumulation
4. Keep state flat where possible for easier updates
"""

from datetime import datetime
from typing import Annotated, Any, Dict, List, Literal, Optional, TypedDict

from langgraph.graph.message import add_messages
from langchain_core.messages import BaseMessage


# =============================================================================
# Reducer Functions for State Accumulation
# =============================================================================


def replace_value(current: Any, new: Any) -> Any:
    """Replace current value with new value (default behavior)."""
    return new


def append_to_list(current: List[Any], new: List[Any]) -> List[Any]:
    """Append new items to existing list."""
    if current is None:
        current = []
    if new is None:
        return current
    return current + new


def merge_dict(current: Dict[str, Any], new: Dict[str, Any]) -> Dict[str, Any]:
    """Merge new dict into current dict."""
    if current is None:
        current = {}
    if new is None:
        return current
    return {**current, **new}


# =============================================================================
# Workflow Phase Types
# =============================================================================


WorkflowPhaseType = Literal[
    "init",
    "planning",
    "generating",
    "validating",
    "awaiting_approval",
    "deploying",
    "complete",
    "failed",
]


# =============================================================================
# Main Agent State (LangGraph TypedDict)
# =============================================================================


class AgentState(TypedDict, total=False):
    """
    Main state object passed through LangGraph nodes.

    This is the central state that flows through the entire workflow.
    Each node receives this state and returns a PARTIAL update.

    IMPORTANT:
    - Use TypedDict (not Pydantic) for LangGraph compatibility
    - All fields should have Optional or default handling
    - Return only changed keys from nodes
    - Use Annotated with reducers for accumulating data

    State Flow:
    1. init: request_id, intent_json set
    2. planning: planner_output, metadata_context set
    3. generating: generator_output set
    4. validating: validator_output set
    5. awaiting_approval: human_approval_* fields set
    6. deploying: deployer_output set
    7. complete/failed: completed_at, error_* fields set
    """

    # =========================================================================
    # Request Identification
    # =========================================================================

    # Unique identifier for this workflow execution
    request_id: str

    # Correlation ID for tracing across systems
    correlation_id: Optional[str]

    # =========================================================================
    # Input Data
    # =========================================================================

    # The validated intent JSON from UI (IntentSchema as dict)
    intent_json: Dict[str, Any]

    # Raw intent for audit purposes
    raw_intent: Optional[Dict[str, Any]]

    # =========================================================================
    # Workflow Tracking
    # =========================================================================

    # Current phase of the workflow
    current_phase: WorkflowPhaseType

    # History of phases for debugging
    phase_history: Annotated[List[str], append_to_list]

    # Current retry attempt (0-indexed)
    retry_count: int

    # Maximum retries allowed
    max_retries: int

    # =========================================================================
    # Agent Outputs (stored as dicts for serialization)
    # =========================================================================

    # Output from Planner Agent (PlannerOutput as dict)
    planner_output: Optional[Dict[str, Any]]

    # Output from Generator Agent (GeneratorOutput as dict)
    generator_output: Optional[Dict[str, Any]]

    # Output from Validator Agent (ValidatorOutput as dict)
    validator_output: Optional[Dict[str, Any]]

    # Output from Deployer Agent (DeployerOutput as dict)
    deployer_output: Optional[Dict[str, Any]]

    # =========================================================================
    # Human Approval Flow
    # =========================================================================

    # Whether human approval is required for this execution
    human_approval_required: bool

    # Whether human approval has been received
    human_approval_received: bool

    # Who approved (if approved)
    approved_by: Optional[str]

    # Approval timestamp
    approved_at: Optional[str]

    # Approval comments
    approval_comments: Optional[str]

    # =========================================================================
    # Error Handling
    # =========================================================================

    # Error message if workflow failed
    error_message: Optional[str]

    # Which agent caused the error
    error_agent: Optional[str]

    # Detailed error information
    error_details: Optional[Dict[str, Any]]

    # Whether the error is retryable
    is_retryable: bool

    # Accumulated errors throughout workflow
    errors: Annotated[List[Dict[str, Any]], append_to_list]

    # Accumulated warnings
    warnings: Annotated[List[str], append_to_list]

    # =========================================================================
    # Metadata Context (from PostgreSQL)
    # =========================================================================

    # Context loaded from metadata database
    metadata_context: Dict[str, Any]

    # Pipeline ID (if existing pipeline)
    pipeline_id: Optional[int]

    # Whether this is a new pipeline
    is_new_pipeline: bool

    # =========================================================================
    # Timestamps
    # =========================================================================

    # When the workflow started
    started_at: str

    # When the workflow completed (success or failure)
    completed_at: Optional[str]

    # Duration in milliseconds
    duration_ms: Optional[int]

    # =========================================================================
    # Audit Trail
    # =========================================================================

    # Accumulated audit log entries
    audit_log: Annotated[List[Dict[str, Any]], append_to_list]

    # =========================================================================
    # Messages (for LangGraph message handling)
    # =========================================================================

    # LangChain messages (if using chat-based agents)
    messages: Annotated[List[BaseMessage], add_messages]


# =============================================================================
# Checkpoint State (for persistence)
# =============================================================================


class CheckpointState(TypedDict, total=False):
    """
    Minimal state for checkpointing.

    This is used when persisting state to PostgreSQL for recovery.
    It contains only the essential fields needed to resume a workflow.
    """

    request_id: str
    current_phase: WorkflowPhaseType
    intent_json: Dict[str, Any]
    planner_output: Optional[Dict[str, Any]]
    generator_output: Optional[Dict[str, Any]]
    validator_output: Optional[Dict[str, Any]]
    human_approval_required: bool
    human_approval_received: bool
    error_message: Optional[str]
    started_at: str
    metadata_context: Dict[str, Any]


# =============================================================================
# Node Input/Output Types
# =============================================================================


class ValidateIntentInput(TypedDict):
    """Input for validate_intent node."""

    request_id: str
    intent_json: Dict[str, Any]


class ValidateIntentOutput(TypedDict, total=False):
    """Output from validate_intent node."""

    current_phase: WorkflowPhaseType
    intent_json: Dict[str, Any]
    error_message: Optional[str]
    error_agent: Optional[str]


class PlanPipelineInput(TypedDict):
    """Input for plan_pipeline node."""

    request_id: str
    intent_json: Dict[str, Any]
    metadata_context: Dict[str, Any]


class PlanPipelineOutput(TypedDict, total=False):
    """Output from plan_pipeline node."""

    current_phase: WorkflowPhaseType
    planner_output: Dict[str, Any]
    metadata_context: Dict[str, Any]
    human_approval_required: bool
    error_message: Optional[str]
    error_agent: Optional[str]


class GenerateArtifactsInput(TypedDict):
    """Input for generate_artifacts node."""

    request_id: str
    intent_json: Dict[str, Any]
    planner_output: Dict[str, Any]
    metadata_context: Dict[str, Any]


class GenerateArtifactsOutput(TypedDict, total=False):
    """Output from generate_artifacts node."""

    current_phase: WorkflowPhaseType
    generator_output: Dict[str, Any]
    error_message: Optional[str]
    error_agent: Optional[str]


class ValidateArtifactsInput(TypedDict):
    """Input for validate_artifacts node."""

    request_id: str
    generator_output: Dict[str, Any]
    intent_json: Dict[str, Any]


class ValidateArtifactsOutput(TypedDict, total=False):
    """Output from validate_artifacts node."""

    current_phase: WorkflowPhaseType
    validator_output: Dict[str, Any]
    error_message: Optional[str]
    error_agent: Optional[str]


class DeployArtifactsInput(TypedDict):
    """Input for deploy_artifacts node."""

    request_id: str
    generator_output: Dict[str, Any]
    validator_output: Dict[str, Any]
    intent_json: Dict[str, Any]
    human_approval_received: bool


class DeployArtifactsOutput(TypedDict, total=False):
    """Output from deploy_artifacts node."""

    current_phase: WorkflowPhaseType
    deployer_output: Dict[str, Any]
    completed_at: str
    error_message: Optional[str]
    error_agent: Optional[str]


# =============================================================================
# State Factory Functions
# =============================================================================


def create_initial_state(
    request_id: str,
    intent_json: Dict[str, Any],
    correlation_id: Optional[str] = None,
) -> AgentState:
    """
    Create initial state for a new workflow execution.

    Args:
        request_id: Unique identifier for this execution
        intent_json: Validated intent from UI
        correlation_id: Optional correlation ID for tracing

    Returns:
        Initial AgentState
    """
    return AgentState(
        # Identification
        request_id=request_id,
        correlation_id=correlation_id,
        # Input
        intent_json=intent_json,
        raw_intent=intent_json.copy(),
        # Workflow
        current_phase="init",
        phase_history=["init"],
        retry_count=0,
        max_retries=3,
        # Agent outputs
        planner_output=None,
        generator_output=None,
        validator_output=None,
        deployer_output=None,
        # Approval
        human_approval_required=False,
        human_approval_received=False,
        approved_by=None,
        approved_at=None,
        approval_comments=None,
        # Errors
        error_message=None,
        error_agent=None,
        error_details=None,
        is_retryable=False,
        errors=[],
        warnings=[],
        # Metadata
        metadata_context={},
        pipeline_id=None,
        is_new_pipeline=True,
        # Timestamps
        started_at=datetime.utcnow().isoformat() + "Z",
        completed_at=None,
        duration_ms=None,
        # Audit
        audit_log=[],
        # Messages
        messages=[],
    )


def create_error_state(
    current_state: AgentState,
    error_message: str,
    error_agent: str,
    error_details: Optional[Dict[str, Any]] = None,
    is_retryable: bool = False,
) -> Dict[str, Any]:
    """
    Create partial state update for error condition.

    Args:
        current_state: Current workflow state
        error_message: Error message
        error_agent: Agent that caused the error
        error_details: Additional error details
        is_retryable: Whether the error can be retried

    Returns:
        Partial state update dict
    """
    return {
        "current_phase": "failed",
        "error_message": error_message,
        "error_agent": error_agent,
        "error_details": error_details or {},
        "is_retryable": is_retryable,
        "completed_at": datetime.utcnow().isoformat() + "Z",
        "errors": [
            {
                "agent": error_agent,
                "message": error_message,
                "details": error_details or {},
                "timestamp": datetime.utcnow().isoformat() + "Z",
            }
        ],
    }


def create_success_state() -> Dict[str, Any]:
    """
    Create partial state update for successful completion.

    Returns:
        Partial state update dict
    """
    return {
        "current_phase": "complete",
        "completed_at": datetime.utcnow().isoformat() + "Z",
    }
