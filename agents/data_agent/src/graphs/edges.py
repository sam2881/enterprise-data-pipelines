"""
Conditional edge functions for LangGraph workflow.

These functions determine the routing between nodes based on the current state.
Each function returns a string literal indicating the next node to execute.

ROUTING RULES:
1. Error in any phase → route to error handler
2. No change needed → end workflow early
3. PROD or schema upgrade → require human approval
4. Validation failure → route to error handler
"""

from typing import Literal

from src.state.agent_state import AgentState


# =============================================================================
# Intent Validation Routing
# =============================================================================


def should_continue_after_intent(state: AgentState) -> Literal["plan", "error"]:
    """
    Route after intent validation.

    RULES:
    - If error occurred, route to error handler
    - Otherwise, proceed to planning

    Args:
        state: Current workflow state

    Returns:
        Next node: "plan" or "error"
    """
    if state.get("error_message"):
        return "error"
    return "plan"


# =============================================================================
# Planning Routing
# =============================================================================


def should_continue_after_planning(
    state: AgentState,
) -> Literal["generate", "no_change", "error"]:
    """
    Determine next step after planning.

    RULES:
    - If error occurred, route to error handler
    - If no changes needed (pipeline unchanged), end workflow
    - Otherwise, proceed to generation

    Args:
        state: Current workflow state

    Returns:
        Next node: "generate", "no_change", or "error"
    """
    # Check for errors first
    if state.get("error_message"):
        return "error"

    # Check if planner determined no changes needed
    planner_output = state.get("planner_output", {})
    if planner_output.get("pipeline_action") == "no_change":
        return "no_change"

    return "generate"


# =============================================================================
# Generation Routing
# =============================================================================


def should_continue_after_generation(
    state: AgentState,
) -> Literal["validate", "error"]:
    """
    Determine next step after generation.

    RULES:
    - If error occurred, route to error handler
    - Otherwise, proceed to validation

    Args:
        state: Current workflow state

    Returns:
        Next node: "validate" or "error"
    """
    if state.get("error_message"):
        return "error"
    return "validate"


# =============================================================================
# Validation Routing
# =============================================================================


def should_continue_after_validation(
    state: AgentState,
) -> Literal["check_approval", "deploy", "error"]:
    """
    Determine next step after validation.

    RULES:
    - If error occurred, route to error handler
    - If validation failed (is_valid=False), route to error handler
    - If PROD environment → require approval
    - If schema upgrade → require approval
    - If execution_policy.human_approval_required → require approval
    - Otherwise, proceed directly to deployment

    Args:
        state: Current workflow state

    Returns:
        Next node: "check_approval", "deploy", or "error"
    """
    # Check for explicit errors
    if state.get("error_message"):
        return "error"

    # Check validation result
    validator_output = state.get("validator_output", {})
    if not validator_output.get("is_valid", False):
        return "error"

    # Determine if approval is required
    intent = state.get("intent_json", {})
    pipeline_identity = intent.get("pipeline_identity", {})
    environment = pipeline_identity.get("environment", "dev")

    planner_output = state.get("planner_output", {})
    pipeline_action = planner_output.get("pipeline_action", "")

    execution_policy = intent.get("execution_policy", {})
    explicit_approval_required = execution_policy.get("human_approval_required", False)

    # Check all conditions that require approval
    needs_approval = (
        environment == "prod" or
        pipeline_action == "upgrade_schema" or
        explicit_approval_required
    )

    if needs_approval:
        return "check_approval"

    return "deploy"


# =============================================================================
# Approval Routing
# =============================================================================


def should_continue_after_approval(
    state: AgentState,
) -> Literal["deploy", "timeout", "rejected"]:
    """
    Determine next step after human approval checkpoint.

    RULES:
    - If approved (human_approval_received=True), proceed to deployment
    - If error contains "rejected", stop with rejection
    - If error contains "timeout", stop with timeout
    - Default to timeout if no approval received

    Args:
        state: Current workflow state

    Returns:
        Next node: "deploy", "timeout", or "rejected"
    """
    # Check if approval was received
    if state.get("human_approval_received"):
        return "deploy"

    # Check error message for specific conditions
    error_msg = state.get("error_message", "")
    if error_msg:
        error_lower = error_msg.lower()
        if "rejected" in error_lower:
            return "rejected"
        if "timeout" in error_lower:
            return "timeout"

    # Default to timeout (approval not received)
    return "timeout"


# =============================================================================
# Helper Functions
# =============================================================================


def needs_human_approval(state: AgentState) -> bool:
    """
    Check if human approval is required for this workflow.

    Approval is required when:
    - Environment is "prod"
    - Pipeline action is "upgrade_schema"
    - execution_policy.human_approval_required is True

    Args:
        state: Current workflow state

    Returns:
        True if approval is required
    """
    intent = state.get("intent_json", {})
    pipeline_identity = intent.get("pipeline_identity", {})
    environment = pipeline_identity.get("environment", "dev")

    planner_output = state.get("planner_output", {})
    pipeline_action = planner_output.get("pipeline_action", "")

    execution_policy = intent.get("execution_policy", {})
    explicit_approval_required = execution_policy.get("human_approval_required", False)

    return (
        environment == "prod" or
        pipeline_action == "upgrade_schema" or
        explicit_approval_required
    )


def is_validation_successful(state: AgentState) -> bool:
    """
    Check if validation passed.

    Args:
        state: Current workflow state

    Returns:
        True if validation was successful
    """
    validator_output = state.get("validator_output", {})
    return validator_output.get("is_valid", False)


def has_error(state: AgentState) -> bool:
    """
    Check if an error has occurred.

    Args:
        state: Current workflow state

    Returns:
        True if an error exists
    """
    return bool(state.get("error_message"))


def get_current_phase(state: AgentState) -> str:
    """
    Get current workflow phase.

    Args:
        state: Current workflow state

    Returns:
        Current phase name
    """
    return state.get("current_phase", "unknown")
