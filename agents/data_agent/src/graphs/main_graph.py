"""
Main LangGraph workflow definition.

This module defines the core pipeline generation workflow using LangGraph.
The workflow orchestrates the flow through agents:
  START → VALIDATE_INTENT → PLAN → GENERATE → VALIDATE → [APPROVAL] → DEPLOY → END

All failures route to the ERROR handler node.

WORKFLOW PHASES:
1. validate_intent: Check intent JSON structure
2. plan_pipeline: Analyze intent, detect changes, select templates
3. generate_artifacts: Generate DAGs, Spark jobs, SQL from templates
4. validate_artifacts: Validate syntax, security, schema compatibility
5. wait_for_approval: Human approval checkpoint (for PROD/schema changes)
6. deploy_artifacts: Commit to Git, create PR, trigger CI/CD
7. handle_error: Error handling and cleanup
"""

from datetime import datetime
from typing import Any, AsyncIterator, Dict, Optional

from langgraph.graph import END, StateGraph
from langgraph.checkpoint.memory import MemorySaver

from src.state.agent_state import AgentState
from src.graphs.nodes import (
    validate_intent_node,
    plan_pipeline_node,
    generate_artifacts_node,
    validate_artifacts_node,
    wait_for_approval_node,
    deploy_artifacts_node,
    handle_error_node,
)
from src.graphs.edges import (
    should_continue_after_intent,
    should_continue_after_planning,
    should_continue_after_generation,
    should_continue_after_validation,
    should_continue_after_approval,
)
from src.utils.logging import get_logger

logger = get_logger(__name__)


# =============================================================================
# Graph Creation
# =============================================================================


def create_pipeline_graph(checkpointer: Optional[MemorySaver] = None) -> StateGraph:
    """
    Create the main pipeline generation workflow graph.

    Flow:
    START → VALIDATE_INTENT → PLAN → GENERATE → VALIDATE → [APPROVAL] → DEPLOY → END

    All failures route to ERROR handler.

    Args:
        checkpointer: Optional checkpointer for state persistence

    Returns:
        Compiled StateGraph
    """
    logger.info("Creating pipeline workflow graph")

    # Initialize graph with state schema
    workflow = StateGraph(AgentState)

    # =========================================================================
    # Add Nodes
    # =========================================================================

    workflow.add_node("validate_intent", validate_intent_node)
    workflow.add_node("plan_pipeline", plan_pipeline_node)
    workflow.add_node("generate_artifacts", generate_artifacts_node)
    workflow.add_node("validate_artifacts", validate_artifacts_node)
    workflow.add_node("wait_for_approval", wait_for_approval_node)
    workflow.add_node("deploy_artifacts", deploy_artifacts_node)
    workflow.add_node("handle_error", handle_error_node)

    # =========================================================================
    # Set Entry Point
    # =========================================================================

    workflow.set_entry_point("validate_intent")

    # =========================================================================
    # Add Conditional Edges
    # =========================================================================

    # After intent validation
    workflow.add_conditional_edges(
        "validate_intent",
        should_continue_after_intent,
        {
            "plan": "plan_pipeline",
            "error": "handle_error",
        }
    )

    # After planning
    workflow.add_conditional_edges(
        "plan_pipeline",
        should_continue_after_planning,
        {
            "generate": "generate_artifacts",
            "no_change": END,  # Early exit if no changes needed
            "error": "handle_error",
        }
    )

    # After generation
    workflow.add_conditional_edges(
        "generate_artifacts",
        should_continue_after_generation,
        {
            "validate": "validate_artifacts",
            "error": "handle_error",
        }
    )

    # After validation
    workflow.add_conditional_edges(
        "validate_artifacts",
        should_continue_after_validation,
        {
            "check_approval": "wait_for_approval",
            "deploy": "deploy_artifacts",
            "error": "handle_error",
        }
    )

    # After approval checkpoint
    workflow.add_conditional_edges(
        "wait_for_approval",
        should_continue_after_approval,
        {
            "deploy": "deploy_artifacts",
            "timeout": "handle_error",
            "rejected": "handle_error",
        }
    )

    # =========================================================================
    # Add Final Edges
    # =========================================================================

    # Deployer goes to END on success
    workflow.add_edge("deploy_artifacts", END)

    # Error handler goes to END
    workflow.add_edge("handle_error", END)

    # =========================================================================
    # Compile Graph
    # =========================================================================

    if checkpointer:
        compiled = workflow.compile(checkpointer=checkpointer)
        logger.info("Graph compiled with checkpointer")
    else:
        compiled = workflow.compile()
        logger.info("Graph compiled without checkpointer")

    return compiled


def create_graph_with_memory() -> StateGraph:
    """
    Create graph with in-memory checkpointing for state persistence.

    This is useful for development and testing. For production,
    use create_graph_with_checkpointing() with PostgreSQL.

    Returns:
        Compiled StateGraph with MemorySaver checkpointer
    """
    checkpointer = MemorySaver()
    return create_pipeline_graph(checkpointer=checkpointer)


def create_graph_with_checkpointing(connection_string: str) -> StateGraph:
    """
    Create graph with PostgreSQL checkpointing for durability.

    Args:
        connection_string: PostgreSQL connection string

    Returns:
        Compiled StateGraph with PostgreSQL checkpointer
    """
    try:
        from langgraph.checkpoint.postgres import PostgresSaver

        checkpointer = PostgresSaver.from_conn_string(connection_string)
        logger.info("Created PostgreSQL checkpointer")
        return create_pipeline_graph(checkpointer=checkpointer)

    except ImportError:
        logger.warning(
            "PostgresSaver not available, falling back to MemorySaver. "
            "Install langgraph-checkpoint-postgres for PostgreSQL support."
        )
        return create_graph_with_memory()


# =============================================================================
# Pipeline Execution
# =============================================================================


def create_initial_state(intent_json: Dict[str, Any], request_id: str) -> AgentState:
    """
    Create initial state for pipeline execution.

    Args:
        intent_json: Pipeline intent JSON from UI
        request_id: Unique request identifier

    Returns:
        Initialized AgentState
    """
    return AgentState(
        request_id=request_id,
        intent_json=intent_json,
        current_phase="init",
        planner_output=None,
        generator_output=None,
        validator_output=None,
        deployer_output=None,
        human_approval_required=False,
        human_approval_received=False,
        error_message=None,
        error_agent=None,
        metadata_context={},
        started_at=datetime.utcnow().isoformat(),
        completed_at=None,
    )


async def run_pipeline_async(
    intent_json: Dict[str, Any],
    request_id: str,
    config: Optional[Dict[str, Any]] = None,
) -> AsyncIterator[Dict[str, Any]]:
    """
    Run pipeline and yield state updates asynchronously.

    This is the primary entry point for async pipeline execution.
    Yields intermediate states for real-time status updates.

    Args:
        intent_json: Pipeline intent JSON from UI
        request_id: Unique request identifier
        config: Optional LangGraph config (thread_id, etc.)

    Yields:
        State updates after each node execution
    """
    logger.info("Starting async pipeline workflow", request_id=request_id)

    # Create graph
    graph = create_pipeline_graph()

    # Create initial state
    initial_state = create_initial_state(intent_json, request_id)

    # Build config with thread_id for checkpointing
    run_config = config or {}
    if "configurable" not in run_config:
        run_config["configurable"] = {}
    if "thread_id" not in run_config["configurable"]:
        run_config["configurable"]["thread_id"] = request_id

    try:
        # Stream state updates
        async for state in graph.astream(initial_state, config=run_config):
            logger.debug(
                "Pipeline state update",
                request_id=request_id,
                keys=list(state.keys()),
            )
            yield state

        logger.info("Pipeline workflow complete", request_id=request_id)

    except Exception as e:
        logger.error(
            "Pipeline workflow failed",
            request_id=request_id,
            error=str(e),
        )
        # Yield error state
        yield {
            "current_phase": "failed",
            "error_message": str(e),
            "error_agent": "workflow",
            "completed_at": datetime.utcnow().isoformat(),
        }


def run_pipeline_sync(
    intent_json: Dict[str, Any],
    request_id: str,
    config: Optional[Dict[str, Any]] = None,
) -> AgentState:
    """
    Run pipeline synchronously (for testing).

    This is a blocking call that runs the entire pipeline
    and returns the final state.

    Args:
        intent_json: Pipeline intent JSON from UI
        request_id: Unique request identifier
        config: Optional LangGraph config

    Returns:
        Final AgentState after workflow completion
    """
    logger.info("Starting sync pipeline workflow", request_id=request_id)

    # Create graph
    graph = create_pipeline_graph()

    # Create initial state
    initial_state = create_initial_state(intent_json, request_id)

    # Build config with thread_id
    run_config = config or {}
    if "configurable" not in run_config:
        run_config["configurable"] = {}
    if "thread_id" not in run_config["configurable"]:
        run_config["configurable"]["thread_id"] = request_id

    try:
        # Invoke synchronously
        result = graph.invoke(initial_state, config=run_config)

        logger.info(
            "Pipeline workflow complete",
            request_id=request_id,
            final_phase=result.get("current_phase"),
        )

        return result

    except Exception as e:
        logger.error(
            "Pipeline workflow failed",
            request_id=request_id,
            error=str(e),
        )
        # Return error state
        return AgentState(
            request_id=request_id,
            intent_json=intent_json,
            current_phase="failed",
            planner_output=None,
            generator_output=None,
            validator_output=None,
            deployer_output=None,
            human_approval_required=False,
            human_approval_received=False,
            error_message=str(e),
            error_agent="workflow",
            metadata_context={},
            started_at=initial_state.get("started_at"),
            completed_at=datetime.utcnow().isoformat(),
        )


# =============================================================================
# Resume Workflow
# =============================================================================


async def resume_pipeline_async(
    request_id: str,
    updates: Dict[str, Any],
    checkpointer: Optional[MemorySaver] = None,
) -> AsyncIterator[Dict[str, Any]]:
    """
    Resume a paused pipeline (e.g., after human approval).

    This is used to continue a workflow that was waiting for approval.

    Args:
        request_id: Request ID of the paused pipeline
        updates: State updates to apply (e.g., human_approval_received=True)
        checkpointer: Checkpointer with saved state

    Yields:
        State updates after each node execution
    """
    logger.info("Resuming pipeline workflow", request_id=request_id)

    if checkpointer is None:
        raise ValueError("Checkpointer required to resume workflow")

    # Create graph with checkpointer
    graph = create_pipeline_graph(checkpointer=checkpointer)

    # Build config
    config = {
        "configurable": {
            "thread_id": request_id,
        }
    }

    try:
        # Update state and resume
        async for state in graph.astream(updates, config=config):
            logger.debug(
                "Pipeline state update (resume)",
                request_id=request_id,
                keys=list(state.keys()),
            )
            yield state

        logger.info("Pipeline workflow resumed and complete", request_id=request_id)

    except Exception as e:
        logger.error(
            "Pipeline resume failed",
            request_id=request_id,
            error=str(e),
        )
        yield {
            "current_phase": "failed",
            "error_message": str(e),
            "error_agent": "workflow",
            "completed_at": datetime.utcnow().isoformat(),
        }


# =============================================================================
# Graph Visualization
# =============================================================================


def get_graph_mermaid() -> str:
    """
    Get Mermaid diagram representation of the workflow graph.

    Returns:
        Mermaid diagram string
    """
    return """
graph TD
    START([Start]) --> validate_intent
    validate_intent --> |plan| plan_pipeline
    validate_intent --> |error| handle_error

    plan_pipeline --> |generate| generate_artifacts
    plan_pipeline --> |no_change| END_NC([End - No Change])
    plan_pipeline --> |error| handle_error

    generate_artifacts --> |validate| validate_artifacts
    generate_artifacts --> |error| handle_error

    validate_artifacts --> |check_approval| wait_for_approval
    validate_artifacts --> |deploy| deploy_artifacts
    validate_artifacts --> |error| handle_error

    wait_for_approval --> |deploy| deploy_artifacts
    wait_for_approval --> |timeout| handle_error
    wait_for_approval --> |rejected| handle_error

    deploy_artifacts --> END([End - Complete])
    handle_error --> END_ERR([End - Failed])
"""
