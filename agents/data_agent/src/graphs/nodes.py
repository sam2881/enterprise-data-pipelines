"""
Node function implementations for LangGraph workflow.

Each node function receives the current AgentState and returns a partial
state update (Dict[str, Any]). LangGraph merges the returned dict into
the current state.

RULES:
1. Each node returns PARTIAL state update only
2. Errors return error_message and error_agent fields
3. Status is published to Kafka after each phase
4. All operations are logged with request_id
"""

from datetime import datetime
from typing import Any, Dict

from src.state.agent_state import AgentState
from src.agents.planner_agent import PlannerAgent
from src.agents.generator_agent import GeneratorAgent
from src.agents.validator_agent import ValidatorAgent
from src.agents.deployer_agent import DeployerAgent
from src.integrations.kafka_client import get_kafka_producer
from src.utils.logging import get_logger

logger = get_logger(__name__)


# =============================================================================
# Kafka Status Publisher Helper
# =============================================================================


class StatusPublisher:
    """Helper for publishing status updates to Kafka."""

    def __init__(self):
        self._producer = None

    @property
    def producer(self):
        """Lazy-load Kafka producer."""
        if self._producer is None:
            self._producer = get_kafka_producer()
        return self._producer

    def publish_status(
        self,
        request_id: str,
        status: str,
        message: str,
        progress_pct: int,
        pipeline_id: int = None,
        error: str = None,
    ) -> None:
        """
        Publish pipeline status update.

        Args:
            request_id: Unique request identifier
            status: Current status (init, planning, generating, etc.)
            message: Human-readable status message
            progress_pct: Progress percentage (0-100)
            pipeline_id: Optional pipeline ID
            error: Optional error message
        """
        try:
            status_update = {
                "request_id": request_id,
                "pipeline_id": pipeline_id,
                "status": status,
                "phase": status,
                "message": message,
                "progress_pct": progress_pct,
                "timestamp": datetime.utcnow().isoformat(),
            }

            if error:
                status_update["error"] = error

            self.producer.publish_pipeline_status(status_update)

        except Exception as e:
            logger.warning(
                "Failed to publish status",
                request_id=request_id,
                error=str(e),
            )


# Global status publisher
status_publisher = StatusPublisher()


# =============================================================================
# Intent Validation Node
# =============================================================================


def validate_intent_node(state: AgentState) -> Dict[str, Any]:
    """
    Validate incoming intent JSON structure.

    Checks for required fields:
    - pipeline_identity
    - source_config
    - schema_definition
    - target_config

    Args:
        state: Current workflow state

    Returns:
        Partial state update with current_phase
    """
    request_id = state["request_id"]
    logger.info("Validating intent", request_id=request_id)

    try:
        intent = state["intent_json"]

        # Required top-level fields
        required_fields = [
            "pipeline_identity",
            "source_config",
            "schema_definition",
            "target_config",
        ]

        missing = [f for f in required_fields if f not in intent]

        if missing:
            error_msg = f"Missing required fields: {missing}"
            logger.error("Intent validation failed", request_id=request_id, missing=missing)

            status_publisher.publish_status(
                request_id=request_id,
                status="failed",
                message=error_msg,
                progress_pct=5,
                error=error_msg,
            )

            return {
                "current_phase": "failed",
                "error_message": error_msg,
                "error_agent": "intent_validator",
            }

        # Validate pipeline_identity
        pipeline_identity = intent["pipeline_identity"]
        if not pipeline_identity.get("pipeline_name"):
            error_msg = "Missing pipeline_name in pipeline_identity"
            logger.error("Intent validation failed", request_id=request_id, error=error_msg)

            return {
                "current_phase": "failed",
                "error_message": error_msg,
                "error_agent": "intent_validator",
            }

        # Publish success status
        status_publisher.publish_status(
            request_id=request_id,
            status="init",
            message="Intent validated successfully",
            progress_pct=10,
        )

        logger.info("Intent validation passed", request_id=request_id)

        return {
            "current_phase": "planning",
            "phase_history": ["init"],
        }

    except Exception as e:
        error_msg = f"Intent validation error: {str(e)}"
        logger.error("Intent validation exception", request_id=request_id, error=str(e))

        return {
            "current_phase": "failed",
            "error_message": error_msg,
            "error_agent": "intent_validator",
        }


# =============================================================================
# Planner Node
# =============================================================================


def plan_pipeline_node(state: AgentState) -> Dict[str, Any]:
    """
    Execute planner agent to analyze intent and determine strategy.

    RULES:
    1. Query metadata FIRST - always check existing pipelines
    2. Detect schema changes by comparison
    3. Select templates based on source_type + processing_mode
    4. NEVER guess or infer missing values
    5. Return partial state update only

    Args:
        state: Current workflow state

    Returns:
        Partial state update with planner_output
    """
    request_id = state["request_id"]
    logger.info("Starting pipeline planning", request_id=request_id)

    # Publish status
    status_publisher.publish_status(
        request_id=request_id,
        status="planning",
        message="Analyzing intent and determining strategy...",
        progress_pct=15,
    )

    try:
        # Execute planner agent
        agent = PlannerAgent()
        result = agent.execute(state)

        # Check for errors from agent
        if result.get("error_message"):
            status_publisher.publish_status(
                request_id=request_id,
                status="failed",
                message=result["error_message"],
                progress_pct=15,
                error=result["error_message"],
            )
            logger.error(
                "Planning failed",
                request_id=request_id,
                error=result["error_message"],
            )
        else:
            planner_output = result.get("planner_output", {})
            action = planner_output.get("pipeline_action", "unknown")

            status_publisher.publish_status(
                request_id=request_id,
                status="planning",
                message=f"Plan complete: {action}",
                progress_pct=25,
            )
            logger.info(
                "Planning complete",
                request_id=request_id,
                action=action,
            )

        # Add phase history
        result["phase_history"] = ["planning"]

        return result

    except Exception as e:
        error_msg = f"Planning failed: {str(e)}"
        logger.error("Planning exception", request_id=request_id, error=str(e))

        status_publisher.publish_status(
            request_id=request_id,
            status="failed",
            message=error_msg,
            progress_pct=15,
            error=error_msg,
        )

        return {
            "current_phase": "failed",
            "error_message": error_msg,
            "error_agent": "planner",
        }


# =============================================================================
# Generator Node
# =============================================================================


def generate_artifacts_node(state: AgentState) -> Dict[str, Any]:
    """
    Execute generator agent to create DAGs, Spark jobs, and SQL.

    Args:
        state: Current workflow state

    Returns:
        Partial state update with generator_output
    """
    request_id = state["request_id"]
    logger.info("Starting artifact generation", request_id=request_id)

    # Publish status
    status_publisher.publish_status(
        request_id=request_id,
        status="generating",
        message="Generating DAGs and Spark jobs...",
        progress_pct=35,
    )

    try:
        # Execute generator agent
        agent = GeneratorAgent()
        result = agent.execute(state)

        # Check for errors
        if result.get("error_message"):
            status_publisher.publish_status(
                request_id=request_id,
                status="failed",
                message=result["error_message"],
                progress_pct=35,
                error=result["error_message"],
            )
            logger.error(
                "Generation failed",
                request_id=request_id,
                error=result["error_message"],
            )
        else:
            generator_output = result.get("generator_output", {})
            spark_job_count = len(generator_output.get("spark_jobs", {}))

            status_publisher.publish_status(
                request_id=request_id,
                status="generating",
                message=f"Generated 1 DAG and {spark_job_count} Spark jobs",
                progress_pct=50,
            )
            logger.info(
                "Generation complete",
                request_id=request_id,
                spark_jobs=spark_job_count,
            )

        # Add phase history
        result["phase_history"] = ["generating"]

        return result

    except Exception as e:
        error_msg = f"Generation failed: {str(e)}"
        logger.error("Generation exception", request_id=request_id, error=str(e))

        status_publisher.publish_status(
            request_id=request_id,
            status="failed",
            message=error_msg,
            progress_pct=35,
            error=error_msg,
        )

        return {
            "current_phase": "failed",
            "error_message": error_msg,
            "error_agent": "generator",
        }


# =============================================================================
# Validator Node
# =============================================================================


def validate_artifacts_node(state: AgentState) -> Dict[str, Any]:
    """
    Execute validator agent to validate all generated artifacts.

    Args:
        state: Current workflow state

    Returns:
        Partial state update with validator_output
    """
    request_id = state["request_id"]
    logger.info("Starting artifact validation", request_id=request_id)

    # Publish status
    status_publisher.publish_status(
        request_id=request_id,
        status="validating",
        message="Validating syntax and security...",
        progress_pct=55,
    )

    try:
        # Execute validator agent
        agent = ValidatorAgent()
        result = agent.execute(state)

        # Check for errors
        if result.get("error_message"):
            status_publisher.publish_status(
                request_id=request_id,
                status="failed",
                message=result["error_message"],
                progress_pct=55,
                error=result["error_message"],
            )
            logger.error(
                "Validation failed",
                request_id=request_id,
                error=result["error_message"],
            )
        else:
            validator_output = result.get("validator_output", {})
            is_valid = validator_output.get("is_valid", False)
            error_count = len(validator_output.get("errors", []))

            if is_valid:
                status_publisher.publish_status(
                    request_id=request_id,
                    status="validating",
                    message="Validation passed",
                    progress_pct=70,
                )
                logger.info("Validation passed", request_id=request_id)
            else:
                status_publisher.publish_status(
                    request_id=request_id,
                    status="validating",
                    message=f"Validation found {error_count} errors",
                    progress_pct=70,
                )
                logger.warning(
                    "Validation found errors",
                    request_id=request_id,
                    error_count=error_count,
                )

        # Add phase history
        result["phase_history"] = ["validating"]

        return result

    except Exception as e:
        error_msg = f"Validation failed: {str(e)}"
        logger.error("Validation exception", request_id=request_id, error=str(e))

        status_publisher.publish_status(
            request_id=request_id,
            status="failed",
            message=error_msg,
            progress_pct=55,
            error=error_msg,
        )

        return {
            "current_phase": "failed",
            "error_message": error_msg,
            "error_agent": "validator",
        }


# =============================================================================
# Approval Node
# =============================================================================


def wait_for_approval_node(state: AgentState) -> Dict[str, Any]:
    """
    Human approval checkpoint.

    For PROD deployments and schema upgrades, requires human approval
    before proceeding to deployment.

    Args:
        state: Current workflow state

    Returns:
        Partial state update with approval status
    """
    request_id = state["request_id"]
    logger.info("Checking approval status", request_id=request_id)

    # Check if approval already received (from external source)
    if state.get("human_approval_received"):
        approved_by = state.get("approved_by", "unknown")
        logger.info(
            "Approval already received",
            request_id=request_id,
            approved_by=approved_by,
        )

        status_publisher.publish_status(
            request_id=request_id,
            status="approved",
            message=f"Approved by {approved_by}",
            progress_pct=80,
        )

        return {
            "current_phase": "deploying",
            "phase_history": ["awaiting_approval"],
        }

    # Publish waiting status
    status_publisher.publish_status(
        request_id=request_id,
        status="awaiting_approval",
        message="Waiting for human approval...",
        progress_pct=75,
    )

    logger.info("Waiting for human approval", request_id=request_id)

    # Return state indicating approval is required
    # The workflow will pause here until approval is received
    return {
        "current_phase": "awaiting_approval",
        "human_approval_required": True,
        "phase_history": ["awaiting_approval"],
    }


# =============================================================================
# Deployer Node
# =============================================================================


def deploy_artifacts_node(state: AgentState) -> Dict[str, Any]:
    """
    Execute deployer agent to deploy artifacts to Git and CI/CD.

    Args:
        state: Current workflow state

    Returns:
        Partial state update with deployer_output
    """
    request_id = state["request_id"]
    logger.info("Starting deployment", request_id=request_id)

    # Publish status
    status_publisher.publish_status(
        request_id=request_id,
        status="deploying",
        message="Committing to Git and triggering CI/CD...",
        progress_pct=85,
    )

    try:
        # Execute deployer agent
        agent = DeployerAgent()
        result = agent.execute(state)

        # Check for errors
        if result.get("error_message"):
            status_publisher.publish_status(
                request_id=request_id,
                status="failed",
                message=result["error_message"],
                progress_pct=85,
                error=result["error_message"],
            )
            logger.error(
                "Deployment failed",
                request_id=request_id,
                error=result["error_message"],
            )
        else:
            deployer_output = result.get("deployer_output", {})
            pr_url = deployer_output.get("pr_url")
            branch = deployer_output.get("branch_name")

            message = f"Deployed to branch: {branch}"
            if pr_url:
                message += f" - PR: {pr_url}"

            status_publisher.publish_status(
                request_id=request_id,
                status="complete",
                message=message,
                progress_pct=100,
            )
            logger.info(
                "Deployment complete",
                request_id=request_id,
                branch=branch,
                pr_url=pr_url,
            )

            # Update result with completion
            result["current_phase"] = "complete"
            result["completed_at"] = datetime.utcnow().isoformat()

        # Add phase history
        result["phase_history"] = ["deploying"]

        return result

    except Exception as e:
        error_msg = f"Deployment failed: {str(e)}"
        logger.error("Deployment exception", request_id=request_id, error=str(e))

        status_publisher.publish_status(
            request_id=request_id,
            status="failed",
            message=error_msg,
            progress_pct=85,
            error=error_msg,
        )

        return {
            "current_phase": "failed",
            "error_message": error_msg,
            "error_agent": "deployer",
        }


# =============================================================================
# Error Handler Node
# =============================================================================


def handle_error_node(state: AgentState) -> Dict[str, Any]:
    """
    Handle errors and cleanup.

    This node is called when any other node fails. It logs the error,
    publishes failure status, and sets final error state.

    Args:
        state: Current workflow state

    Returns:
        Partial state update with error details
    """
    request_id = state["request_id"]
    error_message = state.get("error_message", "Unknown error")
    error_agent = state.get("error_agent", "unknown")
    progress_pct = _get_progress_for_phase(state.get("current_phase", "unknown"))

    logger.error(
        "Workflow error",
        request_id=request_id,
        error_message=error_message,
        error_agent=error_agent,
    )

    # Publish failure status
    status_publisher.publish_status(
        request_id=request_id,
        status="failed",
        message=error_message,
        progress_pct=progress_pct,
        error=error_message,
    )

    return {
        "current_phase": "failed",
        "completed_at": datetime.utcnow().isoformat(),
        "phase_history": ["failed"],
    }


def _get_progress_for_phase(phase: str) -> int:
    """Get progress percentage for a given phase."""
    progress_map = {
        "init": 5,
        "planning": 25,
        "generating": 50,
        "validating": 70,
        "awaiting_approval": 75,
        "deploying": 90,
        "complete": 100,
        "failed": 0,
    }
    return progress_map.get(phase, 0)
