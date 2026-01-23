"""
Supervisor Agent - Workflow Orchestration

WHY: Orchestrates the complete pipeline generation workflow.
     Routes between agents, handles status updates, and manages approvals.

HOW:
1. Receive pipeline request
2. Route through: planner → generator → validator → [approval] → deployer
3. Publish status updates to Kafka
4. Update Jira ticket status
5. Handle errors and escalations

WORKFLOW PHASES:
init → planning → generating → validating → awaiting_approval → deploying → complete
                                                                          ↓
                                                                       failed
"""

from typing import Any, Dict, List, Optional
from datetime import datetime
import uuid

from src.agents.base_agent import BaseAgent
from src.utils.exceptions import DataAgentError


class SupervisorAgent(BaseAgent):
    """
    Supervisor Agent for workflow orchestration.

    RESPONSIBILITIES:
    1. Coordinate workflow phases
    2. Route to appropriate agents
    3. Publish Kafka status updates
    4. Update Jira tickets
    5. Handle human approval workflow
    6. Handle errors and escalations

    This agent acts as the controller for the LangGraph workflow,
    determining next steps based on current state.
    """

    def __init__(self) -> None:
        super().__init__("supervisor")
        self._kafka_producer = None
        self._jira_client = None

    @property
    def kafka_producer(self):
        """Lazy-load Kafka producer."""
        if self._kafka_producer is None:
            from src.integrations.kafka_client import get_kafka_producer
            self._kafka_producer = get_kafka_producer()
        return self._kafka_producer

    @property
    def jira_client(self):
        """Lazy-load Jira client."""
        if self._jira_client is None:
            from src.integrations.jira_client import JiraClient
            self._jira_client = JiraClient()
        return self._jira_client

    def execute(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute supervisor logic based on current phase.

        The supervisor determines what action to take based on
        the current workflow phase and state.

        Args:
            state: Current workflow state

        Returns:
            Partial state update with routing decision
        """
        current_phase = state.get("current_phase", "init")
        request_id = state.get("request_id", str(uuid.uuid4()))

        self.logger.info(
            "Supervisor evaluating state",
            request_id=request_id,
            current_phase=current_phase,
        )

        try:
            # Publish status update
            self._publish_status(state)

            # Update Jira if ticket exists
            jira_ticket = state.get("intent_json", {}).get("jira_ticket")
            if jira_ticket:
                self._update_jira_status(jira_ticket, current_phase, state)

            # Route based on current phase
            if current_phase == "init":
                return self._handle_init(state)
            elif current_phase == "planning":
                return self._handle_planning_complete(state)
            elif current_phase == "generating":
                return self._handle_generation_complete(state)
            elif current_phase == "validating":
                return self._handle_validation_complete(state)
            elif current_phase == "awaiting_approval":
                return self._handle_approval_status(state)
            elif current_phase == "deploying":
                return self._handle_deployment_complete(state)
            elif current_phase == "complete":
                return self._handle_complete(state)
            elif current_phase == "failed":
                return self._handle_failed(state)
            else:
                self.logger.warning(f"Unknown phase: {current_phase}")
                return {"current_phase": "failed", "error_message": f"Unknown phase: {current_phase}"}

        except Exception as e:
            self.logger.error("Supervisor error", error=str(e))
            return self.create_error_state(f"Supervisor error: {str(e)}", e)

    def _handle_init(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Handle initialization phase."""
        self.log_decision(
            decision="Initialize workflow",
            reasoning="Starting new pipeline request",
            context={"request_id": state.get("request_id")},
        )

        return {
            "current_phase": "planning",
            "phase_history": ["init"],
            "started_at": datetime.utcnow().isoformat(),
        }

    def _handle_planning_complete(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Handle planning phase completion."""
        planner_output = state.get("planner_output", {})

        # Check for errors
        if state.get("error_message"):
            return {"current_phase": "failed"}

        # Check if no changes needed
        if planner_output.get("pipeline_action") == "no_change":
            self.log_decision(
                decision="Skip generation - no changes",
                reasoning="Pipeline unchanged, nothing to generate",
            )
            return {
                "current_phase": "complete",
                "phase_history": ["init", "planning", "complete"],
            }

        self.log_decision(
            decision="Proceed to generation",
            reasoning=f"Pipeline action: {planner_output.get('pipeline_action')}",
        )

        return {
            "current_phase": "generating",
            "phase_history": ["init", "planning"],
        }

    def _handle_generation_complete(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Handle generation phase completion."""
        if state.get("error_message"):
            return {"current_phase": "failed"}

        self.log_decision(
            decision="Proceed to validation",
            reasoning="Artifacts generated successfully",
        )

        return {
            "current_phase": "validating",
            "phase_history": ["init", "planning", "generating"],
        }

    def _handle_validation_complete(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Handle validation phase completion."""
        validator_output = state.get("validator_output", {})

        if state.get("error_message") or not validator_output.get("is_valid"):
            return {"current_phase": "failed"}

        # Check if approval is required
        intent = state.get("intent_json", {})
        environment = intent.get("pipeline_identity", {}).get("environment", "dev")
        planner_output = state.get("planner_output", {})

        needs_approval = (
            environment == "prod" or
            planner_output.get("pipeline_action") == "upgrade_schema" or
            intent.get("execution_policy", {}).get("human_approval_required", False)
        )

        if needs_approval:
            self.log_decision(
                decision="Request human approval",
                reasoning=f"Approval required for {environment} environment",
                alternatives=["Direct deployment"],
            )

            # Create approval request
            self._request_approval(state)

            return {
                "current_phase": "awaiting_approval",
                "human_approval_required": True,
                "phase_history": ["init", "planning", "generating", "validating"],
            }

        self.log_decision(
            decision="Proceed to deployment",
            reasoning="No approval required for dev environment",
        )

        return {
            "current_phase": "deploying",
            "phase_history": ["init", "planning", "generating", "validating"],
        }

    def _handle_approval_status(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Handle approval waiting phase."""
        if state.get("human_approval_received"):
            self.log_decision(
                decision="Approval received - proceed to deployment",
                reasoning="Human approved the deployment",
            )
            return {
                "current_phase": "deploying",
                "phase_history": ["init", "planning", "generating", "validating", "awaiting_approval"],
            }

        if state.get("error_message"):
            return {"current_phase": "failed"}

        # Still waiting
        return {"current_phase": "awaiting_approval"}

    def _handle_deployment_complete(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Handle deployment phase completion."""
        if state.get("error_message"):
            return {"current_phase": "failed"}

        self.log_decision(
            decision="Deployment complete",
            reasoning="Artifacts deployed successfully",
        )

        return {
            "current_phase": "complete",
            "completed_at": datetime.utcnow().isoformat(),
        }

    def _handle_complete(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Handle completion phase."""
        self.logger.info(
            "Workflow completed successfully",
            request_id=state.get("request_id"),
        )

        # Final status update
        self._publish_status(state, status="complete", progress_pct=100)

        return {
            "current_phase": "complete",
            "completed_at": state.get("completed_at", datetime.utcnow().isoformat()),
        }

    def _handle_failed(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Handle failure phase."""
        error_message = state.get("error_message", "Unknown error")
        error_agent = state.get("error_agent", "unknown")

        self.logger.error(
            "Workflow failed",
            request_id=state.get("request_id"),
            error_message=error_message,
            error_agent=error_agent,
        )

        # Publish failure status
        self._publish_status(state, status="failed", progress_pct=0)

        return {
            "current_phase": "failed",
            "completed_at": datetime.utcnow().isoformat(),
        }

    def _publish_status(
        self,
        state: Dict[str, Any],
        status: Optional[str] = None,
        progress_pct: Optional[int] = None,
    ) -> None:
        """Publish status update to Kafka."""
        try:
            current_phase = state.get("current_phase", "unknown")

            # Calculate progress if not provided
            if progress_pct is None:
                phase_progress = {
                    "init": 0,
                    "planning": 20,
                    "generating": 40,
                    "validating": 60,
                    "awaiting_approval": 70,
                    "deploying": 90,
                    "complete": 100,
                    "failed": 0,
                }
                progress_pct = phase_progress.get(current_phase, 0)

            status_update = {
                "request_id": state.get("request_id"),
                "pipeline_id": state.get("metadata_context", {}).get("pipeline_id"),
                "status": status or current_phase,
                "phase": current_phase,
                "message": self._get_phase_message(current_phase, state),
                "progress_pct": progress_pct,
                "timestamp": datetime.utcnow().isoformat(),
                "error": state.get("error_message"),
            }

            self.kafka_producer.publish_pipeline_status(status_update)

        except Exception as e:
            self.logger.warning(f"Failed to publish status: {e}")

    def _update_jira_status(
        self,
        ticket_id: str,
        phase: str,
        state: Dict[str, Any],
    ) -> None:
        """Update Jira ticket with current status."""
        try:
            comment = self._get_phase_message(phase, state)
            self.jira_client.add_comment(ticket_id, comment)

            # Transition ticket based on phase
            if phase == "complete":
                self.jira_client.transition_ticket(ticket_id, "Done")
            elif phase == "failed":
                self.jira_client.transition_ticket(ticket_id, "Blocked")
            elif phase == "awaiting_approval":
                self.jira_client.transition_ticket(ticket_id, "In Review")

        except Exception as e:
            self.logger.warning(f"Failed to update Jira: {e}")

    def _get_phase_message(self, phase: str, state: Dict[str, Any]) -> str:
        """Get human-readable message for phase."""
        messages = {
            "init": "Starting pipeline generation workflow",
            "planning": "Analyzing pipeline configuration and detecting changes",
            "generating": "Generating DAG and Spark job artifacts",
            "validating": "Validating generated artifacts",
            "awaiting_approval": "Waiting for human approval",
            "deploying": "Deploying artifacts to Git and triggering CI/CD",
            "complete": "Pipeline generation completed successfully",
            "failed": f"Workflow failed: {state.get('error_message', 'Unknown error')}",
        }
        return messages.get(phase, f"Phase: {phase}")

    def _request_approval(self, state: Dict[str, Any]) -> None:
        """Create approval request in database."""
        try:
            intent = state.get("intent_json", {})

            approval_data = {
                "request_id": state.get("request_id"),
                "pipeline_name": intent.get("pipeline_identity", {}).get("pipeline_name"),
                "environment": intent.get("pipeline_identity", {}).get("environment"),
                "requested_by": intent.get("created_by"),
                "changes_summary": str(state.get("planner_output", {}).get("schema_plan", {})),
                "status": "pending",
            }

            self.repo.insert_approval_request(approval_data)

            self.logger.info(
                "Approval request created",
                request_id=state.get("request_id"),
            )

        except Exception as e:
            self.logger.warning(f"Failed to create approval request: {e}")
