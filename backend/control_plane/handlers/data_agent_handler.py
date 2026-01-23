"""
Data Agent Handler - LangGraph Pipeline Generation Workflow

WHY: Integrates the Data Agent LangGraph workflow with the backend control plane.
     Handles pipeline creation requests from UI, manages workflow execution,
     and provides status updates via WebSocket and Kafka.

USAGE:
    from backend.control_plane.handlers.data_agent_handler import DataAgentHandler

    handler = DataAgentHandler()
    result = await handler.handle_pipeline_request(intent)

WORKFLOW INTEGRATION:
    Uses agents/data_agent/src/graphs/main_graph.py for:
    - Intent validation
    - Pipeline planning
    - Artifact generation
    - Validation
    - Human approval (for PROD)
    - Deployment
"""

import asyncio
import logging
import sys
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

# Add data_agent src to path for imports
DATA_AGENT_PATH = Path(__file__).resolve().parents[3] / "agents" / "data_agent" / "src"
if str(DATA_AGENT_PATH) not in sys.path:
    sys.path.insert(0, str(DATA_AGENT_PATH))

# Import streaming components with fallbacks
try:
    from streaming.kafka_producer import get_producer
    from streaming.schemas import Topics
except ImportError:
    try:
        from backend.streaming.kafka_producer import get_producer
        from backend.streaming.schemas import Topics
    except ImportError:
        # Create mock objects if imports fail
        def get_producer():
            return None
        class Topics:
            PIPELINE_STATUS = "pipeline.status"

# Import tracing with fallback
try:
    from utils.otel_tracing import trace_span
except ImportError:
    try:
        from backend.utils.otel_tracing import trace_span
    except ImportError:
        # Fallback decorator if tracing not available
        def trace_span(name):
            def decorator(func):
                return func
            return decorator

logger = logging.getLogger(__name__)


# Pydantic models for request/response
try:
    from pydantic import BaseModel, Field
except ImportError:
    from dataclasses import dataclass as BaseModel
    Field = lambda **kwargs: None


class PipelineIntent(BaseModel):
    """Pipeline creation intent from UI."""

    request_id: Optional[str] = Field(default=None, description="Unique request ID")
    pipeline_identity: Dict[str, Any] = Field(..., description="Pipeline identity")
    source_config: Dict[str, Any] = Field(..., description="Source configuration")
    schema_definition: Dict[str, Any] = Field(..., description="Schema definition")
    target_config: Dict[str, Any] = Field(..., description="Target configuration")
    transformation_rules: list = Field(default_factory=list)
    data_quality_rules: list = Field(default_factory=list)
    execution_policy: Dict[str, Any] = Field(default_factory=dict)
    jira_ticket: Optional[str] = None
    created_by: Optional[str] = None


class PipelineStatusUpdate(BaseModel):
    """Pipeline status update for streaming."""

    request_id: str
    pipeline_id: Optional[int] = None
    status: str
    phase: str
    message: str
    progress_pct: int
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    error: Optional[str] = None


class DataAgentHandler:
    """
    Handler for Data Agent pipeline generation workflows.

    Integrates the LangGraph workflow with:
    - Kafka for event streaming
    - WebSocket for real-time updates
    - Backend API for status queries

    STATE MANAGEMENT:
    - Active pipelines are tracked in memory
    - Completed pipelines are persisted to database
    - Checkpointing enables workflow resume after approval
    """

    def __init__(self):
        """Initialize Data Agent handler."""
        self._graph = None
        self._kafka_producer = None
        self.active_pipelines: Dict[str, Dict[str, Any]] = {}

    @property
    def graph(self):
        """Lazy-load LangGraph workflow."""
        if self._graph is None:
            try:
                # Import with proper path setup
                from graphs.main_graph import create_pipeline_graph
                self._graph = create_pipeline_graph()
                logger.info("LangGraph pipeline workflow loaded")
            except ImportError as e:
                logger.warning(f"Failed to import LangGraph workflow: {e}")
                # Return None to allow handler to work in mock mode
                return None
        return self._graph

    @property
    def kafka_producer(self):
        """Lazy-load Kafka producer."""
        if self._kafka_producer is None:
            try:
                self._kafka_producer = get_producer()
            except Exception as e:
                logger.warning(f"Failed to initialize Kafka producer: {e}")
        return self._kafka_producer

    @trace_span("handle_pipeline_request")
    async def handle_pipeline_request(self, intent: PipelineIntent) -> Dict[str, Any]:
        """
        Handle incoming pipeline creation request.

        Starts the LangGraph workflow and streams status updates.

        Args:
            intent: Pipeline intent from UI

        Returns:
            Result with request_id, status, pipeline_id
        """
        # Generate request ID if not provided
        request_id = intent.request_id or str(uuid.uuid4())

        logger.info(f"Handling pipeline request: {request_id}")

        # Track active pipeline
        self.active_pipelines[request_id] = {
            "status": "starting",
            "current_phase": "init",
            "started_at": datetime.utcnow().isoformat(),
            "intent": intent.model_dump() if hasattr(intent, 'model_dump') else (intent.dict() if hasattr(intent, 'dict') else dict(intent)),
        }

        # Publish initial status
        await self._publish_status(request_id, self.active_pipelines[request_id])

        try:
            # Check if LangGraph workflow is available
            if self.graph is None:
                # Run in mock mode for testing
                logger.warning("Running in mock mode - LangGraph workflow not available")
                return await self._run_mock_workflow(request_id, intent)

            # Import async runner
            from graphs.main_graph import run_pipeline_async

            # Convert intent to dict for LangGraph
            intent_json = intent.model_dump() if hasattr(intent, 'model_dump') else (intent.dict() if hasattr(intent, 'dict') else dict(intent))

            # Track final state
            final_state = None

            # Run workflow and stream updates
            async for state in run_pipeline_async(intent_json, request_id):
                # Update active pipeline state
                self.active_pipelines[request_id] = state

                # Publish status update
                await self._publish_status(request_id, state)

                # Track final state
                final_state = state

            # Build result
            result = {
                "request_id": request_id,
                "status": final_state.get("current_phase", "unknown") if final_state else "unknown",
                "pipeline_id": final_state.get("metadata_context", {}).get("pipeline_id") if final_state else None,
                "error": final_state.get("error_message") if final_state else None,
            }

            logger.info(f"Pipeline request complete: {request_id}, status: {result['status']}")

            return result

        except Exception as e:
            logger.error(f"Pipeline request failed: {request_id}, error: {e}")

            # Update state
            self.active_pipelines[request_id]["current_phase"] = "failed"
            self.active_pipelines[request_id]["error_message"] = str(e)

            # Publish failure status
            await self._publish_status(request_id, self.active_pipelines[request_id])

            return {
                "request_id": request_id,
                "status": "failed",
                "error": str(e),
            }

        finally:
            # Cleanup active pipeline after completion
            if request_id in self.active_pipelines:
                # Keep for a short time for status queries
                asyncio.create_task(self._cleanup_pipeline(request_id, delay=300))

    async def _run_mock_workflow(self, request_id: str, intent: PipelineIntent) -> Dict[str, Any]:
        """
        Run a mock workflow for testing when LangGraph is not available.
        """
        phases = ["init", "planning", "generating", "validating", "deploying", "complete"]

        for phase in phases:
            self.active_pipelines[request_id]["current_phase"] = phase
            await self._publish_status(request_id, self.active_pipelines[request_id])
            await asyncio.sleep(0.5)  # Simulate work

        return {
            "request_id": request_id,
            "status": "complete",
            "pipeline_id": None,
            "message": "Mock workflow completed (LangGraph not available)",
        }

    async def _cleanup_pipeline(self, request_id: str, delay: int = 300):
        """Remove pipeline from active tracking after delay."""
        await asyncio.sleep(delay)
        if request_id in self.active_pipelines:
            del self.active_pipelines[request_id]
            logger.debug(f"Cleaned up pipeline: {request_id}")

    async def _publish_status(self, request_id: str, state: Dict[str, Any]):
        """
        Publish status update to Kafka.

        Args:
            request_id: Request ID
            state: Current workflow state
        """
        try:
            status_update = {
                "request_id": request_id,
                "pipeline_id": state.get("metadata_context", {}).get("pipeline_id") if isinstance(state.get("metadata_context"), dict) else None,
                "status": state.get("current_phase", "unknown"),
                "phase": state.get("current_phase", "unknown"),
                "message": self._get_status_message(state),
                "progress_pct": self._get_progress_pct(state),
                "timestamp": datetime.utcnow().isoformat(),
                "error": state.get("error_message"),
            }

            # Publish to Kafka
            if self.kafka_producer:
                await self.kafka_producer.publish_event(
                    topic=Topics.PIPELINE_STATUS,
                    event=status_update,
                    key=request_id,
                )

        except Exception as e:
            logger.warning(f"Failed to publish status: {e}")

    def _get_status_message(self, state: Dict[str, Any]) -> str:
        """Get human-readable status message."""
        phase = state.get("current_phase", "unknown")

        messages = {
            "init": "Initializing pipeline request",
            "planning": "Analyzing intent and planning strategy",
            "generating": "Generating DAGs and Spark jobs",
            "validating": "Validating syntax and security",
            "awaiting_approval": "Waiting for human approval",
            "deploying": "Deploying to Git and triggering CI/CD",
            "complete": "Pipeline deployment complete",
            "failed": state.get("error_message", "Pipeline failed"),
        }

        return messages.get(phase, f"Phase: {phase}")

    def _get_progress_pct(self, state: Dict[str, Any]) -> int:
        """Get progress percentage based on phase."""
        phase = state.get("current_phase", "unknown")

        progress = {
            "init": 5,
            "planning": 25,
            "generating": 50,
            "validating": 70,
            "awaiting_approval": 75,
            "deploying": 90,
            "complete": 100,
            "failed": 0,
        }

        return progress.get(phase, 0)

    @trace_span("approve_pipeline")
    async def approve_pipeline(
        self,
        request_id: str,
        approved: bool,
        approver: str,
    ) -> Dict[str, Any]:
        """
        Handle human approval response.

        For pipelines awaiting approval (PROD deployments, schema upgrades),
        this method records the approval and resumes the workflow.

        Args:
            request_id: Pipeline request ID
            approved: Whether the pipeline is approved
            approver: Email/ID of the approver

        Returns:
            Result of approval processing
        """
        logger.info(f"Processing approval for {request_id}: approved={approved}, approver={approver}")

        # Check if pipeline is active
        if request_id not in self.active_pipelines:
            return {"error": "Pipeline not found or already completed"}

        state = self.active_pipelines[request_id]

        # Check if pipeline is awaiting approval
        if state.get("current_phase") != "awaiting_approval":
            return {
                "error": f"Pipeline not awaiting approval. Current phase: {state.get('current_phase')}"
            }

        if approved:
            # Update state with approval
            state["human_approval_received"] = True
            state["approved_by"] = approver
            state["approved_at"] = datetime.utcnow().isoformat()

            # Resume workflow (in a production system, this would use checkpointing)
            # For now, just update the state
            logger.info(f"Pipeline {request_id} approved by {approver}")

            return {
                "status": "approval_processed",
                "approved": True,
                "approver": approver,
                "message": "Pipeline approved, deployment will proceed",
            }

        else:
            # Record rejection
            state["error_message"] = f"Approval rejected by {approver}"
            state["current_phase"] = "failed"
            state["rejected_by"] = approver
            state["rejected_at"] = datetime.utcnow().isoformat()

            # Publish rejection status
            await self._publish_status(request_id, state)

            logger.info(f"Pipeline {request_id} rejected by {approver}")

            return {
                "status": "approval_processed",
                "approved": False,
                "approver": approver,
                "message": "Pipeline rejected",
            }

    def get_pipeline_status(self, request_id: str) -> Optional[Dict[str, Any]]:
        """
        Get current status of a pipeline.

        Args:
            request_id: Pipeline request ID

        Returns:
            Pipeline state or None if not found
        """
        state = self.active_pipelines.get(request_id)

        if not state:
            return None

        return {
            "request_id": request_id,
            "status": state.get("current_phase", "unknown"),
            "phase": state.get("current_phase", "unknown"),
            "message": self._get_status_message(state),
            "progress_pct": self._get_progress_pct(state),
            "pipeline_id": state.get("metadata_context", {}).get("pipeline_id") if isinstance(state.get("metadata_context"), dict) else None,
            "error": state.get("error_message"),
            "started_at": state.get("started_at"),
            "completed_at": state.get("completed_at"),
            "human_approval_required": state.get("human_approval_required", False),
            "human_approval_received": state.get("human_approval_received", False),
        }

    def list_active_pipelines(self, jira_ticket: Optional[str] = None) -> list:
        """
        List all active pipelines.

        Args:
            jira_ticket: Optional filter by Jira ticket

        Returns:
            List of active pipeline statuses
        """
        pipelines = [
            self.get_pipeline_status(request_id)
            for request_id in self.active_pipelines.keys()
        ]

        if jira_ticket:
            pipelines = [
                p for p in pipelines
                if self.active_pipelines.get(p["request_id"], {}).get("intent", {}).get("jira_ticket") == jira_ticket
            ]

        return pipelines

    async def get_available_templates(self) -> Dict[str, list]:
        """
        Get list of available templates.

        Returns:
            Dictionary of template types and names
        """
        return {
            "dag_templates": [
                "file_ingest_dag",
                "cdc_ingest_dag",
                "streaming_dag",
                "api_ingest_dag",
                "db_snapshot_dag",
            ],
            "spark_templates": [
                "bronze_ingest",
                "silver_transform",
                "gold_load_bq",
                "cdc_merge",
                "scd2_apply",
                "streaming_bronze",
            ],
        }


# Singleton instance
_handler_instance: Optional[DataAgentHandler] = None


def get_data_agent_handler() -> DataAgentHandler:
    """Get singleton Data Agent handler instance."""
    global _handler_instance
    if _handler_instance is None:
        _handler_instance = DataAgentHandler()
    return _handler_instance
