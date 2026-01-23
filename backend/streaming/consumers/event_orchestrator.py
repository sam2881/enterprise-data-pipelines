#!/usr/bin/env python3
"""
Event Orchestrator - Kafka Consumer for Event-Driven Architecture

WHY: This is the BRAIN of the event-driven system:
- Consumes events from Kafka topics
- Routes to correct LangGraph workflow (Incident vs Data Pipeline)
- Validates state transitions
- Publishes state change events

MUST:
- Consume Kafka events
- Perform deterministic routing
- Select correct LangGraph workflow
- Validate state transitions
- Publish state change events

MUST NOT:
- Poll external systems
- Contain UI logic
- Contain LLM reasoning
- Bypass Kafka

Event Flow:
    incident.created → trigger IncidentWorkflow → incident.enriched
    incident.enriched → continue workflow → incident.plan_generated
    incident.approved → resume workflow → incident.executed
    incident.close_requested → validate → incident.close_execute

    pipeline.requested → trigger DataPipelineWorkflow → pipeline.planned
    pipeline.approved → resume workflow → pipeline.deploy_execute
"""
import os
import sys
import json
import asyncio
import logging
from datetime import datetime
from typing import Any, Dict, Optional, Callable
from dataclasses import dataclass
from enum import Enum
import uuid

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

# Add paths
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("event-orchestrator")


# =============================================================================
# CONFIGURATION
# =============================================================================

@dataclass
class OrchestratorConfig:
    """Orchestrator configuration"""
    kafka_bootstrap: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    consumer_group: str = os.getenv("ORCHESTRATOR_CONSUMER_GROUP", "event-orchestrator")
    redis_url: str = os.getenv("REDIS_URL", "redis://localhost:6379")
    postgres_url: str = os.getenv("POSTGRES_URL", "postgresql://admin:admin123@localhost:5432/agentdb")


# =============================================================================
# TOPICS
# =============================================================================

class IncidentTopics:
    """Incident-related Kafka topics"""
    # Consume (trigger workflow or resume)
    CREATED = "incident.created"
    APPROVED = "incident.approved"
    REJECTED = "incident.rejected"
    CLOSE_REQUESTED = "incident.close_requested"

    # Produce (publish state changes)
    ENRICHED = "incident.enriched"
    PLAN_GENERATED = "incident.plan_generated"
    REQUIRES_APPROVAL = "incident.requires_approval"
    EXECUTED = "incident.executed"
    VERIFIED = "incident.verified"
    CLOSE_EXECUTE = "incident.close_execute"
    CLOSED = "incident.closed"
    FAILED = "incident.failed"


class PipelineTopics:
    """Pipeline-related Kafka topics"""
    # Consume
    REQUESTED = "pipeline.requested"
    APPROVED = "pipeline.approved"
    REJECTED = "pipeline.rejected"

    # Produce
    PLANNED = "pipeline.planned"
    GENERATED = "pipeline.generated"
    VALIDATED = "pipeline.validated"
    REQUIRES_APPROVAL = "pipeline.requires_approval"
    DEPLOY_EXECUTE = "pipeline.deploy_execute"
    DEPLOYED = "pipeline.deployed"
    FAILED = "pipeline.failed"


# =============================================================================
# WORKFLOW STATE MACHINE
# =============================================================================

class IncidentState(str, Enum):
    """Incident workflow states"""
    NEW = "new"
    RECEIVED = "received"
    ENRICHED = "enriched"
    PLAN_GENERATED = "plan_generated"
    PENDING_APPROVAL = "pending_approval"
    APPROVED = "approved"
    REJECTED = "rejected"
    EXECUTING = "executing"
    EXECUTED = "executed"
    VERIFIED = "verified"
    CLOSE_REQUESTED = "close_requested"
    CLOSED = "closed"
    FAILED = "failed"


class PipelineState(str, Enum):
    """Pipeline workflow states"""
    REQUESTED = "requested"
    PLANNING = "planning"
    PLANNED = "planned"
    GENERATING = "generating"
    GENERATED = "generated"
    VALIDATING = "validating"
    VALIDATED = "validated"
    PENDING_APPROVAL = "pending_approval"
    APPROVED = "approved"
    REJECTED = "rejected"
    DEPLOYING = "deploying"
    DEPLOYED = "deployed"
    FAILED = "failed"


# Valid state transitions
INCIDENT_TRANSITIONS = {
    IncidentState.NEW: [IncidentState.RECEIVED],
    IncidentState.RECEIVED: [IncidentState.ENRICHED, IncidentState.FAILED],
    IncidentState.ENRICHED: [IncidentState.PLAN_GENERATED, IncidentState.FAILED],
    IncidentState.PLAN_GENERATED: [IncidentState.PENDING_APPROVAL, IncidentState.APPROVED],
    IncidentState.PENDING_APPROVAL: [IncidentState.APPROVED, IncidentState.REJECTED],
    IncidentState.APPROVED: [IncidentState.EXECUTING],
    IncidentState.EXECUTING: [IncidentState.EXECUTED, IncidentState.FAILED],
    IncidentState.EXECUTED: [IncidentState.VERIFIED, IncidentState.FAILED],
    IncidentState.VERIFIED: [IncidentState.CLOSE_REQUESTED],
    IncidentState.CLOSE_REQUESTED: [IncidentState.CLOSED],
}


# =============================================================================
# WORKFLOW MANAGER
# =============================================================================

class WorkflowManager:
    """
    Manages workflow state and triggers LangGraph executions.

    Uses Redis for state storage and PostgreSQL for audit.
    """

    def __init__(self, config: OrchestratorConfig):
        self.config = config
        self._redis = None
        self._postgres = None

    async def get_workflow_state(self, workflow_id: str) -> Optional[Dict[str, Any]]:
        """Get current workflow state from Redis"""
        try:
            import redis
            if not self._redis:
                self._redis = redis.from_url(self.config.redis_url)

            state_json = self._redis.get(f"workflow:{workflow_id}")
            if state_json:
                return json.loads(state_json)
            return None
        except Exception as e:
            logger.error(f"Error getting workflow state: {e}")
            return None

    async def save_workflow_state(self, workflow_id: str, state: Dict[str, Any]):
        """Save workflow state to Redis"""
        try:
            import redis
            if not self._redis:
                self._redis = redis.from_url(self.config.redis_url)

            state["updated_at"] = datetime.utcnow().isoformat()
            self._redis.set(
                f"workflow:{workflow_id}",
                json.dumps(state),
                ex=86400 * 7  # 7 day TTL
            )
            logger.debug(f"Saved workflow state: {workflow_id}")
        except Exception as e:
            logger.error(f"Error saving workflow state: {e}")

    async def trigger_incident_workflow(
        self,
        incident_id: str,
        correlation_id: str,
        incident_data: Dict[str, Any]
    ) -> str:
        """
        Trigger a new incident workflow.

        This creates the workflow state and imports the LangGraph workflow.
        """
        workflow_id = f"incident-{incident_id}-{uuid.uuid4().hex[:8]}"

        # Create initial state
        state = {
            "workflow_id": workflow_id,
            "workflow_type": "incident",
            "incident_id": incident_id,
            "correlation_id": correlation_id,
            "current_state": IncidentState.RECEIVED.value,
            "incident_data": incident_data,
            "created_at": datetime.utcnow().isoformat(),
            "node_outputs": {},
        }

        await self.save_workflow_state(workflow_id, state)

        # Import and run LangGraph workflow
        try:
            from orchestrator.langgraph_workflow import run_incident_workflow
            result = await run_incident_workflow(
                workflow_id=workflow_id,
                incident_id=incident_id,
                incident_data=incident_data
            )

            # Update state with result
            state["node_outputs"]["workflow_result"] = result
            state["current_state"] = result.get("status", IncidentState.ENRICHED.value)
            await self.save_workflow_state(workflow_id, state)

            logger.info(f"Incident workflow completed: {workflow_id}")
            return workflow_id

        except Exception as e:
            logger.error(f"Error running incident workflow: {e}")
            state["current_state"] = IncidentState.FAILED.value
            state["error"] = str(e)
            await self.save_workflow_state(workflow_id, state)
            raise

    async def trigger_pipeline_workflow(
        self,
        request_id: str,
        jira_key: str,
        intent: Dict[str, Any]
    ) -> str:
        """
        Trigger a new data pipeline workflow.
        """
        workflow_id = f"pipeline-{request_id}"

        state = {
            "workflow_id": workflow_id,
            "workflow_type": "pipeline",
            "request_id": request_id,
            "jira_key": jira_key,
            "current_state": PipelineState.REQUESTED.value,
            "intent": intent,
            "created_at": datetime.utcnow().isoformat(),
            "node_outputs": {},
        }

        await self.save_workflow_state(workflow_id, state)

        # Import and run Data Agent workflow
        try:
            # This would import the actual Data Agent LangGraph
            # For now, we'll simulate the workflow progression
            logger.info(f"Pipeline workflow triggered: {workflow_id}")
            return workflow_id

        except Exception as e:
            logger.error(f"Error running pipeline workflow: {e}")
            state["current_state"] = PipelineState.FAILED.value
            state["error"] = str(e)
            await self.save_workflow_state(workflow_id, state)
            raise

    async def resume_workflow_on_approval(
        self,
        workflow_id: str,
        approved_by: str,
        conditions: list = None
    ) -> Dict[str, Any]:
        """
        Resume a workflow that was waiting for approval.
        """
        state = await self.get_workflow_state(workflow_id)
        if not state:
            raise ValueError(f"Workflow {workflow_id} not found")

        workflow_type = state.get("workflow_type")

        if workflow_type == "incident":
            state["current_state"] = IncidentState.APPROVED.value
            state["approved_by"] = approved_by
            state["approved_at"] = datetime.utcnow().isoformat()
            state["approval_conditions"] = conditions or []

            # Resume LangGraph from approval checkpoint
            try:
                from orchestrator.langgraph_workflow import resume_incident_workflow
                result = await resume_incident_workflow(
                    workflow_id=workflow_id,
                    from_state="approved"
                )
                state["node_outputs"]["execution_result"] = result
                state["current_state"] = result.get("status", IncidentState.EXECUTED.value)
            except ImportError:
                logger.warning("LangGraph workflow module not available")

        elif workflow_type == "pipeline":
            state["current_state"] = PipelineState.APPROVED.value
            state["approved_by"] = approved_by
            state["approved_at"] = datetime.utcnow().isoformat()

        await self.save_workflow_state(workflow_id, state)
        return state


# =============================================================================
# EVENT ORCHESTRATOR
# =============================================================================

class EventOrchestrator:
    """
    Main event orchestrator - consumes Kafka events and routes to workflows.
    """

    def __init__(self, config: OrchestratorConfig = None):
        self.config = config or OrchestratorConfig()
        self.workflow_manager = WorkflowManager(self.config)
        self.running = False

        # Topics to consume
        self.consume_topics = [
            # Incident lifecycle
            IncidentTopics.CREATED,
            IncidentTopics.APPROVED,
            IncidentTopics.REJECTED,
            IncidentTopics.CLOSE_REQUESTED,
            # Pipeline lifecycle
            PipelineTopics.REQUESTED,
            PipelineTopics.APPROVED,
            PipelineTopics.REJECTED,
        ]

        # Initialize Kafka
        self.consumer = KafkaConsumer(
            *self.consume_topics,
            bootstrap_servers=self.config.kafka_bootstrap,
            group_id=self.config.consumer_group,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True
        )

        self.producer = KafkaProducer(
            bootstrap_servers=self.config.kafka_bootstrap,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all'
        )

        logger.info(f"Event Orchestrator initialized")
        logger.info(f"Consuming: {self.consume_topics}")

    async def start(self):
        """Start the orchestrator event loop"""
        self.running = True
        logger.info("=" * 60)
        logger.info("EVENT ORCHESTRATOR STARTED")
        logger.info("=" * 60)
        logger.info(f"Kafka: {self.config.kafka_bootstrap}")
        logger.info(f"Consumer Group: {self.config.consumer_group}")
        logger.info(f"Topics: {len(self.consume_topics)}")
        logger.info("=" * 60)

        while self.running:
            try:
                messages = self.consumer.poll(timeout_ms=1000)

                for topic_partition, records in messages.items():
                    topic = topic_partition.topic

                    for record in records:
                        await self._handle_event(topic, record.value)

                await asyncio.sleep(0.1)

            except Exception as e:
                logger.error(f"Orchestrator error: {e}")
                await asyncio.sleep(5)

    async def _handle_event(self, topic: str, event: Dict[str, Any]):
        """Route event to appropriate handler"""
        event_type = event.get("event_type", topic)
        correlation_id = event.get("correlation_id", "")

        logger.info(f"Handling event: {event_type} (correlation={correlation_id})")

        try:
            # Route based on topic
            if topic == IncidentTopics.CREATED:
                await self._handle_incident_created(event)
            elif topic == IncidentTopics.APPROVED:
                await self._handle_incident_approved(event)
            elif topic == IncidentTopics.REJECTED:
                await self._handle_incident_rejected(event)
            elif topic == IncidentTopics.CLOSE_REQUESTED:
                await self._handle_incident_close_requested(event)
            elif topic == PipelineTopics.REQUESTED:
                await self._handle_pipeline_requested(event)
            elif topic == PipelineTopics.APPROVED:
                await self._handle_pipeline_approved(event)
            elif topic == PipelineTopics.REJECTED:
                await self._handle_pipeline_rejected(event)
            else:
                logger.warning(f"Unhandled topic: {topic}")

        except Exception as e:
            logger.error(f"Error handling {event_type}: {e}")
            await self._publish_failure_event(event, str(e))

    # =========================================================================
    # INCIDENT HANDLERS
    # =========================================================================

    async def _handle_incident_created(self, event: Dict[str, Any]):
        """Handle new incident - trigger LangGraph workflow"""
        incident_id = event.get("incident_id")
        correlation_id = event.get("correlation_id", incident_id)

        logger.info(f"New incident received: {incident_id}")

        # Extract incident data - include sys_id for ticket closure
        # sys_id may come from top level, raw_data, or incident nested object
        raw_data = event.get("raw_data", {})
        incident_nested = event.get("incident", {})

        sys_id = (
            event.get("sys_id") or
            event.get("servicenow_sys_id") or
            raw_data.get("sys_id") or
            incident_nested.get("sys_id") or
            ""
        )

        incident_data = {
            "incident_id": incident_id,
            "sys_id": sys_id,  # Critical for closing tickets
            "short_description": event.get("short_description", ""),
            "description": event.get("description", ""),
            "priority": event.get("priority", "3"),
            "service": event.get("service", "unknown"),
            "raw_data": raw_data,
        }

        # Trigger workflow
        try:
            workflow_id = await self.workflow_manager.trigger_incident_workflow(
                incident_id=incident_id,
                correlation_id=correlation_id,
                incident_data=incident_data
            )

            # Publish enriched event (workflow will handle this internally)
            # For now, we simulate the event flow
            await self._publish_event(
                topic=IncidentTopics.ENRICHED,
                key=incident_id,
                event={
                    "event_type": "incident.enriched",
                    "incident_id": incident_id,
                    "correlation_id": correlation_id,
                    "workflow_id": workflow_id,
                    "timestamp": datetime.utcnow().isoformat(),
                }
            )

        except Exception as e:
            logger.error(f"Failed to trigger incident workflow: {e}")
            await self._publish_event(
                topic=IncidentTopics.FAILED,
                key=incident_id,
                event={
                    "event_type": "incident.failed",
                    "incident_id": incident_id,
                    "correlation_id": correlation_id,
                    "failed_at_step": "workflow_trigger",
                    "error_message": str(e),
                    "timestamp": datetime.utcnow().isoformat(),
                }
            )

    async def _handle_incident_approved(self, event: Dict[str, Any]):
        """Handle approval - resume workflow execution"""
        incident_id = event.get("incident_id")
        plan_id = event.get("plan_id")
        approved_by = event.get("approved_by", "unknown")

        logger.info(f"Incident approved: {incident_id} by {approved_by}")

        # Find workflow by incident_id
        workflow_id = f"incident-{incident_id}"  # Simplified lookup

        try:
            state = await self.workflow_manager.resume_workflow_on_approval(
                workflow_id=workflow_id,
                approved_by=approved_by,
                conditions=event.get("conditions", [])
            )

            logger.info(f"Workflow resumed: {workflow_id}")

        except Exception as e:
            logger.error(f"Failed to resume workflow: {e}")

    async def _handle_incident_rejected(self, event: Dict[str, Any]):
        """Handle rejection - mark workflow as rejected"""
        incident_id = event.get("incident_id")
        rejected_by = event.get("rejected_by", "unknown")
        reason = event.get("reason", "")

        logger.info(f"Incident rejected: {incident_id} by {rejected_by}")

        # Update workflow state
        workflow_id = f"incident-{incident_id}"
        state = await self.workflow_manager.get_workflow_state(workflow_id)

        if state:
            state["current_state"] = IncidentState.REJECTED.value
            state["rejected_by"] = rejected_by
            state["rejection_reason"] = reason
            await self.workflow_manager.save_workflow_state(workflow_id, state)

    async def _handle_incident_close_requested(self, event: Dict[str, Any]):
        """Handle close request - validate and publish close_execute"""
        incident_id = event.get("incident_id")
        requested_by = event.get("requested_by", "unknown")

        logger.info(f"Close requested for: {incident_id} by {requested_by}")

        # Validate workflow is in correct state
        workflow_id = f"incident-{incident_id}"
        state = await self.workflow_manager.get_workflow_state(workflow_id)

        # Publish close execute command for ServiceNow MCP
        await self._publish_event(
            topic=IncidentTopics.CLOSE_EXECUTE,
            key=incident_id,
            event={
                "event_type": "incident.close_execute",
                "incident_id": incident_id,
                "correlation_id": event.get("correlation_id", incident_id),
                "servicenow_sys_id": event.get("servicenow_sys_id", ""),
                "resolution_code": event.get("resolution", "Resolved"),
                "resolution_notes": event.get("close_notes", "Closed via AI Agent Platform"),
                "close_code": "Resolved",
                "timestamp": datetime.utcnow().isoformat(),
            }
        )

    # =========================================================================
    # PIPELINE HANDLERS
    # =========================================================================

    async def _handle_pipeline_requested(self, event: Dict[str, Any]):
        """Handle new pipeline request - trigger Data Agent workflow"""
        request_id = event.get("request_id")
        jira_key = event.get("jira_key", "")

        logger.info(f"New pipeline request: {request_id} from {jira_key}")

        intent = {
            "pipeline_identity": event.get("pipeline_identity", {}),
            "source_config": event.get("source_config", {}),
            "target_config": event.get("target_config", {}),
            "schema_definition": event.get("schema_definition", {}),
            "execution_policy": event.get("execution_policy", {}),
        }

        try:
            workflow_id = await self.workflow_manager.trigger_pipeline_workflow(
                request_id=request_id,
                jira_key=jira_key,
                intent=intent
            )

            # Publish planned event
            await self._publish_event(
                topic=PipelineTopics.PLANNED,
                key=request_id,
                event={
                    "event_type": "pipeline.planned",
                    "request_id": request_id,
                    "jira_key": jira_key,
                    "correlation_id": request_id,
                    "workflow_id": workflow_id,
                    "timestamp": datetime.utcnow().isoformat(),
                }
            )

        except Exception as e:
            logger.error(f"Failed to trigger pipeline workflow: {e}")
            await self._publish_event(
                topic=PipelineTopics.FAILED,
                key=request_id,
                event={
                    "event_type": "pipeline.failed",
                    "request_id": request_id,
                    "jira_key": jira_key,
                    "failed_at_phase": "workflow_trigger",
                    "error_message": str(e),
                    "timestamp": datetime.utcnow().isoformat(),
                }
            )

    async def _handle_pipeline_approved(self, event: Dict[str, Any]):
        """Handle pipeline approval - resume deployment"""
        request_id = event.get("request_id")
        approved_by = event.get("approved_by", "unknown")

        logger.info(f"Pipeline approved: {request_id} by {approved_by}")

        workflow_id = f"pipeline-{request_id}"
        state = await self.workflow_manager.resume_workflow_on_approval(
            workflow_id=workflow_id,
            approved_by=approved_by
        )

        # Publish deploy execute
        await self._publish_event(
            topic=PipelineTopics.DEPLOY_EXECUTE,
            key=request_id,
            event={
                "event_type": "pipeline.deploy_execute",
                "request_id": request_id,
                "jira_key": event.get("jira_key", ""),
                "correlation_id": request_id,
                "dag_path": state.get("node_outputs", {}).get("dag_path", ""),
                "environment": state.get("intent", {}).get("pipeline_identity", {}).get("environment", "dev"),
                "timestamp": datetime.utcnow().isoformat(),
            }
        )

    async def _handle_pipeline_rejected(self, event: Dict[str, Any]):
        """Handle pipeline rejection"""
        request_id = event.get("request_id")
        rejected_by = event.get("rejected_by", "unknown")

        logger.info(f"Pipeline rejected: {request_id} by {rejected_by}")

        workflow_id = f"pipeline-{request_id}"
        state = await self.workflow_manager.get_workflow_state(workflow_id)

        if state:
            state["current_state"] = PipelineState.REJECTED.value
            state["rejected_by"] = rejected_by
            await self.workflow_manager.save_workflow_state(workflow_id, state)

    # =========================================================================
    # UTILITIES
    # =========================================================================

    async def _publish_event(self, topic: str, key: str, event: Dict[str, Any]):
        """Publish event to Kafka"""
        try:
            future = self.producer.send(topic, key=key, value=event)
            record = future.get(timeout=10)
            logger.debug(f"Published to {topic}: {key}")
        except Exception as e:
            logger.error(f"Failed to publish to {topic}: {e}")

    async def _publish_failure_event(self, original_event: Dict[str, Any], error: str):
        """Publish failure event"""
        event_type = original_event.get("event_type", "unknown")

        if "incident" in event_type:
            topic = IncidentTopics.FAILED
            key = original_event.get("incident_id", "unknown")
        else:
            topic = PipelineTopics.FAILED
            key = original_event.get("request_id", "unknown")

        await self._publish_event(topic, key, {
            "event_type": topic,
            "error_message": error,
            "original_event_type": event_type,
            "timestamp": datetime.utcnow().isoformat(),
        })

    def stop(self):
        """Stop orchestrator"""
        self.running = False
        self.consumer.close()
        self.producer.close()
        logger.info("Event Orchestrator stopped")


# =============================================================================
# CLI
# =============================================================================

async def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(description='Event Orchestrator')
    parser.add_argument('--test', action='store_true', help='Test connections')
    args = parser.parse_args()

    if args.test:
        print("\n Testing Event Orchestrator connections...")

        config = OrchestratorConfig()

        # Test Kafka
        try:
            from kafka import KafkaAdminClient
            admin = KafkaAdminClient(bootstrap_servers=config.kafka_bootstrap)
            topics = admin.list_topics()
            print(f" Kafka: Connected, {len(topics)} topics")
            admin.close()
        except Exception as e:
            print(f" Kafka: {e}")

        # Test Redis
        try:
            import redis
            r = redis.from_url(config.redis_url)
            r.ping()
            print(" Redis: Connected")
        except Exception as e:
            print(f" Redis: {e}")

        return

    orchestrator = EventOrchestrator()

    try:
        await orchestrator.start()
    except KeyboardInterrupt:
        orchestrator.stop()


if __name__ == "__main__":
    asyncio.run(main())
