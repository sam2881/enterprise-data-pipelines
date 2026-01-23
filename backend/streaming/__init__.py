"""
Streaming Module - Kafka-based Event Streaming

ARCHITECTURE:
============

1. ServiceNow → ServiceNow Agent (IT Service)
   - Platform incidents, infra issues, runtime failures
   - Final step: Update RAG knowledge base

2. Jira → Data Agent ONLY
   - STRICT SCOPE: Data pipeline build/change/enhancement ONLY
   - Final step: Create GitHub Merge Request
   - CI/CD validates and deploys to Airflow

3. Kafka (Mandatory Middleware)
   - Decoupling, reliability, retry/replay
   - Backpressure handling

COMPONENTS:
- event_publisher.py: Unified event publishing (new)
- kafka_producer.py: Legacy Kafka producer

TOPICS:
- workflow.events: Workflow lifecycle events
- workflow.approvals: Pending approval events
- incident.created: ServiceNow incident events
- pipeline.requested: Pipeline generation requests
- agent.events: Agent actions and decisions
"""

# New production event publisher
from .event_publisher import (
    EventPublisher,
    get_publisher,
    Event,
    WorkflowEvent,
    IncidentEvent,
    PipelineEvent,
    WorkflowEventType,
    IncidentEventType,
    PipelineEventType,
)

__all__ = [
    # Event Publisher
    "EventPublisher",
    "get_publisher",
    "Event",
    "WorkflowEvent",
    "IncidentEvent",
    "PipelineEvent",
    "WorkflowEventType",
    "IncidentEventType",
    "PipelineEventType",
]
