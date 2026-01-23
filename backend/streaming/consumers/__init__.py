"""
Kafka Consumers - Event-Driven Architecture

WHY: These consumers implement the Kafka-first event-driven model:
- Consume events from Kafka topics
- Route to appropriate LangGraph workflows
- Publish state transition events

CONSUMERS:
- EventOrchestrator: Main router - consumes all events, routes to workflows
- IncidentConsumer: Legacy - processes servicenow.incidents directly
- DataPipelineConsumer: Legacy - processes pipeline.requested events

ARCHITECTURE:
    MCPs poll external systems → publish to Kafka
    EventOrchestrator consumes → triggers LangGraph
    LangGraph processes → publishes state events
    FastAPI reads → serves UI
"""
# Import main orchestrator
try:
    from .event_orchestrator import EventOrchestrator, OrchestratorConfig
    __all__ = ["EventOrchestrator", "OrchestratorConfig"]
except ImportError:
    __all__ = []

# Legacy consumers (for backward compatibility)
# These will be deprecated in favor of EventOrchestrator
