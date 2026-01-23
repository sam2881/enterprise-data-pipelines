#!/bin/bash
# =============================================================================
# Docker Entrypoint for AI Agent Platform
# =============================================================================
# WHY: Single entrypoint that routes to different services based on SERVICE env
# HOW: Case statement routes to appropriate Python module
# =============================================================================

set -e

echo "=========================================="
echo "AI Agent Platform - Starting Service"
echo "=========================================="
echo "Service: ${SERVICE:-orchestrator}"
echo "Environment: ${ENVIRONMENT:-dev}"
echo "=========================================="

case "${SERVICE}" in
    # Backend Orchestrator (main API)
    orchestrator)
        echo "Starting Backend Orchestrator on port ${PORT:-8000}..."
        exec python -m uvicorn backend.app:app \
            --host 0.0.0.0 \
            --port ${PORT:-8000} \
            ${UVICORN_RELOAD:+--reload}
        ;;

    # ServiceNow Agent (backend mode for incident processing)
    servicenow-agent)
        echo "Starting ServiceNow Agent on port ${PORT:-8010}..."
        exec python -m uvicorn backend.app:app \
            --host 0.0.0.0 \
            --port ${PORT:-8010} \
            ${UVICORN_RELOAD:+--reload}
        ;;

    # Data Pipeline Agent
    data-agent)
        echo "Starting Data Agent on port ${PORT:-8001}..."
        cd /app/agents/data_agent
        exec python -m uvicorn src.api.main:app \
            --host 0.0.0.0 \
            --port ${PORT:-8001} \
            ${UVICORN_RELOAD:+--reload}
        ;;

    # ServiceNow MCP Server (event-driven poller + consumer)
    servicenow-mcp)
        echo "Starting ServiceNow MCP Server..."
        exec python /app/mcp-servers/servicenow-mcp/event_driven_server.py
        ;;

    # Jira MCP Server (event-driven poller + consumer)
    jira-mcp)
        echo "Starting Jira MCP Server..."
        exec python /app/mcp-servers/jira-mcp/event_driven_server.py
        ;;

    # Kafka Consumer (incident consumer from backend)
    incident-consumer)
        echo "Starting Incident Consumer..."
        exec python -m backend.streaming.consumers.incident_consumer
        ;;

    # Pipeline Consumer (for data agent)
    pipeline-consumer)
        echo "Starting Pipeline Consumer..."
        exec python -m backend.streaming.consumers.pipeline_consumer
        ;;

    # Custom command (pass through)
    custom)
        echo "Running custom command: $@"
        exec "$@"
        ;;

    *)
        echo "Unknown service: ${SERVICE}"
        echo ""
        echo "Available services:"
        echo "  - orchestrator       Backend Orchestrator API (port 8000)"
        echo "  - servicenow-agent   ServiceNow Agent API (port 8010)"
        echo "  - data-agent         Data Pipeline Agent API (port 8001)"
        echo "  - servicenow-mcp     ServiceNow MCP Server"
        echo "  - jira-mcp           Jira MCP Server"
        echo "  - incident-consumer  Kafka Incident Consumer"
        echo "  - pipeline-consumer  Kafka Pipeline Consumer"
        echo "  - custom             Run custom command"
        echo ""
        echo "Usage: docker run -e SERVICE=<service-name> ai-agent-platform"
        exit 1
        ;;
esac
