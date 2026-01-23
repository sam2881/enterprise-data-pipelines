# =============================================================================
# Unified Dockerfile for AI Agent Platform
# =============================================================================
# WHY: Single Dockerfile for all Python services (backend, agents, MCP servers)
# HOW: Multi-target builds with shared base image and service-specific configs
#
# SERVICES:
#   - orchestrator:      Backend orchestrator API (port 8000)
#   - servicenow-agent:  ServiceNow incident management (port 8010)
#   - data-agent:        Data pipeline generation (port 8001)
#   - servicenow-mcp:    ServiceNow MCP server (polls + consumes)
#   - jira-mcp:          Jira MCP server (polls + consumes)
#
# USAGE:
#   # Build all services
#   docker build -t ai-agent-platform .
#
#   # Run specific service
#   docker run -e SERVICE=orchestrator ai-agent-platform
#   docker run -e SERVICE=data-agent ai-agent-platform
#   docker run -e SERVICE=servicenow-mcp ai-agent-platform
#
#   # Or use docker-compose with build targets
# =============================================================================

FROM python:3.11-slim AS base

# Set common environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    git \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# =============================================================================
# Dependencies Stage
# =============================================================================
FROM base AS deps

# Copy and install main requirements
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy data agent requirements if they exist and install additional deps
COPY agents/data_agent/requirements.txt /app/agents/data_agent/requirements.txt
RUN pip install --no-cache-dir -r /app/agents/data_agent/requirements.txt 2>/dev/null || true

# =============================================================================
# Application Stage
# =============================================================================
FROM deps AS app

# Copy all source code
COPY backend/ /app/backend/
COPY agents/ /app/agents/
COPY mcp-servers/ /app/mcp-servers/

# Copy configuration files
COPY registry.json /app/registry.json 2>/dev/null || true

# Copy templates and prompts for data agent
COPY agents/data_agent/src/templates/ /app/agents/data_agent/src/templates/ 2>/dev/null || true
COPY agents/data_agent/prompts/ /app/agents/data_agent/prompts/ 2>/dev/null || true

# Create necessary directories
RUN mkdir -p /app/output /app/templates /opt/airflow/dags

# =============================================================================
# Service Entrypoint
# =============================================================================

# Default service (can be overridden)
ENV SERVICE=orchestrator

# Copy entrypoint script
COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

# Health check (generic - services expose their own health endpoints)
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:${PORT:-8000}/health || exit 1

# Expose common ports
EXPOSE 8000 8001 8010

ENTRYPOINT ["/docker-entrypoint.sh"]
