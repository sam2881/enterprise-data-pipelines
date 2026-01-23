# Backend Services Dockerfile
#
# WHY: Builds container for running backend services including:
# - Kafka consumers (incident_consumer, jira_consumer, pipeline_consumer)
# - Backend orchestrator API
#
# USAGE:
#   docker build -t ai-agent-backend -f backend/Dockerfile .
#   docker run -d --name incident-consumer ai-agent-backend python -m backend.streaming.incident_consumer

FROM python:3.11-slim

WORKDIR /app

# Install system dependencies including build tools for hdbscan
RUN apt-get update && apt-get install -y \
    curl \
    git \
    gcc \
    g++ \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy backend code
COPY backend /app/backend

# Copy platform_services (shared infrastructure clients)
COPY platform_services /app/platform_services

# Copy agents module
COPY agents /app/agents

# Copy registry files
COPY registry.json /app/registry.json

# Set Python path
ENV PYTHONPATH=/app

# Expose ports
# 8000: Orchestrator API
EXPOSE 8000

# Health check (for orchestrator mode)
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Default command (orchestrator API)
CMD ["python", "-m", "uvicorn", "backend.orchestrator.main:app", "--host", "0.0.0.0", "--port", "8000"]
