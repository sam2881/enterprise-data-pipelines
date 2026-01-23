# =============================================================================
# Data Pipeline Agent Dockerfile
# =============================================================================
# WHY: Containerizes the Data Agent for pipeline generation
# HOW: Python 3.11 + LangGraph + Jinja2 templates
# =============================================================================

FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY agents/data_agent/requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code (context is project root)
COPY agents/data_agent/src/ ./src/
COPY agents/data_agent/prompts/ ./prompts/

# Create directories for output
RUN mkdir -p /app/output

# Environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8001/health || exit 1

# Expose port
EXPOSE 8001

# Default command - run the API server
CMD ["python", "-m", "uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8001"]
