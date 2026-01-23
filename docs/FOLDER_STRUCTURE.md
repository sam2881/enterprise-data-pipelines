# AI Agent Platform - Folder Structure Guide

## Overview

This document describes the standardized folder structure for the AI Agent Platform.
The structure is organized by **domain** and **responsibility** to ensure:
- Clear separation of concerns
- Easy navigation
- Minimal coupling between modules

## Backend Structure

```
backend/
├── app.py                    # FastAPI application entry point
├── Dockerfile                # Container build configuration
│
├── agents/                   # Domain-organized AI agents
│   ├── __init__.py          # Agent module documentation
│   ├── base_agent.py        # Abstract base class for agents
│   │
│   ├── it_service/          # ServiceNow incident handling
│   │   ├── servicenow_agent.py    # Incident intake from ServiceNow
│   │   ├── remediation_agent.py   # Script execution and remediation
│   │   └── enterprise_matcher.py  # Hybrid script matching (TF-IDF + semantic)
│   │
│   ├── data_pipeline/       # Jira → Data Pipeline generation
│   │   ├── jira_agent.py    # Jira story processing
│   │   ├── jira_client.py   # Jira REST API wrapper
│   │   └── pipeline_agent.py # Spark/DAG/DQ code generation
│   │
│   ├── infrastructure/      # GCP infrastructure management
│   │   ├── gcp_agent.py     # GCP resource monitoring
│   │   ├── enhanced_agent.py # LLM-powered infrastructure analysis
│   │   └── vm_recovery_agent.py # Autonomous VM recovery
│   │
│   └── a2a/                 # Agent-to-Agent protocol (deprecated - use protocols/)
│       ├── client.py
│       ├── mesh.py
│       └── messages.py
│
├── config/                   # Configuration management
│   ├── settings.py          # Pydantic settings with environment support
│   ├── thresholds.py        # Confidence thresholds for auto-approval
│   └── environments/        # Environment-specific configs (local/dev/prod)
│
├── control_plane/           # Central workflow orchestration
│   ├── __init__.py          # Module exports
│   ├── orchestrator.py      # Saga-pattern workflow coordinator
│   ├── policy_engine.py     # Risk assessment and approval routing
│   └── handlers/            # Workflow-specific handlers
│       ├── it_service_handler.py
│       └── data_pipeline_handler.py
│
├── governance/              # Compliance and audit
│   └── ...
│
├── guardrails/              # LLM input/output validation
│   └── llm_guardrails.py
│
├── infrastructure/          # External service clients
│   ├── __init__.py          # Module exports with documentation
│   ├── dataproc_client.py   # GCP Dataproc for Spark jobs
│   ├── kafka_client.py      # Kafka producer/consumer
│   ├── redis_client.py      # Redis caching with embedding support
│   ├── postgres_client.py   # PostgreSQL state persistence
│   └── circuit_breaker.py   # Fault tolerance patterns
│
├── mcp/                     # Model Context Protocol servers
│   ├── __init__.py
│   └── servers/
│       └── ...
│
├── observability/           # Unified observability
│   └── __init__.py          # Metrics, tracing, structured logging
│
├── orchestrator/            # FastAPI service layer (legacy)
│   ├── main.py              # REST API endpoints
│   ├── metrics.py           # Prometheus metrics
│   ├── llm_intelligence.py  # LLM integration
│   └── services/
│
├── protocols/               # Inter-agent communication protocols
│   └── a2a/                 # Agent-to-Agent protocol
│       ├── __init__.py
│       ├── client.py
│       ├── mesh.py
│       └── messages.py
│
├── rag/                     # Retrieval-Augmented Generation
│   ├── embedding_service.py
│   ├── hybrid_search_engine.py
│   ├── intelligent_retriever.py
│   └── agents/              # RAG-specific agents
│
├── runbooks/                # Remediation scripts and templates
│   ├── scripts/             # Python/Bash scripts
│   ├── terraform/           # Infrastructure as Code
│   ├── ansible/             # Configuration management
│   ├── kubernetes/          # K8s manifests
│   └── pipelines/           # CI/CD pipelines
│
├── secrets/                 # Secret management
│   └── manager.py           # GCP Secret Manager integration
│
├── streaming/               # Kafka event streaming
│   ├── __init__.py          # Event publisher exports
│   ├── event_publisher.py   # Unified event publishing
│   ├── jira_consumer.py     # Jira → Data Pipeline consumer
│   └── consumers/           # Additional consumers
│
└── utils/                   # Shared utilities
    ├── circuit_breaker.py   # (deprecated - use infrastructure/)
    ├── cost_tracker.py      # LLM cost tracking
    ├── github_actions.py    # GitHub API integration
    ├── otel_tracing.py      # OpenTelemetry setup
    └── slack_notifier.py    # Slack notifications
```

## Key Design Principles

### 1. Domain Organization
Agents are organized by the domain they serve:
- **it_service**: ServiceNow → Diagnosis → Remediation
- **data_pipeline**: Jira → Code Generation → Deployment
- **infrastructure**: Monitoring → Assessment → Recovery

### 2. Separation of Concerns
- **control_plane**: Orchestration, policy, approvals
- **infrastructure**: External service clients
- **rag**: Retrieval and search
- **streaming**: Event processing

### 3. Import Patterns
```python
# Infrastructure clients
from infrastructure import get_redis_client, get_kafka_producer
from infrastructure import DataprocClient, CircuitBreaker

# Control plane
from control_plane import get_control_plane, WorkflowType
from control_plane import PolicyEngine, ApprovalRoute

# Agents (import from submodules)
from agents.it_service import ServiceNowAgent, RemediationAgent
from agents.data_pipeline import JiraAgent, PipelineAgent
from agents.infrastructure import GCPAgent, VMRecoveryAgent

# Config
from config.settings import get_settings
```

### 4. Workflow Patterns

**IT Service Workflow:**
```
ServiceNow → control_plane → it_service/servicenow_agent
                          → it_service/enterprise_matcher (RAG)
                          → it_service/remediation_agent
                          → infrastructure/runbooks
```

**Data Pipeline Workflow:**
```
Jira → streaming/jira_consumer → control_plane
                              → data_pipeline/pipeline_agent
                              → infrastructure/dataproc_client
                              → GitHub (MR creation)
```

## Migration Notes

### Deprecated Locations
- `agents/control_plane.py` → `control_plane/policy_engine.py`
- `agents/infra/` → `agents/infrastructure/`
- `agents/servicenow/` → `agents/it_service/`
- `agents/remediation/` → `agents/it_service/`
- `agents/jira/` → `agents/data_pipeline/`
- `agents/data/` → `agents/data_pipeline/`
- `utils/redis_client.py` → `infrastructure/redis_client.py`

### Backward Compatibility
Old imports will fail. Update imports to use new locations.

## Adding New Components

### New Agent
1. Identify the domain (it_service, data_pipeline, infrastructure, or new)
2. Create folder under `agents/<domain>/`
3. Add `__init__.py` with exports
4. Implement agent inheriting from `BaseAgent`
5. Register with control_plane if workflow orchestration needed

### New Infrastructure Client
1. Add client file to `infrastructure/`
2. Export in `infrastructure/__init__.py`
3. Follow singleton pattern with `get_*()` function

### New Consumer
1. Add to `streaming/consumers/`
2. Follow pattern from `jira_consumer.py`
3. Use `EventPublisher` for publishing events
