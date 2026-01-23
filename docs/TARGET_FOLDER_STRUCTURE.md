# AI Agent Platform - Clean Architecture Structure

## Core Principles

1. **Agent Isolation**: Agent logic lives ONLY in `agents/` - never in backend
2. **Platform as Shared Layer**: Infrastructure, protocols, runbooks are shared via `platform_services/`
3. **Symmetric Agents**: Data Agent and ServiceNow Agent live side-by-side in `agents/`
4. **Backend is Runtime**: Backend contains orchestration and APIs, not reasoning

## Top-Level Structure (Clear Ownership)

```
ai_agent_app/
│
├── agents/                    # ALL agent logic (data_agent + servicenow_agent)
│   ├── shared/               # Common base classes and interfaces
│   ├── data_agent/           # Data Engineering Agent
│   └── servicenow_agent/     # ServiceNow Platform Agent
│
├── backend/                   # Application runtime & APIs only
│
├── platform_services/         # Shared platform capabilities (named to avoid stdlib conflict)
│
├── frontend/                  # UI only
│
├── infrastructure/            # Deployment & cloud setup
│
├── monitoring/                # Observability configs
│
├── tests/                     # Integration & E2E tests
│
├── docs/                      # Architecture & compliance docs
│
├── scripts/                   # Dev & CI helpers
│
├── .env.example
├── README.md
└── Makefile
```

## Agents Layer (Symmetric & Isolated)

```
agents/
│
├── __init__.py                # Package exports, registry
├── registry.py                # Agent discovery & registration
│
├── shared/                    # Common agent infrastructure
│   ├── __init__.py
│   ├── base.py               # BaseAgent class (all agents extend)
│   ├── interfaces.py         # IAgentService, IAgentTask, IAgentResult
│   ├── config.py             # AgentConfig
│   └── types.py              # Shared types (RiskLevel, etc.)
│
├── servicenow_agent/          # ServiceNow Platform Agent
│   ├── __init__.py
│   ├── service.py            # Unified service (routes to sub-agents)
│   │
│   ├── it_service/           # Incident management
│   │   ├── __init__.py
│   │   ├── incident_agent.py # Triage & classification
│   │   └── matcher_agent.py  # Script matching
│   │
│   ├── remediation/          # Script execution
│   │   ├── __init__.py
│   │   └── remediation_agent.py
│   │
│   └── infrastructure/       # GCP operations (optional)
│       ├── __init__.py
│       ├── gcp_agent.py
│       └── vm_recovery_agent.py
│
└── data_agent/                # Data Engineering Agent (LIVES INSIDE agents/)
    │
    ├── src/
    │   ├── __init__.py
    │   ├── api.py            # Agent interface (called by backend)
    │   │
    │   ├── agents/           # Pipeline agents
    │   │   ├── __init__.py
    │   │   ├── base_agent.py # Data-specific base (extends shared.BaseAgent)
    │   │   ├── analysis_agent.py
    │   │   ├── planning_agent.py
    │   │   ├── spark_generator_agent.py
    │   │   ├── dag_generator_agent.py
    │   │   └── dq_generator_agent.py
    │   │
    │   ├── orchestration/    # Internal workflow
    │   │   ├── __init__.py
    │   │   ├── workflow.py
    │   │   └── saga.py
    │   │
    │   ├── metadata/         # Pipeline metadata
    │   ├── quality/          # Great Expectations
    │   └── templates/        # Code templates
    │
    ├── pipelines/            # Generated DAGs
    ├── tests/
    ├── docs/
    ├── Dockerfile
    └── requirements.txt
```

NOTE: A2A protocol is NOT in agents/ - it lives in platform_services/protocols/a2a/

## Backend (NO Agent Logic)

```
backend/
│
├── app.py                    # FastAPI application factory
├── Dockerfile
│
├── api/                      # REST endpoints only
│   ├── __init__.py
│   ├── routes/
│   │   ├── incidents.py
│   │   ├── pipelines.py
│   │   └── health.py
│   └── middleware/
│
├── services/                 # Business logic (uses agents via interface)
│   ├── __init__.py
│   └── agent_service.py     # Backend → Agent interaction
│
├── orchestrator/             # Workflow engine
│   ├── __init__.py
│   ├── main.py              # LangGraph workflow entry
│   ├── state_manager.py
│   └── llm_judge.py
│
├── control_plane/            # Policy & approval
│   ├── __init__.py
│   ├── orchestrator.py      # Saga coordinator
│   ├── policy_engine.py     # Risk assessment
│   └── handlers/
│
├── rag/                      # RAG runtime (no agent logic)
│   ├── __init__.py
│   ├── search_engine.py
│   ├── embedding_service.py
│   └── indexers/
│
├── streaming/                # Kafka consumers/producers
│   ├── __init__.py
│   ├── event_publisher.py
│   └── consumers/
│
├── governance/               # Compliance & audit
├── observability/            # Metrics, tracing
├── config/                   # Settings
├── secrets/                  # GCP Secret Manager
│
└── infrastructure/           # RE-EXPORTS from platform (backward compat)
    └── __init__.py          # Points to platform.infrastructure_clients
```

## Platform Layer (Shared Capabilities)

NOTE: Named `platform_services` to avoid conflict with Python's stdlib `platform` module.

```
platform_services/
│
├── __init__.py              # Re-exports common utils
│
├── infrastructure_clients/   # GCP, Kafka, DB clients (SINGLE SOURCE)
│   ├── __init__.py
│   ├── dataproc_client.py
│   ├── kafka_client.py
│   ├── redis_client.py
│   ├── postgres_client.py
│   └── circuit_breaker.py
│
├── protocols/                # A2A, schemas, contracts (SINGLE SOURCE)
│   ├── __init__.py
│   └── a2a/
│       ├── __init__.py
│       ├── client.py
│       ├── mesh.py
│       └── messages.py
│
├── runbooks/                 # Scripts + registry (SINGLE SOURCE)
│   ├── __init__.py
│   ├── registry.json        # Script metadata
│   ├── scripts/
│   ├── ansible/
│   ├── terraform/
│   └── kubernetes/
│
├── metadata/                 # Shared data models
│   └── __init__.py          # IncidentMetadata, PipelineMetadata
│
└── utils/                    # Common utilities
    └── __init__.py          # Logging, hashing, config
```

## Frontend (Unchanged)

```
frontend/
│
├── src/
│   ├── app/                  # Next.js pages
│   ├── components/           # React components
│   ├── lib/                  # Utilities
│   └── types/                # TypeScript types
│
├── public/
├── package.json
└── Dockerfile
```

## Infrastructure (Pure Ops)

```
infrastructure/
│
├── docker/
│   ├── docker-compose.yml
│   ├── docker-compose.dev.yml
│   └── docker-compose.prod.yml
│
├── kubernetes/
│   ├── base/
│   └── overlays/
│
├── terraform/
│   ├── gcp/
│   └── modules/
│
├── scripts/
│   ├── setup-gcp.sh
│   ├── setup-secrets.sh
│   └── setup-dataproc.sh
│
└── init/
    └── postgres/
```

## Import Rules

### From Agents
```python
# Agents import from shared and platform_services only
from agents.shared import BaseAgent, IAgentTask
from platform_services.infrastructure_clients import get_redis_client
from platform_services.protocols import A2AClient

# NEVER import from backend
# from backend.rag import ...  # ❌ WRONG
```

### From Backend
```python
# Backend imports from agents via interface
from agents import get_agent_registry, IAgentTask
from backend.services import get_backend_agent_service

# Use platform_services for infrastructure (or backward-compat backend.infrastructure)
from platform_services.infrastructure_clients import get_kafka_producer
from platform_services.runbooks import get_script_registry

# Backward compatible (existing code)
from backend.infrastructure import get_redis_client  # Re-exports from platform_services
```

### From Data Agent
```python
# Data agent is inside agents/ and can use platform_services
from platform_services.infrastructure_clients import DataprocClient
from platform_services import get_logger
```

## Key Benefits

1. **Agents are first-class citizens**, not backend helpers
2. **Backend is a runtime**, not a brain
3. **Both agents are symmetric** → easier onboarding
4. **Platform removes duplication** → single source of truth
5. **Easy to scale** → add third agent, replace Kafka, swap UI
6. **Production-grade** → looks like a real enterprise system
