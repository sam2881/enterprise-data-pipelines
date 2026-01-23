# AI Agent Platform - Folder Reference

> **Last Updated**: 2026-01-19
> **Version**: 6.0 (Event-Driven Architecture)
> **Purpose**: Complete reference for all folders and their contents

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                     AI AGENT PLATFORM v6.0                          │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│   ┌─────────┐    ┌─────────┐    ┌──────────────┐    ┌──────────┐   │
│   │ Frontend│───>│  Kafka  │───>│ LangGraph    │───>│ MCP      │   │
│   │ (Next)  │<───│ (Events)│<───│ (Orchestrate)│<───│ Servers  │   │
│   └─────────┘    └─────────┘    └──────────────┘    └──────────┘   │
│                       │                 │                │          │
│                       v                 v                v          │
│                  ┌─────────┐      ┌──────────┐    ┌──────────┐     │
│                  │ Backend │      │  Agents  │    │ External │     │
│                  │ (FastAPI│      │ (Service │    │ (SNow,   │     │
│                  │  + RAG) │      │   + Data)│    │ Jira,GCP)│     │
│                  └─────────┘      └──────────┘    └──────────┘     │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Folder Map

| Folder | Purpose | README |
|--------|---------|--------|
| [`backend/`](#backend) | FastAPI + LangGraph + RAG | [backend/README.md](backend/README.md) |
| [`agents/`](#agents) | Agent implementations | [agents/README.md](agents/README.md) |
| [`mcp-servers/`](#mcp-servers) | External system integrations | [mcp-servers/README.md](mcp-servers/README.md) |
| [`frontend/`](#frontend) | Next.js UI | [frontend/src/README.md](frontend/src/README.md) |
| [`platform_services/`](#platform_services) | Shared infrastructure | [platform_services/README.md](platform_services/README.md) |
| [`scripts/`](#scripts) | Utility scripts | [scripts/README.md](scripts/README.md) |
| [`tests/`](#tests) | Test suite | [tests/README.md](tests/README.md) |
| [`docs/`](#docs) | Documentation | - |
| [`monitoring/`](#monitoring) | Prometheus + Grafana | - |
| [`infrastructure/`](#infrastructure) | Terraform IaC | - |

---

## Root Files

| File | Purpose |
|------|---------|
| `CLAUDE.md` | Claude Code instructions |
| `PROJECT_SETUP.md` | Setup guide |
| `Dockerfile` | Main container build |
| `docker-compose.yml` | Service orchestration |
| `docker-entrypoint.sh` | Container entrypoint |
| `requirements.txt` | Python dependencies |
| `registry.json` | Script registry |
| `.env` | Environment variables |
| `.env.example` | Environment template |
| `.pre-commit-config.yaml` | Pre-commit hooks |
| `.gitignore` | Git ignore rules |

---

## backend/

**Purpose**: FastAPI backend, LangGraph workflow, RAG search

**Key Components**:
- `orchestrator/main.py` - FastAPI application
- `orchestrator/langgraph_workflow.py` - 12-node incident workflow
- `rag/hybrid_search_engine.py` - Swarm RAG search
- `streaming/consumers/event_orchestrator.py` - Kafka event router

**Structure**:
```
backend/
├── agents/              # DEPRECATED - use root agents/
├── config/              # Settings, thresholds
├── control_plane/       # HITL approval system
├── data/                # Registry, feedback
├── governance/          # Compliance, audit
├── guardrails/          # LLM safety
├── infrastructure/      # Re-exports from platform_services/
├── mcp/                 # MCP client + internal servers
├── observability/       # Metrics, tracing, logging
├── orchestrator/        # FastAPI + LangGraph
├── rag/                 # RAG search system
├── secrets_manager/     # GCP Secret Manager
├── services/            # Service layer
├── streaming/           # Kafka producers/consumers
└── utils/               # Utilities
```

[Full details: backend/README.md](backend/README.md)

---

## agents/

**Purpose**: Agent implementations for IT Service and Data Pipeline

**Key Components**:
- `servicenow_agent/` - IT Service incident handling
- `data_agent/` - Data pipeline generation
- `shared/` - Common interfaces

**Structure**:
```
agents/
├── __init__.py          # Main exports
├── registry.py          # Agent registry
├── shared/              # Interfaces, base classes
├── servicenow_agent/    # IT Service Agent
│   ├── service.py
│   ├── it_service/
│   └── remediation/
└── data_agent/          # Data Pipeline Agent
    ├── src/agents/      # LangGraph agents
    ├── src/graphs/      # Workflow graph
    ├── src/templates/   # Jinja2 templates
    └── src/deployers/   # Git, Airflow clients
```

[Full details: agents/README.md](agents/README.md)

---

## mcp-servers/

**Purpose**: MCP servers for external system integrations

**Servers**:
| Server | External System |
|--------|-----------------|
| `servicenow-mcp/` | ServiceNow |
| `jira-mcp/` | Jira |
| `github-mcp/` | GitHub |
| `gcp-mcp/` | GCP |
| `airflow-mcp/` | Airflow/Composer |

**Structure**:
```
mcp-servers/
├── servicenow-mcp/server.py  # Event-driven ServiceNow
├── jira-mcp/server.py        # Event-driven Jira
├── github-mcp/server.py      # Workflow dispatch + commits
├── gcp-mcp/server.py         # VM operations
├── airflow-mcp/server.py     # DAG management
└── shared/                   # Shared utilities
```

[Full details: mcp-servers/README.md](mcp-servers/README.md)

---

## frontend/

**Purpose**: Next.js 14 frontend UI

**Structure**:
```
frontend/
├── src/
│   ├── app/             # Next.js pages (App Router)
│   ├── components/      # React components
│   ├── contexts/        # React Context
│   ├── lib/             # API clients
│   └── types/           # TypeScript types
├── package.json
└── Dockerfile
```

[Full details: frontend/src/README.md](frontend/src/README.md)

---

## platform_services/

**Purpose**: Shared platform infrastructure

**Structure**:
```
platform_services/
├── infrastructure_clients/  # Redis, Kafka, Postgres, Dataproc
├── protocols/a2a/          # Agent-to-Agent protocol
├── runbooks/               # Remediation scripts
│   ├── ansible/
│   ├── kubernetes/
│   ├── terraform/
│   └── scripts/
├── metadata/
└── utils/
```

[Full details: platform_services/README.md](platform_services/README.md)

---

## scripts/

**Purpose**: Utility scripts for setup, testing, operations

**Categories**:
- Setup: `setup.sh`, `setup-pre-commit.sh`
- System: `start_system.sh`, `stop_system.sh`
- Testing: `e2e_validator.py`, `test_e2e.sh`
- Data: `populate_rag_data.py`
- GitHub: `push_to_enterprise_repo.sh`

[Full details: scripts/README.md](scripts/README.md)

---

## tests/

**Purpose**: Comprehensive test suite

**Structure**:
```
tests/
├── unit/           # Unit tests
├── integration/    # Integration tests
├── e2e/            # End-to-end tests
├── performance/    # Load tests
├── security/       # Security tests
├── llm/            # LLM-specific tests
├── compliance/     # Compliance checks
├── chaos/          # Chaos engineering
└── fixtures/       # Test data
```

[Full details: tests/README.md](tests/README.md)

---

## docs/

**Purpose**: Documentation files

**Key Files**:
| File | Purpose |
|------|---------|
| `ARCHITECTURE_V6_EVENT_DRIVEN.md` | Event-driven architecture |
| `CLAUDE_CODE_MASTER_CONTEXT.md` | Complete platform context |
| `WORKFLOW_FLOWS.md` | Workflow diagrams |
| `KAFKA_TOPICS.md` | Kafka topic reference |
| `OBSERVABILITY_WHITEPAPER.md` | Monitoring guide |

---

## monitoring/

**Purpose**: Observability stack

**Structure**:
```
monitoring/
├── prometheus.yml           # Prometheus config
├── tempo.yaml               # Tempo (tracing) config
├── alerts/                  # Alert rules
└── grafana/
    ├── dashboards/          # Dashboard JSON
    └── provisioning/        # Auto-provisioning
```

---

## infrastructure/

**Purpose**: Infrastructure as Code (Terraform)

**Structure**:
```
infrastructure/
├── terraform/
│   ├── main.tf
│   ├── gcp/
│   └── modules/
└── kubernetes/
```

---

## Cleanup Log

| Date | File(s) Removed | Reason |
|------|-----------------|--------|
| 2026-01-19 | `backend/streaming/servicenow_producer.py` | Deprecated |
| 2026-01-19 | `backend/streaming/consumers/jira_data_consumer.py` | Duplicate |
| 2026-01-19 | `mcp-servers/jira-mcp/server.py` (old) | Replaced by event-driven |
| 2026-01-19 | `mcp-servers/servicenow-mcp/server.py` (old) | Replaced by event-driven |

---

## Import Guidelines

```python
# Agents
from agents import BaseAgent, IAgentService
from agents.servicenow_agent import ServiceNowAgentService
from agents.data_agent.src.graphs import create_pipeline_graph

# Backend
from backend.config.settings import Settings
from backend.rag.hybrid_search_engine import hybrid_search_engine
from backend.streaming.kafka_producer import unified_producer
from backend.guardrails.llm_guardrails import guardrails

# Platform Services
from platform_services.infrastructure_clients import redis_client, kafka_client
from platform_services.protocols.a2a import A2AClient

# DON'T use deprecated paths:
# from backend.agents import ...  # DEPRECATED
# from backend.infrastructure import ...  # Use platform_services
```

---

## Quick Reference

### Start Development
```bash
./scripts/setup.sh
./scripts/start_system.sh
python scripts/populate_rag_data.py
```

### Run Tests
```bash
pytest tests/unit/ -v
python scripts/e2e_validator.py --all
```

### Check Health
```bash
./scripts/health_check.sh
./scripts/verify_system.sh
```

### Push to GitHub
```bash
./scripts/push_to_enterprise_repo.sh
```
