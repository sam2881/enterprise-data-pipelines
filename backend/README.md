# Backend Module Reference

> **Last Updated**: 2026-01-19
> **Purpose**: FastAPI backend for IT Service Management & Incident Remediation

## Quick Navigation

| Folder | Purpose | Key Files |
|--------|---------|-----------|
| [agents/](#agents) | DEPRECATED - Re-exports from root `agents/` | `__init__.py` |
| [config/](#config) | Settings and thresholds | `settings.py`, `thresholds.py` |
| [control_plane/](#control_plane) | HITL approval orchestration | `orchestrator.py`, `policy_engine.py` |
| [data/](#data) | Static data files | `registry.json`, `feedback/` |
| [governance/](#governance) | Compliance & auditing | `eu_ai_act_compliance.py`, `audit_logger.py` |
| [guardrails/](#guardrails) | LLM input/output validation | `llm_guardrails.py` |
| [infrastructure/](#infrastructure) | Re-exports from `platform_services/` | `__init__.py` |
| [mcp/](#mcp) | MCP client & servers | `client.py`, `servers/` |
| [observability/](#observability) | Metrics, tracing, logging | `__init__.py` |
| [orchestrator/](#orchestrator) | Main API & LangGraph workflow | `main.py`, `langgraph_workflow.py` |
| [rag/](#rag) | Swarm RAG search system | `hybrid_search_engine.py`, `agents/` |
| [secrets_manager/](#secrets_manager) | Secret management | `manager.py` |
| [services/](#services) | Backend service layer | `agent_service.py` |
| [streaming/](#streaming) | Kafka producers & consumers | `kafka_producer.py`, `consumers/` |
| [utils/](#utils) | Utility modules | `circuit_breaker.py`, `github_actions.py` |

---

## Folder Details

### agents/
**Status**: DEPRECATED - Do not add new code here

Re-exports from root `agents/` module for backward compatibility.

| File | Purpose | Status |
|------|---------|--------|
| `__init__.py` | Re-exports `BaseAgent`, `IAgentService`, `IAgentTask` | Kept for compatibility |

**Use Instead**: `from agents import BaseAgent`

---

### config/
Application configuration and thresholds.

| File | Purpose |
|------|---------|
| `__init__.py` | Exports config classes |
| `settings.py` | Environment-based settings (Pydantic) |
| `thresholds.py` | Confidence thresholds, risk levels, execution policies |

---

### control_plane/
Human-in-the-Loop (HITL) approval system.

| File | Purpose |
|------|---------|
| `__init__.py` | Exports control plane components |
| `orchestrator.py` | Main control plane orchestrator - routes approvals |
| `policy_engine.py` | Policy rules for auto-approve vs HITL |
| `handlers/` | Handler implementations |
| `handlers/it_service_handler.py` | IT Service incident handling |
| `handlers/data_pipeline_handler.py` | Data pipeline request handling |
| `handlers/data_agent_handler.py` | Data agent coordination |

---

### data/
Static data files and registries.

| Item | Purpose |
|------|---------|
| `registry.json` | Script registry with metadata |
| `feedback/` | User feedback data for RAG optimization |
| `registry_history/` | Historical registry versions |

---

### governance/
Compliance, auditing, and data retention.

| File | Purpose |
|------|---------|
| `__init__.py` | Exports governance components |
| `eu_ai_act_compliance.py` | EU AI Act compliance checks |
| `audit_logger.py` | Audit trail logging |
| `data_retention.py` | Data retention policies |
| `project_validator.py` | Project validation rules |

---

### guardrails/
LLM input/output validation and safety.

| File | Purpose |
|------|---------|
| `__init__.py` | Exports guardrails |
| `llm_guardrails.py` | Input validation, output sanitization, rate limiting |

---

### infrastructure/
**Status**: Re-exports from `platform_services/infrastructure_clients`

| File | Purpose |
|------|---------|
| `__init__.py` | Re-exports Redis, Kafka, Postgres, Dataproc clients |

**Use Instead**: `from platform_services.infrastructure_clients import redis_client`

---

### mcp/
Model Context Protocol client and servers.

| File | Purpose |
|------|---------|
| `__init__.py` | Exports MCP components |
| `client.py` | MCP client for calling MCP servers |
| `Dockerfile` | Container build for MCP |
| `servers/` | MCP server implementations |
| `servers/gcs_server.py` | GCS operations (list, infer schema) |
| `servers/iceberg_server.py` | Iceberg table operations |
| `servers/llm_server.py` | LLM analysis operations |
| `servers/airflow_server.py` | Airflow DAG operations |
| `servers/rag_server.py` | RAG search operations |
| `servers/start_all.py` | Start all MCP servers |

---

### observability/
Unified metrics, tracing, and logging.

| File | Purpose |
|------|---------|
| `__init__.py` | Unified exports for metrics, tracing, logging |

Consolidates:
- Prometheus metrics from `orchestrator/metrics.py`
- OpenTelemetry tracing from `utils/otel_tracing.py`
- Structured JSON logging

---

### orchestrator/
Main FastAPI application and LangGraph workflow.

| File | Purpose |
|------|---------|
| `__init__.py` | Package init |
| `main.py` | **MAIN API** - FastAPI app with all endpoints |
| `langgraph_workflow.py` | **CORE** - 12-node LangGraph workflow |
| `llm_intelligence.py` | LLM-based incident analysis |
| `llm_judge.py` | LLM judge for plan validation |
| `metrics.py` | Prometheus metrics definitions |
| `rollback_generator.py` | Rollback plan generation |
| `services/` | Service layer (if any) |
| `Dockerfile` | Container build |

---

### rag/
Swarm RAG (Retrieval-Augmented Generation) system.

| File | Purpose |
|------|---------|
| `__init__.py` | Exports RAG components |
| `hybrid_search_engine.py` | **CORE** - Hybrid search (vector + keyword + graph) |
| `intelligent_retriever.py` | Smart retrieval with query understanding |
| `swarm_retriever.py` | Multi-agent swarm retrieval |
| `swarm_script_selector.py` | Script selection via swarm |
| `embedding_service.py` | Text embedding service |
| `cross_encoder_reranker.py` | Cross-encoder reranking |
| `query_understanding.py` | Query intent classification |
| `graph_scorer.py` | Graph-based scoring |
| `smart_chunker.py` | Document chunking |
| `feedback_optimizer.py` | Feedback-based optimization |
| `script_ingestion.py` | Script indexing |
| `script_library_indexer.py` | Library indexing |
| `neo4j_client.py` | Neo4j graph database client |
| `weaviate_client.py` | Weaviate vector database client |
| `agents/` | RAG sub-agents |
| `agents/base_rag_agent.py` | Base RAG agent interface |
| `agents/vector_agent.py` | Vector similarity search |
| `agents/keyword_agent.py` | BM25/TF-IDF keyword search |
| `agents/graph_agent.py` | Graph traversal search |
| `agents/metadata_agent.py` | Metadata filtering |

---

### secrets_manager/
Secret management (GCP Secret Manager integration).

| File | Purpose |
|------|---------|
| `__init__.py` | Exports secrets manager |
| `manager.py` | Secret retrieval and caching |

---

### services/
Backend service layer (thin wrapper over agents).

| File | Purpose |
|------|---------|
| `__init__.py` | Exports services |
| `agent_service.py` | Agent service interface |

---

### streaming/
Kafka event streaming - producers and consumers.

| File | Purpose |
|------|---------|
| `__init__.py` | Exports streaming components |
| `kafka_producer.py` | **UNIFIED** - Kafka producer for all events |
| `event_publisher.py` | High-level event publishing |
| `schemas.py` | Kafka topic definitions and event schemas |
| `incident_sources.py` | Incident source integrations |
| `gcp_vm_monitor.py` | GCP VM monitoring |
| `Dockerfile` | Container build |
| `consumers/` | Kafka consumers |
| `consumers/__init__.py` | Consumer exports |
| `consumers/event_orchestrator.py` | **CORE** - Main event router |
| `consumers/incident_consumer.py` | ServiceNow incident consumer |
| `consumers/jira_consumer.py` | Jira story consumer |
| `consumers/data_pipeline_consumer.py` | Data pipeline event consumer |

---

### utils/
Utility modules for cross-cutting concerns.

| File | Purpose |
|------|---------|
| `__init__.py` | Exports utilities |
| `circuit_breaker.py` | Circuit breaker re-export |
| `cost_tracker.py` | LLM cost tracking |
| `github_actions.py` | GitHub Actions integration |
| `otel_tracing.py` | OpenTelemetry tracing setup |
| `registry_manager.py` | Script registry management |
| `slack_notifier.py` | Slack notifications |

---

## Entry Points

1. **FastAPI Server**: `python -m backend.orchestrator.main`
2. **Event Orchestrator**: `python -m backend.streaming.consumers.event_orchestrator`
3. **MCP Servers**: `python -m backend.mcp.servers.start_all`

---

## Import Guidelines

```python
# Configuration
from backend.config.settings import Settings
from backend.config.thresholds import confidence_thresholds

# RAG
from backend.rag.hybrid_search_engine import hybrid_search_engine

# Streaming
from backend.streaming.kafka_producer import unified_producer

# Guardrails
from backend.guardrails.llm_guardrails import guardrails

# DON'T: Import from deprecated modules
# from backend.agents import ...  # Use: from agents import ...
# from backend.infrastructure import ...  # Use: from platform_services.infrastructure_clients import ...
```

---

## Architecture Notes

- **Kafka is the system of record** - All state transitions flow through Kafka
- **MCP servers** handle external integrations (GCS, Iceberg, Airflow)
- **LangGraph workflow** orchestrates the 12-node remediation flow
- **Swarm RAG** uses 4 agents for hybrid search
