# AI Agent Platform - File Usage Map

**Generated:** 2026-01-01
**Version:** 5.0.0

This document maps every file in the project to its purpose and usage.

---

## Table of Contents
1. [Backend - Core Modules](#backend---core-modules)
2. [Backend - Agents](#backend---agents)
3. [Backend - RAG System](#backend---rag-system)
4. [Backend - Orchestrator](#backend---orchestrator)
5. [Backend - Utilities](#backend---utilities)
6. [Backend - Governance](#backend---governance)
7. [Backend - Guardrails](#backend---guardrails)
8. [Backend - Streaming](#backend---streaming)
9. [Backend - MCP](#backend---mcp)
10. [Backend - Config](#backend---config)
11. [Frontend](#frontend)
12. [Scripts](#scripts)
13. [Tests](#tests)
14. [Deployment](#deployment)
15. [Documentation](#documentation)

---

## Backend - Core Modules

### Agents (`backend/agents/`)

| File | Purpose | Used By |
|------|---------|---------|
| `__init__.py` | Exports BaseAgent, ControlPlane, ApprovalRoute | All agent modules |
| `base_agent.py` | Base class with OpenAI client, metrics, logging | ServiceNow, Jira, Infra, Remediation agents |
| `control_plane.py` | Policy-based approval routing (AUTO/ASYNC/MANUAL) | `langgraph_workflow.py` - workflow node |

### A2A Protocol (`backend/agents/a2a/`)

| File | Purpose | Used By |
|------|---------|---------|
| `__init__.py` | Exports A2A message types and client | llm_judge, swarm_retriever, langgraph_workflow |
| `messages.py` | Message dataclasses (SwarmQuery, JudgeEvaluate, etc.) | All A2A consumers |
| `client.py` | WebSocket client for agent communication | swarm_retriever, llm_judge |
| `mesh.py` | FastAPI WebSocket server for message routing | Standalone service (not actively deployed) |

### ServiceNow Agent (`backend/agents/servicenow/`)

| File | Purpose | Used By |
|------|---------|---------|
| `__init__.py` | Module exports | orchestrator/main.py |
| `agent.py` | Incident triage, diagnosis, resolution via RAG + LLM | API endpoint `/process`, langgraph workflow |

### Jira Agent (`backend/agents/jira/`)

| File | Purpose | Used By |
|------|---------|---------|
| `__init__.py` | Module exports | orchestrator/main.py |
| `agent.py` | Story analysis, implementation planning via RAG + LLM | API endpoint `/process` |
| `jira_client.py` | Jira REST API wrapper | agent.py |

### Infrastructure Agent (`backend/agents/infra/`)

| File | Purpose | Used By |
|------|---------|---------|
| `__init__.py` | Module exports | orchestrator/main.py |
| `agent.py` | GCP infrastructure monitoring and remediation | API endpoint `/analyze` |
| `enhanced_agent.py` | Enhanced version with script matching | agent.py |
| `autonomous_vm_recovery.py` | Autonomous VM recovery logic | enhanced_agent.py |

### Remediation Agent (`backend/agents/remediation/`)

| File | Purpose | Used By |
|------|---------|---------|
| `__init__.py` | Module exports | langgraph_workflow.py |
| `agent.py` | Remediation script execution | langgraph workflow |
| `enterprise_matcher.py` | Hybrid script matching (Vector + Metadata + Graph + LLM) | agent.py |

---

## Backend - RAG System

### Core RAG (`backend/rag/`)

| File | Purpose | Used By |
|------|---------|---------|
| `__init__.py` | Exports all RAG components, version info | All RAG consumers |
| `hybrid_search_engine.py` | RRF-based multi-source search (main RAG engine) | ServiceNow agent, Jira agent, intelligent_retriever |
| `intelligent_retriever.py` | Full pipeline: Query → Agents → RRF → Rerank | Higher-level search API |
| `embedding_service.py` | OpenAI text-embedding-3-small wrapper | hybrid_search_engine, smart_chunker |
| `weaviate_client.py` | Weaviate vector DB client | hybrid_search_engine |
| `neo4j_client.py` | Neo4j graph DB client | graph_scorer, hybrid_search_engine |
| `cross_encoder_reranker.py` | ms-marco-MiniLM reranking | intelligent_retriever |
| `query_understanding.py` | Intent/entity extraction from queries | intelligent_retriever |
| `graph_scorer.py` | Neo4j FIXED_BY relationship scoring | hybrid_search_engine |
| `smart_chunker.py` | Document chunking for embedding | script_ingestion |
| `feedback_optimizer.py` | Adaptive learning from user feedback | hybrid_search_engine |
| `swarm_retriever.py` | Multi-agent consensus retrieval | intelligent_retriever |
| `swarm_script_selector.py` | Swarm-based script selection | remediation agent |
| `script_ingestion.py` | Ingest scripts into vector DB | CLI utility |
| `script_library_indexer.py` | Index runbook library | script_ingestion |

### RAG Agents (`backend/rag/agents/`)

| File | Purpose | Used By |
|------|---------|---------|
| `__init__.py` | Exports RAG agents | swarm_retriever |
| `base_rag_agent.py` | Base class for RAG agents | All RAG agents |
| `vector_agent.py` | Vector similarity search agent | swarm_retriever |
| `keyword_agent.py` | BM25/keyword search agent | swarm_retriever |
| `graph_agent.py` | Neo4j graph search agent | swarm_retriever |
| `metadata_agent.py` | Metadata filtering agent | swarm_retriever |

---

## Backend - Orchestrator

### Orchestrator (`backend/orchestrator/`)

| File | Purpose | Used By |
|------|---------|---------|
| `__init__.py` | Module exports | main.py |
| `main.py` | FastAPI app - main API server | Entry point, Docker |
| `langgraph_workflow.py` | 18-node LangGraph workflow for incident processing | main.py API endpoints |
| `llm_intelligence.py` | LLM-based incident analysis and planning | langgraph_workflow |
| `llm_judge.py` | LLM-as-Judge for plan validation | langgraph_workflow |
| `metrics.py` | Prometheus metrics definitions | All orchestrator modules |
| `rollback_generator.py` | Generate rollback plans for remediations | langgraph_workflow |

### Orchestrator Services (`backend/orchestrator/services/`)

| File | Purpose | Used By |
|------|---------|---------|
| `__init__.py` | Module exports | main.py |
| `mcp_client.py` | MCP server client wrapper | main.py |

---

## Backend - Utilities

### Utils (`backend/utils/`)

| File | Purpose | Used By |
|------|---------|---------|
| `__init__.py` | Lazy imports for clients | All modules needing utilities |
| `kafka_client.py` | Kafka producer/consumer wrapper | streaming module |
| `redis_client.py` | Redis client for caching | hybrid_search_engine, feedback_optimizer |
| `postgres_client.py` | PostgreSQL client | Data persistence |
| `circuit_breaker.py` | Fault tolerance pattern | All external API calls |
| `cost_tracker.py` | LLM cost monitoring | orchestrator, agents |
| `github_actions.py` | GitHub Actions workflow trigger | langgraph_workflow (future) |
| `slack_notifier.py` | Slack notification sender | control_plane (async approvals) |
| `registry_manager.py` | Runbook registry management | enterprise_matcher |
| `otel_tracing.py` | OpenTelemetry distributed tracing | All modules |

---

## Backend - Governance

### Governance (`backend/governance/`)

| File | Purpose | Used By |
|------|---------|---------|
| `__init__.py` | Module exports | agents, orchestrator |
| `audit_logger.py` | EU AI Act compliant audit logging | ServiceNow agent, Jira agent, llm_intelligence |
| `eu_ai_act_compliance.py` | EU AI Act Article validators | compliance tests |
| `project_validator.py` | Project structure validation | CI/CD |
| `data_retention.py` | GDPR data retention policies | audit_logger |

---

## Backend - Guardrails

### Guardrails (`backend/guardrails/`)

| File | Purpose | Used By |
|------|---------|---------|
| `__init__.py` | Module exports | orchestrator, agents |
| `llm_guardrails.py` | Input/output validation, prompt injection detection, PII filtering | All LLM calls |

---

## Backend - Streaming

### Streaming (`backend/streaming/`)

| File | Purpose | Used By |
|------|---------|---------|
| `__init__.py` | Module documentation | - |
| `schemas.py` | Kafka event schemas | All streaming modules |
| `kafka_producer.py` | Send events to Kafka topics | gcp_vm_monitor, servicenow_producer |
| `incident_consumer.py` | Consume incident events from Kafka | Standalone consumer service |
| `incident_sources.py` | Incident source definitions | incident_consumer |
| `servicenow_producer.py` | Poll ServiceNow and produce events | Standalone producer service |
| `gcp_vm_monitor.py` | Monitor GCP VMs and produce alerts | Standalone monitor service |

**Note:** Streaming module is self-contained - designed for Kafka integration but not actively used by main workflow.

---

## Backend - MCP

### MCP Servers (`backend/mcp/`)

| File | Purpose | Used By |
|------|---------|---------|
| `__init__.py` | Module exports | mcp_client |
| `client.py` | MCP client wrapper | orchestrator/services/mcp_client.py |

### MCP Servers (`backend/mcp/servers/`)

| File | Purpose | Used By |
|------|---------|---------|
| `__init__.py` | Module exports | start_all.py |
| `rag_server.py` | RAG MCP server exposing search tools | Claude Code integration |
| `start_all.py` | Start all MCP servers | CLI utility |

### External MCP Servers (`mcp-servers/`)

| File | Purpose | Used By |
|------|---------|---------|
| `jira-mcp/server.py` | Jira MCP server | Claude Code integration |
| `servicenow-mcp/server.py` | ServiceNow MCP server | Claude Code integration |
| `gcp-mcp/server.py` | GCP MCP server | Claude Code integration |
| `github-mcp/server.py` | GitHub MCP server | Claude Code integration |
| `shared/metrics.py` | Shared Prometheus metrics | All MCP servers |

---

## Backend - Config

### Config (`backend/config/`)

| File | Purpose | Used By |
|------|---------|---------|
| `__init__.py` | Module exports | All modules |
| `thresholds.py` | Configurable thresholds (confidence, risk levels) | control_plane, llm_judge |

---

## Backend - Runbooks

### Runbooks (`backend/runbooks/`)

| File | Purpose | Used By |
|------|---------|---------|
| `registry.json` | Runbook metadata registry | enterprise_matcher |
| `scripts/*.sh` | Shell remediation scripts | GitHub Actions |
| `ansible/*.yml` | Ansible playbooks | GitHub Actions |
| `kubernetes/*.yaml` | K8s manifests | GitHub Actions |
| `pipelines/*.yaml` | Pipeline configs | GitHub Actions |

---

## Frontend

### Frontend Pages (`frontend/src/app/`)

| File | Purpose |
|------|---------|
| `page.tsx` | Dashboard home page |
| `layout.tsx` | Root layout with providers |
| `graph/[id]/page.tsx` | Incident graph visualization |
| `settings/page.tsx` | Settings page |
| `workflows/page.tsx` | Workflow management |

### Frontend Components (`frontend/src/components/`)

| Directory | Purpose |
|-----------|---------|
| `ui/` | Reusable UI components (Button, Card, Modal, etc.) |
| `chat/` | Chat interface (FloatingChat, ChatWrapper) |
| `agents/` | Agent visualization (AgentGrid, AgentMetrics, AgentLogs) |
| `events/` | Event stream (EventCard, EventStream, EventFilter) |
| `workflow/` | Workflow visualization |
| `incidents/` | Incident management (IncidentDetail, RemediationPanel) |
| `layout/` | Page layouts (Header, Sidebar, PageLayout) |
| `dashboard/` | Dashboard widgets (StatsCard, AgentStatus) |

---

## Scripts

### Scripts (`scripts/`)

| File | Purpose | Run By |
|------|---------|--------|
| `start-all.sh` | Start all services | Manual / CI |
| `stop-all.sh` | Stop all services | Manual |
| `setup.sh` | Initial setup | Manual |
| `health_check.sh` | System health verification | CI / Monitoring |
| `start_system.sh` | Start core system | start-all.sh |
| `stop_system.sh` | Stop core system | stop-all.sh |
| `verify_system.sh` | Verify system state | health_check.sh |
| `compliance_scanner.py` | Scan codebase for compliance | Manual / CI |
| `populate_all_data.py` | Populate demo data | Manual |
| `seed_historical_incidents.py` | Seed historical incidents | Manual |
| `sync_servicenow_incidents.py` | Sync from ServiceNow | Cron job |
| `view_incidents.py` | View incidents CLI | Manual |
| `agentic_workflow.py` | Run agentic workflow demo | Manual |
| `create_github_pr.py` | Create GitHub PRs | CI/CD |
| `run_compliance_check.sh` | Run compliance checks | CI |
| `test_e2e.sh` | End-to-end tests | CI |
| `run_full_demo.sh` | Full demo runner | Manual |
| `start_gcp_instance.sh` | Start GCP VM | Remediation |
| `stop_gcp_instance.sh` | Stop GCP VM | Remediation |
| `clear_disk_space.sh` | Clear disk space | Remediation |
| `push_to_github.sh` | Push to GitHub | Manual |

---

## Tests

### Tests (`tests/`)

| Directory | Purpose |
|-----------|---------|
| `unit/` | Unit tests for agents, RAG, utils, guardrails |
| `integration/` | Integration tests (API, ServiceNow, Jira, observability) |
| `e2e/` | End-to-end workflow tests |
| `smoke/` | Smoke tests for basic functionality |
| `regression/` | Regression tests |
| `performance/` | Load/performance tests |
| `security/` | Security tests |
| `llm/` | LLM-specific tests (hallucination, bias, adversarial) |
| `compliance/` | Compliance checker tests |
| `chaos/` | Chaos engineering tests |
| `fixtures/` | Test data fixtures |

---

## Deployment

### Deployment (`deployment/`)

| File | Purpose |
|------|---------|
| `docker-compose.yml` | Docker Compose for all services |
| `monitoring/prometheus.yml` | Prometheus configuration |
| `monitoring/grafana/` | Grafana dashboards and provisioning |

### GitHub Actions (`.github/workflows/`)

| File | Purpose |
|------|---------|
| `shell-execute.yml` | Execute shell scripts |
| `ansible-execute.yml` | Execute Ansible playbooks |
| `kubernetes-execute.yml` | Apply K8s manifests |
| `terraform-execute.yml` | Execute Terraform |

---

## Documentation

### Docs (`docs/`)

| File | Purpose |
|------|---------|
| `README.md` | Project overview |
| `ARCHITECTURE_V5.md` | V5 architecture documentation |
| `OBSERVABILITY_WHITEPAPER.md` | Observability architecture |
| `INCIDENT_LIFECYCLE_WHITEPAPER.md` | Incident lifecycle documentation |
| `COMPLIANCE_MATRIX.md` | Compliance matrix |
| `EU_AI_ACT_COMPLIANCE_GUIDE.md` | EU AI Act compliance guide |
| `ENHANCED_RAG_FEATURES.md` | RAG features documentation |
| `FILE_USAGE_MAP.md` | This file |

---

## Monitoring

### Monitoring (`monitoring/`)

| File | Purpose |
|------|---------|
| `prometheus.yml` | Prometheus scrape config |
| `alerts/ai_agent_alerts.yml` | Prometheus alert rules |
| `grafana/dashboards/` | Grafana dashboard JSONs |

---

## Data

### Data (`data/`)

| Directory | Purpose |
|-----------|---------|
| `demo_data/` | Demo incidents, Jira stories |
| `feedback/` | User feedback records |
| `embeddings_cache/` | Cached embeddings |

---

## Summary Statistics

| Category | File Count |
|----------|------------|
| Backend Python | ~75 files |
| Frontend TypeScript/TSX | ~35 files |
| Scripts | ~20 files |
| Tests | ~25 files |
| Config/YAML | ~15 files |
| Documentation | ~10 files |
| **Total** | **~180 files** |

---

## Unused/Deprecated Files

The following files have been removed as they were not used:

| File | Reason for Removal |
|------|-------------------|
| `backend/agents/execution_orchestrator.py` | Only referenced in comments, never instantiated |
| `backend/dspy_service/` | DSPy removed - using direct LLM calls instead |
| `backend/agents/jira/dspy_modules.py` | DSPy removed |
| `backend/agents/servicenow/dspy_modules.py` | DSPy removed |
