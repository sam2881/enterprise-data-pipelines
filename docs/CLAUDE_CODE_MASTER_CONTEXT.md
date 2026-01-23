# Enterprise Agentic Platform - Master Context for Claude Code

## 🎯 Purpose
This document provides Claude Code with complete context to understand, develop, test, and validate the entire Enterprise Agentic Platform end-to-end. **Read this entire document before making any changes.**

---

## 📁 Project Root Structure

```
AI_AGENT_APP/
├── agents/                      # All agent implementations
│   ├── data_agent/             # NEW: Data Pipeline Agent (LangGraph)
│   ├── servicenow_agent/       # ServiceNow incident agent
│   └── shared/                 # Shared agent utilities
├── backend/                     # Core platform services
│   ├── orchestrator/           # LangGraph workflow engine
│   ├── rag/                    # Swarm RAG system
│   ├── mcp/                    # MCP servers
│   ├── streaming/              # Kafka consumers/producers
│   ├── guardrails/             # AI safety guardrails
│   └── governance/             # Compliance & audit
├── frontend/                    # Next.js 14 UI
├── platform_services/          # Infrastructure clients
├── mcp-servers/                # External MCP server implementations
├── infrastructure/             # Docker, scripts
├── deployment/                 # Monitoring, Grafana, Prometheus
├── tests/                      # All test types
└── docs/                       # Documentation
```

---

## 🏗️ Platform Architecture Overview

### Two Major Subsystems

#### 1. **Incident Management System** (Existing - Production Ready)
- **Purpose**: Automated IT incident resolution
- **Flow**: ServiceNow → Kafka → LangGraph (7 nodes) → GitHub Actions → Verification
- **Key Components**:
  - Swarm RAG (4 agents: Vector, Keyword, Graph, Metadata)
  - LLM-as-Judge validation
  - Human-in-the-Loop approval
  - Feedback learning loop

#### 2. **Data Engineering Agent** (New - In Development)
- **Purpose**: Automated data pipeline generation
- **Flow**: Jira Intent → LangGraph (5 agents) → Generated Artifacts → Human Approval → Deploy
- **Key Components**:
  - Supervisor Agent (orchestration)
  - Planner Agent (pipeline design)
  - Generator Agent (Spark/DAG code)
  - Validator Agent (security/quality)
  - Deployer Agent (CI/CD trigger)

---

## 🔄 Hybrid Protocol Architecture

### Protocol Usage Matrix

| Protocol | Use Case | When to Use |
|----------|----------|-------------|
| **Kafka** | External systems, audit trail, state transitions | ServiceNow events, incident state changes, approved plans |
| **A2A** | Agent-to-agent real-time coordination | Swarm consensus, judge evaluation, agent execution |
| **MCP** | Agent-to-tool invocation (JSON-RPC 2.0) | RAG queries, ServiceNow API, GitHub tools |
| **REST/Webhook** | External APIs, GitHub Actions | workflow_dispatch, Jira integration |
| **SDK (Direct)** | Low-latency internal calls | Redis ~1ms, PostgreSQL ~10ms |

### Incident State Machine
```
NEW → RECEIVED → ENRICHED → JUDGED → PLAN_GEN → APPROVED → EXECUTED → VERIFIED → CLOSED
                                                    ↓
                                               FAILED → ROLLBACK → MANUAL
```

---

## 📦 Core Component Details

### 1. LangGraph Workflow Engine (`backend/orchestrator/`)

**7-Node Incident Workflow** (`langgraph_workflow.py`):
```python
# Node 1: receive_parse     → Extract incident context
# Node 2: swarm_rag         → Multi-agent search for remediation scripts
# Node 3: generate_plan     → LLM generates execution plan
# Node 4: llm_judge         → Independent validation (different model)
# Node 5: control_plane     → Approval routing based on risk
# Node 6: execute           → GitHub Actions trigger
# Node 7: verify_close      → Verify fix and update knowledge base
```

**State Schema**:
```python
class WorkflowState(TypedDict):
    workflow_id: str
    incident_id: str
    current_node: str
    status: Literal["active", "completed", "failed", "pending_approval"]
    node_outputs: Dict[str, Any]
    error_message: Optional[str]
    revision_count: int
    created_at: datetime
```

### 2. Swarm RAG System (`backend/rag/`)

**Four Search Agents** (weighted consensus):
| Agent | Weight | Data Source | Method |
|-------|--------|-------------|--------|
| Vector Agent | 0.40 | Weaviate | Semantic similarity (all-MiniLM-L6-v2) |
| Keyword Agent | 0.25 | TF-IDF index | BM25 + bigram matching |
| Graph Agent | 0.25 | Neo4j | FIXED_BY relationship traversal |
| Metadata Agent | 0.10 | Script registry | Exact field matching |

**Key Files**:
- `swarm_retriever.py` → Swarm coordinator
- `swarm_script_selector.py` → Script-specific swarm
- `cross_encoder_reranker.py` → MS-MARCO reranking (+20-30% precision)
- `feedback_optimizer.py` → Adaptive weight learning

### 3. MCP Servers (`backend/mcp/`, `mcp-servers/`)

| Server | Protocol | Purpose |
|--------|----------|---------|
| `servicenow_server.py` | JSON-RPC 2.0 | Incident CRUD, ticket closure |
| `github_server.py` | JSON-RPC 2.0 | PR creation, workflow trigger |
| `rag_server.py` | JSON-RPC 2.0 | Search, update embeddings |
| `gcp_server.py` | JSON-RPC 2.0 | VM operations, Cloud APIs |

### 4. Kafka Streaming (`backend/streaming/`)

**Topics**:
- `servicenow.incidents` → New incidents from ServiceNow
- `gcp.alerts` → GCP monitoring alerts
- `incident.approved` → Approved execution plans
- `incident.executed` → Completed executions
- `incident.closed` → Final resolution events
- `*.dlq` → Dead letter queues for failures

**Key Files**:
- `incident_consumer.py` → Main Kafka consumer
- `kafka_producer.py` → Event publication
- `schemas.py` → Pydantic event models

---

## 🤖 Data Engineering Agent (`agents/data_agent/`)

### Directory Structure
```
agents/data_agent/
├── src/
│   ├── agents/              # 5 LangGraph agents
│   │   ├── base_agent.py
│   │   ├── supervisor_agent.py
│   │   ├── planner_agent.py
│   │   ├── generator_agent.py
│   │   ├── validator_agent.py
│   │   └── deployer_agent.py
│   ├── graphs/              # LangGraph workflow
│   │   ├── main_graph.py    # StateGraph definition
│   │   ├── nodes.py         # Node functions
│   │   └── edges.py         # Conditional edges
│   ├── state/               # Pydantic state models
│   │   ├── pipeline_state.py
│   │   ├── agent_state.py
│   │   └── execution_context.py
│   ├── templates/           # Jinja2 templates
│   │   ├── dag/*.jinja2     # Airflow DAG templates
│   │   ├── spark/*.jinja2   # PySpark job templates
│   │   └── sql/*.jinja2     # Metadata SQL templates
│   ├── generators/          # Code generators
│   ├── validators/          # Validation logic
│   ├── deployers/           # CI/CD integration
│   ├── metadata/            # PostgreSQL repository
│   └── config/              # Environment settings
├── prompts/                 # Agent prompts (markdown)
├── ddl/                     # Database schema (7 files)
├── tests/                   # Unit/integration tests
├── terraform/               # GCP infrastructure
└── deployment/              # Generated artifacts
```

### Agent Workflow (LangGraph StateGraph)
```
[START] → supervisor → planner → generator → validator → (human_approval) → deployer → [END]
                  ↑                    │
                  └────── revision ────┘
```

### Critical Rules for Data Agent

1. **Framework**: USE LangGraph StateGraph, NEVER ReAct pattern
2. **State**: Explicit Pydantic/TypedDict, NEVER implicit LLM memory
3. **Errors**: Fail fast, return error state, NEVER auto-fix
4. **Input**: Validated JSON from UI only, NEVER parse free-text from Jira
5. **Deployment**: ALWAYS require human approval for PROD
6. **Code Gen**: Jinja2 templates only, NEVER hard-code business logic

### Template Selection Matrix

| Source | Mode | CDC | DAG Template | Spark Templates |
|--------|------|-----|--------------|-----------------|
| file | batch | No | file_ingest_dag | bronze, silver, gold_bq |
| file | micro_batch | No | streaming_ingest_dag | bronze, silver, gold_bq |
| database | batch | No | db_snapshot_dag | bronze, silver, gold_bq |
| database | batch | Yes | cdc_ingest_dag | cdc_merge, scd2, gold_bq |
| streaming | streaming | N/A | streaming_ingest_dag | streaming_bronze, gold_bq |
| api | batch | No | api_ingest_dag | bronze, silver, gold_bq |

---

## 🧪 Testing Strategy

### Test Directory Structure
```
tests/
├── unit/                    # Fast, isolated tests
│   ├── test_agents.py
│   ├── test_guardrails.py
│   ├── test_orchestrator.py
│   ├── test_rag.py
│   └── test_utils.py
├── integration/             # Component integration
│   ├── test_api_endpoints.py
│   ├── test_servicenow_incidents.py
│   ├── test_jira_tasks.py
│   └── test_observability.py
├── e2e/                     # End-to-end flows
│   ├── test_servicenow_workflow.py
│   └── test_jira_workflow.py
├── llm/                     # LLM-specific tests
│   ├── test_hallucination.py
│   ├── test_bias_fairness.py
│   └── test_prompt_quality.py
├── performance/             # Load testing
├── security/                # Security scanning
├── chaos/                   # Chaos engineering
└── compliance/              # Regulatory checks
```

### Running Tests
```bash
# All unit tests
pytest tests/unit -v

# Integration tests (requires Docker services)
pytest tests/integration -v

# E2E tests (requires full stack)
pytest tests/e2e -v --tb=short

# Specific test file
pytest tests/unit/test_rag.py -v

# With coverage
pytest tests/unit --cov=backend --cov-report=html

# Data agent tests
cd agents/data_agent && pytest tests/ -v
```

### Test Fixtures (`tests/fixtures/`)
- `test_incidents.json` → Sample ServiceNow incidents
- `test_jira_tasks.json` → Sample Jira stories
- `sample_intent.json` → Data agent intent payloads

---

## 🚀 End-to-End Validation Checklist

### Pre-Flight Checks
```bash
# 1. Verify Docker services
docker-compose ps  # Should show: kafka, redis, postgres, weaviate, neo4j

# 2. Check environment variables
cat .env | grep -E "^(KAFKA|REDIS|POSTGRES|OPENAI|GITHUB)"

# 3. Verify database schemas
psql -h localhost -U postgres -d aiagent -c "\dt"

# 4. Check Kafka topics
kafka-topics --list --bootstrap-server localhost:29092
```

### Incident Management E2E Test
```bash
# 1. Start backend
cd backend && uvicorn orchestrator.main:app --reload

# 2. Start consumer
python -m streaming.incident_consumer

# 3. Create test incident
curl -X POST http://localhost:8000/api/incidents \
  -H "Content-Type: application/json" \
  -d '{"short_description": "Test VM down", "priority": "3"}'

# 4. Monitor workflow
curl http://localhost:8000/api/langgraph/workflow/{workflow_id}

# 5. Approve if needed
curl -X POST http://localhost:8000/api/approvals/{approval_id}/approve

# 6. Verify completion
curl http://localhost:8000/api/incidents/{incident_id}
```

### Data Agent E2E Test
```bash
# 1. Start data agent service
cd agents/data_agent && python -m src.main

# 2. Submit pipeline intent
curl -X POST http://localhost:8001/api/pipelines \
  -H "Content-Type: application/json" \
  -d @tests/fixtures/sample_intent.json

# 3. Monitor agent workflow
curl http://localhost:8001/api/workflows/{workflow_id}

# 4. Review generated artifacts
ls deployment/dags/
ls deployment/spark_jobs/

# 5. Approve for deployment (non-PROD)
curl -X POST http://localhost:8001/api/approvals/{approval_id}/approve
```

---

## 🔧 Development Commands

### Quick Start
```bash
# Clone and setup
git clone <repo> && cd AI_AGENT_APP
cp .env.example .env  # Edit with your keys
pip install -r requirements.txt

# Start infrastructure
docker-compose up -d

# Initialize databases
psql -h localhost -U postgres -f infrastructure/init-postgres.sql
psql -h localhost -U postgres -f agents/data_agent/ddl/*.sql

# Start services
./scripts/start_system.sh
```

### Code Quality
```bash
# Lint
ruff check backend/ agents/ --fix

# Type check
mypy backend/ agents/ --ignore-missing-imports

# Format
black backend/ agents/

# Security scan
bandit -r backend/ agents/
```

### Database Operations
```bash
# Connect to PostgreSQL
psql -h localhost -U postgres -d aiagent

# View incident history
SELECT * FROM incidents ORDER BY created_at DESC LIMIT 10;

# View pipeline metadata
SELECT * FROM pipeline ORDER BY created_at DESC LIMIT 10;

# View audit logs
SELECT * FROM audit_log ORDER BY timestamp DESC LIMIT 20;
```

---

## 📊 Observability

### Metrics (Prometheus)
- `incident_resolution_total{outcome}` → Resolution counts
- `incident_resolution_duration_seconds` → Latency histogram
- `swarm_rag_search_duration_seconds` → RAG performance
- `llm_judge_score` → Judge evaluation scores
- `human_approval_wait_seconds` → Approval latency

### Logs (Structured JSON)
```python
logger.info("Processing incident", extra={
    "incident_id": "INC0010001",
    "workflow_id": "wf-abc123",
    "node": "swarm_rag",
    "duration_ms": 1842
})
```

### Traces (OpenTelemetry)
- Span: `workflow.{workflow_id}`
- Child spans for each node
- Attributes: `incident_id`, `status`, `confidence`

### Dashboards
- Grafana: `http://localhost:3000` (admin/admin)
- Prometheus: `http://localhost:9090`
- LangSmith: Configured via `LANGCHAIN_API_KEY`

---

## ⚠️ Critical Constraints

### Security
- **Never commit secrets** → Use `.env` and Secret Manager
- **Validate all inputs** → Guardrails in `backend/guardrails/`
- **Audit all actions** → `backend/governance/audit_logger.py`

### Performance
- **Circuit breakers** → All external API calls protected
- **Caching** → Redis for embeddings and state
- **Timeouts** → 30s for LLM calls, 600s for executions

### Compliance
- **SOC2 Type II** → Audit logging enabled
- **EU AI Act** → Human oversight for high-risk
- **NIST AI RMF** → Risk assessment in control plane

---

## 📝 File Naming Conventions

| Type | Convention | Example |
|------|------------|---------|
| Python files | snake_case | `planner_agent.py` |
| Classes | PascalCase | `PlannerAgent` |
| Functions | snake_case | `plan_pipeline()` |
| Constants | UPPER_SNAKE | `MAX_RETRIES` |
| DB tables | snake_case singular | `pipeline` |
| DB columns | snake_case | `created_at` |
| DAG files | `{domain}_{name}_dag.py` | `sales_orders_dag.py` |
| Spark jobs | `{name}_{layer}.py` | `orders_bronze.py` |
| Kafka topics | `{domain}.{event}` | `incident.approved` |

---

## 🔗 Key File References

### Must-Read Before Implementation
1. `docs/CLAUDE_CODE_CONTEXT.md` → Data agent patterns
2. `docs/ENTERPRISE_AGENTIC_DATA_PLATFORM_README.md` → Full spec
3. `docs/INCIDENT_LIFECYCLE_WHITEPAPER.md` → 24-step flow
4. `backend/orchestrator/langgraph_workflow.py` → Workflow engine

### Configuration Files
- `.env` → Environment variables
- `backend/config/settings.py` → Pydantic settings
- `backend/config/thresholds.py` → Approval thresholds
- `agents/data_agent/src/config/settings.py` → Data agent config

### Database Schemas
- `infrastructure/init-postgres.sql` → Core tables
- `agents/data_agent/ddl/` → 7 DDL files for data agent

---

## ✅ Pre-Commit Checklist

Before completing any implementation:

- [ ] Uses LangGraph StateGraph (not ReAct)
- [ ] State is explicit Pydantic/TypedDict
- [ ] No free-text parsing
- [ ] No auto-fixing errors
- [ ] Audit logging present
- [ ] Error handling returns proper state
- [ ] Jinja2 templates used (no hard-coded logic)
- [ ] Naming conventions followed
- [ ] PROD requires human approval
- [ ] Tests written
- [ ] Circuit breakers for external calls
- [ ] Secrets not committed

---

## 🆘 Troubleshooting

### Common Issues

**Kafka connection refused**:
```bash
docker-compose restart kafka
# Wait 30s for broker to initialize
```

**OpenAI rate limit**:
```python
# Circuit breaker will handle, check logs
grep "circuit_breaker" logs/backend.log
```

**Weaviate schema error**:
```bash
# Reset Weaviate
docker-compose down weaviate && docker-compose up -d weaviate
python scripts/populate_rag_data.py
```

**LangGraph state corruption**:
```python
# Check Redis state
redis-cli GET "workflow:{workflow_id}"
# Reset if needed
redis-cli DEL "workflow:{workflow_id}"
```

---

## 📚 Additional Resources

- [LangGraph Documentation](https://python.langchain.com/docs/langgraph)
- [MCP Protocol Spec](https://modelcontextprotocol.io)
- [A2A Protocol](https://github.com/google/a2a-protocol)
- [Weaviate Docs](https://weaviate.io/developers/weaviate)
- [Neo4j Cypher](https://neo4j.com/docs/cypher-manual)

---

*Last Updated: January 2025*
*Version: 5.0.0*