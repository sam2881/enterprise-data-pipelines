# Enterprise Agentic Platform - Claude Code Instructions

## 🎯 Architecture v6.0 - Event-Driven

> **Kafka is the system of record. All state transitions flow through Kafka.**

```
MCPs sense → Kafka remembers → Orchestrator routes → LangGraph reasons & acts → FastAPI governs
```

---

## 📚 Required Context Files

**READ THESE FILES BEFORE ANY IMPLEMENTATION:**

1. **`docs/ARCHITECTURE_V6_EVENT_DRIVEN.md`** - Event-driven architecture patterns, Kafka topics, event flow
2. **`docs/CLAUDE_CODE_MASTER_CONTEXT.md`** - Complete platform architecture, both systems, E2E testing
3. **`docs/CLAUDE_CODE_CONTEXT.md`** - Data agent implementation patterns, code templates
4. **`docs/ENTERPRISE_AGENTIC_DATA_PLATFORM_README.md`** - Full platform specification

---

## 🏗️ Two Systems Overview

### 1. Incident Management System (`backend/`)
- **Purpose**: Automated IT incident resolution from ServiceNow
- **Workflow**: 12-node LangGraph (ingest → parse → classify → swarm_rag → generate_plan → judge → control_plane → await_approval → execute → verify → close_ticket → feedback_loop)
- **Key Files**: `backend/orchestrator/langgraph_workflow.py`
- **Architecture**: Event-driven via Kafka, pause/resume for approvals

### 2. Data Engineering Agent (`agents/data_agent/`)
- **Purpose**: Automated data pipeline generation
- **Workflow**: 5-agent LangGraph (supervisor → planner → generator → validator → deployer)
- **Key Files**: `agents/data_agent/src/graphs/main_graph.py`

---

## 🚨 Critical Rules (NON-NEGOTIABLE)

| DO ✅ | NEVER ❌ |
|-------|----------|
| LangGraph StateGraph | ReAct pattern |
| Explicit Pydantic/TypedDict state | Implicit LLM memory |
| Fail fast with error state | Auto-fix errors |
| Validated JSON from UI | Parse free-text from Jira |
| Human approval for PROD | Auto-deploy to PROD |
| Jinja2 templates | Hard-coded business logic |

---

## 📂 Project Structure

```
AI_AGENT_APP/
├── backend/                    # Incident Management System
│   ├── orchestrator/          # LangGraph workflow (7 nodes)
│   ├── rag/                   # Swarm RAG (4 agents)
│   ├── mcp/                   # MCP servers
│   └── streaming/             # Kafka consumers
├── agents/
│   └── data_agent/            # Data Pipeline Agent
│       ├── src/agents/        # 5 LangGraph agents
│       ├── src/graphs/        # StateGraph workflow
│       ├── src/templates/     # Jinja2 (DAG, Spark, SQL)
│       └── tests/
├── frontend/                   # Next.js 14 UI
└── docs/                       # Context files
```

---

## 🧪 Testing & Validation

```bash
# Full E2E validation
python scripts/e2e_validator.py --all

# Health checks only
python scripts/e2e_validator.py --health

# Unit tests
pytest tests/unit -v

# Data agent tests
cd agents/data_agent && pytest tests/ -v
```

---

## 🔧 Quick Reference

### LangGraph Node Pattern
```python
def node_name(state: AgentState) -> Dict[str, Any]:
    try:
        # Logic here
        return {"key": "partial_state_update"}
    except Exception as e:
        return {"error_message": str(e), "error_agent": "node_name"}
```

### Conditional Edge Pattern
```python
def should_continue(state: AgentState) -> Literal["next", "error"]:
    return "error" if state.get("error_message") else "next"
```

---

## 📡 Event-Driven Architecture

### Kafka Topics (System of Record)
| Topic | Purpose |
|-------|---------|
| `incident.created` | MCP detected new incident |
| `incident.received` | LangGraph started processing |
| `incident.enriched` | Classification complete |
| `incident.plan_generated` | Remediation plan ready |
| `incident.requires_approval` | Pending human approval |
| `incident.approved` | Human approved (from FastAPI) |
| `incident.close_execute` | Command to MCP to close |
| `incident.closed` | Workflow complete |

### Component Responsibilities
| Component | Role |
|-----------|------|
| **MCP Servers** | Poll external systems, publish events to Kafka |
| **EventOrchestrator** | Consume events, route to LangGraph workflows |
| **LangGraph** | Process events, publish state transitions, pause for approvals |
| **FastAPI** | Control plane only - serve UI, publish approval events |

### Protocol Usage
| Protocol | When to Use |
|----------|-------------|
| **Kafka** | All state transitions (SYSTEM OF RECORD) |
| **MCP** | Agent-to-tool invocation (RAG, LLM, GCS) |
| **REST** | GitHub Actions, external APIs |

---

## ✅ Pre-Commit Checklist

- [ ] Uses LangGraph StateGraph (not ReAct)
- [ ] State is explicit Pydantic/TypedDict
- [ ] Error handling returns proper state
- [ ] Audit logging present
- [ ] Tests written
- [ ] PROD requires human approval

---

## 📝 Key Principles

- Platform is a **COMPILER**, not ETL tool
- Business logic lives in **metadata**, not code
- Generated artifacts are **deterministic**
- Memory is **explicit and versioned** (PostgreSQL + Git)
- Human approval is **mandatory** for production

---

**For complete details, see `docs/CLAUDE_CODE_MASTER_CONTEXT.md`**