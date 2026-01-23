# Component Responsibility Matrix - v6.0 Event-Driven Architecture

## Overview

This document defines the responsibilities of each component in the Enterprise Agentic Platform. Clear separation of concerns ensures maintainability and compliance.

---

## Mental Model

```
MCPs sense → Kafka remembers → Orchestrator routes → LangGraph reasons & acts → FastAPI governs
```

---

## Component Responsibilities

### 1. MCP Servers (Edge Adapters)

**Location**: `mcp-servers/`

| Responsibility | Description |
|----------------|-------------|
| Poll external systems | Periodically fetch data from ServiceNow, Jira |
| Normalize events | Convert external formats to platform events |
| Publish to Kafka | Send `incident.created`, `pipeline.requested` |
| Consume commands | Listen for `incident.close_execute`, `pipeline.deploy_execute` |
| Execute actions | Call external APIs (close ticket, deploy DAG) |
| Handle authentication | Manage credentials for external systems |
| Implement retry logic | Handle transient failures with backoff |

**Publishes**:
- `incident.created` - New incident from ServiceNow
- `incident.closed` - After closing ticket in ServiceNow
- `pipeline.requested` - New request from Jira
- `pipeline.deployed` - After deploying to Airflow

**Consumes**:
- `incident.close_execute` - Command to close ServiceNow ticket
- `pipeline.deploy_execute` - Command to deploy to Airflow

**Does NOT**:
- Store state (stateless adapter)
- Make business decisions
- Call LangGraph directly
- Access databases directly

---

### 2. Kafka (System of Record)

**Location**: External service (configured via `docker-compose.yml`)

| Responsibility | Description |
|----------------|-------------|
| Store all events | Immutable, ordered event log |
| Guarantee ordering | Per-partition ordering by key |
| Enable replay | Consumers can replay from any offset |
| Decouple components | Publishers don't know consumers |
| Provide durability | Configurable retention and replication |

**Topics**: See [KAFKA_TOPICS.md](KAFKA_TOPICS.md) for complete list.

**Does NOT**:
- Process events (only stores them)
- Make routing decisions
- Transform data

---

### 3. EventOrchestrator (Central Router)

**Location**: `backend/streaming/consumers/event_orchestrator.py`

| Responsibility | Description |
|----------------|-------------|
| Consume lifecycle events | Subscribe to incident.*, pipeline.* topics |
| Route to workflows | Start LangGraph for `incident.created` |
| Resume paused workflows | Handle `incident.approved` → resume workflow |
| Track active workflows | Map incident_id → thread_id for resume |
| Manage consumer offsets | Commit after successful processing |
| Handle failures | Dead-letter queue, alerting |

**Consumes**:
- All `incident.*` topics
- All `pipeline.*` topics
- Approval events

**Triggers**:
- `WorkflowOrchestrator.run()` - Start new workflow
- `WorkflowOrchestrator.resume()` - Resume paused workflow

**Does NOT**:
- Publish events (only consumes)
- Make business decisions
- Call external APIs directly
- Store state (uses Redis for tracking)

---

### 4. LangGraph Workflow (Execution Engine)

**Location**: `backend/orchestrator/langgraph_workflow.py`

| Responsibility | Description |
|----------------|-------------|
| Execute workflow nodes | Run 12-node incident workflow |
| Publish state transitions | Emit Kafka events at each step |
| Call LLM for reasoning | Classification, plan generation, judging |
| Call RAG for retrieval | Swarm RAG for runbook search |
| Pause for approval | Checkpoint and wait for human |
| Resume from checkpoint | Continue after approval event |
| Handle errors | Fail fast with proper state |

**Publishes**:
- `incident.received` - Workflow started
- `incident.enriched` - Classification complete
- `incident.plan_generated` - Plan ready
- `incident.requires_approval` - Needs human
- `remediation.started` - Execution begins
- `remediation.executed` - Execution complete
- `incident.verified` - Fix verified
- `incident.close_execute` - Command to close

**Does NOT**:
- Call external APIs directly (publishes commands)
- Manage Kafka consumers
- Serve HTTP requests
- Store persistent state (uses checkpointer)

---

### 5. FastAPI Control Plane (Governance)

**Location**: `backend/app.py`

| Responsibility | Description |
|----------------|-------------|
| Serve UI endpoints | GET requests for dashboard data |
| Handle approvals | POST approval → publish to Kafka |
| Provide API for UI | REST endpoints for React frontend |
| Enforce policies | Check permissions, validate requests |
| Read from Redis/Postgres | CQRS read model queries |

**Publishes**:
- `incident.approved` - Human approved plan
- `incident.rejected` - Human rejected plan
- `incident.created` (via API) - Manual incident creation
- `pipeline.requested` (via API) - Manual pipeline request

**Does NOT**:
- Execute workflows directly
- Call `process_workflow()` (use Kafka)
- Poll external systems
- Store Kafka events
- Make AI decisions

---

### 6. LLM Intelligence (AI Reasoning)

**Location**: `backend/orchestrator/llm_intelligence.py`

| Responsibility | Description |
|----------------|-------------|
| Classify incidents | Determine incident type |
| Generate plans | Create remediation plans |
| Provide reasoning | Chain-of-thought explanations |

**Called by**: LangGraph nodes (`node_classify`, `node_generate_plan`)

**Does NOT**:
- Control workflow flow
- Make approval decisions
- Call external APIs
- Publish Kafka events

---

### 7. LLM Judge (AI Evaluation)

**Location**: `backend/orchestrator/llm_judge.py`

| Responsibility | Description |
|----------------|-------------|
| Evaluate plan quality | Score 1-10 on multiple criteria |
| Check safety | Detect dangerous commands |
| Provide feedback | Recommendations for improvement |

**Called by**: LangGraph node (`node_judge_evaluation`)

**Does NOT**:
- Approve/reject (just evaluates)
- Execute plans
- Control workflow flow

---

### 8. Swarm RAG (Document Retrieval)

**Location**: `backend/rag/swarm_retriever.py`

| Responsibility | Description |
|----------------|-------------|
| Multi-agent retrieval | 4 agents search in parallel |
| RRF fusion | Combine rankings from agents |
| Query Weaviate | Vector similarity search |
| Query Neo4j | Graph-based search |

**Called by**: LangGraph node (`node_swarm_rag`)

**Does NOT**:
- Make decisions based on results
- Execute remediation
- Publish Kafka events

---

### 9. Control Plane Policy Engine

**Location**: `backend/control_plane/policy_engine.py`

| Responsibility | Description |
|----------------|-------------|
| Evaluate risk | Score plan risk level |
| Determine approval route | Auto, async, manual |
| Apply policies | Environment, severity rules |

**Called by**: LangGraph node (`node_control_plane`)

**Does NOT**:
- Approve/reject (just determines route)
- Execute plans
- Publish Kafka events directly

---

### 10. Audit Logger (Compliance)

**Location**: `backend/governance/audit_logger.py`

| Responsibility | Description |
|----------------|-------------|
| Log all AI decisions | EU AI Act compliance |
| Log human oversight | Approval tracking |
| Log data access | GDPR compliance |
| Provide audit trail | Immutable logs |

**Called by**: All components for logging

**Does NOT**:
- Make decisions
- Block operations
- Modify workflow flow

---

## Interaction Matrix

| Component | Kafka Pub | Kafka Sub | LLM | RAG | HTTP | External API |
|-----------|-----------|-----------|-----|-----|------|--------------|
| MCP Servers | Yes | Yes | No | No | No | Yes |
| EventOrchestrator | No | Yes | No | No | No | No |
| LangGraph Workflow | Yes | No | Yes | Yes | No | No |
| FastAPI | Yes | No | No | No | Yes | No |
| LLM Intelligence | No | No | Yes | No | No | No |
| LLM Judge | No | No | Yes | No | No | No |
| Swarm RAG | No | No | No | Yes | No | No |
| Policy Engine | No | No | No | No | No | No |
| Audit Logger | No | No | No | No | No | No |

---

## Event Ownership

### Who Publishes What

| Event | Publisher | Trigger |
|-------|-----------|---------|
| `incident.created` | ServiceNow MCP | Polling detects new incident |
| `incident.received` | LangGraph | `node_ingest` completes |
| `incident.enriched` | LangGraph | `node_classify` completes |
| `incident.plan_generated` | LangGraph | `node_generate_plan` completes |
| `incident.requires_approval` | LangGraph | `node_await_approval` (manual route) |
| `incident.approved` | FastAPI | Human clicks approve |
| `incident.rejected` | FastAPI | Human clicks reject |
| `remediation.started` | LangGraph | `node_execute` starts |
| `remediation.executed` | LangGraph | `node_execute` completes |
| `incident.verified` | LangGraph | `node_verify` completes |
| `incident.close_execute` | LangGraph | `node_close_ticket` (command) |
| `incident.closed` | ServiceNow MCP | After closing ticket |
| `pipeline.requested` | Jira MCP / FastAPI | New request detected / API call |
| `pipeline.requires_approval` | Data Agent | PROD deployment needed |
| `pipeline.approved` | FastAPI | Human approves |
| `pipeline.deploy_execute` | Data Agent | Deployment approved |
| `pipeline.deployed` | Airflow MCP | Deployment complete |

---

## Data Flow

### Incident Workflow Data Flow

```
ServiceNow MCP
    ↓ polls
ServiceNow API
    ↓ publishes
Kafka: incident.created
    ↓ consumes
EventOrchestrator
    ↓ calls
WorkflowOrchestrator.run()
    ↓ executes
LangGraph Nodes
    ↓ publishes
Kafka: incident.* events
    ↓ consumes
State Projector
    ↓ updates
Redis / Postgres
    ↓ queries
FastAPI UI Endpoints
    ↓ serves
React Frontend
```

### Approval Flow Data Flow

```
LangGraph: node_await_approval
    ↓ publishes
Kafka: incident.requires_approval
    ↓ consumes
UI / Slack (displays approval request)
    ↓ user clicks
FastAPI: POST /incidents/{id}/approve
    ↓ publishes
Kafka: incident.approved
    ↓ consumes
EventOrchestrator
    ↓ calls
WorkflowOrchestrator.resume()
    ↓ continues
LangGraph: node_execute
```

---

## See Also

- [ARCHITECTURE_V6_EVENT_DRIVEN.md](ARCHITECTURE_V6_EVENT_DRIVEN.md) - Full architecture
- [KAFKA_TOPICS.md](KAFKA_TOPICS.md) - Topic reference
- [WORKFLOW_FLOWS.md](WORKFLOW_FLOWS.md) - Flow diagrams
- [PATTERNS.md](PATTERNS.md) - Architecture patterns
