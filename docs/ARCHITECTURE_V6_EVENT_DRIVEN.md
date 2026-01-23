# Architecture v6.0 - Event-Driven Architecture

## Overview

This document describes the v6.0 event-driven architecture refactoring. The key principle is:

> **Kafka is the system of record. All state transitions flow through Kafka.**

## Mental Model

```
MCPs sense → Kafka remembers → Orchestrator routes → LangGraph reasons & acts → FastAPI governs
```

## Core Principles

### 1. Kafka is the System of Record
- All state transitions are Kafka events
- Events are immutable and ordered
- Consumers can replay from any point
- Idempotency keys prevent duplicate processing

### 2. FastAPI is Control Plane Only
- Serves UI (reads from Redis/Postgres - CQRS)
- Handles human approvals (publishes to Kafka)
- Provides policy management
- Does NOT directly execute workflows

### 3. MCP Servers are Edge Adapters
- Poll external systems (ServiceNow, Jira)
- Publish events to Kafka
- Consume command events from Kafka
- Execute actions on external systems

### 4. LangGraph Owns Execution and Reasoning
- Consumes events from Kafka (via EventOrchestrator)
- Processes through workflow nodes
- Publishes state transition events
- Pauses at approval nodes, resumes from Kafka events

### 5. Humans Approve, Agents Execute
- Every significant action requires approval
- Approvals flow through Kafka
- Agents only execute after explicit approval

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         EVENT-DRIVEN ARCHITECTURE v6.0                          │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  ┌──────────────────┐                           ┌──────────────────┐           │
│  │  ServiceNow MCP  │────poll────▶ ServiceNow   │    Jira MCP      │──poll───▶│
│  │  (Edge Adapter)  │◀───────────              │  (Edge Adapter)  │◀──────── │
│  └────────┬─────────┘                           └────────┬─────────┘           │
│           │ publish                                      │ publish              │
│           ▼                                              ▼                      │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │                           KAFKA CLUSTER                                   │  │
│  │  ┌───────────────────────────────────────────────────────────────────┐   │  │
│  │  │                    INCIDENT LIFECYCLE TOPICS                       │   │  │
│  │  │  incident.created → received → enriched → plan_generated →        │   │  │
│  │  │  requires_approval → approved/rejected → executed → verified →    │   │  │
│  │  │  close_execute → closed                                           │   │  │
│  │  └───────────────────────────────────────────────────────────────────┘   │  │
│  │  ┌───────────────────────────────────────────────────────────────────┐   │  │
│  │  │                    PIPELINE LIFECYCLE TOPICS                       │   │  │
│  │  │  pipeline.requested → planned → generated → validated →           │   │  │
│  │  │  requires_approval → approved/rejected → deploy_execute → deployed│   │  │
│  │  └───────────────────────────────────────────────────────────────────┘   │  │
│  │  ┌───────────────────────────────────────────────────────────────────┐   │  │
│  │  │                    REMEDIATION TOPICS                              │   │  │
│  │  │  remediation.started → executed → failed → rollback               │   │  │
│  │  └───────────────────────────────────────────────────────────────────┘   │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
│                    │ consume                        │ consume                   │
│                    ▼                                ▼                           │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │                      EVENT ORCHESTRATOR                                   │  │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐          │  │
│  │  │ WorkflowManager │  │  StateTracker   │  │  EventRouter    │          │  │
│  │  │ (Redis state)   │  │ (Redis/Postgres)│  │ (topic mapping) │          │  │
│  │  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘          │  │
│  │           │                    │                    │                     │  │
│  │           └────────────────────┼────────────────────┘                     │  │
│  │                                │                                          │  │
│  │                                ▼                                          │  │
│  │  ┌──────────────────────────────────────────────────────────────────┐    │  │
│  │  │                    LANGGRAPH WORKFLOW                             │    │  │
│  │  │  ingest → parse → classify → swarm_rag → generate_plan →         │    │  │
│  │  │  judge_evaluation → control_plane → await_approval →              │    │  │
│  │  │  [PAUSE] ←──── incident.approved from Kafka                      │    │  │
│  │  │  [RESUME] → execute → verify → close_ticket → feedback_loop      │    │  │
│  │  └──────────────────────────────────────────────────────────────────┘    │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
│                    │ publish events                 ▲                          │
│                    ▼                                │ approval events          │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │                        FASTAPI CONTROL PLANE                              │  │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐          │  │
│  │  │   UI Serving    │  │ Approval APIs   │  │ Policy Engine   │          │  │
│  │  │ (reads Redis/PG)│  │(publishes Kafka)│  │ (governance)    │          │  │
│  │  └─────────────────┘  └─────────────────┘  └─────────────────┘          │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## Event Flow: Incident Lifecycle

### 1. Incident Detection
```
ServiceNow MCP polls → detects new incident → publishes incident.created
```

### 2. Workflow Initiation
```
EventOrchestrator consumes incident.created → triggers LangGraph workflow
```

### 3. Processing
```
LangGraph processes:
  node_ingest      → publishes incident.received
  node_parse       → (internal)
  node_classify    → publishes incident.enriched
  node_swarm_rag   → (internal)
  node_generate_plan → publishes incident.plan_generated
  node_judge_evaluation → (internal)
  node_control_plane → determines approval route
  node_await_approval → publishes incident.requires_approval
                        [WORKFLOW PAUSES]
```

### 4. Human Approval
```
UI displays pending approval → Human clicks Approve/Reject
FastAPI publishes incident.approved or incident.rejected
EventOrchestrator consumes → resumes LangGraph workflow
```

### 5. Execution
```
LangGraph resumes:
  node_process_approval → updates state
  node_execute → publishes remediation.started → triggers GitHub Actions
               → waits for completion → publishes remediation.executed
  node_verify  → publishes incident.verified
  node_close_ticket → publishes incident.close_execute
```

### 6. Ticket Closure
```
ServiceNow MCP consumes incident.close_execute → calls ServiceNow API
ServiceNow MCP publishes incident.closed
```

---

## Key Components

### 1. MCP Servers (`mcp-servers/`)

| Server | Purpose | Publishes | Consumes |
|--------|---------|-----------|----------|
| ServiceNow MCP | Poll incidents, close tickets | incident.created | incident.close_execute |
| Jira MCP | Poll pipeline requests | pipeline.requested | pipeline.completed |

### 2. Kafka Topics (`backend/streaming/schemas.py`)

#### Incident Lifecycle
- `incident.created` - New incident detected by MCP
- `incident.received` - LangGraph started processing
- `incident.enriched` - Classification complete
- `incident.plan_generated` - Remediation plan ready
- `incident.requires_approval` - Pending human approval
- `incident.approved` - Human approved
- `incident.rejected` - Human rejected
- `incident.close_execute` - Command to MCP to close
- `incident.closed` - Workflow complete

#### Pipeline Lifecycle
- `pipeline.requested` - New request from Jira
- `pipeline.planned` - Planning complete
- `pipeline.generated` - Code generated
- `pipeline.validated` - Validation passed
- `pipeline.requires_approval` - Pending human
- `pipeline.approved` - Human approved
- `pipeline.deploy_execute` - Command to deploy
- `pipeline.deployed` - Deployment complete

#### Remediation
- `remediation.started` - Execution began
- `remediation.executed` - Execution complete
- `remediation.failed` - Execution failed
- `remediation.rollback` - Rollback triggered

### 3. Event Orchestrator (`backend/streaming/consumers/event_orchestrator.py`)

The central router that:
- Consumes all lifecycle events
- Routes to appropriate LangGraph workflows
- Manages workflow state in Redis
- Handles pause/resume for approvals

### 4. LangGraph Workflow (`backend/orchestrator/langgraph_workflow.py`)

v6.0 changes:
- Publishes Kafka events at each state transition
- Uses MemorySaver checkpointer for pause/resume
- `interrupt_after=["await_approval"]` enables pause
- `node_close_ticket` publishes command event (doesn't call ServiceNow directly)

### 5. FastAPI Control Plane (`backend/app.py`)

v6.0 responsibilities:
- Serve UI (CQRS - reads from Redis/Postgres)
- Approval endpoints publish to Kafka
- Policy engine (governance)
- Does NOT directly execute workflows

---

## CQRS Pattern

```
┌─────────────────────────────────────────────────────────────────┐
│                       CQRS ARCHITECTURE                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  COMMAND SIDE                      QUERY SIDE                   │
│  ════════════                      ══════════                   │
│                                                                 │
│  ┌─────────────┐                   ┌─────────────┐             │
│  │   FastAPI   │                   │   FastAPI   │             │
│  │  Approval   │                   │     UI      │             │
│  │  Endpoints  │                   │  Endpoints  │             │
│  └──────┬──────┘                   └──────┬──────┘             │
│         │                                 │                     │
│         ▼                                 ▼                     │
│  ┌─────────────┐                   ┌─────────────┐             │
│  │    Kafka    │                   │   Redis /   │             │
│  │   Topics    │──────────────────▶│  Postgres   │             │
│  └─────────────┘  (consumers       └─────────────┘             │
│                    update)                                      │
│                                                                 │
│  Commands go to Kafka              Queries read from Redis/PG  │
│  Events are processed              State is eventually         │
│  by consumers                      consistent                   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Pause/Resume Architecture

### How Approvals Work

1. **LangGraph publishes** `incident.requires_approval` with:
   - `approval_token` - Unique ID for this approval request
   - `plan` - The remediation plan to approve
   - `judge_score` - LLM-as-Judge evaluation

2. **Workflow pauses** at `node_await_approval` (via `interrupt_after`)

3. **FastAPI** receives approval from UI, publishes `incident.approved`:
   - `incident_id` - Which incident
   - `approved` - Boolean
   - `approver` - Who approved
   - `approval_token` - Links to original request

4. **EventOrchestrator** consumes approval, calls:
   ```python
   workflow_orchestrator.resume(
       incident_id=incident_id,
       approval_decision={
           "approved": True,
           "approver": "user@company.com"
       }
   )
   ```

5. **LangGraph resumes** from checkpoint, continues execution

---

## Directory Structure

```
backend/
├── streaming/
│   ├── consumers/
│   │   ├── __init__.py
│   │   └── event_orchestrator.py    # Main Kafka consumer, routes to workflows
│   ├── kafka_producer.py            # Unified producer for all events
│   └── schemas.py                   # Topics, event schemas, serialization
├── orchestrator/
│   ├── langgraph_workflow.py        # v6.0 event-driven workflow
│   ├── llm_judge.py                 # LLM-as-Judge evaluator
│   └── llm_intelligence.py          # LLM operations
├── mcp/
│   └── servers/                     # Tool servers (RAG, GCS, LLM)
└── app.py                           # FastAPI control plane

mcp-servers/
├── servicenow-mcp/
│   └── event_driven_server.py       # Polls ServiceNow, publishes events
└── jira-mcp/
    └── event_driven_server.py       # Polls Jira, publishes events
```

---

## Migration from v5.0

### What Changed

| Component | v5.0 | v6.0 |
|-----------|------|------|
| Approval waiting | Poll Redis | Kafka event + checkpoint |
| ServiceNow close | Direct API call | Publish `incident.close_execute` |
| Workflow start | FastAPI calls LangGraph | EventOrchestrator triggers |
| State updates | Direct in workflow | Kafka events → consumers update |

### Breaking Changes

1. **Approval flow** - UI must publish to Kafka, not call workflow directly
2. **ServiceNow closure** - MCP server must consume close events
3. **State queries** - Read from Redis/Postgres, not Kafka

---

## Best Practices

### 1. Event Design
- Include `idempotency_key` for deduplication
- Include `correlation_id` for tracing
- Timestamp in ISO 8601 format

### 2. Consumer Design
- Commit offsets after successful processing
- Handle duplicates gracefully
- Log with correlation_id for tracing

### 3. Error Handling
- Publish to dead-letter queue on failure
- Include error details in failure events
- Enable retry with backoff

---

## Running the System

```bash
# 1. Start Kafka
docker-compose up -d kafka zookeeper

# 2. Start MCP servers
python -m mcp-servers.servicenow-mcp.event_driven_server &
python -m mcp-servers.jira-mcp.event_driven_server &

# 3. Start Event Orchestrator
python -c "from backend.streaming.consumers import EventOrchestrator; EventOrchestrator().start()"

# 4. Start FastAPI
uvicorn backend.app:app --host 0.0.0.0 --port 8000
```

---

## Observability

All events include:
- `correlation_id` - Trace across systems
- `timestamp` - Event time
- `event_type` - Classification

Use Grafana + Prometheus + Tempo for:
- Event flow visualization
- Latency tracking
- Error alerting
