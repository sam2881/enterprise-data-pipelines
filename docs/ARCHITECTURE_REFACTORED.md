# AI Agent Platform - Refactored Architecture

## Overview

This document describes the refactored architecture that enforces **strict separation** between agents, backend, and frontend.

## Key Principle: Agent Isolation

**Agents do NOT live in backend or frontend.**

```
ai_agent_app/
├── agents/                 # ALL agent code lives here (ISOLATED)
│   ├── shared/            # Common base classes and interfaces
│   ├── servicenow_agent/  # ServiceNow Platform Agent
│   ├── protocols/         # A2A protocol
│   └── registry.py        # Agent discovery
│
├── data_agent/            # Data Engineering Agent (standalone)
│
├── backend/               # Backend services (NO agent logic)
│   ├── services/          # Uses agents via interfaces
│   ├── control_plane/     # Workflow orchestration
│   ├── infrastructure/    # External clients
│   └── ...
│
└── frontend/              # UI (NO agent logic)
```

## Architecture Layers

### 1. Agents Layer (`agents/`)

**Purpose**: Contains ALL agent logic, completely isolated from backend/frontend.

**Rules**:
- Agents have NO imports from `backend/` or `frontend/`
- Agents implement `IAgentService` interface
- Agents are async-first with determinism tracking
- Agents receive context as task payload (not by accessing backend systems)

**Structure**:
```
agents/
├── __init__.py              # Package exports
├── registry.py              # Agent discovery and registration
│
├── shared/                  # Shared infrastructure
│   ├── interfaces.py       # IAgentService, IAgentTask, IAgentResult
│   ├── base.py             # BaseAgent (all agents extend this)
│   ├── config.py           # AgentConfig
│   └── types.py            # Shared types (RiskLevel, etc.)
│
├── servicenow_agent/        # ServiceNow Platform Agent
│   ├── it_service/         # Incident management
│   │   ├── incident_agent.py
│   │   └── matcher_agent.py
│   ├── remediation/        # Script execution
│   │   └── remediation_agent.py
│   └── service.py          # Unified service (routes to sub-agents)
│
└── protocols/               # Inter-agent communication
    └── a2a/                # Agent-to-Agent protocol
        ├── client.py
        ├── mesh.py
        └── messages.py
```

### 2. Backend Layer (`backend/`)

**Purpose**: Orchestration, external integrations, and API endpoints.

**Rules**:
- Backend does NOT contain agent logic
- Backend uses agents via `IAgentService` interface
- Backend provides context TO agents (RAG results, etc.)
- Backend handles persistence, Kafka, ServiceNow API

**Structure**:
```
backend/
├── app.py                   # FastAPI application
│
├── services/                # Agent interaction layer
│   └── agent_service.py    # Uses agents via registry
│
├── control_plane/           # Workflow orchestration
│   ├── orchestrator.py     # Saga pattern coordinator
│   └── policy_engine.py    # Risk assessment
│
├── infrastructure/          # External clients
│   ├── kafka_client.py
│   ├── redis_client.py
│   └── dataproc_client.py
│
├── rag/                     # Retrieval system (owned by backend)
│   └── hybrid_search_engine.py
│
└── streaming/               # Kafka consumers/producers
```

### 3. Data Agent (`data_agent/`)

**Purpose**: Standalone data engineering agent for pipeline generation.

**Status**: Already properly isolated. Runs as independent microservice.

**Structure**:
```
data_agent/
└── src/
    ├── agents/             # Data engineering agents
    │   ├── analysis_agent.py
    │   ├── planning_agent.py
    │   └── spark_generator_agent.py
    ├── orchestration/      # Internal workflow
    └── api.py              # FastAPI entry point
```

## Communication Patterns

### Pattern 1: Backend → Agent (via Registry)

```python
# Backend creates task with context
from agents import get_agent_registry, IAgentTask

registry = get_agent_registry()
agent = registry.find_by_capability(AgentCapability.INCIDENT_TRIAGE)[0]

# Backend gets RAG context (backend owns RAG)
context = rag.search(query)

# Create task with context
task = IAgentTask(
    task_id="...",
    task_type="incident_triage",
    payload={"incident_id": "...", "description": "..."},
    context={"similar_incidents": context}  # Backend provides context
)

# Execute agent
result = await agent.execute(task)
```

### Pattern 2: Agent → Agent (via A2A Protocol)

```python
# Inside an agent (NOT in backend)
from agents.protocols import A2AClient, MessageType

client = A2AClient(agent_id="my-agent", mesh_url="ws://...")
await client.connect()

# Send message to another agent
await client.send(A2AMessage(
    message_type=MessageType.SWARM_QUERY,
    payload={"query": "..."}
))
```

### Pattern 3: External System → Backend → Agent

```
ServiceNow Incident
    ↓
Kafka Topic (incident.created)
    ↓
Backend Consumer (backend/streaming/)
    ↓
BackendAgentService.triage_incident()
    ↓
Agent Registry → IncidentAgent.execute()
    ↓
IAgentResult
    ↓
Backend persists, publishes events
```

## End-to-End Workflows

### ServiceNow Workflow

```
1. ServiceNow → Kafka (incident.created)
2. Backend Consumer receives event
3. Backend gets RAG context (similar incidents, runbooks)
4. Backend creates IAgentTask with context
5. Registry finds IncidentAgent
6. IncidentAgent.execute() → analysis
7. Registry finds ScriptMatcherAgent
8. ScriptMatcherAgent.execute() → matches
9. PolicyEngine assesses risk
10. If approved: RemediationAgent.execute() → plan
11. Backend executes plan via infrastructure layer
12. Backend updates ServiceNow via API
```

### Data Pipeline Workflow

```
1. Jira → Kafka (ticket.created)
2. Backend Consumer receives event
3. Backend routes to data_agent via HTTP/A2A
4. DataAgent analyzes source schema
5. DataAgent generates Spark code
6. DataAgent generates Airflow DAG
7. Backend creates Git PR
8. CI/CD deploys to Airflow
```

## Migration Guide

### From Old Structure

Old (agent in backend):
```python
# backend/agents/it_service/servicenow_agent.py
from agents.base_agent import BaseAgent
from rag import hybrid_rag  # ❌ Backend import in agent

class ServiceNowAgent(BaseAgent):
    def process_task(self, task):
        context = self.rag.search(...)  # ❌ Agent accesses backend
```

New (isolated agent):
```python
# agents/servicenow_agent/it_service/incident_agent.py
from agents.shared import BaseAgent, IAgentTask

class IncidentAgent(BaseAgent):  # ✅ No backend imports
    async def _execute(self, task: IAgentTask):
        # Context provided by backend via task.context
        context = task.context  # ✅ Backend provides context
```

### Backend Usage

Old:
```python
from backend.agents.it_service import ServiceNowAgent
agent = ServiceNowAgent()
agent.process_task({"description": "..."})
```

New:
```python
from backend.services import get_backend_agent_service
service = get_backend_agent_service()
result = await service.triage_incident(
    incident_id="INC001",
    description="..."
)
```

## Benefits of New Architecture

1. **Agent Isolation**: Agents can be tested independently
2. **Clear Boundaries**: No mixed responsibilities
3. **Swappable**: Change agent implementation without touching backend
4. **Testable**: Mock agents for backend tests, mock backend for agent tests
5. **Scalable**: Agents can run as separate services
6. **Maintainable**: New engineers understand structure immediately

## File Ownership

| Module | Owner | Contains |
|--------|-------|----------|
| `agents/` | Agent Team | Agent logic, interfaces, protocols |
| `data_agent/` | Data Team | Data engineering agent |
| `backend/` | Platform Team | Orchestration, APIs, integrations |
| `frontend/` | UI Team | User interface |
| `infrastructure/` | DevOps | IaC, deployment |
