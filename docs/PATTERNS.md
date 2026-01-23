# Architecture and AI Patterns - v6.0 Enterprise Agentic Platform

## Overview

This document describes the architecture patterns and AI patterns used in the Enterprise Agentic Platform.

---

## Architecture Patterns

### 1. Event Sourcing via Kafka

**What**: Store all state changes as immutable events in Kafka.

**Why**:
- Complete audit trail for compliance
- Ability to replay events for debugging
- Decoupled components communicate via events

**Implementation**:
```python
# Every state change publishes an event
async def publish_workflow_event(topic: str, state: WorkflowState, event_type: str):
    event = {
        "event_type": event_type,
        "incident_id": state.get("incident_id"),
        "correlation_id": state.get("correlation_id"),
        "timestamp": datetime.utcnow().isoformat(),
        "idempotency_key": f"{state.get('incident_id')}:{event_type}:{timestamp}"
    }
    await kafka_producer.publish_event(topic=topic, event=event, key=incident_id)
```

**File**: [backend/orchestrator/langgraph_workflow.py](../backend/orchestrator/langgraph_workflow.py)

---

### 2. CQRS (Command Query Responsibility Segregation)

**What**: Separate write path (commands → Kafka) from read path (queries → Redis/Postgres).

**Why**:
- Commands go to Kafka (system of record)
- Queries read from optimized read models
- Eventually consistent but highly scalable

**Implementation**:
```
WRITE PATH (Commands):
  FastAPI → Kafka → Consumers update Redis/Postgres

READ PATH (Queries):
  FastAPI → Redis/Postgres → UI
```

**Files**:
- Write: [backend/app.py](../backend/app.py) - Approval endpoints publish to Kafka
- Read: [backend/app.py](../backend/app.py) - UI endpoints read from Redis/Postgres

---

### 3. Saga Pattern (LangGraph Orchestration)

**What**: Long-running workflows with compensation (rollback) on failure.

**Why**:
- Distributed transactions across multiple services
- Automatic rollback on failure
- Human approval as a saga step

**Implementation**:
```python
# LangGraph workflow as a saga
workflow = StateGraph(WorkflowState)
workflow.add_node("ingest", node_ingest)
workflow.add_node("execute", node_execute)
workflow.add_node("rollback", node_rollback)  # Compensation

# Conditional routing on failure
workflow.add_conditional_edges(
    "execute",
    lambda state: "rollback" if state.get("errors") else "verify",
    {"rollback": "rollback", "verify": "verify"}
)
```

**File**: [backend/orchestrator/langgraph_workflow.py](../backend/orchestrator/langgraph_workflow.py)

---

### 4. Hub-and-Spoke Architecture

**What**: EventOrchestrator as central hub routing events to workflows (spokes).

**Why**:
- Single point for event routing logic
- Decoupled MCPs and workflows
- Easy to add new event types

**Implementation**:
```python
class EventOrchestrator:
    async def route_event(self, event: dict):
        event_type = event.get("event_type")

        if event_type == "incident.created":
            await self.start_incident_workflow(event)
        elif event_type == "incident.approved":
            await self.resume_incident_workflow(event)
        elif event_type == "pipeline.requested":
            await self.start_pipeline_workflow(event)
```

**File**: [backend/streaming/consumers/event_orchestrator.py](../backend/streaming/consumers/event_orchestrator.py)

---

### 5. Adapter Pattern (MCP Servers)

**What**: MCP servers as edge adapters translating between external systems and Kafka.

**Why**:
- Isolate external system complexity
- Standardize event format
- Handle polling, authentication, retry logic

**Implementation**:
```
External System → MCP Server → Kafka (normalized event)
Kafka (command) → MCP Server → External System API
```

**Files**:
- [mcp-servers/servicenow-mcp/event_driven_server.py](../mcp-servers/servicenow-mcp/event_driven_server.py)
- [mcp-servers/jira-mcp/event_driven_server.py](../mcp-servers/jira-mcp/event_driven_server.py)

---

## AI Patterns

### 1. LangGraph StateGraph (Deterministic Orchestration)

**What**: Use LangGraph StateGraph for workflow control, NOT ReAct pattern.

**Why**:
- Deterministic flow control (graph structure)
- LLM reasons within nodes, doesn't control flow
- Explicit state transitions, auditable

**Implementation**:
```python
# CORRECT: LangGraph StateGraph
workflow = StateGraph(WorkflowState)
workflow.add_node("classify", node_classify)  # LLM classifies
workflow.add_node("generate_plan", node_generate_plan)  # LLM generates
workflow.add_edge("classify", "generate_plan")  # Deterministic flow

# WRONG: ReAct pattern (LLM controls loop)
# agent = create_react_agent(llm, tools)  # DON'T DO THIS
```

**File**: [backend/orchestrator/langgraph_workflow.py](../backend/orchestrator/langgraph_workflow.py)

---

### 2. Chain-of-Thought (Plan Generation)

**What**: LLM generates step-by-step reasoning for remediation plans.

**Why**:
- Transparent decision making
- Better quality plans
- Auditable reasoning for compliance

**Implementation**:
```python
async def generate_remediation_plan(self, incident_context: dict, rag_results: list):
    prompt = f"""
    You are an IT operations expert. Analyze this incident and create a remediation plan.

    INCIDENT:
    {json.dumps(incident_context, indent=2)}

    AVAILABLE RUNBOOKS:
    {self._format_rag_results(rag_results)}

    Think step-by-step:
    1. What is the root cause?
    2. Which runbook best addresses this?
    3. What are the execution steps?
    4. What could go wrong?
    5. What is the rollback plan?

    Respond in JSON format with reasoning field.
    """
    response = await self.llm.invoke(prompt)
    return self._parse_plan(response)
```

**File**: [backend/orchestrator/llm_intelligence.py](../backend/orchestrator/llm_intelligence.py)

---

### 3. Self-Reflection (LLM-as-Judge)

**What**: Use a separate LLM call to evaluate and critique generated plans.

**Why**:
- Catch errors before execution
- Improve plan quality through feedback
- Safety check (prevent dangerous actions)

**Implementation**:
```python
class LLMJudge:
    async def evaluate_plan(self, plan: dict, incident_context: dict):
        prompt = f"""
        You are a senior SRE reviewing a remediation plan.

        PLAN:
        {json.dumps(plan, indent=2)}

        INCIDENT:
        {json.dumps(incident_context, indent=2)}

        Evaluate on these criteria (score 1-10):
        1. Quality: Is the plan well-structured and complete?
        2. Safety: Could this cause harm? Check for rm -rf, DROP TABLE, etc.
        3. Factual: Is the plan grounded in the runbook evidence?
        4. Feasibility: Can this be executed automatically?

        Return JSON with scores and reasoning.
        """
        return await self._evaluate(prompt)
```

**File**: [backend/orchestrator/llm_judge.py](../backend/orchestrator/llm_judge.py)

---

### 4. Swarm Intelligence (RAG Agent Voting)

**What**: Multiple specialized RAG agents vote on best runbook using RRF fusion.

**Why**:
- Diverse retrieval strategies
- Robust to single-agent failures
- Higher quality results through consensus

**Implementation**:
```python
class SwarmRetriever:
    def __init__(self):
        self.agents = [
            VectorRAGAgent(),      # Semantic similarity
            KeywordRAGAgent(),     # BM25 keyword matching
            GraphRAGAgent(),       # Neo4j relationship traversal
            HybridRAGAgent(),      # Combined approach
        ]

    async def retrieve(self, query: str) -> List[Document]:
        # Each agent retrieves independently
        results = await asyncio.gather(*[
            agent.retrieve(query) for agent in self.agents
        ])

        # RRF fusion to combine rankings
        return self._rrf_fusion(results)

    def _rrf_fusion(self, result_lists: List[List[Document]], k: int = 60) -> List[Document]:
        scores = {}
        for results in result_lists:
            for rank, doc in enumerate(results):
                if doc.id not in scores:
                    scores[doc.id] = {"doc": doc, "score": 0}
                scores[doc.id]["score"] += 1 / (k + rank + 1)

        return sorted(scores.values(), key=lambda x: x["score"], reverse=True)
```

**File**: [backend/rag/swarm_retriever.py](../backend/rag/swarm_retriever.py)

---

### 5. Plan-Execute (Data Agent)

**What**: Separate planning phase from execution phase with validation in between.

**Why**:
- Validate before executing
- Human approval at plan stage
- Easier debugging and iteration

**Implementation**:
```python
# Data Agent LangGraph workflow
workflow = StateGraph(DataAgentState)

# Phase 1: Planning (LLM reasons)
workflow.add_node("planner", planner_agent)

# Phase 2: Generation (deterministic Jinja2)
workflow.add_node("generator", generator_agent)

# Phase 3: Validation (code analysis, dry run)
workflow.add_node("validator", validator_agent)

# Phase 4: Human Approval (if PROD)
workflow.add_node("await_approval", approval_node)

# Phase 5: Deployment (git, CI/CD)
workflow.add_node("deployer", deployer_agent)
```

**File**: [agents/data_agent/src/graphs/main_graph.py](../agents/data_agent/src/graphs/main_graph.py)

---

### 6. Human-in-the-Loop (Approval Checkpoints)

**What**: Pause workflow for human approval, resume from Kafka event.

**Why**:
- Mandatory for production changes
- EU AI Act compliance (human oversight)
- Catch AI errors before execution

**Implementation**:
```python
# LangGraph with interrupt_after for pause/resume
compiled = workflow.compile(
    checkpointer=MemorySaver(),
    interrupt_after=["await_approval"]  # Pause here
)

# Workflow publishes approval request
await publish_workflow_event(
    topic=Topics.INCIDENT_REQUIRES_APPROVAL,
    state=state,
    event_type="incident.requires_approval"
)
# Workflow pauses at checkpoint

# EventOrchestrator resumes when incident.approved arrives
async def handle_approval(event: dict):
    await workflow_orchestrator.resume(
        incident_id=event["incident_id"],
        approval_decision={"approved": True, "approver": event["approved_by"]}
    )
```

**File**: [backend/orchestrator/langgraph_workflow.py](../backend/orchestrator/langgraph_workflow.py)

---

## Anti-Patterns to Avoid

### 1. ReAct Pattern for Workflow Control

**Wrong**:
```python
# LLM decides what to do next - unpredictable
agent = create_react_agent(llm, tools=[
    search_tool, execute_tool, verify_tool
])
result = agent.invoke("Fix the database issue")
```

**Right**:
```python
# LangGraph controls flow - deterministic
workflow = StateGraph(WorkflowState)
workflow.add_node("search", node_search)
workflow.add_node("execute", node_execute)
workflow.add_edge("search", "execute")  # Explicit flow
```

### 2. Direct API Calls from Workflows

**Wrong**:
```python
# LangGraph calling ServiceNow directly
def node_close_ticket(state):
    requests.post(f"{SERVICENOW_URL}/api/now/table/incident/{sys_id}", ...)
```

**Right**:
```python
# Publish command event - MCP executes
def node_close_ticket(state):
    await publish_workflow_event(
        topic=Topics.INCIDENT_CLOSE_EXECUTE,
        state=state,
        event_type="incident.close_execute"
    )
```

### 3. Implicit State in LLM Memory

**Wrong**:
```python
# Relying on LLM conversation history for state
response = llm.invoke("What was the error again?")
```

**Right**:
```python
# Explicit state in TypedDict
class WorkflowState(TypedDict):
    incident_id: str
    error_message: str
    classification: str
    # ... all state is explicit
```

---

## Pattern Selection Guide

| Scenario | Pattern | File |
|----------|---------|------|
| State transitions | Event Sourcing | langgraph_workflow.py |
| UI reads | CQRS | app.py |
| Multi-step workflow | Saga | langgraph_workflow.py |
| Event routing | Hub-and-Spoke | event_orchestrator.py |
| External integrations | Adapter | mcp-servers/ |
| Workflow control | LangGraph StateGraph | langgraph_workflow.py |
| Plan generation | Chain-of-Thought | llm_intelligence.py |
| Plan evaluation | Self-Reflection | llm_judge.py |
| Document retrieval | Swarm Intelligence | swarm_retriever.py |
| Code generation | Plan-Execute | data_agent/ |
| Production changes | Human-in-the-Loop | langgraph_workflow.py |

---

## See Also

- [ARCHITECTURE_V6_EVENT_DRIVEN.md](ARCHITECTURE_V6_EVENT_DRIVEN.md) - Full architecture
- [WORKFLOW_FLOWS.md](WORKFLOW_FLOWS.md) - Flow diagrams
- [RESPONSIBILITY_MATRIX.md](RESPONSIBILITY_MATRIX.md) - Component responsibilities
