# AI Incident Management Platform: Complete Technical Whitepaper

## Enterprise Incident Resolution System v5.0

**Document Classification**: Internal Engineering Reference
**Version**: 5.0.0
**Last Updated**: December 2024

---

## Executive Summary

This document provides a complete, step-by-step explanation of an AI-powered incident management platform that automatically resolves infrastructure incidents originating from ServiceNow. The system uses a multi-agent architecture with Kafka event streaming, Swarm RAG for intelligent script retrieval, LLM-as-Judge for plan validation, and human-in-the-loop approval workflows.

**Total Steps in Lifecycle**: 24 discrete steps from incident creation to closure.

---

## Table of Contents

1. [System Overview](#system-overview)
2. [Step-by-Step Incident Lifecycle](#step-by-step-incident-lifecycle)
3. [End-to-End Timeline Summary](#end-to-end-timeline-summary)
4. [Architecture Layer Map](#architecture-layer-map)
5. [Design Patterns Used](#design-patterns-used)
6. [Why This Architecture Works](#why-this-architecture-works)
7. [Component Dependency Analysis](#component-dependency-analysis)

---

## System Overview

### What This System Does

When an IT incident occurs (e.g., a GCP VM goes down), this platform:

1. Detects the incident via ServiceNow
2. Automatically analyzes the problem using AI agents
3. Finds the best remediation script using multi-source search
4. Validates the plan using an independent LLM judge
5. Routes for human approval based on risk level
6. Executes the fix via GitHub Actions
7. Verifies the resolution and updates its knowledge base

### Core Technologies

| Component | Technology | Purpose |
|-----------|------------|---------|
| Event Bus | Apache Kafka | Reliable event streaming |
| Workflow Engine | LangGraph | State machine orchestration |
| Vector Database | Weaviate | Semantic search |
| Graph Database | Neo4j | Relationship-based retrieval |
| Cache | Redis | State and embedding cache |
| Execution | GitHub Actions | Infrastructure automation |
| LLM | OpenAI GPT-4 | Reasoning and generation |
| Frontend | Next.js 14 | User interface |
| Backend | FastAPI (Python) | API and orchestration |

---

## Step-by-Step Incident Lifecycle

---

### STEP 1: Incident Creation in ServiceNow

**Purpose:**
- Capture the initial problem report in the enterprise ITSM system
- Establish the incident as the authoritative record for tracking and compliance

**What Happens (Layman View):**
A support engineer or automated monitoring system creates a ticket in ServiceNow describing a problem, such as "Production VM test-incident-vm-01 is not responding in GCP zone us-central1-a."

**How It Works (Technical View):**
- **Protocol**: ServiceNow REST API (Table API)
- **Sync/Async**: Synchronous creation
- **Data Format**: ServiceNow incident record with fields:
  - `number`: INC0010001
  - `short_description`: Problem summary
  - `description`: Detailed information
  - `priority`: 1-5 scale
  - `state`: 1 (New)
  - `assignment_group`: Infrastructure team
  - `cmdb_ci`: Configuration item reference

**Layers Involved:**
- **Infrastructure Layer**: ServiceNow SaaS platform
- **Protocol Layer**: HTTPS/REST
- **Agent Layer**: Not yet involved
- **Reasoning Layer**: Not yet involved

**Files Involved:**
- `backend/agents/servicenow/agent.py` → ServiceNow API client (will poll this later)
- `backend/mcp/servicenow_server.py` → MCP server for ServiceNow operations

**Failure & Fallback:**
- **What can fail**: ServiceNow API unavailable, authentication failure
- **System reaction**: Incident creation fails; user receives error
- **Circuit breaker**: Not applicable at this stage (user-initiated)

---

### STEP 2: Incident Detection and Polling

**Purpose:**
- Detect new incidents in ServiceNow that require automated processing
- Transform external ITSM data into internal event format

**What Happens (Layman View):**
The AI platform periodically checks ServiceNow for new incidents. When it finds one assigned to the AI-enabled group, it captures the incident details.

**How It Works (Technical View):**
- **Protocol**: ServiceNow REST API (Table API with query parameters)
- **Sync/Async**: Asynchronous polling (every 30 seconds)
- **Push vs Pull**: Pull-based (system polls ServiceNow)
- **Query**:
  ```
  GET /api/now/table/incident?sysparm_query=state=1^assignment_group=AI_AGENT_GROUP&sysparm_limit=50
  ```
- **Decision Logic**: Only incidents in state=1 (New) with correct assignment group are processed

**Layers Involved:**
- **Infrastructure Layer**: ServiceNow API, Network
- **Protocol Layer**: HTTPS/REST, JSON
- **Agent Layer**: ServiceNow Agent (poller)
- **Reasoning Layer**: Not yet involved

**Files Involved:**
- `backend/agents/servicenow/agent.py:fetch_new_incidents()` → Polls ServiceNow API
- `backend/streaming/incident_sources.py` → Alternative source handlers
- `backend/utils/circuit_breaker.py:servicenow_breaker` → Protects against API failures

**Failure & Fallback:**
- **What can fail**: ServiceNow API rate limiting, network timeout, authentication expiry
- **System reaction**:
  - Circuit breaker opens after 5 consecutive failures
  - System waits 30 seconds before retry
  - Incidents remain in ServiceNow queue (no data loss)
- **Circuit breaker behavior**:
  - State: CLOSED → OPEN after 5 failures
  - Timeout: 30 seconds
  - Half-open: Allows 1 test request

---

### STEP 3: Kafka Producer - Event Publication

**Purpose:**
- Transform the ServiceNow incident into a standardized event format
- Publish to Kafka for reliable, ordered processing
- Decouple ingestion from processing

**What Happens (Layman View):**
The system converts the ServiceNow ticket into a message and places it in a queue. This ensures the incident won't be lost even if later systems are temporarily unavailable.

**How It Works (Technical View):**
- **Protocol**: Kafka Producer API
- **Sync/Async**: Asynchronous with acknowledgment
- **Topic**: `servicenow.incidents`
- **Partition Key**: `incident_id` (ensures ordering per incident)
- **Data Format**: JSON event schema
  ```json
  {
    "event_type": "incident.created",
    "incident_id": "INC0010001",
    "timestamp": "2024-12-31T10:00:00Z",
    "source": "servicenow",
    "payload": {
      "short_description": "VM test-incident-vm-01 is down",
      "priority": "3",
      "state": "1",
      "cmdb_ci": "test-incident-vm-01"
    }
  }
  ```
- **Delivery Guarantee**: At-least-once (acks=all)

**Layers Involved:**
- **Infrastructure Layer**: Kafka cluster (Docker deployment)
- **Protocol Layer**: Kafka binary protocol
- **Agent Layer**: ServiceNow Agent (producer role)
- **Reasoning Layer**: Not yet involved

**Files Involved:**
- `backend/utils/kafka_client.py:KafkaEventProducer.publish()` → Publishes events
- `backend/streaming/schemas.py` → Event schema definitions
- `backend/agents/servicenow/agent.py:process_incident()` → Triggers publication

**Failure & Fallback:**
- **What can fail**: Kafka broker unavailable, network partition, serialization error
- **System reaction**:
  - Retries with exponential backoff (3 attempts)
  - Events buffered in memory temporarily
  - Failed events logged with full payload for manual recovery
- **Circuit breaker**: Kafka producer has built-in retry mechanism

---

### STEP 4: Kafka Topic Storage and Ordering

**Purpose:**
- Durably store events until consumers process them
- Maintain strict ordering within partitions
- Enable replay capability for debugging and recovery

**What Happens (Layman View):**
The message sits in a queue organized by incident ID. Multiple incidents can be processed in parallel, but updates to the same incident are always processed in order.

**How It Works (Technical View):**
- **Protocol**: Kafka internal replication protocol
- **Topics Used**:
  - `servicenow.incidents` → New incidents from ServiceNow
  - `gcp.alerts` → Alerts from GCP monitoring
  - `agent.events` → Inter-agent communication
  - `incident.approved` → Approved execution plans
  - `incident.executed` → Completed executions
- **Partitioning Strategy**: Hash of `incident_id` mod 3 partitions
- **Retention**: 7 days (168 hours)
- **Replication Factor**: 1 (development), 3 (production)

**Layers Involved:**
- **Infrastructure Layer**: Kafka brokers, ZooKeeper
- **Protocol Layer**: Kafka replication protocol
- **Agent Layer**: Not directly involved
- **Reasoning Layer**: Not directly involved

**Files Involved:**
- `deployment/docker-compose.yml` → Kafka broker configuration
- `backend/streaming/schemas.py` → Topic and schema definitions

**Failure & Fallback:**
- **What can fail**: Broker crash, disk full, network partition
- **System reaction**:
  - Kafka replicates to surviving brokers
  - Producers receive timeout error and retry
  - Consumers resume from last committed offset
- **Dead Letter Queue**: Failed messages after max retries go to `*.dlq` topics

---

### STEP 5: Event Consumer Initialization

**Purpose:**
- Subscribe to relevant Kafka topics
- Establish consumer group for load balancing
- Begin processing the event stream

**What Happens (Layman View):**
The AI system's "listener" wakes up and starts reading messages from the queue, ready to process each incident.

**How It Works (Technical View):**
- **Protocol**: Kafka Consumer API
- **Consumer Group**: `ai-agent-orchestrator`
- **Subscription**: `servicenow.incidents`, `gcp.alerts`
- **Offset Management**: Manual commit after successful processing
- **Polling Interval**: 100ms
- **Max Poll Records**: 10 (batch size)

**Layers Involved:**
- **Infrastructure Layer**: Kafka client library
- **Protocol Layer**: Kafka consumer protocol
- **Agent Layer**: Event Orchestrator (hub)
- **Reasoning Layer**: Not yet involved

**Files Involved:**
- `backend/streaming/incident_consumer.py:IncidentConsumer` → Main consumer class
- `backend/streaming/incident_consumer.py:start()` → Consumer loop
- `backend/orchestrator/main.py` → Orchestrator initialization

**Failure & Fallback:**
- **What can fail**: Consumer crash, rebalance storm, deserialization error
- **System reaction**:
  - Consumer group rebalances automatically
  - Failed message processing triggers retry
  - Poison messages (unparseable) sent to DLQ after 3 attempts

---

### STEP 6: Event Deserialization and Validation

**Purpose:**
- Parse the raw Kafka message into structured data
- Validate required fields are present
- Reject malformed events early

**What Happens (Layman View):**
The system unpacks the message and checks that it has all the necessary information (incident ID, description, priority, etc.) before trying to process it.

**How It Works (Technical View):**
- **Protocol**: JSON deserialization
- **Validation Rules**:
  - `incident_id` must be non-empty string
  - `short_description` must exist
  - `priority` must be 1-5 or P1-P4
  - `timestamp` must be valid ISO 8601
- **Schema**: Pydantic model validation
  ```python
  class IncidentEvent(BaseModel):
      event_type: str
      incident_id: str
      timestamp: datetime
      source: Literal["servicenow", "gcp", "manual"]
      payload: IncidentPayload
  ```

**Layers Involved:**
- **Infrastructure Layer**: Python runtime
- **Protocol Layer**: JSON, Pydantic
- **Agent Layer**: Event Orchestrator
- **Reasoning Layer**: Not yet involved

**Files Involved:**
- `backend/streaming/schemas.py:IncidentEvent` → Event model
- `backend/streaming/incident_consumer.py:_deserialize_event()` → Parsing logic
- `backend/guardrails/llm_guardrails.py` → Input validation

**Failure & Fallback:**
- **What can fail**: Malformed JSON, missing required fields, type mismatch
- **System reaction**:
  - Validation error logged with full payload
  - Event marked as failed, offset committed
  - Event sent to DLQ for investigation

---

### STEP 7: LangGraph Workflow Initialization

**Purpose:**
- Create a new workflow instance for this incident
- Initialize the 7-node state machine
- Begin deterministic orchestration of the resolution process

**What Happens (Layman View):**
The system creates a "game plan" for how it will handle this incident, setting up a series of steps it must follow in order.

**How It Works (Technical View):**
- **Protocol**: LangGraph state machine
- **Sync/Async**: Asynchronous (async/await)
- **Workflow ID**: `wf-{uuid4()}`
- **Initial State**:
  ```python
  {
      "workflow_id": "wf-abc123",
      "incident_id": "INC0010001",
      "current_node": "receive_parse",
      "status": "active",
      "node_outputs": {},
      "created_at": "2024-12-31T10:00:00Z"
  }
  ```
- **7-Node Architecture**:
  1. `receive_parse` → Ingestion
  2. `swarm_rag` → Retrieval
  3. `generate_plan` → Planning
  4. `llm_judge` → Validation
  5. `control_plane` → Approval
  6. `execute` → Execution
  7. `verify_close` → Completion

**Layers Involved:**
- **Infrastructure Layer**: Python asyncio, Redis (state storage)
- **Protocol Layer**: LangGraph internal protocol
- **Agent Layer**: LangGraph Workflow Engine
- **Reasoning Layer**: Not yet involved (orchestration only)

**Files Involved:**
- `backend/orchestrator/langgraph_workflow.py:IncidentWorkflow` → Workflow definition
- `backend/orchestrator/langgraph_workflow.py:create_workflow()` → Factory function
- `backend/orchestrator/main.py:WORKFLOW_STATES` → In-memory state store

**Failure & Fallback:**
- **What can fail**: State initialization error, Redis unavailable
- **System reaction**:
  - Workflow creation fails, event returned to Kafka
  - Retry with exponential backoff
  - Alert raised if persistent failure

---

### STEP 8: Node 1 - Receive and Parse Incident Context

**Purpose:**
- Extract structured information from the incident description
- Identify the affected service, severity, and key entities
- Prepare context for downstream processing

**What Happens (Layman View):**
The AI reads the incident ticket and figures out what's actually wrong - identifying that this is a "VM down" problem affecting "test-incident-vm-01" in "GCP".

**How It Works (Technical View):**
- **Protocol**: Internal function call (no network)
- **Sync/Async**: Synchronous within async workflow
- **Processing Logic**:
  1. Parse `short_description` and `description` fields
  2. Extract entities using regex and NLP
  3. Classify incident type (infrastructure, application, network)
  4. Determine severity based on priority and keywords
- **Output Schema**:
  ```python
  {
      "incident_id": "INC0010001",
      "service": "test-incident-vm-01",
      "service_type": "gcp_compute",
      "classification": "infrastructure/gcp/vm_down",
      "severity": "P3",
      "entities": {
          "instance_name": "test-incident-vm-01",
          "zone": "us-central1-a",
          "project": "my-project"
      },
      "keywords": ["vm", "down", "gcp", "compute", "restart"]
  }
  ```

**Layers Involved:**
- **Infrastructure Layer**: Python runtime
- **Protocol Layer**: None (in-process)
- **Agent Layer**: LangGraph Node 1
- **Reasoning Layer**: Rule-based extraction + optional LLM enhancement

**Files Involved:**
- `backend/orchestrator/langgraph_workflow.py:_node_receive_parse()` → Node implementation
- `backend/rag/query_understanding.py:QueryUnderstanding` → Entity extraction
- `backend/orchestrator/llm_intelligence.py:analyze_incident_with_llm()` → LLM enhancement

**Failure & Fallback:**
- **What can fail**: Parsing error, unrecognized incident format
- **System reaction**:
  - Falls back to raw text if structured parsing fails
  - Continues with lower confidence score
  - Logged for improvement of parsing rules

---

### STEP 9: Node 2 - Swarm RAG with RRF Fusion

**Purpose:**
- Find the best remediation script(s) for this incident
- Use multiple search strategies in parallel for maximum recall
- Apply RRF (Reciprocal Rank Fusion) for fair, weight-free consensus
- Rerank with cross-encoder for precision

**What Happens (Layman View):**
Four different "search agents" simultaneously look for solutions: one searches by meaning (semantic), one by exact words (keyword), one by past relationships (graph), and one by metadata. Instead of manually weighted voting, they use RRF - a fair fusion algorithm that combines rankings without bias.

**How It Works (Technical View):**
- **Protocol**: A2A (Agent-to-Agent) for coordination, MCP for tool calls
- **Sync/Async**: Asynchronous parallel execution
- **Architecture**: Swarm RRF Fusion (v5.0)

**Sub-Step 9.1: Query Understanding**
```python
# Input: Incident context from Step 8
# Output: Expanded query with intent
{
    "original_query": "VM test-incident-vm-01 is down",
    "intent": "RESTART",
    "service_type": "GCP",
    "expanded_query": "VM down restart recover fix compute GCP instance",
    "entities": {"instance": "test-incident-vm-01", "zone": "us-central1-a"}
}
```

**Sub-Step 9.2: Parallel Agent Search**

| Agent | Protocol | Data Source | Search Method |
|-------|----------|-------------|---------------|
| Vector Agent | MCP → Weaviate | Weaviate | Semantic similarity (all-MiniLM-L6-v2) |
| Keyword Agent | Internal | TF-IDF index | BM25 + bigram matching |
| Graph Agent | MCP → Neo4j | Neo4j | FIXED_BY relationship traversal |
| Metadata Agent | Internal | Script registry | Exact field matching |

Each agent returns a **ranked list** of scripts sorted by relevance.

**Sub-Step 9.3: RRF Fusion (Reciprocal Rank Fusion)**

RRF Formula:
```
RRF_Score = Σ (1 / (k + rank_i)) for each agent i
k = 60 (industry standard constant)
```

**WHY RRF over Weighted Consensus:**
- **No weight tuning** - Removes manual bias (no 0.40, 0.25, etc.)
- **Scale-invariant** - Works regardless of raw score magnitudes
- **Industry standard** - Used by Google, Bing, OpenAI, Elasticsearch, Pinecone

**Example RRF Calculation:**
```
Script: start_gcp_instance.sh
- Vector Agent Rank: 1  → 1/(60+1) = 0.0164
- Keyword Agent Rank: 3 → 1/(60+3) = 0.0159
- Graph Agent Rank: 1   → 1/(60+1) = 0.0164
- Metadata Agent Rank: 2 → 1/(60+2) = 0.0161

RRF Score = 0.0164 + 0.0159 + 0.0164 + 0.0161 = 0.0648
```

**Sub-Step 9.4: Cross-Encoder Reranking**
- Model: `ms-marco-MiniLM-L-6-v2`
- Input: Top 20 candidates from RRF
- Output: Top 5 reranked by pairwise relevance
- Improvement: +20-30% precision

**Sub-Step 9.5: Blast Radius Filtering**
- Check each script's `risk_level` against incident severity
- Filter out scripts that could affect more systems than necessary
- Attach risk assessment to each remaining candidate

**Output Schema**:
```python
{
    "matched_scripts": [
        {
            "script_id": "gcp_restart_vm_001",
            "name": "start_gcp_instance.sh",
            "rrf_score": 0.0648,
            "rerank_score": 0.94,
            "final_score": 0.94,
            "agent_ranks": {"vector": 1, "keyword": 3, "graph": 1, "metadata": 2},
            "agent_scores": {"vector": 0.95, "keyword": 0.72, "graph": 0.91, "metadata": 0.85},
            "risk_level": "low",
            "historical_success_count": 47,
            "avg_resolution_time": "3.2 minutes"
        }
    ],
    "search_metadata": {
        "total_candidates": 156,
        "after_rrf_fusion": 20,
        "after_rerank": 5,
        "search_time_ms": 1842
    }
}
```

**Layers Involved:**
- **Infrastructure Layer**: Weaviate, Neo4j, Redis (cache)
- **Protocol Layer**: A2A for agent coordination, MCP for tool invocation
- **Agent Layer**: Swarm RAG Coordinator, 4 search agents
- **Reasoning Layer**: RRF fusion algorithm, cross-encoder model

**Files Involved:**
- `backend/rag/hybrid_search_engine.py:HybridSearchEngine.search_rrf()` → RRF search
- `backend/rag/swarm_retriever.py:SwarmRetriever` → Swarm coordinator
- `backend/rag/swarm_script_selector.py:SwarmScriptSelector` → Script-specific swarm
- `backend/rag/cross_encoder_reranker.py:CrossEncoderReranker` → Reranking

**Failure & Fallback:**
- **What can fail**: Weaviate timeout, Neo4j connection error, embedding service failure
- **System reaction**:
  - Failed agent's ranking is excluded (remaining agents continue)
  - RRF automatically handles missing agents (no renormalization needed)
  - Minimum 2 agents required for valid consensus
  - If all fail: Return empty results, escalate to human

---

### STEP 10: Node 3 - Generate Remediation Plan

**Purpose:**
- Create a detailed execution plan using the matched script(s)
- Fill in script parameters from incident context
- Generate rollback strategy for safety

**What Happens (Layman View):**
The AI creates a step-by-step plan: "First, check if the VM exists. Then, run the restart script with these specific settings. If it fails, here's how to undo what we did."

**How It Works (Technical View):**
- **Protocol**: OpenAI API (REST)
- **Sync/Async**: Asynchronous HTTP call
- **Model**: GPT-4-turbo
- **Prompt Structure**: Plan-Execute pattern
- **Input**:
  ```python
  {
      "incident_context": {...},  # From Step 8
      "matched_script": {...},    # Top result from Step 9
      "available_tools": [...]    # MCP tool schemas
  }
  ```

**LLM Prompt Template**:
```
You are an IT operations expert. Given the incident context and matched remediation script, create a detailed execution plan.

Incident:
{incident_context}

Matched Script:
{matched_script}

Generate a plan with:
1. Pre-execution checks
2. Main execution steps with filled parameters
3. Post-execution validation
4. Rollback strategy if execution fails

Output as JSON with the following schema:
{plan_schema}
```

**Output Schema**:
```python
{
    "plan_id": "plan-xyz789",
    "script_id": "gcp_restart_vm_001",
    "confidence": 0.89,
    "pre_checks": [
        {
            "action": "verify_instance_exists",
            "command": "gcloud compute instances describe test-incident-vm-01 --zone=us-central1-a",
            "expected_outcome": "Instance exists"
        }
    ],
    "main_steps": [
        {
            "step_number": 1,
            "action": "start_instance",
            "command": "gcloud compute instances start test-incident-vm-01 --zone=us-central1-a",
            "timeout_seconds": 120,
            "expected_outcome": "Instance status: RUNNING"
        }
    ],
    "post_checks": [
        {
            "action": "verify_instance_running",
            "command": "gcloud compute instances describe test-incident-vm-01 --zone=us-central1-a --format='value(status)'",
            "expected_outcome": "RUNNING"
        }
    ],
    "rollback_steps": [
        {
            "action": "stop_instance_if_unstable",
            "command": "gcloud compute instances stop test-incident-vm-01 --zone=us-central1-a",
            "trigger": "post_check_failed"
        }
    ],
    "parameters": {
        "instance_name": "test-incident-vm-01",
        "zone": "us-central1-a",
        "project": "my-project"
    },
    "risk_assessment": {
        "level": "low",
        "blast_radius": "single_instance",
        "requires_approval": true
    }
}
```

**Layers Involved:**
- **Infrastructure Layer**: OpenAI API, Network
- **Protocol Layer**: HTTPS/REST, JSON
- **Agent Layer**: LangGraph Node 3
- **Reasoning Layer**: GPT-4 (Plan-Execute pattern)

**Files Involved:**
- `backend/orchestrator/langgraph_workflow.py:_node_generate_plan()` → Node implementation
- `backend/orchestrator/llm_intelligence.py:generate_execution_plan()` → LLM call
- `backend/orchestrator/rollback_generator.py:RollbackGenerator` → Rollback strategy

**Failure & Fallback:**
- **What can fail**: OpenAI API timeout, rate limiting, malformed response
- **System reaction**:
  - Circuit breaker protects against repeated failures
  - Retry with exponential backoff (3 attempts)
  - Fallback to template-based plan if LLM unavailable
- **Circuit breaker behavior**:
  - `openai_breaker`: Opens after 5 failures, 30s timeout

---

### STEP 11: Node 4 - LLM-as-Judge Validation

**Purpose:**
- Independently validate the generated plan for quality, safety, and feasibility
- Provide an objective second opinion using a different model perspective
- Prevent hallucinated or dangerous plans from proceeding

**What Happens (Layman View):**
A "second AI" reviews the plan and scores it on several criteria: Is it well-structured? Is it safe? Does it match the original problem? Will it actually work?

**How It Works (Technical View):**
- **Protocol**: OpenAI API (REST) or Anthropic API (REST)
- **Sync/Async**: Asynchronous HTTP call
- **Model**: Claude (different from GPT-4 to avoid shared biases)
- **Why Different Model**: Independent perspective prevents confirmation bias

**Evaluation Criteria**:

| Criterion | Score Range | Threshold | Description |
|-----------|-------------|-----------|-------------|
| Quality | 1-10 | ≥6 | Plan structure, completeness, logical flow |
| Safety | Pass/Fail | Must pass | No dangerous commands, proper guardrails |
| Factual | 1-10 | ≥6 | Matches RAG results, no hallucinations |
| Feasibility | 1-10 | ≥6 | Executable given current context |
| Risk | Low/Med/High | ≤Medium | Blast radius assessment |

**Judge Prompt Template**:
```
You are an independent evaluator reviewing an AI-generated incident remediation plan.

Original Incident:
{incident_context}

RAG Search Results:
{rag_results}

Generated Plan:
{plan}

Evaluate this plan on the following criteria:
1. QUALITY (1-10): Is the plan well-structured and complete?
2. SAFETY (Pass/Fail): Are there any dangerous commands or missing guardrails?
3. FACTUAL (1-10): Does the plan match the RAG results without hallucination?
4. FEASIBILITY (1-10): Can this plan be executed given the context?
5. RISK (Low/Medium/High): What is the blast radius?

Output your evaluation as JSON:
{evaluation_schema}
```

**Output Schema**:
```python
{
    "judge_id": "judge-eval-001",
    "plan_id": "plan-xyz789",
    "verdict": "APPROVED",  # or "NEEDS_REVISION" or "REJECTED"
    "scores": {
        "quality": 8,
        "safety": "PASS",
        "factual": 9,
        "feasibility": 8,
        "risk": "LOW"
    },
    "reasoning": "Plan is well-structured with proper pre/post checks...",
    "concerns": [],
    "revision_suggestions": []
}
```

**Decision Logic**:
```python
if safety == "FAIL":
    verdict = "REJECTED"
elif quality < 6 or factual < 6:
    verdict = "NEEDS_REVISION"
else:
    verdict = "APPROVED"
```

**Layers Involved:**
- **Infrastructure Layer**: Claude/OpenAI API, Network
- **Protocol Layer**: HTTPS/REST, JSON
- **Agent Layer**: LangGraph Node 4 (LLM Judge)
- **Reasoning Layer**: Independent LLM evaluation

**Files Involved:**
- `backend/orchestrator/langgraph_workflow.py:_node_llm_judge()` → Node implementation
- `backend/orchestrator/llm_judge.py:LLMJudge` → Judge implementation
- `backend/orchestrator/llm_judge.py:evaluate_plan()` → Evaluation logic

**Failure & Fallback:**
- **What can fail**: Judge API timeout, parse error, low confidence
- **System reaction**:
  - If judge unavailable: Flag for mandatory human review
  - If verdict is "NEEDS_REVISION": Loop back to Node 3 with feedback
  - Maximum 2 revision loops before human escalation

---

### STEP 12: Revision Loop (Conditional)

**Purpose:**
- Improve the plan based on Judge feedback
- Iterate until quality thresholds are met
- Prevent infinite loops with maximum iteration limit

**What Happens (Layman View):**
If the second AI found problems with the plan, the first AI revises it and resubmits. This can happen up to 2 times before a human must review.

**How It Works (Technical View):**
- **Protocol**: Internal state transition (LangGraph edge)
- **Sync/Async**: Asynchronous workflow
- **Condition**: `verdict == "NEEDS_REVISION" AND revision_count < 2`
- **State Update**:
  ```python
  workflow_state["revision_count"] += 1
  workflow_state["judge_feedback"] = judge_output["revision_suggestions"]
  workflow_state["current_node"] = "generate_plan"  # Loop back
  ```

**Layers Involved:**
- **Infrastructure Layer**: Python runtime
- **Protocol Layer**: LangGraph state transitions
- **Agent Layer**: Workflow orchestrator
- **Reasoning Layer**: Decision logic

**Files Involved:**
- `backend/orchestrator/langgraph_workflow.py:_should_revise()` → Edge condition
- `backend/orchestrator/langgraph_workflow.py:_route_after_judge()` → Routing logic

**Failure & Fallback:**
- **What can fail**: Infinite loop (caught by max iterations)
- **System reaction**:
  - After 2 revisions: Force escalation to human
  - Log all revision attempts for debugging

---

### STEP 13: Node 5 - Control Plane Approval Routing

**Purpose:**
- Determine the appropriate approval workflow based on risk level
- Route low-risk plans for auto-approval
- Route high-risk plans for human review
- Enforce governance policies

**What Happens (Layman View):**
The system decides: "Is this safe enough to do automatically, or does a human need to approve it?" Low-risk actions with high confidence may proceed automatically; everything else needs human sign-off.

**How It Works (Technical View):**
- **Protocol**: Internal decision logic + optional Kafka publish
- **Sync/Async**: Synchronous decision, async notification
- **Decision Matrix**:

| Risk Level | Judge Score | Confidence | Action |
|------------|-------------|------------|--------|
| Low | ≥8 | ≥0.90 | Auto-approve |
| Low | ≥6 | ≥0.80 | Human approval (fast track) |
| Medium | Any | Any | Human approval (standard) |
| High | Any | Any | Human approval + manager |

**Auto-Approval Criteria**:
```python
auto_approve = (
    risk_level == "LOW" and
    judge_score >= 8 and
    plan_confidence >= 0.90 and
    script_has_history and
    historical_success_rate >= 0.95
)
```

**Output Schema**:
```python
{
    "control_plane_decision": {
        "requires_approval": true,
        "approval_type": "standard",  # or "fast_track", "manager_escalation"
        "auto_approved": false,
        "approvers": ["oncall-engineer"],
        "timeout_minutes": 60,
        "escalation_path": ["team-lead", "manager"]
    },
    "pending_approval_id": "approval-abc123"
}
```

**Layers Involved:**
- **Infrastructure Layer**: Redis (approval state), Kafka (notification)
- **Protocol Layer**: Internal + optional REST webhook
- **Agent Layer**: Control Plane agent
- **Reasoning Layer**: Policy-based decision logic

**Files Involved:**
- `backend/orchestrator/langgraph_workflow.py:_node_control_plane()` → Node implementation
- `backend/agents/control_plane.py:ControlPlane` → Decision logic
- `backend/governance/approval_policy.py` → Policy definitions
- `backend/orchestrator/main.py:PENDING_APPROVALS` → Approval state store

**Failure & Fallback:**
- **What can fail**: Policy evaluation error, notification delivery failure
- **System reaction**:
  - Default to "requires_approval" if policy evaluation fails
  - Retry notification delivery 3 times
  - Timeout approval after configured period → escalate

---

### STEP 14: Human Approval Interface

**Purpose:**
- Present the plan to human approvers in a clear, actionable format
- Enable approve/reject decisions with comments
- Track approval SLAs and escalate if needed

**What Happens (Layman View):**
A notification appears in the web interface (and optionally Slack) asking an engineer to review and approve the proposed fix. They can see all the details, approve, reject, or request changes.

**How It Works (Technical View):**
- **Protocol**: REST API for UI, optional Slack webhook
- **Sync/Async**: Asynchronous (human time)
- **UI Components**: `EnterpriseIncidentDetail.tsx`, approval buttons
- **Approval Payload**:
  ```json
  {
    "approval_id": "approval-abc123",
    "incident_id": "INC0010001",
    "plan_summary": "Restart GCP VM test-incident-vm-01",
    "risk_level": "LOW",
    "judge_score": 8.5,
    "confidence": 0.92,
    "estimated_duration": "3 minutes",
    "rollback_available": true
  }
  ```

**API Endpoints**:
- `GET /api/approvals` → List pending approvals
- `GET /api/approvals/{id}` → Get approval details
- `POST /api/approvals/{id}/approve` → Approve plan
- `POST /api/approvals/{id}/reject` → Reject plan

**Layers Involved:**
- **Infrastructure Layer**: FastAPI, Redis, Browser
- **Protocol Layer**: REST/HTTP, WebSocket (real-time updates)
- **Agent Layer**: Not directly involved
- **Reasoning Layer**: Not directly involved (human decision)

**Files Involved:**
- `backend/orchestrator/main.py:/api/approvals/*` → API endpoints
- `frontend/src/components/incidents/EnterpriseIncidentDetail.tsx:handleApprove()` → UI handler
- `frontend/src/app/approvals/page.tsx` → Approvals list page

**Failure & Fallback:**
- **What can fail**: UI unavailable, approver unresponsive, timeout
- **System reaction**:
  - SLA timer (60 minutes default)
  - Escalation to next approver at 50% of timeout
  - Auto-reject if no response at 100% timeout
  - Incident returned to queue for manual handling

---

### STEP 15: Approval Decision Recording

**Purpose:**
- Record the approval decision with full audit trail
- Transition workflow state based on decision
- Publish approval event for downstream processing

**What Happens (Layman View):**
The system records who approved (or rejected) the plan, when, and any comments they provided. This creates a complete audit trail for compliance.

**How It Works (Technical View):**
- **Protocol**: REST (input), Kafka (output), PostgreSQL (storage)
- **Sync/Async**: Synchronous API, async event publish
- **Audit Record**:
  ```python
  {
      "approval_id": "approval-abc123",
      "incident_id": "INC0010001",
      "plan_id": "plan-xyz789",
      "decision": "APPROVED",
      "approver": "engineer@company.com",
      "timestamp": "2024-12-31T10:15:00Z",
      "comments": "Looks good, standard restart procedure",
      "approval_type": "human",
      "review_duration_seconds": 120
  }
  ```
- **Kafka Event** (if approved):
  ```json
  {
      "event_type": "incident.approved",
      "incident_id": "INC0010001",
      "plan_id": "plan-xyz789",
      "approved_by": "engineer@company.com",
      "timestamp": "2024-12-31T10:15:00Z"
  }
  ```

**Layers Involved:**
- **Infrastructure Layer**: PostgreSQL, Kafka, Redis
- **Protocol Layer**: REST, Kafka, SQL
- **Agent Layer**: Control Plane (state update)
- **Reasoning Layer**: Not involved

**Files Involved:**
- `backend/orchestrator/main.py:approve_execution()` → API handler
- `backend/governance/audit_log.py` → Audit recording
- `backend/utils/postgres_client.py` → Database operations
- `backend/utils/kafka_client.py` → Event publishing

**Failure & Fallback:**
- **What can fail**: Database write failure, Kafka publish failure
- **System reaction**:
  - Transaction rollback if database fails
  - Retry Kafka publish 3 times
  - Approval still recorded locally if Kafka unavailable

---

### STEP 16: Node 6 - Execution Preparation

**Purpose:**
- Validate the execution environment
- Prepare script parameters and credentials
- Generate unique execution ID for tracking

**What Happens (Layman View):**
Before actually running the fix, the system double-checks that everything is in place: the script exists, the credentials are valid, and the target system is accessible.

**How It Works (Technical View):**
- **Protocol**: Internal validation + MCP tool checks
- **Sync/Async**: Synchronous validation
- **Pre-execution Checks**:
  1. Verify script exists in registry
  2. Validate all required parameters are filled
  3. Check credentials are available and not expired
  4. Verify target system is reachable (optional ping)
  5. Ensure no conflicting executions in progress

**Validation Output**:
```python
{
    "execution_id": "exec-def456",
    "ready_to_execute": true,
    "validations": {
        "script_exists": true,
        "parameters_valid": true,
        "credentials_valid": true,
        "target_reachable": true,
        "no_conflicts": true
    },
    "execution_context": {
        "script_path": "runbooks/gcp/start_instance.sh",
        "environment": "production",
        "timeout_seconds": 300,
        "dry_run": false
    }
}
```

**Layers Involved:**
- **Infrastructure Layer**: Script registry, Credential store
- **Protocol Layer**: Internal, MCP (tool validation)
- **Agent Layer**: Execution Orchestrator
- **Reasoning Layer**: Validation rules

**Files Involved:**
- `backend/orchestrator/langgraph_workflow.py:_node_execute()` → Node entry
- `backend/agents/execution_orchestrator.py:validate_execution()` → Validation
- `backend/utils/registry_manager.py` → Script registry

**Failure & Fallback:**
- **What can fail**: Missing script, invalid credentials, target unreachable
- **System reaction**:
  - Fail fast with specific error message
  - Return to approval queue for human review
  - Log validation failure for debugging

---

### STEP 17: GitHub Actions Workflow Trigger

**Purpose:**
- Execute the remediation script in a secure, auditable environment
- Leverage GitHub Actions for infrastructure automation
- Maintain separation between AI decision and actual execution

**What Happens (Layman View):**
The system tells GitHub to run a specific automation script. GitHub Actions provides a secure environment to execute infrastructure changes with full logging.

**How It Works (Technical View):**
- **Protocol**: GitHub REST API (`workflow_dispatch`)
- **Sync/Async**: Asynchronous (fire and poll)
- **API Call**:
  ```
  POST /repos/{owner}/{repo}/actions/workflows/{workflow_id}/dispatches
  Authorization: Bearer {GITHUB_TOKEN}
  Content-Type: application/json

  {
      "ref": "main",
      "inputs": {
          "incident_id": "INC0010001",
          "script_path": "runbooks/gcp/start_instance.sh",
          "parameters": "{\"instance\": \"test-incident-vm-01\", \"zone\": \"us-central1-a\"}",
          "execution_id": "exec-def456",
          "dry_run": "false"
      }
  }
  ```
- **Response**: 204 No Content (workflow queued)

**Layers Involved:**
- **Infrastructure Layer**: GitHub Actions, Cloud (GCP/AWS/Azure)
- **Protocol Layer**: REST/HTTPS
- **Agent Layer**: Execution Orchestrator
- **Reasoning Layer**: Not involved (deterministic execution)

**Files Involved:**
- `backend/orchestrator/main.py:trigger_github_workflow()` → API call
- `backend/agents/execution_orchestrator.py:execute_script()` → Orchestration
- `backend/utils/github_actions.py` → GitHub API client
- `.github/workflows/remediation.yml` → GitHub Actions workflow definition

**Failure & Fallback:**
- **What can fail**: GitHub API error, authentication failure, workflow not found
- **System reaction**:
  - Circuit breaker protects against repeated failures
  - Retry 3 times with exponential backoff
  - Alert human if all retries fail
- **Circuit breaker behavior**:
  - `github_breaker`: Opens after 3 failures, 60s timeout

---

### STEP 18: Execution Monitoring and Polling

**Purpose:**
- Track the GitHub Actions workflow execution status
- Capture execution logs and output
- Detect completion or failure

**What Happens (Layman View):**
The system watches the GitHub Actions job, checking every few seconds to see if it's done. It captures all the logs and results.

**How It Works (Technical View):**
- **Protocol**: GitHub REST API (workflow runs)
- **Sync/Async**: Asynchronous polling
- **Polling Strategy**:
  - Initial delay: 2 seconds
  - Polling interval: 10 seconds
  - Maximum wait: 600 seconds (10 minutes)
- **API Call**:
  ```
  GET /repos/{owner}/{repo}/actions/runs/{run_id}
  ```
- **Status Values**: `queued`, `in_progress`, `completed`
- **Conclusion Values**: `success`, `failure`, `cancelled`, `timed_out`

**Polling Loop**:
```python
while elapsed < max_wait:
    run = github_api.get_workflow_run(run_id)
    if run.status == "completed":
        return {
            "status": "completed",
            "conclusion": run.conclusion,
            "duration_seconds": run.duration,
            "logs_url": run.logs_url
        }
    await asyncio.sleep(10)
```

**Layers Involved:**
- **Infrastructure Layer**: GitHub API, Network
- **Protocol Layer**: REST/HTTPS
- **Agent Layer**: Execution Orchestrator
- **Reasoning Layer**: Not involved

**Files Involved:**
- `backend/orchestrator/main.py:poll_github_run()` → Polling logic
- `backend/agents/execution_orchestrator.py:monitor_execution()` → High-level monitor
- `backend/utils/github_actions.py:get_run_status()` → API wrapper

**Failure & Fallback:**
- **What can fail**: Polling timeout, GitHub API unavailable, workflow stuck
- **System reaction**:
  - Timeout after 600 seconds → mark as "unknown" status
  - Alert human for manual verification
  - Continue to verification step regardless

---

### STEP 19: Execution Result Processing

**Purpose:**
- Interpret the GitHub Actions outcome
- Handle success vs failure differently
- Prepare for verification or rollback

**What Happens (Layman View):**
The system checks whether the fix worked. If GitHub says "success," we move to verification. If it says "failure," we consider rolling back.

**How It Works (Technical View):**
- **Protocol**: Internal state processing
- **Sync/Async**: Synchronous
- **Decision Logic**:
  ```python
  if conclusion == "success":
      next_step = "verify_close"
      action = "proceed_to_verification"
  elif conclusion == "failure":
      if rollback_available:
          action = "trigger_rollback"
      else:
          action = "escalate_to_human"
  elif conclusion == "cancelled" or conclusion == "timed_out":
      action = "escalate_to_human"
  ```

**Output Schema**:
```python
{
    "execution_id": "exec-def456",
    "github_run_id": 12345678,
    "status": "completed",
    "conclusion": "success",
    "duration_seconds": 45,
    "logs": "...",
    "artifacts": [],
    "next_action": "proceed_to_verification"
}
```

**Layers Involved:**
- **Infrastructure Layer**: Python runtime
- **Protocol Layer**: Internal
- **Agent Layer**: Execution Orchestrator
- **Reasoning Layer**: Decision rules

**Files Involved:**
- `backend/orchestrator/langgraph_workflow.py:_process_execution_result()` → Result handling
- `backend/agents/execution_orchestrator.py:handle_result()` → Business logic

**Failure & Fallback:**
- **What can fail**: Unexpected conclusion value, missing logs
- **System reaction**:
  - Default to "escalate_to_human" for unknown states
  - Log all available information for debugging

---

### STEP 20: Rollback Execution (Conditional)

**Purpose:**
- Undo the changes if execution failed
- Restore system to pre-execution state
- Maintain system stability

**What Happens (Layman View):**
If the fix made things worse, the system automatically runs the "undo" commands that were prepared earlier to put things back the way they were.

**How It Works (Technical View):**
- **Protocol**: GitHub REST API (same as execution)
- **Sync/Async**: Asynchronous
- **Trigger Condition**: `execution_conclusion == "failure" AND rollback_available`
- **Rollback Script**: Pre-generated in Step 10

**Rollback Workflow**:
```
POST /repos/{owner}/{repo}/actions/workflows/rollback.yml/dispatches
{
    "ref": "main",
    "inputs": {
        "incident_id": "INC0010001",
        "original_execution_id": "exec-def456",
        "rollback_script": "rollback_gcp_restart.sh",
        "parameters": "{...}"
    }
}
```

**Layers Involved:**
- **Infrastructure Layer**: GitHub Actions, Cloud
- **Protocol Layer**: REST/HTTPS
- **Agent Layer**: Execution Orchestrator
- **Reasoning Layer**: Rollback decision rules

**Files Involved:**
- `backend/orchestrator/rollback_generator.py:execute_rollback()` → Rollback trigger
- `backend/agents/execution_orchestrator.py:rollback()` → Orchestration
- `.github/workflows/rollback.yml` → Rollback workflow

**Failure & Fallback:**
- **What can fail**: Rollback itself fails
- **System reaction**:
  - Immediately escalate to human
  - Page on-call engineer
  - Mark incident as "CRITICAL_MANUAL_INTERVENTION"

---

### STEP 21: Node 7 - Verification

**Purpose:**
- Confirm the remediation actually fixed the problem
- Run health checks on the affected system
- Validate expected outcomes from the plan

**What Happens (Layman View):**
The system checks that the fix actually worked: Is the VM running? Can we ping it? Are the services responding? This proves the incident is truly resolved.

**How It Works (Technical View):**
- **Protocol**: MCP (tool invocation), REST (health endpoints)
- **Sync/Async**: Asynchronous
- **Verification Steps**:
  1. Execute post-checks from the plan
  2. Query the affected system's health endpoint
  3. Compare current state to expected state
  4. Calculate verification confidence

**Verification Logic**:
```python
verifications = []
for check in plan["post_checks"]:
    result = await execute_check(check)
    verifications.append({
        "check": check["action"],
        "expected": check["expected_outcome"],
        "actual": result,
        "passed": result == check["expected_outcome"]
    })

all_passed = all(v["passed"] for v in verifications)
confidence = sum(v["passed"] for v in verifications) / len(verifications)
```

**Output Schema**:
```python
{
    "verification_id": "verify-ghi789",
    "incident_id": "INC0010001",
    "execution_id": "exec-def456",
    "status": "VERIFIED",  # or "FAILED"
    "checks": [
        {
            "check": "verify_instance_running",
            "expected": "RUNNING",
            "actual": "RUNNING",
            "passed": true
        }
    ],
    "confidence": 1.0,
    "verification_time_seconds": 15
}
```

**Layers Involved:**
- **Infrastructure Layer**: Target system, Monitoring APIs
- **Protocol Layer**: MCP, REST, SSH (if needed)
- **Agent Layer**: Verification Agent
- **Reasoning Layer**: Comparison logic

**Files Involved:**
- `backend/orchestrator/langgraph_workflow.py:_node_verify_close()` → Node implementation
- `backend/agents/execution_orchestrator.py:verify_execution()` → Verification logic

**Failure & Fallback:**
- **What can fail**: Health check timeout, unexpected system state
- **System reaction**:
  - If verification fails: Consider rollback
  - If inconclusive: Flag for human verification
  - Retry verification once after 30 seconds

---

### STEP 22: ServiceNow Ticket Update and Closure

**Purpose:**
- Update the ServiceNow incident with resolution details
- Close the ticket with appropriate resolution codes
- Maintain ITSM system as source of truth

**What Happens (Layman View):**
The system updates the original ServiceNow ticket to say "Fixed by AI Agent" with all the details of what was done, then closes the ticket.

**How It Works (Technical View):**
- **Protocol**: ServiceNow REST API (Table API)
- **Sync/Async**: Synchronous
- **API Call**:
  ```
  PATCH /api/now/table/incident/{sys_id}
  {
      "state": "6",  // Resolved
      "close_code": "Resolved",
      "close_notes": "Resolved by AI Agent Platform v5.0\n\nExecution ID: exec-def456\nScript: start_gcp_instance.sh\nDuration: 45 seconds\nVerification: PASSED",
      "resolution_code": "Automated Resolution",
      "resolved_by": "ai-agent-service-account"
  }
  ```

**Layers Involved:**
- **Infrastructure Layer**: ServiceNow API
- **Protocol Layer**: REST/HTTPS
- **Agent Layer**: ServiceNow Agent
- **Reasoning Layer**: Not involved (deterministic update)

**Files Involved:**
- `backend/agents/servicenow/agent.py:close_incident()` → API call
- `backend/mcp/servicenow_server.py:update_incident()` → MCP wrapper
- `backend/orchestrator/main.py:/api/incidents/{id}/close` → API endpoint

**Failure & Fallback:**
- **What can fail**: ServiceNow API unavailable, permission denied
- **System reaction**:
  - Retry 3 times
  - If still failing: Mark as "needs manual closure"
  - Alert human to manually close ticket
  - Incident resolution is not blocked by closure failure

---

### STEP 23: Learning Engine - Knowledge Update

**Purpose:**
- Record the successful resolution for future use
- Update RAG indexes with new evidence
- Strengthen the association between this incident type and this script

**What Happens (Layman View):**
The system remembers what worked. It updates its "knowledge base" so that next time a similar problem occurs, it will be even more confident in recommending this solution.

**How It Works (Technical View):**
- **Protocol**: MCP (Neo4j, Weaviate), Internal
- **Sync/Async**: Asynchronous (non-blocking)
- **CRITICAL RULE**: Only update on SUCCESS. Never learn from failures (prevents learning bad patterns).

**Update Operations**:

1. **Neo4j Graph Update** (FIXED_BY relationship):
```cypher
MERGE (i:IncidentType {type: $incident_type})
MERGE (s:Script {id: $script_id})
MERGE (i)-[r:FIXED_BY]->(s)
SET r.success_count = COALESCE(r.success_count, 0) + 1,
    r.last_success = datetime(),
    r.avg_resolution_time =
      (COALESCE(r.avg_resolution_time, 0) * COALESCE(r.success_count, 0) + $duration)
      / (COALESCE(r.success_count, 0) + 1)
```

2. **Feedback Record**:
```python
{
    "feedback_id": "fb-xyz123",
    "incident_id": "INC0010001",
    "incident_type": "infrastructure/gcp/vm_down",
    "script_id": "gcp_restart_vm_001",
    "outcome": "SUCCESS",
    "resolution_time_seconds": 195,
    "confidence_delta": 0.02,  # Boost for next time
    "agent_votes": {
        "vector_agent": 0.95,
        "keyword_agent": 0.88,
        "graph_agent": 0.91,
        "metadata_agent": 1.0
    },
    "timestamp": "2024-12-31T10:18:00Z"
}
```

3. **Weight Optimization**:
```python
# Boost weights for agents that correctly predicted this script
for agent, vote in agent_votes.items():
    if agent_recommended_winning_script(agent, script_id):
        weights[incident_type][agent] += 0.01

# Normalize weights to sum to 1.0
normalize_weights(weights[incident_type])
```

**Layers Involved:**
- **Infrastructure Layer**: Neo4j, Weaviate, Redis
- **Protocol Layer**: MCP (Cypher, GraphQL)
- **Agent Layer**: Learning Engine
- **Reasoning Layer**: Feedback optimization algorithm

**Files Involved:**
- `backend/rag/feedback_optimizer.py:FeedbackOptimizer` → Weight optimization
- `backend/rag/graph_scorer.py:update_relationship()` → Neo4j update
- `backend/orchestrator/langgraph_workflow.py:_update_learning()` → Orchestration

**Failure & Fallback:**
- **What can fail**: Neo4j unavailable, write timeout
- **System reaction**:
  - Feedback buffered in Redis
  - Retry in background
  - System continues without blocking
  - Learning is "best effort" - main workflow not affected

---

### STEP 24: Workflow Completion and Cleanup

**Purpose:**
- Mark the workflow as completed
- Clean up temporary state
- Publish completion event for monitoring
- Calculate and log metrics

**What Happens (Layman View):**
The system marks this incident as "done," cleans up any temporary data, and logs how long the whole process took. The incident lifecycle is complete.

**How It Works (Technical View):**
- **Protocol**: Kafka (event), Prometheus (metrics), Internal (cleanup)
- **Sync/Async**: Mixed
- **Completion Event**:
```json
{
    "event_type": "incident.closed",
    "incident_id": "INC0010001",
    "workflow_id": "wf-abc123",
    "outcome": "SUCCESS",
    "total_duration_seconds": 195,
    "breakdown": {
        "receive_parse": 0.5,
        "swarm_rag": 2.1,
        "generate_plan": 3.2,
        "llm_judge": 2.8,
        "control_plane": 0.1,
        "human_approval": 120.0,
        "execution": 45.0,
        "verification": 15.0,
        "learning": 1.5
    },
    "timestamp": "2024-12-31T10:18:15Z"
}
```

**Metrics Recorded**:
- `incident_resolution_total{outcome="success"}` +1
- `incident_resolution_duration_seconds` histogram
- `swarm_rag_search_duration_seconds` histogram
- `llm_judge_score` gauge
- `human_approval_wait_seconds` histogram

**Cleanup Tasks**:
1. Remove from `WORKFLOW_STATES` in-memory store
2. Remove from `PENDING_APPROVALS` if present
3. Clear any temporary Redis keys
4. Archive detailed logs

**Layers Involved:**
- **Infrastructure Layer**: Kafka, Prometheus, Redis
- **Protocol Layer**: Kafka, HTTP (metrics endpoint)
- **Agent Layer**: Workflow orchestrator
- **Reasoning Layer**: Not involved

**Files Involved:**
- `backend/orchestrator/langgraph_workflow.py:_complete_workflow()` → Completion logic
- `backend/orchestrator/metrics.py:record_completion()` → Metrics
- `backend/utils/kafka_client.py:publish()` → Event publication

**Failure & Fallback:**
- **What can fail**: Kafka publish failure, metrics recording failure
- **System reaction**:
  - Failures are logged but don't affect outcome
  - Workflow considered complete regardless
  - Monitoring may show gaps but no functional impact

---

## End-to-End Timeline Summary

### Typical Successful Resolution: ~3-5 minutes total

| Step | Duration | Cumulative | Description |
|------|----------|------------|-------------|
| 1-6 | ~1s | 0:01 | Incident ingestion and parsing |
| 7-8 | ~0.5s | 0:01.5 | Workflow init and context extraction |
| 9 | ~2s | 0:03.5 | Swarm RAG search |
| 10 | ~3s | 0:06.5 | Plan generation |
| 11-12 | ~3s | 0:09.5 | LLM Judge evaluation |
| 13-15 | ~2min* | 2:10 | Human approval (variable) |
| 16-18 | ~45s | 2:55 | Execution |
| 19-21 | ~15s | 3:10 | Verification |
| 22-24 | ~5s | 3:15 | Closure and learning |

*Human approval time varies; with auto-approval: ~5 seconds

### Auto-Approval Path: ~60 seconds total

When conditions allow auto-approval (low risk, high confidence, proven script):
- Steps 13-15 reduced to ~0.1 seconds
- Total time: ~65-90 seconds

---

## Architecture Layer Map

### Layer → Responsibility → Files

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│ LAYER 1: PRESENTATION                                                           │
├─────────────────────────────────────────────────────────────────────────────────┤
│ Responsibility: User interface, approval workflows, monitoring dashboards       │
│                                                                                 │
│ Files:                                                                          │
│ ├── frontend/src/app/page.tsx                    → Dashboard home               │
│ ├── frontend/src/app/incidents/[id]/page.tsx     → Incident detail              │
│ ├── frontend/src/app/approvals/page.tsx          → Approval queue               │
│ ├── frontend/src/components/incidents/           → Incident components          │
│ │   ├── EnterpriseIncidentDetail.tsx            → Main incident view           │
│ │   ├── RemediationPanel.tsx                    → Remediation workflow         │
│ │   └── IncidentWorkflow.tsx                    → 7-node visualization         │
│ └── frontend/src/lib/constants.ts               → Centralized configuration    │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────┐
│ LAYER 2: API GATEWAY                                                            │
├─────────────────────────────────────────────────────────────────────────────────┤
│ Responsibility: REST endpoints, request routing, authentication                 │
│                                                                                 │
│ Files:                                                                          │
│ ├── backend/orchestrator/main.py                 → FastAPI application          │
│ │   ├── /api/incidents/*                        → Incident CRUD                 │
│ │   ├── /api/approvals/*                        → Approval workflow            │
│ │   ├── /api/execute                            → Execution trigger            │
│ │   ├── /api/langgraph/*                        → Workflow management          │
│ │   └── /api/rag/*                              → RAG search                   │
│ └── backend/orchestrator/metrics.py             → Prometheus metrics            │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────┐
│ LAYER 3: ORCHESTRATION                                                          │
├─────────────────────────────────────────────────────────────────────────────────┤
│ Responsibility: Workflow state machine, node transitions, event routing         │
│                                                                                 │
│ Files:                                                                          │
│ ├── backend/orchestrator/langgraph_workflow.py   → 7-node state machine         │
│ ├── backend/orchestrator/llm_intelligence.py     → LLM-based analysis          │
│ ├── backend/orchestrator/llm_judge.py            → Independent validation       │
│ ├── backend/orchestrator/rollback_generator.py   → Rollback strategy           │
│ └── backend/agents/control_plane.py              → Governance & routing         │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────┐
│ LAYER 4: AGENT MESH                                                             │
├─────────────────────────────────────────────────────────────────────────────────┤
│ Responsibility: Specialized agents, A2A communication, tool invocation          │
│                                                                                 │
│ Files:                                                                          │
│ ├── backend/agents/base_agent.py                 → Abstract base class          │
│ ├── backend/agents/servicenow/agent.py           → ServiceNow integration       │
│ ├── backend/agents/jira/agent.py                 → Jira integration            │
│ ├── backend/agents/infra/enhanced_agent.py       → Infrastructure agent        │
│ ├── backend/agents/execution_orchestrator.py     → Execution management        │
│ ├── backend/agents/remediation/agent.py          → Remediation logic           │
│ └── backend/agents/a2a/                          → A2A protocol handlers        │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────┐
│ LAYER 5: INTELLIGENCE (RAG + LLM)                                               │
├─────────────────────────────────────────────────────────────────────────────────┤
│ Responsibility: Swarm search, embeddings, consensus, LLM reasoning              │
│                                                                                 │
│ Files:                                                                          │
│ ├── backend/rag/__init__.py                      → Module exports              │
│ ├── backend/rag/swarm_retriever.py               → Swarm coordinator           │
│ ├── backend/rag/swarm_script_selector.py         → Script-specific swarm       │
│ ├── backend/rag/intelligent_retriever.py         → Full pipeline               │
│ ├── backend/rag/query_understanding.py           → Intent extraction           │
│ ├── backend/rag/hybrid_search_engine.py          → Multi-source search         │
│ ├── backend/rag/graph_scorer.py                  → Neo4j integration           │
│ ├── backend/rag/cross_encoder_reranker.py        → Precision reranking         │
│ ├── backend/rag/embedding_service.py             → Vector embeddings           │
│ └── backend/rag/feedback_optimizer.py            → Adaptive learning           │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────┐
│ LAYER 6: TOOL INTEGRATION (MCP)                                                 │
├─────────────────────────────────────────────────────────────────────────────────┤
│ Responsibility: External tool invocation, MCP servers, credential management    │
│                                                                                 │
│ Files:                                                                          │
│ ├── backend/mcp/servicenow_server.py             → ServiceNow MCP server       │
│ ├── backend/mcp/github_server.py                 → GitHub MCP server           │
│ ├── backend/mcp/rag_server.py                    → RAG MCP server              │
│ ├── backend/mcp/gcp_server.py                    → GCP MCP server              │
│ └── backend/orchestrator/services/mcp_client.py  → MCP client                  │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────┐
│ LAYER 7: EVENT STREAMING                                                        │
├─────────────────────────────────────────────────────────────────────────────────┤
│ Responsibility: Kafka production/consumption, event schemas, ordering           │
│                                                                                 │
│ Files:                                                                          │
│ ├── backend/streaming/incident_consumer.py       → Kafka consumer              │
│ ├── backend/streaming/incident_sources.py        → Source adapters             │
│ ├── backend/streaming/schemas.py                 → Event schemas               │
│ └── backend/utils/kafka_client.py                → Kafka client wrapper        │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────┐
│ LAYER 8: PERSISTENCE                                                            │
├─────────────────────────────────────────────────────────────────────────────────┤
│ Responsibility: Databases, caching, state storage                               │
│                                                                                 │
│ Files:                                                                          │
│ ├── backend/utils/redis_client.py                → Redis operations            │
│ ├── backend/utils/postgres_client.py             → PostgreSQL client           │
│ ├── backend/rag/weaviate_client.py               → Weaviate vector DB          │
│ └── backend/rag/neo4j_client.py                  → Neo4j graph DB              │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────┐
│ LAYER 9: RESILIENCE                                                             │
├─────────────────────────────────────────────────────────────────────────────────┤
│ Responsibility: Circuit breakers, retries, fallbacks, guardrails               │
│                                                                                 │
│ Files:                                                                          │
│ ├── backend/utils/circuit_breaker.py             → Circuit breaker impl        │
│ └── backend/guardrails/llm_guardrails.py         → Input/output validation     │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## Design Patterns Used

### 1. Event Sourcing (Kafka)
**What**: All state changes are stored as immutable events
**Why**: Enables replay, debugging, and audit trails
**Where**: Kafka topics for all incident state transitions

### 2. CQRS (Command Query Responsibility Segregation)
**What**: Separate models for reads vs writes
**Why**: Optimizes for different access patterns
**Where**: REST API writes to Kafka; UI reads from Redis/Postgres

### 3. Saga Pattern (LangGraph)
**What**: Long-running transactions with compensating actions
**Why**: Handles multi-step workflows with rollback capability
**Where**: 7-node LangGraph workflow with rollback steps

### 4. Circuit Breaker
**What**: Prevent cascade failures by failing fast
**Why**: Protects against unavailable dependencies
**Where**: GitHub, ServiceNow, OpenAI API clients

### 5. Swarm Intelligence with RRF (RAG)
**What**: Multiple agents produce rankings, combined via RRF fusion
**Why**: Fair fusion without manual weight tuning, industry standard
**Where**: Swarm RAG with 4 search agents + RRF + Cross-Encoder

### 6. LLM-as-Judge
**What**: Independent model validates another model's output
**Why**: Catches hallucinations and safety issues
**Where**: Claude evaluates GPT-4's generated plans

### 7. Human-in-the-Loop (HITL)
**What**: Humans approve critical decisions
**Why**: Maintains control over high-risk actions
**Where**: Approval workflow for medium/high risk plans

### 8. Feedback Loop (Online Learning)
**What**: System improves from successful outcomes
**Why**: Increases accuracy over time
**Where**: Weight optimization after successful resolutions

### 9. Hub-and-Spoke (Agent Architecture)
**What**: Central orchestrator coordinates specialized agents
**Why**: Separation of concerns, easier scaling
**Where**: LangGraph orchestrator → specialized agents

### 10. Retry with Exponential Backoff
**What**: Retry failed operations with increasing delays
**Why**: Handles transient failures gracefully
**Where**: All external API calls

---

## Why This Architecture Works

### 1. Reliability Through Decoupling
Kafka decouples ingestion from processing. If the AI system goes down, incidents queue up safely. When it recovers, processing resumes from where it left off.

### 2. Accuracy Through Consensus
No single search method is perfect. By combining semantic search (understands meaning), keyword search (exact matches), graph search (historical relationships), and metadata search (exact attributes), the system achieves higher accuracy than any individual approach.

### 3. Safety Through Multiple Validation Layers
- **Layer 1**: Input validation (guardrails)
- **Layer 2**: RAG quality (blast radius filtering)
- **Layer 3**: LLM-as-Judge (independent review)
- **Layer 4**: Human approval (final check)
- **Layer 5**: Post-execution verification

### 4. Auditability Through Event Sourcing
Every decision, every state change, every approval is recorded as an immutable event. Compliance auditors can replay any incident resolution to understand exactly what happened.

### 5. Adaptability Through Feedback Learning
The system gets better over time. Successful resolutions strengthen the associations between incident types and scripts. Agent weights are optimized based on which agents predicted correctly.

### 6. Resilience Through Circuit Breakers
When external services fail, the system degrades gracefully. Circuit breakers prevent cascade failures. Fallback paths ensure incidents are never lost.

---

## Component Dependency Analysis

### What Breaks If Kafka Is Removed?

| Impact | Description |
|--------|-------------|
| **Critical** | No reliable event delivery between components |
| **Critical** | No ordering guarantees for incident updates |
| **Critical** | No replay capability for debugging |
| **Critical** | No decoupling - direct synchronous calls required |
| **Workaround** | Could replace with Redis Streams (less durable) or direct API calls (tightly coupled) |

### What Breaks If MCP Is Removed?

| Impact | Description |
|--------|-------------|
| **High** | No standardized tool invocation protocol |
| **High** | Each tool requires custom integration code |
| **High** | No tool schema discovery |
| **Medium** | Agents become tightly coupled to tool implementations |
| **Workaround** | Could use direct REST calls (more code, less standardized) |

### What Breaks If Swarm RAG Is Removed?

| Impact | Description |
|--------|-------------|
| **High** | Lower accuracy in script matching |
| **High** | No consensus-based confidence scoring |
| **High** | No adaptive weight optimization |
| **Medium** | Single point of failure in search |
| **Workaround** | Could use single-source search (less accurate, no voting) |

### What Breaks If LLM-as-Judge Is Removed?

| Impact | Description |
|--------|-------------|
| **Critical** | No independent validation of generated plans |
| **Critical** | Higher risk of hallucinated or dangerous plans proceeding |
| **High** | No quality scoring for auto-approval decisions |
| **Workaround** | Could rely entirely on human review (slower, more costly) |

### What Breaks If Human Approval Is Removed?

| Impact | Description |
|--------|-------------|
| **Critical** | No human oversight for high-risk actions |
| **Critical** | Compliance violations (many regulations require human approval) |
| **High** | Increased risk of automated mistakes |
| **Workaround** | Not recommended - human oversight is essential for enterprise systems |

---

## Appendix A: Glossary

| Term | Definition |
|------|------------|
| **A2A** | Agent-to-Agent protocol for inter-agent communication |
| **MCP** | Model Context Protocol for agent-to-tool invocation |
| **LangGraph** | State machine library for LLM-based workflows |
| **Swarm RAG** | Multi-agent retrieval system using RRF fusion |
| **RRF** | Reciprocal Rank Fusion - weight-free rank aggregation |
| **LLM-as-Judge** | Pattern where one LLM evaluates another's output |
| **HITL** | Human-in-the-Loop - requiring human approval |
| **Circuit Breaker** | Pattern to prevent cascade failures |
| **DLQ** | Dead Letter Queue - storage for failed messages |
| **ITSM** | IT Service Management (e.g., ServiceNow) |

---

## Appendix B: Configuration Reference

### Environment Variables

```bash
# ServiceNow
SNOW_INSTANCE_URL=https://instance.service-now.com
SNOW_USERNAME=api_user
SNOW_PASSWORD=<secret>

# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:29092

# Databases
REDIS_URL=redis://localhost:6379
POSTGRES_URL=postgresql://user:pass@localhost:5432/aiagent
WEAVIATE_URL=http://localhost:8081
NEO4J_URI=bolt://localhost:7687

# LLM
OPENAI_API_KEY=<secret>
ANTHROPIC_API_KEY=<secret>

# GitHub
GITHUB_TOKEN=<secret>
GITHUB_REPO=owner/repo

# Thresholds
AUTO_APPROVE_MIN_CONFIDENCE=0.90
AUTO_APPROVE_MIN_JUDGE_SCORE=8
APPROVAL_TIMEOUT_MINUTES=60
```

---

## Appendix C: Visual Architecture Diagram

```
                              ┌─────────────────┐
                              │   ServiceNow    │
                              │   (Incident)    │
                              └────────┬────────┘
                                       │ Step 1-2
                                       ▼
                              ┌─────────────────┐
                              │  ServiceNow     │
                              │    Agent        │
                              └────────┬────────┘
                                       │ Step 3
                                       ▼
┌──────────────────────────────────────────────────────────────────────┐
│                         KAFKA EVENT BUS                               │
│  [servicenow.incidents] [gcp.alerts] [incident.approved] [...]       │
└──────────────────────────────────────────────────────────────────────┘
                                       │ Step 4-6
                                       ▼
                              ┌─────────────────┐
                              │    LangGraph    │
                              │   Orchestrator  │
                              └────────┬────────┘
                                       │
                    ┌──────────────────┼──────────────────┐
                    │                  │                  │
                    ▼                  ▼                  ▼
           ┌───────────────┐  ┌───────────────┐  ┌───────────────┐
           │  Node 1:      │  │  Node 2:      │  │  Node 3:      │
           │ Receive/Parse │  │  Swarm RAG    │  │ Generate Plan │
           └───────────────┘  └───────────────┘  └───────────────┘
                                       │
                    ┌──────────────────┴──────────────────┐
                    │                                     │
                    ▼                                     ▼
           ┌───────────────────────────────────────────────────────┐
           │                    SWARM RAG                          │
           │  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐    │
           │  │ Vector  │ │ Keyword │ │  Graph  │ │Metadata │    │
           │  │ (0.40)  │ │ (0.25)  │ │ (0.25)  │ │ (0.10)  │    │
           │  └────┬────┘ └────┬────┘ └────┬────┘ └────┬────┘    │
           │       └──────────┬┴───────────┴───────────┘          │
           │                  ▼                                    │
           │         ┌──────────────┐                             │
           │         │  Consensus   │                             │
           │         │  + Rerank    │                             │
           │         └──────────────┘                             │
           └───────────────────────────────────────────────────────┘
                                       │
                                       ▼
                              ┌─────────────────┐
                              │   Node 4:       │
                              │   LLM Judge     │
                              └────────┬────────┘
                                       │
                          ┌────────────┼────────────┐
                          │ APPROVED   │ REVISION   │ REJECTED
                          ▼            │            ▼
                 ┌─────────────────┐   │    ┌─────────────────┐
                 │   Node 5:       │   │    │     STOP        │
                 │ Control Plane   │   │    │   (Escalate)    │
                 └────────┬────────┘   │    └─────────────────┘
                          │            │
              ┌───────────┴────────────┘
              │ Auto-approve?
              ▼
     ┌─────────────────┐          ┌─────────────────┐
     │     YES         │          │      NO         │
     │  → Execute      │          │ → Human Review  │
     └────────┬────────┘          └────────┬────────┘
              │                            │
              └────────────┬───────────────┘
                           ▼
                  ┌─────────────────┐
                  │   Node 6:       │
                  │   Execute       │
                  └────────┬────────┘
                           │
                           ▼
                  ┌─────────────────┐
                  │ GitHub Actions  │
                  │  (Remediation)  │
                  └────────┬────────┘
                           │
                           ▼
                  ┌─────────────────┐
                  │   Node 7:       │
                  │ Verify & Close  │
                  └────────┬────────┘
                           │
              ┌────────────┴────────────┐
              │                         │
              ▼                         ▼
     ┌─────────────────┐       ┌─────────────────┐
     │  Close Ticket   │       │  Update RAG     │
     │  (ServiceNow)   │       │  (Learning)     │
     └─────────────────┘       └─────────────────┘
```

---

*Document End*

*This whitepaper provides a complete technical reference for the AI Incident Management Platform v5.0. For questions or clarifications, contact the platform engineering team.*
