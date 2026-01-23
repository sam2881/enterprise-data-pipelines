# AI Agent Platform v5.0 - Observability Architecture Whitepaper

## Complete End-to-End Observability for AI-Powered Incident Management

**Document Version:** 1.0
**Platform Version:** 5.0
**Last Updated:** January 2026
**Classification:** Internal Engineering / SRE / Compliance

---

## Executive Summary

This whitepaper provides a comprehensive, step-by-step explanation of the observability architecture implemented in the AI Agent Platform v5.0. The platform uses a full LMT (Logs, Metrics, Traces) stack combined with specialized AI/LLM observability through Langfuse. This document is suitable for:

- **New Engineer Onboarding** - Understanding how observability works across the system
- **SRE / Platform Teams** - Operating and debugging the production system
- **Architecture Reviews** - Evaluating system reliability and observability coverage
- **Security & Compliance** - Demonstrating EU AI Act, SOC2, and GDPR compliance
- **Non-Technical Stakeholders** - Understanding observability at a conceptual level

---

## Table of Contents

1. [System Overview](#system-overview)
2. [Step-by-Step Observability Flow](#step-by-step-observability-flow)
3. [Logging Architecture](#1-logging-architecture)
4. [Metrics Architecture (Prometheus)](#2-metrics-architecture-prometheus)
5. [Dashboarding (Grafana)](#3-dashboarding-grafana)
6. [Distributed Tracing](#4-distributed-tracing)
7. [Langfuse (LLM Observability)](#5-langfuse-llm-observability)
8. [Correlation Strategy](#6-correlation-strategy)
9. [Failure & Resilience Observability](#7-failure--resilience-observability)
10. [Observability Signal Map](#a-observability-signal-map)
11. [Debugging Playbooks](#b-debugging-playbooks)
12. [SRE View vs Developer View](#c-sre-view-vs-developer-view)
13. [Why This Observability Design Works](#d-why-this-observability-design-works)
14. [What Would Break Without Observability](#e-what-would-break-without-observability)

---

## System Overview

The AI Agent Platform processes incidents from ServiceNow, GCP alerts, and other sources through a sophisticated pipeline:

```
[Incident Sources] → [Kafka] → [Orchestrator] → [RAG/LLM Analysis] → [Human Approval] → [Execution]
                                     ↓
                           [Domain Agents]
                      (ServiceNow, Jira, GitHub, Infra)
```

**Key Components Observed:**
- **Kafka** - Message ingestion and streaming
- **Orchestrator** - LangGraph workflow engine (18-node DAG)
- **RAG System** - Hybrid search with Weaviate + Neo4j
- **LLM Layer** - OpenAI GPT-4 with Langfuse tracing
- **Domain Agents** - ServiceNow, Jira, GitHub, Infrastructure
- **MCP Servers** - Tool execution via Model Context Protocol
- **Execution Layer** - GitHub Actions, direct scripts

---

## Step-by-Step Observability Flow

### STEP 1: INCIDENT INGESTION FROM KAFKA

**Purpose:**
- Capture the moment an incident enters the system
- Establish the primary correlation ID (`incident_id`) for end-to-end tracing
- Track ingestion volume and source distribution

**What Happens (Layman View):**
When an incident arrives from ServiceNow or GCP alerts via Kafka, the system logs its arrival, assigns a unique ID, and starts tracking metrics. This is the "birth certificate" of every incident.

**How It Works (Technical View):**

The Kafka consumer in `backend/streaming/incident_consumer.py` receives messages and immediately instruments them:

```python
# Logging with correlation ID
logger.info(
    "processing_incident_from_kafka",
    incident_id=incident_id,
    source=source_system,
    message_offset=message.offset
)

# Metrics increment
INCIDENTS_PROCESSED.labels(
    source=source_system,  # "servicenow", "gcp_alerts"
    severity=severity,      # "P1", "P2", "P3", "P4"
    status="received"
).inc()

INCIDENTS_ACTIVE.inc()  # Gauge: currently processing
```

**Signals Generated:**

| Signal Type | Name | Labels/Fields | Purpose |
|-------------|------|---------------|---------|
| Log | `processing_incident_from_kafka` | `incident_id`, `source`, `offset` | Audit trail |
| Metric (Counter) | `aiagent_incidents_processed_total` | `source`, `severity`, `status` | Volume tracking |
| Metric (Gauge) | `aiagent_incidents_active` | - | Current load |

**Tools & Backends:**
- **Logging**: structlog → stdout → Log aggregator (Loki/CloudWatch)
- **Metrics**: prometheus_client → Prometheus scrape (:8000/metrics)

**Files Involved:**
- `backend/streaming/incident_consumer.py` → Kafka consumer logic
- `backend/orchestrator/metrics.py` → Metric definitions

**Failure & Fallback:**
- If Prometheus is unavailable, metrics are lost but processing continues
- If logging fails, the system continues (fire-and-forget logging)
- Kafka consumer uses auto-commit with dead-letter queue for failed messages

---

### STEP 2: REQUEST ENTERS ORCHESTRATOR API

**Purpose:**
- Track HTTP request latency and success rates
- Provide SLI/SLO data for API health
- Enable per-endpoint performance analysis

**What Happens (Layman View):**
Every API call to the orchestrator is timed and counted. This tells us how healthy our API is and which endpoints are slow.

**How It Works (Technical View):**

The orchestrator uses a decorator pattern for automatic instrumentation:

```python
# From backend/orchestrator/metrics.py

@track_request(endpoint="/incidents")
async def process_incident_endpoint(incident: IncidentRequest):
    # Processing logic here
    pass

# Decorator implementation
def track_request(endpoint: str):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            start_time = time.time()
            status = "success"
            try:
                result = await func(*args, **kwargs)
                return result
            except Exception as e:
                status = "error"
                ERRORS.labels(component="api", error_type=type(e).__name__).inc()
                raise
            finally:
                duration = time.time() - start_time
                REQUEST_COUNT.labels(method="POST", endpoint=endpoint, status=status).inc()
                REQUEST_LATENCY.labels(method="POST", endpoint=endpoint).observe(duration)
        return wrapper
    return decorator
```

**Signals Generated:**

| Signal Type | Name | Labels/Fields | Bucket Values |
|-------------|------|---------------|---------------|
| Metric (Counter) | `aiagent_requests_total` | `method`, `endpoint`, `status` | N/A |
| Metric (Histogram) | `aiagent_request_latency_seconds` | `method`, `endpoint` | 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0 |
| Metric (Counter) | `aiagent_errors_total` | `component`, `error_type` | N/A |

**Tools & Backends:**
- **Prometheus** scrapes `/metrics` endpoint every 15 seconds
- **Grafana** visualizes request rate and latency percentiles

**Files Involved:**
- `backend/orchestrator/main.py` → FastAPI application with decorated endpoints
- `backend/orchestrator/metrics.py` → `@track_request` decorator and metric definitions

**Failure & Fallback:**
- Metrics collection never blocks request processing
- Uses in-memory counters; no external dependency during request path

---

### STEP 3: RAG CONTEXT RETRIEVAL

**Purpose:**
- Track knowledge retrieval latency for performance tuning
- Monitor result quality (count of results returned)
- Detect vector/graph database connectivity issues

**What Happens (Layman View):**
Before the AI can analyze an incident, it searches for similar past incidents and runbooks. We track how fast this search is and whether it finds useful information.

**How It Works (Technical View):**

The hybrid RAG system queries Weaviate (vector) and Neo4j (graph) in parallel:

```python
# From backend/rag/hybrid_search_engine.py

def search(self, query: str, top_k: int = 5) -> List[SearchResult]:
    start_time = time.time()

    try:
        # Vector search (Weaviate)
        vector_results = self._vector_search(query)

        # Graph search (Neo4j)
        graph_results = self._graph_search(query)

        # RRF fusion
        combined = self._rrf_fusion(vector_results, graph_results)

        latency = time.time() - start_time

        # Record metrics
        record_rag_query(
            collection="scripts",
            status="success",
            latency=latency,
            result_count=len(combined)
        )

        logger.info(
            "rag_search_completed",
            query_length=len(query),
            result_count=len(combined),
            latency_ms=latency * 1000
        )

        return combined

    except Exception as e:
        record_rag_query(collection="scripts", status="error", latency=0, result_count=0)
        logger.error("rag_search_failed", error=str(e))
        raise
```

**Signals Generated:**

| Signal Type | Name | Labels | Bucket Values |
|-------------|------|--------|---------------|
| Metric (Counter) | `aiagent_rag_queries_total` | `collection`, `status` | N/A |
| Metric (Histogram) | `aiagent_rag_latency_seconds` | `collection` | 0.05, 0.1, 0.25, 0.5, 1.0, 2.0 |
| Metric (Histogram) | `aiagent_rag_results_count` | `collection` | 0, 1, 2, 3, 5, 10, 20 |
| Metric (Counter) | `aiagent_graph_queries_total` | `query_type`, `status` | N/A |
| Log | `rag_search_completed` | `query_length`, `result_count`, `latency_ms` | N/A |

**Tools & Backends:**
- **Weaviate** - Vector database for semantic search
- **Neo4j** - Graph database for relationship-based queries
- **Prometheus** - Latency and result count metrics

**Files Involved:**
- `backend/rag/hybrid_search_engine.py` → Main search orchestration
- `backend/rag/swarm_retriever.py` → Swarm-based parallel retrieval
- `backend/orchestrator/metrics.py` → `record_rag_query()`, `record_graph_query()`

**Failure & Fallback:**
- If Weaviate fails, the system falls back to TF-IDF search
- If Neo4j fails, graph scoring is skipped (vector + metadata only)
- Cache layer (Redis) provides resilience against database spikes

---

### STEP 4: LLM ANALYSIS AND TRACING

**Purpose:**
- Track LLM API latency and error rates
- Monitor token consumption and costs
- Provide detailed prompt/response tracing via Langfuse
- Enable debugging of AI decision quality

**What Happens (Layman View):**
The AI analyzes the incident to find the root cause and suggest remediation. Every AI call is timed, token-counted, and the full conversation is recorded for debugging and auditing.

**How It Works (Technical View):**

The LLM Intelligence module tracks every call with both Prometheus metrics and Langfuse traces:

```python
# From backend/orchestrator/llm_intelligence.py

async def analyze_incident(self, incident: Dict) -> AnalysisResult:
    start_time = time.time()
    incident_id = incident.get('incident_id')

    try:
        # Make OpenAI API call
        response = await self.openai_client.chat.completions.create(
            model="gpt-4-turbo-preview",
            messages=[
                {"role": "system", "content": ANALYSIS_PROMPT},
                {"role": "user", "content": json.dumps(incident)}
            ],
            temperature=0.2,
            response_format={"type": "json_object"}
        )

        duration = time.time() - start_time
        tokens = response.usage.total_tokens

        # Prometheus metrics
        LLM_CALLS.labels(model="gpt-4-turbo-preview", purpose="analyze", status="success").inc()
        LLM_LATENCY.labels(model="gpt-4-turbo-preview", purpose="analyze").observe(duration)
        LLM_TOKENS.labels(model="gpt-4-turbo-preview", type="input").inc(response.usage.prompt_tokens)
        LLM_TOKENS.labels(model="gpt-4-turbo-preview", type="output").inc(response.usage.completion_tokens)

        # Langfuse tracing (detailed prompt/response capture)
        _track_llm_call(
            name="analyze_incident",
            model="gpt-4-turbo-preview",
            input_text=json.dumps(incident),
            output_text=response.choices[0].message.content,
            trace_id=incident_id,
            prompt_tokens=response.usage.prompt_tokens,
            completion_tokens=response.usage.completion_tokens,
            duration_ms=duration * 1000
        )

        # Structured logging
        logger.info(
            "llm_analysis_completed",
            incident_id=incident_id,
            duration=f"{duration:.2f}s",
            tokens=tokens
        )

        return AnalysisResult.parse(response.choices[0].message.content)

    except Exception as e:
        LLM_CALLS.labels(model="gpt-4-turbo-preview", purpose="analyze", status="error").inc()
        ERRORS.labels(component="llm", error_type=type(e).__name__).inc()
        logger.error("llm_analysis_failed", incident_id=incident_id, error=str(e))
        raise
```

**Langfuse Trace Creation:**

```python
def _track_llm_call(name, model, input_text, output_text, trace_id, prompt_tokens, completion_tokens, duration_ms):
    """Create Langfuse trace for LLM call"""

    # Create trace (groups all spans for this incident)
    trace = langfuse_client.trace(
        name=f"incident_{trace_id}" if trace_id else f"llm_{name}",
        user_id="ai-agent-platform",
        metadata={
            "incident_id": trace_id,
            "operation": name
        },
        tags=["ai-agent", name, model]
    )

    # Create generation span (the actual LLM call)
    generation = trace.generation(
        name=name,
        model=model,
        model_parameters={"temperature": 0.2, "response_format": "json"},
        input=[
            {"role": "system", "content": "AI Agent Platform SRE Assistant"},
            {"role": "user", "content": input_text[:5000]}  # Truncate for storage
        ],
        output=output_text[:5000],
        usage={
            "input": prompt_tokens,
            "output": completion_tokens,
            "total": prompt_tokens + completion_tokens,
            "unit": "TOKENS"
        }
    )
    generation.end()
    langfuse_client.flush()
```

**Signals Generated:**

| Signal Type | Name | Labels | Purpose |
|-------------|------|--------|---------|
| Metric (Counter) | `aiagent_llm_calls_total` | `model`, `purpose`, `status` | Call volume by model |
| Metric (Histogram) | `aiagent_llm_latency_seconds` | `model`, `purpose` | Latency distribution |
| Metric (Counter) | `aiagent_llm_tokens_total` | `model`, `type` | Token consumption |
| Langfuse Trace | `incident_{incident_id}` | Full prompt/response | Debugging, quality review |
| Log | `llm_analysis_completed` | `incident_id`, `duration`, `tokens` | Operational visibility |

**Tools & Backends:**
- **Prometheus** - Quantitative metrics (latency, tokens, errors)
- **Langfuse** - Qualitative tracing (full prompts and responses)
- **Cost Tracker** - Dollar cost per call

**Files Involved:**
- `backend/orchestrator/llm_intelligence.py` → LLM call orchestration
- `backend/utils/cost_tracker.py` → Cost calculation
- `backend/orchestrator/metrics.py` → `LLM_CALLS`, `LLM_LATENCY`, `LLM_TOKENS`

**Failure & Fallback:**
- If Langfuse is unavailable, tracing is skipped (non-blocking)
- Prometheus metrics always recorded
- OpenAI API failures trigger circuit breaker

---

### STEP 5: SCRIPT MATCHING AND SCORING

**Purpose:**
- Track the hybrid matching algorithm performance
- Monitor confidence score distribution
- Identify when the system cannot find suitable scripts

**What Happens (Layman View):**
The system searches for the best remediation script by combining multiple scoring methods: vector similarity, metadata matching, graph relationships, and safety validation. We track each score to understand which method is most effective.

**How It Works (Technical View):**

```python
# From backend/agents/remediation/enterprise_matcher.py

def match_script(self, incident: Dict) -> MatchResult:
    # Compute individual scores
    vector_score = self._compute_vector_score(incident)
    metadata_score = self._compute_metadata_score(incident)
    graph_score = self._compute_graph_score(incident)
    safety_score = self._compute_safety_score(incident)

    # Weighted fusion
    # Formula: 0.5*vector + 0.25*metadata + 0.15*graph + 0.10*safety
    final_score = (
        0.50 * vector_score +
        0.25 * metadata_score +
        0.15 * graph_score +
        0.10 * safety_score
    )

    # Record all scores for analysis
    record_script_match(
        vector_score=vector_score,
        metadata_score=metadata_score,
        graph_score=graph_score,
        safety_score=safety_score,
        final_score=final_score,
        matched=final_score >= 0.75
    )

    logger.info(
        "script_match_completed",
        incident_id=incident.get('incident_id'),
        final_score=final_score,
        matched=final_score >= 0.75
    )

    return MatchResult(score=final_score, script=best_script)
```

**Signals Generated:**

| Signal Type | Name | Labels | Bucket Values |
|-------------|------|--------|---------------|
| Metric (Histogram) | `aiagent_script_match_score` | `score_type` (vector, metadata, graph, safety, final) | 0.1-1.0 in 0.1 increments |
| Metric (Counter) | `aiagent_script_matches_total` | `result` (success, no_match, low_confidence) | N/A |
| Log | `script_match_completed` | `incident_id`, `final_score`, `matched` | N/A |

**Tools & Backends:**
- **Prometheus** - Score distribution histograms
- **Grafana** - Visualize which scoring method contributes most

**Files Involved:**
- `backend/agents/remediation/enterprise_matcher.py` → Hybrid matching logic
- `backend/orchestrator/metrics.py` → `record_script_match()`, `SCRIPT_MATCH_SCORES`

**Failure & Fallback:**
- If vector search fails, metadata-only matching is used
- If graph database is unavailable, graph_score defaults to 0
- Low confidence triggers human review workflow

---

### STEP 6: WORKFLOW NODE EXECUTION

**Purpose:**
- Track execution time of each LangGraph workflow node
- Identify bottlenecks in the 18-node workflow
- Detect stuck or failed workflow steps

**What Happens (Layman View):**
The incident flows through an 18-step workflow (classify, analyze, match, approve, execute, etc.). We measure how long each step takes to find slow spots.

**How It Works (Technical View):**

```python
# From backend/orchestrator/langgraph_workflow.py

@track_workflow_node(node_name="classify_incident", phase="analyze")
def classify_incident(state: WorkflowState) -> WorkflowState:
    """Classify incident severity and category"""
    # Classification logic
    return updated_state

# Decorator implementation from metrics.py
def track_workflow_node(node_name: str, phase: str):
    def decorator(func):
        def wrapper(state, *args, **kwargs):
            start_time = time.time()
            status = "success"

            # Update current node gauge
            incident_id = state.get('incident_id', 'unknown')
            WORKFLOW_CURRENT_NODE.labels(incident_id=incident_id).set(hash(node_name) % 18)

            try:
                result = func(state, *args, **kwargs)
                return result
            except Exception as e:
                status = "error"
                ERRORS.labels(component="workflow", error_type=type(e).__name__).inc()
                raise
            finally:
                duration = time.time() - start_time
                WORKFLOW_NODE_DURATION.labels(node_name=node_name, phase=phase).observe(duration)
                WORKFLOW_STEP_COUNT.labels(node_name=node_name, status=status).inc()
        return wrapper
    return decorator
```

**18-Node Workflow Phases:**

| Phase | Nodes | Typical Duration |
|-------|-------|------------------|
| Intake | `ingest`, `validate`, `enrich` | 100-500ms |
| Analysis | `classify`, `analyze_root_cause`, `search_context` | 2-10s |
| Matching | `match_scripts`, `rank_candidates`, `validate_safety` | 1-5s |
| Approval | `check_risk`, `route_approval`, `wait_human` | Variable |
| Execution | `generate_plan`, `dry_run`, `execute`, `verify` | 10s-10min |
| Completion | `update_ticket`, `store_knowledge`, `notify` | 1-5s |

**Signals Generated:**

| Signal Type | Name | Labels | Bucket Values |
|-------------|------|--------|---------------|
| Metric (Histogram) | `aiagent_workflow_node_duration_seconds` | `node_name`, `phase` | 0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0 |
| Metric (Counter) | `aiagent_workflow_steps_total` | `node_name`, `status` | N/A |
| Metric (Gauge) | `aiagent_workflow_current_node` | `incident_id` | N/A |
| Metric (Counter) | `aiagent_workflow_executions_total` | `workflow_type`, `status` | N/A |

**Tools & Backends:**
- **Prometheus** - Node duration histograms
- **Grafana** - Workflow flame graph visualization

**Files Involved:**
- `backend/orchestrator/langgraph_workflow.py` → Workflow node definitions
- `backend/orchestrator/metrics.py` → `@track_workflow_node` decorator

**Failure & Fallback:**
- Node failures trigger workflow state save for retry
- Stuck nodes detected via `WorkflowNodeStuck` alert (>5 minutes)
- Circuit breaker protects external dependencies

---

### STEP 7: APPROVAL WORKFLOW TRACKING

**Purpose:**
- Track how long approvals take (SLO metric)
- Monitor approval backlog (operational health)
- Ensure human oversight compliance (EU AI Act Art. 14)

**What Happens (Layman View):**
High-risk remediation actions require human approval. We track how many are pending and how long people take to respond.

**How It Works (Technical View):**

```python
# When approval is requested
APPROVALS_PENDING.inc()
approval_request_time = time.time()

# When approval is received
APPROVALS_PENDING.dec()
wait_time = time.time() - approval_request_time

APPROVALS_PROCESSED.labels(action="approved", risk_level="high").inc()
APPROVAL_WAIT_TIME.labels(risk_level="high").observe(wait_time)

# Audit log for compliance
audit_logger.log_human_oversight(
    user=approver_email,
    action="approve_remediation",
    incident_id=incident_id,
    ai_recommendation=recommended_script,
    user_decision="approved"
)

logger.info(
    "approval_received",
    incident_id=incident_id,
    approver=approver_email,
    wait_time_seconds=wait_time,
    action="approved"
)
```

**Signals Generated:**

| Signal Type | Name | Labels | Bucket Values |
|-------------|------|--------|---------------|
| Metric (Gauge) | `aiagent_approvals_pending` | - | N/A |
| Metric (Counter) | `aiagent_approvals_processed_total` | `action`, `risk_level` | N/A |
| Metric (Histogram) | `aiagent_approval_wait_seconds` | `risk_level` | 60, 300, 600, 1800, 3600 |
| Audit Log | `HUMAN_APPROVAL` event | `user`, `incident_id`, `ai_recommendation` | N/A |
| Log | `approval_received` | `incident_id`, `approver`, `wait_time_seconds` | N/A |

**Tools & Backends:**
- **Prometheus** - Pending count and wait time
- **Audit Logger** - Compliance record
- **Slack** - Approval request notifications

**Files Involved:**
- `backend/agents/control_plane.py` → Approval routing logic
- `backend/governance/audit_logger.py` → `log_human_oversight()`
- `backend/orchestrator/metrics.py` → `APPROVALS_PENDING`, `APPROVAL_WAIT_TIME`

**Failure & Fallback:**
- If Slack is unavailable, approval requests are queued
- Alert fires if pending approvals exceed threshold (>10)
- Auto-escalation after configurable timeout

---

### STEP 8: REMEDIATION EXECUTION

**Purpose:**
- Track execution success/failure rates
- Monitor execution duration by script type
- Record confidence scores for quality analysis

**What Happens (Layman View):**
The system executes the remediation script (either directly or via GitHub Actions). We track whether it succeeded and how long it took.

**How It Works (Technical View):**

```python
# From backend/agents/execution_orchestrator.py

async def execute_remediation(self, plan: ExecutionPlan) -> ExecutionResult:
    start_time = time.time()
    script_type = plan.script.type  # "restart_service", "scale_pods", etc.
    mode = "auto" if plan.auto_approved else "manual"

    try:
        if plan.dry_run:
            record_dry_run(script_type)
            result = await self._dry_run(plan)
        else:
            result = await self._execute(plan)

        duration = time.time() - start_time
        status = "success" if result.success else "failed"

        # Record metrics
        record_remediation_execution(
            script_type=script_type,
            mode=mode,
            status=status,
            confidence=plan.confidence,
            duration=duration
        )

        # Audit log
        audit_logger.log(
            event_type=AuditEventType.REMEDIATION_EXECUTION,
            actor="execution-orchestrator",
            actor_type="system",
            action=f"execute_{script_type}",
            resource=plan.incident_id,
            resource_type="incident",
            outcome=status,
            risk_level=plan.risk_level,
            details={
                "script": plan.script.name,
                "duration_seconds": duration,
                "dry_run": plan.dry_run
            },
            confidence=plan.confidence,
            human_oversight=not plan.auto_approved
        )

        logger.info(
            "remediation_completed",
            incident_id=plan.incident_id,
            script=plan.script.name,
            status=status,
            duration=duration
        )

        return result

    except Exception as e:
        ERRORS.labels(component="execution", error_type=type(e).__name__).inc()
        record_remediation_execution(script_type, mode, "error", plan.confidence, 0)
        logger.error("remediation_failed", incident_id=plan.incident_id, error=str(e))
        raise
```

**Signals Generated:**

| Signal Type | Name | Labels | Bucket Values |
|-------------|------|--------|---------------|
| Metric (Counter) | `aiagent_remediation_executions_total` | `script_type`, `mode`, `status` | N/A |
| Metric (Histogram) | `aiagent_remediation_confidence` | `script_type` | 0.5-1.0 in 0.05 increments |
| Metric (Histogram) | `aiagent_remediation_duration_seconds` | `script_type`, `mode` | 1, 5, 10, 30, 60, 120, 300, 600 |
| Metric (Counter) | `aiagent_dry_run_executions_total` | `script_type` | N/A |
| Audit Log | `REMEDIATION_EXECUTION` event | Full execution details | N/A |

**Tools & Backends:**
- **Prometheus** - Execution metrics
- **GitHub Actions** - Execution engine (for IaC scripts)
- **Audit Logger** - Compliance record

**Files Involved:**
- `backend/agents/execution_orchestrator.py` → Execution logic
- `backend/utils/github_actions.py` → GitHub Actions integration
- `backend/orchestrator/metrics.py` → `record_remediation_execution()`

**Failure & Fallback:**
- Failed executions trigger rollback plan
- Circuit breaker prevents repeated failures
- Alert fires if failure rate exceeds 20%

---

### STEP 9: INCIDENT RESOLUTION AND CLOSEOUT

**Purpose:**
- Track end-to-end resolution time (key SLO)
- Record resolution in ServiceNow
- Store knowledge for future incidents

**What Happens (Layman View):**
When the incident is resolved, we record the total time it took and update ServiceNow. The resolution is also stored in our knowledge base for future reference.

**How It Works (Technical View):**

```python
# Resolution tracking
resolution_time = time.time() - incident_start_time

INCIDENT_RESOLUTION_TIME.labels(
    severity=incident.severity,
    service=incident.service
).observe(resolution_time)

INCIDENTS_ACTIVE.dec()
INCIDENTS_PROCESSED.labels(
    source=incident.source,
    severity=incident.severity,
    status="resolved"
).inc()

# Update ServiceNow
servicenow_update_start = time.time()
await self.servicenow_client.update_incident(
    incident_id=incident.servicenow_id,
    state="resolved",
    resolution_notes=resolution.summary
)
record_servicenow_request("update", "success", time.time() - servicenow_update_start)

# Store in knowledge graph for future retrieval
await self.rag.store_incident_with_graph(
    incident_id=incident.id,
    incident_data=resolution.to_dict(),
    service=incident.service,
    topic=incident.category
)

logger.info(
    "incident_resolved",
    incident_id=incident.id,
    resolution_time_seconds=resolution_time,
    severity=incident.severity
)
```

**Signals Generated:**

| Signal Type | Name | Labels | Bucket Values |
|-------------|------|--------|---------------|
| Metric (Histogram) | `aiagent_incident_resolution_seconds` | `severity`, `service` | 60, 300, 600, 1800, 3600, 7200, 14400 |
| Metric (Counter) | `aiagent_incidents_processed_total` | `source`, `severity`, `status="resolved"` | N/A |
| Metric (Counter) | `aiagent_servicenow_requests_total` | `operation`, `status` | N/A |
| Log | `incident_resolved` | `incident_id`, `resolution_time_seconds`, `severity` | N/A |

**Tools & Backends:**
- **Prometheus** - Resolution time histogram
- **ServiceNow** - Ticket update
- **Neo4j** - Knowledge storage

**Files Involved:**
- `backend/orchestrator/main.py` → Resolution handling
- `backend/agents/servicenow/agent.py` → ServiceNow updates
- `backend/rag/hybrid_search_engine.py` → Knowledge storage

**Failure & Fallback:**
- If ServiceNow update fails, it's queued for retry
- Knowledge storage failure doesn't block resolution
- Resolution time still recorded even if downstream fails

---

## 1. LOGGING ARCHITECTURE

### Structured vs Unstructured Logs

The platform uses **structured logging** exclusively via `structlog`. Every log event is a JSON object with consistent fields.

**Configuration:**
```python
import structlog

logger = structlog.get_logger()

# Example structured log
logger.info(
    "incident_processed",        # Event name
    incident_id="INC-001234",    # Correlation ID
    severity="P2",               # Incident field
    duration_ms=1250,            # Performance data
    agent="orchestrator"         # Component identifier
)
```

**Output Format:**
```json
{
  "event": "incident_processed",
  "incident_id": "INC-001234",
  "severity": "P2",
  "duration_ms": 1250,
  "agent": "orchestrator",
  "timestamp": "2026-01-01T12:00:00.000Z",
  "level": "info"
}
```

### Log Levels

| Level | Usage | Example Events |
|-------|-------|----------------|
| `DEBUG` | Development only | Variable values, execution paths |
| `INFO` | Normal operations | `incident_processed`, `llm_call_completed` |
| `WARNING` | Non-critical issues | `cache_miss`, `slow_response` |
| `ERROR` | Failures | `llm_call_failed`, `database_error` |
| `CRITICAL` | System failures | `kafka_consumer_crash` |

### Correlation IDs

Three primary correlation identifiers flow through the system:

| ID | Format | Scope | Propagation |
|----|--------|-------|-------------|
| `incident_id` | `INC-XXXXXX` | End-to-end incident | Kafka headers → All services |
| `trace_id` | 32-char hex | Request/response | OTEL headers → Langfuse |
| `agent_id` | `{agent_name}_{uuid}` | Per-agent operation | Agent context |

### PII Safety

The `LLMGuardrails` module detects and redacts PII before logging:

```python
# From backend/guardrails/llm_guardrails.py

PII_PATTERNS = [
    (r"\b\d{3}[-.]?\d{2}[-.]?\d{4}\b", "SSN"),
    (r"\b\d{16}\b", "credit_card"),
    (r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b", "email"),
    (r"\b\d{3}[-.]?\d{3}[-.]?\d{4}\b", "phone"),
]

def _check_pii(self, text: str) -> List[str]:
    """Detect PII patterns in text"""
    detected = []
    for pattern, pii_type in self.PII_PATTERNS:
        if re.search(pattern, text):
            detected.append(pii_type)
    return detected
```

### Log Storage

| Environment | Destination | Retention |
|-------------|-------------|-----------|
| Development | stdout | Session |
| Production | CloudWatch/Loki | 30 days |
| Audit Logs | Dedicated store | 7 years |

---

## 2. METRICS ARCHITECTURE (PROMETHEUS)

### Why Metrics Exist

Metrics enable:
- **SLIs** (Service Level Indicators): Measurable values (e.g., P95 latency)
- **SLOs** (Service Level Objectives): Targets (e.g., P95 < 5s)
- **SLAs** (Service Level Agreements): Commitments to users
- **Alerting**: Automated detection of anomalies
- **Capacity Planning**: Understanding resource needs

### Metric Types

The platform uses all four Prometheus metric types:

#### Counter (Monotonically increasing)
```python
INCIDENTS_PROCESSED = Counter(
    'aiagent_incidents_processed_total',
    'Total incidents processed',
    ['source', 'severity', 'status']
)
# Usage: INCIDENTS_PROCESSED.labels(source="servicenow", severity="P2", status="resolved").inc()
```

#### Gauge (Point-in-time value)
```python
INCIDENTS_ACTIVE = Gauge(
    'aiagent_incidents_active',
    'Currently active incidents'
)
# Usage: INCIDENTS_ACTIVE.inc(), INCIDENTS_ACTIVE.dec(), INCIDENTS_ACTIVE.set(5)
```

#### Histogram (Distribution with buckets)
```python
LLM_LATENCY = Histogram(
    'aiagent_llm_latency_seconds',
    'LLM API call latency',
    ['model', 'purpose'],
    buckets=[0.5, 1.0, 2.0, 5.0, 10.0, 30.0]
)
# Usage: LLM_LATENCY.labels(model="gpt-4", purpose="analyze").observe(2.5)
```

#### Info (Static metadata)
```python
SYSTEM_INFO = Info('aiagent_system', 'AI Agent Platform system information')
SYSTEM_INFO.info({
    'version': '4.0.0',
    'workflow': '18-node-langgraph'
})
```

### Complete Metric Catalog

| Category | Metric Name | Type | Labels | Purpose |
|----------|-------------|------|--------|---------|
| **Requests** | `aiagent_requests_total` | Counter | method, endpoint, status | API volume |
| | `aiagent_request_latency_seconds` | Histogram | method, endpoint | API latency |
| **Incidents** | `aiagent_incidents_processed_total` | Counter | source, severity, status | Incident volume |
| | `aiagent_incidents_active` | Gauge | - | Current load |
| | `aiagent_incident_resolution_seconds` | Histogram | severity, service | Resolution SLO |
| **Workflow** | `aiagent_workflow_executions_total` | Counter | workflow_type, status | Workflow runs |
| | `aiagent_workflow_node_duration_seconds` | Histogram | node_name, phase | Node performance |
| | `aiagent_workflow_steps_total` | Counter | node_name, status | Step tracking |
| **LLM** | `aiagent_llm_calls_total` | Counter | model, purpose, status | LLM volume |
| | `aiagent_llm_latency_seconds` | Histogram | model, purpose | LLM performance |
| | `aiagent_llm_tokens_total` | Counter | model, type | Token consumption |
| **RAG** | `aiagent_rag_queries_total` | Counter | collection, status | Search volume |
| | `aiagent_rag_latency_seconds` | Histogram | collection | Search latency |
| | `aiagent_rag_results_count` | Histogram | collection | Result quality |
| **Remediation** | `aiagent_remediation_executions_total` | Counter | script_type, mode, status | Execution volume |
| | `aiagent_remediation_confidence` | Histogram | script_type | Confidence distribution |
| | `aiagent_script_match_score` | Histogram | score_type | Matching quality |
| **Approvals** | `aiagent_approvals_pending` | Gauge | - | Backlog |
| | `aiagent_approvals_processed_total` | Counter | action, risk_level | Approval rate |
| | `aiagent_approval_wait_seconds` | Histogram | risk_level | Wait time |
| **Circuit Breaker** | `aiagent_circuit_breaker_state` | Gauge | service | Health status |
| | `aiagent_circuit_breaker_failures_total` | Counter | service | Failure count |
| **Cache** | `aiagent_cache_hits_total` | Counter | cache_type, tier | Cache efficiency |
| | `aiagent_cache_misses_total` | Counter | cache_type | Cache misses |
| **MCP** | `aiagent_mcp_requests_total` | Counter | server, tool, status | Tool usage |
| | `aiagent_mcp_latency_seconds` | Histogram | server, tool | Tool latency |
| **Integrations** | `aiagent_servicenow_requests_total` | Counter | operation, status | SNOW API |
| | `aiagent_github_actions_runs_total` | Counter | workflow, status | GHA runs |
| **Errors** | `aiagent_errors_total` | Counter | component, error_type | Error tracking |
| **Confidence** | `aiagent_confidence_rejections_total` | Counter | threshold_type | Low-confidence |

### Label Strategy

Labels provide dimensions for filtering and aggregation. Guidelines:

1. **Low Cardinality**: Labels should have limited unique values (<100)
2. **Meaningful Grouping**: Enable useful aggregations
3. **Consistent Naming**: Use snake_case

**Good Labels:**
- `status`: "success", "error"
- `severity`: "P1", "P2", "P3", "P4"
- `model`: "gpt-4-turbo-preview", "gpt-3.5-turbo"

**Bad Labels (High Cardinality):**
- `incident_id`: Unlimited unique values
- `user_email`: PII and high cardinality
- `timestamp`: Always unique

### Pull vs Push Model

The platform uses **Prometheus pull model**:

```yaml
# monitoring/prometheus.yml
scrape_configs:
  - job_name: 'orchestrator'
    static_configs:
      - targets: ['orchestrator:8000']
    scrape_interval: 15s
```

Each service exposes `/metrics` endpoint that Prometheus scrapes every 15 seconds.

---

## 3. DASHBOARDING (GRAFANA)

### Dashboard Structure

The main dashboard (`ai_agent_dashboard.json`) contains 9 panels organized by function:

| Panel | Type | Query | Purpose |
|-------|------|-------|---------|
| Requests/sec | Stat | `rate(aiagent_requests_total[5m])` | API throughput |
| Active Incidents | Stat | `aiagent_incidents_active` | Current load |
| LLM Cost ($) | Stat | `aiagent_llm_cost_dollars_total` | Cost tracking |
| Pending Approvals | Stat | `aiagent_approvals_pending` | Backlog |
| LLM Latency | Timeseries | `histogram_quantile(0.95, rate(aiagent_llm_latency_seconds_bucket[5m]))` | P95 latency |
| Token Usage | Timeseries | `sum by (model) (rate(aiagent_llm_tokens_total[5m]))` | Token consumption |
| Circuit Breaker States | Stat | `aiagent_circuit_breaker_state` | Service health |
| Errors by Component | Timeseries | `sum by (component) (rate(aiagent_errors_total[5m]))` | Error rate |
| RAG Query Latency | Timeseries | `histogram_quantile(0.95, rate(aiagent_rag_latency_seconds_bucket[5m]))` | Search performance |

### Example Alerts

| Alert | Severity | Condition | Action |
|-------|----------|-----------|--------|
| `LLMHighLatency` | Warning | P95 > 10s for 5m | Investigate OpenAI status |
| `KafkaConsumerLag` | Warning | Lag > 1000 messages | Scale consumers |
| `HighPendingApprovals` | Warning | > 10 pending for 30m | Page on-call approver |
| `CircuitBreakerOpen` | Critical | State = 2 for 1m | Investigate service |
| `BudgetDepleted` | Critical | < $100 remaining | Add budget |

### On-Call Engineer Workflow

1. **Start of Shift**: Check "Active Incidents" and "Pending Approvals"
2. **During Shift**: Monitor "Errors by Component" panel
3. **Alert Response**: Drill down from alert → dashboard → logs
4. **End of Shift**: Review "Resolution Time" trends

---

## 4. DISTRIBUTED TRACING

### Trace ID Creation

OpenTelemetry trace IDs are created at the system boundary:

```python
# From backend/utils/otel_tracing.py

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider

def setup_tracing(service_name: str = "ai-agent-platform"):
    provider = TracerProvider()
    trace.set_tracer_provider(provider)

    # Add exporters (OTLP, Jaeger, etc.)
    processor = BatchSpanProcessor(OTLPSpanExporter())
    provider.add_span_processor(processor)
```

### Span Propagation

Traces propagate across service boundaries via HTTP headers:

```python
# Inject trace context into outgoing requests
def inject_trace_headers(headers: Dict[str, str]) -> Dict[str, str]:
    from opentelemetry.propagate import inject
    inject(headers)  # Adds traceparent, tracestate headers
    return headers

# Usage
headers = inject_trace_headers({"Authorization": "Bearer ..."})
response = await httpx.post(url, headers=headers, json=payload)
```

### Critical Spans

| Span Name | Parent | Attributes | Duration |
|-----------|--------|------------|----------|
| `incident_ingestion` | Root | `source`, `incident_id` | 100-500ms |
| `rag_search` | `incident_ingestion` | `collection`, `result_count` | 50-500ms |
| `llm_analysis` | `incident_ingestion` | `model`, `tokens` | 2-30s |
| `script_matching` | `incident_ingestion` | `match_count`, `final_score` | 1-5s |
| `approval_wait` | `incident_ingestion` | `risk_level` | Variable |
| `remediation_execute` | `incident_ingestion` | `script_type`, `mode` | 10s-10min |
| `servicenow_update` | `remediation_execute` | `operation` | 500ms-5s |

### Debugging with Traces

1. **Find trace**: Search by `incident_id` or `trace_id`
2. **View waterfall**: See all spans in order
3. **Identify bottleneck**: Find longest span
4. **Drill down**: Examine span attributes and events

---

## 5. LANGFUSE (LLM OBSERVABILITY)

### Why Langfuse is Needed

Standard observability tools (Prometheus, Jaeger) don't capture:
- **Full prompts and responses** for debugging AI quality
- **Token-level costs** per operation
- **Model parameter tracking** (temperature, etc.)
- **Prompt versioning** and A/B testing

### Integration Architecture

```
[LLM Call] → [Langfuse Client] → [Langfuse Cloud/Self-hosted]
     ↓
[Prometheus Metrics] (latency, tokens, errors)
```

### Trace Structure in Langfuse

```
Trace: incident_INC-001234
├── Generation: analyze_incident
│   ├── Model: gpt-4-turbo-preview
│   ├── Input: [system prompt, incident JSON]
│   ├── Output: [root cause analysis JSON]
│   ├── Tokens: input=1250, output=850
│   └── Duration: 3.2s
├── Generation: match_scripts
│   ├── Model: gpt-4-turbo-preview
│   ├── Input: [incident, available scripts]
│   └── Output: [ranked matches]
└── Generation: generate_plan
    ├── Model: gpt-4-turbo-preview
    └── Output: [execution plan]
```

### Cost Attribution

```python
# From backend/utils/cost_tracker.py

MODEL_PRICING = {
    "gpt-4-turbo-preview": {"input": 0.01, "output": 0.03},  # per 1K tokens
    "gpt-4": {"input": 0.03, "output": 0.06},
    "gpt-3.5-turbo": {"input": 0.0005, "output": 0.0015},
}

def track_cost(model: str, input_tokens: int, output_tokens: int):
    pricing = MODEL_PRICING.get(model, MODEL_PRICING["gpt-4"])
    cost = (input_tokens / 1000) * pricing["input"] + \
           (output_tokens / 1000) * pricing["output"]

    LLM_COST_TOTAL.labels(model=model, purpose=purpose).inc(cost)
```

### Judge vs Primary LLM

The platform uses a separate LLM Judge to validate plans:

| Component | Model | Purpose | Traces |
|-----------|-------|---------|--------|
| Primary LLM | gpt-4-turbo-preview | Analysis, matching, planning | `analyze_incident`, `match_scripts` |
| LLM Judge | gpt-4 | Plan validation, safety check | `judge_evaluation` |

Both are tracked separately in Langfuse with distinct tags.

---

## 6. CORRELATION STRATEGY

### End-to-End Correlation

All observability signals are linked by `incident_id`:

```
Kafka Message (incident_id in headers)
    → Log: "processing_incident", incident_id=X
    → Metric: INCIDENTS_PROCESSED{source="...", ...}.inc()
    → Trace: incident_X (Langfuse)
    → Audit: AUD-{timestamp} with resource=X
```

### Cross-System Linking

| System | Identifier | Links To |
|--------|------------|----------|
| Kafka | Message header `incident_id` | Log correlation |
| Prometheus | Label (where cardinality allows) | Dashboard filtering |
| Langfuse | `trace_id` = `incident_id` | LLM debugging |
| Audit Log | `resource` = `incident_id` | Compliance queries |
| OTEL | `trace_id` in baggage | Distributed tracing |

### Query Examples

**Find all logs for an incident:**
```
event:* incident_id="INC-001234"
```

**Find LLM calls for an incident (Langfuse):**
```
Search: trace name = "incident_INC-001234"
```

**Find metrics for incident source:**
```promql
sum(rate(aiagent_incidents_processed_total{source="servicenow"}[5m]))
```

---

## 7. FAILURE & RESILIENCE OBSERVABILITY

### Circuit Breaker Monitoring

```python
# Circuit breaker states
# 0 = CLOSED (healthy)
# 1 = HALF_OPEN (testing)
# 2 = OPEN (failing)

CIRCUIT_BREAKER_STATE = Gauge(
    'aiagent_circuit_breaker_state',
    'Circuit breaker state',
    ['service']  # openai_api, servicenow, neo4j, weaviate
)
```

**Alert Configuration:**
```yaml
- alert: CircuitBreakerOpen
  expr: circuit_breaker_state == 2
  for: 1m
  labels:
    severity: critical
  annotations:
    summary: "Circuit breaker OPEN for {{ $labels.service }}"
```

### Retry Counters

Each retryable operation tracks attempts:

```python
# After retry exhaustion
ERRORS.labels(component="openai", error_type="max_retries_exceeded").inc()
logger.error("retry_exhausted", service="openai", attempts=3)
```

### Dead Letter Queue (DLQ) Metrics

Failed Kafka messages are tracked:

```python
DLQ_MESSAGES = Counter(
    'aiagent_dlq_messages_total',
    'Messages sent to DLQ',
    ['topic', 'reason']
)
```

### External Dependency Health

| Dependency | Health Check | Metric | Alert |
|------------|--------------|--------|-------|
| OpenAI API | API call success | `circuit_breaker_state{service="openai_api"}` | CircuitBreakerOpen |
| ServiceNow | `/health` endpoint | `servicenow_requests_total{status="success"}` | ServiceNowAuthFailure |
| GitHub | API rate limit check | `github_actions_runs_total` | GitHubActionsHighFailureRate |
| Weaviate | Connection test | `rag_queries_total{status="error"}` | WeaviateConnectionFailed |
| Neo4j | Bolt connection | `graph_queries_total{status="error"}` | Neo4jConnectionFailed |
| Redis | PING command | `cache_hits_total` (presence check) | RedisConnectionFailed |

---

## A. OBSERVABILITY SIGNAL MAP

| Signal | Purpose | Tool | File(s) |
|--------|---------|------|---------|
| `incident_processed` log | Audit trail | structlog | `streaming/incident_consumer.py` |
| `aiagent_incidents_processed_total` | Volume tracking | Prometheus | `orchestrator/metrics.py` |
| `aiagent_llm_latency_seconds` | LLM performance | Prometheus | `orchestrator/metrics.py` |
| Langfuse traces | LLM debugging | Langfuse | `orchestrator/llm_intelligence.py` |
| OTEL spans | Distributed tracing | Jaeger/Tempo | `utils/otel_tracing.py` |
| Audit events | Compliance | Custom | `governance/audit_logger.py` |
| `aiagent_circuit_breaker_state` | Resilience | Prometheus | `orchestrator/metrics.py` |
| Alert rules | Anomaly detection | Alertmanager | `monitoring/alerts/ai_agent_alerts.yml` |
| Dashboard panels | Visualization | Grafana | `monitoring/grafana/dashboards/` |

---

## B. DEBUGGING PLAYBOOKS

### Playbook 1: "Incident Stuck in Approval"

**Symptoms:**
- High `aiagent_approvals_pending` gauge
- `HighPendingApprovals` alert firing
- Incidents not progressing

**Investigation Steps:**

1. **Check approval backlog:**
   ```promql
   aiagent_approvals_pending
   ```

2. **Find stuck incidents:**
   ```
   Log query: event:"approval_requested" AND NOT event:"approval_received"
   ```

3. **Check Slack delivery:**
   ```promql
   rate(aiagent_errors_total{component="slack"}[5m])
   ```

4. **Resolution:**
   - If Slack is down: Check integration status
   - If approvers unavailable: Escalate or auto-approve low-risk
   - If system issue: Check workflow node metrics

---

### Playbook 2: "LLM Latency Spike"

**Symptoms:**
- `LLMHighLatency` alert firing
- P95 latency > 10 seconds
- Users reporting slow responses

**Investigation Steps:**

1. **Check latency by model:**
   ```promql
   histogram_quantile(0.95, rate(aiagent_llm_latency_seconds_bucket[5m])) by (model)
   ```

2. **Check OpenAI status:**
   - Visit status.openai.com
   - Check circuit breaker state:
     ```promql
     aiagent_circuit_breaker_state{service="openai_api"}
     ```

3. **Check token size:**
   ```promql
   rate(aiagent_llm_tokens_total[5m]) by (model, type)
   ```

4. **Resolution:**
   - If OpenAI degraded: Wait or switch to backup model
   - If prompts too large: Review prompt engineering
   - If rate limited: Implement throttling

---

### Playbook 3: "Wrong Script Executed"

**Symptoms:**
- Remediation failed or caused issues
- Audit shows unexpected script execution

**Investigation Steps:**

1. **Find execution in audit log:**
   ```
   event_type:"remediation_execution" resource:"INC-XXXXX"
   ```

2. **Check matching scores:**
   ```promql
   aiagent_script_match_score{score_type="final"}
   ```

3. **Review Langfuse trace:**
   - Find trace for incident
   - Review `match_scripts` generation
   - Check input (incident) and output (matches)

4. **Check confidence threshold:**
   ```promql
   rate(aiagent_confidence_rejections_total[1h])
   ```

5. **Resolution:**
   - Adjust confidence threshold if too low
   - Add negative examples to training data
   - Review script metadata for better matching

---

### Playbook 4: "Kafka Consumer Lag"

**Symptoms:**
- Messages backing up in Kafka
- Incidents taking long to appear
- `KafkaConsumerLag` alert

**Investigation Steps:**

1. **Check consumer lag (via Kafka UI):**
   - Group: `ai-agent-consumer`
   - Topics: `gcp.alerts`, `servicenow.incidents`

2. **Check consumer health:**
   ```promql
   up{job="kafka_consumer"}
   ```

3. **Check processing errors:**
   ```promql
   rate(aiagent_errors_total{component="kafka_consumer"}[5m])
   ```

4. **Check DLQ:**
   ```promql
   rate(aiagent_dlq_messages_total[5m])
   ```

5. **Resolution:**
   - If consumer crashed: Restart consumer
   - If processing slow: Scale consumers
   - If messages malformed: Check DLQ and fix producer

---

## C. SRE VIEW VS DEVELOPER VIEW

### SRE Dashboard Focus

| Metric | Why SRE Cares |
|--------|---------------|
| `aiagent_incidents_active` | Current system load |
| `aiagent_approvals_pending` | Operational backlog |
| `aiagent_circuit_breaker_state` | Service health |
| `aiagent_errors_total` | Error rate for SLO |
| `aiagent_request_latency_seconds` | Latency SLO |
| Alert status | Immediate issues |

### Developer Dashboard Focus

| Metric | Why Developer Cares |
|--------|---------------------|
| `aiagent_llm_latency_seconds` | LLM performance |
| `aiagent_script_match_score` | Matching algorithm quality |
| `aiagent_confidence_distribution` | AI confidence patterns |
| Langfuse traces | Prompt/response debugging |
| `aiagent_rag_results_count` | Knowledge retrieval quality |

### Role-Based Alert Routing

| Alert | Primary | Secondary |
|-------|---------|-----------|
| `ServiceDown` | SRE On-Call | Platform Team |
| `LLMHighLatency` | AI/ML Team | SRE |
| `HighPendingApprovals` | Operations | SRE |
| `BudgetDepleted` | Engineering Manager | Finance |
| `SecurityAlert` | Security Team | SRE |

---

## D. WHY THIS OBSERVABILITY DESIGN WORKS

### 1. Debuggability

Every incident can be traced from ingestion to resolution:
- **What happened**: Structured logs capture every event
- **When it happened**: Timestamps across all signals
- **Why it happened**: Langfuse captures AI reasoning
- **Who approved**: Audit logs track human oversight

### 2. Auditability (Compliance)

EU AI Act Article 12 requirements met:
- All AI decisions logged with explanations
- Human oversight tracked and timestamped
- Checksums prevent tampering
- 7-year retention for audit logs

### 3. Cost Control

- Real-time cost tracking per model
- Budget alerts before overspend
- Token consumption visible
- Cost attribution by operation type

### 4. Safety

- Guardrails block unsafe content
- PII detection prevents data leaks
- Circuit breakers prevent cascading failures
- High-risk actions require approval

---

## E. WHAT WOULD BREAK WITHOUT OBSERVABILITY

### Without Logs

- **Lost**: Event audit trail
- **Impact**: Cannot investigate incidents after the fact
- **Compliance**: EU AI Act violation (no record-keeping)
- **Debugging**: "What happened?" becomes impossible

### Without Metrics

- **Lost**: Quantitative health data
- **Impact**: No SLO tracking, no alerting
- **Capacity**: Cannot plan for scale
- **Performance**: Cannot identify bottlenecks

### Without Traces

- **Lost**: Request flow visibility
- **Impact**: Cannot debug distributed transactions
- **Latency**: Cannot identify which component is slow
- **Dependencies**: Cannot understand service interactions

### Without Langfuse

- **Lost**: AI decision reasoning
- **Impact**: Cannot debug AI quality issues
- **Costs**: Cannot track LLM spending
- **Prompts**: Cannot version or test prompts
- **Compliance**: Cannot explain AI decisions

---

## Conclusion

The AI Agent Platform v5.0 implements a comprehensive observability stack that provides:

- **50+ Prometheus metrics** covering all system components
- **Structured logging** with correlation IDs for traceability
- **Distributed tracing** via OpenTelemetry for request flows
- **LLM observability** via Langfuse for AI debugging
- **40+ alert rules** across 8 categories for proactive monitoring
- **EU AI Act compliant** audit logging with integrity checks

This architecture enables SREs to maintain system health, developers to debug issues, and compliance teams to audit AI decisions. Every incident's journey through the system is observable, traceable, and explainable.

---

**Document Maintainer:** Platform Engineering Team
**Review Cycle:** Quarterly
**Last Audit:** January 2026

---

## Appendix: File Reference

| File Path | Purpose |
|-----------|---------|
| `backend/orchestrator/metrics.py` | Prometheus metric definitions |
| `backend/orchestrator/llm_intelligence.py` | LLM calls with Langfuse tracing |
| `backend/utils/otel_tracing.py` | OpenTelemetry setup |
| `backend/governance/audit_logger.py` | Compliance audit logging |
| `backend/guardrails/llm_guardrails.py` | PII detection and safety |
| `backend/utils/cost_tracker.py` | LLM cost tracking |
| `backend/agents/base_agent.py` | Agent-level Langfuse integration |
| `monitoring/prometheus.yml` | Prometheus scrape configuration |
| `monitoring/alerts/ai_agent_alerts.yml` | Alert rule definitions |
| `monitoring/grafana/dashboards/ai_agent_dashboard.json` | Grafana dashboard |
