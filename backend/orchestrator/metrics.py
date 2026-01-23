"""
Prometheus Metrics for AI Agent Platform v4.0
==============================================

Comprehensive metrics tracking for:
- Request/response metrics
- Workflow execution
- Incident processing
- RAG/LLM operations
- Circuit breaker status
- Cache performance
- External service health

Author: AI Agent Platform
Version: 4.0.0
"""
from prometheus_client import Counter, Histogram, Gauge, Info, generate_latest, CONTENT_TYPE_LATEST
from functools import wraps
import time

# =============================================================================
# System Info
# =============================================================================
SYSTEM_INFO = Info('aiagent_system', 'AI Agent Platform system information')
SYSTEM_INFO.info({
    'version': '4.0.0',
    'workflow': '18-node-langgraph',
    'rag_version': '4.0.0',
    'environment': 'production'
})

# =============================================================================
# Request Metrics
# =============================================================================
REQUEST_COUNT = Counter(
    'aiagent_requests_total',
    'Total number of requests',
    ['method', 'endpoint', 'status']
)

REQUEST_LATENCY = Histogram(
    'aiagent_request_latency_seconds',
    'Request latency in seconds',
    ['method', 'endpoint'],
    buckets=[0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0]
)

# =============================================================================
# Workflow Metrics (18-Node LangGraph)
# =============================================================================
WORKFLOW_EXECUTIONS = Counter(
    'aiagent_workflow_executions_total',
    'Total workflow executions',
    ['workflow_type', 'status']
)

WORKFLOW_NODE_DURATION = Histogram(
    'aiagent_workflow_node_duration_seconds',
    'Duration of each workflow node',
    ['node_name', 'phase'],
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0]
)

WORKFLOW_CURRENT_NODE = Gauge(
    'aiagent_workflow_current_node',
    'Current node being executed',
    ['incident_id']
)

WORKFLOW_STEP_COUNT = Counter(
    'aiagent_workflow_steps_total',
    'Total workflow steps executed',
    ['node_name', 'status']
)

# =============================================================================
# Incident Metrics
# =============================================================================
INCIDENTS_PROCESSED = Counter(
    'aiagent_incidents_processed_total',
    'Total incidents processed',
    ['source', 'severity', 'status']
)

INCIDENTS_ACTIVE = Gauge(
    'aiagent_incidents_active',
    'Currently active incidents'
)

INCIDENT_RESOLUTION_TIME = Histogram(
    'aiagent_incident_resolution_seconds',
    'Time to resolve incidents',
    ['severity', 'service'],
    buckets=[60, 300, 600, 1800, 3600, 7200, 14400]  # 1min to 4hrs
)

# =============================================================================
# Remediation Metrics
# =============================================================================
REMEDIATION_EXECUTIONS = Counter(
    'aiagent_remediation_executions_total',
    'Total remediation script executions',
    ['script_type', 'mode', 'status']
)

REMEDIATION_CONFIDENCE = Histogram(
    'aiagent_remediation_confidence',
    'Remediation match confidence scores',
    ['script_type'],
    buckets=[0.5, 0.6, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 1.0]
)

REMEDIATION_DURATION = Histogram(
    'aiagent_remediation_duration_seconds',
    'Duration of remediation execution',
    ['script_type', 'mode'],
    buckets=[1, 5, 10, 30, 60, 120, 300, 600]
)

# =============================================================================
# Script Matching Metrics (Hybrid Scoring)
# =============================================================================
SCRIPT_MATCH_SCORES = Histogram(
    'aiagent_script_match_score',
    'Script matching scores',
    ['score_type'],  # vector, metadata, graph, safety, final
    buckets=[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
)

SCRIPT_MATCHES = Counter(
    'aiagent_script_matches_total',
    'Total script match attempts',
    ['result']  # success, no_match, low_confidence
)

# =============================================================================
# LLM Metrics
# =============================================================================
LLM_CALLS = Counter(
    'aiagent_llm_calls_total',
    'Total LLM API calls',
    ['model', 'purpose', 'status']
)

LLM_LATENCY = Histogram(
    'aiagent_llm_latency_seconds',
    'LLM API call latency',
    ['model', 'purpose'],
    buckets=[0.5, 1.0, 2.0, 5.0, 10.0, 30.0]
)

LLM_TOKENS = Counter(
    'aiagent_llm_tokens_total',
    'Total LLM tokens used',
    ['model', 'type']  # type: input, output
)

# =============================================================================
# RAG Metrics
# =============================================================================
RAG_QUERIES = Counter(
    'aiagent_rag_queries_total',
    'Total RAG queries',
    ['collection', 'status']
)

RAG_LATENCY = Histogram(
    'aiagent_rag_latency_seconds',
    'RAG query latency',
    ['collection'],
    buckets=[0.05, 0.1, 0.25, 0.5, 1.0, 2.0]
)

RAG_RESULTS = Histogram(
    'aiagent_rag_results_count',
    'Number of RAG results returned',
    ['collection'],
    buckets=[0, 1, 2, 3, 5, 10, 20]
)

# =============================================================================
# Graph DB Metrics (Neo4j)
# =============================================================================
GRAPH_QUERIES = Counter(
    'aiagent_graph_queries_total',
    'Total Neo4j graph queries',
    ['query_type', 'status']
)

GRAPH_LATENCY = Histogram(
    'aiagent_graph_latency_seconds',
    'Neo4j query latency',
    ['query_type'],
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0]
)

# =============================================================================
# MCP Server Metrics
# =============================================================================
MCP_REQUESTS = Counter(
    'aiagent_mcp_requests_total',
    'Total MCP server requests',
    ['server', 'tool', 'status']
)

MCP_LATENCY = Histogram(
    'aiagent_mcp_latency_seconds',
    'MCP server request latency',
    ['server', 'tool'],
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0]
)

# =============================================================================
# Approval Metrics
# =============================================================================
APPROVALS_PENDING = Gauge(
    'aiagent_approvals_pending',
    'Number of pending approvals'
)

APPROVALS_PROCESSED = Counter(
    'aiagent_approvals_processed_total',
    'Total approvals processed',
    ['action', 'risk_level']  # action: approved, rejected
)

APPROVAL_WAIT_TIME = Histogram(
    'aiagent_approval_wait_seconds',
    'Time waiting for approval',
    ['risk_level'],
    buckets=[60, 300, 600, 1800, 3600]
)

# =============================================================================
# Error Metrics
# =============================================================================
ERRORS = Counter(
    'aiagent_errors_total',
    'Total errors',
    ['component', 'error_type']
)

# =============================================================================
# ServiceNow Integration Metrics
# =============================================================================
SERVICENOW_REQUESTS = Counter(
    'aiagent_servicenow_requests_total',
    'Total ServiceNow API requests',
    ['operation', 'status']
)

SERVICENOW_LATENCY = Histogram(
    'aiagent_servicenow_latency_seconds',
    'ServiceNow API latency',
    ['operation'],
    buckets=[0.5, 1.0, 2.0, 5.0, 10.0]
)

# =============================================================================
# GitHub Actions Metrics
# =============================================================================
GITHUB_ACTIONS_RUNS = Counter(
    'aiagent_github_actions_runs_total',
    'Total GitHub Actions workflow runs',
    ['workflow', 'status']
)

GITHUB_ACTIONS_DURATION = Histogram(
    'aiagent_github_actions_duration_seconds',
    'GitHub Actions workflow duration',
    ['workflow'],
    buckets=[10, 30, 60, 120, 300, 600]
)

# =============================================================================
# Decorators for Easy Instrumentation
# =============================================================================
def track_request(endpoint: str):
    """Decorator to track request metrics"""
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


def track_workflow_node(node_name: str, phase: str):
    """Decorator to track workflow node execution"""
    def decorator(func):
        @wraps(func)
        def wrapper(state, *args, **kwargs):
            start_time = time.time()
            status = "success"
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


def track_llm_call(model: str, purpose: str):
    """Decorator to track LLM API calls"""
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
                raise
            finally:
                duration = time.time() - start_time
                LLM_CALLS.labels(model=model, purpose=purpose, status=status).inc()
                LLM_LATENCY.labels(model=model, purpose=purpose).observe(duration)
        return wrapper
    return decorator


# =============================================================================
# Helper Functions
# =============================================================================
def record_remediation_execution(script_type: str, mode: str, status: str, confidence: float, duration: float):
    """Record remediation execution metrics"""
    REMEDIATION_EXECUTIONS.labels(script_type=script_type, mode=mode, status=status).inc()
    REMEDIATION_CONFIDENCE.labels(script_type=script_type).observe(confidence)
    REMEDIATION_DURATION.labels(script_type=script_type, mode=mode).observe(duration)


def record_script_match(vector_score: float, metadata_score: float, graph_score: float,
                       safety_score: float, final_score: float, matched: bool):
    """Record script matching metrics"""
    SCRIPT_MATCH_SCORES.labels(score_type="vector").observe(vector_score)
    SCRIPT_MATCH_SCORES.labels(score_type="metadata").observe(metadata_score)
    SCRIPT_MATCH_SCORES.labels(score_type="graph").observe(graph_score)
    SCRIPT_MATCH_SCORES.labels(score_type="safety").observe(safety_score)
    SCRIPT_MATCH_SCORES.labels(score_type="final").observe(final_score)

    if matched:
        SCRIPT_MATCHES.labels(result="success").inc()
    elif final_score < 0.75:
        SCRIPT_MATCHES.labels(result="low_confidence").inc()
    else:
        SCRIPT_MATCHES.labels(result="no_match").inc()


def record_incident(source: str, severity: str, status: str):
    """Record incident processing metrics"""
    INCIDENTS_PROCESSED.labels(source=source, severity=severity, status=status).inc()


def record_rag_query(collection: str, status: str, latency: float, result_count: int):
    """Record RAG query metrics"""
    RAG_QUERIES.labels(collection=collection, status=status).inc()
    RAG_LATENCY.labels(collection=collection).observe(latency)
    RAG_RESULTS.labels(collection=collection).observe(result_count)


def record_graph_query(query_type: str, status: str, latency: float):
    """Record graph query metrics"""
    GRAPH_QUERIES.labels(query_type=query_type, status=status).inc()
    GRAPH_LATENCY.labels(query_type=query_type).observe(latency)


def record_mcp_request(server: str, tool: str, status: str, latency: float):
    """Record MCP server request metrics"""
    MCP_REQUESTS.labels(server=server, tool=tool, status=status).inc()
    MCP_LATENCY.labels(server=server, tool=tool).observe(latency)


def record_servicenow_request(operation: str, status: str, latency: float):
    """Record ServiceNow API request metrics"""
    SERVICENOW_REQUESTS.labels(operation=operation, status=status).inc()
    SERVICENOW_LATENCY.labels(operation=operation).observe(latency)


def record_github_action(workflow: str, status: str, duration: float):
    """Record GitHub Actions metrics"""
    GITHUB_ACTIONS_RUNS.labels(workflow=workflow, status=status).inc()
    GITHUB_ACTIONS_DURATION.labels(workflow=workflow).observe(duration)


# =============================================================================
# Circuit Breaker Metrics
# =============================================================================
CIRCUIT_BREAKER_STATE = Gauge(
    'aiagent_circuit_breaker_state',
    'Circuit breaker state (0=closed, 1=half_open, 2=open)',
    ['service']
)

CIRCUIT_BREAKER_FAILURES = Counter(
    'aiagent_circuit_breaker_failures_total',
    'Total circuit breaker failures',
    ['service']
)

CIRCUIT_BREAKER_REJECTIONS = Counter(
    'aiagent_circuit_breaker_rejections_total',
    'Total circuit breaker rejections (fast-fail)',
    ['service']
)

CIRCUIT_BREAKER_SUCCESSES = Counter(
    'aiagent_circuit_breaker_successes_total',
    'Total circuit breaker successes',
    ['service']
)


def record_circuit_breaker_state(service: str, state: str):
    """Record circuit breaker state change"""
    state_map = {"closed": 0, "half_open": 1, "open": 2}
    CIRCUIT_BREAKER_STATE.labels(service=service).set(state_map.get(state, 0))


def record_circuit_breaker_event(service: str, event_type: str):
    """Record circuit breaker event"""
    if event_type == "failure":
        CIRCUIT_BREAKER_FAILURES.labels(service=service).inc()
    elif event_type == "rejection":
        CIRCUIT_BREAKER_REJECTIONS.labels(service=service).inc()
    elif event_type == "success":
        CIRCUIT_BREAKER_SUCCESSES.labels(service=service).inc()


# =============================================================================
# Cache Metrics
# =============================================================================
CACHE_HITS = Counter(
    'aiagent_cache_hits_total',
    'Total cache hits',
    ['cache_type', 'tier']  # tier: memory, redis, disk
)

CACHE_MISSES = Counter(
    'aiagent_cache_misses_total',
    'Total cache misses',
    ['cache_type']
)

CACHE_SIZE = Gauge(
    'aiagent_cache_size',
    'Current cache size (items)',
    ['cache_type', 'tier']
)

CACHE_HIT_RATE = Gauge(
    'aiagent_cache_hit_rate',
    'Cache hit rate (0.0-1.0)',
    ['cache_type']
)


def record_cache_hit(cache_type: str, tier: str):
    """Record cache hit"""
    CACHE_HITS.labels(cache_type=cache_type, tier=tier).inc()


def record_cache_miss(cache_type: str):
    """Record cache miss"""
    CACHE_MISSES.labels(cache_type=cache_type).inc()


def update_cache_stats(cache_type: str, memory_size: int, hit_rate: float):
    """Update cache statistics"""
    CACHE_SIZE.labels(cache_type=cache_type, tier="memory").set(memory_size)
    CACHE_HIT_RATE.labels(cache_type=cache_type).set(hit_rate)


# =============================================================================
# Confidence Threshold Metrics
# =============================================================================
CONFIDENCE_REJECTIONS = Counter(
    'aiagent_confidence_rejections_total',
    'Total auto-rejections due to low confidence',
    ['threshold_type']
)

CONFIDENCE_DISTRIBUTION = Histogram(
    'aiagent_confidence_distribution',
    'Distribution of confidence scores',
    ['match_type'],
    buckets=[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
)


def record_confidence_rejection(threshold_type: str):
    """Record confidence-based rejection"""
    CONFIDENCE_REJECTIONS.labels(threshold_type=threshold_type).inc()


def record_confidence_score(match_type: str, score: float):
    """Record confidence score distribution"""
    CONFIDENCE_DISTRIBUTION.labels(match_type=match_type).observe(score)


# =============================================================================
# Dry Run Metrics
# =============================================================================
DRY_RUN_EXECUTIONS = Counter(
    'aiagent_dry_run_executions_total',
    'Total dry run executions',
    ['script_type']
)


def record_dry_run(script_type: str):
    """Record dry run execution"""
    DRY_RUN_EXECUTIONS.labels(script_type=script_type).inc()


def get_metrics():
    """Generate metrics in Prometheus format"""
    return generate_latest()


def get_content_type():
    """Get Prometheus content type"""
    return CONTENT_TYPE_LATEST
