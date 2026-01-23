"""
LangGraph Workflow v6.0 - Event-Driven Architecture

This module provides the production workflow with FULL Kafka event integration:

ARCHITECTURE PRINCIPLES:
- Kafka is the system of record - all state transitions publish events
- FastAPI is control plane only - reads from Redis/Postgres, not Kafka
- MCP servers are edge adapters - poll external systems, publish events
- LangGraph owns execution and reasoning
- Human approvals flow through Kafka events

EVENT FLOW:
    MCPs sense → incident.created
    Orchestrator routes → triggers LangGraph
    LangGraph processes:
        → incident.received (ingest complete)
        → incident.enriched (classify complete)
        → incident.plan_generated (planning complete)
        → incident.requires_approval (needs human)
        → incident.approved / incident.rejected (from UI)
        → remediation.started (execution begins)
        → remediation.executed (execution complete)
        → incident.verified (verification complete)
        → incident.close_execute (command to MCP)
        → incident.closed (workflow complete)

States: NEW → RECEIVED → ENRICHED → JUDGED → PLAN_GENERATED →
        PENDING_APPROVAL → APPROVED → EXECUTING → EXECUTED →
        VERIFIED → CLOSED
"""
import os
import json
import asyncio
import uuid
from typing import TypedDict, Optional, List, Dict, Any, Literal
from datetime import datetime
from enum import Enum
import structlog

# LangGraph imports
try:
    from langgraph.graph import StateGraph, END
    from langgraph.checkpoint.memory import MemorySaver
    LANGGRAPH_AVAILABLE = True
except ImportError:
    LANGGRAPH_AVAILABLE = False
    StateGraph = None
    END = "END"
    MemorySaver = None

# Internal imports
from backend.control_plane import ApprovalRoute, ApprovalDecision
from backend.control_plane.policy_engine import PolicyEngine as ControlPlane
from backend.rag import hybrid_search_engine, graph_scorer, feedback_optimizer
from backend.rag.swarm_retriever import SwarmRetriever
from backend.orchestrator.llm_judge import LLMJudge
from backend.orchestrator.llm_intelligence import LLMIntelligence
from backend.utils.github_actions import GitHubActionsClient
from backend.utils.slack_notifier import SlackNotifier
from backend.governance.audit_logger import audit_logger, AuditEventType, RiskLevel
from backend.streaming.kafka_producer import get_producer
from backend.streaming.schemas import Topics

logger = structlog.get_logger()


# =============================================================================
# AIRFLOW MCP HELPER - Use MCP Server instead of direct REST API calls
# =============================================================================

async def trigger_dag_via_mcp(
    dag_id: str,
    conf: Dict[str, Any] = None,
    incident_id: str = "",
    correlation_id: str = "",
    wait_for_completion: bool = True,
    timeout_seconds: int = 600
) -> bool:
    """
    Trigger a DAG run via Airflow MCP Server (not direct REST API).

    ARCHITECTURE: LangGraph → Kafka (airflow.trigger_dag) → Airflow MCP → Airflow REST API

    This decouples LangGraph from direct Airflow API calls.
    The Airflow MCP server handles the actual API call and publishes results.

    Args:
        dag_id: ID of the DAG to trigger
        conf: Configuration to pass to the DAG
        incident_id: Incident ID for correlation
        correlation_id: Correlation ID for tracking
        wait_for_completion: Whether MCP should wait for DAG completion
        timeout_seconds: Timeout for waiting

    Returns:
        True if event was published successfully
    """
    event = {
        "event_type": "airflow.trigger_dag",
        "dag_id": dag_id,
        "conf": conf or {},
        "incident_id": incident_id,
        "correlation_id": correlation_id,
        "wait_for_completion": wait_for_completion,
        "timeout_seconds": timeout_seconds,
        "timestamp": datetime.utcnow().isoformat()
    }

    try:
        success = await kafka_producer.publish_event(
            topic=Topics.AIRFLOW_TRIGGER_DAG,
            event=event,
            key=correlation_id or incident_id
        )

        logger.info(
            "airflow_trigger_via_mcp",
            dag_id=dag_id,
            incident_id=incident_id,
            success=success
        )
        return success
    except Exception as e:
        logger.error(
            "airflow_trigger_via_mcp_failed",
            dag_id=dag_id,
            error=str(e)
        )
        return False


async def retry_dag_via_mcp(
    dag_id: str,
    dag_run_id: str,
    task_id: str = None,
    incident_id: str = "",
    correlation_id: str = ""
) -> bool:
    """
    Retry a failed DAG via Airflow MCP Server.

    Used for remediation when ServiceNow reports a DAG failure.

    Args:
        dag_id: ID of the DAG
        dag_run_id: ID of the failed DAG run
        task_id: Optional specific task to retry
        incident_id: Incident ID for correlation
        correlation_id: Correlation ID for tracking

    Returns:
        True if event was published successfully
    """
    event = {
        "event_type": "airflow.retry_dag",
        "dag_id": dag_id,
        "dag_run_id": dag_run_id,
        "task_id": task_id,
        "incident_id": incident_id,
        "correlation_id": correlation_id,
        "timestamp": datetime.utcnow().isoformat()
    }

    try:
        success = await kafka_producer.publish_event(
            topic=Topics.AIRFLOW_RETRY_DAG,
            event=event,
            key=correlation_id or incident_id
        )

        logger.info(
            "airflow_retry_via_mcp",
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            incident_id=incident_id,
            success=success
        )
        return success
    except Exception as e:
        logger.error(
            "airflow_retry_via_mcp_failed",
            dag_id=dag_id,
            error=str(e)
        )
        return False


# =============================================================================
# GITHUB MCP HELPER - Use MCP Server for GitHub Actions workflow dispatch
# =============================================================================
# Used for remediation scripts from RAG (Terraform, Ansible, Shell)
# Repository: sam2881/test_01

async def trigger_workflow_via_mcp(
    workflow_id: str,
    inputs: Dict[str, Any] = None,
    incident_id: str = "",
    correlation_id: str = "",
    owner: str = "sam2881",
    repo: str = "test_01",
    ref: str = "main"
) -> bool:
    """
    Trigger a GitHub Actions workflow via GitHub MCP Server.

    ARCHITECTURE: LangGraph → Kafka (github.trigger_workflow) → GitHub MCP →
                  GitHub Actions API → Workflow execution (Terraform/Ansible/Shell)

    This is used for remediation when RAG retrieves scripts that need
    to be executed via GitHub Actions workflows.

    Args:
        workflow_id: Workflow file name (e.g., 'remediation.yml', 'terraform.yml')
        inputs: Workflow input parameters (script path, target VM, etc.)
        incident_id: Incident ID for correlation
        correlation_id: Correlation ID for tracking
        owner: Repository owner (default: sam2881)
        repo: Repository name (default: test_01)
        ref: Branch to run workflow on (default: main)

    Returns:
        True if event was published successfully
    """
    event = {
        "event_type": "github.trigger_workflow",
        "owner": owner,
        "repo": repo,
        "workflow_id": workflow_id,
        "ref": ref,
        "inputs": inputs or {},
        "incident_id": incident_id,
        "correlation_id": correlation_id,
        "timestamp": datetime.utcnow().isoformat()
    }

    try:
        success = await kafka_producer.publish_event(
            topic=Topics.GITHUB_TRIGGER_WORKFLOW,
            event=event,
            key=correlation_id or incident_id
        )

        logger.info(
            "github_workflow_trigger_via_mcp",
            workflow_id=workflow_id,
            repo=f"{owner}/{repo}",
            incident_id=incident_id,
            inputs=inputs,
            success=success
        )
        return success
    except Exception as e:
        logger.error(
            "github_workflow_trigger_via_mcp_failed",
            workflow_id=workflow_id,
            error=str(e)
        )
        return False


async def get_workflow_status_via_mcp(
    run_id: int,
    incident_id: str = "",
    correlation_id: str = "",
    owner: str = "sam2881",
    repo: str = "test_01"
) -> bool:
    """
    Get GitHub Actions workflow run status via GitHub MCP Server.

    Args:
        run_id: Workflow run ID
        incident_id: Incident ID for correlation
        correlation_id: Correlation ID for tracking
        owner: Repository owner
        repo: Repository name

    Returns:
        True if event was published successfully
    """
    event = {
        "event_type": "github.get_workflow_status",
        "owner": owner,
        "repo": repo,
        "run_id": run_id,
        "incident_id": incident_id,
        "correlation_id": correlation_id,
        "timestamp": datetime.utcnow().isoformat()
    }

    try:
        success = await kafka_producer.publish_event(
            topic=Topics.GITHUB_GET_WORKFLOW_STATUS,
            event=event,
            key=correlation_id or incident_id
        )

        logger.info(
            "github_workflow_status_via_mcp",
            run_id=run_id,
            incident_id=incident_id,
            success=success
        )
        return success
    except Exception as e:
        logger.error(
            "github_workflow_status_via_mcp_failed",
            run_id=run_id,
            error=str(e)
        )
        return False


# Initialize production clients
llm_judge = LLMJudge()
llm_intelligence = LLMIntelligence()
github_actions = GitHubActionsClient()
slack_notifier = SlackNotifier()
kafka_producer = get_producer()


# =============================================================================
# REGISTRY-BASED SCRIPT SEARCH FALLBACK
# =============================================================================
# When RAG (Weaviate/Neo4j) returns no results, search registry.json directly

def search_registry_fallback(
    query: str,
    classification: str = "",
    service: str = ""
) -> List[Dict[str, Any]]:
    """
    Fallback script search using registry.json when RAG returns no results.

    Performs keyword-based matching against script keywords and error_patterns.

    Args:
        query: Search query (usually incident description)
        classification: Incident classification (e.g., 'gcp', 'kubernetes', 'airflow')
        service: Service name if known

    Returns:
        List of matching scripts in RAG-compatible format
    """
    results = []

    # Load registry
    registry_paths = [
        "/app/registry.json",
        os.path.join(os.path.dirname(__file__), "../../registry.json"),
        os.path.join(os.path.dirname(__file__), "../data/registry.json"),
    ]

    registry = None
    for path in registry_paths:
        try:
            with open(path, 'r') as f:
                registry = json.load(f)
                break
        except FileNotFoundError:
            continue

    if not registry or "scripts" not in registry:
        logger.warning("registry_fallback_no_registry_found")
        return []

    # Normalize search terms
    query_lower = query.lower()
    classification_lower = classification.lower()

    for script in registry.get("scripts", []):
        score = 0.0

        # Check classification/service match
        script_service = script.get("service", "").lower()
        script_category = script.get("category", "").lower()

        if classification_lower and (
            classification_lower in script_service or
            classification_lower in script_category or
            classification_lower in script.get("id", "").lower()
        ):
            score += 0.4

        # Boost "start" scripts for "terminated/stopped" incidents
        script_id_lower = script.get("id", "").lower()
        if "terminated" in query_lower or "stopped" in query_lower:
            if "start" in script_id_lower:
                score += 0.5  # Strong boost for start scripts
            elif "stop" in script_id_lower or "scale" in script_id_lower:
                score -= 0.3  # Penalize stop/scale scripts

        # Check keywords
        keywords = [k.lower() for k in script.get("keywords", [])]
        for keyword in keywords:
            if keyword in query_lower:
                score += 0.2

        # Check error patterns (regex-like matching)
        error_patterns = script.get("error_patterns", [])
        for pattern in error_patterns:
            import re
            try:
                if re.search(pattern.lower(), query_lower):
                    score += 0.3
            except re.error:
                if pattern.lower() in query_lower:
                    score += 0.2

        # Check description match
        script_desc = script.get("description", "").lower()
        if any(word in script_desc for word in query_lower.split()[:10]):
            score += 0.1

        if score > 0:
            results.append({
                "id": script.get("id"),
                "content": script.get("description", ""),
                "score": min(score, 1.0),  # Cap at 1.0
                "metadata": {
                    "type": script.get("type", "script"),
                    "path": script.get("path", ""),
                    "service": script.get("service", ""),
                    "risk_level": script.get("risk_level", "medium"),
                    "workflow": script.get("workflow", "shell-execute.yml"),
                    "required_inputs": script.get("required_inputs", []),
                    "auto_approve": script.get("auto_approve", False),
                    "name": script.get("name", "")
                }
            })

    # Sort by score descending
    results.sort(key=lambda x: x["score"], reverse=True)

    logger.info(
        "registry_fallback_search",
        query_length=len(query),
        classification=classification,
        results_count=len(results),
        top_scripts=[r["id"] for r in results[:3]]
    )

    return results[:5]  # Return top 5


# =============================================================================
# AIRFLOW REMEDIATION HELPER - Execute DAG retry/trigger via Airflow MCP
# =============================================================================

async def _execute_airflow_remediation(state: Dict[str, Any], plan: Dict[str, Any]) -> Dict[str, Any]:
    """
    Execute Airflow remediation by publishing to Airflow MCP via Kafka.

    This handles different Airflow failure scenarios:
    1. DAG failed → Retry the DAG (clear + retrigger)
    2. Task failed → Retry specific task
    3. Timeout → Adjust timeout and retrigger
    4. Scheduler issue → Restart scheduler (via Ansible)

    Args:
        state: Current workflow state
        plan: Remediation plan with action details

    Returns:
        Dict with execution result
    """
    incident_id = state.get("incident_id", "")
    correlation_id = state.get("correlation_id", "")
    parsed_context = state.get("parsed_context", {})

    # Extract DAG info from incident description or plan
    dag_id = plan.get("dag_id") or parsed_context.get("dag_id") or _extract_dag_id(parsed_context)
    failed_task_id = plan.get("task_id") or parsed_context.get("failed_task_id")
    dag_run_id = plan.get("dag_run_id") or parsed_context.get("dag_run_id")

    # Determine remediation action type
    action_type = plan.get("action_type", "airflow_retry").lower()
    error_type = parsed_context.get("error_type", "").lower()

    logger.info(
        "airflow_remediation_starting",
        incident_id=incident_id,
        dag_id=dag_id,
        action_type=action_type,
        error_type=error_type
    )

    if not dag_id:
        return {
            "success": False,
            "error": "Could not determine DAG ID from incident",
            "status": "failed"
        }

    try:
        # Decide remediation strategy based on error type
        if error_type in ["timeout", "task_timeout", "execution_timeout"]:
            # Timeout issue - retry with increased timeout
            logger.info("airflow_remediation_timeout_retry", dag_id=dag_id)
            success = await trigger_dag_via_mcp(
                dag_id=dag_id,
                conf={
                    "retry_of": dag_run_id,
                    "incident_id": incident_id,
                    "timeout_override": "extended"  # Signal to DAG to use longer timeout
                },
                incident_id=incident_id,
                correlation_id=correlation_id,
                wait_for_completion=True,
                timeout_seconds=900  # Longer timeout for retry
            )

        elif error_type in ["scheduler", "scheduler_heartbeat", "scheduler_down"]:
            # Scheduler issue - trigger via GitHub Actions (Ansible)
            logger.info("airflow_remediation_scheduler_restart", dag_id=dag_id)
            return {
                "success": False,
                "action": "escalate_to_ansible",
                "workflow_name": "ansible-execute.yml",
                "script_path": "platform_services/runbooks/restart_airflow_scheduler.yml",
                "reason": "Scheduler issue requires infrastructure-level remediation",
                "status": "needs_escalation"
            }

        elif failed_task_id and dag_run_id:
            # Specific task failed - retry just that task
            logger.info(
                "airflow_remediation_task_retry",
                dag_id=dag_id,
                task_id=failed_task_id,
                dag_run_id=dag_run_id
            )
            success = await retry_dag_via_mcp(
                dag_id=dag_id,
                dag_run_id=dag_run_id,
                task_id=failed_task_id,
                incident_id=incident_id,
                correlation_id=correlation_id
            )

        else:
            # Generic DAG failure - clear and retrigger
            logger.info("airflow_remediation_dag_retry", dag_id=dag_id)
            success = await trigger_dag_via_mcp(
                dag_id=dag_id,
                conf={
                    "retry_of": dag_run_id,
                    "incident_id": incident_id,
                    "triggered_by": "ai_agent_platform"
                },
                incident_id=incident_id,
                correlation_id=correlation_id,
                wait_for_completion=True,
                timeout_seconds=600
            )

        if success:
            return {
                "success": True,
                "dag_id": dag_id,
                "dag_run_id": dag_run_id,
                "action": action_type,
                "status": "completed",
                "message": f"Successfully remediated Airflow DAG: {dag_id}"
            }
        else:
            return {
                "success": False,
                "dag_id": dag_id,
                "error": "Airflow MCP remediation failed",
                "status": "failed"
            }

    except Exception as e:
        logger.error(
            "airflow_remediation_error",
            dag_id=dag_id,
            error=str(e),
            incident_id=incident_id
        )
        return {
            "success": False,
            "dag_id": dag_id,
            "error": str(e),
            "status": "failed"
        }


def _extract_dag_id(parsed_context: Dict[str, Any]) -> Optional[str]:
    """Extract DAG ID from incident description using pattern matching."""
    import re

    description = parsed_context.get("description", "")
    short_desc = parsed_context.get("short_description", "")
    full_text = f"{short_desc} {description}".lower()

    # Pattern: "DAG 'dag_name' failed" or "dag_id=dag_name"
    patterns = [
        r"dag\s*['\"]([a-zA-Z0-9_-]+)['\"]",  # DAG 'name' or DAG "name"
        r"dag_id[=:\s]+([a-zA-Z0-9_-]+)",      # dag_id=name or dag_id: name
        r"dag\s+([a-zA-Z0-9_-]+)\s+failed",    # dag name failed
        r"\[airflow\]\s*dag\s*['\"]?([a-zA-Z0-9_-]+)",  # [Airflow] DAG name
    ]

    for pattern in patterns:
        match = re.search(pattern, full_text, re.IGNORECASE)
        if match:
            return match.group(1)

    return None


# =============================================================================
# KAFKA EVENT PUBLISHING - All state transitions go through Kafka
# =============================================================================

async def publish_workflow_event(
    topic: str,
    state: "WorkflowState",
    event_type: str,
    additional_data: Dict[str, Any] = None
) -> bool:
    """
    Publish a workflow state transition event to Kafka.

    This is the ONLY way state changes are communicated externally.
    FastAPI reads from Redis/Postgres, which is populated by consumers.
    """
    event = {
        "event_type": event_type,
        "incident_id": state.get("incident_id", ""),
        "correlation_id": state.get("correlation_id", ""),
        "status": state.get("status", ""),
        "current_step": state.get("current_step", ""),
        "timestamp": datetime.utcnow().isoformat(),
        "idempotency_key": f"{state.get('incident_id', '')}:{event_type}:{datetime.utcnow().timestamp()}"
    }

    if additional_data:
        event.update(additional_data)

    try:
        success = await kafka_producer.publish_event(
            topic=topic,
            event=event,
            key=state.get("incident_id", "")
        )

        logger.info(
            "workflow_event_published",
            topic=topic,
            event_type=event_type,
            incident_id=state.get("incident_id"),
            success=success
        )
        return success
    except Exception as e:
        logger.error(
            "workflow_event_publish_failed",
            topic=topic,
            event_type=event_type,
            error=str(e)
        )
        return False


class IncidentStatus(str, Enum):
    NEW = "new"
    RECEIVED = "received"
    ENRICHED = "enriched"
    JUDGED = "judged"
    PLAN_GENERATED = "plan_generated"
    PENDING_APPROVAL = "pending_approval"
    APPROVED = "approved"
    REJECTED = "rejected"
    EXECUTING = "executing"
    EXECUTED = "executed"
    VERIFIED = "verified"
    CLOSED = "closed"
    FAILED = "failed"


class WorkflowState(TypedDict):
    """State for the v5.0 workflow"""
    # Incident identification
    incident_id: str
    correlation_id: str
    status: str

    # Input data
    raw_incident: Dict[str, Any]
    metadata: Dict[str, Any]

    # Phase 1: Parse & Classify
    parsed_context: Dict[str, Any]
    classification: str
    severity: str
    service: str

    # Phase 2: Swarm RAG
    rag_query: str
    rag_results: List[Dict[str, Any]]
    rag_confidence: float

    # Phase 3: Plan Generation
    plan: Dict[str, Any]
    plan_generated_at: str

    # Phase 4: LLM-as-Judge
    judge_score: Optional[Dict[str, Any]]
    judge_passed: bool
    revision_count: int

    # Phase 5: Control Plane
    approval_decision: Optional[Dict[str, Any]]
    approval_route: str
    approval_reason: str
    approval_token: Optional[str]

    # Phase 6: Execution
    execution_plan_id: str
    execution_status: str
    execution_output: Dict[str, Any]
    github_run_id: Optional[int]

    # Phase 7: Verification
    fix_verified: bool
    verification_reason: str

    # Phase 8: Closure
    ticket_closed: bool
    resolution_summary: str

    # Workflow tracking
    current_step: str
    step_history: List[str]
    errors: List[str]
    started_at: str
    completed_at: Optional[str]


# =============================================================================
# WORKFLOW NODES - PRODUCTION IMPLEMENTATIONS
# =============================================================================

async def node_ingest(state: WorkflowState) -> WorkflowState:
    """Step 1: Ingest incident from Kafka event"""
    logger.info("workflow_step", step="ingest", incident_id=state.get("incident_id"))

    state["current_step"] = "ingest"
    state["step_history"] = state.get("step_history", []) + ["ingest"]
    state["status"] = IncidentStatus.RECEIVED.value
    state["started_at"] = datetime.utcnow().isoformat()

    # Audit log - incident received
    audit_logger.log(
        event_type=AuditEventType.DATA_ACCESS,
        actor="workflow-orchestrator",
        actor_type="system",
        action="ingest_incident",
        resource=state.get("incident_id", "unknown"),
        resource_type="incident",
        details={"source": state.get("metadata", {}).get("source", "unknown")}
    )

    # Publish: incident.received - Kafka knows we started processing
    await publish_workflow_event(
        topic=Topics.INCIDENT_RECEIVED,
        state=state,
        event_type="incident.received",
        additional_data={
            "source": state.get("metadata", {}).get("source", "unknown"),
            "raw_incident": state.get("raw_incident", {})
        }
    )

    return state


async def node_parse(state: WorkflowState) -> WorkflowState:
    """Step 2: Parse context from raw incident"""
    logger.info("workflow_step", step="parse", incident_id=state.get("incident_id"))

    state["current_step"] = "parse"
    state["step_history"] = state.get("step_history", []) + ["parse"]

    raw = state.get("raw_incident", {})

    # Extract key fields from ServiceNow/Jira format
    state["parsed_context"] = {
        "description": raw.get("description", raw.get("short_description", "")),
        "service": raw.get("service", raw.get("cmdb_ci", raw.get("assignment_group", ""))),
        "severity": raw.get("priority", raw.get("severity", raw.get("urgency", "3"))),
        "error_message": raw.get("error_message", raw.get("work_notes", "")),
        "affected_resources": raw.get("affected_resources", []),
        "environment": raw.get("environment", raw.get("u_environment", "production")),
        "category": raw.get("category", raw.get("subcategory", "")),
        "caller": raw.get("caller_id", raw.get("opened_by", "")),
        "sys_id": raw.get("sys_id", ""),
    }

    state["service"] = state["parsed_context"]["service"]
    state["severity"] = state["parsed_context"]["severity"]

    return state


async def node_classify(state: WorkflowState) -> WorkflowState:
    """Step 3: Classify incident type using LLM"""
    logger.info("workflow_step", step="classify", incident_id=state.get("incident_id"))

    state["current_step"] = "classify"
    state["step_history"] = state.get("step_history", []) + ["classify"]

    description = state["parsed_context"].get("description", "")
    service = state["parsed_context"].get("service", "")

    # Use LLM Intelligence for real classification
    try:
        classification_result = await llm_intelligence.classify_incident(
            description=description,
            service=service,
            metadata=state["parsed_context"]
        )
        state["classification"] = classification_result.get("category", "infrastructure")

        # Log AI decision for EU AI Act compliance
        audit_logger.log_ai_decision(
            decision=f"classified_as_{state['classification']}",
            incident_id=state.get("incident_id", ""),
            confidence=classification_result.get("confidence", 0.0),
            explanation=classification_result.get("reasoning", ""),
            risk_level=RiskLevel.LOW
        )

    except Exception as e:
        logger.error("llm_classification_failed", error=str(e))
        # Fallback to rule-based classification
        classification = "infrastructure"
        desc_lower = description.lower()
        if "airflow" in desc_lower or "dag" in desc_lower or "pipeline" in desc_lower or "etl" in desc_lower:
            classification = "airflow"
        elif "database" in desc_lower or "db" in desc_lower or "sql" in desc_lower:
            classification = "database"
        elif "network" in desc_lower or "dns" in desc_lower or "connectivity" in desc_lower:
            classification = "network"
        elif "kubernetes" in desc_lower or "pod" in desc_lower or "k8s" in desc_lower:
            classification = "kubernetes"
        elif "gcp" in desc_lower or "vm" in desc_lower or "compute" in desc_lower:
            classification = "gcp"
        elif "memory" in desc_lower or "cpu" in desc_lower or "disk" in desc_lower:
            classification = "performance"
        state["classification"] = classification

    state["status"] = IncidentStatus.ENRICHED.value

    # Publish: incident.enriched - Classification and parsing complete
    await publish_workflow_event(
        topic=Topics.INCIDENT_ENRICHED,
        state=state,
        event_type="incident.enriched",
        additional_data={
            "classification": state["classification"],
            "service": state.get("service", ""),
            "severity": state.get("severity", ""),
            "parsed_context": state.get("parsed_context", {})
        }
    )

    return state


async def node_swarm_rag(state: WorkflowState) -> WorkflowState:
    """Step 4: Search using Swarm RAG with REAL Weaviate + Neo4j"""
    logger.info("workflow_step", step="swarm_rag", incident_id=state.get("incident_id"))

    state["current_step"] = "swarm_rag"
    state["step_history"] = state.get("step_history", []) + ["swarm_rag"]

    # Build comprehensive query from context
    query_parts = [
        state['parsed_context'].get('description', ''),
        state['classification'],
        state['parsed_context'].get('error_message', ''),
        state['parsed_context'].get('service', '')
    ]
    state["rag_query"] = " ".join(filter(None, query_parts))

    try:
        # Use REAL hybrid search engine with RRF fusion
        search_results = hybrid_search_engine.search(
            query=state["rag_query"],
            query_metadata={
                "service": state.get("service", ""),
                "category": state.get("classification", ""),
                "severity": state.get("severity", "")
            },
            top_k=5
        )

        # Convert to dict format
        state["rag_results"] = []
        max_score = 0.0

        for result in search_results:
            result_dict = result.to_dict() if hasattr(result, 'to_dict') else dict(result)
            state["rag_results"].append({
                "id": result_dict.get("doc_id", result_dict.get("id", "")),
                "content": result_dict.get("content", ""),
                "score": result_dict.get("rrf_score", result_dict.get("score", 0.0)),
                "metadata": {
                    "type": result_dict.get("doc_type", result_dict.get("type", "script")),
                    "path": result_dict.get("path", ""),
                    "service": result_dict.get("service", ""),
                    "risk_level": result_dict.get("risk_level", "medium")
                }
            })
            if result_dict.get("rrf_score", result_dict.get("score", 0.0)) > max_score:
                max_score = result_dict.get("rrf_score", result_dict.get("score", 0.0))

        state["rag_confidence"] = max_score

        # Also query Neo4j for FIXED_BY relationships
        graph_results = graph_scorer.get_successful_remediations(
            service=state.get("service", ""),
            category=state.get("classification", ""),
            limit=3
        )

        # Boost results that have successful historical executions
        for rag_result in state["rag_results"]:
            for graph_result in graph_results:
                if rag_result["id"] == graph_result.get("script_id"):
                    rag_result["score"] *= (1 + graph_result.get("success_rate", 0) * 0.2)
                    rag_result["metadata"]["historical_success_count"] = graph_result.get("execution_count", 0)

        logger.info(
            "swarm_rag_completed",
            incident_id=state.get("incident_id"),
            results_count=len(state["rag_results"]),
            top_score=state["rag_confidence"]
        )

    except Exception as e:
        logger.error("swarm_rag_failed", error=str(e), incident_id=state.get("incident_id"))
        state["rag_results"] = []
        state["rag_confidence"] = 0.0
        state["errors"] = state.get("errors", []) + [f"RAG search failed: {str(e)}"]

    # FALLBACK: If RAG returns no results, search registry.json directly
    if not state.get("rag_results"):
        logger.info(
            "swarm_rag_using_registry_fallback",
            incident_id=state.get("incident_id"),
            classification=state.get("classification", "")
        )
        fallback_results = search_registry_fallback(
            query=state.get("rag_query", ""),
            classification=state.get("classification", ""),
            service=state.get("service", "")
        )
        if fallback_results:
            state["rag_results"] = fallback_results
            state["rag_confidence"] = fallback_results[0]["score"] if fallback_results else 0.0
            logger.info(
                "swarm_rag_registry_fallback_succeeded",
                incident_id=state.get("incident_id"),
                results_count=len(fallback_results),
                top_script=fallback_results[0]["id"] if fallback_results else None
            )

    return state


async def node_generate_plan(state: WorkflowState) -> WorkflowState:
    """Step 5: Generate remediation plan using LLM"""
    logger.info("workflow_step", step="generate_plan", incident_id=state.get("incident_id"))

    state["current_step"] = "generate_plan"
    state["step_history"] = state.get("step_history", []) + ["generate_plan"]

    # Track revision count for retry loop detection
    current_revision = state.get("revision_count", 0)
    if "generate_plan" in state.get("step_history", [])[:-1]:
        # This is a retry - increment revision count
        state["revision_count"] = current_revision + 1
        logger.info("plan_revision", revision_count=state["revision_count"])

    rag_results = state.get("rag_results", [])

    if not rag_results:
        state["plan"] = {
            "error": "No suitable runbooks found",
            "recommendation": "Manual investigation required"
        }
        state["plan_generated_at"] = datetime.utcnow().isoformat()
        state["status"] = IncidentStatus.PLAN_GENERATED.value
        return state

    try:
        # Use LLM Intelligence to generate a proper remediation plan
        plan_result = await llm_intelligence.generate_remediation_plan(
            incident_context=state["parsed_context"],
            rag_results=rag_results,
            classification=state.get("classification", ""),
            severity=state.get("severity", "3")
        )

        top_result = rag_results[0]

        state["plan"] = {
            "action_type": plan_result.get("action_type", top_result.get("metadata", {}).get("type", "script")),
            "script_id": top_result.get("id"),
            "script_path": top_result.get("metadata", {}).get("path", ""),
            "workflow_name": _get_workflow_name(top_result.get("metadata", {}).get("type", "shell")),
            "steps": plan_result.get("steps", [
                {"step": 1, "action": "Validate target system accessibility", "timeout": 30},
                {"step": 2, "action": f"Execute {top_result.get('id')}", "timeout": 300},
                {"step": 3, "action": "Verify fix applied successfully", "timeout": 60}
            ]),
            "rollback_plan": plan_result.get("rollback_plan", {
                "steps": [{"step": 1, "action": "Revert changes via rollback script"}],
                "script_path": top_result.get("metadata", {}).get("rollback_path", "")
            }),
            "affected_resources": state["parsed_context"].get("affected_resources", []),
            "target_service": state.get("service", ""),
            "environment": state["parsed_context"].get("environment", "production"),
            "dry_run": state.get("severity", "3") in ["1", "2"],  # Dry run for high severity
            "confidence": state.get("rag_confidence", 0.0),
            "llm_reasoning": plan_result.get("reasoning", ""),
            "estimated_duration_seconds": plan_result.get("estimated_duration", 120),
            "risk_assessment": plan_result.get("risk_assessment", "medium")
        }

        # Log AI decision
        audit_logger.log_ai_decision(
            decision="generate_remediation_plan",
            incident_id=state.get("incident_id", ""),
            confidence=state["plan"]["confidence"],
            explanation=state["plan"]["llm_reasoning"],
            risk_level=RiskLevel.MEDIUM
        )

    except Exception as e:
        logger.error("plan_generation_failed", error=str(e))
        # Fallback to basic plan from RAG results
        top_result = rag_results[0]
        state["plan"] = {
            "action_type": top_result.get("metadata", {}).get("type", "script"),
            "script_id": top_result.get("id"),
            "script_path": top_result.get("metadata", {}).get("path", ""),
            "workflow_name": _get_workflow_name(top_result.get("metadata", {}).get("type", "shell")),
            "steps": [
                {"step": 1, "action": "Validate target", "timeout": 30},
                {"step": 2, "action": f"Execute {top_result.get('id')}", "timeout": 300},
                {"step": 3, "action": "Verify fix", "timeout": 60}
            ],
            "rollback_plan": {"steps": [{"step": 1, "action": "Revert changes"}]},
            "affected_resources": state["parsed_context"].get("affected_resources", []),
            "dry_run": False,
            "confidence": state.get("rag_confidence", 0.0)
        }

    state["plan_generated_at"] = datetime.utcnow().isoformat()
    state["status"] = IncidentStatus.PLAN_GENERATED.value

    # Publish: incident.plan_generated - Plan ready for evaluation
    await publish_workflow_event(
        topic=Topics.INCIDENT_PLAN_GENERATED,
        state=state,
        event_type="incident.plan_generated",
        additional_data={
            "plan": state.get("plan", {}),
            "rag_confidence": state.get("rag_confidence", 0.0),
            "rag_results_count": len(state.get("rag_results", []))
        }
    )

    return state


def _get_workflow_name(action_type: str) -> str:
    """Map action type to GitHub Actions workflow name"""
    workflow_map = {
        "shell": "shell-execute.yml",
        "script": "shell-execute.yml",
        "ansible": "ansible-execute.yml",
        "terraform": "terraform-execute.yml",
        "kubernetes": "kubernetes-execute.yml",
        "k8s": "kubernetes-execute.yml",
        "airflow": "airflow-execute.yml",
        "airflow_retry": "airflow-retry.yml",
        "dag_trigger": "airflow-execute.yml"
    }
    return workflow_map.get(action_type.lower(), "shell-execute.yml")


async def node_judge_evaluation(state: WorkflowState) -> WorkflowState:
    """Step 5b: LLM-as-Judge evaluation of the plan - REAL GPT-4 evaluation"""
    logger.info("workflow_step", step="judge_evaluation", incident_id=state.get("incident_id"))

    state["current_step"] = "judge_evaluation"
    state["step_history"] = state.get("step_history", []) + ["judge_evaluation"]

    plan = state.get("plan", {})

    if "error" in plan:
        state["judge_score"] = {
            "quality_score": 0.0,
            "safety_passed": False,
            "feasibility_score": 0.0,
            "risk_level": "high",
            "reasoning": plan.get("error")
        }
        state["judge_passed"] = False
        state["status"] = IncidentStatus.JUDGED.value
        return state

    try:
        # Call REAL LLM Judge for evaluation
        judge_result = await llm_judge.evaluate_plan(
            plan=plan,
            incident_context=state["parsed_context"],
            rag_results=state.get("rag_results", [])
        )

        state["judge_score"] = {
            "quality_score": judge_result.get("quality_score", 0.0),
            "safety_passed": judge_result.get("safety_passed", False),
            "factual_score": judge_result.get("factual_score", 0.0),
            "feasibility_score": judge_result.get("feasibility_score", 0.0),
            "risk_level": judge_result.get("risk_level", "medium"),
            "reasoning": judge_result.get("reasoning", ""),
            "concerns": judge_result.get("concerns", []),
            "recommendations": judge_result.get("recommendations", [])
        }

        # Plan passes if safety check passes AND quality score >= 6.0
        state["judge_passed"] = (
            state["judge_score"]["safety_passed"] and
            state["judge_score"]["quality_score"] >= 6.0
        )

        # Log AI decision
        audit_logger.log_ai_decision(
            decision="judge_evaluation",
            incident_id=state.get("incident_id", ""),
            confidence=state["judge_score"]["quality_score"] / 10.0,
            explanation=state["judge_score"]["reasoning"],
            risk_level=RiskLevel.MEDIUM if state["judge_passed"] else RiskLevel.HIGH
        )

        logger.info(
            "judge_evaluation_completed",
            incident_id=state.get("incident_id"),
            quality_score=state["judge_score"]["quality_score"],
            safety_passed=state["judge_score"]["safety_passed"],
            passed=state["judge_passed"]
        )

    except Exception as e:
        logger.error("judge_evaluation_failed", error=str(e))
        state["judge_score"] = {"error": str(e)}
        state["judge_passed"] = False
        state["errors"] = state.get("errors", []) + [f"Judge evaluation failed: {str(e)}"]

    state["status"] = IncidentStatus.JUDGED.value
    state["revision_count"] = state.get("revision_count", 0)

    return state


async def node_control_plane(state: WorkflowState) -> WorkflowState:
    """Step 6: Control Plane routing decision"""
    logger.info("workflow_step", step="control_plane", incident_id=state.get("incident_id"))

    state["current_step"] = "control_plane"
    state["step_history"] = state.get("step_history", []) + ["control_plane"]

    if not state.get("judge_passed"):
        state["approval_route"] = "rejected"
        state["approval_reason"] = "Failed LLM-as-Judge evaluation"
        state["status"] = IncidentStatus.REJECTED.value
        return state

    plan = state.get("plan", {})
    judge_score = state.get("judge_score", {})

    try:
        control_plane = ControlPlane()
        decision = control_plane.evaluate(
            plan=plan,
            judge_score=judge_score.get("quality_score", 0.0),
            incident_context={
                "environment": state["parsed_context"].get("environment", "production"),
                "severity": state.get("severity", "3"),
                "service": state.get("service", "")
            }
        )

        state["approval_decision"] = {
            "route": decision.route.value,
            "risk_level": decision.risk_assessment.risk_level.value,
            "risk_score": decision.risk_assessment.risk_score,
            "conditions": decision.conditions,
            "reasoning": decision.reasoning
        }
        state["approval_route"] = decision.route.value
        state["approval_reason"] = decision.reasoning

        if decision.route == ApprovalRoute.AUTO_APPROVE:
            state["status"] = IncidentStatus.APPROVED.value
        elif decision.route == ApprovalRoute.REJECT:
            state["status"] = IncidentStatus.REJECTED.value
        else:
            state["status"] = IncidentStatus.PENDING_APPROVAL.value

    except Exception as e:
        logger.error("control_plane_failed", error=str(e))
        state["approval_route"] = "manual_approve"
        state["approval_reason"] = f"Error in control plane: {str(e)} - defaulting to manual approval"
        state["status"] = IncidentStatus.PENDING_APPROVAL.value

    return state


async def node_await_approval(state: WorkflowState) -> WorkflowState:
    """
    Step 7: Request approval via Kafka event - EVENT-DRIVEN ARCHITECTURE

    IMPORTANT: This node does NOT wait/poll. It:
    1. Publishes incident.requires_approval to Kafka
    2. The workflow PAUSES here (checkpoint)
    3. EventOrchestrator resumes when incident.approved arrives from Kafka
    4. FastAPI/UI publishes incident.approved when human clicks approve

    This is the KEY change for event-driven architecture:
    - OLD: Poll Redis waiting for approval
    - NEW: Publish event, pause workflow, resume from Kafka event
    """
    logger.info("workflow_step", step="await_approval", incident_id=state.get("incident_id"))

    state["current_step"] = "await_approval"
    state["step_history"] = state.get("step_history", []) + ["await_approval"]

    route = state.get("approval_route", "")

    if route == "auto_approve":
        state["status"] = IncidentStatus.APPROVED.value

        # Publish: incident.approved (auto-approved by policy)
        await publish_workflow_event(
            topic=Topics.INCIDENT_APPROVED,
            state=state,
            event_type="incident.approved",
            additional_data={
                "approval_type": "auto",
                "approved_by": "control-plane-policy",
                "approval_reason": state.get("approval_reason", "Auto-approved by policy")
            }
        )
        return state

    elif route == "rejected":
        # Publish: incident.rejected
        await publish_workflow_event(
            topic=Topics.INCIDENT_REJECTED,
            state=state,
            event_type="incident.rejected",
            additional_data={
                "rejected_by": "control-plane-policy",
                "rejection_reason": state.get("approval_reason", "Rejected by policy")
            }
        )
        return state

    # For manual_approve or async_approve - publish event and PAUSE
    # Generate approval token for tracking
    approval_token = str(uuid.uuid4())
    state["approval_token"] = approval_token
    state["status"] = IncidentStatus.PENDING_APPROVAL.value

    # Publish: incident.requires_approval - UI/Slack will consume this
    await publish_workflow_event(
        topic=Topics.INCIDENT_REQUIRES_APPROVAL,
        state=state,
        event_type="incident.requires_approval",
        additional_data={
            "approval_token": approval_token,
            "approval_route": route,
            "plan": state.get("plan", {}),
            "judge_score": state.get("judge_score", {}),
            "risk_level": state.get("approval_decision", {}).get("risk_level", "medium"),
            "timeout_seconds": int(os.getenv("APPROVAL_TIMEOUT_SECONDS", "3600")),
            "callback_topic": Topics.INCIDENT_APPROVED  # Where to send approval
        }
    )

    # Also send Slack notification (for visibility, but approval comes via Kafka)
    try:
        approval_message = slack_notifier.build_approval_message(
            incident_id=state.get("incident_id", ""),
            plan=state.get("plan", {}),
            judge_score=state.get("judge_score", {}),
            risk_level=state.get("approval_decision", {}).get("risk_level", "medium")
        )

        await slack_notifier.send_approval_request(
            channel=os.getenv("SLACK_APPROVAL_CHANNEL", "#incident-approvals"),
            message=approval_message,
            incident_id=state.get("incident_id", ""),
            callback_url=f"{os.getenv('API_BASE_URL', 'http://localhost:8000')}/api/v1/incidents/{state.get('incident_id')}/approve"
        )
    except Exception as e:
        logger.warning("slack_notification_failed", error=str(e))
        # Don't fail - Slack is just a notification channel

    # Log human oversight requirement
    audit_logger.log(
        event_type=AuditEventType.HUMAN_REVIEW,
        actor="workflow-orchestrator",
        actor_type="system",
        action="request_approval",
        resource=state.get("incident_id", ""),
        resource_type="incident",
        risk_level=RiskLevel.MEDIUM,
        details={
            "approval_route": route,
            "approval_token": approval_token,
            "event_driven": True
        },
        human_oversight=True
    )

    logger.info(
        "approval_requested_event_driven",
        incident_id=state.get("incident_id"),
        approval_token=approval_token,
        note="Workflow paused. Resume when incident.approved arrives via Kafka."
    )

    # IMPORTANT: Workflow pauses here via LangGraph checkpoint
    # EventOrchestrator will resume with approval decision
    return state


async def node_process_approval_decision(state: WorkflowState) -> WorkflowState:
    """
    Step 7b: Process approval decision received from Kafka event

    This node is called when the workflow resumes after incident.approved
    or incident.rejected is received by EventOrchestrator.

    The approval decision is injected into state before resuming.
    """
    logger.info("workflow_step", step="process_approval_decision", incident_id=state.get("incident_id"))

    state["current_step"] = "process_approval_decision"
    state["step_history"] = state.get("step_history", []) + ["process_approval_decision"]

    # Check the approval decision (injected by EventOrchestrator)
    approval_decision = state.get("approval_decision", {})
    approved = approval_decision.get("approved", False)
    approver = approval_decision.get("approver", "unknown")
    reason = approval_decision.get("reason", "")

    if approved:
        state["status"] = IncidentStatus.APPROVED.value
        audit_logger.log_human_oversight(
            user=approver,
            action="approve_remediation",
            incident_id=state.get("incident_id", ""),
            ai_recommendation="execute_plan",
            user_decision="approved"
        )
        logger.info("approval_received", incident_id=state.get("incident_id"), approver=approver)
    else:
        state["status"] = IncidentStatus.REJECTED.value
        state["approval_reason"] = reason or "Rejected by human reviewer"
        audit_logger.log_human_oversight(
            user=approver,
            action="reject_remediation",
            incident_id=state.get("incident_id", ""),
            ai_recommendation="execute_plan",
            user_decision="rejected"
        )
        logger.info("rejection_received", incident_id=state.get("incident_id"), approver=approver, reason=reason)

    return state


async def node_execute(state: WorkflowState) -> WorkflowState:
    """Step 8: Execute remediation - via Airflow MCP or GitHub Actions"""
    logger.info("workflow_step", step="execute", incident_id=state.get("incident_id"))

    state["current_step"] = "execute"
    state["step_history"] = state.get("step_history", []) + ["execute"]

    if state["status"] != IncidentStatus.APPROVED.value:
        state["execution_status"] = "blocked"
        state["execution_output"] = {"reason": "Not approved for execution"}
        return state

    state["status"] = IncidentStatus.EXECUTING.value
    plan = state.get("plan", {})
    classification = state.get("classification", "")

    # Publish: remediation.started - Execution begins
    await publish_workflow_event(
        topic=Topics.REMEDIATION_STARTED,
        state=state,
        event_type="remediation.started",
        additional_data={
            "plan": plan,
            "script_id": plan.get("script_id", ""),
            "workflow_name": plan.get("workflow_name", ""),
            "target_service": plan.get("target_service", "")
        }
    )

    try:
        # Check if this is an Airflow incident - use Airflow MCP directly
        action_type = plan.get("action_type", "").lower()
        is_airflow_incident = (
            classification == "airflow" or
            action_type in ["airflow", "airflow_retry", "dag_trigger"] or
            "dag" in plan.get("script_id", "").lower()
        )

        if is_airflow_incident:
            # Execute via Airflow MCP Server (Kafka event-driven)
            execution_result = await _execute_airflow_remediation(state, plan)
            state["execution_status"] = execution_result.get("status", "unknown")
            state["execution_output"] = execution_result

            if execution_result.get("success"):
                state["status"] = IncidentStatus.EXECUTED.value
                logger.info(
                    "airflow_remediation_success",
                    incident_id=state.get("incident_id"),
                    dag_id=execution_result.get("dag_id"),
                    dag_run_id=execution_result.get("dag_run_id")
                )
            else:
                state["status"] = IncidentStatus.FAILED.value
                state["errors"] = state.get("errors", []) + [
                    f"Airflow remediation failed: {execution_result.get('error', 'Unknown error')}"
                ]

            return state

        # Non-Airflow: Trigger REAL GitHub Actions workflow
        workflow_name = plan.get("workflow_name", "shell-execute.yml")
        script_path = plan.get("script_path", "")

        # Prepare workflow inputs
        workflow_inputs = {
            "incident_id": state.get("incident_id", ""),
            "script_path": script_path,
            "target_service": plan.get("target_service", state.get("service", "")),
            "environment": plan.get("environment", "production"),
            "dry_run": str(plan.get("dry_run", False)).lower(),
            "correlation_id": state.get("correlation_id", ""),
            "timeout_minutes": str(plan.get("estimated_duration_seconds", 300) // 60 + 5)
        }

        # Dispatch workflow
        run_result = await github_actions.trigger_workflow(
            workflow_name=workflow_name,
            inputs=workflow_inputs,
            ref=os.getenv("GITHUB_DEFAULT_BRANCH", "main")
        )

        state["github_run_id"] = run_result.get("run_id")
        state["execution_plan_id"] = str(run_result.get("run_id", ""))

        logger.info(
            "github_actions_triggered",
            incident_id=state.get("incident_id"),
            workflow=workflow_name,
            run_id=state["github_run_id"]
        )

        # Wait for workflow completion
        execution_result = await github_actions.wait_for_completion(
            run_id=state["github_run_id"],
            timeout_seconds=plan.get("estimated_duration_seconds", 300) + 120  # Add buffer
        )

        state["execution_status"] = execution_result.get("status", "unknown")
        state["execution_output"] = {
            "status": execution_result.get("status"),
            "conclusion": execution_result.get("conclusion"),
            "exit_code": 0 if execution_result.get("conclusion") == "success" else 1,
            "output": execution_result.get("output", ""),
            "logs_url": execution_result.get("logs_url", ""),
            "duration_seconds": execution_result.get("duration_seconds", 0),
            "run_url": f"https://github.com/{os.getenv('GITHUB_REPOSITORY')}/actions/runs/{state['github_run_id']}"
        }

        if execution_result.get("conclusion") == "success":
            state["status"] = IncidentStatus.EXECUTED.value
        else:
            state["status"] = IncidentStatus.FAILED.value
            state["errors"] = state.get("errors", []) + [f"Execution failed: {execution_result.get('conclusion')}"]

        # Audit log execution
        audit_logger.log(
            event_type=AuditEventType.REMEDIATION_EXECUTION,
            actor="github-actions",
            actor_type="system",
            action="execute_remediation",
            resource=state.get("incident_id", ""),
            resource_type="incident",
            outcome=state["execution_status"],
            risk_level=RiskLevel.MEDIUM,
            details={
                "workflow": workflow_name,
                "run_id": state["github_run_id"],
                "script_path": script_path,
                "conclusion": execution_result.get("conclusion")
            }
        )

    except Exception as e:
        logger.error("execution_failed", error=str(e), incident_id=state.get("incident_id"))
        state["execution_status"] = "failed"
        state["execution_output"] = {"error": str(e)}
        state["status"] = IncidentStatus.FAILED.value
        state["errors"] = state.get("errors", []) + [f"Execution failed: {str(e)}"]

    # Publish: remediation.executed - Execution complete (success or failure)
    await publish_workflow_event(
        topic=Topics.REMEDIATION_EXECUTED,
        state=state,
        event_type="remediation.executed",
        additional_data={
            "execution_status": state.get("execution_status", "unknown"),
            "execution_output": state.get("execution_output", {}),
            "github_run_id": state.get("github_run_id"),
            "success": state.get("status") == IncidentStatus.EXECUTED.value
        }
    )

    return state


async def node_verify(state: WorkflowState) -> WorkflowState:
    """Step 9: Verify fix was successful"""
    logger.info("workflow_step", step="verify", incident_id=state.get("incident_id"))

    state["current_step"] = "verify"
    state["step_history"] = state.get("step_history", []) + ["verify"]

    execution = state.get("execution_output", {})

    if execution.get("conclusion") == "success" or execution.get("exit_code") == 0:
        # Additional verification - could run health checks here
        state["fix_verified"] = True
        state["verification_reason"] = f"Execution completed successfully. Run ID: {state.get('github_run_id')}"
        state["status"] = IncidentStatus.VERIFIED.value
    else:
        state["fix_verified"] = False
        state["verification_reason"] = execution.get("error", f"Execution failed with conclusion: {execution.get('conclusion')}")

    # Publish: incident.verified
    await publish_workflow_event(
        topic=Topics.INCIDENT_VERIFIED,
        state=state,
        event_type="incident.verified",
        additional_data={
            "fix_verified": state.get("fix_verified", False),
            "verification_reason": state.get("verification_reason", ""),
            "github_run_id": state.get("github_run_id")
        }
    )

    return state


async def node_close_ticket(state: WorkflowState) -> WorkflowState:
    """
    Step 10: Request ticket closure via Kafka event - EVENT-DRIVEN ARCHITECTURE

    IMPORTANT: This node does NOT call ServiceNow directly. It:
    1. Publishes incident.close_execute to Kafka
    2. ServiceNow MCP server consumes this event
    3. ServiceNow MCP calls ServiceNow API to close ticket
    4. ServiceNow MCP publishes incident.closed

    This is the KEY change for event-driven architecture:
    - OLD: LangGraph calls ServiceNow API directly
    - NEW: LangGraph publishes command event, MCP executes
    """
    logger.info("workflow_step", step="close_ticket", incident_id=state.get("incident_id"))

    state["current_step"] = "close_ticket"
    state["step_history"] = state.get("step_history", []) + ["close_ticket"]

    if not state.get("fix_verified"):
        state["ticket_closed"] = False
        state["resolution_summary"] = "Fix not verified - manual review required"
        return state

    # Build resolution notes for ServiceNow
    resolution_notes = f"""Automatically resolved by AI Agent Platform.

Script: {state.get('plan', {}).get('script_id', 'N/A')}
GitHub Actions Run: {state.get('github_run_id', 'N/A')}
Execution Duration: {state.get('execution_output', {}).get('duration_seconds', 'N/A')} seconds
Confidence Score: {state.get('rag_confidence', 0) * 100:.1f}%
Judge Score: {state.get('judge_score', {}).get('quality_score', 'N/A')}/10

Workflow Steps: {' → '.join(state.get('step_history', []))}
"""

    # Get the sys_id from parsed context
    sys_id = state["parsed_context"].get("sys_id", "")
    incident_id = state.get("incident_id", "")

    # Publish: incident.close_execute - Command to ServiceNow MCP
    # ServiceNow MCP will consume this and call ServiceNow API
    # NOTE: ServiceNow MCP expects "servicenow_sys_id" key, not "sys_id"
    await publish_workflow_event(
        topic=Topics.INCIDENT_CLOSE_EXECUTE,
        state=state,
        event_type="incident.close_execute",
        additional_data={
            "servicenow_sys_id": sys_id,  # Key expected by ServiceNow MCP
            "sys_id": sys_id,  # Keep for backwards compatibility
            "resolution_notes": resolution_notes,
            "close_code": "Solved (Permanently)",
            "resolution_code": "Solved Remotely (Permanently)",
            "resolved_by": "AI Agent Platform",
            "script_id": state.get("plan", {}).get("script_id", ""),
            "github_run_id": state.get("github_run_id"),
            "execution_duration_seconds": state.get("execution_output", {}).get("duration_seconds", 0)
        }
    )

    # Mark as pending close - actual closure confirmed by ServiceNow MCP
    state["ticket_closed"] = True  # Optimistic - MCP will confirm
    state["resolution_summary"] = f"Automatically resolved by executing {state.get('plan', {}).get('script_id', 'unknown')}"
    state["status"] = IncidentStatus.CLOSED.value
    state["completed_at"] = datetime.utcnow().isoformat()

    logger.info(
        "ticket_close_requested",
        incident_id=incident_id,
        sys_id=sys_id,
        note="Close command published to Kafka. ServiceNow MCP will execute."
    )

    return state


async def node_feedback_loop(state: WorkflowState) -> WorkflowState:
    """Step 11: Update RAG with feedback - REAL Neo4j and feedback updates"""
    logger.info("workflow_step", step="feedback_loop", incident_id=state.get("incident_id"))

    state["current_step"] = "feedback_loop"
    state["step_history"] = state.get("step_history", []) + ["feedback_loop"]

    if not state.get("fix_verified"):
        return state

    try:
        script_id = state.get("plan", {}).get("script_id")
        incident_id = state.get("incident_id")
        service = state.get("service", "")

        # 1. Record FIXED_BY relationship in Neo4j
        graph_scorer.record_successful_remediation(
            incident_id=incident_id,
            script_id=script_id,
            service=service,
            resolution_time_seconds=state.get("execution_output", {}).get("duration_seconds", 0),
            confidence_score=state.get("rag_confidence", 0.0)
        )

        # 2. Update feedback optimizer for adaptive learning
        feedback_optimizer.record_feedback(
            query=state.get("rag_query", ""),
            selected_doc_id=script_id,
            relevance_score=1.0,  # Successful resolution = highly relevant
            user_feedback="auto_success",
            metadata={
                "incident_id": incident_id,
                "service": service,
                "classification": state.get("classification", ""),
                "judge_score": state.get("judge_score", {}).get("quality_score", 0.0)
            }
        )

        logger.info(
            "feedback_recorded",
            incident_id=incident_id,
            script_id=script_id,
            success=True
        )

    except Exception as e:
        logger.warning("feedback_loop_failed", error=str(e), incident_id=state.get("incident_id"))
        # Don't fail the workflow for feedback issues

    # Publish: incident.closed - Final workflow completion event
    await publish_workflow_event(
        topic=Topics.INCIDENT_CLOSED,
        state=state,
        event_type="incident.closed",
        additional_data={
            "resolution_summary": state.get("resolution_summary", ""),
            "script_id": state.get("plan", {}).get("script_id", ""),
            "total_steps": len(state.get("step_history", [])),
            "step_history": state.get("step_history", []),
            "started_at": state.get("started_at", ""),
            "completed_at": state.get("completed_at", ""),
            "rag_confidence": state.get("rag_confidence", 0.0),
            "judge_score": state.get("judge_score", {}).get("quality_score", 0.0)
        }
    )

    return state


# =============================================================================
# CONDITIONAL ROUTING
# =============================================================================

def route_after_judge(state: WorkflowState) -> str:
    """Determine next step after judge evaluation"""
    if not state.get("judge_passed"):
        revision_count = state.get("revision_count", 0)
        if revision_count < 2:
            # Will retry - revision count is incremented in generate_plan node
            return "generate_plan"  # Try again with revised plan
        return "end"  # Give up after 2 retries

    return "control_plane"


def route_after_approval(state: WorkflowState) -> str:
    """Determine next step after approval routing"""
    route = state.get("approval_route", "")

    if route == "rejected":
        return "end"
    elif route == "auto_approve":
        return "execute"
    else:
        return "await_approval"


def route_after_verification(state: WorkflowState) -> str:
    """Determine next step after verification"""
    if state.get("fix_verified"):
        return "close_ticket"
    return "end"


def route_after_await_approval(state: WorkflowState) -> str:
    """
    Determine next step after await_approval node.

    For event-driven architecture:
    - If auto_approve or rejected: go to execute or end immediately
    - If manual_approve: workflow pauses, will resume at process_approval_decision
    """
    status = state.get("status", "")

    if status == IncidentStatus.APPROVED.value:
        return "execute"
    elif status == IncidentStatus.REJECTED.value:
        return "end"
    else:
        # Pending approval - workflow will be resumed by EventOrchestrator
        # After resume, it goes to process_approval_decision
        return "process_approval_decision"


def route_after_process_approval(state: WorkflowState) -> str:
    """Determine next step after processing approval decision"""
    status = state.get("status", "")

    if status == IncidentStatus.APPROVED.value:
        return "execute"
    else:
        return "end"


# =============================================================================
# WORKFLOW BUILDER
# =============================================================================

def build_v6_workflow():
    """
    Build the v6.0 LangGraph workflow with EVENT-DRIVEN architecture.

    KEY CHANGES from v5.0:
    1. All state transitions publish Kafka events
    2. Approval node publishes incident.requires_approval and pauses
    3. Workflow resumes from Kafka event via EventOrchestrator
    4. Close ticket publishes incident.close_execute (MCP executes)
    5. Uses MemorySaver checkpointer for pause/resume
    """

    if not LANGGRAPH_AVAILABLE:
        logger.warning("langgraph_not_available")
        return None, None

    workflow = StateGraph(WorkflowState)

    # Add nodes - including new process_approval_decision for resume
    workflow.add_node("ingest", node_ingest)
    workflow.add_node("parse", node_parse)
    workflow.add_node("classify", node_classify)
    workflow.add_node("swarm_rag", node_swarm_rag)
    workflow.add_node("generate_plan", node_generate_plan)
    workflow.add_node("judge_evaluation", node_judge_evaluation)
    workflow.add_node("control_plane", node_control_plane)
    workflow.add_node("await_approval", node_await_approval)
    workflow.add_node("process_approval_decision", node_process_approval_decision)
    workflow.add_node("execute", node_execute)
    workflow.add_node("verify", node_verify)
    workflow.add_node("close_ticket", node_close_ticket)
    workflow.add_node("feedback_loop", node_feedback_loop)

    # Set entry point
    workflow.set_entry_point("ingest")

    # Linear flow for initial steps
    workflow.add_edge("ingest", "parse")
    workflow.add_edge("parse", "classify")
    workflow.add_edge("classify", "swarm_rag")
    workflow.add_edge("swarm_rag", "generate_plan")
    workflow.add_edge("generate_plan", "judge_evaluation")

    # Conditional after judge
    workflow.add_conditional_edges(
        "judge_evaluation",
        route_after_judge,
        {
            "generate_plan": "generate_plan",  # Revision loop
            "control_plane": "control_plane",
            "end": END
        }
    )

    # Conditional after control plane
    workflow.add_conditional_edges(
        "control_plane",
        route_after_approval,
        {
            "execute": "execute",
            "await_approval": "await_approval",
            "end": END
        }
    )

    # Conditional after await_approval
    # For event-driven: either auto-approved/rejected, or pauses for human
    workflow.add_conditional_edges(
        "await_approval",
        route_after_await_approval,
        {
            "execute": "execute",  # Auto-approved
            "end": END,  # Auto-rejected
            "process_approval_decision": "process_approval_decision"  # Manual - will pause
        }
    )

    # After processing approval decision from Kafka event
    workflow.add_conditional_edges(
        "process_approval_decision",
        route_after_process_approval,
        {
            "execute": "execute",
            "end": END
        }
    )

    workflow.add_edge("execute", "verify")

    # Conditional after verification
    workflow.add_conditional_edges(
        "verify",
        route_after_verification,
        {
            "close_ticket": "close_ticket",
            "end": END
        }
    )

    workflow.add_edge("close_ticket", "feedback_loop")
    workflow.add_edge("feedback_loop", END)

    # Create checkpointer for pause/resume capability
    checkpointer = None
    if MemorySaver:
        checkpointer = MemorySaver()

    # Compile with interrupt capability at await_approval node
    # This allows the workflow to pause and wait for Kafka events
    compiled = workflow.compile(
        checkpointer=checkpointer,
        interrupt_after=["await_approval"]  # Pause after await_approval for manual approvals
    )

    return compiled, checkpointer


# Legacy alias for backward compatibility
def build_v5_workflow():
    """Backward compatible wrapper - now uses v6"""
    compiled, _ = build_v6_workflow()
    return compiled


# =============================================================================
# ORCHESTRATOR CLASS
# =============================================================================

class WorkflowOrchestrator:
    """
    V6.0 Event-Driven Workflow Orchestrator

    KEY CAPABILITIES:
    1. Start new workflow from incident.created Kafka event
    2. Pause workflow at approval node (interrupt_after)
    3. Resume workflow when incident.approved arrives
    4. Thread-safe with configurable checkpointer

    USAGE:
        orchestrator = WorkflowOrchestrator()

        # Start workflow - runs until approval pause or completion
        result = await orchestrator.run(incident_id, raw_incident)

        # Resume paused workflow with approval decision
        result = await orchestrator.resume(thread_id, approval_decision)
    """

    def __init__(self):
        self.workflow, self.checkpointer = build_v6_workflow()
        self.async_nodes = [
            node_ingest, node_parse, node_classify, node_swarm_rag,
            node_generate_plan, node_judge_evaluation, node_control_plane,
            node_await_approval, node_process_approval_decision, node_execute,
            node_verify, node_close_ticket, node_feedback_loop
        ]
        # Track active workflow threads for resume
        self._active_threads: Dict[str, str] = {}  # incident_id -> thread_id

    def _create_initial_state(
        self,
        incident_id: str,
        raw_incident: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None
    ) -> WorkflowState:
        """Create initial workflow state"""
        return {
            "incident_id": incident_id,
            "correlation_id": str(uuid.uuid4()),
            "status": IncidentStatus.NEW.value,
            "raw_incident": raw_incident,
            "metadata": metadata or {},
            "parsed_context": {},
            "classification": "",
            "severity": "",
            "service": "",
            "rag_query": "",
            "rag_results": [],
            "rag_confidence": 0.0,
            "plan": {},
            "plan_generated_at": "",
            "judge_score": None,
            "judge_passed": False,
            "revision_count": 0,
            "approval_decision": None,
            "approval_route": "",
            "approval_reason": "",
            "approval_token": None,
            "execution_plan_id": "",
            "execution_status": "",
            "execution_output": {},
            "github_run_id": None,
            "fix_verified": False,
            "verification_reason": "",
            "ticket_closed": False,
            "resolution_summary": "",
            "current_step": "",
            "step_history": [],
            "errors": [],
            "started_at": "",
            "completed_at": None
        }

    async def run(
        self,
        incident_id: str,
        raw_incident: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None
    ) -> WorkflowState:
        """
        Run the workflow for a new incident.

        The workflow will run until:
        1. Completion (auto-approved or rejected by policy)
        2. Pause at await_approval node (requires human approval)
        3. Failure

        For paused workflows, call resume() when approval is received.
        """
        initial_state = self._create_initial_state(incident_id, raw_incident, metadata)
        thread_id = f"workflow-{incident_id}-{uuid.uuid4().hex[:8]}"

        logger.info(
            "workflow_started",
            incident_id=incident_id,
            correlation_id=initial_state["correlation_id"],
            thread_id=thread_id,
            event_driven=True
        )

        if self.workflow:
            # Use LangGraph with checkpointer for pause/resume
            config = {
                "configurable": {"thread_id": thread_id},
                "recursion_limit": 50
            }

            result = await self.workflow.ainvoke(initial_state, config=config)

            # Check if workflow paused (pending approval)
            if result.get("status") == IncidentStatus.PENDING_APPROVAL.value:
                self._active_threads[incident_id] = thread_id
                logger.info(
                    "workflow_paused_for_approval",
                    incident_id=incident_id,
                    thread_id=thread_id,
                    approval_token=result.get("approval_token")
                )

            return result
        else:
            # Fallback: Sequential async execution (no pause/resume)
            return await self._run_sequential(initial_state)

    async def resume(
        self,
        incident_id: str,
        approval_decision: Dict[str, Any]
    ) -> WorkflowState:
        """
        Resume a paused workflow with approval decision.

        Called by EventOrchestrator when incident.approved or
        incident.rejected event is received from Kafka.

        Args:
            incident_id: The incident ID
            approval_decision: {
                "approved": bool,
                "approver": str,
                "reason": str (optional)
            }
        """
        thread_id = self._active_threads.get(incident_id)
        if not thread_id:
            logger.error("no_active_workflow", incident_id=incident_id)
            raise ValueError(f"No active workflow found for incident {incident_id}")

        logger.info(
            "workflow_resuming",
            incident_id=incident_id,
            thread_id=thread_id,
            approved=approval_decision.get("approved", False)
        )

        if not self.workflow:
            raise RuntimeError("LangGraph not available - cannot resume workflow")

        # Get current state from checkpointer
        config = {
            "configurable": {"thread_id": thread_id},
            "recursion_limit": 50
        }

        # Inject approval decision into state and resume
        # The workflow will continue from where it paused
        state_update = {
            "approval_decision": approval_decision
        }

        result = await self.workflow.ainvoke(state_update, config=config)

        # Clean up active thread tracking
        if result.get("status") in [
            IncidentStatus.CLOSED.value,
            IncidentStatus.REJECTED.value,
            IncidentStatus.FAILED.value
        ]:
            self._active_threads.pop(incident_id, None)
            logger.info(
                "workflow_completed",
                incident_id=incident_id,
                final_status=result.get("status")
            )

        return result

    async def _run_sequential(self, initial_state: WorkflowState) -> WorkflowState:
        """Fallback sequential execution without LangGraph"""
        state = initial_state
        for node_fn in self.async_nodes:
            try:
                state = await node_fn(state)
                if state.get("status") in [
                    IncidentStatus.REJECTED.value,
                    IncidentStatus.FAILED.value,
                    IncidentStatus.PENDING_APPROVAL.value
                ]:
                    break
            except Exception as e:
                state["errors"].append(f"{node_fn.__name__}: {str(e)}")
                logger.error("node_failed", node=node_fn.__name__, error=str(e))
                break

        return state

    def get_thread_id(self, incident_id: str) -> Optional[str]:
        """Get the thread ID for an active workflow"""
        return self._active_threads.get(incident_id)

    def has_active_workflow(self, incident_id: str) -> bool:
        """Check if there's an active workflow for the incident"""
        return incident_id in self._active_threads


# Global instance
workflow_orchestrator = WorkflowOrchestrator()
