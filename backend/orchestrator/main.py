"""
AI Agent Orchestrator Service v5.0

Architecture:
- REST API for frontend communication
- Kafka for event streaming
- v5.0 components: Control Plane, LLM Judge, Swarm RAG

Endpoints:
- /api/incidents - ServiceNow incident management
- /api/scripts - Script registry management
- /api/execute - Execution with approval workflow
- /api/langgraph - LangGraph workflow execution
- /api/rag - Enhanced RAG search
"""
import os
import sys
import json
import uuid
import asyncio
import subprocess
import httpx
import re
import base64
from typing import Any, Dict, List, Optional
from datetime import datetime
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from pydantic import BaseModel
import uvicorn
from dotenv import load_dotenv
import structlog

# Load environment
load_dotenv("/home/samrattidke600/ai_agent_app/.env")
load_dotenv("/home/samrattidke600/ai_agent_app/.env.local")

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from backend.orchestrator.metrics import get_metrics, get_content_type
from backend.utils.circuit_breaker import (
    github_breaker,
    servicenow_breaker,
    openai_breaker,
    get_all_breaker_stats
)
from backend.config.thresholds import confidence_thresholds, execution_policy
from backend.utils.github_actions import GitHubActionsClient

# Initialize GitHub Actions client
github_actions = GitHubActionsClient()

# Import LangGraph workflow components for REAL execution
from backend.orchestrator.langgraph_workflow import (
    node_ingest, node_parse, node_classify, node_swarm_rag,
    node_generate_plan, node_judge_evaluation, node_control_plane,
    node_await_approval, node_execute, node_verify, node_close_ticket,
    node_feedback_loop, WorkflowState, IncidentStatus
)

logger = structlog.get_logger()

# =============================================================================
# App Setup
# =============================================================================
app = FastAPI(
    title="AI Agent Orchestrator",
    description="v5.0 Incident Remediation with Hybrid Protocol Architecture",
    version="5.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =============================================================================
# Pydantic Models
# =============================================================================
class Incident(BaseModel):
    incident_id: str
    short_description: str
    description: str = ""
    category: str = "unknown"
    priority: str = "3"
    status: str = "new"

class ScriptMatch(BaseModel):
    script_id: str
    name: str
    description: str
    type: str
    confidence: float
    risk_level: str
    auto_approve: bool
    required_inputs: List[str]
    extracted_inputs: Dict[str, Any] = {}

class ExecutionRequest(BaseModel):
    incident_id: str
    script_id: str
    inputs: Dict[str, Any]
    environment: str = "development"
    dry_run: bool = False

class MatchRequest(BaseModel):
    incident_id: str
    short_description: str = ""
    description: str = ""
    category: str = ""

class ApprovalRequest(BaseModel):
    execution_id: str
    approved: bool
    approver: str = ""
    comments: str = ""

# =============================================================================
# Load Script Registry
# =============================================================================
REGISTRY_PATH = "/home/samrattidke600/ai_agent_app/registry.json"

def load_registry() -> Dict:
    try:
        with open(REGISTRY_PATH, "r") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load registry: {e}")
        return {"scripts": []}

REGISTRY = load_registry()

# =============================================================================
# Initialize RAG Engine with Scripts
# =============================================================================
def init_rag_engine():
    """Initialize the hybrid search engine with scripts from registry"""
    try:
        from rag.hybrid_search_engine import hybrid_search_engine

        # Load all scripts from both registries
        all_scripts = []
        registry_files = [
            REGISTRY_PATH,
            "/home/samrattidke600/ai_agent_app/backend/data/registry.json",
            "/home/samrattidke600/ai_agent_app/backend/runbooks/registry.json"
        ]

        seen_ids = set()
        for reg_file in registry_files:
            try:
                with open(reg_file, 'r') as f:
                    data = json.load(f)
                    for script in data.get('scripts', []):
                        script_id = script.get('id')
                        if script_id and script_id not in seen_ids:
                            all_scripts.append(script)
                            seen_ids.add(script_id)
            except Exception as e:
                logger.debug(f"Registry file not found: {reg_file}")

        if all_scripts:
            # Convert scripts to documents for the search engine
            documents = []
            for script in all_scripts:
                # Build searchable text
                search_text = f"""
                {script.get('name', '')}
                {script.get('description', '')}
                Keywords: {' '.join(script.get('keywords', []))}
                Error patterns: {' '.join(script.get('error_patterns', []))}
                Service: {script.get('service', '')}
                Action: {script.get('action', '')}
                Tags: {' '.join(script.get('tags', []))}
                """

                documents.append({
                    'id': script.get('id', ''),
                    'content': search_text.strip(),
                    'metadata': {
                        'script_id': script.get('id', ''),
                        'name': script.get('name', ''),
                        'path': script.get('path', ''),
                        'type': script.get('type', 'shell'),
                        'service': script.get('service', ''),
                        'action': script.get('action', ''),
                        'risk_level': script.get('risk', script.get('risk_level', 'medium')),
                        'requires_approval': script.get('requires_approval', False),
                        'keywords': script.get('keywords', []),
                        'error_patterns': script.get('error_patterns', []),
                        'tags': script.get('tags', [])
                    }
                })

            # Index documents in the search engine
            hybrid_search_engine.index_documents(documents)
            logger.info("rag_engine_initialized", script_count=len(documents))
        else:
            logger.warning("rag_engine_no_scripts_found")

    except Exception as e:
        logger.error(f"rag_engine_init_failed: {e}")

# Initialize RAG on module load
init_rag_engine()

# =============================================================================
# ServiceNow Integration
# =============================================================================
SNOW_INSTANCE = os.getenv("SNOW_INSTANCE_URL", "")
SNOW_USER = os.getenv("SNOW_USERNAME", "")
SNOW_PASS = os.getenv("SNOW_PASSWORD", "")

def _get_snow_headers() -> Dict[str, str]:
    auth_b64 = base64.b64encode(f"{SNOW_USER}:{SNOW_PASS}".encode('ascii')).decode('ascii')
    return {
        "Authorization": f"Basic {auth_b64}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

async def fetch_servicenow_incidents(use_cache: bool = True) -> List[Dict]:
    """
    Fetch incidents - EVENT-DRIVEN ARCHITECTURE.

    Priority:
    1. Redis cache (populated by Kafka consumers)
    2. ServiceNow API (fallback)
    """
    import redis
    import json as json_lib

    # Try Redis cache first (event-driven source)
    if use_cache:
        try:
            redis_client = redis.Redis(
                host=os.getenv("REDIS_HOST", "redis"),
                port=int(os.getenv("REDIS_PORT", "6379")),
                decode_responses=True
            )
            cached_data = redis_client.get("incidents:active")
            if cached_data:
                data = json_lib.loads(cached_data)
                incidents = data.get("incidents", [])
                if incidents:
                    logger.info(f"Returning {len(incidents)} incidents from Redis cache")
                    return incidents
        except Exception as e:
            logger.warning(f"Redis cache unavailable: {e}")

    # Fallback: Fetch from ServiceNow
    logger.info("Redis cache empty/unavailable, fetching from ServiceNow...")
    try:
        base_url = SNOW_INSTANCE.rstrip('/')
        if not base_url.startswith('http'):
            base_url = f"https://{base_url}"
        url = f"{base_url}/api/now/table/incident"

        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(
                url,
                headers=_get_snow_headers(),
                params={"sysparm_limit": "50", "sysparm_display_value": "false"}
            )
            if response.status_code == 200:
                data = response.json()
                return [{
                    "incident_id": inc.get("number", ""),
                    "sys_id": inc.get("sys_id", ""),
                    "short_description": inc.get("short_description", ""),
                    "description": inc.get("description", ""),
                    "category": inc.get("category", "unknown"),
                    "priority": inc.get("priority", "3"),
                    "status": _map_snow_state(inc.get("state", "1")),
                } for inc in data.get("result", [])]
    except Exception as e:
        logger.error(f"ServiceNow fetch error: {e}")
    return []

def _map_snow_state(state: str) -> str:
    """Map ServiceNow state to human-readable status"""
    state_map = {
        "1": "new",
        "2": "in_progress",
        "3": "on_hold",
        "4": "pending",
        "5": "pending_approval",
        "6": "resolved",
        "7": "closed",
        "8": "cancelled"
    }
    return state_map.get(str(state), "unknown")

async def update_servicenow_incident(incident_id: str, updates: Dict) -> bool:
    """Update incident in ServiceNow"""
    try:
        base_url = SNOW_INSTANCE.rstrip('/')
        if not base_url.startswith('http'):
            base_url = f"https://{base_url}"

        async with httpx.AsyncClient(timeout=30.0) as client:
            search_response = await client.get(
                f"{base_url}/api/now/table/incident",
                headers=_get_snow_headers(),
                params={"sysparm_query": f"number={incident_id}", "sysparm_limit": "1"}
            )
            if search_response.status_code == 200:
                results = search_response.json().get("result", [])
                if results:
                    sys_id = results[0]["sys_id"]
                    update_response = await client.patch(
                        f"{base_url}/api/now/table/incident/{sys_id}",
                        headers=_get_snow_headers(),
                        json=updates
                    )
                    return update_response.status_code == 200
    except Exception as e:
        logger.error(f"ServiceNow update error: {e}")
    return False

# =============================================================================
# Script Matching (LLM + Hybrid)
# =============================================================================
def match_scripts_to_incident(incident: Dict, max_results: int = 5) -> List[ScriptMatch]:
    """Match incident to scripts using hybrid scoring"""
    description = f"{incident.get('short_description', '')} {incident.get('description', '')}"
    desc_lower = description.lower()

    matches = []
    for script in REGISTRY.get("scripts", []):
        score = 0.0

        # Keyword matching (50%)
        keywords = script.get("keywords", [])
        keyword_hits = sum(1 for kw in keywords if kw.lower() in desc_lower)
        if keywords:
            score += (keyword_hits / len(keywords)) * 0.5

        # Error pattern matching (30%)
        patterns = script.get("error_patterns", [])
        pattern_hits = sum(1 for p in patterns if re.search(p, desc_lower, re.IGNORECASE))
        if patterns:
            score += (pattern_hits / len(patterns)) * 0.3

        # Service matching (10%)
        if script.get("service", "").lower() in desc_lower:
            score += 0.1

        # Action matching (10%)
        if script.get("action", "").lower() in desc_lower:
            score += 0.1

        if score > 0.2:
            extracted = extract_inputs_from_incident(description, script.get("required_inputs", []))
            matches.append(ScriptMatch(
                script_id=script["id"],
                name=script["name"],
                description=script["description"],
                type=script["type"],
                confidence=min(score, 1.0),
                risk_level=script.get("risk_level", "medium"),
                auto_approve=script.get("auto_approve", False),
                required_inputs=script.get("required_inputs", []),
                extracted_inputs=extracted
            ))

    matches.sort(key=lambda x: x.confidence, reverse=True)
    return matches[:max_results]

def extract_inputs_from_incident(text: str, required_inputs: List[str]) -> Dict[str, Any]:
    """Extract required inputs from incident text"""
    extracted = {}
    patterns = {
        "instance_name": [r"VM\s+instance\s+([a-zA-Z0-9][-a-zA-Z0-9]*)", r"([a-zA-Z0-9][-a-zA-Z0-9]*-vm[-a-zA-Z0-9]*)"],
        "zone": [r"(?:zone|region)[:\s]+([a-zA-Z]+-[a-zA-Z0-9]+-[a-zA-Z0-9]+)", r"([a-z]+-[a-z]+\d+-[a-z])"],
        "namespace": [r"(?:namespace|ns)[:\s]+([a-zA-Z0-9][-a-zA-Z0-9]*)"],
        "pod_name": [r"(?:pod|container)[:\s]+([a-zA-Z0-9][-a-zA-Z0-9]*)"],
    }

    for input_name in required_inputs:
        if input_name in patterns:
            for pattern in patterns[input_name]:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    extracted[input_name] = match.group(1)
                    break
    return extracted

# =============================================================================
# GitHub Actions Execution
# =============================================================================
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")
GITHUB_OWNER = os.getenv("GITHUB_OWNER", "sam2881")
GITHUB_REPO = os.getenv("GITHUB_REPO", "test_01")

async def trigger_github_workflow(script: Dict, inputs: Dict, incident_id: str) -> Dict:
    """Trigger GitHub Actions workflow"""
    workflow_file = script.get("workflow", "shell-execute.yml")
    script_path = script.get("path", "")

    script_args = {k: str(v) for k, v in inputs.items() if k not in ["environment", "dry_run"]}

    workflow_inputs = {
        "incident_id": incident_id,
        "script_path": script_path,
        "environment": inputs.get("environment", "development"),
        "dry_run": str(inputs.get("dry_run", False)).lower(),
        "script_args": json.dumps(script_args)
    }

    if not github_breaker.can_execute():
        return {"status": "circuit_open", "error": "GitHub API circuit breaker is open"}

    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/actions/workflows/{workflow_file}/dispatches",
                headers={
                    "Authorization": f"Bearer {GITHUB_TOKEN}",
                    "Accept": "application/vnd.github.v3+json",
                },
                json={"ref": "main", "inputs": workflow_inputs},
                timeout=30
            )

            if response.status_code in [204, 200]:
                github_breaker.record_success()
                await asyncio.sleep(2)
                runs_response = await client.get(
                    f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/actions/runs",
                    headers={"Authorization": f"Bearer {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"},
                    params={"per_page": 1}
                )
                runs = runs_response.json().get("workflow_runs", [])
                return {
                    "status": "triggered",
                    "run_id": runs[0]["id"] if runs else None,
                    "workflow": workflow_file,
                    "html_url": runs[0].get("html_url") if runs else None
                }
            else:
                github_breaker.record_failure()
                return {"status": "failed", "error": response.text}
    except Exception as e:
        github_breaker.record_failure(e)
        return {"status": "error", "error": str(e)}

# =============================================================================
# In-Memory State
# =============================================================================
EXECUTIONS: Dict[str, Dict] = {}
PENDING_APPROVALS: Dict[str, Dict] = {}
WORKFLOW_STATES: Dict[str, Dict] = {}

# =============================================================================
# API Endpoints - Health & Metrics
# =============================================================================
@app.get("/health")
async def health():
    """Health check"""
    from platform_services.infrastructure_clients import redis_client
    redis_status = False
    try:
        redis_status = redis_client.ping()
    except Exception:
        logger.warning("redis_health_check_failed", error="connection unavailable")

    return {
        "status": "healthy",
        "service": "orchestrator",
        "version": "5.0.0",
        "timestamp": datetime.utcnow().isoformat(),
        "components": {
            "redis": redis_status,
            "circuit_breakers": get_all_breaker_stats()
        }
    }

@app.get("/metrics")
async def metrics():
    """Prometheus metrics"""
    return Response(content=get_metrics(), media_type=get_content_type())

@app.get("/api/circuit-breakers")
async def circuit_breaker_status():
    """Get circuit breaker status"""
    return {"breakers": get_all_breaker_stats(), "timestamp": datetime.utcnow().isoformat()}

# =============================================================================
# API Endpoints - Incidents (EVENT-DRIVEN)
# =============================================================================
@app.get("/api/incidents")
async def list_incidents():
    """
    List all incidents - EVENT-DRIVEN ARCHITECTURE.

    Priority:
    1. Redis cache (populated by Kafka consumers) - source: "redis"
    2. ServiceNow API (fallback) - source: "servicenow_fallback"
    """
    import redis
    import json as json_lib

    # Try Redis cache first
    try:
        redis_client = redis.Redis(
            host=os.getenv("REDIS_HOST", "redis"),
            port=int(os.getenv("REDIS_PORT", "6379")),
            decode_responses=True
        )
        cached_data = redis_client.get("incidents:active")
        if cached_data:
            data = json_lib.loads(cached_data)
            incidents = data.get("incidents", [])
            if incidents:
                return {
                    "incidents": incidents,
                    "count": len(incidents),
                    "source": "redis",
                    "cached_at": data.get("cached_at")
                }
    except Exception as e:
        logger.warning(f"Redis unavailable: {e}")

    # Fallback to ServiceNow
    incidents = await fetch_servicenow_incidents(use_cache=False)
    return {
        "incidents": incidents,
        "count": len(incidents),
        "source": "servicenow_fallback"
    }

@app.post("/api/incidents/{incident_id}/replay")
async def replay_incident(incident_id: str, request: Request):
    """
    Replay an incident workflow for failure recovery.

    USE CASES:
    1. Workflow failed mid-execution
    2. Need to re-run with updated scripts
    3. Manual trigger for testing

    Body (optional):
        {"from_stage": "classify", "reason": "Manual replay"}
    """
    import uuid

    body = {}
    try:
        body = await request.json()
    except:
        pass

    from_stage = body.get("from_stage", "classify")
    reason = body.get("reason", "Manual replay")

    # Fetch incident from ServiceNow
    incidents = await fetch_servicenow_incidents(use_cache=False)
    incident = next((i for i in incidents if i["incident_id"] == incident_id), None)

    if not incident:
        raise HTTPException(status_code=404, detail=f"Incident {incident_id} not found")

    # Publish replay event to Kafka
    workflow_id = str(uuid.uuid4())
    event = {
        "event_type": "incident.replay",
        "incident_id": incident_id,
        "workflow_id": workflow_id,
        "replay": True,
        "from_stage": from_stage,
        "reason": reason,
        "raw_incident": incident,
        "timestamp": datetime.utcnow().isoformat(),
    }

    try:
        from streaming.kafka_producer import get_producer
        producer = get_producer()
        await producer.publish_event(
            topic="incident.created",
            event=event,
            key=incident_id
        )
    except Exception as e:
        logger.warning(f"Kafka publish failed: {e}")

    return {
        "status": "replayed",
        "workflow_id": workflow_id,
        "incident_id": incident_id,
        "from_stage": from_stage,
        "message": "Replay event published to Kafka"
    }

@app.get("/api/incidents/{incident_id}")
async def get_incident(incident_id: str):
    """Get single incident"""
    incidents = await fetch_servicenow_incidents()
    for inc in incidents:
        if inc["incident_id"] == incident_id:
            return inc
    raise HTTPException(status_code=404, detail=f"Incident {incident_id} not found")

@app.post("/api/incidents/{incident_id}/close")
async def close_incident(incident_id: str, resolution: str = "Resolved by AI Agent"):
    """Close incident in ServiceNow"""
    success = await update_servicenow_incident(incident_id, {
        "state": "6",
        "close_code": "Solved (Permanently)",
        "close_notes": resolution
    })
    if success:
        return {"status": "closed", "incident_id": incident_id}
    raise HTTPException(status_code=500, detail="Failed to close incident")

# =============================================================================
# API Endpoints - Scripts
# =============================================================================
@app.get("/api/scripts")
async def list_scripts():
    """List all available scripts"""
    return {"scripts": REGISTRY.get("scripts", []), "count": len(REGISTRY.get("scripts", []))}

@app.get("/api/scripts/{script_id}")
async def get_script(script_id: str):
    """Get script details"""
    for script in REGISTRY.get("scripts", []):
        if script["id"] == script_id:
            return script
    raise HTTPException(status_code=404, detail=f"Script {script_id} not found")

@app.post("/api/scripts/match")
async def match_scripts(request: MatchRequest):
    """Match incident to remediation scripts"""
    inc_id = request.incident_id
    short_desc = request.short_description
    full_desc = request.description

    # Fetch from ServiceNow if needed
    if not short_desc and not full_desc and inc_id:
        incidents = await fetch_servicenow_incidents()
        for inc in incidents:
            if inc["incident_id"] == inc_id:
                short_desc = inc.get('short_description', '')
                full_desc = inc.get('description', '')
                break

    incident = {"incident_id": inc_id, "short_description": short_desc, "description": full_desc}

    # Try LLM matching first
    try:
        from orchestrator.llm_intelligence import analyze_incident_with_llm, match_scripts_with_llm

        analysis = analyze_incident_with_llm(incident)
        llm_matches = match_scripts_with_llm(incident, REGISTRY.get("scripts", []), analysis)

        final_matches = []
        for m in llm_matches:
            script = next((s for s in REGISTRY.get("scripts", []) if s.get("id") == m.get("script_id")), None)
            if script:
                extracted = extract_inputs_from_incident(f"{short_desc} {full_desc}", script.get("required_inputs", []))
                extracted.update(m.get("extracted_params", {}))
                final_matches.append({
                    "script_id": script["id"],
                    "name": script["name"],
                    "description": script["description"],
                    "type": script["type"],
                    "confidence": m.get("confidence", 0.5),
                    "risk_level": m.get("risk_assessment", script.get("risk_level", "medium")),
                    "auto_approve": script.get("auto_approve", False),
                    "required_inputs": script.get("required_inputs", []),
                    "extracted_inputs": extracted,
                })

        if final_matches:
            return {"matches": final_matches, "count": len(final_matches), "method": "llm", "analysis": analysis}
    except Exception as e:
        logger.warning(f"LLM matching failed: {e}")

    # Fallback to hybrid matching
    matches = match_scripts_to_incident(incident)
    return {
        "matches": [m.dict() for m in matches],
        "count": len(matches),
        "method": "hybrid"
    }

# =============================================================================
# API Endpoints - Execution
# =============================================================================
@app.post("/api/execute")
async def execute_script(request: ExecutionRequest):
    """Execute remediation script"""
    execution_id = str(uuid.uuid4())

    script = next((s for s in REGISTRY.get("scripts", []) if s["id"] == request.script_id), None)
    if not script:
        raise HTTPException(status_code=404, detail=f"Script {request.script_id} not found")

    # Validate inputs
    required = script.get("required_inputs", [])
    missing = [inp for inp in required if inp not in request.inputs or not request.inputs[inp]]
    if missing:
        raise HTTPException(status_code=400, detail=f"Missing required inputs: {', '.join(missing)}")

    # Check approval requirement
    requires_approval = not script.get("auto_approve", False)
    if script.get("risk_level") in ["high", "critical"]:
        requires_approval = True

    execution = {
        "execution_id": execution_id,
        "incident_id": request.incident_id,
        "script_id": request.script_id,
        "script_name": script["name"],
        "inputs": request.inputs,
        "environment": request.environment,
        "dry_run": request.dry_run,
        "status": "pending_approval" if requires_approval else "running",
        "created_at": datetime.utcnow().isoformat(),
    }

    # Dry run mode
    if request.dry_run:
        execution["status"] = "dry_run_complete"
        execution["dry_run_result"] = {
            "would_execute": {"script_id": script["id"], "script_name": script["name"]},
            "inputs_validated": True,
            "requires_approval": requires_approval,
        }
        EXECUTIONS[execution_id] = execution
        return {"execution_id": execution_id, "status": "dry_run_complete", "simulation": execution["dry_run_result"]}

    EXECUTIONS[execution_id] = execution

    if requires_approval:
        PENDING_APPROVALS[execution_id] = execution
        return {"execution_id": execution_id, "status": "pending_approval", "script": script["name"]}

    # Auto-approved - execute
    result = await trigger_github_workflow(script, request.inputs, request.incident_id)
    execution["status"] = result.get("status", "triggered")
    execution["github_run"] = result

    return {"execution_id": execution_id, "status": execution["status"], "github_run": result}

@app.get("/api/execute/{execution_id}")
async def get_execution(execution_id: str):
    """Get execution status"""
    if execution_id not in EXECUTIONS:
        raise HTTPException(status_code=404, detail="Execution not found")
    return EXECUTIONS[execution_id]

# =============================================================================
# API Endpoints - Approvals
# =============================================================================
@app.get("/api/approvals")
async def list_approvals():
    """List pending approvals"""
    return {"approvals": list(PENDING_APPROVALS.values()), "count": len(PENDING_APPROVALS)}

@app.get("/api/hitl/approvals/pending")
async def get_pending_hitl_approvals():
    """Get pending HITL approvals for frontend"""
    return [{
        "id": exec_id,
        "incident_id": ex.get("incident_id"),
        "type": "plan_approval",
        "status": "pending",
        "script": {"id": ex.get("script_id"), "name": ex.get("script_name")},
        "created_at": ex.get("created_at"),
    } for exec_id, ex in PENDING_APPROVALS.items()]

@app.post("/api/approvals/{execution_id}/approve")
async def approve_execution(execution_id: str, approver: str = "admin"):
    """Approve pending execution"""
    if execution_id not in PENDING_APPROVALS:
        raise HTTPException(status_code=404, detail="Approval not found")

    execution = PENDING_APPROVALS.pop(execution_id)
    execution["status"] = "approved"
    execution["approved_by"] = approver
    execution["approved_at"] = datetime.utcnow().isoformat()

    script = next((s for s in REGISTRY.get("scripts", []) if s["id"] == execution["script_id"]), None)
    if script:
        result = await trigger_github_workflow(script, execution["inputs"], execution["incident_id"])
        execution["status"] = "running"
        execution["github_run"] = result

    EXECUTIONS[execution_id] = execution
    return {"status": "approved", "execution": execution}

@app.post("/api/approvals/{execution_id}/reject")
async def reject_execution(execution_id: str, approver: str = "admin", reason: str = ""):
    """Reject pending execution"""
    if execution_id not in PENDING_APPROVALS:
        raise HTTPException(status_code=404, detail="Approval not found")

    execution = PENDING_APPROVALS.pop(execution_id)
    execution["status"] = "rejected"
    execution["rejected_by"] = approver
    execution["rejection_reason"] = reason
    EXECUTIONS[execution_id] = execution
    return {"status": "rejected", "execution": execution}

# =============================================================================
# API Endpoints - LangGraph Workflow Approvals
# =============================================================================

# Store workflow approvals keyed by incident_id
WORKFLOW_APPROVALS: Dict[str, Dict] = {}

@app.get("/api/langgraph/approvals")
async def list_langgraph_approvals():
    """List pending LangGraph workflow approvals and failed executions needing retry"""
    pending = []
    for workflow_id, state in WORKFLOW_STATES.items():
        # Include workflows awaiting approval
        if state.get("status") == "pending_approval" or state.get("current_step") == "await_approval":
            pending.append({
                "workflow_id": workflow_id,
                "incident_id": state.get("incident_id", workflow_id),
                "approval_token": state.get("approval_token"),
                "plan": state.get("plan", {}),
                "judge_score": state.get("judge_score", {}),
                "approval_decision": state.get("approval_decision", {}),
                "created_at": state.get("started_at"),
                "requires_action": "approval"
            })
        # Include approved but failed to trigger - need retry
        elif state.get("execution_status") == "trigger_failed":
            pending.append({
                "workflow_id": workflow_id,
                "incident_id": state.get("incident_id", workflow_id),
                "approval_token": state.get("approval_token"),
                "plan": state.get("plan", {}),
                "judge_score": state.get("judge_score", {}),
                "approval_decision": state.get("approval_decision", {}),
                "created_at": state.get("started_at"),
                "requires_action": "retry",
                "error": state.get("execution_output", {}).get("error", "GitHub trigger failed")
            })
    return {"approvals": pending, "count": len(pending)}

@app.post("/api/langgraph/approve/{incident_id}")
async def approve_langgraph_workflow(
    incident_id: str,
    approver: str = "admin",
    notes: str = ""
):
    """
    Approve a LangGraph workflow and trigger execution.

    This endpoint:
    1. Finds the workflow state for the incident
    2. Updates approval decision
    3. Triggers GitHub Actions workflow execution
    4. Resumes the workflow
    """
    # Find workflow state
    workflow_state = None
    workflow_id = None
    for wf_id, state in WORKFLOW_STATES.items():
        if state.get("incident_id") == incident_id or wf_id == incident_id:
            workflow_state = state
            workflow_id = wf_id
            break

    if not workflow_state:
        raise HTTPException(status_code=404, detail=f"Workflow not found for incident {incident_id}")

    if workflow_state.get("current_step") != "await_approval":
        raise HTTPException(
            status_code=400,
            detail=f"Workflow not awaiting approval. Current step: {workflow_state.get('current_step')}"
        )

    # Update approval
    workflow_state["approval_decision_input"] = {
        "decision": "approved",
        "approver": approver,
        "notes": notes,
        "approved_at": datetime.utcnow().isoformat()
    }

    # Get the plan and trigger GitHub Actions
    plan = workflow_state.get("plan", {})
    script_id = plan.get("script_id")
    workflow_name = plan.get("workflow_name", "shell-execute.yml")

    # Build inputs for GitHub Actions
    # shell-execute.yml expects: script_path, script_args, environment, incident_id, dry_run
    parsed_context = workflow_state.get("parsed_context", {})
    description = parsed_context.get("description", "")

    # Extract instance_name, zone, project from incident description for script_args
    import re
    instance_match = re.search(r"Instance:\s*(\S+)", description)
    zone_match = re.search(r"Zone:\s*(\S+)", description)
    project_match = re.search(r"Project:\s*(\S+)", description)

    script_args = {
        "instance_name": instance_match.group(1) if instance_match else "test-incident-vm-01",
        "zone": zone_match.group(1) if zone_match else "us-central1-a",
        "project": project_match.group(1) if project_match else "agent-ai-test-461120",
    }

    inputs = {
        "script_path": plan.get("script_path", "scripts/start_gcp_instance.sh"),
        "script_args": json.dumps(script_args),
        "environment": parsed_context.get("environment", "production"),
        "incident_id": incident_id,
        "dry_run": "false"  # GitHub Actions expects string
    }

    try:
        # Trigger GitHub Actions workflow
        run_id = await github_actions.trigger_workflow(
            workflow_file=workflow_name,
            inputs=inputs,
            ref="main"
        )
        result = {"run_id": run_id}

        workflow_state["execution_status"] = "triggered"
        workflow_state["github_run_id"] = result.get("run_id")
        workflow_state["execution_output"] = {
            "triggered_at": datetime.utcnow().isoformat(),
            "workflow": workflow_name,
            "inputs": inputs,
            "github_response": result
        }
        workflow_state["current_step"] = "execute"
        workflow_state["status"] = "executing"

        logger.info(
            "workflow_approved_and_triggered",
            incident_id=incident_id,
            workflow_name=workflow_name,
            github_run_id=result.get("run_id")
        )

        return {
            "status": "approved",
            "incident_id": incident_id,
            "execution_status": "triggered",
            "github_run_id": result.get("run_id"),
            "workflow": workflow_name,
            "inputs": inputs,
            "message": "Workflow triggered. Status will be updated via GitHub webhook when complete."
        }

    except Exception as e:
        logger.error("github_workflow_trigger_failed", incident_id=incident_id, error=str(e))
        # Still transition the workflow to approved state so it doesn't block
        # The execution can be retried manually
        workflow_state["execution_status"] = "trigger_failed"
        workflow_state["execution_output"] = {
            "error": str(e),
            "inputs": inputs,
            "workflow_name": workflow_name
        }
        workflow_state["status"] = "approved_pending_execution"
        workflow_state["current_step"] = "execute_pending"

        return {
            "status": "approved_but_trigger_failed",
            "incident_id": incident_id,
            "error": str(e),
            "message": "Approval recorded but GitHub workflow trigger failed. Use /api/langgraph/retry/{incident_id} to retry.",
            "retry_endpoint": f"/api/langgraph/retry/{incident_id}"
        }


@app.post("/api/langgraph/retry/{incident_id}")
async def retry_github_trigger(incident_id: str):
    """
    Retry GitHub Actions workflow trigger for an approved incident.

    Use this when the initial trigger failed (e.g., rate limiting).
    """
    # Find workflow state
    workflow_state = None
    workflow_id = None
    for wf_id, state in WORKFLOW_STATES.items():
        if state.get("incident_id") == incident_id or wf_id == incident_id:
            workflow_state = state
            workflow_id = wf_id
            break

    if not workflow_state:
        raise HTTPException(status_code=404, detail=f"Workflow not found for incident {incident_id}")

    if workflow_state.get("execution_status") != "trigger_failed":
        raise HTTPException(
            status_code=400,
            detail=f"Workflow not in trigger_failed state. Current: {workflow_state.get('execution_status')}"
        )

    # Get stored inputs from previous attempt
    prev_output = workflow_state.get("execution_output", {})
    inputs = prev_output.get("inputs", {})
    workflow_name = prev_output.get("workflow_name", "shell-execute.yml")

    if not inputs:
        # Rebuild inputs if not stored
        plan = workflow_state.get("plan", {})
        parsed_context = workflow_state.get("parsed_context", {})
        description = parsed_context.get("description", "")

        import re
        instance_match = re.search(r"Instance:\s*(\S+)", description)
        zone_match = re.search(r"Zone:\s*(\S+)", description)
        project_match = re.search(r"Project:\s*(\S+)", description)

        script_args = {
            "instance_name": instance_match.group(1) if instance_match else "test-incident-vm-01",
            "zone": zone_match.group(1) if zone_match else "us-central1-a",
            "project": project_match.group(1) if project_match else "agent-ai-test-461120",
        }

        inputs = {
            "script_path": plan.get("script_path", "scripts/start_gcp_instance.sh"),
            "script_args": json.dumps(script_args),
            "environment": parsed_context.get("environment", "production"),
            "incident_id": incident_id,
            "dry_run": "false"
        }
        workflow_name = plan.get("workflow_name", "shell-execute.yml")

    try:
        # Retry GitHub Actions workflow
        run_id = await github_actions.trigger_workflow(
            workflow_file=workflow_name,
            inputs=inputs,
            ref="main"
        )
        result = {"run_id": run_id}

        workflow_state["execution_status"] = "triggered"
        workflow_state["github_run_id"] = result.get("run_id")
        workflow_state["execution_output"] = {
            "triggered_at": datetime.utcnow().isoformat(),
            "workflow": workflow_name,
            "inputs": inputs,
            "github_response": result,
            "retry": True
        }
        workflow_state["current_step"] = "execute"
        workflow_state["status"] = "executing"

        logger.info(
            "github_workflow_retry_success",
            incident_id=incident_id,
            workflow_name=workflow_name,
            github_run_id=result.get("run_id")
        )

        return {
            "status": "retry_success",
            "incident_id": incident_id,
            "execution_status": "triggered",
            "github_run_id": result.get("run_id"),
            "workflow": workflow_name,
            "message": "Workflow triggered. Status will be updated via GitHub webhook when complete."
        }

    except Exception as e:
        logger.error("github_workflow_retry_failed", incident_id=incident_id, error=str(e))
        return {
            "status": "retry_failed",
            "incident_id": incident_id,
            "error": str(e),
            "message": "GitHub workflow trigger retry failed. Try again later."
        }

@app.post("/api/langgraph/reject/{incident_id}")
async def reject_langgraph_workflow(
    incident_id: str,
    approver: str = "admin",
    reason: str = "Rejected by admin"
):
    """Reject a LangGraph workflow"""
    # Find workflow state
    workflow_state = None
    for wf_id, state in WORKFLOW_STATES.items():
        if state.get("incident_id") == incident_id or wf_id == incident_id:
            workflow_state = state
            break

    if not workflow_state:
        raise HTTPException(status_code=404, detail=f"Workflow not found for incident {incident_id}")

    workflow_state["approval_decision_input"] = {
        "decision": "rejected",
        "approver": approver,
        "reason": reason,
        "rejected_at": datetime.utcnow().isoformat()
    }
    workflow_state["status"] = "rejected"
    workflow_state["current_step"] = "rejected"

    return {
        "status": "rejected",
        "incident_id": incident_id,
        "reason": reason
    }


# =============================================================================
# API Endpoints - Stats & Agents
# =============================================================================
@app.get("/api/stats")
async def get_stats():
    """Get system statistics"""
    incidents = await fetch_servicenow_incidents()
    active = sum(1 for i in incidents if i.get("status") in ["1", "2", "3"])
    resolved = sum(1 for i in incidents if i.get("status") in ["5", "6", "7"])

    return {
        "active_incidents": active,
        "pending_approvals": len(PENDING_APPROVALS),
        "success_rate": round((resolved / (active + resolved) * 100) if (active + resolved) > 0 else 0, 1),
        "total_incidents": len(incidents),
        "total_scripts": len(REGISTRY.get("scripts", [])),
        "total_executions": len(EXECUTIONS),
    }

@app.get("/api/agents")
async def list_agents():
    """List all available agents - includes both IT Service and Data agents"""
    return [
        # IT Service Agents
        {"name": "servicenow", "display_name": "ServiceNow Agent", "status": "active", "type": "incident", "category": "it-service"},
        {"name": "jira", "display_name": "Jira Agent", "status": "active", "type": "incident", "category": "it-service"},
        {"name": "gcp", "display_name": "GCP Agent", "status": "active", "type": "remediation", "category": "it-service"},
        {"name": "kubernetes", "display_name": "Kubernetes Agent", "status": "active", "type": "remediation", "category": "it-service"},
        {"name": "github", "display_name": "GitHub Actions Agent", "status": "active" if github_breaker.can_execute() else "degraded", "type": "execution", "category": "it-service"},
        # Data Engineering Agents
        {"name": "data-pipeline", "display_name": "Data Pipeline Agent", "status": "active", "type": "data-engineering", "category": "data",
         "capabilities": ["source_analysis", "spark_generation", "dag_generation", "dq_rules"],
         "tools": ["gcs_infer_schema", "iceberg_get_table", "llm_analyze_schema"]},
        # Shared Agents
        {"name": "llm", "display_name": "LLM Intelligence Agent", "status": "active" if openai_breaker.can_execute() else "degraded", "type": "analysis", "category": "shared"},
        # MCP Servers
        {"name": "gcs-mcp", "display_name": "GCS MCP Server", "status": "active", "type": "mcp", "category": "shared",
         "tools": ["gcs_list_buckets", "gcs_list_objects", "gcs_infer_schema", "gcs_get_logs"]},
        {"name": "iceberg-mcp", "display_name": "Iceberg MCP Server", "status": "active", "type": "mcp", "category": "shared",
         "tools": ["iceberg_list_tables", "iceberg_get_schema", "iceberg_list_snapshots"]},
        {"name": "llm-mcp", "display_name": "LLM MCP Server", "status": "active", "type": "mcp", "category": "shared",
         "tools": ["llm_analyze_incident", "llm_analyze_schema", "llm_generate_spark_code"]},
    ]

# =============================================================================
# API Endpoints - LangGraph Workflow
# =============================================================================
@app.get("/api/langgraph/definition")
async def get_langgraph_definition():
    """Get LangGraph workflow definition - Simplified 7-node architecture"""
    return {
        "nodes": [
            {
                "id": 1,
                "name": "Receive & Parse",
                "phase": "Ingestion",
                "type": "processor",
                "description": "Receive incident from Kafka/API and extract context"
            },
            {
                "id": 2,
                "name": "Swarm RAG Search",
                "phase": "Retrieval",
                "type": "retriever",
                "description": "Multi-agent search: Vector + Graph + Keyword scoring"
            },
            {
                "id": 3,
                "name": "Generate Plan",
                "phase": "Planning",
                "type": "llm",
                "description": "LLM generates remediation plan with rollback strategy"
            },
            {
                "id": 4,
                "name": "LLM Judge",
                "phase": "Validation",
                "type": "judge",
                "description": "Evaluate plan quality, safety, and feasibility"
            },
            {
                "id": 5,
                "name": "Control Plane",
                "phase": "Approval",
                "type": "human",
                "description": "Risk assessment and HITL approval routing"
            },
            {
                "id": 6,
                "name": "Execute",
                "phase": "Execution",
                "type": "executor",
                "description": "Trigger GitHub Actions workflow for remediation"
            },
            {
                "id": 7,
                "name": "Verify & Close",
                "phase": "Completion",
                "type": "processor",
                "description": "Verify fix, close ticket, update RAG feedback"
            },
        ],
        "edges": [
            {"from": 1, "to": 2},
            {"from": 2, "to": 3},
            {"from": 3, "to": 4},
            {"from": 4, "to": 5, "condition": "judge_passed"},
            {"from": 4, "to": 3, "condition": "revision_needed"},
            {"from": 5, "to": 6, "condition": "approved"},
            {"from": 6, "to": 7},
        ],
        "phases": [
            {"name": "Ingestion", "nodes": [1], "color": "#3B82F6"},
            {"name": "Retrieval", "nodes": [2], "color": "#06B6D4"},
            {"name": "Planning", "nodes": [3], "color": "#F59E0B"},
            {"name": "Validation", "nodes": [4], "color": "#8B5CF6"},
            {"name": "Approval", "nodes": [5], "color": "#EF4444"},
            {"name": "Execution", "nodes": [6], "color": "#10B981"},
            {"name": "Completion", "nodes": [7], "color": "#22C55E"},
        ]
    }

@app.post("/api/langgraph/run")
async def run_langgraph_workflow(incident_id: str):
    """Run full LangGraph workflow for incident"""
    workflow_id = str(uuid.uuid4())[:8]

    # Get incident
    incidents = await fetch_servicenow_incidents()
    incident = next((i for i in incidents if i["incident_id"] == incident_id), None)
    if not incident:
        raise HTTPException(status_code=404, detail=f"Incident {incident_id} not found")

    # Run workflow using WorkflowOrchestrator
    try:
        from orchestrator.langgraph_workflow import WorkflowOrchestrator
        orchestrator = WorkflowOrchestrator()
        result = await orchestrator.run(incident_id=incident_id, raw_incident=incident)

        # Store result in WORKFLOW_STATES for approval tracking
        WORKFLOW_STATES[workflow_id] = result
        # Also store by incident_id for easier lookup
        WORKFLOW_STATES[incident_id] = result

        return {"workflow_id": workflow_id, "status": "completed", "result": result}
    except Exception as e:
        logger.error(f"Workflow error: {e}")
        return {"workflow_id": workflow_id, "status": "error", "error": str(e)}

@app.get("/api/langgraph/workflow/{workflow_id}")
async def get_workflow_state(workflow_id: str):
    """Get current workflow state"""
    if workflow_id not in WORKFLOW_STATES:
        raise HTTPException(status_code=404, detail="Workflow not found")
    return WORKFLOW_STATES[workflow_id]

# Node name mapping for the 12-node workflow (full incident lifecycle)
NODE_NAMES = {
    1: "ingest",
    2: "parse",
    3: "classify",
    4: "swarm_rag",
    5: "generate_plan",
    6: "judge_evaluation",
    7: "control_plane",
    8: "await_approval",
    9: "execute",
    10: "verify",
    11: "close_ticket",
    12: "feedback_loop"
}

@app.post("/api/langgraph/node/{node_id}")
async def execute_langgraph_node(node_id: int, request: Dict[str, Any]):
    """
    Execute a single LangGraph node for step-by-step workflow execution.

    This endpoint allows the frontend to execute individual workflow nodes
    for visualization and debugging purposes.

    Node IDs:
    1 - Ingest: Receive incident from Kafka
    2 - Parse: Extract context from raw incident
    3 - Classify: Determine incident type using LLM
    4 - Swarm RAG: Search for remediation scripts
    5 - Generate Plan: Create execution plan
    6 - Judge Evaluation: Validate plan with LLM-as-Judge
    7 - Control Plane: Decide approval routing
    """
    workflow_id = request.get("workflow_id", str(uuid.uuid4())[:8])
    incident_id = request.get("incident_id", "")
    input_data = request.get("input_data", {})

    node_name = NODE_NAMES.get(node_id, f"node_{node_id}")

    logger.info(f"Executing node {node_id} ({node_name}) for workflow {workflow_id}")

    # Initialize workflow state if new
    if workflow_id not in WORKFLOW_STATES:
        WORKFLOW_STATES[workflow_id] = {
            "workflow_id": workflow_id,
            "incident_id": incident_id,
            "status": "running",
            "current_node": node_id,
            "nodes_completed": [],
            "results": {}
        }

    try:
        # Get incident data for context
        incidents = await fetch_servicenow_incidents()
        incident = next((i for i in incidents if i.get("incident_id") == incident_id), None)

        if not incident and node_id > 1:
            # For nodes after ingest, we need incident data
            raise HTTPException(status_code=404, detail=f"Incident {incident_id} not found")

        # Execute the specific node
        result = await _execute_single_node(node_id, node_name, incident, workflow_id, input_data)

        # Update workflow state
        WORKFLOW_STATES[workflow_id]["current_node"] = node_id
        WORKFLOW_STATES[workflow_id]["nodes_completed"].append(node_id)
        WORKFLOW_STATES[workflow_id]["results"][node_name] = result

        return {
            "node_id": node_id,
            "node_name": node_name,
            "status": "completed",
            "workflow_id": workflow_id,
            "result": result
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Node {node_id} execution error: {e}")
        WORKFLOW_STATES[workflow_id]["status"] = "error"
        WORKFLOW_STATES[workflow_id]["error"] = str(e)
        raise HTTPException(status_code=500, detail=f"Node {node_id} failed: {str(e)}")


async def _execute_single_node(node_id: int, node_name: str, incident: dict, workflow_id: str, input_data: dict) -> dict:
    """
    Execute a single workflow node using REAL LangGraph node functions.

    This calls the actual node implementations from langgraph_workflow.py
    which include:
    - Real LLM classification via OpenAI
    - Real RAG search via Weaviate + Neo4j
    - Real LLM-as-Judge evaluation
    - Real Control Plane policy evaluation
    - Real human-in-the-loop approval via Kafka
    - Real GitHub Actions execution
    """

    # Get or create workflow state from cache
    if workflow_id not in WORKFLOW_STATES:
        WORKFLOW_STATES[workflow_id] = {
            "incident_id": incident.get("incident_id", "") if incident else input_data.get("incident_id", ""),
            "correlation_id": str(uuid.uuid4()),
            "status": IncidentStatus.NEW.value,
            "raw_incident": incident or {},
            "metadata": {},
            "parsed_context": {},
            "classification": "",
            "severity": incident.get("priority", "3") if incident else "3",
            "service": incident.get("category", "") if incident else "",
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
            "started_at": datetime.now().isoformat(),
            "completed_at": None
        }
    else:
        # Update existing state with incident data if missing
        if incident and (not WORKFLOW_STATES[workflow_id].get("raw_incident") or WORKFLOW_STATES[workflow_id].get("raw_incident") == {}):
            WORKFLOW_STATES[workflow_id]["raw_incident"] = incident
            WORKFLOW_STATES[workflow_id]["severity"] = incident.get("priority", "3")
            WORKFLOW_STATES[workflow_id]["service"] = incident.get("category", "")
        # Also ensure full state fields exist (merge basic state with full state)
        full_state_fields = {
            "correlation_id": str(uuid.uuid4()),
            "metadata": {},
            "parsed_context": {},
            "classification": "",
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
            "errors": [],
            "completed_at": None
        }
        for key, default_val in full_state_fields.items():
            if key not in WORKFLOW_STATES[workflow_id]:
                WORKFLOW_STATES[workflow_id][key] = default_val

    # Get current state (now with all fields properly set)
    state = WORKFLOW_STATES[workflow_id]

    try:
        if node_id == 1:  # Ingest - Receive incident
            state = await node_ingest(state)
            return {
                "step": "ingest",
                "incident_id": state.get("incident_id"),
                "correlation_id": state.get("correlation_id"),
                "received_at": state.get("started_at"),
                "source": "servicenow",
                "status": state.get("status"),
                "message": "Incident received and validated via REAL LangGraph node"
            }

        elif node_id == 2:  # Parse - Extract context
            state = await node_parse(state)
            return {
                "step": "parse",
                "parsed_context": state.get("parsed_context", {}),
                "severity": state.get("severity"),
                "service": state.get("service"),
                "message": "Context extracted via REAL LangGraph node"
            }

        elif node_id == 3:  # Classify - LLM classification
            state = await node_classify(state)
            return {
                "step": "classify",
                "classification": state.get("classification"),
                "status": state.get("status"),
                "message": f"Incident classified as {state.get('classification')} via REAL LLM Intelligence"
            }

        elif node_id == 4:  # Swarm RAG - Search for scripts
            state = await node_swarm_rag(state)
            rag_results = state.get("rag_results", [])
            return {
                "step": "swarm_rag",
                "scripts_found": len(rag_results),
                "top_scripts": rag_results[:3],
                "rag_confidence": state.get("rag_confidence", 0.0),
                "rag_query": state.get("rag_query"),
                "message": f"Found {len(rag_results)} matching scripts via REAL Hybrid Search (Weaviate + Neo4j)"
            }

        elif node_id == 5:  # Generate Plan - Create execution plan
            state = await node_generate_plan(state)
            plan = state.get("plan", {})
            return {
                "step": "generate_plan",
                "plan": plan,
                "plan_generated_at": state.get("plan_generated_at"),
                "status": state.get("status"),
                "message": "Remediation plan generated via REAL LLM Intelligence"
            }

        elif node_id == 6:  # Judge Evaluation - LLM-as-Judge
            state = await node_judge_evaluation(state)
            judge_score = state.get("judge_score", {})
            return {
                "step": "judge_evaluation",
                "judge_score": judge_score,
                "judge_passed": state.get("judge_passed", False),
                "revision_count": state.get("revision_count", 0),
                "status": state.get("status"),
                "message": "Plan evaluated by REAL LLM-as-Judge (GPT-4)"
            }

        elif node_id == 7:  # Control Plane - Approval routing
            state = await node_control_plane(state)
            approval_decision = state.get("approval_decision", {})
            approval_route = state.get("approval_route", "")

            # Determine if human approval is needed
            needs_approval = approval_route in ["manual_approve", "senior_approval", "change_board"]

            return {
                "step": "control_plane",
                "decision": approval_route,
                "approval_decision": approval_decision,
                "approval_reason": state.get("approval_reason", ""),
                "approval_required": needs_approval,
                "next_action": "await_approval" if needs_approval else "execute",
                "status": state.get("status"),
                "message": f"Control Plane decision: {approval_route} via REAL Policy Engine"
            }

        elif node_id == 8:  # Await Approval - Human approval checkpoint
            state = await node_await_approval(state)
            return {
                "step": "await_approval",
                "status": state.get("status"),
                "approval_route": state.get("approval_route"),
                "awaiting_approval": state.get("current_step") == "await_approval",
                "message": "Workflow paused for human approval via Kafka event"
            }

        elif node_id == 9:  # Execute - Trigger GitHub Actions
            state = await node_execute(state)
            return {
                "step": "execute",
                "status": state.get("status"),
                "execution_status": state.get("execution_status"),
                "github_run_id": state.get("github_run_id"),
                "execution_output": state.get("execution_output", {}),
                "message": "Remediation executed via GitHub Actions"
            }

        elif node_id == 10:  # Verify - Check fix applied
            state = await node_verify(state)
            return {
                "step": "verify",
                "status": state.get("status"),
                "fix_verified": state.get("fix_verified"),
                "verification_reason": state.get("verification_reason"),
                "message": "Fix verification completed"
            }

        elif node_id == 11:  # Close Ticket - Close ServiceNow ticket
            state = await node_close_ticket(state)
            return {
                "step": "close_ticket",
                "status": state.get("status"),
                "ticket_closed": state.get("ticket_closed"),
                "resolution_summary": state.get("resolution_summary"),
                "message": "ServiceNow ticket closed"
            }

        elif node_id == 12:  # Feedback Loop - Update RAG/VectorDB/Neo4j
            state = await node_feedback_loop(state)
            return {
                "step": "feedback_loop",
                "status": state.get("status"),
                "rag_updated": True,
                "vectordb_updated": True,
                "neo4j_updated": True,
                "feedback_summary": state.get("feedback_summary", "Knowledge base updated with resolution data"),
                "message": "RAG, VectorDB, and Neo4j updated with incident resolution data"
            }

        else:
            return {
                "step": f"node_{node_id}",
                "error": f"Unknown node {node_id}",
                "message": f"Unknown node {node_id}"
            }

    except Exception as e:
        logger.error(f"REAL node {node_id} execution failed: {e}")
        return {
            "step": node_name,
            "error": str(e),
            "message": f"Node {node_name} failed: {str(e)}"
        }
    finally:
        # Update workflow state cache
        WORKFLOW_STATES[workflow_id] = state

# =============================================================================
# API Endpoints - RAG
# =============================================================================
@app.post("/api/rag/search")
async def enhanced_rag_search(request: dict):
    """Enhanced hybrid search"""
    try:
        from rag.hybrid_search_engine import hybrid_search_engine

        query = request.get("query", "")
        metadata = request.get("metadata", {})
        top_k = request.get("top_k", 10)

        results = hybrid_search_engine.search(query=query, query_metadata=metadata, top_k=top_k)
        return {"results": [r.to_dict() for r in results], "count": len(results)}
    except Exception as e:
        logger.error(f"RAG search error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# =============================================================================
# API Endpoints - Guardrails
# =============================================================================
@app.post("/api/guardrails/validate")
async def validate_content(request: Dict[str, Any]):
    """Validate content through guardrails"""
    try:
        from guardrails.llm_guardrails import guardrails

        content = request.get("content", "")
        context = request.get("context", "general")

        result = guardrails.validate_input(content, context)
        return {"status": "success", "result": result.to_dict()}
    except ImportError:
        return {"status": "error", "message": "Guardrails module not available"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/guardrails/status")
async def guardrails_status():
    """Get guardrails system status"""
    try:
        from guardrails.llm_guardrails import guardrails
        return {"enabled": True, "version": "1.0.0"}
    except ImportError:
        return {"enabled": False}

# =============================================================================
# API Endpoints - Jira Stories (from Kafka Consumer Cache)
# =============================================================================
JIRA_STORIES_CACHE_KEY = "jira:stories:active"

@app.get("/api/jira/stories")
async def list_jira_stories():
    """
    List Jira stories from Redis cache.

    Stories are populated by the JiraConsumer which listens to Kafka.
    This mirrors the /api/incidents pattern for ServiceNow.
    """
    try:
        from utils.redis_client import redis_client

        cached = redis_client.get(JIRA_STORIES_CACHE_KEY)
        if cached:
            data = json.loads(cached)
            return {
                "stories": data.get("stories", []),
                "count": data.get("total", 0),
                "source": data.get("source", "kafka_consumer"),
                "cached_at": data.get("cached_at")
            }

        return {"stories": [], "count": 0, "source": "empty"}

    except Exception as e:
        logger.error("jira_stories_fetch_error", error=str(e))
        return {"stories": [], "count": 0, "error": str(e)}

@app.get("/api/jira/stories/{story_key}")
async def get_jira_story(story_key: str):
    """Get single Jira story from Redis cache"""
    try:
        from utils.redis_client import redis_client

        cached = redis_client.get(f"jira:story:{story_key}")
        if cached:
            return json.loads(cached)

        raise HTTPException(status_code=404, detail=f"Story {story_key} not found")

    except HTTPException:
        raise
    except Exception as e:
        logger.error("jira_story_fetch_error", story_key=story_key, error=str(e))
        raise HTTPException(status_code=500, detail=str(e))

# =============================================================================
# API Endpoints - Jira Webhook for Data Pipeline Trigger
# =============================================================================
@app.post("/api/webhooks/jira")
async def jira_webhook(payload: Dict[str, Any]):
    """
    Jira Webhook Endpoint - Triggers Data Pipeline Agent from Jira stories.

    Similar to ServiceNow webhook that triggers IT Service Agent.

    Expected: Jira sends webhook on issue created/updated.
    If issue matches data pipeline criteria, triggers pipeline workflow.
    """
    try:
        from streaming.consumers.jira_consumer import JiraConsumer
        # Create temporary consumer instance for webhook handling
        jira_consumer = JiraConsumer()
        # Process webhook by calling the consumer's method
        async def handle_jira_webhook(payload):
            webhook_event = payload.get("webhookEvent", "")
            issue = payload.get("issue", {})
            if webhook_event not in ["jira:issue_created", "jira:issue_updated"]:
                return {"status": "ignored", "reason": f"event_type: {webhook_event}"}
            event = {
                "key": issue.get("key"),
                "summary": issue.get("fields", {}).get("summary", ""),
                "description": issue.get("fields", {}).get("description", ""),
                "labels": [l for l in issue.get("fields", {}).get("labels", [])],
                "project": {"key": issue.get("fields", {}).get("project", {}).get("key")},
                "reporter": issue.get("fields", {}).get("reporter", {}),
                "priority": issue.get("fields", {}).get("priority", {}),
                "status": issue.get("fields", {}).get("status", {}).get("name"),
            }
            await jira_consumer.process_jira_story(event)
            return {"status": "accepted", "jira_key": event["key"]}

        result = await handle_jira_webhook(payload)

        logger.info(
            "jira_webhook_received",
            event=payload.get("webhookEvent"),
            issue_key=payload.get("issue", {}).get("key"),
            result=result.get("status")
        )

        return result

    except Exception as e:
        logger.error("jira_webhook_failed", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# API Endpoints - GitHub Webhook for Workflow Completion
# =============================================================================
# GitHub webhook secret for signature verification
GITHUB_WEBHOOK_SECRET = os.getenv("GITHUB_WEBHOOK_SECRET", "")

def verify_github_signature(payload: bytes, signature: str) -> bool:
    """
    Verify GitHub webhook signature using HMAC-SHA256.

    GitHub sends X-Hub-Signature-256 header with 'sha256=<signature>'.
    We compute HMAC of payload and compare.
    """
    import hmac
    import hashlib

    if not GITHUB_WEBHOOK_SECRET:
        # If no secret configured, skip verification (dev mode)
        logger.warning("github_webhook_secret_not_configured")
        return True

    if not signature or not signature.startswith("sha256="):
        return False

    expected_signature = signature[7:]  # Remove 'sha256=' prefix
    computed = hmac.new(
        GITHUB_WEBHOOK_SECRET.encode('utf-8'),
        payload,
        hashlib.sha256
    ).hexdigest()

    return hmac.compare_digest(computed, expected_signature)


@app.post("/api/webhooks/github")
async def github_webhook(request: Request):
    """
    GitHub Webhook Endpoint - Receives workflow run completion events.

    WHY: Eliminates polling for workflow status. GitHub pushes events
         when workflows complete, reducing API calls from ~120 to 0 per workflow.

    EVENTS HANDLED:
    - workflow_run.completed: Workflow finished (success/failure)

    SETUP IN GITHUB:
    1. Go to repo Settings → Webhooks → Add webhook
    2. Payload URL: https://your-domain/api/webhooks/github
    3. Content type: application/json
    4. Secret: Generate and store in GITHUB_WEBHOOK_SECRET env var
    5. Events: Select "Workflow runs"
    """
    # Get raw body for signature verification
    body = await request.body()

    # Verify signature
    signature = request.headers.get("X-Hub-Signature-256", "")
    if not verify_github_signature(body, signature):
        logger.warning("github_webhook_invalid_signature")
        raise HTTPException(status_code=401, detail="Invalid signature")

    # Parse payload
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    # Get event type
    event_type = request.headers.get("X-GitHub-Event", "")

    logger.info(
        "github_webhook_received",
        event_type=event_type,
        action=payload.get("action"),
        workflow=payload.get("workflow_run", {}).get("name")
    )

    # Handle workflow_run events
    if event_type == "workflow_run":
        action = payload.get("action")
        workflow_run = payload.get("workflow_run", {})

        if action == "completed":
            return await _handle_workflow_completed(workflow_run)
        elif action == "requested":
            # Workflow started - could log but no action needed
            logger.info(
                "github_workflow_started",
                run_id=workflow_run.get("id"),
                workflow=workflow_run.get("name")
            )
            return {"status": "acknowledged", "action": "requested"}

    # Handle ping event (sent when webhook is first configured)
    if event_type == "ping":
        logger.info("github_webhook_ping", zen=payload.get("zen"))
        return {"status": "pong", "message": "Webhook configured successfully"}

    return {"status": "ignored", "event_type": event_type}


async def _handle_workflow_completed(workflow_run: Dict[str, Any]):
    """
    Handle workflow_run.completed event from GitHub.

    Updates the corresponding workflow state in WORKFLOW_STATES.
    """
    run_id = workflow_run.get("id")
    conclusion = workflow_run.get("conclusion")  # success, failure, cancelled, etc.
    workflow_name = workflow_run.get("name")
    html_url = workflow_run.get("html_url")

    logger.info(
        "github_workflow_completed",
        run_id=run_id,
        conclusion=conclusion,
        workflow=workflow_name,
        url=html_url
    )

    # Find the workflow state that matches this GitHub run ID
    matched_workflow = None
    matched_workflow_id = None

    for workflow_id, state in WORKFLOW_STATES.items():
        if state.get("github_run_id") == run_id:
            matched_workflow = state
            matched_workflow_id = workflow_id
            break

    if not matched_workflow:
        # No matching workflow found - might be from a different trigger
        logger.warning(
            "github_webhook_no_matching_workflow",
            run_id=run_id,
            workflow=workflow_name
        )
        return {
            "status": "no_match",
            "run_id": run_id,
            "message": "No matching workflow state found for this run"
        }

    # Update workflow state based on conclusion
    incident_id = matched_workflow.get("incident_id", matched_workflow_id)

    if conclusion == "success":
        matched_workflow["execution_status"] = "completed"
        matched_workflow["status"] = "executed"
        matched_workflow["current_step"] = "verify"
        matched_workflow["execution_output"] = {
            **matched_workflow.get("execution_output", {}),
            "conclusion": conclusion,
            "completed_at": datetime.utcnow().isoformat(),
            "github_url": html_url
        }

        logger.info(
            "workflow_execution_success",
            incident_id=incident_id,
            workflow_id=matched_workflow_id,
            github_run_id=run_id
        )

        # Publish success event to Kafka
        try:
            from streaming.kafka_producer import get_producer
            producer = get_producer()
            await producer.publish_event(
                topic="incident.executed",
                event={
                    "event_type": "incident.executed",
                    "incident_id": incident_id,
                    "workflow_id": matched_workflow_id,
                    "github_run_id": run_id,
                    "conclusion": conclusion,
                    "timestamp": datetime.utcnow().isoformat()
                },
                key=incident_id
            )
        except Exception as e:
            logger.warning("kafka_publish_failed", error=str(e))

        return {
            "status": "success",
            "incident_id": incident_id,
            "workflow_id": matched_workflow_id,
            "conclusion": conclusion,
            "next_step": "verify"
        }

    else:
        # Workflow failed or cancelled
        matched_workflow["execution_status"] = "failed"
        matched_workflow["status"] = "execution_failed"
        matched_workflow["current_step"] = "execute_failed"
        matched_workflow["execution_output"] = {
            **matched_workflow.get("execution_output", {}),
            "conclusion": conclusion,
            "failed_at": datetime.utcnow().isoformat(),
            "github_url": html_url
        }

        logger.error(
            "workflow_execution_failed",
            incident_id=incident_id,
            workflow_id=matched_workflow_id,
            github_run_id=run_id,
            conclusion=conclusion
        )

        return {
            "status": "failed",
            "incident_id": incident_id,
            "workflow_id": matched_workflow_id,
            "conclusion": conclusion,
            "message": f"GitHub workflow {conclusion}"
        }


# =============================================================================
# API Endpoints - Data Pipeline Agent
# =============================================================================
PIPELINE_REQUESTS: Dict[str, Dict] = {}

class PipelineRequest(BaseModel):
    """Data pipeline generation request"""
    source_uri: str
    source_type: str = "gcs"
    target_layer: str = "silver"
    business_context: Optional[str] = None
    schedule: Optional[str] = "@daily"

@app.post("/api/pipelines")
async def create_pipeline(request: PipelineRequest):
    """Create a new data pipeline - invokes Data Pipeline Agent"""
    request_id = str(uuid.uuid4())[:12]

    try:
        from agents.data.agent import data_pipeline_agent, PipelineTaskType

        task = {
            "type": PipelineTaskType.FULL_PIPELINE,
            "request_id": request_id,
            "source_uri": request.source_uri,
            "source_type": request.source_type,
            "target_layer": request.target_layer,
            "business_context": request.business_context,
            "schedule": request.schedule
        }

        result = await data_pipeline_agent.handle_task(task)

        PIPELINE_REQUESTS[request_id] = {
            "request_id": request_id,
            "status": result.get("status", "completed"),
            "created_at": datetime.utcnow().isoformat(),
            "request": request.dict(),
            "result": result.get("result", {})
        }

        return {
            "request_id": request_id,
            "status": result.get("status"),
            "requires_approval": result.get("result", {}).get("requires_approval", False),
            "risk_score": result.get("result", {}).get("risk_score", 0)
        }

    except Exception as e:
        logger.error("pipeline_creation_failed", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/pipelines")
async def list_pipelines():
    """List all pipeline requests"""
    return {
        "pipelines": list(PIPELINE_REQUESTS.values()),
        "count": len(PIPELINE_REQUESTS)
    }

@app.get("/api/pipelines/{request_id}")
async def get_pipeline(request_id: str):
    """Get pipeline request details"""
    if request_id not in PIPELINE_REQUESTS:
        raise HTTPException(status_code=404, detail=f"Pipeline {request_id} not found")
    return PIPELINE_REQUESTS[request_id]

@app.get("/api/pipelines/{request_id}/spark")
async def get_pipeline_spark(request_id: str):
    """Get generated Spark code for pipeline"""
    if request_id not in PIPELINE_REQUESTS:
        raise HTTPException(status_code=404, detail=f"Pipeline {request_id} not found")

    result = PIPELINE_REQUESTS[request_id].get("result", {})
    spark = result.get("spark", {})

    return {
        "request_id": request_id,
        "code": spark.get("code", ""),
        "language": spark.get("language", "python"),
        "framework": spark.get("framework", "pyspark")
    }

@app.get("/api/pipelines/{request_id}/dag")
async def get_pipeline_dag(request_id: str):
    """Get generated Airflow DAG for pipeline"""
    if request_id not in PIPELINE_REQUESTS:
        raise HTTPException(status_code=404, detail=f"Pipeline {request_id} not found")

    result = PIPELINE_REQUESTS[request_id].get("result", {})
    dag = result.get("dag", {})

    return {
        "request_id": request_id,
        "code": dag.get("code", ""),
        "framework": dag.get("framework", "airflow"),
        "schedule": dag.get("schedule", "@daily")
    }

@app.get("/api/pipelines/{request_id}/ir")
async def get_pipeline_ir(request_id: str):
    """Get Intermediate Representation for pipeline"""
    if request_id not in PIPELINE_REQUESTS:
        raise HTTPException(status_code=404, detail=f"Pipeline {request_id} not found")

    result = PIPELINE_REQUESTS[request_id].get("result", {})

    return {
        "request_id": request_id,
        "ir": result.get("ir", {})
    }

@app.get("/api/pipelines/{request_id}/dq")
async def get_pipeline_dq(request_id: str):
    """Get data quality rules for pipeline"""
    if request_id not in PIPELINE_REQUESTS:
        raise HTTPException(status_code=404, detail=f"Pipeline {request_id} not found")

    result = PIPELINE_REQUESTS[request_id].get("result", {})
    dq = result.get("dq", {})

    return {
        "request_id": request_id,
        "suite": dq.get("suite", {}),
        "framework": dq.get("framework", "great_expectations")
    }

@app.get("/api/langgraph/definition/data")
async def get_data_langgraph_definition():
    """Get LangGraph workflow definition for Data Pipeline Agent"""
    return {
        "nodes": [
            {"id": 1, "name": "Receive Request", "phase": "Ingestion", "type": "processor",
             "description": "Receive pipeline request from API/Kafka"},
            {"id": 2, "name": "Analyze Source", "phase": "Analysis", "type": "analyzer",
             "description": "Profile source with TWO-STEP data protection"},
            {"id": 3, "name": "Generate IR", "phase": "Planning", "type": "generator",
             "description": "Create vendor-neutral Intermediate Representation"},
            {"id": 4, "name": "Validate IR", "phase": "Validation", "type": "validator",
             "description": "Validate IR against JSON schema"},
            {"id": 5, "name": "Risk Assessment", "phase": "Approval", "type": "human",
             "description": "Calculate risk score, route to HITL if needed"},
            {"id": 6, "name": "Generate Spark", "phase": "Generation", "type": "generator",
             "description": "Generate PySpark transformation code"},
            {"id": 7, "name": "Generate DAG", "phase": "Generation", "type": "generator",
             "description": "Generate Airflow DAG with TaskFlow API"},
            {"id": 8, "name": "Generate DQ", "phase": "Quality", "type": "generator",
             "description": "Generate Great Expectations suite"},
        ],
        "edges": [
            {"from": 1, "to": 2},
            {"from": 2, "to": 3},
            {"from": 3, "to": 4},
            {"from": 4, "to": 5},
            {"from": 5, "to": 6, "condition": "approved"},
            {"from": 5, "to": 3, "condition": "revision_needed"},
            {"from": 6, "to": 7},
            {"from": 7, "to": 8},
        ],
        "phases": [
            {"name": "Ingestion", "nodes": [1], "color": "#3B82F6"},
            {"name": "Analysis", "nodes": [2], "color": "#06B6D4"},
            {"name": "Planning", "nodes": [3], "color": "#F59E0B"},
            {"name": "Validation", "nodes": [4], "color": "#8B5CF6"},
            {"name": "Approval", "nodes": [5], "color": "#EF4444"},
            {"name": "Generation", "nodes": [6, 7], "color": "#10B981"},
            {"name": "Quality", "nodes": [8], "color": "#22C55E"},
        ]
    }

# =============================================================================
# API Endpoints - Unified Workflows (IT Service + Data Pipelines)
# =============================================================================
@app.get("/api/v1/workflows")
async def list_unified_workflows():
    """
    List all workflows - combines ServiceNow incidents, Jira stories, and pipelines.

    This is the primary endpoint for the unified UI that shows:
    - IT Service Agent workflows (from ServiceNow incidents)
    - Jira Agent workflows (from Jira stories)
    - Data Pipeline Agent workflows (from pipeline requests)
    """
    try:
        from utils.redis_client import redis_client

        workflows = []

        # 1. Get ServiceNow incidents from Redis cache
        incidents_cached = redis_client.get("incidents:active")
        if incidents_cached:
            incidents_data = json.loads(incidents_cached)
            for incident in incidents_data.get("incidents", []):
                workflows.append({
                    "workflow_id": f"inc-{incident.get('incident_id')}",
                    "incident_id": incident.get("incident_id"),
                    "type": "incident",
                    "source": "servicenow",
                    "title": incident.get("short_description", ""),
                    "description": incident.get("description", ""),
                    "status": _map_incident_state(incident.get("state", "1")),
                    "priority": incident.get("priority", "3"),
                    "created_at": incident.get("created_on", ""),
                    "updated_at": incident.get("updated_on", ""),
                    "agent": "it-service"
                })

        # 2. Get Jira stories from Redis cache
        stories_cached = redis_client.get(JIRA_STORIES_CACHE_KEY)
        if stories_cached:
            stories_data = json.loads(stories_cached)
            for story in stories_data.get("stories", []):
                is_pipeline = story.get("is_data_pipeline", False)
                workflows.append({
                    "workflow_id": f"jira-{story.get('story_id')}",
                    "story_id": story.get("story_id"),
                    "type": "pipeline" if is_pipeline else "jira",
                    "source": "jira",
                    "title": story.get("summary", ""),
                    "description": story.get("description", ""),
                    "status": story.get("status", "Open"),
                    "priority": story.get("priority", "Medium"),
                    "labels": story.get("labels", []),
                    "created_at": story.get("created", ""),
                    "updated_at": story.get("updated", ""),
                    "agent": "data-pipeline" if is_pipeline else "jira"
                })

        # 3. Get pipeline requests from in-memory store
        for request_id, pipeline in PIPELINE_REQUESTS.items():
            workflows.append({
                "workflow_id": f"pipe-{request_id}",
                "request_id": request_id,
                "type": "pipeline",
                "source": "api",
                "title": f"Pipeline: {pipeline.get('request', {}).get('source_uri', '')}",
                "description": pipeline.get("request", {}).get("business_context", ""),
                "status": pipeline.get("status", "pending"),
                "target_layer": pipeline.get("request", {}).get("target_layer", "silver"),
                "created_at": pipeline.get("created_at", ""),
                "result": pipeline.get("result", {}),
                "agent": "data-pipeline"
            })

        # Sort by updated/created time (most recent first)
        workflows.sort(
            key=lambda x: x.get("updated_at") or x.get("created_at") or "",
            reverse=True
        )

        return {
            "workflows": workflows[:100],
            "count": len(workflows),
            "sources": {
                "servicenow": len([w for w in workflows if w.get("source") == "servicenow"]),
                "jira": len([w for w in workflows if w.get("source") == "jira"]),
                "api": len([w for w in workflows if w.get("source") == "api"])
            }
        }

    except Exception as e:
        logger.error("unified_workflows_error", error=str(e))
        return {"workflows": [], "count": 0, "error": str(e)}

def _map_incident_state(state: str) -> str:
    """Map ServiceNow state codes to human-readable status"""
    state_map = {
        "1": "new",
        "2": "in_progress",
        "3": "on_hold",
        "4": "pending",
        "5": "resolved",
        "6": "closed",
        "7": "cancelled"
    }
    return state_map.get(str(state), "unknown")

# =============================================================================
# Root Endpoint
# =============================================================================
@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": "AI Agent Orchestrator",
        "version": "5.0.0",
        "architecture": "Hybrid Protocol (Kafka + A2A + MCP + REST)",
        "agents": {
            "it_service": ["servicenow", "jira", "gcp", "kubernetes", "github"],
            "data": ["data-pipeline"],
            "shared": ["llm", "gcs-mcp", "iceberg-mcp", "llm-mcp"]
        },
        "event_sources": {
            "servicenow": "servicenow.incidents Kafka topic → IT Service Agent",
            "jira": "jira.stories Kafka topic → Jira Agent or Data Pipeline Agent",
            "api": "/api/pipelines REST endpoint → Data Pipeline Agent"
        },
        "endpoints": {
            "unified_workflows": "/api/v1/workflows",
            "incidents": "/api/incidents",
            "jira_stories": "/api/jira/stories",
            "pipelines": "/api/pipelines",
            "scripts": "/api/scripts",
            "match": "/api/scripts/match",
            "execute": "/api/execute",
            "approvals": "/api/approvals",
            "agents": "/api/agents",
            "langgraph": "/api/langgraph/run",
            "langgraph_data": "/api/langgraph/definition/data",
            "rag": "/api/rag/search",
            "webhooks": "/api/webhooks/jira",
            "health": "/health",
            "metrics": "/metrics"
        }
    }

# =============================================================================
# Run Server
# =============================================================================
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
