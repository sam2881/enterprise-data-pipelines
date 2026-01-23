"""
AI Agent Platform - Control Plane API (v6.0 Event-Driven Architecture)

WHY: FastAPI is the CONTROL PLANE only. It:
     - Serves the UI (reads from Redis/Postgres - CQRS)
     - Handles human approvals (publishes to Kafka)
     - Provides policy management
     - Does NOT directly execute workflows

ARCHITECTURE:
    FastAPI → Kafka (approvals) → EventOrchestrator → LangGraph
    FastAPI ← Redis/Postgres ← Kafka Consumers (status updates)

USAGE:
    # Run with uvicorn
    uvicorn app:create_app --factory --host 0.0.0.0 --port 8000

    # Or directly
    python app.py
"""
import asyncio
import logging
import os
import sys
import time
from contextlib import asynccontextmanager
from datetime import datetime
from typing import AsyncGenerator

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv("/home/samrattidke600/ai_agent_app/.env")

from fastapi import FastAPI, Request, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import json
import re
from typing import List, Dict, Any, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


# =============================================================================
# RAG-based Script Selector
# =============================================================================

class ScriptSelector:
    """
    RAG-based script selector that uses registry.json to match incidents to scripts.
    Uses keyword matching, error pattern matching, and LLM for intelligent selection.
    """

    def __init__(self, registry_path: str = "/home/samrattidke600/ai_agent_app/registry.json"):
        self.registry_path = registry_path
        self.registry = self._load_registry()
        self.scripts = self.registry.get("scripts", [])
        logger.info(f"ScriptSelector initialized with {len(self.scripts)} scripts")

    def _load_registry(self) -> Dict[str, Any]:
        """Load the registry.json file"""
        try:
            with open(self.registry_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load registry: {e}")
            return {"scripts": []}

    def match_scripts(self, incident_description: str, category: str = None,
                      max_results: int = 3) -> List[Dict[str, Any]]:
        """
        Match scripts to an incident using keyword and error pattern matching.

        Args:
            incident_description: The incident description text
            category: Optional category filter (e.g., "compute", "storage")
            max_results: Maximum number of scripts to return

        Returns:
            List of matched scripts with scores
        """
        incident_lower = incident_description.lower()
        scored_scripts = []

        for script in self.scripts:
            score = 0.0
            match_reasons = []

            # Keyword matching (40% weight)
            keywords = script.get("keywords", [])
            keyword_matches = sum(1 for kw in keywords if kw.lower() in incident_lower)
            if keyword_matches > 0:
                keyword_score = min(keyword_matches / max(len(keywords), 1), 1.0) * 0.40
                score += keyword_score
                match_reasons.append(f"keywords: {keyword_matches} matches")

            # Error pattern matching (35% weight)
            error_patterns = script.get("error_patterns", [])
            pattern_matches = 0
            for pattern in error_patterns:
                if re.search(pattern, incident_lower, re.IGNORECASE):
                    pattern_matches += 1
            if pattern_matches > 0:
                pattern_score = min(pattern_matches / max(len(error_patterns), 1), 1.0) * 0.35
                score += pattern_score
                match_reasons.append(f"error patterns: {pattern_matches} matches")

            # Category matching (15% weight)
            if category:
                script_category = script.get("category", "")
                if category.lower() in script_category.lower() or script_category.lower() in category.lower():
                    score += 0.15
                    match_reasons.append(f"category match: {script_category}")

            # Service matching (10% weight)
            service = script.get("service", "")
            if service.lower() in incident_lower:
                score += 0.10
                match_reasons.append(f"service match: {service}")

            if score > 0:
                scored_scripts.append({
                    "script": script,
                    "score": round(score, 3),
                    "match_reasons": match_reasons,
                    "name": script.get("name"),
                    "path": script.get("path"),
                    "workflow": script.get("workflow"),
                    "risk_level": script.get("risk_level"),
                    "description": script.get("description"),
                    "type": script.get("type"),
                })

        # Sort by score descending
        scored_scripts.sort(key=lambda x: x["score"], reverse=True)

        return scored_scripts[:max_results]

    def get_best_script(self, incident_description: str, category: str = None) -> Optional[Dict[str, Any]]:
        """Get the best matching script for an incident"""
        matches = self.match_scripts(incident_description, category, max_results=1)
        return matches[0] if matches else None

    def get_script_by_action(self, action: str, component: str = None) -> Optional[Dict[str, Any]]:
        """Get a script by action type (e.g., 'start', 'stop', 'restart')"""
        for script in self.scripts:
            script_action = script.get("action", "")
            script_component = script.get("component", "")

            if action.lower() == script_action.lower():
                if component is None or component.lower() in script_component.lower():
                    return {
                        "script": script,
                        "name": script.get("name"),
                        "path": script.get("path"),
                        "workflow": script.get("workflow"),
                        "risk_level": script.get("risk_level"),
                        "description": script.get("description"),
                    }
        return None


# Global script selector instance
script_selector = ScriptSelector()


# =============================================================================
# Application Lifecycle
# =============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator:
    """
    Application lifecycle manager.

    Handles startup and shutdown events:
    - Initialize configuration
    - Register workflow handlers
    - Start Kafka consumers
    - Connect to databases
    """
    logger.info("Starting AI Agent Platform...")

    # Import here to avoid circular imports
    from backend.config.settings import get_settings
    from backend.control_plane import (
        get_control_plane,
        WorkflowType,
    )
    from backend.control_plane.handlers import ITServiceHandler, DataPipelineHandler, get_data_agent_handler
    from backend.streaming.event_publisher import get_publisher

    settings = get_settings()
    logger.info(f"Environment: {settings.environment.value}")
    logger.info(f"GCP Project: {settings.gcp_project_id}")

    # Initialize control plane and register handlers
    control_plane = get_control_plane()

    it_service_handler = ITServiceHandler(settings)
    data_pipeline_handler = DataPipelineHandler(settings)

    control_plane.register_handler(WorkflowType.IT_SERVICE, it_service_handler)
    control_plane.register_handler(WorkflowType.DATA_PIPELINE, data_pipeline_handler)

    # Register event publisher callback
    publisher = get_publisher()
    control_plane.add_event_callback(
        lambda event: publisher.publish_workflow_event(
            event_type=event["event_type"],
            workflow_id=event["workflow_id"],
            data=event["data"],
        )
    )

    logger.info("Control plane initialized with handlers")

    # Initialize Data Agent handler
    data_agent_handler = get_data_agent_handler()
    logger.info("Data Agent handler initialized")

    # Store in app state for access in routes
    app.state.settings = settings
    app.state.control_plane = control_plane
    app.state.publisher = publisher
    app.state.data_agent_handler = data_agent_handler

    logger.info("AI Agent Platform started successfully")

    yield

    # Shutdown
    logger.info("Shutting down AI Agent Platform...")
    await publisher.close()
    logger.info("Shutdown complete")


# =============================================================================
# Application Factory
# =============================================================================

def create_app() -> FastAPI:
    """
    Create and configure the FastAPI application.

    Returns:
        Configured FastAPI application
    """
    app = FastAPI(
        title="AI Agent Platform",
        description="Agentic AI Enterprise Data Engineering & Migration Platform",
        version="2.0.0",
        lifespan=lifespan,
    )

    # CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # Configure for production
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Register routes
    register_routes(app)

    return app


# =============================================================================
# Route Registration
# =============================================================================

def register_routes(app: FastAPI) -> None:
    """Register all API routes."""

    # Health check
    @app.get("/health")
    async def health_check():
        """Health check endpoint."""
        return {
            "status": "healthy",
            "service": "ai-agent-platform",
            "version": "2.0.0",
        }

    # Metrics
    @app.get("/metrics")
    async def metrics(request: Request):
        """Prometheus metrics endpoint."""
        publisher = request.app.state.publisher
        return {
            "kafka_publisher": publisher.get_metrics(),
        }

    # ==========================================================================
    # Workflow API
    # ==========================================================================

    @app.post("/api/v2/workflows")
    async def start_workflow(request: Request):
        """
        Start a new workflow via Kafka event.

        EVENT-DRIVEN: This endpoint publishes workflow.requested to Kafka.
        The EventOrchestrator consumes this and triggers the appropriate LangGraph workflow.

        Body:
            {
                "workflow_type": "it_service" | "data_pipeline",
                "source_id": "INC001" | "DATA-123",
                "source_system": "servicenow" | "jira",
                "metadata": {...}
            }
        """
        from backend.streaming.kafka_producer import get_producer
        from backend.streaming.schemas import Topics
        import uuid

        body = await request.json()
        workflow_type = body.get("workflow_type", "it_service")
        source_id = body.get("source_id", "")
        source_system = body.get("source_system", "")
        metadata = body.get("metadata", {})

        if workflow_type not in ["it_service", "data_pipeline"]:
            raise HTTPException(400, "Invalid workflow_type. Must be 'it_service' or 'data_pipeline'")

        producer = get_producer()
        workflow_id = str(uuid.uuid4())

        # Determine topic based on workflow type
        if workflow_type == "it_service":
            topic = Topics.INCIDENT_CREATED
            event = {
                "event_type": "incident.created",
                "incident_id": source_id,
                "workflow_id": workflow_id,
                "source_system": source_system,
                "metadata": metadata,
                "timestamp": datetime.now().isoformat(),
            }
        else:
            topic = Topics.PIPELINE_REQUESTED
            event = {
                "event_type": "pipeline.requested",
                "pipeline_id": source_id,
                "request_id": workflow_id,
                "source_system": source_system,
                "metadata": metadata,
                "timestamp": datetime.now().isoformat(),
            }

        # Publish to Kafka - EventOrchestrator will consume and trigger workflow
        success = await producer.publish_event(
            topic=topic,
            event=event,
            key=source_id
        )

        if not success:
            raise HTTPException(500, "Failed to publish workflow event to Kafka")

        logger.info(f"Workflow event published: {workflow_type} for {source_id}")

        return {
            "workflow_id": workflow_id,
            "status": "accepted",
            "message": "Event published to Kafka. EventOrchestrator will process.",
        }

    @app.get("/api/v2/workflows/{workflow_id}")
    async def get_workflow_status(workflow_id: str, request: Request):
        """Get workflow status."""
        control_plane = request.app.state.control_plane

        try:
            return await control_plane.get_workflow_status(workflow_id)
        except ValueError as e:
            raise HTTPException(404, str(e))

    @app.get("/api/v2/workflows/approvals/pending")
    async def get_pending_approvals(request: Request):
        """Get all pending approval requests."""
        control_plane = request.app.state.control_plane
        return await control_plane.get_pending_approvals()

    @app.post("/api/v2/workflows/{workflow_id}/approve")
    async def approve_workflow(workflow_id: str, request: Request):
        """Approve a pending workflow."""
        body = await request.json()
        control_plane = request.app.state.control_plane

        approved_by = body.get("approved_by", "admin")

        try:
            return await control_plane.approve_workflow(workflow_id, approved_by)
        except ValueError as e:
            raise HTTPException(404, str(e))

    @app.post("/api/v2/workflows/{workflow_id}/reject")
    async def reject_workflow(workflow_id: str, request: Request):
        """Reject a pending workflow."""
        body = await request.json()
        control_plane = request.app.state.control_plane

        rejected_by = body.get("rejected_by", "admin")
        reason = body.get("reason", "No reason provided")

        try:
            return await control_plane.reject_workflow(workflow_id, rejected_by, reason)
        except ValueError as e:
            raise HTTPException(404, str(e))

    # ==========================================================================
    # Incident API (ServiceNow Integration)
    # ==========================================================================

    @app.post("/api/v2/incidents")
    async def create_incident_workflow(request: Request):
        """
        Create workflow from ServiceNow incident via Kafka event.

        EVENT-DRIVEN: This endpoint publishes incident.created to Kafka.
        The EventOrchestrator consumes this and triggers the LangGraph workflow.

        Body:
            {
                "incident_id": "INC001",
                "incident": {
                    "number": "INC001",
                    "short_description": "Server down",
                    "description": "Production server unresponsive",
                    "priority": "high",
                    "category": "infrastructure"
                }
            }
        """
        from backend.streaming.kafka_producer import get_producer
        from backend.streaming.schemas import Topics
        import uuid

        body = await request.json()
        incident_id = body.get("incident_id", "")
        incident_data = body.get("incident", {})

        if not incident_id:
            raise HTTPException(400, "incident_id is required")

        producer = get_producer()
        workflow_id = str(uuid.uuid4())

        # Publish incident.created event to Kafka
        event = {
            "event_type": "incident.created",
            "incident_id": incident_id,
            "workflow_id": workflow_id,
            "source_system": "servicenow",
            "raw_incident": incident_data,
            "number": incident_data.get("number", incident_id),
            "short_description": incident_data.get("short_description", ""),
            "description": incident_data.get("description", ""),
            "priority": incident_data.get("priority", "medium"),
            "category": incident_data.get("category", ""),
            "timestamp": datetime.now().isoformat(),
        }

        success = await producer.publish_event(
            topic=Topics.INCIDENT_CREATED,
            event=event,
            key=incident_id
        )

        if not success:
            raise HTTPException(500, "Failed to publish incident event to Kafka")

        logger.info(f"Incident event published: {incident_id}")

        return {
            "workflow_id": workflow_id,
            "incident_id": incident_id,
            "status": "accepted",
            "message": "Event published to Kafka. EventOrchestrator will process.",
        }

    @app.get("/api/incidents")
    async def list_incidents(request: Request):
        """
        List all incidents from Redis cache (EVENT-DRIVEN ARCHITECTURE).

        Flow:
        1. GCP Monitor → ServiceNow incident → Kafka (gcp.alerts)
        2. Kafka Consumer → Reads events → Writes to Redis cache
        3. This API → Reads from Redis cache

        Fallback chain:
        1. Redis cache (event-driven source)
        2. ServiceNow direct API
        3. Mock data for demo/development

        Returns:
            {
                "incidents": [...],
                "count": N,
                "source": "redis" | "servicenow" | "mock"
            }
        """
        import redis
        import httpx
        import json as json_lib

        # Check if demo mode is enabled - default FALSE to use real ServiceNow
        demo_mode = os.getenv("DEMO_MODE", "false").lower() == "true"

        # Load .env file for ServiceNow credentials
        from dotenv import load_dotenv
        load_dotenv("/home/samrattidke600/ai_agent_app/.env")

        # Try Redis first (event-driven source)
        try:
            redis_client = redis.Redis(
                host=os.getenv("REDIS_HOST", "localhost"),
                port=int(os.getenv("REDIS_PORT", "6379")),
                decode_responses=True
            )

            # Check for cached incidents
            cached_data = redis_client.get("incidents:active")
            if cached_data:
                data = json_lib.loads(cached_data)
                incidents = data.get("incidents", [])
                if incidents:
                    logger.info(f"Returning {len(incidents)} incidents from Redis cache")
                    return {
                        "incidents": incidents,
                        "count": len(incidents),
                        "source": "redis",
                        "cached_at": data.get("cached_at"),
                    }
        except Exception as e:
            logger.warning(f"Redis cache unavailable: {e}")

        # Try ServiceNow if credentials are configured
        snow_pass = os.getenv("SNOW_PASSWORD", "")
        if snow_pass and not demo_mode:
            logger.info("Fetching from ServiceNow...")
            snow_url = os.getenv("SNOW_INSTANCE_URL", "https://dev349960.service-now.com")
            snow_user = os.getenv("SNOW_USERNAME", "admin")

            try:
                async with httpx.AsyncClient() as client:
                    response = await client.get(
                        f"{snow_url}/api/now/table/incident",
                        params={
                            "sysparm_limit": 50,
                            "sysparm_query": "ORDERBYDESCsys_created_on",
                            "sysparm_fields": "number,sys_id,short_description,description,priority,state,urgency,impact,category,subcategory,sys_created_on,sys_updated_on,assigned_to"
                        },
                        auth=(snow_user, snow_pass),
                        headers={"Accept": "application/json"},
                        timeout=30.0
                    )

                    if response.status_code == 200:
                        data = response.json()
                        incidents = data.get("result", [])

                        # Map ServiceNow fields to our API format
                        formatted_incidents = []
                        for inc in incidents:
                            formatted_incidents.append({
                                "id": inc.get("sys_id", ""),
                                "incident_id": inc.get("number", ""),
                                "number": inc.get("number", ""),
                                "short_description": inc.get("short_description", ""),
                                "description": inc.get("description", ""),
                                "priority": inc.get("priority", "3"),
                                "status": _map_snow_state(inc.get("state", "1")),
                                "category": inc.get("category", ""),
                                "assigned_to": inc.get("assigned_to", ""),
                                "created_at": inc.get("sys_created_on", ""),
                                "updated_at": inc.get("sys_updated_on", ""),
                            })

                        return {
                            "incidents": formatted_incidents,
                            "count": len(formatted_incidents),
                            "source": "servicenow"
                        }
                    else:
                        logger.warning(f"ServiceNow API error: {response.status_code}")

            except Exception as e:
                logger.warning(f"ServiceNow connection failed: {e}")

        # Return mock data for demo mode
        logger.info("Returning mock incident data for demo mode")
        mock_incidents = _get_mock_incidents()
        return {
            "incidents": mock_incidents,
            "count": len(mock_incidents),
            "source": "mock",
            "demo_mode": True,
        }

    def _get_mock_incidents():
        """Generate realistic mock incident data for demo mode."""
        from datetime import timedelta
        import random

        categories = ["Infrastructure", "Application", "Database", "Network", "Security", "Cloud"]
        services = ["api-gateway", "auth-service", "payment-service", "user-service", "data-pipeline", "kubernetes"]
        descriptions = [
            ("Production API Gateway High Latency", "Response times exceeding 5 seconds on production API gateway. Multiple customers reporting slow page loads."),
            ("Database Connection Pool Exhausted", "PostgreSQL connection pool reaching maximum limit. New connections being rejected."),
            ("Kubernetes Pod CrashLoopBackOff", "Payment service pods restarting repeatedly. OOMKilled errors detected."),
            ("SSL Certificate Expiring Soon", "SSL certificate for api.company.com expires in 7 days. Renewal required."),
            ("Memory Leak in Auth Service", "Auth service memory usage growing steadily. Current: 85% and climbing."),
            ("Failed Spark Job in Data Pipeline", "Nightly ETL job failed at bronze to silver transformation stage."),
            ("High CPU Usage on GCP VM", "Production VM instance-prod-01 CPU at 95%. Autoscaling not triggered."),
            ("Redis Cache Miss Rate Spike", "Cache hit rate dropped from 95% to 60%. Database under heavy load."),
            ("Network Packet Loss Detected", "10% packet loss on internal network. Affecting service communication."),
            ("Log Aggregation Pipeline Failure", "Fluentd failing to forward logs to Elasticsearch. Logs accumulating."),
        ]

        incidents = []
        now = datetime.now()

        for i, (short_desc, full_desc) in enumerate(descriptions):
            priority = str(random.choice([1, 1, 2, 2, 2, 3, 3, 3, 4]))
            # More critical incidents are pending approval
            if priority in ["1", "2"] and i < 4:
                status = "pending_approval"
            elif i < 2:
                status = "1"  # New
            elif i < 5:
                status = "2"  # In Progress
            elif i < 7:
                status = "3"  # On Hold
            else:
                status = "6"  # Resolved

            created_at = (now - timedelta(hours=random.randint(1, 72))).isoformat()
            updated_at = (now - timedelta(minutes=random.randint(5, 180))).isoformat()

            incidents.append({
                "id": f"sys-{i+1:08d}",
                "incident_id": f"INC00{10001 + i}",
                "number": f"INC00{10001 + i}",
                "short_description": short_desc,
                "description": full_desc,
                "priority": priority,
                "status": status,
                "category": random.choice(categories),
                "affected_service": random.choice(services),
                "assigned_to": random.choice(["john.doe", "jane.smith", "ops-team", "sre-oncall"]),
                "created_at": created_at,
                "updated_at": updated_at,
            })

        # Sort by priority (critical first) then by created_at
        incidents.sort(key=lambda x: (x["priority"], x["created_at"]))
        return incidents

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

    @app.get("/api/incidents/{incident_id}")
    async def get_incident(request: Request, incident_id: str):
        """
        Get incident details by ID.

        Returns incident details and workflow status.
        """
        from backend.infrastructure.postgres import get_postgres_pool

        try:
            pool = await get_postgres_pool()
            async with pool.acquire() as conn:
                # Try to find by incident_id or id
                row = await conn.fetchrow("""
                    SELECT
                        id, incident_id, number, short_description,
                        description, priority, status, category,
                        assigned_to, remediation_script, remediation_result,
                        created_at, updated_at
                    FROM incidents
                    WHERE incident_id = $1 OR id::text = $1
                """, incident_id)

                if not row:
                    raise HTTPException(404, f"Incident {incident_id} not found")

                incident = dict(row)

                # Get associated workflow status from control plane if available
                try:
                    control_plane = request.app.state.control_plane
                    workflow = control_plane.get_workflow_by_source(incident_id)
                    if workflow:
                        incident["workflow_id"] = workflow.id
                        incident["workflow_status"] = workflow.status.value
                except Exception:
                    pass

                return incident

        except HTTPException:
            raise
        except Exception as e:
            logger.warning(f"Failed to get incident {incident_id}: {e}")
            raise HTTPException(500, f"Failed to retrieve incident: {e}")

    @app.post("/api/incidents/{incident_id}/close")
    async def close_incident(request: Request, incident_id: str, resolution: str = "Resolved"):
        """
        Close an incident with resolution.

        Query params:
            resolution: Resolution message (default: "Resolved")

        Returns:
            Closed incident details
        """
        from backend.infrastructure.postgres import get_postgres_pool

        try:
            pool = await get_postgres_pool()
            async with pool.acquire() as conn:
                # Update incident status
                row = await conn.fetchrow("""
                    UPDATE incidents
                    SET status = 'closed',
                        updated_at = NOW()
                    WHERE incident_id = $1 OR id::text = $1
                    RETURNING id, incident_id, number, status, updated_at
                """, incident_id)

                if not row:
                    raise HTTPException(404, f"Incident {incident_id} not found")

                incident = dict(row)

                # Publish close event
                try:
                    publisher = request.app.state.publisher
                    from backend.streaming.event_publisher import IncidentEventType
                    await publisher.publish_incident_event(
                        event_type=IncidentEventType.RESOLVED.value,
                        incident_id=incident_id,
                        incident_number=incident.get("number", ""),
                        data={"resolution": resolution},
                    )
                except Exception as e:
                    logger.warning(f"Failed to publish close event: {e}")

                return {
                    **incident,
                    "resolution": resolution,
                    "message": f"Incident {incident_id} closed successfully",
                }

        except HTTPException:
            raise
        except Exception as e:
            logger.warning(f"Failed to close incident {incident_id}: {e}")
            raise HTTPException(500, f"Failed to close incident: {e}")

    # ==========================================================================
    # Incident Replay API - For failure recovery
    # ==========================================================================

    @app.post("/api/v1/incidents/{incident_id}/replay")
    async def replay_incident(incident_id: str, request: Request):
        """
        Replay an incident workflow from the beginning or a specific stage.

        USE CASES:
        1. Workflow failed mid-execution - replay from failed stage
        2. Need to re-run remediation with updated scripts
        3. Manual trigger for testing/debugging

        HOW IT WORKS:
        1. Fetches incident from ServiceNow (source of truth)
        2. Publishes incident.created event to Kafka
        3. EventOrchestrator picks up and triggers LangGraph workflow
        4. Workflow runs through all stages (classify → RAG → plan → execute)

        Body (optional):
            {
                "from_stage": "classify" | "rag" | "plan" | "execute",
                "reason": "Manual replay for testing"
            }

        Returns:
            {
                "status": "replayed",
                "workflow_id": "new-uuid",
                "incident_id": "INC001",
                "from_stage": "classify"
            }
        """
        from backend.streaming.kafka_producer import get_producer
        from backend.streaming.schemas import Topics
        import uuid
        import httpx

        body = await request.json() if request.headers.get("content-type") == "application/json" else {}
        from_stage = body.get("from_stage", "classify")  # Default: start from beginning
        reason = body.get("reason", "Manual replay")

        # Fetch incident from ServiceNow
        snow_url = os.getenv("SNOW_INSTANCE_URL", "https://dev349960.service-now.com")
        snow_user = os.getenv("SNOW_USERNAME", "admin")
        snow_pass = os.getenv("SNOW_PASSWORD", "")

        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{snow_url}/api/now/table/incident",
                    params={
                        "sysparm_query": f"number={incident_id}",
                        "sysparm_limit": 1,
                        "sysparm_fields": "number,sys_id,short_description,description,priority,state,category"
                    },
                    auth=(snow_user, snow_pass),
                    headers={"Accept": "application/json"},
                    timeout=30.0
                )

                if response.status_code != 200:
                    raise HTTPException(500, f"Failed to fetch incident from ServiceNow: {response.status_code}")

                incidents = response.json().get("result", [])
                if not incidents:
                    raise HTTPException(404, f"Incident {incident_id} not found in ServiceNow")

                incident_data = incidents[0]

        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(500, f"Failed to fetch incident: {e}")

        # Publish replay event to Kafka
        producer = get_producer()
        workflow_id = str(uuid.uuid4())

        event = {
            "event_type": "incident.replay",
            "incident_id": incident_id,
            "workflow_id": workflow_id,
            "source_system": "servicenow",
            "replay": True,
            "from_stage": from_stage,
            "reason": reason,
            "raw_incident": incident_data,
            "number": incident_data.get("number", incident_id),
            "short_description": incident_data.get("short_description", ""),
            "description": incident_data.get("description", ""),
            "priority": incident_data.get("priority", "medium"),
            "category": incident_data.get("category", ""),
            "timestamp": datetime.now().isoformat(),
        }

        success = await producer.publish_event(
            topic=Topics.INCIDENT_CREATED,
            event=event,
            key=incident_id
        )

        if not success:
            raise HTTPException(500, "Failed to publish replay event to Kafka")

        logger.info(f"Incident replay event published: {incident_id} from stage {from_stage}")

        return {
            "status": "replayed",
            "workflow_id": workflow_id,
            "incident_id": incident_id,
            "from_stage": from_stage,
            "reason": reason,
            "message": "Replay event published to Kafka. EventOrchestrator will process.",
        }

    # ==========================================================================
    # Incident Approval API - EVENT-DRIVEN (publishes to Kafka)
    # ==========================================================================

    @app.post("/api/v1/incidents/{incident_id}/approve")
    async def approve_incident(incident_id: str, request: Request):
        """
        Approve an incident remediation plan.

        EVENT-DRIVEN: This endpoint publishes incident.approved to Kafka.
        The EventOrchestrator consumes this and resumes the LangGraph workflow.

        Body:
            {
                "approved_by": "user@company.com",
                "approval_token": "uuid-from-approval-request"
            }
        """
        from backend.streaming.kafka_producer import get_producer
        from backend.streaming.schemas import Topics

        body = await request.json()
        approved_by = body.get("approved_by", "admin")
        approval_token = body.get("approval_token")

        producer = get_producer()

        # Publish approval event to Kafka
        event = {
            "event_type": "incident.approved",
            "incident_id": incident_id,
            "approved": True,
            "approver": approved_by,
            "approval_token": approval_token,
            "timestamp": datetime.now().isoformat(),
        }

        success = await producer.publish_event(
            topic=Topics.INCIDENT_APPROVED,
            event=event,
            key=incident_id
        )

        if not success:
            raise HTTPException(500, "Failed to publish approval event")

        logger.info(f"Approval event published for incident {incident_id}")

        return {
            "status": "approved",
            "incident_id": incident_id,
            "approved_by": approved_by,
            "message": "Approval sent. EventOrchestrator will resume workflow.",
        }

    @app.post("/api/v1/incidents/{incident_id}/reject")
    async def reject_incident(incident_id: str, request: Request):
        """
        Reject an incident remediation plan.

        EVENT-DRIVEN: This endpoint publishes incident.rejected to Kafka.
        The EventOrchestrator consumes this and updates the workflow status.

        Body:
            {
                "rejected_by": "user@company.com",
                "reason": "Plan not suitable for production",
                "approval_token": "uuid-from-approval-request"
            }
        """
        from backend.streaming.kafka_producer import get_producer
        from backend.streaming.schemas import Topics

        body = await request.json()
        rejected_by = body.get("rejected_by", "admin")
        reason = body.get("reason", "Rejected by human reviewer")
        approval_token = body.get("approval_token")

        producer = get_producer()

        # Publish rejection event to Kafka
        event = {
            "event_type": "incident.rejected",
            "incident_id": incident_id,
            "approved": False,
            "approver": rejected_by,
            "reason": reason,
            "approval_token": approval_token,
            "timestamp": datetime.now().isoformat(),
        }

        success = await producer.publish_event(
            topic=Topics.INCIDENT_REJECTED,
            event=event,
            key=incident_id
        )

        if not success:
            raise HTTPException(500, "Failed to publish rejection event")

        logger.info(f"Rejection event published for incident {incident_id}")

        return {
            "status": "rejected",
            "incident_id": incident_id,
            "rejected_by": rejected_by,
            "reason": reason,
            "message": "Rejection sent. Workflow will be marked as rejected.",
        }

    @app.get("/api/v1/incidents/pending-approval")
    async def get_pending_approval_incidents(request: Request):
        """
        Get all incidents pending human approval.

        Reads from Redis/Postgres (CQRS pattern).
        Status is populated by Kafka consumers processing workflow events.
        """
        from backend.infrastructure.postgres import get_postgres_pool

        try:
            pool = await get_postgres_pool()
            async with pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT
                        id, incident_id, number, short_description,
                        description, priority, status, category,
                        assigned_to, created_at, updated_at
                    FROM incidents
                    WHERE status = 'pending_approval'
                    ORDER BY created_at DESC
                    LIMIT 50
                """)

                incidents = [dict(row) for row in rows]

                return {
                    "pending_approvals": incidents,
                    "count": len(incidents),
                }
        except Exception as e:
            logger.warning(f"Failed to get pending approvals: {e}")
            return {
                "pending_approvals": [],
                "count": 0,
                "error": str(e),
            }

    # ==========================================================================
    # LangGraph Workflow API (Frontend Compatible)
    # ==========================================================================

    @app.get("/api/langgraph/approvals")
    async def get_langgraph_approvals(request: Request):
        """
        Get all LangGraph workflows pending approval.
        This endpoint is used by the frontend to display pending approvals.
        """
        import redis
        import json as json_lib

        try:
            redis_client = redis.Redis(
                host=os.getenv("REDIS_HOST", "localhost"),
                port=int(os.getenv("REDIS_PORT", "6379")),
                decode_responses=True
            )

            # Check for pending approvals in Redis
            pending_keys = redis_client.keys("workflow:pending:*")
            approvals = []

            for key in pending_keys:
                data = redis_client.get(key)
                if data:
                    approval = json_lib.loads(data)
                    approvals.append(approval)

            # If no Redis data, return mock data for demo
            if not approvals:
                approvals = [
                    {
                        "workflow_id": "demo-workflow-001",
                        "incident_id": "INC0010001",
                        "short_description": "Demo: Server CPU High",
                        "status": "pending_approval",
                        "plan": {
                            "steps": [
                                {"step": 1, "action": "Check CPU processes"},
                                {"step": 2, "action": "Restart service if needed"},
                            ],
                            "script_name": "restart_service.sh",
                            "risk_level": "medium",
                        },
                        "judge_score": 0.85,
                        "approval_decision": {
                            "route": "HITL",
                            "risk_level": "medium",
                            "reasoning": "Script execution requires human approval",
                        },
                        "created_at": datetime.now().isoformat(),
                    }
                ]

            return {"approvals": approvals, "count": len(approvals)}

        except Exception as e:
            logger.warning(f"Failed to get LangGraph approvals: {e}")
            # Return demo data on error
            return {
                "approvals": [
                    {
                        "workflow_id": "demo-workflow-001",
                        "incident_id": "INC0010001",
                        "short_description": "Demo: Server CPU High",
                        "status": "pending_approval",
                        "plan": {
                            "steps": [
                                {"step": 1, "action": "Check CPU processes"},
                                {"step": 2, "action": "Restart service if needed"},
                            ],
                            "script_name": "restart_service.sh",
                            "risk_level": "medium",
                        },
                        "judge_score": 0.85,
                        "created_at": datetime.now().isoformat(),
                    }
                ],
                "count": 1,
            }

    @app.get("/api/langgraph/workflow/{workflow_id}")
    async def get_langgraph_workflow(workflow_id: str, request: Request):
        """Get details of a specific LangGraph workflow."""
        control_plane = request.app.state.control_plane

        try:
            status = await control_plane.get_workflow_status(workflow_id)
            return status
        except ValueError:
            # Return demo data if not found
            return {
                "workflow_id": workflow_id,
                "incident_id": workflow_id,
                "status": "pending_approval",
                "current_node": "await_approval",
                "plan": {
                    "steps": [
                        {"step": 1, "action": "Analyze incident"},
                        {"step": 2, "action": "Execute remediation"},
                    ],
                },
                "created_at": datetime.now().isoformat(),
            }

    @app.post("/api/langgraph/approve/{incident_id}")
    async def approve_langgraph_workflow(incident_id: str, request: Request):
        """
        Approve a LangGraph workflow via Kafka event.
        """
        from backend.streaming.kafka_producer import get_producer
        from backend.streaming.schemas import Topics

        body = await request.json()
        approver = body.get("approver", "admin")
        reason = body.get("reason", "Approved")

        producer = get_producer()

        event = {
            "event_type": "incident.approved",
            "incident_id": incident_id,
            "approved": True,
            "approver": approver,
            "reason": reason,
            "timestamp": datetime.now().isoformat(),
        }

        success = await producer.publish_event(
            topic=Topics.INCIDENT_APPROVED,
            event=event,
            key=incident_id
        )

        if not success:
            raise HTTPException(500, "Failed to publish approval event")

        logger.info(f"LangGraph approval published for {incident_id}")

        return {
            "status": "approved",
            "incident_id": incident_id,
            "approver": approver,
            "message": "Workflow approved and will continue execution.",
        }

    @app.post("/api/langgraph/reject/{incident_id}")
    async def reject_langgraph_workflow(incident_id: str, request: Request):
        """
        Reject a LangGraph workflow via Kafka event.
        """
        from backend.streaming.kafka_producer import get_producer
        from backend.streaming.schemas import Topics

        body = await request.json()
        approver = body.get("approver", "admin")
        reason = body.get("reason", "Rejected")

        producer = get_producer()

        event = {
            "event_type": "incident.rejected",
            "incident_id": incident_id,
            "approved": False,
            "approver": approver,
            "reason": reason,
            "timestamp": datetime.now().isoformat(),
        }

        success = await producer.publish_event(
            topic=Topics.INCIDENT_REJECTED,
            event=event,
            key=incident_id
        )

        if not success:
            raise HTTPException(500, "Failed to publish rejection event")

        logger.info(f"LangGraph rejection published for {incident_id}")

        return {
            "status": "rejected",
            "incident_id": incident_id,
            "approver": approver,
            "reason": reason,
            "message": "Workflow rejected.",
        }

    @app.post("/api/langgraph/retry/{incident_id}")
    async def retry_langgraph_workflow(incident_id: str, request: Request):
        """
        Retry a failed LangGraph workflow (e.g., GitHub Actions failed).
        """
        from backend.streaming.kafka_producer import get_producer
        from backend.streaming.schemas import Topics

        producer = get_producer()

        event = {
            "event_type": "incident.retry",
            "incident_id": incident_id,
            "timestamp": datetime.now().isoformat(),
        }

        success = await producer.publish_event(
            topic=Topics.INCIDENT_CREATED,  # Re-trigger workflow
            event=event,
            key=incident_id
        )

        if not success:
            raise HTTPException(500, "Failed to publish retry event")

        logger.info(f"LangGraph retry published for {incident_id}")

        return {
            "status": "retrying",
            "incident_id": incident_id,
            "message": "Workflow retry triggered.",
        }

    @app.post("/api/langgraph/node/{node_id}")
    async def execute_langgraph_node(node_id: int, request: Request):
        """
        Execute a specific LangGraph node for workflow.
        Node 8 (Await Approval) pauses for human approval.
        Node 9 (Execute) triggers GitHub Actions with selected script.
        """
        import time
        import random
        import httpx

        body = await request.json()
        workflow_id = body.get("workflow_id", "")
        incident_id = body.get("incident_id", "")
        script_name = body.get("script_name", "start_gcp_instance.sh")  # Default script from sam2881/test_01

        # Node configuration with processing times
        node_config = {
            1: {"name": "Ingest", "phase": "Ingestion", "delay": 0.5},
            2: {"name": "Parse", "phase": "Ingestion", "delay": 0.8},
            3: {"name": "Classify", "phase": "Classification", "delay": 1.2},
            4: {"name": "Swarm RAG", "phase": "Retrieval", "delay": 2.0},
            5: {"name": "Generate Plan", "phase": "Planning", "delay": 1.5},
            6: {"name": "LLM Judge", "phase": "Validation", "delay": 1.8},
            7: {"name": "Control Plane", "phase": "Approval", "delay": 0.5},
            8: {"name": "Await Approval", "phase": "Approval", "delay": 0.3},
            9: {"name": "Execute", "phase": "Execution", "delay": 1.0},
            10: {"name": "Verify", "phase": "Execution", "delay": 1.0},
            11: {"name": "Close Ticket", "phase": "Completion", "delay": 0.8},
            12: {"name": "Feedback Loop", "phase": "Completion", "delay": 0.5},
        }

        if node_id not in node_config:
            raise HTTPException(404, f"Node {node_id} not found")

        config = node_config[node_id]

        # Simulate processing time
        time.sleep(config["delay"])

        # Get incident description for RAG matching
        incident_description = body.get("incident_description", "")
        if not incident_description:
            # Try to fetch incident details from ServiceNow API if we have incident_id
            incident_description = f"VM terminated incident {incident_id}"

        # Node 4: SWARM RAG - Use script selector to find matching scripts
        if node_id == 4:
            # Use RAG to match scripts based on incident description
            matched_scripts = script_selector.match_scripts(incident_description, max_results=3)

            if matched_scripts:
                top_match = matched_scripts[0]
                # Extract just the filename from the path (e.g., "scripts/start_gcp_instance.sh" -> "start_gcp_instance.sh")
                script_path = top_match.get("path", "start_gcp_instance.sh")
                top_script_name = script_path.split("/")[-1] if "/" in script_path else script_path

                return {
                    "node_id": node_id,
                    "node_name": config["name"],
                    "phase": config["phase"],
                    "workflow_id": workflow_id,
                    "incident_id": incident_id,
                    "status": "completed",
                    "output": {
                        "scripts_matched": len(matched_scripts),
                        "top_script": top_script_name,
                        "similarity_score": top_match.get("score", 0.0),
                        "scripts": [
                            {
                                "name": m.get("name"),
                                "path": m.get("path"),
                                "score": m.get("score"),
                                "risk_level": m.get("risk_level"),
                                "description": m.get("description"),
                                "workflow": m.get("workflow"),
                                "match_reasons": m.get("match_reasons", []),
                            }
                            for m in matched_scripts
                        ],
                        "recommended_script": {
                            "name": top_match.get("name"),
                            "path": top_match.get("path"),
                            "workflow": top_match.get("workflow"),
                            "risk_level": top_match.get("risk_level"),
                            "description": top_match.get("description"),
                        },
                    },
                    "timestamp": datetime.now().isoformat(),
                }
            else:
                # Fallback if no scripts match
                return {
                    "node_id": node_id,
                    "node_name": config["name"],
                    "phase": config["phase"],
                    "workflow_id": workflow_id,
                    "incident_id": incident_id,
                    "status": "completed",
                    "output": {
                        "scripts_matched": 0,
                        "top_script": "start_gcp_instance.sh",
                        "similarity_score": 0.5,
                        "scripts": [],
                        "message": "No exact match found, using default script",
                    },
                    "timestamp": datetime.now().isoformat(),
                }

        # Node 8: AWAIT APPROVAL - Return awaiting_approval status to pause workflow
        if node_id == 8:
            # Get script details from registry for better context
            script_info = None
            for s in script_selector.scripts:
                script_path = s.get("path", "")
                if script_name in script_path or script_path.endswith(script_name):
                    script_info = s
                    break

            # Determine risk level from registry or use default
            risk_level = script_info.get("risk_level", "medium") if script_info else "medium"
            script_description = script_info.get("description", "Execute remediation script") if script_info else "Execute remediation script"
            auto_approve = script_info.get("auto_approve", False) if script_info else False

            return {
                "node_id": node_id,
                "node_name": config["name"],
                "phase": config["phase"],
                "workflow_id": workflow_id,
                "incident_id": incident_id,
                "status": "awaiting_approval",
                "output": {
                    "requires_approval": True,
                    "approval_type": "human_in_the_loop",
                    "risk_level": risk_level,
                    "auto_approve_eligible": auto_approve,
                    "script": {
                        "name": script_name,
                        "path": f"scripts/{script_name}",
                        "description": script_description,
                        "workflow": script_info.get("workflow", "shell-execute.yml") if script_info else "shell-execute.yml",
                        "type": script_info.get("type", "shell") if script_info else "shell",
                    },
                    "message": "Human approval required before execution"
                },
                "timestamp": datetime.now().isoformat(),
            }

        # Node 9: EXECUTE - Trigger GitHub Actions workflow in sam2881/test_01
        if node_id == 9:
            # Use remediation-specific GitHub credentials from .env
            github_token = os.getenv("GITHUB_REMEDIATION_TOKEN", os.getenv("GITHUB_TOKEN", ""))
            github_owner = os.getenv("GITHUB_REMEDIATION_OWNER", "sam2881")
            github_repo_name = os.getenv("GITHUB_REMEDIATION_REPO", "test_01")
            github_repo = f"{github_owner}/{github_repo_name}"

            github_run_id = None
            github_run_url = None
            github_status = "simulated"

            # Get script arguments from request body (passed from frontend/approval)
            script_args = body.get("script_args", {})
            environment = body.get("environment", "development")

            # Default GCP parameters if not provided - these should come from incident parsing
            instance_name = script_args.get("instance_name") or body.get("instance_name", "dev-vscode")
            zone = script_args.get("zone") or body.get("zone", "us-central1-a")
            project = script_args.get("project") or body.get("project", os.getenv("GCP_PROJECT_ID", "agent-ai-test-461120"))

            logger.info(f"Attempting to trigger GitHub Actions in {github_repo} with script {script_name}")

            if github_token:
                try:
                    async with httpx.AsyncClient() as client:
                        # Use shell-execute.yml for shell script execution
                        # This workflow exists in sam2881/test_01 and expects:
                        # - script_path: Path to shell script
                        # - script_args: Script arguments as JSON (required for GCP scripts)
                        # - environment: development/staging/production
                        # - incident_id: ServiceNow incident ID
                        # - dry_run: boolean (optional)
                        workflow_name = "shell-execute.yml"

                        # Determine script path (scripts are in scripts/ directory in the repo)
                        script_path = f"scripts/{script_name}" if not script_name.startswith("scripts/") else script_name

                        # Build script arguments JSON with ONLY parameters the script expects
                        # start_gcp_instance.sh expects: --instance-name, --zone, --project, --no-wait
                        # DO NOT pass extra params like workflow_id, incident_id to the script
                        script_args_json = json.dumps({
                            "instance_name": instance_name,
                            "zone": zone,
                            "project": project
                        })

                        logger.info(f"Triggering {workflow_name} in {github_repo}")
                        logger.info(f"Script path: {script_path}, Incident: {incident_id}")
                        logger.info(f"Script args: instance_name={instance_name}, zone={zone}, project={project}")

                        response = await client.post(
                            f"https://api.github.com/repos/{github_repo}/actions/workflows/{workflow_name}/dispatches",
                            headers={
                                "Authorization": f"token {github_token}",
                                "Accept": "application/vnd.github.v3+json",
                            },
                            json={
                                "ref": "main",
                                "inputs": {
                                    "script_path": script_path,
                                    "script_args": script_args_json,
                                    "environment": environment,
                                    "incident_id": incident_id,
                                    "dry_run": "false"
                                }
                            },
                            timeout=30.0
                        )

                        if response.status_code in [204, 200]:
                            github_status = "triggered"
                            logger.info(f"GitHub Actions triggered: {github_repo}/{workflow_name} for {incident_id}")

                            # Wait and get the run ID
                            await asyncio.sleep(3)
                            runs_response = await client.get(
                                f"https://api.github.com/repos/{github_repo}/actions/runs",
                                headers={
                                    "Authorization": f"token {github_token}",
                                    "Accept": "application/vnd.github.v3+json",
                                },
                                params={"per_page": 1, "event": "workflow_dispatch"},
                                timeout=30.0
                            )
                            if runs_response.status_code == 200:
                                runs = runs_response.json().get("workflow_runs", [])
                                if runs:
                                    github_run_id = runs[0].get("id")
                                    github_run_url = runs[0].get("html_url")
                        elif response.status_code == 401:
                            github_status = "error: Invalid GitHub token (401 Unauthorized)"
                            logger.error(f"GitHub token is invalid or expired")
                        elif response.status_code == 404:
                            github_status = f"error: Workflow {workflow_name} not found in {github_repo}"
                            logger.error(f"Workflow not found: {workflow_name}")
                        elif response.status_code == 422:
                            github_status = f"error: Invalid workflow inputs (422)"
                            logger.error(f"Invalid inputs for workflow: {response.text}")
                        else:
                            github_status = f"error: GitHub API returned {response.status_code}"
                            logger.warning(f"GitHub Actions failed: {response.status_code} - {response.text}")

                except Exception as e:
                    logger.error(f"GitHub Actions trigger error: {e}")
                    github_status = f"error: {str(e)}"
            else:
                logger.info("GitHub token not configured, simulating execution")

            return {
                "node_id": node_id,
                "node_name": config["name"],
                "phase": config["phase"],
                "workflow_id": workflow_id,
                "incident_id": incident_id,
                "status": "completed",
                "output": {
                    "execution_id": f"exec-{random.randint(1000, 9999)}",
                    "github_status": github_status,
                    "github_run_id": github_run_id,
                    "github_run_url": github_run_url,
                    "github_repo": github_repo,
                    "script_executed": script_name,
                    "script_args": {
                        "instance_name": instance_name,
                        "zone": zone,
                        "project": project,
                    },
                    "triggered_at": datetime.now().isoformat(),
                },
                "timestamp": datetime.now().isoformat(),
            }

        # Generate node-specific output for other nodes
        outputs = {
            1: {"status": "ingested", "source": "kafka", "event_id": f"evt-{random.randint(1000, 9999)}"},
            2: {"status": "parsed", "fields_extracted": 12, "confidence": 0.95, "vm_name": "dev-vscode", "zone": "us-central1-a"},
            3: {"category": "infrastructure", "subcategory": "vm_terminated", "severity": "high", "confidence": 0.94},
            4: {"scripts_matched": 3, "top_script": script_name, "similarity_score": 0.89, "scripts": ["start_gcp_instance.sh", "stop_gcp_instance.sh", "health_check.sh"]},
            5: {"plan_id": f"plan-{random.randint(100, 999)}", "steps": 4, "estimated_time": "5m", "rollback_available": True},
            6: {"verdict": "approved", "confidence": 0.92, "risk_level": "low", "safety_check": "passed"},
            7: {"policy_check": "passed", "approvals_required": 1, "auto_approve": False, "risk_score": 0.3},
            10: {"verification_status": "success", "checks_passed": 5, "checks_total": 5, "service_healthy": True},
            11: {"ticket_status": "resolved", "resolution_code": "automated_fix", "resolution_notes": "VM restarted successfully"},
            12: {"feedback_recorded": True, "ml_data_stored": True, "rag_updated": True},
        }

        return {
            "node_id": node_id,
            "node_name": config["name"],
            "phase": config["phase"],
            "workflow_id": workflow_id,
            "incident_id": incident_id,
            "status": "completed",
            "output": outputs.get(node_id, {}),
            "timestamp": datetime.now().isoformat(),
        }

    @app.get("/api/langgraph/definition")
    async def get_langgraph_definition(request: Request):
        """
        Get the LangGraph workflow definition for visualization.
        Returns the 12-node workflow structure with numeric IDs for the graph.
        """
        return {
            "name": "IT Service Agent Workflow",
            "version": "6.0",
            "nodes": [
                {"id": 1, "name": "Ingest", "phase": "Ingestion", "type": "processor", "description": "Receive incident from Kafka"},
                {"id": 2, "name": "Parse", "phase": "Ingestion", "type": "processor", "description": "Extract incident data"},
                {"id": 3, "name": "Classify", "phase": "Retrieval", "type": "llm", "description": "Categorize incident type"},
                {"id": 4, "name": "Swarm RAG", "phase": "Planning", "type": "retriever", "description": "Multi-agent script matching"},
                {"id": 5, "name": "Generate Plan", "phase": "Planning", "type": "llm", "description": "Create remediation plan"},
                {"id": 6, "name": "LLM Judge", "phase": "Validation", "type": "judge", "description": "Independent plan evaluation"},
                {"id": 7, "name": "Control Plane", "phase": "Approval", "type": "processor", "description": "Policy enforcement"},
                {"id": 8, "name": "Await Approval", "phase": "Approval", "type": "human", "description": "Human approval gate"},
                {"id": 9, "name": "Execute", "phase": "Execution", "type": "executor", "description": "Run remediation (GitHub Actions)"},
                {"id": 10, "name": "Verify", "phase": "Execution", "type": "processor", "description": "Post-execution verification"},
                {"id": 11, "name": "Close Ticket", "phase": "Completion", "type": "processor", "description": "Close incident in ServiceNow"},
                {"id": 12, "name": "Feedback Loop", "phase": "Completion", "type": "processor", "description": "Record feedback for ML"},
            ],
            "edges": [
                {"from": 1, "to": 2},
                {"from": 2, "to": 3},
                {"from": 3, "to": 4},
                {"from": 4, "to": 5},
                {"from": 5, "to": 6},
                {"from": 6, "to": 7},
                {"from": 7, "to": 8},
                {"from": 8, "to": 9},
                {"from": 9, "to": 10},
                {"from": 10, "to": 11},
                {"from": 11, "to": 12},
            ],
            "phases": [
                {"name": "Ingestion", "nodes": [1, 2], "color": "#3B82F6"},
                {"name": "Retrieval", "nodes": [3], "color": "#06B6D4"},
                {"name": "Planning", "nodes": [4, 5], "color": "#F59E0B"},
                {"name": "Validation", "nodes": [6], "color": "#8B5CF6"},
                {"name": "Approval", "nodes": [7, 8], "color": "#EF4444"},
                {"name": "Execution", "nodes": [9, 10], "color": "#10B981"},
                {"name": "Completion", "nodes": [11, 12], "color": "#22C55E"},
            ],
        }

    # ==========================================================================
    # Jira & GitHub API Endpoints (for frontend)
    # ==========================================================================

    @app.get("/api/jira/tickets")
    async def get_jira_tickets(request: Request):
        """
        Get Jira tickets. Returns empty list if Jira is not configured.
        """
        # Check if Jira credentials are configured
        jira_token = os.getenv("JIRA_API_TOKEN", "")
        if not jira_token:
            return {"tickets": [], "count": 0, "source": "none", "message": "Jira not configured"}

        # TODO: Implement actual Jira API call
        return {"tickets": [], "count": 0, "source": "jira"}

    @app.get("/api/github/prs")
    async def get_github_prs(request: Request):
        """
        Get GitHub pull requests. Returns empty list if GitHub is not configured.
        """
        # Check if GitHub token is configured
        github_token = os.getenv("GITHUB_TOKEN", "")
        if not github_token:
            return {"prs": [], "count": 0, "source": "none", "message": "GitHub not configured"}

        # TODO: Implement actual GitHub API call
        return {"prs": [], "count": 0, "source": "github"}

    # ==========================================================================
    # Pipeline API (Jira Integration)
    # ==========================================================================

    @app.post("/api/v2/pipelines")
    async def create_pipeline_workflow(request: Request):
        """
        Create workflow from Jira ticket for data pipeline via Kafka event.

        EVENT-DRIVEN: This endpoint publishes pipeline.requested to Kafka.
        The EventOrchestrator consumes this and triggers the Data Agent LangGraph workflow.

        Body:
            {
                "jira_key": "DATA-123",
                "jira_ticket": {
                    "summary": "Create customer transaction pipeline",
                    "description": "Source: gs://bucket/data, Target: silver zone",
                    "reporter": "user@company.com"
                }
            }
        """
        from backend.streaming.kafka_producer import get_producer
        from backend.streaming.schemas import Topics
        import uuid

        body = await request.json()
        jira_key = body.get("jira_key", "")
        jira_ticket = body.get("jira_ticket", {})

        if not jira_key:
            raise HTTPException(400, "jira_key is required")

        producer = get_producer()
        request_id = str(uuid.uuid4())

        # Publish pipeline.requested event to Kafka
        event = {
            "event_type": "pipeline.requested",
            "pipeline_id": jira_key,
            "request_id": request_id,
            "source_system": "jira",
            "jira_key": jira_key,
            "summary": jira_ticket.get("summary", ""),
            "description": jira_ticket.get("description", ""),
            "reporter": jira_ticket.get("reporter", ""),
            "raw_ticket": jira_ticket,
            "timestamp": datetime.now().isoformat(),
        }

        success = await producer.publish_event(
            topic=Topics.PIPELINE_REQUESTED,
            event=event,
            key=jira_key
        )

        if not success:
            raise HTTPException(500, "Failed to publish pipeline event to Kafka")

        logger.info(f"Pipeline event published: {jira_key}")

        return {
            "request_id": request_id,
            "jira_key": jira_key,
            "status": "accepted",
            "message": "Event published to Kafka. EventOrchestrator will process.",
        }

    # ==========================================================================
    # Webhooks
    # ==========================================================================

    @app.post("/api/v2/webhooks/servicenow")
    async def servicenow_webhook(request: Request):
        """Handle ServiceNow webhook for incident events."""
        body = await request.json()
        publisher = request.app.state.publisher

        # Publish to Kafka for async processing
        from backend.streaming.event_publisher import IncidentEventType

        await publisher.publish_incident_event(
            event_type=IncidentEventType.CREATED.value,
            incident_id=body.get("sys_id", ""),
            incident_number=body.get("number", ""),
            severity=body.get("priority", ""),
            category=body.get("category", ""),
            data=body,
        )

        return {"status": "received"}

    @app.post("/api/v2/webhooks/jira")
    async def jira_webhook(request: Request):
        """Handle Jira webhook for story events."""
        body = await request.json()
        publisher = request.app.state.publisher

        # Check if this is a data pipeline story
        issue = body.get("issue", {})
        issue_type = issue.get("fields", {}).get("issuetype", {}).get("name", "")

        if issue_type in ["Story", "Task"] and "data" in issue.get("fields", {}).get("summary", "").lower():
            from backend.streaming.event_publisher import PipelineEventType

            await publisher.publish_pipeline_event(
                event_type=PipelineEventType.REQUESTED.value,
                pipeline_id=issue.get("key", ""),
                jira_key=issue.get("key", ""),
                stage="requested",
                data=body,
            )

        return {"status": "received"}

    # ==========================================================================
    # Dataproc API
    # ==========================================================================

    @app.get("/api/v2/dataproc/cluster/status")
    async def get_dataproc_cluster_status(request: Request):
        """Get Dataproc cluster status."""
        try:
            from backend.infrastructure import get_dataproc_client

            client = get_dataproc_client()
            state = await client.get_cluster_state()

            return {
                "cluster_name": client.config.cluster_name,
                "state": state.value,
                "region": client.config.region,
            }
        except Exception as e:
            return {
                "cluster_name": "unknown",
                "state": "error",
                "error": str(e),
            }

    @app.post("/api/v2/dataproc/jobs/submit")
    async def submit_spark_job(request: Request):
        """Submit a Spark job to Dataproc."""
        body = await request.json()

        try:
            from backend.infrastructure import get_dataproc_client

            client = get_dataproc_client()
            job_id = await client.submit_pyspark_job(
                script_uri=body.get("script_uri"),
                args=body.get("args", []),
                properties=body.get("properties", {}),
            )

            return {"job_id": job_id, "status": "submitted"}
        except Exception as e:
            raise HTTPException(500, str(e))

    @app.get("/api/v2/dataproc/jobs/{job_id}")
    async def get_spark_job_status(job_id: str, request: Request):
        """Get Spark job status."""
        try:
            from backend.infrastructure import get_dataproc_client

            client = get_dataproc_client()
            result = await client.get_job_status(job_id)

            return {
                "job_id": result.job_id,
                "state": result.state.value,
                "driver_output_uri": result.driver_output_uri,
                "error_message": result.error_message,
            }
        except Exception as e:
            raise HTTPException(500, str(e))

    # ==========================================================================
    # Data Agent API (Pipeline Generation from validated JSON)
    # ==========================================================================

    @app.post("/api/v2/data-agent/pipelines")
    async def create_data_pipeline(request: Request):
        """
        Create a data pipeline from validated intent JSON.

        This endpoint receives the validated pipeline intent from the UI
        and triggers the Data Agent LangGraph workflow. The agent:
        1. Plans the pipeline (template selection, schema detection)
        2. Generates code (DAG, Spark jobs, metadata SQL)
        3. Validates all artifacts
        4. Creates GitHub PR with generated code
        5. Triggers CI/CD for deployment

        Body:
            {
                "request_id": "optional-uuid",
                "pipeline_identity": {...},
                "source_config": {...},
                "target_config": {...},
                "schema_definition": {...},
                "execution_policy": {...}
            }

        Returns:
            {
                "request_id": "uuid",
                "status": "accepted",
                "message": "Pipeline generation started"
            }
        """
        from backend.control_plane.handlers.data_agent_handler import PipelineIntent
        import uuid

        body = await request.json()
        data_agent = request.app.state.data_agent_handler

        # Validate required fields
        if not body.get("pipeline_identity", {}).get("pipeline_name"):
            raise HTTPException(400, "pipeline_identity.pipeline_name is required")

        if not body.get("source_config"):
            raise HTTPException(400, "source_config is required")

        if not body.get("schema_definition"):
            raise HTTPException(400, "schema_definition is required")

        if not body.get("target_config"):
            raise HTTPException(400, "target_config is required")

        # Create PipelineIntent
        intent = PipelineIntent(
            request_id=body.get("request_id") or str(uuid.uuid4()),
            pipeline_identity=body.get("pipeline_identity", {}),
            source_config=body.get("source_config", {}),
            schema_definition=body.get("schema_definition", {}),
            target_config=body.get("target_config", {}),
            transformation_rules=body.get("transformation_rules", []),
            data_quality_rules=body.get("data_quality_rules", []),
            execution_policy=body.get("execution_policy", {}),
            jira_ticket=body.get("jira_ticket"),
            created_by=body.get("created_by"),
        )

        # Start async workflow (fire and forget)
        asyncio.create_task(data_agent.handle_pipeline_request(intent))

        return {
            "request_id": intent.request_id,
            "status": "accepted",
            "message": "Pipeline generation started",
        }

    @app.get("/api/v2/data-agent/pipelines/{request_id}")
    async def get_data_pipeline_status(request_id: str, request: Request):
        """
        Get status of a pipeline generation request.

        Returns current phase, status, and any generated artifacts.
        """
        data_agent = request.app.state.data_agent_handler

        status = data_agent.get_pipeline_status(request_id)

        if not status:
            raise HTTPException(404, f"Pipeline {request_id} not found")

        return status

    @app.get("/api/v2/data-agent/pipelines")
    async def list_active_pipelines(request: Request):
        """
        List all active pipeline generation requests.
        """
        data_agent = request.app.state.data_agent_handler
        return {"pipelines": data_agent.list_active_pipelines()}

    @app.post("/api/v2/data-agent/pipelines/{request_id}/approve")
    async def approve_data_pipeline(request_id: str, request: Request):
        """
        Approve a pipeline awaiting human approval.

        EVENT-DRIVEN: This endpoint publishes pipeline.approved to Kafka.
        The EventOrchestrator consumes this and resumes the Data Agent workflow.

        Required for:
        - Production deployments
        - Schema upgrades with breaking changes
        - Pipelines with high risk scores
        """
        from backend.streaming.kafka_producer import get_producer
        from backend.streaming.schemas import Topics

        body = await request.json()
        approved_by = body.get("approved_by", "admin")
        approval_token = body.get("approval_token")

        producer = get_producer()

        # Publish approval event to Kafka
        event = {
            "event_type": "pipeline.approved",
            "request_id": request_id,
            "pipeline_id": request_id,
            "approved": True,
            "approver": approved_by,
            "approval_token": approval_token,
            "timestamp": datetime.now().isoformat(),
        }

        success = await producer.publish_event(
            topic=Topics.PIPELINE_APPROVED,
            event=event,
            key=request_id
        )

        if not success:
            raise HTTPException(500, "Failed to publish approval event")

        logger.info(f"Pipeline approval event published for {request_id}")

        return {
            "status": "approved",
            "request_id": request_id,
            "approved_by": approved_by,
            "message": "Approval sent. EventOrchestrator will resume workflow.",
        }

    @app.post("/api/v2/data-agent/pipelines/{request_id}/reject")
    async def reject_data_pipeline(request_id: str, request: Request):
        """
        Reject a pipeline awaiting approval.

        EVENT-DRIVEN: This endpoint publishes pipeline.rejected to Kafka.
        The EventOrchestrator consumes this and updates the workflow status.
        """
        from backend.streaming.kafka_producer import get_producer
        from backend.streaming.schemas import Topics

        body = await request.json()
        rejected_by = body.get("rejected_by", "admin")
        reason = body.get("reason", "Rejected by human reviewer")
        approval_token = body.get("approval_token")

        producer = get_producer()

        # Publish rejection event to Kafka
        event = {
            "event_type": "pipeline.rejected",
            "request_id": request_id,
            "pipeline_id": request_id,
            "approved": False,
            "approver": rejected_by,
            "reason": reason,
            "approval_token": approval_token,
            "timestamp": datetime.now().isoformat(),
        }

        success = await producer.publish_event(
            topic=Topics.PIPELINE_REJECTED,
            event=event,
            key=request_id
        )

        if not success:
            raise HTTPException(500, "Failed to publish rejection event")

        logger.info(f"Pipeline rejection event published for {request_id}")

        return {
            "status": "rejected",
            "request_id": request_id,
            "rejected_by": rejected_by,
            "reason": reason,
            "message": "Rejection sent. Workflow will be marked as rejected.",
        }

    @app.get("/api/v2/data-agent/templates")
    async def list_pipeline_templates(request: Request):
        """
        List available pipeline templates.

        Returns the Template Selection Matrix used by the planner.
        """
        data_agent = request.app.state.data_agent_handler
        templates = await data_agent.get_available_templates()

        return {
            "templates": templates,
            "selection_matrix": [
                {"source": "file", "mode": "batch", "cdc": False, "dag": "file_ingest_dag", "spark": ["bronze_ingest", "silver_transform", "gold_load_bq"]},
                {"source": "file", "mode": "micro_batch", "cdc": False, "dag": "streaming_ingest_dag", "spark": ["bronze_ingest", "silver_transform", "gold_load_bq"]},
                {"source": "database", "mode": "batch", "cdc": False, "dag": "db_snapshot_dag", "spark": ["bronze_ingest", "silver_transform", "gold_load_bq"]},
                {"source": "database", "mode": "batch", "cdc": True, "dag": "cdc_ingest_dag", "spark": ["cdc_merge", "scd2_apply", "gold_load_bq"]},
                {"source": "streaming", "mode": "streaming", "cdc": False, "dag": "streaming_ingest_dag", "spark": ["streaming_bronze", "gold_load_bq"]},
                {"source": "api", "mode": "batch", "cdc": False, "dag": "api_ingest_dag", "spark": ["bronze_ingest", "silver_transform", "gold_load_bq"]},
            ],
        }

    # ==========================================================================
    # RAG API (Search and Stats)
    # ==========================================================================

    @app.post("/api/v2/rag/search")
    async def rag_search(request: Request):
        """
        Search the RAG system for relevant scripts and remediation knowledge.

        Body:
            {
                "query": "restart GCP VM instance",
                "top_k": 5,
                "weights": {"semantic": 0.5, "keyword": 0.3, "metadata": 0.2},
                "metadata": {"service": "gcp"}
            }

        Returns:
            {
                "results": [
                    {
                        "chunk_id": "...",
                        "content": "...",
                        "score": 0.85,
                        "metadata": {...}
                    }
                ],
                "count": 5,
                "query_time_ms": 123
            }
        """
        body = await request.json()

        query = body.get("query", "")
        top_k = body.get("top_k", 5)
        weights = body.get("weights")
        metadata_filter = body.get("metadata")

        if not query:
            raise HTTPException(400, "Query is required")

        try:
            from backend.rag.intelligent_retriever import get_intelligent_retriever

            retriever = get_intelligent_retriever()
            start_time = time.time()

            results = await retriever.search(
                query=query,
                top_k=top_k,
                weights=weights,
                metadata_filter=metadata_filter,
            )

            query_time_ms = int((time.time() - start_time) * 1000)

            return {
                "results": [
                    {
                        "chunk_id": r.get("chunk_id", r.get("id", "")),
                        "content": r.get("content", r.get("text", "")),
                        "score": r.get("score", r.get("final_score", 0)),
                        "metadata": r.get("metadata", {}),
                    }
                    for r in results
                ],
                "count": len(results),
                "query_time_ms": query_time_ms,
            }
        except Exception as e:
            logger.error(f"RAG search error: {e}")
            raise HTTPException(500, str(e))

    @app.get("/api/v2/rag/stats")
    async def rag_stats(request: Request):
        """
        Get RAG system statistics.

        Returns:
            {
                "version": "5.0.0",
                "documents_indexed": 100,
                "feedback_stats": {...},
                "script_stats": {...}
            }
        """
        try:
            from backend.rag.intelligent_retriever import get_intelligent_retriever
            from backend.rag.hybrid_search_engine import HybridSearchEngine

            retriever = get_intelligent_retriever()
            engine = HybridSearchEngine(enable_graph=False)

            engine_stats = engine.get_stats()
            feedback_stats = retriever.get_feedback_stats() if hasattr(retriever, 'get_feedback_stats') else {}

            return {
                "version": engine_stats.get("version", "5.0.0"),
                "documents_indexed": engine_stats.get("documents_indexed", 0),
                "is_fitted": engine_stats.get("is_fitted", False),
                "feedback_stats": feedback_stats,
                "script_stats": {
                    "total_scripts": engine_stats.get("documents_indexed", 0),
                },
            }
        except Exception as e:
            logger.error(f"RAG stats error: {e}")
            raise HTTPException(500, str(e))

    # Legacy endpoint alias for backward compatibility
    @app.post("/api/rag/search")
    async def rag_search_legacy(request: Request):
        """Legacy RAG search endpoint - redirects to V2."""
        return await rag_search(request)

    @app.get("/api/rag/stats")
    async def rag_stats_legacy(request: Request):
        """Legacy RAG stats endpoint - redirects to V2."""
        return await rag_stats(request)

    # ==========================================================================
    # WebSocket for Real-Time Pipeline Status
    # ==========================================================================

    @app.websocket("/ws/pipelines/{request_id}")
    async def pipeline_websocket(websocket: WebSocket, request_id: str):
        """
        WebSocket for real-time pipeline status updates.

        Streams status updates as the workflow progresses through phases:
        - init -> planning -> generating -> validating -> [awaiting_approval] -> deploying -> complete
        """
        await websocket.accept()

        data_agent = websocket.app.state.data_agent_handler

        try:
            while True:
                # Poll for status updates
                status = data_agent.get_pipeline_status(request_id)

                if status:
                    await websocket.send_json(status)

                    # Close on completion
                    if status.get("status") in ["complete", "failed"]:
                        break

                # Wait before next poll
                await asyncio.sleep(1)

        except WebSocketDisconnect:
            logger.info(f"WebSocket disconnected for pipeline {request_id}")
        except Exception as e:
            logger.error(f"WebSocket error for pipeline {request_id}: {e}")
        finally:
            await websocket.close()


# =============================================================================
# Main Entry Point
# =============================================================================

app = create_app()

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info",
    )
