"""
Data Pipeline Agent API Server.

WHY: Provides HTTP API for triggering pipeline generation workflows.
HOW: FastAPI server that receives pipeline intents and runs LangGraph workflow.

Endpoints:
- POST /pipelines - Start new pipeline generation
- GET /pipelines/{request_id} - Get pipeline status
- POST /pipelines/{request_id}/approve - Approve pending pipeline
- GET /health - Health check
"""

import os
import uuid
from datetime import datetime
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import structlog

from src.graphs.main_graph import (
    create_pipeline_graph,
    create_initial_state,
    run_pipeline_sync,
)
from src.config.settings import get_settings

# Initialize logger
logger = structlog.get_logger()

# Initialize FastAPI app
app = FastAPI(
    title="Data Pipeline Agent API",
    description="AI-powered data pipeline generation service",
    version="1.0.0",
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory storage for pipeline states (use Redis in production)
pipeline_states: Dict[str, Dict[str, Any]] = {}


# =============================================================================
# Request/Response Models
# =============================================================================


class PipelineIdentity(BaseModel):
    """Pipeline identification info."""
    pipeline_name: str
    project_name: str
    team: str = "data-engineering"


class SourceConfig(BaseModel):
    """Source system configuration."""
    source_type: str  # postgres, mysql, gcs, api, kafka
    connection_id: Optional[str] = None
    schema_name: Optional[str] = None
    table_name: Optional[str] = None
    file_path: Optional[str] = None
    api_endpoint: Optional[str] = None


class TargetConfig(BaseModel):
    """Target system configuration."""
    target_type: str = "bigquery"
    dataset: str
    table: str
    write_mode: str = "append"  # append, overwrite, merge


class SchemaColumn(BaseModel):
    """Column definition."""
    name: str
    type: str
    mode: str = "NULLABLE"  # NULLABLE, REQUIRED, REPEATED
    description: Optional[str] = None


class SchemaDefinition(BaseModel):
    """Schema definition for the pipeline."""
    columns: list[SchemaColumn]
    partition_column: Optional[str] = None
    cluster_columns: Optional[list[str]] = None


class ExecutionPolicy(BaseModel):
    """Execution policy configuration."""
    schedule: str = "0 2 * * *"  # Cron expression
    retry_count: int = 3
    environment: str = "dev"  # dev, qa, prod
    timeout_minutes: int = 60


class PipelineIntentRequest(BaseModel):
    """Request to create a new pipeline."""
    pipeline_identity: PipelineIdentity
    source_config: SourceConfig
    target_config: TargetConfig
    schema_definition: SchemaDefinition
    execution_policy: ExecutionPolicy = Field(default_factory=ExecutionPolicy)
    jira_key: Optional[str] = None


class PipelineResponse(BaseModel):
    """Pipeline status response."""
    request_id: str
    status: str
    current_phase: str
    message: str
    created_at: str
    updated_at: str
    artifacts: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


class ApprovalRequest(BaseModel):
    """Approval request body."""
    approved: bool
    approver: str
    notes: Optional[str] = None


# =============================================================================
# Background Task for Pipeline Execution
# =============================================================================


async def run_pipeline_background(request_id: str, intent_json: Dict[str, Any]):
    """Run pipeline workflow in background."""
    logger.info("Starting background pipeline", request_id=request_id)

    try:
        # Update state to running
        pipeline_states[request_id]["status"] = "running"
        pipeline_states[request_id]["current_phase"] = "validate_intent"
        pipeline_states[request_id]["updated_at"] = datetime.utcnow().isoformat()

        # Run the pipeline
        result = run_pipeline_sync(intent_json, request_id)

        # Update state with result
        pipeline_states[request_id]["status"] = (
            "completed" if result.get("error_message") is None else "failed"
        )
        pipeline_states[request_id]["current_phase"] = result.get("current_phase", "unknown")
        pipeline_states[request_id]["updated_at"] = datetime.utcnow().isoformat()

        if result.get("error_message"):
            pipeline_states[request_id]["error"] = result.get("error_message")
        else:
            # Store artifacts
            pipeline_states[request_id]["artifacts"] = {
                "dag_content": result.get("generator_output", {}).get("dag_content"),
                "spark_jobs": result.get("generator_output", {}).get("spark_jobs"),
                "pr_url": result.get("deployer_output", {}).get("pr_url"),
            }

        logger.info(
            "Pipeline completed",
            request_id=request_id,
            status=pipeline_states[request_id]["status"],
        )

    except Exception as e:
        logger.error("Pipeline failed", request_id=request_id, error=str(e))
        pipeline_states[request_id]["status"] = "failed"
        pipeline_states[request_id]["error"] = str(e)
        pipeline_states[request_id]["updated_at"] = datetime.utcnow().isoformat()


# =============================================================================
# API Endpoints
# =============================================================================


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "data-pipeline-agent",
        "timestamp": datetime.utcnow().isoformat(),
    }


@app.post("/pipelines", response_model=PipelineResponse)
async def create_pipeline(
    request: PipelineIntentRequest,
    background_tasks: BackgroundTasks,
):
    """
    Start a new pipeline generation workflow.

    This endpoint triggers the LangGraph workflow to:
    1. Validate the intent
    2. Plan the pipeline (detect changes, select templates)
    3. Generate artifacts (DAG, Spark jobs, SQL)
    4. Validate artifacts
    5. Wait for approval (if PROD)
    6. Deploy to Git repo
    """
    request_id = str(uuid.uuid4())
    now = datetime.utcnow().isoformat()

    # Convert request to intent JSON
    intent_json = {
        "pipeline_identity": request.pipeline_identity.model_dump(),
        "source_config": request.source_config.model_dump(),
        "target_config": request.target_config.model_dump(),
        "schema_definition": request.schema_definition.model_dump(),
        "execution_policy": request.execution_policy.model_dump(),
        "jira_key": request.jira_key,
    }

    # Initialize state
    pipeline_states[request_id] = {
        "request_id": request_id,
        "status": "pending",
        "current_phase": "init",
        "message": "Pipeline creation started",
        "created_at": now,
        "updated_at": now,
        "intent_json": intent_json,
        "artifacts": None,
        "error": None,
    }

    # Start background task
    background_tasks.add_task(run_pipeline_background, request_id, intent_json)

    logger.info("Pipeline creation initiated", request_id=request_id)

    return PipelineResponse(
        request_id=request_id,
        status="pending",
        current_phase="init",
        message="Pipeline generation started",
        created_at=now,
        updated_at=now,
    )


@app.get("/pipelines/{request_id}", response_model=PipelineResponse)
async def get_pipeline_status(request_id: str):
    """Get the status of a pipeline generation request."""
    if request_id not in pipeline_states:
        raise HTTPException(status_code=404, detail="Pipeline not found")

    state = pipeline_states[request_id]

    return PipelineResponse(
        request_id=request_id,
        status=state["status"],
        current_phase=state["current_phase"],
        message=f"Pipeline is {state['status']}",
        created_at=state["created_at"],
        updated_at=state["updated_at"],
        artifacts=state.get("artifacts"),
        error=state.get("error"),
    )


@app.post("/pipelines/{request_id}/approve", response_model=PipelineResponse)
async def approve_pipeline(request_id: str, approval: ApprovalRequest):
    """
    Approve or reject a pending pipeline.

    This is called when a pipeline requires human approval (e.g., PROD deployment).
    """
    if request_id not in pipeline_states:
        raise HTTPException(status_code=404, detail="Pipeline not found")

    state = pipeline_states[request_id]

    if state["status"] != "pending_approval":
        raise HTTPException(
            status_code=400,
            detail=f"Pipeline is not pending approval (current status: {state['status']})",
        )

    # Update approval status
    state["approval"] = {
        "approved": approval.approved,
        "approver": approval.approver,
        "notes": approval.notes,
        "timestamp": datetime.utcnow().isoformat(),
    }

    if approval.approved:
        state["status"] = "deploying"
        state["message"] = f"Approved by {approval.approver}"
        # TODO: Resume workflow
    else:
        state["status"] = "rejected"
        state["message"] = f"Rejected by {approval.approver}: {approval.notes}"

    state["updated_at"] = datetime.utcnow().isoformat()

    logger.info(
        "Pipeline approval processed",
        request_id=request_id,
        approved=approval.approved,
        approver=approval.approver,
    )

    return PipelineResponse(
        request_id=request_id,
        status=state["status"],
        current_phase=state["current_phase"],
        message=state["message"],
        created_at=state["created_at"],
        updated_at=state["updated_at"],
        artifacts=state.get("artifacts"),
        error=state.get("error"),
    )


@app.get("/pipelines")
async def list_pipelines(
    status: Optional[str] = None,
    limit: int = 50,
):
    """List all pipelines, optionally filtered by status."""
    pipelines = []

    for request_id, state in pipeline_states.items():
        if status and state["status"] != status:
            continue

        pipelines.append(
            PipelineResponse(
                request_id=request_id,
                status=state["status"],
                current_phase=state["current_phase"],
                message=f"Pipeline is {state['status']}",
                created_at=state["created_at"],
                updated_at=state["updated_at"],
                artifacts=state.get("artifacts"),
                error=state.get("error"),
            )
        )

        if len(pipelines) >= limit:
            break

    return {"pipelines": pipelines, "total": len(pipelines)}


# =============================================================================
# Startup/Shutdown Events
# =============================================================================


@app.on_event("startup")
async def startup_event():
    """Initialize on startup."""
    logger.info("Data Pipeline Agent API starting up")
    settings = get_settings()
    logger.info(
        "Configuration loaded",
        environment=settings.environment,
        gcp_project=settings.gcp_project_id,
    )


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown."""
    logger.info("Data Pipeline Agent API shutting down")


# =============================================================================
# Main Entry Point
# =============================================================================

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "src.api.main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8001")),
        reload=os.getenv("ENVIRONMENT", "dev") == "dev",
    )
