#!/usr/bin/env python3
"""
Airflow MCP Server - MCP Server for Apache Airflow Operations

WHY: Provides Airflow operations as MCP tools for:
- Data Agent (DAG triggering, monitoring, management)
- IT Service Agent (pipeline health checks, troubleshooting)

HOW: FastAPI server exposing Airflow REST API operations as MCP-compatible tools.
Uses Cloud Composer or standalone Airflow REST API.

RUN: python -m backend.mcp.servers.airflow_server

TOOLS PROVIDED:
- airflow_trigger_dag: Trigger a DAG run with optional configuration
- airflow_get_dag_runs: Get DAG run history
- airflow_get_dag_status: Get current status of a DAG
- airflow_list_dags: List all available DAGs
- airflow_pause_dag: Pause a DAG
- airflow_unpause_dag: Unpause a DAG
- airflow_get_task_logs: Get logs for a specific task instance
"""
import os
import sys
import json
import httpx
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
import structlog

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))

logger = structlog.get_logger()

SERVER_NAME = "airflow-mcp"
DEFAULT_PORT = 8095


class AirflowConfig:
    """Airflow connection configuration"""

    def __init__(self):
        # For Cloud Composer, use the webserver URL
        # For standalone Airflow, use the Airflow webserver URL
        self.base_url = os.getenv(
            "AIRFLOW_WEBSERVER_URL",
            "http://localhost:8080"  # Default Airflow webserver
        )
        self.username = os.getenv("AIRFLOW_USERNAME", "admin")
        self.password = os.getenv("AIRFLOW_PASSWORD", "admin")

        # Cloud Composer specific
        self.composer_environment = os.getenv("COMPOSER_ENVIRONMENT")
        self.composer_location = os.getenv("COMPOSER_LOCATION", "us-central1")
        self.gcp_project = os.getenv("GCP_PROJECT_ID")

        # Authentication method
        self.auth_method = os.getenv("AIRFLOW_AUTH_METHOD", "basic")  # basic, oauth, iap

    def get_headers(self) -> Dict[str, str]:
        """Get authentication headers"""
        if self.auth_method == "basic":
            import base64
            credentials = base64.b64encode(
                f"{self.username}:{self.password}".encode()
            ).decode()
            return {
                "Authorization": f"Basic {credentials}",
                "Content-Type": "application/json"
            }
        # For Cloud Composer with IAP, you'd use Google auth
        return {"Content-Type": "application/json"}


class ToolCall(BaseModel):
    """MCP Tool call request"""
    name: str
    arguments: Dict[str, Any]


class ToolResult(BaseModel):
    """MCP Tool call result"""
    type: str = "text"
    text: str
    metadata: Optional[Dict[str, Any]] = None


class TriggerDagRequest(BaseModel):
    """Request to trigger a DAG"""
    dag_id: str = Field(..., description="The DAG ID to trigger")
    conf: Optional[Dict[str, Any]] = Field(default=None, description="DAG run configuration")
    logical_date: Optional[str] = Field(default=None, description="Logical date for the run (ISO format)")
    note: Optional[str] = Field(default=None, description="Note to add to the DAG run")


# Global config instance
config = AirflowConfig()


async def make_airflow_request(
    method: str,
    endpoint: str,
    data: Optional[Dict] = None
) -> Dict[str, Any]:
    """Make authenticated request to Airflow REST API"""
    url = f"{config.base_url}/api/v1/{endpoint}"

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            if method == "GET":
                response = await client.get(url, headers=config.get_headers())
            elif method == "POST":
                response = await client.post(url, headers=config.get_headers(), json=data)
            elif method == "PATCH":
                response = await client.patch(url, headers=config.get_headers(), json=data)
            elif method == "DELETE":
                response = await client.delete(url, headers=config.get_headers())
            else:
                raise ValueError(f"Unsupported method: {method}")

            if response.status_code >= 400:
                error_detail = response.text
                logger.error(
                    "airflow_api_error",
                    status=response.status_code,
                    endpoint=endpoint,
                    error=error_detail
                )
                return {
                    "error": True,
                    "status_code": response.status_code,
                    "message": error_detail
                }

            return response.json()

    except httpx.ConnectError as e:
        logger.error("airflow_connection_error", error=str(e))
        return {
            "error": True,
            "status_code": 503,
            "message": f"Cannot connect to Airflow at {config.base_url}: {str(e)}"
        }
    except Exception as e:
        logger.error("airflow_request_error", error=str(e))
        return {
            "error": True,
            "status_code": 500,
            "message": str(e)
        }


def create_airflow_mcp_server() -> FastAPI:
    """
    Create MCP server for Airflow operations.

    Tools for:
    - Triggering DAGs
    - Monitoring DAG runs
    - Managing DAG state
    - Retrieving task logs
    """
    app = FastAPI(
        title="Airflow MCP Server",
        description="MCP server for Apache Airflow DAG operations",
        version="1.0.0"
    )

    @app.get("/health")
    async def health_check():
        """Health check endpoint"""
        # Test Airflow connectivity
        result = await make_airflow_request("GET", "health")
        if result.get("error"):
            return {
                "status": "degraded",
                "airflow_connected": False,
                "message": result.get("message")
            }
        return {
            "status": "healthy",
            "airflow_connected": True,
            "airflow_url": config.base_url
        }

    @app.get("/mcp/tools")
    async def list_tools() -> List[Dict[str, Any]]:
        """List available MCP tools for Airflow"""
        return [
            {
                "name": "airflow_trigger_dag",
                "description": "Trigger a DAG run with optional configuration. Returns the DAG run ID.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "dag_id": {
                            "type": "string",
                            "description": "The DAG ID to trigger"
                        },
                        "conf": {
                            "type": "object",
                            "description": "Configuration to pass to the DAG (optional)"
                        },
                        "logical_date": {
                            "type": "string",
                            "description": "Logical date for the run in ISO format (optional)"
                        },
                        "note": {
                            "type": "string",
                            "description": "Note to attach to the DAG run (optional)"
                        }
                    },
                    "required": ["dag_id"]
                }
            },
            {
                "name": "airflow_get_dag_runs",
                "description": "Get the run history for a specific DAG",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "dag_id": {
                            "type": "string",
                            "description": "The DAG ID"
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Maximum number of runs to return (default 10)"
                        },
                        "state": {
                            "type": "string",
                            "description": "Filter by state (running, success, failed, queued)"
                        }
                    },
                    "required": ["dag_id"]
                }
            },
            {
                "name": "airflow_get_dag_status",
                "description": "Get the current status and info for a DAG",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "dag_id": {
                            "type": "string",
                            "description": "The DAG ID"
                        }
                    },
                    "required": ["dag_id"]
                }
            },
            {
                "name": "airflow_list_dags",
                "description": "List all available DAGs",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "only_active": {
                            "type": "boolean",
                            "description": "Only return active (unpaused) DAGs"
                        },
                        "tags": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Filter by tags"
                        },
                        "dag_id_pattern": {
                            "type": "string",
                            "description": "Filter by DAG ID pattern"
                        }
                    }
                }
            },
            {
                "name": "airflow_pause_dag",
                "description": "Pause a DAG to prevent new runs",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "dag_id": {
                            "type": "string",
                            "description": "The DAG ID to pause"
                        }
                    },
                    "required": ["dag_id"]
                }
            },
            {
                "name": "airflow_unpause_dag",
                "description": "Unpause a DAG to allow new runs",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "dag_id": {
                            "type": "string",
                            "description": "The DAG ID to unpause"
                        }
                    },
                    "required": ["dag_id"]
                }
            },
            {
                "name": "airflow_get_task_logs",
                "description": "Get logs for a specific task instance",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "dag_id": {
                            "type": "string",
                            "description": "The DAG ID"
                        },
                        "dag_run_id": {
                            "type": "string",
                            "description": "The DAG run ID"
                        },
                        "task_id": {
                            "type": "string",
                            "description": "The task ID"
                        },
                        "task_try_number": {
                            "type": "integer",
                            "description": "The task try number (default: latest)"
                        }
                    },
                    "required": ["dag_id", "dag_run_id", "task_id"]
                }
            },
            {
                "name": "airflow_get_task_instances",
                "description": "Get task instances for a DAG run",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "dag_id": {
                            "type": "string",
                            "description": "The DAG ID"
                        },
                        "dag_run_id": {
                            "type": "string",
                            "description": "The DAG run ID"
                        }
                    },
                    "required": ["dag_id", "dag_run_id"]
                }
            }
        ]

    @app.post("/mcp/tools/call")
    async def call_tool(tool_call: ToolCall) -> ToolResult:
        """Execute an MCP tool"""
        name = tool_call.name
        args = tool_call.arguments

        logger.info("airflow_mcp_tool_call", tool=name, args=args)

        try:
            if name == "airflow_trigger_dag":
                result = await trigger_dag(**args)
            elif name == "airflow_get_dag_runs":
                result = await get_dag_runs(**args)
            elif name == "airflow_get_dag_status":
                result = await get_dag_status(**args)
            elif name == "airflow_list_dags":
                result = await list_dags(**args)
            elif name == "airflow_pause_dag":
                result = await pause_dag(**args)
            elif name == "airflow_unpause_dag":
                result = await unpause_dag(**args)
            elif name == "airflow_get_task_logs":
                result = await get_task_logs(**args)
            elif name == "airflow_get_task_instances":
                result = await get_task_instances(**args)
            else:
                raise ValueError(f"Unknown tool: {name}")

            return ToolResult(
                text=json.dumps(result, indent=2, default=str),
                metadata={"tool": name, "success": not result.get("error", False)}
            )

        except Exception as e:
            logger.error("airflow_mcp_tool_error", tool=name, error=str(e))
            return ToolResult(
                text=json.dumps({"error": str(e)}),
                metadata={"tool": name, "success": False}
            )

    # Direct REST endpoints for convenience
    @app.post("/api/dags/{dag_id}/trigger")
    async def api_trigger_dag(dag_id: str, request: Optional[TriggerDagRequest] = None):
        """Direct API endpoint to trigger a DAG"""
        conf = request.conf if request else None
        logical_date = request.logical_date if request else None
        note = request.note if request else None
        return await trigger_dag(dag_id, conf, logical_date, note)

    @app.get("/api/dags/{dag_id}/runs")
    async def api_get_dag_runs(dag_id: str, limit: int = 10, state: Optional[str] = None):
        """Direct API endpoint to get DAG runs"""
        return await get_dag_runs(dag_id, limit, state)

    @app.get("/api/dags/{dag_id}")
    async def api_get_dag(dag_id: str):
        """Direct API endpoint to get DAG status"""
        return await get_dag_status(dag_id)

    @app.get("/api/dags")
    async def api_list_dags(only_active: bool = False, dag_id_pattern: Optional[str] = None):
        """Direct API endpoint to list DAGs"""
        return await list_dags(only_active, None, dag_id_pattern)

    return app


# Tool implementations
async def trigger_dag(
    dag_id: str,
    conf: Optional[Dict[str, Any]] = None,
    logical_date: Optional[str] = None,
    note: Optional[str] = None
) -> Dict[str, Any]:
    """Trigger a DAG run"""
    payload = {}
    if conf:
        payload["conf"] = conf
    if logical_date:
        payload["logical_date"] = logical_date
    if note:
        payload["note"] = note

    result = await make_airflow_request("POST", f"dags/{dag_id}/dagRuns", payload)

    if result.get("error"):
        return result

    return {
        "success": True,
        "dag_id": dag_id,
        "dag_run_id": result.get("dag_run_id"),
        "execution_date": result.get("execution_date") or result.get("logical_date"),
        "state": result.get("state"),
        "message": f"DAG '{dag_id}' triggered successfully"
    }


async def get_dag_runs(
    dag_id: str,
    limit: int = 10,
    state: Optional[str] = None
) -> Dict[str, Any]:
    """Get DAG run history"""
    endpoint = f"dags/{dag_id}/dagRuns?limit={limit}"
    if state:
        endpoint += f"&state={state}"

    result = await make_airflow_request("GET", endpoint)

    if result.get("error"):
        return result

    runs = result.get("dag_runs", [])
    return {
        "dag_id": dag_id,
        "total_runs": result.get("total_entries", len(runs)),
        "runs": [
            {
                "dag_run_id": r.get("dag_run_id"),
                "state": r.get("state"),
                "execution_date": r.get("execution_date") or r.get("logical_date"),
                "start_date": r.get("start_date"),
                "end_date": r.get("end_date"),
                "run_type": r.get("run_type")
            }
            for r in runs
        ]
    }


async def get_dag_status(dag_id: str) -> Dict[str, Any]:
    """Get DAG status and info"""
    result = await make_airflow_request("GET", f"dags/{dag_id}")

    if result.get("error"):
        return result

    return {
        "dag_id": result.get("dag_id"),
        "is_paused": result.get("is_paused"),
        "is_active": result.get("is_active"),
        "description": result.get("description"),
        "schedule_interval": result.get("schedule_interval") or result.get("timetable_description"),
        "file_token": result.get("file_token"),
        "owners": result.get("owners", []),
        "tags": [t.get("name") for t in result.get("tags", [])]
    }


async def list_dags(
    only_active: bool = False,
    tags: Optional[List[str]] = None,
    dag_id_pattern: Optional[str] = None
) -> Dict[str, Any]:
    """List all DAGs"""
    endpoint = "dags?"
    if only_active:
        endpoint += "only_active=true&"
    if tags:
        for tag in tags:
            endpoint += f"tags={tag}&"
    if dag_id_pattern:
        endpoint += f"dag_id_pattern={dag_id_pattern}&"

    result = await make_airflow_request("GET", endpoint.rstrip("&?"))

    if result.get("error"):
        return result

    dags = result.get("dags", [])
    return {
        "total": result.get("total_entries", len(dags)),
        "dags": [
            {
                "dag_id": d.get("dag_id"),
                "is_paused": d.get("is_paused"),
                "is_active": d.get("is_active"),
                "description": d.get("description", "")[:100],
                "schedule": d.get("schedule_interval") or d.get("timetable_description"),
                "owners": d.get("owners", []),
                "tags": [t.get("name") for t in d.get("tags", [])]
            }
            for d in dags
        ]
    }


async def pause_dag(dag_id: str) -> Dict[str, Any]:
    """Pause a DAG"""
    result = await make_airflow_request("PATCH", f"dags/{dag_id}", {"is_paused": True})

    if result.get("error"):
        return result

    return {
        "success": True,
        "dag_id": dag_id,
        "is_paused": True,
        "message": f"DAG '{dag_id}' paused successfully"
    }


async def unpause_dag(dag_id: str) -> Dict[str, Any]:
    """Unpause a DAG"""
    result = await make_airflow_request("PATCH", f"dags/{dag_id}", {"is_paused": False})

    if result.get("error"):
        return result

    return {
        "success": True,
        "dag_id": dag_id,
        "is_paused": False,
        "message": f"DAG '{dag_id}' unpaused successfully"
    }


async def get_task_logs(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    task_try_number: Optional[int] = None
) -> Dict[str, Any]:
    """Get task logs"""
    try_number = task_try_number or 1
    endpoint = f"dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{try_number}"

    result = await make_airflow_request("GET", endpoint)

    if result.get("error"):
        return result

    return {
        "dag_id": dag_id,
        "dag_run_id": dag_run_id,
        "task_id": task_id,
        "try_number": try_number,
        "content": result.get("content", str(result))
    }


async def get_task_instances(dag_id: str, dag_run_id: str) -> Dict[str, Any]:
    """Get task instances for a DAG run"""
    endpoint = f"dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances"

    result = await make_airflow_request("GET", endpoint)

    if result.get("error"):
        return result

    tasks = result.get("task_instances", [])
    return {
        "dag_id": dag_id,
        "dag_run_id": dag_run_id,
        "total_tasks": len(tasks),
        "tasks": [
            {
                "task_id": t.get("task_id"),
                "state": t.get("state"),
                "start_date": t.get("start_date"),
                "end_date": t.get("end_date"),
                "duration": t.get("duration"),
                "try_number": t.get("try_number"),
                "operator": t.get("operator")
            }
            for t in tasks
        ]
    }


# Create the app
app = create_airflow_mcp_server()


if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("AIRFLOW_MCP_PORT", DEFAULT_PORT))
    logger.info("starting_airflow_mcp_server", port=port, airflow_url=config.base_url)

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port,
        log_level="info"
    )
