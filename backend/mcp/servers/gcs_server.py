#!/usr/bin/env python3
"""
GCS MCP Server - Shared MCP Server for Google Cloud Storage Operations

WHY: Provides GCS operations as MCP tools for:
- Both IT Service Agent (log retrieval, backup access)
- Data Agent (data lake access, schema discovery)

HOW: FastAPI server exposing GCS operations as MCP-compatible tools.
Shared by all agents that need cloud storage access.

RUN: python -m backend.mcp.servers.gcs_server
"""
import os
import sys
import json
from typing import Any, Dict, List, Optional
from datetime import datetime
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import structlog

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))

logger = structlog.get_logger()

SERVER_NAME = "gcs-mcp"


class ToolCall(BaseModel):
    """MCP Tool call request"""
    name: str
    arguments: Dict[str, Any]


class ToolResult(BaseModel):
    """MCP Tool call result"""
    type: str = "text"
    text: str
    metadata: Optional[Dict[str, Any]] = None


def create_gcs_mcp_server() -> FastAPI:
    """
    Create MCP server for GCS operations.

    Shared tools for:
    - IT Service Agent: Log retrieval, backup access
    - Data Agent: Data lake access, schema discovery
    """
    app = FastAPI(
        title="GCS MCP Server",
        description="Shared MCP server for Google Cloud Storage",
        version="1.0.0"
    )

    @app.get("/mcp/tools")
    async def list_tools() -> List[Dict[str, Any]]:
        """List available MCP tools"""
        return [
            {
                "name": "gcs_list_buckets",
                "description": "List GCS buckets in the project",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "project_id": {
                            "type": "string",
                            "description": "GCP project ID"
                        },
                        "prefix": {
                            "type": "string",
                            "description": "Filter buckets by prefix"
                        }
                    }
                }
            },
            {
                "name": "gcs_list_objects",
                "description": "List objects in a GCS bucket",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "bucket": {
                            "type": "string",
                            "description": "Bucket name"
                        },
                        "prefix": {
                            "type": "string",
                            "description": "Object prefix filter"
                        },
                        "max_results": {
                            "type": "integer",
                            "description": "Maximum results to return",
                            "default": 100
                        }
                    },
                    "required": ["bucket"]
                }
            },
            {
                "name": "gcs_get_object_metadata",
                "description": "Get metadata for a GCS object",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "bucket": {
                            "type": "string",
                            "description": "Bucket name"
                        },
                        "object_path": {
                            "type": "string",
                            "description": "Object path"
                        }
                    },
                    "required": ["bucket", "object_path"]
                }
            },
            {
                "name": "gcs_read_object",
                "description": "Read content from a GCS object (text files only)",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "bucket": {
                            "type": "string",
                            "description": "Bucket name"
                        },
                        "object_path": {
                            "type": "string",
                            "description": "Object path"
                        },
                        "max_bytes": {
                            "type": "integer",
                            "description": "Maximum bytes to read",
                            "default": 10000
                        }
                    },
                    "required": ["bucket", "object_path"]
                }
            },
            {
                "name": "gcs_infer_schema",
                "description": "Infer schema from parquet/CSV files in GCS (for Data Agent)",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "bucket": {
                            "type": "string",
                            "description": "Bucket name"
                        },
                        "object_path": {
                            "type": "string",
                            "description": "Path to data file"
                        },
                        "format": {
                            "type": "string",
                            "enum": ["parquet", "csv", "json", "avro"],
                            "description": "File format"
                        }
                    },
                    "required": ["bucket", "object_path"]
                }
            },
            {
                "name": "gcs_get_logs",
                "description": "Get logs from GCS (for IT Service Agent)",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "bucket": {
                            "type": "string",
                            "description": "Logs bucket name"
                        },
                        "service": {
                            "type": "string",
                            "description": "Service name"
                        },
                        "start_time": {
                            "type": "string",
                            "description": "Start time (ISO format)"
                        },
                        "end_time": {
                            "type": "string",
                            "description": "End time (ISO format)"
                        }
                    },
                    "required": ["bucket", "service"]
                }
            }
        ]

    @app.post("/mcp/tools/call")
    async def call_tool(tool_call: ToolCall) -> ToolResult:
        """Execute an MCP tool"""
        logger.info("mcp_gcs_tool_called", tool=tool_call.name)

        try:
            if tool_call.name == "gcs_list_buckets":
                result = await gcs_list_buckets(
                    project_id=tool_call.arguments.get("project_id"),
                    prefix=tool_call.arguments.get("prefix")
                )
            elif tool_call.name == "gcs_list_objects":
                result = await gcs_list_objects(
                    bucket=tool_call.arguments["bucket"],
                    prefix=tool_call.arguments.get("prefix"),
                    max_results=tool_call.arguments.get("max_results", 100)
                )
            elif tool_call.name == "gcs_get_object_metadata":
                result = await gcs_get_object_metadata(
                    bucket=tool_call.arguments["bucket"],
                    object_path=tool_call.arguments["object_path"]
                )
            elif tool_call.name == "gcs_read_object":
                result = await gcs_read_object(
                    bucket=tool_call.arguments["bucket"],
                    object_path=tool_call.arguments["object_path"],
                    max_bytes=tool_call.arguments.get("max_bytes", 10000)
                )
            elif tool_call.name == "gcs_infer_schema":
                result = await gcs_infer_schema(
                    bucket=tool_call.arguments["bucket"],
                    object_path=tool_call.arguments["object_path"],
                    format=tool_call.arguments.get("format", "parquet")
                )
            elif tool_call.name == "gcs_get_logs":
                result = await gcs_get_logs(
                    bucket=tool_call.arguments["bucket"],
                    service=tool_call.arguments["service"],
                    start_time=tool_call.arguments.get("start_time"),
                    end_time=tool_call.arguments.get("end_time")
                )
            else:
                raise HTTPException(status_code=404, detail=f"Unknown tool: {tool_call.name}")

            return ToolResult(
                type="text",
                text=json.dumps(result, indent=2),
                metadata={"timestamp": datetime.utcnow().isoformat()}
            )

        except Exception as e:
            logger.error("mcp_gcs_tool_failed", tool=tool_call.name, error=str(e))
            raise HTTPException(status_code=500, detail=str(e))

    async def gcs_list_buckets(project_id: Optional[str], prefix: Optional[str]) -> Dict[str, Any]:
        """List GCS buckets (simulated)"""
        buckets = [
            {"name": "data-lake-landing", "location": "US", "storage_class": "STANDARD"},
            {"name": "data-lake-bronze", "location": "US", "storage_class": "STANDARD"},
            {"name": "data-lake-silver", "location": "US", "storage_class": "STANDARD"},
            {"name": "data-lake-gold", "location": "US", "storage_class": "STANDARD"},
            {"name": "logs-archive", "location": "US", "storage_class": "NEARLINE"},
            {"name": "backup-daily", "location": "US", "storage_class": "COLDLINE"},
        ]

        if prefix:
            buckets = [b for b in buckets if b["name"].startswith(prefix)]

        return {"buckets": buckets, "count": len(buckets)}

    async def gcs_list_objects(bucket: str, prefix: Optional[str], max_results: int) -> Dict[str, Any]:
        """List objects in bucket (simulated)"""
        # Simulated objects based on bucket type
        if "bronze" in bucket or "landing" in bucket:
            objects = [
                {"name": "raw/customers/2024/01/data.parquet", "size": 1024000},
                {"name": "raw/orders/2024/01/data.parquet", "size": 2048000},
                {"name": "raw/products/2024/01/data.parquet", "size": 512000},
            ]
        elif "silver" in bucket:
            objects = [
                {"name": "cleaned/customers/data.parquet", "size": 800000},
                {"name": "cleaned/orders/data.parquet", "size": 1500000},
            ]
        elif "gold" in bucket:
            objects = [
                {"name": "aggregated/sales_daily.parquet", "size": 256000},
                {"name": "aggregated/customer_360.parquet", "size": 512000},
            ]
        elif "logs" in bucket:
            objects = [
                {"name": "nginx/2024/01/15/access.log", "size": 50000},
                {"name": "app/2024/01/15/error.log", "size": 25000},
            ]
        else:
            objects = [{"name": "data.txt", "size": 1000}]

        if prefix:
            objects = [o for o in objects if o["name"].startswith(prefix)]

        return {"bucket": bucket, "objects": objects[:max_results], "count": len(objects)}

    async def gcs_get_object_metadata(bucket: str, object_path: str) -> Dict[str, Any]:
        """Get object metadata (simulated)"""
        return {
            "bucket": bucket,
            "name": object_path,
            "size": 1024000,
            "content_type": "application/octet-stream",
            "created": datetime.utcnow().isoformat(),
            "updated": datetime.utcnow().isoformat(),
            "md5_hash": "abc123def456",
            "metadata": {
                "source": "data-pipeline",
                "version": "1.0"
            }
        }

    async def gcs_read_object(bucket: str, object_path: str, max_bytes: int) -> Dict[str, Any]:
        """Read object content (simulated)"""
        return {
            "bucket": bucket,
            "object_path": object_path,
            "content": f"[Simulated content from {bucket}/{object_path}]",
            "bytes_read": min(max_bytes, 1000),
            "truncated": max_bytes < 1000
        }

    async def gcs_infer_schema(bucket: str, object_path: str, format: str) -> Dict[str, Any]:
        """Infer schema from data file (simulated) - Used by Data Agent"""
        # Simulated schema based on file name
        if "customer" in object_path.lower():
            schema = [
                {"name": "customer_id", "type": "STRING", "nullable": False},
                {"name": "first_name", "type": "STRING", "nullable": True},
                {"name": "last_name", "type": "STRING", "nullable": True},
                {"name": "email", "type": "STRING", "nullable": True},
                {"name": "created_at", "type": "TIMESTAMP", "nullable": True},
            ]
        elif "order" in object_path.lower():
            schema = [
                {"name": "order_id", "type": "STRING", "nullable": False},
                {"name": "customer_id", "type": "STRING", "nullable": False},
                {"name": "order_date", "type": "DATE", "nullable": False},
                {"name": "total_amount", "type": "DECIMAL", "nullable": False},
                {"name": "status", "type": "STRING", "nullable": True},
            ]
        else:
            schema = [
                {"name": "id", "type": "STRING", "nullable": False},
                {"name": "data", "type": "STRING", "nullable": True},
                {"name": "timestamp", "type": "TIMESTAMP", "nullable": True},
            ]

        return {
            "bucket": bucket,
            "object_path": object_path,
            "format": format,
            "schema": schema,
            "row_count_estimate": 10000,
            "file_size_bytes": 1024000
        }

    async def gcs_get_logs(
        bucket: str,
        service: str,
        start_time: Optional[str],
        end_time: Optional[str]
    ) -> Dict[str, Any]:
        """Get logs from GCS (simulated) - Used by IT Service Agent"""
        return {
            "bucket": bucket,
            "service": service,
            "log_files": [
                f"{service}/2024/01/15/app.log",
                f"{service}/2024/01/15/error.log",
            ],
            "total_size_bytes": 75000,
            "time_range": {
                "start": start_time or "2024-01-15T00:00:00Z",
                "end": end_time or "2024-01-15T23:59:59Z"
            }
        }

    @app.get("/health")
    async def health():
        """Health check endpoint"""
        return {
            "status": "healthy",
            "service": "gcs-mcp-server",
            "timestamp": datetime.utcnow().isoformat()
        }

    return app


# Create app instance
app = create_gcs_mcp_server()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8011)
