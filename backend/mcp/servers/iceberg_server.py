#!/usr/bin/env python3
"""
Iceberg MCP Server - Shared MCP Server for Apache Iceberg Operations

WHY: Provides Apache Iceberg catalog operations as MCP tools for:
- Data Agent: Table management, schema operations, data lake access
- IT Service Agent: Data validation, table health checks

HOW: FastAPI server exposing Iceberg REST catalog operations as
MCP-compatible tools. Shared by all agents that need data lakehouse access.

RUN: python -m backend.mcp.servers.iceberg_server
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

SERVER_NAME = "iceberg-mcp"


class ToolCall(BaseModel):
    """MCP Tool call request"""
    name: str
    arguments: Dict[str, Any]


class ToolResult(BaseModel):
    """MCP Tool call result"""
    type: str = "text"
    text: str
    metadata: Optional[Dict[str, Any]] = None


def create_iceberg_mcp_server() -> FastAPI:
    """
    Create MCP server for Iceberg catalog access.

    Shared tools for:
    - Data Agent: Table creation, schema evolution, data lake operations
    - IT Service Agent: Table health checks, data validation
    """
    app = FastAPI(
        title="Iceberg MCP Server",
        description="Shared MCP server for Apache Iceberg catalog",
        version="1.0.0"
    )

    @app.get("/mcp/tools")
    async def list_tools() -> List[Dict[str, Any]]:
        """List available MCP tools"""
        return [
            {
                "name": "iceberg_list_namespaces",
                "description": "List all namespaces in the catalog",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "parent": {
                            "type": "string",
                            "description": "Parent namespace (optional)"
                        }
                    }
                }
            },
            {
                "name": "iceberg_list_tables",
                "description": "List tables in a namespace",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "namespace": {
                            "type": "string",
                            "description": "Namespace name"
                        }
                    },
                    "required": ["namespace"]
                }
            },
            {
                "name": "iceberg_get_table",
                "description": "Get table metadata including schema",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "namespace": {
                            "type": "string",
                            "description": "Namespace name"
                        },
                        "table": {
                            "type": "string",
                            "description": "Table name"
                        }
                    },
                    "required": ["namespace", "table"]
                }
            },
            {
                "name": "iceberg_get_schema",
                "description": "Get current schema for a table",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "namespace": {
                            "type": "string",
                            "description": "Namespace name"
                        },
                        "table": {
                            "type": "string",
                            "description": "Table name"
                        }
                    },
                    "required": ["namespace", "table"]
                }
            },
            {
                "name": "iceberg_list_snapshots",
                "description": "List snapshots for a table",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "namespace": {
                            "type": "string",
                            "description": "Namespace name"
                        },
                        "table": {
                            "type": "string",
                            "description": "Table name"
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Maximum snapshots to return",
                            "default": 10
                        }
                    },
                    "required": ["namespace", "table"]
                }
            },
            {
                "name": "iceberg_get_partitions",
                "description": "Get partition spec for a table",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "namespace": {
                            "type": "string",
                            "description": "Namespace name"
                        },
                        "table": {
                            "type": "string",
                            "description": "Table name"
                        }
                    },
                    "required": ["namespace", "table"]
                }
            },
            {
                "name": "iceberg_get_table_health",
                "description": "Get table health metrics (for IT Service Agent)",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "namespace": {
                            "type": "string",
                            "description": "Namespace name"
                        },
                        "table": {
                            "type": "string",
                            "description": "Table name"
                        }
                    },
                    "required": ["namespace", "table"]
                }
            }
        ]

    @app.post("/mcp/tools/call")
    async def call_tool(tool_call: ToolCall) -> ToolResult:
        """Execute an MCP tool"""
        logger.info("mcp_iceberg_tool_called", tool=tool_call.name)

        try:
            if tool_call.name == "iceberg_list_namespaces":
                result = await iceberg_list_namespaces(
                    parent=tool_call.arguments.get("parent")
                )
            elif tool_call.name == "iceberg_list_tables":
                result = await iceberg_list_tables(
                    namespace=tool_call.arguments["namespace"]
                )
            elif tool_call.name == "iceberg_get_table":
                result = await iceberg_get_table(
                    namespace=tool_call.arguments["namespace"],
                    table=tool_call.arguments["table"]
                )
            elif tool_call.name == "iceberg_get_schema":
                result = await iceberg_get_schema(
                    namespace=tool_call.arguments["namespace"],
                    table=tool_call.arguments["table"]
                )
            elif tool_call.name == "iceberg_list_snapshots":
                result = await iceberg_list_snapshots(
                    namespace=tool_call.arguments["namespace"],
                    table=tool_call.arguments["table"],
                    limit=tool_call.arguments.get("limit", 10)
                )
            elif tool_call.name == "iceberg_get_partitions":
                result = await iceberg_get_partitions(
                    namespace=tool_call.arguments["namespace"],
                    table=tool_call.arguments["table"]
                )
            elif tool_call.name == "iceberg_get_table_health":
                result = await iceberg_get_table_health(
                    namespace=tool_call.arguments["namespace"],
                    table=tool_call.arguments["table"]
                )
            else:
                raise HTTPException(status_code=404, detail=f"Unknown tool: {tool_call.name}")

            return ToolResult(
                type="text",
                text=json.dumps(result, indent=2),
                metadata={"timestamp": datetime.utcnow().isoformat()}
            )

        except Exception as e:
            logger.error("mcp_iceberg_tool_failed", tool=tool_call.name, error=str(e))
            raise HTTPException(status_code=500, detail=str(e))

    async def iceberg_list_namespaces(parent: Optional[str]) -> Dict[str, Any]:
        """List namespaces (simulated medallion architecture)"""
        namespaces = ["landing", "bronze", "silver", "gold"]
        return {"namespaces": [[ns] for ns in namespaces]}

    async def iceberg_list_tables(namespace: str) -> Dict[str, Any]:
        """List tables in namespace (simulated)"""
        tables_by_namespace = {
            "bronze": ["raw_customers", "raw_orders", "raw_products"],
            "silver": ["customers", "orders", "products", "order_items"],
            "gold": ["customer_360", "sales_daily", "product_metrics"]
        }
        tables = tables_by_namespace.get(namespace, ["data"])
        return {
            "namespace": namespace,
            "tables": [{"namespace": [namespace], "name": t} for t in tables]
        }

    async def iceberg_get_table(namespace: str, table: str) -> Dict[str, Any]:
        """Get table metadata (simulated)"""
        schema = await iceberg_get_schema(namespace, table)
        partitions = await iceberg_get_partitions(namespace, table)

        return {
            "namespace": [namespace],
            "name": table,
            "location": f"gs://data-lake/{namespace}/{table}",
            "schema": schema["schema"],
            "partition_spec": partitions["partition_spec"],
            "properties": {
                "write.format.default": "parquet",
                "write.parquet.compression-codec": "snappy"
            },
            "current_snapshot_id": 1234567890123456789,
            "format_version": 2
        }

    async def iceberg_get_schema(namespace: str, table: str) -> Dict[str, Any]:
        """Get table schema (simulated)"""
        schemas = {
            "customers": [
                {"id": 1, "name": "customer_id", "type": "string", "required": True},
                {"id": 2, "name": "first_name", "type": "string", "required": False},
                {"id": 3, "name": "last_name", "type": "string", "required": False},
                {"id": 4, "name": "email", "type": "string", "required": False},
                {"id": 5, "name": "created_at", "type": "timestamp", "required": False},
            ],
            "orders": [
                {"id": 1, "name": "order_id", "type": "string", "required": True},
                {"id": 2, "name": "customer_id", "type": "string", "required": True},
                {"id": 3, "name": "order_date", "type": "date", "required": True},
                {"id": 4, "name": "total_amount", "type": "decimal(10,2)", "required": True},
                {"id": 5, "name": "status", "type": "string", "required": False}
            ],
            "products": [
                {"id": 1, "name": "product_id", "type": "string", "required": True},
                {"id": 2, "name": "name", "type": "string", "required": True},
                {"id": 3, "name": "category", "type": "string", "required": False},
                {"id": 4, "name": "price", "type": "decimal(10,2)", "required": True}
            ]
        }

        base_table = table.replace("raw_", "")
        schema = schemas.get(base_table, [
            {"id": 1, "name": "id", "type": "string", "required": True},
            {"id": 2, "name": "data", "type": "string", "required": False},
        ])

        return {
            "namespace": namespace,
            "table": table,
            "schema": {"type": "struct", "schema_id": 0, "fields": schema}
        }

    async def iceberg_list_snapshots(namespace: str, table: str, limit: int) -> Dict[str, Any]:
        """List table snapshots (simulated)"""
        snapshots = []
        base_ts = datetime.utcnow().timestamp() * 1000

        for i in range(min(limit, 5)):
            snapshots.append({
                "snapshot_id": 1234567890123456789 - i,
                "timestamp_ms": int(base_ts - (i * 86400000)),
                "operation": "append",
                "summary": {"added-files": str(10 - i), "added-records": str((10 - i) * 1000)}
            })

        return {"namespace": namespace, "table": table, "snapshots": snapshots}

    async def iceberg_get_partitions(namespace: str, table: str) -> Dict[str, Any]:
        """Get partition spec (simulated)"""
        if "order" in table.lower():
            partition_spec = [{"source_id": 3, "name": "order_date_day", "transform": "day"}]
        else:
            partition_spec = [{"source_id": 5, "name": "created_at_month", "transform": "month"}]

        return {
            "namespace": namespace,
            "table": table,
            "partition_spec": {"spec_id": 0, "fields": partition_spec}
        }

    async def iceberg_get_table_health(namespace: str, table: str) -> Dict[str, Any]:
        """Get table health metrics (for IT Service Agent)"""
        return {
            "namespace": namespace,
            "table": table,
            "health": {
                "status": "healthy",
                "last_updated": datetime.utcnow().isoformat(),
                "total_records": 1000000,
                "total_files": 50,
                "total_size_bytes": 1024000000,
                "orphan_files": 0,
                "compaction_needed": False
            },
            "quality_metrics": {
                "null_percentage": 0.02,
                "duplicate_percentage": 0.001,
                "schema_violations": 0
            }
        }

    @app.get("/health")
    async def health():
        """Health check endpoint"""
        return {
            "status": "healthy",
            "service": "iceberg-mcp-server",
            "timestamp": datetime.utcnow().isoformat()
        }

    return app


# Create app instance
app = create_iceberg_mcp_server()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8012)
