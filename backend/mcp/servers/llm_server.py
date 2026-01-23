#!/usr/bin/env python3
"""
LLM MCP Server - Shared MCP Server for LLM Operations

WHY: Provides LLM operations as MCP tools for:
- IT Service Agent: Incident analysis, remediation planning
- Data Agent: Schema analysis, code generation, data profiling

HOW: FastAPI server exposing LLM operations as MCP-compatible tools.
Uses temperature=0 for deterministic outputs. Tracks input/output hashes.

RUN: python -m backend.mcp.servers.llm_server
"""
import os
import sys
import json
import hashlib
from typing import Any, Dict, List, Optional
from datetime import datetime
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import structlog

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))

logger = structlog.get_logger()

SERVER_NAME = "llm-mcp"


class ToolCall(BaseModel):
    """MCP Tool call request"""
    name: str
    arguments: Dict[str, Any]


class ToolResult(BaseModel):
    """MCP Tool call result"""
    type: str = "text"
    text: str
    metadata: Optional[Dict[str, Any]] = None


def compute_hash(data: Any) -> str:
    """Compute deterministic hash for input/output tracking"""
    return hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()[:16]


def create_llm_mcp_server() -> FastAPI:
    """
    Create MCP server for LLM operations.

    Shared tools for:
    - IT Service Agent: Incident analysis, plan generation
    - Data Agent: Schema analysis, code generation
    """
    app = FastAPI(
        title="LLM MCP Server",
        description="Shared MCP server for LLM operations",
        version="1.0.0"
    )

    @app.get("/mcp/tools")
    async def list_tools() -> List[Dict[str, Any]]:
        """List available MCP tools"""
        return [
            {
                "name": "llm_analyze_incident",
                "description": "Analyze an incident using LLM (for IT Service Agent)",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "incident_description": {
                            "type": "string",
                            "description": "Incident description"
                        },
                        "context": {
                            "type": "object",
                            "description": "Additional context (logs, metrics)"
                        }
                    },
                    "required": ["incident_description"]
                }
            },
            {
                "name": "llm_generate_remediation_plan",
                "description": "Generate remediation plan for incident",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "incident_analysis": {
                            "type": "object",
                            "description": "Analysis from llm_analyze_incident"
                        },
                        "available_scripts": {
                            "type": "array",
                            "description": "Available remediation scripts"
                        }
                    },
                    "required": ["incident_analysis"]
                }
            },
            {
                "name": "llm_analyze_schema",
                "description": "Analyze data schema and suggest transformations (for Data Agent)",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "source_schema": {
                            "type": "array",
                            "description": "Source schema fields"
                        },
                        "target_layer": {
                            "type": "string",
                            "enum": ["bronze", "silver", "gold"],
                            "description": "Target medallion layer"
                        },
                        "business_context": {
                            "type": "string",
                            "description": "Business context for the data"
                        }
                    },
                    "required": ["source_schema", "target_layer"]
                }
            },
            {
                "name": "llm_generate_spark_code",
                "description": "Generate PySpark transformation code (for Data Agent)",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "transformation_spec": {
                            "type": "object",
                            "description": "Transformation specification (IR)"
                        },
                        "source_path": {
                            "type": "string",
                            "description": "Source data path"
                        },
                        "target_path": {
                            "type": "string",
                            "description": "Target data path"
                        }
                    },
                    "required": ["transformation_spec"]
                }
            },
            {
                "name": "llm_generate_dq_rules",
                "description": "Generate data quality rules (for Data Agent)",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "schema": {
                            "type": "array",
                            "description": "Table schema"
                        },
                        "business_rules": {
                            "type": "array",
                            "description": "Business validation rules"
                        }
                    },
                    "required": ["schema"]
                }
            },
            {
                "name": "llm_text_completion",
                "description": "General text completion with deterministic output",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "prompt": {
                            "type": "string",
                            "description": "Prompt text"
                        },
                        "max_tokens": {
                            "type": "integer",
                            "description": "Maximum tokens",
                            "default": 1000
                        },
                        "system_prompt": {
                            "type": "string",
                            "description": "System prompt"
                        }
                    },
                    "required": ["prompt"]
                }
            }
        ]

    @app.post("/mcp/tools/call")
    async def call_tool(tool_call: ToolCall) -> ToolResult:
        """Execute an MCP tool"""
        logger.info("mcp_llm_tool_called", tool=tool_call.name)
        input_hash = compute_hash(tool_call.arguments)

        try:
            if tool_call.name == "llm_analyze_incident":
                result = await llm_analyze_incident(
                    incident_description=tool_call.arguments["incident_description"],
                    context=tool_call.arguments.get("context")
                )
            elif tool_call.name == "llm_generate_remediation_plan":
                result = await llm_generate_remediation_plan(
                    incident_analysis=tool_call.arguments["incident_analysis"],
                    available_scripts=tool_call.arguments.get("available_scripts", [])
                )
            elif tool_call.name == "llm_analyze_schema":
                result = await llm_analyze_schema(
                    source_schema=tool_call.arguments["source_schema"],
                    target_layer=tool_call.arguments["target_layer"],
                    business_context=tool_call.arguments.get("business_context")
                )
            elif tool_call.name == "llm_generate_spark_code":
                result = await llm_generate_spark_code(
                    transformation_spec=tool_call.arguments["transformation_spec"],
                    source_path=tool_call.arguments.get("source_path", "gs://input"),
                    target_path=tool_call.arguments.get("target_path", "gs://output")
                )
            elif tool_call.name == "llm_generate_dq_rules":
                result = await llm_generate_dq_rules(
                    schema=tool_call.arguments["schema"],
                    business_rules=tool_call.arguments.get("business_rules", [])
                )
            elif tool_call.name == "llm_text_completion":
                result = await llm_text_completion(
                    prompt=tool_call.arguments["prompt"],
                    max_tokens=tool_call.arguments.get("max_tokens", 1000),
                    system_prompt=tool_call.arguments.get("system_prompt")
                )
            else:
                raise HTTPException(status_code=404, detail=f"Unknown tool: {tool_call.name}")

            output_hash = compute_hash(result)

            return ToolResult(
                type="text",
                text=json.dumps(result, indent=2),
                metadata={
                    "timestamp": datetime.utcnow().isoformat(),
                    "input_hash": input_hash,
                    "output_hash": output_hash,
                    "deterministic": True
                }
            )

        except Exception as e:
            logger.error("mcp_llm_tool_failed", tool=tool_call.name, error=str(e))
            raise HTTPException(status_code=500, detail=str(e))

    async def llm_analyze_incident(
        incident_description: str,
        context: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Analyze incident using LLM (simulated)"""
        # In production, this would call Claude/OpenAI API
        return {
            "root_cause": "Resource exhaustion detected",
            "severity": "high",
            "affected_services": ["database", "api"],
            "recommended_actions": [
                "Scale database resources",
                "Clear stale connections",
                "Review query performance"
            ],
            "confidence": 0.85
        }

    async def llm_generate_remediation_plan(
        incident_analysis: Dict[str, Any],
        available_scripts: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Generate remediation plan (simulated)"""
        return {
            "plan_id": f"plan_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
            "steps": [
                {"order": 1, "action": "Scale database", "script_id": "db-scale-001", "risk": "low"},
                {"order": 2, "action": "Clear connections", "script_id": "db-conn-clear", "risk": "medium"},
                {"order": 3, "action": "Verify service health", "script_id": "health-check", "risk": "low"}
            ],
            "estimated_duration_minutes": 15,
            "requires_approval": True,
            "rollback_plan": {
                "steps": ["Revert scaling", "Restore connections"]
            }
        }

    async def llm_analyze_schema(
        source_schema: List[Dict[str, Any]],
        target_layer: str,
        business_context: Optional[str]
    ) -> Dict[str, Any]:
        """Analyze schema for data transformation (for Data Agent)"""
        transformations = []

        for field in source_schema:
            transform = {
                "source_field": field.get("name"),
                "target_field": field.get("name"),
                "transformation": "passthrough"
            }

            # Apply layer-specific transformations
            if target_layer == "silver":
                if "date" in field.get("name", "").lower():
                    transform["transformation"] = "parse_date"
                elif field.get("type") == "STRING" and "id" not in field.get("name", "").lower():
                    transform["transformation"] = "trim_and_normalize"
            elif target_layer == "gold":
                if "amount" in field.get("name", "").lower():
                    transform["transformation"] = "aggregate_sum"

            transformations.append(transform)

        return {
            "source_schema": source_schema,
            "target_layer": target_layer,
            "transformations": transformations,
            "suggested_partitioning": "date" if any("date" in f.get("name", "").lower() for f in source_schema) else None,
            "quality_checks": [
                {"type": "not_null", "fields": [f["name"] for f in source_schema if f.get("required")]},
                {"type": "unique", "fields": [f["name"] for f in source_schema if "id" in f.get("name", "").lower()]}
            ]
        }

    async def llm_generate_spark_code(
        transformation_spec: Dict[str, Any],
        source_path: str,
        target_path: str
    ) -> Dict[str, Any]:
        """Generate PySpark code (simulated)"""
        code = f'''
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("DataTransformation").getOrCreate()

# Read source data
df = spark.read.parquet("{source_path}")

# Apply transformations
df_transformed = df

# Write to target
df_transformed.write.mode("overwrite").parquet("{target_path}")

print("Transformation complete")
'''
        return {
            "code": code,
            "language": "python",
            "framework": "pyspark",
            "source_path": source_path,
            "target_path": target_path
        }

    async def llm_generate_dq_rules(
        schema: List[Dict[str, Any]],
        business_rules: List[str]
    ) -> Dict[str, Any]:
        """Generate data quality rules (for Data Agent)"""
        expectations = []

        for field in schema:
            field_name = field.get("name", "")

            # Not null check for required fields
            if field.get("required"):
                expectations.append({
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": field_name}
                })

            # Type-specific checks
            if "email" in field_name.lower():
                expectations.append({
                    "expectation_type": "expect_column_values_to_match_regex",
                    "kwargs": {"column": field_name, "regex": r"^[\w\.-]+@[\w\.-]+\.\w+$"}
                })
            elif "id" in field_name.lower():
                expectations.append({
                    "expectation_type": "expect_column_values_to_be_unique",
                    "kwargs": {"column": field_name}
                })

        return {
            "expectations": expectations,
            "suite_name": "auto_generated_suite",
            "data_asset_name": "dataset"
        }

    async def llm_text_completion(
        prompt: str,
        max_tokens: int,
        system_prompt: Optional[str]
    ) -> Dict[str, Any]:
        """General text completion (simulated)"""
        return {
            "completion": f"[Simulated LLM response to: {prompt[:100]}...]",
            "tokens_used": min(len(prompt.split()), max_tokens),
            "model": "claude-3-sonnet",
            "temperature": 0  # Deterministic
        }

    @app.get("/health")
    async def health():
        """Health check endpoint"""
        return {
            "status": "healthy",
            "service": "llm-mcp-server",
            "timestamp": datetime.utcnow().isoformat()
        }

    return app


# Create app instance
app = create_llm_mcp_server()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8013)
