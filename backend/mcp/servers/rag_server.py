#!/usr/bin/env python3
"""
RAG MCP Server - Provides MCP Protocol Access to RAG Operations

WHY: Exposes RAG capabilities as MCP tools for:
- Searching runbooks for incident remediation
- Finding Terraform scripts for infrastructure fixes
- Finding Ansible playbooks for configuration
- Updating success rates for learning

HOW: Integrates with the existing RAG infrastructure:
- Uses HybridSearchEngine for multi-strategy search
- Uses GraphScorer for Neo4j relationship scoring
- Tracks success rates for feedback loop

RUN: python -m backend.mcp.servers.rag_server
"""
import os
import sys
import asyncio
from typing import Any, Dict, List, Optional
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent
import structlog

# Ensure backend is in path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))

from backend.rag.hybrid_search_engine import HybridSearchEngine
from backend.rag.graph_scorer import GraphScorer

logger = structlog.get_logger()

SERVER_NAME = "rag-mcp"


class RAGMCPServer:
    """
    MCP Server for RAG operations.

    WHY as MCP Server:
    - Standardized interface for LLM tool calling
    - Input validation via JSON Schema
    - Consistent error handling
    """

    def __init__(self):
        self.server = Server("rag-mcp")
        self.search_engine = None
        self.graph_scorer = None
        self._register_tools()

    def _init_rag_components(self):
        """Lazy initialization of RAG components"""
        if self.search_engine is None:
            try:
                self.search_engine = HybridSearchEngine()
                logger.info("rag_search_engine_initialized")
            except Exception as e:
                logger.warning("rag_search_engine_init_failed", error=str(e))

        if self.graph_scorer is None:
            try:
                self.graph_scorer = GraphScorer()
                logger.info("rag_graph_scorer_initialized")
            except Exception as e:
                logger.warning("rag_graph_scorer_init_failed", error=str(e))

    def _register_tools(self):
        """Register all RAG tools"""

        @self.server.list_tools()
        async def list_tools() -> List[Tool]:
            return [
                Tool(
                    name="search_runbooks",
                    description="Search runbooks for incident remediation steps",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "query": {
                                "type": "string",
                                "description": "Search query describing the incident or problem"
                            },
                            "incident_type": {
                                "type": "string",
                                "description": "Type of incident (e.g., 'database', 'network', 'vm')"
                            },
                            "max_results": {
                                "type": "integer",
                                "description": "Maximum number of results to return",
                                "default": 5
                            }
                        },
                        "required": ["query"]
                    }
                ),
                Tool(
                    name="find_terraform",
                    description="Find Terraform scripts for infrastructure remediation",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "resource_type": {
                                "type": "string",
                                "description": "Type of resource (e.g., 'gcp_instance', 'aws_ec2')"
                            },
                            "action": {
                                "type": "string",
                                "enum": ["scale", "restart", "modify", "create", "destroy"],
                                "description": "Action to perform"
                            },
                            "environment": {
                                "type": "string",
                                "enum": ["dev", "staging", "prod"],
                                "description": "Target environment"
                            }
                        },
                        "required": ["resource_type", "action"]
                    }
                ),
                Tool(
                    name="find_ansible",
                    description="Find Ansible playbooks for configuration remediation",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "service": {
                                "type": "string",
                                "description": "Service name (e.g., 'nginx', 'postgres', 'redis')"
                            },
                            "action": {
                                "type": "string",
                                "enum": ["restart", "configure", "patch", "scale"],
                                "description": "Action to perform"
                            },
                            "tags": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "Optional tags to filter playbooks"
                            }
                        },
                        "required": ["service", "action"]
                    }
                ),
                Tool(
                    name="update_success",
                    description="Update success rate for a runbook after execution",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "runbook_id": {
                                "type": "string",
                                "description": "ID of the runbook that was executed"
                            },
                            "incident_id": {
                                "type": "string",
                                "description": "ID of the incident that was resolved"
                            },
                            "success": {
                                "type": "boolean",
                                "description": "Whether the execution was successful"
                            },
                            "execution_time_seconds": {
                                "type": "number",
                                "description": "How long the execution took"
                            },
                            "notes": {
                                "type": "string",
                                "description": "Optional notes about the execution"
                            }
                        },
                        "required": ["runbook_id", "incident_id", "success"]
                    }
                ),
                Tool(
                    name="get_similar_incidents",
                    description="Find similar past incidents for context",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "incident_description": {
                                "type": "string",
                                "description": "Description of the current incident"
                            },
                            "service": {
                                "type": "string",
                                "description": "Affected service name"
                            },
                            "max_results": {
                                "type": "integer",
                                "default": 3
                            }
                        },
                        "required": ["incident_description"]
                    }
                )
            ]

        @self.server.call_tool()
        async def call_tool(name: str, arguments: Dict[str, Any]) -> List[TextContent]:
            """Handle tool calls"""
            self._init_rag_components()

            if name == "search_runbooks":
                return await self._search_runbooks(
                    arguments["query"],
                    arguments.get("incident_type"),
                    arguments.get("max_results", 5)
                )

            elif name == "find_terraform":
                return await self._find_terraform(
                    arguments["resource_type"],
                    arguments["action"],
                    arguments.get("environment")
                )

            elif name == "find_ansible":
                return await self._find_ansible(
                    arguments["service"],
                    arguments["action"],
                    arguments.get("tags", [])
                )

            elif name == "update_success":
                return await self._update_success(
                    arguments["runbook_id"],
                    arguments["incident_id"],
                    arguments["success"],
                    arguments.get("execution_time_seconds"),
                    arguments.get("notes")
                )

            elif name == "get_similar_incidents":
                return await self._get_similar_incidents(
                    arguments["incident_description"],
                    arguments.get("service"),
                    arguments.get("max_results", 3)
                )

            else:
                raise ValueError(f"Unknown tool: {name}")

    async def _search_runbooks(
        self,
        query: str,
        incident_type: Optional[str],
        max_results: int
    ) -> List[TextContent]:
        """Search runbooks using hybrid search - PRODUCTION"""
        try:
            if not self.search_engine:
                raise RuntimeError("HybridSearchEngine not initialized. Check RAG configuration.")

            # Use RRF search for production (recommended v5.0 method)
            results = self.search_engine.search_rrf(
                query=query,
                query_metadata={"type": incident_type} if incident_type else None,
                top_k=max_results,
                enable_rerank=True
            )

            # Format results from SearchResult dataclass
            text_lines = [f"Found {len(results)} runbooks:\n"]

            for i, result in enumerate(results, 1):
                text_lines.append(f"\n{i}. {result.metadata.get('name', result.chunk_id)}")
                text_lines.append(f"   RRF Score: {result.final_score:.4f}")
                text_lines.append(f"   Sources: {', '.join(result.sources)}")
                text_lines.append(f"   Type: {result.metadata.get('type', 'unknown')}")
                text_lines.append(f"   Path: {result.metadata.get('path', 'N/A')}")
                if result.content:
                    text_lines.append(f"   Content: {result.content[:100]}...")
                if result.match_reasons:
                    text_lines.append(f"   Reasons: {'; '.join(result.match_reasons[:2])}")

            return [TextContent(type="text", text="\n".join(text_lines))]

        except Exception as e:
            logger.error("search_runbooks_failed", error=str(e))
            return [TextContent(type="text", text=f"Error searching runbooks: {str(e)}")]

    async def _find_terraform(
        self,
        resource_type: str,
        action: str,
        environment: Optional[str]
    ) -> List[TextContent]:
        """Find Terraform scripts - PRODUCTION"""
        try:
            if not self.search_engine:
                raise RuntimeError("HybridSearchEngine not initialized. Check RAG configuration.")

            # Build search query with terraform context
            query = f"terraform {resource_type} {action}"
            if environment:
                query += f" {environment} environment"

            # Use RRF search with terraform filter
            results = self.search_engine.search_rrf(
                query=query,
                query_metadata={"script_type": "terraform", "action": action},
                top_k=5,
                enable_rerank=True
            )

            # Get graph scores for historical success data
            text_lines = [f"Found {len(results)} Terraform scripts:\n"]

            for result in results:
                script_id = result.chunk_id
                graph_score = 0.0

                # Get historical success from graph scorer if available
                if self.graph_scorer:
                    graph_result = self.graph_scorer.get_score(script_id)
                    graph_score = graph_result.graph_score
                    success_rate = graph_result.success_rate
                    last_used = graph_result.last_used or "Never"
                else:
                    success_rate = result.agent_scores.get("graph", 0)
                    last_used = "N/A"

                text_lines.append(f"\nScript: {result.metadata.get('path', result.chunk_id)}")
                text_lines.append(f"  Action: {result.metadata.get('action', action)}")
                text_lines.append(f"  RRF Score: {result.final_score:.4f}")
                text_lines.append(f"  Success Rate: {success_rate:.0%}")
                text_lines.append(f"  Last Used: {last_used}")
                if result.match_reasons:
                    text_lines.append(f"  Match: {result.match_reasons[0]}")

            return [TextContent(type="text", text="\n".join(text_lines))]

        except Exception as e:
            logger.error("find_terraform_failed", error=str(e))
            return [TextContent(type="text", text=f"Error finding Terraform: {str(e)}")]

    async def _find_ansible(
        self,
        service: str,
        action: str,
        tags: List[str]
    ) -> List[TextContent]:
        """Find Ansible playbooks - PRODUCTION"""
        try:
            if not self.search_engine:
                raise RuntimeError("HybridSearchEngine not initialized. Check RAG configuration.")

            # Build search query with ansible context
            query = f"ansible playbook {service} {action}"
            if tags:
                query += f" {' '.join(tags)}"

            # Use RRF search with ansible filter
            query_metadata = {"script_type": "ansible", "service": service, "action": action}
            if tags:
                query_metadata["tags"] = tags

            results = self.search_engine.search_rrf(
                query=query,
                query_metadata=query_metadata,
                top_k=5,
                enable_rerank=True
            )

            text_lines = [f"Found {len(results)} Ansible playbooks:\n"]

            for result in results:
                script_id = result.chunk_id

                # Get historical data from graph scorer if available
                if self.graph_scorer:
                    graph_result = self.graph_scorer.get_score(script_id)
                    success_rate = graph_result.success_rate
                    avg_duration = f"{graph_result.avg_resolution_time:.1f}m" if graph_result.avg_resolution_time else "N/A"
                else:
                    success_rate = result.agent_scores.get("graph", 0)
                    avg_duration = "N/A"

                text_lines.append(f"\nPlaybook: {result.metadata.get('path', result.chunk_id)}")
                text_lines.append(f"  Service: {result.metadata.get('service', service)}")
                text_lines.append(f"  RRF Score: {result.final_score:.4f}")
                text_lines.append(f"  Success Rate: {success_rate:.0%}")
                text_lines.append(f"  Avg Duration: {avg_duration}")
                if result.metadata.get('tags'):
                    text_lines.append(f"  Tags: {', '.join(result.metadata['tags'])}")

            return [TextContent(type="text", text="\n".join(text_lines))]

        except Exception as e:
            logger.error("find_ansible_failed", error=str(e))
            return [TextContent(type="text", text=f"Error finding Ansible: {str(e)}")]

    async def _update_success(
        self,
        runbook_id: str,
        incident_id: str,
        success: bool,
        execution_time: Optional[float],
        notes: Optional[str]
    ) -> List[TextContent]:
        """Update success rate in graph database - PRODUCTION"""
        try:
            if not self.graph_scorer:
                raise RuntimeError("GraphScorer not initialized. Check Neo4j configuration.")

            # Record execution in Neo4j graph
            # Convert execution time to minutes for consistency
            resolution_time_minutes = (execution_time or 0) / 60.0 if execution_time else 0

            self.graph_scorer.record_execution(
                incident_id=incident_id,
                script_id=runbook_id,
                service=notes.split(":")[0] if notes and ":" in notes else "unknown",
                success=success,
                resolution_time=resolution_time_minutes,
                verified=False  # Will be verified by feedback loop
            )

            # Get updated stats for the runbook
            stats = self.graph_scorer.get_script_stats(runbook_id)

            text_lines = [
                f"Updated success rate for runbook {runbook_id}:",
                f"  Result: {'SUCCESS' if success else 'FAILED'}",
                f"  Total Executions: {stats.get('total_executions', 0)}",
                f"  Success Rate: {stats.get('success_rate', 'N/A')}",
                f"  Graph Score: {stats.get('graph_score', 'N/A')}"
            ]

            logger.info(
                "success_rate_updated",
                runbook_id=runbook_id,
                incident_id=incident_id,
                success=success,
                execution_time=execution_time
            )

            return [TextContent(type="text", text="\n".join(text_lines))]

        except Exception as e:
            logger.error("update_success_failed", error=str(e))
            return [TextContent(type="text", text=f"Error updating success: {str(e)}")]

    async def _get_similar_incidents(
        self,
        description: str,
        service: Optional[str],
        max_results: int
    ) -> List[TextContent]:
        """Find similar past incidents - PRODUCTION"""
        try:
            if not self.search_engine:
                raise RuntimeError("HybridSearchEngine not initialized. Check RAG configuration.")

            # Build query metadata for incident search
            query_metadata = {"type": "incident"}
            if service:
                query_metadata["service"] = service

            # Use RRF search for similar incidents
            results = self.search_engine.search_rrf(
                query=description,
                query_metadata=query_metadata,
                top_k=max_results,
                enable_rerank=True
            )

            # Also check graph for historically resolved incidents
            graph_incidents = []
            if self.graph_scorer and service:
                graph_incidents = self.graph_scorer.find_similar_resolved_incidents(
                    service=service,
                    limit=max_results
                )

            text_lines = [f"Found {len(results)} similar incidents:\n"]

            # Format RRF search results
            for result in results:
                text_lines.append(f"\nIncident: {result.metadata.get('incident_id', result.chunk_id)}")
                text_lines.append(f"  Description: {result.content[:100]}..." if result.content else "  Description: N/A")
                text_lines.append(f"  Resolution: {result.metadata.get('resolution', 'N/A')[:100] if result.metadata.get('resolution') else 'N/A'}...")
                text_lines.append(f"  RRF Score: {result.final_score:.4f}")
                text_lines.append(f"  Sources: {', '.join(result.sources)}")

            # Add graph-based similar incidents if available
            if graph_incidents:
                text_lines.append(f"\n\n--- Graph-Based Similar Incidents (FIXED_BY) ---\n")
                for gi in graph_incidents:
                    text_lines.append(f"\nIncident: {gi.get('incident_id', 'N/A')}")
                    text_lines.append(f"  Script Used: {gi.get('script_name', gi.get('script_id', 'N/A'))}")
                    text_lines.append(f"  Resolution Time: {gi.get('resolution_time', 'N/A')} min")
                    text_lines.append(f"  Executed At: {gi.get('executed_at', 'N/A')}")

            return [TextContent(type="text", text="\n".join(text_lines))]

        except Exception as e:
            logger.error("get_similar_incidents_failed", error=str(e))
            return [TextContent(type="text", text=f"Error finding similar incidents: {str(e)}")]

    async def run(self):
        """Run the MCP server"""
        async with stdio_server() as (read_stream, write_stream):
            await self.server.run(
                read_stream,
                write_stream,
                self.server.create_initialization_options()
            )


async def main():
    """Main entry point"""
    logger.info("rag_mcp_server_starting")
    server = RAGMCPServer()
    await server.run()


if __name__ == "__main__":
    asyncio.run(main())
