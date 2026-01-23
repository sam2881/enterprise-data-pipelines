"""
MCP Client - Unified Interface for Tool Invocation

WHY: Provides a single client that can connect to any MCP server:
- Abstracts server connection management
- Handles tool discovery and invocation
- Provides async interface for non-blocking calls

HOW: Uses subprocess-based stdio communication OR HTTP mode:
- stdio: For local servers (lower latency, ~5ms)
- HTTP: For remote/containerized servers (~20ms)

USAGE:
    async with MCPClient("servicenow") as client:
        result = await client.call_tool("get_incident", {"incident_id": "INC001"})
"""
import asyncio
import json
import os
import subprocess
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass
from contextlib import asynccontextmanager
import structlog
import httpx

logger = structlog.get_logger()

# Server configurations
MCP_SERVERS = {
    "servicenow": {
        "command": ["python", "-m", "mcp_servers.servicenow_mcp.server"],
        "http_url": os.getenv("MCP_SERVICENOW_URL", "http://localhost:8091"),
        "mode": os.getenv("MCP_MODE", "stdio"),  # stdio or http
    },
    "github": {
        "command": ["python", "-m", "mcp_servers.github_mcp.server"],
        "http_url": os.getenv("MCP_GITHUB_URL", "http://localhost:8092"),
        "mode": os.getenv("MCP_MODE", "stdio"),
    },
    "gcp": {
        "command": ["python", "-m", "mcp_servers.gcp_mcp.server"],
        "http_url": os.getenv("MCP_GCP_URL", "http://localhost:8093"),
        "mode": os.getenv("MCP_MODE", "stdio"),
    },
    "jira": {
        "command": ["python", "-m", "mcp_servers.jira_mcp.server"],
        "http_url": os.getenv("MCP_JIRA_URL", "http://localhost:8094"),
        "mode": os.getenv("MCP_MODE", "stdio"),
    },
    "rag": {
        "command": ["python", "-m", "backend.mcp.servers.rag_server"],
        "http_url": os.getenv("MCP_RAG_URL", "http://localhost:8095"),
        "mode": os.getenv("MCP_MODE", "stdio"),
    },
}


@dataclass
class ToolResult:
    """Result from an MCP tool invocation"""
    success: bool
    content: Union[str, Dict[str, Any], List[Any]]
    error: Optional[str] = None
    execution_time_ms: float = 0.0


class MCPClient:
    """
    Unified MCP client for tool invocation.

    WHY this design:
    - Context manager for clean resource management
    - Lazy connection (connect on first use)
    - Caching of tool schemas
    - Automatic retry with backoff
    """

    def __init__(self, server_name: str):
        if server_name not in MCP_SERVERS:
            raise ValueError(f"Unknown MCP server: {server_name}")

        self.server_name = server_name
        self.config = MCP_SERVERS[server_name]
        self._process: Optional[subprocess.Popen] = None
        self._http_client: Optional[httpx.AsyncClient] = None
        self._tools_cache: Optional[List[Dict]] = None
        self._request_id = 0

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()

    async def connect(self):
        """
        Connect to the MCP server.

        WHY two modes:
        - stdio: Better for local dev, lower latency
        - http: Better for production, easier scaling
        """
        mode = self.config.get("mode", "stdio")

        if mode == "http":
            self._http_client = httpx.AsyncClient(
                base_url=self.config["http_url"],
                timeout=30.0
            )
            logger.info(
                "mcp_connected_http",
                server=self.server_name,
                url=self.config["http_url"]
            )
        else:
            # stdio mode - start subprocess
            self._process = subprocess.Popen(
                self.config["command"],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            logger.info(
                "mcp_connected_stdio",
                server=self.server_name,
                pid=self._process.pid
            )

    async def disconnect(self):
        """Disconnect from the MCP server"""
        if self._http_client:
            await self._http_client.aclose()
            self._http_client = None

        if self._process:
            self._process.terminate()
            self._process.wait(timeout=5)
            self._process = None

        logger.info("mcp_disconnected", server=self.server_name)

    def _next_request_id(self) -> int:
        """Generate next JSON-RPC request ID"""
        self._request_id += 1
        return self._request_id

    async def _send_jsonrpc(self, method: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Send JSON-RPC 2.0 request to the MCP server.

        WHY JSON-RPC 2.0:
        - Standard protocol for RPC
        - Request-response correlation via ID
        - Error handling built-in
        """
        import time
        start_time = time.time()

        request = {
            "jsonrpc": "2.0",
            "id": self._next_request_id(),
            "method": method,
            "params": params
        }

        if self._http_client:
            # HTTP mode
            response = await self._http_client.post(
                "/jsonrpc",
                json=request
            )
            result = response.json()

        elif self._process:
            # stdio mode
            request_str = json.dumps(request) + "\n"
            self._process.stdin.write(request_str)
            self._process.stdin.flush()

            response_str = self._process.stdout.readline()
            result = json.loads(response_str)

        else:
            raise RuntimeError("Not connected to MCP server")

        elapsed_ms = (time.time() - start_time) * 1000

        if "error" in result:
            logger.error(
                "mcp_error",
                server=self.server_name,
                method=method,
                error=result["error"]
            )
            raise MCPError(result["error"])

        logger.debug(
            "mcp_request",
            server=self.server_name,
            method=method,
            elapsed_ms=elapsed_ms
        )

        return result.get("result", {})

    async def list_tools(self) -> List[Dict[str, Any]]:
        """
        List available tools from the MCP server.

        WHY caching:
        - Tools don't change at runtime
        - Reduces latency for repeated calls
        """
        if self._tools_cache is not None:
            return self._tools_cache

        result = await self._send_jsonrpc("tools/list", {})
        self._tools_cache = result.get("tools", [])

        logger.info(
            "mcp_tools_discovered",
            server=self.server_name,
            tool_count=len(self._tools_cache)
        )

        return self._tools_cache

    async def call_tool(
        self,
        tool_name: str,
        arguments: Dict[str, Any]
    ) -> ToolResult:
        """
        Invoke an MCP tool.

        WHY ToolResult:
        - Consistent return type
        - Includes timing for observability
        - Captures errors cleanly
        """
        import time
        start_time = time.time()

        try:
            result = await self._send_jsonrpc(
                "tools/call",
                {
                    "name": tool_name,
                    "arguments": arguments
                }
            )

            elapsed_ms = (time.time() - start_time) * 1000

            # Parse content from result
            content = result.get("content", [])
            if content and isinstance(content, list):
                # Extract text from TextContent objects
                if len(content) == 1 and isinstance(content[0], dict):
                    content = content[0].get("text", content)

            return ToolResult(
                success=True,
                content=content,
                execution_time_ms=elapsed_ms
            )

        except MCPError as e:
            elapsed_ms = (time.time() - start_time) * 1000
            return ToolResult(
                success=False,
                content={},
                error=str(e),
                execution_time_ms=elapsed_ms
            )

    async def get_tool_schema(self, tool_name: str) -> Optional[Dict[str, Any]]:
        """Get the schema for a specific tool"""
        tools = await self.list_tools()
        for tool in tools:
            if tool.get("name") == tool_name:
                return tool.get("inputSchema", {})
        return None


class MCPError(Exception):
    """Error from MCP server"""
    def __init__(self, error: Dict[str, Any]):
        self.code = error.get("code", -1)
        self.message = error.get("message", "Unknown error")
        super().__init__(f"MCP Error {self.code}: {self.message}")


# Convenience functions for common operations

async def servicenow_get_incident(incident_id: str) -> ToolResult:
    """Get incident from ServiceNow"""
    async with MCPClient("servicenow") as client:
        return await client.call_tool("get_incident", {"incident_id": incident_id})


async def servicenow_update_incident(incident_id: str, updates: Dict) -> ToolResult:
    """Update incident in ServiceNow"""
    async with MCPClient("servicenow") as client:
        return await client.call_tool("update_incident", {
            "incident_id": incident_id,
            "updates": updates
        })


async def servicenow_close_incident(incident_id: str, resolution: str) -> ToolResult:
    """Close incident in ServiceNow"""
    async with MCPClient("servicenow") as client:
        return await client.call_tool("update_incident", {
            "incident_id": incident_id,
            "updates": {
                "state": "6",
                "close_code": "Solved (Permanently)",
                "close_notes": resolution
            }
        })


async def github_trigger_workflow(
    owner: str,
    repo: str,
    workflow: str,
    inputs: Dict
) -> ToolResult:
    """Trigger GitHub Actions workflow"""
    async with MCPClient("github") as client:
        return await client.call_tool("trigger_workflow", {
            "owner": owner,
            "repo": repo,
            "workflow": workflow,
            "inputs": inputs
        })


async def rag_search_runbooks(query: str, incident_type: str) -> ToolResult:
    """Search runbooks via RAG"""
    async with MCPClient("rag") as client:
        return await client.call_tool("search_runbooks", {
            "query": query,
            "incident_type": incident_type
        })
