"""
MCP (Model Context Protocol) Layer

WHY: Provides standardized tool invocation for agents using JSON-RPC 2.0:
- RAG search operations (search_runbooks, find_terraform, find_ansible)
- ServiceNow operations (get_incident, update_status, close_ticket)
- GitHub operations (trigger_workflow, get_run_status)

HOW: Uses the official MCP SDK for client-server communication:
- MCPClient: Unified client for connecting to any MCP server
- Servers in mcp-servers/: Standalone processes for each integration

PROTOCOL CHOICE: MCP is used for tool invocation because:
- Standardized: Same interface for all tools
- Type-safe: JSON Schema for input validation
- Streaming: Supports large result sets
- Official SDK: Maintained by Anthropic
"""
from .client import MCPClient

__all__ = ["MCPClient"]
