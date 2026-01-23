"""
Backend Agents - DEPRECATED

!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
!!  ARCHITECTURAL VIOLATION - DO NOT ADD AGENT CODE HERE  !!
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

Agent logic has been MOVED to the agents/ module at project root.

WHY: Agent code must NOT live in backend.
     Backend should use agents via the registry interface.

CORRECT USAGE:
    # Import from agents/ module (NOT backend/agents/)
    from agents import get_agent_registry, IAgentTask
    from agents.servicenow_agent import ServiceNowAgentService

    # Use backend services layer
    from backend.services import get_backend_agent_service
    service = get_backend_agent_service()
    result = await service.triage_incident(...)

MIGRATION:
- backend/agents/it_service/* → agents/servicenow_agent/it_service/*
- backend/agents/data_pipeline/* → agents/servicenow_agent/data_pipeline/* OR data_agent/
- backend/agents/infrastructure/* → agents/servicenow_agent/infrastructure/*
- backend/agents/a2a/* → platform/protocols/a2a/*

This module re-exports from the new location for backward compatibility.
"""
import warnings

warnings.warn(
    "backend.agents is deprecated. Import from 'agents' module instead. "
    "Agent code must NOT live in backend.",
    DeprecationWarning,
    stacklevel=2
)

# Re-export from new location for backward compatibility
from agents import BaseAgent, IAgentService, IAgentTask, IAgentResult

__all__ = [
    "BaseAgent",
    "IAgentService",
    "IAgentTask",
    "IAgentResult",
]

# Legacy imports - these files still exist but should be migrated
# DO NOT use these in new code
