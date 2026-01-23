"""
Backend Services Layer

WHY: Backend services use agents via the registry interface.
     This layer NEVER contains agent logic - only orchestration.

HOW: Services import from agents package, create tasks,
     and call execute(). Agents return results that services
     process and persist.

ISOLATION:
- Services DO NOT contain agent logic
- Services DO NOT inherit from agent base classes
- Services use IAgentService interface only
"""
from .agent_service import BackendAgentService, get_backend_agent_service

__all__ = [
    "BackendAgentService",
    "get_backend_agent_service",
]
