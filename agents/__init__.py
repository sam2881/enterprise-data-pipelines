"""
Agents Module - AI Agent Platform

WHY: Agents must be ISOLATED from backend and frontend layers.
     This is the single source of truth for all agent implementations.
     Backend/frontend use agents via well-defined interfaces, never directly.

ARCHITECTURE:
    agents/                      # Independent of backend/frontend
    ├── shared/                  # Common agent infrastructure
    │   ├── base.py             # Unified base class
    │   ├── interfaces.py       # Agent contracts (ABC interfaces)
    │   ├── config.py           # Agent configuration
    │   └── types.py            # Shared type definitions
    │
    ├── data_agent/             # Data Engineering Agent (Medallion Architecture)
    │   ├── src/agents/         # Pipeline generation agents
    │   ├── src/orchestration/  # Workflow coordination
    │   ├── src/quality/        # Data quality (Great Expectations)
    │   └── src/metadata/       # Pipeline metadata
    │
    └── servicenow_agent/       # ServiceNow Platform Agent
        ├── it_service/         # Incident management
        ├── infrastructure/     # GCP operations
        └── remediation/        # Script execution

NOTE: A2A protocol lives in platform_services/protocols/a2a/ (single source of truth)

DESIGN PRINCIPLES:
1. Agent Isolation: Agents have NO imports from backend or frontend
2. Interface-First: Backend communicates via IAgentService interface
3. Async-First: All agents support async execution
4. Deterministic: Input/output hashing for reproducibility
5. Testable: Each agent is independently unit testable

USAGE (from backend):
    from agents.servicenow_agent import ServiceNowAgentService
    from agents.data_agent.src import DataAgentAPI
    from agents.shared.interfaces import IAgentService

    # Backend uses interface, not implementation
    agent: IAgentService = ServiceNowAgentService()
    result = await agent.execute(task)
"""
# Version
__version__ = "5.0.0"

# Import shared interfaces for type hints
from .shared.interfaces import (
    IAgentService,
    IAgentTask,
    IAgentResult,
    AgentCapability,
    IAgentRegistry,
)

from .shared.base import BaseAgent
from .shared.config import AgentConfig

# Registry for agent discovery
from .registry import (
    AgentRegistry,
    get_agent_registry,
    register_default_agents,
)

__all__ = [
    # Base Class
    "BaseAgent",
    # Interfaces
    "IAgentService",
    "IAgentTask",
    "IAgentResult",
    "AgentCapability",
    "IAgentRegistry",
    # Config
    "AgentConfig",
    # Registry
    "AgentRegistry",
    "get_agent_registry",
    "register_default_agents",
]
