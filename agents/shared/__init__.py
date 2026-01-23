"""
Shared Agent Infrastructure

WHY: Provides common base classes, interfaces, and utilities
     that ALL agents must use for consistency.

HOW: Agents extend BaseAgent and implement IAgentService interface.
     This ensures consistent behavior across data_agent and servicenow_agent.

Components:
- interfaces.py: ABC contracts for all agents
- base.py: Unified base agent class
- config.py: Agent configuration management
- types.py: Shared type definitions
"""
from .interfaces import (
    IAgentService,
    IAgentTask,
    IAgentResult,
    AgentCapability,
    IAgentRegistry,
)
from .base import BaseAgent
from .config import AgentConfig, LLMConfig, LLMProvider
from .types import (
    TaskPriority,
    TaskStatus,
    AgentType,
    RiskLevel,
    ApprovalRoute,
    AgentContext,
    IncidentData,
    ScriptMatch,
    PipelineSpec,
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
    "LLMConfig",
    "LLMProvider",
    # Types
    "TaskPriority",
    "TaskStatus",
    "AgentType",
    "RiskLevel",
    "ApprovalRoute",
    "AgentContext",
    "IncidentData",
    "ScriptMatch",
    "PipelineSpec",
]
