"""
Agent Registry - Centralized Agent Discovery and Management

WHY: Backend needs a way to discover and use agents without
     hardcoding imports. The registry provides:
     - Agent registration and discovery
     - Capability-based lookup
     - Health monitoring
     - Thread-safe singleton access

HOW: Backend imports the registry, registers agents, and uses
     find_by_capability() to get appropriate agents for tasks.

USAGE:
    from agents import get_agent_registry
    from agents.servicenow_agent import ServiceNowAgentService

    # Get registry singleton
    registry = get_agent_registry()

    # Register agents
    registry.register(ServiceNowAgentService())

    # Find agents by capability
    agents = registry.find_by_capability(AgentCapability.INCIDENT_TRIAGE)

    # Execute task on appropriate agent
    result = await agents[0].execute(task)
"""
from typing import Dict, List, Optional
import threading
import structlog

from .shared.interfaces import (
    IAgentService,
    IAgentRegistry,
    AgentCapability,
)

logger = structlog.get_logger()


class AgentRegistry(IAgentRegistry):
    """
    Singleton registry for all agents in the platform.

    THREAD SAFETY: Uses lock for registration operations.
    Reads are lock-free for performance.

    DESIGN:
    - Singleton pattern ensures one registry per process
    - Agents registered by ID, looked up by ID or capability
    - Health checks can be run periodically
    """

    _instance: Optional["AgentRegistry"] = None
    _lock = threading.Lock()

    def __new__(cls) -> "AgentRegistry":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._agents: Dict[str, IAgentService] = {}
                    cls._instance._capabilities: Dict[AgentCapability, List[str]] = {}
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if not self._initialized:
            self._initialized = True
            logger.info("agent_registry_initialized")

    def register(self, agent: IAgentService) -> None:
        """
        Register an agent with the registry.

        Args:
            agent: Agent implementing IAgentService

        Raises:
            ValueError: If agent with same ID already registered
        """
        agent_id = agent.agent_id

        with self._lock:
            if agent_id in self._agents:
                logger.warning(
                    "agent_already_registered",
                    agent_id=agent_id,
                    replacing=True,
                )

            self._agents[agent_id] = agent

            # Index by capability
            for capability in agent.capabilities:
                if capability not in self._capabilities:
                    self._capabilities[capability] = []
                if agent_id not in self._capabilities[capability]:
                    self._capabilities[capability].append(agent_id)

        logger.info(
            "agent_registered",
            agent_id=agent_id,
            agent_name=agent.agent_name,
            capabilities=[c.value for c in agent.capabilities],
        )

    def unregister(self, agent_id: str) -> None:
        """
        Unregister an agent.

        Args:
            agent_id: ID of agent to unregister
        """
        with self._lock:
            if agent_id in self._agents:
                agent = self._agents.pop(agent_id)

                # Remove from capability index
                for capability in agent.capabilities:
                    if capability in self._capabilities:
                        self._capabilities[capability] = [
                            aid for aid in self._capabilities[capability]
                            if aid != agent_id
                        ]

                logger.info("agent_unregistered", agent_id=agent_id)
            else:
                logger.warning("agent_not_found", agent_id=agent_id)

    def get(self, agent_id: str) -> Optional[IAgentService]:
        """
        Get an agent by ID.

        Args:
            agent_id: Agent identifier

        Returns:
            Agent if found, None otherwise
        """
        return self._agents.get(agent_id)

    def find_by_capability(
        self,
        capability: AgentCapability,
    ) -> List[IAgentService]:
        """
        Find all agents with a specific capability.

        Args:
            capability: Capability to search for

        Returns:
            List of agents with that capability
        """
        agent_ids = self._capabilities.get(capability, [])
        return [self._agents[aid] for aid in agent_ids if aid in self._agents]

    def find_by_task_type(self, task_type: str) -> List[IAgentService]:
        """
        Find agents that can handle a task type.

        Args:
            task_type: Type of task

        Returns:
            List of agents that can handle the task
        """
        return [
            agent for agent in self._agents.values()
            if agent.can_handle(task_type)
        ]

    def list_all(self) -> List[IAgentService]:
        """List all registered agents"""
        return list(self._agents.values())

    def list_ids(self) -> List[str]:
        """List all registered agent IDs"""
        return list(self._agents.keys())

    def list_capabilities(self) -> Dict[str, List[str]]:
        """List all capabilities and their agents"""
        return {
            cap.value: agent_ids
            for cap, agent_ids in self._capabilities.items()
        }

    async def health_check_all(self) -> Dict[str, Dict]:
        """
        Run health checks on all agents.

        Returns:
            Dict mapping agent_id to health status
        """
        results = {}
        for agent_id, agent in self._agents.items():
            try:
                results[agent_id] = await agent.health_check()
            except Exception as e:
                results[agent_id] = {
                    "status": "error",
                    "error": str(e),
                }
        return results

    def clear(self) -> None:
        """Clear all registered agents (for testing)"""
        with self._lock:
            self._agents.clear()
            self._capabilities.clear()
            logger.info("agent_registry_cleared")


# Global singleton access
_registry: Optional[AgentRegistry] = None


def get_agent_registry() -> AgentRegistry:
    """
    Get the global agent registry singleton.

    USAGE:
        from agents import get_agent_registry

        registry = get_agent_registry()
        registry.register(MyAgent())
    """
    global _registry
    if _registry is None:
        _registry = AgentRegistry()
    return _registry


def register_default_agents() -> AgentRegistry:
    """
    Register all default agents with the registry.

    Call this at application startup to ensure all agents are available.

    USAGE:
        from agents import register_default_agents

        # In app startup
        registry = register_default_agents()
    """
    registry = get_agent_registry()

    # Import and register ServiceNow agents
    try:
        from .servicenow_agent import ServiceNowAgentService
        registry.register(ServiceNowAgentService())
        logger.info("registered_servicenow_agents")
    except ImportError as e:
        logger.warning("servicenow_agents_not_available", error=str(e))

    # Data agent would be registered here if we port it to new structure
    # try:
    #     from .data_agent import DataAgentService
    #     registry.register(DataAgentService())
    # except ImportError:
    #     pass

    return registry
