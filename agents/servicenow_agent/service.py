"""
ServiceNow Agent Service - Unified Entry Point

WHY: Provides a single IAgentService implementation that backend uses.
     Routes tasks to appropriate sub-agents internally.

HOW: Backend creates ServiceNowAgentService and calls execute().
     Service routes to IncidentAgent, ScriptMatcherAgent, or RemediationAgent
     based on task_type.

USAGE (from backend):
    from agents.servicenow_agent import ServiceNowAgentService

    service = ServiceNowAgentService()
    result = await service.execute(IAgentTask(
        task_id="...",
        task_type="incident_triage",  # or "script_matching", "remediation"
        payload={...}
    ))
"""
from typing import Any, Dict, List, Optional
import structlog

from agents.shared import (
    BaseAgent,
    IAgentService,
    IAgentTask,
    IAgentResult,
    AgentCapability,
    AgentConfig,
)
from .it_service.incident_agent import IncidentAgent
from .it_service.matcher_agent import ScriptMatcherAgent
from .remediation.remediation_agent import RemediationAgent

logger = structlog.get_logger()


class ServiceNowAgentService(IAgentService):
    """
    Unified ServiceNow Agent Service.

    This is the ONLY class backend should import from servicenow_agent.
    It routes tasks to appropriate sub-agents based on task_type.

    Supported task_types:
    - incident_triage: Analyze and classify incident
    - script_matching: Match incident to remediation scripts
    - remediation: Generate execution plan for script

    Example:
        service = ServiceNowAgentService()

        # Triage an incident
        result = await service.execute(IAgentTask(
            task_id="task-001",
            task_type="incident_triage",
            payload={
                "incident_id": "INC0001234",
                "description": "Server down",
            }
        ))

        # Match scripts
        result = await service.execute(IAgentTask(
            task_id="task-002",
            task_type="script_matching",
            payload={
                "incident_id": "INC0001234",
                "description": "...",
                "candidate_scripts": [...]
            }
        ))
    """

    def __init__(self, config: Optional[AgentConfig] = None):
        self.config = config or AgentConfig.from_environment(
            agent_id="servicenow-platform-agent",
            agent_name="ServiceNow Platform Agent",
        )

        # Initialize sub-agents
        self._incident_agent = IncidentAgent(self.config)
        self._matcher_agent = ScriptMatcherAgent(self.config)
        self._remediation_agent = RemediationAgent(self.config)

        # Task routing
        self._task_routing = {
            "incident_triage": self._incident_agent,
            "incident_analysis": self._incident_agent,
            "classify_incident": self._incident_agent,
            "script_matching": self._matcher_agent,
            "match_scripts": self._matcher_agent,
            "find_remediation": self._matcher_agent,
            "remediation": self._remediation_agent,
            "execute_plan": self._remediation_agent,
            "generate_rollback": self._remediation_agent,
        }

        logger.info(
            "servicenow_service_initialized",
            agent_id=self.agent_id,
            sub_agents=["incident", "matcher", "remediation"],
        )

    @property
    def agent_id(self) -> str:
        return "servicenow-platform-agent"

    @property
    def agent_name(self) -> str:
        return "ServiceNow Platform Agent"

    @property
    def capabilities(self) -> List[AgentCapability]:
        return [
            AgentCapability.INCIDENT_TRIAGE,
            AgentCapability.SCRIPT_MATCHING,
            AgentCapability.REMEDIATION,
        ]

    @property
    def version(self) -> str:
        return "5.0.0"

    async def execute(self, task: IAgentTask) -> IAgentResult:
        """
        Execute task by routing to appropriate sub-agent.

        Args:
            task: Task with task_type indicating which sub-agent to use

        Returns:
            IAgentResult from the sub-agent
        """
        task_type = task.task_type.lower()

        # Find appropriate agent
        agent = self._task_routing.get(task_type)

        if not agent:
            logger.warning(
                "unknown_task_type",
                task_type=task_type,
                available_types=list(self._task_routing.keys()),
            )
            return IAgentResult(
                task_id=task.task_id,
                success=False,
                output={},
                error=f"Unknown task type: {task_type}",
                error_details={
                    "task_type": task_type,
                    "available_types": list(self._task_routing.keys()),
                },
            )

        logger.info(
            "routing_task",
            task_id=task.task_id,
            task_type=task_type,
            target_agent=agent.agent_id,
        )

        # Execute on sub-agent
        return await agent.execute(task)

    async def health_check(self) -> Dict[str, Any]:
        """Check health of all sub-agents"""
        sub_agent_health = {}

        for name, agent in [
            ("incident", self._incident_agent),
            ("matcher", self._matcher_agent),
            ("remediation", self._remediation_agent),
        ]:
            try:
                sub_agent_health[name] = await agent.health_check()
            except Exception as e:
                sub_agent_health[name] = {
                    "status": "unhealthy",
                    "error": str(e),
                }

        all_healthy = all(
            h.get("status") == "healthy"
            for h in sub_agent_health.values()
        )

        return {
            "agent_id": self.agent_id,
            "agent_name": self.agent_name,
            "version": self.version,
            "status": "healthy" if all_healthy else "degraded",
            "capabilities": [c.value for c in self.capabilities],
            "sub_agents": sub_agent_health,
        }

    def can_handle(self, task_type: str) -> bool:
        """Check if this service can handle a task type"""
        return task_type.lower() in self._task_routing

    def get_agent_for_task(self, task_type: str) -> Optional[IAgentService]:
        """Get the specific sub-agent for a task type"""
        return self._task_routing.get(task_type.lower())
