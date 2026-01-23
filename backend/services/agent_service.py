"""
Backend Agent Service - Interface for Using Agents

WHY: Backend needs to use agents without containing agent logic.
     This service provides:
     - Agent discovery via registry
     - Task creation and execution
     - Result processing
     - Integration with backend systems (RAG, Kafka, etc.)

HOW: Service creates IAgentTask, finds appropriate agent via registry,
     calls execute(), and processes IAgentResult.

SEPARATION OF CONCERNS:
- Agent logic lives in agents/ module
- Backend orchestration lives here
- RAG/Kafka integration is done BEFORE calling agents

USAGE:
    from backend.services import get_backend_agent_service

    service = get_backend_agent_service()

    # Triage an incident
    result = await service.triage_incident(
        incident_id="INC0001234",
        description="Server down",
        service="payment-api"
    )

    # Match scripts
    result = await service.match_scripts(
        incident_id="INC0001234",
        description="Server down",
        candidate_scripts=[...]
    )
"""
import sys
import os
import uuid
from typing import Any, Dict, List, Optional
from datetime import datetime
import structlog

# Add agents to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from agents import (
    get_agent_registry,
    register_default_agents,
    IAgentTask,
    IAgentResult,
    AgentCapability,
)

logger = structlog.get_logger()


class BackendAgentService:
    """
    Backend service for interacting with agents.

    This class is the ONLY way backend should use agents.
    It handles:
    - Agent registration on startup
    - Task creation with proper context
    - RAG context injection (from backend systems)
    - Result processing and persistence
    """

    def __init__(self):
        # Register default agents
        self.registry = register_default_agents()
        self._rag_client = None  # Lazy initialized
        self._kafka_publisher = None  # Lazy initialized

        logger.info(
            "backend_agent_service_initialized",
            registered_agents=self.registry.list_ids(),
        )

    def _get_rag_context(
        self,
        query: str,
        metadata: Optional[Dict] = None,
        top_k: int = 5,
    ) -> Dict[str, List[Dict]]:
        """
        Get context from RAG system.

        WHY: Backend owns RAG integration, not agents.
        Agents receive context as part of the task.
        """
        try:
            # Lazy import to avoid circular deps
            from rag import hybrid_rag

            results = hybrid_rag.search(
                query=query,
                query_metadata=metadata or {},
                top_k=top_k,
            )

            similar_incidents = []
            runbooks = []

            for result in results or []:
                item = result.to_dict() if hasattr(result, 'to_dict') else result
                doc_type = item.get('doc_type', 'unknown')

                if doc_type == 'incident':
                    similar_incidents.append(item)
                else:
                    runbooks.append(item)

            return {
                "similar_incidents": similar_incidents,
                "runbooks": runbooks,
            }

        except Exception as e:
            logger.warning("rag_context_failed", error=str(e))
            return {"similar_incidents": [], "runbooks": []}

    async def triage_incident(
        self,
        incident_id: str,
        description: str,
        short_description: str = "",
        service: Optional[str] = None,
        category: str = "unknown",
        priority: str = "3",
        include_rag_context: bool = True,
    ) -> Dict[str, Any]:
        """
        Triage an incident using the incident agent.

        Args:
            incident_id: ServiceNow incident ID
            description: Full description
            short_description: Brief summary
            service: Affected service name
            category: Incident category
            priority: Priority level
            include_rag_context: Whether to include RAG context

        Returns:
            Dict with classification, root cause analysis, and resolution
        """
        # Find agent
        agents = self.registry.find_by_capability(AgentCapability.INCIDENT_TRIAGE)
        if not agents:
            logger.error("no_incident_agent_found")
            return {"error": "No incident triage agent available"}

        agent = agents[0]

        # Get RAG context if requested
        context = {}
        if include_rag_context:
            context = self._get_rag_context(
                query=f"{short_description}\n{description}",
                metadata={"service": service} if service else None,
            )

        # Create task
        task = IAgentTask(
            task_id=f"triage-{incident_id}-{uuid.uuid4().hex[:8]}",
            task_type="incident_triage",
            payload={
                "incident_id": incident_id,
                "short_description": short_description,
                "description": description,
                "category": category,
                "priority": priority,
                "service": service,
            },
            context=context,
        )

        logger.info(
            "triaging_incident",
            incident_id=incident_id,
            agent_id=agent.agent_id,
            context_items=len(context.get("similar_incidents", [])) + len(context.get("runbooks", [])),
        )

        # Execute
        result = await agent.execute(task)

        # Process result
        if result.success:
            logger.info(
                "incident_triaged",
                incident_id=incident_id,
                execution_time_ms=result.execution_time_ms,
            )
            return result.output
        else:
            logger.error(
                "incident_triage_failed",
                incident_id=incident_id,
                error=result.error,
            )
            return {
                "error": result.error,
                "error_details": result.error_details,
            }

    async def match_scripts(
        self,
        incident_id: str,
        description: str,
        candidate_scripts: Optional[List[Dict]] = None,
        category: str = "unknown",
    ) -> Dict[str, Any]:
        """
        Match incident to remediation scripts.

        Args:
            incident_id: Incident ID
            description: Incident description
            candidate_scripts: Pre-filtered scripts (if None, will search RAG)
            category: Incident category for filtering

        Returns:
            Dict with matches, best_match, and analysis
        """
        # Find agent
        agents = self.registry.find_by_capability(AgentCapability.SCRIPT_MATCHING)
        if not agents:
            return {"error": "No script matching agent available"}

        agent = agents[0]

        # Get candidate scripts from RAG if not provided
        if candidate_scripts is None:
            try:
                from rag import hybrid_rag
                results = hybrid_rag.search(
                    query=description,
                    query_metadata={"type": "script", "category": category},
                    top_k=10,
                )
                candidate_scripts = [
                    {
                        "script_id": r.doc_id if hasattr(r, 'doc_id') else r.get('doc_id', ''),
                        "name": r.metadata.get('name', '') if hasattr(r, 'metadata') else r.get('name', ''),
                        "description": r.content[:200] if hasattr(r, 'content') else r.get('content', '')[:200],
                        "type": r.metadata.get('type', 'python') if hasattr(r, 'metadata') else r.get('type', 'python'),
                        "required_inputs": r.metadata.get('required_inputs', []) if hasattr(r, 'metadata') else [],
                        "risk_level": r.metadata.get('risk_level', 'medium') if hasattr(r, 'metadata') else 'medium',
                    }
                    for r in (results or [])
                ]
            except Exception as e:
                logger.warning("script_search_failed", error=str(e))
                candidate_scripts = []

        # Create task
        task = IAgentTask(
            task_id=f"match-{incident_id}-{uuid.uuid4().hex[:8]}",
            task_type="script_matching",
            payload={
                "incident_id": incident_id,
                "description": description,
                "category": category,
                "candidate_scripts": candidate_scripts,
            },
        )

        # Execute
        result = await agent.execute(task)

        if result.success:
            return result.output
        else:
            return {"error": result.error}

    async def generate_remediation_plan(
        self,
        incident_id: str,
        script: Dict[str, Any],
        inputs: Dict[str, Any],
        environment: str = "development",
        dry_run: bool = True,
    ) -> Dict[str, Any]:
        """
        Generate remediation execution plan.

        Args:
            incident_id: Incident ID
            script: Script metadata
            inputs: Input values for script
            environment: Target environment
            dry_run: Whether to run in dry-run mode

        Returns:
            Dict with execution_plan, validation, risk_assessment, rollback_plan
        """
        # Find agent
        agents = self.registry.find_by_capability(AgentCapability.REMEDIATION)
        if not agents:
            return {"error": "No remediation agent available"}

        agent = agents[0]

        # Create task
        task = IAgentTask(
            task_id=f"remediate-{incident_id}-{uuid.uuid4().hex[:8]}",
            task_type="remediation",
            payload={
                "incident_id": incident_id,
                "script": script,
                "inputs": inputs,
                "environment": environment,
                "dry_run": dry_run,
            },
        )

        # Execute
        result = await agent.execute(task)

        if result.success:
            return result.output
        else:
            return {"error": result.error}

    async def health_check(self) -> Dict[str, Any]:
        """Check health of all agents"""
        agent_health = await self.registry.health_check_all()

        all_healthy = all(
            h.get("status") == "healthy"
            for h in agent_health.values()
        )

        return {
            "status": "healthy" if all_healthy else "degraded",
            "timestamp": datetime.utcnow().isoformat(),
            "agents": agent_health,
            "registered_count": len(self.registry.list_ids()),
        }


# Singleton instance
_backend_agent_service: Optional[BackendAgentService] = None


def get_backend_agent_service() -> BackendAgentService:
    """Get the singleton backend agent service"""
    global _backend_agent_service
    if _backend_agent_service is None:
        _backend_agent_service = BackendAgentService()
    return _backend_agent_service
