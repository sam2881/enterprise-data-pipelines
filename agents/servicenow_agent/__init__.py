"""
ServiceNow Platform Agent

WHY: Handles ServiceNow incident lifecycle:
     - Incident triage and classification
     - Root cause analysis
     - Script matching and remediation
     - Infrastructure recovery

HOW: Uses shared BaseAgent with async execution, determinism tracking,
     and retry logic. Communicates with backend via IAgentService interface.

ARCHITECTURE:
    servicenow_agent/
    ├── __init__.py              # This file
    ├── it_service/              # Incident management
    │   ├── incident_agent.py    # Incident triage
    │   └── matcher_agent.py     # Script matching
    ├── remediation/             # Script execution
    │   └── remediation_agent.py
    └── infrastructure/          # GCP operations
        ├── gcp_agent.py
        └── vm_recovery_agent.py

USAGE (from backend):
    from agents.servicenow_agent import (
        IncidentAgent,
        RemediationAgent,
        ServiceNowAgentService,
    )

    # Create agent service (implements IAgentService)
    service = ServiceNowAgentService()
    result = await service.execute(task)
"""
from .it_service.incident_agent import IncidentAgent
from .it_service.matcher_agent import ScriptMatcherAgent
from .remediation.remediation_agent import RemediationAgent
from .service import ServiceNowAgentService

__all__ = [
    # Individual Agents
    "IncidentAgent",
    "ScriptMatcherAgent",
    "RemediationAgent",
    # Unified Service
    "ServiceNowAgentService",
]
