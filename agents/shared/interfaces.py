"""
Agent Interfaces - Contracts for Agent Communication

WHY: Backend and frontend must communicate with agents through
     well-defined interfaces, NOT direct imports of agent internals.
     This enables:
     - Agent isolation (agents have no backend/frontend dependencies)
     - Testability (mock implementations for testing)
     - Swappability (change agent implementation without affecting callers)

HOW: All agents implement IAgentService. Backend creates agent instances
     and calls execute() with IAgentTask. Agents return IAgentResult.

USAGE:
    from agents.shared.interfaces import IAgentService, IAgentTask

    class MyAgent(IAgentService):
        async def execute(self, task: IAgentTask) -> IAgentResult:
            # Agent logic here
            return IAgentResult(...)
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Protocol
import hashlib
import json


class AgentCapability(str, Enum):
    """Capabilities an agent can provide"""
    # Data Pipeline capabilities
    SCHEMA_ANALYSIS = "schema_analysis"
    SPARK_GENERATION = "spark_generation"
    DAG_GENERATION = "dag_generation"
    DQ_GENERATION = "dq_generation"
    SSIS_MIGRATION = "ssis_migration"

    # IT Service capabilities
    INCIDENT_TRIAGE = "incident_triage"
    SCRIPT_MATCHING = "script_matching"
    REMEDIATION = "remediation"

    # Infrastructure capabilities
    VM_MONITORING = "vm_monitoring"
    VM_RECOVERY = "vm_recovery"
    GCP_OPERATIONS = "gcp_operations"


@dataclass
class IAgentTask:
    """
    Task input for agent execution.

    Represents a unit of work for an agent to process.
    Tasks are immutable and hashable for determinism tracking.
    """
    task_id: str
    task_type: str
    payload: Dict[str, Any]
    context: Dict[str, Any] = field(default_factory=dict)
    priority: str = "normal"  # low, normal, high, critical
    timeout_seconds: int = 300
    created_at: datetime = field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            "task_id": self.task_id,
            "task_type": self.task_type,
            "payload": self.payload,
            "context": self.context,
            "priority": self.priority,
            "timeout_seconds": self.timeout_seconds,
            "created_at": self.created_at.isoformat(),
            "metadata": self.metadata,
        }

    def compute_hash(self) -> str:
        """
        Compute deterministic hash of task inputs.

        WHY: For reproducibility and caching. Same inputs should
             produce same outputs.
        """
        # Exclude timestamp and metadata from hash
        hash_input = {
            "task_id": self.task_id,
            "task_type": self.task_type,
            "payload": self.payload,
            "context": self.context,
        }
        json_str = json.dumps(hash_input, sort_keys=True, default=str)
        return hashlib.sha256(json_str.encode()).hexdigest()[:16]


@dataclass
class IAgentResult:
    """
    Result output from agent execution.

    Represents the outcome of processing an IAgentTask.
    Includes determinism tracking via input/output hashes.
    """
    task_id: str
    success: bool
    output: Dict[str, Any]
    error: Optional[str] = None
    error_details: Optional[Dict[str, Any]] = None
    execution_time_ms: int = 0
    input_hash: str = ""
    output_hash: str = ""
    completed_at: datetime = field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            "task_id": self.task_id,
            "success": self.success,
            "output": self.output,
            "error": self.error,
            "error_details": self.error_details,
            "execution_time_ms": self.execution_time_ms,
            "input_hash": self.input_hash,
            "output_hash": self.output_hash,
            "completed_at": self.completed_at.isoformat(),
            "metadata": self.metadata,
        }

    def compute_output_hash(self) -> str:
        """Compute hash of output for determinism verification"""
        json_str = json.dumps(self.output, sort_keys=True, default=str)
        return hashlib.sha256(json_str.encode()).hexdigest()[:16]


class IAgentService(ABC):
    """
    Agent Service Interface - Contract for all agents.

    WHY: This is the ONLY interface backend should use to interact
         with agents. Agents MUST NOT have any backend dependencies.

    HOW: Implement this interface in your agent. Backend creates
         instances and calls execute().

    Example:
        class ServiceNowAgent(IAgentService):
            @property
            def agent_id(self) -> str:
                return "servicenow-incident-agent"

            @property
            def capabilities(self) -> List[AgentCapability]:
                return [AgentCapability.INCIDENT_TRIAGE]

            async def execute(self, task: IAgentTask) -> IAgentResult:
                # Process incident
                return IAgentResult(...)
    """

    @property
    @abstractmethod
    def agent_id(self) -> str:
        """Unique identifier for this agent"""
        pass

    @property
    @abstractmethod
    def agent_name(self) -> str:
        """Human-readable name"""
        pass

    @property
    @abstractmethod
    def capabilities(self) -> List[AgentCapability]:
        """List of capabilities this agent provides"""
        pass

    @property
    def version(self) -> str:
        """Agent version"""
        return "1.0.0"

    @abstractmethod
    async def execute(self, task: IAgentTask) -> IAgentResult:
        """
        Execute a task and return result.

        This is the main entry point for agent execution.
        Must be async for consistency.

        Args:
            task: The task to execute

        Returns:
            IAgentResult with success/failure and output
        """
        pass

    async def health_check(self) -> Dict[str, Any]:
        """
        Check agent health status.

        Returns:
            Dict with health information
        """
        return {
            "agent_id": self.agent_id,
            "status": "healthy",
            "capabilities": [c.value for c in self.capabilities],
        }

    def can_handle(self, task_type: str) -> bool:
        """
        Check if this agent can handle a task type.

        Args:
            task_type: The type of task

        Returns:
            True if agent can handle, False otherwise
        """
        # Default: check if task_type matches any capability
        return any(
            task_type.lower() in cap.value.lower()
            for cap in self.capabilities
        )


class IAgentRegistry(Protocol):
    """
    Agent Registry Protocol - For discovering and managing agents.

    Used by orchestration layer to find appropriate agents for tasks.
    """

    def register(self, agent: IAgentService) -> None:
        """Register an agent"""
        ...

    def unregister(self, agent_id: str) -> None:
        """Unregister an agent"""
        ...

    def get(self, agent_id: str) -> Optional[IAgentService]:
        """Get agent by ID"""
        ...

    def find_by_capability(self, capability: AgentCapability) -> List[IAgentService]:
        """Find agents with a specific capability"""
        ...

    def list_all(self) -> List[IAgentService]:
        """List all registered agents"""
        ...
