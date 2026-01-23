"""
Shared Type Definitions for Agents

WHY: Common types used across all agents for consistency.
     Avoids duplication and ensures type safety.
"""
from enum import Enum
from typing import Any, Dict, Optional
from dataclasses import dataclass, field
from datetime import datetime


class TaskPriority(str, Enum):
    """Task priority levels"""
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    CRITICAL = "critical"


class TaskStatus(str, Enum):
    """Task execution status"""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"


class AgentType(str, Enum):
    """Types of agents in the platform"""
    # Data Engineering Agents
    DATA_ANALYSIS = "data_analysis"
    DATA_PLANNING = "data_planning"
    SPARK_GENERATOR = "spark_generator"
    DAG_GENERATOR = "dag_generator"
    DQ_GENERATOR = "dq_generator"
    SSIS_MIGRATION = "ssis_migration"

    # ServiceNow Platform Agents
    SERVICENOW_INCIDENT = "servicenow_incident"
    REMEDIATION = "remediation"
    SCRIPT_MATCHER = "script_matcher"

    # Infrastructure Agents
    GCP_MONITOR = "gcp_monitor"
    VM_RECOVERY = "vm_recovery"
    INFRASTRUCTURE_ENHANCED = "infrastructure_enhanced"


class RiskLevel(str, Enum):
    """Risk assessment levels"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class ApprovalRoute(str, Enum):
    """Approval routing decisions"""
    AUTO_APPROVE = "auto_approve"
    ASYNC_APPROVE = "async_approve"
    MANUAL_APPROVE = "manual_approve"
    SENIOR_APPROVE = "senior_approve"
    DENY = "deny"


@dataclass
class AgentContext:
    """
    Context passed to agents for execution.

    Contains environment, configuration, and runtime information
    that agents may need during execution.
    """
    environment: str = "development"  # development, staging, production
    correlation_id: str = ""
    trace_id: str = ""
    user_id: Optional[str] = None
    tenant_id: Optional[str] = None
    dry_run: bool = False
    timeout_seconds: int = 300
    retry_count: int = 0
    max_retries: int = 3
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "environment": self.environment,
            "correlation_id": self.correlation_id,
            "trace_id": self.trace_id,
            "user_id": self.user_id,
            "tenant_id": self.tenant_id,
            "dry_run": self.dry_run,
            "timeout_seconds": self.timeout_seconds,
            "retry_count": self.retry_count,
            "max_retries": self.max_retries,
            "metadata": self.metadata,
            "created_at": self.created_at.isoformat(),
        }


@dataclass
class IncidentData:
    """Standardized incident representation"""
    incident_id: str
    short_description: str
    description: str = ""
    category: str = "unknown"
    subcategory: str = ""
    priority: str = "3"
    severity: str = "3"
    status: str = "new"
    assigned_to: Optional[str] = None
    assignment_group: Optional[str] = None
    caller_id: Optional[str] = None
    opened_at: Optional[datetime] = None
    configuration_item: Optional[str] = None
    business_service: Optional[str] = None
    custom_fields: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ScriptMatch:
    """Remediation script match result"""
    script_id: str
    name: str
    description: str
    script_type: str  # python, bash, terraform, ansible
    confidence: float  # 0.0 - 1.0
    risk_level: RiskLevel
    auto_approve: bool
    required_inputs: list = field(default_factory=list)
    extracted_inputs: Dict[str, Any] = field(default_factory=dict)
    match_reason: str = ""
    source: str = ""  # rag, llm, hybrid


@dataclass
class PipelineSpec:
    """Data pipeline specification"""
    pipeline_id: str
    name: str
    source_type: str
    target_type: str
    medallion_layer: str  # raw, bronze, silver, gold
    schedule: Optional[str] = None
    schema: Dict[str, Any] = field(default_factory=dict)
    transformations: list = field(default_factory=list)
    quality_rules: list = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
