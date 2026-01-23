"""
Shared Metadata Models - Platform Layer

WHY: Both agents and backend need consistent data models.
     Shared metadata avoids duplication and ensures type safety.

WHAT'S HERE:
- Incident models
- Pipeline models
- Script models
- Common types

WHO USES THIS:
- agents/ (for task payloads)
- backend/ (for persistence, APIs)

USAGE:
    from platform.metadata import IncidentMetadata, PipelineMetadata
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional
from enum import Enum


class IncidentStatus(str, Enum):
    NEW = "new"
    IN_PROGRESS = "in_progress"
    RESOLVED = "resolved"
    CLOSED = "closed"


class PipelineStatus(str, Enum):
    DRAFT = "draft"
    PENDING_REVIEW = "pending_review"
    APPROVED = "approved"
    DEPLOYED = "deployed"
    FAILED = "failed"


class RiskLevel(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class IncidentMetadata:
    """Incident metadata - shared between agents and backend"""
    incident_id: str
    short_description: str
    description: str = ""
    category: str = "unknown"
    subcategory: str = ""
    priority: str = "3"
    severity: str = "3"
    status: IncidentStatus = IncidentStatus.NEW
    assigned_to: Optional[str] = None
    assignment_group: Optional[str] = None
    caller_id: Optional[str] = None
    opened_at: Optional[datetime] = None
    resolved_at: Optional[datetime] = None
    configuration_item: Optional[str] = None
    business_service: Optional[str] = None
    resolution: Optional[str] = None
    custom_fields: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "incident_id": self.incident_id,
            "short_description": self.short_description,
            "description": self.description,
            "category": self.category,
            "subcategory": self.subcategory,
            "priority": self.priority,
            "severity": self.severity,
            "status": self.status.value,
            "assigned_to": self.assigned_to,
            "opened_at": self.opened_at.isoformat() if self.opened_at else None,
            "resolved_at": self.resolved_at.isoformat() if self.resolved_at else None,
            "resolution": self.resolution,
        }


@dataclass
class PipelineMetadata:
    """Pipeline metadata - shared between agents and backend"""
    pipeline_id: str
    name: str
    description: str = ""
    source_type: str = "unknown"
    target_type: str = "unknown"
    medallion_layer: str = "bronze"  # raw, bronze, silver, gold
    status: PipelineStatus = PipelineStatus.DRAFT
    schedule: Optional[str] = None
    owner: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    jira_ticket: Optional[str] = None
    git_branch: Optional[str] = None
    airflow_dag_id: Optional[str] = None
    schema: Dict[str, Any] = field(default_factory=dict)
    transformations: List[Dict] = field(default_factory=list)
    quality_rules: List[Dict] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "pipeline_id": self.pipeline_id,
            "name": self.name,
            "description": self.description,
            "source_type": self.source_type,
            "target_type": self.target_type,
            "medallion_layer": self.medallion_layer,
            "status": self.status.value,
            "schedule": self.schedule,
            "jira_ticket": self.jira_ticket,
            "airflow_dag_id": self.airflow_dag_id,
        }


@dataclass
class ScriptMetadata:
    """Script metadata - matches registry.json structure"""
    script_id: str
    name: str
    description: str = ""
    script_type: str = "python"  # python, bash, ansible, terraform
    category: str = "unknown"
    risk_level: RiskLevel = RiskLevel.MEDIUM
    auto_approve: bool = False
    required_inputs: List[str] = field(default_factory=list)
    timeout_seconds: int = 300
    rollback_available: bool = False
    filename: Optional[str] = None
    version: str = "1.0.0"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "script_id": self.script_id,
            "name": self.name,
            "description": self.description,
            "type": self.script_type,
            "category": self.category,
            "risk_level": self.risk_level.value,
            "auto_approve": self.auto_approve,
            "required_inputs": self.required_inputs,
            "timeout_seconds": self.timeout_seconds,
        }


__all__ = [
    # Enums
    "IncidentStatus",
    "PipelineStatus",
    "RiskLevel",
    # Models
    "IncidentMetadata",
    "PipelineMetadata",
    "ScriptMetadata",
]
