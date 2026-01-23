"""
SQLAlchemy ORM models for the metadata database.

These models map directly to the DDL schema defined in ddl/ directory.
They provide the ORM layer for interacting with the PostgreSQL database.

RULES:
1. Models MUST match DDL schema exactly
2. Use JSONB for flexible JSON storage
3. Include all constraints and indexes
4. Use timezone-aware datetime columns
5. Provide to_dict() methods for serialization
"""

from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


# =============================================================================
# Base Class
# =============================================================================


class Base(DeclarativeBase):
    """Base class for all SQLAlchemy models."""

    pass


# =============================================================================
# Mixin Classes
# =============================================================================


class TimestampMixin:
    """Mixin for created_at and updated_at timestamps."""

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=func.now(),
        nullable=False,
    )
    updated_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        onupdate=func.now(),
        nullable=True,
    )


class AuditMixin:
    """Mixin for audit fields."""

    created_by: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=func.now(),
        nullable=False,
    )


# =============================================================================
# Pipeline Model
# =============================================================================


class Pipeline(Base, AuditMixin):
    """
    Pipeline metadata registry.

    Corresponds to DDL: ddl/02_pipeline.sql
    """

    __tablename__ = "pipeline"

    # Primary Key
    pipeline_id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )

    # Required Fields
    pipeline_name: Mapped[str] = mapped_column(String(255), nullable=False)
    domain: Mapped[str] = mapped_column(String(100), nullable=False)
    source_type: Mapped[str] = mapped_column(String(50), nullable=False)
    source_system: Mapped[str] = mapped_column(String(100), nullable=False)
    environment: Mapped[str] = mapped_column(String(20), nullable=False)
    data_sensitivity: Mapped[str] = mapped_column(String(50), nullable=False)
    business_owner: Mapped[str] = mapped_column(String(255), nullable=False)
    technical_owner: Mapped[str] = mapped_column(String(255), nullable=False)

    # Optional Fields
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    updated_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        onupdate=func.now(),
        nullable=True,
    )

    # Relationships
    schema_versions: Mapped[List["SchemaVersion"]] = relationship(
        "SchemaVersion",
        back_populates="pipeline",
        cascade="all, delete-orphan",
        order_by="SchemaVersion.version.desc()",
    )
    transformation_rules: Mapped[List["TransformationRule"]] = relationship(
        "TransformationRule",
        back_populates="pipeline",
        cascade="all, delete-orphan",
    )
    data_quality_rules: Mapped[List["DataQualityRule"]] = relationship(
        "DataQualityRule",
        back_populates="pipeline",
        cascade="all, delete-orphan",
    )
    executions: Mapped[List["Execution"]] = relationship(
        "Execution",
        back_populates="pipeline",
        order_by="Execution.started_at.desc()",
    )
    audit_logs: Mapped[List["AuditLog"]] = relationship(
        "AuditLog",
        back_populates="pipeline",
    )

    # Constraints
    __table_args__ = (
        CheckConstraint(
            "environment IN ('dev', 'qa', 'prod')",
            name="chk_environment",
        ),
        CheckConstraint(
            "source_type IN ('file', 'database', 'streaming', 'api')",
            name="chk_source_type",
        ),
        CheckConstraint(
            "data_sensitivity IN ('public', 'internal', 'confidential', 'restricted')",
            name="chk_sensitivity",
        ),
        UniqueConstraint(
            "pipeline_name",
            "environment",
            name="uq_pipeline_name_env",
        ),
        Index("idx_pipeline_name_env", "pipeline_name", "environment"),
        Index("idx_pipeline_domain", "domain"),
        Index("idx_pipeline_source_type", "source_type"),
        Index("idx_pipeline_is_active", "is_active"),
    )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "pipeline_id": self.pipeline_id,
            "pipeline_name": self.pipeline_name,
            "domain": self.domain,
            "source_type": self.source_type,
            "source_system": self.source_system,
            "environment": self.environment,
            "data_sensitivity": self.data_sensitivity,
            "business_owner": self.business_owner,
            "technical_owner": self.technical_owner,
            "description": self.description,
            "is_active": self.is_active,
            "created_by": self.created_by,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }

    def __repr__(self) -> str:
        return f"<Pipeline(id={self.pipeline_id}, name={self.pipeline_name}, env={self.environment})>"


# =============================================================================
# Schema Version Model
# =============================================================================


class SchemaVersion(Base):
    """
    Schema version history with SCD Type 2 tracking.

    Corresponds to DDL: ddl/03_schema_version.sql
    """

    __tablename__ = "schema_version"

    # Primary Key
    schema_version_id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )

    # Foreign Key
    pipeline_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("pipeline.pipeline_id", ondelete="CASCADE"),
        nullable=False,
    )

    # Required Fields
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    schema_definition: Mapped[Dict[str, Any]] = mapped_column(JSONB, nullable=False)
    change_type: Mapped[str] = mapped_column(String(50), nullable=False)
    effective_from: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    created_by: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=func.now(),
        nullable=False,
    )

    # Optional Fields
    change_description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    is_current: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    effective_to: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    # Relationships
    pipeline: Mapped["Pipeline"] = relationship(
        "Pipeline",
        back_populates="schema_versions",
    )
    transformation_rules: Mapped[List["TransformationRule"]] = relationship(
        "TransformationRule",
        back_populates="schema_version",
    )

    # Constraints
    __table_args__ = (
        CheckConstraint(
            "change_type IN ('initial', 'add_column', 'remove_column', 'modify_column', 'full_replace')",
            name="chk_change_type",
        ),
        UniqueConstraint(
            "pipeline_id",
            "version",
            name="uq_pipeline_version",
        ),
        Index("idx_schema_pipeline_id", "pipeline_id"),
        Index("idx_schema_is_current", "is_current"),
        Index("idx_schema_effective", "effective_from", "effective_to"),
    )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "schema_version_id": self.schema_version_id,
            "pipeline_id": self.pipeline_id,
            "version": self.version,
            "schema_definition": self.schema_definition,
            "change_type": self.change_type,
            "change_description": self.change_description,
            "is_current": self.is_current,
            "effective_from": self.effective_from.isoformat() if self.effective_from else None,
            "effective_to": self.effective_to.isoformat() if self.effective_to else None,
            "created_by": self.created_by,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }

    def __repr__(self) -> str:
        return f"<SchemaVersion(id={self.schema_version_id}, pipeline={self.pipeline_id}, v={self.version})>"


# =============================================================================
# Transformation Rule Model
# =============================================================================


class TransformationRule(Base, AuditMixin):
    """
    Transformation rules for pipeline processing.

    Corresponds to DDL: ddl/04_transformation.sql
    """

    __tablename__ = "transformation_rule"

    # Primary Key
    rule_id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )

    # Foreign Keys
    pipeline_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("pipeline.pipeline_id", ondelete="CASCADE"),
        nullable=False,
    )
    schema_version_id: Mapped[Optional[int]] = mapped_column(
        Integer,
        ForeignKey("schema_version.schema_version_id", ondelete="SET NULL"),
        nullable=True,
    )

    # Required Fields
    rule_name: Mapped[str] = mapped_column(String(255), nullable=False)
    rule_type: Mapped[str] = mapped_column(String(50), nullable=False)
    target_column: Mapped[str] = mapped_column(String(255), nullable=False)
    transformation_expr: Mapped[str] = mapped_column(Text, nullable=False)
    rule_order: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    # Optional Fields
    source_column: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    updated_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        onupdate=func.now(),
        nullable=True,
    )

    # Relationships
    pipeline: Mapped["Pipeline"] = relationship(
        "Pipeline",
        back_populates="transformation_rules",
    )
    schema_version: Mapped[Optional["SchemaVersion"]] = relationship(
        "SchemaVersion",
        back_populates="transformation_rules",
    )

    # Constraints
    __table_args__ = (
        CheckConstraint(
            "rule_type IN ('rename', 'cast', 'derive', 'filter', 'aggregate', 'join', 'custom')",
            name="chk_rule_type",
        ),
        Index("idx_transform_pipeline_id", "pipeline_id"),
        Index("idx_transform_schema_id", "schema_version_id"),
        Index("idx_transform_rule_order", "pipeline_id", "rule_order"),
    )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "rule_id": self.rule_id,
            "pipeline_id": self.pipeline_id,
            "schema_version_id": self.schema_version_id,
            "rule_name": self.rule_name,
            "rule_type": self.rule_type,
            "source_column": self.source_column,
            "target_column": self.target_column,
            "transformation_expr": self.transformation_expr,
            "rule_order": self.rule_order,
            "is_active": self.is_active,
            "created_by": self.created_by,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }

    def __repr__(self) -> str:
        return f"<TransformationRule(id={self.rule_id}, name={self.rule_name}, type={self.rule_type})>"


# =============================================================================
# Data Quality Rule Model
# =============================================================================


class DataQualityRule(Base, AuditMixin):
    """
    Data quality validation rules.

    Corresponds to DDL: ddl/05_data_quality.sql
    """

    __tablename__ = "data_quality_rule"

    # Primary Key
    dq_rule_id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )

    # Foreign Key
    pipeline_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("pipeline.pipeline_id", ondelete="CASCADE"),
        nullable=False,
    )

    # Required Fields
    rule_name: Mapped[str] = mapped_column(String(255), nullable=False)
    rule_type: Mapped[str] = mapped_column(String(50), nullable=False)
    rule_expression: Mapped[str] = mapped_column(Text, nullable=False)
    severity: Mapped[str] = mapped_column(String(20), default="warning", nullable=False)

    # Optional Fields
    column_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    threshold: Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 4), nullable=True)
    is_blocking: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    updated_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        onupdate=func.now(),
        nullable=True,
    )

    # Relationships
    pipeline: Mapped["Pipeline"] = relationship(
        "Pipeline",
        back_populates="data_quality_rules",
    )

    # Constraints
    __table_args__ = (
        CheckConstraint(
            "rule_type IN ('not_null', 'unique', 'range', 'regex', 'referential', 'custom')",
            name="chk_dq_rule_type",
        ),
        CheckConstraint(
            "severity IN ('info', 'warning', 'error', 'critical')",
            name="chk_dq_severity",
        ),
        Index("idx_dq_pipeline_id", "pipeline_id"),
        Index("idx_dq_rule_type", "rule_type"),
        Index("idx_dq_is_blocking", "is_blocking"),
    )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "dq_rule_id": self.dq_rule_id,
            "pipeline_id": self.pipeline_id,
            "rule_name": self.rule_name,
            "rule_type": self.rule_type,
            "column_name": self.column_name,
            "rule_expression": self.rule_expression,
            "threshold": float(self.threshold) if self.threshold else None,
            "severity": self.severity,
            "is_blocking": self.is_blocking,
            "is_active": self.is_active,
            "created_by": self.created_by,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }

    def __repr__(self) -> str:
        return f"<DataQualityRule(id={self.dq_rule_id}, name={self.rule_name}, type={self.rule_type})>"


# =============================================================================
# Execution Model
# =============================================================================


class Execution(Base):
    """
    Pipeline generation execution records.

    Corresponds to DDL: ddl/06_execution.sql
    """

    __tablename__ = "execution"

    # Primary Key
    execution_id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )

    # Foreign Key
    pipeline_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("pipeline.pipeline_id", ondelete="CASCADE"),
        nullable=False,
    )

    # Required Fields
    request_id: Mapped[str] = mapped_column(String(255), nullable=False)
    execution_status: Mapped[str] = mapped_column(String(50), nullable=False)
    environment: Mapped[str] = mapped_column(String(20), nullable=False)
    triggered_by: Mapped[str] = mapped_column(String(255), nullable=False)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=func.now(),
        nullable=False,
    )

    # Optional Fields
    artifacts_generated: Mapped[Optional[Dict[str, Any]]] = mapped_column(
        JSONB,
        nullable=True,
    )
    branch_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    commit_sha: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    pr_url: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    cicd_build_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    completed_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    execution_metadata: Mapped[Optional[Dict[str, Any]]] = mapped_column(
        JSONB,
        nullable=True,
    )

    # Relationships
    pipeline: Mapped["Pipeline"] = relationship(
        "Pipeline",
        back_populates="executions",
    )
    approval_requests: Mapped[List["ApprovalRequest"]] = relationship(
        "ApprovalRequest",
        back_populates="execution",
        cascade="all, delete-orphan",
    )

    # Constraints
    __table_args__ = (
        CheckConstraint(
            "execution_status IN ('pending', 'planning', 'generating', 'validating', "
            "'awaiting_approval', 'deploying', 'success', 'failed', 'cancelled')",
            name="chk_exec_status",
        ),
        CheckConstraint(
            "environment IN ('dev', 'qa', 'prod')",
            name="chk_exec_environment",
        ),
        Index("idx_exec_pipeline_id", "pipeline_id"),
        Index("idx_exec_request_id", "request_id"),
        Index("idx_exec_status", "execution_status"),
        Index("idx_exec_started_at", "started_at"),
        Index("idx_exec_environment", "environment"),
    )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "execution_id": self.execution_id,
            "pipeline_id": self.pipeline_id,
            "request_id": self.request_id,
            "execution_status": self.execution_status,
            "environment": self.environment,
            "triggered_by": self.triggered_by,
            "artifacts_generated": self.artifacts_generated,
            "branch_name": self.branch_name,
            "commit_sha": self.commit_sha,
            "pr_url": self.pr_url,
            "cicd_build_id": self.cicd_build_id,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "error_message": self.error_message,
            "execution_metadata": self.execution_metadata,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }

    def __repr__(self) -> str:
        return f"<Execution(id={self.execution_id}, request={self.request_id}, status={self.execution_status})>"


# =============================================================================
# Audit Log Model
# =============================================================================


class AuditLog(Base):
    """
    Audit trail for all agent actions.

    Corresponds to DDL: ddl/07_audit.sql (audit_log table)
    """

    __tablename__ = "audit_log"

    # Primary Key (UUID string)
    log_id: Mapped[str] = mapped_column(String(255), primary_key=True)

    # Required Fields
    agent_name: Mapped[str] = mapped_column(String(100), nullable=False)
    action: Mapped[str] = mapped_column(String(100), nullable=False)
    status: Mapped[str] = mapped_column(String(50), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=func.now(),
        nullable=False,
    )

    # Optional Fields
    pipeline_id: Mapped[Optional[int]] = mapped_column(
        Integer,
        ForeignKey("pipeline.pipeline_id", ondelete="SET NULL"),
        nullable=True,
    )
    request_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    input_state: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSONB, nullable=True)
    output_state: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSONB, nullable=True)
    decision_reasoning: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    duration_ms: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    error_details: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSONB, nullable=True)

    # Relationships
    pipeline: Mapped[Optional["Pipeline"]] = relationship(
        "Pipeline",
        back_populates="audit_logs",
    )

    # Constraints
    __table_args__ = (
        CheckConstraint(
            "status IN ('started', 'success', 'failed', 'skipped')",
            name="chk_audit_status",
        ),
        Index("idx_audit_agent_name", "agent_name"),
        Index("idx_audit_pipeline_id", "pipeline_id"),
        Index("idx_audit_request_id", "request_id"),
        Index("idx_audit_created_at", "created_at"),
        Index("idx_audit_status", "status"),
    )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "log_id": self.log_id,
            "agent_name": self.agent_name,
            "action": self.action,
            "pipeline_id": self.pipeline_id,
            "request_id": self.request_id,
            "input_state": self.input_state,
            "output_state": self.output_state,
            "decision_reasoning": self.decision_reasoning,
            "duration_ms": self.duration_ms,
            "status": self.status,
            "error_details": self.error_details,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }

    def __repr__(self) -> str:
        return f"<AuditLog(id={self.log_id}, agent={self.agent_name}, action={self.action})>"


# =============================================================================
# Approval Request Model
# =============================================================================


class ApprovalRequest(Base):
    """
    Human approval requests for sensitive operations.

    Corresponds to DDL: ddl/07_audit.sql (approval_request table)
    """

    __tablename__ = "approval_request"

    # Primary Key
    approval_id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )

    # Foreign Key
    execution_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("execution.execution_id", ondelete="CASCADE"),
        nullable=False,
    )

    # Required Fields
    approval_type: Mapped[str] = mapped_column(String(50), nullable=False)
    requested_by: Mapped[str] = mapped_column(String(255), nullable=False)
    requested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=func.now(),
        nullable=False,
    )
    status: Mapped[str] = mapped_column(String(50), default="pending", nullable=False)

    # Optional Fields
    approver: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    approved_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    comments: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    expires_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    # Relationships
    execution: Mapped["Execution"] = relationship(
        "Execution",
        back_populates="approval_requests",
    )

    # Constraints
    __table_args__ = (
        CheckConstraint(
            "approval_type IN ('prod_deployment', 'schema_change', 'breaking_change', 'manual_override')",
            name="chk_approval_type",
        ),
        CheckConstraint(
            "status IN ('pending', 'approved', 'rejected', 'expired', 'cancelled')",
            name="chk_approval_status",
        ),
        Index("idx_approval_execution_id", "execution_id"),
        Index("idx_approval_status", "status"),
        Index("idx_approval_expires_at", "expires_at"),
    )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "approval_id": self.approval_id,
            "execution_id": self.execution_id,
            "approval_type": self.approval_type,
            "requested_by": self.requested_by,
            "requested_at": self.requested_at.isoformat() if self.requested_at else None,
            "approver": self.approver,
            "approved_at": self.approved_at.isoformat() if self.approved_at else None,
            "status": self.status,
            "comments": self.comments,
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
        }

    def is_expired(self) -> bool:
        """Check if approval request has expired."""
        if not self.expires_at:
            return False
        return datetime.now(self.expires_at.tzinfo) > self.expires_at

    def __repr__(self) -> str:
        return f"<ApprovalRequest(id={self.approval_id}, type={self.approval_type}, status={self.status})>"
