"""
PostgreSQL metadata repository layer for the Data Engineering Platform.

This module provides:
- SQLAlchemy ORM models for all database tables
- MetadataRepository for database operations
- Connection pooling and session management

Exports:
    - MetadataRepository: Main repository class for database operations
    - Pipeline: Pipeline configuration model
    - SchemaVersion: Schema version tracking model
    - TransformationRule: Transformation rule model
    - DataQualityRule: Data quality rule model
    - Execution: Pipeline execution record model
    - AuditLog: Audit log entry model
    - ApprovalRequest: Human approval request model
    - Base: SQLAlchemy declarative base (for migrations)
"""

from src.metadata.repository import MetadataRepository
from src.metadata.models import (
    Base,
    Pipeline,
    SchemaVersion,
    TransformationRule,
    DataQualityRule,
    Execution,
    AuditLog,
    ApprovalRequest,
)

__all__ = [
    # Repository
    "MetadataRepository",
    # Models
    "Base",
    "Pipeline",
    "SchemaVersion",
    "TransformationRule",
    "DataQualityRule",
    "Execution",
    "AuditLog",
    "ApprovalRequest",
]
