"""
APEX Data Agent - DAG Utilities Module

This module provides all reusable utilities for Airflow DAGs.
DAGs should ONLY call utilities - they should NOT contain business logic.

Submodules:
    - core: Metadata client, execution context, configuration
    - spark: Spark job submission and cluster management
    - storage: GCS, S3, ADLS file operations
    - validation: Schema, semantic, and quality validation
    - notification: Email, Slack, PagerDuty alerts
    - logging: Audit logging, metrics, lineage tracking
    - remediation: Self-healing, retry, incident management
    - pipeline: Reusable task functions for DAG patterns
"""

from dag_utilities.core import (
    MetadataClient,
    ExecutionContext,
    ConfigLoader,
)
from dag_utilities.spark import (
    SparkJobSubmitter,
    ClusterManager,
    SparkConfigBuilder,
)
from dag_utilities.storage import (
    FileOperations,
    GCSClient,
)
from dag_utilities.validation import (
    SchemaValidator,
    SemanticValidator,
    QualityChecker,
)
from dag_utilities.notification import (
    EmailNotifier,
    SlackNotifier,
)
from dag_utilities.logging import (
    AuditLogger,
    MetricsCollector,
    LineageTracker,
)
from dag_utilities.remediation import (
    SelfHealer,
    RetryHandler,
    IncidentManager,
)
from dag_utilities.pipeline import pipeline_tasks, pattern_tasks

__version__ = "1.0.0"
__all__ = [
    # Core
    "MetadataClient",
    "ExecutionContext",
    "ConfigLoader",
    # Spark
    "SparkJobSubmitter",
    "ClusterManager",
    "SparkConfigBuilder",
    # Storage
    "FileOperations",
    "GCSClient",
    # Validation
    "SchemaValidator",
    "SemanticValidator",
    "QualityChecker",
    # Notification
    "EmailNotifier",
    "SlackNotifier",
    # Logging
    "AuditLogger",
    "MetricsCollector",
    "LineageTracker",
    # Remediation
    "SelfHealer",
    "RetryHandler",
    "IncidentManager",
    # Pipeline Tasks
    "pipeline_tasks",
    "pattern_tasks",
]
