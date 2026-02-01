"""
DAG Utilities - Core Module

Provides core functionality for APEX DAGs:
- MetadataClient: Query and update PostgreSQL metadata
- ExecutionContext: Pipeline execution context management
- ConfigLoader: Load configurations from metadata
- Exceptions: Custom exceptions for APEX
"""

from dag_utilities.core.metadata_client import MetadataClient
from dag_utilities.core.execution_context import ExecutionContext
from dag_utilities.core.config_loader import ConfigLoader, APEXConfig
from dag_utilities.core.exceptions import (
    APEXError,
    MetadataError,
    ValidationError,
    SparkJobError,
    StorageError,
)

__all__ = [
    "MetadataClient",
    "ExecutionContext",
    "ConfigLoader",
    "APEXConfig",
    "APEXError",
    "MetadataError",
    "ValidationError",
    "SparkJobError",
    "StorageError",
]
