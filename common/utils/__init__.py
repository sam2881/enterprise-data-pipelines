"""
Common Utilities for Enterprise Metadata-Driven Pipelines.

These utilities are SHARED across all feeds and must contain
NO file-specific logic. All behavior is driven by metadata.

NOTE: PySpark-dependent modules (FileValidator, SchemaBuilder) are imported
lazily to avoid import errors in Airflow which doesn't have PySpark installed.
These modules run on Dataproc, not in Airflow.

ENTERPRISE FEATURES:
- FileValidator: File discovery, duplicate detection, staging, and validation
- MetadataReader: Read feed configuration from PostgreSQL
- AuditLogger: Audit trail for all pipeline operations
- XComUtils: XCom utilities for Airflow task communication
"""

# Safe imports (no PySpark dependency)
from .metadata_reader import MetadataReader
from .audit_logger import AuditLogger
from .xcom_utils import XComUtils

# Dataclasses are safe to import (no PySpark)
from .file_validator import FileInfo, ValidationResult, ValidationSummary


# Lazy imports for PySpark-dependent modules
def get_file_validator():
    """Lazy import of FileValidator (requires PySpark for validation methods)."""
    from .file_validator import FileValidator
    return FileValidator


def get_schema_builder():
    """Lazy import of SchemaBuilder (requires PySpark)."""
    from .schema_builder import SchemaBuilder
    return SchemaBuilder


# For backward compatibility - these will fail if PySpark not installed
try:
    from .file_validator import FileValidator
    from .schema_builder import SchemaBuilder
except ImportError:
    FileValidator = None  # type: ignore
    SchemaBuilder = None  # type: ignore

__all__ = [
    # Safe imports (no PySpark)
    "MetadataReader",
    "AuditLogger",
    "XComUtils",
    "FileInfo",
    "ValidationResult",
    "ValidationSummary",
    # Lazy loaders
    "get_file_validator",
    "get_schema_builder",
    # PySpark-dependent (may be None if PySpark not installed)
    "FileValidator",
    "SchemaBuilder",
]
