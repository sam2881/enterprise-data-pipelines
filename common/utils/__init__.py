"""
Common Utilities for Metadata-Driven Pipelines.

WHY: Utilities are shared across all feeds and layers.
     All behavior is driven by metadata, not hardcoded logic.
"""

from .metadata_reader import MetadataReader
from .audit_logger import AuditLogger

# Lazy imports for PySpark-dependent modules
def get_file_validator():
    """Get FileValidator class (requires PySpark for validation)."""
    from .file_validator import FileValidator
    return FileValidator

__all__ = [
    "MetadataReader",
    "AuditLogger",
    "get_file_validator",
]
