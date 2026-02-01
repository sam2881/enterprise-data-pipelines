"""
APEX Data Agent - Custom Exceptions

Defines all custom exceptions for the APEX platform.
All exceptions inherit from APEXError for consistent error handling.
"""

from typing import Optional, Dict, Any


class APEXError(Exception):
    """Base exception for all APEX errors."""

    def __init__(
        self,
        message: str,
        error_code: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(message)
        self.message = message
        self.error_code = error_code
        self.context = context or {}

    def to_dict(self) -> Dict[str, Any]:
        """Convert exception to dictionary for logging."""
        return {
            "error_type": self.__class__.__name__,
            "message": self.message,
            "error_code": self.error_code,
            "context": self.context,
        }


class MetadataError(APEXError):
    """Raised when metadata operations fail."""

    def __init__(
        self,
        message: str,
        table_name: Optional[str] = None,
        operation: Optional[str] = None,
        **kwargs,
    ):
        context = kwargs.get("context", {})
        context.update({"table_name": table_name, "operation": operation})
        super().__init__(message, error_code="METADATA_ERROR", context=context)


class ValidationError(APEXError):
    """Raised when data validation fails."""

    def __init__(
        self,
        message: str,
        zone: Optional[str] = None,
        rule_name: Optional[str] = None,
        failed_count: Optional[int] = None,
        total_count: Optional[int] = None,
        **kwargs,
    ):
        context = kwargs.get("context", {})
        context.update({
            "zone": zone,
            "rule_name": rule_name,
            "failed_count": failed_count,
            "total_count": total_count,
        })
        super().__init__(message, error_code="VALIDATION_ERROR", context=context)


class SparkJobError(APEXError):
    """Raised when Spark job execution fails."""

    def __init__(
        self,
        message: str,
        job_name: Optional[str] = None,
        application_id: Optional[str] = None,
        exit_code: Optional[int] = None,
        **kwargs,
    ):
        context = kwargs.get("context", {})
        context.update({
            "job_name": job_name,
            "application_id": application_id,
            "exit_code": exit_code,
        })
        super().__init__(message, error_code="SPARK_JOB_ERROR", context=context)


class StorageError(APEXError):
    """Raised when storage operations fail."""

    def __init__(
        self,
        message: str,
        path: Optional[str] = None,
        operation: Optional[str] = None,
        storage_type: Optional[str] = None,
        **kwargs,
    ):
        context = kwargs.get("context", {})
        context.update({
            "path": path,
            "operation": operation,
            "storage_type": storage_type,
        })
        super().__init__(message, error_code="STORAGE_ERROR", context=context)


class TemplateError(APEXError):
    """Raised when template operations fail."""

    def __init__(
        self,
        message: str,
        template_id: Optional[str] = None,
        template_code: Optional[str] = None,
        **kwargs,
    ):
        context = kwargs.get("context", {})
        context.update({
            "template_id": template_id,
            "template_code": template_code,
        })
        super().__init__(message, error_code="TEMPLATE_ERROR", context=context)


class DependencyError(APEXError):
    """Raised when pipeline dependencies are not met."""

    def __init__(
        self,
        message: str,
        upstream_feed: Optional[str] = None,
        required_status: Optional[str] = None,
        **kwargs,
    ):
        context = kwargs.get("context", {})
        context.update({
            "upstream_feed": upstream_feed,
            "required_status": required_status,
        })
        super().__init__(message, error_code="DEPENDENCY_ERROR", context=context)


class SLABreachError(APEXError):
    """Raised when SLA is breached."""

    def __init__(
        self,
        message: str,
        sla_type: Optional[str] = None,
        expected_value: Optional[int] = None,
        actual_value: Optional[int] = None,
        **kwargs,
    ):
        context = kwargs.get("context", {})
        context.update({
            "sla_type": sla_type,
            "expected_value": expected_value,
            "actual_value": actual_value,
        })
        super().__init__(message, error_code="SLA_BREACH", context=context)


class SchemaEvolutionError(APEXError):
    """Raised when schema evolution cannot be performed."""

    def __init__(
        self,
        message: str,
        breaking_changes: Optional[list] = None,
        **kwargs,
    ):
        context = kwargs.get("context", {})
        context.update({"breaking_changes": breaking_changes})
        super().__init__(message, error_code="SCHEMA_EVOLUTION_ERROR", context=context)
