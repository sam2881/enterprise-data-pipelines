"""
Custom exceptions for the Data Agent platform.

This module defines a hierarchy of exceptions used throughout the platform.
All exceptions inherit from DataAgentError and provide:
- Structured error information
- Error codes for programmatic handling
- Serialization for API responses and logging
- Retryability indication

RULES:
1. Always raise specific exceptions (not generic DataAgentError)
2. Include relevant context in the details dict
3. Use is_retryable to indicate if operation can be retried
4. Never include sensitive data in exception messages
"""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
import traceback


# =============================================================================
# Error Codes Enum
# =============================================================================


class ErrorCode(str, Enum):
    """Standard error codes for the platform."""

    # Base errors
    DATA_AGENT_ERROR = "DATA_AGENT_ERROR"
    UNKNOWN_ERROR = "UNKNOWN_ERROR"

    # Validation errors
    VALIDATION_ERROR = "VALIDATION_ERROR"
    SCHEMA_VALIDATION_ERROR = "SCHEMA_VALIDATION_ERROR"
    INTENT_VALIDATION_ERROR = "INTENT_VALIDATION_ERROR"
    CONFIG_VALIDATION_ERROR = "CONFIG_VALIDATION_ERROR"

    # Planning errors
    PLANNING_ERROR = "PLANNING_ERROR"
    PIPELINE_NOT_FOUND = "PIPELINE_NOT_FOUND"
    SCHEMA_CONFLICT = "SCHEMA_CONFLICT"
    TEMPLATE_NOT_FOUND = "TEMPLATE_NOT_FOUND"

    # Generation errors
    GENERATION_ERROR = "GENERATION_ERROR"
    TEMPLATE_RENDER_ERROR = "TEMPLATE_RENDER_ERROR"
    MISSING_TEMPLATE_VARS = "MISSING_TEMPLATE_VARS"
    CODE_SYNTAX_ERROR = "CODE_SYNTAX_ERROR"

    # Deployment errors
    DEPLOYMENT_ERROR = "DEPLOYMENT_ERROR"
    GIT_ERROR = "GIT_ERROR"
    CICD_ERROR = "CICD_ERROR"
    ROLLBACK_FAILED = "ROLLBACK_FAILED"

    # Approval errors
    APPROVAL_ERROR = "APPROVAL_ERROR"
    APPROVAL_TIMEOUT = "APPROVAL_TIMEOUT"
    APPROVAL_REJECTED = "APPROVAL_REJECTED"

    # Metadata errors
    METADATA_ERROR = "METADATA_ERROR"
    DATABASE_ERROR = "DATABASE_ERROR"
    QUERY_ERROR = "QUERY_ERROR"
    CONNECTION_ERROR = "CONNECTION_ERROR"

    # Integration errors
    INTEGRATION_ERROR = "INTEGRATION_ERROR"
    PUBSUB_ERROR = "PUBSUB_ERROR"
    SECRET_MANAGER_ERROR = "SECRET_MANAGER_ERROR"
    JIRA_ERROR = "JIRA_ERROR"
    GCS_ERROR = "GCS_ERROR"
    BIGQUERY_ERROR = "BIGQUERY_ERROR"

    # Security errors
    SECURITY_ERROR = "SECURITY_ERROR"
    AUTHENTICATION_ERROR = "AUTHENTICATION_ERROR"
    AUTHORIZATION_ERROR = "AUTHORIZATION_ERROR"

    # Timeout errors
    TIMEOUT_ERROR = "TIMEOUT_ERROR"
    WORKFLOW_TIMEOUT = "WORKFLOW_TIMEOUT"
    STEP_TIMEOUT = "STEP_TIMEOUT"


# =============================================================================
# Base Exception
# =============================================================================


class DataAgentError(Exception):
    """
    Base exception for all Data Agent errors.

    All custom exceptions in this platform inherit from this class.
    Provides structured error information for logging and API responses.

    Attributes:
        message: Human-readable error message
        error_code: Machine-readable error code
        details: Additional error context
        is_retryable: Whether the operation can be retried
        timestamp: When the error occurred
        cause: Original exception if wrapped
    """

    def __init__(
        self,
        message: str,
        error_code: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        is_retryable: bool = False,
        cause: Optional[Exception] = None,
    ) -> None:
        """
        Initialize the exception.

        Args:
            message: Human-readable error message
            error_code: Machine-readable error code
            details: Additional error context
            is_retryable: Whether the operation can be retried
            cause: Original exception if wrapping another error
        """
        super().__init__(message)
        self.message = message
        self.error_code = error_code or ErrorCode.DATA_AGENT_ERROR.value
        self.details = details or {}
        self.is_retryable = is_retryable
        self.timestamp = datetime.utcnow()
        self.cause = cause

        # Capture stack trace
        self._traceback = traceback.format_exc() if cause else None

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to dictionary for API responses and logging.

        Returns:
            Dictionary representation of the error
        """
        result = {
            "error_code": self.error_code,
            "message": self.message,
            "details": self.details,
            "is_retryable": self.is_retryable,
            "timestamp": self.timestamp.isoformat() + "Z",
        }

        if self.cause:
            result["cause"] = str(self.cause)

        return result

    def to_log_dict(self) -> Dict[str, Any]:
        """
        Convert to dictionary optimized for structured logging.

        Returns:
            Dictionary with logging-friendly format
        """
        result = self.to_dict()
        result["exception_type"] = self.__class__.__name__

        if self._traceback:
            result["traceback"] = self._traceback

        return result

    def __str__(self) -> str:
        """String representation."""
        return f"[{self.error_code}] {self.message}"

    def __repr__(self) -> str:
        """Debug representation."""
        return (
            f"{self.__class__.__name__}("
            f"message={self.message!r}, "
            f"error_code={self.error_code!r}, "
            f"is_retryable={self.is_retryable})"
        )


# =============================================================================
# Validation Exceptions
# =============================================================================


class ValidationError(DataAgentError):
    """
    Raised when validation fails.

    Used for schema validation, intent validation, and config validation.
    """

    def __init__(
        self,
        message: str,
        errors: Optional[List[str]] = None,
        field: Optional[str] = None,
        error_code: str = ErrorCode.VALIDATION_ERROR.value,
        is_retryable: bool = False,
    ) -> None:
        """
        Initialize validation error.

        Args:
            message: Error message
            errors: List of validation error messages
            field: Specific field that failed validation
            error_code: Specific validation error code
            is_retryable: Whether validation can be retried
        """
        details = {
            "errors": errors or [],
            "field": field,
            "error_count": len(errors) if errors else 0,
        }
        super().__init__(
            message=message,
            error_code=error_code,
            details=details,
            is_retryable=is_retryable,
        )
        self.errors = errors or []
        self.field = field


class SchemaValidationError(ValidationError):
    """Raised when schema validation fails."""

    def __init__(
        self,
        message: str,
        errors: Optional[List[str]] = None,
        schema_name: Optional[str] = None,
    ) -> None:
        super().__init__(
            message=message,
            errors=errors,
            error_code=ErrorCode.SCHEMA_VALIDATION_ERROR.value,
        )
        self.details["schema_name"] = schema_name


class IntentValidationError(ValidationError):
    """Raised when intent JSON validation fails."""

    def __init__(
        self,
        message: str,
        errors: Optional[List[str]] = None,
        intent_id: Optional[str] = None,
    ) -> None:
        super().__init__(
            message=message,
            errors=errors,
            error_code=ErrorCode.INTENT_VALIDATION_ERROR.value,
        )
        self.details["intent_id"] = intent_id


# =============================================================================
# Planning Exceptions
# =============================================================================


class PlanningError(DataAgentError):
    """
    Raised when planning fails.

    Used when the planner agent cannot determine the correct action
    or encounters issues with pipeline configuration.
    """

    def __init__(
        self,
        message: str,
        pipeline_name: Optional[str] = None,
        environment: Optional[str] = None,
        reason: Optional[str] = None,
        is_retryable: bool = False,
    ) -> None:
        """
        Initialize planning error.

        Args:
            message: Error message
            pipeline_name: Name of the pipeline being planned
            environment: Target environment
            reason: Specific reason for failure
            is_retryable: Whether planning can be retried
        """
        details = {
            "pipeline_name": pipeline_name,
            "environment": environment,
            "reason": reason,
        }
        super().__init__(
            message=message,
            error_code=ErrorCode.PLANNING_ERROR.value,
            details=details,
            is_retryable=is_retryable,
        )
        self.pipeline_name = pipeline_name
        self.environment = environment


class PipelineNotFoundError(PlanningError):
    """Raised when a referenced pipeline does not exist."""

    def __init__(
        self,
        pipeline_name: str,
        environment: str,
    ) -> None:
        super().__init__(
            message=f"Pipeline '{pipeline_name}' not found in environment '{environment}'",
            pipeline_name=pipeline_name,
            environment=environment,
        )
        self.error_code = ErrorCode.PIPELINE_NOT_FOUND.value


class SchemaConflictError(PlanningError):
    """Raised when schema changes conflict with existing data."""

    def __init__(
        self,
        message: str,
        pipeline_name: str,
        conflicting_columns: Optional[List[str]] = None,
    ) -> None:
        super().__init__(
            message=message,
            pipeline_name=pipeline_name,
        )
        self.error_code = ErrorCode.SCHEMA_CONFLICT.value
        self.details["conflicting_columns"] = conflicting_columns or []


# =============================================================================
# Generation Exceptions
# =============================================================================


class GenerationError(DataAgentError):
    """
    Raised when code generation fails.

    Used when template rendering or code generation encounters errors.
    """

    def __init__(
        self,
        message: str,
        template_name: Optional[str] = None,
        missing_vars: Optional[List[str]] = None,
        artifact_type: Optional[str] = None,
        is_retryable: bool = False,
    ) -> None:
        """
        Initialize generation error.

        Args:
            message: Error message
            template_name: Name of the template that failed
            missing_vars: List of missing template variables
            artifact_type: Type of artifact being generated
            is_retryable: Whether generation can be retried
        """
        details = {
            "template_name": template_name,
            "missing_vars": missing_vars or [],
            "artifact_type": artifact_type,
        }
        super().__init__(
            message=message,
            error_code=ErrorCode.GENERATION_ERROR.value,
            details=details,
            is_retryable=is_retryable,
        )
        self.template_name = template_name
        self.missing_vars = missing_vars or []


class TemplateNotFoundError(GenerationError):
    """Raised when a template file cannot be found."""

    def __init__(self, template_name: str) -> None:
        super().__init__(
            message=f"Template '{template_name}' not found",
            template_name=template_name,
        )
        self.error_code = ErrorCode.TEMPLATE_NOT_FOUND.value


class TemplateRenderError(GenerationError):
    """Raised when template rendering fails."""

    def __init__(
        self,
        message: str,
        template_name: str,
        line_number: Optional[int] = None,
    ) -> None:
        super().__init__(
            message=message,
            template_name=template_name,
        )
        self.error_code = ErrorCode.TEMPLATE_RENDER_ERROR.value
        self.details["line_number"] = line_number


# =============================================================================
# Deployment Exceptions
# =============================================================================


class DeploymentError(DataAgentError):
    """
    Raised when deployment fails.

    Used for Git operations, CI/CD triggers, and artifact deployment.
    """

    def __init__(
        self,
        message: str,
        step: Optional[str] = None,
        rollback_performed: bool = False,
        rollback_commit: Optional[str] = None,
        is_retryable: bool = True,
    ) -> None:
        """
        Initialize deployment error.

        Args:
            message: Error message
            step: Deployment step that failed
            rollback_performed: Whether rollback was executed
            rollback_commit: Commit SHA rolled back to
            is_retryable: Whether deployment can be retried
        """
        details = {
            "step": step,
            "rollback_performed": rollback_performed,
            "rollback_commit": rollback_commit,
        }
        super().__init__(
            message=message,
            error_code=ErrorCode.DEPLOYMENT_ERROR.value,
            details=details,
            is_retryable=is_retryable,
        )
        self.step = step
        self.rollback_performed = rollback_performed


class GitError(DeploymentError):
    """Raised when Git operations fail."""

    def __init__(
        self,
        message: str,
        operation: str,
        branch: Optional[str] = None,
    ) -> None:
        super().__init__(message=message, step=f"git_{operation}")
        self.error_code = ErrorCode.GIT_ERROR.value
        self.details["operation"] = operation
        self.details["branch"] = branch


class CICDError(DeploymentError):
    """Raised when CI/CD pipeline fails."""

    def __init__(
        self,
        message: str,
        build_id: Optional[str] = None,
        build_status: Optional[str] = None,
    ) -> None:
        super().__init__(message=message, step="cicd_trigger")
        self.error_code = ErrorCode.CICD_ERROR.value
        self.details["build_id"] = build_id
        self.details["build_status"] = build_status


# =============================================================================
# Approval Exceptions
# =============================================================================


class ApprovalError(DataAgentError):
    """
    Raised when approval process fails.

    Used for human-in-the-loop approval gates.
    """

    def __init__(
        self,
        message: str,
        approval_type: str,
        timeout: bool = False,
        rejected: bool = False,
        rejected_by: Optional[str] = None,
        rejection_reason: Optional[str] = None,
    ) -> None:
        """
        Initialize approval error.

        Args:
            message: Error message
            approval_type: Type of approval required
            timeout: Whether approval timed out
            rejected: Whether approval was rejected
            rejected_by: Who rejected the approval
            rejection_reason: Reason for rejection
        """
        if timeout:
            error_code = ErrorCode.APPROVAL_TIMEOUT.value
        elif rejected:
            error_code = ErrorCode.APPROVAL_REJECTED.value
        else:
            error_code = ErrorCode.APPROVAL_ERROR.value

        details = {
            "approval_type": approval_type,
            "timeout": timeout,
            "rejected": rejected,
            "rejected_by": rejected_by,
            "rejection_reason": rejection_reason,
        }
        super().__init__(
            message=message,
            error_code=error_code,
            details=details,
            is_retryable=not rejected,  # Can retry if timeout, not if rejected
        )
        self.approval_type = approval_type
        self.timeout = timeout
        self.rejected = rejected


# =============================================================================
# Metadata Exceptions
# =============================================================================


class MetadataError(DataAgentError):
    """
    Raised when metadata operations fail.

    Used for database operations on the metadata repository.
    """

    def __init__(
        self,
        message: str,
        operation: Optional[str] = None,
        entity_type: Optional[str] = None,
        entity_id: Optional[Any] = None,
        is_retryable: bool = True,
        cause: Optional[Exception] = None,
    ) -> None:
        """
        Initialize metadata error.

        Args:
            message: Error message
            operation: Database operation that failed
            entity_type: Type of entity being operated on
            entity_id: ID of the entity
            is_retryable: Whether operation can be retried
            cause: Original database exception
        """
        details = {
            "operation": operation,
            "entity_type": entity_type,
            "entity_id": str(entity_id) if entity_id else None,
        }
        super().__init__(
            message=message,
            error_code=ErrorCode.METADATA_ERROR.value,
            details=details,
            is_retryable=is_retryable,
            cause=cause,
        )
        self.operation = operation
        self.entity_type = entity_type


class DatabaseConnectionError(MetadataError):
    """Raised when database connection fails."""

    def __init__(
        self,
        message: str,
        host: Optional[str] = None,
        cause: Optional[Exception] = None,
    ) -> None:
        super().__init__(
            message=message,
            operation="connect",
            is_retryable=True,
            cause=cause,
        )
        self.error_code = ErrorCode.CONNECTION_ERROR.value
        self.details["host"] = host


class QueryError(MetadataError):
    """Raised when a database query fails."""

    def __init__(
        self,
        message: str,
        query_name: Optional[str] = None,
        cause: Optional[Exception] = None,
    ) -> None:
        super().__init__(
            message=message,
            operation="query",
            is_retryable=False,
            cause=cause,
        )
        self.error_code = ErrorCode.QUERY_ERROR.value
        self.details["query_name"] = query_name


# =============================================================================
# Integration Exceptions
# =============================================================================


class IntegrationError(DataAgentError):
    """
    Raised when external integration fails.

    Used for Pub/Sub, Secret Manager, Jira, GCS, BigQuery, etc.
    """

    def __init__(
        self,
        message: str,
        service: str,
        status_code: Optional[int] = None,
        is_retryable: bool = True,
        cause: Optional[Exception] = None,
    ) -> None:
        """
        Initialize integration error.

        Args:
            message: Error message
            service: External service name
            status_code: HTTP status code if applicable
            is_retryable: Whether operation can be retried
            cause: Original exception
        """
        details = {
            "service": service,
            "status_code": status_code,
        }
        super().__init__(
            message=message,
            error_code=ErrorCode.INTEGRATION_ERROR.value,
            details=details,
            is_retryable=is_retryable,
            cause=cause,
        )
        self.service = service
        self.status_code = status_code


class PubSubError(IntegrationError):
    """Raised when Pub/Sub operations fail."""

    def __init__(
        self,
        message: str,
        topic: Optional[str] = None,
        subscription: Optional[str] = None,
        cause: Optional[Exception] = None,
    ) -> None:
        super().__init__(
            message=message,
            service="pubsub",
            cause=cause,
        )
        self.error_code = ErrorCode.PUBSUB_ERROR.value
        self.details["topic"] = topic
        self.details["subscription"] = subscription


class SecretManagerError(IntegrationError):
    """Raised when Secret Manager operations fail."""

    def __init__(
        self,
        message: str,
        secret_name: Optional[str] = None,
        cause: Optional[Exception] = None,
    ) -> None:
        super().__init__(
            message=message,
            service="secret_manager",
            cause=cause,
        )
        self.error_code = ErrorCode.SECRET_MANAGER_ERROR.value
        # Don't include secret name in details for security
        self.details["secret_name_length"] = len(secret_name) if secret_name else None


# =============================================================================
# Security Exceptions
# =============================================================================


class SecurityError(DataAgentError):
    """
    Raised when security checks fail.

    Used for authentication, authorization, and security validation.
    """

    def __init__(
        self,
        message: str,
        check_type: Optional[str] = None,
        is_retryable: bool = False,
    ) -> None:
        """
        Initialize security error.

        Args:
            message: Error message
            check_type: Type of security check that failed
            is_retryable: Whether operation can be retried
        """
        details = {"check_type": check_type}
        super().__init__(
            message=message,
            error_code=ErrorCode.SECURITY_ERROR.value,
            details=details,
            is_retryable=is_retryable,
        )


class AuthenticationError(SecurityError):
    """Raised when authentication fails."""

    def __init__(self, message: str = "Authentication failed") -> None:
        super().__init__(message=message, check_type="authentication")
        self.error_code = ErrorCode.AUTHENTICATION_ERROR.value


class AuthorizationError(SecurityError):
    """Raised when authorization fails."""

    def __init__(
        self,
        message: str = "Not authorized",
        required_permission: Optional[str] = None,
    ) -> None:
        super().__init__(message=message, check_type="authorization")
        self.error_code = ErrorCode.AUTHORIZATION_ERROR.value
        self.details["required_permission"] = required_permission


# =============================================================================
# Timeout Exceptions
# =============================================================================


class TimeoutError(DataAgentError):
    """
    Raised when operations timeout.

    Used for workflow and step timeouts.
    """

    def __init__(
        self,
        message: str,
        timeout_seconds: int,
        elapsed_seconds: Optional[int] = None,
        is_retryable: bool = True,
    ) -> None:
        """
        Initialize timeout error.

        Args:
            message: Error message
            timeout_seconds: Configured timeout
            elapsed_seconds: Actual elapsed time
            is_retryable: Whether operation can be retried
        """
        details = {
            "timeout_seconds": timeout_seconds,
            "elapsed_seconds": elapsed_seconds,
        }
        super().__init__(
            message=message,
            error_code=ErrorCode.TIMEOUT_ERROR.value,
            details=details,
            is_retryable=is_retryable,
        )


class WorkflowTimeoutError(TimeoutError):
    """Raised when entire workflow times out."""

    def __init__(
        self,
        timeout_seconds: int,
        elapsed_seconds: Optional[int] = None,
        current_phase: Optional[str] = None,
    ) -> None:
        super().__init__(
            message=f"Workflow timed out after {timeout_seconds} seconds",
            timeout_seconds=timeout_seconds,
            elapsed_seconds=elapsed_seconds,
        )
        self.error_code = ErrorCode.WORKFLOW_TIMEOUT.value
        self.details["current_phase"] = current_phase


class StepTimeoutError(TimeoutError):
    """Raised when a specific step times out."""

    def __init__(
        self,
        step_name: str,
        timeout_seconds: int,
        elapsed_seconds: Optional[int] = None,
    ) -> None:
        super().__init__(
            message=f"Step '{step_name}' timed out after {timeout_seconds} seconds",
            timeout_seconds=timeout_seconds,
            elapsed_seconds=elapsed_seconds,
        )
        self.error_code = ErrorCode.STEP_TIMEOUT.value
        self.details["step_name"] = step_name


# =============================================================================
# Additional Exceptions (for __init__.py compatibility)
# =============================================================================


class ConfigurationError(ValidationError):
    """Raised when configuration is invalid."""

    def __init__(
        self,
        message: str,
        config_key: Optional[str] = None,
        errors: Optional[List[str]] = None,
    ) -> None:
        super().__init__(
            message=message,
            errors=errors,
            error_code=ErrorCode.CONFIG_VALIDATION_ERROR.value,
        )
        self.details["config_key"] = config_key


class TemplateSelectionError(PlanningError):
    """Raised when template selection fails."""

    def __init__(
        self,
        message: str,
        source_type: Optional[str] = None,
        mode: Optional[str] = None,
    ) -> None:
        super().__init__(
            message=message,
            reason="template_selection_failed",
        )
        self.error_code = ErrorCode.TEMPLATE_NOT_FOUND.value
        self.details["source_type"] = source_type
        self.details["mode"] = mode


class CodeGenerationError(GenerationError):
    """Raised when code generation fails."""

    def __init__(
        self,
        message: str,
        artifact_type: Optional[str] = None,
        cause: Optional[Exception] = None,
    ) -> None:
        super().__init__(
            message=message,
            artifact_type=artifact_type,
        )
        self.error_code = ErrorCode.CODE_SYNTAX_ERROR.value
        if cause:
            self.cause = cause


class ComposerError(DeploymentError):
    """Raised when Cloud Composer operations fail."""

    def __init__(
        self,
        message: str,
        environment: Optional[str] = None,
        dag_name: Optional[str] = None,
    ) -> None:
        super().__init__(message=message, step="composer_deploy")
        self.details["environment"] = environment
        self.details["dag_name"] = dag_name


class TransactionError(MetadataError):
    """Raised when a database transaction fails."""

    def __init__(
        self,
        message: str,
        operation: Optional[str] = None,
        cause: Optional[Exception] = None,
    ) -> None:
        super().__init__(
            message=message,
            operation=operation,
            is_retryable=True,
            cause=cause,
        )


class ApprovalTimeoutError(ApprovalError):
    """Raised when approval times out."""

    def __init__(
        self,
        approval_type: str,
        timeout_seconds: int,
    ) -> None:
        super().__init__(
            message=f"Approval '{approval_type}' timed out after {timeout_seconds} seconds",
            approval_type=approval_type,
            timeout=True,
        )


class ApprovalRejectedError(ApprovalError):
    """Raised when approval is rejected."""

    def __init__(
        self,
        approval_type: str,
        rejected_by: str,
        reason: Optional[str] = None,
    ) -> None:
        super().__init__(
            message=f"Approval '{approval_type}' rejected by {rejected_by}",
            approval_type=approval_type,
            rejected=True,
            rejected_by=rejected_by,
            rejection_reason=reason,
        )


class AccessDeniedError(SecurityError):
    """Raised when access is denied."""

    def __init__(
        self,
        message: str = "Access denied",
        resource: Optional[str] = None,
    ) -> None:
        super().__init__(message=message, check_type="access")
        self.error_code = ErrorCode.AUTHORIZATION_ERROR.value
        self.details["resource"] = resource


class SecretAccessError(SecurityError):
    """Raised when secret access fails."""

    def __init__(
        self,
        message: str,
        cause: Optional[Exception] = None,
    ) -> None:
        super().__init__(message=message, check_type="secret_access")
        if cause:
            self.cause = cause


class OperationTimeoutError(TimeoutError):
    """Raised when a generic operation times out."""

    def __init__(
        self,
        operation: str,
        timeout_seconds: int,
        elapsed_seconds: Optional[int] = None,
    ) -> None:
        super().__init__(
            message=f"Operation '{operation}' timed out after {timeout_seconds} seconds",
            timeout_seconds=timeout_seconds,
            elapsed_seconds=elapsed_seconds,
        )
        self.details["operation"] = operation


class LLMTimeoutError(TimeoutError):
    """Raised when LLM call times out."""

    def __init__(
        self,
        model: str,
        timeout_seconds: int,
    ) -> None:
        super().__init__(
            message=f"LLM call to '{model}' timed out after {timeout_seconds} seconds",
            timeout_seconds=timeout_seconds,
        )
        self.details["model"] = model
