"""
Utility functions and helpers for the Data Engineering Platform.

This module provides:
- Structured logging with structlog
- Custom exception hierarchy
- Common helper functions

Exports:
    - get_logger: Get a configured structlog logger
    - configure_logging: Configure logging for the application
    - AgentLogger: Specialized logger for agents
    - WorkflowLogger: Logger for workflow phases
    - DataAgentError: Base exception for all agent errors
    - ValidationError: Schema/intent validation errors
    - PlanningError: Pipeline planning errors
    - GenerationError: Code generation errors
    - DeploymentError: Deployment/CI-CD errors
    - MetadataError: Database/metadata errors
    - ApprovalError: Human approval errors
    - SecurityError: Security validation errors
    - TimeoutError: Operation timeout errors
"""

from src.utils.logging import (
    get_logger,
    configure_logging,
    AgentLogger,
    WorkflowLogger,
    set_request_context,
    clear_request_context,
)
from src.utils.exceptions import (
    # Base exception
    DataAgentError,
    # Validation errors
    ValidationError,
    SchemaValidationError,
    IntentValidationError,
    ConfigurationError,
    # Planning errors
    PlanningError,
    PipelineNotFoundError,
    SchemaConflictError,
    TemplateSelectionError,
    # Generation errors
    GenerationError,
    TemplateNotFoundError,
    TemplateRenderError,
    CodeGenerationError,
    # Deployment errors
    DeploymentError,
    GitError,
    CICDError,
    ComposerError,
    # Metadata errors
    MetadataError,
    DatabaseConnectionError,
    QueryError,
    TransactionError,
    # Approval errors
    ApprovalError,
    ApprovalTimeoutError,
    ApprovalRejectedError,
    # Security errors
    SecurityError,
    AccessDeniedError,
    SecretAccessError,
    # Timeout errors
    TimeoutError,
    OperationTimeoutError,
    LLMTimeoutError,
)

__all__ = [
    # Logging
    "get_logger",
    "configure_logging",
    "AgentLogger",
    "WorkflowLogger",
    "set_request_context",
    "clear_request_context",
    # Base exception
    "DataAgentError",
    # Validation errors
    "ValidationError",
    "SchemaValidationError",
    "IntentValidationError",
    "ConfigurationError",
    # Planning errors
    "PlanningError",
    "PipelineNotFoundError",
    "SchemaConflictError",
    "TemplateSelectionError",
    # Generation errors
    "GenerationError",
    "TemplateNotFoundError",
    "TemplateRenderError",
    "CodeGenerationError",
    # Deployment errors
    "DeploymentError",
    "GitError",
    "CICDError",
    "ComposerError",
    # Metadata errors
    "MetadataError",
    "DatabaseConnectionError",
    "QueryError",
    "TransactionError",
    # Approval errors
    "ApprovalError",
    "ApprovalTimeoutError",
    "ApprovalRejectedError",
    # Security errors
    "SecurityError",
    "AccessDeniedError",
    "SecretAccessError",
    # Timeout errors
    "TimeoutError",
    "OperationTimeoutError",
    "LLMTimeoutError",
]
