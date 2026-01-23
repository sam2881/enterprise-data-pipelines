"""
Structured logging configuration using structlog.

This module provides structured JSON logging with:
- Correlation ID support for request tracing
- Context variables for automatic field propagation
- Performance timing helpers
- Integration with standard library logging

RULES:
1. Always use get_logger(__name__) to get loggers
2. Use bind() to add context that persists across log calls
3. Use structlog.contextvars for request-scoped context
4. Never log sensitive data (secrets, passwords, tokens)
"""

import logging
import sys
import time
from contextvars import ContextVar
from datetime import datetime
from functools import wraps
from typing import Any, Callable, Dict, Optional, TypeVar

import structlog
from structlog.types import EventDict, Processor, WrappedLogger


# =============================================================================
# Context Variables for Request Tracing
# =============================================================================

# Request-scoped context variables
_request_id_ctx: ContextVar[Optional[str]] = ContextVar("request_id", default=None)
_correlation_id_ctx: ContextVar[Optional[str]] = ContextVar("correlation_id", default=None)
_pipeline_name_ctx: ContextVar[Optional[str]] = ContextVar("pipeline_name", default=None)
_environment_ctx: ContextVar[Optional[str]] = ContextVar("environment", default=None)

# Module-level state
_configured = False


# =============================================================================
# Context Management Functions
# =============================================================================


def set_request_context(
    request_id: Optional[str] = None,
    correlation_id: Optional[str] = None,
    pipeline_name: Optional[str] = None,
    environment: Optional[str] = None,
) -> None:
    """
    Set request-scoped context variables.

    These values will be automatically included in all log entries
    within the current async context.

    Args:
        request_id: Unique request identifier
        correlation_id: Correlation ID for distributed tracing
        pipeline_name: Name of the pipeline being processed
        environment: Target environment (dev, qa, prod)
    """
    if request_id is not None:
        _request_id_ctx.set(request_id)
    if correlation_id is not None:
        _correlation_id_ctx.set(correlation_id)
    if pipeline_name is not None:
        _pipeline_name_ctx.set(pipeline_name)
    if environment is not None:
        _environment_ctx.set(environment)


def clear_request_context() -> None:
    """Clear all request-scoped context variables."""
    _request_id_ctx.set(None)
    _correlation_id_ctx.set(None)
    _pipeline_name_ctx.set(None)
    _environment_ctx.set(None)


def get_request_context() -> Dict[str, Optional[str]]:
    """
    Get current request context.

    Returns:
        Dictionary of context variables
    """
    return {
        "request_id": _request_id_ctx.get(),
        "correlation_id": _correlation_id_ctx.get(),
        "pipeline_name": _pipeline_name_ctx.get(),
        "environment": _environment_ctx.get(),
    }


# =============================================================================
# Custom Processors
# =============================================================================


def add_request_context(
    logger: WrappedLogger,
    method_name: str,
    event_dict: EventDict,
) -> EventDict:
    """
    Add request context variables to log entries.

    This processor adds request_id, correlation_id, etc. to each log entry.
    """
    request_id = _request_id_ctx.get()
    correlation_id = _correlation_id_ctx.get()
    pipeline_name = _pipeline_name_ctx.get()
    environment = _environment_ctx.get()

    if request_id:
        event_dict["request_id"] = request_id
    if correlation_id:
        event_dict["correlation_id"] = correlation_id
    if pipeline_name:
        event_dict["pipeline_name"] = pipeline_name
    if environment:
        event_dict["environment"] = environment

    return event_dict


def add_service_info(
    service_name: str,
    service_version: str = "1.0.0",
) -> Processor:
    """
    Create processor that adds service information.

    Args:
        service_name: Name of the service
        service_version: Version of the service

    Returns:
        Processor function
    """
    def processor(
        logger: WrappedLogger,
        method_name: str,
        event_dict: EventDict,
    ) -> EventDict:
        event_dict["service"] = service_name
        event_dict["version"] = service_version
        return event_dict

    return processor


def sanitize_sensitive_data(
    logger: WrappedLogger,
    method_name: str,
    event_dict: EventDict,
) -> EventDict:
    """
    Sanitize sensitive data from log entries.

    Removes or masks fields that might contain sensitive information.
    """
    sensitive_keys = {
        "password", "secret", "token", "key", "credential",
        "api_key", "auth", "bearer", "authorization",
    }

    def sanitize(obj: Any, path: str = "") -> Any:
        if isinstance(obj, dict):
            return {
                k: "***REDACTED***" if any(s in k.lower() for s in sensitive_keys)
                else sanitize(v, f"{path}.{k}")
                for k, v in obj.items()
            }
        elif isinstance(obj, list):
            return [sanitize(item, f"{path}[{i}]") for i, item in enumerate(obj)]
        return obj

    return sanitize(event_dict)


def add_caller_info(
    logger: WrappedLogger,
    method_name: str,
    event_dict: EventDict,
) -> EventDict:
    """
    Add caller module and function information.

    Extracts the actual caller from the stack, skipping structlog internals.
    """
    # The logger name typically contains the module path
    if "logger" in event_dict:
        event_dict["module"] = event_dict.get("logger", "unknown")
    return event_dict


# =============================================================================
# Logging Configuration
# =============================================================================


def configure_logging(
    level: str = "INFO",
    format_type: str = "json",
    service_name: str = "data-agent",
    service_version: str = "1.0.0",
) -> None:
    """
    Configure structured logging for the application.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        format_type: Output format ("json" or "console")
        service_name: Service name for log entries
        service_version: Service version for log entries

    Example:
        configure_logging(level="DEBUG", format_type="console")
        logger = get_logger(__name__)
        logger.info("Application started", component="main")
    """
    global _configured
    if _configured:
        return

    log_level = getattr(logging, level.upper(), logging.INFO)

    # Configure standard library logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=log_level,
        force=True,
    )

    # Suppress noisy loggers
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("google").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)

    # Build processor chain
    shared_processors: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        add_request_context,
        add_service_info(service_name, service_version),
        sanitize_sensitive_data,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.UnicodeDecoder(),
    ]

    if format_type == "json":
        # JSON format for production
        shared_processors.append(
            structlog.processors.format_exc_info,
        )
        renderer = structlog.processors.JSONRenderer()
    else:
        # Console format for development
        renderer = structlog.dev.ConsoleRenderer(
            colors=True,
            exception_formatter=structlog.dev.plain_traceback,
        )

    structlog.configure(
        processors=shared_processors + [renderer],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )

    _configured = True


def reset_logging() -> None:
    """Reset logging configuration (useful for testing)."""
    global _configured
    structlog.reset_defaults()
    _configured = False


# =============================================================================
# Logger Factory
# =============================================================================


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """
    Get a structured logger instance.

    Args:
        name: Logger name (typically __name__)

    Returns:
        Configured structlog BoundLogger

    Example:
        logger = get_logger(__name__)
        logger.info("Processing started", item_count=100)
    """
    if not _configured:
        configure_logging()
    return structlog.get_logger(name)


# =============================================================================
# Logging Utilities
# =============================================================================


def log_execution_time(
    logger: structlog.stdlib.BoundLogger,
    operation: str,
    start_time: float,
    success: bool = True,
    **extra: Any,
) -> None:
    """
    Log operation execution time.

    Args:
        logger: Logger instance
        operation: Name of the operation
        start_time: Start time from time.perf_counter()
        success: Whether operation succeeded
        **extra: Additional context fields
    """
    duration_ms = int((time.perf_counter() - start_time) * 1000)

    if success:
        logger.info(
            f"{operation} completed",
            operation=operation,
            duration_ms=duration_ms,
            success=True,
            **extra,
        )
    else:
        logger.warning(
            f"{operation} failed",
            operation=operation,
            duration_ms=duration_ms,
            success=False,
            **extra,
        )


T = TypeVar("T")


def logged(
    operation: Optional[str] = None,
    log_args: bool = False,
    log_result: bool = False,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Decorator to log function entry, exit, and timing.

    Args:
        operation: Operation name (defaults to function name)
        log_args: Whether to log function arguments
        log_result: Whether to log return value

    Returns:
        Decorated function

    Example:
        @logged(operation="process_data")
        def process_data(items: list) -> int:
            return len(items)
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        op_name = operation or func.__name__
        logger = get_logger(func.__module__)

        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            start_time = time.perf_counter()

            log_context: Dict[str, Any] = {"operation": op_name}
            if log_args:
                log_context["args"] = str(args)[:200]  # Truncate
                log_context["kwargs"] = str(kwargs)[:200]

            logger.debug(f"Starting {op_name}", **log_context)

            try:
                result = func(*args, **kwargs)
                duration_ms = int((time.perf_counter() - start_time) * 1000)

                log_context["duration_ms"] = duration_ms
                log_context["success"] = True
                if log_result:
                    log_context["result"] = str(result)[:200]

                logger.info(f"Completed {op_name}", **log_context)
                return result

            except Exception as e:
                duration_ms = int((time.perf_counter() - start_time) * 1000)
                logger.error(
                    f"Failed {op_name}",
                    operation=op_name,
                    duration_ms=duration_ms,
                    success=False,
                    error=str(e),
                    error_type=type(e).__name__,
                    exc_info=True,
                )
                raise

        return wrapper
    return decorator


# =============================================================================
# Specialized Loggers
# =============================================================================


class AgentLogger:
    """
    Specialized logger for agent operations.

    Provides convenience methods for common agent logging patterns.
    """

    def __init__(self, agent_name: str) -> None:
        """
        Initialize agent logger.

        Args:
            agent_name: Name of the agent
        """
        self.agent_name = agent_name
        self._logger = get_logger(f"agent.{agent_name}")

    def log_start(self, request_id: str, **context: Any) -> None:
        """Log agent execution start."""
        self._logger.info(
            f"Agent {self.agent_name} starting",
            agent=self.agent_name,
            request_id=request_id,
            event_type="agent_start",
            **context,
        )

    def log_complete(
        self,
        request_id: str,
        duration_ms: int,
        **context: Any,
    ) -> None:
        """Log agent execution completion."""
        self._logger.info(
            f"Agent {self.agent_name} completed",
            agent=self.agent_name,
            request_id=request_id,
            duration_ms=duration_ms,
            event_type="agent_complete",
            **context,
        )

    def log_error(
        self,
        request_id: str,
        error: Exception,
        duration_ms: Optional[int] = None,
        **context: Any,
    ) -> None:
        """Log agent execution error."""
        self._logger.error(
            f"Agent {self.agent_name} failed",
            agent=self.agent_name,
            request_id=request_id,
            duration_ms=duration_ms,
            error=str(error),
            error_type=type(error).__name__,
            event_type="agent_error",
            exc_info=True,
            **context,
        )

    def log_decision(
        self,
        request_id: str,
        decision: str,
        reasoning: str,
        **context: Any,
    ) -> None:
        """Log agent decision for audit trail."""
        self._logger.info(
            f"Agent {self.agent_name} decision: {decision}",
            agent=self.agent_name,
            request_id=request_id,
            decision=decision,
            reasoning=reasoning,
            event_type="agent_decision",
            **context,
        )

    def bind(self, **context: Any) -> "AgentLogger":
        """Create a new logger with bound context."""
        new_logger = AgentLogger(self.agent_name)
        new_logger._logger = self._logger.bind(**context)
        return new_logger


class WorkflowLogger:
    """
    Specialized logger for workflow operations.

    Tracks workflow phases and transitions.
    """

    def __init__(self, workflow_name: str = "pipeline_workflow") -> None:
        """
        Initialize workflow logger.

        Args:
            workflow_name: Name of the workflow
        """
        self.workflow_name = workflow_name
        self._logger = get_logger(f"workflow.{workflow_name}")

    def log_phase_start(
        self,
        request_id: str,
        phase: str,
        **context: Any,
    ) -> None:
        """Log workflow phase start."""
        self._logger.info(
            f"Workflow phase started: {phase}",
            workflow=self.workflow_name,
            request_id=request_id,
            phase=phase,
            event_type="phase_start",
            **context,
        )

    def log_phase_complete(
        self,
        request_id: str,
        phase: str,
        duration_ms: int,
        next_phase: Optional[str] = None,
        **context: Any,
    ) -> None:
        """Log workflow phase completion."""
        self._logger.info(
            f"Workflow phase completed: {phase}",
            workflow=self.workflow_name,
            request_id=request_id,
            phase=phase,
            duration_ms=duration_ms,
            next_phase=next_phase,
            event_type="phase_complete",
            **context,
        )

    def log_workflow_complete(
        self,
        request_id: str,
        total_duration_ms: int,
        final_status: str,
        **context: Any,
    ) -> None:
        """Log workflow completion."""
        level = "info" if final_status == "complete" else "warning"
        log_method = getattr(self._logger, level)
        log_method(
            f"Workflow completed with status: {final_status}",
            workflow=self.workflow_name,
            request_id=request_id,
            total_duration_ms=total_duration_ms,
            final_status=final_status,
            event_type="workflow_complete",
            **context,
        )
