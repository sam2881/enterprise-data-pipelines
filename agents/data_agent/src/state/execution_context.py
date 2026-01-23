"""
Runtime execution context for workflow operations.

This module provides the ExecutionContext class which holds transient
runtime information that shouldn't be persisted in the workflow state.

RULES:
1. ExecutionContext is NOT persisted - it's recreated on each execution
2. Use for environment config, metrics, and transient data
3. Never store business logic decisions here
4. Thread-safe for concurrent access
"""

import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, Generator, List, Optional
from functools import wraps


# =============================================================================
# Enums
# =============================================================================


class LogLevel(str, Enum):
    """Log levels for execution context."""

    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


class MetricType(str, Enum):
    """Types of metrics that can be recorded."""

    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    TIMER = "timer"


# =============================================================================
# Step Result
# =============================================================================


@dataclass
class StepResult:
    """Result of an execution step."""

    step_name: str
    status: str
    duration_ms: int
    started_at: datetime
    completed_at: datetime
    details: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "step_name": self.step_name,
            "status": self.status,
            "duration_ms": self.duration_ms,
            "started_at": self.started_at.isoformat() + "Z",
            "completed_at": self.completed_at.isoformat() + "Z",
            "details": self.details,
            "error": self.error,
        }


# =============================================================================
# Metric
# =============================================================================


@dataclass
class Metric:
    """A recorded metric."""

    name: str
    value: Any
    metric_type: MetricType
    timestamp: datetime
    tags: Dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "value": self.value,
            "type": self.metric_type.value,
            "timestamp": self.timestamp.isoformat() + "Z",
            "tags": self.tags,
        }


# =============================================================================
# Execution Context
# =============================================================================


@dataclass
class ExecutionContext:
    """
    Runtime context for workflow execution.

    Contains environment-specific configuration, metrics collection,
    and transient state that shouldn't be persisted to the database.

    This class is thread-safe for concurrent metric recording.

    Usage:
        ctx = ExecutionContext(
            environment="dev",
            project_id="my-project",
            region="us-central1"
        )

        with ctx.timed_step("load_data"):
            # do work
            pass

        ctx.record_metric("rows_processed", 1000, MetricType.COUNTER)
    """

    # =========================================================================
    # Environment Configuration
    # =========================================================================

    environment: str
    project_id: str
    region: str

    # =========================================================================
    # Runtime Configuration
    # =========================================================================

    dry_run: bool = False
    verbose: bool = False
    debug: bool = False

    # Request identification
    request_id: Optional[str] = None
    correlation_id: Optional[str] = None

    # Timeouts
    default_timeout_seconds: int = 3600
    step_timeout_seconds: int = 600

    # =========================================================================
    # Timing
    # =========================================================================

    started_at: datetime = field(default_factory=datetime.utcnow)

    # =========================================================================
    # Collections (thread-safe)
    # =========================================================================

    metrics: List[Metric] = field(default_factory=list)
    execution_log: List[StepResult] = field(default_factory=list)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    # =========================================================================
    # Feature Flags
    # =========================================================================

    feature_flags: Dict[str, bool] = field(default_factory=dict)

    # =========================================================================
    # Secrets Cache (runtime only, never persisted)
    # =========================================================================

    _secrets_cache: Dict[str, str] = field(default_factory=dict, repr=False)

    # =========================================================================
    # Methods
    # =========================================================================

    def log_step(
        self,
        step_name: str,
        status: str,
        duration_ms: int,
        details: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
    ) -> StepResult:
        """
        Log an execution step.

        Args:
            step_name: Name of the step
            status: Status (success, failed, skipped)
            duration_ms: Duration in milliseconds
            details: Additional details
            error: Error message if failed

        Returns:
            StepResult object
        """
        now = datetime.utcnow()
        result = StepResult(
            step_name=step_name,
            status=status,
            duration_ms=duration_ms,
            started_at=datetime.utcnow(),  # Approximate
            completed_at=now,
            details=details or {},
            error=error,
        )

        with self._lock:
            self.execution_log.append(result)

        return result

    @contextmanager
    def timed_step(
        self,
        step_name: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Context manager for timing a step.

        Args:
            step_name: Name of the step
            details: Additional details to include

        Yields:
            Mutable dict for adding details during execution

        Usage:
            with ctx.timed_step("process_data") as step_details:
                step_details["rows"] = process_rows()
        """
        step_details = details.copy() if details else {}
        start_time = time.perf_counter()
        started_at = datetime.utcnow()
        error_msg: Optional[str] = None
        status = "success"

        try:
            yield step_details
        except Exception as e:
            error_msg = str(e)
            status = "failed"
            raise
        finally:
            end_time = time.perf_counter()
            duration_ms = int((end_time - start_time) * 1000)
            completed_at = datetime.utcnow()

            result = StepResult(
                step_name=step_name,
                status=status,
                duration_ms=duration_ms,
                started_at=started_at,
                completed_at=completed_at,
                details=step_details,
                error=error_msg,
            )

            with self._lock:
                self.execution_log.append(result)

    def record_metric(
        self,
        name: str,
        value: Any,
        metric_type: MetricType = MetricType.GAUGE,
        tags: Optional[Dict[str, str]] = None,
    ) -> Metric:
        """
        Record a metric.

        Args:
            name: Metric name
            value: Metric value
            metric_type: Type of metric
            tags: Additional tags

        Returns:
            Metric object
        """
        metric = Metric(
            name=name,
            value=value,
            metric_type=metric_type,
            timestamp=datetime.utcnow(),
            tags=tags or {},
        )

        with self._lock:
            self.metrics.append(metric)

        return metric

    def increment_counter(
        self,
        name: str,
        delta: int = 1,
        tags: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Increment a counter metric.

        Args:
            name: Counter name
            delta: Amount to increment
            tags: Additional tags
        """
        self.record_metric(name, delta, MetricType.COUNTER, tags)

    def record_timer(
        self,
        name: str,
        duration_ms: int,
        tags: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Record a timer metric.

        Args:
            name: Timer name
            duration_ms: Duration in milliseconds
            tags: Additional tags
        """
        self.record_metric(name, duration_ms, MetricType.TIMER, tags)

    def get_elapsed_ms(self) -> int:
        """Get elapsed time since context creation in milliseconds."""
        delta = datetime.utcnow() - self.started_at
        return int(delta.total_seconds() * 1000)

    def is_timeout_exceeded(self) -> bool:
        """Check if default timeout has been exceeded."""
        return self.get_elapsed_ms() > (self.default_timeout_seconds * 1000)

    def get_feature_flag(self, name: str, default: bool = False) -> bool:
        """
        Get a feature flag value.

        Args:
            name: Feature flag name
            default: Default value if not set

        Returns:
            Feature flag value
        """
        return self.feature_flags.get(name, default)

    def set_feature_flag(self, name: str, value: bool) -> None:
        """
        Set a feature flag value.

        Args:
            name: Feature flag name
            value: Feature flag value
        """
        self.feature_flags[name] = value

    def cache_secret(self, name: str, value: str) -> None:
        """
        Cache a secret value (for current execution only).

        Args:
            name: Secret name
            value: Secret value
        """
        self._secrets_cache[name] = value

    def get_cached_secret(self, name: str) -> Optional[str]:
        """
        Get a cached secret value.

        Args:
            name: Secret name

        Returns:
            Cached secret value or None
        """
        return self._secrets_cache.get(name)

    def clear_secrets_cache(self) -> None:
        """Clear all cached secrets."""
        self._secrets_cache.clear()

    def get_summary(self) -> Dict[str, Any]:
        """
        Get execution summary.

        Returns:
            Summary dict with metrics and step results
        """
        with self._lock:
            return {
                "environment": self.environment,
                "project_id": self.project_id,
                "region": self.region,
                "request_id": self.request_id,
                "correlation_id": self.correlation_id,
                "started_at": self.started_at.isoformat() + "Z",
                "elapsed_ms": self.get_elapsed_ms(),
                "dry_run": self.dry_run,
                "step_count": len(self.execution_log),
                "metric_count": len(self.metrics),
                "steps": [step.to_dict() for step in self.execution_log],
                "metrics": [metric.to_dict() for metric in self.metrics],
            }

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary (for logging/debugging)."""
        return {
            "environment": self.environment,
            "project_id": self.project_id,
            "region": self.region,
            "request_id": self.request_id,
            "dry_run": self.dry_run,
            "verbose": self.verbose,
            "debug": self.debug,
            "started_at": self.started_at.isoformat() + "Z",
            "elapsed_ms": self.get_elapsed_ms(),
        }


# =============================================================================
# Factory Functions
# =============================================================================


def create_execution_context(
    environment: str,
    project_id: str,
    region: str = "us-central1",
    request_id: Optional[str] = None,
    dry_run: bool = False,
    verbose: bool = False,
) -> ExecutionContext:
    """
    Create a new execution context.

    Args:
        environment: Target environment (dev, qa, prod)
        project_id: GCP project ID
        region: GCP region
        request_id: Request identifier
        dry_run: Whether to run in dry-run mode
        verbose: Whether to enable verbose logging

    Returns:
        New ExecutionContext
    """
    return ExecutionContext(
        environment=environment,
        project_id=project_id,
        region=region,
        request_id=request_id,
        dry_run=dry_run,
        verbose=verbose,
        debug=verbose,  # Debug follows verbose
    )


# =============================================================================
# Decorator for Timed Functions
# =============================================================================


def timed(ctx_attr: str = "ctx", step_name: Optional[str] = None) -> Callable:
    """
    Decorator to time a function using execution context.

    Args:
        ctx_attr: Name of the context attribute/argument
        step_name: Custom step name (defaults to function name)

    Usage:
        @timed(ctx_attr="context")
        def my_function(context: ExecutionContext):
            pass
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Try to find context in kwargs or first arg
            ctx = kwargs.get(ctx_attr)
            if ctx is None and args:
                ctx = args[0] if isinstance(args[0], ExecutionContext) else None

            name = step_name or func.__name__

            if ctx is not None:
                with ctx.timed_step(name):
                    return func(*args, **kwargs)
            else:
                return func(*args, **kwargs)

        return wrapper
    return decorator
