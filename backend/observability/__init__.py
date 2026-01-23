"""
Observability Module - Unified Metrics, Tracing, and Logging

Consolidates all observability concerns:
- Metrics: Prometheus metrics (from orchestrator/metrics.py)
- Tracing: OpenTelemetry distributed tracing (from utils/otel_tracing.py)
- Logging: Structured JSON logging

Usage:
    from observability import get_tracer, metrics, logger

    # Tracing
    with get_tracer().start_as_current_span("my_operation"):
        do_work()

    # Metrics
    metrics.request_counter.inc()
    metrics.request_duration.observe(elapsed)

    # Logging
    logger.info("operation completed", extra={"workflow_id": wf_id})
"""
import os

# Re-export from existing modules
try:
    from utils.otel_tracing import init_tracing, get_tracer
except ImportError:
    get_tracer = None
    init_tracing = None

try:
    from orchestrator.metrics import (
        TASK_EXECUTION_TIME,
        TASK_COUNTER,
        SCRIPT_MATCH_CONFIDENCE,
        ACTIVE_TASKS,
    )
except ImportError:
    TASK_EXECUTION_TIME = None
    TASK_COUNTER = None
    SCRIPT_MATCH_CONFIDENCE = None
    ACTIVE_TASKS = None

# Structured logging
import logging
import json
from datetime import datetime


class JSONFormatter(logging.Formatter):
    """JSON formatter for structured logging."""

    def format(self, record):
        log_record = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Add extra fields
        if hasattr(record, "workflow_id"):
            log_record["workflow_id"] = record.workflow_id
        if hasattr(record, "agent"):
            log_record["agent"] = record.agent
        if hasattr(record, "duration_ms"):
            log_record["duration_ms"] = record.duration_ms

        # Add exception info
        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_record)


def configure_logging(level: str = "INFO", json_format: bool = True):
    """Configure application logging."""
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper()))

    # Remove existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Add console handler
    console_handler = logging.StreamHandler()
    if json_format:
        console_handler.setFormatter(JSONFormatter())
    else:
        console_handler.setFormatter(
            logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        )
    root_logger.addHandler(console_handler)


# Configure on import if not already done
if not logging.getLogger().handlers:
    configure_logging(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        json_format=os.environ.get("LOG_FORMAT", "json") == "json",
    )

logger = logging.getLogger("ai_agent_platform")

__all__ = [
    # Tracing
    "init_tracing",
    "get_tracer",
    # Metrics
    "TASK_EXECUTION_TIME",
    "TASK_COUNTER",
    "SCRIPT_MATCH_CONFIDENCE",
    "ACTIVE_TASKS",
    # Logging
    "logger",
    "configure_logging",
    "JSONFormatter",
]
