"""
Backend Utils - Re-exports from Platform Services

WHY: Backend uses platform_services for infrastructure clients.
     This module re-exports for backward compatibility.

PREFERRED: Import directly from platform_services
    from platform_services.infrastructure_clients import get_redis_client

BACKWARD COMPATIBLE (existing code):
    from backend.utils import get_redis_client
"""
import warnings

warnings.warn(
    "backend.utils infrastructure clients are deprecated. "
    "Import from platform_services.infrastructure_clients instead.",
    DeprecationWarning,
    stacklevel=2
)

# Re-export from platform_services for backward compatibility
from platform_services.infrastructure_clients import (
    get_kafka_producer,
    get_kafka_consumer,
    get_redis_client,
    get_postgres_client,
    redis_client,
    postgres_client,
    kafka_client,
)

# Additional utilities that are unique to backend
from .cost_tracker import CostTracker, cost_tracker
from .otel_tracing import get_tracer, trace_async, configure_tracing
from .registry_manager import RegistryManager
from .slack_notifier import SlackNotifier, slack_notifier
from .github_actions import GitHubActionsClient

__all__ = [
    # Infrastructure (re-exported from platform_services)
    "get_kafka_producer",
    "get_kafka_consumer",
    "get_redis_client",
    "get_postgres_client",
    "redis_client",
    "postgres_client",
    "kafka_client",
    # Unique backend utilities
    "CostTracker",
    "cost_tracker",
    "get_tracer",
    "trace_async",
    "configure_tracing",
    "RegistryManager",
    "SlackNotifier",
    "slack_notifier",
    "GitHubActionsClient",
]
