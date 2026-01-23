"""
Circuit Breaker - Re-export from Platform Services

WHY: Backward compatibility for existing imports.
PREFERRED: Import from platform_services.infrastructure_clients directly.
"""
import warnings

warnings.warn(
    "backend.utils.circuit_breaker is deprecated. "
    "Import from platform_services.infrastructure_clients instead.",
    DeprecationWarning,
    stacklevel=2
)

from platform_services.infrastructure_clients.circuit_breaker import (
    CircuitBreaker,
    CircuitState,
    CircuitBreakerConfig,
    CircuitStats,
    github_breaker,
    servicenow_breaker,
    openai_breaker,
    neo4j_breaker,
    weaviate_breaker,
    get_all_breaker_stats,
)

__all__ = [
    "CircuitBreaker",
    "CircuitState",
    "CircuitBreakerConfig",
    "CircuitStats",
    "github_breaker",
    "servicenow_breaker",
    "openai_breaker",
    "neo4j_breaker",
    "weaviate_breaker",
    "get_all_breaker_stats",
]
