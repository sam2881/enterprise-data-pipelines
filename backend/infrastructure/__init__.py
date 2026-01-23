"""
Backend Infrastructure - Re-exports from Platform Services

WHY: Backend uses platform_services.infrastructure_clients for actual implementations.
     This module re-exports for backward compatibility.

PREFERRED: Import directly from platform_services.infrastructure_clients

USAGE:
    # Preferred (new code)
    from platform_services.infrastructure_clients import get_redis_client

    # Backward compatible (existing code)
    from backend.infrastructure import get_redis_client
"""
# Re-export from platform_services for backward compatibility
from platform_services.infrastructure_clients import (
    # GCP
    DataprocClient,
    get_dataproc_client,
    JobState,
    JobResult,
    # Messaging
    KafkaClient,
    kafka_client,
    get_kafka_producer,
    get_kafka_consumer,
    # Databases
    RedisClient,
    redis_client,
    get_redis_client,
    redis_cache,
    PostgresClient,
    postgres_client,
    get_postgres_client,
    # Resilience
    CircuitBreaker,
    CircuitState,
)

__all__ = [
    # GCP
    "DataprocClient",
    "get_dataproc_client",
    "JobState",
    "JobResult",
    # Messaging
    "KafkaClient",
    "kafka_client",
    "get_kafka_producer",
    "get_kafka_consumer",
    # Databases
    "RedisClient",
    "redis_client",
    "get_redis_client",
    "redis_cache",
    "PostgresClient",
    "postgres_client",
    "get_postgres_client",
    # Resilience
    "CircuitBreaker",
    "CircuitState",
]
