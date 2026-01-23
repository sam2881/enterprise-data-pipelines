"""
Infrastructure Clients - Platform Layer

WHY: Shared infrastructure clients used by BOTH agents AND backend.
     Lives in platform/ to avoid duplication and ensure consistency.

WHO USES THIS:
- agents/ (for caching, external API calls)
- backend/ (for persistence, messaging)

WHAT'S HERE:
- GCP: Dataproc, (TODO) GCS, BigQuery
- Messaging: Kafka
- Databases: Redis, PostgreSQL
- Resilience: Circuit breaker

USAGE:
    from platform.infrastructure_clients import get_redis_client
    from platform.infrastructure_clients import DataprocClient

    redis = get_redis_client()
    redis.set("key", "value", ex=300)
"""
# GCP Clients
from .dataproc_client import DataprocClient, get_dataproc_client, JobState, JobResult

# Messaging
from .kafka_client import KafkaClient, kafka_client, get_kafka_producer, get_kafka_consumer

# Databases
from .redis_client import RedisClient, redis_client, get_redis_client, redis_cache
from .postgres_client import PostgresClient, postgres_client, get_postgres_client

# Resilience
from .circuit_breaker import (
    CircuitBreaker,
    CircuitState,
    github_breaker,
    servicenow_breaker,
    openai_breaker,
    neo4j_breaker,
    weaviate_breaker,
    get_all_breaker_stats,
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
    "github_breaker",
    "servicenow_breaker",
    "openai_breaker",
    "neo4j_breaker",
    "weaviate_breaker",
    "get_all_breaker_stats",
]
