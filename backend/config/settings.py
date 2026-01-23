"""
Unified Configuration Management - Production Settings

WHY: Centralizes all configuration in one place.
     Supports environment-specific overrides.
     Validates configuration at startup.
     No hardcoded values in application code.

PATTERN: Pydantic Settings with environment variable override.
         Hierarchical configuration: base -> environment -> secrets.

USAGE:
    from config.settings import get_settings

    settings = get_settings()
    print(settings.kafka.bootstrap_servers)
"""
import os
from functools import lru_cache
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings
from enum import Enum


class Environment(str, Enum):
    """Deployment environments."""
    LOCAL = "local"
    DEV = "dev"
    STAGING = "staging"
    PROD = "prod"


# =============================================================================
# Infrastructure Configuration
# =============================================================================

class KafkaConfig(BaseModel):
    """Kafka configuration."""
    bootstrap_servers: str = "kafka:9092"
    consumer_group_prefix: str = "ai-agent"
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = True

    # Topic configuration
    topics: Dict[str, str] = {
        # ServiceNow/IT Service topics
        "servicenow_incidents": "servicenow.incidents",
        "gcp_alerts": "gcp.alerts",
        "incident_created": "incident.created",
        "incident_resolved": "incident.resolved",
        "agent_events": "agent.events",

        # Jira/Data Pipeline topics
        "jira_stories": "jira.stories",
        "jira_story_created": "jira.story.created",
        "pipeline_requested": "pipeline.requested",
        "pipeline_progress": "pipeline.progress",
        "pipeline_completed": "pipeline.completed",
    }


class RedisConfig(BaseModel):
    """Redis configuration."""
    host: str = "redis"
    port: int = 6379
    db: int = 0
    password: Optional[str] = None

    # TTL settings (seconds)
    default_ttl: int = 3600
    incident_ttl: int = 600
    embedding_ttl: int = 86400
    session_ttl: int = 7200


class PostgresConfig(BaseModel):
    """PostgreSQL configuration."""
    host: str = "postgres"
    port: int = 5432
    database: str = "agentdb"
    user: str = "admin"
    # Password from secrets manager
    pool_size: int = 10
    max_overflow: int = 20


class WeaviateConfig(BaseModel):
    """Weaviate vector database configuration."""
    host: str = "weaviate"
    port: int = 8080
    grpc_port: int = 50051
    class_name: str = "Script"
    embedding_model: str = "text-embedding-3-small"


class Neo4jConfig(BaseModel):
    """Neo4j graph database configuration."""
    host: str = "neo4j"
    port: int = 7687
    http_port: int = 7474
    user: str = "neo4j"
    # Password from secrets manager


# =============================================================================
# GCP Configuration
# =============================================================================

class GCSConfig(BaseModel):
    """Google Cloud Storage configuration."""
    project_id: str = "agent-ai-test-461120"
    region: str = "us-central1"

    # Medallion architecture buckets
    bucket_raw: str = "agent-ai-test-461120-raw-data"
    bucket_bronze: str = "agent-ai-test-461120-bronze"
    bucket_silver: str = "agent-ai-test-461120-silver"
    bucket_gold: str = "agent-ai-test-461120-gold"
    bucket_temp: str = "agent-ai-test-461120-temp"


class BigQueryConfig(BaseModel):
    """BigQuery configuration."""
    project_id: str = "agent-ai-test-461120"
    dataset: str = "data_warehouse"
    location: str = "US"


class DataprocConfig(BaseModel):
    """Dataproc Spark cluster configuration."""
    project_id: str = "agent-ai-test-461120"
    region: str = "us-central1"
    cluster_name: str = "ai-agent-spark"

    # Cluster sizing (minimal for cost optimization)
    master_machine_type: str = "n1-standard-2"
    master_disk_size: int = 100  # GB
    worker_machine_type: str = "n1-standard-2"
    worker_disk_size: int = 100  # GB
    num_workers: int = 2
    num_preemptible_workers: int = 0  # Add for cost savings

    # Auto-scaling
    autoscaling_enabled: bool = True
    min_workers: int = 2
    max_workers: int = 4

    # Cluster lifecycle
    idle_delete_ttl: str = "1800s"  # Delete after 30 min idle
    max_age: str = "14400s"  # Max 4 hours

    # Spark configuration
    spark_version: str = "2.1-debian11"  # Dataproc image version
    initialization_actions: List[str] = []

    @property
    def cluster_config(self) -> Dict[str, Any]:
        """Generate Dataproc cluster configuration."""
        return {
            "project_id": self.project_id,
            "region": self.region,
            "cluster_name": self.cluster_name,
            "config": {
                "master_config": {
                    "num_instances": 1,
                    "machine_type_uri": self.master_machine_type,
                    "disk_config": {
                        "boot_disk_type": "pd-standard",
                        "boot_disk_size_gb": self.master_disk_size,
                    },
                },
                "worker_config": {
                    "num_instances": self.num_workers,
                    "machine_type_uri": self.worker_machine_type,
                    "disk_config": {
                        "boot_disk_type": "pd-standard",
                        "boot_disk_size_gb": self.worker_disk_size,
                    },
                },
                "software_config": {
                    "image_version": self.spark_version,
                    "properties": {
                        "spark:spark.executor.memory": "2g",
                        "spark:spark.driver.memory": "2g",
                        "spark:spark.sql.adaptive.enabled": "true",
                    },
                },
                "lifecycle_config": {
                    "idle_delete_ttl": self.idle_delete_ttl,
                    "auto_delete_time": None,
                },
            },
        }


# =============================================================================
# LLM Configuration
# =============================================================================

class LLMConfig(BaseModel):
    """LLM provider configuration."""
    # Primary model for general tasks
    primary_model: str = "gpt-4-turbo-preview"
    primary_provider: str = "openai"

    # Judge model (different provider for independence)
    judge_model: str = "claude-3-opus-20240229"
    judge_provider: str = "anthropic"

    # Embedding model
    embedding_model: str = "text-embedding-3-small"
    embedding_dimensions: int = 1536

    # Parameters
    temperature: float = 0.0  # Deterministic outputs
    max_tokens: int = 4096
    timeout: int = 60

    # Cost tracking
    track_costs: bool = True
    cost_alert_threshold: float = 100.0  # USD per day


# =============================================================================
# Agent Configuration
# =============================================================================

class AgentConfig(BaseModel):
    """Agent-specific configuration."""
    # Retry settings
    max_retries: int = 3
    retry_delay_seconds: float = 1.0
    retry_backoff_multiplier: float = 2.0

    # Timeouts
    task_timeout_seconds: int = 300
    health_check_interval: int = 30

    # Circuit breaker
    circuit_breaker_threshold: int = 5
    circuit_breaker_timeout: int = 60


class ServiceNowAgentConfig(AgentConfig):
    """ServiceNow agent configuration."""
    api_port: int = 8010
    incident_table: str = "incident"
    batch_size: int = 100


class JiraAgentConfig(AgentConfig):
    """Jira agent configuration."""
    api_port: int = 8011
    project_key: str = "ENG"
    story_types: List[str] = ["Story", "Task", "Bug"]


class DataAgentConfig(AgentConfig):
    """Data pipeline agent configuration."""
    api_port: int = 8014
    default_target_format: str = "parquet"
    default_compression: str = "snappy"


# =============================================================================
# Orchestration Configuration
# =============================================================================

class OrchestrationConfig(BaseModel):
    """Orchestration and workflow configuration."""
    api_port: int = 8000

    # Confidence thresholds
    auto_reject_threshold: float = 0.30
    low_confidence_threshold: float = 0.50
    high_confidence_threshold: float = 0.75
    critical_action_threshold: float = 0.85

    # Execution policy
    max_concurrent_executions: int = 5
    execution_cooldown_seconds: int = 60
    require_approval_for_critical: bool = True

    # Workflow settings
    workflow_timeout_seconds: int = 3600
    max_workflow_retries: int = 3


class RAGConfig(BaseModel):
    """RAG system configuration."""
    # Search settings
    top_k: int = 10
    rerank_top_k: int = 5
    min_relevance_score: float = 0.5

    # RRF fusion parameter
    rrf_k: int = 60

    # Cross-encoder reranking
    reranker_model: str = "cross-encoder/ms-marco-MiniLM-L-6-v2"
    use_reranker: bool = True


# =============================================================================
# Observability Configuration
# =============================================================================

class ObservabilityConfig(BaseModel):
    """Observability and monitoring configuration."""
    # Langfuse
    langfuse_enabled: bool = True
    langfuse_host: str = "http://langfuse:3000"

    # OpenTelemetry
    otel_enabled: bool = True
    otel_endpoint: str = "http://tempo:4317"

    # Prometheus
    prometheus_enabled: bool = True
    prometheus_port: int = 9090

    # Logging
    log_level: str = "INFO"
    log_format: str = "json"


# =============================================================================
# Main Settings Class
# =============================================================================

class Settings(BaseSettings):
    """
    Main application settings.

    Load order:
    1. Default values in this class
    2. Environment variables
    3. .env file
    4. Secrets from GCP Secret Manager (runtime)
    """

    # Environment
    environment: Environment = Environment.LOCAL
    debug: bool = False
    app_name: str = "ai-agent-platform"
    version: str = "2.0.0"

    # Infrastructure
    kafka: KafkaConfig = KafkaConfig()
    redis: RedisConfig = RedisConfig()
    postgres: PostgresConfig = PostgresConfig()
    weaviate: WeaviateConfig = WeaviateConfig()
    neo4j: Neo4jConfig = Neo4jConfig()

    # GCP
    gcp_project_id: str = "agent-ai-test-461120"
    gcs: GCSConfig = GCSConfig()
    bigquery: BigQueryConfig = BigQueryConfig()
    dataproc: DataprocConfig = DataprocConfig()

    # LLM
    llm: LLMConfig = LLMConfig()

    # Agents
    servicenow_agent: ServiceNowAgentConfig = ServiceNowAgentConfig()
    jira_agent: JiraAgentConfig = JiraAgentConfig()
    data_agent: DataAgentConfig = DataAgentConfig()

    # Orchestration
    orchestration: OrchestrationConfig = OrchestrationConfig()
    rag: RAGConfig = RAGConfig()

    # Observability
    observability: ObservabilityConfig = ObservabilityConfig()

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        env_nested_delimiter = "__"
        case_sensitive = False
        extra = "ignore"  # Ignore extra env vars not defined in this model

    @field_validator("environment", mode="before")
    @classmethod
    def parse_environment(cls, v):
        if isinstance(v, str):
            return Environment(v.lower())
        return v

    def is_production(self) -> bool:
        """Check if running in production."""
        return self.environment == Environment.PROD

    def is_local(self) -> bool:
        """Check if running locally."""
        return self.environment == Environment.LOCAL


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()


# Environment-specific overrides
ENVIRONMENT_OVERRIDES: Dict[Environment, Dict[str, Any]] = {
    Environment.LOCAL: {
        "debug": True,
        "observability": {"log_level": "DEBUG"},
    },
    Environment.DEV: {
        "debug": True,
    },
    Environment.STAGING: {
        "debug": False,
    },
    Environment.PROD: {
        "debug": False,
        "orchestration": {"require_approval_for_critical": True},
        "observability": {"log_level": "WARNING"},
    },
}
