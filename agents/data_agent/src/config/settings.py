"""
Application settings using Pydantic BaseSettings.

This module provides centralized configuration management with:
- Environment variable loading
- Type validation
- Sensible defaults
- Environment-specific overrides

RULES:
1. All sensitive values from Secret Manager or env vars - NEVER hard-coded
2. Validate all settings on startup
3. Use environment-specific defaults where appropriate
4. Cache settings instance for performance
"""

from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


# =============================================================================
# Base Settings
# =============================================================================


class Settings(BaseSettings):
    """
    Application settings loaded from environment variables.

    Settings are loaded in this order (later overrides earlier):
    1. Default values defined here
    2. .env file (if exists)
    3. Environment variables
    4. Explicit constructor arguments

    Environment Variable Naming:
    - All settings can be set via environment variables
    - Use uppercase with underscores: DATABASE_URL, GCP_PROJECT_ID
    - Nested settings use double underscore: DATABASE__POOL_SIZE
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        env_nested_delimiter="__",
        extra="ignore",
    )

    # =========================================================================
    # Environment Configuration
    # =========================================================================

    environment: Literal["dev", "qa", "prod"] = Field(
        default="dev",
        description="Deployment environment",
    )
    debug: bool = Field(
        default=False,
        description="Enable debug mode",
    )
    service_name: str = Field(
        default="data-agent",
        description="Service name for logging and tracing",
    )
    service_version: str = Field(
        default="1.0.0",
        description="Service version",
    )

    # =========================================================================
    # GCP Configuration
    # =========================================================================

    gcp_project_id: str = Field(
        default="",
        description="GCP project ID",
    )
    gcp_region: str = Field(
        default="us-central1",
        description="GCP region for resources",
    )
    gcp_zone: str = Field(
        default="us-central1-a",
        description="GCP zone for resources",
    )

    # =========================================================================
    # Database Configuration
    # =========================================================================

    database_url: SecretStr = Field(
        default=SecretStr("postgresql://localhost:5432/data_agent"),
        description="PostgreSQL connection string",
    )
    database_pool_size: int = Field(
        default=5,
        ge=1,
        le=20,
        description="Database connection pool size",
    )
    database_max_overflow: int = Field(
        default=10,
        ge=0,
        le=30,
        description="Maximum pool overflow connections",
    )
    database_pool_timeout: int = Field(
        default=30,
        ge=5,
        description="Pool connection timeout in seconds",
    )
    database_echo: bool = Field(
        default=False,
        description="Echo SQL statements (debug only)",
    )

    # =========================================================================
    # Kafka Configuration (Event Bus - System of Record)
    # =========================================================================

    kafka_bootstrap_servers: str = Field(
        default="localhost:29092",
        description="Kafka bootstrap servers",
    )
    kafka_consumer_group: str = Field(
        default="data-agent-consumer",
        description="Kafka consumer group ID",
    )
    kafka_pipeline_requested_topic: str = Field(
        default="pipeline.requested",
        description="Topic for incoming pipeline requests",
    )
    kafka_pipeline_completed_topic: str = Field(
        default="pipeline.completed",
        description="Topic for pipeline completion events",
    )
    kafka_pipeline_failed_topic: str = Field(
        default="pipeline.failed",
        description="Topic for pipeline failure events",
    )

    # =========================================================================
    # Local Airflow Configuration (Docker deployment)
    # =========================================================================

    airflow_api_url: str = Field(
        default="http://localhost:8083",
        description="Airflow REST API URL",
    )
    airflow_username: str = Field(
        default="admin",
        description="Airflow username for API auth",
    )
    airflow_password: SecretStr = Field(
        default=SecretStr("admin123"),
        description="Airflow password for API auth",
    )
    airflow_dags_folder: str = Field(
        default="/opt/airflow/dags",
        description="Path to Airflow DAGs folder (mounted volume)",
    )
    airflow_spark_folder: str = Field(
        default="/opt/airflow/dags/spark",
        description="Path to Spark jobs folder",
    )

    # =========================================================================
    # DEPRECATED: Pub/Sub Configuration (use Kafka instead)
    # Kept for backwards compatibility - will be removed in v2.0
    # =========================================================================

    pubsub_project_id: Optional[str] = Field(
        default=None,
        description="[DEPRECATED] Use Kafka. Pub/Sub project ID",
    )
    pubsub_intent_topic: str = Field(
        default="data-agent-intents",
        description="[DEPRECATED] Use Kafka. Topic for incoming pipeline intents",
    )
    pubsub_intent_subscription: str = Field(
        default="data-agent-intents-sub",
        description="[DEPRECATED] Use Kafka. Subscription for intent topic",
    )
    pubsub_status_topic: str = Field(
        default="data-agent-status",
        description="[DEPRECATED] Use Kafka. Topic for status updates",
    )
    pubsub_max_messages: int = Field(
        default=10,
        ge=1,
        le=100,
        description="[DEPRECATED] Use Kafka. Max messages to pull at once",
    )
    pubsub_ack_deadline: int = Field(
        default=600,
        ge=10,
        le=600,
        description="[DEPRECATED] Use Kafka. Message ack deadline in seconds",
    )

    # =========================================================================
    # LLM Configuration
    # =========================================================================

    llm_provider: Literal["openai", "anthropic", "google"] = Field(
        default="openai",
        description="LLM provider to use",
    )
    llm_model: str = Field(
        default="gpt-4-turbo-preview",
        description="LLM model identifier",
    )
    llm_temperature: float = Field(
        default=0.0,
        ge=0.0,
        le=2.0,
        description="LLM temperature (0 = deterministic)",
    )
    llm_max_tokens: int = Field(
        default=4096,
        ge=100,
        le=128000,
        description="Maximum tokens for LLM response",
    )
    llm_timeout: int = Field(
        default=120,
        ge=10,
        description="LLM request timeout in seconds",
    )

    # API Keys (loaded from environment or Secret Manager)
    openai_api_key: Optional[SecretStr] = Field(
        default=None,
        description="OpenAI API key",
    )
    anthropic_api_key: Optional[SecretStr] = Field(
        default=None,
        description="Anthropic API key",
    )
    google_api_key: Optional[SecretStr] = Field(
        default=None,
        description="Google AI API key",
    )

    # =========================================================================
    # DEPRECATED: Cloud Composer Configuration (use Local Airflow instead)
    # For GCP production, use the GitHub Actions workflow to sync to Composer
    # =========================================================================

    composer_environment: str = Field(
        default="",
        description="[DEPRECATED] Cloud Composer environment name",
    )
    composer_location: Optional[str] = Field(
        default=None,
        description="[DEPRECATED] Composer location (defaults to gcp_region)",
    )
    composer_dags_bucket: str = Field(
        default="",
        description="[DEPRECATED] GCS bucket for Composer DAGs",
    )

    # =========================================================================
    # Git Configuration
    # =========================================================================

    git_repo_url: str = Field(
        default="",
        description="Git repository URL",
    )
    git_branch_prefix: str = Field(
        default="data-agent/",
        description="Prefix for generated branch names",
    )
    git_main_branch: str = Field(
        default="main",
        description="Main branch name",
    )
    git_committer_name: str = Field(
        default="Data Agent",
        description="Git committer name",
    )
    git_committer_email: str = Field(
        default="data-agent@example.com",
        description="Git committer email",
    )

    # =========================================================================
    # GitHub Configuration (for artifact deployment)
    # =========================================================================
    # Uses enterprise-data-pipelines repo for DAG and Spark job deployment
    # Environment variables: GITHUB_PIPELINES_OWNER, GITHUB_PIPELINES_REPO, GITHUB_PIPELINES_TOKEN

    github_owner: str = Field(
        default="sam2881",
        description="GitHub owner/organization for pipeline artifacts",
        validation_alias="GITHUB_PIPELINES_OWNER",
    )
    github_repo: str = Field(
        default="enterprise-data-pipelines",
        description="GitHub repository name for pipeline artifacts",
        validation_alias="GITHUB_PIPELINES_REPO",
    )
    github_token: Optional[SecretStr] = Field(
        default=None,
        description="GitHub personal access token",
        validation_alias="GITHUB_PIPELINES_TOKEN",
    )
    git_clear_repo_before_push: bool = Field(
        default=True,
        description="Delete all existing files in repo before pushing new artifacts",
    )

    # =========================================================================
    # Jira Configuration
    # =========================================================================

    jira_base_url: str = Field(
        default="",
        description="Jira base URL",
    )
    jira_project_key: str = Field(
        default="",
        description="Jira project key for tickets",
    )
    jira_api_token: Optional[SecretStr] = Field(
        default=None,
        description="Jira API token",
    )
    jira_username: str = Field(
        default="",
        description="Jira username for API auth",
    )

    # =========================================================================
    # BigQuery Configuration
    # =========================================================================

    bigquery_project_id: Optional[str] = Field(
        default=None,
        description="BigQuery project ID (defaults to gcp_project_id)",
    )
    bigquery_dataset_location: str = Field(
        default="US",
        description="BigQuery dataset location",
    )
    bigquery_default_dataset: str = Field(
        default="data_warehouse",
        description="Default BigQuery dataset",
    )

    # =========================================================================
    # GCS Configuration
    # =========================================================================

    gcs_landing_bucket: str = Field(
        default="",
        description="GCS bucket for landing zone",
    )
    gcs_bronze_bucket: str = Field(
        default="",
        description="GCS bucket for bronze layer",
    )
    gcs_silver_bucket: str = Field(
        default="",
        description="GCS bucket for silver layer",
    )
    gcs_gold_bucket: str = Field(
        default="",
        description="GCS bucket for gold layer",
    )
    gcs_artifacts_bucket: str = Field(
        default="",
        description="GCS bucket for generated artifacts",
    )

    # =========================================================================
    # Logging Configuration
    # =========================================================================

    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(
        default="INFO",
        description="Log level",
    )
    log_format: Literal["json", "console"] = Field(
        default="json",
        description="Log output format",
    )
    log_include_timestamp: bool = Field(
        default=True,
        description="Include timestamp in logs",
    )

    # =========================================================================
    # Timeout Configuration
    # =========================================================================

    default_timeout_seconds: int = Field(
        default=3600,
        ge=60,
        description="Default operation timeout",
    )
    step_timeout_seconds: int = Field(
        default=600,
        ge=30,
        description="Individual step timeout",
    )
    approval_timeout_seconds: int = Field(
        default=86400,
        ge=3600,
        description="Human approval timeout (default 24h)",
    )
    cicd_timeout_seconds: int = Field(
        default=1800,
        ge=300,
        description="CI/CD pipeline timeout",
    )

    # =========================================================================
    # Retry Configuration
    # =========================================================================

    max_retries: int = Field(
        default=3,
        ge=0,
        le=10,
        description="Maximum retry attempts",
    )
    retry_delay_seconds: int = Field(
        default=5,
        ge=1,
        description="Initial retry delay",
    )
    retry_backoff_multiplier: float = Field(
        default=2.0,
        ge=1.0,
        le=5.0,
        description="Retry backoff multiplier",
    )
    retry_max_delay_seconds: int = Field(
        default=60,
        ge=10,
        description="Maximum retry delay",
    )

    # =========================================================================
    # Feature Flags
    # =========================================================================

    enable_dry_run: bool = Field(
        default=False,
        description="Enable dry-run mode globally",
    )
    enable_human_approval: bool = Field(
        default=True,
        description="Require human approval for prod deployments",
    )
    enable_schema_validation: bool = Field(
        default=True,
        description="Enable strict schema validation",
    )
    enable_security_checks: bool = Field(
        default=True,
        description="Enable security validation checks",
    )
    enable_audit_logging: bool = Field(
        default=True,
        description="Enable audit logging to database",
    )

    # =========================================================================
    # Paths Configuration
    # =========================================================================

    templates_dir: Path = Field(
        default=Path(__file__).parent.parent / "templates",
        description="Path to Jinja2 templates directory",
    )
    prompts_dir: Path = Field(
        default=Path(__file__).parent.parent.parent / "prompts",
        description="Path to agent prompts directory",
    )

    # =========================================================================
    # Validators
    # =========================================================================

    @field_validator("database_url", mode="before")
    @classmethod
    def validate_database_url(cls, v: Any) -> Any:
        """Ensure database URL starts with postgresql://."""
        if isinstance(v, str) and v and not v.startswith(("postgresql://", "postgres://")):
            raise ValueError("database_url must be a PostgreSQL connection string")
        return v

    @field_validator("log_level", mode="before")
    @classmethod
    def validate_log_level(cls, v: Any) -> str:
        """Normalize log level to uppercase."""
        if isinstance(v, str):
            return v.upper()
        return v

    @model_validator(mode="after")
    def set_derived_defaults(self) -> "Settings":
        """Set derived default values."""
        # Use GCP project ID for services that don't specify their own
        if not self.pubsub_project_id:
            self.pubsub_project_id = self.gcp_project_id
        if not self.bigquery_project_id:
            self.bigquery_project_id = self.gcp_project_id
        if not self.composer_location:
            self.composer_location = self.gcp_region
        return self

    # =========================================================================
    # Helper Methods
    # =========================================================================

    def get_database_url_str(self) -> str:
        """Get database URL as string (for SQLAlchemy)."""
        return self.database_url.get_secret_value()

    def get_airflow_password(self) -> str:
        """Get Airflow password as string."""
        return self.airflow_password.get_secret_value()

    def get_llm_api_key(self) -> Optional[str]:
        """Get the appropriate LLM API key based on provider."""
        if self.llm_provider == "openai" and self.openai_api_key:
            return self.openai_api_key.get_secret_value()
        elif self.llm_provider == "anthropic" and self.anthropic_api_key:
            return self.anthropic_api_key.get_secret_value()
        elif self.llm_provider == "google" and self.google_api_key:
            return self.google_api_key.get_secret_value()
        return None

    def is_production(self) -> bool:
        """Check if running in production environment."""
        return self.environment == "prod"

    def requires_approval(self) -> bool:
        """Check if human approval is required for current environment."""
        return self.is_production() and self.enable_human_approval

    def to_safe_dict(self) -> Dict[str, Any]:
        """
        Convert settings to dict with secrets masked.

        Safe for logging and debugging.
        """
        data = {}
        for field_name, field_info in self.model_fields.items():
            value = getattr(self, field_name)
            if isinstance(value, SecretStr):
                data[field_name] = "***REDACTED***"
            elif isinstance(value, Path):
                data[field_name] = str(value)
            else:
                data[field_name] = value
        return data


# =============================================================================
# Environment-Specific Settings
# =============================================================================


class DevSettings(Settings):
    """Development environment settings with relaxed defaults."""

    environment: Literal["dev", "qa", "prod"] = "dev"
    debug: bool = True
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "DEBUG"
    log_format: Literal["json", "console"] = "console"
    enable_human_approval: bool = False
    database_echo: bool = True


class QASettings(Settings):
    """QA environment settings."""

    environment: Literal["dev", "qa", "prod"] = "qa"
    debug: bool = False
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    enable_human_approval: bool = True


class ProdSettings(Settings):
    """Production environment settings with strict defaults."""

    environment: Literal["dev", "qa", "prod"] = "prod"
    debug: bool = False
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    log_format: Literal["json", "console"] = "json"
    enable_human_approval: bool = True
    enable_security_checks: bool = True
    database_echo: bool = False


# =============================================================================
# Settings Factory
# =============================================================================


@lru_cache
def get_settings() -> Settings:
    """
    Get cached settings instance.

    Settings are loaded once and cached for the lifetime of the application.
    The environment is determined by the ENVIRONMENT variable.

    Returns:
        Configured Settings instance

    Example:
        settings = get_settings()
        print(f"Running in {settings.environment}")
    """
    import os

    env = os.getenv("ENVIRONMENT", "dev").lower()

    if env == "prod":
        return ProdSettings()
    elif env == "qa":
        return QASettings()
    else:
        return DevSettings()


def clear_settings_cache() -> None:
    """Clear the settings cache (useful for testing)."""
    get_settings.cache_clear()
