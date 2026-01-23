"""
GCP Secret Manager Integration - Production Secrets Management

WHY: Centralizes all secret access through GCP Secret Manager.
     Eliminates hardcoded credentials from source code.
     Supports local development with fallback to environment variables.

PATTERN: Singleton pattern for secret caching with TTL.
         Secrets are loaded once and cached for performance.

USAGE:
    from secrets.manager import SecretManager

    secrets = SecretManager()
    api_key = secrets.get("OPENAI_API_KEY")
"""
import os
import logging
from typing import Any, Dict, Optional
from functools import lru_cache
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)


class Environment(str, Enum):
    """Deployment environments."""
    LOCAL = "local"
    DEV = "dev"
    STAGING = "staging"
    PROD = "prod"


@dataclass
class SecretConfig:
    """Configuration for a secret."""
    name: str
    gcp_secret_id: str
    required: bool = True
    default: Optional[str] = None


@dataclass
class CachedSecret:
    """Cached secret with TTL."""
    value: str
    expires_at: datetime


class SecretManager:
    """
    Production-grade secrets manager using GCP Secret Manager.

    SECURITY:
    - All secrets fetched from GCP Secret Manager in production
    - Falls back to environment variables for local development
    - Secrets cached with configurable TTL (default 1 hour)
    - Never logs secret values

    CONFIGURATION:
    - GCP_PROJECT_ID: Required for Secret Manager access
    - ENVIRONMENT: local/dev/staging/prod (affects secret prefix)
    - SECRET_CACHE_TTL_SECONDS: Cache TTL (default 3600)
    """

    # Secret definitions - maps logical names to GCP Secret IDs
    SECRET_DEFINITIONS: Dict[str, SecretConfig] = {
        # LLM Providers
        "OPENAI_API_KEY": SecretConfig(
            name="OPENAI_API_KEY",
            gcp_secret_id="openai-api-key",
            required=True
        ),
        "ANTHROPIC_API_KEY": SecretConfig(
            name="ANTHROPIC_API_KEY",
            gcp_secret_id="anthropic-api-key",
            required=True
        ),

        # Observability
        "LANGFUSE_PUBLIC_KEY": SecretConfig(
            name="LANGFUSE_PUBLIC_KEY",
            gcp_secret_id="langfuse-public-key",
            required=False
        ),
        "LANGFUSE_SECRET_KEY": SecretConfig(
            name="LANGFUSE_SECRET_KEY",
            gcp_secret_id="langfuse-secret-key",
            required=False
        ),

        # ServiceNow
        "SNOW_INSTANCE_URL": SecretConfig(
            name="SNOW_INSTANCE_URL",
            gcp_secret_id="servicenow-instance-url",
            required=True
        ),
        "SNOW_USERNAME": SecretConfig(
            name="SNOW_USERNAME",
            gcp_secret_id="servicenow-username",
            required=True
        ),
        "SNOW_PASSWORD": SecretConfig(
            name="SNOW_PASSWORD",
            gcp_secret_id="servicenow-password",
            required=True
        ),
        "SNOW_API_KEY": SecretConfig(
            name="SNOW_API_KEY",
            gcp_secret_id="servicenow-api-key",
            required=False
        ),
        "SNOW_CLIENT_ID": SecretConfig(
            name="SNOW_CLIENT_ID",
            gcp_secret_id="servicenow-client-id",
            required=False
        ),
        "SNOW_CLIENT_SECRET": SecretConfig(
            name="SNOW_CLIENT_SECRET",
            gcp_secret_id="servicenow-client-secret",
            required=False
        ),

        # Jira
        "JIRA_URL": SecretConfig(
            name="JIRA_URL",
            gcp_secret_id="jira-url",
            required=True
        ),
        "JIRA_USERNAME": SecretConfig(
            name="JIRA_USERNAME",
            gcp_secret_id="jira-username",
            required=True
        ),
        "JIRA_API_TOKEN": SecretConfig(
            name="JIRA_API_TOKEN",
            gcp_secret_id="jira-api-token",
            required=True
        ),

        # GitHub
        "GITHUB_TOKEN": SecretConfig(
            name="GITHUB_TOKEN",
            gcp_secret_id="github-token",
            required=True
        ),
        "GITHUB_ORG": SecretConfig(
            name="GITHUB_ORG",
            gcp_secret_id="github-org",
            required=True
        ),
        "GITHUB_REPO": SecretConfig(
            name="GITHUB_REPO",
            gcp_secret_id="github-repo",
            required=True
        ),

        # Database
        "POSTGRES_PASSWORD": SecretConfig(
            name="POSTGRES_PASSWORD",
            gcp_secret_id="postgres-password",
            required=True,
            default="admin123"
        ),
        "NEO4J_PASSWORD": SecretConfig(
            name="NEO4J_PASSWORD",
            gcp_secret_id="neo4j-password",
            required=True,
            default="adminadmin"
        ),

        # Slack
        "SLACK_BOT_TOKEN": SecretConfig(
            name="SLACK_BOT_TOKEN",
            gcp_secret_id="slack-bot-token",
            required=False
        ),
        "SLACK_CHANNEL": SecretConfig(
            name="SLACK_CHANNEL",
            gcp_secret_id="slack-channel",
            required=False,
            default="#ai-agent-alerts"
        ),
    }

    _instance: Optional["SecretManager"] = None
    _cache: Dict[str, CachedSecret] = field(default_factory=dict)

    def __new__(cls) -> "SecretManager":
        """Singleton pattern - only one instance."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        """Initialize the secret manager."""
        if self._initialized:
            return

        self._cache: Dict[str, CachedSecret] = {}
        self._gcp_client = None
        self._project_id = os.environ.get("GCP_PROJECT_ID", "agent-ai-test-461120")
        self._environment = Environment(os.environ.get("ENVIRONMENT", "local"))
        self._cache_ttl = int(os.environ.get("SECRET_CACHE_TTL_SECONDS", 3600))

        # Initialize GCP client for non-local environments
        if self._environment != Environment.LOCAL:
            self._init_gcp_client()

        self._initialized = True
        logger.info(f"SecretManager initialized for environment: {self._environment}")

    def _init_gcp_client(self) -> None:
        """Initialize GCP Secret Manager client."""
        try:
            from google.cloud import secretmanager
            self._gcp_client = secretmanager.SecretManagerServiceClient()
            logger.info("GCP Secret Manager client initialized")
        except ImportError:
            logger.warning(
                "google-cloud-secret-manager not installed. "
                "Falling back to environment variables."
            )
        except Exception as e:
            logger.error(f"Failed to initialize GCP Secret Manager: {e}")

    def get(self, secret_name: str, default: Optional[str] = None) -> Optional[str]:
        """
        Get a secret value.

        Args:
            secret_name: Logical secret name (e.g., "OPENAI_API_KEY")
            default: Default value if secret not found

        Returns:
            Secret value or default

        Raises:
            ValueError: If required secret is not found
        """
        # Check cache first
        if secret_name in self._cache:
            cached = self._cache[secret_name]
            if datetime.utcnow() < cached.expires_at:
                return cached.value
            else:
                del self._cache[secret_name]

        # Get secret config
        config = self.SECRET_DEFINITIONS.get(secret_name)
        if not config:
            # Unknown secret - try environment variable
            value = os.environ.get(secret_name, default)
            if value:
                self._cache_secret(secret_name, value)
            return value

        # Try to get from GCP Secret Manager
        value = self._get_from_gcp(config)

        # Fallback to environment variable
        if value is None:
            value = os.environ.get(secret_name)

        # Use config default
        if value is None:
            value = config.default

        # Use provided default
        if value is None:
            value = default

        # Check if required
        if value is None and config.required:
            raise ValueError(
                f"Required secret '{secret_name}' not found. "
                f"Set it in GCP Secret Manager or as environment variable."
            )

        # Cache the value
        if value is not None:
            self._cache_secret(secret_name, value)

        return value

    def _get_from_gcp(self, config: SecretConfig) -> Optional[str]:
        """Get secret from GCP Secret Manager."""
        if self._gcp_client is None:
            return None

        try:
            # Build secret name with environment prefix
            secret_id = f"{self._environment.value}-{config.gcp_secret_id}"
            name = f"projects/{self._project_id}/secrets/{secret_id}/versions/latest"

            response = self._gcp_client.access_secret_version(request={"name": name})
            return response.payload.data.decode("UTF-8")

        except Exception as e:
            # Try without environment prefix for shared secrets
            try:
                name = f"projects/{self._project_id}/secrets/{config.gcp_secret_id}/versions/latest"
                response = self._gcp_client.access_secret_version(request={"name": name})
                return response.payload.data.decode("UTF-8")
            except Exception:
                logger.debug(f"Secret not found in GCP: {config.name}")
                return None

    def _cache_secret(self, name: str, value: str) -> None:
        """Cache a secret value with TTL."""
        expires_at = datetime.utcnow() + timedelta(seconds=self._cache_ttl)
        self._cache[name] = CachedSecret(value=value, expires_at=expires_at)

    def clear_cache(self) -> None:
        """Clear the secret cache (useful for rotation)."""
        self._cache.clear()
        logger.info("Secret cache cleared")

    def get_all(self, prefix: str = None) -> Dict[str, str]:
        """
        Get all secrets, optionally filtered by prefix.

        WARNING: Only use for debugging/initialization, not in production code paths.
        """
        result = {}
        for name, config in self.SECRET_DEFINITIONS.items():
            if prefix and not name.startswith(prefix):
                continue
            try:
                value = self.get(name)
                if value:
                    result[name] = value
            except ValueError:
                pass  # Skip required secrets that aren't set
        return result

    @property
    def environment(self) -> Environment:
        """Get current environment."""
        return self._environment

    @property
    def project_id(self) -> str:
        """Get GCP project ID."""
        return self._project_id


# Convenience function for quick access
@lru_cache(maxsize=1)
def get_secret_manager() -> SecretManager:
    """Get the singleton SecretManager instance."""
    return SecretManager()


def get_secret(name: str, default: Optional[str] = None) -> Optional[str]:
    """Convenience function to get a secret."""
    return get_secret_manager().get(name, default)
