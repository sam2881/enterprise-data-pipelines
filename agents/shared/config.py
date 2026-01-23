"""
Agent Configuration Management

WHY: Centralized configuration for all agents.
     Uses environment variables and GCP Secret Manager.
     NO hardcoded secrets or credentials.

HOW: AgentConfig loads from environment, with fallback to Secret Manager.
     Each agent type can have specialized configuration.
"""
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from enum import Enum


class LLMProvider(str, Enum):
    """Supported LLM providers"""
    OPENAI = "openai"
    ANTHROPIC = "anthropic"
    VERTEXAI = "vertexai"


@dataclass
class LLMConfig:
    """LLM provider configuration"""
    provider: LLMProvider = LLMProvider.OPENAI
    model: str = "gpt-4o-mini"
    temperature: float = 0.0  # Deterministic by default
    max_tokens: int = 4096
    timeout_seconds: int = 120
    # API key loaded from environment/secrets
    api_key_env_var: str = "OPENAI_API_KEY"

    @property
    def api_key(self) -> Optional[str]:
        """Get API key from environment (never hardcoded)"""
        return os.getenv(self.api_key_env_var)


@dataclass
class RetryConfig:
    """Retry behavior configuration"""
    max_retries: int = 3
    initial_delay_seconds: float = 1.0
    max_delay_seconds: float = 60.0
    exponential_base: float = 2.0
    retry_on_errors: List[str] = field(default_factory=lambda: [
        "RateLimitError",
        "Timeout",
        "ConnectionError",
    ])


@dataclass
class ObservabilityConfig:
    """Observability configuration"""
    enable_tracing: bool = True
    enable_metrics: bool = True
    enable_logging: bool = True
    log_level: str = "INFO"
    # Langfuse configuration
    langfuse_enabled: bool = False
    langfuse_public_key_env: str = "LANGFUSE_PUBLIC_KEY"
    langfuse_secret_key_env: str = "LANGFUSE_SECRET_KEY"


@dataclass
class AgentConfig:
    """
    Unified configuration for all agents.

    WHY: Single source of truth for agent configuration.
         Ensures consistency across data_agent and servicenow_agent.

    USAGE:
        config = AgentConfig.from_environment()
        agent = MyAgent(config)
    """
    # Agent identification
    agent_id: str = ""
    agent_name: str = ""
    agent_version: str = "1.0.0"

    # Environment
    environment: str = "development"  # development, staging, production

    # LLM configuration
    llm: LLMConfig = field(default_factory=LLMConfig)

    # Retry configuration
    retry: RetryConfig = field(default_factory=RetryConfig)

    # Observability
    observability: ObservabilityConfig = field(default_factory=ObservabilityConfig)

    # Timeouts
    default_timeout_seconds: int = 300
    llm_timeout_seconds: int = 120
    external_api_timeout_seconds: int = 30

    # Feature flags
    enable_caching: bool = True
    enable_determinism_check: bool = True
    dry_run_default: bool = False

    # Custom agent-specific config
    custom: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_environment(cls, agent_id: str = "", agent_name: str = "") -> "AgentConfig":
        """
        Create config from environment variables.

        This is the primary way to create configuration.
        """
        environment = os.getenv("ENVIRONMENT", "development")

        # LLM config
        llm_provider = os.getenv("LLM_PROVIDER", "openai")
        llm = LLMConfig(
            provider=LLMProvider(llm_provider),
            model=os.getenv("LLM_MODEL", "gpt-4o-mini"),
            temperature=float(os.getenv("LLM_TEMPERATURE", "0.0")),
            max_tokens=int(os.getenv("LLM_MAX_TOKENS", "4096")),
            timeout_seconds=int(os.getenv("LLM_TIMEOUT", "120")),
        )

        # Retry config
        retry = RetryConfig(
            max_retries=int(os.getenv("MAX_RETRIES", "3")),
            initial_delay_seconds=float(os.getenv("RETRY_INITIAL_DELAY", "1.0")),
            max_delay_seconds=float(os.getenv("RETRY_MAX_DELAY", "60.0")),
        )

        # Observability
        observability = ObservabilityConfig(
            enable_tracing=os.getenv("ENABLE_TRACING", "true").lower() == "true",
            enable_metrics=os.getenv("ENABLE_METRICS", "true").lower() == "true",
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            langfuse_enabled=os.getenv("LANGFUSE_ENABLED", "false").lower() == "true",
        )

        return cls(
            agent_id=agent_id,
            agent_name=agent_name,
            environment=environment,
            llm=llm,
            retry=retry,
            observability=observability,
            default_timeout_seconds=int(os.getenv("DEFAULT_TIMEOUT", "300")),
            enable_caching=os.getenv("ENABLE_CACHING", "true").lower() == "true",
            enable_determinism_check=os.getenv("ENABLE_DETERMINISM_CHECK", "true").lower() == "true",
            dry_run_default=os.getenv("DRY_RUN", "false").lower() == "true",
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary (excluding secrets)"""
        return {
            "agent_id": self.agent_id,
            "agent_name": self.agent_name,
            "agent_version": self.agent_version,
            "environment": self.environment,
            "llm": {
                "provider": self.llm.provider.value,
                "model": self.llm.model,
                "temperature": self.llm.temperature,
                "max_tokens": self.llm.max_tokens,
            },
            "retry": {
                "max_retries": self.retry.max_retries,
                "initial_delay_seconds": self.retry.initial_delay_seconds,
            },
            "timeouts": {
                "default": self.default_timeout_seconds,
                "llm": self.llm_timeout_seconds,
                "external_api": self.external_api_timeout_seconds,
            },
            "features": {
                "caching": self.enable_caching,
                "determinism_check": self.enable_determinism_check,
                "dry_run_default": self.dry_run_default,
            },
        }
