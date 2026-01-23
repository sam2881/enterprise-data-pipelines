"""
Shared Utilities - Platform Layer

WHY: Common utilities used by agents, backend, and other modules.
     Avoids duplication and ensures consistent behavior.

WHAT'S HERE:
- Logging: Structured logging setup
- Hashing: Determinism tracking
- Config: Environment loading
- Secrets: GCP Secret Manager (wrapper)

USAGE:
    from platform.utils import get_logger, compute_hash
"""
import hashlib
import json
import os
import logging
from typing import Any, Dict, Optional
import structlog


def get_logger(name: str = __name__) -> structlog.stdlib.BoundLogger:
    """
    Get a structured logger.

    Args:
        name: Logger name (usually __name__)

    Returns:
        Configured structlog logger
    """
    return structlog.get_logger(name)


def compute_hash(data: Any) -> str:
    """
    Compute deterministic hash of data.

    Args:
        data: Any JSON-serializable data

    Returns:
        16-character hex hash
    """
    json_str = json.dumps(data, sort_keys=True, default=str)
    return hashlib.sha256(json_str.encode()).hexdigest()[:16]


def get_env(key: str, default: Optional[str] = None, required: bool = False) -> Optional[str]:
    """
    Get environment variable with validation.

    Args:
        key: Environment variable name
        default: Default value if not set
        required: Raise if not set and no default

    Returns:
        Environment variable value

    Raises:
        ValueError: If required and not set
    """
    value = os.getenv(key, default)
    if required and value is None:
        raise ValueError(f"Required environment variable {key} not set")
    return value


def get_secret(secret_name: str, project_id: Optional[str] = None) -> Optional[str]:
    """
    Get secret from GCP Secret Manager.

    Falls back to environment variable if Secret Manager unavailable.

    Args:
        secret_name: Secret identifier
        project_id: GCP project (defaults to GCP_PROJECT_ID env var)

    Returns:
        Secret value or None
    """
    # First try environment variable (for local dev)
    env_value = os.getenv(secret_name)
    if env_value:
        return env_value

    # Try GCP Secret Manager
    try:
        from google.cloud import secretmanager

        project = project_id or os.getenv("GCP_PROJECT_ID")
        if not project:
            return None

        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project}/secrets/{secret_name}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")

    except Exception:
        return None


__all__ = [
    "get_logger",
    "compute_hash",
    "get_env",
    "get_secret",
]
