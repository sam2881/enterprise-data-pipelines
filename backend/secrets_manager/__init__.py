"""
Secrets Management Module

Provides secure access to credentials via GCP Secret Manager.
Falls back to environment variables for local development.
"""
from .manager import SecretManager, get_secret_manager, get_secret, Environment

__all__ = ["SecretManager", "get_secret_manager", "get_secret", "Environment"]
