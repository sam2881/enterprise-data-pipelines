"""
Configuration management for the Data Engineering Platform.

This module provides:
- Pydantic BaseSettings for environment configuration
- Environment-specific settings (dev, qa, prod)
- Cached settings access

Exports:
    - Settings: Base settings class
    - DevSettings: Development environment settings
    - QASettings: QA environment settings
    - ProdSettings: Production environment settings
    - get_settings: Get cached settings instance
    - clear_settings_cache: Clear settings cache (for testing)
"""

from src.config.settings import (
    Settings,
    DevSettings,
    QASettings,
    ProdSettings,
    get_settings,
    clear_settings_cache,
)

__all__ = [
    "Settings",
    "DevSettings",
    "QASettings",
    "ProdSettings",
    "get_settings",
    "clear_settings_cache",
]
