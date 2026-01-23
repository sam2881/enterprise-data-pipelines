"""
Self-Healing Package.

Provides autonomous error detection and remediation for data pipelines.

Pattern: VIGIL (Observe → Diagnose → Remediate → Verify)

Components:
- ErrorPatternMatcher: Fast pattern-based error classification
- SelfHealingAgent: LLM-powered diagnosis and remediation
- Remediation Strategies: Pluggable handlers for different error types
"""

from .self_healing_agent import (
    # Enums
    ErrorCategory,
    RemediationAction,
    # Data classes
    ErrorContext,
    Diagnosis,
    Remediation,
    HealingResult,
    # Classes
    ErrorPatternMatcher,
    SelfHealingAgent,
    # Strategies
    RemediationStrategy,
    SchemaDriftStrategy,
    SparkResourceStrategy,
    DataQualityStrategy,
    CodeFixStrategy,
)

__all__ = [
    # Enums
    "ErrorCategory",
    "RemediationAction",
    # Data classes
    "ErrorContext",
    "Diagnosis",
    "Remediation",
    "HealingResult",
    # Classes
    "ErrorPatternMatcher",
    "SelfHealingAgent",
    # Strategies
    "RemediationStrategy",
    "SchemaDriftStrategy",
    "SparkResourceStrategy",
    "DataQualityStrategy",
    "CodeFixStrategy",
]
