"""
Data Quality Package.

Provides enterprise-grade data quality validation using:
- Great Expectations integration
- Metadata-driven quality rules
- Quality score calculation (Airbnb Midas pattern)
- PySpark validation code generation

Components:
- GreatExpectationsManager: Manages GE context, suites, checkpoints
- ExpectationBuilder: Converts metadata rules to GE expectations
- QualityScoreCalculator: Weighted quality score calculation
- GESparkCodeGenerator: Generates standalone Spark validation jobs
"""

from .great_expectations_integration import (
    # Enums
    ExpectationType,
    QualitySeverity,
    ValidationOutcome,
    # Data classes
    ExpectationConfig,
    ExpectationSuiteConfig,
    ValidationResult,
    CheckpointResult,
    # Classes
    ExpectationBuilder,
    QualityScoreCalculator,
    GreatExpectationsManager,
    GESparkCodeGenerator,
    # Functions
    create_quality_validator,
    validate_dataframe_quality,
    # Constants
    RULE_TYPE_TO_GE_EXPECTATION,
)

__all__ = [
    # Enums
    "ExpectationType",
    "QualitySeverity",
    "ValidationOutcome",
    # Data classes
    "ExpectationConfig",
    "ExpectationSuiteConfig",
    "ValidationResult",
    "CheckpointResult",
    # Classes
    "ExpectationBuilder",
    "QualityScoreCalculator",
    "GreatExpectationsManager",
    "GESparkCodeGenerator",
    # Functions
    "create_quality_validator",
    "validate_dataframe_quality",
    # Constants
    "RULE_TYPE_TO_GE_EXPECTATION",
]
