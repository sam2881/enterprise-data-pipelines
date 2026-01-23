"""
State management models for LangGraph workflows.

This module exports all state-related models for the pipeline workflow.

Models are organized into three categories:
1. Pydantic Models (pipeline_state.py) - Type-safe data validation
2. TypedDict State (agent_state.py) - LangGraph workflow state
3. Execution Context (execution_context.py) - Runtime transient state
"""

# =============================================================================
# Enums
# =============================================================================

from src.state.pipeline_state import (
    SourceType,
    ProcessingMode,
    Environment,
    PipelineAction,
    DataSensitivity,
    ChangeType,
    DeploymentStatus,
    WorkflowPhase,
)

# =============================================================================
# Schema Models
# =============================================================================

from src.state.pipeline_state import (
    ColumnDefinition,
    SchemaDefinition,
    SchemaChange,
)

# =============================================================================
# Source Configuration Models
# =============================================================================

from src.state.pipeline_state import (
    FileSourceConfig,
    DatabaseSourceConfig,
    StreamingSourceConfig,
    APISourceConfig,
    SourceConfig,
)

# =============================================================================
# Target Configuration Models
# =============================================================================

from src.state.pipeline_state import (
    BigQueryTargetConfig,
    GCSTargetConfig,
    TargetConfig,
)

# =============================================================================
# Rules Models
# =============================================================================

from src.state.pipeline_state import (
    TransformationRule,
    DataQualityRule,
    ExecutionPolicy,
)

# =============================================================================
# Identity and Intent Models
# =============================================================================

from src.state.pipeline_state import (
    PipelineIdentity,
    IntentSchema,
)

# =============================================================================
# Agent Output Models
# =============================================================================

from src.state.pipeline_state import (
    TemplateSelection,
    SchemaPlan,
    PlannerOutput,
    GeneratedArtifact,
    GeneratorOutput,
    ValidationResult,
    ValidatorOutput,
    DeployerOutput,
)

# =============================================================================
# Error and Context Models
# =============================================================================

from src.state.pipeline_state import (
    WorkflowError,
    MetadataContext,
)

# =============================================================================
# LangGraph State (TypedDict)
# =============================================================================

from src.state.agent_state import (
    # Main state
    AgentState,
    CheckpointState,
    WorkflowPhaseType,
    # Reducer functions
    replace_value,
    append_to_list,
    merge_dict,
    # Node I/O types
    ValidateIntentInput,
    ValidateIntentOutput,
    PlanPipelineInput,
    PlanPipelineOutput,
    GenerateArtifactsInput,
    GenerateArtifactsOutput,
    ValidateArtifactsInput,
    ValidateArtifactsOutput,
    DeployArtifactsInput,
    DeployArtifactsOutput,
    # Factory functions
    create_initial_state,
    create_error_state,
    create_success_state,
)

# =============================================================================
# Execution Context
# =============================================================================

from src.state.execution_context import (
    LogLevel,
    MetricType,
    StepResult,
    Metric,
    ExecutionContext,
    create_execution_context,
    timed,
)

# =============================================================================
# Public API
# =============================================================================

__all__ = [
    # Enums
    "SourceType",
    "ProcessingMode",
    "Environment",
    "PipelineAction",
    "DataSensitivity",
    "ChangeType",
    "DeploymentStatus",
    "WorkflowPhase",
    # Schema
    "ColumnDefinition",
    "SchemaDefinition",
    "SchemaChange",
    # Source Config
    "FileSourceConfig",
    "DatabaseSourceConfig",
    "StreamingSourceConfig",
    "APISourceConfig",
    "SourceConfig",
    # Target Config
    "BigQueryTargetConfig",
    "GCSTargetConfig",
    "TargetConfig",
    # Rules
    "TransformationRule",
    "DataQualityRule",
    "ExecutionPolicy",
    # Identity
    "PipelineIdentity",
    "IntentSchema",
    # Agent Outputs
    "TemplateSelection",
    "SchemaPlan",
    "PlannerOutput",
    "GeneratedArtifact",
    "GeneratorOutput",
    "ValidationResult",
    "ValidatorOutput",
    "DeployerOutput",
    # Error/Context
    "WorkflowError",
    "MetadataContext",
    # LangGraph State
    "AgentState",
    "CheckpointState",
    "WorkflowPhaseType",
    "replace_value",
    "append_to_list",
    "merge_dict",
    "ValidateIntentInput",
    "ValidateIntentOutput",
    "PlanPipelineInput",
    "PlanPipelineOutput",
    "GenerateArtifactsInput",
    "GenerateArtifactsOutput",
    "ValidateArtifactsInput",
    "ValidateArtifactsOutput",
    "DeployArtifactsInput",
    "DeployArtifactsOutput",
    "create_initial_state",
    "create_error_state",
    "create_success_state",
    # Execution Context
    "LogLevel",
    "MetricType",
    "StepResult",
    "Metric",
    "ExecutionContext",
    "create_execution_context",
    "timed",
]
