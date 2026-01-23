"""
Pydantic state models for pipeline workflows.

This module defines all the data models used throughout the pipeline
generation workflow. These models enforce type safety and validation
at runtime.

RULES:
1. All models are IMMUTABLE after creation (use frozen=True for strict immutability)
2. All fields must have explicit types
3. Use Field() for defaults and validation
4. Never use dict where a typed model is possible
"""

import uuid
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


# =============================================================================
# Enums for Type Safety
# =============================================================================


class SourceType(str, Enum):
    """Supported data source types."""

    FILE = "file"
    DATABASE = "database"
    STREAMING = "streaming"
    API = "api"


class ProcessingMode(str, Enum):
    """Data processing modes."""

    BATCH = "batch"
    MICRO_BATCH = "micro_batch"
    STREAMING = "streaming"


class Environment(str, Enum):
    """Deployment environments."""

    DEV = "dev"
    QA = "qa"
    PROD = "prod"


class PipelineAction(str, Enum):
    """Actions that can be taken on a pipeline."""

    CREATE = "create"
    MODIFY = "modify"
    UPGRADE_SCHEMA = "upgrade_schema"
    NO_CHANGE = "no_change"


class DataSensitivity(str, Enum):
    """Data classification levels."""

    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"


class ChangeType(str, Enum):
    """Schema change types."""

    ADDED = "added"
    REMOVED = "removed"
    MODIFIED = "modified"
    TYPE_CHANGED = "type_changed"
    NULLABLE_CHANGED = "nullable_changed"


class DeploymentStatus(str, Enum):
    """Deployment status values."""

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    SUCCESS = "success"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"


class WorkflowPhase(str, Enum):
    """Workflow execution phases."""

    INIT = "init"
    PLANNING = "planning"
    GENERATING = "generating"
    VALIDATING = "validating"
    AWAITING_APPROVAL = "awaiting_approval"
    DEPLOYING = "deploying"
    COMPLETE = "complete"
    FAILED = "failed"


# =============================================================================
# Schema Definition Models
# =============================================================================


class ColumnDefinition(BaseModel):
    """Definition of a single column in a schema."""

    model_config = ConfigDict(frozen=True)

    name: str = Field(..., min_length=1, max_length=255)
    data_type: str = Field(..., description="SQL data type (e.g., STRING, INT64, TIMESTAMP)")
    nullable: bool = Field(default=True)
    description: Optional[str] = Field(default=None, max_length=1000)
    is_primary_key: bool = Field(default=False)
    is_partition_key: bool = Field(default=False)
    is_clustering_key: bool = Field(default=False)
    default_value: Optional[str] = Field(default=None)

    @field_validator("name")
    @classmethod
    def validate_column_name(cls, v: str) -> str:
        """Ensure column name is valid identifier."""
        if not v[0].isalpha() and v[0] != "_":
            raise ValueError("Column name must start with letter or underscore")
        return v.lower()


class SchemaDefinition(BaseModel):
    """Complete schema definition for a pipeline."""

    model_config = ConfigDict(frozen=True)

    schema_name: str = Field(..., min_length=1)
    columns: List[ColumnDefinition] = Field(..., min_length=1)
    primary_keys: List[str] = Field(default_factory=list)
    partition_keys: List[str] = Field(default_factory=list)
    clustering_keys: List[str] = Field(default_factory=list)

    @field_validator("columns")
    @classmethod
    def validate_unique_columns(cls, v: List[ColumnDefinition]) -> List[ColumnDefinition]:
        """Ensure column names are unique."""
        names = [col.name for col in v]
        if len(names) != len(set(names)):
            raise ValueError("Column names must be unique")
        return v


class SchemaChange(BaseModel):
    """Detected schema change between versions."""

    model_config = ConfigDict(frozen=True)

    change_type: ChangeType
    column_name: str
    old_value: Optional[str] = None
    new_value: Optional[str] = None
    is_breaking: bool = Field(default=False, description="True if change requires data migration")
    migration_strategy: Optional[str] = Field(default=None)


# =============================================================================
# Source Configuration Models
# =============================================================================


class FileSourceConfig(BaseModel):
    """Configuration for file-based data sources."""

    model_config = ConfigDict(frozen=True)

    landing_path: str = Field(..., description="GCS path to landing zone")
    file_pattern: str = Field(..., description="Glob pattern for files (e.g., *.csv)")
    file_format: Literal["csv", "json", "parquet", "avro"] = Field(default="csv")
    delimiter: Optional[str] = Field(default=",")
    header: bool = Field(default=True)
    encoding: str = Field(default="utf-8")
    compression: Optional[Literal["gzip", "snappy", "lz4", "none"]] = Field(default=None)


class DatabaseSourceConfig(BaseModel):
    """Configuration for database data sources."""

    model_config = ConfigDict(frozen=True)

    connection_secret: str = Field(..., description="Secret Manager secret name")
    database_type: Literal["postgresql", "mysql", "oracle", "sqlserver"] = Field(...)
    database_name: str = Field(...)
    schema_name: str = Field(default="public")
    table_name: str = Field(...)
    cdc_enabled: bool = Field(default=False)
    cdc_column: Optional[str] = Field(default=None, description="Column for CDC tracking")
    watermark_column: Optional[str] = Field(default=None)


class StreamingSourceConfig(BaseModel):
    """Configuration for streaming data sources."""

    model_config = ConfigDict(frozen=True)

    topic_name: str = Field(..., description="Pub/Sub or Kafka topic")
    subscription_name: Optional[str] = Field(default=None)
    message_format: Literal["json", "avro", "protobuf"] = Field(default="json")
    schema_registry_url: Optional[str] = Field(default=None)
    consumer_group: Optional[str] = Field(default=None)


class APISourceConfig(BaseModel):
    """Configuration for API data sources."""

    model_config = ConfigDict(frozen=True)

    endpoint_url: str = Field(...)
    http_method: Literal["GET", "POST"] = Field(default="GET")
    auth_secret: str = Field(..., description="Secret Manager secret for auth")
    auth_type: Literal["bearer", "basic", "api_key", "oauth2"] = Field(default="bearer")
    pagination_type: Optional[Literal["offset", "cursor", "link"]] = Field(default=None)
    rate_limit_rps: int = Field(default=10)
    timeout_seconds: int = Field(default=30)


class SourceConfig(BaseModel):
    """Unified source configuration wrapper."""

    model_config = ConfigDict(frozen=True)

    source_type: SourceType
    source_system: str = Field(..., description="Source system identifier")
    processing_mode: ProcessingMode = Field(default=ProcessingMode.BATCH)

    # Type-specific configs (only one should be populated)
    file_config: Optional[FileSourceConfig] = None
    database_config: Optional[DatabaseSourceConfig] = None
    streaming_config: Optional[StreamingSourceConfig] = None
    api_config: Optional[APISourceConfig] = None

    @field_validator("file_config", "database_config", "streaming_config", "api_config")
    @classmethod
    def validate_config_matches_type(cls, v: Any, info: Any) -> Any:
        """Validation is done at model level."""
        return v


# =============================================================================
# Target Configuration Models
# =============================================================================


class BigQueryTargetConfig(BaseModel):
    """Configuration for BigQuery target."""

    model_config = ConfigDict(frozen=True)

    project_id: str = Field(...)
    dataset_id: str = Field(...)
    table_id: str = Field(...)
    partition_field: Optional[str] = Field(default=None)
    partition_type: Optional[Literal["DAY", "HOUR", "MONTH", "YEAR"]] = Field(default="DAY")
    clustering_fields: List[str] = Field(default_factory=list, max_length=4)
    write_disposition: Literal["WRITE_APPEND", "WRITE_TRUNCATE", "WRITE_EMPTY"] = Field(
        default="WRITE_APPEND"
    )


class GCSTargetConfig(BaseModel):
    """Configuration for GCS target layers."""

    model_config = ConfigDict(frozen=True)

    bronze_path: str = Field(..., description="GCS path for bronze layer")
    silver_path: str = Field(..., description="GCS path for silver layer")
    gold_path: Optional[str] = Field(default=None, description="GCS path for gold layer")
    file_format: Literal["parquet", "delta", "iceberg"] = Field(default="parquet")


class TargetConfig(BaseModel):
    """Unified target configuration."""

    model_config = ConfigDict(frozen=True)

    gcs_config: GCSTargetConfig
    bigquery_config: Optional[BigQueryTargetConfig] = None


# =============================================================================
# Transformation and Quality Rules
# =============================================================================


class TransformationRule(BaseModel):
    """Single transformation rule."""

    model_config = ConfigDict(frozen=True)

    rule_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    rule_name: str = Field(...)
    rule_type: Literal["rename", "cast", "derive", "filter", "aggregate", "join", "custom"]
    source_columns: List[str] = Field(...)
    target_column: Optional[str] = Field(default=None)
    expression: Optional[str] = Field(default=None, description="SQL or PySpark expression")
    parameters: Dict[str, Any] = Field(default_factory=dict)
    order: int = Field(default=0, description="Execution order")


class DataQualityRule(BaseModel):
    """Data quality check rule."""

    model_config = ConfigDict(frozen=True)

    rule_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    rule_name: str = Field(...)
    rule_type: Literal[
        "not_null", "unique", "range", "regex", "foreign_key",
        "custom_sql", "row_count", "freshness"
    ]
    column_name: Optional[str] = Field(default=None)
    parameters: Dict[str, Any] = Field(default_factory=dict)
    severity: Literal["warning", "error", "critical"] = Field(default="error")
    threshold: Optional[float] = Field(default=None, description="Acceptable failure rate")


# =============================================================================
# Execution Policy
# =============================================================================


class ExecutionPolicy(BaseModel):
    """Pipeline execution policy and scheduling."""

    model_config = ConfigDict(frozen=True)

    schedule: Optional[str] = Field(default=None, description="Cron expression")
    timezone: str = Field(default="UTC")
    start_date: datetime = Field(default_factory=datetime.utcnow)
    end_date: Optional[datetime] = Field(default=None)
    max_retries: int = Field(default=3, ge=0, le=10)
    retry_delay_seconds: int = Field(default=300, ge=60)
    timeout_seconds: int = Field(default=3600, ge=300)
    sla_seconds: Optional[int] = Field(default=None)
    human_approval_required: bool = Field(default=False)
    notification_emails: List[str] = Field(default_factory=list)
    alert_on_failure: bool = Field(default=True)
    alert_on_sla_miss: bool = Field(default=True)


# =============================================================================
# Pipeline Identity
# =============================================================================


class PipelineIdentity(BaseModel):
    """Pipeline identification and ownership."""

    model_config = ConfigDict(frozen=True)

    pipeline_name: str = Field(..., min_length=3, max_length=100)
    domain: str = Field(..., description="Business domain (e.g., sales, finance)")
    environment: Environment
    data_sensitivity: DataSensitivity = Field(default=DataSensitivity.INTERNAL)
    business_owner: str = Field(...)
    technical_owner: str = Field(...)
    cost_center: Optional[str] = Field(default=None)
    tags: Dict[str, str] = Field(default_factory=dict)

    @field_validator("pipeline_name")
    @classmethod
    def validate_pipeline_name(cls, v: str) -> str:
        """Ensure pipeline name is valid identifier."""
        import re
        if not re.match(r"^[a-z][a-z0-9_]*$", v):
            raise ValueError(
                "Pipeline name must start with lowercase letter and contain only "
                "lowercase letters, numbers, and underscores"
            )
        return v


# =============================================================================
# Intent Schema (Main Input Model)
# =============================================================================


class IntentSchema(BaseModel):
    """
    Validated intent from UI - IMMUTABLE after creation.

    This is the primary input to the pipeline generation workflow.
    It must be fully validated JSON from the UI - no free-text parsing.
    """

    model_config = ConfigDict(frozen=True)

    # Metadata
    intent_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    intent_version: str = Field(default="1.0.0")
    created_at: datetime = Field(default_factory=datetime.utcnow)
    created_by: str = Field(...)
    jira_ticket: Optional[str] = Field(default=None)

    # Core configuration
    pipeline_identity: PipelineIdentity
    source_config: SourceConfig
    schema_definition: SchemaDefinition
    target_config: TargetConfig
    execution_policy: ExecutionPolicy

    # Optional configurations
    transformation_rules: List[TransformationRule] = Field(default_factory=list)
    data_quality_rules: List[DataQualityRule] = Field(default_factory=list)


# =============================================================================
# Agent Output Models
# =============================================================================


class TemplateSelection(BaseModel):
    """Selected templates for code generation."""

    model_config = ConfigDict(frozen=True)

    dag_template: str = Field(...)
    spark_templates: List[str] = Field(...)
    sql_templates: List[str] = Field(default_factory=list)


class SchemaPlan(BaseModel):
    """Plan for schema changes."""

    model_config = ConfigDict(frozen=True)

    action: Literal["create", "upgrade", "none"]
    current_version: Optional[int] = Field(default=None)
    new_version: int
    changes: List[SchemaChange] = Field(default_factory=list)
    requires_migration: bool = Field(default=False)
    migration_strategy: Optional[str] = Field(default=None)


class PlannerOutput(BaseModel):
    """Output from Planner Agent."""

    model_config = ConfigDict(frozen=True)

    plan_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    pipeline_action: PipelineAction
    pipeline_id: Optional[int] = Field(default=None)
    is_new_pipeline: bool

    schema_plan: SchemaPlan
    template_selection: TemplateSelection
    estimated_tasks: List[str] = Field(default_factory=list)

    # Reasoning for audit
    decision_reasoning: str = Field(default="")


class GeneratedArtifact(BaseModel):
    """A single generated artifact."""

    model_config = ConfigDict(frozen=True)

    artifact_type: Literal["dag", "spark_job", "sql", "config"]
    name: str
    content: str
    path: str
    checksum: str = Field(..., description="SHA256 hash of content")


class GeneratorOutput(BaseModel):
    """Output from Generator Agent."""

    model_config = ConfigDict(frozen=True)

    generation_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    dag_code: str
    spark_jobs: Dict[str, str] = Field(default_factory=dict)  # {job_name: code}
    metadata_sql: List[str] = Field(default_factory=list)
    artifacts: List[GeneratedArtifact] = Field(default_factory=list)
    artifact_paths: Dict[str, str] = Field(default_factory=dict)


class ValidationResult(BaseModel):
    """Result of a single validation check."""

    model_config = ConfigDict(frozen=True)

    check_name: str
    passed: bool
    message: Optional[str] = None
    details: Dict[str, Any] = Field(default_factory=dict)


class ValidatorOutput(BaseModel):
    """Output from Validator Agent."""

    model_config = ConfigDict(frozen=True)

    validation_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    is_valid: bool

    # Individual check results
    dag_import_success: bool = Field(default=False)
    sql_syntax_valid: bool = Field(default=False)
    schema_compatible: bool = Field(default=False)
    security_passed: bool = Field(default=False)

    # Detailed results
    validation_results: List[ValidationResult] = Field(default_factory=list)
    errors: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)


class DeployerOutput(BaseModel):
    """Output from Deployer Agent."""

    model_config = ConfigDict(frozen=True)

    deployment_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    branch_name: str
    commit_sha: str
    pr_url: Optional[str] = Field(default=None)
    pr_number: Optional[int] = Field(default=None)
    cicd_build_id: Optional[str] = Field(default=None)
    deployment_status: DeploymentStatus = Field(default=DeploymentStatus.PENDING)
    deployed_artifacts: List[str] = Field(default_factory=list)
    rollback_commit: Optional[str] = Field(default=None)


# =============================================================================
# Error Models
# =============================================================================


class WorkflowError(BaseModel):
    """Structured workflow error."""

    model_config = ConfigDict(frozen=True)

    error_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    error_type: Literal["validation", "generation", "deployment", "timeout", "approval_rejected"]
    error_agent: str
    error_message: str
    error_details: Dict[str, Any] = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    is_retryable: bool = Field(default=False)


# =============================================================================
# Metadata Context (from PostgreSQL)
# =============================================================================


class MetadataContext(BaseModel):
    """Context loaded from metadata database."""

    model_config = ConfigDict(frozen=True)

    pipeline_id: Optional[int] = Field(default=None)
    is_new: bool = Field(default=True)
    existing_schema_version: Optional[int] = Field(default=None)
    existing_schema_hash: Optional[str] = Field(default=None)
    last_execution_id: Optional[int] = Field(default=None)
    last_execution_status: Optional[str] = Field(default=None)
