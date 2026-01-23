"""
Enterprise Metadata Models - Inspired by Netflix, Uber, Airbnb patterns.

WHY: Enterprise-grade metadata schema that supports:
- Multi-source ingestion (CSV, EBCDIC/Copybook, Database, API, Streaming)
- Medallion architecture (Landing → Bronze → Silver → Gold → Trusted)
- Self-healing pipelines with LLM agents
- Dynamic business logic (SQL, DTSX, Natural Language)
- Data quality scoring (Airbnb Midas pattern)
- Config-driven Spark tuning (Uber Sparkle pattern)

DESIGN PRINCIPLES (from industry leaders):
1. Netflix UDA: Unified Data Architecture with knowledge graph
2. Uber DataMesh: Self-governed data domains, config-driven ETL
3. Airbnb Midas: Data quality scoring, spec/data/code reviews

NAMING CONVENTIONS:
- Tables use snake_case, logical names
- Primary keys: {table_name}_id
- Foreign keys: reference full name (e.g., data_source_id)
- Audit columns: created_at, updated_at, created_by, updated_by

References:
- https://netflixtechblog.com (Netflix Tech Blog)
- https://www.uber.com/blog/datamesh/ (Uber DataMesh)
- https://medium.com/airbnb-engineering/data-quality-at-airbnb (Airbnb DQ)
"""

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


# =============================================================================
# Enums - Logical, Extensible
# =============================================================================


class SourceFormat(str, Enum):
    """Supported data source formats."""
    CSV = "csv"
    CSV_WITH_METADATA = "csv_with_metadata"      # CSV with sidecar schema file
    JSON = "json"
    JSONL = "jsonl"                               # JSON Lines
    PARQUET = "parquet"
    AVRO = "avro"
    ORC = "orc"
    EBCDIC_COPYBOOK = "ebcdic_copybook"          # Mainframe COBOL copybook
    FIXED_WIDTH = "fixed_width"                   # Fixed-width text
    XML = "xml"
    DELTA = "delta"
    ICEBERG = "iceberg"


class SourceType(str, Enum):
    """Data source connection types."""
    FILE_GCS = "file_gcs"                         # Google Cloud Storage
    FILE_S3 = "file_s3"                           # AWS S3
    FILE_AZURE = "file_azure"                     # Azure Blob
    FILE_SFTP = "file_sftp"                       # SFTP server
    DATABASE_POSTGRES = "database_postgres"
    DATABASE_MYSQL = "database_mysql"
    DATABASE_ORACLE = "database_oracle"
    DATABASE_SQLSERVER = "database_sqlserver"
    DATABASE_BIGQUERY = "database_bigquery"
    DATABASE_SNOWFLAKE = "database_snowflake"
    STREAMING_KAFKA = "streaming_kafka"
    STREAMING_PUBSUB = "streaming_pubsub"
    STREAMING_KINESIS = "streaming_kinesis"
    API_REST = "api_rest"
    API_GRAPHQL = "api_graphql"
    MAINFRAME_FTP = "mainframe_ftp"              # Mainframe file transfer


class ProcessingMode(str, Enum):
    """Data processing modes."""
    BATCH_FULL = "batch_full"                     # Full refresh
    BATCH_INCREMENTAL = "batch_incremental"       # Delta/CDC
    BATCH_SNAPSHOT = "batch_snapshot"             # Point-in-time snapshot
    MICRO_BATCH = "micro_batch"                   # Near real-time batches
    STREAMING = "streaming"                       # True streaming


class DataZone(str, Enum):
    """Medallion architecture zones."""
    LANDING = "landing"                           # Raw, untouched data
    BRONZE = "bronze"                             # Cleansed, typed, partitioned
    SILVER = "silver"                             # Transformed, deduplicated, DQ passed
    GOLD = "gold"                                 # Aggregated, business-ready
    TRUSTED = "trusted"                           # Certified, governed, production


class LoadStrategy(str, Enum):
    """Data loading strategies."""
    APPEND = "append"                             # Insert new rows
    OVERWRITE = "overwrite"                       # Replace all data
    MERGE_UPSERT = "merge_upsert"                 # Update existing, insert new
    SCD_TYPE_1 = "scd_type_1"                     # Overwrite history
    SCD_TYPE_2 = "scd_type_2"                     # Track history with effective dates
    SOFT_DELETE = "soft_delete"                   # Mark deleted, don't remove


class DataSensitivity(str, Enum):
    """Data classification levels (GDPR/CCPA aligned)."""
    PUBLIC = "public"                             # Open data
    INTERNAL = "internal"                         # Company internal
    CONFIDENTIAL = "confidential"                 # Business sensitive
    PII = "pii"                                   # Personally Identifiable Information
    PHI = "phi"                                   # Protected Health Information
    PCI = "pci"                                   # Payment Card Industry
    RESTRICTED = "restricted"                     # Highest security


class PipelineStatus(str, Enum):
    """Pipeline lifecycle status."""
    DRAFT = "draft"                               # Being designed
    PENDING_REVIEW = "pending_review"             # Awaiting spec review
    APPROVED = "approved"                         # Ready for deployment
    ACTIVE = "active"                             # Running in production
    PAUSED = "paused"                             # Temporarily stopped
    DEPRECATED = "deprecated"                     # Scheduled for removal
    ARCHIVED = "archived"                         # Historical reference


class QualityDimension(str, Enum):
    """Data quality dimensions (ISO 8000 aligned)."""
    COMPLETENESS = "completeness"                 # No missing values
    ACCURACY = "accuracy"                         # Correct values
    CONSISTENCY = "consistency"                   # Same across sources
    TIMELINESS = "timeliness"                     # Fresh data
    VALIDITY = "validity"                         # Meets business rules
    UNIQUENESS = "uniqueness"                     # No duplicates


class BusinessLogicType(str, Enum):
    """Business logic input types."""
    SQL_QUERY = "sql_query"                       # Direct SQL
    SQL_FILE = "sql_file"                         # SQL file reference
    DTSX_EXTRACT = "dtsx_extract"                 # SSIS package extraction
    NATURAL_LANGUAGE = "natural_language"         # LLM-interpreted
    COLUMN_MAPPING = "column_mapping"             # Simple mapping rules
    PYSPARK_CODE = "pyspark_code"                 # Direct PySpark
    DBT_MODEL = "dbt_model"                       # dbt transformation


# =============================================================================
# Base Classes
# =============================================================================


class Base(DeclarativeBase):
    """SQLAlchemy declarative base."""
    pass


class AuditMixin:
    """Standard audit columns for all tables."""
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=func.now(),
        nullable=False,
    )
    updated_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        onupdate=func.now(),
        nullable=True,
    )
    created_by: Mapped[str] = mapped_column(String(255), nullable=False)
    updated_by: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)


# =============================================================================
# Data Domain - Netflix UDA inspired
# =============================================================================


class DataDomain(Base, AuditMixin):
    """
    Business domain registry - organizes data by business capability.

    Netflix UDA Pattern: Domains provide logical boundaries for data governance.
    Uber DataMesh: Self-governed domains with clear ownership.

    Examples: sales, finance, customer, product, operations
    """
    __tablename__ = "data_domain"

    domain_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    domain_name: Mapped[str] = mapped_column(String(100), nullable=False, unique=True)
    domain_description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Ownership
    domain_owner_email: Mapped[str] = mapped_column(String(255), nullable=False)
    domain_steward_email: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    slack_channel: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)

    # Governance
    cost_center: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    data_retention_days: Mapped[int] = mapped_column(Integer, default=365, nullable=False)

    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    # Relationships
    data_sources: Mapped[List["DataSource"]] = relationship(back_populates="domain")
    data_products: Mapped[List["DataProduct"]] = relationship(back_populates="domain")

    __table_args__ = (
        Index("idx_domain_name", "domain_name"),
        Index("idx_domain_owner", "domain_owner_email"),
    )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "domain_id": self.domain_id,
            "domain_name": self.domain_name,
            "domain_description": self.domain_description,
            "domain_owner_email": self.domain_owner_email,
            "is_active": self.is_active,
        }


# =============================================================================
# Data Source - Multi-format support
# =============================================================================


class DataSource(Base, AuditMixin):
    """
    Data source registry - defines where data comes from.

    Supports: CSV, EBCDIC/Copybook, Database, API, Streaming

    WHY: Centralized source management enables:
    - Connection pooling and reuse
    - Credential management via Secret Manager
    - Source-level SLAs and monitoring
    """
    __tablename__ = "data_source"

    source_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_name: Mapped[str] = mapped_column(String(255), nullable=False)
    source_code: Mapped[str] = mapped_column(String(50), nullable=False)  # Short code: SFDC, SAP, MAIN

    # Classification
    source_type: Mapped[str] = mapped_column(String(50), nullable=False)  # SourceType enum
    source_format: Mapped[str] = mapped_column(String(50), nullable=False)  # SourceFormat enum

    # Domain ownership
    domain_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("data_domain.domain_id", ondelete="RESTRICT"),
        nullable=False,
    )

    # Connection details (stored in Secret Manager)
    connection_secret_name: Mapped[str] = mapped_column(String(255), nullable=False)

    # Source-specific configuration (JSONB for flexibility)
    source_config: Mapped[Dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
        comment="""
        Configuration varies by source_type:
        - FILE_*: {path_pattern, file_format, compression, encoding}
        - DATABASE_*: {database, schema, connection_pool_size}
        - STREAMING_*: {topic, consumer_group, offset_reset}
        - API_*: {base_url, auth_type, rate_limit}
        - MAINFRAME_*: {host, encoding, record_format}
        """
    )

    # EBCDIC/Copybook specific
    copybook_content: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
        comment="COBOL copybook definition for EBCDIC files"
    )
    copybook_encoding: Mapped[Optional[str]] = mapped_column(
        String(50),
        nullable=True,
        default="cp037",
        comment="EBCDIC code page (cp037=US, cp500=International)"
    )

    # Metadata
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    data_sensitivity: Mapped[str] = mapped_column(
        String(50),
        default=DataSensitivity.INTERNAL.value,
        nullable=False,
    )

    # SLA and monitoring
    expected_frequency_cron: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    max_delay_minutes: Mapped[int] = mapped_column(Integer, default=60, nullable=False)
    alert_on_missing: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    # Relationships
    domain: Mapped["DataDomain"] = relationship(back_populates="data_sources")
    data_contracts: Mapped[List["DataContract"]] = relationship(back_populates="data_source")

    __table_args__ = (
        CheckConstraint(
            f"source_type IN ({','.join([repr(t.value) for t in SourceType])})",
            name="chk_source_type",
        ),
        UniqueConstraint("source_code", name="uq_source_code"),
        Index("idx_source_domain", "domain_id"),
        Index("idx_source_type", "source_type"),
        Index("idx_source_active", "is_active"),
    )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source_id": self.source_id,
            "source_name": self.source_name,
            "source_code": self.source_code,
            "source_type": self.source_type,
            "source_format": self.source_format,
            "domain_id": self.domain_id,
            "data_sensitivity": self.data_sensitivity,
            "is_active": self.is_active,
        }


# =============================================================================
# Data Contract - File/API patterns and paths
# =============================================================================


class DataContract(Base, AuditMixin):
    """
    Data contract - defines the agreement between producer and consumer.

    Inspired by: Airbnb Midas Spec Review process

    WHY: Contracts enforce:
    - Schema compatibility
    - File naming patterns
    - Path conventions
    - SLAs and freshness requirements
    """
    __tablename__ = "data_contract"

    contract_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    contract_name: Mapped[str] = mapped_column(String(255), nullable=False)
    contract_version: Mapped[str] = mapped_column(String(20), default="1.0.0", nullable=False)

    # Source reference
    source_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("data_source.source_id", ondelete="CASCADE"),
        nullable=False,
    )

    # File pattern matching
    file_name_pattern: Mapped[Optional[str]] = mapped_column(
        String(500),
        nullable=True,
        comment="Regex pattern: ^orders_\\d{8}\\.csv$"
    )

    # Medallion zone paths
    landing_path: Mapped[str] = mapped_column(String(500), nullable=False)
    bronze_path: Mapped[str] = mapped_column(String(500), nullable=False)
    silver_path: Mapped[str] = mapped_column(String(500), nullable=False)
    gold_path: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    trusted_path: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    rejected_path: Mapped[str] = mapped_column(String(500), nullable=False)

    # Processing configuration
    processing_mode: Mapped[str] = mapped_column(
        String(50),
        default=ProcessingMode.BATCH_INCREMENTAL.value,
        nullable=False,
    )
    load_strategy: Mapped[str] = mapped_column(
        String(50),
        default=LoadStrategy.APPEND.value,
        nullable=False,
    )

    # File-specific settings
    file_format_config: Mapped[Dict[str, Any]] = mapped_column(
        JSONB,
        default=dict,
        nullable=False,
        comment="""
        Format-specific configuration:
        - CSV: {delimiter, quote_char, escape_char, header_rows, encoding}
        - EBCDIC: {record_length, variable_length, rdw_format}
        - JSON: {multiline, root_path}
        - PARQUET: {compression, row_group_size}
        """
    )

    # Scheduling
    ingestion_schedule: Mapped[Optional[str]] = mapped_column(
        String(100),
        nullable=True,
        comment="Cron expression: 0 6 * * *"
    )
    poke_interval_seconds: Mapped[int] = mapped_column(Integer, default=60, nullable=False)
    timeout_minutes: Mapped[int] = mapped_column(Integer, default=120, nullable=False)
    soft_fail: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    # Watermark for incremental
    watermark_column: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    watermark_format: Mapped[Optional[str]] = mapped_column(
        String(100),
        nullable=True,
        comment="Date format: yyyy-MM-dd HH:mm:ss"
    )

    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    # Relationships
    data_source: Mapped["DataSource"] = relationship(back_populates="data_contracts")
    schema_definitions: Mapped[List["SchemaDefinition"]] = relationship(back_populates="data_contract")
    data_products: Mapped[List["DataProduct"]] = relationship(back_populates="data_contract")

    __table_args__ = (
        UniqueConstraint("contract_name", "contract_version", name="uq_contract_version"),
        Index("idx_contract_source", "source_id"),
        Index("idx_contract_active", "is_active"),
    )


# =============================================================================
# Schema Definition - With versioning (SCD2)
# =============================================================================


class SchemaDefinition(Base, AuditMixin):
    """
    Schema definition with SCD Type 2 versioning.

    WHY: Schema evolution support enables:
    - Backward compatibility checking
    - Automatic migration scripts
    - Schema drift detection
    """
    __tablename__ = "schema_definition"

    schema_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    contract_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("data_contract.contract_id", ondelete="CASCADE"),
        nullable=False,
    )

    # Version tracking (SCD2)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    effective_from: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    effective_to: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    is_current: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    # Schema content
    schema_name: Mapped[str] = mapped_column(String(255), nullable=False)
    columns: Mapped[List[Dict[str, Any]]] = mapped_column(
        JSONB,
        nullable=False,
        comment="""
        Column definitions:
        [{
            "name": "order_id",
            "data_type": "STRING",
            "nullable": false,
            "description": "Unique order identifier",
            "is_primary_key": true,
            "is_partition_key": false,
            "pii_classification": null,
            "default_value": null,
            "validation_regex": "^ORD-\\d{10}$"
        }]
        """
    )

    # Keys and partitioning
    primary_keys: Mapped[List[str]] = mapped_column(ARRAY(String), default=list, nullable=False)
    partition_keys: Mapped[List[str]] = mapped_column(ARRAY(String), default=list, nullable=False)
    clustering_keys: Mapped[List[str]] = mapped_column(ARRAY(String), default=list, nullable=False)

    # For EBCDIC/Fixed-width
    record_length: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    header_rows: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    footer_rows: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    # Change tracking
    change_type: Mapped[str] = mapped_column(
        String(50),
        default="initial",
        nullable=False,
        comment="initial, add_column, drop_column, modify_column, rename_column"
    )
    change_description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    is_breaking_change: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    # Schema hash for drift detection
    schema_hash: Mapped[str] = mapped_column(String(64), nullable=False)

    # Relationships
    data_contract: Mapped["DataContract"] = relationship(back_populates="schema_definitions")

    __table_args__ = (
        UniqueConstraint("contract_id", "version", name="uq_schema_version"),
        Index("idx_schema_contract", "contract_id"),
        Index("idx_schema_current", "is_current"),
        Index("idx_schema_effective", "effective_from", "effective_to"),
    )


# =============================================================================
# Data Product (Pipeline) - Main orchestration unit
# =============================================================================


class DataProduct(Base, AuditMixin):
    """
    Data Product - The deployable pipeline unit.

    Netflix Pattern: Data products are self-contained, discoverable assets
    Uber Pattern: Config-driven, domain-aligned pipelines

    WHY: Encapsulates all pipeline metadata in one place:
    - Source → Transform → Target flow
    - Quality rules
    - Scheduling
    - Ownership
    """
    __tablename__ = "data_product"

    product_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    product_name: Mapped[str] = mapped_column(String(255), nullable=False)
    product_code: Mapped[str] = mapped_column(String(100), nullable=False)  # snake_case identifier

    # Domain and ownership
    domain_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("data_domain.domain_id", ondelete="RESTRICT"),
        nullable=False,
    )
    contract_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("data_contract.contract_id", ondelete="RESTRICT"),
        nullable=False,
    )

    # Ownership
    product_owner_email: Mapped[str] = mapped_column(String(255), nullable=False)
    technical_owner_email: Mapped[str] = mapped_column(String(255), nullable=False)

    # Lifecycle
    status: Mapped[str] = mapped_column(
        String(50),
        default=PipelineStatus.DRAFT.value,
        nullable=False,
    )
    environment: Mapped[str] = mapped_column(String(20), nullable=False)  # dev, qa, prod

    # Description
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    documentation_url: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)

    # Tags for discovery
    tags: Mapped[List[str]] = mapped_column(ARRAY(String), default=list, nullable=False)

    # Airflow DAG configuration
    dag_id: Mapped[str] = mapped_column(String(255), nullable=False)
    schedule_interval: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    start_date: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    end_date: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    catchup: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    max_active_runs: Mapped[int] = mapped_column(Integer, default=1, nullable=False)

    # Retries and timeouts
    retries: Mapped[int] = mapped_column(Integer, default=3, nullable=False)
    retry_delay_minutes: Mapped[int] = mapped_column(Integer, default=5, nullable=False)
    execution_timeout_minutes: Mapped[int] = mapped_column(Integer, default=120, nullable=False)

    # Self-healing configuration
    self_healing_enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    auto_retry_on_schema_drift: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    max_healing_attempts: Mapped[int] = mapped_column(Integer, default=3, nullable=False)

    # Quality gate
    min_quality_score: Mapped[Decimal] = mapped_column(
        Numeric(5, 2),
        default=Decimal("80.00"),
        nullable=False,
        comment="Minimum DQ score to proceed (Airbnb pattern)"
    )
    block_on_quality_failure: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    # Notifications
    alert_emails: Mapped[List[str]] = mapped_column(ARRAY(String), default=list, nullable=False)
    slack_webhook: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)

    # Git/deployment tracking
    git_repo: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    git_branch: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    last_commit_sha: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    last_deployed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    # Relationships
    domain: Mapped["DataDomain"] = relationship(back_populates="data_products")
    data_contract: Mapped["DataContract"] = relationship(back_populates="data_products")
    spark_configurations: Mapped[List["SparkConfiguration"]] = relationship(back_populates="data_product")
    transformation_rules: Mapped[List["TransformationRule"]] = relationship(back_populates="data_product")
    quality_rules: Mapped[List["QualityRule"]] = relationship(back_populates="data_product")
    business_logic_blocks: Mapped[List["BusinessLogicBlock"]] = relationship(back_populates="data_product")
    target_mappings: Mapped[List["TargetMapping"]] = relationship(back_populates="data_product")
    execution_history: Mapped[List["ExecutionRecord"]] = relationship(back_populates="data_product")

    __table_args__ = (
        UniqueConstraint("product_code", "environment", name="uq_product_env"),
        UniqueConstraint("dag_id", name="uq_dag_id"),
        Index("idx_product_domain", "domain_id"),
        Index("idx_product_status", "status"),
        Index("idx_product_owner", "product_owner_email"),
    )


# =============================================================================
# Spark Configuration - Per zone tuning (Uber Sparkle pattern)
# =============================================================================


class SparkConfiguration(Base, AuditMixin):
    """
    Spark job configuration per data zone.

    Uber Sparkle Pattern: Config-driven Spark jobs with zone-specific tuning.

    WHY: Different zones have different compute requirements:
    - Bronze: High throughput, low compute
    - Silver: Medium compute for transformations
    - Gold: Aggregation-heavy, needs more memory
    """
    __tablename__ = "spark_configuration"

    config_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    product_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("data_product.product_id", ondelete="CASCADE"),
        nullable=False,
    )

    # Zone-specific config
    data_zone: Mapped[str] = mapped_column(String(50), nullable=False)  # DataZone enum

    # Cluster resources
    executor_instances: Mapped[int] = mapped_column(Integer, default=2, nullable=False)
    executor_memory: Mapped[str] = mapped_column(String(20), default="4g", nullable=False)
    executor_cores: Mapped[int] = mapped_column(Integer, default=2, nullable=False)
    driver_memory: Mapped[str] = mapped_column(String(20), default="2g", nullable=False)
    driver_cores: Mapped[int] = mapped_column(Integer, default=1, nullable=False)

    # Shuffle and partitioning
    shuffle_partitions: Mapped[int] = mapped_column(Integer, default=200, nullable=False)
    adaptive_enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    broadcast_threshold_mb: Mapped[int] = mapped_column(Integer, default=10, nullable=False)

    # Delta/Iceberg specific
    optimize_write: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    auto_compact: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    z_order_columns: Mapped[List[str]] = mapped_column(ARRAY(String), default=list, nullable=False)

    # Custom Spark properties
    custom_properties: Mapped[Dict[str, Any]] = mapped_column(
        JSONB,
        default=dict,
        nullable=False,
        comment="Additional spark.* properties"
    )

    # Relationships
    data_product: Mapped["DataProduct"] = relationship(back_populates="spark_configurations")

    __table_args__ = (
        UniqueConstraint("product_id", "data_zone", name="uq_spark_config_zone"),
        Index("idx_spark_product", "product_id"),
    )


# =============================================================================
# Transformation Rule - Column-level transformations
# =============================================================================


class TransformationRule(Base, AuditMixin):
    """
    Column-level transformation rules.

    WHY: Metadata-driven transformations enable:
    - No code changes for business rule updates
    - Audit trail of transformation logic
    - Reusable patterns across pipelines
    """
    __tablename__ = "transformation_rule"

    rule_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    product_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("data_product.product_id", ondelete="CASCADE"),
        nullable=False,
    )

    # Rule definition
    rule_name: Mapped[str] = mapped_column(String(255), nullable=False)
    rule_description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Target zone
    applies_to_zone: Mapped[str] = mapped_column(
        String(50),
        default=DataZone.SILVER.value,
        nullable=False,
    )

    # Transformation type
    rule_type: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        comment="rename, cast, derive, filter, aggregate, join, mask, hash, encrypt"
    )

    # Column mapping
    source_columns: Mapped[List[str]] = mapped_column(ARRAY(String), nullable=False)
    target_column: Mapped[str] = mapped_column(String(255), nullable=False)

    # Transformation expression (Spark SQL compatible)
    transformation_expression: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        comment="Spark SQL expression: UPPER(trim(name))"
    )

    # Parameters for complex transformations
    parameters: Mapped[Dict[str, Any]] = mapped_column(
        JSONB,
        default=dict,
        nullable=False,
        comment="""
        Type-specific parameters:
        - mask: {mask_char, visible_chars}
        - hash: {algorithm: sha256|md5}
        - encrypt: {key_secret_name}
        - aggregate: {group_by, function}
        - join: {right_table, join_type, condition}
        """
    )

    # Execution order
    execution_order: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    # Relationships
    data_product: Mapped["DataProduct"] = relationship(back_populates="transformation_rules")

    __table_args__ = (
        Index("idx_transform_product", "product_id"),
        Index("idx_transform_zone", "applies_to_zone"),
        Index("idx_transform_order", "product_id", "execution_order"),
    )


# =============================================================================
# Quality Rule - Data quality checks (Airbnb Midas inspired)
# =============================================================================


class QualityRule(Base, AuditMixin):
    """
    Data quality rules with scoring (Airbnb DQ Score pattern).

    Airbnb Pattern: Each column has a quality score, averaged for dataset score.

    WHY: Quality gates prevent bad data from reaching production:
    - Block on critical failures
    - Track quality trends over time
    - Enable data consumers to trust the data
    """
    __tablename__ = "quality_rule"

    rule_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    product_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("data_product.product_id", ondelete="CASCADE"),
        nullable=False,
    )

    # Rule definition
    rule_name: Mapped[str] = mapped_column(String(255), nullable=False)
    rule_description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Validation zone
    validation_zone: Mapped[str] = mapped_column(
        String(50),
        default=DataZone.SILVER.value,
        nullable=False,
    )

    # Quality dimension (ISO 8000)
    quality_dimension: Mapped[str] = mapped_column(
        String(50),
        default=QualityDimension.VALIDITY.value,
        nullable=False,
    )

    # Rule type
    rule_type: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        comment="not_null, unique, range, regex, in_set, foreign_key, custom_sql, row_count, freshness"
    )

    # Column (optional for table-level rules)
    column_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    # Rule expression
    rule_expression: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        comment="SQL expression that should return TRUE for valid rows"
    )

    # Parameters
    parameters: Mapped[Dict[str, Any]] = mapped_column(
        JSONB,
        default=dict,
        nullable=False,
        comment="""
        Type-specific parameters:
        - range: {min, max}
        - regex: {pattern}
        - in_set: {values: [...]}
        - foreign_key: {reference_table, reference_column}
        - freshness: {max_hours}
        - row_count: {min, max}
        """
    )

    # Threshold and severity
    pass_threshold: Mapped[Decimal] = mapped_column(
        Numeric(5, 4),
        default=Decimal("1.0000"),
        nullable=False,
        comment="Minimum pass rate (0.95 = 95% must pass)"
    )
    severity: Mapped[str] = mapped_column(
        String(20),
        default="error",
        nullable=False,
        comment="info, warning, error, critical"
    )
    is_blocking: Mapped[bool] = mapped_column(
        Boolean,
        default=False,
        nullable=False,
        comment="If true, pipeline fails on rule violation"
    )

    # Scoring weight (for DQ Score calculation)
    weight: Mapped[Decimal] = mapped_column(
        Numeric(5, 2),
        default=Decimal("1.00"),
        nullable=False,
    )

    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    # Relationships
    data_product: Mapped["DataProduct"] = relationship(back_populates="quality_rules")

    __table_args__ = (
        Index("idx_quality_product", "product_id"),
        Index("idx_quality_zone", "validation_zone"),
        Index("idx_quality_blocking", "is_blocking"),
    )


# =============================================================================
# Business Logic Block - Dynamic logic (SQL, DTSX, NL)
# =============================================================================


class BusinessLogicBlock(Base, AuditMixin):
    """
    Business logic container - supports multiple input formats.

    WHY: Enables non-developers to define business rules:
    - SQL queries from analysts
    - DTSX extraction from SSIS packages
    - Natural language from business users
    - LLM generates executable PySpark

    Self-healing: LLM can fix generated code if it fails validation.
    """
    __tablename__ = "business_logic_block"

    logic_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    product_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("data_product.product_id", ondelete="CASCADE"),
        nullable=False,
    )

    # Logic identification
    logic_name: Mapped[str] = mapped_column(String(255), nullable=False)
    logic_description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Input type
    logic_type: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        comment="sql_query, sql_file, dtsx_extract, natural_language, column_mapping, pyspark_code"
    )

    # Target zone
    applies_to_zone: Mapped[str] = mapped_column(
        String(50),
        default=DataZone.SILVER.value,
        nullable=False,
    )

    # Input specification
    input_specification: Mapped[Dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        comment="""
        Input varies by logic_type:

        sql_query:
            {raw_sql: "SELECT ...", dialect: "spark"}

        dtsx_extract:
            {dtsx_path: "gs://...", package_name: "...", dataflow_name: "..."}

        natural_language:
            {description: "Calculate total sales by region..."}

        column_mapping:
            {mappings: [{source: "col1", target: "new_col1", transform: "UPPER"}]}

        All types can include:
            source_tables: [{table: "bronze.orders", alias: "o"}]
            target_columns: ["col1", "col2"]
            joins: [{left: "o.id", right: "c.order_id", type: "inner"}]
            filters: "status = 'active'"
            aggregations: [{column: "amount", func: "SUM", alias: "total"}]
        """
    )

    # LLM-generated output
    generated_pyspark_code: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
        comment="PySpark code generated by LLM"
    )
    generation_model: Mapped[Optional[str]] = mapped_column(
        String(100),
        nullable=True,
        comment="LLM model used: claude-3-opus, gpt-4, etc."
    )
    generation_timestamp: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    # Validation status
    is_validated: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    validation_errors: Mapped[Optional[List[str]]] = mapped_column(
        ARRAY(String),
        nullable=True,
    )
    last_validated_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    # Self-healing tracking
    healing_attempts: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    last_healing_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Execution order
    execution_order: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    # Relationships
    data_product: Mapped["DataProduct"] = relationship(back_populates="business_logic_blocks")

    __table_args__ = (
        Index("idx_logic_product", "product_id"),
        Index("idx_logic_type", "logic_type"),
        Index("idx_logic_validated", "is_validated"),
    )


# =============================================================================
# Target Mapping - Multi-destination support
# =============================================================================


class TargetMapping(Base, AuditMixin):
    """
    Target destination mapping - one pipeline, multiple targets.

    WHY: Data products often serve multiple consumers:
    - BigQuery for analytics
    - Snowflake for data sharing
    - Delta Lake for ML training
    - Pub/Sub for real-time subscribers
    """
    __tablename__ = "target_mapping"

    mapping_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    product_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("data_product.product_id", ondelete="CASCADE"),
        nullable=False,
    )

    # Target identification
    target_name: Mapped[str] = mapped_column(String(255), nullable=False)
    target_type: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        comment="bigquery, snowflake, delta, iceberg, pubsub, gcs, s3"
    )

    # Connection
    connection_secret_name: Mapped[str] = mapped_column(String(255), nullable=False)

    # Target-specific configuration
    target_config: Mapped[Dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        comment="""
        Configuration varies by target_type:

        bigquery:
            {project: "...", dataset: "...", table: "..."}

        snowflake:
            {database: "...", schema: "...", table: "..."}

        delta/iceberg:
            {path: "gs://...", table_name: "..."}

        pubsub:
            {project: "...", topic: "..."}
        """
    )

    # Load configuration
    load_strategy: Mapped[str] = mapped_column(
        String(50),
        default=LoadStrategy.APPEND.value,
        nullable=False,
    )
    partition_columns: Mapped[List[str]] = mapped_column(ARRAY(String), default=list, nullable=False)
    clustering_columns: Mapped[List[str]] = mapped_column(ARRAY(String), default=list, nullable=False)

    # Merge keys for upsert
    merge_keys: Mapped[List[str]] = mapped_column(ARRAY(String), default=list, nullable=False)

    # SCD2 configuration
    scd2_effective_from_column: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    scd2_effective_to_column: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    scd2_current_flag_column: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    # Priority (for ordered execution)
    execution_priority: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    # Relationships
    data_product: Mapped["DataProduct"] = relationship(back_populates="target_mappings")

    __table_args__ = (
        UniqueConstraint("product_id", "target_name", name="uq_target_mapping"),
        Index("idx_target_product", "product_id"),
        Index("idx_target_type", "target_type"),
    )


# =============================================================================
# Execution Record - Pipeline run history
# =============================================================================


class ExecutionRecord(Base):
    """
    Pipeline execution history for monitoring and debugging.

    WHY: Comprehensive execution tracking enables:
    - Performance trending
    - Failure analysis
    - Self-healing decisions
    - SLA monitoring
    """
    __tablename__ = "execution_record"

    execution_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    product_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("data_product.product_id", ondelete="CASCADE"),
        nullable=False,
    )

    # Identifiers
    dag_run_id: Mapped[str] = mapped_column(String(255), nullable=False)
    execution_date: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    # Status
    status: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        comment="pending, running, success, failed, skipped, up_for_retry"
    )

    # Timing
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    duration_seconds: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # Data metrics
    records_read: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    records_written: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    records_rejected: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    bytes_processed: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # Quality metrics (Airbnb DQ Score)
    quality_score: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(5, 2),
        nullable=True,
        comment="0-100 score based on quality rules"
    )
    quality_details: Mapped[Optional[Dict[str, Any]]] = mapped_column(
        JSONB,
        nullable=True,
        comment="Per-rule pass/fail counts"
    )

    # Error tracking
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    error_task: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    error_traceback: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Self-healing tracking
    healing_attempted: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    healing_action: Mapped[Optional[str]] = mapped_column(
        String(255),
        nullable=True,
        comment="Action taken: schema_fix, retry, alert, escalate"
    )
    healing_result: Mapped[Optional[str]] = mapped_column(
        String(50),
        nullable=True,
        comment="success, failed, escalated"
    )

    # Retry tracking
    retry_number: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    # Metadata
    triggered_by: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        comment="scheduler, manual, backfill, self_healing"
    )
    airflow_log_url: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)

    # Relationships
    data_product: Mapped["DataProduct"] = relationship(back_populates="execution_history")

    __table_args__ = (
        UniqueConstraint("dag_run_id", name="uq_dag_run"),
        Index("idx_exec_product", "product_id"),
        Index("idx_exec_status", "status"),
        Index("idx_exec_date", "execution_date"),
        Index("idx_exec_quality", "quality_score"),
    )


# =============================================================================
# Self-Healing Event - LLM agent actions
# =============================================================================


class SelfHealingEvent(Base):
    """
    Self-healing event log - tracks LLM agent interventions.

    VIGIL Pattern: Observe → Diagnose → Remediate → Verify

    WHY: Audit trail for autonomous actions:
    - What was detected
    - What action was taken
    - What was the outcome
    - Enable learning and improvement
    """
    __tablename__ = "self_healing_event"

    event_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    execution_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("execution_record.execution_id", ondelete="CASCADE"),
        nullable=False,
    )

    # Detection
    detected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=func.now(),
        nullable=False,
    )
    error_type: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        comment="schema_drift, data_quality_failure, missing_file, timeout, resource_exhaustion"
    )
    error_details: Mapped[Dict[str, Any]] = mapped_column(JSONB, nullable=False)

    # Diagnosis (LLM analysis)
    diagnosis: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        comment="LLM-generated diagnosis"
    )
    root_cause: Mapped[Optional[str]] = mapped_column(
        String(255),
        nullable=True,
        comment="Identified root cause category"
    )
    confidence_score: Mapped[Decimal] = mapped_column(
        Numeric(5, 4),
        nullable=False,
        comment="LLM confidence in diagnosis (0-1)"
    )

    # Remediation
    remediation_action: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        comment="retry, schema_update, config_adjust, skip, escalate"
    )
    remediation_details: Mapped[Dict[str, Any]] = mapped_column(JSONB, nullable=False)

    # Code fix (if applicable)
    original_code: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    fixed_code: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Outcome
    remediation_status: Mapped[str] = mapped_column(
        String(50),
        default="pending",
        nullable=False,
        comment="pending, applied, verified_success, verified_failed, rolled_back"
    )
    verified_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    # LLM tracking
    llm_model: Mapped[str] = mapped_column(String(100), nullable=False)
    llm_prompt_tokens: Mapped[int] = mapped_column(Integer, nullable=False)
    llm_completion_tokens: Mapped[int] = mapped_column(Integer, nullable=False)
    llm_latency_ms: Mapped[int] = mapped_column(Integer, nullable=False)

    __table_args__ = (
        Index("idx_healing_execution", "execution_id"),
        Index("idx_healing_type", "error_type"),
        Index("idx_healing_status", "remediation_status"),
        Index("idx_healing_time", "detected_at"),
    )


# =============================================================================
# Pydantic Models for API/Validation
# =============================================================================


class DataProductRequest(BaseModel):
    """API request model for creating/updating a data product."""

    model_config = ConfigDict(extra="forbid")

    # Identity
    product_name: str = Field(..., min_length=3, max_length=255)
    product_code: str = Field(..., pattern=r"^[a-z][a-z0-9_]*$")
    domain_name: str = Field(...)

    # Source
    source_code: str = Field(...)
    contract_name: str = Field(...)

    # Ownership
    product_owner_email: str = Field(..., pattern=r"^[\w\.-]+@[\w\.-]+\.\w+$")
    technical_owner_email: str = Field(..., pattern=r"^[\w\.-]+@[\w\.-]+\.\w+$")

    # Environment
    environment: str = Field(..., pattern=r"^(dev|qa|prod)$")

    # Schedule
    schedule_interval: Optional[str] = Field(None)
    start_date: datetime = Field(default_factory=datetime.utcnow)

    # Configuration
    self_healing_enabled: bool = Field(default=True)
    min_quality_score: Decimal = Field(default=Decimal("80.00"), ge=0, le=100)

    # Tags
    tags: List[str] = Field(default_factory=list)

    # Spark config per zone
    spark_configurations: Optional[Dict[str, Dict[str, Any]]] = Field(default=None)

    # Business logic blocks
    business_logic: Optional[List[Dict[str, Any]]] = Field(default=None)

    # Quality rules
    quality_rules: Optional[List[Dict[str, Any]]] = Field(default=None)

    # Target mappings
    targets: Optional[List[Dict[str, Any]]] = Field(default=None)


class BusinessLogicInput(BaseModel):
    """Input model for business logic definition."""

    model_config = ConfigDict(extra="forbid")

    logic_name: str = Field(...)
    logic_type: BusinessLogicType = Field(...)
    applies_to_zone: DataZone = Field(default=DataZone.SILVER)

    # Input varies by type
    raw_sql: Optional[str] = Field(None, description="For sql_query type")
    dtsx_path: Optional[str] = Field(None, description="For dtsx_extract type")
    natural_language_description: Optional[str] = Field(None, description="For natural_language type")

    # Common fields
    source_tables: Optional[List[Dict[str, str]]] = Field(
        None,
        description="[{table: 'bronze.orders', alias: 'o'}]"
    )
    target_columns: Optional[List[str]] = Field(None)
    joins: Optional[List[Dict[str, str]]] = Field(
        None,
        description="[{left: 'o.id', right: 'c.order_id', type: 'inner'}]"
    )
    filters: Optional[str] = Field(None)
    aggregations: Optional[List[Dict[str, str]]] = Field(None)

    execution_order: int = Field(default=0)

    @field_validator("logic_type", mode="before")
    @classmethod
    def validate_logic_type(cls, v):
        if isinstance(v, str):
            return BusinessLogicType(v)
        return v


class QualityRuleInput(BaseModel):
    """Input model for quality rule definition."""

    model_config = ConfigDict(extra="forbid")

    rule_name: str = Field(...)
    rule_type: str = Field(...)
    validation_zone: DataZone = Field(default=DataZone.SILVER)
    quality_dimension: QualityDimension = Field(default=QualityDimension.VALIDITY)

    column_name: Optional[str] = Field(None)
    rule_expression: str = Field(...)
    parameters: Dict[str, Any] = Field(default_factory=dict)

    pass_threshold: Decimal = Field(default=Decimal("1.0"), ge=0, le=1)
    severity: str = Field(default="error", pattern=r"^(info|warning|error|critical)$")
    is_blocking: bool = Field(default=False)
    weight: Decimal = Field(default=Decimal("1.0"), ge=0)
