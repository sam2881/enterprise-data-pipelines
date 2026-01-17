"""
Metadata Reader - Query metadata tables dynamically at runtime.

WHY: Central point for all metadata queries. No hardcoded values.
     DAG tasks call this to get feed configuration dynamically.

HOW: Uses SQLAlchemy to query PostgreSQL metadata tables.
     Returns typed dataclasses for type safety.

DESIGN DECISIONS:
1. Cache metadata per feed_id to reduce DB calls
2. Return dataclasses, not raw dicts
3. Thread-safe for parallel task execution
4. LAZY CONNECTION: DB connection is only established when first query is made,
   not at DAG parse time. This prevents connection errors in Airflow.

WHY LAZY CONNECTION:
- Airflow parses DAG files frequently (every 30s by default)
- If MetadataReader connects at import/parse time, it will fail if:
  - PostgreSQL is not running
  - Network is unavailable
  - Environment variables are wrong
- By deferring connection to first query, DAG parsing succeeds and
  connection errors only occur at task execution time (which is correct)
"""

import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from datetime import datetime
import threading
import logging

logger = logging.getLogger(__name__)

# Lazy imports for SQLAlchemy - only import when needed
_sqlalchemy_imported = False
_create_engine = None
_text = None
_sessionmaker = None


def _ensure_sqlalchemy():
    """Lazy import SQLAlchemy to avoid issues at DAG parse time."""
    global _sqlalchemy_imported, _create_engine, _text, _sessionmaker
    if not _sqlalchemy_imported:
        from sqlalchemy import create_engine, text
        from sqlalchemy.orm import sessionmaker
        _create_engine = create_engine
        _text = text
        _sessionmaker = sessionmaker
        _sqlalchemy_imported = True


# =============================================================================
# Data Classes for Type Safety
# =============================================================================

@dataclass
class FeedConfig:
    """Feed registry configuration."""
    feed_id: str
    feed_name: str
    source_type: str  # csv, parquet, json, database
    file_pattern: str
    file_delimiter: str = ","
    file_header: bool = True
    schedule_interval: str = "@daily"
    owner_email: str = ""
    team: str = ""
    retry_count: int = 3
    retry_delay_minutes: int = 5
    is_active: bool = True
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@dataclass
class ColumnConfig:
    """Column definition from metadata."""
    column_id: int
    feed_id: str
    column_name: str
    column_order: int
    source_type: str  # Original type in source
    target_type: str  # Target type for Silver layer
    is_primary_key: bool = False
    is_nullable: bool = True
    default_value: Optional[str] = None
    transform_expression: Optional[str] = None  # SQL expression for Silver
    description: Optional[str] = None


@dataclass
class ValidationRule:
    """Data quality validation rule."""
    rule_id: int
    feed_id: str
    rule_name: str
    rule_type: str  # not_null, unique, range, pattern, custom
    column_name: Optional[str]  # None for table-level rules
    parameters: Dict[str, Any] = field(default_factory=dict)
    severity: str = "error"  # warning, error, critical
    action_on_failure: str = "reject"  # reject, warn, skip
    apply_in_layer: str = "silver"  # bronze, silver, gold
    is_active: bool = True


@dataclass
class TargetConfig:
    """Target table configuration per layer."""
    target_id: int
    feed_id: str
    layer: str  # bronze, silver, gold
    database_name: str
    schema_name: str
    table_name: str
    partition_columns: List[str] = field(default_factory=list)
    cluster_columns: List[str] = field(default_factory=list)
    write_mode: str = "append"  # append, overwrite, merge
    file_format: str = "parquet"
    compression: str = "snappy"


@dataclass
class FeedState:
    """Feed execution state for incremental loads."""
    state_id: int
    feed_id: str
    posting_date: str
    last_watermark: Optional[str] = None
    last_successful_run: Optional[datetime] = None
    last_row_count: int = 0
    status: str = "pending"  # pending, running, success, failed


# =============================================================================
# Metadata Reader Class
# =============================================================================

class MetadataReader:
    """
    Read metadata from PostgreSQL tables.

    Usage:
        reader = MetadataReader()
        feed = reader.get_feed_config("customer_transactions")
        columns = reader.get_columns("customer_transactions")
        rules = reader.get_validation_rules("customer_transactions")

    Thread-safe with per-feed caching.

    LAZY CONNECTION: The database connection is NOT established until
    the first query is made. This allows Airflow to parse DAG files
    without requiring a database connection.
    """

    _instance = None
    _lock = threading.Lock()

    def __new__(cls, database_url: Optional[str] = None):
        """Singleton pattern for shared connection pool."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
                    cls._instance._connected = False
        return cls._instance

    def __init__(self, database_url: Optional[str] = None):
        """
        Initialize MetadataReader.

        NOTE: Database connection is NOT established here.
        It is deferred to first query to allow DAG parsing without DB.
        """
        if self._initialized:
            return

        self._database_url_override = database_url
        self.engine = None
        self.SessionLocal = None

        # Cache for metadata (feed_id -> data)
        self._feed_cache: Dict[str, FeedConfig] = {}
        self._columns_cache: Dict[str, List[ColumnConfig]] = {}
        self._rules_cache: Dict[str, List[ValidationRule]] = {}
        self._targets_cache: Dict[str, Dict[str, TargetConfig]] = {}

        self._initialized = True

    def _ensure_connected(self) -> None:
        """
        Establish database connection if not already connected.

        WHY: Lazy connection allows DAG parsing without DB access.
        Called before any database operation.
        """
        if self._connected:
            return

        with self._lock:
            if self._connected:
                return

            _ensure_sqlalchemy()

            database_url = self._database_url_override or self._get_database_url()
            logger.info(f"Connecting to metadata database...")

            self.engine = _create_engine(
                database_url,
                pool_pre_ping=True,
                pool_size=5,
                max_overflow=10,
            )
            self.SessionLocal = _sessionmaker(bind=self.engine)
            self._connected = True
            logger.info("Metadata database connection established")

    def _get_database_url(self) -> str:
        """Build database URL from environment variables."""
        host = os.getenv("POSTGRES_HOST", "localhost")
        port = os.getenv("POSTGRES_PORT", "5432")
        user = os.getenv("POSTGRES_USER", "admin")
        password = os.getenv("POSTGRES_PASSWORD", "admin123")
        database = os.getenv("POSTGRES_DB", "agentdb")
        return f"postgresql://{user}:{password}@{host}:{port}/{database}"

    def clear_cache(self, feed_id: Optional[str] = None) -> None:
        """Clear cached metadata. Call after metadata updates."""
        if feed_id:
            self._feed_cache.pop(feed_id, None)
            self._columns_cache.pop(feed_id, None)
            self._rules_cache.pop(feed_id, None)
            self._targets_cache.pop(feed_id, None)
        else:
            self._feed_cache.clear()
            self._columns_cache.clear()
            self._rules_cache.clear()
            self._targets_cache.clear()

    # =========================================================================
    # Feed Configuration
    # =========================================================================

    def get_feed_config(self, feed_id: str) -> Optional[FeedConfig]:
        """
        Get feed configuration from feed_registry table.

        Args:
            feed_id: Unique feed identifier

        Returns:
            FeedConfig dataclass or None if not found
        """
        if feed_id in self._feed_cache:
            return self._feed_cache[feed_id]

        self._ensure_connected()
        query = _text("""
            SELECT
                feed_id, feed_name, source_type, file_pattern,
                file_delimiter, file_header, schedule_interval,
                owner_email, team, retry_count, retry_delay_minutes,
                is_active, created_at, updated_at
            FROM feed_registry
            WHERE feed_id = :feed_id AND is_active = TRUE
        """)

        with self.SessionLocal() as session:
            result = session.execute(query, {"feed_id": feed_id}).fetchone()

            if not result:
                return None

            feed = FeedConfig(
                feed_id=result[0],
                feed_name=result[1],
                source_type=result[2],
                file_pattern=result[3],
                file_delimiter=result[4] or ",",
                file_header=result[5] if result[5] is not None else True,
                schedule_interval=result[6] or "@daily",
                owner_email=result[7] or "",
                team=result[8] or "",
                retry_count=result[9] or 3,
                retry_delay_minutes=result[10] or 5,
                is_active=result[11],
                created_at=result[12],
                updated_at=result[13],
            )

            self._feed_cache[feed_id] = feed
            return feed

    # =========================================================================
    # Column Definitions
    # =========================================================================

    def get_columns(self, feed_id: str) -> List[ColumnConfig]:
        """
        Get column definitions for a feed.

        Args:
            feed_id: Unique feed identifier

        Returns:
            List of ColumnConfig ordered by column_order
        """
        if feed_id in self._columns_cache:
            return self._columns_cache[feed_id]

        self._ensure_connected()
        query = _text("""
            SELECT
                column_id, feed_id, column_name, column_order,
                source_type, target_type, is_primary_key, is_nullable,
                default_value, transform_expression, description
            FROM feed_columns
            WHERE feed_id = :feed_id
            ORDER BY column_order ASC
        """)

        with self.SessionLocal() as session:
            results = session.execute(query, {"feed_id": feed_id}).fetchall()

            columns = [
                ColumnConfig(
                    column_id=row[0],
                    feed_id=row[1],
                    column_name=row[2],
                    column_order=row[3],
                    source_type=row[4],
                    target_type=row[5],
                    is_primary_key=row[6] or False,
                    is_nullable=row[7] if row[7] is not None else True,
                    default_value=row[8],
                    transform_expression=row[9],
                    description=row[10],
                )
                for row in results
            ]

            self._columns_cache[feed_id] = columns
            return columns

    def get_primary_keys(self, feed_id: str) -> List[str]:
        """Get primary key column names for a feed."""
        columns = self.get_columns(feed_id)
        return [c.column_name for c in columns if c.is_primary_key]

    # =========================================================================
    # Validation Rules
    # =========================================================================

    def get_validation_rules(
        self,
        feed_id: str,
        layer: Optional[str] = None
    ) -> List[ValidationRule]:
        """
        Get validation rules for a feed.

        Args:
            feed_id: Unique feed identifier
            layer: Optional filter by layer (bronze, silver, gold)

        Returns:
            List of ValidationRule for the feed
        """
        cache_key = f"{feed_id}_{layer or 'all'}"
        if cache_key in self._rules_cache:
            return self._rules_cache[cache_key]

        self._ensure_connected()
        query = _text("""
            SELECT
                rule_id, feed_id, rule_name, rule_type, column_name,
                parameters, severity, action_on_failure, apply_in_layer, is_active
            FROM feed_validation
            WHERE feed_id = :feed_id AND is_active = TRUE
            ORDER BY rule_id ASC
        """)

        with self.SessionLocal() as session:
            results = session.execute(query, {"feed_id": feed_id}).fetchall()

            rules = []
            for row in results:
                rule = ValidationRule(
                    rule_id=row[0],
                    feed_id=row[1],
                    rule_name=row[2],
                    rule_type=row[3],
                    column_name=row[4],
                    parameters=row[5] or {},
                    severity=row[6] or "error",
                    action_on_failure=row[7] or "reject",
                    apply_in_layer=row[8] or "silver",
                    is_active=row[9],
                )

                # Filter by layer if specified
                if layer is None or rule.apply_in_layer == layer:
                    rules.append(rule)

            self._rules_cache[cache_key] = rules
            return rules

    # =========================================================================
    # Target Configuration
    # =========================================================================

    def get_target_config(self, feed_id: str, layer: str) -> Optional[TargetConfig]:
        """
        Get target table configuration for a specific layer.

        Args:
            feed_id: Unique feed identifier
            layer: Layer name (bronze, silver, gold)

        Returns:
            TargetConfig for the layer or None
        """
        cache_key = feed_id
        if cache_key in self._targets_cache:
            return self._targets_cache[cache_key].get(layer)

        self._ensure_connected()
        query = _text("""
            SELECT
                target_id, feed_id, layer, database_name, schema_name,
                table_name, partition_columns, cluster_columns,
                write_mode, file_format, compression
            FROM feed_targets
            WHERE feed_id = :feed_id
        """)

        with self.SessionLocal() as session:
            results = session.execute(query, {"feed_id": feed_id}).fetchall()

            targets = {}
            for row in results:
                target = TargetConfig(
                    target_id=row[0],
                    feed_id=row[1],
                    layer=row[2],
                    database_name=row[3],
                    schema_name=row[4],
                    table_name=row[5],
                    partition_columns=row[6] or [],
                    cluster_columns=row[7] or [],
                    write_mode=row[8] or "append",
                    file_format=row[9] or "parquet",
                    compression=row[10] or "snappy",
                )
                targets[target.layer] = target

            self._targets_cache[cache_key] = targets
            return targets.get(layer)

    def get_all_targets(self, feed_id: str) -> Dict[str, TargetConfig]:
        """Get all target configurations for a feed."""
        # Trigger cache population
        self.get_target_config(feed_id, "bronze")
        return self._targets_cache.get(feed_id, {})

    # =========================================================================
    # Aggregation Rules (Gold Layer)
    # =========================================================================

    def get_aggregation_rules(self, feed_id: str) -> List[Dict[str, Any]]:
        """
        Get Gold layer aggregation rules for a feed.

        Args:
            feed_id: Unique feed identifier

        Returns:
            List of aggregation configurations
        """
        self._ensure_connected()
        query = _text("""
            SELECT
                aggregation_id, feed_id, aggregation_name,
                group_by_columns, aggregation_expressions,
                filter_condition, target_database, target_schema,
                target_table, partition_columns, write_mode,
                file_format, compression, is_active
            FROM feed_aggregations
            WHERE feed_id = :feed_id AND is_active = TRUE
            ORDER BY aggregation_id ASC
        """)

        with self.SessionLocal() as session:
            results = session.execute(query, {"feed_id": feed_id}).fetchall()

            aggregations = []
            for row in results:
                agg = {
                    "aggregation_id": row[0],
                    "feed_id": row[1],
                    "aggregation_name": row[2],
                    "group_by_columns": row[3] or [],
                    "aggregation_expressions": row[4] or {},
                    "filter_condition": row[5],
                    "target_config": {
                        "database_name": row[6] or "gold",
                        "schema_name": row[7] or "analytics",
                        "table_name": row[8],
                        "partition_columns": row[9] or ["_posting_date"],
                        "write_mode": row[10] or "overwrite",
                        "file_format": row[11] or "parquet",
                        "compression": row[12] or "snappy",
                    },
                    "is_active": row[13],
                }
                aggregations.append(agg)

            return aggregations

    # =========================================================================
    # Feed State
    # =========================================================================

    def get_feed_state(self, feed_id: str, posting_date: str) -> Optional[FeedState]:
        """
        Get feed execution state for a specific posting date.

        Args:
            feed_id: Unique feed identifier
            posting_date: Date string (YYYY-MM-DD)

        Returns:
            FeedState or None if not found
        """
        self._ensure_connected()
        query = _text("""
            SELECT
                state_id, feed_id, posting_date, last_watermark,
                last_successful_run, last_row_count, status
            FROM feed_state
            WHERE feed_id = :feed_id AND posting_date = :posting_date
        """)

        with self.SessionLocal() as session:
            result = session.execute(
                query,
                {"feed_id": feed_id, "posting_date": posting_date}
            ).fetchone()

            if not result:
                return None

            return FeedState(
                state_id=result[0],
                feed_id=result[1],
                posting_date=result[2],
                last_watermark=result[3],
                last_successful_run=result[4],
                last_row_count=result[5] or 0,
                status=result[6] or "pending",
            )

    def update_feed_state(
        self,
        feed_id: str,
        posting_date: str,
        status: str,
        row_count: int = 0,
        watermark: Optional[str] = None,
    ) -> None:
        """Update or insert feed state."""
        self._ensure_connected()
        upsert_query = _text("""
            INSERT INTO feed_state (feed_id, posting_date, status, last_row_count, last_watermark, last_successful_run)
            VALUES (:feed_id, :posting_date, :status, :row_count, :watermark,
                    CASE WHEN :status = 'success' THEN NOW() ELSE NULL END)
            ON CONFLICT (feed_id, posting_date)
            DO UPDATE SET
                status = :status,
                last_row_count = :row_count,
                last_watermark = COALESCE(:watermark, feed_state.last_watermark),
                last_successful_run = CASE WHEN :status = 'success' THEN NOW() ELSE feed_state.last_successful_run END
        """)

        with self.SessionLocal() as session:
            session.execute(upsert_query, {
                "feed_id": feed_id,
                "posting_date": posting_date,
                "status": status,
                "row_count": row_count,
                "watermark": watermark,
            })
            session.commit()


# =============================================================================
# Convenience Function
# =============================================================================

def get_metadata_reader() -> MetadataReader:
    """Get singleton MetadataReader instance."""
    return MetadataReader()
