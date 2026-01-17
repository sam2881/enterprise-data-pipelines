"""
Audit Logger - Enterprise audit trail for all pipeline operations.

================================================================================
WHY: Every pipeline operation must be auditable for:
     1. Compliance (SOX, GDPR, PCI-DSS)
     2. Debugging failed pipelines
     3. Performance monitoring
     4. Data lineage tracking
     5. Row count reconciliation
     6. SLA tracking

HOW: Writes to PostgreSQL audit tables with:
     - Group run ID (batch-level tracking)
     - Individual run ID (file-level tracking)
     - Layer progression (source → bronze → silver → gold)
     - Row counts and validation metrics
     - Error details for failures

DESIGN DECISIONS:
1. LAZY DB CONNECTION - No connection at import time (Airflow-safe)
2. Thread-safe singleton for shared connections
3. Log at start and end of each layer
4. Capture row counts for reconciliation
5. Store validation results summary
6. Include timing information
================================================================================
"""

import os
import json
import uuid
import logging
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, asdict

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


@dataclass
class AuditRecord:
    """Single audit log record."""
    audit_id: str
    feed_id: str
    run_id: str
    posting_date: str
    layer: str
    status: str  # started, running, success, failed
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration_seconds: Optional[int] = None
    input_row_count: int = 0
    output_row_count: int = 0
    rejected_row_count: int = 0
    validation_pass_rate: Optional[float] = None
    error_message: Optional[str] = None
    error_details: Optional[Dict] = None
    metrics: Optional[Dict] = None
    created_at: Optional[datetime] = None


class AuditLogger:
    """
    Log pipeline execution metrics to database.

    LAZY CONNECTION: Database connection is NOT established at init time.
    This allows Airflow to parse DAGs without requiring DB connectivity.

    Usage:
        logger = AuditLogger()

        # At start of layer
        run_id = logger.log_start(feed_id, "silver", posting_date)

        # At end of layer
        logger.log_complete(
            feed_id, run_id, "silver",
            status="success",
            input_rows=1000,
            output_rows=998,
            rejected_rows=2,
        )
    """

    _instance = None
    _lock = threading.Lock()

    def __new__(cls, database_url: Optional[str] = None):
        """Singleton pattern with thread safety."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
                    cls._instance._connected = False
        return cls._instance

    def __init__(self, database_url: Optional[str] = None):
        """
        Initialize AuditLogger.

        NOTE: Database connection is NOT established here.
        It is deferred to first logging call to allow DAG parsing without DB.
        """
        if self._initialized:
            return

        self._database_url_override = database_url
        self.engine = None
        self.SessionLocal = None
        self._initialized = True

    def _ensure_connected(self) -> None:
        """Establish database connection if not already connected."""
        if self._connected:
            return

        with self._lock:
            if self._connected:
                return

            _ensure_sqlalchemy()

            database_url = self._database_url_override or self._get_database_url()
            logger.info("Connecting to audit database...")

            self.engine = _create_engine(
                database_url,
                pool_pre_ping=True,
                pool_size=3,
                max_overflow=5,
            )
            self.SessionLocal = _sessionmaker(bind=self.engine)
            self._connected = True
            logger.info("Audit database connection established")

    def _get_database_url(self) -> str:
        """Build database URL from environment variables."""
        host = os.getenv("POSTGRES_HOST", "localhost")
        port = os.getenv("POSTGRES_PORT", "5432")
        user = os.getenv("POSTGRES_USER", "admin")
        password = os.getenv("POSTGRES_PASSWORD", "admin123")
        database = os.getenv("POSTGRES_DB", "agentdb")
        return f"postgresql://{user}:{password}@{host}:{port}/{database}"

    def generate_run_id(self, feed_id: str, posting_date: str) -> str:
        """Generate unique run ID."""
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        short_uuid = str(uuid.uuid4())[:8]
        return f"{feed_id}_{posting_date}_{timestamp}_{short_uuid}"

    # =========================================================================
    # Logging Methods
    # =========================================================================

    def log_start(
        self,
        feed_id: str,
        layer: str,
        posting_date: str,
        run_id: Optional[str] = None,
    ) -> str:
        """
        Log start of layer processing.

        Args:
            feed_id: Feed identifier
            layer: Layer name (bronze, silver, gold)
            posting_date: Processing date
            run_id: Optional existing run ID (for same run across layers)

        Returns:
            run_id for tracking
        """
        self._ensure_connected()
        run_id = run_id or self.generate_run_id(feed_id, posting_date)
        audit_id = str(uuid.uuid4())

        insert_query = _text("""
            INSERT INTO feed_audit_log (
                audit_id, feed_id, run_id, posting_date, layer,
                status, started_at, created_at
            ) VALUES (
                :audit_id, :feed_id, :run_id, :posting_date, :layer,
                'started', :started_at, :created_at
            )
        """)

        now = datetime.utcnow()
        with self.SessionLocal() as session:
            session.execute(insert_query, {
                "audit_id": audit_id,
                "feed_id": feed_id,
                "run_id": run_id,
                "posting_date": posting_date,
                "layer": layer,
                "started_at": now,
                "created_at": now,
            })
            session.commit()

        return run_id

    def log_complete(
        self,
        feed_id: str,
        run_id: str,
        layer: str,
        status: str,
        input_rows: int = 0,
        output_rows: int = 0,
        rejected_rows: int = 0,
        validation_pass_rate: Optional[float] = None,
        error_message: Optional[str] = None,
        error_details: Optional[Dict] = None,
        metrics: Optional[Dict] = None,
    ) -> None:
        """
        Log completion of layer processing.

        Args:
            feed_id: Feed identifier
            run_id: Run ID from log_start
            layer: Layer name
            status: Final status (success, failed)
            input_rows: Rows read
            output_rows: Rows written
            rejected_rows: Rows rejected by validation
            validation_pass_rate: Overall validation pass rate
            error_message: Error message if failed
            error_details: Additional error details
            metrics: Custom metrics (timing, etc.)
        """
        self._ensure_connected()
        update_query = _text("""
            UPDATE feed_audit_log
            SET
                status = :status,
                completed_at = :completed_at,
                duration_seconds = EXTRACT(EPOCH FROM (:completed_at - started_at))::INT,
                input_row_count = :input_rows,
                output_row_count = :output_rows,
                rejected_row_count = :rejected_rows,
                validation_pass_rate = :validation_pass_rate,
                error_message = :error_message,
                error_details = :error_details,
                metrics = :metrics
            WHERE feed_id = :feed_id
              AND run_id = :run_id
              AND layer = :layer
              AND status = 'started'
        """)

        now = datetime.utcnow()
        with self.SessionLocal() as session:
            session.execute(update_query, {
                "feed_id": feed_id,
                "run_id": run_id,
                "layer": layer,
                "status": status,
                "completed_at": now,
                "input_rows": input_rows,
                "output_rows": output_rows,
                "rejected_rows": rejected_rows,
                "validation_pass_rate": validation_pass_rate,
                "error_message": error_message,
                "error_details": json.dumps(error_details) if error_details else None,
                "metrics": json.dumps(metrics) if metrics else None,
            })
            session.commit()

    def log_error(
        self,
        feed_id: str,
        run_id: str,
        layer: str,
        error_message: str,
        error_details: Optional[Dict] = None,
    ) -> None:
        """Convenience method to log failure."""
        self.log_complete(
            feed_id=feed_id,
            run_id=run_id,
            layer=layer,
            status="failed",
            error_message=error_message,
            error_details=error_details,
        )

    # =========================================================================
    # Query Methods
    # =========================================================================

    def get_last_successful_run(
        self,
        feed_id: str,
        layer: str,
    ) -> Optional[AuditRecord]:
        """Get most recent successful run for a feed/layer."""
        self._ensure_connected()
        query = _text("""
            SELECT
                audit_id, feed_id, run_id, posting_date, layer, status,
                started_at, completed_at, duration_seconds,
                input_row_count, output_row_count, rejected_row_count,
                validation_pass_rate, error_message, error_details, metrics, created_at
            FROM feed_audit_log
            WHERE feed_id = :feed_id
              AND layer = :layer
              AND status = 'success'
            ORDER BY completed_at DESC
            LIMIT 1
        """)

        with self.SessionLocal() as session:
            result = session.execute(query, {"feed_id": feed_id, "layer": layer}).fetchone()
            if not result:
                return None

            return AuditRecord(
                audit_id=result[0],
                feed_id=result[1],
                run_id=result[2],
                posting_date=result[3],
                layer=result[4],
                status=result[5],
                started_at=result[6],
                completed_at=result[7],
                duration_seconds=result[8],
                input_row_count=result[9] or 0,
                output_row_count=result[10] or 0,
                rejected_row_count=result[11] or 0,
                validation_pass_rate=result[12],
                error_message=result[13],
                error_details=result[14],
                metrics=result[15],
                created_at=result[16],
            )

    def get_run_history(
        self,
        feed_id: str,
        limit: int = 10,
    ) -> List[AuditRecord]:
        """Get recent run history for a feed."""
        self._ensure_connected()
        query = _text("""
            SELECT
                audit_id, feed_id, run_id, posting_date, layer, status,
                started_at, completed_at, duration_seconds,
                input_row_count, output_row_count, rejected_row_count,
                validation_pass_rate, error_message, error_details, metrics, created_at
            FROM feed_audit_log
            WHERE feed_id = :feed_id
            ORDER BY created_at DESC
            LIMIT :limit
        """)

        with self.SessionLocal() as session:
            results = session.execute(query, {"feed_id": feed_id, "limit": limit}).fetchall()

            return [
                AuditRecord(
                    audit_id=row[0],
                    feed_id=row[1],
                    run_id=row[2],
                    posting_date=row[3],
                    layer=row[4],
                    status=row[5],
                    started_at=row[6],
                    completed_at=row[7],
                    duration_seconds=row[8],
                    input_row_count=row[9] or 0,
                    output_row_count=row[10] or 0,
                    rejected_row_count=row[11] or 0,
                    validation_pass_rate=row[12],
                    error_message=row[13],
                    error_details=row[14],
                    metrics=row[15],
                    created_at=row[16],
                )
                for row in results
            ]

    def get_row_count_for_reconciliation(
        self,
        feed_id: str,
        run_id: str,
    ) -> Dict[str, int]:
        """Get row counts per layer for reconciliation."""
        self._ensure_connected()
        query = _text("""
            SELECT layer, output_row_count
            FROM feed_audit_log
            WHERE feed_id = :feed_id AND run_id = :run_id AND status = 'success'
        """)

        with self.SessionLocal() as session:
            results = session.execute(query, {"feed_id": feed_id, "run_id": run_id}).fetchall()
            return {row[0]: row[1] or 0 for row in results}
