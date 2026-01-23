"""
PostgreSQL metadata repository with connection pooling.

This module provides the data access layer for the metadata database.
All database operations should go through this repository.

RULES:
1. All database access goes through this class
2. Use context managers for sessions
3. All queries must be parameterized (no string concatenation)
4. Log all database operations
5. Handle connection errors gracefully with retries
"""

import uuid
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, Generator, List, Optional

from sqlalchemy import create_engine, select, update, and_, func
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError, OperationalError, SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import QueuePool
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from src.config.settings import Settings, get_settings
from src.metadata.models import (
    Base,
    Pipeline,
    SchemaVersion,
    TransformationRule,
    DataQualityRule,
    Execution,
    AuditLog,
    ApprovalRequest,
)
from src.utils.exceptions import (
    DatabaseConnectionError,
    MetadataError,
    QueryError,
)
from src.utils.logging import get_logger


logger = get_logger(__name__)


# =============================================================================
# Repository Class
# =============================================================================


class MetadataRepository:
    """
    PostgreSQL metadata repository.

    Provides data access methods for all metadata entities.
    Uses connection pooling and context managers for session management.

    Usage:
        repo = MetadataRepository()
        with repo.get_session() as session:
            pipeline = repo.get_pipeline_by_name("my_pipeline", "dev", session)
    """

    def __init__(
        self,
        connection_string: Optional[str] = None,
        settings: Optional[Settings] = None,
    ) -> None:
        """
        Initialize repository with database connection.

        Args:
            connection_string: PostgreSQL connection string (optional)
            settings: Settings instance (optional, uses default if not provided)
        """
        self._settings = settings or get_settings()

        if connection_string:
            self._connection_string = connection_string
        else:
            self._connection_string = self._settings.get_database_url_str()

        self._engine: Optional[Engine] = None
        self._session_factory: Optional[sessionmaker[Session]] = None

        logger.info(
            "MetadataRepository initialized",
            pool_size=self._settings.database_pool_size,
        )

    # =========================================================================
    # Connection Management
    # =========================================================================

    def _get_engine(self) -> Engine:
        """
        Get or create database engine with connection pooling.

        Returns:
            SQLAlchemy Engine instance
        """
        if self._engine is None:
            logger.debug("Creating database engine")

            self._engine = create_engine(
                self._connection_string,
                poolclass=QueuePool,
                pool_pre_ping=True,  # Verify connections before use
                pool_size=self._settings.database_pool_size,
                max_overflow=self._settings.database_max_overflow,
                pool_timeout=self._settings.database_pool_timeout,
                echo=self._settings.database_echo,
            )

            self._session_factory = sessionmaker(
                bind=self._engine,
                autocommit=False,
                autoflush=False,
                expire_on_commit=False,
            )

            logger.info("Database engine created successfully")

        return self._engine

    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """
        Get database session with automatic cleanup.

        Yields:
            SQLAlchemy Session instance

        Raises:
            DatabaseConnectionError: If connection fails
            MetadataError: If session operations fail

        Usage:
            with repo.get_session() as session:
                # Use session
                pass
        """
        if self._session_factory is None:
            self._get_engine()

        session = self._session_factory()
        try:
            yield session
            session.commit()
            logger.debug("Session committed successfully")
        except OperationalError as e:
            session.rollback()
            logger.error("Database connection error", error=str(e))
            raise DatabaseConnectionError(
                message="Failed to connect to database",
                cause=e,
            )
        except SQLAlchemyError as e:
            session.rollback()
            logger.error("Database error", error=str(e))
            raise MetadataError(
                message=f"Database operation failed: {str(e)}",
                cause=e,
            )
        except Exception as e:
            session.rollback()
            logger.error("Unexpected error in session", error=str(e))
            raise
        finally:
            session.close()

    def create_tables(self) -> None:
        """Create all database tables (for development/testing)."""
        engine = self._get_engine()
        Base.metadata.create_all(engine)
        logger.info("Database tables created")

    def drop_tables(self) -> None:
        """Drop all database tables (for development/testing only!)."""
        engine = self._get_engine()
        Base.metadata.drop_all(engine)
        logger.warning("Database tables dropped")

    def health_check(self) -> bool:
        """
        Check database connectivity.

        Returns:
            True if database is accessible
        """
        try:
            with self.get_session() as session:
                session.execute(select(func.now()))
            return True
        except Exception as e:
            logger.error("Health check failed", error=str(e))
            return False

    # =========================================================================
    # Pipeline Operations
    # =========================================================================

    def get_pipeline_by_name(
        self,
        pipeline_name: str,
        environment: str,
        session: Optional[Session] = None,
    ) -> Optional[Pipeline]:
        """
        Get pipeline by name and environment.

        Args:
            pipeline_name: Unique pipeline identifier
            environment: Target environment (dev/qa/prod)
            session: Optional existing session

        Returns:
            Pipeline model or None if not found
        """
        def _query(sess: Session) -> Optional[Pipeline]:
            stmt = select(Pipeline).where(
                and_(
                    Pipeline.pipeline_name == pipeline_name,
                    Pipeline.environment == environment,
                )
            )
            result = sess.execute(stmt)
            return result.scalar_one_or_none()

        if session:
            return _query(session)

        with self.get_session() as sess:
            pipeline = _query(sess)
            if pipeline:
                # Detach from session for return
                sess.expunge(pipeline)
            return pipeline

    def get_pipeline_by_id(
        self,
        pipeline_id: int,
        session: Optional[Session] = None,
    ) -> Optional[Pipeline]:
        """
        Get pipeline by ID.

        Args:
            pipeline_id: Pipeline primary key
            session: Optional existing session

        Returns:
            Pipeline model or None if not found
        """
        def _query(sess: Session) -> Optional[Pipeline]:
            return sess.get(Pipeline, pipeline_id)

        if session:
            return _query(session)

        with self.get_session() as sess:
            pipeline = _query(sess)
            if pipeline:
                sess.expunge(pipeline)
            return pipeline

    def insert_pipeline(
        self,
        pipeline_data: Dict[str, Any],
        session: Optional[Session] = None,
    ) -> int:
        """
        Insert new pipeline and return ID.

        Args:
            pipeline_data: Pipeline fields dict
            session: Optional existing session

        Returns:
            New pipeline ID

        Raises:
            MetadataError: If insert fails
        """
        def _insert(sess: Session) -> int:
            pipeline = Pipeline(**pipeline_data)
            sess.add(pipeline)
            sess.flush()  # Get the ID
            logger.info(
                "Pipeline inserted",
                pipeline_id=pipeline.pipeline_id,
                pipeline_name=pipeline.pipeline_name,
            )
            return pipeline.pipeline_id

        try:
            if session:
                return _insert(session)

            with self.get_session() as sess:
                return _insert(sess)
        except IntegrityError as e:
            logger.error("Pipeline insert failed - duplicate", error=str(e))
            raise MetadataError(
                message="Pipeline already exists with this name and environment",
                operation="insert",
                entity_type="pipeline",
                is_retryable=False,
                cause=e,
            )

    def update_pipeline(
        self,
        pipeline_id: int,
        updates: Dict[str, Any],
        session: Optional[Session] = None,
    ) -> bool:
        """
        Update pipeline fields.

        Args:
            pipeline_id: Pipeline primary key
            updates: Fields to update
            session: Optional existing session

        Returns:
            True if updated, False if not found
        """
        def _update(sess: Session) -> bool:
            stmt = (
                update(Pipeline)
                .where(Pipeline.pipeline_id == pipeline_id)
                .values(**updates, updated_at=func.now())
            )
            result = sess.execute(stmt)
            return result.rowcount > 0

        if session:
            return _update(session)

        with self.get_session() as sess:
            return _update(sess)

    # =========================================================================
    # Schema Version Operations
    # =========================================================================

    def get_current_schema(
        self,
        pipeline_id: int,
        session: Optional[Session] = None,
    ) -> Optional[SchemaVersion]:
        """
        Get current schema version for a pipeline.

        Args:
            pipeline_id: Pipeline primary key
            session: Optional existing session

        Returns:
            Current SchemaVersion or None
        """
        def _query(sess: Session) -> Optional[SchemaVersion]:
            stmt = select(SchemaVersion).where(
                and_(
                    SchemaVersion.pipeline_id == pipeline_id,
                    SchemaVersion.is_current == True,
                )
            )
            result = sess.execute(stmt)
            return result.scalar_one_or_none()

        if session:
            return _query(session)

        with self.get_session() as sess:
            schema = _query(sess)
            if schema:
                sess.expunge(schema)
            return schema

    def get_schema_history(
        self,
        pipeline_id: int,
        limit: int = 10,
        session: Optional[Session] = None,
    ) -> List[SchemaVersion]:
        """
        Get schema version history for a pipeline.

        Args:
            pipeline_id: Pipeline primary key
            limit: Maximum versions to return
            session: Optional existing session

        Returns:
            List of SchemaVersion models (newest first)
        """
        def _query(sess: Session) -> List[SchemaVersion]:
            stmt = (
                select(SchemaVersion)
                .where(SchemaVersion.pipeline_id == pipeline_id)
                .order_by(SchemaVersion.version.desc())
                .limit(limit)
            )
            result = sess.execute(stmt)
            return list(result.scalars().all())

        if session:
            return _query(session)

        with self.get_session() as sess:
            schemas = _query(sess)
            for schema in schemas:
                sess.expunge(schema)
            return schemas

    def insert_schema_version(
        self,
        schema_data: Dict[str, Any],
        session: Optional[Session] = None,
    ) -> int:
        """
        Insert new schema version.

        Automatically sets previous version's is_current to False.

        Args:
            schema_data: Schema version fields
            session: Optional existing session

        Returns:
            New schema version ID
        """
        def _insert(sess: Session) -> int:
            pipeline_id = schema_data["pipeline_id"]

            # Deactivate current version
            sess.execute(
                update(SchemaVersion)
                .where(
                    and_(
                        SchemaVersion.pipeline_id == pipeline_id,
                        SchemaVersion.is_current == True,
                    )
                )
                .values(is_current=False, effective_to=func.now())
            )

            # Insert new version
            schema = SchemaVersion(**schema_data)
            sess.add(schema)
            sess.flush()

            logger.info(
                "Schema version inserted",
                schema_version_id=schema.schema_version_id,
                pipeline_id=pipeline_id,
                version=schema.version,
            )
            return schema.schema_version_id

        if session:
            return _insert(session)

        with self.get_session() as sess:
            return _insert(sess)

    # =========================================================================
    # Execution Operations
    # =========================================================================

    def insert_execution(
        self,
        execution_data: Dict[str, Any],
        session: Optional[Session] = None,
    ) -> int:
        """
        Insert new execution record.

        Args:
            execution_data: Execution fields
            session: Optional existing session

        Returns:
            New execution ID
        """
        def _insert(sess: Session) -> int:
            execution = Execution(**execution_data)
            sess.add(execution)
            sess.flush()

            logger.info(
                "Execution inserted",
                execution_id=execution.execution_id,
                request_id=execution.request_id,
            )
            return execution.execution_id

        if session:
            return _insert(session)

        with self.get_session() as sess:
            return _insert(sess)

    def update_execution_status(
        self,
        execution_id: int,
        status: str,
        error_message: Optional[str] = None,
        completed_at: Optional[datetime] = None,
        session: Optional[Session] = None,
    ) -> bool:
        """
        Update execution status.

        Args:
            execution_id: Execution primary key
            status: New status
            error_message: Error message if failed
            completed_at: Completion timestamp
            session: Optional existing session

        Returns:
            True if updated
        """
        def _update(sess: Session) -> bool:
            updates: Dict[str, Any] = {"execution_status": status}
            if error_message:
                updates["error_message"] = error_message
            if completed_at:
                updates["completed_at"] = completed_at

            stmt = (
                update(Execution)
                .where(Execution.execution_id == execution_id)
                .values(**updates)
            )
            result = sess.execute(stmt)

            logger.info(
                "Execution status updated",
                execution_id=execution_id,
                status=status,
            )
            return result.rowcount > 0

        if session:
            return _update(session)

        with self.get_session() as sess:
            return _update(sess)

    def get_execution_by_request_id(
        self,
        request_id: str,
        session: Optional[Session] = None,
    ) -> Optional[Execution]:
        """
        Get execution by request ID.

        Args:
            request_id: Request identifier
            session: Optional existing session

        Returns:
            Execution model or None
        """
        def _query(sess: Session) -> Optional[Execution]:
            stmt = select(Execution).where(Execution.request_id == request_id)
            result = sess.execute(stmt)
            return result.scalar_one_or_none()

        if session:
            return _query(session)

        with self.get_session() as sess:
            execution = _query(sess)
            if execution:
                sess.expunge(execution)
            return execution

    # =========================================================================
    # Audit Log Operations
    # =========================================================================

    def insert_audit_log(
        self,
        audit_data: Dict[str, Any],
        session: Optional[Session] = None,
    ) -> str:
        """
        Insert audit log entry.

        Args:
            audit_data: Audit log fields (log_id is auto-generated if not provided)
            session: Optional existing session

        Returns:
            Log ID
        """
        def _insert(sess: Session) -> str:
            if "log_id" not in audit_data:
                audit_data["log_id"] = str(uuid.uuid4())

            audit_log = AuditLog(**audit_data)
            sess.add(audit_log)

            logger.debug(
                "Audit log inserted",
                log_id=audit_log.log_id,
                agent_name=audit_log.agent_name,
                action=audit_log.action,
            )
            return audit_log.log_id

        if session:
            return _insert(session)

        with self.get_session() as sess:
            return _insert(sess)

    def get_audit_logs_for_request(
        self,
        request_id: str,
        limit: int = 100,
        session: Optional[Session] = None,
    ) -> List[AuditLog]:
        """
        Get audit logs for a request.

        Args:
            request_id: Request identifier
            limit: Maximum logs to return
            session: Optional existing session

        Returns:
            List of AuditLog models
        """
        def _query(sess: Session) -> List[AuditLog]:
            stmt = (
                select(AuditLog)
                .where(AuditLog.request_id == request_id)
                .order_by(AuditLog.created_at.desc())
                .limit(limit)
            )
            result = sess.execute(stmt)
            return list(result.scalars().all())

        if session:
            return _query(session)

        with self.get_session() as sess:
            logs = _query(sess)
            for log in logs:
                sess.expunge(log)
            return logs

    # =========================================================================
    # Approval Request Operations
    # =========================================================================

    def insert_approval_request(
        self,
        approval_data: Dict[str, Any],
        session: Optional[Session] = None,
    ) -> int:
        """
        Insert approval request.

        Args:
            approval_data: Approval request fields
            session: Optional existing session

        Returns:
            Approval request ID
        """
        def _insert(sess: Session) -> int:
            approval = ApprovalRequest(**approval_data)
            sess.add(approval)
            sess.flush()

            logger.info(
                "Approval request inserted",
                approval_id=approval.approval_id,
                approval_type=approval.approval_type,
            )
            return approval.approval_id

        if session:
            return _insert(session)

        with self.get_session() as sess:
            return _insert(sess)

    def update_approval_status(
        self,
        approval_id: int,
        status: str,
        approver: Optional[str] = None,
        comments: Optional[str] = None,
        session: Optional[Session] = None,
    ) -> bool:
        """
        Update approval request status.

        Args:
            approval_id: Approval request primary key
            status: New status
            approver: Who approved/rejected
            comments: Approval comments
            session: Optional existing session

        Returns:
            True if updated
        """
        def _update(sess: Session) -> bool:
            updates: Dict[str, Any] = {"status": status}
            if approver:
                updates["approver"] = approver
            if comments:
                updates["comments"] = comments
            if status in ("approved", "rejected"):
                updates["approved_at"] = func.now()

            stmt = (
                update(ApprovalRequest)
                .where(ApprovalRequest.approval_id == approval_id)
                .values(**updates)
            )
            result = sess.execute(stmt)

            logger.info(
                "Approval status updated",
                approval_id=approval_id,
                status=status,
            )
            return result.rowcount > 0

        if session:
            return _update(session)

        with self.get_session() as sess:
            return _update(sess)

    def get_pending_approval(
        self,
        execution_id: int,
        session: Optional[Session] = None,
    ) -> Optional[ApprovalRequest]:
        """
        Get pending approval for an execution.

        Args:
            execution_id: Execution primary key
            session: Optional existing session

        Returns:
            Pending ApprovalRequest or None
        """
        def _query(sess: Session) -> Optional[ApprovalRequest]:
            stmt = select(ApprovalRequest).where(
                and_(
                    ApprovalRequest.execution_id == execution_id,
                    ApprovalRequest.status == "pending",
                )
            )
            result = sess.execute(stmt)
            return result.scalar_one_or_none()

        if session:
            return _query(session)

        with self.get_session() as sess:
            approval = _query(sess)
            if approval:
                sess.expunge(approval)
            return approval


# =============================================================================
# Repository Factory
# =============================================================================


_repository_instance: Optional[MetadataRepository] = None


def get_repository() -> MetadataRepository:
    """
    Get singleton repository instance.

    Returns:
        MetadataRepository instance
    """
    global _repository_instance
    if _repository_instance is None:
        _repository_instance = MetadataRepository()
    return _repository_instance


def reset_repository() -> None:
    """Reset repository instance (for testing)."""
    global _repository_instance
    _repository_instance = None
