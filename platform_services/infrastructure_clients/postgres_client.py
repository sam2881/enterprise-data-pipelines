"""PostgreSQL client utility for state persistence and audit logging"""
import os
from typing import Any, Dict, List, Optional
from sqlalchemy import create_engine, Column, String, Integer, JSON, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from datetime import datetime
import structlog

logger = structlog.get_logger()

Base = declarative_base()


class AgentEvent(Base):
    """Agent event audit log table"""
    __tablename__ = "agent_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    event_id = Column(String(100), unique=True, nullable=False, index=True)
    agent_name = Column(String(50), nullable=False, index=True)
    event_type = Column(String(50), nullable=False)
    payload = Column(JSON, nullable=True)
    event_metadata = Column(JSON, nullable=True)
    status = Column(String(20), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class WorkflowState(Base):
    """Workflow state persistence table"""
    __tablename__ = "workflow_states"

    id = Column(Integer, primary_key=True, autoincrement=True)
    workflow_id = Column(String(100), unique=True, nullable=False, index=True)
    state = Column(JSON, nullable=False)
    status = Column(String(20), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class PostgresClient:
    """PostgreSQL client for state persistence"""

    def __init__(self):
        host = os.getenv("POSTGRES_HOST", "postgres")
        port = os.getenv("POSTGRES_PORT", "5432")
        user = os.getenv("POSTGRES_USER", "admin")
        password = os.getenv("POSTGRES_PASSWORD", "admin123")
        db = os.getenv("POSTGRES_DB", "agentdb")

        self.connection_string = f"postgresql://{user}:{password}@{host}:{port}/{db}"
        self.engine = create_engine(self.connection_string, pool_pre_ping=True)
        self.SessionLocal = sessionmaker(bind=self.engine)

        # Create tables
        Base.metadata.create_all(self.engine)
        logger.info("postgres_initialized", connection_string=f"postgresql://{user}:***@{host}:{port}/{db}")

    def get_session(self) -> Session:
        """Get database session"""
        return self.SessionLocal()

    def log_event(self, event_id: str, agent_name: str, event_type: str,
                  payload: Dict[str, Any], metadata: Optional[Dict[str, Any]] = None,
                  status: str = "pending"):
        """Log agent event to database"""
        try:
            session = self.get_session()
            event = AgentEvent(
                event_id=event_id,
                agent_name=agent_name,
                event_type=event_type,
                payload=payload,
                event_metadata=metadata or {},
                status=status
            )
            session.add(event)
            session.commit()
            logger.info("event_logged", event_id=event_id, agent=agent_name)
        except Exception as e:
            logger.error("event_log_error", error=str(e), event_id=event_id)
            session.rollback()
        finally:
            session.close()

    def update_event_status(self, event_id: str, status: str, metadata: Optional[Dict[str, Any]] = None):
        """Update event status"""
        try:
            session = self.get_session()
            event = session.query(AgentEvent).filter_by(event_id=event_id).first()
            if event:
                event.status = status
                if metadata:
                    event.event_metadata = {**event.event_metadata, **metadata}
                event.updated_at = datetime.utcnow()
                session.commit()
                logger.info("event_status_updated", event_id=event_id, status=status)
        except Exception as e:
            logger.error("event_update_error", error=str(e), event_id=event_id)
            session.rollback()
        finally:
            session.close()

    def save_workflow_state(self, workflow_id: str, state: Dict[str, Any], status: str = "active"):
        """Save workflow state"""
        try:
            session = self.get_session()
            workflow = session.query(WorkflowState).filter_by(workflow_id=workflow_id).first()
            if workflow:
                workflow.state = state
                workflow.status = status
                workflow.updated_at = datetime.utcnow()
            else:
                workflow = WorkflowState(
                    workflow_id=workflow_id,
                    state=state,
                    status=status
                )
                session.add(workflow)
            session.commit()
            logger.info("workflow_state_saved", workflow_id=workflow_id)
        except Exception as e:
            logger.error("workflow_save_error", error=str(e), workflow_id=workflow_id)
            session.rollback()
        finally:
            session.close()

    def get_workflow_state(self, workflow_id: str) -> Optional[Dict[str, Any]]:
        """Get workflow state"""
        try:
            session = self.get_session()
            workflow = session.query(WorkflowState).filter_by(workflow_id=workflow_id).first()
            return workflow.state if workflow else None
        except Exception as e:
            logger.error("workflow_get_error", error=str(e), workflow_id=workflow_id)
            return None
        finally:
            session.close()


# Global Postgres client instance (lazy initialized)
_postgres_client = None


def get_postgres_client() -> PostgresClient:
    """Get or create the global Postgres client."""
    global _postgres_client
    if _postgres_client is None:
        _postgres_client = PostgresClient()
    return _postgres_client


# Backward compatibility alias (lazy)
class _LazyPostgresClient:
    """Lazy proxy for backward compatibility."""
    def __getattr__(self, name):
        return getattr(get_postgres_client(), name)


postgres_client = _LazyPostgresClient()
