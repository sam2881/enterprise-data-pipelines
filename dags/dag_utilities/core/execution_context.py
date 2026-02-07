"""
APEX Data Agent - Execution Context

Manages pipeline execution context and state throughout DAG runs.
Provides a centralized way to pass context between tasks.
"""

from datetime import datetime
from typing import Any, Dict, Optional
from dataclasses import dataclass, field
import uuid


@dataclass
class ExecutionContext:
    """
    Pipeline execution context.

    This class manages the state and context for a single pipeline execution.
    It tracks the execution ID, feed information, and runtime parameters.
    """

    feed_id: str
    contract_id: str
    execution_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    execution_date: datetime = field(default_factory=datetime.utcnow)
    reporting_date: Optional[datetime] = None  # Business date (may differ from execution_date for backfills)
    dag_run_id: Optional[str] = None
    trigger_type: str = "scheduled"

    # Runtime state
    current_zone: str = "INIT"
    source_file: Optional[str] = None
    records_read: int = 0
    records_written: int = 0
    records_rejected: int = 0

    # Parameters
    parameters: Dict[str, Any] = field(default_factory=dict)

    # Flags
    is_incremental: bool = False
    watermark_value: Optional[str] = None

    def update_zone(self, zone: str) -> None:
        """Update current processing zone."""
        self.current_zone = zone

    def update_metrics(
        self,
        records_read: int = 0,
        records_written: int = 0,
        records_rejected: int = 0,
    ) -> None:
        """Update processing metrics."""
        self.records_read += records_read
        self.records_written += records_written
        self.records_rejected += records_rejected

    def to_dict(self) -> Dict[str, Any]:
        """Convert context to dictionary for serialization."""
        reporting = self.reporting_date or self.execution_date
        return {
            "execution_id": self.execution_id,
            "feed_id": self.feed_id,
            "contract_id": self.contract_id,
            "execution_date": self.execution_date.isoformat(),
            "reporting_date": reporting.isoformat(),
            "dag_run_id": self.dag_run_id,
            "trigger_type": self.trigger_type,
            "current_zone": self.current_zone,
            "source_file": self.source_file,
            "records_read": self.records_read,
            "records_written": self.records_written,
            "records_rejected": self.records_rejected,
            "parameters": self.parameters,
            "is_incremental": self.is_incremental,
            "watermark_value": self.watermark_value,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ExecutionContext":
        """Create context from dictionary."""
        ctx = cls(
            feed_id=data["feed_id"],
            contract_id=data["contract_id"],
            execution_id=data.get("execution_id", str(uuid.uuid4())),
        )
        ctx.dag_run_id = data.get("dag_run_id")
        ctx.trigger_type = data.get("trigger_type", "scheduled")
        ctx.current_zone = data.get("current_zone", "INIT")
        ctx.source_file = data.get("source_file")
        ctx.records_read = data.get("records_read", 0)
        ctx.records_written = data.get("records_written", 0)
        ctx.records_rejected = data.get("records_rejected", 0)
        ctx.parameters = data.get("parameters", {})
        ctx.is_incremental = data.get("is_incremental", False)
        ctx.watermark_value = data.get("watermark_value")

        if "reporting_date" in data:
            if isinstance(data["reporting_date"], str):
                ctx.reporting_date = datetime.fromisoformat(data["reporting_date"])
            else:
                ctx.reporting_date = data["reporting_date"]

        if "execution_date" in data:
            if isinstance(data["execution_date"], str):
                ctx.execution_date = datetime.fromisoformat(data["execution_date"])
            else:
                ctx.execution_date = data["execution_date"]

        return ctx

    @classmethod
    def from_airflow_context(
        cls,
        feed_id: str,
        contract_id: str,
        airflow_context: Dict[str, Any],
    ) -> "ExecutionContext":
        """Create context from Airflow task context."""
        return cls(
            feed_id=feed_id,
            contract_id=contract_id,
            dag_run_id=airflow_context.get("dag_run", {}).run_id
            if airflow_context.get("dag_run")
            else None,
            execution_date=airflow_context.get("execution_date", datetime.utcnow()),
            trigger_type=airflow_context.get("dag_run", {}).run_type
            if airflow_context.get("dag_run")
            else "manual",
            parameters=airflow_context.get("params", {}),
        )


class ExecutionContextManager:
    """
    Manages execution contexts across tasks.

    Provides methods to store and retrieve context from XCom
    for sharing state between Airflow tasks.
    """

    @staticmethod
    def push_context(context: ExecutionContext, ti) -> None:
        """Push execution context to XCom."""
        ti.xcom_push(key="execution_context", value=context.to_dict())
        ti.xcom_push(key="execution_id", value=context.execution_id)

    @staticmethod
    def pull_context(ti) -> ExecutionContext:
        """Pull execution context from XCom."""
        data = ti.xcom_pull(key="execution_context")
        if data is None:
            raise ValueError("No execution context found in XCom")
        return ExecutionContext.from_dict(data)

    @staticmethod
    def get_execution_id(ti) -> str:
        """Get execution ID from XCom."""
        execution_id = ti.xcom_pull(key="execution_id")
        if execution_id is None:
            raise ValueError("No execution ID found in XCom")
        return execution_id
