"""
APEX Data Agent - Metrics Collector

Collects and reports pipeline metrics.
"""

from typing import Dict, Any, Optional
from datetime import datetime
import json

from dag_utilities.core.metadata_client import MetadataClient


class MetricsCollector:
    """
    Collects and reports pipeline metrics.

    Tracks:
    - Record counts per zone
    - Processing times
    - Data volumes
    - Resource utilization
    """

    def __init__(self):
        """Initialize metrics collector."""
        self.metadata_client = MetadataClient()
        self._metrics: Dict[str, Any] = {}

    def start_timer(self, metric_name: str) -> None:
        """Start a timer for a metric."""
        self._metrics[f"{metric_name}_start"] = datetime.utcnow()

    def stop_timer(self, metric_name: str) -> float:
        """Stop a timer and return elapsed seconds."""
        start = self._metrics.get(f"{metric_name}_start")
        if start:
            elapsed = (datetime.utcnow() - start).total_seconds()
            self._metrics[f"{metric_name}_duration"] = elapsed
            return elapsed
        return 0.0

    def record_count(self, metric_name: str, count: int) -> None:
        """Record a count metric."""
        self._metrics[metric_name] = count

    def increment(self, metric_name: str, value: int = 1) -> None:
        """Increment a counter metric."""
        self._metrics[metric_name] = self._metrics.get(metric_name, 0) + value

    def record_bytes(self, metric_name: str, bytes_count: int) -> None:
        """Record bytes processed."""
        self._metrics[metric_name] = bytes_count

    def get_metric(self, metric_name: str) -> Any:
        """Get a metric value."""
        return self._metrics.get(metric_name)

    def get_all_metrics(self) -> Dict[str, Any]:
        """Get all collected metrics."""
        return self._metrics.copy()

    def flush_to_execution(self, execution_id: str) -> None:
        """Flush metrics to pipeline execution record."""
        records = self._metrics.get("total_records_processed", 0)
        bytes_processed = self._metrics.get("total_bytes_processed", 0)

        self.metadata_client.update_execution_metrics(
            execution_id=execution_id,
            records_processed=records,
            bytes_processed=bytes_processed,
        )

    def log_cost(
        self,
        execution_id: str,
        resource_type: str,
        quantity: float,
        unit: str,
        unit_cost: float,
    ) -> None:
        """Log resource usage and cost."""
        import uuid

        total_cost = quantity * unit_cost

        query = """
            INSERT INTO execution_cost_log (
                cost_log_id, execution_id, resource_type,
                quantity, unit, unit_cost, total_cost, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        with self.metadata_client.conn.cursor() as cur:
            cur.execute(query, [
                str(uuid.uuid4()),
                execution_id,
                resource_type,
                quantity,
                unit,
                unit_cost,
                total_cost,
                datetime.utcnow(),
            ])
            self.metadata_client.conn.commit()

    def log_spark_metrics(
        self,
        execution_id: str,
        application_id: str,
        metrics: Dict[str, Any],
    ) -> None:
        """Log Spark job metrics."""
        # Calculate estimated cost
        core_hours = metrics.get("executor_run_time_ms", 0) / 3600000
        self.log_cost(
            execution_id=execution_id,
            resource_type="SPARK",
            quantity=core_hours,
            unit="CORE_HOURS",
            unit_cost=0.05,  # Example cost per core-hour
        )

        # Record metrics
        self.record_count("spark_records_read", metrics.get("records_read", 0))
        self.record_count("spark_records_written", metrics.get("records_written", 0))
        self.record_bytes("spark_bytes_read", metrics.get("bytes_read", 0))
        self.record_bytes("spark_bytes_written", metrics.get("bytes_written", 0))
