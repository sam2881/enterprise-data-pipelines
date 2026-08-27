"""
APEX Data Agent - Metrics Collector

Collects and reports pipeline metrics.
"""

from typing import Dict, Any, Optional
from datetime import datetime
import json
import os

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

    def flush_to_execution(
        self,
        execution_id: str,
        feed_id: Optional[str] = None,
        domain: Optional[str] = None,
        environment: Optional[str] = None,
    ) -> None:
        """Flush metrics to pipeline execution record and Cloud Monitoring."""
        records = self._metrics.get("total_records_processed", 0)
        bytes_processed = self._metrics.get("total_bytes_processed", 0)

        self.metadata_client.update_execution_metrics(
            execution_id=execution_id,
            records_processed=records,
            bytes_processed=bytes_processed,
        )

        # Emit to GCP Cloud Monitoring if configured
        if feed_id:
            try:
                from dag_utilities.logging.cloud_monitoring import CloudMonitoringEmitter
                emitter = CloudMonitoringEmitter()
                emitter.emit_pipeline_metrics(
                    feed_id=feed_id,
                    domain=domain or "unknown",
                    environment=environment or os.getenv("APEX_ENVIRONMENT", "dev"),
                    records_processed=records,
                    bytes_processed=bytes_processed,
                    quality_score=self._metrics.get("quality_score", 0.0),
                    duration_seconds=self._metrics.get("total_duration_seconds", 0.0),
                    cost_dollars=self._metrics.get("total_cost_dollars", 0.0),
                    records_rejected=self._metrics.get("total_records_rejected", 0),
                )
            except ImportError:
                pass

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
            INSERT INTO platform_execution_cost_log (
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

    @staticmethod
    def estimate_dataproc_cost(
        core_hours: float,
        preemptible_ratio: float = 0.0,
        on_demand_rate: float = 0.05,
        preemptible_rate: float = 0.01,
    ) -> float:
        """
        Estimate Dataproc cost considering preemptible workers.

        Args:
            core_hours: Total core-hours consumed
            preemptible_ratio: Fraction of workers that are preemptible (0.0-1.0)
            on_demand_rate: Cost per core-hour for on-demand (default $0.05)
            preemptible_rate: Cost per core-hour for preemptible (default $0.01)

        Returns:
            Estimated cost in dollars
        """
        on_demand_hours = core_hours * (1 - preemptible_ratio)
        preemptible_hours = core_hours * preemptible_ratio
        return (on_demand_hours * on_demand_rate) + (preemptible_hours * preemptible_rate)

    def log_spark_metrics(
        self,
        execution_id: str,
        application_id: str,
        metrics: Dict[str, Any],
        cost_labels: Optional[Dict[str, str]] = None,
        preemptible_ratio: float = 0.0,
    ) -> None:
        """Log Spark job metrics with cost labels and preemptible cost savings."""
        # Calculate estimated cost with preemptible savings
        core_hours = metrics.get("executor_run_time_ms", 0) / 3600000
        estimated_cost = self.estimate_dataproc_cost(core_hours, preemptible_ratio)

        self.log_cost(
            execution_id=execution_id,
            resource_type="SPARK",
            quantity=core_hours,
            unit="CORE_HOURS",
            unit_cost=estimated_cost / core_hours if core_hours > 0 else 0.05,
        )

        # Track total cost for Cloud Monitoring emission
        self._metrics["total_cost_dollars"] = (
            self._metrics.get("total_cost_dollars", 0.0) + estimated_cost
        )

        # Record metrics
        self.record_count("spark_records_read", metrics.get("records_read", 0))
        self.record_count("spark_records_written", metrics.get("records_written", 0))
        self.record_bytes("spark_bytes_read", metrics.get("bytes_read", 0))
        self.record_bytes("spark_bytes_written", metrics.get("bytes_written", 0))
