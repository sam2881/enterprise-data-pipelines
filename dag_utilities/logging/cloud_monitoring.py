"""
APEX Data Agent — GCP Cloud Monitoring Integration

Emits custom metrics to GCP Cloud Monitoring for pipeline observability.
Metrics are namespaced under 'custom.googleapis.com/apex/pipeline/'.

Requires: google-cloud-monitoring package.
Falls back to logging if not available.
"""

import os
import time
from datetime import datetime
from typing import Any, Dict, Optional

import structlog

logger = structlog.get_logger()


# ═══════════════════════════════════════════════════════════════════════════
# METRIC DEFINITIONS
# ═══════════════════════════════════════════════════════════════════════════

APEX_METRICS = {
    "records_processed":   "custom.googleapis.com/apex/pipeline/records_processed",
    "quality_score":       "custom.googleapis.com/apex/pipeline/quality_score",
    "duration_seconds":    "custom.googleapis.com/apex/pipeline/duration_seconds",
    "cost_dollars":        "custom.googleapis.com/apex/pipeline/cost_dollars",
    "bytes_processed":     "custom.googleapis.com/apex/pipeline/bytes_processed",
    "records_rejected":    "custom.googleapis.com/apex/pipeline/records_rejected",
    "sla_breach":          "custom.googleapis.com/apex/pipeline/sla_breach",
}


# ═══════════════════════════════════════════════════════════════════════════
# CLOUD MONITORING EMITTER
# ═══════════════════════════════════════════════════════════════════════════

class CloudMonitoringEmitter:
    """
    Emits custom metrics to GCP Cloud Monitoring.

    Gracefully degrades to logging if google-cloud-monitoring is not installed.
    """

    def __init__(
        self,
        project_id: Optional[str] = None,
        enabled: bool = True,
    ):
        self.project_id = project_id or os.getenv("GCP_PROJECT", os.getenv("GOOGLE_CLOUD_PROJECT"))
        self.enabled = enabled and self.project_id is not None
        self._client = None

        if self.enabled:
            try:
                from google.cloud import monitoring_v3
                self._client = monitoring_v3.MetricServiceClient()
                self._project_name = f"projects/{self.project_id}"
                logger.info("cloud_monitoring_initialized", project_id=self.project_id)
            except ImportError:
                logger.warning("cloud_monitoring_unavailable", reason="google-cloud-monitoring not installed")
                self.enabled = False
            except Exception as e:
                logger.warning("cloud_monitoring_init_failed", error=str(e))
                self.enabled = False

    def emit_metric(
        self,
        metric_name: str,
        value: float,
        labels: Optional[Dict[str, str]] = None,
    ) -> bool:
        """
        Emit a single metric to Cloud Monitoring.

        Args:
            metric_name: Key from APEX_METRICS (e.g., "records_processed")
            value: Metric value
            labels: Additional labels (feed_id, domain, zone, environment)

        Returns:
            True if emitted successfully
        """
        metric_type = APEX_METRICS.get(metric_name)
        if not metric_type:
            logger.warning("unknown_metric", metric_name=metric_name)
            return False

        if not self.enabled or not self._client:
            logger.info("cloud_monitoring_metric",
                        metric=metric_name, value=value, labels=labels or {})
            return False

        try:
            from google.cloud import monitoring_v3
            from google.protobuf import timestamp_pb2

            series = monitoring_v3.TimeSeries()
            series.metric.type = metric_type
            series.resource.type = "global"

            # Add labels
            if labels:
                for key, val in labels.items():
                    series.metric.labels[key] = str(val)

            # Set the data point
            now = time.time()
            interval = monitoring_v3.TimeInterval()
            interval.end_time = timestamp_pb2.Timestamp(seconds=int(now))

            point = monitoring_v3.Point()
            point.interval = interval
            point.value.double_value = float(value)
            series.points = [point]

            self._client.create_time_series(
                name=self._project_name,
                time_series=[series],
            )

            logger.debug("cloud_monitoring_emitted", metric=metric_name, value=value)
            return True

        except Exception as e:
            logger.error("cloud_monitoring_emit_failed", metric=metric_name, error=str(e))
            return False

    def emit_pipeline_metrics(
        self,
        feed_id: str,
        domain: str,
        environment: str,
        records_processed: int = 0,
        quality_score: float = 0.0,
        duration_seconds: float = 0.0,
        cost_dollars: float = 0.0,
        bytes_processed: int = 0,
        records_rejected: int = 0,
    ) -> None:
        """
        Emit all pipeline metrics at once after execution.

        Called from metrics_collector.flush_to_execution().
        """
        labels = {
            "feed_id": feed_id,
            "domain": domain,
            "environment": environment,
        }

        metrics = {
            "records_processed": records_processed,
            "quality_score": quality_score,
            "duration_seconds": duration_seconds,
            "cost_dollars": cost_dollars,
            "bytes_processed": bytes_processed,
            "records_rejected": records_rejected,
        }

        for name, value in metrics.items():
            if value:  # Only emit non-zero metrics
                self.emit_metric(name, float(value), labels)

    def emit_sla_breach(
        self,
        feed_id: str,
        sla_type: str,
        expected_value: float,
        actual_value: float,
    ) -> None:
        """Emit SLA breach metric."""
        self.emit_metric(
            "sla_breach",
            1.0,
            labels={
                "feed_id": feed_id,
                "sla_type": sla_type,
                "expected": str(expected_value),
                "actual": str(actual_value),
            },
        )
