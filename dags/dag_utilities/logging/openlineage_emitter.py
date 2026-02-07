"""
APEX Data Agent - OpenLineage Emitter

Converts internal APEX lineage events to OpenLineage spec JSON and emits
them to a configurable endpoint (Marquez, DataHub, file, or HTTP).

OpenLineage Spec: https://openlineage.io/spec/2-0-0/OpenLineage.json

Usage:
    emitter = OpenLineageEmitter(endpoint="http://marquez:5000/api/v1")
    emitter.emit_run_event(
        job_name="raw_to_bronze_sales_daily",
        run_id="abc-123",
        event_type="COMPLETE",
        inputs=[{"namespace": "gcs", "name": "gs://bucket/sales.csv"}],
        outputs=[{"namespace": "bigquery", "name": "project.bronze.sales"}],
    )
"""

import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import structlog

logger = structlog.get_logger(__name__)


class OpenLineageEmitter:
    """Emit OpenLineage events to a configurable endpoint."""

    SPEC_VERSION = "https://openlineage.io/spec/2-0-0/OpenLineage.json"

    def __init__(
        self,
        endpoint: Optional[str] = None,
        namespace: str = "apex-data-agent",
        output_dir: Optional[str] = None,
    ):
        """
        Initialize emitter.

        Args:
            endpoint: HTTP endpoint for OpenLineage API (e.g., Marquez)
            namespace: Default namespace for jobs
            output_dir: Directory to write JSON files (fallback if no endpoint)
        """
        self.endpoint = endpoint or os.getenv("OPENLINEAGE_URL")
        self.namespace = namespace or os.getenv("OPENLINEAGE_NAMESPACE", "apex-data-agent")
        self.output_dir = output_dir or os.getenv(
            "OPENLINEAGE_OUTPUT_DIR", "/tmp/openlineage"
        )
        self.logger = logger.bind(component="openlineage_emitter")

    def _build_dataset(
        self,
        namespace: str,
        name: str,
        facets: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Build an OpenLineage dataset."""
        dataset = {
            "namespace": namespace,
            "name": name,
        }
        if facets:
            dataset["facets"] = facets
        return dataset

    def _build_schema_facet(
        self, columns: List[Dict[str, str]]
    ) -> Dict[str, Any]:
        """Build schema dataset facet."""
        return {
            "schema": {
                "_producer": self.SPEC_VERSION,
                "_schemaURL": f"{self.SPEC_VERSION}#/$defs/SchemaDatasetFacet",
                "fields": [
                    {
                        "name": col.get("name", col.get("column_name", "")),
                        "type": col.get("type", col.get("data_type", "string")),
                    }
                    for col in columns
                ],
            }
        }

    def build_run_event(
        self,
        job_name: str,
        run_id: str,
        event_type: str = "COMPLETE",
        inputs: Optional[List[Dict[str, str]]] = None,
        outputs: Optional[List[Dict[str, str]]] = None,
        job_facets: Optional[Dict[str, Any]] = None,
        run_facets: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Build an OpenLineage RunEvent.

        Args:
            job_name: Name of the job (e.g., "raw_to_bronze_sales_daily")
            run_id: UUID of the run
            event_type: START, RUNNING, COMPLETE, ABORT, FAIL
            inputs: List of input datasets [{"namespace": "...", "name": "..."}]
            outputs: List of output datasets
            job_facets: Additional job facets
            run_facets: Additional run facets
        """
        event = {
            "eventTime": datetime.now(timezone.utc).isoformat(),
            "eventType": event_type,
            "producer": self.SPEC_VERSION,
            "schemaURL": self.SPEC_VERSION,
            "job": {
                "namespace": self.namespace,
                "name": job_name,
                "facets": job_facets or {},
            },
            "run": {
                "runId": run_id,
                "facets": run_facets or {},
            },
            "inputs": [
                self._build_dataset(d.get("namespace", "unknown"), d["name"], d.get("facets"))
                for d in (inputs or [])
            ],
            "outputs": [
                self._build_dataset(d.get("namespace", "unknown"), d["name"], d.get("facets"))
                for d in (outputs or [])
            ],
        }
        return event

    def emit_run_event(self, **kwargs) -> bool:
        """Build and emit an OpenLineage run event."""
        event = self.build_run_event(**kwargs)
        return self._emit(event)

    def emit_zone_transition(
        self,
        feed_id: str,
        execution_id: str,
        source_zone: str,
        target_zone: str,
        source_path: str,
        target_path: str,
        records_read: int,
        records_written: int,
        event_type: str = "COMPLETE",
    ) -> bool:
        """
        Emit an OpenLineage event for a zone transition (Bronze→Silver, etc).

        This is the primary method called from LineageTracker.
        """
        job_name = f"{source_zone.lower()}_to_{target_zone.lower()}_{feed_id}"
        run_facets = {
            "apex_metrics": {
                "_producer": self.SPEC_VERSION,
                "records_read": records_read,
                "records_written": records_written,
                "feed_id": feed_id,
            }
        }

        return self.emit_run_event(
            job_name=job_name,
            run_id=execution_id,
            event_type=event_type,
            inputs=[{"namespace": source_zone.lower(), "name": source_path}],
            outputs=[{"namespace": target_zone.lower(), "name": target_path}],
            run_facets=run_facets,
        )

    def _emit(self, event: Dict[str, Any]) -> bool:
        """Emit event to endpoint or file."""
        if self.endpoint:
            return self._emit_http(event)
        else:
            return self._emit_file(event)

    def _emit_http(self, event: Dict[str, Any]) -> bool:
        """Send event to HTTP endpoint."""
        try:
            import requests
            response = requests.post(
                f"{self.endpoint}/lineage",
                json=event,
                headers={"Content-Type": "application/json"},
                timeout=10,
            )
            response.raise_for_status()
            self.logger.info("openlineage_emitted", endpoint=self.endpoint)
            return True
        except Exception as e:
            self.logger.warning("openlineage_emit_failed", error=str(e))
            # Fall back to file
            return self._emit_file(event)

    def _emit_file(self, event: Dict[str, Any]) -> bool:
        """Write event to JSON file."""
        try:
            os.makedirs(self.output_dir, exist_ok=True)
            run_id = event.get("run", {}).get("runId", str(uuid.uuid4()))
            event_type = event.get("eventType", "UNKNOWN").lower()
            filename = f"{run_id}_{event_type}.json"
            filepath = os.path.join(self.output_dir, filename)
            with open(filepath, "w") as f:
                json.dump(event, f, indent=2)
            self.logger.info("openlineage_written", path=filepath)
            return True
        except Exception as e:
            self.logger.error("openlineage_write_failed", error=str(e))
            return False
