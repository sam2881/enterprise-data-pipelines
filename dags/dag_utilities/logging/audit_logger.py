"""
APEX Data Agent - Audit Logger

Logs all pipeline events to PostgreSQL for complete auditability.
ALL actions must be logged - if it's not logged, it didn't happen.
"""

import uuid
import json
from datetime import datetime
from typing import Any, Dict, Optional

from dag_utilities.core.metadata_client import MetadataClient


class AuditLogger:
    """
    Centralized audit logging for APEX pipelines.

    Logs to PostgreSQL tables:
    - audit_log: Zone-level action logging
    - validation_log: Validation results
    - error_log: Error details
    - agent_decision_log: Agent decisions
    """

    def __init__(self, connection_string: Optional[str] = None):
        """Initialize audit logger."""
        self.metadata_client = MetadataClient(connection_string)

    # ═══════════════════════════════════════════════════════════════════════
    # AUDIT LOGGING
    # ═══════════════════════════════════════════════════════════════════════

    def log_event(
        self,
        execution_id: str,
        zone: str,
        action: str,
        entity: str,
        record_count: int = 0,
        message: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Log a pipeline event.

        Args:
            execution_id: Pipeline execution ID
            zone: Data zone (INIT, SOURCE, RAW, BRONZE, SILVER, GOLD, FINAL)
            action: Action type (FILE_FOUND, COPY, TRANSFORM, VALIDATE, etc.)
            entity: Entity name (file path, table name, etc.)
            record_count: Number of records affected
            message: Optional message
            metadata: Optional additional metadata

        Returns:
            Audit log ID
        """
        audit_id = str(uuid.uuid4())

        query = """
            INSERT INTO pipeline_audit_log (
                audit_id, execution_id, zone_level, action_type,
                entity_name, record_count, message, metadata, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        with self.metadata_client.conn.cursor() as cur:
            cur.execute(query, [
                audit_id,
                execution_id,
                zone,
                action,
                entity,
                record_count,
                message,
                json.dumps(metadata) if metadata else None,
                datetime.utcnow(),
            ])
            self.metadata_client.conn.commit()

        return audit_id

    # ═══════════════════════════════════════════════════════════════════════
    # DATA CATALOG — AUTO-REGISTER ASSETS
    # ═══════════════════════════════════════════════════════════════════════

    def register_zone_asset(
        self,
        asset_name: str,
        zone: str,
        feed_id: Optional[str] = None,
        contract_id: Optional[str] = None,
        dataset_path: Optional[str] = None,
        record_count: int = 0,
        domain: Optional[str] = None,
        owner: Optional[str] = None,
        schema_json: Optional[Dict[str, Any]] = None,
        quality_score: Optional[float] = None,
    ) -> Optional[str]:
        """
        Auto-register a data asset in the catalog after zone transition.

        Called automatically during zone transitions (BRONZE, SILVER, GOLD).
        Uses UPSERT so repeated runs update rather than duplicate.
        """
        try:
            from repository.catalog_repository import CatalogRepository
            catalog = CatalogRepository()
            asset_id = catalog.register_asset(
                asset_name=asset_name,
                asset_type="TABLE",
                zone_level=zone,
                feed_id=feed_id,
                contract_id=contract_id,
                dataset_path=dataset_path,
                row_count=record_count,
                domain=domain,
                owner=owner,
                schema_json=schema_json,
                quality_score=quality_score,
            )
            return asset_id
        except Exception:
            # Catalog registration is best-effort; don't fail the pipeline
            return None

    # ═══════════════════════════════════════════════════════════════════════
    # VALIDATION LOGGING
    # ═══════════════════════════════════════════════════════════════════════

    def log_validation(
        self,
        execution_id: str,
        validation_id: str,
        zone: str,
        validation_type: str,
        rule_name: str,
        total_records: int,
        passed_records: int,
        threshold_pct: float,
        is_blocking: bool,
        sample_failures: Optional[list] = None,
        execution_time_ms: Optional[int] = None,
    ) -> str:
        """Log a validation result."""
        failed_records = total_records - passed_records
        pass_pct = (passed_records / total_records * 100) if total_records > 0 else 100.0
        is_passed = pass_pct >= (100 - threshold_pct)

        log_id = str(uuid.uuid4())

        query = """
            INSERT INTO validation_log (
                validation_log_id, execution_id, validation_id, zone_level,
                validation_type, rule_name, total_records, passed_records,
                failed_records, pass_percentage, threshold_percentage,
                is_passed, is_blocking, sample_failures, execution_time_ms,
                created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        with self.metadata_client.conn.cursor() as cur:
            cur.execute(query, [
                log_id,
                execution_id,
                validation_id,
                zone,
                validation_type,
                rule_name,
                total_records,
                passed_records,
                failed_records,
                pass_pct,
                threshold_pct,
                is_passed,
                is_blocking,
                json.dumps(sample_failures[:10]) if sample_failures else None,
                execution_time_ms,
                datetime.utcnow(),
            ])
            self.metadata_client.conn.commit()

        return log_id

    # ═══════════════════════════════════════════════════════════════════════
    # ERROR LOGGING
    # ═══════════════════════════════════════════════════════════════════════

    def log_error(
        self,
        execution_id: str,
        task_exec_id: Optional[str],
        error_type: str,
        error_message: str,
        error_code: Optional[str] = None,
        stack_trace: Optional[str] = None,
        error_context: Optional[Dict[str, Any]] = None,
        is_transient: bool = False,
    ) -> str:
        """Log an error with full context."""
        error_id = str(uuid.uuid4())

        query = """
            INSERT INTO error_log (
                error_log_id, execution_id, task_exec_id, error_type,
                error_code, error_message, stack_trace, error_context,
                is_transient, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        with self.metadata_client.conn.cursor() as cur:
            cur.execute(query, [
                error_id,
                execution_id,
                task_exec_id,
                error_type,
                error_code,
                error_message,
                stack_trace,
                json.dumps(error_context) if error_context else None,
                is_transient,
                datetime.utcnow(),
            ])
            self.metadata_client.conn.commit()

        return error_id

    # ═══════════════════════════════════════════════════════════════════════
    # AGENT DECISION LOGGING
    # ═══════════════════════════════════════════════════════════════════════

    def log_agent_decision(
        self,
        decision_type: str,
        input_context: Dict[str, Any],
        decision_made: str,
        decision_rationale: str,
        alternatives_considered: Optional[list] = None,
        confidence_score: Optional[float] = None,
        execution_id: Optional[str] = None,
    ) -> str:
        """Log an autonomous agent decision."""
        decision_id = str(uuid.uuid4())

        query = """
            INSERT INTO agent_decision_log (
                decision_id, decision_type, input_context, decision_made,
                decision_rationale, alternatives_considered, confidence_score,
                execution_id, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        with self.metadata_client.conn.cursor() as cur:
            cur.execute(query, [
                decision_id,
                decision_type,
                json.dumps(input_context),
                decision_made,
                decision_rationale,
                json.dumps(alternatives_considered) if alternatives_considered else None,
                confidence_score,
                execution_id,
                datetime.utcnow(),
            ])
            self.metadata_client.conn.commit()

        return decision_id

    # ═══════════════════════════════════════════════════════════════════════
    # AIRFLOW CALLBACKS
    # ═══════════════════════════════════════════════════════════════════════

    def on_task_failure(self, context: Dict[str, Any]) -> None:
        """Callback for Airflow task failure."""
        import traceback

        execution_id = context.get("ti").xcom_pull(key="execution_id")
        if not execution_id:
            return

        exception = context.get("exception")
        error_message = str(exception) if exception else "Unknown error"
        stack = traceback.format_exc() if exception else None

        self.log_error(
            execution_id=execution_id,
            task_exec_id=None,
            error_type="TASK_FAILURE",
            error_message=error_message,
            stack_trace=stack,
            error_context={
                "task_id": context.get("task_instance", {}).task_id,
                "dag_id": context.get("dag", {}).dag_id,
                "execution_date": str(context.get("execution_date")),
            },
        )

    def on_task_retry(self, context: Dict[str, Any]) -> None:
        """Callback for Airflow task retry."""
        execution_id = context.get("ti").xcom_pull(key="execution_id")
        if not execution_id:
            return

        self.log_event(
            execution_id=execution_id,
            zone="TASK",
            action="RETRY",
            entity=context.get("task_instance", {}).task_id,
            message=f"Task retry attempt {context.get('task_instance', {}).try_number}",
        )
