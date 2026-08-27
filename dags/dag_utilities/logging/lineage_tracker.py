"""
APEX Data Agent - Lineage Tracker

Tracks data lineage throughout pipeline execution.
"""

import uuid
import json
from datetime import datetime
from typing import Dict, Any, List, Optional

from dag_utilities.core.metadata_client import MetadataClient


class LineageTracker:
    """
    Tracks data lineage for audit and debugging.

    Records:
    - Source to target entity mappings
    - Column-level lineage
    - Transformation types
    - Row counts
    """

    def __init__(self):
        """Initialize lineage tracker."""
        self.metadata_client = MetadataClient()

    def track_lineage(
        self,
        execution_id: str,
        source_entity: str,
        target_entity: str,
        transform_type: str,
        column_mapping: Optional[Dict[str, str]] = None,
        row_count: Optional[int] = None,
    ) -> str:
        """
        Track a data lineage relationship.

        Args:
            execution_id: Pipeline execution ID
            source_entity: Source entity (table/file path)
            target_entity: Target entity (table/file path)
            transform_type: Type of transformation (COPY, VIEW, SPARK, MERGE)
            column_mapping: Optional column name mappings
            row_count: Optional row count

        Returns:
            Lineage record ID
        """
        lineage_id = str(uuid.uuid4())

        query = """
            INSERT INTO platform_data_lineage (
                lineage_id, execution_id, source_entity, target_entity,
                transform_type, column_mapping, row_count, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        with self.metadata_client.conn.cursor() as cur:
            cur.execute(query, [
                lineage_id,
                execution_id,
                source_entity,
                target_entity,
                transform_type,
                json.dumps(column_mapping) if column_mapping else None,
                row_count,
                datetime.utcnow(),
            ])
            self.metadata_client.conn.commit()

        return lineage_id

    def track_zone_transition(
        self,
        execution_id: str,
        source_zone: str,
        target_zone: str,
        source_table: str,
        target_table: str,
        row_count: int,
        feed_id: Optional[str] = None,
    ) -> str:
        """Track a zone-to-zone data movement."""
        lineage_id = self.track_lineage(
            execution_id=execution_id,
            source_entity=f"{source_zone.lower()}.{source_table}",
            target_entity=f"{target_zone.lower()}.{target_table}",
            transform_type="ZONE_TRANSITION",
            row_count=row_count,
        )

        # Emit OpenLineage event
        try:
            from dag_utilities.logging.openlineage_emitter import OpenLineageEmitter
            emitter = OpenLineageEmitter()
            emitter.emit_zone_transition(
                feed_id=feed_id or "unknown",
                execution_id=execution_id,
                source_zone=source_zone,
                target_zone=target_zone,
                source_path=source_table,
                target_path=target_table,
                records_read=row_count,
                records_written=row_count,
            )
        except Exception:
            pass  # OpenLineage is best-effort

        return lineage_id

    def track_file_to_table(
        self,
        execution_id: str,
        file_path: str,
        target_table: str,
        target_zone: str,
        row_count: int,
    ) -> str:
        """Track file to table ingestion."""
        return self.track_lineage(
            execution_id=execution_id,
            source_entity=file_path,
            target_entity=f"{target_zone.lower()}.{target_table}",
            transform_type="FILE_INGEST",
            row_count=row_count,
        )

    def get_lineage_for_execution(
        self,
        execution_id: str,
    ) -> List[Dict[str, Any]]:
        """Get all lineage records for an execution."""
        query = """
            SELECT
                lineage_id, source_entity, target_entity,
                transform_type, column_mapping, row_count, created_at
            FROM platform_data_lineage
            WHERE execution_id = %s
            ORDER BY created_at
        """
        with self.metadata_client.conn.cursor() as cur:
            cur.execute(query, [execution_id])
            rows = cur.fetchall()

        return [dict(row) for row in rows]

    def get_upstream_lineage(
        self,
        entity: str,
        max_depth: int = 10,
    ) -> List[Dict[str, Any]]:
        """Get upstream lineage for an entity (trace back to source)."""
        query = """
            WITH RECURSIVE lineage_trace AS (
                -- Base case
                SELECT
                    lineage_id, source_entity, target_entity,
                    transform_type, 1 as depth
                FROM platform_data_lineage
                WHERE target_entity = %s

                UNION ALL

                -- Recursive case
                SELECT
                    dl.lineage_id, dl.source_entity, dl.target_entity,
                    dl.transform_type, lt.depth + 1
                FROM platform_data_lineage dl
                JOIN lineage_trace lt ON dl.target_entity = lt.source_entity
                WHERE lt.depth < %s
            )
            SELECT DISTINCT * FROM lineage_trace
            ORDER BY depth DESC
        """
        with self.metadata_client.conn.cursor() as cur:
            cur.execute(query, [entity, max_depth])
            rows = cur.fetchall()

        return [dict(row) for row in rows]

    def get_downstream_lineage(
        self,
        entity: str,
        max_depth: int = 10,
    ) -> List[Dict[str, Any]]:
        """Get downstream lineage for an entity (trace forward to consumers)."""
        query = """
            WITH RECURSIVE lineage_trace AS (
                -- Base case
                SELECT
                    lineage_id, source_entity, target_entity,
                    transform_type, 1 as depth
                FROM platform_data_lineage
                WHERE source_entity = %s

                UNION ALL

                -- Recursive case
                SELECT
                    dl.lineage_id, dl.source_entity, dl.target_entity,
                    dl.transform_type, lt.depth + 1
                FROM platform_data_lineage dl
                JOIN lineage_trace lt ON dl.source_entity = lt.target_entity
                WHERE lt.depth < %s
            )
            SELECT DISTINCT * FROM lineage_trace
            ORDER BY depth
        """
        with self.metadata_client.conn.cursor() as cur:
            cur.execute(query, [entity, max_depth])
            rows = cur.fetchall()

        return [dict(row) for row in rows]
