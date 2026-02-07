"""
APEX Data Agent - Metadata Client

Central client for all PostgreSQL metadata operations.
ALL metadata access MUST go through this client.
"""

import json
import uuid
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass

import psycopg2
from psycopg2.extras import RealDictCursor, Json

from dag_utilities.core.exceptions import MetadataError


@dataclass
class FeedConfig:
    """Feed configuration from metadata."""
    feed_id: str
    feed_code: str
    feed_name: str
    feed_type: str
    schedule_cron: Optional[str]
    notification_email: Optional[str]
    owner: Optional[str]


@dataclass
class ContractConfig:
    """Data contract configuration from metadata."""
    contract_id: str
    feed_id: str
    contract_type: str
    file_pattern: Optional[str]
    file_format: Optional[str]
    source_path: Optional[str]
    raw_path: Optional[str]
    transient_path: Optional[str]
    rejected_path: Optional[str]
    bronze_path: Optional[str]
    silver_path: Optional[str]
    gold_path: Optional[str]
    load_type: str
    primary_keys: List[str]
    partition_columns: List[str]


@dataclass
class SparkConfigData:
    """Spark configuration from metadata."""
    executor_instances: int
    executor_memory: str
    executor_cores: int
    driver_memory: str
    shuffle_partitions: int
    adaptive_enabled: bool
    extra_conf: Dict[str, str]


class MetadataClient:
    """
    Central client for PostgreSQL metadata operations.

    This client handles all interactions with the APEX metadata store.
    It provides methods for querying configurations and logging executions.
    """

    def __init__(self, connection_string: Optional[str] = None):
        """
        Initialize the metadata client.

        Args:
            connection_string: PostgreSQL connection string.
                             If not provided, uses APEX_METADATA_DB env var.
        """
        import os
        self.connection_string = connection_string or os.getenv(
            "APEX_METADATA_DB",
            "postgresql://apex:apex@localhost:5432/apex_metadata"
        )
        self._conn = None

    @property
    def conn(self):
        """Get database connection (lazy initialization)."""
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(
                self.connection_string,
                cursor_factory=RealDictCursor
            )
            self._conn.autocommit = False
        return self._conn

    def close(self):
        """Close database connection."""
        if self._conn and not self._conn.closed:
            self._conn.close()

    # ═══════════════════════════════════════════════════════════════════════
    # FEED CONFIGURATION
    # ═══════════════════════════════════════════════════════════════════════

    def get_feed_config(self, feed_id: str) -> FeedConfig:
        """Get feed configuration by ID."""
        query = """
            SELECT
                f.feed_id,
                f.feed_code,
                f.feed_name,
                f.feed_type,
                f.schedule_cron,
                fg.notification_email,
                f.owner
            FROM feed f
            JOIN feed_group fg ON f.feed_group_id = fg.feed_group_id
            WHERE f.feed_id = %s AND f.is_active = true
        """
        with self.conn.cursor() as cur:
            cur.execute(query, [feed_id])
            row = cur.fetchone()

        if not row:
            raise MetadataError(
                f"Feed not found: {feed_id}",
                table_name="feed",
                operation="SELECT"
            )

        return FeedConfig(
            feed_id=str(row["feed_id"]),
            feed_code=row["feed_code"],
            feed_name=row["feed_name"],
            feed_type=row["feed_type"],
            schedule_cron=row["schedule_cron"],
            notification_email=row["notification_email"],
            owner=row["owner"],
        )

    def get_feed_by_code(self, feed_code: str) -> FeedConfig:
        """Get feed configuration by code."""
        query = """
            SELECT feed_id FROM feed
            WHERE feed_code = %s AND is_active = true
        """
        with self.conn.cursor() as cur:
            cur.execute(query, [feed_code])
            row = cur.fetchone()

        if not row:
            raise MetadataError(f"Feed not found: {feed_code}")

        return self.get_feed_config(str(row["feed_id"]))

    # ═══════════════════════════════════════════════════════════════════════
    # CONTRACT CONFIGURATION
    # ═══════════════════════════════════════════════════════════════════════

    def get_contract(self, contract_id: str) -> ContractConfig:
        """Get data contract by ID."""
        query = """
            SELECT
                contract_id, feed_id, contract_type,
                file_pattern, file_format,
                source_path, raw_path, transient_path, rejected_path,
                bronze_path, silver_path, gold_path,
                load_type, primary_keys, partition_columns
            FROM data_contract
            WHERE contract_id = %s AND is_active = true
        """
        with self.conn.cursor() as cur:
            cur.execute(query, [contract_id])
            row = cur.fetchone()

        if not row:
            raise MetadataError(f"Contract not found: {contract_id}")

        return ContractConfig(
            contract_id=str(row["contract_id"]),
            feed_id=str(row["feed_id"]),
            contract_type=row["contract_type"],
            file_pattern=row["file_pattern"],
            file_format=row["file_format"],
            source_path=row["source_path"],
            raw_path=row["raw_path"],
            transient_path=row["transient_path"],
            rejected_path=row["rejected_path"],
            bronze_path=row["bronze_path"],
            silver_path=row["silver_path"],
            gold_path=row["gold_path"],
            load_type=row["load_type"],
            primary_keys=row["primary_keys"] or [],
            partition_columns=row["partition_columns"] or [],
        )

    def get_contract_by_feed(self, feed_id: str) -> ContractConfig:
        """Get data contract by feed ID."""
        query = """
            SELECT contract_id FROM data_contract
            WHERE feed_id = %s AND is_active = true
            LIMIT 1
        """
        with self.conn.cursor() as cur:
            cur.execute(query, [feed_id])
            row = cur.fetchone()

        if not row:
            raise MetadataError(f"Contract not found for feed: {feed_id}")

        return self.get_contract(str(row["contract_id"]))

    # ═══════════════════════════════════════════════════════════════════════
    # SPARK CONFIGURATION
    # ═══════════════════════════════════════════════════════════════════════

    def get_spark_config(self, feed_group_id: str) -> SparkConfigData:
        """Get Spark configuration for a feed group."""
        query = """
            SELECT
                executor_instances, executor_memory, executor_cores,
                driver_memory, shuffle_partitions, adaptive_enabled,
                extra_conf
            FROM spark_config
            WHERE feed_group_id = %s AND is_active = true
            LIMIT 1
        """
        with self.conn.cursor() as cur:
            cur.execute(query, [feed_group_id])
            row = cur.fetchone()

        if not row:
            # Return defaults if no config exists
            return SparkConfigData(
                executor_instances=4,
                executor_memory="4g",
                executor_cores=2,
                driver_memory="2g",
                shuffle_partitions=200,
                adaptive_enabled=True,
                extra_conf={},
            )

        return SparkConfigData(
            executor_instances=row["executor_instances"],
            executor_memory=row["executor_memory"],
            executor_cores=row["executor_cores"],
            driver_memory=row["driver_memory"],
            shuffle_partitions=row["shuffle_partitions"],
            adaptive_enabled=row["adaptive_enabled"],
            extra_conf=row["extra_conf"] or {},
        )

    # ═══════════════════════════════════════════════════════════════════════
    # VIEW DEFINITIONS
    # ═══════════════════════════════════════════════════════════════════════

    def get_view_definition(
        self, contract_id: str, zone_level: str
    ) -> Optional[Dict[str, Any]]:
        """Get view definition for a contract and zone."""
        query = """
            SELECT view_id, view_name, view_sql, materialized, refresh_mode
            FROM view_definition
            WHERE contract_id = %s AND zone_level = %s AND is_active = true
        """
        with self.conn.cursor() as cur:
            cur.execute(query, [contract_id, zone_level])
            row = cur.fetchone()

        return dict(row) if row else None

    def refresh_views(self, contract_id: str, zone: str) -> None:
        """Refresh materialized views for a contract."""
        view = self.get_view_definition(contract_id, zone)
        if view and view.get("materialized"):
            # Execute view refresh logic
            pass

    # ═══════════════════════════════════════════════════════════════════════
    # VALIDATION RULES
    # ═══════════════════════════════════════════════════════════════════════

    def get_validation_rules(
        self, contract_id: str, zone_level: str
    ) -> List[Dict[str, Any]]:
        """Get validation rules for a contract and zone."""
        query = """
            SELECT
                validation_id, rule_name, rule_expression,
                validation_type, severity, threshold_pct, is_blocking
            FROM validation_rule
            WHERE contract_id = %s AND zone_level = %s AND is_active = true
            ORDER BY severity DESC
        """
        with self.conn.cursor() as cur:
            cur.execute(query, [contract_id, zone_level])
            rows = cur.fetchall()

        return [dict(row) for row in rows]

    # ═══════════════════════════════════════════════════════════════════════
    # SCHEMA MANAGEMENT
    # ═══════════════════════════════════════════════════════════════════════

    def get_current_schema(self, contract_id: str) -> Dict[str, Any]:
        """Get current schema version for a contract."""
        query = """
            SELECT
                schema_version_id, version_number, schema_json,
                col_delimiter, row_delimiter, header_rows,
                encoding, copybook_content
            FROM schema_version
            WHERE contract_id = %s AND is_current = true
        """
        with self.conn.cursor() as cur:
            cur.execute(query, [contract_id])
            row = cur.fetchone()

        if not row:
            raise MetadataError(f"No current schema for contract: {contract_id}")

        return dict(row)

    # ═══════════════════════════════════════════════════════════════════════
    # EXECUTION TRACKING
    # ═══════════════════════════════════════════════════════════════════════

    def create_execution(
        self,
        feed_id: str,
        params: Dict[str, Any]
    ) -> str:
        """Create a new pipeline execution record with auto-incrementing sequence."""
        execution_id = str(uuid.uuid4())
        execution_date = params.get("execution_date", date.today())

        with self.conn.cursor() as cur:
            # Compute next sequence for this feed + execution_date
            cur.execute("""
                SELECT COALESCE(MAX(sequence), 0) + 1 AS next_seq
                FROM pipeline_execution
                WHERE feed_id = %s AND execution_date = %s
            """, [feed_id, execution_date])
            next_seq = cur.fetchone()["next_seq"]

            query = """
                INSERT INTO pipeline_execution (
                    execution_id, feed_id, dag_run_id, execution_date,
                    sequence, start_ts, status, trigger_type, parameters
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING execution_id
            """
            cur.execute(query, [
                execution_id,
                feed_id,
                params.get("dag_run_id"),
                execution_date,
                next_seq,
                datetime.utcnow(),
                "RUNNING",
                params.get("trigger_type", "scheduled"),
                Json(params),
            ])
            self.conn.commit()

        return execution_id

    def update_execution_status(
        self,
        execution_id: str,
        status: str,
        error_message: Optional[str] = None,
    ) -> None:
        """Update pipeline execution status."""
        query = """
            UPDATE pipeline_execution
            SET status = %s,
                end_ts = %s,
                error_message = %s,
                updated_at = %s
            WHERE execution_id = %s
        """
        with self.conn.cursor() as cur:
            cur.execute(query, [
                status,
                datetime.utcnow(),
                error_message,
                datetime.utcnow(),
                execution_id,
            ])
            self.conn.commit()

    def update_execution_metrics(
        self,
        execution_id: str,
        records_processed: int,
        bytes_processed: int,
    ) -> None:
        """Update execution metrics."""
        query = """
            UPDATE pipeline_execution
            SET records_processed = %s,
                bytes_processed = %s,
                updated_at = %s
            WHERE execution_id = %s
        """
        with self.conn.cursor() as cur:
            cur.execute(query, [
                records_processed,
                bytes_processed,
                datetime.utcnow(),
                execution_id,
            ])
            self.conn.commit()

    # ═══════════════════════════════════════════════════════════════════════
    # TEMPLATE MANAGEMENT
    # ═══════════════════════════════════════════════════════════════════════

    def get_template(self, template_id: str) -> Dict[str, Any]:
        """Get DAG template by ID."""
        query = """
            SELECT * FROM dag_template
            WHERE template_id = %s AND is_active = true
        """
        with self.conn.cursor() as cur:
            cur.execute(query, [template_id])
            row = cur.fetchone()

        if not row:
            raise MetadataError(f"Template not found: {template_id}")

        return dict(row)

    def get_active_templates(self) -> List[Dict[str, Any]]:
        """Get all active DAG templates."""
        query = """
            SELECT template_id, template_code, template_name,
                   pattern_id, template_type
            FROM dag_template
            WHERE is_active = true
            ORDER BY pattern_id
        """
        with self.conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

        return [dict(row) for row in rows]

    # ═══════════════════════════════════════════════════════════════════════
    # JOIN DEPENDENCIES
    # ═══════════════════════════════════════════════════════════════════════

    def get_join_dependencies(self, contract_id: str) -> List[Dict[str, Any]]:
        """Get join dependencies for a contract, ordered by join_order."""
        query = """
            SELECT
                join_id, source_table, target_table, join_type,
                join_keys, join_order, select_columns, null_safe, broadcast
            FROM join_dependency
            WHERE contract_id = %s AND is_active = true
            ORDER BY join_order
        """
        with self.conn.cursor() as cur:
            cur.execute(query, [contract_id])
            rows = cur.fetchall()

        return [dict(row) for row in rows]

    # ═══════════════════════════════════════════════════════════════════════
    # PIPELINE DEPENDENCIES
    # ═══════════════════════════════════════════════════════════════════════

    def get_pipeline_dependencies(self, feed_id: str) -> List[Dict[str, Any]]:
        """Get upstream dependencies for a feed (for ExternalTaskSensor generation)."""
        query = """
            SELECT
                upstream_feed_id, upstream_dag_id, upstream_task_id,
                dependency_type, required_zone,
                timeout_seconds, poke_interval
            FROM pipeline_dependency
            WHERE downstream_feed_id = %s AND is_active = true
            ORDER BY upstream_feed_id
        """
        with self.conn.cursor() as cur:
            cur.execute(query, [feed_id])
            rows = cur.fetchall()

        return [dict(row) for row in rows]

    # ═══════════════════════════════════════════════════════════════════════
    # TEMPLATE MANAGEMENT (continued)
    # ═══════════════════════════════════════════════════════════════════════

    # ═══════════════════════════════════════════════════════════════════════
    # DATA CATALOG
    # ═══════════════════════════════════════════════════════════════════════

    def search_data_assets(
        self, query: str, zone: Optional[str] = None, domain: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Full-text search across data assets."""
        sql = """
            SELECT asset_id, asset_name, asset_type, zone_level,
                   domain, owner, row_count, description
            FROM data_asset
            WHERE is_active = true
              AND (
                  asset_name ILIKE %s
                  OR description ILIKE %s
                  OR domain ILIKE %s
              )
        """
        params: list = [f"%{query}%", f"%{query}%", f"%{query}%"]
        if zone:
            sql += " AND zone_level = %s"
            params.append(zone)
        if domain:
            sql += " AND domain = %s"
            params.append(domain)
        sql += " ORDER BY updated_at DESC LIMIT 50"

        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            return [dict(row) for row in cur.fetchall()]

    def get_asset_by_path(self, dataset_path: str) -> Optional[Dict[str, Any]]:
        """Look up a data asset by GCS/BQ path."""
        sql = """
            SELECT * FROM data_asset
            WHERE dataset_path = %s AND is_active = true
            LIMIT 1
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, [dataset_path])
            row = cur.fetchone()
        return dict(row) if row else None

    def register_data_asset(
        self,
        asset_name: str,
        asset_type: str,
        zone_level: str,
        feed_id: Optional[str] = None,
        dataset_path: Optional[str] = None,
        row_count: Optional[int] = None,
        domain: Optional[str] = None,
        owner: Optional[str] = None,
        schema_json: Optional[Dict] = None,
    ) -> str:
        """Register or update a data asset in the catalog."""
        sql = """
            INSERT INTO data_asset (
                asset_name, asset_type, zone_level, feed_id,
                dataset_path, row_count, domain, owner, schema_json,
                last_refreshed_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
            ON CONFLICT (asset_name, zone_level)
            DO UPDATE SET row_count = EXCLUDED.row_count,
                          schema_json = COALESCE(EXCLUDED.schema_json, data_asset.schema_json),
                          last_refreshed_at = NOW(),
                          updated_at = NOW()
            RETURNING asset_id
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, [
                asset_name, asset_type, zone_level, feed_id,
                dataset_path, row_count, domain, owner,
                Json(schema_json) if schema_json else None,
            ])
            asset_id = str(cur.fetchone()["asset_id"])
            self.conn.commit()
        return asset_id

    # ═══════════════════════════════════════════════════════════════════════
    # DATA PRODUCTS
    # ═══════════════════════════════════════════════════════════════════════

    def get_data_products(self, domain: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get data products, optionally filtered by domain."""
        sql = """
            SELECT product_id, product_name, domain, owner, description,
                   sla_freshness_hours, sla_quality_score, version, status
            FROM data_product
            WHERE is_active = true
        """
        params: list = []
        if domain:
            sql += " AND domain = %s"
            params.append(domain)
        sql += " ORDER BY domain, product_name"

        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            return [dict(row) for row in cur.fetchall()]

    def publish_data_product(
        self,
        product_name: str,
        domain: str,
        owner: str,
        description: Optional[str] = None,
        sla_freshness_hours: Optional[int] = None,
        sla_quality_score: Optional[float] = None,
        output_assets: Optional[list] = None,
        input_feeds: Optional[list] = None,
    ) -> str:
        """Publish a new data product or update existing."""
        sql = """
            INSERT INTO data_product (
                product_name, domain, owner, description,
                sla_freshness_hours, sla_quality_score,
                output_assets, input_feeds,
                status, published_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'PUBLISHED', NOW())
            ON CONFLICT (product_name) DO UPDATE SET
                status = 'PUBLISHED',
                published_at = NOW(),
                description = EXCLUDED.description,
                sla_freshness_hours = EXCLUDED.sla_freshness_hours,
                sla_quality_score = EXCLUDED.sla_quality_score
            RETURNING product_id
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, [
                product_name, domain, owner, description,
                sla_freshness_hours, sla_quality_score,
                Json(output_assets or []),
                Json(input_feeds or []),
            ])
            product_id = str(cur.fetchone()["product_id"])
            self.conn.commit()
        return product_id

    def subscribe_to_product(self, product_id: str, subscriber: str) -> str:
        """Request a subscription to a data product."""
        sql = """
            INSERT INTO data_product_subscription (product_id, subscriber)
            VALUES (%s, %s)
            RETURNING subscription_id
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, [product_id, subscriber])
            sub_id = str(cur.fetchone()["subscription_id"])
            self.conn.commit()
        return sub_id

    # ═══════════════════════════════════════════════════════════════════════
    # DATA CLASSIFICATION
    # ═══════════════════════════════════════════════════════════════════════

    def get_classifications(self, asset_id: str) -> List[Dict[str, Any]]:
        """Get data classifications for an asset."""
        sql = """
            SELECT classification_id, column_name, classification_type,
                   pii_type, confidence, masking_strategy, policy_tag
            FROM data_classification
            WHERE asset_id = %s
            ORDER BY column_name
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, [asset_id])
            return [dict(row) for row in cur.fetchall()]

    # ═══════════════════════════════════════════════════════════════════════
    # TEMPLATE MANAGEMENT (continued)
    # ═══════════════════════════════════════════════════════════════════════

    def get_pipelines_by_template(self, template_id: str) -> List[Dict[str, Any]]:
        """Get all pipelines using a template."""
        query = """
            SELECT f.feed_id, f.feed_code, f.feed_name
            FROM feed f
            JOIN feed_group fg ON f.feed_group_id = fg.feed_group_id
            WHERE fg.template_id = %s AND f.is_active = true
        """
        with self.conn.cursor() as cur:
            cur.execute(query, [template_id])
            rows = cur.fetchall()

        return [dict(row) for row in rows]
