"""
GE Result Writer — Persists validation results to PostgreSQL.

Writes to:
- platform_validation_result: one row per expectation
- platform_validation_summary: one row per checkpoint run

Usage:
    from dag_utilities.validation.ge_result_writer import GEResultWriter

    writer = GEResultWriter("postgresql://admin:admin123@postgres:5432/agentdb")
    writer.write_results(
        feed_id="feed_123",
        run_id="run_001",
        suite_name="feed_123_bronze_schema",
        validation_type="SCHEMA",
        zone_level="BRONZE",
        checkpoint_result=result,
    )
"""

import json
import logging
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


class GEResultWriter:
    """Persists GE checkpoint results to PostgreSQL."""

    def __init__(self, pg_connection_string: str):
        """
        Args:
            pg_connection_string: PostgreSQL connection string
                e.g. postgresql://admin:admin123@postgres:5432/agentdb
        """
        self.pg_connection_string = pg_connection_string

    def write_results(
        self,
        feed_id: str,
        run_id: str,
        suite_name: str,
        validation_type: str,
        zone_level: str,
        checkpoint_result,
        batch_identifier: Optional[str] = None,
        checkpoint_name: Optional[str] = None,
    ) -> None:
        """
        Write validation results to PostgreSQL.

        Args:
            feed_id: Pipeline/feed identifier
            run_id: Execution run identifier
            suite_name: GE expectation suite name
            validation_type: SCHEMA, SEMANTIC, QUALITY
            zone_level: BRONZE, SILVER, GOLD
            checkpoint_result: CheckpointResult from GEHelper
            batch_identifier: Optional batch/partition identifier
            checkpoint_name: Optional checkpoint name
        """
        import psycopg2

        checkpoint_name = checkpoint_name or f"{suite_name}_checkpoint"
        batch_identifier = batch_identifier or ""

        conn = None
        try:
            conn = psycopg2.connect(self.pg_connection_string)
            cur = conn.cursor()

            # Write individual expectation results
            for seq, v in enumerate(checkpoint_result.validations):
                partial = json.dumps(
                    v.partial_unexpected_list[:20]
                    if hasattr(v, "partial_unexpected_list") and v.partial_unexpected_list
                    else []
                )
                kwargs_json = json.dumps(v.kwargs if hasattr(v, "kwargs") else {})

                cur.execute("""
                    INSERT INTO platform_validation_result (
                        feed_id, run_id, sequence, validation_type, zone_level,
                        batch_identifier, expectation_suite_name, column_name,
                        expectation_type, result, element_count, error_count,
                        error_rate, partial_unexpected_list, observed_value,
                        kwargs, severity, weight, rule_name
                    ) VALUES (
                        %s, %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s, %s
                    )
                """, (
                    feed_id, run_id, seq, validation_type, zone_level,
                    batch_identifier, suite_name, v.column,
                    v.expectation_type,
                    "PASSED" if v.success else "FAILED",
                    v.element_count, v.unexpected_count,
                    round(v.unexpected_percent / 100, 6) if v.unexpected_percent else 0,
                    partial,
                    str(v.observed_value)[:500] if v.observed_value else None,
                    kwargs_json,
                    v.severity, float(v.weight), v.rule_name,
                ))

            # Write summary
            cur.execute("""
                INSERT INTO platform_validation_summary (
                    feed_id, run_id, checkpoint_name,
                    expectation_suite_name, validation_type, zone_level,
                    total_expectations, successful_expectations,
                    unsuccessful_expectations, quality_score, success,
                    records_validated, duration_seconds
                ) VALUES (
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s, %s,
                    %s, %s
                )
                ON CONFLICT (feed_id, run_id, checkpoint_name)
                DO UPDATE SET
                    total_expectations = EXCLUDED.total_expectations,
                    successful_expectations = EXCLUDED.successful_expectations,
                    unsuccessful_expectations = EXCLUDED.unsuccessful_expectations,
                    quality_score = EXCLUDED.quality_score,
                    success = EXCLUDED.success,
                    records_validated = EXCLUDED.records_validated,
                    duration_seconds = EXCLUDED.duration_seconds,
                    created_at = CURRENT_TIMESTAMP
            """, (
                feed_id, run_id, checkpoint_name,
                suite_name, validation_type, zone_level,
                checkpoint_result.total, checkpoint_result.passed,
                checkpoint_result.failed, checkpoint_result.quality_score,
                checkpoint_result.success,
                checkpoint_result.records_validated,
                checkpoint_result.duration_seconds,
            ))

            conn.commit()
            logger.info(
                f"Wrote {checkpoint_result.total} results + summary for "
                f"{feed_id}/{run_id} ({validation_type}/{zone_level})"
            )

        except Exception as e:
            logger.error(f"Failed to write GE results: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()

    def write_results_spark(
        self,
        spark,
        feed_id: str,
        run_id: str,
        suite_name: str,
        validation_type: str,
        zone_level: str,
        checkpoint_result,
        jdbc_url: str,
        jdbc_properties: dict,
        batch_identifier: Optional[str] = None,
    ) -> None:
        """
        Alternative: write results using Spark JDBC (for Dataproc jobs).

        Uses Spark DataFrame write to PostgreSQL instead of psycopg2.
        """
        from pyspark.sql.types import (
            StructType, StructField, StringType, IntegerType,
            FloatType, BooleanType,
        )

        rows = []
        for seq, v in enumerate(checkpoint_result.validations):
            rows.append((
                feed_id, run_id, seq, validation_type, zone_level,
                batch_identifier or "", suite_name, v.column or "",
                v.expectation_type,
                "PASSED" if v.success else "FAILED",
                v.element_count, v.unexpected_count,
                round(v.unexpected_percent / 100, 6) if v.unexpected_percent else 0.0,
                json.dumps(v.partial_unexpected_list[:20] if v.partial_unexpected_list else []),
                str(v.observed_value)[:500] if v.observed_value else "",
                v.severity, float(v.weight), v.rule_name,
            ))

        schema = StructType([
            StructField("feed_id", StringType()),
            StructField("run_id", StringType()),
            StructField("sequence", IntegerType()),
            StructField("validation_type", StringType()),
            StructField("zone_level", StringType()),
            StructField("batch_identifier", StringType()),
            StructField("expectation_suite_name", StringType()),
            StructField("column_name", StringType()),
            StructField("expectation_type", StringType()),
            StructField("result", StringType()),
            StructField("element_count", IntegerType()),
            StructField("error_count", IntegerType()),
            StructField("error_rate", FloatType()),
            StructField("partial_unexpected_list", StringType()),
            StructField("observed_value", StringType()),
            StructField("severity", StringType()),
            StructField("weight", FloatType()),
            StructField("rule_name", StringType()),
        ])

        df = spark.createDataFrame(rows, schema)
        df.write.jdbc(
            url=jdbc_url,
            table="platform_validation_result",
            mode="append",
            properties=jdbc_properties,
        )

        # Summary
        summary_rows = [(
            feed_id, run_id, f"{suite_name}_checkpoint",
            suite_name, validation_type, zone_level,
            checkpoint_result.total, checkpoint_result.passed,
            checkpoint_result.failed, float(checkpoint_result.quality_score),
            checkpoint_result.success,
            checkpoint_result.records_validated,
            float(checkpoint_result.duration_seconds),
        )]

        summary_schema = StructType([
            StructField("feed_id", StringType()),
            StructField("run_id", StringType()),
            StructField("checkpoint_name", StringType()),
            StructField("expectation_suite_name", StringType()),
            StructField("validation_type", StringType()),
            StructField("zone_level", StringType()),
            StructField("total_expectations", IntegerType()),
            StructField("successful_expectations", IntegerType()),
            StructField("unsuccessful_expectations", IntegerType()),
            StructField("quality_score", FloatType()),
            StructField("success", BooleanType()),
            StructField("records_validated", IntegerType()),
            StructField("duration_seconds", FloatType()),
        ])

        summary_df = spark.createDataFrame(summary_rows, summary_schema)
        summary_df.write.jdbc(
            url=jdbc_url,
            table="platform_validation_summary",
            mode="append",
            properties=jdbc_properties,
        )

        logger.info(
            f"Wrote {checkpoint_result.total} results + summary via Spark JDBC "
            f"for {feed_id}/{run_id}"
        )
