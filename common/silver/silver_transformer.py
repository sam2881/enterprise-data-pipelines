"""
Silver Transformer - Type casting, validation, and cleansing.

WHY: Silver is the trusted, typed layer. This is where:
     - STRING → proper types (from metadata)
     - Data quality validation
     - Deduplication on business keys
     - Bad records quarantined

HOW: Uses metadata for type casting and validation rules.
     Same transformer works for ALL feeds.

DESIGN DECISIONS:
1. Types come from feed_columns.target_type
2. Validation rules from feed_validation
3. Bad records written to quarantine table
4. Deduplication using metadata primary keys
"""

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from ..utils.metadata_reader import MetadataReader, TargetConfig
from ..utils.schema_builder import SchemaBuilder
from ..utils.file_validator import FileValidator, ValidationSummary
from ..utils.audit_logger import AuditLogger


class SilverTransformer:
    """
    Transform Bronze data into Silver layer.

    Usage:
        transformer = SilverTransformer(spark)
        result = transformer.transform(
            feed_id="customer_transactions",
            posting_date="2024-01-15",
            run_id="run_123",
        )
    """

    def __init__(
        self,
        spark: SparkSession,
        metadata_reader: Optional[MetadataReader] = None,
        audit_logger: Optional[AuditLogger] = None,
    ):
        """Initialize Silver transformer."""
        self.spark = spark
        self.reader = metadata_reader or MetadataReader()
        self.schema_builder = SchemaBuilder(self.reader)
        self.validator = FileValidator(self.reader)
        self.audit_logger = audit_logger or AuditLogger()

    def transform(
        self,
        feed_id: str,
        posting_date: str,
        run_id: str,
        bronze_table: Optional[str] = None,
        min_pass_rate: float = 0.95,
    ) -> Dict[str, Any]:
        """
        Transform Bronze data into Silver.

        Args:
            feed_id: Feed identifier
            posting_date: Processing date
            run_id: Run ID for audit tracking
            bronze_table: Optional Bronze table override
            min_pass_rate: Minimum validation pass rate

        Returns:
            Dictionary with transform results
        """
        result = {
            "status": "unknown",
            "feed_id": feed_id,
            "posting_date": posting_date,
            "input_rows": 0,
            "output_rows": 0,
            "rejected_rows": 0,
            "validation_pass_rate": 0.0,
            "target_path": None,
            "error_message": None,
        }

        # Start audit logging
        self.audit_logger.log_start(feed_id, "silver", posting_date, run_id)

        try:
            # Get configurations
            bronze_config = self.reader.get_target_config(feed_id, "bronze")
            silver_config = self.reader.get_target_config(feed_id, "silver")

            if not bronze_config or not silver_config:
                raise ValueError(f"Bronze or Silver target not configured for: {feed_id}")

            # Read from Bronze
            bronze_table = bronze_table or f"{bronze_config.database_name}.{bronze_config.schema_name}.{bronze_config.table_name}"
            df_bronze = self._read_bronze(bronze_table, posting_date)
            result["input_rows"] = df_bronze.count()

            # Apply type casting
            df_typed = self._apply_type_casting(df_bronze, feed_id)

            # Run validation
            validation_summary = self.validator.validate(
                df_typed, feed_id, "silver", posting_date
            )
            result["validation_pass_rate"] = validation_summary.overall_pass_rate

            # Check if validation passes threshold
            if not validation_summary.is_valid(min_pass_rate):
                # Get valid and rejected rows
                df_valid = self.validator.get_valid_rows(df_typed, feed_id, "silver")
                df_rejected = self.validator.get_rejected_rows(df_typed, feed_id, "silver")
                result["rejected_rows"] = df_rejected.count()

                # Write rejected to quarantine
                self._write_quarantine(df_rejected, feed_id, posting_date, run_id)
            else:
                df_valid = df_typed
                result["rejected_rows"] = 0

            # Deduplicate
            df_deduped = self._deduplicate(df_valid, feed_id)

            # Add Silver audit columns
            df_final = self._add_silver_audit(df_deduped, validation_summary.is_valid())

            # Write to Silver
            target_path = self._write_to_silver(df_final, silver_config, posting_date)
            result["output_rows"] = df_final.count()
            result["target_path"] = target_path

            # Log completion
            self.audit_logger.log_complete(
                feed_id=feed_id,
                run_id=run_id,
                layer="silver",
                status="success",
                input_rows=result["input_rows"],
                output_rows=result["output_rows"],
                rejected_rows=result["rejected_rows"],
                validation_pass_rate=result["validation_pass_rate"],
            )

            result["status"] = "success"
            result["run_id"] = run_id
            result["validation_summary"] = {
                "total_rules": validation_summary.total_rules,
                "passed_rules": validation_summary.passed_rules,
                "failed_rules": validation_summary.failed_rules,
            }

        except Exception as e:
            result["status"] = "failed"
            result["error_message"] = str(e)
            self.audit_logger.log_error(
                feed_id=feed_id,
                run_id=run_id,
                layer="silver",
                error_message=str(e),
            )
            raise

        return result

    def _read_bronze(self, bronze_table: str, posting_date: str) -> DataFrame:
        """Read from Bronze table for specific posting_date."""
        return self.spark.table(bronze_table).filter(
            F.col("posting_date") == posting_date
        )

    def _apply_type_casting(self, df: DataFrame, feed_id: str) -> DataFrame:
        """
        Apply type casting from STRING to target types.

        Types come from feed_columns.target_type in metadata.
        """
        cast_expressions = self.schema_builder.get_cast_expressions(feed_id)

        for col_name, cast_expr, target_type in cast_expressions:
            try:
                df = df.withColumn(
                    col_name,
                    F.expr(cast_expr)
                )
            except Exception as e:
                # Log casting error but continue (NULL will be set)
                print(f"Warning: Cast failed for {col_name}: {e}")
                df = df.withColumn(col_name, F.lit(None))

        return df

    def _deduplicate(self, df: DataFrame, feed_id: str) -> DataFrame:
        """
        Deduplicate using primary keys from metadata.

        Uses latest record based on _ingestion_ts.
        """
        pk_columns = self.reader.get_primary_keys(feed_id)

        if not pk_columns:
            # No dedup if no PK defined
            return df

        # Window to get latest record per key
        window = Window.partitionBy(*pk_columns).orderBy(
            F.col("_ingestion_ts").desc()
        )

        return df \
            .withColumn("_row_num", F.row_number().over(window)) \
            .filter(F.col("_row_num") == 1) \
            .drop("_row_num")

    def _add_silver_audit(self, df: DataFrame, is_valid: bool) -> DataFrame:
        """Add Silver layer audit columns."""
        return df \
            .withColumn("_silver_ts", F.current_timestamp()) \
            .withColumn("_is_valid", F.lit(is_valid))

    def _write_to_silver(
        self,
        df: DataFrame,
        target_config: TargetConfig,
        posting_date: str,
    ) -> str:
        """Write DataFrame to Silver table."""
        target_path = f"{target_config.database_name}.{target_config.schema_name}.{target_config.table_name}"

        writer = df.write \
            .format(target_config.file_format) \
            .mode(target_config.write_mode) \
            .option("compression", target_config.compression)

        # Partition
        partition_cols = ["posting_date"] + [c for c in target_config.partition_columns if c != "posting_date"]
        writer = writer.partitionBy(*partition_cols)

        writer.saveAsTable(target_path)
        return target_path

    def _write_quarantine(
        self,
        df: DataFrame,
        feed_id: str,
        posting_date: str,
        run_id: str,
    ) -> None:
        """Write rejected records to quarantine table."""
        if df.count() == 0:
            return

        # Add quarantine metadata
        df_quarantine = df \
            .withColumn("_feed_id", F.lit(feed_id)) \
            .withColumn("_posting_date", F.lit(posting_date)) \
            .withColumn("_run_id", F.lit(run_id)) \
            .withColumn("_quarantine_ts", F.current_timestamp()) \
            .withColumn("_quarantine_reason", F.lit("validation_failed"))

        # Write to quarantine table
        quarantine_table = f"audit.quarantine_{feed_id}"
        df_quarantine.write \
            .format("parquet") \
            .mode("append") \
            .partitionBy("_posting_date") \
            .saveAsTable(quarantine_table)


# =============================================================================
# Convenience Function for Airflow Tasks
# =============================================================================

def transform_silver(
    spark: SparkSession,
    feed_id: str,
    posting_date: str,
    run_id: str,
    bronze_table: Optional[str] = None,
    min_pass_rate: float = 0.95,
) -> Dict[str, Any]:
    """
    Convenience function for Airflow task.

    Usage in DAG:
        @task
        def transform_silver_task(**context):
            result = transform_silver(
                spark=get_spark(),
                feed_id=FEED_ID,
                posting_date=context["params"]["posting_date"],
                run_id=XComUtils.pull_run_id(context, "load_bronze"),
            )
            return result
    """
    transformer = SilverTransformer(spark)
    return transformer.transform(
        feed_id=feed_id,
        posting_date=posting_date,
        run_id=run_id,
        bronze_table=bronze_table,
        min_pass_rate=min_pass_rate,
    )
