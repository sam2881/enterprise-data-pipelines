"""
Bronze Loader - Generic raw data ingestion.

WHY: Bronze layer preserves raw data exactly as received.
     ALL columns stored as STRING to prevent parse errors.
     Audit columns added for lineage tracking.

HOW: Uses metadata to determine file format and schema.
     Same loader works for ALL feeds - no file-specific logic.

DESIGN DECISIONS:
1. ALL columns as STRING (no exceptions)
2. Add standard audit columns
3. Compute row hash for deduplication
4. Partition by posting_date
"""

import hashlib
from datetime import datetime
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from ..utils.metadata_reader import MetadataReader, FeedConfig, TargetConfig
from ..utils.schema_builder import SchemaBuilder
from ..utils.audit_logger import AuditLogger


class BronzeLoader:
    """
    Load raw data into Bronze layer.

    Usage:
        loader = BronzeLoader(spark)
        result = loader.load(
            feed_id="customer_transactions",
            file_path="gs://bucket/customer_transactions_2024_01_15.csv",
            posting_date="2024-01-15",
            batch_id="batch_123",
        )
    """

    def __init__(
        self,
        spark: SparkSession,
        metadata_reader: Optional[MetadataReader] = None,
        audit_logger: Optional[AuditLogger] = None,
    ):
        """Initialize Bronze loader."""
        self.spark = spark
        self.reader = metadata_reader or MetadataReader()
        self.schema_builder = SchemaBuilder(self.reader)
        self.audit_logger = audit_logger or AuditLogger()

    def load(
        self,
        feed_id: str,
        file_path: str,
        posting_date: str,
        batch_id: str,
        run_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Load file into Bronze layer.

        Args:
            feed_id: Feed identifier
            file_path: Path to source file
            posting_date: Processing date (YYYY-MM-DD)
            batch_id: Unique batch identifier
            run_id: Run ID for audit tracking

        Returns:
            Dictionary with load results (row_count, target_path, etc.)
        """
        result = {
            "status": "unknown",
            "feed_id": feed_id,
            "file_path": file_path,
            "posting_date": posting_date,
            "input_rows": 0,
            "output_rows": 0,
            "target_path": None,
            "error_message": None,
        }

        # Start audit logging
        run_id = run_id or self.audit_logger.generate_run_id(feed_id, posting_date)
        self.audit_logger.log_start(feed_id, "bronze", posting_date, run_id)

        try:
            # Get feed configuration
            feed_config = self.reader.get_feed_config(feed_id)
            if not feed_config:
                raise ValueError(f"Feed not found: {feed_id}")

            target_config = self.reader.get_target_config(feed_id, "bronze")
            if not target_config:
                raise ValueError(f"Bronze target not configured for: {feed_id}")

            # Read source file (all as STRING)
            df = self._read_source_file(file_path, feed_config)
            result["input_rows"] = df.count()

            # Add audit columns
            df = self._add_audit_columns(df, file_path, batch_id)

            # Compute row hash
            df = self._add_row_hash(df)

            # Write to Bronze table
            target_path = self._write_to_bronze(df, target_config, posting_date)
            result["output_rows"] = df.count()
            result["target_path"] = target_path

            # Log success
            self.audit_logger.log_complete(
                feed_id=feed_id,
                run_id=run_id,
                layer="bronze",
                status="success",
                input_rows=result["input_rows"],
                output_rows=result["output_rows"],
            )

            result["status"] = "success"
            result["run_id"] = run_id

        except Exception as e:
            result["status"] = "failed"
            result["error_message"] = str(e)
            self.audit_logger.log_error(
                feed_id=feed_id,
                run_id=run_id,
                layer="bronze",
                error_message=str(e),
            )
            raise

        return result

    def _read_source_file(
        self,
        file_path: str,
        feed_config: FeedConfig,
    ) -> DataFrame:
        """
        Read source file based on feed configuration.

        IMPORTANT: All columns are read as STRING regardless of source type.
        """
        source_type = feed_config.source_type.lower()

        # Get column names from metadata
        columns = self.reader.get_columns(feed_config.feed_id)
        column_names = [c.column_name for c in columns]

        if source_type == "csv":
            df = self.spark.read.format("csv") \
                .option("header", str(feed_config.file_header).lower()) \
                .option("delimiter", feed_config.file_delimiter) \
                .option("inferSchema", "false") \
                .load(file_path)

            # Rename columns to match metadata if header=true
            # Otherwise, use metadata column names
            if not feed_config.file_header:
                for i, col_name in enumerate(column_names):
                    if i < len(df.columns):
                        df = df.withColumnRenamed(df.columns[i], col_name)

        elif source_type == "parquet":
            df = self.spark.read.parquet(file_path)
            # Cast all to STRING for Bronze
            for col in df.columns:
                df = df.withColumn(col, F.col(col).cast(StringType()))

        elif source_type == "json":
            df = self.spark.read.json(file_path)
            # Cast all to STRING for Bronze
            for col in df.columns:
                df = df.withColumn(col, F.col(col).cast(StringType()))

        else:
            # Default: try CSV
            df = self.spark.read.csv(file_path, header=True, inferSchema=False)

        # Ensure all columns are STRING (Bronze rule)
        for col in df.columns:
            df = df.withColumn(col, F.col(col).cast(StringType()))

        return df

    def _add_audit_columns(
        self,
        df: DataFrame,
        file_path: str,
        batch_id: str,
    ) -> DataFrame:
        """Add standard audit columns."""
        return df \
            .withColumn("_source_file", F.lit(file_path)) \
            .withColumn("_ingestion_ts", F.current_timestamp()) \
            .withColumn("_batch_id", F.lit(batch_id))

    def _add_row_hash(self, df: DataFrame) -> DataFrame:
        """
        Add MD5 hash of row content for deduplication.

        Excludes audit columns from hash calculation.
        """
        # Get business columns (exclude audit columns)
        business_cols = [c for c in df.columns if not c.startswith("_")]

        # Concatenate all columns and compute MD5
        concat_expr = F.concat_ws("|", *[F.coalesce(F.col(c), F.lit("NULL")) for c in business_cols])
        return df.withColumn("_row_hash", F.md5(concat_expr))

    def _write_to_bronze(
        self,
        df: DataFrame,
        target_config: TargetConfig,
        posting_date: str,
    ) -> str:
        """Write DataFrame to Bronze table."""
        # Add posting_date partition column
        df = df.withColumn("posting_date", F.lit(posting_date))

        # Build target path
        target_path = f"{target_config.database_name}.{target_config.schema_name}.{target_config.table_name}"

        # Write based on configuration
        writer = df.write \
            .format(target_config.file_format) \
            .mode(target_config.write_mode) \
            .option("compression", target_config.compression)

        # Partition by posting_date and any additional partition columns
        partition_cols = ["posting_date"] + [c for c in target_config.partition_columns if c != "posting_date"]
        writer = writer.partitionBy(*partition_cols)

        # Write to table
        writer.saveAsTable(target_path)

        return target_path


# =============================================================================
# Convenience Function for Airflow Tasks
# =============================================================================

def load_bronze(
    spark: SparkSession,
    feed_id: str,
    file_path: str,
    posting_date: str,
    batch_id: str,
    run_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Convenience function for Airflow task.

    Usage in DAG:
        @task
        def load_bronze_task(**context):
            result = load_bronze(
                spark=get_spark(),
                feed_id=FEED_ID,
                file_path=context["params"]["file_path"],
                posting_date=context["params"]["posting_date"],
                batch_id=context["params"]["batch_id"],
            )
            return result
    """
    loader = BronzeLoader(spark)
    return loader.load(
        feed_id=feed_id,
        file_path=file_path,
        posting_date=posting_date,
        batch_id=batch_id,
        run_id=run_id,
    )
