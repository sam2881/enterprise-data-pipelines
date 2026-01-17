"""
Gold Aggregator - Business aggregations and metrics.

WHY: Gold is the consumption layer. This is where:
     - Business aggregations are computed
     - Metrics are calculated
     - Data is shaped for specific use cases (reporting, ML, etc.)

HOW: Uses metadata-defined aggregation rules.
     Same aggregator works for ALL feeds that have Gold targets.

DESIGN DECISIONS:
1. Aggregation rules come from feed_aggregations table
2. Supports multiple output tables per feed (daily, monthly, by_region, etc.)
3. Incremental processing based on Silver data
4. Audit columns track lineage back to Silver
"""

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from ..utils.metadata_reader import MetadataReader, TargetConfig
from ..utils.audit_logger import AuditLogger


class GoldAggregator:
    """
    Aggregate Silver data into Gold layer.

    Usage:
        aggregator = GoldAggregator(spark)
        result = aggregator.aggregate(
            feed_id="customer_transactions",
            posting_date="2024-01-15",
            run_id="run_123",
            aggregation_id="daily_summary",
        )
    """

    def __init__(
        self,
        spark: SparkSession,
        metadata_reader: Optional[MetadataReader] = None,
        audit_logger: Optional[AuditLogger] = None,
    ):
        """Initialize Gold aggregator."""
        self.spark = spark
        self.reader = metadata_reader or MetadataReader()
        self.audit_logger = audit_logger or AuditLogger()

    def aggregate(
        self,
        feed_id: str,
        posting_date: str,
        run_id: str,
        aggregation_id: Optional[str] = None,
        silver_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Aggregate Silver data into Gold.

        Args:
            feed_id: Feed identifier
            posting_date: Processing date
            run_id: Run ID for audit tracking
            aggregation_id: Optional specific aggregation to run
            silver_table: Optional Silver table override

        Returns:
            Dictionary with aggregation results
        """
        result = {
            "status": "unknown",
            "feed_id": feed_id,
            "posting_date": posting_date,
            "input_rows": 0,
            "aggregations": [],
            "error_message": None,
        }

        # Start audit logging
        self.audit_logger.log_start(feed_id, "gold", posting_date, run_id)

        try:
            # Get configurations
            silver_config = self.reader.get_target_config(feed_id, "silver")
            gold_configs = self._get_gold_configs(feed_id, aggregation_id)

            if not silver_config:
                raise ValueError(f"Silver target not configured for: {feed_id}")

            if not gold_configs:
                # No Gold aggregations defined - this is OK, just skip
                result["status"] = "skipped"
                result["error_message"] = "No Gold aggregations defined"
                return result

            # Read from Silver
            silver_table = silver_table or f"{silver_config.database_name}.{silver_config.schema_name}.{silver_config.table_name}"
            df_silver = self._read_silver(silver_table, posting_date)
            result["input_rows"] = df_silver.count()

            # Process each aggregation
            for gold_config in gold_configs:
                agg_result = self._process_aggregation(
                    df_silver=df_silver,
                    gold_config=gold_config,
                    feed_id=feed_id,
                    posting_date=posting_date,
                    run_id=run_id,
                )
                result["aggregations"].append(agg_result)

            # Log completion
            total_output = sum(a.get("output_rows", 0) for a in result["aggregations"])
            self.audit_logger.log_complete(
                feed_id=feed_id,
                run_id=run_id,
                layer="gold",
                status="success",
                input_rows=result["input_rows"],
                output_rows=total_output,
            )

            result["status"] = "success"
            result["run_id"] = run_id

        except Exception as e:
            result["status"] = "failed"
            result["error_message"] = str(e)
            self.audit_logger.log_error(
                feed_id=feed_id,
                run_id=run_id,
                layer="gold",
                error_message=str(e),
            )
            raise

        return result

    def _get_gold_configs(
        self,
        feed_id: str,
        aggregation_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get Gold aggregation configurations from metadata.

        Returns list of aggregation configs, each containing:
        - aggregation_id
        - target_config
        - group_by_columns
        - aggregation_expressions
        - filter_condition (optional)
        """
        # Get all Gold targets for this feed
        gold_config = self.reader.get_target_config(feed_id, "gold")
        if not gold_config:
            return []

        # Get aggregation rules from metadata
        aggregations = self.reader.get_aggregation_rules(feed_id)

        if aggregation_id:
            # Filter to specific aggregation
            aggregations = [a for a in aggregations if a.get("aggregation_id") == aggregation_id]

        return aggregations

    def _read_silver(self, silver_table: str, posting_date: str) -> DataFrame:
        """Read from Silver table for specific posting_date."""
        return self.spark.table(silver_table).filter(
            (F.col("posting_date") == posting_date) &
            (F.col("_is_valid") == True)  # Only valid records
        )

    def _process_aggregation(
        self,
        df_silver: DataFrame,
        gold_config: Dict[str, Any],
        feed_id: str,
        posting_date: str,
        run_id: str,
    ) -> Dict[str, Any]:
        """Process a single aggregation configuration."""
        agg_id = gold_config.get("aggregation_id", "default")
        result = {
            "aggregation_id": agg_id,
            "status": "unknown",
            "output_rows": 0,
            "target_path": None,
        }

        try:
            # Apply filter if defined
            df_filtered = df_silver
            filter_condition = gold_config.get("filter_condition")
            if filter_condition:
                df_filtered = df_silver.filter(F.expr(filter_condition))

            # Get group by columns
            group_by_cols = gold_config.get("group_by_columns", [])

            # Get aggregation expressions
            agg_expressions = gold_config.get("aggregation_expressions", {})

            # Build aggregation
            if group_by_cols:
                # Grouped aggregation
                agg_list = []
                for alias, expr in agg_expressions.items():
                    agg_list.append(F.expr(expr).alias(alias))

                if agg_list:
                    df_agg = df_filtered.groupBy(*group_by_cols).agg(*agg_list)
                else:
                    # No aggregations, just distinct
                    df_agg = df_filtered.select(*group_by_cols).distinct()
            else:
                # Full table aggregation
                agg_list = []
                for alias, expr in agg_expressions.items():
                    agg_list.append(F.expr(expr).alias(alias))

                if agg_list:
                    df_agg = df_filtered.agg(*agg_list)
                else:
                    df_agg = df_filtered

            # Add Gold audit columns
            df_final = self._add_gold_audit(df_agg, feed_id, posting_date, run_id, agg_id)

            # Write to Gold
            target_config = gold_config.get("target_config")
            if target_config:
                target_path = self._write_to_gold(df_final, target_config, posting_date)
                result["target_path"] = target_path

            result["output_rows"] = df_final.count()
            result["status"] = "success"

        except Exception as e:
            result["status"] = "failed"
            result["error_message"] = str(e)

        return result

    def _add_gold_audit(
        self,
        df: DataFrame,
        feed_id: str,
        posting_date: str,
        run_id: str,
        aggregation_id: str,
    ) -> DataFrame:
        """Add Gold layer audit columns."""
        return df \
            .withColumn("_gold_ts", F.current_timestamp()) \
            .withColumn("_feed_id", F.lit(feed_id)) \
            .withColumn("_posting_date", F.lit(posting_date)) \
            .withColumn("_run_id", F.lit(run_id)) \
            .withColumn("_aggregation_id", F.lit(aggregation_id))

    def _write_to_gold(
        self,
        df: DataFrame,
        target_config: Dict[str, Any],
        posting_date: str,
    ) -> str:
        """Write DataFrame to Gold table."""
        database = target_config.get("database_name", "gold")
        schema = target_config.get("schema_name", "analytics")
        table = target_config.get("table_name")
        file_format = target_config.get("file_format", "parquet")
        write_mode = target_config.get("write_mode", "overwrite")
        compression = target_config.get("compression", "snappy")
        partition_cols = target_config.get("partition_columns", ["_posting_date"])

        target_path = f"{database}.{schema}.{table}"

        writer = df.write \
            .format(file_format) \
            .mode(write_mode) \
            .option("compression", compression)

        if partition_cols:
            writer = writer.partitionBy(*partition_cols)

        writer.saveAsTable(target_path)
        return target_path


# =============================================================================
# Convenience Function for Airflow Tasks
# =============================================================================

def aggregate_gold(
    spark: SparkSession,
    feed_id: str,
    posting_date: str,
    run_id: str,
    aggregation_id: Optional[str] = None,
    silver_table: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Convenience function for Airflow task.

    Usage in DAG:
        @task
        def aggregate_gold_task(**context):
            result = aggregate_gold(
                spark=get_spark(),
                feed_id=FEED_ID,
                posting_date=context["params"]["posting_date"],
                run_id=XComUtils.pull_run_id(context, "transform_silver"),
            )
            return result
    """
    aggregator = GoldAggregator(spark)
    return aggregator.aggregate(
        feed_id=feed_id,
        posting_date=posting_date,
        run_id=run_id,
        aggregation_id=aggregation_id,
        silver_table=silver_table,
    )
