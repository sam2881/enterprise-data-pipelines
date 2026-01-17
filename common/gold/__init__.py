"""
Gold Layer - Business aggregations and metrics.

WHY LAZY IMPORTS: Airflow workers don't have PySpark installed.
PySpark code runs on Dataproc, not Airflow. We use lazy imports to
prevent ModuleNotFoundError when Airflow parses DAG files.

HOW: Airflow submits jobs to Dataproc where PySpark is available.
These modules are only imported when actually executed on Spark cluster.
"""


def get_gold_aggregator():
    """Lazy import GoldAggregator to avoid PySpark import errors in Airflow."""
    from .gold_aggregator import GoldAggregator
    return GoldAggregator


def get_aggregate_gold():
    """Lazy import aggregate_gold function to avoid PySpark import errors in Airflow."""
    from .gold_aggregator import aggregate_gold
    return aggregate_gold


# For backward compatibility - wrapped in try/except
try:
    from .gold_aggregator import GoldAggregator, aggregate_gold
except ImportError:
    GoldAggregator = None
    aggregate_gold = None

__all__ = ["GoldAggregator", "aggregate_gold", "get_gold_aggregator", "get_aggregate_gold"]
