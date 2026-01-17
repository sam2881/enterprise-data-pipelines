"""
Bronze Layer - Raw data ingestion.

WHY LAZY IMPORTS: Airflow workers don't have PySpark installed.
PySpark code runs on Dataproc, not Airflow. We use lazy imports to
prevent ModuleNotFoundError when Airflow parses DAG files.

HOW: Airflow submits jobs to Dataproc where PySpark is available.
These modules are only imported when actually executed on Spark cluster.
"""


def get_bronze_loader():
    """Lazy import BronzeLoader to avoid PySpark import errors in Airflow."""
    from .bronze_loader import BronzeLoader
    return BronzeLoader


# For backward compatibility - wrapped in try/except
try:
    from .bronze_loader import BronzeLoader
except ImportError:
    BronzeLoader = None

__all__ = ["BronzeLoader", "get_bronze_loader"]
