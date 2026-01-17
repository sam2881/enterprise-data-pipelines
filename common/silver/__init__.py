"""
Silver Layer - Type casting and validation.

WHY LAZY IMPORTS: Airflow workers don't have PySpark installed.
PySpark code runs on Dataproc, not Airflow. We use lazy imports to
prevent ModuleNotFoundError when Airflow parses DAG files.

HOW: Airflow submits jobs to Dataproc where PySpark is available.
These modules are only imported when actually executed on Spark cluster.
"""


def get_silver_transformer():
    """Lazy import SilverTransformer to avoid PySpark import errors in Airflow."""
    from .silver_transformer import SilverTransformer
    return SilverTransformer


# For backward compatibility - wrapped in try/except
try:
    from .silver_transformer import SilverTransformer
except ImportError:
    SilverTransformer = None

__all__ = ["SilverTransformer", "get_silver_transformer"]
