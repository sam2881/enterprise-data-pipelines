"""
Bronze Layer Processing Module.

WHY: Bronze layer stores raw data with minimal transformation (all STRING types).
     This preserves the original data for debugging and reprocessing.

HOW: BronzeLoader reads from source (GCS) and writes to Bronze bucket.
     All columns are cast to STRING with audit columns added.
"""

# Lazy import to avoid PySpark dependency in Airflow
def get_bronze_loader():
    """Get BronzeLoader class (requires PySpark)."""
    from .loader import BronzeLoader
    return BronzeLoader

__all__ = ["get_bronze_loader"]
