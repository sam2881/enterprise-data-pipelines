"""
Gold Layer Processing Module.

WHY: Gold layer contains business-level aggregations and metrics.
     This is optimized for analytics and reporting.

HOW: GoldAggregator joins silver tables and computes aggregations.
     Supports multiple modeling strategies (Star Schema, Data Vault, etc.)
"""

# Lazy import to avoid PySpark dependency in Airflow
def get_gold_aggregator():
    """Get GoldAggregator class (requires PySpark)."""
    from .aggregator import GoldAggregator
    return GoldAggregator

__all__ = ["get_gold_aggregator"]
