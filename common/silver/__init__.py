"""
Silver Layer Processing Module.

WHY: Silver layer contains cleansed, typed, and validated data.
     This is the "single source of truth" for downstream processing.

HOW: SilverTransformer applies schema, data quality rules, and cleansing.
     Invalid records are quarantined for review.
"""

# Lazy import to avoid PySpark dependency in Airflow
def get_silver_transformer():
    """Get SilverTransformer class (requires PySpark)."""
    from .transformer import SilverTransformer
    return SilverTransformer

__all__ = ["get_silver_transformer"]
