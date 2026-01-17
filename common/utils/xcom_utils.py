"""
XCom Utilities - Helper functions for Airflow XCom communication.

WHY: Tasks need to pass data between each other:
     - Row counts from Bronze to Silver to Gold
     - Validation results
     - Run IDs for audit trail
     - File paths

HOW: Wrapper functions for consistent XCom usage.

DESIGN DECISIONS:
1. Standardized XCom keys across all feeds
2. Type-safe push/pull methods
3. JSON serialization for complex objects
"""

import json
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, asdict


# =============================================================================
# Standard XCom Keys
# =============================================================================

class XComKeys:
    """Standard XCom key names used across all feeds."""
    RUN_ID = "run_id"
    POSTING_DATE = "posting_date"
    FILE_PATH = "file_path"
    FILE_PATHS = "file_paths"

    # Row counts
    BRONZE_ROW_COUNT = "bronze_row_count"
    SILVER_ROW_COUNT = "silver_row_count"
    GOLD_ROW_COUNT = "gold_row_count"
    REJECTED_ROW_COUNT = "rejected_row_count"

    # Validation
    VALIDATION_RESULT = "validation_result"
    VALIDATION_PASSED = "validation_passed"
    VALIDATION_PASS_RATE = "validation_pass_rate"

    # Status
    LAYER_STATUS = "layer_status"
    ERROR_MESSAGE = "error_message"

    # Metadata
    FEED_CONFIG = "feed_config"
    SCHEMA = "schema"


@dataclass
class LayerMetrics:
    """Metrics passed between layers."""
    layer: str
    input_rows: int
    output_rows: int
    rejected_rows: int
    validation_pass_rate: float
    duration_seconds: int
    status: str
    error_message: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LayerMetrics":
        return cls(**data)


# =============================================================================
# XCom Helper Class
# =============================================================================

class XComUtils:
    """
    Utilities for XCom operations.

    Usage (in Airflow task):
        from common.utils import XComUtils

        # Push data
        XComUtils.push_row_count(context, "bronze", 1000)
        XComUtils.push_validation_result(context, validation_summary)

        # Pull data
        bronze_count = XComUtils.pull_row_count(context, "bronze", "load_bronze")
        is_valid = XComUtils.pull_validation_passed(context, "validate")
    """

    @staticmethod
    def push(context: Dict, key: str, value: Any) -> None:
        """Push value to XCom."""
        ti = context["ti"]
        # Serialize complex objects
        if isinstance(value, (dict, list)):
            value = json.dumps(value)
        elif hasattr(value, "to_dict"):
            value = json.dumps(value.to_dict())
        ti.xcom_push(key=key, value=value)

    @staticmethod
    def pull(
        context: Dict,
        key: str,
        task_id: str,
        default: Any = None
    ) -> Any:
        """Pull value from XCom."""
        ti = context["ti"]
        value = ti.xcom_pull(key=key, task_ids=task_id)
        if value is None:
            return default
        # Try to deserialize JSON
        if isinstance(value, str):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return value
        return value

    # =========================================================================
    # Row Count Methods
    # =========================================================================

    @staticmethod
    def push_row_count(context: Dict, layer: str, count: int) -> None:
        """Push row count for a layer."""
        key = f"{layer}_row_count"
        XComUtils.push(context, key, count)

    @staticmethod
    def pull_row_count(
        context: Dict,
        layer: str,
        task_id: str,
        default: int = 0
    ) -> int:
        """Pull row count for a layer."""
        key = f"{layer}_row_count"
        value = XComUtils.pull(context, key, task_id, default)
        return int(value) if value else default

    @staticmethod
    def push_all_row_counts(
        context: Dict,
        bronze: int = 0,
        silver: int = 0,
        gold: int = 0,
        rejected: int = 0,
    ) -> None:
        """Push all row counts at once."""
        XComUtils.push(context, XComKeys.BRONZE_ROW_COUNT, bronze)
        XComUtils.push(context, XComKeys.SILVER_ROW_COUNT, silver)
        XComUtils.push(context, XComKeys.GOLD_ROW_COUNT, gold)
        XComUtils.push(context, XComKeys.REJECTED_ROW_COUNT, rejected)

    # =========================================================================
    # Validation Methods
    # =========================================================================

    @staticmethod
    def push_validation_result(
        context: Dict,
        passed: bool,
        pass_rate: float,
        details: Optional[Dict] = None,
    ) -> None:
        """Push validation result."""
        XComUtils.push(context, XComKeys.VALIDATION_PASSED, passed)
        XComUtils.push(context, XComKeys.VALIDATION_PASS_RATE, pass_rate)
        if details:
            XComUtils.push(context, XComKeys.VALIDATION_RESULT, details)

    @staticmethod
    def pull_validation_passed(
        context: Dict,
        task_id: str,
    ) -> bool:
        """Pull validation passed status."""
        return XComUtils.pull(context, XComKeys.VALIDATION_PASSED, task_id, False)

    # =========================================================================
    # Run Tracking Methods
    # =========================================================================

    @staticmethod
    def push_run_id(context: Dict, run_id: str) -> None:
        """Push run ID for audit tracking."""
        XComUtils.push(context, XComKeys.RUN_ID, run_id)

    @staticmethod
    def pull_run_id(context: Dict, task_id: str) -> Optional[str]:
        """Pull run ID from earlier task."""
        return XComUtils.pull(context, XComKeys.RUN_ID, task_id)

    @staticmethod
    def push_posting_date(context: Dict, posting_date: str) -> None:
        """Push posting date."""
        XComUtils.push(context, XComKeys.POSTING_DATE, posting_date)

    @staticmethod
    def pull_posting_date(context: Dict, task_id: str) -> Optional[str]:
        """Pull posting date."""
        return XComUtils.pull(context, XComKeys.POSTING_DATE, task_id)

    # =========================================================================
    # Layer Metrics Methods
    # =========================================================================

    @staticmethod
    def push_layer_metrics(context: Dict, metrics: LayerMetrics) -> None:
        """Push complete layer metrics."""
        key = f"{metrics.layer}_metrics"
        XComUtils.push(context, key, metrics.to_dict())

    @staticmethod
    def pull_layer_metrics(
        context: Dict,
        layer: str,
        task_id: str,
    ) -> Optional[LayerMetrics]:
        """Pull layer metrics."""
        key = f"{layer}_metrics"
        data = XComUtils.pull(context, key, task_id)
        if data:
            return LayerMetrics.from_dict(data)
        return None

    # =========================================================================
    # File Path Methods
    # =========================================================================

    @staticmethod
    def push_file_path(context: Dict, file_path: str) -> None:
        """Push processed file path."""
        XComUtils.push(context, XComKeys.FILE_PATH, file_path)

    @staticmethod
    def pull_file_path(context: Dict, task_id: str) -> Optional[str]:
        """Pull file path."""
        return XComUtils.pull(context, XComKeys.FILE_PATH, task_id)

    @staticmethod
    def push_file_paths(context: Dict, file_paths: List[str]) -> None:
        """Push multiple file paths."""
        XComUtils.push(context, XComKeys.FILE_PATHS, file_paths)

    @staticmethod
    def pull_file_paths(context: Dict, task_id: str) -> List[str]:
        """Pull multiple file paths."""
        return XComUtils.pull(context, XComKeys.FILE_PATHS, task_id, [])

    # =========================================================================
    # Error Handling
    # =========================================================================

    @staticmethod
    def push_error(context: Dict, error_message: str) -> None:
        """Push error message."""
        XComUtils.push(context, XComKeys.ERROR_MESSAGE, error_message)

    @staticmethod
    def pull_error(context: Dict, task_id: str) -> Optional[str]:
        """Pull error message."""
        return XComUtils.pull(context, XComKeys.ERROR_MESSAGE, task_id)
