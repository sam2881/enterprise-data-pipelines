#!/usr/bin/env python3
"""
APEX Data Agent - Generic Join Executor

Resolves and executes multi-table joins from metadata JoinConfig.
Supports: INNER, LEFT, RIGHT, FULL, CROSS, SEMI, ANTI joins.
Includes grain verification (fanout detection) after each join.

Used by silver_to_gold.py when join_dependency entries exist for a contract.
"""

from typing import Dict, Any, List, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, broadcast


# ═══════════════════════════════════════════════════════════════════════════
# JOIN TYPE MAPPING
# ═══════════════════════════════════════════════════════════════════════════

VALID_JOIN_TYPES = {
    "inner": "inner",
    "left": "left",
    "left_outer": "left",
    "right": "right",
    "right_outer": "right",
    "full": "full",
    "full_outer": "full",
    "cross": "cross",
    "semi": "left_semi",
    "left_semi": "left_semi",
    "anti": "left_anti",
    "left_anti": "left_anti",
}


# ═══════════════════════════════════════════════════════════════════════════
# JOIN DEPENDENCY LOADING
# ═══════════════════════════════════════════════════════════════════════════

def load_join_dependencies(
    spark: SparkSession,
    metadata_db: str,
    contract_id: str,
) -> List[Dict[str, Any]]:
    """Load join dependencies from metadata, ordered by join_order."""
    df = spark.read \
        .format("jdbc") \
        .option("url", metadata_db) \
        .option("dbtable", f"""(
            SELECT * FROM join_dependency
            WHERE contract_id = '{contract_id}'
              AND is_active = true
            ORDER BY join_order
        ) AS joins""") \
        .load()

    return [row.asDict() for row in df.collect()]


# ═══════════════════════════════════════════════════════════════════════════
# GRAIN VERIFICATION
# ═══════════════════════════════════════════════════════════════════════════

def verify_grain(
    before_count: int,
    after_count: int,
    join_type: str,
    source_table: str,
    target_table: str,
    max_fanout_ratio: float = 2.0,
) -> None:
    """
    Verify join didn't cause unexpected fanout.

    Raises ValueError if row count increases beyond max_fanout_ratio
    for non-cross joins.
    """
    if join_type == "cross":
        return  # Cross joins always multiply rows

    if join_type in ("left_anti", "left_semi"):
        return  # These always reduce or maintain count

    if before_count == 0:
        return

    ratio = after_count / before_count
    if ratio > max_fanout_ratio:
        raise ValueError(
            f"Join fanout detected: {source_table} JOIN {target_table} "
            f"increased rows from {before_count} to {after_count} "
            f"(ratio {ratio:.1f}x exceeds threshold {max_fanout_ratio}x). "
            f"Check join keys for many-to-many relationship."
        )


# ═══════════════════════════════════════════════════════════════════════════
# JOIN EXECUTION
# ═══════════════════════════════════════════════════════════════════════════

def build_join_condition(
    left_df: DataFrame,
    right_df: DataFrame,
    join_keys: List[Dict[str, str]],
    null_safe: bool = False,
) -> Any:
    """Build PySpark join condition from key mappings."""
    conditions = []
    for key_pair in join_keys:
        left_col = key_pair["left"]
        right_col = key_pair["right"]
        if null_safe:
            conditions.append(
                left_df[left_col].eqNullSafe(right_df[right_col])
            )
        else:
            conditions.append(
                left_df[left_col] == right_df[right_col]
            )

    # Combine with AND
    result = conditions[0]
    for cond in conditions[1:]:
        result = result & cond
    return result


def resolve_column_conflicts(
    left_df: DataFrame,
    right_df: DataFrame,
    join_keys: List[Dict[str, str]],
    select_columns: Optional[List[str]] = None,
) -> List[str]:
    """
    Determine which columns to keep after join.
    Drops duplicate join key columns from the right side.
    """
    right_join_cols = {kp["right"] for kp in join_keys}
    left_cols = set(left_df.columns)

    # If explicit select, use that
    if select_columns:
        return select_columns

    # Otherwise keep all left cols + non-overlapping right cols
    keep_cols = list(left_df.columns)
    for rc in right_df.columns:
        if rc in right_join_cols:
            continue  # Skip duplicate join keys
        if rc in left_cols:
            continue  # Skip columns that already exist in left
        keep_cols.append(rc)
    return keep_cols


def execute_single_join(
    left_df: DataFrame,
    right_df: DataFrame,
    join_type: str,
    join_keys: List[Dict[str, str]],
    select_columns: Optional[List[str]] = None,
    null_safe: bool = False,
    broadcast_right: bool = False,
) -> DataFrame:
    """Execute a single join between two DataFrames."""
    spark_join_type = VALID_JOIN_TYPES.get(join_type.lower())
    if not spark_join_type:
        raise ValueError(
            f"Invalid join type: {join_type}. "
            f"Valid types: {list(VALID_JOIN_TYPES.keys())}"
        )

    # Apply broadcast hint for small tables
    if broadcast_right:
        right_df = broadcast(right_df)

    # Handle cross join separately (no keys needed)
    if spark_join_type == "cross":
        result = left_df.crossJoin(right_df)
    else:
        condition = build_join_condition(left_df, right_df, join_keys, null_safe)
        result = left_df.join(right_df, condition, spark_join_type)

    # Resolve column conflicts
    if select_columns:
        result = result.select(*select_columns)
    else:
        # Drop duplicate join key columns from right side
        right_join_cols = [kp["right"] for kp in join_keys]
        for rc in right_join_cols:
            if rc in left_df.columns and rc in right_df.columns:
                # Drop the right side's version
                result = result.drop(right_df[rc])

    return result


def execute_joins(
    spark: SparkSession,
    base_df: DataFrame,
    join_configs: List[Dict[str, Any]],
    execution_id: str,
    max_fanout_ratio: float = 2.0,
) -> DataFrame:
    """
    Execute a chain of joins from metadata configs.

    Args:
        spark: SparkSession
        base_df: Starting DataFrame (typically the primary Silver table)
        join_configs: List of join dependency dicts from metadata, ordered by join_order
        execution_id: Current execution ID for filtering
        max_fanout_ratio: Max allowed row multiplication per join

    Returns:
        Joined DataFrame
    """
    result_df = base_df

    for i, jc in enumerate(join_configs):
        source_table = jc["source_table"]
        target_table = jc["target_table"]
        join_type = jc.get("join_type", "inner")
        join_keys = jc.get("join_keys", [])
        select_columns = jc.get("select_columns")
        null_safe = jc.get("null_safe", False)
        broadcast_hint = jc.get("broadcast", False)

        # Parse join_keys if stored as JSON string
        if isinstance(join_keys, str):
            import json
            join_keys = json.loads(join_keys)

        print(f"  Join {i+1}/{len(join_configs)}: "
              f"{source_table} {join_type.upper()} JOIN {target_table} "
              f"ON {join_keys}")

        # Load the right-side table
        try:
            right_df = spark.table(target_table)
        except Exception:
            # Try as a path
            right_df = spark.read.format("delta").load(target_table)

        before_count = result_df.count()

        # Execute the join
        result_df = execute_single_join(
            left_df=result_df,
            right_df=right_df,
            join_type=join_type,
            join_keys=join_keys,
            select_columns=select_columns,
            null_safe=null_safe,
            broadcast_right=broadcast_hint,
        )

        after_count = result_df.count()

        # Grain verification
        verify_grain(
            before_count=before_count,
            after_count=after_count,
            join_type=join_type,
            source_table=source_table,
            target_table=target_table,
            max_fanout_ratio=max_fanout_ratio,
        )

        print(f"    Rows: {before_count} → {after_count}")

    return result_df
