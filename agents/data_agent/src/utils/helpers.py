"""Helper functions and utilities."""

import hashlib
import json
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

# Placeholder - implement in Phase 3
# See docs/CLAUDE_CODE_CONTEXT.md for full implementation pattern


def generate_dag_id(domain: str, pipeline_name: str) -> str:
    """
    Generate DAG ID from domain and pipeline name.

    Format: {domain}_{pipeline_name}

    Args:
        domain: Business domain
        pipeline_name: Pipeline name

    Returns:
        Formatted DAG ID
    """
    # Sanitize inputs
    domain = sanitize_identifier(domain)
    pipeline_name = sanitize_identifier(pipeline_name)
    return f"{domain}_{pipeline_name}"


def generate_spark_job_name(pipeline_name: str, layer: str) -> str:
    """
    Generate Spark job name.

    Format: {pipeline_name}_{layer}

    Args:
        pipeline_name: Pipeline name
        layer: Data layer (bronze, silver, gold)

    Returns:
        Formatted job name
    """
    pipeline_name = sanitize_identifier(pipeline_name)
    layer = sanitize_identifier(layer)
    return f"{pipeline_name}_{layer}"


def sanitize_identifier(value: str) -> str:
    """
    Sanitize string for use as identifier.

    - Convert to lowercase
    - Replace spaces and hyphens with underscores
    - Remove special characters
    - Collapse multiple underscores

    Args:
        value: Input string

    Returns:
        Sanitized identifier
    """
    result = value.lower()
    result = re.sub(r"[\s\-]+", "_", result)
    result = re.sub(r"[^a-z0-9_]", "", result)
    result = re.sub(r"_+", "_", result)
    return result.strip("_")


def compute_schema_hash(schema: Dict[str, Any]) -> str:
    """
    Compute deterministic hash of schema definition.

    Args:
        schema: Schema definition dict

    Returns:
        SHA256 hash string
    """
    # Sort keys for deterministic serialization
    canonical = json.dumps(schema, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


def deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """
    Deep merge two dictionaries.

    Args:
        base: Base dictionary
        override: Override dictionary

    Returns:
        Merged dictionary
    """
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def format_timestamp(dt: Optional[datetime] = None) -> str:
    """
    Format datetime as ISO 8601 string.

    Args:
        dt: Datetime to format (default: now)

    Returns:
        ISO 8601 formatted string
    """
    if dt is None:
        dt = datetime.utcnow()
    return dt.isoformat() + "Z"


def chunk_list(items: List[Any], chunk_size: int) -> List[List[Any]]:
    """
    Split list into chunks.

    Args:
        items: List to split
        chunk_size: Size of each chunk

    Returns:
        List of chunks
    """
    return [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]
