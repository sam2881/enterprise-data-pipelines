"""
Runbooks - Platform Layer

WHY: Remediation scripts are a SHARED resource.
     Both agents (for matching) and backend (for execution) need access.

SINGLE SOURCE OF TRUTH:
- registry.json: Script metadata (id, name, risk, inputs)
- scripts/: Actual script files
- ansible/: Ansible playbooks
- terraform/: Terraform configs
- kubernetes/: K8s manifests

WHO USES THIS:
- agents/servicenow_agent: Script matching (reads registry)
- backend/orchestrator: Script execution (runs scripts)

USAGE:
    from platform.runbooks import get_script_registry, get_script_by_id

    registry = get_script_registry()
    script = get_script_by_id("restart-service-001")
"""
import json
import os
from typing import Any, Dict, List, Optional
from pathlib import Path

RUNBOOKS_DIR = Path(__file__).parent
REGISTRY_PATH = RUNBOOKS_DIR / "registry.json"


def get_script_registry() -> List[Dict[str, Any]]:
    """
    Load the script registry (single source of truth).

    Returns:
        List of script metadata dictionaries
    """
    if not REGISTRY_PATH.exists():
        return []

    with open(REGISTRY_PATH) as f:
        data = json.load(f)

    return data.get("scripts", [])


def get_script_by_id(script_id: str) -> Optional[Dict[str, Any]]:
    """
    Get script metadata by ID.

    Args:
        script_id: Script identifier

    Returns:
        Script metadata or None if not found
    """
    registry = get_script_registry()
    for script in registry:
        if script.get("script_id") == script_id:
            return script
    return None


def get_scripts_by_category(category: str) -> List[Dict[str, Any]]:
    """
    Get all scripts for a category.

    Args:
        category: Script category (infrastructure, application, etc.)

    Returns:
        List of matching scripts
    """
    registry = get_script_registry()
    return [s for s in registry if s.get("category") == category]


def get_script_path(script_id: str) -> Optional[Path]:
    """
    Get the file path for a script.

    Args:
        script_id: Script identifier

    Returns:
        Path to script file or None
    """
    script = get_script_by_id(script_id)
    if not script:
        return None

    script_type = script.get("type", "python")
    filename = script.get("filename", f"{script_id}.py")

    # Determine subdirectory based on type
    if script_type == "ansible":
        subdir = "ansible"
    elif script_type == "terraform":
        subdir = "terraform"
    elif script_type == "kubernetes":
        subdir = "kubernetes"
    else:
        subdir = "scripts"

    path = RUNBOOKS_DIR / subdir / filename
    return path if path.exists() else None


__all__ = [
    "get_script_registry",
    "get_script_by_id",
    "get_scripts_by_category",
    "get_script_path",
    "RUNBOOKS_DIR",
    "REGISTRY_PATH",
]
