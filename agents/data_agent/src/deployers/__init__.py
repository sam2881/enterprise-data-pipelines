"""
Deployment logic for Git, CI/CD, Airflow, and Cloud Composer.

This module provides deployment clients:
- GitClient: Git operations for branch creation, commits, PRs
- CICDTrigger: Cloud Build API client for pipeline deployment
- AirflowClient: Local Airflow REST API for DAG management (Docker)
- ComposerClient: Cloud Composer client for GCP deployments (deprecated)

Architecture:
- Local/Docker: Use AirflowClient to upload DAGs to mounted volume
- GCP Production: Use GitHub Actions to sync DAGs to Cloud Composer
"""

from src.deployers.airflow_client import AirflowClient, get_airflow_client
from src.deployers.cicd_trigger import CICDTrigger
from src.deployers.git_client import GitClient

# Optional: Import ComposerClient for backwards compatibility
try:
    from src.deployers.composer_client import ComposerClient, get_composer_client
except ImportError:
    ComposerClient = None
    get_composer_client = None

__all__ = [
    "GitClient",
    "CICDTrigger",
    "AirflowClient",
    "get_airflow_client",
    # Deprecated - use AirflowClient for local, GitHub Actions for GCP
    "ComposerClient",
    "get_composer_client",
]
