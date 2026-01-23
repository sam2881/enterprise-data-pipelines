"""
Platform Layer - Shared Capabilities

WHY: Platform provides shared infrastructure that BOTH agents and backend use.
     This removes duplication and keeps agents thin.

WHAT'S HERE:
- infrastructure_clients/: GCP, Kafka, Redis, Postgres clients
- protocols/: A2A, schemas, contracts
- runbooks/: Scripts + registry (single source of truth)
- metadata/: Shared metadata models
- utils/: Common utilities

WHAT'S NOT HERE:
- Agent logic (lives in agents/)
- API routes (lives in backend/)
- UI code (lives in frontend/)

OWNERSHIP:
- Platform Team owns this layer
- Both Agent Team and Backend Team consume it

NOTE: Named 'platform_services' to avoid conflict with Python's 'platform' module.

USAGE:
    # From agents
    from platform_services.infrastructure_clients import get_redis_client

    # From backend
    from platform_services.protocols import A2AClient, MessageType

    # Metadata models
    from platform_services.metadata import IncidentMetadata, PipelineMetadata

    # Runbooks
    from platform_services.runbooks import get_script_registry
"""
__version__ = "5.0.0"

# Re-export commonly used items for convenience
from .utils import get_logger, compute_hash, get_env, get_secret

__all__ = [
    "get_logger",
    "compute_hash",
    "get_env",
    "get_secret",
]
