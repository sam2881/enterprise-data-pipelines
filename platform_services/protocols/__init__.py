"""
Communication Protocols - Platform Layer

WHY: Shared protocols used by BOTH agents AND backend.
     Protocols define HOW components communicate, not WHAT they do.

WHO USES THIS:
- agents/ (for A2A mesh communication)
- backend/ (for orchestration, event publishing)

WHAT'S HERE:
- a2a/: Agent-to-Agent protocol (WebSocket mesh)
- (TODO) schemas/: Shared Pydantic schemas
- (TODO) contracts/: Interface contracts

USAGE:
    from platform.protocols import A2AClient, MessageType
    from platform.protocols.a2a import A2AMessage
"""
from .a2a import (
    A2AClient,
    A2AMesh,
    A2AMessage,
    MessageType,
    AgentInfo,
)

__all__ = [
    "A2AClient",
    "A2AMesh",
    "A2AMessage",
    "MessageType",
    "AgentInfo",
]
