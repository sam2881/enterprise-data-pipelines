"""
A2A (Agent-to-Agent) Protocol Layer

WHY: Enables real-time, low-latency communication between agents for:
- Swarm consensus (RAG agents voting on results)
- Judge evaluation (LLM-as-Judge quality checks)
- Agent coordination (execution orchestration)

HOW: Uses WebSocket-based mesh network with:
- Message dataclasses for type-safe communication
- A2AClient for sending/receiving messages
- A2AMesh FastAPI server for routing

PROTOCOL CHOICE: A2A is used instead of Kafka for agent-to-agent because:
- Lower latency (~10ms vs ~100ms for Kafka)
- Request-response pattern (Kafka is fire-and-forget)
- No need for audit trail (internal coordination)

LOCATION NOTE: This is the canonical location for A2A protocol.
Backend should import from agents.protocols.a2a, not backend/agents/a2a.
"""
from .messages import (
    MessageType,
    A2AMessage,
    SwarmQueryMessage,
    SwarmVoteMessage,
    JudgeEvaluateMessage,
    JudgeScoreMessage,
    AgentExecuteMessage,
    AgentResultMessage,
)
from .client import A2AClient, AgentInfo
from .mesh import A2AMesh

__all__ = [
    # Messages
    "MessageType",
    "A2AMessage",
    "SwarmQueryMessage",
    "SwarmVoteMessage",
    "JudgeEvaluateMessage",
    "JudgeScoreMessage",
    "AgentExecuteMessage",
    "AgentResultMessage",
    # Client
    "A2AClient",
    "AgentInfo",
    # Server
    "A2AMesh",
]
