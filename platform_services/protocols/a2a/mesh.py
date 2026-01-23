"""
A2A Mesh Server - Central Hub for Agent-to-Agent Communication

WHY: Provides a central routing point for all A2A messages:
- Agent registration and discovery
- Message routing (point-to-point and broadcast)
- Request-response correlation
- Health monitoring via heartbeats

HOW: FastAPI + WebSocket server that maintains:
- Connected agents registry
- Message routing tables
- Correlation tracking for request-response

RUN: python -m backend.agents.a2a.mesh
"""
import asyncio
import json
from datetime import datetime
from typing import Dict, List, Optional, Set
from dataclasses import dataclass, field
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import structlog

from .messages import A2AMessage, MessageType

logger = structlog.get_logger()


@dataclass
class ConnectedAgent:
    """Tracks a connected agent"""
    agent_id: str
    agent_type: str
    capabilities: List[str]
    websocket: WebSocket
    connected_at: datetime = field(default_factory=datetime.utcnow)
    last_heartbeat: datetime = field(default_factory=datetime.utcnow)


class A2AMesh:
    """
    Central mesh server for A2A communication.

    WHY this architecture:
    - Decoupled agents: Agents don't need to know each other's addresses
    - Centralized routing: Easy to add load balancing, filtering
    - Discovery service: Agents can find each other dynamically
    - Resilient: Handles agent disconnections gracefully
    """

    def __init__(self):
        self._agents: Dict[str, ConnectedAgent] = {}
        self._type_registry: Dict[str, Set[str]] = {}  # type -> agent_ids
        self._lock = asyncio.Lock()

    async def register_agent(
        self,
        agent_id: str,
        agent_type: str,
        capabilities: List[str],
        websocket: WebSocket
    ):
        """Register a new agent connection"""
        async with self._lock:
            agent = ConnectedAgent(
                agent_id=agent_id,
                agent_type=agent_type,
                capabilities=capabilities,
                websocket=websocket
            )
            self._agents[agent_id] = agent

            # Update type registry
            if agent_type not in self._type_registry:
                self._type_registry[agent_type] = set()
            self._type_registry[agent_type].add(agent_id)

            logger.info(
                "agent_registered",
                agent_id=agent_id,
                agent_type=agent_type,
                capabilities=capabilities
            )

    async def unregister_agent(self, agent_id: str):
        """Unregister a disconnected agent"""
        async with self._lock:
            if agent_id in self._agents:
                agent = self._agents.pop(agent_id)
                # Remove from type registry
                if agent.agent_type in self._type_registry:
                    self._type_registry[agent.agent_type].discard(agent_id)

                logger.info("agent_unregistered", agent_id=agent_id)

    async def route_message(self, message: A2AMessage):
        """
        Route a message to its destination(s).

        WHY routing logic:
        - target_id = None → Broadcast to all
        - target_id = specific → Point-to-point
        - target_id = type:xxx → All agents of that type
        """
        if message.target_id is None:
            # Broadcast to all except sender
            await self._broadcast(message)
        elif message.target_id.startswith("type:"):
            # Send to all agents of a type
            agent_type = message.target_id.split(":", 1)[1]
            await self._send_to_type(message, agent_type)
        else:
            # Point-to-point
            await self._send_to_agent(message, message.target_id)

    async def _broadcast(self, message: A2AMessage):
        """Broadcast message to all connected agents except sender"""
        async with self._lock:
            for agent_id, agent in self._agents.items():
                if agent_id != message.sender_id:
                    try:
                        await agent.websocket.send_text(message.to_json())
                    except Exception as e:
                        logger.warning(
                            "broadcast_failed",
                            target=agent_id,
                            error=str(e)
                        )

    async def _send_to_type(self, message: A2AMessage, agent_type: str):
        """Send message to all agents of a specific type"""
        async with self._lock:
            agent_ids = self._type_registry.get(agent_type, set())
            for agent_id in agent_ids:
                if agent_id != message.sender_id:
                    agent = self._agents.get(agent_id)
                    if agent:
                        try:
                            await agent.websocket.send_text(message.to_json())
                        except Exception as e:
                            logger.warning(
                                "send_to_type_failed",
                                target=agent_id,
                                error=str(e)
                            )

    async def _send_to_agent(self, message: A2AMessage, target_id: str):
        """Send message to a specific agent"""
        async with self._lock:
            agent = self._agents.get(target_id)
            if agent:
                try:
                    await agent.websocket.send_text(message.to_json())
                except Exception as e:
                    logger.warning(
                        "send_to_agent_failed",
                        target=target_id,
                        error=str(e)
                    )
            else:
                logger.warning("agent_not_found", target=target_id)

    async def handle_discover(self, message: A2AMessage) -> A2AMessage:
        """Handle agent discovery request"""
        requested_type = message.payload.get("agent_type")

        async with self._lock:
            agents_info = []
            for agent_id, agent in self._agents.items():
                if requested_type is None or agent.agent_type == requested_type:
                    agents_info.append({
                        "agent_id": agent.agent_id,
                        "agent_type": agent.agent_type,
                        "capabilities": agent.capabilities,
                        "endpoint": f"ws://mesh/{agent.agent_id}",
                        "last_heartbeat": agent.last_heartbeat.isoformat()
                    })

        return A2AMessage(
            message_type=MessageType.AGENT_DISCOVER,
            sender_id="mesh",
            target_id=message.sender_id,
            correlation_id=message.correlation_id,
            payload={"agents": agents_info}
        )

    async def update_heartbeat(self, agent_id: str):
        """Update agent's last heartbeat time"""
        async with self._lock:
            if agent_id in self._agents:
                self._agents[agent_id].last_heartbeat = datetime.utcnow()

    def get_stats(self) -> Dict:
        """Get mesh statistics"""
        return {
            "total_agents": len(self._agents),
            "agents_by_type": {
                agent_type: len(agents)
                for agent_type, agents in self._type_registry.items()
            },
            "connected_agents": list(self._agents.keys())
        }


# Global mesh instance
mesh = A2AMesh()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler"""
    logger.info("a2a_mesh_starting")
    yield
    logger.info("a2a_mesh_stopping")


# FastAPI application
app = FastAPI(
    title="A2A Mesh Server",
    description="Agent-to-Agent communication mesh for AI Agent Platform",
    version="1.0.0",
    lifespan=lifespan
)


@app.websocket("/ws/{agent_id}")
async def websocket_endpoint(websocket: WebSocket, agent_id: str):
    """
    WebSocket endpoint for agent connections.

    WHY WebSocket:
    - Bidirectional: Both send and receive
    - Low latency: Persistent connection
    - Async: Non-blocking message handling
    """
    await websocket.accept()
    logger.info("websocket_connected", agent_id=agent_id)

    try:
        # Wait for registration message
        raw_msg = await websocket.receive_text()
        msg = A2AMessage.from_json(raw_msg)

        if msg.message_type != MessageType.AGENT_REGISTER:
            logger.error("expected_registration", agent_id=agent_id)
            await websocket.close()
            return

        # Register agent
        await mesh.register_agent(
            agent_id=agent_id,
            agent_type=msg.payload.get("agent_type", "unknown"),
            capabilities=msg.payload.get("capabilities", []),
            websocket=websocket
        )

        # Message handling loop
        while True:
            raw_msg = await websocket.receive_text()
            msg = A2AMessage.from_json(raw_msg)

            logger.debug(
                "message_received",
                agent_id=agent_id,
                message_type=msg.message_type.value,
                target=msg.target_id
            )

            # Handle message based on type
            if msg.message_type == MessageType.AGENT_HEARTBEAT:
                await mesh.update_heartbeat(agent_id)

            elif msg.message_type == MessageType.AGENT_DISCOVER:
                response = await mesh.handle_discover(msg)
                await websocket.send_text(response.to_json())

            else:
                # Route to destination(s)
                await mesh.route_message(msg)

    except WebSocketDisconnect:
        logger.info("websocket_disconnected", agent_id=agent_id)
    except Exception as e:
        logger.error("websocket_error", agent_id=agent_id, error=str(e))
    finally:
        await mesh.unregister_agent(agent_id)


@app.get("/health")
async def health():
    """Health check endpoint"""
    return {"status": "healthy", "stats": mesh.get_stats()}


@app.get("/agents")
async def list_agents():
    """List all connected agents"""
    return mesh.get_stats()


def run_mesh(host: str = "0.0.0.0", port: int = 8765):
    """Run the A2A mesh server"""
    import uvicorn
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    run_mesh()
