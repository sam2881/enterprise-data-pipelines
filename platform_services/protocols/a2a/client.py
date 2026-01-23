"""
A2A Client for Agent-to-Agent Communication

WHY: Provides a simple interface for agents to communicate with each other
without needing to manage WebSocket connections directly. Enables:
- Point-to-point messaging (send to specific agent)
- Broadcast messaging (send to all agents)
- Request-response pattern (send and wait for reply)
- Agent discovery (find available agents)

HOW: Uses WebSocket connections to the A2A mesh server, with:
- Automatic reconnection on failure
- Correlation ID tracking for request-response
- Async message handling
"""
import asyncio
import json
from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass
import websockets
from websockets.client import WebSocketClientProtocol
import structlog

from .messages import (
    A2AMessage,
    MessageType,
    SwarmQueryMessage,
    SwarmVoteMessage,
    JudgeEvaluateMessage,
    JudgeScoreMessage,
    AgentExecuteMessage,
    AgentResultMessage,
)

logger = structlog.get_logger()


@dataclass
class AgentInfo:
    """Information about a registered agent"""
    agent_id: str
    agent_type: str
    capabilities: List[str]
    endpoint: str
    last_heartbeat: str


class A2AClient:
    """
    Client for A2A mesh communication.

    WHY this design:
    - Single connection per agent (efficient)
    - Message handlers for async processing
    - Pending requests dict for correlation
    - Reconnection logic for resilience
    """

    def __init__(
        self,
        agent_id: str,
        agent_type: str,
        mesh_url: str = "ws://localhost:8765",
        capabilities: Optional[List[str]] = None,
    ):
        self.agent_id = agent_id
        self.agent_type = agent_type
        self.mesh_url = mesh_url
        self.capabilities = capabilities or []

        self._ws: Optional[WebSocketClientProtocol] = None
        self._connected = False
        self._handlers: Dict[MessageType, Callable] = {}
        self._pending_requests: Dict[str, asyncio.Future] = {}
        self._receive_task: Optional[asyncio.Task] = None

    async def connect(self) -> bool:
        """
        Connect to the A2A mesh and register this agent.

        WHY: Registration allows the mesh to route messages correctly
        and enables agent discovery.
        """
        try:
            self._ws = await websockets.connect(self.mesh_url)
            self._connected = True

            # Register with mesh
            register_msg = A2AMessage(
                message_type=MessageType.AGENT_REGISTER,
                sender_id=self.agent_id,
                payload={
                    "agent_type": self.agent_type,
                    "capabilities": self.capabilities,
                }
            )
            await self._ws.send(register_msg.to_json())

            # Start receive loop
            self._receive_task = asyncio.create_task(self._receive_loop())

            logger.info(
                "a2a_connected",
                agent_id=self.agent_id,
                mesh_url=self.mesh_url
            )
            return True

        except Exception as e:
            logger.error("a2a_connect_failed", error=str(e))
            return False

    async def disconnect(self):
        """Disconnect from the A2A mesh"""
        self._connected = False
        if self._receive_task:
            self._receive_task.cancel()
        if self._ws:
            await self._ws.close()
        logger.info("a2a_disconnected", agent_id=self.agent_id)

    async def _receive_loop(self):
        """
        Background task to receive and dispatch messages.

        WHY: Async receive loop allows non-blocking message processing
        while the agent does other work.
        """
        while self._connected and self._ws:
            try:
                raw_msg = await self._ws.recv()
                msg = A2AMessage.from_json(raw_msg)

                logger.debug(
                    "a2a_received",
                    message_type=msg.message_type.value,
                    sender=msg.sender_id,
                    correlation_id=msg.correlation_id
                )

                # Check if this is a response to a pending request
                if msg.correlation_id in self._pending_requests:
                    future = self._pending_requests.pop(msg.correlation_id)
                    future.set_result(msg)

                # Dispatch to registered handler
                elif msg.message_type in self._handlers:
                    handler = self._handlers[msg.message_type]
                    asyncio.create_task(handler(msg))

            except websockets.ConnectionClosed:
                logger.warning("a2a_connection_closed", agent_id=self.agent_id)
                await self._reconnect()
            except Exception as e:
                logger.error("a2a_receive_error", error=str(e))

    async def _reconnect(self):
        """Attempt to reconnect to the mesh"""
        self._connected = False
        for attempt in range(5):
            await asyncio.sleep(2 ** attempt)  # Exponential backoff
            if await self.connect():
                return
        logger.error("a2a_reconnect_failed", agent_id=self.agent_id)

    def on_message(self, message_type: MessageType):
        """
        Decorator to register a message handler.

        Usage:
            @client.on_message(MessageType.SWARM_QUERY)
            async def handle_query(msg: A2AMessage):
                # Process query
        """
        def decorator(handler: Callable):
            self._handlers[message_type] = handler
            return handler
        return decorator

    async def send(self, message: A2AMessage) -> bool:
        """
        Send a message without waiting for response.

        WHY: Fire-and-forget for events that don't need acknowledgment,
        like heartbeats or state updates.
        """
        if not self._ws or not self._connected:
            logger.error("a2a_not_connected", agent_id=self.agent_id)
            return False

        try:
            await self._ws.send(message.to_json())
            logger.debug(
                "a2a_sent",
                message_type=message.message_type.value,
                target=message.target_id
            )
            return True
        except Exception as e:
            logger.error("a2a_send_failed", error=str(e))
            return False

    async def request(
        self,
        message: A2AMessage,
        timeout: float = 30.0
    ) -> Optional[A2AMessage]:
        """
        Send a message and wait for a correlated response.

        WHY: Request-response pattern for operations that need acknowledgment,
        like judge evaluation or execution confirmation.
        """
        if not self._ws or not self._connected:
            logger.error("a2a_not_connected", agent_id=self.agent_id)
            return None

        # Create future for response
        future: asyncio.Future = asyncio.Future()
        self._pending_requests[message.correlation_id] = future

        try:
            # Send request
            await self._ws.send(message.to_json())

            # Wait for response with timeout
            response = await asyncio.wait_for(future, timeout=timeout)
            return response

        except asyncio.TimeoutError:
            logger.error(
                "a2a_request_timeout",
                correlation_id=message.correlation_id,
                timeout=timeout
            )
            self._pending_requests.pop(message.correlation_id, None)
            return None
        except Exception as e:
            logger.error("a2a_request_failed", error=str(e))
            self._pending_requests.pop(message.correlation_id, None)
            return None

    async def broadcast(self, message: A2AMessage) -> bool:
        """
        Broadcast a message to all agents.

        WHY: Used for swarm queries where all RAG agents need to search.
        """
        message.target_id = None  # None = broadcast
        return await self.send(message)

    async def discover(self, agent_type: Optional[str] = None) -> List[AgentInfo]:
        """
        Discover available agents in the mesh.

        WHY: Allows dynamic discovery of agents for load balancing
        and capability matching.
        """
        discover_msg = A2AMessage(
            message_type=MessageType.AGENT_DISCOVER,
            sender_id=self.agent_id,
            payload={"agent_type": agent_type} if agent_type else {}
        )

        response = await self.request(discover_msg, timeout=5.0)
        if not response:
            return []

        agents = response.payload.get("agents", [])
        return [AgentInfo(**agent) for agent in agents]

    # Convenience methods for specific message types

    async def send_swarm_query(self, query: SwarmQueryMessage) -> bool:
        """Send a swarm query to all RAG agents"""
        return await self.broadcast(query.to_a2a())

    async def send_swarm_vote(self, vote: SwarmVoteMessage) -> bool:
        """Send a swarm vote back to the aggregator"""
        return await self.send(vote.to_a2a())

    async def request_judge_evaluation(
        self,
        request: JudgeEvaluateMessage,
        timeout: float = 60.0
    ) -> Optional[JudgeScoreMessage]:
        """Request LLM-as-Judge evaluation and wait for score"""
        response = await self.request(request.to_a2a(), timeout=timeout)
        if not response:
            return None

        payload = response.payload
        return JudgeScoreMessage(
            correlation_id=response.correlation_id,
            incident_id=payload["incident_id"],
            quality_score=payload["quality_score"],
            safety_passed=payload["safety_passed"],
            factual_score=payload["factual_score"],
            feasibility_score=payload["feasibility_score"],
            risk_level=payload["risk_level"],
            reasoning=payload["reasoning"],
            suggested_improvements=payload.get("suggested_improvements", []),
        )

    async def request_agent_execute(
        self,
        request: AgentExecuteMessage,
        timeout: float = 300.0  # 5 minutes for execution
    ) -> Optional[AgentResultMessage]:
        """Request agent execution and wait for result"""
        response = await self.request(request.to_a2a(), timeout=timeout)
        if not response:
            return None

        payload = response.payload
        return AgentResultMessage(
            correlation_id=response.correlation_id,
            incident_id=payload["incident_id"],
            agent_id=response.sender_id,
            status=payload["status"],
            execution_time_ms=payload["execution_time_ms"],
            output=payload["output"],
            error=payload.get("error"),
            rollback_available=payload.get("rollback_available", False),
            github_run_id=payload.get("github_run_id"),
        )

    async def heartbeat(self):
        """Send heartbeat to keep connection alive"""
        heartbeat_msg = A2AMessage(
            message_type=MessageType.AGENT_HEARTBEAT,
            sender_id=self.agent_id,
            payload={"status": "alive"}
        )
        await self.send(heartbeat_msg)
