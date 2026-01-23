"""
Base RAG Agent - Foundation for Swarm Search Agents

WHY: Provides common infrastructure for all RAG swarm agents:
- A2A client integration for message handling
- Standard search interface
- Metrics and logging
- Error handling

HOW: Subclasses implement _perform_search() with their specific strategy.
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
import asyncio
import time
import structlog

from platform_services.protocols.a2a import A2AClient, SwarmVoteMessage, MessageType

logger = structlog.get_logger()


@dataclass
class SearchContext:
    """Context for a search operation"""
    query: str
    incident_id: str
    incident_type: str
    filters: Dict[str, Any] = field(default_factory=dict)
    max_results: int = 10


@dataclass
class SearchResult:
    """Individual search result from an agent"""
    id: str
    content: str
    score: float
    metadata: Dict[str, Any] = field(default_factory=dict)
    source: str = ""  # Which agent found this


class BaseRAGAgent(ABC):
    """
    Base class for RAG swarm agents.

    WHY inheritance:
    - Consistent interface for all search strategies
    - Shared A2A communication logic
    - Unified error handling and metrics

    Subclasses must implement:
    - _perform_search(): Execute the search strategy
    - agent_type: Property returning the agent type string
    """

    # Weight for this agent in final scoring
    WEIGHT: float = 0.25

    def __init__(
        self,
        agent_id: Optional[str] = None,
        mesh_url: str = "ws://localhost:8765"
    ):
        self.agent_id = agent_id or f"rag_{self.agent_type}"
        self.mesh_url = mesh_url
        self._a2a_client: Optional[A2AClient] = None
        self._running = False

        logger.info(
            "rag_agent_initialized",
            agent_id=self.agent_id,
            agent_type=self.agent_type,
            weight=self.WEIGHT
        )

    @property
    @abstractmethod
    def agent_type(self) -> str:
        """Return the agent type (vector, keyword, graph, metadata)"""
        pass

    @abstractmethod
    async def _perform_search(
        self,
        context: SearchContext
    ) -> List[SearchResult]:
        """
        Perform the actual search operation.

        Args:
            context: Search context with query and filters

        Returns:
            List of SearchResult objects
        """
        pass

    async def start(self):
        """
        Start the agent and connect to A2A mesh.

        Sets up message handlers and begins processing queries.
        """
        self._a2a_client = A2AClient(
            agent_id=self.agent_id,
            agent_type=f"rag_{self.agent_type}",
            mesh_url=self.mesh_url,
            capabilities=["search", self.agent_type]
        )

        # Register query handler
        @self._a2a_client.on_message(MessageType.SWARM_QUERY)
        async def handle_query(msg):
            await self._handle_swarm_query(msg)

        # Connect to mesh
        connected = await self._a2a_client.connect()
        if not connected:
            raise RuntimeError(f"Failed to connect agent {self.agent_id} to A2A mesh")

        self._running = True
        logger.info("rag_agent_started", agent_id=self.agent_id)

        # Start heartbeat
        asyncio.create_task(self._heartbeat_loop())

    async def stop(self):
        """Stop the agent and disconnect from mesh"""
        self._running = False
        if self._a2a_client:
            await self._a2a_client.disconnect()
        logger.info("rag_agent_stopped", agent_id=self.agent_id)

    async def _handle_swarm_query(self, msg):
        """
        Handle incoming swarm query message.

        Performs search and sends vote back to aggregator.
        """
        start_time = time.time()

        try:
            payload = msg.payload
            context = SearchContext(
                query=payload["query"],
                incident_id=payload["incident_id"],
                incident_type=payload.get("incident_type", ""),
                filters=payload.get("filters", {}),
                max_results=payload.get("max_results", 10)
            )

            # Perform search
            results = await self._perform_search(context)

            # Calculate confidence based on result quality
            confidence = self._calculate_confidence(results)

            elapsed_ms = (time.time() - start_time) * 1000

            # Send vote back
            vote = SwarmVoteMessage(
                agent_id=self.agent_id,
                agent_type=self.agent_type,
                correlation_id=msg.correlation_id,
                results=[self._result_to_dict(r) for r in results],
                confidence=confidence,
                search_time_ms=elapsed_ms
            )

            await self._a2a_client.send_swarm_vote(vote)

            logger.info(
                "swarm_query_processed",
                agent_id=self.agent_id,
                results_count=len(results),
                confidence=f"{confidence:.2f}",
                time_ms=f"{elapsed_ms:.1f}"
            )

        except Exception as e:
            elapsed_ms = (time.time() - start_time) * 1000
            logger.error(
                "swarm_query_failed",
                agent_id=self.agent_id,
                error=str(e)
            )

            # Send empty vote on error
            vote = SwarmVoteMessage(
                agent_id=self.agent_id,
                agent_type=self.agent_type,
                correlation_id=msg.correlation_id,
                results=[],
                confidence=0.0,
                search_time_ms=elapsed_ms
            )
            await self._a2a_client.send_swarm_vote(vote)

    def _calculate_confidence(self, results: List[SearchResult]) -> float:
        """
        Calculate confidence score for results.

        Based on:
        - Number of results
        - Score distribution
        - Score magnitude
        """
        if not results:
            return 0.0

        # Base confidence on having results
        base_confidence = min(len(results) / 5, 1.0) * 0.3

        # Score-based confidence
        avg_score = sum(r.score for r in results) / len(results)
        score_confidence = min(avg_score, 1.0) * 0.4

        # Top result confidence
        top_score = results[0].score if results else 0
        top_confidence = min(top_score, 1.0) * 0.3

        confidence = base_confidence + score_confidence + top_confidence

        return min(confidence, 1.0)

    def _result_to_dict(self, result: SearchResult) -> Dict[str, Any]:
        """Convert SearchResult to dictionary"""
        return {
            "id": result.id,
            "content": result.content,
            "score": result.score,
            "metadata": result.metadata,
            "source": self.agent_type
        }

    async def _heartbeat_loop(self):
        """Send periodic heartbeats"""
        while self._running:
            try:
                if self._a2a_client:
                    await self._a2a_client.heartbeat()
            except Exception:
                pass
            await asyncio.sleep(30)

    # Direct search method (without A2A, for testing)
    async def search(self, context: SearchContext) -> List[SearchResult]:
        """
        Perform search directly without A2A.

        Useful for testing and direct invocation.
        """
        return await self._perform_search(context)
