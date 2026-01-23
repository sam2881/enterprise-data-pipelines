"""
Swarm Retriever v5.0 - RRF Fusion with Cross-Encoder Reranking

WHY: Combines multiple search strategies using Reciprocal Rank Fusion (RRF):
- Parallel search across 4 specialized agents
- RRF fusion (NO manual weight tuning required)
- Cross-encoder reranking for precision
- A2A-based communication for real-time coordination

RRF Advantages over Weighted Consensus:
- No manual weight bias (0.40, 0.25, etc.)
- Scale-invariant (works regardless of raw score magnitudes)
- Industry standard (Google, Bing, OpenAI, Elasticsearch)
- Fair fusion based on relative ranking, not absolute scores

HOW:
1. Query understanding (intent, entities)
2. Broadcast query to all swarm agents (A2A)
3. Collect ranked results from each agent
4. RRF Fusion: Score = Σ (1 / (k + rank_i)) for each agent
5. Cross-encoder reranking (top 20 → top 5)
6. Blast radius filtering and return

Architecture:
    Query → [Understanding] → [4 Agents] → [RRF Fusion] → [Cross-Encoder] → Results
"""
import asyncio
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
import time
import structlog

from platform_services.protocols.a2a import A2AClient, SwarmQueryMessage, SwarmVoteMessage, MessageType
from .query_understanding import QueryUnderstanding, QueryContext, IncidentIntent

logger = structlog.get_logger()


# RRF Configuration (replaces manual weights)
RRF_K = 60  # Standard RRF constant (used by Elasticsearch, Pinecone, etc.)
TOP_CANDIDATES_FOR_RERANK = 20  # Send top N to cross-encoder
FINAL_RESULTS = 5  # Return top N after reranking

# Legacy weights (kept for backward compatibility only)
SWARM_WEIGHTS_LEGACY = {
    "vector": 0.40,    # Semantic similarity
    "keyword": 0.25,   # TF-IDF exact match
    "graph": 0.25,     # Neo4j FIXED_BY success
    "metadata": 0.10   # Exact field match
}

# Alias for backward compatibility
SWARM_WEIGHTS = SWARM_WEIGHTS_LEGACY


@dataclass
class SwarmResult:
    """Final result from swarm RRF fusion"""
    id: str
    content: str
    final_score: float  # Final score (RRF or reranked)
    rrf_score: float = 0.0  # Raw RRF score
    rerank_score: float = 0.0  # Cross-encoder score
    metadata: Dict[str, Any] = field(default_factory=dict)
    agent_ranks: Dict[str, int] = field(default_factory=dict)  # {agent: rank}
    agent_scores: Dict[str, float] = field(default_factory=dict)  # {agent: raw_score}
    sources: List[str] = field(default_factory=list)
    rank: int = 0

    # Backward compatibility
    @property
    def score_breakdown(self) -> Dict[str, float]:
        """Legacy score breakdown (for backward compatibility)"""
        return self.agent_scores


@dataclass
class SwarmVote:
    """Vote from a single agent"""
    agent_id: str
    agent_type: str
    results: List[Dict[str, Any]]
    confidence: float
    search_time_ms: float


class SwarmRetriever:
    """
    Orchestrates distributed RAG search with swarm consensus.

    WHY swarm approach:
    - Each agent specializes in one search strategy
    - Parallel execution for speed
    - Weighted consensus for accuracy
    - Resilient to individual agent failures

    Usage:
        retriever = SwarmRetriever()
        await retriever.connect()
        results = await retriever.search("VM instance down", incident_id="INC001")
    """

    def __init__(
        self,
        mesh_url: str = "ws://localhost:8765",
        timeout: float = 10.0,
        enable_reranking: bool = True
    ):
        self.mesh_url = mesh_url
        self.timeout = timeout
        self.enable_reranking = enable_reranking

        self._a2a_client: Optional[A2AClient] = None
        self._query_understanding: Optional[QueryUnderstanding] = None
        self._pending_votes: Dict[str, List[SwarmVote]] = {}
        self._reranker = None

    async def connect(self):
        """Connect to A2A mesh as the swarm aggregator"""
        self._a2a_client = A2AClient(
            agent_id="swarm_aggregator",
            agent_type="orchestrator",
            mesh_url=self.mesh_url,
            capabilities=["aggregator", "consensus"]
        )

        # Register vote handler
        @self._a2a_client.on_message(MessageType.SWARM_VOTE)
        async def handle_vote(msg):
            await self._handle_vote(msg)

        connected = await self._a2a_client.connect()
        if not connected:
            logger.warning("swarm_aggregator_connection_failed")

        # Initialize query understanding
        try:
            self._query_understanding = QueryUnderstanding()
        except Exception as e:
            logger.warning("query_understanding_init_failed", error=str(e))

        # Initialize reranker
        if self.enable_reranking:
            try:
                from .cross_encoder_reranker import CrossEncoderReranker
                self._reranker = CrossEncoderReranker()
            except Exception as e:
                logger.warning("reranker_init_failed", error=str(e))

        logger.info("swarm_retriever_connected")

    async def disconnect(self):
        """Disconnect from A2A mesh"""
        if self._a2a_client:
            await self._a2a_client.disconnect()

    async def search(
        self,
        query: str,
        incident_id: str,
        incident_type: str = "",
        filters: Optional[Dict[str, Any]] = None,
        max_results: int = 10,
        weights: Optional[Dict[str, float]] = None
    ) -> List[SwarmResult]:
        """
        Perform swarm-based distributed search.

        Args:
            query: Search query text
            incident_id: ID of the incident being processed
            incident_type: Type of incident (optional)
            filters: Metadata filters (optional)
            max_results: Maximum results to return
            weights: Custom weights (optional, defaults to SWARM_WEIGHTS)

        Returns:
            List of SwarmResult objects with consensus scores
        """
        start_time = time.time()
        weights = weights or SWARM_WEIGHTS

        # 1. Query understanding
        intent = await self._understand_query(query)

        # Merge extracted entities into filters
        filters = filters or {}
        if intent and intent.extracted_entities:
            filters.update(intent.extracted_entities)

        # 2. Broadcast query to swarm
        correlation_id = await self._broadcast_query(
            query=query,
            incident_id=incident_id,
            incident_type=incident_type or (intent.incident_type if intent else ""),
            filters=filters,
            max_results=max_results * 2  # Get more from each agent, filter later
        )

        # 3. Wait for votes
        votes = await self._collect_votes(correlation_id)

        # 4. Aggregate with weighted consensus
        aggregated = self._aggregate_votes(votes, weights)

        # 5. Rerank if enabled
        if self._reranker and aggregated:
            aggregated = await self._rerank_results(query, aggregated)

        # 6. Limit and assign ranks
        final_results = aggregated[:max_results]
        for i, result in enumerate(final_results):
            result.rank = i + 1

        elapsed_ms = (time.time() - start_time) * 1000

        logger.info(
            "swarm_search_complete",
            incident_id=incident_id,
            query_len=len(query),
            votes_received=len(votes),
            results=len(final_results),
            elapsed_ms=f"{elapsed_ms:.1f}"
        )

        return final_results

    async def _understand_query(self, query: str) -> Optional[QueryContext]:
        """Run query understanding to extract intent and entities"""
        if not self._query_understanding:
            return None

        try:
            # understand() is synchronous, wrap in executor if needed
            return self._query_understanding.understand(query)
        except Exception as e:
            logger.warning("query_understanding_failed", error=str(e))
            return None

    async def _broadcast_query(
        self,
        query: str,
        incident_id: str,
        incident_type: str,
        filters: Dict[str, Any],
        max_results: int
    ) -> str:
        """Broadcast query to all swarm agents"""
        message = SwarmQueryMessage(
            query=query,
            incident_id=incident_id,
            incident_type=incident_type,
            filters=filters,
            max_results=max_results
        )

        # Initialize vote collection
        self._pending_votes[message.correlation_id] = []

        # Broadcast via A2A
        if self._a2a_client:
            await self._a2a_client.send_swarm_query(message)

        return message.correlation_id

    async def _handle_vote(self, msg):
        """Handle incoming vote from an agent"""
        correlation_id = msg.correlation_id
        payload = msg.payload

        if correlation_id in self._pending_votes:
            vote = SwarmVote(
                agent_id=msg.sender_id,
                agent_type=payload.get("agent_type", "unknown"),
                results=payload.get("results", []),
                confidence=payload.get("confidence", 0.0),
                search_time_ms=payload.get("search_time_ms", 0.0)
            )
            self._pending_votes[correlation_id].append(vote)

            logger.debug(
                "vote_received",
                agent=vote.agent_id,
                results=len(vote.results),
                confidence=f"{vote.confidence:.2f}"
            )

    async def _collect_votes(self, correlation_id: str) -> List[SwarmVote]:
        """Wait for votes from all agents with timeout"""
        expected_agents = len(SWARM_WEIGHTS)  # 4 agents
        deadline = time.time() + self.timeout

        while time.time() < deadline:
            votes = self._pending_votes.get(correlation_id, [])
            if len(votes) >= expected_agents:
                break
            await asyncio.sleep(0.1)

        # Get collected votes
        votes = self._pending_votes.pop(correlation_id, [])

        logger.debug(
            "votes_collected",
            expected=expected_agents,
            received=len(votes)
        )

        return votes

    def _aggregate_votes(
        self,
        votes: List[SwarmVote],
        weights: Dict[str, float]
    ) -> List[SwarmResult]:
        """
        Aggregate votes using Reciprocal Rank Fusion (RRF).

        RRF Formula: Score = Σ (1 / (k + rank_i)) for each agent i

        WHY RRF over weighted consensus:
        - No manual weight tuning required
        - Scale-invariant (works regardless of raw score magnitudes)
        - Industry standard (Google, Bing, OpenAI, Elasticsearch)
        """
        # First, collect ranked results per agent
        agent_ranked_results: Dict[str, List[Tuple[str, float, int]]] = {}

        for vote in votes:
            # Sort results by score to get ranking
            sorted_results = sorted(
                vote.results,
                key=lambda x: x.get("score", 0.0),
                reverse=True
            )

            ranked = []
            for rank, result in enumerate(sorted_results, start=1):
                doc_id = result.get("id", "")
                if doc_id:
                    ranked.append((doc_id, result.get("score", 0.0), rank))

            agent_ranked_results[vote.agent_type] = ranked

        # Apply RRF fusion
        doc_data: Dict[str, Dict[str, Any]] = {}

        for agent_type, ranked_results in agent_ranked_results.items():
            for doc_id, raw_score, rank in ranked_results:
                if doc_id not in doc_data:
                    # Find the document content from votes
                    content = ""
                    metadata = {}
                    for vote in votes:
                        for result in vote.results:
                            if result.get("id") == doc_id:
                                content = result.get("content", "")
                                metadata = result.get("metadata", {})
                                break

                    doc_data[doc_id] = {
                        "content": content,
                        "metadata": metadata,
                        "rrf_score": 0.0,
                        "agent_ranks": {},
                        "agent_scores": {},
                        "sources": []
                    }

                # RRF contribution: 1 / (k + rank)
                rrf_contribution = 1.0 / (RRF_K + rank)
                doc_data[doc_id]["rrf_score"] += rrf_contribution
                doc_data[doc_id]["agent_ranks"][agent_type] = rank
                doc_data[doc_id]["agent_scores"][agent_type] = raw_score
                doc_data[doc_id]["sources"].append(agent_type)

        # Build results
        results = []
        for doc_id, data in doc_data.items():
            results.append(SwarmResult(
                id=doc_id,
                content=data["content"],
                final_score=round(data["rrf_score"], 4),
                rrf_score=round(data["rrf_score"], 4),
                rerank_score=0.0,
                metadata=data["metadata"],
                agent_ranks=data["agent_ranks"],
                agent_scores=data["agent_scores"],
                sources=list(set(data["sources"]))
            ))

        # Sort by RRF score
        results.sort(key=lambda x: x.rrf_score, reverse=True)

        logger.debug(
            "rrf_aggregation_complete",
            agents=len(agent_ranked_results),
            documents=len(results),
            top_rrf_score=results[0].rrf_score if results else 0
        )

        return results

    async def _rerank_results(
        self,
        query: str,
        results: List[SwarmResult]
    ) -> List[SwarmResult]:
        """
        Rerank results using cross-encoder model.

        Cross-encoder jointly encodes query and document for more accurate
        relevance scoring than bi-encoder approaches.
        """
        if not self._reranker or not results:
            return results

        try:
            # Take top candidates for reranking
            candidates = results[:TOP_CANDIDATES_FOR_RERANK]

            # Prepare candidates for reranking
            candidate_dicts = [
                {
                    "chunk_id": r.id,
                    "content": r.content,
                    "metadata": r.metadata,
                    "final_score": r.rrf_score  # Use RRF as base
                }
                for r in candidates
            ]

            # Get rerank scores
            reranked = self._reranker.rerank(
                query=query,
                candidates=candidate_dicts,
                top_k=len(candidates)
            )

            # Map back to SwarmResult objects
            id_to_result = {r.id: r for r in candidates}
            final_results = []

            for r in reranked:
                original = id_to_result.get(r.chunk_id)
                if original:
                    # Update with rerank score
                    original.rerank_score = r.rerank_score
                    original.final_score = r.final_score  # Combined score
                    final_results.append(original)

            # Re-sort by final score
            final_results.sort(key=lambda x: x.final_score, reverse=True)

            logger.debug(
                "cross_encoder_rerank_complete",
                candidates=len(candidates),
                reranked=len(final_results)
            )

            return final_results

        except Exception as e:
            logger.warning("reranking_failed", error=str(e))
            return results

    # Direct search method (without A2A, for testing/fallback)
    async def search_direct(
        self,
        query: str,
        documents: List[Dict[str, Any]],
        filters: Optional[Dict[str, Any]] = None,
        max_results: int = 10
    ) -> List[SwarmResult]:
        """
        Perform search directly without A2A (for testing).

        Creates agents locally and runs search.
        """
        from .agents import VectorAgent, KeywordAgent, GraphAgent, MetadataAgent

        # Create agents
        agents = [
            VectorAgent(mesh_url=self.mesh_url),
            KeywordAgent(mesh_url=self.mesh_url),
            GraphAgent(mesh_url=self.mesh_url),
            MetadataAgent(mesh_url=self.mesh_url)
        ]

        # Index documents
        for agent in agents:
            agent.index_documents(documents)

        # Create context
        from .agents.base_rag_agent import SearchContext
        context = SearchContext(
            query=query,
            incident_id="direct_search",
            incident_type="",
            filters=filters or {},
            max_results=max_results * 2
        )

        # Collect votes
        votes = []
        for agent in agents:
            results = await agent.search(context)
            confidence = agent._calculate_confidence(results)

            votes.append(SwarmVote(
                agent_id=agent.agent_id,
                agent_type=agent.agent_type,
                results=[agent._result_to_dict(r) for r in results],
                confidence=confidence,
                search_time_ms=0
            ))

        # Aggregate
        aggregated = self._aggregate_votes(votes, SWARM_WEIGHTS)

        # Limit and rank
        final = aggregated[:max_results]
        for i, r in enumerate(final):
            r.rank = i + 1

        return final


# Global instance
swarm_retriever = SwarmRetriever()
