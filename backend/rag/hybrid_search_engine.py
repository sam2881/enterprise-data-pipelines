"""
Hybrid Search Engine v5.0 - RRF Fusion + Cross-Encoder Reranking
=================================================================

Enterprise-Grade Multi-Source Search with Reciprocal Rank Fusion (RRF).

Architecture (Industry Standard - Google/Bing/OpenAI Pattern):
    Query → [Query Understanding] → [4 Parallel Agents] → [RRF Fusion] → [Cross-Encoder Rerank] → Results

    Step 1: Query Understanding (LLM expands + extracts entities)
    Step 2: 4 Agents Search in Parallel:
            - Vector Agent (Semantic similarity - Weaviate)
            - Keyword Agent (BM25/TF-IDF exact match)
            - Graph Agent (Neo4j FIXED_BY relationships)
            - Metadata Agent (Exact field matching)
    Step 3: RRF Fusion (NO manual weights - rank-based fusion)
    Step 4: Cross-Encoder Reranking (pairwise LLM relevance)
    Step 5: Blast Radius Filter (risk assessment)
    Step 6: Final Script Selection

RRF Formula (Reciprocal Rank Fusion):
    RRF_Score = Σ (1 / (k + rank_i)) for each agent i
    k = 60 (standard constant, reduces impact of outliers)

WHY RRF over Weighted Scores:
    - No manual weight tuning required
    - Scale-invariant (works regardless of raw score magnitudes)
    - Fair fusion (each agent contributes based on relative ordering)
    - Industry standard (used by Elasticsearch, Pinecone, Cohere, OpenAI)

Version: 5.0.0
Author: AI Agent Platform
"""

import os
import re
import numpy as np
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field, asdict
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import structlog

logger = structlog.get_logger()


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class SearchWeights:
    """
    DEPRECATED in v5.0 - RRF doesn't require manual weights.

    Kept for backward compatibility with legacy code.
    New code should use RRF fusion which is weight-free.

    Legacy Weights (v4.0):
    - semantic: 0.40 (Vector similarity)
    - keyword:  0.25 (TF-IDF keyword match)
    - metadata: 0.10 (Exact field match)
    - graph:    0.25 (Historical success - FIXED_BY)
    """
    semantic: float = 0.40   # Vector similarity weight
    keyword: float = 0.25    # TF-IDF keyword weight
    metadata: float = 0.10   # Metadata exact match weight
    graph: float = 0.25      # Graph/historical success weight

    def __post_init__(self):
        """Normalize weights to sum to 1.0"""
        total = self.semantic + self.keyword + self.metadata + self.graph
        if abs(total - 1.0) > 0.01:
            self.semantic /= total
            self.keyword /= total
            self.metadata /= total
            self.graph /= total

    def to_dict(self) -> Dict[str, float]:
        return {
            "semantic": round(self.semantic, 3),
            "keyword": round(self.keyword, 3),
            "metadata": round(self.metadata, 3),
            "graph": round(self.graph, 3)
        }

    @classmethod
    def legacy(cls) -> 'SearchWeights':
        """Legacy v3.0 weights (no graph)"""
        return cls(semantic=0.60, keyword=0.30, metadata=0.10, graph=0.00)


@dataclass
class RRFConfig:
    """
    Configuration for Reciprocal Rank Fusion (RRF).

    RRF Formula: Score = Σ (1 / (k + rank_i))

    k = 60 is the standard constant used by:
    - Elasticsearch
    - Pinecone
    - Cohere
    - OpenAI RAG implementations

    Higher k reduces the impact of high-ranking documents.
    Lower k amplifies differences in top rankings.
    """
    k: int = 60  # RRF constant (standard: 60)
    min_agents_required: int = 2  # Minimum agents for valid consensus
    top_candidates_for_rerank: int = 20  # Send top N to cross-encoder
    final_results: int = 5  # Return top N after reranking

    def to_dict(self) -> Dict[str, Any]:
        return {
            "k": self.k,
            "min_agents_required": self.min_agents_required,
            "top_candidates_for_rerank": self.top_candidates_for_rerank,
            "final_results": self.final_results
        }


@dataclass
class AgentRankedResult:
    """Result from a single agent with its rank"""
    doc_id: str
    content: str
    metadata: Dict[str, Any]
    score: float  # Agent's raw score
    rank: int     # Agent's ranking (1 = best)
    agent_type: str  # vector, keyword, graph, metadata


@dataclass
class SearchResult:
    """
    Result from hybrid search with RRF fusion score breakdown.

    RRF scoring provides fair, weight-free fusion of multiple agents.
    """
    chunk_id: str
    content: str
    metadata: Dict[str, Any]
    final_score: float  # RRF score (or reranked score if cross-encoder applied)
    rrf_score: float = 0.0  # Raw RRF score before reranking
    rerank_score: float = 0.0  # Cross-encoder score (0 if not reranked)
    agent_ranks: Dict[str, int] = field(default_factory=dict)  # {agent: rank}
    agent_scores: Dict[str, float] = field(default_factory=dict)  # {agent: raw_score}
    score_breakdown: Dict[str, float] = field(default_factory=dict)  # Legacy v4.0 breakdown
    match_reasons: List[str] = field(default_factory=list)
    sources: List[str] = field(default_factory=list)  # Which agents found this
    rank: int = 0  # Final rank after all processing

    def to_dict(self) -> Dict[str, Any]:
        return {
            "chunk_id": self.chunk_id,
            "content": self.content,
            "metadata": self.metadata,
            "final_score": round(self.final_score, 4),
            "rrf_score": round(self.rrf_score, 4),
            "rerank_score": round(self.rerank_score, 4),
            "agent_ranks": self.agent_ranks,
            "agent_scores": {k: round(v, 4) for k, v in self.agent_scores.items()},
            "score_breakdown": {k: round(v, 4) for k, v in self.score_breakdown.items()},
            "match_reasons": self.match_reasons,
            "sources": self.sources,
            "rank": self.rank
        }


# =============================================================================
# HYBRID SEARCH ENGINE v5.0 - RRF FUSION
# =============================================================================

class HybridSearchEngine:
    """
    Enterprise-Grade Hybrid Search Engine v5.0 with RRF Fusion

    Key Features:
    1. Reciprocal Rank Fusion (RRF) - No manual weight tuning required
    2. Cross-Encoder Reranking - Improved precision with pairwise relevance
    3. Blast Radius Filtering - Risk-aware result filtering
    4. Multi-Agent Architecture - 4 specialized search agents

    Search Agents:
    1. Vector Agent - Semantic similarity using embeddings (Weaviate)
    2. Keyword Agent - BM25/TF-IDF keyword matching
    3. Graph Agent - Historical success from Neo4j FIXED_BY relationships
    4. Metadata Agent - Exact match on structured fields

    RRF Formula:
        RRF_Score = Σ (1 / (k + rank_i)) for each agent i
        k = 60 (industry standard constant)

    Usage:
        engine = HybridSearchEngine()
        engine.index_documents(documents)
        results = engine.search_rrf("VM instance down in us-central1")
    """

    # Legacy weight presets (kept for backward compatibility)
    WEIGHT_PRESETS = {
        "security": SearchWeights(semantic=0.30, keyword=0.35, metadata=0.10, graph=0.25),
        "performance": SearchWeights(semantic=0.45, keyword=0.20, metadata=0.10, graph=0.25),
        "infrastructure": SearchWeights(semantic=0.35, keyword=0.25, metadata=0.15, graph=0.25),
        "network": SearchWeights(semantic=0.35, keyword=0.30, metadata=0.10, graph=0.25),
        "database": SearchWeights(semantic=0.40, keyword=0.25, metadata=0.10, graph=0.25),
        "application": SearchWeights(semantic=0.45, keyword=0.20, metadata=0.10, graph=0.25),
        "default": SearchWeights(semantic=0.40, keyword=0.25, metadata=0.10, graph=0.25)
    }

    def __init__(
        self,
        weights: Optional[SearchWeights] = None,
        rrf_config: Optional[RRFConfig] = None,
        embedding_model: str = "local",
        enable_graph: bool = True,
        enable_reranking: bool = True
    ):
        """
        Initialize the Hybrid Search Engine v5.0.

        Args:
            weights: Legacy weights (deprecated, use RRF instead)
            rrf_config: RRF configuration (k constant, thresholds)
            embedding_model: "local" (SentenceTransformer) or "openai"
            enable_graph: Enable Neo4j graph scoring
            enable_reranking: Enable cross-encoder reranking
        """
        self.weights = weights or SearchWeights()
        self.rrf_config = rrf_config or RRFConfig()
        self.embedding_model = embedding_model
        self.enable_graph = enable_graph
        self.enable_reranking = enable_reranking

        # TF-IDF vectorizer for keyword search
        self.tfidf_vectorizer = TfidfVectorizer(
            max_features=10000,
            stop_words='english',
            ngram_range=(1, 2),
            lowercase=True
        )

        # Storage for indexed documents
        self.documents: List[Dict[str, Any]] = []
        self.semantic_embeddings: Optional[np.ndarray] = None
        self.tfidf_matrix = None
        self.is_fitted = False

        # Lazy-loaded components
        self._local_model = None
        self._graph_scorer = None
        self._cross_encoder = None

        logger.info(
            "hybrid_search_engine_v5_initialized",
            rrf_config=self.rrf_config.to_dict(),
            embedding_model=embedding_model,
            graph_enabled=enable_graph,
            reranking_enabled=enable_reranking
        )

    # =========================================================================
    # LAZY LOADERS
    # =========================================================================

    @property
    def local_model(self):
        """Lazy load local embedding model"""
        if self._local_model is None:
            try:
                from sentence_transformers import SentenceTransformer
                self._local_model = SentenceTransformer('all-MiniLM-L6-v2')
                logger.info("local_embedding_model_loaded", model="all-MiniLM-L6-v2")
            except ImportError:
                logger.warning("sentence_transformers_not_installed")
        return self._local_model

    @property
    def graph_scorer(self):
        """Lazy load graph scorer"""
        if self._graph_scorer is None and self.enable_graph:
            try:
                from .graph_scorer import GraphScorer
                self._graph_scorer = GraphScorer()
                logger.info("graph_scorer_loaded")
            except Exception as e:
                logger.warning("graph_scorer_load_failed", error=str(e))
        return self._graph_scorer

    @property
    def cross_encoder(self):
        """Lazy load cross-encoder for reranking"""
        if self._cross_encoder is None and self.enable_reranking:
            try:
                from .cross_encoder_reranker import CrossEncoderReranker
                self._cross_encoder = CrossEncoderReranker()
                logger.info("cross_encoder_loaded")
            except Exception as e:
                logger.warning("cross_encoder_load_failed", error=str(e))
        return self._cross_encoder

    # =========================================================================
    # INDEXING
    # =========================================================================

    def index_documents(self, documents: List[Dict[str, Any]]) -> int:
        """
        Index documents for hybrid search.

        Args:
            documents: List of documents with 'content', 'metadata', and optional 'embedding'

        Returns:
            Number of documents indexed
        """
        self.documents = documents

        # Extract text content for TF-IDF
        texts = [self._prepare_text_for_search(doc) for doc in documents]

        # Fit TF-IDF vectorizer
        if texts:
            self.tfidf_matrix = self.tfidf_vectorizer.fit_transform(texts)

        # Generate semantic embeddings
        self.semantic_embeddings = self._generate_embeddings(texts)

        self.is_fitted = True

        logger.info(
            "documents_indexed_v4",
            count=len(documents),
            tfidf_features=self.tfidf_matrix.shape[1] if self.tfidf_matrix is not None else 0
        )

        return len(documents)

    def _prepare_text_for_search(self, doc: Dict[str, Any]) -> str:
        """Prepare document text for search by combining content and metadata"""
        parts = [doc.get("content", "")]

        metadata = doc.get("metadata", {})
        for field_name in ["name", "description", "keywords", "tags", "service", "action", "error_patterns"]:
            if field_name in metadata:
                value = metadata[field_name]
                if isinstance(value, list):
                    parts.extend(str(v) for v in value)
                else:
                    parts.append(str(value))

        return " ".join(parts)

    def _generate_embeddings(self, texts: List[str]) -> np.ndarray:
        """Generate embeddings using configured model"""
        if not texts:
            return np.array([])

        if self.embedding_model == "local" and self.local_model:
            embeddings = self.local_model.encode(texts, show_progress_bar=False)
            return np.array(embeddings)
        else:
            logger.warning("using_mock_embeddings")
            return np.random.rand(len(texts), 384)

    # =========================================================================
    # SEARCH
    # =========================================================================

    def search(
        self,
        query: str,
        query_metadata: Optional[Dict[str, Any]] = None,
        top_k: int = 10,
        weights: Optional[SearchWeights] = None,
        service_context: Optional[str] = None
    ) -> List[SearchResult]:
        """
        Perform hybrid search combining semantic, keyword, metadata, and graph scores.

        Args:
            query: Search query text
            query_metadata: Optional metadata for filtering
            top_k: Number of results to return
            weights: Optional custom weights for this search
            service_context: Service for graph scoring context

        Returns:
            List of SearchResult objects with score breakdowns
        """
        if not self.is_fitted:
            logger.warning("search_on_unfitted_engine")
            return []

        weights = weights or self.weights
        query_metadata = query_metadata or {}

        # Calculate individual scores
        semantic_scores = self._calculate_semantic_scores(query)
        keyword_scores = self._calculate_keyword_scores(query)
        metadata_scores = self._calculate_metadata_scores(query_metadata)
        graph_scores = self._calculate_graph_scores(service_context)

        # Combine scores with weights
        final_scores = (
            weights.semantic * semantic_scores +
            weights.keyword * keyword_scores +
            weights.metadata * metadata_scores +
            weights.graph * graph_scores
        )

        # Get top-k indices
        top_indices = np.argsort(final_scores)[-top_k:][::-1]

        # Build results
        results = []
        for rank, idx in enumerate(top_indices):
            if final_scores[idx] > 0:
                doc = self.documents[idx]

                # Generate match reasons
                match_reasons = self._generate_match_reasons(
                    semantic_scores[idx],
                    keyword_scores[idx],
                    metadata_scores[idx],
                    graph_scores[idx]
                )

                result = SearchResult(
                    chunk_id=doc.get("id", f"doc_{idx}"),
                    content=doc.get("content", ""),
                    metadata=doc.get("metadata", {}),
                    final_score=round(float(final_scores[idx]), 3),
                    score_breakdown={
                        "semantic": round(float(semantic_scores[idx]), 3),
                        "keyword": round(float(keyword_scores[idx]), 3),
                        "metadata": round(float(metadata_scores[idx]), 3),
                        "graph": round(float(graph_scores[idx]), 3)
                    },
                    match_reasons=match_reasons,
                    rank=rank + 1
                )
                results.append(result)

        logger.info(
            "hybrid_search_v4_complete",
            query_length=len(query),
            results_count=len(results),
            top_score=results[0].final_score if results else 0,
            weights=weights.to_dict()
        )

        return results

    def _calculate_semantic_scores(self, query: str) -> np.ndarray:
        """Calculate semantic similarity scores using embeddings"""
        if self.semantic_embeddings is None or len(self.semantic_embeddings) == 0:
            return np.zeros(len(self.documents))

        query_embedding = self._generate_embeddings([query])[0]
        similarities = cosine_similarity([query_embedding], self.semantic_embeddings)[0]

        # Normalize to 0-1 range
        return (similarities + 1) / 2

    def _calculate_keyword_scores(self, query: str) -> np.ndarray:
        """Calculate keyword match scores using TF-IDF"""
        if self.tfidf_matrix is None:
            return np.zeros(len(self.documents))

        query_vector = self.tfidf_vectorizer.transform([query])
        similarities = cosine_similarity(query_vector, self.tfidf_matrix)[0]

        return similarities

    def _calculate_metadata_scores(self, query_metadata: Dict[str, Any]) -> np.ndarray:
        """Calculate metadata exact match scores"""
        if not query_metadata:
            return np.full(len(self.documents), 0.1)  # Baseline

        scores = np.zeros(len(self.documents))
        match_fields = ["cloud_provider", "environment", "service", "resource_type", "action", "severity"]

        for idx, doc in enumerate(self.documents):
            doc_metadata = doc.get("metadata", {})
            matches = 0
            total_fields = 0

            for field_name in match_fields:
                if field_name in query_metadata:
                    total_fields += 1
                    query_val = str(query_metadata[field_name]).lower()
                    doc_val = str(doc_metadata.get(field_name, "")).lower()

                    if query_val == doc_val:
                        matches += 1
                    elif query_val in doc_val or doc_val in query_val:
                        matches += 0.5

            if total_fields > 0:
                scores[idx] = matches / total_fields

        return scores

    def _calculate_graph_scores(self, service_context: Optional[str] = None) -> np.ndarray:
        """
        Calculate graph-based scores using Neo4j FIXED_BY relationships.

        This queries historical data to see which scripts have successfully
        fixed similar incidents in the past.
        """
        if not self.enable_graph or not self.graph_scorer:
            # Return baseline scores if graph not available
            return np.full(len(self.documents), 0.1)

        scores = []
        for doc in self.documents:
            script_id = doc.get("id") or doc.get("metadata", {}).get("id", "")
            if script_id:
                try:
                    result = self.graph_scorer.get_score(
                        script_id=script_id,
                        service=service_context
                    )
                    scores.append(result.graph_score)
                except Exception as e:
                    logger.debug("graph_score_failed", script_id=script_id, error=str(e))
                    scores.append(0.1)
            else:
                scores.append(0.1)

        return np.array(scores)

    def _generate_match_reasons(
        self,
        semantic: float,
        keyword: float,
        metadata: float,
        graph: float
    ) -> List[str]:
        """Generate human-readable match reasons"""
        reasons = []

        if semantic > 0.5:
            reasons.append(f"High semantic similarity ({semantic:.0%})")
        elif semantic > 0.3:
            reasons.append(f"Moderate semantic match ({semantic:.0%})")

        if keyword > 0.4:
            reasons.append(f"Strong keyword overlap ({keyword:.0%})")
        elif keyword > 0.2:
            reasons.append(f"Keyword match ({keyword:.0%})")

        if metadata > 0.5:
            reasons.append(f"Metadata alignment ({metadata:.0%})")

        if graph > 0.3:
            reasons.append(f"Historical success (FIXED_BY: {graph:.0%})")
        elif graph > 0.15:
            reasons.append(f"Some historical data ({graph:.0%})")

        if not reasons:
            reasons.append("General match")

        return reasons

    # =========================================================================
    # RRF FUSION SEARCH (v5.0 - RECOMMENDED)
    # =========================================================================

    def search_rrf(
        self,
        query: str,
        query_metadata: Optional[Dict[str, Any]] = None,
        top_k: int = 5,
        service_context: Optional[str] = None,
        enable_rerank: bool = True
    ) -> List[SearchResult]:
        """
        Perform hybrid search using Reciprocal Rank Fusion (RRF).

        This is the recommended search method for v5.0+.
        RRF provides fair, weight-free fusion of multiple search agents.

        Pipeline:
            1. Each agent (vector, keyword, graph, metadata) produces ranked results
            2. RRF combines rankings: Score = Σ (1 / (k + rank_i))
            3. Cross-encoder reranks top candidates (optional)
            4. Final results returned

        Args:
            query: Search query text
            query_metadata: Optional metadata for filtering
            top_k: Number of final results to return
            service_context: Service for graph scoring context
            enable_rerank: Apply cross-encoder reranking (default: True)

        Returns:
            List of SearchResult objects with RRF scores
        """
        import time
        start_time = time.time()

        if not self.is_fitted:
            logger.warning("search_on_unfitted_engine")
            return []

        query_metadata = query_metadata or {}

        # Step 1: Get ranked results from each agent
        agent_results = self._get_agent_ranked_results(query, query_metadata, service_context)

        # Step 2: Apply RRF fusion
        rrf_results = self._apply_rrf_fusion(agent_results)

        # Step 3: Get top candidates for reranking
        top_candidates = rrf_results[:self.rrf_config.top_candidates_for_rerank]

        # Step 4: Cross-encoder reranking (optional)
        if enable_rerank and self.enable_reranking and self.cross_encoder and top_candidates:
            final_results = self._rerank_with_cross_encoder(query, top_candidates)
        else:
            final_results = top_candidates

        # Step 5: Limit to top_k and assign final ranks
        final_results = final_results[:top_k]
        for i, result in enumerate(final_results):
            result.rank = i + 1

        elapsed_ms = (time.time() - start_time) * 1000

        logger.info(
            "rrf_search_complete",
            query_length=len(query),
            agents_participated=len(agent_results),
            candidates_after_rrf=len(rrf_results),
            final_results=len(final_results),
            reranked=enable_rerank and self.enable_reranking,
            elapsed_ms=f"{elapsed_ms:.1f}",
            top_score=final_results[0].final_score if final_results else 0
        )

        return final_results

    def _get_agent_ranked_results(
        self,
        query: str,
        query_metadata: Dict[str, Any],
        service_context: Optional[str]
    ) -> Dict[str, List[Tuple[int, float, int]]]:
        """
        Get ranked results from each search agent.

        Returns:
            Dict mapping agent_type -> List[(doc_idx, score, rank)]
        """
        agent_results = {}

        # Vector Agent (Semantic Search)
        semantic_scores = self._calculate_semantic_scores(query)
        if len(semantic_scores) > 0:
            ranked_indices = np.argsort(semantic_scores)[::-1]
            agent_results["vector"] = [
                (int(idx), float(semantic_scores[idx]), rank + 1)
                for rank, idx in enumerate(ranked_indices)
                if semantic_scores[idx] > 0.1  # Threshold
            ]

        # Keyword Agent (TF-IDF/BM25)
        keyword_scores = self._calculate_keyword_scores(query)
        if len(keyword_scores) > 0:
            ranked_indices = np.argsort(keyword_scores)[::-1]
            agent_results["keyword"] = [
                (int(idx), float(keyword_scores[idx]), rank + 1)
                for rank, idx in enumerate(ranked_indices)
                if keyword_scores[idx] > 0.05  # Lower threshold for keywords
            ]

        # Graph Agent (Neo4j FIXED_BY)
        graph_scores = self._calculate_graph_scores(service_context)
        if len(graph_scores) > 0:
            ranked_indices = np.argsort(graph_scores)[::-1]
            agent_results["graph"] = [
                (int(idx), float(graph_scores[idx]), rank + 1)
                for rank, idx in enumerate(ranked_indices)
                if graph_scores[idx] > 0.1  # Threshold
            ]

        # Metadata Agent (Exact Match)
        metadata_scores = self._calculate_metadata_scores(query_metadata)
        if len(metadata_scores) > 0:
            ranked_indices = np.argsort(metadata_scores)[::-1]
            agent_results["metadata"] = [
                (int(idx), float(metadata_scores[idx]), rank + 1)
                for rank, idx in enumerate(ranked_indices)
                if metadata_scores[idx] > 0.1  # Threshold
            ]

        logger.debug(
            "agent_results_collected",
            vector_count=len(agent_results.get("vector", [])),
            keyword_count=len(agent_results.get("keyword", [])),
            graph_count=len(agent_results.get("graph", [])),
            metadata_count=len(agent_results.get("metadata", []))
        )

        return agent_results

    def _apply_rrf_fusion(
        self,
        agent_results: Dict[str, List[Tuple[int, float, int]]]
    ) -> List[SearchResult]:
        """
        Apply Reciprocal Rank Fusion to combine agent rankings.

        RRF Formula: Score = Σ (1 / (k + rank_i)) for each agent i

        Args:
            agent_results: Dict mapping agent_type -> List[(doc_idx, score, rank)]

        Returns:
            List of SearchResult objects sorted by RRF score
        """
        k = self.rrf_config.k

        # Collect all document data
        doc_data: Dict[int, Dict[str, Any]] = {}

        for agent_type, results in agent_results.items():
            for doc_idx, raw_score, rank in results:
                if doc_idx not in doc_data:
                    doc_data[doc_idx] = {
                        "rrf_score": 0.0,
                        "agent_ranks": {},
                        "agent_scores": {},
                        "sources": []
                    }

                # RRF contribution: 1 / (k + rank)
                rrf_contribution = 1.0 / (k + rank)
                doc_data[doc_idx]["rrf_score"] += rrf_contribution
                doc_data[doc_idx]["agent_ranks"][agent_type] = rank
                doc_data[doc_idx]["agent_scores"][agent_type] = raw_score
                doc_data[doc_idx]["sources"].append(agent_type)

        # Build SearchResult objects
        results = []
        for doc_idx, data in doc_data.items():
            doc = self.documents[doc_idx]

            # Generate match reasons from agent contributions
            match_reasons = self._generate_rrf_match_reasons(
                data["agent_ranks"],
                data["agent_scores"]
            )

            result = SearchResult(
                chunk_id=doc.get("id", f"doc_{doc_idx}"),
                content=doc.get("content", ""),
                metadata=doc.get("metadata", {}),
                final_score=data["rrf_score"],
                rrf_score=data["rrf_score"],
                rerank_score=0.0,
                agent_ranks=data["agent_ranks"],
                agent_scores=data["agent_scores"],
                match_reasons=match_reasons,
                sources=list(set(data["sources"])),
                rank=0  # Will be set after sorting
            )
            results.append(result)

        # Sort by RRF score (descending)
        results.sort(key=lambda x: x.rrf_score, reverse=True)

        logger.debug(
            "rrf_fusion_complete",
            total_documents=len(results),
            top_rrf_score=results[0].rrf_score if results else 0
        )

        return results

    def _rerank_with_cross_encoder(
        self,
        query: str,
        candidates: List[SearchResult]
    ) -> List[SearchResult]:
        """
        Rerank candidates using cross-encoder model.

        Cross-encoder jointly encodes query and document for more accurate
        relevance scoring than bi-encoder approaches.

        Args:
            query: Search query
            candidates: List of candidates to rerank

        Returns:
            Reranked list of SearchResult objects
        """
        if not self.cross_encoder or not candidates:
            return candidates

        try:
            # Prepare candidates for reranking
            candidate_dicts = [
                {
                    "chunk_id": c.chunk_id,
                    "content": c.content,
                    "metadata": c.metadata,
                    "final_score": c.rrf_score  # Use RRF score as base
                }
                for c in candidates
            ]

            # Rerank using cross-encoder
            reranked = self.cross_encoder.rerank(
                query=query,
                candidates=candidate_dicts,
                top_k=len(candidates),
                content_field="content",
                score_field="final_score"
            )

            # Map back to SearchResult objects
            chunk_id_to_original = {c.chunk_id: c for c in candidates}
            final_results = []

            for r in reranked:
                original = chunk_id_to_original.get(r.chunk_id)
                if original:
                    # Update with rerank score
                    original.rerank_score = r.rerank_score
                    original.final_score = r.final_score  # Combined score
                    original.match_reasons.append(
                        f"Cross-encoder relevance: {r.rerank_score:.0%}"
                    )
                    final_results.append(original)

            logger.debug(
                "cross_encoder_rerank_complete",
                candidates=len(candidates),
                reranked=len(final_results)
            )

            return final_results

        except Exception as e:
            logger.warning("cross_encoder_rerank_failed", error=str(e))
            return candidates

    def _generate_rrf_match_reasons(
        self,
        agent_ranks: Dict[str, int],
        agent_scores: Dict[str, float]
    ) -> List[str]:
        """Generate human-readable match reasons from RRF data"""
        reasons = []

        agent_names = {
            "vector": "Semantic similarity",
            "keyword": "Keyword match",
            "graph": "Historical success (FIXED_BY)",
            "metadata": "Metadata alignment"
        }

        for agent, rank in sorted(agent_ranks.items(), key=lambda x: x[1]):
            score = agent_scores.get(agent, 0)
            name = agent_names.get(agent, agent)

            if rank <= 3:
                reasons.append(f"Top-{rank} in {name} ({score:.0%})")
            elif rank <= 10:
                reasons.append(f"Rank {rank} in {name}")

        if not reasons:
            reasons.append("General match across agents")

        return reasons

    def explain_rrf_result(self, query: str, result: SearchResult) -> str:
        """Generate detailed explanation of RRF search result"""
        k = self.rrf_config.k

        rrf_breakdown = []
        for agent, rank in result.agent_ranks.items():
            contribution = 1.0 / (k + rank)
            score = result.agent_scores.get(agent, 0)
            rrf_breakdown.append(f"| {agent.capitalize()} | {rank} | {score:.3f} | {contribution:.4f} |")

        explanation = f"""
## RRF Search Result Analysis (v5.0)

**Query:** {query[:100]}...
**Matched Document:** {result.chunk_id}

### RRF Score Breakdown

Formula: `RRF_Score = Σ (1 / (k + rank))` where k = {k}

| Agent | Rank | Raw Score | RRF Contribution |
|-------|------|-----------|------------------|
{chr(10).join(rrf_breakdown)}

**Total RRF Score:** {result.rrf_score:.4f}
**Cross-Encoder Score:** {result.rerank_score:.4f}
**Final Score:** {result.final_score:.4f}

### Sources (Agents that found this document):
{', '.join(result.sources)}

### Match Reasons:
{chr(10).join('- ' + reason for reason in result.match_reasons)}
"""
        return explanation.strip()

    # =========================================================================
    # WEIGHT ADJUSTMENT (LEGACY - Use RRF instead)
    # =========================================================================

    def adjust_weights(
        self,
        incident_type: str,
        severity: str = "medium"
    ) -> SearchWeights:
        """
        Dynamically adjust weights based on incident characteristics.

        Args:
            incident_type: Type of incident (security, performance, etc.)
            severity: Incident severity level

        Returns:
            Adjusted SearchWeights
        """
        base_weights = self.WEIGHT_PRESETS.get(
            incident_type.lower(),
            self.WEIGHT_PRESETS["default"]
        )

        # Create a copy for adjustment
        weights = SearchWeights(
            semantic=base_weights.semantic,
            keyword=base_weights.keyword,
            metadata=base_weights.metadata,
            graph=base_weights.graph
        )

        # Boost metadata weight for high severity (more precise matching needed)
        if severity.lower() in ["high", "critical"]:
            weights.metadata += 0.05
            weights.semantic -= 0.05

        # Ensure normalization
        weights.__post_init__()

        logger.info(
            "weights_adjusted_v4",
            incident_type=incident_type,
            severity=severity,
            weights=weights.to_dict()
        )

        return weights

    def get_weights_for_context(
        self,
        query: str,
        context: Dict[str, Any]
    ) -> SearchWeights:
        """
        Intelligently select weights based on query and context.

        Uses query understanding to determine optimal weights.
        """
        query_lower = query.lower()

        # Detect incident type from query
        incident_type = "default"
        type_indicators = {
            "security": ["security", "vulnerability", "breach", "attack", "unauthorized"],
            "performance": ["slow", "latency", "timeout", "performance", "cpu", "memory"],
            "infrastructure": ["instance", "vm", "server", "down", "terminated"],
            "network": ["network", "dns", "connectivity", "routing", "firewall"],
            "database": ["database", "db", "query", "connection", "deadlock"],
            "application": ["error", "exception", "crash", "bug", "deployment"]
        }

        for type_name, indicators in type_indicators.items():
            if any(ind in query_lower for ind in indicators):
                incident_type = type_name
                break

        severity = context.get("severity", "medium")

        return self.adjust_weights(incident_type, severity)

    # =========================================================================
    # UTILITIES
    # =========================================================================

    def explain_search(self, query: str, result: SearchResult) -> str:
        """Generate explanation of search result"""
        weights = self.weights

        explanation = f"""
## Search Result Analysis

**Query:** {query[:100]}...
**Matched Document:** {result.chunk_id}
**Final Score:** {result.final_score:.1%}

### Score Breakdown (v4.0 Formula):

| Component | Weight | Raw Score | Contribution |
|-----------|--------|-----------|--------------|
| Semantic | {weights.semantic:.0%} | {result.score_breakdown['semantic']:.1%} | {weights.semantic * result.score_breakdown['semantic']:.1%} |
| Keyword | {weights.keyword:.0%} | {result.score_breakdown['keyword']:.1%} | {weights.keyword * result.score_breakdown['keyword']:.1%} |
| Metadata | {weights.metadata:.0%} | {result.score_breakdown['metadata']:.1%} | {weights.metadata * result.score_breakdown['metadata']:.1%} |
| Graph (FIXED_BY) | {weights.graph:.0%} | {result.score_breakdown['graph']:.1%} | {weights.graph * result.score_breakdown['graph']:.1%} |

### Match Reasons:
{chr(10).join('- ' + reason for reason in result.match_reasons)}
"""
        return explanation.strip()

    def get_stats(self) -> Dict[str, Any]:
        """Get engine statistics"""
        return {
            "version": "5.0.0",
            "fusion_method": "RRF (Reciprocal Rank Fusion)",
            "documents_indexed": len(self.documents),
            "is_fitted": self.is_fitted,
            "embedding_model": self.embedding_model,
            "graph_enabled": self.enable_graph and self.graph_scorer is not None,
            "reranking_enabled": self.enable_reranking,
            "rrf_config": self.rrf_config.to_dict(),
            "legacy_weights": self.weights.to_dict(),
            "tfidf_features": self.tfidf_matrix.shape[1] if self.tfidf_matrix is not None else 0
        }


# =============================================================================
# RRF UTILITY FUNCTION
# =============================================================================

def calculate_rrf_score(
    rankings: Dict[str, int],
    k: int = 60
) -> float:
    """
    Calculate RRF score from multiple agent rankings.

    This is a standalone utility function for RRF calculation.

    Args:
        rankings: Dict mapping agent_type to rank (1-indexed)
        k: RRF constant (default: 60, industry standard)

    Returns:
        RRF score (sum of 1/(k+rank) for each agent)

    Example:
        >>> rankings = {"vector": 1, "keyword": 3, "graph": 2, "metadata": 5}
        >>> score = calculate_rrf_score(rankings)
        >>> print(f"RRF Score: {score:.4f}")
        RRF Score: 0.0639
    """
    return sum(1.0 / (k + rank) for rank in rankings.values())


# =============================================================================
# GLOBAL INSTANCES
# =============================================================================

# v5.0 default engine with RRF fusion and cross-encoder reranking
hybrid_search_engine = HybridSearchEngine(
    enable_graph=True,
    enable_reranking=True
)

# Legacy engine for backward compatibility (no RRF, no reranking)
hybrid_search_engine_legacy = HybridSearchEngine(
    weights=SearchWeights.legacy(),
    enable_graph=False,
    enable_reranking=False
)
