"""
RAG (Retrieval Augmented Generation) Module v5.0
================================================

Enterprise-Grade Intelligent RAG System with RRF Fusion.

Architecture (Industry Standard - Google/Bing/OpenAI Pattern):
    Query → Query Understanding → 4 Agents → RRF Fusion → Cross-Encoder Rerank → Filter

Scoring Formula v5.0 (Reciprocal Rank Fusion):
    RRF_Score = Σ (1 / (k + rank_i)) for each agent i
    k = 60 (industry standard constant)

WHY RRF over Weighted Scores:
    - No manual weight tuning required
    - Scale-invariant (works regardless of raw score magnitudes)
    - Fair fusion (each agent contributes based on relative ordering)
    - Industry standard (used by Elasticsearch, Pinecone, Cohere, OpenAI)

Components:
    - HybridSearchEngine: RRF-based multi-source search (v5.0)
    - SwarmRetriever: Distributed RAG with RRF consensus
    - CrossEncoderReranker: Precision reranking (ms-marco-MiniLM)
    - QueryUnderstanding: Intent/entity extraction
    - GraphScorer: Neo4j FIXED_BY scoring
    - IntelligentRetriever: Full pipeline orchestrator
    - SmartChunker: Document chunking
    - EmbeddingService: Vector embeddings
    - FeedbackOptimizer: Adaptive learning

Version: 5.0.0
Author: AI Agent Platform
"""

# =============================================================================
# CORE CLIENTS
# =============================================================================

from .weaviate_client import weaviate_client
from .neo4j_client import neo4j_client
# hybrid_rag is now an alias to hybrid_search_engine for backward compatibility

# =============================================================================
# HYBRID SEARCH ENGINE v5.0 (RRF Fusion + Cross-Encoder)
# =============================================================================

from .hybrid_search_engine import (
    hybrid_search_engine,
    hybrid_search_engine_legacy,
    HybridSearchEngine,
    SearchWeights,  # Deprecated - kept for backward compatibility
    RRFConfig,
    AgentRankedResult,
    SearchResult,
    calculate_rrf_score
)

# Backward compatibility alias
hybrid_rag = hybrid_search_engine

# =============================================================================
# INTELLIGENT RETRIEVER (Full Pipeline)
# =============================================================================

from .intelligent_retriever import (
    intelligent_retriever,
    IntelligentRetriever,
    RetrievalWeights,
    RetrievalResult,
    QueryContext as RetrieverQueryContext,
    IncidentIntent as RetrieverIncidentIntent
)

# =============================================================================
# QUERY UNDERSTANDING
# =============================================================================

from .query_understanding import (
    query_understanding,
    QueryUnderstanding,
    QueryContext,
    IncidentIntent,
    ServiceType,
    Severity,
    ExtractedEntity
)

# =============================================================================
# GRAPH SCORER (Neo4j Integration)
# =============================================================================

from .graph_scorer import (
    graph_scorer,
    GraphScorer,
    GraphScoreResult,
    ServiceDependency,
    HistoricalRemediation
)

# =============================================================================
# CROSS-ENCODER RERANKING
# =============================================================================

from .cross_encoder_reranker import (
    cross_encoder_reranker,
    reranker_with_fallback,
    CrossEncoderReranker,
    RerankedResult,
    RerankerWithFallback
)

# =============================================================================
# SMART CHUNKING
# =============================================================================

from .smart_chunker import (
    smart_chunker,
    SmartChunker,
    Chunk
)

# =============================================================================
# EMBEDDING SERVICE
# =============================================================================

from .embedding_service import (
    embedding_service,
    EmbeddingService
)

# =============================================================================
# FEEDBACK OPTIMIZER (Adaptive Learning)
# =============================================================================

from .feedback_optimizer import (
    feedback_optimizer,
    FeedbackOptimizer,
    FeedbackRecord,
    OptimizedWeights
)

# =============================================================================
# VERSION INFO
# =============================================================================

__version__ = "5.0.0"
__author__ = "AI Agent Platform"

# =============================================================================
# ALL EXPORTS
# =============================================================================

__all__ = [
    # Version
    "__version__",
    "__author__",

    # Core clients
    "weaviate_client",
    "neo4j_client",
    "hybrid_rag",

    # Hybrid Search v5.0 (RRF Fusion)
    "hybrid_search_engine",
    "hybrid_search_engine_legacy",
    "HybridSearchEngine",
    "SearchWeights",  # Deprecated
    "RRFConfig",
    "AgentRankedResult",
    "SearchResult",
    "calculate_rrf_score",

    # Intelligent Retriever
    "intelligent_retriever",
    "IntelligentRetriever",
    "RetrievalWeights",
    "RetrievalResult",
    "RetrieverQueryContext",
    "RetrieverIncidentIntent",

    # Query Understanding
    "query_understanding",
    "QueryUnderstanding",
    "QueryContext",
    "IncidentIntent",
    "ServiceType",
    "Severity",
    "ExtractedEntity",

    # Graph Scorer
    "graph_scorer",
    "GraphScorer",
    "GraphScoreResult",
    "ServiceDependency",
    "HistoricalRemediation",

    # Re-ranking
    "cross_encoder_reranker",
    "reranker_with_fallback",
    "CrossEncoderReranker",
    "RerankedResult",
    "RerankerWithFallback",

    # Chunking
    "smart_chunker",
    "SmartChunker",
    "Chunk",

    # Embeddings
    "embedding_service",
    "EmbeddingService",

    # Feedback
    "feedback_optimizer",
    "FeedbackOptimizer",
    "FeedbackRecord",
    "OptimizedWeights",
]
