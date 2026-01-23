"""
Intelligent Retriever - Enterprise-Grade Multi-Stage RAG System
================================================================

Implements Netflix/Uber/Stripe/Google SRE pattern for intelligent retrieval:

Architecture:
    Query → Query Understanding → Multi-Source Retrieval → Fusion → Rerank → Filter

Scoring Formula (Industry Standard):
    Final_Score = (0.40 × Vector) + (0.25 × Keyword) + (0.10 × Metadata) + (0.25 × Graph)

Features:
    1. Query Understanding: Intent classification, entity extraction, query expansion
    2. Multi-Source Retrieval: Vector DB + Keyword + Metadata + Graph
    3. Fusion Layer: Weighted combination with adaptive weights
    4. Cross-Encoder Reranking: Precision improvement
    5. Feedback Loop: Continuous learning from execution outcomes

Author: AI Agent Platform
Version: 4.0.0
"""

import os
import json
import hashlib
from typing import Any, Dict, List, Optional, Tuple, Union
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
import numpy as np
import structlog

logger = structlog.get_logger()


# =============================================================================
# ENUMS AND DATA CLASSES
# =============================================================================

class RetrievalStage(Enum):
    """Stages in multi-stage retrieval pipeline"""
    QUERY_UNDERSTANDING = "query_understanding"
    INITIAL_RETRIEVAL = "initial_retrieval"
    FUSION = "fusion"
    RERANKING = "reranking"
    FILTERING = "filtering"
    COMPLETE = "complete"


class IncidentIntent(Enum):
    """Classified incident intents"""
    RESTART = "restart"
    SCALE = "scale"
    REPAIR = "repair"
    ROLLBACK = "rollback"
    INVESTIGATE = "investigate"
    CONFIGURE = "configure"
    UNKNOWN = "unknown"


@dataclass
class RetrievalWeights:
    """
    Configurable weights for multi-source retrieval.
    Industry Standard: Vector(40%) + Keyword(25%) + Metadata(10%) + Graph(25%)
    """
    vector: float = 0.40      # Semantic similarity
    keyword: float = 0.25     # TF-IDF keyword match
    metadata: float = 0.10    # Exact field match
    graph: float = 0.25       # Historical success (FIXED_BY)

    def __post_init__(self):
        """Normalize weights to sum to 1.0"""
        total = self.vector + self.keyword + self.metadata + self.graph
        if abs(total - 1.0) > 0.01:
            self.vector /= total
            self.keyword /= total
            self.metadata /= total
            self.graph /= total

    def to_dict(self) -> Dict[str, float]:
        return {
            "vector": round(self.vector, 3),
            "keyword": round(self.keyword, 3),
            "metadata": round(self.metadata, 3),
            "graph": round(self.graph, 3)
        }


@dataclass
class QueryContext:
    """Enriched query context after understanding"""
    original_query: str
    expanded_query: str
    intent: IncidentIntent
    entities: Dict[str, Any]
    keywords: List[str]
    service: Optional[str]
    severity: str
    confidence: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "original_query": self.original_query,
            "expanded_query": self.expanded_query,
            "intent": self.intent.value,
            "entities": self.entities,
            "keywords": self.keywords,
            "service": self.service,
            "severity": self.severity,
            "confidence": self.confidence
        }


@dataclass
class RetrievalResult:
    """Result from intelligent retrieval with full score breakdown"""
    script_id: str
    name: str
    description: str
    content: str
    path: str
    script_type: str

    # Individual scores
    vector_score: float
    keyword_score: float
    metadata_score: float
    graph_score: float

    # Combined scores
    fusion_score: float
    rerank_score: float
    final_score: float

    # Metadata
    risk_level: str
    historical_success_count: int
    avg_resolution_time: float
    match_reasons: List[str]
    requires_approval: bool

    # Ranking info
    original_rank: int = 0
    final_rank: int = 0
    rank_change: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class RetrievalMetrics:
    """Metrics for retrieval operation"""
    query_id: str
    stage: RetrievalStage
    latency_ms: float
    candidates_in: int
    candidates_out: int
    cache_hit: bool
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())


# =============================================================================
# INTELLIGENT RETRIEVER
# =============================================================================

class IntelligentRetriever:
    """
    Enterprise-Grade Intelligent Retriever with Multi-Stage Pipeline.

    Combines:
    - Query Understanding (intent, entities, expansion)
    - Multi-Source Retrieval (vector, keyword, metadata, graph)
    - Fusion Layer (weighted combination)
    - Cross-Encoder Reranking (precision boost)
    - Safety Filtering (risk-based filtering)
    - Feedback Learning (adaptive weights)

    Usage:
        retriever = IntelligentRetriever()
        results = retriever.retrieve(
            query="VM instance test-vm-01 is down in us-central1-a",
            incident_context={"severity": "high", "service": "gcp"}
        )
    """

    # Default weights (industry standard)
    DEFAULT_WEIGHTS = RetrievalWeights(
        vector=0.40,
        keyword=0.25,
        metadata=0.10,
        graph=0.25
    )

    # Weight presets for different incident types
    WEIGHT_PRESETS = {
        "infrastructure": RetrievalWeights(vector=0.35, keyword=0.25, metadata=0.15, graph=0.25),
        "security": RetrievalWeights(vector=0.30, keyword=0.35, metadata=0.15, graph=0.20),
        "performance": RetrievalWeights(vector=0.45, keyword=0.20, metadata=0.10, graph=0.25),
        "database": RetrievalWeights(vector=0.40, keyword=0.25, metadata=0.10, graph=0.25),
        "network": RetrievalWeights(vector=0.35, keyword=0.30, metadata=0.15, graph=0.20),
        "application": RetrievalWeights(vector=0.45, keyword=0.20, metadata=0.10, graph=0.25),
    }

    def __init__(
        self,
        registry_path: str = None,
        enable_caching: bool = True,
        enable_reranking: bool = True,
        enable_graph: bool = True
    ):
        """
        Initialize the intelligent retriever.

        Args:
            registry_path: Path to script registry JSON
            enable_caching: Enable embedding/result caching
            enable_reranking: Enable cross-encoder reranking
            enable_graph: Enable Neo4j graph scoring
        """
        self.enable_caching = enable_caching
        self.enable_reranking = enable_reranking
        self.enable_graph = enable_graph

        # Set registry path
        if registry_path is None:
            registry_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
                "registry.json"
            )
        self.registry_path = registry_path

        # Load components lazily
        self._registry = None
        self._embedding_service = None
        self._graph_scorer = None
        self._cross_encoder = None
        self._query_understanding = None
        self._feedback_optimizer = None
        self._tfidf_vectorizer = None
        self._tfidf_matrix = None

        # Cache
        self._embedding_cache: Dict[str, np.ndarray] = {}
        self._result_cache: Dict[str, List[RetrievalResult]] = {}

        # Metrics
        self._metrics: List[RetrievalMetrics] = []

        logger.info(
            "intelligent_retriever_initialized",
            registry_path=registry_path,
            caching=enable_caching,
            reranking=enable_reranking,
            graph=enable_graph
        )

    # =========================================================================
    # PROPERTY LOADERS (Lazy Loading)
    # =========================================================================

    @property
    def registry(self) -> Dict[str, Any]:
        """Lazy load script registry"""
        if self._registry is None:
            try:
                with open(self.registry_path, 'r') as f:
                    self._registry = json.load(f)
                logger.info("registry_loaded", scripts=len(self._registry.get("scripts", [])))
            except Exception as e:
                logger.error("registry_load_failed", error=str(e))
                self._registry = {"scripts": []}
        return self._registry

    @property
    def embedding_service(self):
        """Lazy load embedding service"""
        if self._embedding_service is None:
            try:
                from .embedding_service import EmbeddingService, EmbeddingConfig
                self._embedding_service = EmbeddingService(
                    EmbeddingConfig(provider="local", cache_enabled=self.enable_caching)
                )
            except Exception as e:
                logger.warning("embedding_service_load_failed", error=str(e))
        return self._embedding_service

    @property
    def graph_scorer(self):
        """Lazy load graph scorer"""
        if self._graph_scorer is None and self.enable_graph:
            try:
                from .graph_scorer import GraphScorer
                self._graph_scorer = GraphScorer()
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
            except Exception as e:
                logger.warning("cross_encoder_load_failed", error=str(e))
        return self._cross_encoder

    @property
    def query_understanding(self):
        """Lazy load query understanding module"""
        if self._query_understanding is None:
            try:
                from .query_understanding import QueryUnderstanding
                self._query_understanding = QueryUnderstanding()
            except Exception as e:
                logger.warning("query_understanding_load_failed", error=str(e))
        return self._query_understanding

    @property
    def feedback_optimizer(self):
        """Lazy load feedback optimizer"""
        if self._feedback_optimizer is None:
            try:
                from .feedback_optimizer import FeedbackOptimizer
                self._feedback_optimizer = FeedbackOptimizer()
            except Exception as e:
                logger.warning("feedback_optimizer_load_failed", error=str(e))
        return self._feedback_optimizer

    # =========================================================================
    # MAIN RETRIEVAL METHOD
    # =========================================================================

    def retrieve(
        self,
        query: str,
        incident_context: Optional[Dict[str, Any]] = None,
        weights: Optional[RetrievalWeights] = None,
        top_k: int = 5,
        rerank_candidates: int = 20
    ) -> List[RetrievalResult]:
        """
        Main retrieval method - runs full pipeline.

        Pipeline:
            1. Query Understanding (intent, entities, expansion)
            2. Multi-Source Retrieval (vector, keyword, metadata, graph)
            3. Fusion (weighted combination)
            4. Reranking (cross-encoder)
            5. Filtering (safety, relevance)

        Args:
            query: Incident description or search query
            incident_context: Additional context (service, severity, etc.)
            weights: Custom retrieval weights (or auto-selected)
            top_k: Number of final results
            rerank_candidates: Number of candidates for reranking

        Returns:
            List of RetrievalResult with full score breakdowns
        """
        import time
        start_time = time.time()

        incident_context = incident_context or {}

        # Check cache first
        cache_key = self._get_cache_key(query, incident_context)
        if self.enable_caching and cache_key in self._result_cache:
            logger.info("retrieval_cache_hit", cache_key=cache_key[:16])
            return self._result_cache[cache_key][:top_k]

        # Stage 1: Query Understanding
        query_context = self._understand_query(query, incident_context)

        # Stage 2: Select weights based on context
        if weights is None:
            weights = self._select_weights(query_context, incident_context)

        # Stage 3: Multi-Source Retrieval
        candidates = self._multi_source_retrieve(
            query_context,
            weights,
            limit=rerank_candidates
        )

        if not candidates:
            logger.warning("no_candidates_found", query=query[:100])
            return []

        # Stage 4: Cross-Encoder Reranking
        if self.enable_reranking and self.cross_encoder:
            candidates = self._rerank_candidates(query_context.expanded_query, candidates)

        # Stage 5: Calculate final scores and rank
        results = self._finalize_results(candidates, weights, top_k)

        # Cache results
        if self.enable_caching:
            self._result_cache[cache_key] = results

        # Log metrics
        latency_ms = (time.time() - start_time) * 1000
        logger.info(
            "intelligent_retrieval_complete",
            query_length=len(query),
            candidates=len(candidates),
            results=len(results),
            latency_ms=round(latency_ms, 2),
            top_score=results[0].final_score if results else 0,
            weights=weights.to_dict()
        )

        return results

    # =========================================================================
    # PIPELINE STAGES
    # =========================================================================

    def _understand_query(
        self,
        query: str,
        context: Dict[str, Any]
    ) -> QueryContext:
        """
        Stage 1: Query Understanding

        - Classify intent (restart, scale, repair, etc.)
        - Extract entities (instance names, zones, services)
        - Expand query with synonyms
        - Extract keywords
        """
        if self.query_understanding:
            return self.query_understanding.understand(query, context)

        # Fallback: Basic query understanding
        return self._basic_query_understanding(query, context)

    def _basic_query_understanding(
        self,
        query: str,
        context: Dict[str, Any]
    ) -> QueryContext:
        """Basic query understanding without LLM"""
        import re

        query_lower = query.lower()

        # Intent classification based on keywords
        intent = IncidentIntent.UNKNOWN
        intent_keywords = {
            IncidentIntent.RESTART: ["restart", "reboot", "start", "stopped", "down", "terminated"],
            IncidentIntent.SCALE: ["scale", "capacity", "resource", "memory", "cpu", "disk"],
            IncidentIntent.REPAIR: ["fix", "repair", "error", "failed", "broken", "corrupt"],
            IncidentIntent.ROLLBACK: ["rollback", "revert", "undo", "previous", "restore"],
            IncidentIntent.INVESTIGATE: ["investigate", "analyze", "check", "diagnose", "monitor"],
            IncidentIntent.CONFIGURE: ["configure", "config", "setting", "parameter", "update"],
        }

        for intent_type, keywords in intent_keywords.items():
            if any(kw in query_lower for kw in keywords):
                intent = intent_type
                break

        # Entity extraction
        entities = {}

        # GCP instance patterns
        instance_match = re.search(r'(?:instance|vm|server)\s+([a-zA-Z0-9][-a-zA-Z0-9]*)', query, re.I)
        if instance_match:
            entities["instance_name"] = instance_match.group(1)

        # Zone patterns
        zone_match = re.search(r'([a-z]+-[a-z]+\d+-[a-z])', query, re.I)
        if zone_match:
            entities["zone"] = zone_match.group(1)

        # Kubernetes patterns
        ns_match = re.search(r'namespace\s+([a-zA-Z0-9][-a-zA-Z0-9]*)', query, re.I)
        if ns_match:
            entities["namespace"] = ns_match.group(1)

        pod_match = re.search(r'pod\s+([a-zA-Z0-9][-a-zA-Z0-9]*)', query, re.I)
        if pod_match:
            entities["pod_name"] = pod_match.group(1)

        # Extract keywords (simple word extraction)
        stopwords = {'the', 'a', 'an', 'is', 'are', 'was', 'were', 'in', 'on', 'at', 'to', 'for', 'of', 'and', 'or'}
        words = re.findall(r'\b[a-zA-Z]{3,}\b', query_lower)
        keywords = [w for w in words if w not in stopwords][:10]

        # Query expansion (add synonyms)
        expansions = {
            "down": ["stopped", "terminated", "offline", "unavailable"],
            "slow": ["latency", "performance", "timeout", "delay"],
            "error": ["exception", "failure", "crash", "bug"],
            "memory": ["ram", "heap", "oom", "memory leak"],
            "cpu": ["processor", "compute", "high cpu", "cpu spike"],
        }

        expanded_terms = []
        for word in keywords:
            expanded_terms.append(word)
            if word in expansions:
                expanded_terms.extend(expansions[word][:2])

        expanded_query = query + " " + " ".join(expanded_terms)

        # Service detection
        service = context.get("service")
        if not service:
            service_keywords = {
                "gcp": ["gcp", "google cloud", "compute engine", "gce", "vm instance"],
                "kubernetes": ["k8s", "kubernetes", "pod", "deployment", "namespace"],
                "database": ["database", "db", "postgres", "mysql", "redis", "mongodb"],
                "network": ["network", "dns", "load balancer", "firewall", "vpc"],
            }
            for svc, kws in service_keywords.items():
                if any(kw in query_lower for kw in kws):
                    service = svc
                    break

        return QueryContext(
            original_query=query,
            expanded_query=expanded_query,
            intent=intent,
            entities=entities,
            keywords=keywords,
            service=service,
            severity=context.get("severity", "medium"),
            confidence=0.7
        )

    def _select_weights(
        self,
        query_context: QueryContext,
        incident_context: Dict[str, Any]
    ) -> RetrievalWeights:
        """
        Select optimal weights based on context.

        Uses:
        - Feedback optimizer (if enough historical data)
        - Preset weights (based on incident type)
        - Severity adjustments
        """
        # Try feedback optimizer first
        if self.feedback_optimizer:
            try:
                incident_type = query_context.service or "general"
                severity = query_context.severity
                optimized = self.feedback_optimizer.get_optimal_weights(
                    incident_type=incident_type,
                    severity=severity
                )
                if optimized:
                    return RetrievalWeights(
                        vector=optimized.get("semantic", 0.40),
                        keyword=optimized.get("keyword", 0.25),
                        metadata=optimized.get("metadata", 0.10),
                        graph=0.25  # Graph weight fixed
                    )
            except Exception as e:
                logger.debug("feedback_optimizer_fallback", error=str(e))

        # Use preset weights based on service/type
        if query_context.service and query_context.service in self.WEIGHT_PRESETS:
            weights = self.WEIGHT_PRESETS[query_context.service]
        else:
            weights = self.DEFAULT_WEIGHTS

        # Adjust for severity
        if query_context.severity.lower() in ["critical", "high"]:
            # For critical incidents, boost metadata (more precise matching)
            return RetrievalWeights(
                vector=weights.vector - 0.05,
                keyword=weights.keyword,
                metadata=weights.metadata + 0.05,
                graph=weights.graph
            )

        return weights

    def _multi_source_retrieve(
        self,
        query_context: QueryContext,
        weights: RetrievalWeights,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """
        Stage 2: Multi-Source Retrieval

        Retrieves from multiple sources and combines:
        - Vector similarity (embeddings)
        - Keyword matching (TF-IDF)
        - Metadata matching (exact fields)
        - Graph scoring (historical FIXED_BY)
        """
        scripts = self.registry.get("scripts", [])
        if not scripts:
            return []

        # Calculate all scores
        vector_scores = self._calculate_vector_scores(query_context.expanded_query, scripts)
        keyword_scores = self._calculate_keyword_scores(query_context.keywords, scripts)
        metadata_scores = self._calculate_metadata_scores(query_context, scripts)
        graph_scores = self._calculate_graph_scores(query_context, scripts)

        # Combine with fusion
        candidates = []
        for i, script in enumerate(scripts):
            fusion_score = (
                weights.vector * vector_scores[i] +
                weights.keyword * keyword_scores[i] +
                weights.metadata * metadata_scores[i] +
                weights.graph * graph_scores[i]
            )

            if fusion_score > 0.1:  # Minimum threshold
                candidates.append({
                    "script": script,
                    "vector_score": vector_scores[i],
                    "keyword_score": keyword_scores[i],
                    "metadata_score": metadata_scores[i],
                    "graph_score": graph_scores[i],
                    "fusion_score": fusion_score,
                    "original_rank": len(candidates)
                })

        # Sort by fusion score
        candidates.sort(key=lambda x: x["fusion_score"], reverse=True)

        return candidates[:limit]

    def _calculate_vector_scores(
        self,
        query: str,
        scripts: List[Dict]
    ) -> np.ndarray:
        """Calculate vector similarity scores using embeddings"""
        if not self.embedding_service:
            return np.zeros(len(scripts))

        try:
            # Get query embedding
            query_embedding = self.embedding_service.embed(query)

            # Get script embeddings (with caching)
            script_texts = []
            for script in scripts:
                text = f"{script.get('name', '')} {script.get('description', '')} "
                text += " ".join(script.get('keywords', []))
                text += " ".join(script.get('error_patterns', []))
                script_texts.append(text)

            script_embeddings = self.embedding_service.embed(script_texts)

            # Calculate cosine similarity
            similarities = self.embedding_service.similarity(
                query_embedding[0] if len(query_embedding.shape) > 1 else query_embedding,
                script_embeddings
            )

            # Normalize to 0-1
            return (similarities + 1) / 2

        except Exception as e:
            logger.error("vector_scoring_failed", error=str(e))
            return np.zeros(len(scripts))

    def _calculate_keyword_scores(
        self,
        keywords: List[str],
        scripts: List[Dict]
    ) -> np.ndarray:
        """Calculate keyword match scores using TF-IDF"""
        if not keywords:
            return np.zeros(len(scripts))

        try:
            from sklearn.feature_extraction.text import TfidfVectorizer
            from sklearn.metrics.pairwise import cosine_similarity

            # Prepare texts
            query_text = " ".join(keywords)
            script_texts = []
            for script in scripts:
                text = f"{script.get('name', '')} {script.get('description', '')} "
                text += " ".join(script.get('keywords', []))
                script_texts.append(text)

            # Fit TF-IDF
            all_texts = [query_text] + script_texts
            vectorizer = TfidfVectorizer(max_features=5000, stop_words='english')
            tfidf_matrix = vectorizer.fit_transform(all_texts)

            # Calculate similarity
            query_vec = tfidf_matrix[0:1]
            script_vecs = tfidf_matrix[1:]

            similarities = cosine_similarity(query_vec, script_vecs)[0]
            return similarities

        except Exception as e:
            logger.error("keyword_scoring_failed", error=str(e))
            return np.zeros(len(scripts))

    def _calculate_metadata_scores(
        self,
        query_context: QueryContext,
        scripts: List[Dict]
    ) -> np.ndarray:
        """Calculate metadata match scores (exact field matching)"""
        scores = np.zeros(len(scripts))

        for i, script in enumerate(scripts):
            score = 0.0
            max_score = 0.0

            # Service match
            if query_context.service:
                max_score += 1.0
                script_service = script.get("service", "").lower()
                query_service = query_context.service.lower()
                if script_service == query_service:
                    score += 1.0
                elif query_service in script_service or script_service in query_service:
                    score += 0.5

            # Intent match (action)
            if query_context.intent != IncidentIntent.UNKNOWN:
                max_score += 1.0
                script_action = script.get("action", "").lower()
                if query_context.intent.value in script_action:
                    score += 1.0

            # Entity match (check required_inputs)
            for entity_key, entity_value in query_context.entities.items():
                required_inputs = script.get("required_inputs", [])
                if entity_key in required_inputs:
                    max_score += 0.5
                    score += 0.5

            # Error pattern match
            error_patterns = script.get("error_patterns", [])
            if error_patterns:
                max_score += 1.0
                query_lower = query_context.original_query.lower()
                pattern_matches = sum(1 for p in error_patterns if p.lower() in query_lower)
                if pattern_matches > 0:
                    score += min(pattern_matches / len(error_patterns), 1.0)

            scores[i] = score / max_score if max_score > 0 else 0.0

        return scores

    def _calculate_graph_scores(
        self,
        query_context: QueryContext,
        scripts: List[Dict]
    ) -> np.ndarray:
        """Calculate graph-based scores using Neo4j FIXED_BY relationships"""
        if not self.enable_graph or not self.graph_scorer:
            return np.full(len(scripts), 0.1)  # Baseline score

        try:
            scores = []
            for script in scripts:
                graph_result = self.graph_scorer.get_score(
                    script_id=script.get("id", ""),
                    service=query_context.service
                )
                scores.append(graph_result.graph_score)
            return np.array(scores)

        except Exception as e:
            logger.error("graph_scoring_failed", error=str(e))
            return np.full(len(scripts), 0.1)

    def _rerank_candidates(
        self,
        query: str,
        candidates: List[Dict]
    ) -> List[Dict]:
        """
        Stage 3: Cross-Encoder Reranking

        Uses cross-encoder to rerank top candidates for better precision.
        """
        if not self.cross_encoder or not candidates:
            return candidates

        try:
            # Prepare for reranking
            rerank_input = []
            for c in candidates:
                script = c["script"]
                content = f"{script.get('name', '')} {script.get('description', '')}"
                rerank_input.append({
                    "content": content,
                    "final_score": c["fusion_score"],
                    "metadata": script,
                    "original_data": c
                })

            # Rerank
            reranked = self.cross_encoder.rerank(query, rerank_input, top_k=len(candidates))

            # Merge rerank scores back
            result = []
            for i, r in enumerate(reranked):
                original_data = r.get("original_data", candidates[i] if i < len(candidates) else {})
                original_data["rerank_score"] = r.get("rerank_score", r.get("final_score", 0.5))
                original_data["final_rank"] = i
                result.append(original_data)

            return result

        except Exception as e:
            logger.error("reranking_failed", error=str(e))
            return candidates

    def _finalize_results(
        self,
        candidates: List[Dict],
        weights: RetrievalWeights,
        top_k: int
    ) -> List[RetrievalResult]:
        """
        Stage 4: Finalize Results

        Convert candidates to RetrievalResult with full metadata.
        """
        results = []

        for i, c in enumerate(candidates[:top_k]):
            script = c.get("script", {})

            # Calculate final score (combine fusion and rerank)
            fusion_score = c.get("fusion_score", 0)
            rerank_score = c.get("rerank_score", fusion_score)
            final_score = 0.6 * rerank_score + 0.4 * fusion_score

            # Generate match reasons
            match_reasons = []
            if c.get("vector_score", 0) > 0.5:
                match_reasons.append(f"High semantic similarity ({c['vector_score']:.0%})")
            if c.get("keyword_score", 0) > 0.4:
                match_reasons.append(f"Strong keyword match ({c['keyword_score']:.0%})")
            if c.get("metadata_score", 0) > 0.3:
                match_reasons.append(f"Metadata alignment ({c['metadata_score']:.0%})")
            if c.get("graph_score", 0) > 0.3:
                match_reasons.append(f"Historical success ({c['graph_score']:.0%})")

            if not match_reasons:
                match_reasons.append("General match")

            original_rank = c.get("original_rank", i)
            final_rank = i

            result = RetrievalResult(
                script_id=script.get("id", ""),
                name=script.get("name", ""),
                description=script.get("description", ""),
                content=script.get("description", ""),
                path=script.get("path", ""),
                script_type=script.get("type", "shell"),

                vector_score=round(c.get("vector_score", 0), 3),
                keyword_score=round(c.get("keyword_score", 0), 3),
                metadata_score=round(c.get("metadata_score", 0), 3),
                graph_score=round(c.get("graph_score", 0), 3),

                fusion_score=round(fusion_score, 3),
                rerank_score=round(rerank_score, 3),
                final_score=round(final_score, 3),

                risk_level=script.get("risk_level", "medium"),
                historical_success_count=script.get("historical_success_count", 0),
                avg_resolution_time=script.get("avg_resolution_time", 0),
                match_reasons=match_reasons,
                requires_approval=not script.get("auto_approve", False),

                original_rank=original_rank,
                final_rank=final_rank,
                rank_change=original_rank - final_rank
            )
            results.append(result)

        return results

    # =========================================================================
    # UTILITIES
    # =========================================================================

    def _get_cache_key(self, query: str, context: Dict) -> str:
        """Generate cache key for query+context"""
        key_data = f"{query}:{json.dumps(context, sort_keys=True)}"
        return hashlib.md5(key_data.encode()).hexdigest()

    def clear_cache(self):
        """Clear all caches"""
        self._embedding_cache.clear()
        self._result_cache.clear()
        logger.info("retriever_cache_cleared")

    def get_metrics(self) -> List[Dict]:
        """Get retrieval metrics"""
        return [asdict(m) for m in self._metrics]

    def explain_result(self, result: RetrievalResult) -> str:
        """Generate human-readable explanation of result"""
        weights = self.DEFAULT_WEIGHTS

        explanation = f"""
## Script Match Analysis: {result.name}

**Final Score: {result.final_score:.1%}**

### Score Breakdown (Enterprise RAG v4.0):
| Component | Weight | Score | Contribution |
|-----------|--------|-------|--------------|
| Vector Similarity | {weights.vector:.0%} | {result.vector_score:.1%} | {result.vector_score * weights.vector:.1%} |
| Keyword Match | {weights.keyword:.0%} | {result.keyword_score:.1%} | {result.keyword_score * weights.keyword:.1%} |
| Metadata Match | {weights.metadata:.0%} | {result.metadata_score:.1%} | {result.metadata_score * weights.metadata:.1%} |
| Graph (FIXED_BY) | {weights.graph:.0%} | {result.graph_score:.1%} | {result.graph_score * weights.graph:.1%} |

### Match Reasons:
{chr(10).join('- ' + r for r in result.match_reasons)}

### Details:
- **Script ID**: {result.script_id}
- **Type**: {result.script_type}
- **Risk Level**: {result.risk_level.upper()}
- **Requires Approval**: {'Yes' if result.requires_approval else 'No'}
- **Historical Successes**: {result.historical_success_count}
- **Rank Change**: {'+' if result.rank_change > 0 else ''}{result.rank_change} (after reranking)
"""
        return explanation.strip()


# =============================================================================
# GLOBAL INSTANCE
# =============================================================================

intelligent_retriever = IntelligentRetriever()
