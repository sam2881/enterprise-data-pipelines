"""
Vector Agent - Semantic Similarity Search

WHY: Uses embedding-based search for semantic understanding:
- Finds documents with similar meaning even with different words
- Best for general queries and concept matching
- Higher weight (0.40) because semantic match is most important

HOW: Uses SentenceTransformer embeddings with cosine similarity.
"""
from typing import List, Optional
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
import structlog

from .base_rag_agent import BaseRAGAgent, SearchContext, SearchResult

logger = structlog.get_logger()


class VectorAgent(BaseRAGAgent):
    """
    Semantic search using vector embeddings.

    WHY 0.40 weight:
    - Semantic similarity captures meaning
    - Works well for natural language queries
    - Handles paraphrasing and synonyms
    """

    WEIGHT = 0.40

    def __init__(
        self,
        agent_id: Optional[str] = None,
        mesh_url: str = "ws://localhost:8765"
    ):
        super().__init__(agent_id, mesh_url)

        self._model = None
        self._documents = []
        self._embeddings = None

    @property
    def agent_type(self) -> str:
        return "vector"

    @property
    def model(self):
        """Lazy load embedding model"""
        if self._model is None:
            try:
                from sentence_transformers import SentenceTransformer
                self._model = SentenceTransformer('all-MiniLM-L6-v2')
                logger.info("vector_model_loaded", model="all-MiniLM-L6-v2")
            except ImportError:
                logger.warning("sentence_transformers_not_installed")
        return self._model

    def index_documents(self, documents: List[dict]) -> int:
        """
        Index documents for vector search.

        Args:
            documents: List of dicts with 'id', 'content', 'metadata'

        Returns:
            Number of documents indexed
        """
        self._documents = documents

        # Generate embeddings
        if self.model and documents:
            texts = [self._prepare_text(doc) for doc in documents]
            self._embeddings = self.model.encode(texts, show_progress_bar=False)
            logger.info("documents_indexed", count=len(documents))

        return len(documents)

    def _prepare_text(self, doc: dict) -> str:
        """Prepare document text for embedding"""
        parts = [doc.get("content", "")]

        metadata = doc.get("metadata", {})
        for field in ["name", "description", "keywords"]:
            if field in metadata:
                val = metadata[field]
                if isinstance(val, list):
                    parts.extend(str(v) for v in val)
                else:
                    parts.append(str(val))

        return " ".join(parts)

    async def _perform_search(
        self,
        context: SearchContext
    ) -> List[SearchResult]:
        """
        Perform vector similarity search.

        Uses cosine similarity between query embedding and document embeddings.
        """
        if not self._documents or self._embeddings is None:
            logger.warning("vector_search_no_documents")
            return []

        try:
            # Encode query
            if self.model:
                query_embedding = self.model.encode([context.query])[0]
            else:
                # Mock embedding for testing
                query_embedding = np.random.rand(384)

            # Calculate similarities
            similarities = cosine_similarity(
                [query_embedding],
                self._embeddings
            )[0]

            # Normalize to 0-1 range
            normalized = (similarities + 1) / 2

            # Get top results
            top_indices = np.argsort(normalized)[-context.max_results:][::-1]

            results = []
            for idx in top_indices:
                if normalized[idx] > 0.3:  # Threshold
                    doc = self._documents[idx]
                    results.append(SearchResult(
                        id=doc.get("id", f"doc_{idx}"),
                        content=doc.get("content", ""),
                        score=float(normalized[idx]),
                        metadata=doc.get("metadata", {}),
                        source="vector"
                    ))

            logger.debug(
                "vector_search_complete",
                query_len=len(context.query),
                results=len(results)
            )

            return results

        except Exception as e:
            logger.error("vector_search_failed", error=str(e))
            return []
