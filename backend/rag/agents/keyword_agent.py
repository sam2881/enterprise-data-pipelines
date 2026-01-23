"""
Keyword Agent - TF-IDF Based Search

WHY: Uses keyword matching for exact term search:
- Best for technical terms, error codes, service names
- Complements semantic search for precise matches
- Weight 0.25 balances with other strategies

HOW: Uses scikit-learn TF-IDF vectorizer with n-gram matching.
"""
from typing import List, Optional
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import structlog

from .base_rag_agent import BaseRAGAgent, SearchContext, SearchResult

logger = structlog.get_logger()


class KeywordAgent(BaseRAGAgent):
    """
    Keyword-based search using TF-IDF.

    WHY 0.25 weight:
    - Good for exact technical terms
    - Complements semantic search
    - Important for error codes, IDs, specific terms
    """

    WEIGHT = 0.25

    def __init__(
        self,
        agent_id: Optional[str] = None,
        mesh_url: str = "ws://localhost:8765"
    ):
        super().__init__(agent_id, mesh_url)

        self._vectorizer = TfidfVectorizer(
            max_features=10000,
            stop_words='english',
            ngram_range=(1, 2),
            lowercase=True
        )
        self._documents = []
        self._tfidf_matrix = None

    @property
    def agent_type(self) -> str:
        return "keyword"

    def index_documents(self, documents: List[dict]) -> int:
        """
        Index documents for keyword search.

        Args:
            documents: List of dicts with 'id', 'content', 'metadata'

        Returns:
            Number of documents indexed
        """
        self._documents = documents

        if documents:
            texts = [self._prepare_text(doc) for doc in documents]
            self._tfidf_matrix = self._vectorizer.fit_transform(texts)
            logger.info(
                "keyword_index_created",
                documents=len(documents),
                features=self._tfidf_matrix.shape[1]
            )

        return len(documents)

    def _prepare_text(self, doc: dict) -> str:
        """Prepare document text for TF-IDF"""
        parts = [doc.get("content", "")]

        metadata = doc.get("metadata", {})
        for field in ["name", "description", "keywords", "tags", "error_patterns"]:
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
        Perform TF-IDF keyword search.

        Uses cosine similarity on TF-IDF vectors.
        """
        if not self._documents or self._tfidf_matrix is None:
            logger.warning("keyword_search_no_documents")
            return []

        try:
            # Transform query
            query_vector = self._vectorizer.transform([context.query])

            # Calculate similarities
            similarities = cosine_similarity(query_vector, self._tfidf_matrix)[0]

            # Get top results
            top_indices = np.argsort(similarities)[-context.max_results:][::-1]

            results = []
            for idx in top_indices:
                if similarities[idx] > 0.1:  # Threshold
                    doc = self._documents[idx]
                    results.append(SearchResult(
                        id=doc.get("id", f"doc_{idx}"),
                        content=doc.get("content", ""),
                        score=float(similarities[idx]),
                        metadata=doc.get("metadata", {}),
                        source="keyword"
                    ))

            logger.debug(
                "keyword_search_complete",
                query_len=len(context.query),
                results=len(results)
            )

            return results

        except Exception as e:
            logger.error("keyword_search_failed", error=str(e))
            return []
