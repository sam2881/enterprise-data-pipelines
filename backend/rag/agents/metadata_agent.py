"""
Metadata Agent - Exact Field Matching

WHY: Uses structured metadata for precise filtering:
- Matches cloud provider, environment, service exactly
- Good for narrowing search when context is known
- Weight 0.10 as it's more for filtering than discovery

HOW: Scores documents based on how many metadata fields match exactly.
"""
from typing import List, Optional, Dict, Any
import structlog

from .base_rag_agent import BaseRAGAgent, SearchContext, SearchResult

logger = structlog.get_logger()


class MetadataAgent(BaseRAGAgent):
    """
    Metadata-based exact match search.

    WHY 0.10 weight:
    - Metadata filtering is precise but narrow
    - Best used to boost results that match context
    - Complements broader semantic/keyword search
    """

    WEIGHT = 0.10

    # Fields to match in metadata
    MATCH_FIELDS = [
        "cloud_provider",
        "environment",
        "service",
        "resource_type",
        "action",
        "severity",
        "region",
        "team"
    ]

    def __init__(
        self,
        agent_id: Optional[str] = None,
        mesh_url: str = "ws://localhost:8765"
    ):
        super().__init__(agent_id, mesh_url)
        self._documents = []

    @property
    def agent_type(self) -> str:
        return "metadata"

    def index_documents(self, documents: List[dict]) -> int:
        """Store documents for metadata search"""
        self._documents = documents
        return len(documents)

    async def _perform_search(
        self,
        context: SearchContext
    ) -> List[SearchResult]:
        """
        Search by matching metadata fields.

        Scores each document based on how many fields match.
        """
        if not self._documents:
            logger.warning("metadata_search_no_documents")
            return []

        try:
            query_metadata = context.filters

            # Also extract metadata hints from query
            extracted = self._extract_metadata_from_query(context.query)
            query_metadata.update(extracted)

            if not query_metadata:
                logger.debug("metadata_search_no_filters")
                return []

            results = []
            for idx, doc in enumerate(self._documents):
                score = self._calculate_match_score(
                    query_metadata,
                    doc.get("metadata", {})
                )

                if score > 0:
                    results.append(SearchResult(
                        id=doc.get("id", f"doc_{idx}"),
                        content=doc.get("content", ""),
                        score=score,
                        metadata=doc.get("metadata", {}),
                        source="metadata"
                    ))

            # Sort by score and limit
            results.sort(key=lambda x: x.score, reverse=True)
            results = results[:context.max_results]

            logger.debug(
                "metadata_search_complete",
                filters=len(query_metadata),
                results=len(results)
            )

            return results

        except Exception as e:
            logger.error("metadata_search_failed", error=str(e))
            return []

    def _calculate_match_score(
        self,
        query_metadata: Dict[str, Any],
        doc_metadata: Dict[str, Any]
    ) -> float:
        """
        Calculate match score between query and document metadata.

        Exact match = 1.0 per field
        Partial match = 0.5 per field
        """
        if not query_metadata:
            return 0.0

        matches = 0
        total_fields = 0

        for field in self.MATCH_FIELDS:
            if field in query_metadata:
                total_fields += 1
                query_val = str(query_metadata[field]).lower()
                doc_val = str(doc_metadata.get(field, "")).lower()

                if query_val == doc_val:
                    matches += 1.0
                elif query_val in doc_val or doc_val in query_val:
                    matches += 0.5

        if total_fields == 0:
            return 0.0

        return matches / total_fields

    def _extract_metadata_from_query(
        self,
        query: str
    ) -> Dict[str, str]:
        """
        Extract metadata hints from natural language query.

        Looks for patterns like cloud providers, environments, etc.
        """
        query_lower = query.lower()
        extracted = {}

        # Cloud providers
        if "gcp" in query_lower or "google cloud" in query_lower:
            extracted["cloud_provider"] = "gcp"
        elif "aws" in query_lower or "amazon" in query_lower:
            extracted["cloud_provider"] = "aws"
        elif "azure" in query_lower or "microsoft" in query_lower:
            extracted["cloud_provider"] = "azure"

        # Environments
        if "prod" in query_lower or "production" in query_lower:
            extracted["environment"] = "production"
        elif "staging" in query_lower:
            extracted["environment"] = "staging"
        elif "dev" in query_lower or "development" in query_lower:
            extracted["environment"] = "development"

        # Severity
        if "critical" in query_lower or "p1" in query_lower:
            extracted["severity"] = "critical"
        elif "high" in query_lower or "p2" in query_lower:
            extracted["severity"] = "high"

        # Resource types
        resource_patterns = {
            "vm": "compute_instance",
            "instance": "compute_instance",
            "database": "database",
            "bucket": "storage",
            "load balancer": "load_balancer",
            "kubernetes": "kubernetes",
            "pod": "kubernetes_pod",
            "container": "container"
        }

        for pattern, resource_type in resource_patterns.items():
            if pattern in query_lower:
                extracted["resource_type"] = resource_type
                break

        # Actions
        action_patterns = {
            "restart": "restart",
            "scale": "scale",
            "stop": "stop",
            "start": "start",
            "create": "create",
            "delete": "delete",
            "fix": "remediate",
            "patch": "patch"
        }

        for pattern, action in action_patterns.items():
            if pattern in query_lower:
                extracted["action"] = action
                break

        return extracted
