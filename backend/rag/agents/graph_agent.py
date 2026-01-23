"""
Graph Agent - Neo4j FIXED_BY Relationship Search

WHY: Uses historical success data for ranking:
- Scripts that fixed similar incidents get higher scores
- Learns from past resolutions
- Weight 0.25 because historical success is highly predictive

HOW: Queries Neo4j for FIXED_BY relationships between incidents and scripts.
"""
from typing import List, Optional, Dict, Any
import structlog

from .base_rag_agent import BaseRAGAgent, SearchContext, SearchResult

logger = structlog.get_logger()


class GraphAgent(BaseRAGAgent):
    """
    Graph-based search using Neo4j relationships.

    WHY 0.25 weight:
    - Historical success is highly predictive
    - FIXED_BY relationships track what worked
    - Enables learning from past incidents
    """

    WEIGHT = 0.25

    def __init__(
        self,
        agent_id: Optional[str] = None,
        mesh_url: str = "ws://localhost:8765"
    ):
        super().__init__(agent_id, mesh_url)
        self._neo4j_client = None
        self._documents = []

    @property
    def agent_type(self) -> str:
        return "graph"

    @property
    def neo4j_client(self):
        """Lazy load Neo4j client"""
        if self._neo4j_client is None:
            try:
                from backend.rag.neo4j_client import neo4j_client
                self._neo4j_client = neo4j_client
                logger.info("graph_agent_neo4j_connected")
            except Exception as e:
                logger.warning("neo4j_client_init_failed", error=str(e))
        return self._neo4j_client

    def index_documents(self, documents: List[dict]) -> int:
        """
        Store documents for graph-enhanced search.

        Note: The actual graph data is in Neo4j, this just stores
        document metadata for result building.
        """
        self._documents = documents
        return len(documents)

    async def _perform_search(
        self,
        context: SearchContext
    ) -> List[SearchResult]:
        """
        Search using Neo4j graph relationships.

        Finds scripts that have FIXED_BY relationships with similar incidents.
        """
        try:
            # Find similar resolved incidents
            similar = await self._find_similar_incidents(context)

            if not similar:
                logger.debug("graph_search_no_similar_incidents")
                return []

            # Get scripts that fixed those incidents
            scripts = await self._get_fixing_scripts(similar)

            results = []
            for script in scripts:
                results.append(SearchResult(
                    id=script["script_id"],
                    content=script.get("content", ""),
                    score=script["score"],
                    metadata={
                        "fixed_count": script["fixed_count"],
                        "success_rate": script["success_rate"],
                        "avg_resolution_time": script["avg_resolution_time"],
                        "similar_incidents": script["similar_incidents"]
                    },
                    source="graph"
                ))

            # Sort by score and limit
            results.sort(key=lambda x: x.score, reverse=True)
            results = results[:context.max_results]

            logger.debug(
                "graph_search_complete",
                similar_incidents=len(similar),
                results=len(results)
            )

            return results

        except Exception as e:
            logger.error("graph_search_failed", error=str(e))
            return []

    async def _find_similar_incidents(
        self,
        context: SearchContext
    ) -> List[Dict[str, Any]]:
        """Find similar past incidents from graph"""
        if not self.neo4j_client:
            return []

        try:
            # Query for similar incidents based on service and type
            query = """
            MATCH (i:Incident)
            WHERE i.status = 'resolved'
            """

            params = {}

            # Filter by incident type if available
            if context.incident_type:
                query += " AND i.type = $incident_type"
                params["incident_type"] = context.incident_type

            # Filter by service if in filters
            if "service" in context.filters:
                query += """
                MATCH (i)-[:AFFECTS]->(s:Service {name: $service})
                """
                params["service"] = context.filters["service"]

            query += """
            RETURN i.incident_id as incident_id,
                   i.description as description,
                   i.type as type,
                   i.service as service
            LIMIT 20
            """

            results = self.neo4j_client.query_cypher(query, params)
            return [dict(r) for r in results]

        except Exception as e:
            logger.warning("find_similar_incidents_failed", error=str(e))
            return []

    async def _get_fixing_scripts(
        self,
        similar_incidents: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Get scripts that fixed similar incidents"""
        if not self.neo4j_client or not similar_incidents:
            return []

        try:
            incident_ids = [i["incident_id"] for i in similar_incidents]

            query = """
            MATCH (i:Incident)-[r:FIXED_BY]->(s:Script)
            WHERE i.incident_id IN $incident_ids AND r.success = true
            WITH s, collect(i.incident_id) as incidents,
                 count(r) as fixed_count,
                 avg(r.resolution_time) as avg_time
            RETURN s.id as script_id,
                   s.content as content,
                   fixed_count,
                   avg_time as avg_resolution_time,
                   incidents as similar_incidents
            ORDER BY fixed_count DESC
            """

            results = self.neo4j_client.query_cypher(query, {
                "incident_ids": incident_ids
            })

            scripts = []
            for r in results:
                fixed_count = r.get("fixed_count", 0)
                # Calculate score based on how many similar incidents were fixed
                score = min(fixed_count / 10, 1.0)  # Normalize: 10+ fixes = max score

                scripts.append({
                    "script_id": r.get("script_id", ""),
                    "content": r.get("content", ""),
                    "fixed_count": fixed_count,
                    "success_rate": 1.0,  # Only counting successful fixes
                    "avg_resolution_time": r.get("avg_resolution_time", 0),
                    "similar_incidents": r.get("similar_incidents", []),
                    "score": score
                })

            return scripts

        except Exception as e:
            logger.warning("get_fixing_scripts_failed", error=str(e))
            return []
