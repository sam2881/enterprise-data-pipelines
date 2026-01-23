"""
Graph Scorer - Neo4j-based Historical Success Scoring
======================================================

Uses Neo4j graph database to score scripts based on:
1. FIXED_BY relationships (how many times script fixed similar incidents)
2. Success rate (percentage of successful executions)
3. Resolution time (average time to resolve)
4. Recency bonus (recent usage is weighted higher)

Industry Pattern: Graph-based learning from historical outcomes.

Author: AI Agent Platform
Version: 4.0.0
"""

import os
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
import structlog

logger = structlog.get_logger()


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class GraphScoreResult:
    """Result from graph-based scoring"""
    script_id: str
    fixed_by_count: int          # Number of times script fixed similar incidents
    success_rate: float          # 0.0 - 1.0
    avg_resolution_time: float   # Minutes
    last_used: Optional[str]     # ISO timestamp
    recency_score: float         # 0.0 - 1.0 (higher = more recent)
    graph_score: float           # Final combined score 0.0 - 1.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ServiceDependency:
    """Service dependency from graph"""
    service_name: str
    dependency_type: str  # "DEPENDS_ON", "AFFECTS", etc.
    depth: int
    properties: Dict[str, Any]


@dataclass
class HistoricalRemediation:
    """Historical remediation record from graph"""
    incident_id: str
    script_id: str
    service: str
    executed_at: str
    resolution_time: float  # Minutes
    success: bool
    verified: bool


# =============================================================================
# GRAPH SCORER
# =============================================================================

class GraphScorer:
    """
    Neo4j-based Graph Scorer for intelligent script ranking.

    Uses historical data from Neo4j to score scripts:
    - FIXED_BY relationships track successful remediations
    - Service relationships provide context
    - Time-based weighting for recency

    Scoring Formula:
        Graph_Score = (0.4 × Fixed_Count_Score) + (0.3 × Success_Rate) +
                      (0.2 × Speed_Score) + (0.1 × Recency_Score)

    Usage:
        scorer = GraphScorer()
        result = scorer.get_score("script-restart-vm", service="gcp")
    """

    # Scoring weights
    FIXED_COUNT_WEIGHT = 0.40
    SUCCESS_RATE_WEIGHT = 0.30
    SPEED_WEIGHT = 0.20
    RECENCY_WEIGHT = 0.10

    # Baseline score for scripts with no history
    BASELINE_SCORE = 0.10

    # Max values for normalization
    MAX_FIXED_COUNT = 20  # 20+ fixes = max score
    MAX_RESOLUTION_TIME = 60  # 60 minutes = min speed score

    def __init__(self):
        """Initialize Graph Scorer with Neo4j connection"""
        self._neo4j_client = None
        self._cache: Dict[str, GraphScoreResult] = {}
        self._cache_ttl = 300  # 5 minutes cache

        logger.info("graph_scorer_initialized")

    @property
    def neo4j_client(self):
        """Lazy load Neo4j client"""
        if self._neo4j_client is None:
            try:
                from .neo4j_client import neo4j_client
                self._neo4j_client = neo4j_client
                logger.info("neo4j_client_connected")
            except Exception as e:
                logger.warning("neo4j_client_init_failed", error=str(e))
        return self._neo4j_client

    def get_score(
        self,
        script_id: str,
        service: Optional[str] = None,
        incident_type: Optional[str] = None
    ) -> GraphScoreResult:
        """
        Get graph-based score for a script.

        Args:
            script_id: Script identifier
            service: Optional service filter
            incident_type: Optional incident type filter

        Returns:
            GraphScoreResult with detailed breakdown
        """
        # Check cache
        cache_key = f"{script_id}:{service}:{incident_type}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        try:
            # Query historical remediations
            remediations = self._query_remediations(script_id, service, incident_type)

            if not remediations:
                # No history - return baseline
                result = GraphScoreResult(
                    script_id=script_id,
                    fixed_by_count=0,
                    success_rate=0.0,
                    avg_resolution_time=0.0,
                    last_used=None,
                    recency_score=0.0,
                    graph_score=self.BASELINE_SCORE
                )
            else:
                # Calculate scores from history
                result = self._calculate_scores(script_id, remediations)

            # Cache result
            self._cache[cache_key] = result

            return result

        except Exception as e:
            logger.error("graph_scoring_failed", script_id=script_id, error=str(e))
            return GraphScoreResult(
                script_id=script_id,
                fixed_by_count=0,
                success_rate=0.0,
                avg_resolution_time=0.0,
                last_used=None,
                recency_score=0.0,
                graph_score=self.BASELINE_SCORE
            )

    def _query_remediations(
        self,
        script_id: str,
        service: Optional[str] = None,
        incident_type: Optional[str] = None
    ) -> List[HistoricalRemediation]:
        """Query historical remediations from Neo4j"""
        if not self.neo4j_client:
            return []

        try:
            # Build Cypher query
            query = """
            MATCH (i:Incident)-[r:FIXED_BY]->(s:Script {id: $script_id})
            """

            params = {"script_id": script_id}

            if service:
                query += """
                MATCH (i)-[:AFFECTS]->(svc:Service {name: $service})
                """
                params["service"] = service

            query += """
            RETURN i.incident_id as incident_id,
                   s.id as script_id,
                   i.service as service,
                   r.executed_at as executed_at,
                   r.resolution_time as resolution_time,
                   r.success as success,
                   r.verified as verified
            ORDER BY r.executed_at DESC
            LIMIT 50
            """

            results = self.neo4j_client.query_cypher(query, params)

            remediations = []
            for record in results:
                remediations.append(HistoricalRemediation(
                    incident_id=record.get("incident_id", ""),
                    script_id=record.get("script_id", ""),
                    service=record.get("service", ""),
                    executed_at=record.get("executed_at", ""),
                    resolution_time=float(record.get("resolution_time", 0) or 0),
                    success=bool(record.get("success", False)),
                    verified=bool(record.get("verified", False))
                ))

            return remediations

        except Exception as e:
            logger.warning("neo4j_query_failed", error=str(e))
            return []

    def _calculate_scores(
        self,
        script_id: str,
        remediations: List[HistoricalRemediation]
    ) -> GraphScoreResult:
        """Calculate comprehensive graph score from remediations"""

        # 1. Fixed count score
        successful = [r for r in remediations if r.success]
        fixed_count = len(successful)
        fixed_count_score = min(fixed_count / self.MAX_FIXED_COUNT, 1.0)

        # 2. Success rate
        total_executions = len(remediations)
        success_rate = len(successful) / total_executions if total_executions > 0 else 0.0

        # 3. Average resolution time and speed score
        resolution_times = [r.resolution_time for r in successful if r.resolution_time > 0]
        avg_resolution_time = sum(resolution_times) / len(resolution_times) if resolution_times else 0.0

        # Speed score: faster = better (inverse of time, normalized)
        if avg_resolution_time > 0:
            speed_score = max(0, 1.0 - (avg_resolution_time / self.MAX_RESOLUTION_TIME))
        else:
            speed_score = 0.5  # Unknown speed = neutral

        # 4. Recency score
        last_used = None
        recency_score = 0.0

        if remediations:
            # Find most recent
            sorted_by_time = sorted(
                [r for r in remediations if r.executed_at],
                key=lambda x: x.executed_at,
                reverse=True
            )
            if sorted_by_time:
                last_used = sorted_by_time[0].executed_at
                try:
                    # Calculate days since last use
                    last_dt = datetime.fromisoformat(last_used.replace('Z', '+00:00'))
                    days_ago = (datetime.now(last_dt.tzinfo) - last_dt).days
                    # Recency score: higher for recent use (decay over 30 days)
                    recency_score = max(0, 1.0 - (days_ago / 30))
                except:
                    recency_score = 0.5

        # 5. Calculate final graph score
        graph_score = (
            self.FIXED_COUNT_WEIGHT * fixed_count_score +
            self.SUCCESS_RATE_WEIGHT * success_rate +
            self.SPEED_WEIGHT * speed_score +
            self.RECENCY_WEIGHT * recency_score
        )

        # Ensure minimum baseline
        graph_score = max(graph_score, self.BASELINE_SCORE)

        logger.debug(
            "graph_score_calculated",
            script_id=script_id,
            fixed_count=fixed_count,
            success_rate=f"{success_rate:.2%}",
            avg_resolution_time=f"{avg_resolution_time:.1f}min",
            graph_score=f"{graph_score:.2%}"
        )

        return GraphScoreResult(
            script_id=script_id,
            fixed_by_count=fixed_count,
            success_rate=round(success_rate, 3),
            avg_resolution_time=round(avg_resolution_time, 1),
            last_used=last_used,
            recency_score=round(recency_score, 3),
            graph_score=round(graph_score, 3)
        )

    def record_execution(
        self,
        incident_id: str,
        script_id: str,
        service: str,
        success: bool,
        resolution_time: float,
        verified: bool = False
    ):
        """
        Record a script execution in the graph.

        Creates FIXED_BY relationship between incident and script.

        Args:
            incident_id: Incident identifier
            script_id: Script that was executed
            service: Service affected
            success: Whether execution succeeded
            resolution_time: Time to resolve in minutes
            verified: Whether resolution was verified
        """
        if not self.neo4j_client:
            logger.warning("cannot_record_execution_no_neo4j")
            return

        try:
            query = """
            MERGE (i:Incident {incident_id: $incident_id})
            SET i.service = $service

            MERGE (s:Script {id: $script_id})

            MERGE (i)-[r:FIXED_BY]->(s)
            SET r.executed_at = $executed_at,
                r.success = $success,
                r.resolution_time = $resolution_time,
                r.verified = $verified

            MERGE (svc:Service {name: $service})
            MERGE (i)-[:AFFECTS]->(svc)
            """

            params = {
                "incident_id": incident_id,
                "script_id": script_id,
                "service": service,
                "success": success,
                "resolution_time": resolution_time,
                "verified": verified,
                "executed_at": datetime.now().isoformat()
            }

            self.neo4j_client.query_cypher(query, params)

            # Invalidate cache for this script
            self._invalidate_cache(script_id)

            logger.info(
                "execution_recorded",
                incident_id=incident_id,
                script_id=script_id,
                success=success
            )

        except Exception as e:
            logger.error("record_execution_failed", error=str(e))

    def get_service_dependencies(
        self,
        service: str,
        max_depth: int = 3
    ) -> List[ServiceDependency]:
        """
        Get service dependencies from graph.

        Useful for understanding blast radius and related services.

        Args:
            service: Service name
            max_depth: Maximum traversal depth

        Returns:
            List of ServiceDependency objects
        """
        if not self.neo4j_client:
            return []

        try:
            query = """
            MATCH path = (s:Service {name: $service})-[r:DEPENDS_ON*1..]->(downstream)
            WHERE length(path) <= $max_depth
            RETURN downstream.name as service_name,
                   type(last(relationships(path))) as dependency_type,
                   length(path) as depth,
                   properties(downstream) as properties
            ORDER BY depth ASC
            """

            results = self.neo4j_client.query_cypher(query, {
                "service": service,
                "max_depth": max_depth
            })

            dependencies = []
            for record in results:
                dependencies.append(ServiceDependency(
                    service_name=record.get("service_name", ""),
                    dependency_type=record.get("dependency_type", "DEPENDS_ON"),
                    depth=record.get("depth", 1),
                    properties=record.get("properties", {})
                ))

            return dependencies

        except Exception as e:
            logger.warning("get_dependencies_failed", error=str(e))
            return []

    def find_similar_resolved_incidents(
        self,
        service: str,
        incident_type: Optional[str] = None,
        limit: int = 5
    ) -> List[Dict[str, Any]]:
        """
        Find similar incidents that were successfully resolved.

        Useful for understanding what worked before.

        Args:
            service: Service name
            incident_type: Optional incident type filter
            limit: Maximum results

        Returns:
            List of resolved incident records
        """
        if not self.neo4j_client:
            return []

        try:
            query = """
            MATCH (i:Incident)-[r:FIXED_BY {success: true}]->(s:Script)
            MATCH (i)-[:AFFECTS]->(svc:Service {name: $service})
            """

            params = {"service": service, "limit": limit}

            if incident_type:
                query += "WHERE i.type = $incident_type\n"
                params["incident_type"] = incident_type

            query += """
            RETURN i.incident_id as incident_id,
                   i.description as description,
                   s.id as script_id,
                   s.name as script_name,
                   r.resolution_time as resolution_time,
                   r.executed_at as executed_at
            ORDER BY r.executed_at DESC
            LIMIT $limit
            """

            results = self.neo4j_client.query_cypher(query, params)

            return [dict(r) for r in results]

        except Exception as e:
            logger.warning("find_similar_incidents_failed", error=str(e))
            return []

    def get_script_stats(self, script_id: str) -> Dict[str, Any]:
        """
        Get comprehensive statistics for a script.

        Args:
            script_id: Script identifier

        Returns:
            Dictionary with statistics
        """
        result = self.get_score(script_id)

        return {
            "script_id": script_id,
            "total_executions": result.fixed_by_count,
            "success_rate": f"{result.success_rate:.1%}",
            "avg_resolution_time": f"{result.avg_resolution_time:.1f} minutes",
            "last_used": result.last_used or "Never",
            "graph_score": f"{result.graph_score:.1%}",
            "score_breakdown": {
                "fixed_count_contribution": f"{result.fixed_by_count / self.MAX_FIXED_COUNT * self.FIXED_COUNT_WEIGHT:.1%}",
                "success_rate_contribution": f"{result.success_rate * self.SUCCESS_RATE_WEIGHT:.1%}",
                "recency_contribution": f"{result.recency_score * self.RECENCY_WEIGHT:.1%}"
            }
        }

    def _invalidate_cache(self, script_id: str):
        """Invalidate cache entries for a script"""
        keys_to_remove = [k for k in self._cache if k.startswith(f"{script_id}:")]
        for key in keys_to_remove:
            del self._cache[key]

    def clear_cache(self):
        """Clear all cached scores"""
        self._cache.clear()
        logger.info("graph_scorer_cache_cleared")

    def get_successful_remediations(
        self,
        service: str = "",
        category: str = "",
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Get successful historical remediations from the graph.

        Args:
            service: Filter by service name
            category: Filter by incident category/type
            limit: Maximum results to return

        Returns:
            List of successful remediation records with script info
        """
        if not self.neo4j_client:
            return []

        try:
            # Build query based on filters
            query = """
            MATCH (i:Incident)-[r:FIXED_BY {success: true}]->(s:Script)
            """

            params = {"limit": limit}
            where_clauses = []

            if service:
                where_clauses.append("(i)-[:AFFECTS]->(:Service {name: $service})")
                params["service"] = service

            if category:
                where_clauses.append("(i.category = $category OR i.type = $category)")
                params["category"] = category

            if where_clauses:
                query += "WHERE " + " AND ".join(where_clauses) + "\n"

            query += """
            RETURN s.id as script_id,
                   s.name as script_name,
                   s.path as script_path,
                   count(r) as execution_count,
                   avg(r.resolution_time) as avg_resolution_time,
                   sum(CASE WHEN r.success THEN 1 ELSE 0 END) * 1.0 / count(r) as success_rate,
                   max(r.executed_at) as last_used
            ORDER BY execution_count DESC
            LIMIT $limit
            """

            results = self.neo4j_client.query_cypher(query, params)

            return [
                {
                    "script_id": r.get("script_id", ""),
                    "script_name": r.get("script_name", ""),
                    "script_path": r.get("script_path", ""),
                    "execution_count": r.get("execution_count", 0),
                    "avg_resolution_time": r.get("avg_resolution_time", 0),
                    "success_rate": r.get("success_rate", 0.0),
                    "last_used": r.get("last_used", "")
                }
                for r in results
            ]

        except Exception as e:
            logger.warning("get_successful_remediations_failed", error=str(e))
            return []


# =============================================================================
# GLOBAL INSTANCE
# =============================================================================

graph_scorer = GraphScorer()
