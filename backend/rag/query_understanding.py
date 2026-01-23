"""
Query Understanding Module - Intelligent Query Processing
==========================================================

Processes incident queries to extract:
1. Intent Classification (restart, scale, repair, rollback, etc.)
2. Entity Extraction (instance names, zones, services)
3. Query Expansion (synonyms, related terms)
4. Confidence Scoring

Supports both LLM-based and rule-based understanding.

Author: AI Agent Platform
Version: 4.0.0
"""

import os
import re
import json
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
from enum import Enum
import structlog

logger = structlog.get_logger()


# =============================================================================
# ENUMS AND DATA CLASSES
# =============================================================================

class IncidentIntent(Enum):
    """Classified incident intents"""
    RESTART = "restart"
    SCALE = "scale"
    REPAIR = "repair"
    ROLLBACK = "rollback"
    INVESTIGATE = "investigate"
    CONFIGURE = "configure"
    DEPLOY = "deploy"
    BACKUP = "backup"
    SECURITY = "security"
    NETWORK = "network"
    UNKNOWN = "unknown"


class ServiceType(Enum):
    """Service/Platform types"""
    GCP = "gcp"
    AWS = "aws"
    AZURE = "azure"
    KUBERNETES = "kubernetes"
    DATABASE = "database"
    NETWORK = "network"
    APPLICATION = "application"
    MONITORING = "monitoring"
    SECURITY = "security"
    UNKNOWN = "unknown"


class Severity(Enum):
    """Incident severity levels"""
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


@dataclass
class ExtractedEntity:
    """Extracted entity from query"""
    entity_type: str
    value: str
    confidence: float
    start_pos: int = 0
    end_pos: int = 0


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
    understanding_method: str  # "llm" or "rule_based"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "original_query": self.original_query,
            "expanded_query": self.expanded_query,
            "intent": self.intent.value,
            "entities": self.entities,
            "keywords": self.keywords,
            "service": self.service,
            "severity": self.severity,
            "confidence": self.confidence,
            "understanding_method": self.understanding_method
        }


# =============================================================================
# QUERY UNDERSTANDING ENGINE
# =============================================================================

class QueryUnderstanding:
    """
    Intelligent Query Understanding Engine.

    Features:
    - Intent classification (rule-based + LLM)
    - Entity extraction (regex + NER patterns)
    - Query expansion (synonyms, related terms)
    - Severity detection
    - Confidence scoring

    Usage:
        qu = QueryUnderstanding()
        context = qu.understand(
            "VM instance test-vm-01 is down in us-central1-a",
            {"severity": "high"}
        )
    """

    # Intent keyword mappings
    INTENT_KEYWORDS = {
        IncidentIntent.RESTART: [
            "restart", "reboot", "start", "stopped", "down", "terminated",
            "not running", "offline", "unavailable", "start instance"
        ],
        IncidentIntent.SCALE: [
            "scale", "capacity", "resource", "memory", "cpu", "disk",
            "autoscale", "resize", "expand", "increase", "oom", "out of memory"
        ],
        IncidentIntent.REPAIR: [
            "fix", "repair", "error", "failed", "broken", "corrupt",
            "crash", "bug", "issue", "problem", "malfunction"
        ],
        IncidentIntent.ROLLBACK: [
            "rollback", "revert", "undo", "previous", "restore",
            "roll back", "previous version", "last known good"
        ],
        IncidentIntent.INVESTIGATE: [
            "investigate", "analyze", "check", "diagnose", "monitor",
            "inspect", "examine", "troubleshoot", "debug", "logs"
        ],
        IncidentIntent.CONFIGURE: [
            "configure", "config", "setting", "parameter", "update",
            "change", "modify", "adjust", "tune"
        ],
        IncidentIntent.DEPLOY: [
            "deploy", "release", "push", "publish", "promote",
            "launch", "ship", "go live"
        ],
        IncidentIntent.BACKUP: [
            "backup", "snapshot", "archive", "save", "preserve",
            "export", "dump"
        ],
        IncidentIntent.SECURITY: [
            "security", "vulnerability", "breach", "attack", "intrusion",
            "unauthorized", "permission", "access", "firewall", "ssl", "certificate"
        ],
        IncidentIntent.NETWORK: [
            "network", "dns", "connectivity", "latency", "timeout",
            "connection", "routing", "load balancer", "proxy"
        ],
    }

    # Service detection patterns
    SERVICE_PATTERNS = {
        ServiceType.GCP: [
            r"\bgcp\b", r"\bgoogle\s*cloud\b", r"\bcompute\s*engine\b",
            r"\bgce\b", r"\bvm\s*instance\b", r"\bgke\b", r"\bbigquery\b",
            r"\bcloud\s*storage\b", r"\bcloud\s*sql\b"
        ],
        ServiceType.AWS: [
            r"\baws\b", r"\bec2\b", r"\bs3\b", r"\brds\b", r"\blambda\b",
            r"\beks\b", r"\bdynamodb\b", r"\bcloudfront\b"
        ],
        ServiceType.AZURE: [
            r"\bazure\b", r"\baks\b", r"\bazure\s*vm\b", r"\bcosmosdb\b"
        ],
        ServiceType.KUBERNETES: [
            r"\bk8s\b", r"\bkubernetes\b", r"\bpod\b", r"\bdeployment\b",
            r"\bnamespace\b", r"\bcontainer\b", r"\bhelm\b", r"\bkubectl\b"
        ],
        ServiceType.DATABASE: [
            r"\bdatabase\b", r"\bdb\b", r"\bpostgres\b", r"\bmysql\b",
            r"\bredis\b", r"\bmongodb\b", r"\bcassandra\b", r"\belastic\b"
        ],
        ServiceType.NETWORK: [
            r"\bnetwork\b", r"\bdns\b", r"\bload\s*balancer\b",
            r"\bfirewall\b", r"\bvpc\b", r"\bsubnet\b", r"\brouting\b"
        ],
    }

    # Entity extraction patterns
    ENTITY_PATTERNS = {
        "instance_name": [
            r"(?:VM\s+instance|instance|vm|server)\s+([a-zA-Z0-9][-a-zA-Z0-9]*)",
            r"([a-zA-Z0-9][-a-zA-Z0-9]*-vm[-a-zA-Z0-9]*)",
            r"Instance:\s*([a-zA-Z0-9][-a-zA-Z0-9]*)",
        ],
        "zone": [
            r"(?:zone|region)[:\s]+([a-zA-Z]+-[a-zA-Z0-9]+-[a-zA-Z0-9]+)",
            r"in\s+(?:zone\s+)?([a-zA-Z]+-[a-zA-Z0-9]+-[a-z])",
            r"([a-z]+-[a-z]+\d+-[a-z])",
        ],
        "namespace": [
            r"(?:namespace|ns)[:\s]+([a-zA-Z0-9][-a-zA-Z0-9]*)",
            r"in\s+namespace\s+([a-zA-Z0-9][-a-zA-Z0-9]*)",
        ],
        "pod_name": [
            r"(?:pod|container)[:\s]+([a-zA-Z0-9][-a-zA-Z0-9]*)",
            r"pod\s+([a-zA-Z0-9][-a-zA-Z0-9]*-[a-zA-Z0-9]+)",
        ],
        "deployment_name": [
            r"(?:deployment|deploy)[:\s]+([a-zA-Z0-9][-a-zA-Z0-9]*)",
        ],
        "service_name": [
            r"(?:service)[:\s]+([a-zA-Z0-9][-a-zA-Z0-9]*)",
        ],
        "database_name": [
            r"(?:database|db)[:\s]+([a-zA-Z0-9][-a-zA-Z0-9_]*)",
        ],
        "project_id": [
            r"(?:project)[:\s]+([a-zA-Z0-9][-a-zA-Z0-9]*)",
            r"(?:project\s+id)[:\s]+([a-zA-Z0-9][-a-zA-Z0-9]*)",
        ],
        "error_code": [
            r"(?:error|code)[:\s]+(\d{3,5})",
            r"HTTP\s+(\d{3})",
            r"status\s+(\d{3})",
        ],
    }

    # Query expansion synonyms
    EXPANSIONS = {
        "down": ["stopped", "terminated", "offline", "unavailable", "not responding"],
        "slow": ["latency", "performance", "timeout", "delay", "lag"],
        "error": ["exception", "failure", "crash", "bug", "fault"],
        "memory": ["ram", "heap", "oom", "memory leak", "out of memory"],
        "cpu": ["processor", "compute", "high cpu", "cpu spike", "throttling"],
        "disk": ["storage", "volume", "iops", "disk full", "space"],
        "restart": ["reboot", "start", "recover", "bring up"],
        "scale": ["resize", "expand", "autoscale", "horizontal scale"],
        "connection": ["connectivity", "network", "timeout", "refused"],
        "database": ["db", "postgres", "mysql", "redis", "datastore"],
    }

    # Severity indicators
    SEVERITY_INDICATORS = {
        Severity.CRITICAL: [
            "critical", "emergency", "p1", "sev1", "outage", "down",
            "complete failure", "all users", "production down"
        ],
        Severity.HIGH: [
            "high", "urgent", "p2", "sev2", "severe", "major",
            "significant impact", "many users"
        ],
        Severity.MEDIUM: [
            "medium", "moderate", "p3", "sev3", "partial",
            "some users", "degraded"
        ],
        Severity.LOW: [
            "low", "minor", "p4", "sev4", "cosmetic",
            "few users", "non-critical"
        ],
    }

    def __init__(self, enable_llm: bool = True):
        """
        Initialize Query Understanding engine.

        Args:
            enable_llm: Enable LLM-based understanding (fallback to rule-based if unavailable)
        """
        self.enable_llm = enable_llm
        self._openai_client = None

        logger.info("query_understanding_initialized", enable_llm=enable_llm)

    @property
    def openai_client(self):
        """Lazy load OpenAI client"""
        if self._openai_client is None and self.enable_llm:
            try:
                from openai import OpenAI
                api_key = os.getenv("OPENAI_API_KEY")
                if api_key:
                    self._openai_client = OpenAI(api_key=api_key)
            except Exception as e:
                logger.warning("openai_client_init_failed", error=str(e))
        return self._openai_client

    def understand(
        self,
        query: str,
        context: Optional[Dict[str, Any]] = None
    ) -> QueryContext:
        """
        Main method: Understand and enrich query.

        Args:
            query: Raw incident query/description
            context: Additional context (severity, service hints)

        Returns:
            QueryContext with enriched information
        """
        context = context or {}

        # Try LLM-based understanding first
        if self.enable_llm and self.openai_client:
            try:
                return self._llm_understand(query, context)
            except Exception as e:
                logger.warning("llm_understanding_failed_fallback", error=str(e))

        # Fallback to rule-based understanding
        return self._rule_based_understand(query, context)

    def _llm_understand(
        self,
        query: str,
        context: Dict[str, Any]
    ) -> QueryContext:
        """LLM-based query understanding"""
        prompt = f"""Analyze this incident query and extract structured information.

INCIDENT QUERY:
{query}

ADDITIONAL CONTEXT:
{json.dumps(context)}

Extract and return JSON with:
{{
    "intent": "restart|scale|repair|rollback|investigate|configure|deploy|backup|security|network|unknown",
    "entities": {{
        "instance_name": "extracted value or null",
        "zone": "extracted value or null",
        "namespace": "extracted value or null",
        "pod_name": "extracted value or null",
        "service_name": "extracted value or null",
        "database_name": "extracted value or null",
        "project_id": "extracted value or null"
    }},
    "service_type": "gcp|aws|azure|kubernetes|database|network|application|unknown",
    "severity": "critical|high|medium|low",
    "keywords": ["keyword1", "keyword2", ...],
    "expanded_terms": ["synonym1", "related_term1", ...],
    "confidence": 0.0-1.0
}}
"""

        response = self.openai_client.chat.completions.create(
            model="gpt-4-turbo-preview",
            messages=[
                {"role": "system", "content": "You are an expert SRE analyzing incident queries. Extract structured information. Respond only with valid JSON."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,
            response_format={"type": "json_object"}
        )

        result = json.loads(response.choices[0].message.content)

        # Map intent string to enum
        intent_str = result.get("intent", "unknown")
        try:
            intent = IncidentIntent(intent_str)
        except ValueError:
            intent = IncidentIntent.UNKNOWN

        # Build expanded query
        keywords = result.get("keywords", [])
        expanded_terms = result.get("expanded_terms", [])
        expanded_query = query + " " + " ".join(keywords + expanded_terms)

        return QueryContext(
            original_query=query,
            expanded_query=expanded_query,
            intent=intent,
            entities={k: v for k, v in result.get("entities", {}).items() if v},
            keywords=keywords,
            service=result.get("service_type"),
            severity=result.get("severity", "medium"),
            confidence=result.get("confidence", 0.8),
            understanding_method="llm"
        )

    def _rule_based_understand(
        self,
        query: str,
        context: Dict[str, Any]
    ) -> QueryContext:
        """Rule-based query understanding"""
        query_lower = query.lower()

        # 1. Intent Classification
        intent, intent_confidence = self._classify_intent(query_lower)

        # 2. Entity Extraction
        entities = self._extract_entities(query)

        # 3. Service Detection
        service = self._detect_service(query_lower) or context.get("service")

        # 4. Severity Detection
        severity = self._detect_severity(query_lower) or context.get("severity", "medium")

        # 5. Keyword Extraction
        keywords = self._extract_keywords(query_lower)

        # 6. Query Expansion
        expanded_terms = self._expand_query(keywords)
        expanded_query = query + " " + " ".join(expanded_terms)

        # Calculate overall confidence
        confidence = (intent_confidence + 0.7) / 2  # Average with base confidence

        return QueryContext(
            original_query=query,
            expanded_query=expanded_query,
            intent=intent,
            entities=entities,
            keywords=keywords,
            service=service,
            severity=severity,
            confidence=confidence,
            understanding_method="rule_based"
        )

    def _classify_intent(self, query_lower: str) -> Tuple[IncidentIntent, float]:
        """Classify intent based on keyword matching"""
        best_intent = IncidentIntent.UNKNOWN
        best_score = 0.0

        for intent, keywords in self.INTENT_KEYWORDS.items():
            matches = sum(1 for kw in keywords if kw in query_lower)
            if matches > 0:
                score = matches / len(keywords)
                if score > best_score:
                    best_score = score
                    best_intent = intent

        return best_intent, min(best_score * 2, 1.0)  # Boost score

    def _extract_entities(self, query: str) -> Dict[str, str]:
        """Extract entities using regex patterns"""
        entities = {}

        for entity_type, patterns in self.ENTITY_PATTERNS.items():
            for pattern in patterns:
                match = re.search(pattern, query, re.IGNORECASE)
                if match:
                    entities[entity_type] = match.group(1)
                    break

        return entities

    def _detect_service(self, query_lower: str) -> Optional[str]:
        """Detect service/platform type"""
        for service_type, patterns in self.SERVICE_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, query_lower):
                    return service_type.value
        return None

    def _detect_severity(self, query_lower: str) -> Optional[str]:
        """Detect severity level"""
        for severity, indicators in self.SEVERITY_INDICATORS.items():
            if any(ind in query_lower for ind in indicators):
                return severity.value
        return None

    def _extract_keywords(self, query_lower: str) -> List[str]:
        """Extract meaningful keywords"""
        # Remove common stopwords
        stopwords = {
            'the', 'a', 'an', 'is', 'are', 'was', 'were', 'in', 'on', 'at',
            'to', 'for', 'of', 'and', 'or', 'but', 'with', 'by', 'from',
            'this', 'that', 'these', 'those', 'it', 'its', 'be', 'been',
            'being', 'have', 'has', 'had', 'do', 'does', 'did', 'will',
            'would', 'could', 'should', 'may', 'might', 'must', 'can'
        }

        # Extract words
        words = re.findall(r'\b[a-zA-Z]{3,}\b', query_lower)
        keywords = [w for w in words if w not in stopwords]

        # Deduplicate while preserving order
        seen = set()
        unique_keywords = []
        for kw in keywords:
            if kw not in seen:
                seen.add(kw)
                unique_keywords.append(kw)

        return unique_keywords[:15]  # Limit to 15 keywords

    def _expand_query(self, keywords: List[str]) -> List[str]:
        """Expand query with synonyms and related terms"""
        expanded = []

        for keyword in keywords:
            if keyword in self.EXPANSIONS:
                # Add first 2 expansions for each matching keyword
                expanded.extend(self.EXPANSIONS[keyword][:2])

        return list(set(expanded))[:10]  # Deduplicate and limit

    def get_intent_description(self, intent: IncidentIntent) -> str:
        """Get human-readable intent description"""
        descriptions = {
            IncidentIntent.RESTART: "Restart or recover a stopped/failed service",
            IncidentIntent.SCALE: "Scale resources to handle load/capacity issues",
            IncidentIntent.REPAIR: "Repair or fix an error/bug/malfunction",
            IncidentIntent.ROLLBACK: "Rollback to a previous working state",
            IncidentIntent.INVESTIGATE: "Investigate and diagnose the issue",
            IncidentIntent.CONFIGURE: "Update configuration or settings",
            IncidentIntent.DEPLOY: "Deploy or release a new version",
            IncidentIntent.BACKUP: "Create backup or snapshot",
            IncidentIntent.SECURITY: "Address security vulnerability or threat",
            IncidentIntent.NETWORK: "Fix network connectivity or routing issue",
            IncidentIntent.UNKNOWN: "Intent not clearly identified",
        }
        return descriptions.get(intent, "Unknown intent")


# =============================================================================
# GLOBAL INSTANCE
# =============================================================================

query_understanding = QueryUnderstanding()
