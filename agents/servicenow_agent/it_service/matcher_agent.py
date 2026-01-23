"""
Script Matcher Agent - Remediation Script Matching

WHY: Matches incidents to remediation scripts using:
     - Semantic similarity (embeddings)
     - Keyword matching (TF-IDF)
     - LLM reasoning for complex cases

HOW: Receives incident + script catalog from backend,
     returns ranked script matches with confidence scores.

NOTE: Agent does NOT access RAG directly. Backend provides
      candidate scripts and context. Agent performs matching logic.
"""
import json
import os
from typing import Any, Dict, List, Optional
import structlog

from agents.shared import (
    BaseAgent,
    IAgentTask,
    AgentCapability,
    AgentConfig,
    ScriptMatch,
    RiskLevel,
)

logger = structlog.get_logger()


class ScriptMatcherAgent(BaseAgent):
    """
    Agent for matching incidents to remediation scripts.

    Capabilities:
    - Script matching with confidence scoring
    - Risk assessment for each match
    - Input extraction from incident description

    ISOLATION: This agent does NOT:
    - Access RAG/vector databases directly
    - Import backend modules
    - Connect to external systems
    """

    def __init__(self, config: Optional[AgentConfig] = None):
        super().__init__(config)
        self._init_llm_client()

    def _init_llm_client(self):
        """Initialize LLM client"""
        try:
            from openai import OpenAI
            api_key = self.config.llm.api_key or os.getenv("OPENAI_API_KEY")
            if api_key:
                self.llm_client = OpenAI(api_key=api_key)
            else:
                self.llm_client = None
        except ImportError:
            self.llm_client = None

    @property
    def agent_id(self) -> str:
        return "servicenow-script-matcher"

    @property
    def agent_name(self) -> str:
        return "Script Matcher Agent"

    @property
    def capabilities(self) -> List[AgentCapability]:
        return [AgentCapability.SCRIPT_MATCHING]

    async def _execute(self, task: IAgentTask) -> Dict[str, Any]:
        """
        Execute script matching.

        Expected task payload:
        {
            "incident_id": "INC0001234",
            "short_description": "High CPU on server",
            "description": "Server XYZ showing 95% CPU",
            "category": "infrastructure",
            "candidate_scripts": [
                {
                    "script_id": "script-001",
                    "name": "cpu_diagnostic.py",
                    "description": "Diagnoses high CPU issues",
                    "type": "python",
                    "required_inputs": ["server_name"],
                    "risk_level": "low"
                },
                ...
            ]
        }

        Returns:
        {
            "matches": [
                {
                    "script_id": "script-001",
                    "confidence": 0.92,
                    "risk_level": "low",
                    "auto_approve": true,
                    "extracted_inputs": {"server_name": "XYZ"},
                    "match_reason": "..."
                }
            ],
            "best_match": {...},
            "analysis": {...}
        }
        """
        payload = task.payload
        incident_id = payload.get("incident_id", task.task_id)
        description = payload.get("description", "")
        short_description = payload.get("short_description", "")
        category = payload.get("category", "unknown")
        candidate_scripts = payload.get("candidate_scripts", [])

        if not candidate_scripts:
            return {
                "matches": [],
                "best_match": None,
                "analysis": {
                    "status": "no_candidates",
                    "message": "No candidate scripts provided"
                }
            }

        logger.info(
            "matching_scripts",
            incident_id=incident_id,
            candidate_count=len(candidate_scripts),
        )

        # Perform matching
        if self.llm_client:
            matches = await self._match_with_llm(
                incident_id=incident_id,
                description=f"{short_description}\n{description}",
                category=category,
                candidates=candidate_scripts,
            )
        else:
            matches = self._keyword_match(
                description=f"{short_description}\n{description}",
                candidates=candidate_scripts,
            )

        # Sort by confidence
        matches.sort(key=lambda x: x["confidence"], reverse=True)

        # Determine best match
        best_match = matches[0] if matches and matches[0]["confidence"] >= 0.5 else None

        return {
            "incident_id": incident_id,
            "matches": matches,
            "best_match": best_match,
            "analysis": {
                "total_candidates": len(candidate_scripts),
                "matches_found": len([m for m in matches if m["confidence"] >= 0.5]),
                "auto_approvable": best_match["auto_approve"] if best_match else False,
            }
        }

    async def _match_with_llm(
        self,
        incident_id: str,
        description: str,
        category: str,
        candidates: List[Dict],
    ) -> List[Dict[str, Any]]:
        """Match scripts using LLM reasoning"""

        # Format candidates for prompt
        candidates_text = "\n".join([
            f"[{i+1}] ID: {c['script_id']}\n"
            f"    Name: {c['name']}\n"
            f"    Description: {c.get('description', 'N/A')}\n"
            f"    Type: {c.get('type', 'unknown')}\n"
            f"    Required Inputs: {c.get('required_inputs', [])}\n"
            f"    Risk Level: {c.get('risk_level', 'unknown')}"
            for i, c in enumerate(candidates)
        ])

        prompt = f"""Match this incident to the best remediation scripts.

INCIDENT:
ID: {incident_id}
Category: {category}
Description: {description}

CANDIDATE SCRIPTS:
{candidates_text}

For each candidate, provide:
1. confidence: 0.0-1.0 match confidence
2. match_reason: Why this script matches (or doesn't)
3. extracted_inputs: Extract values for required inputs from description

Return JSON array with one object per candidate:
[
  {{
    "script_id": "...",
    "confidence": 0.85,
    "match_reason": "...",
    "extracted_inputs": {{"param": "value"}}
  }}
]

Return ONLY valid JSON array."""

        try:
            response = self.llm_client.chat.completions.create(
                model=self.config.llm.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are an expert at matching IT incidents to remediation scripts. "
                                   "Return only valid JSON."
                    },
                    {"role": "user", "content": prompt}
                ],
                temperature=self.config.llm.temperature,
                max_tokens=2000,
            )

            content = response.choices[0].message.content.strip()
            if content.startswith("```"):
                content = content.split("```")[1]
                if content.startswith("json"):
                    content = content[4:]
                content = content.strip()

            llm_matches = json.loads(content)

            # Merge LLM results with candidate metadata
            matches = []
            for llm_match in llm_matches:
                script_id = llm_match.get("script_id")
                candidate = next((c for c in candidates if c["script_id"] == script_id), None)
                if candidate:
                    confidence = float(llm_match.get("confidence", 0))
                    risk_level = candidate.get("risk_level", "medium")

                    matches.append({
                        "script_id": script_id,
                        "name": candidate["name"],
                        "description": candidate.get("description", ""),
                        "script_type": candidate.get("type", "unknown"),
                        "confidence": confidence,
                        "risk_level": risk_level,
                        "auto_approve": confidence >= 0.8 and risk_level == "low",
                        "required_inputs": candidate.get("required_inputs", []),
                        "extracted_inputs": llm_match.get("extracted_inputs", {}),
                        "match_reason": llm_match.get("match_reason", ""),
                        "source": "llm",
                    })

            return matches

        except Exception as e:
            logger.error("llm_matching_failed", error=str(e))
            return self._keyword_match(description, candidates)

    def _keyword_match(
        self,
        description: str,
        candidates: List[Dict],
    ) -> List[Dict[str, Any]]:
        """Fallback keyword-based matching"""
        description_lower = description.lower()
        matches = []

        for candidate in candidates:
            # Simple keyword matching
            name_lower = candidate.get("name", "").lower()
            desc_lower = candidate.get("description", "").lower()

            # Count keyword matches
            keywords = name_lower.split("_") + name_lower.split("-") + desc_lower.split()
            matches_found = sum(1 for kw in keywords if kw in description_lower and len(kw) > 2)
            confidence = min(matches_found * 0.15, 0.75)  # Cap at 0.75 for keyword matching

            risk_level = candidate.get("risk_level", "medium")

            matches.append({
                "script_id": candidate["script_id"],
                "name": candidate["name"],
                "description": candidate.get("description", ""),
                "script_type": candidate.get("type", "unknown"),
                "confidence": confidence,
                "risk_level": risk_level,
                "auto_approve": confidence >= 0.6 and risk_level == "low",
                "required_inputs": candidate.get("required_inputs", []),
                "extracted_inputs": {},
                "match_reason": f"Keyword match ({matches_found} keywords)",
                "source": "keyword",
            })

        return matches
