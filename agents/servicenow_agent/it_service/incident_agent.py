"""
Incident Agent - ServiceNow Incident Triage and Analysis

WHY: Automates incident triage process:
     - Classifies incidents by severity and category
     - Identifies root cause
     - Suggests resolution steps
     - Provides confidence scoring

HOW: Uses LLM with structured prompts to analyze incidents.
     Extends BaseAgent for consistent execution pattern.

NOTE: This agent has NO backend dependencies. It receives
      incidents as IAgentTask and returns IAgentResult.
      Backend is responsible for ServiceNow API integration.
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
    IncidentData,
)

logger = structlog.get_logger()


class IncidentAgent(BaseAgent):
    """
    Agent for incident triage and root cause analysis.

    Capabilities:
    - Incident classification (severity, category)
    - Root cause identification
    - Resolution step generation
    - Confidence scoring

    ISOLATION: This agent does NOT:
    - Connect to ServiceNow directly (backend does this)
    - Import any backend modules
    - Access databases or caches directly
    """

    def __init__(self, config: Optional[AgentConfig] = None):
        super().__init__(config)
        self._init_llm_client()

    def _init_llm_client(self):
        """Initialize LLM client based on config"""
        # Lazy import to avoid backend dependencies
        try:
            from openai import OpenAI
            api_key = self.config.llm.api_key or os.getenv("OPENAI_API_KEY")
            if api_key:
                self.llm_client = OpenAI(api_key=api_key)
            else:
                self.llm_client = None
                logger.warning("llm_client_not_configured", reason="No API key")
        except ImportError:
            self.llm_client = None
            logger.warning("openai_not_installed")

    @property
    def agent_id(self) -> str:
        return "servicenow-incident-agent"

    @property
    def agent_name(self) -> str:
        return "ServiceNow Incident Agent"

    @property
    def capabilities(self) -> List[AgentCapability]:
        return [AgentCapability.INCIDENT_TRIAGE]

    async def _execute(self, task: IAgentTask) -> Dict[str, Any]:
        """
        Execute incident analysis.

        Expected task payload:
        {
            "incident_id": "INC0001234",
            "short_description": "Server down",
            "description": "Production server is not responding",
            "category": "infrastructure",
            "priority": "1",
            "service": "payment-service",
            "context": {  # Optional: RAG context from backend
                "similar_incidents": [...],
                "runbooks": [...],
            }
        }

        Returns:
        {
            "classification": {...},
            "root_cause_analysis": {...},
            "resolution": {...},
            "confidence": 0.85
        }
        """
        payload = task.payload
        incident_id = payload.get("incident_id", task.task_id)

        # Extract incident data
        incident = IncidentData(
            incident_id=incident_id,
            short_description=payload.get("short_description", ""),
            description=payload.get("description", ""),
            category=payload.get("category", "unknown"),
            priority=payload.get("priority", "3"),
            status=payload.get("status", "new"),
        )

        # Get context (provided by backend from RAG)
        context = task.context or {}
        similar_incidents = context.get("similar_incidents", [])
        runbooks = context.get("runbooks", [])

        logger.info(
            "analyzing_incident",
            incident_id=incident.incident_id,
            category=incident.category,
            context_items=len(similar_incidents) + len(runbooks),
        )

        # Perform analysis
        if self.llm_client:
            analysis = await self._analyze_with_llm(incident, similar_incidents, runbooks)
        else:
            analysis = self._fallback_analysis(incident)

        return {
            "incident_id": incident.incident_id,
            "classification": {
                "severity": analysis.get("severity", "P3"),
                "category": analysis.get("category", incident.category),
                "urgency": analysis.get("urgency", "medium"),
                "reasoning": analysis.get("classification_reasoning", ""),
            },
            "root_cause_analysis": {
                "root_cause": analysis.get("root_cause", "Unknown"),
                "confidence": analysis.get("confidence_score", 0) / 100.0,
                "evidence": analysis.get("supporting_evidence", []),
                "alternatives": analysis.get("alternative_causes", []),
            },
            "resolution": {
                "steps": analysis.get("resolution_steps", "Manual investigation required"),
                "estimated_time": analysis.get("estimated_time", "Unknown"),
                "risk_level": analysis.get("risk_level", "medium"),
                "rollback_plan": analysis.get("rollback_plan", ""),
            },
            "context_used": {
                "similar_incidents_count": len(similar_incidents),
                "runbooks_count": len(runbooks),
            },
        }

    async def _analyze_with_llm(
        self,
        incident: IncidentData,
        similar_incidents: List[Dict],
        runbooks: List[Dict],
    ) -> Dict[str, Any]:
        """Analyze incident using LLM"""

        # Format context
        historical_context = self._format_similar_incidents(similar_incidents)
        runbook_context = self._format_runbooks(runbooks)

        prompt = f"""Analyze this IT incident and provide structured analysis.

INCIDENT:
ID: {incident.incident_id}
Title: {incident.short_description}
Description: {incident.description}
Category: {incident.category}
Priority: {incident.priority}

HISTORICAL CONTEXT (Similar Incidents):
{historical_context}

RELEVANT RUNBOOKS:
{runbook_context}

Analyze and provide JSON response with:
- severity: P1, P2, P3, or P4
- category: infrastructure, application, network, database, security, or other
- urgency: high, medium, or low
- root_cause: Brief description of the likely root cause
- confidence_score: 0-100 confidence in your analysis
- resolution_steps: Step-by-step resolution instructions
- classification_reasoning: Why you classified it this way
- supporting_evidence: List of evidence supporting your analysis
- alternative_causes: List of other possible causes
- estimated_time: Estimated resolution time
- risk_level: high, medium, or low
- rollback_plan: Steps to rollback if needed

Return ONLY valid JSON."""

        try:
            # Use sync client in async context (OpenAI client handles this)
            response = self.llm_client.chat.completions.create(
                model=self.config.llm.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are an expert IT incident analyst. "
                                   "Provide structured JSON responses only."
                    },
                    {"role": "user", "content": prompt}
                ],
                temperature=self.config.llm.temperature,
                max_tokens=2000,
            )

            content = response.choices[0].message.content.strip()

            # Remove markdown code blocks if present
            if content.startswith("```"):
                content = content.split("```")[1]
                if content.startswith("json"):
                    content = content[4:]
                content = content.strip()

            return json.loads(content)

        except Exception as e:
            logger.error("llm_analysis_failed", error=str(e))
            return self._fallback_analysis(incident)

    def _format_similar_incidents(self, incidents: List[Dict]) -> str:
        """Format similar incidents for prompt"""
        if not incidents:
            return "No similar incidents found."

        formatted = []
        for i, inc in enumerate(incidents[:3], 1):
            formatted.append(
                f"{i}. {inc.get('short_description', 'N/A')}\n"
                f"   Resolution: {inc.get('resolution', 'N/A')[:200]}"
            )
        return "\n".join(formatted)

    def _format_runbooks(self, runbooks: List[Dict]) -> str:
        """Format runbooks for prompt"""
        if not runbooks:
            return "No relevant runbooks found."

        formatted = []
        for i, rb in enumerate(runbooks[:3], 1):
            formatted.append(
                f"{i}. {rb.get('name', rb.get('doc_id', 'N/A'))}\n"
                f"   {rb.get('content', rb.get('description', 'N/A'))[:300]}"
            )
        return "\n".join(formatted)

    def _fallback_analysis(self, incident: IncidentData) -> Dict[str, Any]:
        """Fallback analysis when LLM is unavailable"""
        return {
            "severity": "P3",
            "category": incident.category or "other",
            "urgency": "medium",
            "root_cause": "Manual analysis required - LLM unavailable",
            "confidence_score": 0,
            "resolution_steps": "1. Review incident details\n2. Check related systems\n3. Escalate if needed",
            "classification_reasoning": "Default classification - LLM analysis unavailable",
            "supporting_evidence": [],
            "alternative_causes": [],
            "estimated_time": "Unknown",
            "risk_level": "medium",
            "rollback_plan": "Consult incident manager",
        }
