#!/usr/bin/env python3
"""
Swarm-based Script Selector v5.0 - RRF Fusion

Multi-agent system for evaluating and selecting the best remediation script
using Reciprocal Rank Fusion (RRF) for fair, weight-free consensus.

Architecture:
    4 Specialized Agents → RRF Fusion → Cross-Encoder Rerank → Final Selection

Agents:
1. SemanticAgent: Evaluates semantic similarity to incident
2. SafetyAgent: Evaluates risk and safety considerations
3. ContextAgent: Evaluates based on incident context and history
4. ConsensusAgent: Applies RRF fusion and makes final selection

RRF Formula: Score = Σ (1 / (k + rank_i)) for each agent i
k = 60 (industry standard)

WHY RRF:
- No manual weight bias (removes 0.40, 0.35, 0.25 tuning)
- Scale-invariant (works regardless of raw score magnitudes)
- Industry standard (Google, Bing, OpenAI, Elasticsearch)
"""
import os
import json
import asyncio
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
from abc import ABC, abstractmethod

from openai import OpenAI
from dotenv import load_dotenv

load_dotenv("/home/samrattidke600/ai_agent_app/.env")

# Initialize OpenAI
openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


class AgentRole(Enum):
    SEMANTIC = "semantic"
    SAFETY = "safety"
    CONTEXT = "context"
    CONSENSUS = "consensus"


@dataclass
class AgentEvaluation:
    """Evaluation result from a swarm agent"""
    agent_role: AgentRole
    script_id: str
    score: float  # 0.0 to 1.0
    confidence: float  # 0.0 to 1.0
    reasoning: str
    flags: List[str]  # Warnings or special considerations


@dataclass
class ScriptCandidate:
    """A candidate script for evaluation"""
    script_id: str
    name: str
    path: str
    workflow: str
    description: str
    risk_level: str
    vector_score: float
    graph_score: float
    keywords: List[str] = None
    incident_patterns: List[str] = None


@dataclass
class SwarmDecision:
    """Final decision from the swarm"""
    selected_script: ScriptCandidate
    final_score: float
    confidence: float
    reasoning: str
    agent_evaluations: List[AgentEvaluation]
    approved: bool
    requires_human_approval: bool


class SwarmAgent(ABC):
    """Base class for swarm agents"""

    def __init__(self, role: AgentRole):
        self.role = role
        self.client = openai_client

    @abstractmethod
    async def evaluate(
        self,
        incident: Dict,
        candidates: List[ScriptCandidate]
    ) -> List[AgentEvaluation]:
        """Evaluate candidates and return evaluations"""
        pass

    def _call_llm(self, system_prompt: str, user_prompt: str) -> str:
        """Call LLM for evaluation"""
        response = self.client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.3,
            max_tokens=1000
        )
        return response.choices[0].message.content


class SemanticAgent(SwarmAgent):
    """Evaluates semantic similarity between incident and scripts using rule-based matching"""

    def __init__(self):
        super().__init__(AgentRole.SEMANTIC)

    async def evaluate(
        self,
        incident: Dict,
        candidates: List[ScriptCandidate]
    ) -> List[AgentEvaluation]:
        evaluations = []

        incident_text = f"{incident.get('short_description', '')} {incident.get('description', '')}"
        incident_lower = incident_text.lower()
        incident_words = set(incident_lower.split())

        for candidate in candidates:
            score = 0.0
            reasoning_parts = []

            # Check keyword overlap
            keyword_matches = 0
            if candidate.keywords:
                for kw in candidate.keywords:
                    if kw.lower() in incident_lower:
                        keyword_matches += 1

                if keyword_matches > 0:
                    keyword_score = min(0.3, keyword_matches * 0.1)
                    score += keyword_score
                    reasoning_parts.append(f"Matched {keyword_matches} keywords")

            # Check incident pattern matching
            pattern_matches = 0
            if candidate.incident_patterns:
                for pattern in candidate.incident_patterns:
                    if pattern.lower() in incident_lower:
                        pattern_matches += 1

                if pattern_matches > 0:
                    pattern_score = min(0.5, pattern_matches * 0.2)
                    score += pattern_score
                    reasoning_parts.append(f"Matched {pattern_matches} incident patterns")

            # Add vector score component
            if candidate.vector_score > 0:
                vector_contrib = candidate.vector_score * 0.2
                score += vector_contrib
                reasoning_parts.append(f"Vector similarity: {candidate.vector_score:.2f}")

            # Cap score at 1.0
            score = min(1.0, score)

            # Calculate confidence based on evidence
            confidence = 0.5 + (0.1 * keyword_matches) + (0.15 * pattern_matches)
            confidence = min(0.95, confidence)

            evaluations.append(AgentEvaluation(
                agent_role=self.role,
                script_id=candidate.script_id,
                score=score,
                confidence=confidence,
                reasoning="; ".join(reasoning_parts) if reasoning_parts else "No direct matches",
                flags=[]
            ))

        return evaluations


class SafetyAgent(SwarmAgent):
    """Evaluates safety and risk considerations"""

    def __init__(self):
        super().__init__(AgentRole.SAFETY)

    async def evaluate(
        self,
        incident: Dict,
        candidates: List[ScriptCandidate]
    ) -> List[AgentEvaluation]:
        evaluations = []

        risk_scores = {
            "low": 1.0,
            "medium": 0.8,
            "high": 0.5,
            "critical": 0.2
        }

        for candidate in candidates:
            risk_level = candidate.risk_level.lower()
            base_score = risk_scores.get(risk_level, 0.5)

            flags = []
            if risk_level == "high":
                flags.append("requires_careful_review")
            if risk_level == "critical":
                flags.append("requires_human_approval")
                flags.append("high_risk_action")

            # Adjust score based on incident priority
            incident_priority = incident.get("priority", "3")
            if incident_priority in ["1", "2"] and risk_level in ["low", "medium"]:
                # High priority incident, lower risk script = good match
                base_score = min(1.0, base_score + 0.1)

            evaluations.append(AgentEvaluation(
                agent_role=self.role,
                script_id=candidate.script_id,
                score=base_score,
                confidence=0.9,  # High confidence for rule-based safety
                reasoning=f"Risk level '{risk_level}' evaluated. Priority incident compatibility checked.",
                flags=flags
            ))

        return evaluations


class ContextAgent(SwarmAgent):
    """Evaluates based on incident context and historical patterns"""

    def __init__(self):
        super().__init__(AgentRole.CONTEXT)

    async def evaluate(
        self,
        incident: Dict,
        candidates: List[ScriptCandidate]
    ) -> List[AgentEvaluation]:
        evaluations = []

        incident_text = f"{incident.get('short_description', '')} {incident.get('description', '')}"
        incident_lower = incident_text.lower()
        incident_category = incident.get("category", "").lower()

        for candidate in candidates:
            score = 0.5  # Base score
            flags = []
            reasoning_parts = []

            # Check pattern matching
            patterns_matched = 0
            if candidate.incident_patterns:
                for pattern in candidate.incident_patterns:
                    if pattern.lower() in incident_lower:
                        patterns_matched += 1

                if patterns_matched > 0:
                    score += 0.1 * min(patterns_matched, 3)
                    reasoning_parts.append(f"Matched {patterns_matched} incident patterns")

            # Check keyword matching
            keywords_matched = 0
            if candidate.keywords:
                for keyword in candidate.keywords:
                    if keyword.lower() in incident_lower:
                        keywords_matched += 1

                if keywords_matched > 0:
                    score += 0.05 * min(keywords_matched, 4)
                    reasoning_parts.append(f"Matched {keywords_matched} keywords")

            # Check graph score (from Neo4j)
            if candidate.graph_score > 0:
                score += 0.1 * min(candidate.graph_score / 10, 0.3)
                reasoning_parts.append(f"Graph relevance: {candidate.graph_score}")

            # Cap score at 1.0
            score = min(1.0, score)

            evaluations.append(AgentEvaluation(
                agent_role=self.role,
                script_id=candidate.script_id,
                score=score,
                confidence=0.7,
                reasoning="; ".join(reasoning_parts) if reasoning_parts else "No specific context matches",
                flags=flags
            ))

        return evaluations


class ConsensusAgent(SwarmAgent):
    """Aggregates evaluations from other agents and makes final selection"""

    def __init__(self):
        super().__init__(AgentRole.CONSENSUS)

    async def evaluate(
        self,
        incident: Dict,
        candidates: List[ScriptCandidate]
    ) -> List[AgentEvaluation]:
        # This agent doesn't evaluate candidates directly
        # It's used for final consensus in make_decision
        return []

    def make_decision(
        self,
        incident: Dict,
        candidates: List[ScriptCandidate],
        all_evaluations: List[AgentEvaluation]
    ) -> SwarmDecision:
        """
        Aggregate evaluations using RRF (Reciprocal Rank Fusion).

        RRF Formula: Score = Σ (1 / (k + rank_i)) for each agent i
        k = 60 (industry standard)

        WHY RRF over weighted consensus:
        - No manual weight tuning (removes bias from 0.40, 0.35, 0.25)
        - Scale-invariant (works regardless of raw score magnitudes)
        - Industry standard (Google, Bing, OpenAI, Elasticsearch)
        """
        RRF_K = 60  # Standard RRF constant

        # Group evaluations by script
        script_evals: Dict[str, List[AgentEvaluation]] = {}
        for eval in all_evaluations:
            if eval.script_id not in script_evals:
                script_evals[eval.script_id] = []
            script_evals[eval.script_id].append(eval)

        # Step 1: Get rankings from each agent
        agent_rankings: Dict[AgentRole, List[Tuple[str, float]]] = {}
        for role in [AgentRole.SEMANTIC, AgentRole.SAFETY, AgentRole.CONTEXT]:
            # Collect all scores for this agent role
            role_scores = []
            for script_id, evals in script_evals.items():
                for eval in evals:
                    if eval.agent_role == role:
                        role_scores.append((script_id, eval.score))

            # Sort by score (descending) to get ranking
            role_scores.sort(key=lambda x: x[1], reverse=True)
            agent_rankings[role] = role_scores

        # Step 2: Calculate RRF scores for each script
        script_rrf_scores: Dict[str, Dict] = {}

        for script_id, evals in script_evals.items():
            rrf_score = 0.0
            agent_ranks = {}
            agent_scores = {}
            all_flags = []
            all_reasoning = []
            confidence_sum = 0.0

            for role, rankings in agent_rankings.items():
                # Find this script's rank for this agent
                for rank, (sid, score) in enumerate(rankings, start=1):
                    if sid == script_id:
                        # RRF contribution: 1 / (k + rank)
                        rrf_contribution = 1.0 / (RRF_K + rank)
                        rrf_score += rrf_contribution
                        agent_ranks[role.value] = rank
                        agent_scores[role.value] = score
                        break

            # Collect flags and reasoning from evaluations
            for eval in evals:
                all_flags.extend(eval.flags)
                all_reasoning.append(f"[{eval.agent_role.value}] Rank {agent_ranks.get(eval.agent_role.value, '?')}: {eval.reasoning}")
                confidence_sum += eval.confidence

            avg_confidence = confidence_sum / len(evals) if evals else 0.0

            script_rrf_scores[script_id] = {
                "rrf_score": rrf_score,
                "confidence": avg_confidence,
                "agent_ranks": agent_ranks,
                "agent_scores": agent_scores,
                "flags": list(set(all_flags)),
                "reasoning": all_reasoning,
                "evaluations": evals
            }

        # Step 3: Select best script by RRF score
        best_script_id = max(script_rrf_scores.keys(), key=lambda x: script_rrf_scores[x]["rrf_score"])
        best_score_data = script_rrf_scores[best_script_id]

        # Find the candidate
        selected_candidate = None
        for c in candidates:
            if c.script_id == best_script_id:
                selected_candidate = c
                break

        if not selected_candidate:
            raise ValueError(f"Selected script {best_script_id} not found in candidates")

        # Determine if human approval is required
        requires_approval = (
            "requires_human_approval" in best_score_data["flags"] or
            selected_candidate.risk_level in ["high", "critical"] or
            (best_score_data["rrf_score"] < 0.02 and best_score_data["confidence"] < 0.5)
        )

        # Auto-approve if:
        # - No explicit approval required AND
        # - Reasonable RRF score AND
        # - Risk is not high/critical
        approved = (
            not requires_approval and
            selected_candidate.risk_level in ["low", "medium"] and
            best_score_data["rrf_score"] >= 0.03  # Good RRF score threshold
        )

        return SwarmDecision(
            selected_script=selected_candidate,
            final_score=best_score_data["rrf_score"],
            confidence=best_score_data["confidence"],
            reasoning="\n".join(best_score_data["reasoning"]),
            agent_evaluations=best_score_data["evaluations"],
            approved=approved,
            requires_human_approval=requires_approval
        )


class SwarmScriptSelector:
    """
    Swarm-based script selector that coordinates multiple agents
    to evaluate and select the best remediation script.
    """

    def __init__(self):
        self.semantic_agent = SemanticAgent()
        self.safety_agent = SafetyAgent()
        self.context_agent = ContextAgent()
        self.consensus_agent = ConsensusAgent()

    async def select_script(
        self,
        incident: Dict,
        candidates: List[Dict]
    ) -> SwarmDecision:
        """
        Run swarm evaluation and select the best script.

        Args:
            incident: Incident data from ServiceNow
            candidates: List of candidate scripts from RAG search

        Returns:
            SwarmDecision with selected script and reasoning
        """
        print("\n[SWARM] Starting multi-agent script evaluation...")

        # Convert candidates to ScriptCandidate objects
        script_candidates = [
            ScriptCandidate(
                script_id=c.get("script_id", ""),
                name=c.get("name", ""),
                path=c.get("path", ""),
                workflow=c.get("workflow", ""),
                description=c.get("description", ""),
                risk_level=c.get("risk_level", "medium"),
                vector_score=c.get("vector_score", 0.0),
                graph_score=c.get("graph_score", 0.0),
                keywords=c.get("keywords", []),
                incident_patterns=c.get("incident_patterns", [])
            )
            for c in candidates
        ]

        if not script_candidates:
            raise ValueError("No candidates to evaluate")

        # Run all agents in parallel
        print("[SWARM] Running evaluation agents...")
        semantic_evals, safety_evals, context_evals = await asyncio.gather(
            self.semantic_agent.evaluate(incident, script_candidates),
            self.safety_agent.evaluate(incident, script_candidates),
            self.context_agent.evaluate(incident, script_candidates)
        )

        # Combine all evaluations
        all_evaluations = semantic_evals + safety_evals + context_evals

        # Log agent evaluations
        print("\n[SWARM] Agent Evaluations:")
        for eval in all_evaluations:
            print(f"  [{eval.agent_role.value}] {eval.script_id}: "
                  f"score={eval.score:.2f}, confidence={eval.confidence:.2f}")

        # Make consensus decision
        print("\n[SWARM] Making consensus decision...")
        decision = self.consensus_agent.make_decision(
            incident,
            script_candidates,
            all_evaluations
        )

        print(f"\n[SWARM] Selected: {decision.selected_script.name}")
        print(f"  RRF Score: {decision.final_score:.4f}")
        print(f"  Confidence: {decision.confidence:.2f}")
        print(f"  Auto-Approved: {decision.approved}")
        print(f"  Requires Human Approval: {decision.requires_human_approval}")

        return decision


# Convenience function for direct use
async def select_best_script(incident: Dict, candidates: List[Dict]) -> SwarmDecision:
    """
    Select the best remediation script for an incident.

    Args:
        incident: ServiceNow incident data
        candidates: RAG search results (candidate scripts)

    Returns:
        SwarmDecision with selected script
    """
    selector = SwarmScriptSelector()
    return await selector.select_script(incident, candidates)


if __name__ == "__main__":
    # Test the swarm selector
    test_incident = {
        "short_description": "test-incident-vm-01 instance is off",
        "description": "GCP VM test-incident-vm-01 is in TERMINATED state and needs to be started",
        "priority": "2",
        "category": "infrastructure"
    }

    test_candidates = [
        {
            "script_id": "start_gcp_instance",
            "name": "Start GCP Instance",
            "path": "scripts/start_gcp_instance.sh",
            "workflow": "shell-execute.yml",
            "description": "Start a stopped/terminated GCP Compute Engine instance",
            "risk_level": "medium",
            "vector_score": 0.92,
            "graph_score": 8,
            "keywords": ["gcp", "vm", "instance", "start", "boot", "power on", "terminated", "stopped", "off"],
            "incident_patterns": ["instance is off", "instance is stopped", "instance is terminated", "vm is down"]
        },
        {
            "script_id": "health_check",
            "name": "Health Check",
            "path": "scripts/health_check.sh",
            "workflow": "shell-execute.yml",
            "description": "Run comprehensive health checks on target system",
            "risk_level": "low",
            "vector_score": 0.45,
            "graph_score": 2,
            "keywords": ["health", "check", "status", "diagnostic"],
            "incident_patterns": ["system unresponsive", "service degraded"]
        }
    ]

    async def test():
        decision = await select_best_script(test_incident, test_candidates)
        print(f"\n\nFinal Decision: {decision.selected_script.name}")
        print(f"Path: {decision.selected_script.path}")
        print(f"Workflow: {decision.selected_script.workflow}")

    asyncio.run(test())
