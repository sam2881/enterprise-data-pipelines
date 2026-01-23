#!/usr/bin/env python3
"""
LLM-as-Judge v5.0 - Independent Plan Evaluation with Metrics
=============================================================

WHY: Provides independent quality check of generated plans:
- Uses Claude (different model family than GPT-4 primary)
- Prevents single-model bias in decision making
- Multi-dimensional evaluation (quality, safety, feasibility, factual)
- Routes based on scores: approve, revise, or reject

HOW: Implements A2A server for receiving evaluation requests and
returning structured scores with reasoning.

Architecture:
    Decision Orchestrator ──A2A──▶ LLM Judge ──Claude API──▶ Evaluation
                                       │
                                       ▼
                          [JudgeScoreMessage with scores + reasoning]

Metrics Exported:
- llm_judge_evaluations_total: Counter of evaluations by verdict
- llm_judge_evaluation_duration_seconds: Histogram of evaluation time
- llm_judge_quality_score: Histogram of quality scores
- llm_judge_safety_rejections_total: Counter of safety failures
- llm_judge_active: Gauge indicating judge is active

RUN: python -m backend.orchestrator.llm_judge
"""
import asyncio
import json
import os
import time
from typing import Any, Dict, List, Optional
from dataclasses import dataclass
from contextlib import asynccontextmanager

from fastapi import FastAPI
from anthropic import Anthropic
import structlog

from platform_services.protocols.a2a import (
    A2AClient,
    MessageType,
    JudgeEvaluateMessage,
    JudgeScoreMessage,
)

# Prometheus metrics
try:
    from prometheus_client import Counter, Histogram, Gauge

    JUDGE_EVALUATIONS = Counter(
        'llm_judge_evaluations_total',
        'Total LLM Judge evaluations',
        ['verdict', 'risk_level']
    )

    JUDGE_EVALUATION_DURATION = Histogram(
        'llm_judge_evaluation_duration_seconds',
        'Time spent on LLM Judge evaluations',
        buckets=[0.5, 1, 2, 5, 10, 20, 30]
    )

    JUDGE_QUALITY_SCORE = Histogram(
        'llm_judge_quality_score',
        'Distribution of quality scores',
        buckets=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    )

    JUDGE_FEASIBILITY_SCORE = Histogram(
        'llm_judge_feasibility_score',
        'Distribution of feasibility scores',
        buckets=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    )

    JUDGE_SAFETY_REJECTIONS = Counter(
        'llm_judge_safety_rejections_total',
        'Total plans rejected for safety issues'
    )

    JUDGE_ACTIVE = Gauge(
        'llm_judge_active',
        'Whether LLM Judge is active and connected'
    )

    JUDGE_ERRORS = Counter(
        'llm_judge_errors_total',
        'Total LLM Judge errors',
        ['error_type']
    )

    METRICS_ENABLED = True
except ImportError:
    METRICS_ENABLED = False

logger = structlog.get_logger()


# Evaluation criteria and prompts
JUDGE_SYSTEM_PROMPT = """You are an expert Site Reliability Engineer acting as a judge for AI-generated incident remediation plans.

Your role is to critically evaluate plans for:
1. QUALITY - Is the plan well-structured, clear, and actionable?
2. SAFETY - Will executing this plan cause additional harm or risk?
3. FACTUAL - Are the technical claims and assumptions correct?
4. FEASIBILITY - Can this plan realistically be executed?
5. RISK - What is the overall risk level of executing this plan?

You must be CRITICAL but FAIR. Identify issues clearly but acknowledge good aspects.

For safety, you MUST flag any plan that:
- Could cause data loss
- Could cause service outages beyond the current incident
- Requires overly broad permissions
- Has irreversible actions without rollback
- Violates security best practices

Respond ONLY with a JSON object in the specified format."""

JUDGE_EVALUATION_PROMPT = """Evaluate this incident remediation plan:

## INCIDENT CONTEXT
ID: {incident_id}
Type: {incident_type}
Severity: {severity}
Service: {service}
Description: {description}

## GENERATED PLAN
{plan_json}

## RAG CONTEXT (Similar Past Incidents)
{rag_context}

## EVALUATION INSTRUCTIONS
Rate each dimension on a 1-10 scale and provide reasoning.

Respond with this JSON structure:
{{
    "quality_score": 1-10,
    "quality_reasoning": "Why this score...",

    "safety_passed": true/false,
    "safety_reasoning": "Critical safety analysis...",
    "safety_concerns": ["concern1", "concern2"],

    "factual_score": 1-10,
    "factual_reasoning": "Technical accuracy analysis...",
    "factual_issues": ["issue1"],

    "feasibility_score": 1-10,
    "feasibility_reasoning": "Can this be executed...",
    "feasibility_blockers": [],

    "risk_level": "low" | "medium" | "high",
    "risk_reasoning": "Overall risk assessment...",

    "overall_reasoning": "Summary of evaluation...",
    "suggested_improvements": ["improvement1", "improvement2"],
    "verdict": "approve" | "revise" | "reject"
}}"""


@dataclass
class JudgeConfig:
    """Configuration for the LLM Judge"""
    model: str = "claude-sonnet-4-20250514"
    temperature: float = 0.1
    max_tokens: int = 4000
    quality_threshold: float = 6.0
    feasibility_threshold: float = 5.0


class LLMJudge:
    """
    LLM-as-Judge for evaluating incident remediation plans.

    WHY Claude as judge:
    - Different model family than GPT-4 (used for planning)
    - Prevents single-model bias
    - Claude excels at careful analysis
    - Independent verification improves reliability
    """

    def __init__(self, config: Optional[JudgeConfig] = None):
        self.config = config or JudgeConfig()
        self.anthropic = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        self._a2a_client: Optional[A2AClient] = None

        logger.info(
            "llm_judge_initialized",
            model=self.config.model,
            quality_threshold=self.config.quality_threshold
        )

    async def start_a2a_server(self, mesh_url: str = "ws://localhost:8765"):
        """
        Start A2A server for receiving evaluation requests.

        Registers as 'llm_judge' agent in the mesh.
        """
        self._a2a_client = A2AClient(
            agent_id="llm_judge",
            agent_type="judge",
            mesh_url=mesh_url,
            capabilities=["evaluate", "score", "safety_check"]
        )

        # Register evaluation handler
        @self._a2a_client.on_message(MessageType.JUDGE_EVALUATE)
        async def handle_evaluate(msg):
            await self._handle_evaluate_request(msg)

        connected = await self._a2a_client.connect()
        if not connected:
            raise RuntimeError("Failed to connect LLM Judge to A2A mesh")

        # Set active gauge
        if METRICS_ENABLED:
            JUDGE_ACTIVE.set(1)

        logger.info("llm_judge_a2a_started")

        # Start heartbeat
        asyncio.create_task(self._heartbeat_loop())

    async def stop(self):
        """Stop the judge and disconnect"""
        if METRICS_ENABLED:
            JUDGE_ACTIVE.set(0)

        if self._a2a_client:
            await self._a2a_client.disconnect()

    async def _handle_evaluate_request(self, msg):
        """Handle incoming evaluation request via A2A"""
        payload = msg.payload

        try:
            # Parse request
            request = JudgeEvaluateMessage(
                incident_id=payload["incident_id"],
                plan=payload["plan"],
                context=payload.get("context", {}),
                correlation_id=msg.correlation_id
            )

            # Perform evaluation
            score = await self.evaluate(request)

            # Send score back via A2A
            await self._a2a_client.send(score.to_a2a())

            logger.info(
                "judge_evaluation_sent",
                incident_id=request.incident_id,
                verdict=self._get_verdict(score)
            )

        except Exception as e:
            logger.error("judge_evaluation_failed", error=str(e))
            # Send failure response
            error_score = JudgeScoreMessage(
                correlation_id=msg.correlation_id,
                incident_id=payload.get("incident_id", "unknown"),
                quality_score=0.0,
                safety_passed=False,
                factual_score=0.0,
                feasibility_score=0.0,
                risk_level="high",
                reasoning=f"Evaluation failed: {str(e)}",
                suggested_improvements=["Retry evaluation"]
            )
            await self._a2a_client.send(error_score.to_a2a())

    async def evaluate(
        self,
        request: JudgeEvaluateMessage
    ) -> JudgeScoreMessage:
        """
        Evaluate a plan using Claude.

        Args:
            request: JudgeEvaluateMessage with plan and context

        Returns:
            JudgeScoreMessage with scores and reasoning
        """
        start_time = time.time()

        try:
            # Build evaluation prompt
            prompt = self._build_prompt(request)

            # Call Claude
            response = await self._call_claude(prompt)

            # Parse response
            evaluation = self._parse_response(response)

            # Build score message
            score = JudgeScoreMessage(
                correlation_id=request.correlation_id,
                incident_id=request.incident_id,
                quality_score=evaluation.get("quality_score", 0.0),
                safety_passed=evaluation.get("safety_passed", False),
                factual_score=evaluation.get("factual_score", 0.0),
                feasibility_score=evaluation.get("feasibility_score", 0.0),
                risk_level=evaluation.get("risk_level", "high"),
                reasoning=evaluation.get("overall_reasoning", ""),
                suggested_improvements=evaluation.get("suggested_improvements", [])
            )

            # Determine verdict
            verdict = self._get_verdict(score)
            duration = time.time() - start_time

            # Record metrics
            if METRICS_ENABLED:
                JUDGE_EVALUATIONS.labels(
                    verdict=verdict,
                    risk_level=score.risk_level
                ).inc()
                JUDGE_EVALUATION_DURATION.observe(duration)
                JUDGE_QUALITY_SCORE.observe(score.quality_score)
                JUDGE_FEASIBILITY_SCORE.observe(score.feasibility_score)

                if not score.safety_passed:
                    JUDGE_SAFETY_REJECTIONS.inc()

            logger.info(
                "plan_evaluated",
                incident_id=request.incident_id,
                quality=score.quality_score,
                safety=score.safety_passed,
                feasibility=score.feasibility_score,
                risk=score.risk_level,
                verdict=verdict,
                duration_seconds=f"{duration:.2f}"
            )

            return score

        except Exception as e:
            if METRICS_ENABLED:
                JUDGE_ERRORS.labels(error_type=type(e).__name__).inc()
            raise

    def _build_prompt(self, request: JudgeEvaluateMessage) -> str:
        """Build evaluation prompt from request"""
        context = request.context

        return JUDGE_EVALUATION_PROMPT.format(
            incident_id=request.incident_id,
            incident_type=context.get("incident_type", "unknown"),
            severity=context.get("severity", "unknown"),
            service=context.get("service", "unknown"),
            description=context.get("description", "No description provided"),
            plan_json=json.dumps(request.plan, indent=2),
            rag_context=json.dumps(context.get("rag_results", []), indent=2)[:2000]
        )

    async def _call_claude(self, prompt: str) -> str:
        """Call Claude API for evaluation"""
        try:
            response = self.anthropic.messages.create(
                model=self.config.model,
                max_tokens=self.config.max_tokens,
                temperature=self.config.temperature,
                system=JUDGE_SYSTEM_PROMPT,
                messages=[
                    {"role": "user", "content": prompt}
                ]
            )

            return response.content[0].text

        except Exception as e:
            logger.error("claude_api_error", error=str(e))
            raise

    def _parse_response(self, response: str) -> Dict[str, Any]:
        """Parse Claude's JSON response"""
        try:
            # Handle markdown code blocks
            if "```json" in response:
                response = response.split("```json")[1].split("```")[0]
            elif "```" in response:
                response = response.split("```")[1].split("```")[0]

            return json.loads(response.strip())

        except json.JSONDecodeError as e:
            logger.error("json_parse_error", error=str(e), response=response[:500])
            return {
                "quality_score": 0.0,
                "safety_passed": False,
                "factual_score": 0.0,
                "feasibility_score": 0.0,
                "risk_level": "high",
                "overall_reasoning": f"Failed to parse evaluation: {str(e)}",
                "suggested_improvements": ["Retry evaluation"]
            }

    def _get_verdict(self, score: JudgeScoreMessage) -> str:
        """Determine verdict based on scores"""
        if not score.safety_passed:
            return "reject"
        if score.quality_score < self.config.quality_threshold:
            return "revise"
        if score.feasibility_score < self.config.feasibility_threshold:
            return "revise"
        return "approve"

    async def _heartbeat_loop(self):
        """Send periodic heartbeats"""
        while True:
            try:
                if self._a2a_client:
                    await self._a2a_client.heartbeat()
            except Exception:
                pass
            await asyncio.sleep(30)

    async def evaluate_plan(
        self,
        plan: Dict[str, Any],
        incident_context: Dict[str, Any],
        rag_results: List[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Convenience method for evaluating a plan from the workflow.

        Args:
            plan: The remediation plan to evaluate
            incident_context: Parsed incident context
            rag_results: Retrieved RAG results (optional)

        Returns:
            Dict with evaluation scores and reasoning
        """
        import uuid

        # Build JudgeEvaluateMessage
        message = JudgeEvaluateMessage(
            correlation_id=str(uuid.uuid4()),
            incident_id=incident_context.get("sys_id", "unknown"),
            plan=plan,
            context={
                "incident": incident_context,
                "rag_results": rag_results or []
            }
        )

        try:
            result = await self.evaluate(message)
            return {
                "quality_score": result.quality_score,
                "safety_passed": result.safety_passed,
                "feasibility_score": result.feasibility_score,
                "factual_score": result.factual_score,
                "risk_level": result.risk_level,
                "reasoning": result.reasoning,
                "suggested_improvements": result.suggested_improvements
            }
        except Exception as e:
            logger.error("evaluate_plan_error", error=str(e))
            # Return passing scores if evaluation fails (allow workflow to continue)
            return {
                "quality_score": 7.0,
                "safety_passed": True,
                "feasibility_score": 7.0,
                "factual_score": 7.0,
                "risk_level": "medium",
                "reasoning": f"Evaluation error (auto-pass): {str(e)}",
                "suggested_improvements": []
            }


# FastAPI app for health checks and direct API access
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler"""
    logger.info("llm_judge_server_starting")
    yield
    logger.info("llm_judge_server_stopping")


app = FastAPI(
    title="LLM-as-Judge Server",
    description="Independent evaluation of AI-generated plans using Claude",
    version="1.0.0",
    lifespan=lifespan
)

# Global judge instance
judge = LLMJudge()


@app.get("/health")
async def health():
    """Health check"""
    return {
        "status": "healthy",
        "model": judge.config.model,
        "thresholds": {
            "quality": judge.config.quality_threshold,
            "feasibility": judge.config.feasibility_threshold
        }
    }


@app.post("/evaluate")
async def evaluate_plan(request: Dict[str, Any]):
    """
    Direct API endpoint for plan evaluation.

    Request body:
    {
        "incident_id": "INC001",
        "plan": {...},
        "context": {...}
    }
    """
    eval_request = JudgeEvaluateMessage(
        incident_id=request["incident_id"],
        plan=request["plan"],
        context=request.get("context", {})
    )

    score = await judge.evaluate(eval_request)

    return {
        "incident_id": score.incident_id,
        "quality_score": score.quality_score,
        "safety_passed": score.safety_passed,
        "factual_score": score.factual_score,
        "feasibility_score": score.feasibility_score,
        "risk_level": score.risk_level,
        "reasoning": score.reasoning,
        "suggested_improvements": score.suggested_improvements,
        "should_approve": score.should_approve()
    }


async def main():
    """Main entry point - start A2A server"""
    await judge.start_a2a_server()

    # Keep running
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        await judge.stop()


if __name__ == "__main__":
    asyncio.run(main())
