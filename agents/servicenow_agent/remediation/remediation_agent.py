"""
Remediation Agent - Script Execution and Monitoring

WHY: Executes remediation scripts with:
     - Dry-run validation
     - Input validation
     - Execution monitoring
     - Rollback generation

HOW: Receives execution plan from backend, validates inputs,
     returns execution-ready payload. Actual execution happens
     in backend infrastructure layer.

NOTE: Agent does NOT execute scripts directly. It:
     1. Validates inputs against required params
     2. Generates execution plan
     3. Creates rollback plan
     4. Backend handles actual execution via infrastructure layer
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
    RiskLevel,
)

logger = structlog.get_logger()


class RemediationAgent(BaseAgent):
    """
    Agent for remediation planning and validation.

    Capabilities:
    - Input validation
    - Execution plan generation
    - Rollback plan generation
    - Risk assessment

    ISOLATION: This agent does NOT:
    - Execute scripts directly
    - Access file systems
    - Connect to servers
    - Import backend modules
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
        return "servicenow-remediation-agent"

    @property
    def agent_name(self) -> str:
        return "Remediation Agent"

    @property
    def capabilities(self) -> List[AgentCapability]:
        return [AgentCapability.REMEDIATION]

    async def _execute(self, task: IAgentTask) -> Dict[str, Any]:
        """
        Generate remediation execution plan.

        Expected task payload:
        {
            "incident_id": "INC0001234",
            "script": {
                "script_id": "script-001",
                "name": "restart_service.py",
                "type": "python",
                "content": "...",  # Optional: script content for analysis
                "required_inputs": ["service_name", "server"],
                "risk_level": "medium"
            },
            "inputs": {
                "service_name": "payment-api",
                "server": "prod-server-01"
            },
            "environment": "production",
            "dry_run": false
        }

        Returns:
        {
            "execution_plan": {...},
            "validation": {...},
            "rollback_plan": {...},
            "risk_assessment": {...},
            "ready_to_execute": true/false
        }
        """
        payload = task.payload
        incident_id = payload.get("incident_id", task.task_id)
        script = payload.get("script", {})
        inputs = payload.get("inputs", {})
        environment = payload.get("environment", "development")
        dry_run = payload.get("dry_run", True)

        logger.info(
            "generating_execution_plan",
            incident_id=incident_id,
            script_id=script.get("script_id"),
            environment=environment,
            dry_run=dry_run,
        )

        # Step 1: Validate inputs
        validation = self._validate_inputs(script, inputs)

        if not validation["valid"]:
            return {
                "incident_id": incident_id,
                "execution_plan": None,
                "validation": validation,
                "rollback_plan": None,
                "risk_assessment": None,
                "ready_to_execute": False,
                "error": "Input validation failed",
            }

        # Step 2: Assess risk
        risk_assessment = self._assess_risk(script, inputs, environment)

        # Step 3: Generate execution plan
        execution_plan = self._generate_execution_plan(
            script=script,
            inputs=inputs,
            environment=environment,
            dry_run=dry_run,
        )

        # Step 4: Generate rollback plan
        rollback_plan = await self._generate_rollback_plan(
            script=script,
            inputs=inputs,
            environment=environment,
        )

        # Determine if ready to execute
        ready_to_execute = (
            validation["valid"] and
            risk_assessment["approved"] and
            (dry_run or risk_assessment["risk_level"] != "critical")
        )

        return {
            "incident_id": incident_id,
            "execution_plan": execution_plan,
            "validation": validation,
            "rollback_plan": rollback_plan,
            "risk_assessment": risk_assessment,
            "ready_to_execute": ready_to_execute,
            "requires_approval": not risk_assessment["auto_approve"],
        }

    def _validate_inputs(
        self,
        script: Dict[str, Any],
        inputs: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Validate inputs against script requirements"""
        required_inputs = script.get("required_inputs", [])
        missing = [inp for inp in required_inputs if inp not in inputs]
        empty = [inp for inp in required_inputs if inp in inputs and not inputs[inp]]

        return {
            "valid": len(missing) == 0 and len(empty) == 0,
            "missing_inputs": missing,
            "empty_inputs": empty,
            "provided_inputs": list(inputs.keys()),
            "required_inputs": required_inputs,
        }

    def _assess_risk(
        self,
        script: Dict[str, Any],
        inputs: Dict[str, Any],
        environment: str,
    ) -> Dict[str, Any]:
        """Assess execution risk"""
        script_risk = script.get("risk_level", "medium")
        script_type = script.get("type", "unknown")

        # Risk multipliers
        env_multiplier = {
            "production": 2.0,
            "staging": 1.5,
            "development": 0.5,
            "test": 0.3,
        }.get(environment, 1.0)

        type_risk = {
            "python": 0.3,
            "bash": 0.5,
            "terraform": 0.7,
            "ansible": 0.6,
            "kubectl": 0.6,
        }.get(script_type, 0.5)

        base_risk = {
            "low": 0.2,
            "medium": 0.5,
            "high": 0.8,
            "critical": 1.0,
        }.get(script_risk, 0.5)

        # Calculate final risk score
        risk_score = min(base_risk * env_multiplier + type_risk * 0.2, 1.0)

        # Determine risk level
        if risk_score < 0.3:
            risk_level = "low"
        elif risk_score < 0.5:
            risk_level = "medium"
        elif risk_score < 0.8:
            risk_level = "high"
        else:
            risk_level = "critical"

        # Auto-approve rules
        auto_approve = (
            risk_level == "low" and
            environment != "production"
        )

        return {
            "risk_level": risk_level,
            "risk_score": risk_score,
            "factors": {
                "script_risk": script_risk,
                "environment": environment,
                "script_type": script_type,
            },
            "auto_approve": auto_approve,
            "approved": risk_level != "critical",
            "requires_senior_approval": risk_level in ["high", "critical"],
        }

    def _generate_execution_plan(
        self,
        script: Dict[str, Any],
        inputs: Dict[str, Any],
        environment: str,
        dry_run: bool,
    ) -> Dict[str, Any]:
        """Generate execution plan"""
        return {
            "script_id": script.get("script_id"),
            "script_name": script.get("name"),
            "script_type": script.get("type", "python"),
            "inputs": inputs,
            "environment": environment,
            "dry_run": dry_run,
            "execution_mode": "dry_run" if dry_run else "live",
            "timeout_seconds": script.get("timeout", 300),
            "retry_on_failure": script.get("retry", False),
            "max_retries": script.get("max_retries", 1),
        }

    async def _generate_rollback_plan(
        self,
        script: Dict[str, Any],
        inputs: Dict[str, Any],
        environment: str,
    ) -> Dict[str, Any]:
        """Generate rollback plan using LLM if available"""
        script_name = script.get("name", "unknown")
        script_type = script.get("type", "unknown")
        script_content = script.get("content", "")

        if self.llm_client and script_content:
            try:
                prompt = f"""Generate a rollback plan for this script execution.

SCRIPT:
Name: {script_name}
Type: {script_type}
Content:
{script_content[:2000]}

INPUTS:
{json.dumps(inputs, indent=2)}

ENVIRONMENT: {environment}

Generate a rollback plan as JSON:
{{
  "steps": ["step1", "step2", ...],
  "commands": ["command1", "command2", ...],
  "estimated_time": "X minutes",
  "risk_notes": "Any important notes"
}}

Return ONLY valid JSON."""

                response = self.llm_client.chat.completions.create(
                    model=self.config.llm.model,
                    messages=[
                        {
                            "role": "system",
                            "content": "You are an expert at generating rollback plans. Return only valid JSON."
                        },
                        {"role": "user", "content": prompt}
                    ],
                    temperature=self.config.llm.temperature,
                    max_tokens=1000,
                )

                content = response.choices[0].message.content.strip()
                if content.startswith("```"):
                    content = content.split("```")[1]
                    if content.startswith("json"):
                        content = content[4:]
                    content = content.strip()

                return json.loads(content)

            except Exception as e:
                logger.warning("rollback_generation_failed", error=str(e))

        # Default rollback plan
        return {
            "steps": [
                "Stop the affected service",
                "Revert configuration changes",
                "Restart service with previous configuration",
                "Verify service health",
            ],
            "commands": [],
            "estimated_time": "5-10 minutes",
            "risk_notes": "Manual verification recommended",
        }
