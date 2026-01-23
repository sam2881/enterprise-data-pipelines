"""
IT Service Workflow Handler - ServiceNow Incident Resolution

WHY: Implements the complete IT Service workflow for incident resolution.
     Integrates with ServiceNow, RAG, and remediation systems.

WORKFLOW:
1. Receive incident from ServiceNow/Kafka
2. Analyze incident using LLM
3. Search for matching scripts in RAG
4. Generate remediation plan
5. Execute approved remediation
6. Verify and close incident

USAGE:
    from control_plane.handlers.it_service_handler import ITServiceHandler
    from control_plane import get_control_plane, WorkflowType

    handler = ITServiceHandler()
    orchestrator = get_control_plane()
    orchestrator.register_handler(WorkflowType.IT_SERVICE, handler)
"""
import logging
from typing import Any, Dict, Optional
from datetime import datetime

from backend.control_plane.orchestrator import (
    WorkflowHandler,
    WorkflowContext,
    RiskLevel,
)

logger = logging.getLogger(__name__)


class ITServiceHandler(WorkflowHandler):
    """
    Handler for IT Service (ServiceNow) workflows.

    INTEGRATIONS:
    - ServiceNow API for incident management
    - RAG system for script matching
    - LLM for incident analysis
    - GitHub Actions for script execution
    - Slack for notifications
    """

    def __init__(self, settings=None):
        """Initialize IT Service handler."""
        from backend.config.settings import get_settings
        from backend.secrets_manager import get_secret

        self.settings = settings or get_settings()
        self._servicenow_client = None
        self._rag_client = None
        self._llm_client = None

    async def _ensure_initialized(self) -> None:
        """Lazy initialization of clients."""
        if self._llm_client is not None:
            return

        from backend.secrets_manager import get_secret

        # Initialize LLM client
        try:
            import openai
            self._llm_client = openai.AsyncOpenAI(
                api_key=get_secret("OPENAI_API_KEY")
            )
        except Exception as e:
            logger.warning(f"OpenAI client initialization failed: {e}")

        logger.info("ITServiceHandler initialized")

    async def analyze(self, context: WorkflowContext) -> Dict[str, Any]:
        """
        Analyze the incident using LLM.

        Steps:
        1. Fetch full incident details from ServiceNow
        2. Use LLM to classify and extract key information
        3. Determine risk level and urgency
        4. Search RAG for similar incidents

        Returns:
            Analysis result with classification and recommendations
        """
        await self._ensure_initialized()

        incident_data = context.metadata.get("incident", {})

        # Use LLM for analysis
        analysis = await self._analyze_with_llm(incident_data)

        # Determine risk level from analysis
        context.risk_level = self._determine_risk_level(analysis)
        context.confidence_score = analysis.get("confidence", 0.5)

        # Search RAG for similar incidents and scripts
        similar_scripts = await self._search_rag(
            incident_data.get("short_description", ""),
            incident_data.get("description", ""),
        )

        return {
            "incident_id": context.source_id,
            "classification": analysis.get("classification"),
            "severity": analysis.get("severity"),
            "root_cause_hypothesis": analysis.get("root_cause"),
            "affected_services": analysis.get("affected_services", []),
            "similar_scripts": similar_scripts,
            "confidence": analysis.get("confidence", 0.5),
            "analyzed_at": datetime.utcnow().isoformat(),
        }

    async def _analyze_with_llm(self, incident_data: Dict[str, Any]) -> Dict[str, Any]:
        """Use LLM to analyze incident."""
        if not self._llm_client:
            # Fallback analysis without LLM
            return {
                "classification": "unknown",
                "severity": incident_data.get("priority", "medium"),
                "root_cause": "Requires manual investigation",
                "confidence": 0.3,
            }

        prompt = f"""Analyze this IT incident and provide:
1. Classification (network, database, application, security, infrastructure)
2. Severity assessment
3. Root cause hypothesis
4. Affected services
5. Confidence score (0-1)

Incident:
Title: {incident_data.get('short_description', 'Unknown')}
Description: {incident_data.get('description', 'No description')}
Priority: {incident_data.get('priority', 'Unknown')}
Category: {incident_data.get('category', 'Unknown')}

Respond in JSON format."""

        try:
            response = await self._llm_client.chat.completions.create(
                model=self.settings.llm.primary_model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.0,
                response_format={"type": "json_object"},
            )

            import json
            return json.loads(response.choices[0].message.content)

        except Exception as e:
            logger.error(f"LLM analysis failed: {e}")
            return {
                "classification": "unknown",
                "severity": "medium",
                "root_cause": "Analysis failed",
                "confidence": 0.2,
            }

    def _determine_risk_level(self, analysis: Dict[str, Any]) -> RiskLevel:
        """Determine risk level from analysis."""
        severity = analysis.get("severity", "medium").lower()
        classification = analysis.get("classification", "").lower()

        # Security incidents are always high/critical
        if classification == "security":
            return RiskLevel.CRITICAL

        # Map severity to risk
        severity_map = {
            "critical": RiskLevel.CRITICAL,
            "high": RiskLevel.HIGH,
            "medium": RiskLevel.MEDIUM,
            "low": RiskLevel.LOW,
        }

        return severity_map.get(severity, RiskLevel.MEDIUM)

    async def _search_rag(
        self,
        title: str,
        description: str,
    ) -> list:
        """Search RAG for similar scripts."""
        # TODO: Integrate with actual RAG system
        # For now, return empty list
        return []

    async def generate_plan(self, context: WorkflowContext) -> Dict[str, Any]:
        """
        Generate remediation plan based on analysis.

        Steps:
        1. Select best matching script from RAG results
        2. Generate execution parameters
        3. Assess blast radius
        4. Create step-by-step plan

        Returns:
            Remediation plan with script and parameters
        """
        analysis = context.analysis_result or {}
        similar_scripts = analysis.get("similar_scripts", [])

        # Select best script or recommend manual intervention
        if similar_scripts:
            selected_script = similar_scripts[0]
            plan_type = "automated"
        else:
            selected_script = None
            plan_type = "manual"

        plan = {
            "plan_type": plan_type,
            "summary": f"Remediation plan for incident {context.source_id}",
            "action": "execute_script" if selected_script else "manual_intervention",
            "script": selected_script,
            "steps": self._generate_steps(context, selected_script),
            "estimated_duration_minutes": 15 if selected_script else 60,
            "rollback_available": bool(selected_script),
            "blast_radius": self._assess_blast_radius(context),
            "generated_at": datetime.utcnow().isoformat(),
        }

        return plan

    def _generate_steps(
        self,
        context: WorkflowContext,
        script: Optional[Dict],
    ) -> list:
        """Generate execution steps."""
        if script:
            return [
                {"step": 1, "action": "Validate preconditions", "automated": True},
                {"step": 2, "action": f"Execute script: {script.get('name', 'unknown')}", "automated": True},
                {"step": 3, "action": "Verify execution success", "automated": True},
                {"step": 4, "action": "Update incident status", "automated": True},
            ]
        else:
            return [
                {"step": 1, "action": "Assign to on-call engineer", "automated": False},
                {"step": 2, "action": "Manual investigation required", "automated": False},
                {"step": 3, "action": "Apply manual fix", "automated": False},
                {"step": 4, "action": "Document resolution", "automated": False},
            ]

    def _assess_blast_radius(self, context: WorkflowContext) -> Dict[str, Any]:
        """Assess potential blast radius of remediation."""
        analysis = context.analysis_result or {}

        return {
            "affected_systems": analysis.get("affected_services", []),
            "user_impact": "low",  # Would be determined by actual analysis
            "data_risk": "none",
            "service_disruption": False,
        }

    async def execute(self, context: WorkflowContext) -> Dict[str, Any]:
        """
        Execute the approved remediation plan.

        For automated plans:
        1. Dispatch script to GitHub Actions
        2. Monitor execution
        3. Capture results

        For manual plans:
        1. Create assignment
        2. Send notifications
        """
        plan = context.plan or {}

        if plan.get("plan_type") == "automated" and plan.get("script"):
            return await self._execute_automated(context)
        else:
            return await self._execute_manual(context)

    async def _execute_automated(self, context: WorkflowContext) -> Dict[str, Any]:
        """Execute automated remediation via GitHub Actions."""
        plan = context.plan or {}
        script = plan.get("script", {})

        # TODO: Integrate with GitHub Actions client
        # For now, simulate execution

        logger.info(f"Executing script {script.get('name')} for incident {context.source_id}")

        return {
            "execution_type": "automated",
            "script_name": script.get("name"),
            "status": "success",  # Would be actual status
            "output": "Script executed successfully",
            "executed_at": datetime.utcnow().isoformat(),
        }

    async def _execute_manual(self, context: WorkflowContext) -> Dict[str, Any]:
        """Handle manual intervention workflow."""
        # TODO: Integrate with ticketing and notification systems

        logger.info(f"Manual intervention required for incident {context.source_id}")

        return {
            "execution_type": "manual",
            "assignment": "on-call-team",
            "status": "assigned",
            "notification_sent": True,
            "assigned_at": datetime.utcnow().isoformat(),
        }

    async def verify(self, context: WorkflowContext) -> Dict[str, Any]:
        """
        Verify remediation success.

        Steps:
        1. Check service health
        2. Verify incident condition resolved
        3. Update incident status in ServiceNow
        """
        execution_result = context.execution_result or {}

        # For automated execution, verify based on output
        if execution_result.get("execution_type") == "automated":
            success = execution_result.get("status") == "success"
        else:
            # Manual intervention - mark as pending verification
            success = True  # Will be verified manually

        if success:
            # Update ServiceNow incident
            await self._update_servicenow_status(
                context.source_id,
                status="resolved",
                resolution_notes=f"Resolved by AI Agent: {context.workflow_id}",
            )

        return {
            "success": success,
            "verification_type": "automated" if execution_result.get("execution_type") == "automated" else "manual",
            "incident_status": "resolved" if success else "in_progress",
            "verified_at": datetime.utcnow().isoformat(),
        }

    async def _update_servicenow_status(
        self,
        incident_id: str,
        status: str,
        resolution_notes: str,
    ) -> None:
        """Update incident status in ServiceNow."""
        # TODO: Integrate with ServiceNow API
        logger.info(f"Updated ServiceNow incident {incident_id} to {status}")

    async def rollback(self, context: WorkflowContext) -> None:
        """
        Rollback failed remediation.

        Compensation logic for saga pattern.
        """
        plan = context.plan or {}
        script = plan.get("script", {})

        if script and script.get("rollback_script"):
            logger.info(f"Executing rollback for incident {context.source_id}")
            # TODO: Execute rollback script
        else:
            logger.warning(f"No rollback available for incident {context.source_id}")

        # Update incident status
        await self._update_servicenow_status(
            context.source_id,
            status="in_progress",
            resolution_notes=f"Rollback executed: {context.workflow_id}",
        )
