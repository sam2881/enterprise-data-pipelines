"""
LLM-Based Intelligence for Incident Resolution
Uses OpenAI to understand incidents and match remediation scripts

Features:
- LangFuse integration for observability
- Guardrails for input/output validation
- Structured JSON responses
- Token usage tracking

Author: AI Agent Platform
Version: 4.0.0
"""
import os
import json
import time
from typing import Dict, List, Any, Optional
from openai import OpenAI
import structlog

# Import guardrails
try:
    from guardrails.llm_guardrails import guardrails
    GUARDRAILS_ENABLED = True
except ImportError:
    GUARDRAILS_ENABLED = False

# Import audit logger for EU AI Act compliance
try:
    from governance.audit_logger import audit_logger, AuditEventType, RiskLevel
    AUDIT_ENABLED = True
except ImportError:
    AUDIT_ENABLED = False

# Import LangFuse for observability
try:
    from langfuse import Langfuse
    from langfuse.decorators import observe, langfuse_context
    LANGFUSE_ENABLED = True
except ImportError:
    LANGFUSE_ENABLED = False
    # Create dummy decorator if LangFuse not available
    def observe(*args, **kwargs):
        def decorator(func):
            return func
        return decorator

logger = structlog.get_logger()

# Initialize OpenAI client
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# Initialize LangFuse client
langfuse_client = None
if LANGFUSE_ENABLED:
    try:
        langfuse_client = Langfuse(
            public_key=os.getenv("LANGFUSE_PUBLIC_KEY"),
            secret_key=os.getenv("LANGFUSE_SECRET_KEY"),
            host=os.getenv("LANGFUSE_HOST", "https://cloud.langfuse.com"),
            flush_at=1,  # Flush after each trace for real-time visibility
            flush_interval=1  # Flush every 1 second
        )
        logger.info("langfuse_initialized", host=os.getenv("LANGFUSE_HOST", "cloud"))
    except Exception as e:
        logger.warning("langfuse_init_failed", error=str(e))
        LANGFUSE_ENABLED = False


def _track_llm_call(
    name: str,
    model: str,
    input_text: str,
    output_text: str,
    metadata: Dict = None,
    trace_id: str = None,
    prompt_tokens: int = 0,
    completion_tokens: int = 0,
    duration_ms: float = 0
):
    """Track LLM call in LangFuse with proper trace structure"""
    if not LANGFUSE_ENABLED or not langfuse_client:
        return

    try:
        # Create trace for the incident
        trace = langfuse_client.trace(
            name=f"incident_{trace_id}" if trace_id else f"llm_{name}",
            user_id="ai-agent-platform",
            metadata={
                "incident_id": trace_id,
                "operation": name,
                **(metadata or {})
            },
            tags=["ai-agent", name, model]
        )

        # Create generation span with proper token usage
        generation = trace.generation(
            name=name,
            model=model,
            model_parameters={
                "temperature": 0.2,
                "response_format": "json"
            },
            input=[
                {"role": "system", "content": "AI Agent Platform SRE Assistant"},
                {"role": "user", "content": input_text[:5000]}  # Truncate for display
            ],
            output=output_text[:5000] if output_text else None,  # Truncate for display
            usage={
                "input": prompt_tokens,
                "output": completion_tokens,
                "total": prompt_tokens + completion_tokens,
                "unit": "TOKENS"
            },
            metadata={
                "duration_ms": duration_ms,
                "incident_id": trace_id,
                **(metadata or {})
            }
        )

        # End the generation
        generation.end()

        # Flush to ensure data is sent
        langfuse_client.flush()

        logger.debug(
            "langfuse_trace_sent",
            name=name,
            trace_id=trace_id,
            tokens=prompt_tokens + completion_tokens
        )

    except Exception as e:
        logger.warning("langfuse_tracking_failed", error=str(e), name=name)

# =============================================================================
# LLM-Based Incident Analysis
# =============================================================================

def analyze_incident_with_llm(incident: Dict[str, Any]) -> Dict[str, Any]:
    """
    Use LLM to deeply analyze an incident and extract:
    - Root cause
    - Affected components
    - Severity level
    - Recommended remediation approach
    - Required parameters

    Includes guardrails validation for input and output.
    """
    try:
        # Guardrails: Validate incident input
        if GUARDRAILS_ENABLED:
            input_result = guardrails.validate_incident(incident)
            if not input_result.passed:
                logger.warning(
                    "incident_validation_failed",
                    incident_id=incident.get('incident_id'),
                    issues=input_result.issues
                )
                return {
                    "root_cause": "Input validation failed",
                    "affected_components": [],
                    "service_type": "unknown",
                    "severity": "medium",
                    "remediation_approach": "investigate",
                    "required_params": {},
                    "risk_level": "medium",
                    "confidence": 0.0,
                    "guardrail_issues": input_result.issues
                }

        prompt = f"""You are an expert SRE analyzing a production incident. Analyze this incident and provide structured insights:

INCIDENT:
ID: {incident.get('incident_id')}
Title: {incident.get('short_description')}
Description: {incident.get('description')}
Category: {incident.get('category', 'unknown')}
Priority: {incident.get('priority', 'unknown')}

TASK: Analyze this incident and provide:
1. Root cause analysis
2. Affected services/components
3. Required remediation actions
4. Technical parameters needed for remediation
5. Risk assessment

Respond in JSON format:
{{
  "root_cause": "...",
  "affected_components": ["component1", "component2"],
  "service_type": "gcp|kubernetes|database|network|application",
  "severity": "critical|high|medium|low",
  "remediation_approach": "restart|scale|repair|rollback|configure",
  "required_params": {{
    "instance_name": "extracted value or null",
    "zone": "extracted value or null",
    "namespace": "extracted value or null",
    "pod_name": "extracted value or null"
  }},
  "risk_level": "low|medium|high|critical",
  "confidence": 0.0-1.0
}}
"""

        start_time = time.time()
        model = "gpt-4-turbo-preview"

        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "You are an expert SRE with deep knowledge of cloud infrastructure, Kubernetes, databases, and incident remediation. Respond only with valid JSON."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.2,
            response_format={"type": "json_object"}
        )

        output_content = response.choices[0].message.content
        duration = time.time() - start_time

        # Track in LangFuse
        _track_llm_call(
            name="analyze_incident",
            model=model,
            input_text=prompt,
            output_text=output_content,
            metadata={
                "incident_id": incident.get('incident_id'),
                "category": incident.get('category', 'unknown')
            },
            trace_id=incident.get('incident_id'),
            prompt_tokens=response.usage.prompt_tokens if response.usage else 0,
            completion_tokens=response.usage.completion_tokens if response.usage else 0,
            duration_ms=duration * 1000
        )

        # Guardrails: Validate LLM output
        if GUARDRAILS_ENABLED:
            output_result = guardrails.validate_output(output_content, expected_format="json")
            if not output_result.passed:
                logger.warning(
                    "output_validation_failed",
                    incident_id=incident.get('incident_id'),
                    issues=output_result.issues
                )
                # Still try to parse but log the issues
                logger.info("output_validation_issues", issues=output_result.issues)

        analysis = json.loads(output_content)
        logger.info(
            "llm_analysis_completed",
            incident_id=incident.get('incident_id'),
            duration=f"{duration:.2f}s",
            tokens=response.usage.total_tokens if response.usage else 0
        )

        # EU AI Act Article 12: Log AI decision for audit trail
        if AUDIT_ENABLED:
            audit_logger.log_ai_decision(
                decision="analyze_incident",
                incident_id=incident.get('incident_id', 'unknown'),
                confidence=analysis.get('confidence', 0.5),
                explanation=analysis.get('root_cause', 'No explanation'),
                risk_level=RiskLevel.MEDIUM if analysis.get('risk_level') == 'medium' else RiskLevel.HIGH,
                human_oversight=False
            )

        return analysis

    except Exception as e:
        logger.error("llm_analysis_failed", error=str(e))
        # Return fallback analysis
        return {
            "root_cause": "Analysis failed - using fallback",
            "affected_components": [],
            "service_type": "unknown",
            "severity": "medium",
            "remediation_approach": "investigate",
            "required_params": {},
            "risk_level": "medium",
            "confidence": 0.3
        }


def match_scripts_with_llm(
    incident: Dict[str, Any],
    available_scripts: List[Dict[str, Any]],
    incident_analysis: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Use LLM to intelligently match incident to remediation scripts
    """
    try:
        # Prepare script summaries for LLM
        scripts_info = []
        for script in available_scripts:
            scripts_info.append({
                "script_id": script.get("script_id"),
                "name": script.get("name"),
                "description": script.get("description"),
                "type": script.get("type"),
                "service": script.get("service"),
                "action": script.get("action"),
                "keywords": script.get("keywords", []),
                "risk_level": script.get("risk_level", "medium")
            })

        prompt = f"""You are an expert SRE matching incidents to remediation scripts.

INCIDENT ANALYSIS:
{json.dumps(incident_analysis, indent=2)}

INCIDENT DETAILS:
ID: {incident.get('incident_id')}
Title: {incident.get('short_description')}
Description: {incident.get('description')}

AVAILABLE REMEDIATION SCRIPTS:
{json.dumps(scripts_info, indent=2)}

TASK: Match the BEST remediation scripts for this incident. Consider:
1. Root cause alignment
2. Service type match
3. Remediation approach compatibility
4. Risk level appropriateness
5. Parameter availability

Rank the top 5 scripts by relevance and provide confidence scores.

Respond in JSON format:
{{
  "matches": [
    {{
      "script_id": "...",
      "confidence": 0.0-1.0,
      "reasoning": "why this script matches",
      "extracted_params": {{
        "param_name": "extracted_value"
      }},
      "risk_assessment": "low|medium|high|critical",
      "recommended": true|false
    }}
  ]
}}
"""

        start_time = time.time()
        model = "gpt-4-turbo-preview"

        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "You are an expert SRE with deep knowledge of incident remediation. Match incidents to remediation scripts with high precision. Respond only with valid JSON."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
            response_format={"type": "json_object"}
        )

        output_content = response.choices[0].message.content
        duration = time.time() - start_time

        # Track in LangFuse
        _track_llm_call(
            name="match_scripts",
            model=model,
            input_text=prompt,
            output_text=output_content,
            metadata={
                "incident_id": incident.get('incident_id'),
                "available_scripts": len(available_scripts)
            },
            trace_id=incident.get('incident_id'),
            prompt_tokens=response.usage.prompt_tokens if response.usage else 0,
            completion_tokens=response.usage.completion_tokens if response.usage else 0,
            duration_ms=duration * 1000
        )

        result = json.loads(output_content)
        matches = result.get("matches", [])

        logger.info(
            "llm_matching_completed",
            incident_id=incident.get('incident_id'),
            match_count=len(matches),
            duration=f"{duration:.2f}s"
        )

        return matches

    except Exception as e:
        logger.error("llm_matching_failed", error=str(e))
        return []


def generate_execution_plan_with_llm(
    incident: Dict[str, Any],
    script: Dict[str, Any],
    extracted_params: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Use LLM to generate detailed execution plan
    """
    try:
        prompt = f"""You are an expert SRE creating an execution plan for incident remediation.

INCIDENT:
{json.dumps(incident, indent=2)}

SELECTED SCRIPT:
{json.dumps(script, indent=2)}

EXTRACTED PARAMETERS:
{json.dumps(extracted_params, indent=2)}

TASK: Create a detailed execution plan including:
1. Pre-execution validation steps
2. Execution steps
3. Post-execution verification
4. Rollback plan if needed
5. Risk mitigation strategies

Respond in JSON format:
{{
  "plan_id": "unique_id",
  "pre_checks": [
    {{"step": "...", "expected": "...", "validation": "..."}}
  ],
  "execution_steps": [
    {{"step": "...", "command": "...", "expected_outcome": "..."}}
  ],
  "verification_steps": [
    {{"step": "...", "check": "...", "success_criteria": "..."}}
  ],
  "rollback_plan": [
    {{"step": "...", "action": "..."}}
  ],
  "estimated_duration": "5-10 minutes",
  "risk_level": "low|medium|high|critical",
  "approval_required": true|false,
  "safety_score": 0.0-1.0
}}
"""

        start_time = time.time()
        model = "gpt-4-turbo-preview"

        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "You are an expert SRE creating detailed, safe execution plans. Respond only with valid JSON."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.2,
            response_format={"type": "json_object"}
        )

        output_content = response.choices[0].message.content
        duration = time.time() - start_time

        # Track in LangFuse
        _track_llm_call(
            name="generate_execution_plan",
            model=model,
            input_text=prompt,
            output_text=output_content,
            metadata={
                "incident_id": incident.get('incident_id'),
                "script_id": script.get('id')
            },
            trace_id=incident.get('incident_id'),
            prompt_tokens=response.usage.prompt_tokens if response.usage else 0,
            completion_tokens=response.usage.completion_tokens if response.usage else 0,
            duration_ms=duration * 1000
        )

        plan = json.loads(output_content)
        logger.info("execution_plan_generated", plan_id=plan.get("plan_id"), duration=f"{duration:.2f}s")
        return plan

    except Exception as e:
        logger.error("execution_plan_generation_failed", error=str(e))
        return {
            "plan_id": "fallback_plan",
            "pre_checks": [],
            "execution_steps": [{"step": "Execute script", "command": script.get("github_workflow"), "expected_outcome": "Success"}],
            "verification_steps": [],
            "rollback_plan": [],
            "estimated_duration": "unknown",
            "risk_level": "medium",
            "approval_required": True,
            "safety_score": 0.5
        }


def validate_plan_safety_with_llm(
    incident: Dict[str, Any],
    plan: Dict[str, Any],
    script: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Use LLM to validate execution plan safety
    """
    try:
        prompt = f"""You are a senior SRE reviewing an execution plan for safety.

INCIDENT:
{json.dumps(incident, indent=2)}

EXECUTION PLAN:
{json.dumps(plan, indent=2)}

SCRIPT:
{json.dumps(script, indent=2)}

TASK: Perform a safety review and determine:
1. Is this plan safe to execute?
2. What are the potential risks?
3. Are there any missing safeguards?
4. Should this require human approval?
5. Any recommendations for improvement?

Respond in JSON format:
{{
  "safe_to_execute": true|false,
  "safety_score": 0.0-1.0,
  "risks": ["risk1", "risk2"],
  "missing_safeguards": ["safeguard1"],
  "requires_approval": true|false,
  "approval_reason": "...",
  "recommendations": ["rec1", "rec2"],
  "final_decision": "approve|reject|needs_review"
}}
"""

        start_time = time.time()
        model = "gpt-4-turbo-preview"

        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "You are a senior SRE performing safety reviews. Be conservative and prioritize safety. Respond only with valid JSON."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,
            response_format={"type": "json_object"}
        )

        output_content = response.choices[0].message.content
        duration = time.time() - start_time

        # Track in LangFuse
        _track_llm_call(
            name="validate_plan_safety",
            model=model,
            input_text=prompt,
            output_text=output_content,
            metadata={
                "incident_id": incident.get('incident_id'),
                "plan_id": plan.get('plan_id')
            },
            trace_id=incident.get('incident_id'),
            prompt_tokens=response.usage.prompt_tokens if response.usage else 0,
            completion_tokens=response.usage.completion_tokens if response.usage else 0,
            duration_ms=duration * 1000
        )

        validation = json.loads(output_content)
        logger.info(
            "safety_validation_completed",
            decision=validation.get("final_decision"),
            safety_score=validation.get("safety_score"),
            duration=f"{duration:.2f}s"
        )
        return validation

    except Exception as e:
        logger.error("safety_validation_failed", error=str(e))
        return {
            "safe_to_execute": False,
            "safety_score": 0.5,
            "risks": ["Validation failed"],
            "missing_safeguards": [],
            "requires_approval": True,
            "approval_reason": "Safety validation failed - manual review required",
            "recommendations": [],
            "final_decision": "needs_review"
        }


def extract_parameters_with_llm(
    incident: Dict[str, Any],
    required_params: List[str]
) -> Dict[str, Any]:
    """
    Use LLM to extract required parameters from incident description
    """
    try:
        prompt = f"""You are an expert at extracting technical parameters from incident descriptions.

INCIDENT:
Title: {incident.get('short_description')}
Description: {incident.get('description')}

REQUIRED PARAMETERS:
{json.dumps(required_params)}

TASK: Extract the values for these parameters from the incident description.
Look for:
- instance_name: GCP VM instance names (e.g., test-vm-01, my-instance)
- zone: GCP zones (e.g., us-central1-a, us-east1-b)
- namespace: Kubernetes namespaces
- pod_name: Kubernetes pod names
- deployment_name: Kubernetes deployment names
- project: GCP project ID

Be precise and extract exact values mentioned in the text.

Respond in JSON format:
{{
  "extracted": {{
    "param_name": "exact_value" or null
  }},
  "confidence": 0.0-1.0
}}
"""

        start_time = time.time()
        model = "gpt-4-turbo-preview"

        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "You are an expert at extracting technical parameters. Be precise and only extract values explicitly mentioned. Respond only with valid JSON."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,
            response_format={"type": "json_object"}
        )

        output_content = response.choices[0].message.content
        duration = time.time() - start_time

        # Track in LangFuse
        _track_llm_call(
            name="extract_parameters",
            model=model,
            input_text=prompt,
            output_text=output_content,
            metadata={
                "incident_id": incident.get('incident_id'),
                "required_params": required_params
            },
            trace_id=incident.get('incident_id'),
            prompt_tokens=response.usage.prompt_tokens if response.usage else 0,
            completion_tokens=response.usage.completion_tokens if response.usage else 0,
            duration_ms=duration * 1000
        )

        result = json.loads(output_content)
        extracted = result.get("extracted", {})

        logger.info(
            "parameters_extracted",
            incident_id=incident.get('incident_id'),
            extracted=extracted,
            duration=f"{duration:.2f}s"
        )

        return extracted

    except Exception as e:
        logger.error("parameter_extraction_failed", error=str(e))
        return {}


class LLMIntelligence:
    """
    Class wrapper for LLM-based intelligence functions.

    WHY: Provides a clean interface for the LangGraph workflow to use LLM capabilities
    for incident analysis, script matching, and execution planning.

    HOW: Wraps the existing function-based implementations in a class that can be
    instantiated once and reused across workflow executions.
    """

    def __init__(self):
        """Initialize LLM Intelligence with OpenAI client"""
        self.client = client
        self.langfuse_enabled = LANGFUSE_ENABLED
        self.guardrails_enabled = GUARDRAILS_ENABLED
        logger.info("llm_intelligence_initialized",
                   langfuse=self.langfuse_enabled,
                   guardrails=self.guardrails_enabled)

    def analyze_incident(self, incident: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyze an incident using LLM to extract key information.

        Args:
            incident: Incident data dictionary

        Returns:
            Analysis results including category, severity, affected systems
        """
        return analyze_incident_with_llm(incident)

    def match_scripts(
        self,
        incident: Dict[str, Any],
        available_scripts: List[Dict[str, Any]],
        rag_results: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Match remediation scripts to an incident using LLM reasoning.

        Args:
            incident: Incident data
            available_scripts: List of available remediation scripts
            rag_results: Optional RAG search results for context

        Returns:
            Matching results with confidence scores
        """
        return match_scripts_with_llm(incident, available_scripts, rag_results)

    def generate_execution_plan(
        self,
        incident: Dict[str, Any],
        matched_scripts: List[Dict[str, Any]],
        context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Generate an execution plan for remediation.

        Args:
            incident: Incident data
            matched_scripts: Scripts matched for this incident
            context: Additional context (graph data, similar incidents)

        Returns:
            Execution plan with steps and safety checks
        """
        return generate_execution_plan_with_llm(incident, matched_scripts, context)

    def validate_plan_safety(
        self,
        plan: Dict[str, Any],
        incident: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Validate that an execution plan is safe to run.

        Args:
            plan: The execution plan to validate
            incident: Original incident data

        Returns:
            Safety validation results
        """
        return validate_plan_safety_with_llm(plan, incident)

    def extract_parameters(
        self,
        incident: Dict[str, Any],
        required_params: List[str]
    ) -> Dict[str, Any]:
        """
        Extract required parameters from incident description.

        Args:
            incident: Incident data
            required_params: List of parameter names to extract

        Returns:
            Extracted parameter values
        """
        return extract_parameters_with_llm(incident, required_params)
