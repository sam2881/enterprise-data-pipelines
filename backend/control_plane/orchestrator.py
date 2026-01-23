"""
Control Plane Orchestrator - Central Workflow Coordination

WHY: Provides unified orchestration for all agent workflows.
     Handles approvals, policy enforcement, and state management.
     Single point of control for both IT Service and Data workflows.

PATTERN: Saga pattern with compensation logic.
         Event-driven state machine for workflow progression.

RESPONSIBILITIES:
- Workflow state management
- Approval routing (AUTO_APPROVE, HITL, SENIOR, DENY)
- Policy enforcement
- Audit logging
- Event publishing

USAGE:
    from control_plane.orchestrator import ControlPlaneOrchestrator

    orchestrator = ControlPlaneOrchestrator()

    # Start a workflow
    workflow_id = await orchestrator.start_workflow(
        workflow_type=WorkflowType.IT_SERVICE,
        context=incident_context
    )

    # Check status
    status = await orchestrator.get_workflow_status(workflow_id)
"""
import asyncio
import logging
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field, asdict
from enum import Enum
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


# =============================================================================
# Workflow Types and States
# =============================================================================

class WorkflowType(str, Enum):
    """Types of workflows the control plane manages."""
    IT_SERVICE = "it_service"      # ServiceNow incident resolution
    DATA_PIPELINE = "data_pipeline"  # Jira-driven pipeline generation
    INFRASTRUCTURE = "infrastructure"  # GCP resource management


class WorkflowState(str, Enum):
    """Unified workflow states across all workflow types."""
    # Initial states
    NEW = "new"
    RECEIVED = "received"

    # Analysis states
    ANALYZING = "analyzing"
    ENRICHED = "enriched"

    # Judgment states
    JUDGING = "judging"
    JUDGED = "judged"

    # Planning states
    PLANNING = "planning"
    PLAN_GENERATED = "plan_generated"

    # Approval states
    PENDING_APPROVAL = "pending_approval"
    APPROVED = "approved"
    REJECTED = "rejected"

    # Execution states
    EXECUTING = "executing"
    EXECUTED = "executed"

    # Verification states
    VERIFYING = "verifying"
    VERIFIED = "verified"

    # Terminal states
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ApprovalRoute(str, Enum):
    """
    Approval routing decisions for workflow orchestrator.

    NOTE: This is a simplified version for workflow-level routing.
          policy_engine.ApprovalRoute has additional values (ASYNC_APPROVE, REJECT)
          for more granular plan-level approval decisions.
          Values are compatible - both use the same string values for common routes.
    """
    AUTO_APPROVE = "auto_approve"  # Low risk, high confidence
    HITL = "hitl"                  # Human-in-the-loop required
    SENIOR = "senior"              # Senior approval required
    DENY = "deny"                  # Auto-denied based on policy


# Import RiskLevel from policy_engine to avoid duplication
# (Re-exported here for backward compatibility)
from .policy_engine import RiskLevel


# =============================================================================
# Data Classes
# =============================================================================

@dataclass
class WorkflowContext:
    """Context data passed through workflow execution."""
    workflow_id: str
    workflow_type: WorkflowType
    source_id: str  # Incident ID, Jira ticket, etc.
    source_system: str  # servicenow, jira, gcp
    created_at: datetime = field(default_factory=datetime.utcnow)
    created_by: str = "system"

    # Dynamic context
    metadata: Dict[str, Any] = field(default_factory=dict)
    analysis_result: Optional[Dict[str, Any]] = None
    plan: Optional[Dict[str, Any]] = None
    execution_result: Optional[Dict[str, Any]] = None

    # Risk assessment
    risk_level: RiskLevel = RiskLevel.LOW
    confidence_score: float = 0.0

    # Audit trail
    state_history: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class ApprovalRequest:
    """Request for workflow approval."""
    workflow_id: str
    approval_route: ApprovalRoute
    risk_level: RiskLevel
    confidence_score: float
    plan_summary: str
    requested_at: datetime = field(default_factory=datetime.utcnow)
    requested_by: str = "system"
    approved_by: Optional[str] = None
    approved_at: Optional[datetime] = None
    rejection_reason: Optional[str] = None


@dataclass
class PolicyDecision:
    """Result of policy evaluation."""
    allowed: bool
    approval_route: ApprovalRoute
    reasons: List[str] = field(default_factory=list)
    required_approvers: List[str] = field(default_factory=list)


# =============================================================================
# Workflow Policy Engine (lightweight, for orchestrator decisions)
# =============================================================================

class WorkflowPolicyEngine:
    """
    Lightweight policy evaluation engine for workflow routing decisions.

    NOTE: This is distinct from backend.control_plane.policy_engine.PolicyEngine
          which provides comprehensive plan-level policy evaluation with judge scores.
          This class provides simpler workflow-level routing based on risk and confidence.

    POLICIES:
    - Risk-based approval routing
    - Confidence threshold enforcement
    - Action-specific restrictions
    - Time-based constraints
    """

    def __init__(self, settings=None):
        """Initialize with optional settings override."""
        from backend.config.settings import get_settings
        self.settings = settings or get_settings()
        self.thresholds = self.settings.orchestration

    def evaluate(
        self,
        context: WorkflowContext,
        action: str,
    ) -> PolicyDecision:
        """
        Evaluate policies for a workflow action.

        Args:
            context: Workflow context
            action: Proposed action (e.g., "restart_vm", "deploy_dag")

        Returns:
            Policy decision with approval route
        """
        reasons = []
        required_approvers = []

        # Rule 1: Auto-reject low confidence
        if context.confidence_score < self.thresholds.auto_reject_threshold:
            return PolicyDecision(
                allowed=False,
                approval_route=ApprovalRoute.DENY,
                reasons=[f"Confidence {context.confidence_score:.2f} below threshold {self.thresholds.auto_reject_threshold}"],
            )

        # Rule 2: Critical risk requires senior approval
        if context.risk_level == RiskLevel.CRITICAL:
            if self.thresholds.require_approval_for_critical:
                return PolicyDecision(
                    allowed=True,
                    approval_route=ApprovalRoute.SENIOR,
                    reasons=["Critical risk level requires senior approval"],
                    required_approvers=["senior-ops@company.com"],
                )

        # Rule 3: High risk requires HITL
        if context.risk_level == RiskLevel.HIGH:
            return PolicyDecision(
                allowed=True,
                approval_route=ApprovalRoute.HITL,
                reasons=["High risk level requires human approval"],
            )

        # Rule 4: Medium risk with low confidence requires HITL
        if context.risk_level == RiskLevel.MEDIUM:
            if context.confidence_score < self.thresholds.high_confidence_threshold:
                return PolicyDecision(
                    allowed=True,
                    approval_route=ApprovalRoute.HITL,
                    reasons=["Medium risk with moderate confidence requires review"],
                )

        # Rule 5: Auto-approve low risk with high confidence
        if context.risk_level == RiskLevel.LOW:
            if context.confidence_score >= self.thresholds.high_confidence_threshold:
                return PolicyDecision(
                    allowed=True,
                    approval_route=ApprovalRoute.AUTO_APPROVE,
                    reasons=["Low risk with high confidence - auto-approved"],
                )

        # Default: HITL approval
        return PolicyDecision(
            allowed=True,
            approval_route=ApprovalRoute.HITL,
            reasons=["Default policy requires human review"],
        )


# =============================================================================
# Workflow Handler Interface
# =============================================================================

class WorkflowHandler(ABC):
    """
    Abstract base class for workflow type handlers.

    Each workflow type (IT Service, Data Pipeline, Infrastructure)
    implements this interface with type-specific logic.
    """

    @abstractmethod
    async def analyze(self, context: WorkflowContext) -> Dict[str, Any]:
        """Analyze the incoming request."""
        pass

    @abstractmethod
    async def generate_plan(self, context: WorkflowContext) -> Dict[str, Any]:
        """Generate execution plan."""
        pass

    @abstractmethod
    async def execute(self, context: WorkflowContext) -> Dict[str, Any]:
        """Execute the approved plan."""
        pass

    @abstractmethod
    async def verify(self, context: WorkflowContext) -> Dict[str, Any]:
        """Verify execution results."""
        pass

    @abstractmethod
    async def rollback(self, context: WorkflowContext) -> None:
        """Rollback on failure (compensation)."""
        pass


# =============================================================================
# Control Plane Orchestrator
# =============================================================================

class ControlPlaneOrchestrator:
    """
    Central orchestrator for all agent workflows.

    RESPONSIBILITIES:
    1. Workflow lifecycle management
    2. State transitions and persistence
    3. Policy enforcement via WorkflowPolicyEngine
    4. Approval routing and tracking
    5. Event publishing to Kafka
    6. Audit logging

    SUPPORTED WORKFLOWS:
    - IT Service: ServiceNow incident → diagnosis → remediation
    - Data Pipeline: Jira ticket → analysis → code generation → deployment
    - Infrastructure: Alert → assessment → action
    """

    def __init__(self, settings=None):
        """Initialize the orchestrator."""
        from backend.config.settings import get_settings
        self.settings = settings or get_settings()

        self.policy_engine = WorkflowPolicyEngine(settings)
        self._handlers: Dict[WorkflowType, WorkflowHandler] = {}
        self._workflows: Dict[str, WorkflowContext] = {}  # In-memory for now
        self._pending_approvals: Dict[str, ApprovalRequest] = {}

        # Event callbacks
        self._event_callbacks: List[Callable] = []

        logger.info("ControlPlaneOrchestrator initialized")

    def register_handler(
        self,
        workflow_type: WorkflowType,
        handler: WorkflowHandler,
    ) -> None:
        """Register a handler for a workflow type."""
        self._handlers[workflow_type] = handler
        logger.info(f"Registered handler for {workflow_type}")

    def add_event_callback(self, callback: Callable) -> None:
        """Add callback for workflow events (e.g., Kafka publishing)."""
        self._event_callbacks.append(callback)

    async def _emit_event(
        self,
        event_type: str,
        workflow_id: str,
        data: Dict[str, Any],
    ) -> None:
        """Emit workflow event to all registered callbacks."""
        event = {
            "event_type": event_type,
            "workflow_id": workflow_id,
            "timestamp": datetime.utcnow().isoformat(),
            "data": data,
        }

        for callback in self._event_callbacks:
            try:
                await callback(event)
            except Exception as e:
                logger.error(f"Event callback failed: {e}")

    async def start_workflow(
        self,
        workflow_type: WorkflowType,
        source_id: str,
        source_system: str,
        metadata: Optional[Dict[str, Any]] = None,
        created_by: str = "system",
    ) -> str:
        """
        Start a new workflow.

        Args:
            workflow_type: Type of workflow
            source_id: ID from source system (incident ID, Jira key, etc.)
            source_system: Source system name
            metadata: Initial context metadata
            created_by: User/system initiating the workflow

        Returns:
            Workflow ID
        """
        workflow_id = f"{workflow_type.value}-{uuid.uuid4().hex[:8]}"

        context = WorkflowContext(
            workflow_id=workflow_id,
            workflow_type=workflow_type,
            source_id=source_id,
            source_system=source_system,
            created_by=created_by,
            metadata=metadata or {},
        )

        self._workflows[workflow_id] = context
        await self._transition_state(context, WorkflowState.NEW)

        logger.info(f"Started workflow {workflow_id} for {source_id}")
        await self._emit_event("workflow.started", workflow_id, {
            "workflow_type": workflow_type.value,
            "source_id": source_id,
        })

        return workflow_id

    async def _transition_state(
        self,
        context: WorkflowContext,
        new_state: WorkflowState,
        reason: str = "",
    ) -> None:
        """Record state transition in audit trail."""
        old_state = context.state_history[-1]["state"] if context.state_history else "none"

        context.state_history.append({
            "from_state": old_state,
            "to_state": new_state.value,
            "timestamp": datetime.utcnow().isoformat(),
            "reason": reason,
        })

        logger.info(f"Workflow {context.workflow_id}: {old_state} → {new_state.value}")

    async def process_workflow(self, workflow_id: str) -> Dict[str, Any]:
        """
        Process a workflow through its lifecycle.

        This is the main workflow execution method that drives the workflow
        through analysis, planning, approval, execution, and verification.

        Args:
            workflow_id: ID of workflow to process

        Returns:
            Final workflow result
        """
        context = self._workflows.get(workflow_id)
        if not context:
            raise ValueError(f"Workflow {workflow_id} not found")

        handler = self._handlers.get(context.workflow_type)
        if not handler:
            raise ValueError(f"No handler for workflow type {context.workflow_type}")

        try:
            # Step 1: Analyze
            await self._transition_state(context, WorkflowState.ANALYZING)
            context.analysis_result = await handler.analyze(context)
            await self._transition_state(context, WorkflowState.ENRICHED)

            await self._emit_event("workflow.analyzed", workflow_id, {
                "analysis": context.analysis_result,
            })

            # Step 2: Generate Plan
            await self._transition_state(context, WorkflowState.PLANNING)
            context.plan = await handler.generate_plan(context)
            await self._transition_state(context, WorkflowState.PLAN_GENERATED)

            await self._emit_event("workflow.planned", workflow_id, {
                "plan": context.plan,
            })

            # Step 3: Evaluate Policy
            policy_decision = self.policy_engine.evaluate(
                context,
                action=context.plan.get("action", "unknown"),
            )

            if not policy_decision.allowed:
                await self._transition_state(
                    context,
                    WorkflowState.REJECTED,
                    reason="; ".join(policy_decision.reasons),
                )
                return {"status": "rejected", "reasons": policy_decision.reasons}

            # Step 4: Handle Approval
            if policy_decision.approval_route != ApprovalRoute.AUTO_APPROVE:
                await self._transition_state(context, WorkflowState.PENDING_APPROVAL)

                approval = ApprovalRequest(
                    workflow_id=workflow_id,
                    approval_route=policy_decision.approval_route,
                    risk_level=context.risk_level,
                    confidence_score=context.confidence_score,
                    plan_summary=context.plan.get("summary", ""),
                )
                self._pending_approvals[workflow_id] = approval

                await self._emit_event("workflow.approval_required", workflow_id, {
                    "approval_route": policy_decision.approval_route.value,
                    "risk_level": context.risk_level.value,
                })

                return {
                    "status": "pending_approval",
                    "approval_route": policy_decision.approval_route.value,
                    "workflow_id": workflow_id,
                }

            # Step 5: Execute (auto-approved)
            await self._transition_state(context, WorkflowState.APPROVED)
            return await self._execute_workflow(context, handler)

        except Exception as e:
            logger.error(f"Workflow {workflow_id} failed: {e}")
            await self._transition_state(
                context,
                WorkflowState.FAILED,
                reason=str(e),
            )

            # Attempt rollback
            try:
                await handler.rollback(context)
            except Exception as rollback_error:
                logger.error(f"Rollback failed: {rollback_error}")

            await self._emit_event("workflow.failed", workflow_id, {
                "error": str(e),
            })

            return {"status": "failed", "error": str(e)}

    async def _execute_workflow(
        self,
        context: WorkflowContext,
        handler: WorkflowHandler,
    ) -> Dict[str, Any]:
        """Execute an approved workflow."""
        await self._transition_state(context, WorkflowState.EXECUTING)

        context.execution_result = await handler.execute(context)

        await self._transition_state(context, WorkflowState.EXECUTED)
        await self._emit_event("workflow.executed", context.workflow_id, {
            "result": context.execution_result,
        })

        # Step 6: Verify
        await self._transition_state(context, WorkflowState.VERIFYING)
        verification = await handler.verify(context)

        if verification.get("success", False):
            await self._transition_state(context, WorkflowState.COMPLETED)
            await self._emit_event("workflow.completed", context.workflow_id, {
                "verification": verification,
            })
            return {"status": "completed", "result": context.execution_result}
        else:
            await self._transition_state(
                context,
                WorkflowState.FAILED,
                reason="Verification failed",
            )
            return {"status": "verification_failed", "verification": verification}

    async def approve_workflow(
        self,
        workflow_id: str,
        approved_by: str,
    ) -> Dict[str, Any]:
        """Approve a pending workflow."""
        approval = self._pending_approvals.get(workflow_id)
        if not approval:
            raise ValueError(f"No pending approval for workflow {workflow_id}")

        context = self._workflows.get(workflow_id)
        if not context:
            raise ValueError(f"Workflow {workflow_id} not found")

        handler = self._handlers.get(context.workflow_type)
        if not handler:
            raise ValueError(f"No handler for workflow type {context.workflow_type}")

        # Record approval
        approval.approved_by = approved_by
        approval.approved_at = datetime.utcnow()
        del self._pending_approvals[workflow_id]

        await self._transition_state(
            context,
            WorkflowState.APPROVED,
            reason=f"Approved by {approved_by}",
        )

        await self._emit_event("workflow.approved", workflow_id, {
            "approved_by": approved_by,
        })

        # Execute
        return await self._execute_workflow(context, handler)

    async def reject_workflow(
        self,
        workflow_id: str,
        rejected_by: str,
        reason: str,
    ) -> Dict[str, Any]:
        """Reject a pending workflow."""
        approval = self._pending_approvals.get(workflow_id)
        if not approval:
            raise ValueError(f"No pending approval for workflow {workflow_id}")

        context = self._workflows.get(workflow_id)
        if not context:
            raise ValueError(f"Workflow {workflow_id} not found")

        approval.rejection_reason = reason
        del self._pending_approvals[workflow_id]

        await self._transition_state(
            context,
            WorkflowState.REJECTED,
            reason=f"Rejected by {rejected_by}: {reason}",
        )

        await self._emit_event("workflow.rejected", workflow_id, {
            "rejected_by": rejected_by,
            "reason": reason,
        })

        return {"status": "rejected", "reason": reason}

    async def get_workflow_status(self, workflow_id: str) -> Dict[str, Any]:
        """Get current workflow status."""
        context = self._workflows.get(workflow_id)
        if not context:
            raise ValueError(f"Workflow {workflow_id} not found")

        current_state = context.state_history[-1]["state"] if context.state_history else "unknown"

        return {
            "workflow_id": workflow_id,
            "workflow_type": context.workflow_type.value,
            "source_id": context.source_id,
            "state": current_state,
            "risk_level": context.risk_level.value,
            "confidence_score": context.confidence_score,
            "created_at": context.created_at.isoformat(),
            "state_history": context.state_history,
        }

    async def get_pending_approvals(self) -> List[Dict[str, Any]]:
        """Get all pending approval requests."""
        return [
            {
                "workflow_id": req.workflow_id,
                "approval_route": req.approval_route.value,
                "risk_level": req.risk_level.value,
                "confidence_score": req.confidence_score,
                "plan_summary": req.plan_summary,
                "requested_at": req.requested_at.isoformat(),
            }
            for req in self._pending_approvals.values()
        ]


# Singleton instance
_orchestrator: Optional[ControlPlaneOrchestrator] = None


def get_control_plane() -> ControlPlaneOrchestrator:
    """Get singleton orchestrator instance."""
    global _orchestrator
    if _orchestrator is None:
        _orchestrator = ControlPlaneOrchestrator()
    return _orchestrator
