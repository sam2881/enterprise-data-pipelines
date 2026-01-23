"""
Policy Engine - Risk-Based Approval Routing

WHY: Implements governance layer for remediation plans:
- Policy validation (allowed actions, environments, times)
- Risk assessment (based on action type, blast radius)
- Approval routing (auto, async, manual)
- Combines with Judge scores for final decision

HOW: Evaluates plans against policies and routes to appropriate
approval workflow based on risk level and Judge score.

Routing Rules:
    LOW risk + Judge > 8 → Auto-approve
    MEDIUM risk → Async approval (Slack notification)
    HIGH risk → Manual approval required
"""
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime, time
from enum import Enum
import structlog

logger = structlog.get_logger()


class RiskLevel(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class ApprovalRoute(str, Enum):
    AUTO_APPROVE = "auto_approve"
    ASYNC_APPROVE = "async_approve"  # Slack notification
    MANUAL_APPROVE = "manual_approve"  # Block until human
    HITL = "hitl"  # Human-in-the-loop (alias for manual)
    SENIOR = "senior"  # Senior approval required
    DENY = "deny"
    REJECT = "reject"


@dataclass
class Policy:
    """Policy definition for action validation"""
    name: str
    allowed_actions: List[str]
    allowed_environments: List[str]
    allowed_times: Optional[Dict[str, str]] = None  # {"start": "09:00", "end": "17:00"}
    max_blast_radius: int = 10  # Max affected resources
    requires_approval_above: RiskLevel = RiskLevel.MEDIUM
    cooldown_minutes: int = 0  # Minimum time between executions


@dataclass
class RiskAssessment:
    """Result of risk assessment"""
    risk_level: RiskLevel
    risk_score: float  # 0.0 - 1.0
    factors: List[str]
    mitigations: List[str] = field(default_factory=list)


@dataclass
class ApprovalDecision:
    """Final approval decision from Policy Engine"""
    route: ApprovalRoute
    approved: bool
    risk_assessment: RiskAssessment
    policy_violations: List[str]
    conditions: List[str] = field(default_factory=list)
    reasoning: str = ""


class PolicyEngine:
    """
    Governance and approval routing for remediation plans.

    WHY this component:
    - Separates policy from execution
    - Enables configurable risk thresholds
    - Provides audit trail for approvals
    - Integrates human oversight where needed
    """

    # Default policies
    DEFAULT_POLICIES = [
        Policy(
            name="production_protection",
            allowed_actions=["restart", "scale", "config_update"],
            allowed_environments=["production", "staging", "development"],
            max_blast_radius=5,
            requires_approval_above=RiskLevel.LOW
        ),
        Policy(
            name="business_hours",
            allowed_actions=["*"],
            allowed_environments=["production"],
            allowed_times={"start": "09:00", "end": "17:00"},
            requires_approval_above=RiskLevel.LOW
        ),
        Policy(
            name="high_risk_actions",
            allowed_actions=["restart", "scale"],
            allowed_environments=["*"],
            requires_approval_above=RiskLevel.MEDIUM
        )
    ]

    # Risk weights for different action types
    ACTION_RISK_WEIGHTS = {
        "restart": 0.3,
        "scale": 0.4,
        "config_update": 0.5,
        "deploy": 0.6,
        "patch": 0.6,
        "delete": 0.9,
        "terminate": 0.9,
        "modify_infrastructure": 0.7,
        "database_migration": 0.8,
        "shell": 0.3,  # Shell scripts for infrastructure ops (start/stop VM)
        "ansible": 0.4,  # Ansible playbooks
        "terraform": 0.6,  # Terraform changes
        "unknown": 0.7
    }

    # Environment risk multipliers
    ENV_RISK_MULTIPLIERS = {
        "production": 2.0,
        "staging": 1.5,
        "development": 0.5,
        "test": 0.3
    }

    def __init__(
        self,
        policies: Optional[List[Policy]] = None,
        judge_threshold: float = 8.0,
        auto_approve_risk: RiskLevel = RiskLevel.LOW,
        settings=None
    ):
        self.policies = policies or self.DEFAULT_POLICIES
        self.judge_threshold = judge_threshold
        self.auto_approve_risk = auto_approve_risk
        self.settings = settings

        logger.info(
            "policy_engine_initialized",
            policies=len(self.policies),
            judge_threshold=judge_threshold
        )

    def evaluate(
        self,
        plan: Dict[str, Any],
        judge_score: float = 0.0,
        incident_context: Optional[Dict[str, Any]] = None
    ) -> ApprovalDecision:
        """
        Evaluate plan and determine approval route.

        Args:
            plan: The remediation plan to evaluate
            judge_score: Score from LLM-as-Judge (1-10)
            incident_context: Context about the incident

        Returns:
            ApprovalDecision with route and reasoning
        """
        context = incident_context or {}

        # 1. Validate against policies
        violations = self._check_policy_violations(plan, context)

        # 2. Assess risk
        risk_assessment = self._assess_risk(plan, context)

        # 3. Determine route
        route, reasoning = self._determine_route(
            risk_assessment,
            judge_score,
            violations
        )

        approved = route == ApprovalRoute.AUTO_APPROVE

        decision = ApprovalDecision(
            route=route,
            approved=approved,
            risk_assessment=risk_assessment,
            policy_violations=violations,
            conditions=self._get_conditions(plan, risk_assessment),
            reasoning=reasoning
        )

        logger.info(
            "approval_decision",
            route=route.value,
            approved=approved,
            risk_level=risk_assessment.risk_level.value,
            judge_score=judge_score,
            violations=len(violations)
        )

        return decision

    def _check_policy_violations(
        self,
        plan: Dict[str, Any],
        context: Dict[str, Any]
    ) -> List[str]:
        """Check plan against all policies"""
        violations = []

        action = plan.get("action_type", plan.get("action", "unknown"))
        environment = context.get("environment", "unknown")
        affected_resources = plan.get("affected_resources", [])

        for policy in self.policies:
            # Check allowed actions
            if policy.allowed_actions != ["*"]:
                if action not in policy.allowed_actions:
                    violations.append(
                        f"Action '{action}' not in allowed actions for policy '{policy.name}'"
                    )

            # Check allowed environments
            if policy.allowed_environments != ["*"]:
                if environment not in policy.allowed_environments:
                    violations.append(
                        f"Environment '{environment}' not allowed for policy '{policy.name}'"
                    )

            # Check blast radius
            if len(affected_resources) > policy.max_blast_radius:
                violations.append(
                    f"Blast radius ({len(affected_resources)}) exceeds max ({policy.max_blast_radius})"
                )

            # Check time window
            if policy.allowed_times and environment == "production":
                if not self._in_allowed_time_window(policy.allowed_times):
                    violations.append(
                        f"Outside allowed time window for production ({policy.allowed_times})"
                    )

        return violations

    def _in_allowed_time_window(self, allowed_times: Dict[str, str]) -> bool:
        """Check if current time is within allowed window (uses UTC)"""
        now = datetime.utcnow().time()
        start = time.fromisoformat(allowed_times["start"])
        end = time.fromisoformat(allowed_times["end"])

        if start <= end:
            return start <= now <= end
        else:  # Wraps around midnight
            return now >= start or now <= end

    def _assess_risk(
        self,
        plan: Dict[str, Any],
        context: Dict[str, Any]
    ) -> RiskAssessment:
        """Assess risk level of the plan"""
        factors = []
        mitigations = []

        # Base risk from action type
        action = plan.get("action_type", plan.get("action", "unknown"))
        action_risk = self.ACTION_RISK_WEIGHTS.get(action, 0.7)
        factors.append(f"Action type '{action}' (base risk: {action_risk:.2f})")

        # Environment multiplier
        environment = context.get("environment", "production")
        env_multiplier = self.ENV_RISK_MULTIPLIERS.get(environment, 1.5)
        factors.append(f"Environment '{environment}' (multiplier: {env_multiplier:.1f})")

        # Blast radius factor
        affected = len(plan.get("affected_resources", []))
        blast_factor = min(affected / 10, 1.0)
        factors.append(f"Blast radius: {affected} resources")

        # Rollback availability
        if plan.get("rollback_plan"):
            mitigations.append("Rollback plan available")
            rollback_reduction = 0.2
        else:
            factors.append("No rollback plan")
            rollback_reduction = 0.0

        # Dry run flag
        if plan.get("dry_run", True):
            mitigations.append("Dry run enabled")
            dry_run_reduction = 0.3
        else:
            dry_run_reduction = 0.0

        # Calculate final risk score
        base_score = action_risk * env_multiplier
        blast_adjustment = blast_factor * 0.2
        risk_score = max(0, min(1.0, base_score + blast_adjustment - rollback_reduction - dry_run_reduction))

        # Determine risk level
        if risk_score < 0.3:
            risk_level = RiskLevel.LOW
        elif risk_score < 0.5:
            risk_level = RiskLevel.MEDIUM
        elif risk_score < 0.8:
            risk_level = RiskLevel.HIGH
        else:
            risk_level = RiskLevel.CRITICAL

        return RiskAssessment(
            risk_level=risk_level,
            risk_score=risk_score,
            factors=factors,
            mitigations=mitigations
        )

    def _determine_route(
        self,
        risk: RiskAssessment,
        judge_score: float,
        violations: List[str]
    ) -> tuple:
        """Determine approval route based on risk and judge score"""
        # Reject if critical violations
        if any("not allowed" in v.lower() for v in violations):
            return ApprovalRoute.DENY, f"Policy violations: {', '.join(violations)}"

        # Reject if critical risk
        if risk.risk_level == RiskLevel.CRITICAL:
            return ApprovalRoute.DENY, f"Risk level CRITICAL ({risk.risk_score:.2f})"

        # High risk always requires manual approval
        if risk.risk_level == RiskLevel.HIGH:
            return ApprovalRoute.SENIOR, f"High risk requires senior approval"

        # Medium risk with good judge score can be async
        if risk.risk_level == RiskLevel.MEDIUM:
            if judge_score >= self.judge_threshold:
                return ApprovalRoute.ASYNC_APPROVE, f"Medium risk with high judge score ({judge_score})"
            else:
                return ApprovalRoute.HITL, f"Medium risk with low judge score ({judge_score})"

        # Low risk with good judge score can auto-approve
        if risk.risk_level == RiskLevel.LOW:
            if judge_score >= self.judge_threshold:
                return ApprovalRoute.AUTO_APPROVE, f"Low risk with high judge score ({judge_score})"
            else:
                return ApprovalRoute.ASYNC_APPROVE, f"Low risk but judge score below threshold ({judge_score})"

        # Default to manual
        return ApprovalRoute.HITL, "Default to manual approval"

    def _get_conditions(
        self,
        plan: Dict[str, Any],
        risk: RiskAssessment
    ) -> List[str]:
        """Get conditions that must be met for execution"""
        conditions = []

        # Always require dry run for high risk
        if risk.risk_level in [RiskLevel.HIGH, RiskLevel.CRITICAL]:
            conditions.append("Must execute dry run first")

        # Require monitoring for production
        if plan.get("environment") == "production":
            conditions.append("Enhanced monitoring required during execution")

        # Require rollback for destructive actions
        action = plan.get("action_type", plan.get("action", ""))
        if action in ["delete", "terminate", "modify_infrastructure"]:
            conditions.append("Rollback plan must be tested before execution")

        return conditions


# Global instance
_policy_engine: Optional[PolicyEngine] = None


def get_policy_engine() -> PolicyEngine:
    """Get singleton policy engine instance."""
    global _policy_engine
    if _policy_engine is None:
        _policy_engine = PolicyEngine()
    return _policy_engine
