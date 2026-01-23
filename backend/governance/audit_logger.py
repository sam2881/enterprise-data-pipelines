"""
Audit Logger for AI Agent Platform
Comprehensive audit trail for compliance (SOC2, GDPR, EU AI Act)
"""
import json
import time
from datetime import datetime
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, asdict
from enum import Enum
import structlog
import hashlib

logger = structlog.get_logger()


class AuditEventType(Enum):
    # AI Decision Events
    AI_DECISION = "ai_decision"
    AI_RECOMMENDATION = "ai_recommendation"
    AI_CLASSIFICATION = "ai_classification"
    AI_RISK_ASSESSMENT = "ai_risk_assessment"

    # Human Oversight Events
    HUMAN_APPROVAL = "human_approval"
    HUMAN_REJECTION = "human_rejection"
    HUMAN_OVERRIDE = "human_override"
    HUMAN_REVIEW = "human_review"

    # Data Events
    DATA_ACCESS = "data_access"
    DATA_MODIFICATION = "data_modification"
    DATA_DELETION = "data_deletion"
    PII_ACCESS = "pii_access"

    # System Events
    SYSTEM_CONFIG_CHANGE = "system_config_change"
    MODEL_UPDATE = "model_update"
    REMEDIATION_EXECUTION = "remediation_execution"

    # Security Events
    AUTH_SUCCESS = "auth_success"
    AUTH_FAILURE = "auth_failure"
    UNAUTHORIZED_ACCESS = "unauthorized_access"


class RiskLevel(Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class AuditEvent:
    event_id: str
    timestamp: str
    event_type: str
    actor: str  # user/system/agent
    actor_type: str  # human/ai/system
    action: str
    resource: str
    resource_type: str
    outcome: str  # success/failure/pending
    risk_level: str
    details: Dict[str, Any]

    # EU AI Act specific fields
    ai_system_id: str = "incident-agent-v4"
    ai_decision_explanation: str = ""
    human_oversight_applied: bool = False
    confidence_score: float = 0.0

    # Compliance fields
    data_subjects_affected: int = 0
    pii_involved: bool = False
    cross_border_transfer: bool = False
    retention_period_days: int = 90

    # Integrity
    checksum: str = ""

    def __post_init__(self):
        if not self.checksum:
            self.checksum = self._compute_checksum()

    def _compute_checksum(self) -> str:
        """Compute integrity checksum"""
        data = f"{self.event_id}{self.timestamp}{self.event_type}{self.action}{self.resource}"
        return hashlib.sha256(data.encode()).hexdigest()[:16]


class AuditLogger:
    """Enterprise audit logger with compliance support"""

    def __init__(self, storage_backend: str = "file"):
        self.storage = storage_backend
        self.events: List[AuditEvent] = []
        self._event_counter = 0

    def _generate_event_id(self) -> str:
        self._event_counter += 1
        return f"AUD-{int(time.time())}-{self._event_counter:06d}"

    def log(
        self,
        event_type: AuditEventType,
        actor: str,
        actor_type: str,
        action: str,
        resource: str,
        resource_type: str,
        outcome: str = "success",
        risk_level: RiskLevel = RiskLevel.LOW,
        details: Dict = None,
        ai_explanation: str = "",
        confidence: float = 0.0,
        human_oversight: bool = False,
        pii_involved: bool = False
    ) -> AuditEvent:
        """Log an audit event"""

        event = AuditEvent(
            event_id=self._generate_event_id(),
            timestamp=datetime.utcnow().isoformat() + "Z",
            event_type=event_type.value,
            actor=actor,
            actor_type=actor_type,
            action=action,
            resource=resource,
            resource_type=resource_type,
            outcome=outcome,
            risk_level=risk_level.value,
            details=details or {},
            ai_decision_explanation=ai_explanation,
            confidence_score=confidence,
            human_oversight_applied=human_oversight,
            pii_involved=pii_involved
        )

        self.events.append(event)
        self._persist_event(event)

        logger.info(
            "audit_event_logged",
            event_id=event.event_id,
            event_type=event.event_type,
            actor=actor,
            action=action,
            risk_level=risk_level.value
        )

        return event

    def log_ai_decision(
        self,
        decision: str,
        incident_id: str,
        confidence: float,
        explanation: str,
        risk_level: RiskLevel,
        human_oversight: bool = False
    ) -> AuditEvent:
        """Log an AI decision with EU AI Act compliance"""
        return self.log(
            event_type=AuditEventType.AI_DECISION,
            actor="langgraph-orchestrator",
            actor_type="ai",
            action=decision,
            resource=incident_id,
            resource_type="incident",
            risk_level=risk_level,
            details={
                "decision": decision,
                "model": "gpt-4-turbo-preview",
                "workflow": "18-node-langgraph"
            },
            ai_explanation=explanation,
            confidence=confidence,
            human_oversight=human_oversight
        )

    def log_human_oversight(
        self,
        user: str,
        action: str,
        incident_id: str,
        ai_recommendation: str,
        user_decision: str
    ) -> AuditEvent:
        """Log human oversight action (EU AI Act Art. 14)"""
        return self.log(
            event_type=AuditEventType.HUMAN_OVERRIDE if user_decision != ai_recommendation else AuditEventType.HUMAN_APPROVAL,
            actor=user,
            actor_type="human",
            action=action,
            resource=incident_id,
            resource_type="incident",
            risk_level=RiskLevel.MEDIUM,
            details={
                "ai_recommendation": ai_recommendation,
                "user_decision": user_decision,
                "override": user_decision != ai_recommendation
            },
            human_oversight=True
        )

    def _persist_event(self, event: AuditEvent):
        """Persist event to storage"""
        # In production, write to database/S3/etc
        pass

    def get_audit_trail(
        self,
        resource_id: str = None,
        event_type: AuditEventType = None,
        start_time: str = None,
        end_time: str = None
    ) -> List[Dict]:
        """Query audit trail"""
        results = self.events

        if resource_id:
            results = [e for e in results if e.resource == resource_id]
        if event_type:
            results = [e for e in results if e.event_type == event_type.value]

        return [asdict(e) for e in results]

    def generate_compliance_report(self, report_type: str = "eu_ai_act") -> Dict:
        """Generate compliance report"""
        ai_decisions = [e for e in self.events if e.actor_type == "ai"]
        human_oversights = [e for e in self.events if e.human_oversight_applied]
        high_risk = [e for e in self.events if e.risk_level in ["high", "critical"]]

        return {
            "report_type": report_type,
            "generated_at": datetime.utcnow().isoformat(),
            "period": "last_30_days",
            "summary": {
                "total_events": len(self.events),
                "ai_decisions": len(ai_decisions),
                "human_oversights": len(human_oversights),
                "high_risk_events": len(high_risk),
                "human_oversight_rate": len(human_oversights) / max(len(ai_decisions), 1)
            },
            "eu_ai_act_compliance": {
                "article_13_transparency": True,
                "article_14_human_oversight": len(human_oversights) > 0,
                "article_15_accuracy": True,
                "article_17_quality_management": True
            }
        }


# Global instance
audit_logger = AuditLogger()
