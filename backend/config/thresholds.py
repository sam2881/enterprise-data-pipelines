"""
Confidence Thresholds Configuration
====================================

Configures auto-reject and approval thresholds for script matching.

Author: AI Agent Platform
Version: 4.0.0
"""

import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class ConfidenceThresholds:
    """
    Confidence threshold configuration for script matching.

    Thresholds:
    - auto_reject: Below this, script is automatically rejected (not shown)
    - low_confidence: Below this, requires manual review (not auto-approved)
    - high_confidence: Above this, can be auto-approved for low-risk scripts
    - critical_action: Minimum for critical/destructive actions
    """
    auto_reject: float = 0.30      # Below 30% = auto reject
    low_confidence: float = 0.50   # 30-50% = show with warning
    high_confidence: float = 0.75  # Above 75% = can auto-approve (if low risk)
    critical_action: float = 0.85  # Above 85% required for critical actions

    @classmethod
    def from_env(cls) -> "ConfidenceThresholds":
        """Load thresholds from environment variables"""
        return cls(
            auto_reject=float(os.getenv("CONFIDENCE_AUTO_REJECT", "0.30")),
            low_confidence=float(os.getenv("CONFIDENCE_LOW", "0.50")),
            high_confidence=float(os.getenv("CONFIDENCE_HIGH", "0.75")),
            critical_action=float(os.getenv("CONFIDENCE_CRITICAL", "0.85")),
        )

    def should_reject(self, confidence: float) -> bool:
        """Check if match should be auto-rejected"""
        return confidence < self.auto_reject

    def needs_review(self, confidence: float, risk_level: str = "medium") -> bool:
        """Check if match needs manual review"""
        if risk_level in ("high", "critical"):
            return confidence < self.critical_action
        return confidence < self.high_confidence

    def can_auto_approve(self, confidence: float, risk_level: str = "low") -> bool:
        """Check if match can be auto-approved"""
        if risk_level in ("high", "critical"):
            return False  # Never auto-approve high-risk
        if risk_level == "medium":
            return confidence >= self.critical_action
        return confidence >= self.high_confidence

    def get_confidence_level(self, confidence: float) -> str:
        """Get human-readable confidence level"""
        if confidence < self.auto_reject:
            return "rejected"
        elif confidence < self.low_confidence:
            return "very_low"
        elif confidence < self.high_confidence:
            return "moderate"
        elif confidence < self.critical_action:
            return "high"
        else:
            return "very_high"

    def get_recommendation(self, confidence: float, risk_level: str = "medium") -> str:
        """Get action recommendation based on confidence and risk"""
        if confidence < self.auto_reject:
            return "auto_rejected"
        elif confidence < self.low_confidence:
            return "manual_review_required"
        elif risk_level in ("high", "critical"):
            if confidence < self.critical_action:
                return "requires_senior_approval"
            return "requires_approval"
        elif confidence >= self.high_confidence and risk_level == "low":
            return "can_auto_approve"
        else:
            return "requires_approval"


@dataclass
class ExecutionPolicy:
    """
    Execution policy based on confidence and risk.

    Defines what actions are allowed at each confidence/risk level.
    """
    # Minimum confidence for each risk level
    min_confidence_low_risk: float = 0.40
    min_confidence_medium_risk: float = 0.60
    min_confidence_high_risk: float = 0.80
    min_confidence_critical_risk: float = 0.90

    # Maximum allowed concurrent executions
    max_concurrent_low: int = 10
    max_concurrent_medium: int = 5
    max_concurrent_high: int = 2
    max_concurrent_critical: int = 1

    # Cooldown between executions (seconds)
    cooldown_low: int = 0
    cooldown_medium: int = 30
    cooldown_high: int = 300
    cooldown_critical: int = 600

    @classmethod
    def from_env(cls) -> "ExecutionPolicy":
        """Load policy from environment variables"""
        return cls(
            min_confidence_low_risk=float(os.getenv("MIN_CONF_LOW_RISK", "0.40")),
            min_confidence_medium_risk=float(os.getenv("MIN_CONF_MED_RISK", "0.60")),
            min_confidence_high_risk=float(os.getenv("MIN_CONF_HIGH_RISK", "0.80")),
            min_confidence_critical_risk=float(os.getenv("MIN_CONF_CRIT_RISK", "0.90")),
        )

    def get_min_confidence(self, risk_level: str) -> float:
        """Get minimum confidence for a risk level"""
        mapping = {
            "low": self.min_confidence_low_risk,
            "medium": self.min_confidence_medium_risk,
            "high": self.min_confidence_high_risk,
            "critical": self.min_confidence_critical_risk,
        }
        return mapping.get(risk_level, self.min_confidence_medium_risk)

    def can_execute(self, confidence: float, risk_level: str) -> tuple:
        """
        Check if execution is allowed.

        Returns:
            (allowed: bool, reason: str)
        """
        min_conf = self.get_min_confidence(risk_level)
        if confidence < min_conf:
            return False, f"Confidence {confidence:.0%} below minimum {min_conf:.0%} for {risk_level} risk"
        return True, "Execution allowed"

    def get_cooldown(self, risk_level: str) -> int:
        """Get cooldown period for risk level"""
        mapping = {
            "low": self.cooldown_low,
            "medium": self.cooldown_medium,
            "high": self.cooldown_high,
            "critical": self.cooldown_critical,
        }
        return mapping.get(risk_level, self.cooldown_medium)


# Global instances (loaded from environment)
confidence_thresholds = ConfidenceThresholds.from_env()
execution_policy = ExecutionPolicy.from_env()
