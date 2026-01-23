"""
LLM Cost Tracking Module
Tracks token usage and calculates costs for all LLM operations
"""
import time
from typing import Dict, Optional
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from prometheus_client import Counter, Gauge
import structlog

logger = structlog.get_logger()

# Pricing per 1K tokens (as of 2024)
MODEL_PRICING = {
    "gpt-4-turbo-preview": {"input": 0.01, "output": 0.03},
    "gpt-4": {"input": 0.03, "output": 0.06},
    "gpt-3.5-turbo": {"input": 0.0005, "output": 0.0015},
    "claude-3-opus": {"input": 0.015, "output": 0.075},
    "claude-3-sonnet": {"input": 0.003, "output": 0.015},
}

# Prometheus metrics for cost
LLM_COST_TOTAL = Counter('aiagent_llm_cost_dollars_total', 'Total LLM cost in dollars', ['model', 'purpose'])
LLM_COST_DAILY = Gauge('aiagent_llm_cost_dollars_daily', 'Daily LLM cost in dollars', ['model'])
BUDGET_REMAINING = Gauge('aiagent_budget_remaining_dollars', 'Remaining budget in dollars')


@dataclass
class CostRecord:
    timestamp: datetime
    model: str
    purpose: str
    input_tokens: int
    output_tokens: int
    cost: float


@dataclass
class CostTracker:
    daily_budget: float = 100.0
    monthly_budget: float = 2000.0
    records: list = field(default_factory=list)
    _daily_cost: float = 0.0
    _monthly_cost: float = 0.0
    _last_reset: datetime = field(default_factory=datetime.now)

    def calculate_cost(self, model: str, input_tokens: int, output_tokens: int) -> float:
        """Calculate cost for a single LLM call"""
        pricing = MODEL_PRICING.get(model, {"input": 0.01, "output": 0.03})
        input_cost = (input_tokens / 1000) * pricing["input"]
        output_cost = (output_tokens / 1000) * pricing["output"]
        return input_cost + output_cost

    def record_usage(
        self,
        model: str,
        input_tokens: int,
        output_tokens: int,
        purpose: str = "general"
    ) -> Dict:
        """Record token usage and update costs"""
        self._check_reset()

        cost = self.calculate_cost(model, input_tokens, output_tokens)
        record = CostRecord(
            timestamp=datetime.now(),
            model=model,
            purpose=purpose,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cost=cost
        )
        self.records.append(record)
        self._daily_cost += cost
        self._monthly_cost += cost

        # Update Prometheus metrics
        LLM_COST_TOTAL.labels(model=model, purpose=purpose).inc(cost)
        LLM_COST_DAILY.labels(model=model).set(self._daily_cost)
        BUDGET_REMAINING.set(self.daily_budget - self._daily_cost)

        logger.info(
            "cost_recorded",
            model=model,
            tokens=input_tokens + output_tokens,
            cost=f"${cost:.4f}",
            daily_total=f"${self._daily_cost:.2f}"
        )

        # Check budget alerts
        self._check_budget_alerts()

        return {
            "cost": cost,
            "daily_total": self._daily_cost,
            "budget_remaining": self.daily_budget - self._daily_cost
        }

    def _check_reset(self):
        """Reset daily counter if new day"""
        now = datetime.now()
        if now.date() > self._last_reset.date():
            self._daily_cost = 0.0
            self._last_reset = now
            logger.info("daily_cost_reset")

        if now.month > self._last_reset.month:
            self._monthly_cost = 0.0

    def _check_budget_alerts(self):
        """Check and log budget alerts"""
        daily_pct = (self._daily_cost / self.daily_budget) * 100
        if daily_pct >= 90:
            logger.warning("budget_alert_critical", daily_usage_pct=daily_pct)
        elif daily_pct >= 75:
            logger.warning("budget_alert_high", daily_usage_pct=daily_pct)

    def get_summary(self) -> Dict:
        """Get cost summary"""
        return {
            "daily_cost": self._daily_cost,
            "daily_budget": self.daily_budget,
            "daily_remaining": self.daily_budget - self._daily_cost,
            "monthly_cost": self._monthly_cost,
            "monthly_budget": self.monthly_budget,
            "total_records": len(self.records)
        }


# Global instance
cost_tracker = CostTracker()
