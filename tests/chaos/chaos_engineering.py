"""
Chaos Engineering Framework
Injects failures to test system resilience
"""
import asyncio
import random
import time
from typing import Dict, List, Callable, Any
from dataclasses import dataclass
from enum import Enum
import structlog

logger = structlog.get_logger()


class FailureType(Enum):
    LATENCY = "latency"
    ERROR = "error"
    TIMEOUT = "timeout"
    PARTIAL_FAILURE = "partial_failure"
    RESOURCE_EXHAUSTION = "resource_exhaustion"


@dataclass
class ChaosExperiment:
    name: str
    target: str  # service/component to target
    failure_type: FailureType
    probability: float = 0.5  # 0-1
    duration_seconds: int = 60
    params: Dict = None


class ChaosMonkey:
    """Chaos engineering controller"""

    def __init__(self):
        self.active_experiments: Dict[str, ChaosExperiment] = {}
        self.results: List[Dict] = []
        self.enabled = False

    def enable(self):
        self.enabled = True
        logger.warning("chaos_engineering_enabled")

    def disable(self):
        self.enabled = False
        self.active_experiments.clear()
        logger.info("chaos_engineering_disabled")

    def start_experiment(self, experiment: ChaosExperiment) -> str:
        """Start a chaos experiment"""
        exp_id = f"{experiment.name}_{int(time.time())}"
        self.active_experiments[exp_id] = experiment
        logger.warning(
            "chaos_experiment_started",
            id=exp_id,
            target=experiment.target,
            failure_type=experiment.failure_type.value
        )
        return exp_id

    def stop_experiment(self, exp_id: str):
        """Stop a specific experiment"""
        if exp_id in self.active_experiments:
            del self.active_experiments[exp_id]
            logger.info("chaos_experiment_stopped", id=exp_id)

    def should_inject_failure(self, target: str) -> tuple[bool, FailureType]:
        """Check if failure should be injected for target"""
        if not self.enabled:
            return False, None

        for exp in self.active_experiments.values():
            if exp.target == target or exp.target == "*":
                if random.random() < exp.probability:
                    return True, exp.failure_type
        return False, None

    def inject_latency(self, base_latency: float = 0) -> float:
        """Inject additional latency"""
        inject, failure_type = self.should_inject_failure("latency")
        if inject and failure_type == FailureType.LATENCY:
            extra_latency = random.uniform(1.0, 5.0)
            logger.debug("chaos_latency_injected", extra_seconds=extra_latency)
            return base_latency + extra_latency
        return base_latency

    def maybe_raise_error(self, target: str):
        """Maybe raise an error based on active experiments"""
        inject, failure_type = self.should_inject_failure(target)
        if inject and failure_type == FailureType.ERROR:
            logger.warning("chaos_error_injected", target=target)
            raise ChaosInducedError(f"Chaos-induced failure for {target}")

    async def wrap_async_call(self, func: Callable, target: str, *args, **kwargs) -> Any:
        """Wrap async function with chaos injection"""
        # Check for timeout
        inject, failure_type = self.should_inject_failure(target)
        if inject and failure_type == FailureType.TIMEOUT:
            logger.warning("chaos_timeout_injected", target=target)
            await asyncio.sleep(300)  # Force timeout

        # Check for latency
        latency = self.inject_latency()
        if latency > 0:
            await asyncio.sleep(latency)

        # Check for error
        self.maybe_raise_error(target)

        return await func(*args, **kwargs)


class ChaosInducedError(Exception):
    """Exception raised by chaos engineering"""
    pass


# Predefined experiments
EXPERIMENTS = {
    "llm_latency": ChaosExperiment(
        name="llm_latency",
        target="openai",
        failure_type=FailureType.LATENCY,
        probability=0.3,
        duration_seconds=300
    ),
    "servicenow_error": ChaosExperiment(
        name="servicenow_error",
        target="servicenow",
        failure_type=FailureType.ERROR,
        probability=0.2,
        duration_seconds=120
    ),
    "kafka_timeout": ChaosExperiment(
        name="kafka_timeout",
        target="kafka",
        failure_type=FailureType.TIMEOUT,
        probability=0.1,
        duration_seconds=60
    ),
    "database_partial": ChaosExperiment(
        name="database_partial",
        target="postgresql",
        failure_type=FailureType.PARTIAL_FAILURE,
        probability=0.15,
        duration_seconds=180
    )
}

# Global instance
chaos_monkey = ChaosMonkey()
