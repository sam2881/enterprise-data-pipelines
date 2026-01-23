"""
Circuit Breaker Pattern Implementation
======================================

Protects external service calls from cascading failures.

States:
- CLOSED: Normal operation, requests pass through
- OPEN: Circuit is tripped, requests fail fast
- HALF_OPEN: Testing if service is recovered

Usage:
    breaker = CircuitBreaker(name="github_api")

    @breaker
    async def call_github():
        ...

    # Or manually:
    if breaker.can_execute():
        try:
            result = await external_call()
            breaker.record_success()
        except Exception as e:
            breaker.record_failure(e)

Author: AI Agent Platform
Version: 4.0.0
"""

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, Optional, Type, Union
from functools import wraps
import structlog

logger = structlog.get_logger()


class CircuitState(Enum):
    """Circuit breaker states"""
    CLOSED = "closed"       # Normal operation
    OPEN = "open"           # Failing fast
    HALF_OPEN = "half_open" # Testing recovery


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker"""
    failure_threshold: int = 5          # Failures before opening
    success_threshold: int = 3          # Successes in half-open before closing
    timeout: float = 30.0               # Seconds before trying half-open
    half_open_max_calls: int = 3        # Max calls in half-open state
    excluded_exceptions: tuple = ()      # Exceptions that don't count as failures


@dataclass
class CircuitStats:
    """Statistics for circuit breaker"""
    total_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    rejected_calls: int = 0
    state_changes: int = 0
    last_failure_time: Optional[float] = None
    last_success_time: Optional[float] = None
    consecutive_failures: int = 0
    consecutive_successes: int = 0


class CircuitBreaker:
    """
    Circuit Breaker implementation for protecting external service calls.

    Features:
    - Automatic state transitions based on success/failure
    - Configurable thresholds and timeouts
    - Async and sync support
    - Detailed statistics and logging
    - Decorator and context manager patterns
    """

    def __init__(
        self,
        name: str,
        config: Optional[CircuitBreakerConfig] = None,
        fallback: Optional[Callable] = None
    ):
        """
        Initialize circuit breaker.

        Args:
            name: Identifier for this breaker (used in logging)
            config: Configuration settings
            fallback: Optional fallback function when circuit is open
        """
        self.name = name
        self.config = config or CircuitBreakerConfig()
        self.fallback = fallback

        self._state = CircuitState.CLOSED
        self._stats = CircuitStats()
        self._last_state_change = time.time()
        self._half_open_calls = 0
        self._lock = asyncio.Lock()

        logger.info(
            "circuit_breaker_initialized",
            name=name,
            failure_threshold=self.config.failure_threshold,
            timeout=self.config.timeout
        )

    @property
    def state(self) -> CircuitState:
        """Get current state, checking for timeout-based transitions"""
        if self._state == CircuitState.OPEN:
            if time.time() - self._last_state_change >= self.config.timeout:
                self._transition_to(CircuitState.HALF_OPEN)
        return self._state

    @property
    def stats(self) -> Dict[str, Any]:
        """Get circuit breaker statistics"""
        return {
            "name": self.name,
            "state": self._state.value,
            "total_calls": self._stats.total_calls,
            "successful_calls": self._stats.successful_calls,
            "failed_calls": self._stats.failed_calls,
            "rejected_calls": self._stats.rejected_calls,
            "consecutive_failures": self._stats.consecutive_failures,
            "consecutive_successes": self._stats.consecutive_successes,
            "last_failure": self._stats.last_failure_time,
            "last_success": self._stats.last_success_time,
        }

    def can_execute(self) -> bool:
        """Check if a call can be executed"""
        state = self.state  # This may trigger state transition

        if state == CircuitState.CLOSED:
            return True
        elif state == CircuitState.OPEN:
            return False
        elif state == CircuitState.HALF_OPEN:
            return self._half_open_calls < self.config.half_open_max_calls

        return False

    def record_success(self):
        """Record a successful call"""
        self._stats.total_calls += 1
        self._stats.successful_calls += 1
        self._stats.consecutive_successes += 1
        self._stats.consecutive_failures = 0
        self._stats.last_success_time = time.time()

        if self._state == CircuitState.HALF_OPEN:
            self._half_open_calls += 1
            if self._stats.consecutive_successes >= self.config.success_threshold:
                self._transition_to(CircuitState.CLOSED)

        logger.debug(
            "circuit_breaker_success",
            name=self.name,
            consecutive_successes=self._stats.consecutive_successes
        )

    def record_failure(self, exception: Optional[Exception] = None):
        """Record a failed call"""
        # Check if exception should be excluded
        if exception and isinstance(exception, self.config.excluded_exceptions):
            logger.debug(
                "circuit_breaker_excluded_exception",
                name=self.name,
                exception=type(exception).__name__
            )
            return

        self._stats.total_calls += 1
        self._stats.failed_calls += 1
        self._stats.consecutive_failures += 1
        self._stats.consecutive_successes = 0
        self._stats.last_failure_time = time.time()

        if self._state == CircuitState.HALF_OPEN:
            # Any failure in half-open opens the circuit
            self._transition_to(CircuitState.OPEN)
        elif self._state == CircuitState.CLOSED:
            if self._stats.consecutive_failures >= self.config.failure_threshold:
                self._transition_to(CircuitState.OPEN)

        logger.warning(
            "circuit_breaker_failure",
            name=self.name,
            consecutive_failures=self._stats.consecutive_failures,
            exception=str(exception) if exception else None
        )

    def record_rejection(self):
        """Record a rejected call (circuit open)"""
        self._stats.total_calls += 1
        self._stats.rejected_calls += 1

        logger.warning(
            "circuit_breaker_rejected",
            name=self.name,
            state=self._state.value
        )

    def _transition_to(self, new_state: CircuitState):
        """Transition to a new state"""
        old_state = self._state
        self._state = new_state
        self._last_state_change = time.time()
        self._stats.state_changes += 1

        if new_state == CircuitState.HALF_OPEN:
            self._half_open_calls = 0
            self._stats.consecutive_successes = 0

        if new_state == CircuitState.CLOSED:
            self._stats.consecutive_failures = 0

        logger.info(
            "circuit_breaker_state_change",
            name=self.name,
            old_state=old_state.value,
            new_state=new_state.value
        )

    def reset(self):
        """Manually reset circuit to closed state"""
        self._transition_to(CircuitState.CLOSED)
        self._stats.consecutive_failures = 0
        self._stats.consecutive_successes = 0
        logger.info("circuit_breaker_reset", name=self.name)

    def force_open(self):
        """Manually force circuit open"""
        self._transition_to(CircuitState.OPEN)
        logger.info("circuit_breaker_forced_open", name=self.name)

    def __call__(self, func: Callable) -> Callable:
        """Decorator for protecting functions"""
        if asyncio.iscoroutinefunction(func):
            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                return await self.call_async(func, *args, **kwargs)
            return async_wrapper
        else:
            @wraps(func)
            def sync_wrapper(*args, **kwargs):
                return self.call_sync(func, *args, **kwargs)
            return sync_wrapper

    async def call_async(self, func: Callable, *args, **kwargs) -> Any:
        """Execute async function with circuit breaker protection"""
        if not self.can_execute():
            self.record_rejection()
            if self.fallback:
                return await self.fallback(*args, **kwargs) if asyncio.iscoroutinefunction(self.fallback) else self.fallback(*args, **kwargs)
            raise CircuitOpenError(f"Circuit breaker '{self.name}' is open")

        try:
            result = await func(*args, **kwargs)
            self.record_success()
            return result
        except Exception as e:
            self.record_failure(e)
            raise

    def call_sync(self, func: Callable, *args, **kwargs) -> Any:
        """Execute sync function with circuit breaker protection"""
        if not self.can_execute():
            self.record_rejection()
            if self.fallback:
                return self.fallback(*args, **kwargs)
            raise CircuitOpenError(f"Circuit breaker '{self.name}' is open")

        try:
            result = func(*args, **kwargs)
            self.record_success()
            return result
        except Exception as e:
            self.record_failure(e)
            raise


class CircuitOpenError(Exception):
    """Raised when circuit breaker is open"""
    pass


# =============================================================================
# GLOBAL CIRCUIT BREAKERS FOR COMMON SERVICES
# =============================================================================

# GitHub API circuit breaker
github_breaker = CircuitBreaker(
    name="github_api",
    config=CircuitBreakerConfig(
        failure_threshold=3,
        timeout=60.0,
        success_threshold=2
    )
)

# ServiceNow API circuit breaker
servicenow_breaker = CircuitBreaker(
    name="servicenow_api",
    config=CircuitBreakerConfig(
        failure_threshold=5,
        timeout=30.0,
        success_threshold=3
    )
)

# OpenAI API circuit breaker
openai_breaker = CircuitBreaker(
    name="openai_api",
    config=CircuitBreakerConfig(
        failure_threshold=3,
        timeout=120.0,  # Longer timeout for rate limits
        success_threshold=2
    )
)

# Neo4j circuit breaker
neo4j_breaker = CircuitBreaker(
    name="neo4j",
    config=CircuitBreakerConfig(
        failure_threshold=5,
        timeout=30.0,
        success_threshold=3
    )
)

# Weaviate circuit breaker
weaviate_breaker = CircuitBreaker(
    name="weaviate",
    config=CircuitBreakerConfig(
        failure_threshold=5,
        timeout=30.0,
        success_threshold=3
    )
)


def get_all_breaker_stats() -> Dict[str, Any]:
    """Get statistics for all circuit breakers"""
    return {
        "github": github_breaker.stats,
        "servicenow": servicenow_breaker.stats,
        "openai": openai_breaker.stats,
        "neo4j": neo4j_breaker.stats,
        "weaviate": weaviate_breaker.stats,
    }
