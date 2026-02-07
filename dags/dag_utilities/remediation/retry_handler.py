"""
APEX Data Agent - Retry Handler

Intelligent retry logic for pipeline tasks.
"""

from typing import Callable, Any, Optional
from datetime import timedelta
import time
import random


class RetryHandler:
    """
    Intelligent retry handler with exponential backoff.

    Features:
    - Exponential backoff with jitter
    - Maximum retry limits
    - Transient error detection
    - Circuit breaker pattern
    """

    def __init__(
        self,
        max_retries: int = 3,
        base_delay_seconds: float = 1.0,
        max_delay_seconds: float = 60.0,
        exponential_base: float = 2.0,
        jitter: bool = True,
    ):
        """
        Initialize retry handler.

        Args:
            max_retries: Maximum number of retry attempts
            base_delay_seconds: Initial delay between retries
            max_delay_seconds: Maximum delay between retries
            exponential_base: Base for exponential backoff
            jitter: Add random jitter to delays
        """
        self.max_retries = max_retries
        self.base_delay = base_delay_seconds
        self.max_delay = max_delay_seconds
        self.exponential_base = exponential_base
        self.jitter = jitter

    def execute_with_retry(
        self,
        func: Callable,
        *args,
        retryable_exceptions: tuple = (Exception,),
        on_retry: Optional[Callable] = None,
        **kwargs,
    ) -> Any:
        """
        Execute a function with retry logic.

        Args:
            func: Function to execute
            *args: Positional arguments for function
            retryable_exceptions: Tuple of exceptions that trigger retry
            on_retry: Optional callback on each retry
            **kwargs: Keyword arguments for function

        Returns:
            Function result

        Raises:
            Last exception if all retries fail
        """
        last_exception = None

        for attempt in range(self.max_retries + 1):
            try:
                return func(*args, **kwargs)
            except retryable_exceptions as e:
                last_exception = e

                if attempt == self.max_retries:
                    break

                # Calculate delay
                delay = self._calculate_delay(attempt)

                # Call retry callback
                if on_retry:
                    on_retry(attempt + 1, delay, e)

                # Wait before retry
                time.sleep(delay)

        raise last_exception

    def _calculate_delay(self, attempt: int) -> float:
        """Calculate delay for a retry attempt."""
        # Exponential backoff
        delay = self.base_delay * (self.exponential_base ** attempt)

        # Cap at maximum
        delay = min(delay, self.max_delay)

        # Add jitter
        if self.jitter:
            delay = delay * (0.5 + random.random())

        return delay

    def get_airflow_retry_args(self) -> dict:
        """Get Airflow task retry arguments."""
        return {
            "retries": self.max_retries,
            "retry_delay": timedelta(seconds=self.base_delay),
            "retry_exponential_backoff": True,
            "max_retry_delay": timedelta(seconds=self.max_delay),
        }


class CircuitBreaker:
    """
    Circuit breaker pattern for failing services.

    States:
    - CLOSED: Normal operation, requests pass through
    - OPEN: Service failing, requests fail fast
    - HALF_OPEN: Testing if service recovered
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        success_threshold: int = 2,
    ):
        """
        Initialize circuit breaker.

        Args:
            failure_threshold: Failures before opening circuit
            recovery_timeout: Seconds before attempting recovery
            success_threshold: Successes needed to close circuit
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.success_threshold = success_threshold

        self._state = "CLOSED"
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time = 0.0

    @property
    def state(self) -> str:
        """Get current circuit state."""
        if self._state == "OPEN":
            # Check if recovery timeout passed
            if time.time() - self._last_failure_time > self.recovery_timeout:
                self._state = "HALF_OPEN"
                self._success_count = 0
        return self._state

    def execute(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function through circuit breaker."""
        state = self.state

        if state == "OPEN":
            raise CircuitBreakerOpenError("Circuit breaker is open")

        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise

    def _on_success(self) -> None:
        """Handle successful execution."""
        if self._state == "HALF_OPEN":
            self._success_count += 1
            if self._success_count >= self.success_threshold:
                self._state = "CLOSED"
                self._failure_count = 0
        else:
            self._failure_count = 0

    def _on_failure(self) -> None:
        """Handle failed execution."""
        self._failure_count += 1
        self._last_failure_time = time.time()

        if self._failure_count >= self.failure_threshold:
            self._state = "OPEN"


class CircuitBreakerOpenError(Exception):
    """Raised when circuit breaker is open."""
    pass
