"""
Abstract base class for all agents in the Data Engineering Platform.

This module provides the foundation for all agent implementations with:
- Abstract execute() method that all agents must implement
- Audit logging for all agent decisions and actions
- State sanitization to remove sensitive data before logging
- Error handling utilities
- Performance timing

RULES:
1. ALL agents MUST inherit from BaseAgent
2. ALL agents MUST implement execute() method
3. ALL agents MUST log decisions to audit table
4. ALL agents MUST handle errors gracefully
5. ALL agents MUST return partial state updates only
"""

from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, TypeVar, Union
import time
import traceback
import uuid
import functools

from pydantic import BaseModel, Field

from src.config.settings import get_settings
from src.utils.exceptions import DataAgentError
from src.utils.logging import get_logger, AgentLogger


# =============================================================================
# Audit Log Model
# =============================================================================


class AgentAuditLog(BaseModel):
    """
    Audit log entry for agent actions.

    Every agent decision and action is logged with full context
    for traceability, debugging, and compliance.

    Attributes:
        log_id: Unique identifier for this log entry
        agent_name: Name of the agent that performed the action
        action: Type of action performed (e.g., "plan", "generate", "validate")
        request_id: Correlation ID for the workflow request
        pipeline_id: ID of the pipeline being processed (if applicable)
        input_state: Sanitized input state (secrets redacted)
        output_state: Sanitized output state (secrets redacted)
        decision_reasoning: Explanation of why this decision was made
        duration_ms: Time taken for the action in milliseconds
        status: Result status (success, error, skipped)
        error_details: Error information if status is error
        created_at: Timestamp of the log entry
    """

    log_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    agent_name: str
    action: str
    request_id: Optional[str] = None
    pipeline_id: Optional[int] = None
    input_state: Dict[str, Any] = Field(default_factory=dict)
    output_state: Dict[str, Any] = Field(default_factory=dict)
    decision_reasoning: str = ""
    duration_ms: int = 0
    status: str = "success"
    error_details: Optional[str] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    model_config = {"frozen": True}


# =============================================================================
# Agent Execution Result
# =============================================================================


class AgentResult(BaseModel):
    """
    Result of agent execution.

    Encapsulates the output of an agent's execute() method with
    metadata about the execution.
    """

    success: bool
    state_update: Dict[str, Any]
    reasoning: str = ""
    warnings: List[str] = Field(default_factory=list)
    duration_ms: int = 0


# =============================================================================
# Base Agent Class
# =============================================================================


T = TypeVar("T", bound="BaseAgent")


class BaseAgent(ABC):
    """
    Abstract base class for all agents in the Data Engineering Platform.

    ALL agents MUST:
    1. Inherit from this class
    2. Implement execute() method
    3. Log all decisions to audit table
    4. Handle errors gracefully
    5. Return partial state updates only

    The BaseAgent provides:
    - Structured logging via AgentLogger
    - Audit logging to PostgreSQL
    - State sanitization for security
    - Error handling utilities
    - Performance timing decorators

    Example:
        class PlannerAgent(BaseAgent):
            def __init__(self):
                super().__init__("planner")

            def execute(self, state: Dict[str, Any]) -> Dict[str, Any]:
                # Implementation here
                return {"planner_output": {...}}
    """

    # Sensitive keys to redact from state before logging
    SENSITIVE_KEYS: List[str] = [
        "password",
        "secret",
        "token",
        "key",
        "credential",
        "api_key",
        "apikey",
        "auth",
        "bearer",
        "private",
        "access_token",
        "refresh_token",
        "client_secret",
        "connection_string",
    ]

    def __init__(self, agent_name: str) -> None:
        """
        Initialize the base agent.

        Args:
            agent_name: Unique identifier for this agent type
                       (e.g., "planner", "generator", "validator")
        """
        self.agent_name = agent_name
        self._logger = get_logger(f"agent.{agent_name}")
        self._agent_logger = AgentLogger(agent_name)
        self._settings = get_settings()

        # Lazy-load repository to avoid circular imports
        self._repo: Optional[Any] = None

    @property
    def repo(self) -> Any:
        """
        Lazy-load the metadata repository.

        Returns:
            MetadataRepository instance
        """
        if self._repo is None:
            from src.metadata.repository import MetadataRepository
            self._repo = MetadataRepository()
        return self._repo

    @property
    def logger(self):
        """Get the structured logger."""
        return self._logger

    # =========================================================================
    # Abstract Methods
    # =========================================================================

    @abstractmethod
    def execute(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute the agent's main logic.

        This method MUST be implemented by all agent subclasses.
        It should:
        1. Read required data from state
        2. Perform agent-specific logic
        3. Return ONLY partial state updates (not full state)
        4. Handle errors by returning error state

        Args:
            state: Current workflow state dictionary

        Returns:
            Partial state update dictionary. On success, include
            the agent's output. On error, include:
            - error_message: Description of what failed
            - error_agent: This agent's name
            - current_phase: "failed"

        Example:
            def execute(self, state: Dict[str, Any]) -> Dict[str, Any]:
                try:
                    # Do work
                    return {
                        "planner_output": {...},
                        "current_phase": "planning"
                    }
                except Exception as e:
                    return {
                        "error_message": str(e),
                        "error_agent": self.agent_name,
                        "current_phase": "failed"
                    }
        """
        pass

    # =========================================================================
    # Execution Wrapper
    # =========================================================================

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute the agent with full instrumentation.

        This wrapper provides:
        - Performance timing
        - Audit logging
        - Error handling
        - State sanitization

        Use this method instead of calling execute() directly
        when you want full audit trail.

        Args:
            state: Current workflow state

        Returns:
            Partial state update with execution metadata
        """
        request_id = state.get("request_id", "unknown")
        pipeline_id = state.get("metadata_context", {}).get("pipeline_id")

        # Start timing
        start_time = time.perf_counter()
        self._agent_logger.log_start(request_id, {"phase": state.get("current_phase")})

        # Capture sanitized input
        sanitized_input = self._sanitize_state(state)

        try:
            # Execute agent logic
            result = self.execute(state)

            # Calculate duration
            duration_ms = int((time.perf_counter() - start_time) * 1000)

            # Determine status
            status = "error" if result.get("error_message") else "success"

            # Log completion
            self._agent_logger.log_complete(
                request_id=request_id,
                result=result,
                duration_ms=duration_ms,
            )

            # Write audit log
            self.log_audit(
                action=f"{self.agent_name}_execute",
                request_id=request_id,
                pipeline_id=pipeline_id,
                input_state=sanitized_input,
                output_state=self._sanitize_state(result),
                reasoning=result.get("decision_reasoning", ""),
                duration_ms=duration_ms,
                status=status,
            )

            return result

        except Exception as e:
            # Calculate duration
            duration_ms = int((time.perf_counter() - start_time) * 1000)
            error_trace = traceback.format_exc()

            # Log error
            self._agent_logger.log_error(
                request_id=request_id,
                error=e,
                context={"traceback": error_trace},
            )

            # Create error state
            error_result = {
                "error_message": f"{self.agent_name} failed: {str(e)}",
                "error_agent": self.agent_name,
                "current_phase": "failed",
            }

            # Write audit log
            self.log_audit(
                action=f"{self.agent_name}_execute",
                request_id=request_id,
                pipeline_id=pipeline_id,
                input_state=sanitized_input,
                output_state=self._sanitize_state(error_result),
                reasoning=f"Execution failed with error: {str(e)}",
                duration_ms=duration_ms,
                status="error",
                error_details=error_trace,
            )

            return error_result

    # =========================================================================
    # Audit Logging
    # =========================================================================

    def log_audit(
        self,
        action: str,
        request_id: Optional[str] = None,
        pipeline_id: Optional[int] = None,
        input_state: Optional[Dict[str, Any]] = None,
        output_state: Optional[Dict[str, Any]] = None,
        reasoning: str = "",
        duration_ms: int = 0,
        status: str = "success",
        error_details: Optional[str] = None,
    ) -> Optional[str]:
        """
        Log agent action to the audit table.

        Creates an immutable audit record in PostgreSQL for
        compliance, debugging, and traceability.

        Args:
            action: Type of action performed
            request_id: Correlation ID for the request
            pipeline_id: Pipeline ID if applicable
            input_state: Sanitized input state
            output_state: Sanitized output state
            reasoning: Explanation of the decision
            duration_ms: Execution time in milliseconds
            status: Result status (success, error, skipped)
            error_details: Error traceback if applicable

        Returns:
            Audit log ID if successful, None otherwise
        """
        try:
            audit_entry = AgentAuditLog(
                agent_name=self.agent_name,
                action=action,
                request_id=request_id,
                pipeline_id=pipeline_id,
                input_state=input_state or {},
                output_state=output_state or {},
                decision_reasoning=reasoning,
                duration_ms=duration_ms,
                status=status,
                error_details=error_details,
            )

            # Insert into database
            audit_id = self.repo.insert_audit_log(audit_entry.model_dump())

            self._logger.debug(
                "Audit log created",
                audit_id=audit_id,
                action=action,
                status=status,
            )

            return audit_id

        except Exception as e:
            # Log but don't fail the main operation
            self._logger.warning(
                "Failed to create audit log",
                error=str(e),
                action=action,
            )
            return None

    def log_decision(
        self,
        decision: str,
        reasoning: str,
        alternatives: Optional[List[str]] = None,
        context: Optional[Dict[str, Any]] = None,
        request_id: Optional[str] = None,
    ) -> None:
        """
        Log an agent decision with reasoning.

        Use this to document important decisions made during
        agent execution for explainability.

        Args:
            decision: What was decided
            reasoning: Why this decision was made
            alternatives: Other options that were considered
            context: Additional context for the decision
            request_id: Request ID for correlation
        """
        self._agent_logger.log_decision(
            request_id=request_id or "unknown",
            decision=decision,
            reasoning=reasoning,
            alternatives=alternatives or [],
            context=context or {},
        )

    # =========================================================================
    # State Sanitization
    # =========================================================================

    def _sanitize_state(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Remove sensitive data from state before logging.

        Recursively traverses the state dictionary and redacts
        any values associated with sensitive keys.

        Args:
            state: State dictionary to sanitize

        Returns:
            New dictionary with sensitive values redacted
        """
        if not state:
            return {}

        return self._sanitize_value(state)

    def _sanitize_value(self, obj: Any, path: str = "") -> Any:
        """
        Recursively sanitize a value.

        Args:
            obj: Value to sanitize
            path: Current path in the object tree (for debugging)

        Returns:
            Sanitized value
        """
        if isinstance(obj, dict):
            return {
                k: self._redact_if_sensitive(k, v, path)
                for k, v in obj.items()
            }
        elif isinstance(obj, list):
            return [
                self._sanitize_value(item, f"{path}[{i}]")
                for i, item in enumerate(obj)
            ]
        elif isinstance(obj, (str, int, float, bool, type(None))):
            return obj
        elif hasattr(obj, "model_dump"):
            # Handle Pydantic models
            return self._sanitize_value(obj.model_dump(), path)
        elif hasattr(obj, "__dict__"):
            # Handle other objects with __dict__
            return self._sanitize_value(obj.__dict__, path)
        else:
            # Convert to string for unknown types
            return str(obj)

    def _redact_if_sensitive(self, key: str, value: Any, path: str) -> Any:
        """
        Check if a key is sensitive and redact if necessary.

        Args:
            key: Dictionary key
            value: Value to potentially redact
            path: Current path for debugging

        Returns:
            Redacted value or recursively sanitized value
        """
        key_lower = key.lower()

        # Check if key contains any sensitive patterns
        if any(sensitive in key_lower for sensitive in self.SENSITIVE_KEYS):
            return "***REDACTED***"

        # Recursively sanitize nested structures
        return self._sanitize_value(value, f"{path}.{key}")

    # =========================================================================
    # Utility Methods
    # =========================================================================

    def create_error_state(
        self,
        message: str,
        exception: Optional[Exception] = None,
    ) -> Dict[str, Any]:
        """
        Create a standardized error state dictionary.

        Args:
            message: Error description
            exception: Optional exception that caused the error

        Returns:
            Error state dictionary for return from execute()
        """
        error_msg = message
        if exception:
            error_msg = f"{message}: {str(exception)}"

        return {
            "error_message": error_msg,
            "error_agent": self.agent_name,
            "current_phase": "failed",
        }

    def validate_required_state_keys(
        self,
        state: Dict[str, Any],
        required_keys: List[str],
    ) -> Optional[Dict[str, Any]]:
        """
        Validate that required keys are present in state.

        Args:
            state: State dictionary to validate
            required_keys: List of required key names

        Returns:
            None if valid, error state dictionary if invalid
        """
        missing = [key for key in required_keys if key not in state or state[key] is None]

        if missing:
            return self.create_error_state(
                f"Missing required state keys: {', '.join(missing)}"
            )

        return None


# =============================================================================
# Decorators for Agent Methods
# =============================================================================


def timed_execution(func: Callable) -> Callable:
    """
    Decorator to time agent method execution.

    Logs the duration of the decorated method.

    Example:
        @timed_execution
        def process_schema(self, schema: dict) -> dict:
            # Processing logic
            return result
    """
    @functools.wraps(func)
    def wrapper(self: BaseAgent, *args, **kwargs) -> Any:
        start_time = time.perf_counter()
        try:
            result = func(self, *args, **kwargs)
            duration_ms = int((time.perf_counter() - start_time) * 1000)
            self._logger.debug(
                f"{func.__name__} completed",
                duration_ms=duration_ms,
            )
            return result
        except Exception as e:
            duration_ms = int((time.perf_counter() - start_time) * 1000)
            self._logger.error(
                f"{func.__name__} failed",
                duration_ms=duration_ms,
                error=str(e),
            )
            raise
    return wrapper


def requires_state_keys(*keys: str) -> Callable:
    """
    Decorator to validate required state keys before execution.

    Example:
        @requires_state_keys("intent_json", "request_id")
        def execute(self, state: Dict[str, Any]) -> Dict[str, Any]:
            # state is guaranteed to have intent_json and request_id
            pass
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(self: BaseAgent, state: Dict[str, Any], *args, **kwargs) -> Any:
            error_state = self.validate_required_state_keys(state, list(keys))
            if error_state:
                return error_state
            return func(self, state, *args, **kwargs)
        return wrapper
    return decorator
