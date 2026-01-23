"""
Unified Base Agent Class

WHY: Single base class that ALL agents must extend.
     Combines best practices from data_agent and servicenow_agent:
     - Async-first execution (from data_agent)
     - Determinism tracking with input/output hashing (from data_agent)
     - Retry with exponential backoff (from data_agent)
     - Structured logging and observability (from servicenow_agent)
     - Langfuse integration for LLM tracing (from servicenow_agent)

HOW: Extend BaseAgent and implement:
     - _execute(): Your agent's core logic
     - agent_id, agent_name, capabilities properties

USAGE:
    class MyAgent(BaseAgent):
        @property
        def agent_id(self) -> str:
            return "my-agent"

        @property
        def agent_name(self) -> str:
            return "My Agent"

        @property
        def capabilities(self) -> List[AgentCapability]:
            return [AgentCapability.INCIDENT_TRIAGE]

        async def _execute(self, task: IAgentTask) -> Dict[str, Any]:
            # Your logic here
            return {"result": "success"}
"""
import asyncio
import hashlib
import json
import time
import traceback
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Optional
import structlog

from .interfaces import (
    IAgentService,
    IAgentTask,
    IAgentResult,
    AgentCapability,
)
from .config import AgentConfig

logger = structlog.get_logger()


class BaseAgent(IAgentService, ABC):
    """
    Unified base class for all agents in the platform.

    DESIGN DECISIONS:
    1. Async-first: All execution is async for consistency
    2. Deterministic: Input/output hashing for reproducibility
    3. Observable: Structured logging + optional Langfuse
    4. Resilient: Built-in retry with exponential backoff
    5. Configurable: Uses AgentConfig for all settings

    LIFECYCLE:
    1. execute() called with IAgentTask
    2. Pre-execution: hash input, start timing, log
    3. _execute() called (implemented by subclass)
    4. Post-execution: hash output, stop timing, log
    5. Return IAgentResult with all metadata
    """

    def __init__(self, config: Optional[AgentConfig] = None):
        """
        Initialize base agent.

        Args:
            config: Agent configuration. If None, loads from environment.
        """
        self.config = config or AgentConfig.from_environment(
            agent_id=self.agent_id,
            agent_name=self.agent_name,
        )
        self._initialized_at = datetime.utcnow()
        self._execution_count = 0
        self._error_count = 0

        logger.info(
            "agent_initialized",
            agent_id=self.agent_id,
            agent_name=self.agent_name,
            version=self.version,
            capabilities=[c.value for c in self.capabilities],
        )

    @property
    @abstractmethod
    def agent_id(self) -> str:
        """Unique identifier for this agent"""
        pass

    @property
    @abstractmethod
    def agent_name(self) -> str:
        """Human-readable name"""
        pass

    @property
    @abstractmethod
    def capabilities(self) -> List[AgentCapability]:
        """List of capabilities this agent provides"""
        pass

    @abstractmethod
    async def _execute(self, task: IAgentTask) -> Dict[str, Any]:
        """
        Execute the agent's core logic.

        Override this method with your agent's specific implementation.
        This is called by execute() with proper error handling and logging.

        Args:
            task: The task to execute

        Returns:
            Dict with the agent's output
        """
        pass

    async def execute(self, task: IAgentTask) -> IAgentResult:
        """
        Execute a task with full instrumentation.

        This is the main entry point. It wraps _execute() with:
        - Input hashing for determinism
        - Timing measurement
        - Error handling with retry
        - Output hashing
        - Structured logging

        Args:
            task: The task to execute

        Returns:
            IAgentResult with full execution details
        """
        start_time = time.time()
        self._execution_count += 1
        input_hash = task.compute_hash()

        logger.info(
            "agent_execution_started",
            agent_id=self.agent_id,
            task_id=task.task_id,
            task_type=task.task_type,
            input_hash=input_hash,
            execution_number=self._execution_count,
        )

        try:
            # Execute with retry
            output = await self._execute_with_retry(task)

            # Compute output hash for determinism
            output_hash = self._compute_output_hash(output)

            execution_time_ms = int((time.time() - start_time) * 1000)

            logger.info(
                "agent_execution_completed",
                agent_id=self.agent_id,
                task_id=task.task_id,
                input_hash=input_hash,
                output_hash=output_hash,
                execution_time_ms=execution_time_ms,
                success=True,
            )

            return IAgentResult(
                task_id=task.task_id,
                success=True,
                output=output,
                execution_time_ms=execution_time_ms,
                input_hash=input_hash,
                output_hash=output_hash,
                metadata={
                    "agent_id": self.agent_id,
                    "agent_version": self.version,
                    "execution_number": self._execution_count,
                },
            )

        except Exception as e:
            self._error_count += 1
            execution_time_ms = int((time.time() - start_time) * 1000)

            logger.error(
                "agent_execution_failed",
                agent_id=self.agent_id,
                task_id=task.task_id,
                error=str(e),
                error_type=type(e).__name__,
                execution_time_ms=execution_time_ms,
                traceback=traceback.format_exc(),
            )

            return IAgentResult(
                task_id=task.task_id,
                success=False,
                output={},
                error=str(e),
                error_details={
                    "error_type": type(e).__name__,
                    "traceback": traceback.format_exc(),
                },
                execution_time_ms=execution_time_ms,
                input_hash=input_hash,
                metadata={
                    "agent_id": self.agent_id,
                    "error_count": self._error_count,
                },
            )

    async def _execute_with_retry(self, task: IAgentTask) -> Dict[str, Any]:
        """
        Execute with exponential backoff retry.

        WHY: LLM calls and external APIs can fail transiently.
             Retry with backoff improves reliability.
        """
        last_exception = None
        retry_config = self.config.retry

        for attempt in range(retry_config.max_retries + 1):
            try:
                return await self._execute(task)

            except Exception as e:
                last_exception = e
                error_type = type(e).__name__

                # Check if this error should be retried
                should_retry = any(
                    err_type in error_type
                    for err_type in retry_config.retry_on_errors
                )

                if not should_retry or attempt >= retry_config.max_retries:
                    raise

                # Calculate delay with exponential backoff
                delay = min(
                    retry_config.initial_delay_seconds * (retry_config.exponential_base ** attempt),
                    retry_config.max_delay_seconds
                )

                logger.warning(
                    "agent_execution_retry",
                    agent_id=self.agent_id,
                    task_id=task.task_id,
                    attempt=attempt + 1,
                    max_retries=retry_config.max_retries,
                    delay_seconds=delay,
                    error=str(e),
                )

                await asyncio.sleep(delay)

        # Should not reach here, but raise last exception if we do
        if last_exception:
            raise last_exception
        raise RuntimeError("Unexpected retry loop exit")

    def _compute_output_hash(self, output: Dict[str, Any]) -> str:
        """
        Compute deterministic hash of output.

        WHY: For reproducibility verification. Same inputs should
             produce same outputs (when using temperature=0).
        """
        json_str = json.dumps(output, sort_keys=True, default=str)
        return hashlib.sha256(json_str.encode()).hexdigest()[:16]

    async def health_check(self) -> Dict[str, Any]:
        """Check agent health and status"""
        uptime_seconds = (datetime.utcnow() - self._initialized_at).total_seconds()

        return {
            "agent_id": self.agent_id,
            "agent_name": self.agent_name,
            "version": self.version,
            "status": "healthy",
            "capabilities": [c.value for c in self.capabilities],
            "stats": {
                "uptime_seconds": uptime_seconds,
                "execution_count": self._execution_count,
                "error_count": self._error_count,
                "error_rate": self._error_count / max(1, self._execution_count),
            },
            "config": {
                "environment": self.config.environment,
                "llm_provider": self.config.llm.provider.value,
                "llm_model": self.config.llm.model,
            },
        }

    def can_handle(self, task_type: str) -> bool:
        """Check if this agent can handle a task type"""
        return any(
            task_type.lower() in cap.value.lower()
            for cap in self.capabilities
        )

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}(id={self.agent_id}, caps={[c.value for c in self.capabilities]})>"
