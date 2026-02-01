"""
APEX Data Agent - Self Healer

Automated remediation for common pipeline issues.
"""

from typing import Dict, Any, Optional, Callable
from datetime import datetime
import traceback

from dag_utilities.logging.audit_logger import AuditLogger
from dag_utilities.notification.email_notifier import EmailNotifier
from dag_utilities.notification.slack_notifier import SlackNotifier


class SelfHealer:
    """
    Self-healing system for APEX pipelines.

    Handles:
    - Transient failures (automatic retry)
    - Resource issues (scale up/down)
    - Schema mismatches (evolution attempt)
    - Missing files (wait and retry)
    """

    # Remediation strategies for different error types
    STRATEGIES = {
        "TRANSIENT_ERROR": "retry",
        "RESOURCE_EXHAUSTED": "scale_up",
        "FILE_NOT_FOUND": "wait_and_retry",
        "SCHEMA_MISMATCH": "attempt_evolution",
        "VALIDATION_FAILED": "quarantine",
        "TIMEOUT": "extend_timeout",
    }

    def __init__(self):
        """Initialize self healer."""
        self.audit_logger = AuditLogger()
        self.email_notifier = EmailNotifier()
        self.slack_notifier = SlackNotifier()
        self._remediation_count: Dict[str, int] = {}

    def on_task_failure(self, context: Dict[str, Any]) -> None:
        """
        Airflow callback for task failure.

        Analyzes the failure and attempts remediation.
        """
        task_instance = context.get("task_instance")
        exception = context.get("exception")
        execution_id = context.get("ti").xcom_pull(key="execution_id")

        # Classify the error
        error_type = self._classify_error(exception)

        # Get remediation strategy
        strategy = self.STRATEGIES.get(error_type, "escalate")

        # Log the failure
        self.audit_logger.log_error(
            execution_id=execution_id,
            task_exec_id=None,
            error_type=error_type,
            error_message=str(exception),
            stack_trace=traceback.format_exc(),
            error_context={
                "task_id": task_instance.task_id if task_instance else None,
                "strategy": strategy,
            },
        )

        # Attempt remediation
        remediated = self._attempt_remediation(
            error_type=error_type,
            strategy=strategy,
            context=context,
            execution_id=execution_id,
        )

        if not remediated:
            # Escalate if remediation failed
            self._escalate(context, error_type, exception)

    def _classify_error(self, exception: Exception) -> str:
        """Classify an exception into an error type."""
        if exception is None:
            return "UNKNOWN"

        error_message = str(exception).lower()

        # Transient errors
        if any(term in error_message for term in [
            "connection reset", "timeout", "temporary", "retry",
            "service unavailable", "rate limit"
        ]):
            return "TRANSIENT_ERROR"

        # Resource errors
        if any(term in error_message for term in [
            "out of memory", "oom", "resource exhausted",
            "container killed", "executor lost"
        ]):
            return "RESOURCE_EXHAUSTED"

        # File errors
        if any(term in error_message for term in [
            "file not found", "no such file", "path does not exist"
        ]):
            return "FILE_NOT_FOUND"

        # Schema errors
        if any(term in error_message for term in [
            "schema mismatch", "column not found", "type mismatch"
        ]):
            return "SCHEMA_MISMATCH"

        # Validation errors
        if any(term in error_message for term in [
            "validation failed", "quality check failed", "constraint violated"
        ]):
            return "VALIDATION_FAILED"

        # Timeout
        if "timeout" in error_message or "timed out" in error_message:
            return "TIMEOUT"

        return "UNKNOWN"

    def _attempt_remediation(
        self,
        error_type: str,
        strategy: str,
        context: Dict[str, Any],
        execution_id: str,
    ) -> bool:
        """Attempt remediation based on strategy."""
        # Track remediation attempts
        key = f"{execution_id}:{error_type}"
        self._remediation_count[key] = self._remediation_count.get(key, 0) + 1

        # Don't retry indefinitely
        if self._remediation_count[key] > 3:
            return False

        # Log remediation attempt
        self.audit_logger.log_agent_decision(
            decision_type="REMEDIATION_ATTEMPT",
            input_context={"error_type": error_type, "attempt": self._remediation_count[key]},
            decision_made=strategy,
            decision_rationale=f"Attempting {strategy} for {error_type}",
            execution_id=execution_id,
        )

        if strategy == "retry":
            return True  # Airflow will handle retry

        elif strategy == "scale_up":
            # Attempt to increase resources
            return self._scale_up_resources(context)

        elif strategy == "wait_and_retry":
            # Wait for file to appear
            return self._wait_for_resource(context)

        elif strategy == "attempt_evolution":
            # Try schema evolution
            return self._attempt_schema_evolution(context)

        elif strategy == "quarantine":
            # Move bad data to quarantine
            return self._quarantine_data(context)

        elif strategy == "extend_timeout":
            # Suggest timeout extension
            return False  # Can't auto-fix, needs config change

        return False

    def _scale_up_resources(self, context: Dict[str, Any]) -> bool:
        """Attempt to scale up Spark resources."""
        # This would integrate with cluster manager
        # For now, just log the suggestion
        return False

    def _wait_for_resource(self, context: Dict[str, Any]) -> bool:
        """Wait for a missing resource to appear."""
        import time
        # Wait up to 5 minutes for file
        for _ in range(10):
            time.sleep(30)
            # Check if resource exists now
            # If yes, return True
        return False

    def _attempt_schema_evolution(self, context: Dict[str, Any]) -> bool:
        """Attempt automatic schema evolution."""
        # This would check if evolution is safe
        # (only additive changes) and apply it
        return False

    def _quarantine_data(self, context: Dict[str, Any]) -> bool:
        """Move failing data to quarantine location."""
        # Move data to rejected path
        return False

    def _escalate(
        self,
        context: Dict[str, Any],
        error_type: str,
        exception: Exception,
    ) -> None:
        """Escalate failure to humans."""
        task_instance = context.get("task_instance")
        dag = context.get("dag")

        pipeline_name = dag.dag_id if dag else "Unknown"
        task_name = task_instance.task_id if task_instance else "Unknown"
        execution_id = context.get("ti").xcom_pull(key="execution_id") or "Unknown"

        # Send email alert
        self.email_notifier.send_failure_alert(
            pipeline_name=pipeline_name,
            task_name=task_name,
            error_message=str(exception),
            execution_id=execution_id,
            recipients=self._get_notification_recipients(context),
        )

        # Send Slack alert
        self.slack_notifier.send_failure_alert(
            pipeline_name=pipeline_name,
            task_name=task_name,
            error_message=str(exception),
            execution_id=execution_id,
        )

    def _get_notification_recipients(
        self,
        context: Dict[str, Any],
    ) -> list:
        """Get notification recipients from DAG config."""
        dag = context.get("dag")
        if dag and dag.default_args:
            email = dag.default_args.get("email", [])
            if isinstance(email, str):
                return [email]
            return email
        return []
