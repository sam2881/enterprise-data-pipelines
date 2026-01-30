"""
APEX Data Agent - Incident Manager

Creates and manages incidents for pipeline issues.
"""

import uuid
from datetime import datetime
from typing import Dict, Any, Optional, List

from dag_utilities.core.metadata_client import MetadataClient
from dag_utilities.notification.email_notifier import EmailNotifier
from dag_utilities.notification.slack_notifier import SlackNotifier


class IncidentManager:
    """
    Manages incidents for pipeline failures.

    Features:
    - Incident creation and tracking
    - Severity classification
    - Escalation management
    - SLA tracking
    """

    # Severity levels
    SEVERITY_LEVELS = {
        "P1": {"name": "Critical", "response_time_minutes": 15},
        "P2": {"name": "High", "response_time_minutes": 60},
        "P3": {"name": "Medium", "response_time_minutes": 240},
        "P4": {"name": "Low", "response_time_minutes": 1440},
    }

    def __init__(self):
        """Initialize incident manager."""
        self.metadata_client = MetadataClient()
        self.email_notifier = EmailNotifier()
        self.slack_notifier = SlackNotifier()

    def create_incident(
        self,
        title: str,
        description: str,
        severity: str,
        execution_id: str,
        feed_id: str,
        error_type: str,
        error_message: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Create a new incident.

        Args:
            title: Incident title
            description: Detailed description
            severity: P1, P2, P3, or P4
            execution_id: Pipeline execution ID
            feed_id: Feed ID
            error_type: Type of error
            error_message: Error message
            metadata: Additional metadata

        Returns:
            Created incident details
        """
        incident_id = str(uuid.uuid4())

        incident = {
            "incident_id": incident_id,
            "title": title,
            "description": description,
            "severity": severity,
            "status": "OPEN",
            "execution_id": execution_id,
            "feed_id": feed_id,
            "error_type": error_type,
            "error_message": error_message,
            "metadata": metadata or {},
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
        }

        # Store incident (in production, would go to incident tracking system)
        self._store_incident(incident)

        # Notify based on severity
        self._notify_incident(incident)

        return incident

    def create_from_failure(
        self,
        execution_id: str,
        feed_id: str,
        error_type: str,
        error_message: str,
        context: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Create incident from a pipeline failure."""
        # Determine severity based on error type and feed importance
        severity = self._determine_severity(error_type, feed_id)

        title = f"[{severity}] Pipeline failure: {context.get('feed_name', feed_id)}"
        description = f"""
Pipeline execution failed.

Feed: {context.get('feed_name', feed_id)}
Error Type: {error_type}
Error: {error_message}

Execution ID: {execution_id}
Task: {context.get('task_id', 'Unknown')}
"""

        return self.create_incident(
            title=title,
            description=description,
            severity=severity,
            execution_id=execution_id,
            feed_id=feed_id,
            error_type=error_type,
            error_message=error_message,
            metadata=context,
        )

    def update_incident(
        self,
        incident_id: str,
        status: Optional[str] = None,
        resolution: Optional[str] = None,
        notes: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Update an existing incident."""
        # In production, would update incident tracking system
        return {
            "incident_id": incident_id,
            "status": status,
            "resolution": resolution,
            "notes": notes,
            "updated_at": datetime.utcnow().isoformat(),
        }

    def close_incident(
        self,
        incident_id: str,
        resolution: str,
        root_cause: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Close an incident with resolution."""
        return self.update_incident(
            incident_id=incident_id,
            status="CLOSED",
            resolution=resolution,
            notes=f"Root cause: {root_cause}" if root_cause else None,
        )

    def _determine_severity(
        self,
        error_type: str,
        feed_id: str,
    ) -> str:
        """Determine incident severity based on error and feed."""
        # Critical errors
        if error_type in ["DATA_CORRUPTION", "SECURITY_VIOLATION"]:
            return "P1"

        # High severity for production feeds
        # (would check feed metadata for environment/criticality)

        # Medium for most errors
        if error_type in ["VALIDATION_FAILED", "SCHEMA_MISMATCH"]:
            return "P3"

        # Default to P3
        return "P3"

    def _store_incident(self, incident: Dict[str, Any]) -> None:
        """Store incident (placeholder for integration)."""
        # In production, would store in:
        # - ServiceNow
        # - PagerDuty
        # - Jira
        # - Custom incident database
        pass

    def _notify_incident(self, incident: Dict[str, Any]) -> None:
        """Send notifications for incident."""
        severity = incident["severity"]

        # P1 and P2 get immediate notifications
        if severity in ["P1", "P2"]:
            # Slack notification
            self.slack_notifier.send_message(
                text=f":rotating_light: *{severity} Incident Created*\n{incident['title']}",
                attachments=[{
                    "color": "danger" if severity == "P1" else "warning",
                    "fields": [
                        {"title": "Severity", "value": severity, "short": True},
                        {"title": "Status", "value": incident["status"], "short": True},
                        {"title": "Error Type", "value": incident["error_type"], "short": True},
                    ],
                    "text": incident["description"][:500],
                }],
            )

        # Email for all severities
        self.email_notifier.send_alert(
            subject=incident["title"],
            body=incident["description"],
            recipients=self._get_oncall_team(severity),
        )

    def _get_oncall_team(self, severity: str) -> List[str]:
        """Get on-call team for severity level."""
        # In production, would query PagerDuty or on-call schedule
        return ["data-oncall@company.com"]

    def check_sla_breach(
        self,
        execution_id: str,
        feed_id: str,
    ) -> Optional[Dict[str, Any]]:
        """Check if execution breached SLA."""
        # Get SLA definition
        query = """
            SELECT sla_id, sla_type, target_value, warning_threshold, critical_threshold
            FROM sla_definition
            WHERE feed_id = %s AND is_active = true
        """

        with self.metadata_client.conn.cursor() as cur:
            cur.execute(query, [feed_id])
            slas = cur.fetchall()

        # Get execution
        query = """
            SELECT start_ts, end_ts, status
            FROM pipeline_execution
            WHERE execution_id = %s
        """

        with self.metadata_client.conn.cursor() as cur:
            cur.execute(query, [execution_id])
            execution = cur.fetchone()

        if not execution or not slas:
            return None

        # Check each SLA
        for sla in slas:
            if sla["sla_type"] == "COMPLETION_TIME":
                if execution["end_ts"] and execution["start_ts"]:
                    duration_minutes = (
                        execution["end_ts"] - execution["start_ts"]
                    ).total_seconds() / 60

                    if duration_minutes > sla["critical_threshold"]:
                        return self._create_sla_breach(
                            sla, execution_id, feed_id,
                            sla["target_value"], int(duration_minutes), "CRITICAL"
                        )
                    elif duration_minutes > sla["warning_threshold"]:
                        return self._create_sla_breach(
                            sla, execution_id, feed_id,
                            sla["target_value"], int(duration_minutes), "WARNING"
                        )

        return None

    def _create_sla_breach(
        self,
        sla: Dict[str, Any],
        execution_id: str,
        feed_id: str,
        expected: int,
        actual: int,
        breach_type: str,
    ) -> Dict[str, Any]:
        """Create SLA breach record."""
        breach_id = str(uuid.uuid4())

        query = """
            INSERT INTO sla_breach_log (
                breach_id, sla_id, execution_id, breach_type,
                expected_value, actual_value, breach_message, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        breach_message = f"SLA breach: expected {expected}, actual {actual}"

        with self.metadata_client.conn.cursor() as cur:
            cur.execute(query, [
                breach_id,
                sla["sla_id"],
                execution_id,
                breach_type,
                expected,
                actual,
                breach_message,
                datetime.utcnow(),
            ])
            self.metadata_client.conn.commit()

        return {
            "breach_id": breach_id,
            "breach_type": breach_type,
            "expected": expected,
            "actual": actual,
        }
