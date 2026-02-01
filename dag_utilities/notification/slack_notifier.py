"""
APEX Data Agent - Slack Notifier

Sends Slack notifications for pipeline events.
"""

import os
import json
from typing import Dict, Any, List, Optional
import urllib.request


class SlackNotifier:
    """
    Slack notification service.

    Sends formatted Slack messages for pipeline events.
    Uses Slack webhooks for simplicity.
    """

    def __init__(self, webhook_url: Optional[str] = None):
        """Initialize Slack notifier."""
        self.webhook_url = webhook_url or os.getenv("SLACK_WEBHOOK_URL")

    def send_message(
        self,
        text: str,
        channel: Optional[str] = None,
        username: str = "APEX Agent",
        icon_emoji: str = ":robot_face:",
        attachments: Optional[List[Dict[str, Any]]] = None,
    ) -> bool:
        """
        Send a Slack message.

        Args:
            text: Message text
            channel: Optional channel override
            username: Bot username
            icon_emoji: Bot emoji
            attachments: Optional rich attachments

        Returns:
            True if sent successfully
        """
        if not self.webhook_url:
            return False

        payload = {
            "text": text,
            "username": username,
            "icon_emoji": icon_emoji,
        }

        if channel:
            payload["channel"] = channel
        if attachments:
            payload["attachments"] = attachments

        try:
            data = json.dumps(payload).encode("utf-8")
            req = urllib.request.Request(
                self.webhook_url,
                data=data,
                headers={"Content-Type": "application/json"},
            )
            urllib.request.urlopen(req)
            return True
        except Exception as e:
            print(f"Failed to send Slack message: {e}")
            return False

    def send_failure_alert(
        self,
        pipeline_name: str,
        task_name: str,
        error_message: str,
        execution_id: str,
        log_url: Optional[str] = None,
    ) -> bool:
        """Send a pipeline failure alert to Slack."""
        attachments = [
            {
                "color": "danger",
                "title": f"Pipeline Failed: {pipeline_name}",
                "fields": [
                    {"title": "Task", "value": task_name, "short": True},
                    {"title": "Execution ID", "value": execution_id, "short": True},
                ],
                "text": f"```{error_message[:500]}```",
                "footer": "APEX Data Agent",
            }
        ]

        if log_url:
            attachments[0]["actions"] = [
                {
                    "type": "button",
                    "text": "View Logs",
                    "url": log_url,
                }
            ]

        return self.send_message(
            text=f":red_circle: *Pipeline Failed*: {pipeline_name}",
            attachments=attachments,
        )

    def send_success_notification(
        self,
        pipeline_name: str,
        execution_id: str,
        records_processed: int,
        duration_minutes: float,
    ) -> bool:
        """Send a pipeline success notification."""
        attachments = [
            {
                "color": "good",
                "title": f"Pipeline Completed: {pipeline_name}",
                "fields": [
                    {"title": "Records Processed", "value": f"{records_processed:,}", "short": True},
                    {"title": "Duration", "value": f"{duration_minutes:.1f} min", "short": True},
                    {"title": "Execution ID", "value": execution_id, "short": False},
                ],
                "footer": "APEX Data Agent",
            }
        ]

        return self.send_message(
            text=f":white_check_mark: *Pipeline Completed*: {pipeline_name}",
            attachments=attachments,
        )

    def send_sla_breach_alert(
        self,
        pipeline_name: str,
        sla_type: str,
        expected_value: int,
        actual_value: int,
        severity: str = "WARNING",
    ) -> bool:
        """Send an SLA breach alert."""
        color = "warning" if severity == "WARNING" else "danger"
        emoji = ":warning:" if severity == "WARNING" else ":rotating_light:"

        attachments = [
            {
                "color": color,
                "title": f"SLA Breach: {pipeline_name}",
                "fields": [
                    {"title": "SLA Type", "value": sla_type, "short": True},
                    {"title": "Severity", "value": severity, "short": True},
                    {"title": "Expected", "value": str(expected_value), "short": True},
                    {"title": "Actual", "value": str(actual_value), "short": True},
                ],
                "footer": "APEX Data Agent",
            }
        ]

        return self.send_message(
            text=f"{emoji} *SLA Breach*: {pipeline_name}",
            attachments=attachments,
        )
