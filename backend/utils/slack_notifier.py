"""
Slack Webhook Notification Service
===================================

Sends notifications to Slack for important events:
- Incident assignments
- Script execution status
- Approval requests
- Circuit breaker alerts
- Error notifications

Author: AI Agent Platform
Version: 4.0.0
"""

import os
import json
import asyncio
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, asdict
from enum import Enum
from datetime import datetime
import httpx
import structlog

logger = structlog.get_logger()


class NotificationType(Enum):
    """Types of notifications"""
    INCIDENT_NEW = "incident_new"
    INCIDENT_RESOLVED = "incident_resolved"
    EXECUTION_STARTED = "execution_started"
    EXECUTION_SUCCESS = "execution_success"
    EXECUTION_FAILED = "execution_failed"
    APPROVAL_NEEDED = "approval_needed"
    APPROVAL_GRANTED = "approval_granted"
    APPROVAL_REJECTED = "approval_rejected"
    CIRCUIT_BREAKER_OPEN = "circuit_breaker_open"
    CIRCUIT_BREAKER_CLOSED = "circuit_breaker_closed"
    ERROR_ALERT = "error_alert"


@dataclass
class SlackConfig:
    """Slack configuration"""
    webhook_url: str = ""
    channel: str = "#incidents"
    username: str = "AI Agent Platform"
    icon_emoji: str = ":robot_face:"
    enabled: bool = True

    @classmethod
    def from_env(cls) -> "SlackConfig":
        """Load configuration from environment"""
        return cls(
            webhook_url=os.getenv("SLACK_WEBHOOK_URL", ""),
            channel=os.getenv("SLACK_CHANNEL", "#incidents"),
            username=os.getenv("SLACK_USERNAME", "AI Agent Platform"),
            icon_emoji=os.getenv("SLACK_ICON", ":robot_face:"),
            enabled=os.getenv("SLACK_ENABLED", "true").lower() == "true"
        )


class SlackNotifier:
    """
    Slack notification service for AI Agent Platform.

    Sends formatted messages to Slack webhooks with:
    - Rich formatting with blocks
    - Color-coded messages by severity
    - Action buttons for approvals
    - Thread support for updates
    """

    # Color codes for different message types
    COLORS = {
        NotificationType.INCIDENT_NEW: "#FF6B6B",        # Red
        NotificationType.INCIDENT_RESOLVED: "#4ECDC4",   # Teal
        NotificationType.EXECUTION_STARTED: "#45B7D1",   # Blue
        NotificationType.EXECUTION_SUCCESS: "#95E66D",   # Green
        NotificationType.EXECUTION_FAILED: "#FF6B6B",    # Red
        NotificationType.APPROVAL_NEEDED: "#FFE66D",     # Yellow
        NotificationType.APPROVAL_GRANTED: "#95E66D",    # Green
        NotificationType.APPROVAL_REJECTED: "#FF6B6B",   # Red
        NotificationType.CIRCUIT_BREAKER_OPEN: "#FF6B6B",# Red
        NotificationType.CIRCUIT_BREAKER_CLOSED: "#95E66D", # Green
        NotificationType.ERROR_ALERT: "#FF6B6B",         # Red
    }

    EMOJI = {
        NotificationType.INCIDENT_NEW: ":rotating_light:",
        NotificationType.INCIDENT_RESOLVED: ":white_check_mark:",
        NotificationType.EXECUTION_STARTED: ":rocket:",
        NotificationType.EXECUTION_SUCCESS: ":tada:",
        NotificationType.EXECUTION_FAILED: ":x:",
        NotificationType.APPROVAL_NEEDED: ":hourglass:",
        NotificationType.APPROVAL_GRANTED: ":thumbsup:",
        NotificationType.APPROVAL_REJECTED: ":thumbsdown:",
        NotificationType.CIRCUIT_BREAKER_OPEN: ":warning:",
        NotificationType.CIRCUIT_BREAKER_CLOSED: ":green_circle:",
        NotificationType.ERROR_ALERT: ":fire:",
    }

    def __init__(self, config: Optional[SlackConfig] = None):
        self.config = config or SlackConfig.from_env()
        self._message_cache: Dict[str, str] = {}  # For thread tracking

        if self.config.enabled and not self.config.webhook_url:
            logger.warning("slack_webhook_url_not_set")
            self.config.enabled = False

        logger.info(
            "slack_notifier_initialized",
            enabled=self.config.enabled,
            channel=self.config.channel
        )

    async def send(
        self,
        notification_type: NotificationType,
        title: str,
        message: str,
        fields: Optional[Dict[str, str]] = None,
        thread_key: Optional[str] = None,
        actions: Optional[List[Dict]] = None
    ) -> bool:
        """
        Send a notification to Slack.

        Args:
            notification_type: Type of notification
            title: Message title
            message: Main message body
            fields: Optional key-value pairs to display
            thread_key: Optional key for threading related messages
            actions: Optional action buttons

        Returns:
            True if sent successfully
        """
        if not self.config.enabled:
            logger.debug("slack_notification_disabled", title=title)
            return False

        try:
            payload = self._build_payload(
                notification_type, title, message, fields, actions
            )

            async with httpx.AsyncClient() as client:
                response = await client.post(
                    self.config.webhook_url,
                    json=payload,
                    timeout=10
                )

                if response.status_code == 200:
                    logger.info(
                        "slack_notification_sent",
                        type=notification_type.value,
                        title=title
                    )
                    return True
                else:
                    logger.warning(
                        "slack_notification_failed",
                        status=response.status_code,
                        response=response.text
                    )
                    return False

        except Exception as e:
            logger.error("slack_notification_error", error=str(e))
            return False

    def _build_payload(
        self,
        notification_type: NotificationType,
        title: str,
        message: str,
        fields: Optional[Dict[str, str]] = None,
        actions: Optional[List[Dict]] = None
    ) -> Dict:
        """Build Slack message payload with blocks"""
        emoji = self.EMOJI.get(notification_type, ":bell:")
        color = self.COLORS.get(notification_type, "#808080")

        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{emoji} {title}",
                    "emoji": True
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": message
                }
            }
        ]

        # Add fields if provided
        if fields:
            field_blocks = []
            for key, value in fields.items():
                field_blocks.append({
                    "type": "mrkdwn",
                    "text": f"*{key}:*\n{value}"
                })

            # Slack limits to 10 fields per section
            for i in range(0, len(field_blocks), 2):
                blocks.append({
                    "type": "section",
                    "fields": field_blocks[i:i+2]
                })

        # Add timestamp
        blocks.append({
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f":clock1: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}"
                }
            ]
        })

        # Add actions if provided
        if actions:
            blocks.append({
                "type": "actions",
                "elements": actions
            })

        return {
            "channel": self.config.channel,
            "username": self.config.username,
            "icon_emoji": self.config.icon_emoji,
            "attachments": [
                {
                    "color": color,
                    "blocks": blocks
                }
            ]
        }

    # ==========================================================================
    # Convenience Methods
    # ==========================================================================

    async def notify_incident_new(
        self,
        incident_id: str,
        title: str,
        severity: str,
        source: str,
        description: str = ""
    ):
        """Notify about new incident"""
        await self.send(
            NotificationType.INCIDENT_NEW,
            f"New Incident: {incident_id}",
            description or title,
            fields={
                "Incident ID": incident_id,
                "Severity": severity.upper(),
                "Source": source,
                "Title": title
            }
        )

    async def notify_incident_resolved(
        self,
        incident_id: str,
        script_used: str,
        resolution_time: float
    ):
        """Notify about resolved incident"""
        await self.send(
            NotificationType.INCIDENT_RESOLVED,
            f"Incident Resolved: {incident_id}",
            f"Successfully resolved using *{script_used}*",
            fields={
                "Incident ID": incident_id,
                "Script Used": script_used,
                "Resolution Time": f"{resolution_time:.1f} minutes"
            }
        )

    async def notify_execution_started(
        self,
        execution_id: str,
        script_name: str,
        incident_id: str
    ):
        """Notify about execution start"""
        await self.send(
            NotificationType.EXECUTION_STARTED,
            "Script Execution Started",
            f"Executing *{script_name}* for incident {incident_id}",
            fields={
                "Execution ID": execution_id,
                "Script": script_name,
                "Incident": incident_id
            }
        )

    async def notify_execution_result(
        self,
        execution_id: str,
        script_name: str,
        success: bool,
        details: str = ""
    ):
        """Notify about execution result"""
        notification_type = NotificationType.EXECUTION_SUCCESS if success else NotificationType.EXECUTION_FAILED
        status = "completed successfully" if success else "failed"

        await self.send(
            notification_type,
            f"Execution {status.title()}",
            f"Script *{script_name}* {status}",
            fields={
                "Execution ID": execution_id,
                "Script": script_name,
                "Status": "Success" if success else "Failed",
                "Details": details or "N/A"
            }
        )

    async def notify_approval_needed(
        self,
        execution_id: str,
        script_name: str,
        risk_level: str,
        incident_id: str
    ):
        """Notify about approval request"""
        await self.send(
            NotificationType.APPROVAL_NEEDED,
            "Approval Required",
            f"Script *{script_name}* requires approval before execution",
            fields={
                "Execution ID": execution_id,
                "Script": script_name,
                "Risk Level": risk_level.upper(),
                "Incident": incident_id
            }
        )

    async def notify_circuit_breaker(self, service: str, is_open: bool):
        """Notify about circuit breaker state change"""
        if is_open:
            await self.send(
                NotificationType.CIRCUIT_BREAKER_OPEN,
                f"Circuit Breaker OPEN: {service}",
                f"The *{service}* service is experiencing issues. Requests are being rejected.",
                fields={
                    "Service": service,
                    "State": "OPEN",
                    "Action": "Requests will fail-fast until service recovers"
                }
            )
        else:
            await self.send(
                NotificationType.CIRCUIT_BREAKER_CLOSED,
                f"Circuit Breaker CLOSED: {service}",
                f"The *{service}* service has recovered. Normal operation resumed.",
                fields={
                    "Service": service,
                    "State": "CLOSED"
                }
            )

    async def notify_error(self, component: str, error: str, severity: str = "high"):
        """Notify about an error"""
        await self.send(
            NotificationType.ERROR_ALERT,
            f"Error in {component}",
            error,
            fields={
                "Component": component,
                "Severity": severity.upper(),
                "Error": error[:500]  # Truncate long errors
            }
        )

    # ==========================================================================
    # Approval Request Methods (for Human-in-the-Loop)
    # ==========================================================================

    def build_approval_message(
        self,
        incident_id: str,
        plan: Dict[str, Any],
        judge_score: Dict[str, Any],
        risk_level: str
    ) -> Dict[str, Any]:
        """
        Build a Slack message for approval request with interactive buttons.

        Args:
            incident_id: The incident ID
            plan: The remediation plan
            judge_score: LLM Judge evaluation scores
            risk_level: Risk level from control plane

        Returns:
            Slack message payload with interactive components
        """
        quality_score = judge_score.get("quality_score", 0.0)
        safety_passed = judge_score.get("safety_passed", False)
        reasoning = judge_score.get("reasoning", "N/A")

        script_id = plan.get("script_id", "unknown")
        script_path = plan.get("script_path", "N/A")
        action_type = plan.get("action_type", "script")
        confidence = plan.get("confidence", 0.0)

        # Build step summary
        steps = plan.get("steps", [])
        step_summary = "\n".join([f"  {s.get('step', i+1)}. {s.get('action', 'Unknown')}" for i, s in enumerate(steps[:5])])
        if len(steps) > 5:
            step_summary += f"\n  ... and {len(steps) - 5} more steps"

        return {
            "incident_id": incident_id,
            "text": f"Approval Required for Incident {incident_id}",
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": f":hourglass: Approval Required - {incident_id}",
                        "emoji": True
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Remediation plan requires human approval before execution.*"
                    }
                },
                {"type": "divider"},
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*Incident ID:*\n{incident_id}"},
                        {"type": "mrkdwn", "text": f"*Risk Level:*\n{risk_level.upper()}"},
                        {"type": "mrkdwn", "text": f"*Script:*\n{script_id}"},
                        {"type": "mrkdwn", "text": f"*Action Type:*\n{action_type}"},
                        {"type": "mrkdwn", "text": f"*Confidence:*\n{confidence * 100:.1f}%"},
                        {"type": "mrkdwn", "text": f"*Judge Score:*\n{quality_score:.1f}/10"},
                    ]
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Execution Steps:*\n```{step_summary}```"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Judge Reasoning:*\n{reasoning[:500]}"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Safety Check:* {'Passed' if safety_passed else 'Failed'}"
                    }
                },
                {"type": "divider"},
                {
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": f":clock1: Requested at {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}"
                        }
                    ]
                }
            ]
        }

    async def send_approval_request(
        self,
        channel: str,
        message: Dict[str, Any],
        incident_id: str,
        callback_url: str
    ) -> Dict[str, Any]:
        """
        Send an approval request to Slack with interactive buttons.

        This uses the Slack Web API (not webhooks) to send interactive messages
        that support button callbacks.

        Args:
            channel: Slack channel to send to
            message: Message payload from build_approval_message
            incident_id: Incident ID for tracking
            callback_url: URL to receive approval/rejection callbacks

        Returns:
            Response with approval_token for tracking
        """
        import uuid

        slack_bot_token = os.getenv("SLACK_BOT_TOKEN", "")

        if not slack_bot_token:
            raise ValueError("SLACK_BOT_TOKEN environment variable not set")

        # Generate approval token for callback tracking
        approval_token = str(uuid.uuid4())

        # Add interactive buttons to the message
        message["blocks"].append({
            "type": "actions",
            "block_id": f"approval_{incident_id}",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "Approve", "emoji": True},
                    "style": "primary",
                    "action_id": "approve_remediation",
                    "value": json.dumps({
                        "incident_id": incident_id,
                        "approval_token": approval_token,
                        "action": "approve",
                        "callback_url": callback_url
                    })
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "Reject", "emoji": True},
                    "style": "danger",
                    "action_id": "reject_remediation",
                    "value": json.dumps({
                        "incident_id": incident_id,
                        "approval_token": approval_token,
                        "action": "reject",
                        "callback_url": callback_url
                    })
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "View Details", "emoji": True},
                    "action_id": "view_details",
                    "url": f"{os.getenv('FRONTEND_URL', 'http://localhost:3000')}/incidents/{incident_id}"
                }
            ]
        })

        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    "https://slack.com/api/chat.postMessage",
                    headers={
                        "Authorization": f"Bearer {slack_bot_token}",
                        "Content-Type": "application/json"
                    },
                    json={
                        "channel": channel,
                        "text": message.get("text", f"Approval Required for {incident_id}"),
                        "blocks": message.get("blocks", [])
                    },
                    timeout=10
                )

                response_data = response.json()

                if not response_data.get("ok"):
                    raise ValueError(f"Slack API error: {response_data.get('error', 'Unknown error')}")

                logger.info(
                    "slack_approval_request_sent",
                    incident_id=incident_id,
                    channel=channel,
                    message_ts=response_data.get("ts")
                )

                # Store the message timestamp for later updates
                self._message_cache[approval_token] = {
                    "channel": channel,
                    "ts": response_data.get("ts"),
                    "incident_id": incident_id
                }

                return {
                    "approval_token": approval_token,
                    "message_ts": response_data.get("ts"),
                    "channel": channel
                }

        except Exception as e:
            logger.error("slack_approval_request_failed", error=str(e), incident_id=incident_id)
            raise

    async def update_approval_message(
        self,
        approval_token: str,
        approved: bool,
        approver: str
    ) -> bool:
        """
        Update the approval message after a decision is made.

        Args:
            approval_token: The approval token from send_approval_request
            approved: Whether the request was approved
            approver: Username of the person who approved/rejected

        Returns:
            True if update was successful
        """
        slack_bot_token = os.getenv("SLACK_BOT_TOKEN", "")

        if not slack_bot_token:
            logger.warning("slack_bot_token_not_set")
            return False

        message_info = self._message_cache.get(approval_token)
        if not message_info:
            logger.warning("approval_message_not_found", approval_token=approval_token)
            return False

        status = "Approved" if approved else "Rejected"
        emoji = ":white_check_mark:" if approved else ":x:"
        color = "#95E66D" if approved else "#FF6B6B"

        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    "https://slack.com/api/chat.update",
                    headers={
                        "Authorization": f"Bearer {slack_bot_token}",
                        "Content-Type": "application/json"
                    },
                    json={
                        "channel": message_info["channel"],
                        "ts": message_info["ts"],
                        "blocks": [
                            {
                                "type": "header",
                                "text": {
                                    "type": "plain_text",
                                    "text": f"{emoji} Remediation {status}",
                                    "emoji": True
                                }
                            },
                            {
                                "type": "section",
                                "text": {
                                    "type": "mrkdwn",
                                    "text": f"*Incident:* {message_info['incident_id']}\n*{status} by:* {approver}\n*Time:* {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}"
                                }
                            }
                        ],
                        "attachments": [{"color": color, "blocks": []}]
                    },
                    timeout=10
                )

                response_data = response.json()

                if response_data.get("ok"):
                    logger.info(
                        "slack_approval_message_updated",
                        incident_id=message_info["incident_id"],
                        status=status,
                        approver=approver
                    )
                    return True
                else:
                    logger.warning(
                        "slack_message_update_failed",
                        error=response_data.get("error")
                    )
                    return False

        except Exception as e:
            logger.error("slack_message_update_error", error=str(e))
            return False


# Global instance
slack_notifier = SlackNotifier()


# =============================================================================
# Async Helper for Background Notifications
# =============================================================================

def send_notification_async(
    notification_type: NotificationType,
    title: str,
    message: str,
    **kwargs
):
    """
    Send notification in background (fire-and-forget).

    Use this for non-critical notifications where you don't need to wait.
    """
    async def _send():
        try:
            await slack_notifier.send(notification_type, title, message, **kwargs)
        except Exception as e:
            logger.warning("background_notification_failed", error=str(e))

    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.create_task(_send())
        else:
            loop.run_until_complete(_send())
    except RuntimeError:
        # No event loop, create a new one
        asyncio.run(_send())
