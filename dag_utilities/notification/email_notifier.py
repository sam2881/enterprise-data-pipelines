"""
APEX Data Agent - Email Notifier

Sends email notifications for pipeline events.
"""

import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import List, Optional


class EmailNotifier:
    """
    Email notification service.

    Sends formatted emails for:
    - Pipeline failures
    - SLA breaches
    - Data quality alerts
    - Validation failures
    """

    def __init__(
        self,
        smtp_host: Optional[str] = None,
        smtp_port: int = 587,
        smtp_user: Optional[str] = None,
        smtp_password: Optional[str] = None,
        use_tls: bool = True,
    ):
        """Initialize email notifier."""
        self.smtp_host = smtp_host or os.getenv("SMTP_HOST", "smtp.gmail.com")
        self.smtp_port = smtp_port
        self.smtp_user = smtp_user or os.getenv("SMTP_USER")
        self.smtp_password = smtp_password or os.getenv("SMTP_PASSWORD")
        self.use_tls = use_tls
        self.default_from = os.getenv("APEX_EMAIL_FROM", "apex@company.com")

    def send_alert(
        self,
        subject: str,
        body: str,
        recipients: List[str],
        html_body: Optional[str] = None,
        from_addr: Optional[str] = None,
    ) -> bool:
        """
        Send an email alert.

        Args:
            subject: Email subject
            body: Plain text body
            recipients: List of recipient email addresses
            html_body: Optional HTML body
            from_addr: Optional from address

        Returns:
            True if sent successfully
        """
        if not self.smtp_host or not recipients:
            return False

        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = from_addr or self.default_from
            msg["To"] = ", ".join(recipients)

            # Attach plain text
            msg.attach(MIMEText(body, "plain"))

            # Attach HTML if provided
            if html_body:
                msg.attach(MIMEText(html_body, "html"))

            # Send email
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                if self.use_tls:
                    server.starttls()
                if self.smtp_user and self.smtp_password:
                    server.login(self.smtp_user, self.smtp_password)
                server.sendmail(msg["From"], recipients, msg.as_string())

            return True

        except Exception as e:
            print(f"Failed to send email: {e}")
            return False

    def send_failure_alert(
        self,
        pipeline_name: str,
        task_name: str,
        error_message: str,
        execution_id: str,
        recipients: List[str],
        log_url: Optional[str] = None,
    ) -> bool:
        """Send a pipeline failure alert."""
        subject = f"[APEX ALERT] Pipeline Failed: {pipeline_name}"

        body = f"""
Pipeline Failure Alert
======================

Pipeline: {pipeline_name}
Task: {task_name}
Execution ID: {execution_id}

Error:
{error_message}

{'Log URL: ' + log_url if log_url else ''}

This is an automated alert from APEX Data Agent.
Please investigate and resolve the issue.
        """

        html_body = f"""
<html>
<body style="font-family: Arial, sans-serif;">
<h2 style="color: #d9534f;">Pipeline Failure Alert</h2>
<table style="border-collapse: collapse; width: 100%;">
    <tr>
        <td style="padding: 8px; border: 1px solid #ddd;"><strong>Pipeline</strong></td>
        <td style="padding: 8px; border: 1px solid #ddd;">{pipeline_name}</td>
    </tr>
    <tr>
        <td style="padding: 8px; border: 1px solid #ddd;"><strong>Task</strong></td>
        <td style="padding: 8px; border: 1px solid #ddd;">{task_name}</td>
    </tr>
    <tr>
        <td style="padding: 8px; border: 1px solid #ddd;"><strong>Execution ID</strong></td>
        <td style="padding: 8px; border: 1px solid #ddd;">{execution_id}</td>
    </tr>
</table>
<h3>Error Details</h3>
<pre style="background-color: #f5f5f5; padding: 10px; border-radius: 4px;">
{error_message}
</pre>
{f'<p><a href="{log_url}">View Logs</a></p>' if log_url else ''}
<hr>
<p style="color: #888; font-size: 12px;">This is an automated alert from APEX Data Agent.</p>
</body>
</html>
        """

        return self.send_alert(subject, body, recipients, html_body)

    def send_sla_breach_alert(
        self,
        pipeline_name: str,
        sla_type: str,
        expected_value: int,
        actual_value: int,
        execution_id: str,
        recipients: List[str],
    ) -> bool:
        """Send an SLA breach alert."""
        subject = f"[APEX SLA BREACH] {pipeline_name} - {sla_type}"

        body = f"""
SLA Breach Alert
================

Pipeline: {pipeline_name}
SLA Type: {sla_type}
Expected: {expected_value}
Actual: {actual_value}
Execution ID: {execution_id}

The pipeline has breached its SLA. Please investigate.
        """

        return self.send_alert(subject, body, recipients)

    def send_quality_alert(
        self,
        pipeline_name: str,
        zone: str,
        failed_rules: List[str],
        execution_id: str,
        recipients: List[str],
    ) -> bool:
        """Send a data quality alert."""
        subject = f"[APEX QUALITY] {pipeline_name} - {zone} Zone Issues"

        rules_list = "\n".join([f"  - {rule}" for rule in failed_rules])

        body = f"""
Data Quality Alert
==================

Pipeline: {pipeline_name}
Zone: {zone}
Execution ID: {execution_id}

Failed Quality Rules:
{rules_list}

Please investigate the data quality issues.
        """

        return self.send_alert(subject, body, recipients)
