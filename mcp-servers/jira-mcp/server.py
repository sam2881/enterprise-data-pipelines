#!/usr/bin/env python3
"""
Jira MCP Server - Event-Driven Architecture

WHY: This server follows the "MCPs sense → Kafka remembers" principle for
the Data Engineering Agent workflow.
- Polls Jira for pipeline request tickets (PRODUCER)
- Publishes pipeline.requested events to Kafka
- Consumes pipeline completion events to update Jira (CONSUMER)
- Updates ticket status and adds comments

HOW: Combines MCP tool interface with Kafka integration:
- Background poller looks for tickets with specific labels/types
- Kafka consumer listens for pipeline completion/failure events
- MCP interface available for direct tool invocation

TICKET DETECTION:
- Label: "pipeline-request" or type: "Pipeline Request"
- Must have structured JSON in description or custom field
- Only processes tickets in "To Do" or "Open" status

MUST NOT:
- Call LangGraph directly
- Execute pipeline generation logic
- Contain business logic
- Contain approval logic
"""
import os
import sys
import json
import asyncio
import logging
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

# Third-party imports
from jira import JIRA
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

# Add paths for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
backend_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'backend')
sys.path.insert(0, backend_path)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("jira-mcp")


# =============================================================================
# CONFIGURATION
# =============================================================================

@dataclass
class JiraConfig:
    """Jira connection configuration"""
    url: str = os.getenv("JIRA_URL", "https://your-org.atlassian.net")
    username: str = os.getenv("JIRA_USERNAME", "")
    api_token: str = os.getenv("JIRA_API_TOKEN", "")
    project_key: str = os.getenv("JIRA_PROJECT_KEY", "DATA")
    pipeline_label: str = os.getenv("JIRA_PIPELINE_LABEL", "pipeline-request")
    poll_interval_seconds: int = int(os.getenv("JIRA_POLL_INTERVAL", "120"))
    intent_custom_field: str = os.getenv("JIRA_INTENT_FIELD", "customfield_10100")


@dataclass
class KafkaConfig:
    """Kafka connection configuration"""
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    # Topics we PRODUCE to
    pipeline_requested_topic: str = "pipeline.requested"
    jira_tickets_topic: str = "jira.tickets"
    # Topics we CONSUME from
    pipeline_completed_topic: str = "pipeline.completed"
    pipeline_failed_topic: str = "pipeline.failed"
    pipeline_requires_approval_topic: str = "pipeline.requires_approval"
    jira_commands_topic: str = "mcp.jira.commands"
    consumer_group: str = "jira-mcp-consumer"


# =============================================================================
# EVENT SCHEMAS
# =============================================================================

def create_pipeline_requested_event(
    jira_key: str,
    intent: Dict[str, Any],
    request_id: str
) -> Dict[str, Any]:
    """Create a pipeline.requested Kafka event"""
    import uuid

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "pipeline.requested",
        "timestamp": datetime.utcnow().isoformat(),
        "correlation_id": request_id,
        "source": "jira-mcp",
        "version": "5.0",
        # Request fields
        "request_id": request_id,
        "jira_key": jira_key,
        "pipeline_identity": intent.get("pipeline_identity", {}),
        "source_config": intent.get("source_config", {}),
        "target_config": intent.get("target_config", {}),
        "schema_definition": intent.get("schema_definition", {}),
        "execution_policy": intent.get("execution_policy", {}),
    }


# =============================================================================
# JIRA CLIENT
# =============================================================================

class JiraClient:
    """Jira API client wrapper"""

    def __init__(self, config: JiraConfig):
        self.config = config
        self.client = JIRA(
            server=config.url,
            basic_auth=(config.username, config.api_token)
        )
        logger.info(f"Jira client initialized for {config.url}")

    def get_pipeline_request_tickets(self) -> List[Dict[str, Any]]:
        """
        Fetch Jira tickets that represent pipeline requests.

        Detection criteria:
        - Has label "pipeline-request" OR
        - Issue type is "Pipeline Request"
        - Status is "To Do" or "Open" or "Backlog"
        """
        jql = f'''
            project = {self.config.project_key}
            AND (labels = "{self.config.pipeline_label}" OR issuetype = "Pipeline Request")
            AND status IN ("To Do", "Open", "Backlog", "New")
            ORDER BY created DESC
        '''

        try:
            issues = self.client.search_issues(jql, maxResults=50)
            logger.info(f"Found {len(issues)} pipeline request ticket(s)")

            results = []
            for issue in issues:
                ticket = {
                    "key": issue.key,
                    "summary": issue.fields.summary,
                    "description": issue.fields.description or "",
                    "status": issue.fields.status.name,
                    "created": str(issue.fields.created),
                    "reporter": issue.fields.reporter.displayName if issue.fields.reporter else "Unknown",
                    "labels": [l for l in issue.fields.labels],
                }

                # Try to get intent from custom field
                if hasattr(issue.fields, self.config.intent_custom_field):
                    custom_value = getattr(issue.fields, self.config.intent_custom_field)
                    if custom_value:
                        ticket["intent_json"] = custom_value

                results.append(ticket)

            return results

        except Exception as e:
            logger.error(f"Error fetching pipeline tickets: {e}")
            return []

    def extract_intent_from_ticket(self, ticket: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        """
        Extract pipeline intent JSON from ticket.

        Looks for:
        1. Custom field "intent_json"
        2. JSON block in description (```json ... ```)
        3. JSON block without markdown markers

        Returns: (intent_dict, error_message)
        """
        # Check custom field first
        if ticket.get("intent_json"):
            try:
                if isinstance(ticket["intent_json"], str):
                    return json.loads(ticket["intent_json"]), None
                return ticket["intent_json"], None
            except json.JSONDecodeError as e:
                return None, f"Invalid JSON in custom field: {e}"

        # Try to extract from description
        description = ticket.get("description", "")

        # Look for ```json ... ``` block
        json_match = re.search(r'```json\s*([\s\S]*?)\s*```', description)
        if json_match:
            try:
                return json.loads(json_match.group(1)), None
            except json.JSONDecodeError as e:
                return None, f"Invalid JSON in description block: {e}"

        # Look for { ... } block
        json_match = re.search(r'\{[\s\S]*\}', description)
        if json_match:
            try:
                return json.loads(json_match.group(0)), None
            except json.JSONDecodeError as e:
                return None, f"Could not parse JSON from description: {e}"

        return None, "No pipeline intent JSON found in ticket"

    def transition_to_in_progress(self, issue_key: str) -> bool:
        """Move ticket to In Progress"""
        try:
            transitions = self.client.transitions(issue_key)
            for t in transitions:
                if t['name'].lower() in ['in progress', 'start progress', 'processing']:
                    self.client.transition_issue(issue_key, t['id'])
                    logger.info(f"Transitioned {issue_key} to In Progress")
                    return True
            logger.warning(f"No 'In Progress' transition found for {issue_key}")
            return False
        except Exception as e:
            logger.error(f"Error transitioning {issue_key}: {e}")
            return False

    def transition_to_done(self, issue_key: str) -> bool:
        """Move ticket to Done"""
        try:
            transitions = self.client.transitions(issue_key)
            for t in transitions:
                if t['name'].lower() in ['done', 'complete', 'completed', 'resolved']:
                    self.client.transition_issue(issue_key, t['id'])
                    logger.info(f"Transitioned {issue_key} to Done")
                    return True
            return False
        except Exception as e:
            logger.error(f"Error transitioning {issue_key}: {e}")
            return False

    def add_comment(self, issue_key: str, comment: str) -> bool:
        """Add comment to ticket"""
        try:
            self.client.add_comment(issue_key, comment)
            logger.info(f"Added comment to {issue_key}")
            return True
        except Exception as e:
            logger.error(f"Error adding comment to {issue_key}: {e}")
            return False

    def add_pipeline_status_comment(
        self,
        issue_key: str,
        phase: str,
        status: str,
        details: Optional[Dict[str, Any]] = None
    ):
        """Add a formatted status update comment"""
        comment = f"""
*Pipeline Status Update*

| Field | Value |
|-------|-------|
| Phase | {phase} |
| Status | {status} |
| Timestamp | {datetime.utcnow().isoformat()} |

{f"*Details:* {json.dumps(details, indent=2)}" if details else ""}
        """.strip()

        self.add_comment(issue_key, comment)


# =============================================================================
# KAFKA PRODUCER
# =============================================================================

class JiraEventProducer:
    """Kafka producer for Jira/pipeline events"""

    def __init__(self, config: KafkaConfig):
        self.config = config
        self.producer = KafkaProducer(
            bootstrap_servers=config.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',
            retries=3
        )
        logger.info(f"Kafka producer initialized: {config.bootstrap_servers}")

    def publish_pipeline_request(
        self,
        jira_key: str,
        intent: Dict[str, Any],
        request_id: str
    ) -> bool:
        """Publish pipeline.requested event"""
        try:
            event = create_pipeline_requested_event(jira_key, intent, request_id)

            future = self.producer.send(
                self.config.pipeline_requested_topic,
                key=request_id,
                value=event
            )

            record = future.get(timeout=10)
            logger.info(f"Published pipeline.requested for {jira_key} "
                       f"(partition={record.partition}, offset={record.offset})")
            return True

        except KafkaError as e:
            logger.error(f"Kafka error publishing request: {e}")
            return False
        except Exception as e:
            logger.error(f"Error publishing request: {e}")
            return False

    def close(self):
        """Close producer"""
        self.producer.close()


# =============================================================================
# KAFKA CONSUMER
# =============================================================================

class JiraCommandConsumer:
    """Kafka consumer for pipeline completion events and Jira commands"""

    def __init__(self, config: KafkaConfig, jira_client: JiraClient):
        self.config = config
        self.jira_client = jira_client
        self.running = False

        self.topics = [
            config.pipeline_completed_topic,
            config.pipeline_failed_topic,
            config.pipeline_requires_approval_topic,
            config.jira_commands_topic,
        ]

        self.consumer = KafkaConsumer(
            *self.topics,
            bootstrap_servers=config.bootstrap_servers,
            group_id=config.consumer_group,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True
        )
        logger.info(f"Kafka consumer initialized for: {self.topics}")

    async def start(self):
        """Start consuming events"""
        self.running = True
        logger.info("Starting Jira command consumer...")

        while self.running:
            try:
                messages = self.consumer.poll(timeout_ms=1000)

                for topic_partition, records in messages.items():
                    for record in records:
                        await self._handle_event(record.topic, record.value)

                await asyncio.sleep(0.1)

            except Exception as e:
                logger.error(f"Consumer error: {e}")
                await asyncio.sleep(5)

    async def _handle_event(self, topic: str, message: Dict[str, Any]):
        """Handle incoming event"""
        event_type = message.get("event_type", "")
        jira_key = message.get("jira_key", "")

        logger.info(f"Received {event_type} for {jira_key}")

        try:
            if event_type == "pipeline.completed":
                await self._handle_pipeline_completed(message)
            elif event_type == "pipeline.failed":
                await self._handle_pipeline_failed(message)
            elif event_type == "pipeline.requires_approval":
                await self._handle_requires_approval(message)
            elif event_type == "mcp.jira.command":
                await self._handle_jira_command(message)
            else:
                logger.warning(f"Unknown event type: {event_type}")

        except Exception as e:
            logger.error(f"Error handling event: {e}")

    async def _handle_pipeline_completed(self, message: Dict[str, Any]):
        """Handle pipeline completion"""
        jira_key = message.get("jira_key")
        if not jira_key:
            return

        pr_url = message.get("pr_url", "N/A")
        dag_path = message.get("dag_path", "N/A")
        duration = message.get("total_duration_seconds", 0)

        comment = f"""
*Pipeline Generation Complete*

| Field | Value |
|-------|-------|
| Status | SUCCESS |
| PR URL | {pr_url} |
| DAG Path | {dag_path} |
| Duration | {duration:.1f}s |

The pipeline has been generated and is ready for deployment.
        """.strip()

        self.jira_client.add_comment(jira_key, comment)
        self.jira_client.transition_to_done(jira_key)

    async def _handle_pipeline_failed(self, message: Dict[str, Any]):
        """Handle pipeline failure"""
        jira_key = message.get("jira_key")
        if not jira_key:
            return

        phase = message.get("failed_at_phase", "unknown")
        error = message.get("error_message", "Unknown error")
        recommendation = message.get("recommended_action", "Review and retry")

        comment = f"""
*Pipeline Generation Failed*

| Field | Value |
|-------|-------|
| Status | FAILED |
| Failed At | {phase} |
| Error | {error} |

*Recommended Action:* {recommendation}
        """.strip()

        self.jira_client.add_comment(jira_key, comment)

    async def _handle_requires_approval(self, message: Dict[str, Any]):
        """Handle approval required notification"""
        jira_key = message.get("jira_key")
        if not jira_key:
            return

        environment = message.get("environment", "unknown")
        reason = message.get("approval_reason", "Required by policy")
        risk_level = message.get("risk_level", "unknown")

        comment = f"""
*Human Approval Required*

| Field | Value |
|-------|-------|
| Environment | {environment} |
| Reason | {reason} |
| Risk Level | {risk_level} |

Please review the generated artifacts and approve in the AI Agent Platform UI.
        """.strip()

        self.jira_client.add_comment(jira_key, comment)

    async def _handle_jira_command(self, message: Dict[str, Any]):
        """Handle direct Jira command"""
        command = message.get("command")
        issue_key = message.get("issue_key")
        payload = message.get("payload", {})

        if command == "add_comment":
            self.jira_client.add_comment(issue_key, payload.get("comment", ""))
        elif command == "transition":
            transition = payload.get("transition", "Done")
            if transition.lower() == "done":
                self.jira_client.transition_to_done(issue_key)
            elif transition.lower() == "in progress":
                self.jira_client.transition_to_in_progress(issue_key)

    def stop(self):
        """Stop consumer"""
        self.running = False
        self.consumer.close()


# =============================================================================
# POLLING SERVICE
# =============================================================================

class JiraPoller:
    """Background poller for Jira pipeline request tickets"""

    def __init__(
        self,
        jira_client: JiraClient,
        producer: JiraEventProducer,
        poll_interval: int = 120
    ):
        self.jira_client = jira_client
        self.producer = producer
        self.poll_interval = poll_interval
        self.running = False
        self.processed_tickets: set = set()

    async def start(self):
        """Start polling loop"""
        self.running = True
        logger.info(f"Starting Jira poller (interval={self.poll_interval}s)")

        while self.running:
            try:
                await self._poll_once()
                await asyncio.sleep(self.poll_interval)
            except Exception as e:
                logger.error(f"Polling error: {e}")
                await asyncio.sleep(10)

    async def _poll_once(self):
        """Execute single poll cycle"""
        tickets = self.jira_client.get_pipeline_request_tickets()

        for ticket in tickets:
            jira_key = ticket["key"]

            # Skip already processed
            if jira_key in self.processed_tickets:
                continue

            # Extract intent
            intent, error = self.jira_client.extract_intent_from_ticket(ticket)

            if error:
                logger.warning(f"Skipping {jira_key}: {error}")
                self.jira_client.add_comment(
                    jira_key,
                    f"*AI Agent Warning*: Could not process - {error}"
                )
                self.processed_tickets.add(jira_key)
                continue

            # Validate intent has required fields
            if not self._validate_intent(intent):
                logger.warning(f"Skipping {jira_key}: Invalid intent structure")
                self.jira_client.add_comment(
                    jira_key,
                    "*AI Agent Warning*: Intent JSON missing required fields "
                    "(pipeline_identity, source_config, target_config, schema_definition)"
                )
                self.processed_tickets.add(jira_key)
                continue

            # Generate request ID
            import uuid
            request_id = f"REQ-{jira_key}-{uuid.uuid4().hex[:8]}"

            # Publish to Kafka
            success = self.producer.publish_pipeline_request(
                jira_key=jira_key,
                intent=intent,
                request_id=request_id
            )

            if success:
                # Update ticket
                self.jira_client.transition_to_in_progress(jira_key)
                self.jira_client.add_comment(
                    jira_key,
                    f"*Pipeline Request Received*\n\nRequest ID: `{request_id}`\n\n"
                    "Your pipeline request is being processed by the AI Agent Platform."
                )
                self.processed_tickets.add(jira_key)
                logger.info(f"Processed pipeline request from {jira_key}")
            else:
                logger.error(f"Failed to publish request for {jira_key}")

    def _validate_intent(self, intent: Dict[str, Any]) -> bool:
        """Validate intent has required fields"""
        required_fields = [
            "pipeline_identity",
            "source_config",
            "target_config",
            "schema_definition",
        ]

        for field in required_fields:
            if field not in intent:
                return False

        # Validate pipeline_identity
        identity = intent.get("pipeline_identity", {})
        if not identity.get("pipeline_name") or not identity.get("environment"):
            return False

        return True

    def stop(self):
        """Stop polling"""
        self.running = False


# =============================================================================
# MAIN SERVER
# =============================================================================

class JiraEventDrivenServer:
    """
    Event-driven Jira MCP server for Data Pipeline requests.

    Components:
    1. Poller: Polls Jira for pipeline request tickets
    2. Consumer: Listens for completion events, updates Jira
    """

    def __init__(self):
        self.jira_config = JiraConfig()
        self.kafka_config = KafkaConfig()

        # Initialize clients
        self.jira_client = JiraClient(self.jira_config)
        self.producer = JiraEventProducer(self.kafka_config)

        # Initialize services
        self.poller = JiraPoller(
            jira_client=self.jira_client,
            producer=self.producer,
            poll_interval=self.jira_config.poll_interval_seconds
        )
        self.consumer = JiraCommandConsumer(
            config=self.kafka_config,
            jira_client=self.jira_client
        )

    async def run(self):
        """Run server with all components"""
        logger.info("=" * 60)
        logger.info("Jira Event-Driven MCP Server Starting")
        logger.info("=" * 60)
        logger.info(f"Jira: {self.jira_config.url}")
        logger.info(f"Project: {self.jira_config.project_key}")
        logger.info(f"Label: {self.jira_config.pipeline_label}")
        logger.info(f"Kafka: {self.kafka_config.bootstrap_servers}")
        logger.info(f"Poll interval: {self.jira_config.poll_interval_seconds}s")
        logger.info("=" * 60)

        try:
            await asyncio.gather(
                self.poller.start(),
                self.consumer.start()
            )
        except KeyboardInterrupt:
            logger.info("Shutting down...")
        finally:
            self.shutdown()

    def shutdown(self):
        """Cleanup resources"""
        self.poller.stop()
        self.consumer.stop()
        self.producer.close()
        logger.info("Server shutdown complete")


# =============================================================================
# CLI
# =============================================================================

async def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(description='Jira Event-Driven MCP Server')
    parser.add_argument('--test', action='store_true', help='Test connections')
    parser.add_argument('--poll-once', action='store_true', help='Poll once and exit')
    args = parser.parse_args()

    if args.test:
        print("\n Testing connections...")

        # Test Jira
        try:
            config = JiraConfig()
            client = JiraClient(config)
            tickets = client.get_pipeline_request_tickets()
            print(f" Jira: Connected, found {len(tickets)} pipeline request(s)")
        except Exception as e:
            print(f" Jira: {e}")

        # Test Kafka
        try:
            kafka_config = KafkaConfig()
            producer = JiraEventProducer(kafka_config)
            producer.producer.send('agent.events', value={'test': True}).get(timeout=5)
            print(" Kafka: Connected")
            producer.close()
        except Exception as e:
            print(f" Kafka: {e}")

        return

    if args.poll_once:
        config = JiraConfig()
        kafka_config = KafkaConfig()

        client = JiraClient(config)
        producer = JiraEventProducer(kafka_config)
        poller = JiraPoller(client, producer)

        await poller._poll_once()
        producer.close()
        return

    # Normal run
    server = JiraEventDrivenServer()
    await server.run()


if __name__ == "__main__":
    asyncio.run(main())
