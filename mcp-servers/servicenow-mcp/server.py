#!/usr/bin/env python3
"""
ServiceNow MCP Server - Event-Driven Architecture

WHY: This server follows the "MCPs sense → Kafka remembers" principle.
- Polls ServiceNow for new/updated incidents (PRODUCER)
- Publishes incident.created events to Kafka
- Consumes incident.close_execute commands from Kafka (CONSUMER)
- Executes ticket closure in ServiceNow

HOW: Combines MCP tool interface with Kafka integration:
- Background poller runs every N seconds
- Kafka consumer listens for close commands
- MCP interface available for direct tool invocation

MUST NOT:
- Call LangGraph directly
- Execute remediation logic
- Contain business logic
- Contain approval logic
"""
import os
import sys
import json
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from dataclasses import dataclass
from enum import Enum

# Third-party imports
import pysnow
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
logger = logging.getLogger("servicenow-mcp")


# =============================================================================
# CONFIGURATION
# =============================================================================

@dataclass
class ServiceNowConfig:
    """ServiceNow connection configuration"""
    instance_url: str = os.getenv("SNOW_INSTANCE_URL", "https://dev275804.service-now.com")
    username: str = os.getenv("SNOW_USERNAME", "admin")
    password: str = os.getenv("SNOW_PASSWORD", "")
    poll_interval_seconds: int = int(os.getenv("SNOW_POLL_INTERVAL", "60"))
    poll_lookback_minutes: int = int(os.getenv("SNOW_POLL_LOOKBACK", "5"))


@dataclass
class KafkaConfig:
    """Kafka connection configuration"""
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    # Topics we PRODUCE to
    incident_created_topic: str = "incident.created"
    servicenow_incidents_topic: str = "servicenow.incidents"
    # Topics we CONSUME from
    close_execute_topic: str = "incident.close_execute"
    servicenow_commands_topic: str = "mcp.servicenow.commands"
    consumer_group: str = "servicenow-mcp-consumer"


# =============================================================================
# EVENT SCHEMAS (inline for MCP server isolation)
# =============================================================================

class IncidentEventType(str, Enum):
    """Incident event types"""
    CREATED = "incident.created"
    UPDATED = "incident.updated"
    CLOSED = "incident.closed"


def create_incident_event(
    incident_data: Dict[str, Any],
    event_type: IncidentEventType = IncidentEventType.CREATED
) -> Dict[str, Any]:
    """Create a Kafka event from ServiceNow incident data"""
    import uuid

    incident_id = incident_data.get("number", "UNKNOWN")

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type.value,
        "timestamp": datetime.utcnow().isoformat(),
        "correlation_id": incident_id,
        "source": "servicenow-mcp",
        "version": "5.0",
        # Incident fields
        "incident_id": incident_id,
        "short_description": incident_data.get("short_description", ""),
        "description": incident_data.get("description", ""),
        "priority": incident_data.get("priority", "3"),
        "service": incident_data.get("category", "unknown"),
        "source_system": "servicenow",
        "raw_data": {
            "sys_id": incident_data.get("sys_id"),
            "state": incident_data.get("state"),
            "urgency": incident_data.get("urgency"),
            "impact": incident_data.get("impact"),
            "category": incident_data.get("category"),
            "subcategory": incident_data.get("subcategory"),
            "assignment_group": incident_data.get("assignment_group"),
            "assigned_to": incident_data.get("assigned_to"),
            "created_on": incident_data.get("sys_created_on"),
            "updated_on": incident_data.get("sys_updated_on"),
        }
    }


# =============================================================================
# SERVICENOW CLIENT
# =============================================================================

class ServiceNowClient:
    """ServiceNow API client wrapper"""

    def __init__(self, config: ServiceNowConfig):
        self.config = config
        instance = config.instance_url.replace("https://", "").replace("http://", "")

        self.client = pysnow.Client(
            instance=instance,
            user=config.username,
            password=config.password
        )
        self.incident_table = self.client.resource(api_path='/table/incident')
        logger.info(f"ServiceNow client initialized for {instance}")

    def fetch_recent_incidents(self, since_minutes: int = 5) -> List[Dict[str, Any]]:
        """Fetch incidents updated in the last N minutes"""
        since_time = (datetime.utcnow() - timedelta(minutes=since_minutes)).strftime('%Y-%m-%d %H:%M:%S')

        try:
            response = self.incident_table.get(
                query={
                    'active': 'true',
                    'sys_updated_on': f'>={since_time}'
                },
                limit=100,
                order_by='-sys_updated_on'
            )
            incidents = list(response.all())
            logger.info(f"Fetched {len(incidents)} incident(s) from ServiceNow")
            return incidents
        except Exception as e:
            logger.error(f"Error fetching incidents: {e}")
            return []

    def get_incident_by_id(self, incident_id: str) -> Optional[Dict[str, Any]]:
        """Get a specific incident by number"""
        try:
            response = self.incident_table.get(
                query={'number': incident_id},
                limit=1
            )
            results = list(response.all())
            return results[0] if results else None
        except Exception as e:
            logger.error(f"Error getting incident {incident_id}: {e}")
            return None

    def close_incident(
        self,
        sys_id: str,
        close_code: str = "Resolved",
        close_notes: str = "Resolved by AI Agent Platform"
    ) -> bool:
        """Close an incident in ServiceNow"""
        try:
            self.incident_table.update(
                query={'sys_id': sys_id},
                payload={
                    'state': '6',  # Resolved
                    'close_code': close_code,
                    'close_notes': close_notes,
                    'resolved_at': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
                }
            )
            logger.info(f"Closed incident {sys_id} in ServiceNow")
            return True
        except Exception as e:
            logger.error(f"Error closing incident {sys_id}: {e}")
            return False

    def add_work_notes(self, sys_id: str, notes: str) -> bool:
        """Add work notes to an incident"""
        try:
            self.incident_table.update(
                query={'sys_id': sys_id},
                payload={'work_notes': notes}
            )
            logger.info(f"Added work notes to incident {sys_id}")
            return True
        except Exception as e:
            logger.error(f"Error adding work notes to {sys_id}: {e}")
            return False


# =============================================================================
# KAFKA PRODUCER (publishes incident events)
# =============================================================================

class ServiceNowEventProducer:
    """Kafka producer for ServiceNow events"""

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

    def publish_incident(self, incident_data: Dict[str, Any], is_new: bool = True) -> bool:
        """Publish incident event to Kafka"""
        try:
            event_type = IncidentEventType.CREATED if is_new else IncidentEventType.UPDATED
            event = create_incident_event(incident_data, event_type)
            incident_id = incident_data.get("number", "UNKNOWN")

            # Publish to incident.created for new incidents
            topic = self.config.incident_created_topic if is_new else self.config.servicenow_incidents_topic

            future = self.producer.send(
                topic,
                key=incident_id,
                value=event
            )

            record = future.get(timeout=10)
            logger.info(f"Published {event_type.value} for {incident_id} to {topic} "
                       f"(partition={record.partition}, offset={record.offset})")
            return True

        except KafkaError as e:
            logger.error(f"Kafka error publishing incident: {e}")
            return False
        except Exception as e:
            logger.error(f"Error publishing incident: {e}")
            return False

    def close(self):
        """Close producer"""
        self.producer.close()
        logger.info("Kafka producer closed")


# =============================================================================
# KAFKA CONSUMER (listens for close commands)
# =============================================================================

class ServiceNowCommandConsumer:
    """Kafka consumer for ServiceNow commands (close, update, etc.)"""

    def __init__(self, config: KafkaConfig, snow_client: ServiceNowClient):
        self.config = config
        self.snow_client = snow_client
        self.running = False

        # Topics to consume
        self.topics = [
            config.close_execute_topic,
            config.servicenow_commands_topic
        ]

        self.consumer = KafkaConsumer(
            *self.topics,
            bootstrap_servers=config.bootstrap_servers,
            group_id=config.consumer_group,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True
        )
        logger.info(f"Kafka consumer initialized for topics: {self.topics}")

    async def start(self):
        """Start consuming commands"""
        self.running = True
        logger.info("Starting command consumer...")

        while self.running:
            try:
                # Poll with timeout
                messages = self.consumer.poll(timeout_ms=1000)

                for topic_partition, records in messages.items():
                    for record in records:
                        await self._handle_command(record.topic, record.value)

                # Yield to other tasks
                await asyncio.sleep(0.1)

            except Exception as e:
                logger.error(f"Consumer error: {e}")
                await asyncio.sleep(5)

    async def _handle_command(self, topic: str, message: Dict[str, Any]):
        """Handle incoming command"""
        logger.info(f"Received command from {topic}: {message.get('event_type', 'unknown')}")

        try:
            event_type = message.get("event_type", "")

            if event_type == "incident.close_execute":
                await self._handle_close_execute(message)
            elif event_type == "mcp.servicenow.command":
                await self._handle_mcp_command(message)
            else:
                logger.warning(f"Unknown event type: {event_type}")

        except Exception as e:
            logger.error(f"Error handling command: {e}")

    async def _handle_close_execute(self, message: Dict[str, Any]):
        """Handle incident closure command"""
        sys_id = message.get("servicenow_sys_id")
        incident_id = message.get("incident_id")
        resolution = message.get("resolution_notes", "Resolved by AI Agent Platform")
        close_code = message.get("close_code", "Resolved")

        if not sys_id:
            logger.error("Missing servicenow_sys_id in close_execute command")
            return

        success = self.snow_client.close_incident(
            sys_id=sys_id,
            close_code=close_code,
            close_notes=resolution
        )

        if success:
            logger.info(f"Successfully closed incident {incident_id} ({sys_id})")
        else:
            logger.error(f"Failed to close incident {incident_id} ({sys_id})")

    async def _handle_mcp_command(self, message: Dict[str, Any]):
        """Handle generic MCP command"""
        command = message.get("command")
        sys_id = message.get("servicenow_sys_id")
        payload = message.get("payload", {})

        if command == "close_ticket":
            self.snow_client.close_incident(
                sys_id=sys_id,
                close_notes=payload.get("close_notes", "Closed via MCP command")
            )
        elif command == "add_work_notes":
            self.snow_client.add_work_notes(
                sys_id=sys_id,
                notes=payload.get("notes", "")
            )
        elif command == "update_ticket":
            # Generic update - handled by ServiceNow client
            pass
        else:
            logger.warning(f"Unknown MCP command: {command}")

    def stop(self):
        """Stop consumer"""
        self.running = False
        self.consumer.close()
        logger.info("Kafka consumer stopped")


# =============================================================================
# POLLING SERVICE (background task)
# =============================================================================

class ServiceNowPoller:
    """Background poller for ServiceNow incidents"""

    def __init__(
        self,
        snow_client: ServiceNowClient,
        producer: ServiceNowEventProducer,
        poll_interval: int = 60,
        lookback_minutes: int = 5
    ):
        self.snow_client = snow_client
        self.producer = producer
        self.poll_interval = poll_interval
        self.lookback_minutes = lookback_minutes
        self.running = False
        self.seen_incidents: Dict[str, str] = {}  # incident_id -> last_updated

    async def start(self):
        """Start polling loop"""
        self.running = True
        logger.info(f"Starting ServiceNow poller (interval={self.poll_interval}s)")

        while self.running:
            try:
                await self._poll_once()
                await asyncio.sleep(self.poll_interval)
            except Exception as e:
                logger.error(f"Polling error: {e}")
                await asyncio.sleep(10)  # Wait before retry

    async def _poll_once(self):
        """Execute a single poll cycle"""
        logger.debug(f"Polling ServiceNow (lookback={self.lookback_minutes}m)")

        incidents = self.snow_client.fetch_recent_incidents(self.lookback_minutes)

        new_count = 0
        updated_count = 0

        for incident in incidents:
            incident_id = incident.get("number")
            updated_on = incident.get("sys_updated_on")
            created_on = incident.get("sys_created_on")

            # Skip if we've already processed this version
            if incident_id in self.seen_incidents:
                if self.seen_incidents[incident_id] == updated_on:
                    continue
                # This is an update
                self.producer.publish_incident(incident, is_new=False)
                updated_count += 1
            else:
                # New incident (or first time seeing it)
                is_actually_new = created_on == updated_on
                self.producer.publish_incident(incident, is_new=is_actually_new)
                if is_actually_new:
                    new_count += 1
                else:
                    updated_count += 1

            # Mark as seen
            self.seen_incidents[incident_id] = updated_on

        if new_count > 0 or updated_count > 0:
            logger.info(f"Published {new_count} new, {updated_count} updated incident(s)")

    def stop(self):
        """Stop polling"""
        self.running = False
        logger.info("ServiceNow poller stopped")


# =============================================================================
# MAIN SERVER
# =============================================================================

class ServiceNowEventDrivenServer:
    """
    Event-driven ServiceNow MCP server.

    Components:
    1. Poller: Polls ServiceNow, publishes incident.created events
    2. Consumer: Listens for close commands, executes in ServiceNow
    3. (Optional) MCP interface for direct tool calls
    """

    def __init__(self):
        self.snow_config = ServiceNowConfig()
        self.kafka_config = KafkaConfig()

        # Initialize clients
        self.snow_client = ServiceNowClient(self.snow_config)
        self.producer = ServiceNowEventProducer(self.kafka_config)

        # Initialize services
        self.poller = ServiceNowPoller(
            snow_client=self.snow_client,
            producer=self.producer,
            poll_interval=self.snow_config.poll_interval_seconds,
            lookback_minutes=self.snow_config.poll_lookback_minutes
        )
        self.consumer = ServiceNowCommandConsumer(
            config=self.kafka_config,
            snow_client=self.snow_client
        )

    async def run(self):
        """Run the server with all components"""
        logger.info("=" * 60)
        logger.info("ServiceNow Event-Driven MCP Server Starting")
        logger.info("=" * 60)
        logger.info(f"ServiceNow: {self.snow_config.instance_url}")
        logger.info(f"Kafka: {self.kafka_config.bootstrap_servers}")
        logger.info(f"Poll interval: {self.snow_config.poll_interval_seconds}s")
        logger.info(f"Publishing to: {self.kafka_config.incident_created_topic}")
        logger.info(f"Consuming from: {self.kafka_config.close_execute_topic}")
        logger.info("=" * 60)

        try:
            # Run poller and consumer concurrently
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
# CLI ENTRY POINT
# =============================================================================

async def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(description='ServiceNow Event-Driven MCP Server')
    parser.add_argument('--test', action='store_true', help='Test connections and exit')
    parser.add_argument('--poll-once', action='store_true', help='Poll once and exit')
    args = parser.parse_args()

    if args.test:
        # Test mode
        config = ServiceNowConfig()
        kafka_config = KafkaConfig()

        print("\n🧪 Testing connections...")

        # Test ServiceNow
        try:
            client = ServiceNowClient(config)
            incidents = client.fetch_recent_incidents(1)
            print(f"✅ ServiceNow: Connected, found {len(incidents)} recent incident(s)")
        except Exception as e:
            print(f"❌ ServiceNow: {e}")

        # Test Kafka
        try:
            producer = ServiceNowEventProducer(kafka_config)
            producer.producer.send('agent.events', value={'test': True}).get(timeout=5)
            print("✅ Kafka: Connected")
            producer.close()
        except Exception as e:
            print(f"❌ Kafka: {e}")

        print()
        return

    if args.poll_once:
        # Single poll mode
        config = ServiceNowConfig()
        kafka_config = KafkaConfig()

        client = ServiceNowClient(config)
        producer = ServiceNowEventProducer(kafka_config)

        incidents = client.fetch_recent_incidents(config.poll_lookback_minutes)
        for incident in incidents:
            producer.publish_incident(incident, is_new=True)

        print(f"✅ Published {len(incidents)} incident(s)")
        producer.close()
        return

    # Normal run
    server = ServiceNowEventDrivenServer()
    await server.run()


if __name__ == "__main__":
    asyncio.run(main())
