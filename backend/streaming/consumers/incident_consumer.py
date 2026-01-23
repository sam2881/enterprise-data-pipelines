#!/usr/bin/env python3
"""
Kafka Incident Consumer - Real-Time Event-Driven Processing
============================================================
Consumes incidents from Kafka topics and triggers AI Agent remediation.

This is the REAL enterprise pattern:
1. ServiceNow Producer → servicenow.incidents topic
2. GCP Monitor → gcp.alerts topic
3. This Consumer → Reads events, triggers AI analysis
4. Publishes results → agent.events topic
"""
import os
import sys
import json
import asyncio
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
import threading

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from agents.remediation.agent import remediation_agent, ExecutionMode
from utils.kafka_client import kafka_client
from utils.redis_client import redis_client
import structlog

logger = structlog.get_logger()

# Configuration
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092')
CONSUMER_GROUP = os.getenv('KAFKA_CONSUMER_GROUP', 'ai-agent-consumer')
AUTO_REMEDIATE = os.getenv('AUTO_REMEDIATE', 'false').lower() == 'true'

# Redis keys for incident cache
INCIDENTS_CACHE_KEY = "incidents:active"
INCIDENTS_CACHE_TTL = 600  # 10 minutes - longer TTL for reliability


class IncidentConsumer:
    """
    Real-time Kafka consumer for incident processing.

    Subscribes to:
    - servicenow.incidents: New/updated incidents from ServiceNow
    - gcp.alerts: Alerts from GCP Monitoring

    Publishes to:
    - agent.events: AI agent actions and decisions
    """

    def __init__(self):
        """Initialize Kafka consumer and producer"""
        logger.info("kafka_consumer_initializing",
                   bootstrap=KAFKA_BOOTSTRAP,
                   group=CONSUMER_GROUP)

        # Consumer for reading incidents
        self.consumer = KafkaConsumer(
            'servicenow.incidents',
            'gcp.alerts',
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id=CONSUMER_GROUP,
            auto_offset_reset='latest',  # Start from latest for real-time
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            key_deserializer=lambda x: x.decode('utf-8') if x else None,
            consumer_timeout_ms=1000  # Poll timeout
        )

        # Producer for publishing events
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all'
        )

        self.running = False
        self.processed_count = 0
        self.incidents_cache = {}  # In-memory cache for current incidents

        logger.info("kafka_consumer_initialized")

    def _store_incident_in_redis(self, incident_data: dict):
        """
        Store incident in Redis cache for API to read.

        This maintains a list of active incidents that the /api/incidents
        endpoint can read without calling ServiceNow directly.
        """
        incident_id = incident_data.get('number', incident_data.get('incident_id'))
        if not incident_id:
            return

        try:
            # Format incident for API consumption
            formatted_incident = {
                "incident_id": incident_data.get("number", incident_id),
                "sys_id": incident_data.get("sys_id", ""),
                "short_description": incident_data.get("short_description", ""),
                "description": incident_data.get("description", ""),
                "priority": incident_data.get("priority", "3"),
                "state": incident_data.get("state", "1"),
                "urgency": incident_data.get("urgency", "2"),
                "impact": incident_data.get("impact", "2"),
                "category": incident_data.get("category", ""),
                "subcategory": incident_data.get("subcategory", ""),
                "created_on": incident_data.get("sys_created_on", ""),
                "updated_on": incident_data.get("sys_updated_on", datetime.now().isoformat()),
            }

            # Store individual incident
            incident_key = f"incident:{incident_id}"
            redis_client.set(incident_key, json.dumps(formatted_incident), ex=INCIDENTS_CACHE_TTL)

            # Update in-memory cache
            self.incidents_cache[incident_id] = formatted_incident

            # Update the active incidents list in Redis
            self._update_incidents_list()

            logger.info("incident_stored_in_redis", incident_id=incident_id)

        except Exception as e:
            logger.error("redis_store_error", incident_id=incident_id, error=str(e))

    def _update_incidents_list(self):
        """Update the full incidents list in Redis for API consumption"""
        try:
            incidents_list = list(self.incidents_cache.values())

            # Sort by updated_on descending
            incidents_list.sort(key=lambda x: x.get('updated_on', ''), reverse=True)

            # Limit to 50 most recent
            incidents_list = incidents_list[:50]

            cache_data = {
                "total": len(incidents_list),
                "source": "kafka_consumer",
                "kafka_published": True,
                "cached_at": datetime.now().isoformat(),
                "incidents": incidents_list
            }

            redis_client.set(INCIDENTS_CACHE_KEY, json.dumps(cache_data), ex=INCIDENTS_CACHE_TTL)
            logger.info("incidents_list_updated", count=len(incidents_list))

        except Exception as e:
            logger.error("redis_list_update_error", error=str(e))

    async def process_servicenow_incident(self, incident_data: dict):
        """
        Process a ServiceNow incident from Kafka.

        Flow:
        1. Extract incident details
        2. Check if already processed (idempotency)
        3. Run AI analysis
        4. Match runbooks
        5. Optionally auto-remediate (if LOW risk + AUTO_REMEDIATE=true)
        6. Publish results to agent.events
        """
        incident_id = incident_data.get('number', incident_data.get('incident_id'))

        if not incident_id:
            logger.warning("incident_missing_id", data=incident_data)
            return

        logger.info("processing_incident_from_kafka",
                   incident_id=incident_id,
                   source="servicenow.incidents")

        # ALWAYS store incident in Redis cache (for API to read)
        self._store_incident_in_redis(incident_data)

        # Check idempotency - skip AI processing if recently processed
        cache_key = f"processed:{incident_id}"
        if redis_client.get(cache_key):
            logger.info("incident_already_processed_for_ai", incident_id=incident_id)
            return

        try:
            # Run AI remediation analysis
            result = await remediation_agent.remediate_incident(
                incident_id=incident_id,
                incident_data=incident_data,
                logs=incident_data.get('work_notes', ''),
                mode=ExecutionMode.DRY_RUN
            )

            # Publish analysis result to agent.events
            event = {
                "event_type": "incident_analyzed",
                "source": "kafka_consumer",
                "incident_id": incident_id,
                "timestamp": datetime.now().isoformat(),
                "workflow_id": result.get("workflow_id"),
                "analysis": {
                    "service": result.get("step1_context", {}).get("service"),
                    "component": result.get("step1_context", {}).get("component"),
                    "root_cause": result.get("step1_context", {}).get("root_cause_hypothesis"),
                    "severity": result.get("step1_context", {}).get("severity")
                },
                "runbook_match": {
                    "script_id": result.get("step3_decision", {}).get("selected_script", {}).get("script_id") if result.get("step3_decision", {}).get("selected_script") else None,
                    "confidence": result.get("step3_decision", {}).get("overall_confidence", 0),
                    "risk_level": result.get("step3_decision", {}).get("selected_script", {}).get("risk_level") if result.get("step3_decision", {}).get("selected_script") else None,
                    "requires_approval": result.get("step3_decision", {}).get("requires_approval", True)
                },
                "auto_remediate": False
            }

            # Auto-remediate if LOW risk and AUTO_REMEDIATE enabled
            selected_script = result.get("step3_decision", {}).get("selected_script")
            if (AUTO_REMEDIATE and
                selected_script and
                selected_script.get("risk_level") == "low" and
                not result.get("step3_decision", {}).get("requires_approval")):

                logger.info("auto_remediating_low_risk",
                           incident_id=incident_id,
                           script_id=selected_script.get("script_id"))

                # TODO: Trigger actual execution
                event["auto_remediate"] = True
                event["auto_remediate_status"] = "triggered"

            # Publish to agent.events
            self.producer.send(
                'agent.events',
                key=incident_id,
                value=event
            )
            self.producer.flush()

            # Mark as processed (TTL 1 hour)
            redis_client.set(cache_key, "processed", ex=3600)

            self.processed_count += 1
            logger.info("incident_processed_successfully",
                       incident_id=incident_id,
                       total_processed=self.processed_count)

        except Exception as e:
            logger.error("incident_processing_error",
                        incident_id=incident_id,
                        error=str(e))

            # Publish error event
            self.producer.send(
                'agent.events',
                key=incident_id,
                value={
                    "event_type": "incident_processing_error",
                    "incident_id": incident_id,
                    "error": str(e),
                    "timestamp": datetime.now().isoformat()
                }
            )

    async def process_gcp_alert(self, alert_data: dict):
        """
        Process a GCP alert from Kafka.

        GCP alerts may need to be converted to incident format first.
        """
        alert_id = alert_data.get('incident_id', alert_data.get('alertId'))

        logger.info("processing_gcp_alert", alert_id=alert_id)

        # Convert GCP alert to incident format
        incident_data = {
            "number": f"GCP-{alert_id}",
            "short_description": alert_data.get('summary', alert_data.get('condition', {}).get('displayName', 'GCP Alert')),
            "description": json.dumps(alert_data, indent=2),
            "priority": self._map_gcp_severity(alert_data.get('severity', 'WARNING')),
            "category": "gcp",
            "source": "gcp_monitoring"
        }

        await self.process_servicenow_incident(incident_data)

    def _map_gcp_severity(self, severity: str) -> str:
        """Map GCP severity to ServiceNow priority"""
        mapping = {
            "CRITICAL": "1",
            "ERROR": "2",
            "WARNING": "3",
            "INFO": "4"
        }
        return mapping.get(severity.upper(), "3")

    def start(self):
        """Start the consumer loop"""
        self.running = True
        logger.info("kafka_consumer_started",
                   topics=['servicenow.incidents', 'gcp.alerts'])

        print("=" * 60)
        print("  AI AGENT KAFKA CONSUMER STARTED")
        print("=" * 60)
        print(f"  Bootstrap: {KAFKA_BOOTSTRAP}")
        print(f"  Group ID:  {CONSUMER_GROUP}")
        print(f"  Topics:    servicenow.incidents, gcp.alerts")
        print(f"  Auto-Remediate: {AUTO_REMEDIATE}")
        print("=" * 60)
        print("  Waiting for incidents...")
        print()

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            while self.running:
                # Poll for messages
                messages = self.consumer.poll(timeout_ms=1000)

                for topic_partition, records in messages.items():
                    topic = topic_partition.topic

                    for record in records:
                        print(f"\n{'='*60}")
                        print(f"  NEW EVENT from {topic}")
                        print(f"  Key: {record.key}")
                        print(f"  Timestamp: {datetime.now().isoformat()}")
                        print(f"{'='*60}")

                        if topic == 'servicenow.incidents':
                            loop.run_until_complete(
                                self.process_servicenow_incident(record.value)
                            )
                        elif topic == 'gcp.alerts':
                            loop.run_until_complete(
                                self.process_gcp_alert(record.value)
                            )

        except KeyboardInterrupt:
            logger.info("kafka_consumer_interrupted")
        finally:
            self.stop()
            loop.close()

    def stop(self):
        """Stop the consumer"""
        self.running = False
        self.consumer.close()
        self.producer.close()
        logger.info("kafka_consumer_stopped", total_processed=self.processed_count)
        print(f"\nConsumer stopped. Total processed: {self.processed_count}")


def main():
    """Main entry point"""
    consumer = IncidentConsumer()
    consumer.start()


if __name__ == "__main__":
    main()
