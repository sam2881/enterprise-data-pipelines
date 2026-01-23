"""
Kafka Producer for All Agent Events
Industry-standard event-driven architecture shared by IT Service and Data agents
"""
import asyncio
import os
import json
from typing import Any, Dict
from datetime import datetime
import structlog
from kafka import KafkaProducer
from kafka.errors import KafkaError

logger = structlog.get_logger()


class UnifiedKafkaProducer:
    """
    Kafka producer for publishing lifecycle events for all agents

    IT Service Agent Topics:
    - incident.created
    - incident.llm.analysis_complete
    - incident.approval.requested
    - incident.execution.approved
    - incident.github.pr_created
    - incident.jira.ticket_created
    - incident.execution.completed

    Data Pipeline Agent Topics:
    - pipeline.created
    - pipeline.source.analyzed
    - pipeline.ir.generated
    - pipeline.approval.requested
    - pipeline.spark.generated
    - pipeline.dag.generated
    - pipeline.dq.generated
    - pipeline.completed
    """

    def __init__(self):
        self.bootstrap_servers = os.getenv(
            'KAFKA_BOOTSTRAP_SERVERS',
            'localhost:9092'
        ).replace('kafka://', '')
        self.mock_mode = os.getenv('MOCK_KAFKA', 'true').lower() == 'true'
        self.mock_events = []  # Store events in mock mode

        if self.mock_mode:
            logger.info("kafka_producer_mock_mode", reason="MOCK_KAFKA=true")
            self.producer = None
            return

        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',  # Wait for all replicas
                retries=3,
                max_in_flight_requests_per_connection=1  # Ensure ordering
            )
            logger.info("kafka_producer_initialized", servers=self.bootstrap_servers)
        except Exception as e:
            logger.error("kafka_producer_init_failed", error=str(e))
            logger.info("kafka_producer_fallback_to_mock")
            self.producer = None
            self.mock_mode = True

    async def publish_event(
        self,
        topic: str,
        event: Dict[str, Any],
        key: str = None
    ) -> bool:
        """
        Publish event to Kafka topic

        Args:
            topic: Kafka topic name
            event: Event payload (will be JSON serialized)
            key: Optional partition key

        Returns:
            True if successful, False otherwise
        """
        # Mock mode - store events locally
        if self.mock_mode or not self.producer:
            if 'timestamp' not in event:
                event['timestamp'] = datetime.utcnow().isoformat()
            self.mock_events.append({"topic": topic, "event": event, "key": key})
            logger.info("kafka_mock_event_published", topic=topic, key=key)
            return True

        try:
            # Add timestamp if not present (use UTC for consistency)
            if 'timestamp' not in event:
                event['timestamp'] = datetime.utcnow().isoformat()

            # Determine key based on event type
            if not key:
                if 'incident_id' in event:
                    key = event['incident_id']
                elif 'request_id' in event:
                    key = event['request_id']
                elif 'pipeline_id' in event:
                    key = event['pipeline_id']

            # Send to Kafka using thread pool to avoid blocking event loop
            def _sync_send():
                future = self.producer.send(
                    topic=topic,
                    value=event,
                    key=key
                )
                # Wait for confirmation with timeout
                return future.get(timeout=10)

            # Run sync Kafka send in thread pool
            record_metadata = await asyncio.get_event_loop().run_in_executor(
                None, _sync_send
            )

            logger.info(
                "kafka_event_published",
                topic=topic,
                partition=record_metadata.partition,
                offset=record_metadata.offset,
                key=key
            )

            return True

        except KafkaError as e:
            logger.error("kafka_publish_error", topic=topic, error=str(e))
            return False
        except Exception as e:
            logger.error("kafka_publish_exception", topic=topic, error=str(e))
            return False

    async def publish_incident_event(self, event_type: str, event: Dict[str, Any]) -> bool:
        """Publish IT Service Agent incident event"""
        return await self.publish_event(f"incident.{event_type}", event)

    async def publish_pipeline_event(self, event_type: str, event: Dict[str, Any]) -> bool:
        """Publish Data Pipeline Agent event"""
        return await self.publish_event(f"pipeline.{event_type}", event)

    def close(self):
        """Close Kafka producer connection"""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info("kafka_producer_closed")


# Global instance - shared by all agents
unified_producer = UnifiedKafkaProducer()

# Backward compatible alias
incident_producer = unified_producer


def get_producer() -> UnifiedKafkaProducer:
    """Get the global Kafka producer instance."""
    return unified_producer
