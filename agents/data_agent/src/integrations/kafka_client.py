"""
Kafka client for pipeline event messaging.

WHY: Publishes pipeline status updates and consumes pipeline intents.
     Enables event-driven architecture for workflow coordination.

TOPICS:
- pipeline-intents: Incoming pipeline creation requests
- pipeline-status: Status updates for pipeline generation workflow

Uses the same pattern as backend/streaming/kafka_producer.py
"""

from typing import Any, Dict, List, Optional, Callable
from datetime import datetime
import os
import json
import structlog
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

from src.config.settings import get_settings

logger = structlog.get_logger()


# =============================================================================
# Topic Definitions
# =============================================================================


class PipelineTopics:
    """Kafka topic names for data pipeline agent."""

    # Incoming requests
    PIPELINE_INTENTS = "pipeline-intents"

    # Status updates
    PIPELINE_STATUS = "pipeline-status"

    # Lifecycle events (matching backend/streaming/kafka_producer.py pattern)
    PIPELINE_CREATED = "pipeline.created"
    PIPELINE_SOURCE_ANALYZED = "pipeline.source.analyzed"
    PIPELINE_IR_GENERATED = "pipeline.ir.generated"
    PIPELINE_APPROVAL_REQUESTED = "pipeline.approval.requested"
    PIPELINE_SPARK_GENERATED = "pipeline.spark.generated"
    PIPELINE_DAG_GENERATED = "pipeline.dag.generated"
    PIPELINE_DQ_GENERATED = "pipeline.dq.generated"
    PIPELINE_COMPLETED = "pipeline.completed"
    PIPELINE_FAILED = "pipeline.failed"


# =============================================================================
# Kafka Producer
# =============================================================================


class PipelineKafkaProducer:
    """
    Kafka producer for publishing pipeline events.

    Follows same pattern as backend/streaming/kafka_producer.py
    """

    def __init__(self) -> None:
        """Initialize Kafka producer."""
        self.bootstrap_servers = os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS",
            "localhost:29092"
        ).replace("kafka://", "")

        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
                max_in_flight_requests_per_connection=1,
            )
            logger.info("Pipeline Kafka producer initialized", servers=self.bootstrap_servers)
        except Exception as e:
            logger.error("Failed to initialize Kafka producer", error=str(e))
            self.producer = None

    def publish_pipeline_status(self, status_update: Dict[str, Any]) -> bool:
        """
        Publish pipeline status update.

        Args:
            status_update: Status update with request_id, status, phase, etc.

        Returns:
            True if published successfully
        """
        return self._publish(
            topic=PipelineTopics.PIPELINE_STATUS,
            event=status_update,
            key=status_update.get("request_id"),
        )

    def publish_pipeline_event(
        self,
        event_type: str,
        event: Dict[str, Any],
    ) -> bool:
        """
        Publish pipeline lifecycle event.

        Args:
            event_type: Event type (e.g., "created", "completed")
            event: Event payload

        Returns:
            True if published successfully
        """
        topic = f"pipeline.{event_type}"
        return self._publish(
            topic=topic,
            event=event,
            key=event.get("request_id") or event.get("pipeline_id"),
        )

    def _publish(
        self,
        topic: str,
        event: Dict[str, Any],
        key: Optional[str] = None,
    ) -> bool:
        """
        Publish event to Kafka topic.

        Args:
            topic: Kafka topic name
            event: Event payload
            key: Optional partition key

        Returns:
            True if successful
        """
        if not self.producer:
            logger.warning("Kafka producer not available", topic=topic)
            return False

        try:
            # Add timestamp if not present
            if "timestamp" not in event:
                event["timestamp"] = datetime.utcnow().isoformat()

            # Send to Kafka
            future = self.producer.send(
                topic=topic,
                value=event,
                key=key,
            )

            # Wait for confirmation
            record_metadata = future.get(timeout=10)

            logger.info(
                "Pipeline event published",
                topic=topic,
                partition=record_metadata.partition,
                offset=record_metadata.offset,
                key=key,
            )

            return True

        except KafkaError as e:
            logger.error("Kafka publish error", topic=topic, error=str(e))
            return False
        except Exception as e:
            logger.error("Kafka publish exception", topic=topic, error=str(e))
            return False

    def flush(self) -> None:
        """Flush pending messages."""
        if self.producer:
            self.producer.flush()

    def close(self) -> None:
        """Close Kafka producer."""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info("Pipeline Kafka producer closed")


# =============================================================================
# Kafka Consumer
# =============================================================================


class PipelineKafkaConsumer:
    """
    Kafka consumer for consuming pipeline intents.
    """

    def __init__(
        self,
        topics: List[str],
        group_id: str = "data-agent-consumers",
    ) -> None:
        """Initialize Kafka consumer."""
        self.bootstrap_servers = os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS",
            "localhost:29092"
        ).replace("kafka://", "")

        self.topics = topics
        self.group_id = group_id
        self.consumer: Optional[KafkaConsumer] = None

    def start(self) -> None:
        """Start consumer."""
        try:
            self.consumer = KafkaConsumer(
                *self.topics,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                max_poll_interval_ms=600000,  # 10 minutes for long-running processing
            )
            logger.info(
                "Pipeline Kafka consumer started",
                topics=self.topics,
                group_id=self.group_id,
            )
        except Exception as e:
            logger.error("Failed to start Kafka consumer", error=str(e))
            raise

    def consume(
        self,
        callback: Callable[[Dict[str, Any]], None],
        timeout_ms: int = 1000,
    ) -> None:
        """
        Consume messages and process with callback.

        Args:
            callback: Function to handle each message
            timeout_ms: Poll timeout in milliseconds
        """
        if not self.consumer:
            raise RuntimeError("Consumer not started")

        try:
            while True:
                records = self.consumer.poll(timeout_ms=timeout_ms)

                for topic_partition, messages in records.items():
                    for message in messages:
                        try:
                            logger.info(
                                "Processing pipeline intent",
                                topic=topic_partition.topic,
                                offset=message.offset,
                            )
                            callback(message.value)

                        except Exception as e:
                            logger.error(
                                "Error processing message",
                                topic=topic_partition.topic,
                                offset=message.offset,
                                error=str(e),
                            )

        except KeyboardInterrupt:
            logger.info("Consumer interrupted")
        finally:
            self.close()

    def close(self) -> None:
        """Close consumer."""
        if self.consumer:
            self.consumer.close()
            logger.info("Pipeline Kafka consumer closed")


# =============================================================================
# Singleton Factory
# =============================================================================


_producer_instance: Optional[PipelineKafkaProducer] = None


def get_kafka_producer() -> PipelineKafkaProducer:
    """Get singleton Kafka producer instance."""
    global _producer_instance
    if _producer_instance is None:
        _producer_instance = PipelineKafkaProducer()
    return _producer_instance


def create_intent_consumer() -> PipelineKafkaConsumer:
    """Create consumer for pipeline intents."""
    return PipelineKafkaConsumer(
        topics=[PipelineTopics.PIPELINE_INTENTS],
        group_id="data-agent-intent-consumers",
    )
