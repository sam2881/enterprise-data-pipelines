"""Kafka client utility for event streaming"""
import json
import os
from typing import Any, Dict, Optional
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import structlog

logger = structlog.get_logger()


class KafkaClient:
    """Kafka client for publishing and consuming events"""

    def __init__(self):
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        self.producer: Optional[KafkaProducer] = None
        self.consumer: Optional[KafkaConsumer] = None

    def get_producer(self) -> KafkaProducer:
        """Get or create Kafka producer"""
        if not self.producer:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3
            )
        return self.producer

    def publish_event(self, topic: str, event: Dict[str, Any], key: Optional[str] = None):
        """Publish event to Kafka topic"""
        try:
            producer = self.get_producer()
            future = producer.send(
                topic,
                value=event,
                key=key.encode('utf-8') if key else None
            )
            record_metadata = future.get(timeout=10)
            logger.info(
                "event_published",
                topic=topic,
                partition=record_metadata.partition,
                offset=record_metadata.offset
            )
        except KafkaError as e:
            logger.error("kafka_publish_error", error=str(e), topic=topic)
            raise

    def create_consumer(self, topics: list[str], group_id: str) -> KafkaConsumer:
        """Create Kafka consumer for topics"""
        return KafkaConsumer(
            *topics,
            bootstrap_servers=self.bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True
        )

    def close(self):
        """Close Kafka connections"""
        if self.producer:
            self.producer.close()
        if self.consumer:
            self.consumer.close()


# Global Kafka client instance
kafka_client = KafkaClient()


def get_kafka_producer():
    """Get the global Kafka producer."""
    return kafka_client.get_producer()


def get_kafka_consumer(topics: list, group_id: str):
    """Create a Kafka consumer for the specified topics."""
    return kafka_client.create_consumer(topics, group_id)
