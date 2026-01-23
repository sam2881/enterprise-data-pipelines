"""
External service integrations (Jira, Kafka, Secret Manager).

This module provides clients for external services:
- JiraClient: Jira REST API client for ticket updates
- PipelineKafkaProducer: Kafka producer for pipeline events
- PipelineKafkaConsumer: Kafka consumer for pipeline intents
"""

from src.integrations.jira_client import JiraClient
from src.integrations.kafka_client import PipelineKafkaConsumer, PipelineKafkaProducer

__all__ = [
    "JiraClient",
    "PipelineKafkaProducer",
    "PipelineKafkaConsumer",
]
