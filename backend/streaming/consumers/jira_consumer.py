#!/usr/bin/env python3
"""
Kafka Jira Consumer - Data Pipeline Agent ONLY
===============================================
Consumes Jira stories from Kafka and triggers Data Pipeline Agent.

STRICT SCOPE:
- Jira tickets are ONLY for data pipeline build/change/enhancement
- All Jira stories go to Data Agent (no IT Service routing)
- ServiceNow handles all IT Service / operational issues

Flow:
1. Jira Webhook → jira.stories topic
2. This Consumer → Data Pipeline Agent
3. Agent generates pipeline code (Spark, DAG, DQ)
4. Creates GitHub Merge Request
5. CI/CD validates and deploys
6. Airflow executes the pipeline

Final Output: GitHub MR with pipeline code
"""
import os
import sys
import json
import asyncio
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from utils.redis_client import redis_client
import structlog

logger = structlog.get_logger()

# Configuration
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092')
CONSUMER_GROUP = os.getenv('JIRA_CONSUMER_GROUP', 'jira-data-pipeline-consumer')

# GitHub configuration for MR creation
GITHUB_TOKEN = os.getenv('GITHUB_TOKEN', '')
GITHUB_OWNER = os.getenv('GITHUB_OWNER', '')
GITHUB_REPO = os.getenv('GITHUB_DATA_REPO', 'data-pipelines')
GITHUB_BASE_BRANCH = os.getenv('GITHUB_BASE_BRANCH', 'main')

# Redis keys for Jira cache
JIRA_STORIES_CACHE_KEY = "jira:pipelines:active"
JIRA_CACHE_TTL = 600  # 10 minutes


class JiraConsumer:
    """
    Kafka consumer for Jira → Data Pipeline Agent workflow.

    SCOPE: Data pipeline build/change/enhancement ONLY
    - No IT Service routing
    - All stories trigger Data Pipeline Agent
    - Final output is GitHub MR

    Subscribes to:
    - jira.stories: New/updated stories from Jira
    - jira.story.created: Story creation events (from webhook)

    Publishes to:
    - pipeline.requested: Triggers Data Pipeline Agent
    - agent.events: Agent actions and decisions
    - pipeline.mr.created: When GitHub MR is created
    """

    def __init__(self):
        """Initialize Kafka consumer and producer"""
        logger.info("jira_data_consumer_initializing",
                   bootstrap=KAFKA_BOOTSTRAP,
                   group=CONSUMER_GROUP)

        # Consumer for reading Jira events
        self.consumer = KafkaConsumer(
            'jira.stories',
            'jira.story.created',
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id=CONSUMER_GROUP,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            key_deserializer=lambda x: x.decode('utf-8') if x else None,
            consumer_timeout_ms=1000
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
        self.stories_cache = {}

        logger.info("jira_data_consumer_initialized")

    def _store_story_in_redis(self, story_data: dict):
        """Store story in Redis cache for API to read"""
        story_key = story_data.get('key', story_data.get('story_id'))
        if not story_key:
            return

        try:
            formatted_story = {
                "story_id": story_key,
                "key": story_key,
                "summary": story_data.get("summary", ""),
                "description": story_data.get("description", ""),
                "status": story_data.get("status", "Open"),
                "priority": story_data.get("priority", {}).get("name", "Medium"),
                "labels": story_data.get("labels", []),
                "project": story_data.get("project", {}),
                "assignee": story_data.get("assignee", {}).get("displayName"),
                "reporter": story_data.get("reporter", {}).get("displayName"),
                "created": story_data.get("created", datetime.now().isoformat()),
                "updated": story_data.get("updated", datetime.now().isoformat()),
                "type": "data_pipeline",  # All Jira stories are data pipeline
                "pipeline_status": "pending",
            }

            # Store individual story
            redis_key = f"jira:pipeline:{story_key}"
            redis_client.set(redis_key, json.dumps(formatted_story), ex=JIRA_CACHE_TTL)

            # Update in-memory cache
            self.stories_cache[story_key] = formatted_story

            # Update the stories list
            self._update_stories_list()

            logger.info("pipeline_story_stored_in_redis", story_key=story_key)

        except Exception as e:
            logger.error("redis_store_error", story_key=story_key, error=str(e))

    def _update_stories_list(self):
        """Update the full stories list in Redis for API consumption"""
        try:
            stories_list = list(self.stories_cache.values())
            stories_list.sort(key=lambda x: x.get('updated', ''), reverse=True)
            stories_list = stories_list[:50]

            cache_data = {
                "total": len(stories_list),
                "source": "jira_data_consumer",
                "type": "data_pipeline",
                "cached_at": datetime.now().isoformat(),
                "stories": stories_list
            }

            redis_client.set(JIRA_STORIES_CACHE_KEY, json.dumps(cache_data), ex=JIRA_CACHE_TTL)
            logger.info("pipeline_stories_list_updated", count=len(stories_list))

        except Exception as e:
            logger.error("redis_list_update_error", error=str(e))

    async def process_jira_story(self, story_data: dict):
        """
        Process a Jira story for Data Pipeline generation.

        ALL Jira stories go to Data Pipeline Agent.
        No IT Service routing - that's handled by ServiceNow.

        Flow:
        1. Parse pipeline requirements from story
        2. Publish to pipeline.requested topic
        3. Data Agent generates code
        4. Create GitHub MR
        """
        story_key = story_data.get('key', story_data.get('story_id'))

        if not story_key:
            logger.warning("story_missing_key", data=story_data)
            return

        logger.info("processing_jira_for_data_pipeline",
                   story_key=story_key,
                   source="jira.stories")

        # Store in Redis for UI
        self._store_story_in_redis(story_data)

        # Check idempotency
        cache_key = f"processed:pipeline:{story_key}"
        if redis_client.get(cache_key):
            logger.info("story_already_processed", story_key=story_key)
            return

        try:
            # Parse pipeline configuration from Jira story
            pipeline_request = self._parse_pipeline_config(story_data)

            # Create correlation ID for tracking
            correlation_id = f"jira-{story_key}-{datetime.now().strftime('%Y%m%d%H%M%S')}"

            # Publish to pipeline.requested topic
            # This triggers the Data Pipeline Agent
            event = {
                "event_type": "pipeline.requested",
                "timestamp": datetime.now().isoformat(),
                "source": "jira",
                "correlation_id": correlation_id,
                "jira_key": story_key,
                "request": pipeline_request,
                "workflow": {
                    "steps": [
                        "analyze_source",
                        "generate_ir",
                        "generate_spark",
                        "generate_dag",
                        "generate_dq",
                        "create_github_mr"
                    ],
                    "current_step": "analyze_source",
                    "github_mr_required": True
                }
            }

            self.producer.send(
                'pipeline.requested',
                key=story_key,
                value=event
            )
            self.producer.flush()

            # Publish tracking event
            self.producer.send(
                'agent.events',
                key=story_key,
                value={
                    "event_type": "jira_story_received",
                    "story_key": story_key,
                    "agent": "data-pipeline",
                    "correlation_id": correlation_id,
                    "status": "processing",
                    "timestamp": datetime.now().isoformat()
                }
            )

            # Update story status in Redis
            self._update_story_status(story_key, "processing", correlation_id)

            # Mark as processed
            redis_client.set(cache_key, correlation_id, ex=86400)  # 24 hour TTL
            self.processed_count += 1

            logger.info("pipeline_request_published",
                       story_key=story_key,
                       correlation_id=correlation_id,
                       total_processed=self.processed_count)

        except Exception as e:
            logger.error("story_processing_error",
                        story_key=story_key,
                        error=str(e))

            self.producer.send(
                'agent.events',
                key=story_key,
                value={
                    "event_type": "pipeline_request_error",
                    "story_key": story_key,
                    "error": str(e),
                    "timestamp": datetime.now().isoformat()
                }
            )

    def _update_story_status(self, story_key: str, status: str, correlation_id: str):
        """Update story status in Redis cache"""
        try:
            redis_key = f"jira:pipeline:{story_key}"
            cached = redis_client.get(redis_key)
            if cached:
                story = json.loads(cached)
                story["pipeline_status"] = status
                story["correlation_id"] = correlation_id
                story["updated"] = datetime.now().isoformat()
                redis_client.set(redis_key, json.dumps(story), ex=JIRA_CACHE_TTL)
        except Exception as e:
            logger.error("status_update_error", story_key=story_key, error=str(e))

    def _parse_pipeline_config(self, story_data: dict) -> dict:
        """
        Parse pipeline configuration from Jira story.

        Expected format in description:
        - Source: gs://bucket/path or bigquery://project.dataset.table
        - Source Type: gcs, bigquery, postgres, mysql
        - Target Layer: bronze, silver, gold
        - Schedule: @daily, @hourly, cron expression
        - Business Context: Description of pipeline purpose
        """
        description = story_data.get("description", "")
        summary = story_data.get("summary", "")

        def extract_field(text, field_name):
            import re
            patterns = [
                rf"{field_name}\s*:\s*(.+?)(?:\n|$)",
                rf"{field_name}\s*=\s*(.+?)(?:\n|$)",
                rf"\*\*{field_name}\*\*\s*:\s*(.+?)(?:\n|$)",
            ]
            for pattern in patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    return match.group(1).strip()
            return None

        source_uri = extract_field(description, "Source") or ""
        source_type = extract_field(description, "Source Type") or self._infer_source_type(source_uri)
        target_layer = extract_field(description, "Target Layer") or "silver"
        schedule = extract_field(description, "Schedule") or "@daily"
        business_context = extract_field(description, "Business Context") or summary

        # Extract transformations if specified
        transformations = []
        if "transform" in description.lower():
            transform_section = extract_field(description, "Transformations")
            if transform_section:
                transformations = [t.strip() for t in transform_section.split(",")]

        return {
            "source_uri": source_uri,
            "source_type": source_type,
            "target_layer": target_layer.lower(),
            "schedule": schedule,
            "business_context": business_context,
            "transformations": transformations,
            "jira_key": story_data.get("key"),
            "jira_summary": summary,
            "jira_project": story_data.get("project", {}).get("key"),
            "requested_by": story_data.get("reporter", {}).get("displayName"),
            "assignee": story_data.get("assignee", {}).get("displayName"),
            "priority": story_data.get("priority", {}).get("name", "Medium"),
            "labels": story_data.get("labels", []),
            # GitHub MR settings
            "github": {
                "owner": GITHUB_OWNER,
                "repo": GITHUB_REPO,
                "base_branch": GITHUB_BASE_BRANCH,
                "branch_name": f"pipeline/{story_data.get('key', 'unknown').lower()}",
                "create_mr": True
            }
        }

    def _infer_source_type(self, source_uri: str) -> str:
        """Infer source type from URI"""
        if not source_uri:
            return "gcs"

        uri_lower = source_uri.lower()
        if uri_lower.startswith("gs://"):
            return "gcs"
        elif "bigquery" in uri_lower or uri_lower.startswith("bq://"):
            return "bigquery"
        elif uri_lower.startswith("postgres://") or "postgresql" in uri_lower:
            return "postgres"
        elif uri_lower.startswith("mysql://"):
            return "mysql"
        elif uri_lower.startswith("s3://"):
            return "s3"
        elif uri_lower.startswith("jdbc:"):
            return "jdbc"
        return "gcs"

    def start(self):
        """Start the consumer loop"""
        self.running = True
        logger.info("jira_data_consumer_started",
                   topics=['jira.stories', 'jira.story.created'])

        print("=" * 60)
        print("  JIRA → DATA PIPELINE CONSUMER STARTED")
        print("=" * 60)
        print(f"  Bootstrap: {KAFKA_BOOTSTRAP}")
        print(f"  Group ID:  {CONSUMER_GROUP}")
        print(f"  Topics:    jira.stories, jira.story.created")
        print("=" * 60)
        print("  SCOPE: Data Pipeline Build/Change/Enhancement ONLY")
        print("  - All Jira stories → Data Pipeline Agent")
        print("  - IT Service issues → Use ServiceNow instead")
        print("=" * 60)
        print("  Workflow:")
        print("    1. Jira Story → Parse pipeline config")
        print("    2. Data Agent → Generate Spark/DAG/DQ code")
        print("    3. Create GitHub Merge Request")
        print("    4. CI/CD → Validate & Deploy")
        print("    5. Airflow → Execute pipeline")
        print("=" * 60)
        print("  Waiting for Jira stories...")
        print()

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            while self.running:
                messages = self.consumer.poll(timeout_ms=1000)

                for topic_partition, records in messages.items():
                    topic = topic_partition.topic

                    for record in records:
                        print(f"\n{'='*60}")
                        print(f"  NEW JIRA STORY from {topic}")
                        print(f"  Key: {record.key}")
                        print(f"  → Routing to Data Pipeline Agent")
                        print(f"  Timestamp: {datetime.now().isoformat()}")
                        print(f"{'='*60}")

                        loop.run_until_complete(
                            self.process_jira_story(record.value)
                        )

        except KeyboardInterrupt:
            logger.info("jira_data_consumer_interrupted")
        finally:
            self.stop()
            loop.close()

    def stop(self):
        """Stop the consumer"""
        self.running = False
        self.consumer.close()
        self.producer.close()
        logger.info("jira_data_consumer_stopped", total_processed=self.processed_count)
        print(f"\nConsumer stopped. Total processed: {self.processed_count}")


def main():
    """Main entry point"""
    consumer = JiraConsumer()
    consumer.start()


if __name__ == "__main__":
    main()
