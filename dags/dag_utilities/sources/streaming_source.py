"""
APEX Data Agent - Streaming Source Utilities

Pub/Sub, Kafka consumer management and micro-batch windowing.
"""

from typing import Any, Dict, List, Optional


def get_pubsub_consumer(subscription: str, project: Optional[str] = None, **context) -> Dict[str, Any]:
    """Consume messages from Google Pub/Sub subscription."""
    from google.cloud import pubsub_v1
    import os

    project = project or os.getenv("GCP_PROJECT", "")
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project, subscription)

    messages = []
    response = subscriber.pull(request={"subscription": subscription_path, "max_messages": 1000})

    for msg in response.received_messages:
        messages.append(msg.message.data.decode("utf-8"))

    # Acknowledge
    if response.received_messages:
        ack_ids = [msg.ack_id for msg in response.received_messages]
        subscriber.acknowledge(request={"subscription": subscription_path, "ack_ids": ack_ids})

    context["ti"].xcom_push(key="message_count", value=len(messages))
    print(f"[streaming_source] Consumed {len(messages)} messages from {subscription}")
    return {"messages": messages, "count": len(messages)}


def get_kafka_consumer(topic: str, bootstrap_servers: str, group_id: str, **context) -> Dict[str, Any]:
    """Consume batch of messages from Kafka topic."""
    try:
        from kafka import KafkaConsumer
        import json

        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers.split(","),
            group_id=group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            consumer_timeout_ms=30000,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )

        messages = []
        for msg in consumer:
            messages.append(msg.value)
            if len(messages) >= 10000:
                break

        consumer.commit()
        consumer.close()

        context["ti"].xcom_push(key="message_count", value=len(messages))
        return {"messages": messages, "count": len(messages)}
    except ImportError:
        print("[streaming_source] kafka-python not installed. Skipping Kafka consumption.")
        return {"messages": [], "count": 0}


def stage_messages_to_gcs(messages: List[Any], target_path: str, **context) -> str:
    """Stage consumed messages to GCS as JSON for downstream processing."""
    import json
    from google.cloud import storage

    bucket_name = target_path.replace("gs://", "").split("/")[0]
    blob_path = "/".join(target_path.replace("gs://", "").split("/")[1:])

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)

    content = "\n".join(json.dumps(m) if isinstance(m, dict) else str(m) for m in messages)
    blob.upload_from_string(content, content_type="application/json")

    print(f"[streaming_source] Staged {len(messages)} messages to {target_path}")
    return target_path
