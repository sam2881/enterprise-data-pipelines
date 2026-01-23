"""Unit tests for utility modules (V5 Architecture)"""
import pytest
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# Add backend to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../backend'))


@pytest.mark.skip(reason="redis_client.py was removed - Redis is used via direct redis.Redis")
class TestRedisClient:
    """Test Redis client utilities - DEPRECATED"""
    pass


@pytest.mark.skip(reason="kafka_client.py was removed - Use backend.streaming.kafka_producer")
class TestKafkaClient:
    """Test Kafka client utilities - DEPRECATED"""
    pass


class TestCircuitBreaker:
    """Test circuit breaker utility"""

    def test_circuit_breaker_import(self):
        """Test circuit breaker can be imported"""
        from backend.utils.circuit_breaker import CircuitBreaker, CircuitState

        assert CircuitBreaker is not None
        assert CircuitState is not None

    def test_circuit_breaker_initialization(self):
        """Test circuit breaker initializes with correct defaults"""
        from backend.utils.circuit_breaker import CircuitBreaker, CircuitState

        cb = CircuitBreaker(name="test-breaker")

        assert cb.stats['consecutive_failures'] == 0
        assert cb.state == CircuitState.CLOSED

    def test_circuit_breaker_call_success(self):
        """Test successful calls don't affect circuit state"""
        from backend.utils.circuit_breaker import CircuitBreaker

        cb = CircuitBreaker(name="test-breaker-2")

        def success_func():
            return "success"

        result = cb.call_sync(success_func)
        assert result == "success"
        assert cb.stats['consecutive_failures'] == 0


class TestCostTracker:
    """Test cost tracking utility"""

    def test_cost_tracker_import(self):
        """Test cost tracker can be imported"""
        from backend.utils.cost_tracker import CostTracker, cost_tracker

        assert CostTracker is not None
        assert cost_tracker is not None

    def test_cost_tracker_calculate_cost(self):
        """Test cost calculation"""
        from backend.utils.cost_tracker import CostTracker

        tracker = CostTracker()
        # GPT-4 pricing: $30/1M input, $60/1M output
        cost = tracker.calculate_cost(
            model="gpt-4",
            input_tokens=1000,
            output_tokens=500
        )
        assert cost > 0


class TestOtelTracing:
    """Test OpenTelemetry tracing utility"""

    def test_otel_tracing_import(self):
        """Test OTEL tracing can be imported"""
        from backend.utils.otel_tracing import get_tracer

        assert get_tracer is not None

    def test_get_tracer(self):
        """Test getting a tracer"""
        from backend.utils.otel_tracing import get_tracer

        tracer = get_tracer("test-service")
        assert tracer is not None


class TestRegistryManager:
    """Test script registry manager"""

    def test_registry_manager_import(self):
        """Test registry manager can be imported"""
        from backend.utils.registry_manager import RegistryManager, registry_manager

        assert RegistryManager is not None
        assert registry_manager is not None

    def test_registry_manager_singleton(self):
        """Test registry manager is singleton"""
        from backend.utils.registry_manager import registry_manager, RegistryManager

        # The global instance should be available
        assert isinstance(registry_manager, RegistryManager)


class TestSlackNotifier:
    """Test Slack notification utility"""

    def test_slack_notifier_import(self):
        """Test Slack notifier can be imported"""
        from backend.utils.slack_notifier import SlackNotifier

        assert SlackNotifier is not None

    @patch.dict(os.environ, {'SLACK_BOT_TOKEN': '', 'SLACK_CHANNEL': ''})
    def test_slack_notifier_disabled_without_token(self):
        """Test Slack notifier is disabled without token"""
        from backend.utils.slack_notifier import SlackNotifier

        notifier = SlackNotifier()
        # Should not raise even without token
        assert notifier is not None


class TestKafkaProducer:
    """Test the unified Kafka producer"""

    def test_kafka_producer_import(self):
        """Test unified Kafka producer can be imported"""
        from backend.streaming.kafka_producer import UnifiedKafkaProducer, get_producer

        assert UnifiedKafkaProducer is not None
        assert get_producer is not None

    def test_get_producer_returns_instance(self):
        """Test get_producer returns singleton"""
        from backend.streaming.kafka_producer import get_producer

        producer = get_producer()
        assert producer is not None

    @pytest.mark.asyncio
    async def test_publish_event_method(self):
        """Test publish_event method exists"""
        from backend.streaming.kafka_producer import get_producer

        producer = get_producer()
        assert hasattr(producer, 'publish_event')
        assert hasattr(producer, 'publish_incident_event')
        assert hasattr(producer, 'publish_pipeline_event')
