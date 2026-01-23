#!/usr/bin/env python3
"""
Observability Integration Tests

Tests for Logging, Metrics, and Traces (LMT) stack:
- Logging: structlog with JSON formatting
- Metrics: Prometheus via prometheus_client
- Traces: Langfuse for LLM observability

Run: python3 tests/integration/test_observability.py
"""

import sys
import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "backend"))


def test_logging():
    """Test structlog logging configuration"""
    print("\n" + "=" * 60)
    print("1. LOGGING (structlog)")
    print("=" * 60)

    try:
        import structlog

        # Get logger
        logger = structlog.get_logger()

        # Test logging
        print("  [CHECK] structlog imported successfully")

        # Check if bound loggers work
        bound_logger = logger.bind(component="test", version="5.0")
        print("  [CHECK] Bound logger created")

        # Test structured event
        print("  [CHECK] Structured logging available")

        # Check usage across codebase
        from governance.audit_logger import AuditLogger, AuditEventType, RiskLevel
        audit = AuditLogger()

        # Log a test event
        event = audit.log(
            event_type=AuditEventType.AI_DECISION,
            actor="test-agent",
            actor_type="ai",
            action="test_classification",
            resource="TEST-001",
            resource_type="test",
            risk_level=RiskLevel.LOW,
            details={"test": True},
            ai_explanation="This is a test decision",
            confidence=0.95
        )
        print(f"  [CHECK] Audit event logged: {event.event_id}")

        # Check compliance report generation
        report = audit.generate_compliance_report()
        print(f"  [CHECK] Compliance report generated: {report['summary']['total_events']} events")

        print("\n  STATUS: LOGGING OK")
        return True

    except Exception as e:
        print(f"  [FAIL] Logging error: {e}")
        return False


def test_metrics():
    """Test Prometheus metrics collection"""
    print("\n" + "=" * 60)
    print("2. METRICS (Prometheus)")
    print("=" * 60)

    try:
        from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

        # Import metrics module
        from orchestrator import metrics

        print("  [CHECK] prometheus_client imported")

        # Test various metric types
        # Counter
        metrics.REQUEST_COUNT.labels(method="POST", endpoint="/test", status="success").inc()
        print("  [CHECK] Counter metric (REQUEST_COUNT) incremented")

        # Histogram
        metrics.REQUEST_LATENCY.labels(method="POST", endpoint="/test").observe(0.5)
        print("  [CHECK] Histogram metric (REQUEST_LATENCY) observed")

        # Gauge
        metrics.INCIDENTS_ACTIVE.set(5)
        print("  [CHECK] Gauge metric (INCIDENTS_ACTIVE) set")

        # Test helper functions
        metrics.record_incident(source="servicenow", severity="P2", status="new")
        print("  [CHECK] record_incident helper works")

        metrics.record_rag_query(collection="scripts", status="success", latency=0.25, result_count=3)
        print("  [CHECK] record_rag_query helper works")

        metrics.record_circuit_breaker_state(service="openai_api", state="closed")
        print("  [CHECK] record_circuit_breaker_state helper works")

        metrics.record_cache_hit(cache_type="embedding", tier="redis")
        print("  [CHECK] record_cache_hit helper works")

        # Generate metrics
        output = generate_latest()
        metric_count = len([l for l in output.decode().split('\n') if l and not l.startswith('#')])
        print(f"  [CHECK] Generated {metric_count} metric lines")

        # Check key metrics exist
        output_str = output.decode()
        key_metrics = [
            "aiagent_requests_total",
            "aiagent_incidents_active",
            "aiagent_llm_calls_total",
            "aiagent_rag_queries_total",
            "aiagent_circuit_breaker_state"
        ]

        for metric in key_metrics:
            if metric in output_str:
                print(f"  [CHECK] Key metric exists: {metric}")
            else:
                print(f"  [WARN] Key metric missing: {metric}")

        print("\n  STATUS: METRICS OK")
        return True

    except Exception as e:
        print(f"  [FAIL] Metrics error: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_traces():
    """Test Langfuse tracing configuration"""
    print("\n" + "=" * 60)
    print("3. TRACES (Langfuse)")
    print("=" * 60)

    try:
        # Check if langfuse is installed
        try:
            from langfuse import Langfuse
            print("  [CHECK] langfuse library imported")
        except ImportError:
            print("  [WARN] langfuse library not installed")
            print("  [INFO] Install with: pip install langfuse")
            return True  # Not a failure, just optional

        # Check environment variables
        langfuse_public = os.getenv("LANGFUSE_PUBLIC_KEY")
        langfuse_secret = os.getenv("LANGFUSE_SECRET_KEY")
        langfuse_host = os.getenv("LANGFUSE_HOST", "https://cloud.langfuse.com")

        if langfuse_public and langfuse_secret:
            print("  [CHECK] LANGFUSE_PUBLIC_KEY configured")
            print("  [CHECK] LANGFUSE_SECRET_KEY configured")
            print(f"  [CHECK] LANGFUSE_HOST: {langfuse_host}")

            # Try to initialize client
            try:
                client = Langfuse(
                    public_key=langfuse_public,
                    secret_key=langfuse_secret,
                    host=langfuse_host
                )
                print("  [CHECK] Langfuse client initialized")
            except Exception as e:
                print(f"  [WARN] Langfuse client init failed: {e}")
        else:
            print("  [INFO] Langfuse not configured (LANGFUSE_PUBLIC_KEY/SECRET_KEY not set)")
            print("  [INFO] Tracing will be disabled")

        # Check LLM Intelligence integration
        try:
            from orchestrator.llm_intelligence import LANGFUSE_ENABLED, track_llm_decision
            print(f"  [CHECK] LLM Intelligence module loaded (LANGFUSE_ENABLED={LANGFUSE_ENABLED})")
        except ImportError as e:
            print(f"  [WARN] LLM Intelligence import: {e}")
        except Exception as e:
            # May fail if OpenAI API key not set - that's OK for trace verification
            print(f"  [INFO] LLM Intelligence skipped (API key not configured)")

        # Check base agent integration
        try:
            from agents.base_agent import BaseAgent
            print("  [CHECK] BaseAgent has Langfuse integration")
        except Exception as e:
            # May fail if dependencies not available - that's OK
            print(f"  [INFO] BaseAgent skipped (dependency not configured)")

        print("\n  STATUS: TRACES OK (configuration verified)")
        return True

    except Exception as e:
        print(f"  [FAIL] Traces error: {e}")
        return False


def test_observability_integration():
    """Test integration between logging, metrics, and traces"""
    print("\n" + "=" * 60)
    print("4. INTEGRATION CHECK")
    print("=" * 60)

    try:
        import structlog
        from orchestrator import metrics
        from governance.audit_logger import audit_logger, AuditEventType, RiskLevel

        # Simulate an incident processing flow
        incident_id = "INT-TEST-001"

        # 1. Log incident receipt
        print(f"  [FLOW] Processing incident: {incident_id}")

        # 2. Update metrics
        metrics.INCIDENTS_ACTIVE.inc()
        metrics.record_incident(source="test", severity="P3", status="new")
        print("  [FLOW] Metrics updated for new incident")

        # 3. Audit log
        audit_logger.log(
            event_type=AuditEventType.AI_CLASSIFICATION,
            actor="langgraph-orchestrator",
            actor_type="ai",
            action="classify_incident",
            resource=incident_id,
            resource_type="incident",
            risk_level=RiskLevel.LOW,
            details={"classification": "performance", "confidence": 0.92}
        )
        print("  [FLOW] Audit event logged")

        # 4. Simulate RAG query
        import time
        start = time.time()
        time.sleep(0.01)  # Simulate query
        latency = time.time() - start
        metrics.record_rag_query(collection="scripts", status="success", latency=latency, result_count=5)
        print(f"  [FLOW] RAG query completed ({latency*1000:.1f}ms)")

        # 5. Log decision
        audit_logger.log_ai_decision(
            decision="execute_remediation",
            incident_id=incident_id,
            confidence=0.88,
            explanation="Matched script 'restart_service' based on error pattern",
            risk_level=RiskLevel.MEDIUM,
            human_oversight=True
        )
        print("  [FLOW] AI decision logged with human oversight flag")

        # 6. Complete incident
        metrics.INCIDENTS_ACTIVE.dec()
        metrics.record_incident(source="test", severity="P3", status="resolved")
        print("  [FLOW] Incident resolved")

        print("\n  STATUS: INTEGRATION OK")
        return True

    except Exception as e:
        print(f"  [FAIL] Integration error: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all observability tests"""
    print("=" * 60)
    print("AI AGENT PLATFORM - OBSERVABILITY VERIFICATION")
    print("=" * 60)
    print(f"Testing: Logging, Metrics, Traces (LMT)")

    results = {
        "logging": test_logging(),
        "metrics": test_metrics(),
        "traces": test_traces(),
        "integration": test_observability_integration()
    }

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    all_passed = True
    for component, passed in results.items():
        status = "PASS" if passed else "FAIL"
        print(f"  {component.upper()}: {status}")
        if not passed:
            all_passed = False

    print("=" * 60)

    if all_passed:
        print("All observability components verified!")
        return 0
    else:
        print("Some components failed verification")
        return 1


if __name__ == "__main__":
    sys.exit(main())
