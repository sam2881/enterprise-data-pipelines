"""
OpenTelemetry Distributed Tracing v5.0
======================================

Provides distributed tracing across all services using OpenTelemetry.

WHY OpenTelemetry:
- Industry standard for distributed tracing
- Vendor-agnostic (works with Jaeger, Zipkin, DataDog, etc.)
- Auto-instrumentation for FastAPI, Redis, Kafka, httpx
- Trace context propagation across services

HOW:
1. Call setup_tracing() once at application startup
2. Use @traced decorator for custom spans
3. Use get_tracer() for manual span creation
4. Trace context is automatically propagated via HTTP headers

Components:
- TracerProvider: Creates and manages traces
- SpanProcessor: Batches and exports spans
- OTLPSpanExporter: Exports to OTLP collector (Jaeger, Tempo, etc.)
- Auto-instrumentations: FastAPI, Redis, Kafka, httpx

Environment Variables:
- OTEL_SERVICE_NAME: Service name (default: ai-agent-platform)
- OTEL_EXPORTER_OTLP_ENDPOINT: OTLP collector endpoint
- OTEL_EXPORTER_OTLP_HEADERS: Auth headers for collector
- OTEL_TRACES_SAMPLER: Sampling strategy (default: always_on)
- OTEL_ENABLED: Enable/disable tracing (default: true)

Version: 5.0.0
Author: AI Agent Platform
"""

import os
import functools
from typing import Any, Callable, Dict, Optional, TypeVar
from contextlib import contextmanager
import structlog

logger = structlog.get_logger()

# Type variable for decorator
F = TypeVar('F', bound=Callable[..., Any])

# =============================================================================
# CONFIGURATION
# =============================================================================

OTEL_ENABLED = os.getenv("OTEL_ENABLED", "true").lower() == "true"
OTEL_SERVICE_NAME = os.getenv("OTEL_SERVICE_NAME", "ai-agent-platform")
OTEL_EXPORTER_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")

# Global tracer instance
_tracer = None
_tracer_provider = None


# =============================================================================
# SETUP FUNCTIONS
# =============================================================================

def setup_tracing(
    service_name: Optional[str] = None,
    otlp_endpoint: Optional[str] = None,
    enable_fastapi: bool = True,
    enable_redis: bool = True,
    enable_kafka: bool = True,
    enable_httpx: bool = True
) -> bool:
    """
    Initialize OpenTelemetry tracing for the application.

    Call this once at application startup (in main.py).

    Args:
        service_name: Override OTEL_SERVICE_NAME env var
        otlp_endpoint: Override OTEL_EXPORTER_OTLP_ENDPOINT env var
        enable_fastapi: Enable FastAPI auto-instrumentation
        enable_redis: Enable Redis auto-instrumentation
        enable_kafka: Enable Kafka auto-instrumentation
        enable_httpx: Enable httpx auto-instrumentation

    Returns:
        True if tracing was successfully initialized

    Example:
        from backend.utils.otel_tracing import setup_tracing

        app = FastAPI()

        @app.on_event("startup")
        async def startup():
            setup_tracing(service_name="orchestrator")
    """
    global _tracer, _tracer_provider

    if not OTEL_ENABLED:
        logger.info("otel_tracing_disabled")
        return False

    try:
        from opentelemetry import trace
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
        from opentelemetry.sdk.resources import Resource, SERVICE_NAME
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

        # Create resource with service name
        svc_name = service_name or OTEL_SERVICE_NAME
        resource = Resource.create({
            SERVICE_NAME: svc_name,
            "service.version": "5.0.0",
            "deployment.environment": os.getenv("ENVIRONMENT", "development")
        })

        # Create tracer provider
        _tracer_provider = TracerProvider(resource=resource)

        # Configure OTLP exporter
        endpoint = otlp_endpoint or OTEL_EXPORTER_ENDPOINT
        exporter = OTLPSpanExporter(endpoint=endpoint, insecure=True)

        # Add batch processor for efficient export
        span_processor = BatchSpanProcessor(exporter)
        _tracer_provider.add_span_processor(span_processor)

        # Set as global tracer provider
        trace.set_tracer_provider(_tracer_provider)

        # Get tracer for this service
        _tracer = trace.get_tracer(svc_name, "5.0.0")

        # Auto-instrumentation
        if enable_fastapi:
            _instrument_fastapi()

        if enable_redis:
            _instrument_redis()

        if enable_kafka:
            _instrument_kafka()

        if enable_httpx:
            _instrument_httpx()

        logger.info(
            "otel_tracing_initialized",
            service_name=svc_name,
            endpoint=endpoint,
            fastapi=enable_fastapi,
            redis=enable_redis,
            kafka=enable_kafka,
            httpx=enable_httpx
        )

        return True

    except ImportError as e:
        logger.warning("otel_tracing_import_error", error=str(e))
        return False
    except Exception as e:
        logger.error("otel_tracing_setup_failed", error=str(e))
        return False


def _instrument_fastapi():
    """Auto-instrument FastAPI"""
    try:
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
        FastAPIInstrumentor().instrument()
        logger.debug("otel_fastapi_instrumented")
    except Exception as e:
        logger.warning("otel_fastapi_instrumentation_failed", error=str(e))


def _instrument_redis():
    """Auto-instrument Redis"""
    try:
        from opentelemetry.instrumentation.redis import RedisInstrumentor
        RedisInstrumentor().instrument()
        logger.debug("otel_redis_instrumented")
    except Exception as e:
        logger.warning("otel_redis_instrumentation_failed", error=str(e))


def _instrument_kafka():
    """Auto-instrument Kafka"""
    try:
        from opentelemetry.instrumentation.kafka import KafkaInstrumentor
        KafkaInstrumentor().instrument()
        logger.debug("otel_kafka_instrumented")
    except Exception as e:
        logger.warning("otel_kafka_instrumentation_failed", error=str(e))


def _instrument_httpx():
    """Auto-instrument httpx"""
    try:
        from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
        HTTPXClientInstrumentor().instrument()
        logger.debug("otel_httpx_instrumented")
    except Exception as e:
        logger.warning("otel_httpx_instrumentation_failed", error=str(e))


# =============================================================================
# TRACER ACCESS
# =============================================================================

def get_tracer(name: Optional[str] = None):
    """
    Get an OpenTelemetry tracer instance.

    Args:
        name: Optional tracer name (defaults to service name)

    Returns:
        Tracer instance or NoOpTracer if tracing not enabled

    Example:
        tracer = get_tracer("my-component")
        with tracer.start_as_current_span("my-operation") as span:
            span.set_attribute("key", "value")
            # ... do work ...
    """
    if _tracer is not None:
        return _tracer

    # Return a no-op tracer if not initialized
    try:
        from opentelemetry import trace
        return trace.get_tracer(name or OTEL_SERVICE_NAME)
    except ImportError:
        return _NoOpTracer()


class _NoOpTracer:
    """No-op tracer for when OpenTelemetry is not available"""

    def start_span(self, name: str, **kwargs):
        return _NoOpSpan()

    def start_as_current_span(self, name: str, **kwargs):
        return _NoOpSpanContext()


class _NoOpSpan:
    """No-op span"""

    def set_attribute(self, key: str, value: Any):
        pass

    def set_status(self, status):
        pass

    def record_exception(self, exception):
        pass

    def end(self):
        pass


class _NoOpSpanContext:
    """No-op span context manager"""

    def __enter__(self):
        return _NoOpSpan()

    def __exit__(self, *args):
        pass


# =============================================================================
# DECORATORS
# =============================================================================

def traced(
    span_name: Optional[str] = None,
    attributes: Optional[Dict[str, Any]] = None,
    record_exception: bool = True
) -> Callable[[F], F]:
    """
    Decorator to trace a function with OpenTelemetry.

    Args:
        span_name: Custom span name (defaults to function name)
        attributes: Static attributes to add to span
        record_exception: Whether to record exceptions in span

    Returns:
        Decorated function

    Example:
        @traced(span_name="process_incident", attributes={"component": "orchestrator"})
        async def process_incident(incident_id: str):
            # ... processing logic ...
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            tracer = get_tracer()
            name = span_name or func.__name__

            with tracer.start_as_current_span(name) as span:
                # Add static attributes
                if attributes:
                    for key, value in attributes.items():
                        span.set_attribute(key, value)

                try:
                    result = await func(*args, **kwargs)
                    return result
                except Exception as e:
                    if record_exception:
                        span.record_exception(e)
                        try:
                            from opentelemetry.trace import StatusCode
                            span.set_status(StatusCode.ERROR, str(e))
                        except ImportError:
                            pass
                    raise

        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            tracer = get_tracer()
            name = span_name or func.__name__

            with tracer.start_as_current_span(name) as span:
                # Add static attributes
                if attributes:
                    for key, value in attributes.items():
                        span.set_attribute(key, value)

                try:
                    result = func(*args, **kwargs)
                    return result
                except Exception as e:
                    if record_exception:
                        span.record_exception(e)
                        try:
                            from opentelemetry.trace import StatusCode
                            span.set_status(StatusCode.ERROR, str(e))
                        except ImportError:
                            pass
                    raise

        import asyncio
        if asyncio.iscoroutinefunction(func):
            return async_wrapper  # type: ignore
        return sync_wrapper  # type: ignore

    return decorator


# =============================================================================
# CONTEXT MANAGERS
# =============================================================================

@contextmanager
def trace_span(
    name: str,
    attributes: Optional[Dict[str, Any]] = None,
    record_exception: bool = True
):
    """
    Context manager for creating a trace span.

    Args:
        name: Span name
        attributes: Attributes to add to span
        record_exception: Whether to record exceptions

    Yields:
        The span object

    Example:
        with trace_span("database_query", {"db": "postgres"}) as span:
            result = db.execute(query)
            span.set_attribute("row_count", len(result))
    """
    tracer = get_tracer()

    with tracer.start_as_current_span(name) as span:
        if attributes:
            for key, value in attributes.items():
                span.set_attribute(key, value)

        try:
            yield span
        except Exception as e:
            if record_exception:
                span.record_exception(e)
                try:
                    from opentelemetry.trace import StatusCode
                    span.set_status(StatusCode.ERROR, str(e))
                except ImportError:
                    pass
            raise


# =============================================================================
# TRACE CONTEXT HELPERS
# =============================================================================

def get_current_trace_id() -> Optional[str]:
    """
    Get the current trace ID as a hex string.

    Returns:
        Trace ID hex string or None if no active trace

    Example:
        trace_id = get_current_trace_id()
        logger.info("processing_request", trace_id=trace_id)
    """
    try:
        from opentelemetry import trace
        span = trace.get_current_span()
        if span:
            context = span.get_span_context()
            if context.is_valid:
                return format(context.trace_id, '032x')
    except Exception:
        pass
    return None


def get_current_span_id() -> Optional[str]:
    """
    Get the current span ID as a hex string.

    Returns:
        Span ID hex string or None if no active span
    """
    try:
        from opentelemetry import trace
        span = trace.get_current_span()
        if span:
            context = span.get_span_context()
            if context.is_valid:
                return format(context.span_id, '016x')
    except Exception:
        pass
    return None


def inject_trace_headers(headers: Dict[str, str]) -> Dict[str, str]:
    """
    Inject trace context headers for propagation.

    Use this when making HTTP calls to other services to propagate
    the trace context.

    Args:
        headers: Existing headers dict

    Returns:
        Headers dict with trace context headers added

    Example:
        headers = {"Authorization": "Bearer ..."}
        headers = inject_trace_headers(headers)
        response = await httpx.get(url, headers=headers)
    """
    try:
        from opentelemetry.propagate import inject
        inject(headers)
    except Exception:
        pass
    return headers


# =============================================================================
# SHUTDOWN
# =============================================================================

def shutdown_tracing():
    """
    Gracefully shutdown tracing and flush pending spans.

    Call this on application shutdown to ensure all spans are exported.

    Example:
        @app.on_event("shutdown")
        async def shutdown():
            shutdown_tracing()
    """
    global _tracer_provider

    if _tracer_provider is not None:
        try:
            _tracer_provider.shutdown()
            logger.info("otel_tracing_shutdown")
        except Exception as e:
            logger.warning("otel_tracing_shutdown_failed", error=str(e))


# =============================================================================
# ALIASES FOR BACKWARD COMPATIBILITY
# =============================================================================

# Alias: trace_async -> traced (decorator works for both sync and async)
trace_async = traced

# Alias: configure_tracing -> setup_tracing
configure_tracing = setup_tracing
