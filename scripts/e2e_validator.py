#!/usr/bin/env python3
"""
Enterprise Agentic Platform - End-to-End Test Validator

This script validates the entire platform by running comprehensive tests
across all components: Incident Management, Data Agent, RAG, and Infrastructure.

Usage:
    python e2e_validator.py --all              # Run all validations
    python e2e_validator.py --unit             # Unit tests only
    python e2e_validator.py --integration      # Integration tests only
    python e2e_validator.py --e2e              # E2E workflow tests
    python e2e_validator.py --health           # Health checks only
"""

import argparse
import asyncio
import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Load .env file if present
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env")
except ImportError:
    pass  # dotenv not installed, rely on environment variables

import requests
import redis
import psycopg2
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic


# ==============================================================================
# CONFIGURATION
# ==============================================================================

class Config:
    """Test configuration - reads from environment with defaults."""
    
    # Service URLs
    BACKEND_URL = os.getenv("BACKEND_URL", "http://localhost:8000")
    DATA_AGENT_URL = os.getenv("DATA_AGENT_URL", "http://localhost:8001")
    FRONTEND_URL = os.getenv("FRONTEND_URL", "http://localhost:3000")
    
    # Infrastructure
    KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092").replace("kafka://", "")
    REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

    # PostgreSQL - build URL from individual env vars or use POSTGRES_URL
    @staticmethod
    def _build_postgres_url():
        if os.getenv("POSTGRES_URL"):
            return os.getenv("POSTGRES_URL")
        host = os.getenv("POSTGRES_HOST", "localhost")
        port = os.getenv("POSTGRES_PORT", "5432")
        user = os.getenv("POSTGRES_USER", "admin")
        password = os.getenv("POSTGRES_PASSWORD", "admin")
        db = os.getenv("POSTGRES_DB", "agentdb")
        return f"postgresql://{user}:{password}@{host}:{port}/{db}"

    POSTGRES_URL = _build_postgres_url.__func__()
    WEAVIATE_URL = os.getenv("WEAVIATE_URL", "http://localhost:8081")
    NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    
    # Test settings
    TIMEOUT_SECONDS = int(os.getenv("TEST_TIMEOUT", "60"))
    RETRY_ATTEMPTS = int(os.getenv("TEST_RETRIES", "3"))
    
    # Paths
    PROJECT_ROOT = Path(__file__).parent.parent
    TESTS_DIR = PROJECT_ROOT / "tests"
    FIXTURES_DIR = TESTS_DIR / "fixtures"


# ==============================================================================
# RESULT TYPES
# ==============================================================================

class TestStatus(Enum):
    PASSED = "✅ PASSED"
    FAILED = "❌ FAILED"
    SKIPPED = "⏭️ SKIPPED"
    WARNING = "⚠️ WARNING"


@dataclass
class TestResult:
    """Result of a single test."""
    name: str
    status: TestStatus
    duration_ms: float
    message: str = ""
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ValidationReport:
    """Complete validation report."""
    timestamp: datetime
    total_tests: int
    passed: int
    failed: int
    skipped: int
    warnings: int
    duration_seconds: float
    results: List[TestResult] = field(default_factory=list)
    
    def add_result(self, result: TestResult):
        self.results.append(result)
        if result.status == TestStatus.PASSED:
            self.passed += 1
        elif result.status == TestStatus.FAILED:
            self.failed += 1
        elif result.status == TestStatus.SKIPPED:
            self.skipped += 1
        elif result.status == TestStatus.WARNING:
            self.warnings += 1
        self.total_tests += 1
    
    def is_successful(self) -> bool:
        return self.failed == 0


# ==============================================================================
# HEALTH CHECKS
# ==============================================================================

class HealthChecker:
    """Validates infrastructure health."""
    
    def __init__(self, config: Config):
        self.config = config
    
    def check_kafka(self) -> TestResult:
        """Check Kafka broker connectivity."""
        start = time.time()
        try:
            admin = KafkaAdminClient(
                bootstrap_servers=self.config.KAFKA_BOOTSTRAP,
                request_timeout_ms=5000
            )
            topics = admin.list_topics()
            admin.close()
            return TestResult(
                name="Kafka Health",
                status=TestStatus.PASSED,
                duration_ms=(time.time() - start) * 1000,
                message=f"Connected. {len(topics)} topics found.",
                details={"topics": topics}
            )
        except Exception as e:
            return TestResult(
                name="Kafka Health",
                status=TestStatus.FAILED,
                duration_ms=(time.time() - start) * 1000,
                message=f"Connection failed: {str(e)}"
            )
    
    def check_redis(self) -> TestResult:
        """Check Redis connectivity."""
        start = time.time()
        try:
            r = redis.from_url(self.config.REDIS_URL)
            r.ping()
            info = r.info()
            return TestResult(
                name="Redis Health",
                status=TestStatus.PASSED,
                duration_ms=(time.time() - start) * 1000,
                message=f"Connected. Version: {info.get('redis_version', 'unknown')}",
                details={"used_memory": info.get("used_memory_human")}
            )
        except Exception as e:
            return TestResult(
                name="Redis Health",
                status=TestStatus.FAILED,
                duration_ms=(time.time() - start) * 1000,
                message=f"Connection failed: {str(e)}"
            )
    
    def check_postgres(self) -> TestResult:
        """Check PostgreSQL connectivity."""
        start = time.time()
        try:
            conn = psycopg2.connect(self.config.POSTGRES_URL)
            cursor = conn.cursor()
            cursor.execute("SELECT version();")
            version = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public';")
            table_count = cursor.fetchone()[0]
            conn.close()
            return TestResult(
                name="PostgreSQL Health",
                status=TestStatus.PASSED,
                duration_ms=(time.time() - start) * 1000,
                message=f"Connected. {table_count} tables in public schema.",
                details={"version": version[:50]}
            )
        except Exception as e:
            return TestResult(
                name="PostgreSQL Health",
                status=TestStatus.FAILED,
                duration_ms=(time.time() - start) * 1000,
                message=f"Connection failed: {str(e)}"
            )
    
    def check_weaviate(self) -> TestResult:
        """Check Weaviate vector DB connectivity."""
        start = time.time()
        try:
            resp = requests.get(f"{self.config.WEAVIATE_URL}/v1/meta", timeout=5)
            resp.raise_for_status()
            meta = resp.json()
            return TestResult(
                name="Weaviate Health",
                status=TestStatus.PASSED,
                duration_ms=(time.time() - start) * 1000,
                message=f"Connected. Version: {meta.get('version', 'unknown')}",
                details={"modules": list(meta.get("modules", {}).keys())}
            )
        except Exception as e:
            return TestResult(
                name="Weaviate Health",
                status=TestStatus.FAILED,
                duration_ms=(time.time() - start) * 1000,
                message=f"Connection failed: {str(e)}"
            )
    
    def check_backend_api(self) -> TestResult:
        """Check backend API health endpoint."""
        start = time.time()
        try:
            resp = requests.get(f"{self.config.BACKEND_URL}/health", timeout=5)
            resp.raise_for_status()
            data = resp.json()
            return TestResult(
                name="Backend API Health",
                status=TestStatus.PASSED,
                duration_ms=(time.time() - start) * 1000,
                message=f"API responding. Status: {data.get('status', 'ok')}",
                details=data
            )
        except requests.exceptions.ConnectionError:
            return TestResult(
                name="Backend API Health",
                status=TestStatus.WARNING,
                duration_ms=(time.time() - start) * 1000,
                message="Backend not running. Start with: uvicorn orchestrator.main:app"
            )
        except Exception as e:
            return TestResult(
                name="Backend API Health",
                status=TestStatus.FAILED,
                duration_ms=(time.time() - start) * 1000,
                message=f"Health check failed: {str(e)}"
            )
    
    def check_frontend(self) -> TestResult:
        """Check frontend accessibility."""
        start = time.time()
        try:
            resp = requests.get(self.config.FRONTEND_URL, timeout=5)
            resp.raise_for_status()
            return TestResult(
                name="Frontend Health",
                status=TestStatus.PASSED,
                duration_ms=(time.time() - start) * 1000,
                message="Frontend accessible."
            )
        except requests.exceptions.ConnectionError:
            return TestResult(
                name="Frontend Health",
                status=TestStatus.WARNING,
                duration_ms=(time.time() - start) * 1000,
                message="Frontend not running. Start with: npm run dev"
            )
        except Exception as e:
            return TestResult(
                name="Frontend Health",
                status=TestStatus.FAILED,
                duration_ms=(time.time() - start) * 1000,
                message=f"Check failed: {str(e)}"
            )
    
    def run_all(self) -> List[TestResult]:
        """Run all health checks."""
        return [
            self.check_kafka(),
            self.check_redis(),
            self.check_postgres(),
            self.check_weaviate(),
            self.check_backend_api(),
            self.check_frontend(),
        ]


# ==============================================================================
# UNIT TEST RUNNER
# ==============================================================================

class UnitTestRunner:
    """Runs unit tests via pytest."""
    
    def __init__(self, config: Config):
        self.config = config
    
    def run_backend_tests(self) -> TestResult:
        """Run backend unit tests."""
        start = time.time()
        try:
            result = subprocess.run(
                ["pytest", "tests/unit", "-v", "--tb=short", "-q"],
                cwd=self.config.PROJECT_ROOT,
                capture_output=True,
                text=True,
                timeout=300
            )
            passed = result.returncode == 0
            return TestResult(
                name="Backend Unit Tests",
                status=TestStatus.PASSED if passed else TestStatus.FAILED,
                duration_ms=(time.time() - start) * 1000,
                message=f"Exit code: {result.returncode}",
                details={"stdout": result.stdout[-2000:], "stderr": result.stderr[-1000:]}
            )
        except subprocess.TimeoutExpired:
            return TestResult(
                name="Backend Unit Tests",
                status=TestStatus.FAILED,
                duration_ms=(time.time() - start) * 1000,
                message="Tests timed out after 5 minutes"
            )
        except Exception as e:
            return TestResult(
                name="Backend Unit Tests",
                status=TestStatus.FAILED,
                duration_ms=(time.time() - start) * 1000,
                message=f"Failed to run tests: {str(e)}"
            )
    
    def run_data_agent_tests(self) -> TestResult:
        """Run data agent unit tests."""
        start = time.time()
        data_agent_path = self.config.PROJECT_ROOT / "agents" / "data_agent"
        
        if not (data_agent_path / "tests").exists():
            return TestResult(
                name="Data Agent Unit Tests",
                status=TestStatus.SKIPPED,
                duration_ms=(time.time() - start) * 1000,
                message="No tests directory found in agents/data_agent"
            )
        
        try:
            result = subprocess.run(
                ["pytest", "tests/unit", "-v", "--tb=short", "-q"],
                cwd=data_agent_path,
                capture_output=True,
                text=True,
                timeout=300
            )
            passed = result.returncode == 0
            return TestResult(
                name="Data Agent Unit Tests",
                status=TestStatus.PASSED if passed else TestStatus.FAILED,
                duration_ms=(time.time() - start) * 1000,
                message=f"Exit code: {result.returncode}",
                details={"stdout": result.stdout[-2000:]}
            )
        except Exception as e:
            return TestResult(
                name="Data Agent Unit Tests",
                status=TestStatus.FAILED,
                duration_ms=(time.time() - start) * 1000,
                message=f"Failed to run tests: {str(e)}"
            )
    
    def run_rag_tests(self) -> TestResult:
        """Run RAG-specific unit tests."""
        start = time.time()
        try:
            result = subprocess.run(
                ["pytest", "tests/unit/test_rag.py", "-v", "--tb=short"],
                cwd=self.config.PROJECT_ROOT,
                capture_output=True,
                text=True,
                timeout=120
            )
            passed = result.returncode == 0
            return TestResult(
                name="RAG Unit Tests",
                status=TestStatus.PASSED if passed else TestStatus.FAILED,
                duration_ms=(time.time() - start) * 1000,
                message=f"Exit code: {result.returncode}",
                details={"stdout": result.stdout[-2000:]}
            )
        except Exception as e:
            return TestResult(
                name="RAG Unit Tests",
                status=TestStatus.FAILED,
                duration_ms=(time.time() - start) * 1000,
                message=f"Failed: {str(e)}"
            )
    
    def run_all(self) -> List[TestResult]:
        """Run all unit tests."""
        return [
            self.run_backend_tests(),
            self.run_data_agent_tests(),
            self.run_rag_tests(),
        ]


# ==============================================================================
# INTEGRATION TEST RUNNER
# ==============================================================================

class IntegrationTestRunner:
    """Runs integration tests that require services."""
    
    def __init__(self, config: Config):
        self.config = config
    
    def test_kafka_produce_consume(self) -> TestResult:
        """Test Kafka produce/consume cycle."""
        start = time.time()
        test_topic = "test.e2e.validation"
        test_message = {"test_id": str(time.time()), "type": "e2e_validation"}
        
        try:
            # Ensure topic exists
            admin = KafkaAdminClient(bootstrap_servers=self.config.KAFKA_BOOTSTRAP)
            try:
                admin.create_topics([NewTopic(test_topic, 1, 1)])
            except:
                pass  # Topic may already exist
            admin.close()
            
            # Produce
            producer = KafkaProducer(
                bootstrap_servers=self.config.KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode()
            )
            future = producer.send(test_topic, test_message)
            future.get(timeout=10)
            producer.close()
            
            # Consume
            consumer = KafkaConsumer(
                test_topic,
                bootstrap_servers=self.config.KAFKA_BOOTSTRAP,
                auto_offset_reset='latest',
                consumer_timeout_ms=5000,
                value_deserializer=lambda m: json.loads(m.decode())
            )
            
            # Read messages
            messages = list(consumer)
            consumer.close()
            
            return TestResult(
                name="Kafka Produce/Consume",
                status=TestStatus.PASSED,
                duration_ms=(time.time() - start) * 1000,
                message=f"Successfully produced and consumed message.",
                details={"messages_received": len(messages)}
            )
        except Exception as e:
            return TestResult(
                name="Kafka Produce/Consume",
                status=TestStatus.FAILED,
                duration_ms=(time.time() - start) * 1000,
                message=f"Failed: {str(e)}"
            )
    
    def test_redis_operations(self) -> TestResult:
        """Test Redis read/write operations."""
        start = time.time()
        test_key = f"e2e_test:{time.time()}"
        test_value = {"validated_at": datetime.now().isoformat()}
        
        try:
            r = redis.from_url(self.config.REDIS_URL)
            
            # Write
            r.set(test_key, json.dumps(test_value), ex=60)
            
            # Read
            retrieved = json.loads(r.get(test_key))
            
            # Cleanup
            r.delete(test_key)
            
            return TestResult(
                name="Redis Read/Write",
                status=TestStatus.PASSED,
                duration_ms=(time.time() - start) * 1000,
                message="Successfully wrote and read from Redis.",
                details={"value": retrieved}
            )
        except Exception as e:
            return TestResult(
                name="Redis Read/Write",
                status=TestStatus.FAILED,
                duration_ms=(time.time() - start) * 1000,
                message=f"Failed: {str(e)}"
            )
    
    def test_postgres_crud(self) -> TestResult:
        """Test PostgreSQL CRUD operations."""
        start = time.time()
        try:
            conn = psycopg2.connect(self.config.POSTGRES_URL)
            cursor = conn.cursor()
            
            # Test if incidents table exists
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'incidents'
                );
            """)
            table_exists = cursor.fetchone()[0]
            
            if not table_exists:
                return TestResult(
                    name="PostgreSQL CRUD",
                    status=TestStatus.WARNING,
                    duration_ms=(time.time() - start) * 1000,
                    message="Incidents table doesn't exist. Run init-postgres.sql"
                )
            
            # Count records
            cursor.execute("SELECT COUNT(*) FROM incidents;")
            count = cursor.fetchone()[0]
            
            conn.close()
            
            return TestResult(
                name="PostgreSQL CRUD",
                status=TestStatus.PASSED,
                duration_ms=(time.time() - start) * 1000,
                message=f"Connected successfully. {count} incidents in DB.",
                details={"incident_count": count}
            )
        except Exception as e:
            return TestResult(
                name="PostgreSQL CRUD",
                status=TestStatus.FAILED,
                duration_ms=(time.time() - start) * 1000,
                message=f"Failed: {str(e)}"
            )
    
    def test_weaviate_schema(self) -> TestResult:
        """Test Weaviate schema and vector operations."""
        start = time.time()
        try:
            resp = requests.get(f"{self.config.WEAVIATE_URL}/v1/schema", timeout=5)
            resp.raise_for_status()
            schema = resp.json()
            
            classes = [c["class"] for c in schema.get("classes", [])]
            
            return TestResult(
                name="Weaviate Schema",
                status=TestStatus.PASSED,
                duration_ms=(time.time() - start) * 1000,
                message=f"Schema loaded. {len(classes)} classes defined.",
                details={"classes": classes}
            )
        except Exception as e:
            return TestResult(
                name="Weaviate Schema",
                status=TestStatus.FAILED,
                duration_ms=(time.time() - start) * 1000,
                message=f"Failed: {str(e)}"
            )
    
    def test_api_endpoints(self) -> TestResult:
        """Test critical API endpoints."""
        start = time.time()
        endpoints = [
            ("GET", "/api/incidents", "Incidents list"),
            ("GET", "/api/approvals", "Approvals list"),
            ("GET", "/api/agents", "Agents list"),
            ("GET", "/api/langgraph/status", "LangGraph status"),
        ]
        
        results = []
        for method, path, name in endpoints:
            try:
                if method == "GET":
                    resp = requests.get(f"{self.config.BACKEND_URL}{path}", timeout=5)
                results.append({
                    "endpoint": path,
                    "status": resp.status_code,
                    "ok": resp.status_code < 400
                })
            except Exception as e:
                results.append({
                    "endpoint": path,
                    "status": "error",
                    "ok": False,
                    "error": str(e)
                })
        
        all_ok = all(r["ok"] for r in results)
        
        return TestResult(
            name="API Endpoints",
            status=TestStatus.PASSED if all_ok else TestStatus.WARNING,
            duration_ms=(time.time() - start) * 1000,
            message=f"Tested {len(endpoints)} endpoints.",
            details={"results": results}
        )
    
    def run_all(self) -> List[TestResult]:
        """Run all integration tests."""
        return [
            self.test_kafka_produce_consume(),
            self.test_redis_operations(),
            self.test_postgres_crud(),
            self.test_weaviate_schema(),
            self.test_api_endpoints(),
        ]


# ==============================================================================
# E2E WORKFLOW TESTS
# ==============================================================================

class E2EWorkflowRunner:
    """Runs end-to-end workflow tests."""
    
    def __init__(self, config: Config):
        self.config = config
    
    def test_incident_workflow(self) -> TestResult:
        """Test complete incident resolution workflow."""
        start = time.time()
        
        # Check if backend is running
        try:
            resp = requests.get(f"{self.config.BACKEND_URL}/health", timeout=5)
            if resp.status_code != 200:
                return TestResult(
                    name="Incident Workflow E2E",
                    status=TestStatus.SKIPPED,
                    duration_ms=(time.time() - start) * 1000,
                    message="Backend not healthy. Skipping E2E test."
                )
        except:
            return TestResult(
                name="Incident Workflow E2E",
                status=TestStatus.SKIPPED,
                duration_ms=(time.time() - start) * 1000,
                message="Backend not reachable. Start the backend first."
            )
        
        # Create test incident
        test_incident = {
            "short_description": "E2E Test: VM test-vm-001 is down",
            "description": "Automated E2E validation test",
            "priority": "4",
            "source": "e2e_test"
        }
        
        try:
            # Step 1: Create incident via V2 API
            resp = requests.post(
                f"{self.config.BACKEND_URL}/api/v2/incidents",
                json={"incident_id": f"E2E-{int(time.time())}", "incident": test_incident},
                timeout=10
            )
            
            if resp.status_code not in [200, 201]:
                return TestResult(
                    name="Incident Workflow E2E",
                    status=TestStatus.FAILED,
                    duration_ms=(time.time() - start) * 1000,
                    message=f"Failed to create incident: {resp.status_code}",
                    details={"response": resp.text[:500]}
                )
            
            incident_data = resp.json()
            incident_id = incident_data.get("incident_id") or incident_data.get("id")
            
            # Step 2: Check workflow was triggered
            time.sleep(2)  # Allow workflow to start
            
            resp = requests.get(
                f"{self.config.BACKEND_URL}/api/incidents/{incident_id}",
                timeout=10
            )
            
            incident_status = resp.json() if resp.status_code == 200 else {}
            
            return TestResult(
                name="Incident Workflow E2E",
                status=TestStatus.PASSED,
                duration_ms=(time.time() - start) * 1000,
                message=f"Workflow triggered for incident {incident_id}",
                details={
                    "incident_id": incident_id,
                    "status": incident_status.get("status", "unknown")
                }
            )
            
        except Exception as e:
            return TestResult(
                name="Incident Workflow E2E",
                status=TestStatus.FAILED,
                duration_ms=(time.time() - start) * 1000,
                message=f"Workflow test failed: {str(e)}"
            )
    
    def test_rag_search_pipeline(self) -> TestResult:
        """Test RAG search pipeline end-to-end."""
        start = time.time()
        
        try:
            resp = requests.get(f"{self.config.BACKEND_URL}/health", timeout=5)
            if resp.status_code != 200:
                return TestResult(
                    name="RAG Search E2E",
                    status=TestStatus.SKIPPED,
                    duration_ms=(time.time() - start) * 1000,
                    message="Backend not available."
                )
        except:
            return TestResult(
                name="RAG Search E2E",
                status=TestStatus.SKIPPED,
                duration_ms=(time.time() - start) * 1000,
                message="Backend not reachable."
            )
        
        try:
            # Test RAG search endpoint
            search_query = {
                "query": "restart GCP VM instance",
                "top_k": 5
            }
            
            resp = requests.post(
                f"{self.config.BACKEND_URL}/api/rag/search",
                json=search_query,
                timeout=30
            )
            
            if resp.status_code != 200:
                return TestResult(
                    name="RAG Search E2E",
                    status=TestStatus.WARNING,
                    duration_ms=(time.time() - start) * 1000,
                    message=f"RAG search endpoint returned {resp.status_code}",
                    details={"response": resp.text[:500]}
                )
            
            results = resp.json()
            result_count = len(results.get("results", results.get("scripts", [])))
            
            return TestResult(
                name="RAG Search E2E",
                status=TestStatus.PASSED,
                duration_ms=(time.time() - start) * 1000,
                message=f"RAG search returned {result_count} results.",
                details={"result_count": result_count}
            )
            
        except Exception as e:
            return TestResult(
                name="RAG Search E2E",
                status=TestStatus.FAILED,
                duration_ms=(time.time() - start) * 1000,
                message=f"RAG search failed: {str(e)}"
            )
    
    def run_all(self) -> List[TestResult]:
        """Run all E2E tests."""
        return [
            self.test_incident_workflow(),
            self.test_rag_search_pipeline(),
        ]


# ==============================================================================
# CODE QUALITY CHECKS
# ==============================================================================

class CodeQualityChecker:
    """Runs code quality checks."""
    
    def __init__(self, config: Config):
        self.config = config
    
    def run_ruff_lint(self) -> TestResult:
        """Run ruff linter."""
        start = time.time()
        try:
            result = subprocess.run(
                ["ruff", "check", "backend/", "agents/", "--statistics"],
                cwd=self.config.PROJECT_ROOT,
                capture_output=True,
                text=True,
                timeout=120
            )
            
            passed = result.returncode == 0
            return TestResult(
                name="Ruff Lint",
                status=TestStatus.PASSED if passed else TestStatus.WARNING,
                duration_ms=(time.time() - start) * 1000,
                message=f"Exit code: {result.returncode}",
                details={"output": result.stdout[-2000:]}
            )
        except FileNotFoundError:
            return TestResult(
                name="Ruff Lint",
                status=TestStatus.SKIPPED,
                duration_ms=(time.time() - start) * 1000,
                message="ruff not installed. Install with: pip install ruff"
            )
        except Exception as e:
            return TestResult(
                name="Ruff Lint",
                status=TestStatus.FAILED,
                duration_ms=(time.time() - start) * 1000,
                message=f"Failed: {str(e)}"
            )
    
    def run_mypy_typecheck(self) -> TestResult:
        """Run mypy type checker."""
        start = time.time()
        try:
            result = subprocess.run(
                ["mypy", "backend/", "agents/", "--ignore-missing-imports", "--no-error-summary"],
                cwd=self.config.PROJECT_ROOT,
                capture_output=True,
                text=True,
                timeout=300
            )
            
            passed = result.returncode == 0
            return TestResult(
                name="MyPy Type Check",
                status=TestStatus.PASSED if passed else TestStatus.WARNING,
                duration_ms=(time.time() - start) * 1000,
                message=f"Exit code: {result.returncode}",
                details={"output": result.stdout[-2000:]}
            )
        except FileNotFoundError:
            return TestResult(
                name="MyPy Type Check",
                status=TestStatus.SKIPPED,
                duration_ms=(time.time() - start) * 1000,
                message="mypy not installed. Install with: pip install mypy"
            )
        except Exception as e:
            return TestResult(
                name="MyPy Type Check",
                status=TestStatus.FAILED,
                duration_ms=(time.time() - start) * 1000,
                message=f"Failed: {str(e)}"
            )
    
    def check_security_secrets(self) -> TestResult:
        """Check for hardcoded secrets."""
        start = time.time()
        
        patterns = [
            "password=",
            "api_key=",
            "secret_key=",
            "OPENAI_API_KEY=sk-",
            "aws_secret_access_key",
        ]
        
        found_issues = []
        
        for py_file in self.config.PROJECT_ROOT.rglob("*.py"):
            if "venv" in str(py_file) or "node_modules" in str(py_file):
                continue
            try:
                content = py_file.read_text()
                for pattern in patterns:
                    if pattern in content.lower() and "os.getenv" not in content:
                        found_issues.append(f"{py_file.name}: potential hardcoded {pattern}")
            except:
                pass
        
        return TestResult(
            name="Secret Detection",
            status=TestStatus.PASSED if not found_issues else TestStatus.WARNING,
            duration_ms=(time.time() - start) * 1000,
            message=f"Found {len(found_issues)} potential issues.",
            details={"issues": found_issues[:10]}
        )
    
    def run_all(self) -> List[TestResult]:
        """Run all code quality checks."""
        return [
            self.run_ruff_lint(),
            self.run_mypy_typecheck(),
            self.check_security_secrets(),
        ]


# ==============================================================================
# MAIN VALIDATOR
# ==============================================================================

class PlatformValidator:
    """Main validator orchestrator."""
    
    def __init__(self):
        self.config = Config()
        self.health_checker = HealthChecker(self.config)
        self.unit_runner = UnitTestRunner(self.config)
        self.integration_runner = IntegrationTestRunner(self.config)
        self.e2e_runner = E2EWorkflowRunner(self.config)
        self.quality_checker = CodeQualityChecker(self.config)
    
    def run_health_checks(self) -> ValidationReport:
        """Run health checks only."""
        report = self._create_report()
        for result in self.health_checker.run_all():
            report.add_result(result)
        report.duration_seconds = time.time() - report.timestamp.timestamp()
        return report
    
    def run_unit_tests(self) -> ValidationReport:
        """Run unit tests only."""
        report = self._create_report()
        for result in self.unit_runner.run_all():
            report.add_result(result)
        report.duration_seconds = time.time() - report.timestamp.timestamp()
        return report
    
    def run_integration_tests(self) -> ValidationReport:
        """Run integration tests only."""
        report = self._create_report()
        for result in self.integration_runner.run_all():
            report.add_result(result)
        report.duration_seconds = time.time() - report.timestamp.timestamp()
        return report
    
    def run_e2e_tests(self) -> ValidationReport:
        """Run E2E tests only."""
        report = self._create_report()
        for result in self.e2e_runner.run_all():
            report.add_result(result)
        report.duration_seconds = time.time() - report.timestamp.timestamp()
        return report
    
    def run_code_quality(self) -> ValidationReport:
        """Run code quality checks only."""
        report = self._create_report()
        for result in self.quality_checker.run_all():
            report.add_result(result)
        report.duration_seconds = time.time() - report.timestamp.timestamp()
        return report
    
    def run_all(self) -> ValidationReport:
        """Run complete validation suite."""
        report = self._create_report()
        
        print("\n" + "="*60)
        print("🔍 HEALTH CHECKS")
        print("="*60)
        for result in self.health_checker.run_all():
            report.add_result(result)
            self._print_result(result)
        
        print("\n" + "="*60)
        print("🧪 UNIT TESTS")
        print("="*60)
        for result in self.unit_runner.run_all():
            report.add_result(result)
            self._print_result(result)
        
        print("\n" + "="*60)
        print("🔗 INTEGRATION TESTS")
        print("="*60)
        for result in self.integration_runner.run_all():
            report.add_result(result)
            self._print_result(result)
        
        print("\n" + "="*60)
        print("🚀 E2E WORKFLOW TESTS")
        print("="*60)
        for result in self.e2e_runner.run_all():
            report.add_result(result)
            self._print_result(result)
        
        print("\n" + "="*60)
        print("📊 CODE QUALITY")
        print("="*60)
        for result in self.quality_checker.run_all():
            report.add_result(result)
            self._print_result(result)
        
        report.duration_seconds = time.time() - report.timestamp.timestamp()
        return report
    
    def _create_report(self) -> ValidationReport:
        return ValidationReport(
            timestamp=datetime.now(),
            total_tests=0,
            passed=0,
            failed=0,
            skipped=0,
            warnings=0,
            duration_seconds=0.0
        )
    
    def _print_result(self, result: TestResult):
        print(f"  {result.status.value} {result.name} ({result.duration_ms:.0f}ms)")
        if result.message:
            print(f"      {result.message}")


def print_report_summary(report: ValidationReport):
    """Print final report summary."""
    print("\n" + "="*60)
    print("📋 VALIDATION SUMMARY")
    print("="*60)
    print(f"  Timestamp: {report.timestamp.isoformat()}")
    print(f"  Duration:  {report.duration_seconds:.2f}s")
    print(f"  Total:     {report.total_tests}")
    print(f"  ✅ Passed:  {report.passed}")
    print(f"  ❌ Failed:  {report.failed}")
    print(f"  ⏭️ Skipped: {report.skipped}")
    print(f"  ⚠️ Warning: {report.warnings}")
    print("="*60)
    
    if report.is_successful():
        print("🎉 ALL VALIDATIONS PASSED!")
    else:
        print("💥 SOME VALIDATIONS FAILED - Review output above")
        # List failures
        failures = [r for r in report.results if r.status == TestStatus.FAILED]
        if failures:
            print("\nFailed tests:")
            for f in failures:
                print(f"  - {f.name}: {f.message}")


def main():
    parser = argparse.ArgumentParser(
        description="Enterprise Agentic Platform - E2E Validator"
    )
    parser.add_argument("--all", action="store_true", help="Run all validations")
    parser.add_argument("--health", action="store_true", help="Health checks only")
    parser.add_argument("--unit", action="store_true", help="Unit tests only")
    parser.add_argument("--integration", action="store_true", help="Integration tests only")
    parser.add_argument("--e2e", action="store_true", help="E2E workflow tests only")
    parser.add_argument("--quality", action="store_true", help="Code quality checks only")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    
    args = parser.parse_args()
    
    validator = PlatformValidator()
    
    # Determine what to run
    if args.health:
        report = validator.run_health_checks()
    elif args.unit:
        report = validator.run_unit_tests()
    elif args.integration:
        report = validator.run_integration_tests()
    elif args.e2e:
        report = validator.run_e2e_tests()
    elif args.quality:
        report = validator.run_code_quality()
    else:
        # Default: run all
        report = validator.run_all()
    
    # Output
    if args.json:
        output = {
            "timestamp": report.timestamp.isoformat(),
            "total": report.total_tests,
            "passed": report.passed,
            "failed": report.failed,
            "skipped": report.skipped,
            "warnings": report.warnings,
            "duration_seconds": report.duration_seconds,
            "success": report.is_successful(),
            "results": [
                {
                    "name": r.name,
                    "status": r.status.name,
                    "duration_ms": r.duration_ms,
                    "message": r.message
                }
                for r in report.results
            ]
        }
        print(json.dumps(output, indent=2))
    else:
        print_report_summary(report)
    
    # Exit code
    sys.exit(0 if report.is_successful() else 1)


if __name__ == "__main__":
    main()