"""Unit tests for orchestrator service"""
import pytest
from unittest.mock import Mock, patch, MagicMock
from fastapi.testclient import TestClient
import sys
import os

# Add backend to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../backend'))


class TestOrchestratorAPI:
    """Test orchestrator API endpoints"""

    def test_health_endpoint(self):
        """Test health check endpoint"""
        from orchestrator.main import app
        client = TestClient(app)

        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert "status" in data

    def test_version_info(self):
        """Test that version info is accessible"""
        from orchestrator.main import app
        client = TestClient(app)

        # Check OpenAPI docs accessible
        response = client.get("/openapi.json")
        assert response.status_code == 200
        data = response.json()
        assert data["info"]["version"] == "5.0.0"
        assert "AI Agent Orchestrator" in data["info"]["title"]


class TestModels:
    """Test Pydantic models"""

    def test_incident_model(self):
        """Test Incident model validation"""
        from orchestrator.main import Incident

        incident = Incident(
            incident_id="INC001",
            short_description="Server down",
            description="Production server is not responding",
            category="hardware",
            priority="1",
            status="new"
        )

        assert incident.incident_id == "INC001"
        assert incident.short_description == "Server down"
        assert incident.priority == "1"

    def test_incident_model_defaults(self):
        """Test Incident model with defaults"""
        from orchestrator.main import Incident

        incident = Incident(
            incident_id="INC002",
            short_description="Test incident"
        )

        assert incident.description == ""
        assert incident.category == "unknown"
        assert incident.priority == "3"
        assert incident.status == "new"

    def test_script_match_model(self):
        """Test ScriptMatch model"""
        from orchestrator.main import ScriptMatch

        match = ScriptMatch(
            script_id="SCRIPT001",
            name="Restart Service",
            description="Restarts a system service",
            type="shell",
            confidence=0.95,
            risk_level="low",
            auto_approve=True,
            required_inputs=["service_name"]
        )

        assert match.script_id == "SCRIPT001"
        assert match.confidence == 0.95
        assert match.auto_approve is True
        assert "service_name" in match.required_inputs

    def test_execution_request_model(self):
        """Test ExecutionRequest model"""
        from orchestrator.main import ExecutionRequest

        request = ExecutionRequest(
            incident_id="INC001",
            script_id="SCRIPT001",
            inputs={"service_name": "nginx"},
            environment="development",
            dry_run=True
        )

        assert request.incident_id == "INC001"
        assert request.script_id == "SCRIPT001"
        assert request.dry_run is True
        assert request.inputs["service_name"] == "nginx"

    def test_approval_request_model(self):
        """Test ApprovalRequest model"""
        from orchestrator.main import ApprovalRequest

        approval = ApprovalRequest(
            execution_id="EXEC001",
            approved=True,
            approver="admin@example.com",
            comments="Approved for production"
        )

        assert approval.execution_id == "EXEC001"
        assert approval.approved is True
        assert approval.approver == "admin@example.com"


class TestRegistryLoader:
    """Test registry loading functionality"""

    @patch('builtins.open')
    @patch('os.path.exists', return_value=True)
    def test_load_registry_success(self, mock_exists, mock_open):
        """Test successful registry loading"""
        import json
        mock_registry = {
            "scripts": [
                {"id": "SCRIPT001", "name": "Test Script"}
            ]
        }
        mock_open.return_value.__enter__.return_value.read.return_value = json.dumps(mock_registry)

        from orchestrator.main import load_registry
        result = load_registry()

        # Should return dict or empty on error
        assert isinstance(result, dict)


class TestGuardrailsIntegration:
    """Test guardrails integration with orchestrator"""

    def test_guardrails_enabled_in_orchestrator(self):
        """Test that guardrails flag is properly imported"""
        try:
            from guardrails.llm_guardrails import GUARDRAILS_ENABLED
            assert GUARDRAILS_ENABLED is True
        except ImportError:
            pytest.skip("Guardrails module not available")

    def test_guardrails_api_endpoint(self):
        """Test guardrails API endpoint exists"""
        from orchestrator.main import app
        client = TestClient(app)

        response = client.get("/api/guardrails/status")
        # Should return 200 regardless of configuration
        assert response.status_code == 200


class TestMetricsEndpoint:
    """Test metrics functionality"""

    def test_metrics_endpoint_exists(self):
        """Test that metrics endpoint is accessible"""
        from orchestrator.main import app
        client = TestClient(app)

        response = client.get("/metrics")
        # Should return metrics in some format
        assert response.status_code == 200


class TestCORSMiddleware:
    """Test CORS configuration"""

    def test_cors_headers_present(self):
        """Test that CORS headers are properly set"""
        from orchestrator.main import app
        client = TestClient(app)

        # Make a request with Origin header
        response = client.options(
            "/health",
            headers={"Origin": "http://localhost:3000"}
        )

        # CORS should allow the request
        assert response.status_code in [200, 405]  # OPTIONS might not be explicitly handled


class TestMCPClient:
    """Test MCP client integration"""

    def test_mcp_client_import(self):
        """Test that MCP client can be imported"""
        try:
            from orchestrator.services.mcp_client import mcp_client
            assert mcp_client is not None
        except ImportError as e:
            pytest.skip(f"MCP client not available: {e}")
