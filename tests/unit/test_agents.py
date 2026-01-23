"""Unit tests for individual agents (V5 Architecture)"""
import pytest
from unittest.mock import Mock, patch, MagicMock, AsyncMock
import sys
import os
import asyncio

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))


class TestIncidentAgent:
    """Test ServiceNow Incident Agent functionality"""

    @pytest.fixture
    def mock_env(self):
        """Set up environment for tests"""
        with patch.dict(os.environ, {
            'OPENAI_API_KEY': 'test_key'
        }):
            yield

    def test_agent_initialization(self, mock_env):
        """Test IncidentAgent initializes correctly"""
        from agents.servicenow_agent import IncidentAgent

        agent = IncidentAgent()

        assert agent.agent_id == "servicenow-incident-agent"
        assert agent.agent_name == "ServiceNow Incident Agent"

    def test_agent_capabilities(self, mock_env):
        """Test IncidentAgent has correct capabilities"""
        from agents.servicenow_agent import IncidentAgent
        from agents.shared import AgentCapability

        agent = IncidentAgent()
        capabilities = agent.capabilities

        assert AgentCapability.INCIDENT_TRIAGE in capabilities

    @pytest.mark.asyncio
    async def test_execute_with_valid_task(self, mock_env):
        """Test executing incident analysis with valid task"""
        from agents.servicenow_agent import IncidentAgent
        from agents.shared import IAgentTask

        agent = IncidentAgent()

        # Mock the LLM client
        mock_response = MagicMock()
        mock_response.choices = [
            MagicMock(message=MagicMock(content='{"severity": "P2", "category": "performance", "root_cause": "High CPU usage", "confidence_score": 85}'))
        ]
        agent.llm_client = MagicMock()
        agent.llm_client.chat.completions.create = MagicMock(return_value=mock_response)

        task = IAgentTask(
            task_id="task-001",
            task_type="incident_triage",
            payload={
                "incident_id": "INC0001234",
                "short_description": "Server is slow",
                "description": "Production server responding slowly",
                "service": "payment-service"
            }
        )

        result = await agent.execute(task)

        assert result.success is True
        assert result.task_id == "task-001"

    @pytest.mark.asyncio
    async def test_execute_without_llm_client(self, mock_env):
        """Test executing when LLM client is not configured"""
        from agents.servicenow_agent import IncidentAgent
        from agents.shared import IAgentTask

        agent = IncidentAgent()
        agent.llm_client = None  # No LLM configured

        task = IAgentTask(
            task_id="task-002",
            task_type="incident_triage",
            payload={
                "incident_id": "INC0001234",
                "short_description": "Test incident"
            }
        )

        result = await agent.execute(task)

        # Should still succeed but with fallback analysis
        assert result.task_id == "task-002"


class TestScriptMatcherAgent:
    """Test Script Matcher Agent functionality"""

    @pytest.fixture
    def mock_env(self):
        """Set up environment for tests"""
        with patch.dict(os.environ, {
            'OPENAI_API_KEY': 'test_key'
        }):
            yield

    def test_agent_initialization(self, mock_env):
        """Test ScriptMatcherAgent initializes correctly"""
        from agents.servicenow_agent import ScriptMatcherAgent

        agent = ScriptMatcherAgent()

        assert agent.agent_id == "servicenow-script-matcher"
        assert agent.agent_name == "Script Matcher Agent"

    def test_agent_capabilities(self, mock_env):
        """Test ScriptMatcherAgent has correct capabilities"""
        from agents.servicenow_agent import ScriptMatcherAgent
        from agents.shared import AgentCapability

        agent = ScriptMatcherAgent()
        capabilities = agent.capabilities

        assert AgentCapability.SCRIPT_MATCHING in capabilities


class TestRemediationAgent:
    """Test Remediation Agent functionality"""

    @pytest.fixture
    def mock_env(self):
        """Set up environment for tests"""
        with patch.dict(os.environ, {
            'OPENAI_API_KEY': 'test_key'
        }):
            yield

    def test_agent_initialization(self, mock_env):
        """Test RemediationAgent initializes correctly"""
        from agents.servicenow_agent import RemediationAgent

        agent = RemediationAgent()

        assert agent.agent_id == "servicenow-remediation-agent"
        assert agent.agent_name == "Remediation Agent"

    def test_agent_capabilities(self, mock_env):
        """Test RemediationAgent has correct capabilities"""
        from agents.servicenow_agent import RemediationAgent
        from agents.shared import AgentCapability

        agent = RemediationAgent()
        capabilities = agent.capabilities

        assert AgentCapability.REMEDIATION in capabilities


class TestServiceNowAgentService:
    """Test ServiceNow Agent Service (orchestration layer)"""

    @pytest.fixture
    def mock_env(self):
        """Set up environment for tests"""
        with patch.dict(os.environ, {
            'OPENAI_API_KEY': 'test_key'
        }):
            yield

    def test_service_initialization(self, mock_env):
        """Test ServiceNowAgentService initializes correctly"""
        from agents.servicenow_agent import ServiceNowAgentService

        service = ServiceNowAgentService()

        assert service is not None
        # Service should have task routing configured
        assert hasattr(service, '_task_routing')


class TestBaseAgent:
    """Test BaseAgent common functionality"""

    @pytest.fixture
    def mock_env(self):
        """Set up environment for tests"""
        with patch.dict(os.environ, {
            'OPENAI_API_KEY': 'test_key'
        }):
            yield

    def test_agent_config_default(self, mock_env):
        """Test agent uses default config when none provided"""
        from agents.servicenow_agent import IncidentAgent

        agent = IncidentAgent()

        assert agent.config is not None
        assert agent.config.default_timeout_seconds > 0
        assert agent.config.retry is not None

    def test_agent_config_custom(self, mock_env):
        """Test agent accepts custom config"""
        from agents.servicenow_agent import IncidentAgent
        from agents.shared import AgentConfig, LLMConfig

        custom_config = AgentConfig(
            default_timeout_seconds=120,
            llm=LLMConfig(
                provider="openai",
                model="gpt-4o",
                temperature=0.5
            )
        )

        agent = IncidentAgent(config=custom_config)

        assert agent.config.default_timeout_seconds == 120
        assert agent.config.llm.model == "gpt-4o"


@pytest.mark.skip(reason="Data Agent requires running from agents/data_agent directory")
class TestDataAgent:
    """Test Data Pipeline Agent (LangGraph-based)

    NOTE: These tests require PYTHONPATH to include agents/data_agent
    Run with: cd agents/data_agent && pytest tests/unit -v
    """

    @pytest.fixture
    def mock_env(self):
        """Set up environment for tests"""
        with patch.dict(os.environ, {
            'OPENAI_API_KEY': 'test_key',
            'POSTGRES_HOST': 'localhost',
            'POSTGRES_PORT': '5432',
            'POSTGRES_USER': 'admin',
            'POSTGRES_PASSWORD': 'admin123',
            'POSTGRES_DB': 'agentdb',
        }):
            yield

    def test_data_agent_imports(self, mock_env):
        """Test Data Agent modules can be imported"""
        pass  # Run from agents/data_agent directory

    def test_planner_agent_initialization(self, mock_env):
        """Test PlannerAgent initializes correctly"""
        pass  # Run from agents/data_agent directory

    def test_generator_agent_initialization(self, mock_env):
        """Test GeneratorAgent initializes correctly"""
        pass  # Run from agents/data_agent directory

    def test_validator_agent_initialization(self, mock_env):
        """Test ValidatorAgent initializes correctly"""
        pass  # Run from agents/data_agent directory

    def test_deployer_agent_initialization(self, mock_env):
        """Test DeployerAgent initializes correctly"""
        pass  # Run from agents/data_agent directory


class TestAgentInterfaces:
    """Test agent interface contracts"""

    def test_iagent_task_creation(self):
        """Test IAgentTask can be created"""
        from agents.shared import IAgentTask

        task = IAgentTask(
            task_id="test-123",
            task_type="incident_triage",
            payload={"key": "value"}
        )

        assert task.task_id == "test-123"
        assert task.task_type == "incident_triage"
        assert task.payload["key"] == "value"

    def test_iagent_result_creation(self):
        """Test IAgentResult can be created"""
        from agents.shared import IAgentResult

        result = IAgentResult(
            task_id="test-123",
            success=True,
            output={"result": "success"}
        )

        assert result.task_id == "test-123"
        assert result.success is True
        assert result.output["result"] == "success"

    def test_agent_capability_enum(self):
        """Test AgentCapability enum values"""
        from agents.shared import AgentCapability

        assert AgentCapability.INCIDENT_TRIAGE is not None
        assert AgentCapability.SCRIPT_MATCHING is not None
        assert AgentCapability.REMEDIATION is not None
