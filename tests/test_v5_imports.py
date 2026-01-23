"""
Test v5.0 Module Imports

This test verifies that all new v5.0 modules can be imported correctly.
Run with: python3 tests/test_v5_imports.py
"""
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def test_a2a_imports():
    """Test A2A Protocol imports"""
    print("Testing A2A Protocol imports...")
    try:
        from backend.agents.a2a import (
            MessageType,
            A2AMessage,
            SwarmQueryMessage,
            SwarmVoteMessage,
            JudgeEvaluateMessage,
            JudgeScoreMessage,
            AgentExecuteMessage,
            AgentResultMessage,
            A2AClient,
        )
        print("  ✓ A2A Protocol imports successful")
        return True
    except Exception as e:
        print(f"  ✗ A2A Protocol imports failed: {e}")
        return False


def test_mcp_imports():
    """Test MCP Protocol imports"""
    print("Testing MCP Protocol imports...")
    try:
        from backend.mcp import MCPClient
        from backend.mcp.servers.rag_server import RAGMCPServer
        print("  ✓ MCP Protocol imports successful")
        return True
    except Exception as e:
        print(f"  ✗ MCP Protocol imports failed: {e}")
        return False


def test_streaming_imports():
    """Test Streaming Schemas imports"""
    print("Testing Streaming Schemas imports...")
    try:
        from backend.streaming.schemas import (
            IncidentCreatedEvent,
            PlanGeneratedEvent,
            IncidentExecutedEvent,
            IncidentApprovedEvent,
        )
        print("  ✓ Streaming Schemas imports successful")
        return True
    except Exception as e:
        print(f"  ✗ Streaming Schemas imports failed: {e}")
        return False


def test_control_plane_imports():
    """Test Control Plane imports"""
    print("Testing Control Plane imports...")
    try:
        from backend.agents.control_plane import ControlPlane, Policy, RiskLevel
        print("  ✓ Control Plane imports successful")
        return True
    except Exception as e:
        print(f"  ✗ Control Plane imports failed: {e}")
        return False


def test_execution_orchestrator_imports():
    """Test Execution Orchestrator imports"""
    print("Testing Execution Orchestrator imports...")
    try:
        from backend.agents.execution_orchestrator import (
            ExecutionOrchestrator,
            ExecutionPlan,
            ExecutionStep,
            ExecutionStatus,
        )
        print("  ✓ Execution Orchestrator imports successful")
        return True
    except Exception as e:
        print(f"  ✗ Execution Orchestrator imports failed: {e}")
        return False


def test_github_actions_imports():
    """Test GitHub Actions Client imports"""
    print("Testing GitHub Actions Client imports...")
    try:
        from backend.utils.github_actions import (
            GitHubActionsClient,
            WorkflowRun,
            WorkflowResult,
        )
        print("  ✓ GitHub Actions Client imports successful")
        return True
    except Exception as e:
        print(f"  ✗ GitHub Actions Client imports failed: {e}")
        return False


def test_llm_judge_imports():
    """Test LLM Judge imports"""
    print("Testing LLM Judge imports...")
    try:
        from backend.orchestrator.llm_judge import LLMJudge, JudgeConfig
        print("  ✓ LLM Judge imports successful")
        return True
    except Exception as e:
        print(f"  ✗ LLM Judge imports failed: {e}")
        return False


def test_swarm_retriever_imports():
    """Test Swarm Retriever imports"""
    print("Testing Swarm Retriever imports...")
    try:
        from backend.rag.swarm_retriever import SwarmRetriever, SwarmResult
        print("  ✓ Swarm Retriever imports successful")
        return True
    except Exception as e:
        print(f"  ✗ Swarm Retriever imports failed: {e}")
        return False


def test_rag_agents_imports():
    """Test RAG Agents imports"""
    print("Testing RAG Agents imports...")
    try:
        from backend.rag.agents import (
            VectorAgent,
            KeywordAgent,
            GraphAgent,
            MetadataAgent,
        )
        print("  ✓ RAG Agents imports successful")
        return True
    except Exception as e:
        print(f"  ✗ RAG Agents imports failed: {e}")
        return False


def test_langgraph_workflow_imports():
    """Test LangGraph Workflow imports"""
    print("Testing LangGraph Workflow imports...")
    try:
        from backend.orchestrator.langgraph_workflow import (
            WorkflowOrchestrator,
            WorkflowState,
            IncidentStatus,
        )
        print("  ✓ LangGraph Workflow imports successful")
        return True
    except Exception as e:
        print(f"  ✗ LangGraph Workflow imports failed: {e}")
        return False


def main():
    """Run all import tests"""
    print("=" * 60)
    print("AI Agent Platform v5.0 - Import Tests")
    print("=" * 60)
    print()

    tests = [
        test_a2a_imports,
        test_mcp_imports,
        test_streaming_imports,
        test_control_plane_imports,
        test_execution_orchestrator_imports,
        test_github_actions_imports,
        test_llm_judge_imports,
        test_swarm_retriever_imports,
        test_rag_agents_imports,
        test_langgraph_workflow_imports,
    ]

    results = []
    for test in tests:
        print()
        try:
            result = test()
            results.append(result)
        except Exception as e:
            print(f"  ✗ Test failed with exception: {e}")
            results.append(False)

    print()
    print("=" * 60)
    passed = sum(results)
    total = len(results)
    print(f"Results: {passed}/{total} tests passed")
    print("=" * 60)

    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
