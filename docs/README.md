# AI Agent Platform for Enterprise Incident Remediation v5.0

> Enterprise-grade, Event-Driven Multi-Agent Platform with Hybrid Protocol Architecture (Kafka + A2A + MCP)

[![Version](https://img.shields.io/badge/version-5.0.0-blue.svg)](https://github.com/sam2881/test_01)
[![Python](https://img.shields.io/badge/python-3.11+-green.svg)](https://python.org)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![SOC2](https://img.shields.io/badge/SOC2-Type_II-green.svg)](docs/compliance)
[![ISO](https://img.shields.io/badge/ISO-42001-purple.svg)](docs/compliance)

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture Patterns](#architecture-patterns)
3. [Design Patterns](#design-patterns)
4. [Protocol Layers](#protocol-layers)
5. [System Architecture](#system-architecture)
6. [Component Details](#component-details)
7. [Workflow (14 Steps + Judge)](#workflow-14-steps--judge)
8. [Swarm RAG System](#swarm-rag-system)
9. [Execution via GitHub Actions](#execution-via-github-actions)
10. [Quick Start](#quick-start)
11. [API Reference](#api-reference)
12. [Project Structure](#project-structure)
13. [Configuration](#configuration)
14. [Compliance & Safety](#compliance--safety)

---

## Overview

The AI Agent Platform v5.0 is an enterprise-grade system that automates incident detection, analysis, and remediation using a **hybrid protocol architecture**:

- **Kafka** - External system integration, audit trail, state transitions
- **A2A (Agent-to-Agent)** - Real-time agent communication, swarm coordination
- **MCP (Model Context Protocol)** - Agent-to-tool invocation (JSON-RPC 2.0)
- **REST/Webhooks** - GitHub Actions triggers, external API calls

### Key Features

| Feature | Description | Protocol |
|---------|-------------|----------|
| **Event-Driven Architecture** | Kafka-based incident ingestion from external systems | Kafka |
| **A2A Agent Mesh** | Real-time agent coordination and swarm intelligence | A2A |
| **MCP Tool Integration** | Standardized tool invocation for RAG, ServiceNow, GitHub | MCP |
| **LangGraph Orchestration** | 14-step workflow with 8 phases + LLM-as-Judge | Internal |
| **Swarm RAG** | Multi-agent consensus for script retrieval | A2A + MCP |
| **GitHub Actions Execution** | Terraform/Ansible via workflow_dispatch | REST |
| **LLM-as-Judge** | Separate evaluation model for quality/safety | A2A |
| **Human-in-the-Loop (HITL)** | Approval workflow for high-risk actions | Kafka |
| **Automatic Rollback** | Generated rollback plans for safe execution | Internal |

### Compliance Certifications

- SOC2 Type II
- ISO 42001 (AI Management)
- NIST AI RMF
- EU AI Act Ready
- MITRE ATLAS (Adversarial ML)

---

## Architecture Patterns

The platform implements industry-standard architecture patterns from Netflix, Uber, Stripe, and Google SRE:

### 1. Hub & Spoke Pattern

```
                    ┌─────────────────────┐
                    │  Event Orchestrator │
                    │       (HUB)         │
                    └──────────┬──────────┘
                               │
           ┌───────────────────┼───────────────────┐
           │                   │                   │
           ▼                   ▼                   ▼
    ┌──────────────┐   ┌──────────────┐   ┌──────────────┐
    │   Decision   │   │   Control    │   │  Execution   │
    │     Orch     │   │    Plane     │   │    Orch      │
    │   (Spoke)    │   │   (Spoke)    │   │   (Spoke)    │
    └──────────────┘   └──────────────┘   └──────────────┘
```

**Implementation**: `backend/orchestrator/event_orchestrator.py`

- Event Orchestrator acts as central hub consuming from Kafka
- Spokes are specialized orchestrators for different concerns
- Communication via A2A protocol for real-time coordination

### 2. Hierarchical Agent Pattern

```
                    ┌─────────────────────┐
                    │    Control Plane    │
                    │   (Policy + Risk)   │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │ Execution Orchestrator│
                    │   (No LLM - Execute) │
                    └──────────┬──────────┘
                               │
           ┌───────────────────┼───────────────────┐
           │                   │                   │
           ▼                   ▼                   ▼
    ┌──────────────┐   ┌──────────────┐   ┌──────────────┐
    │  Terraform   │   │   Ansible    │   │    Code      │
    │    Agent     │   │    Agent     │   │   Agent      │
    └──────────────┘   └──────────────┘   └──────────────┘
```

**Implementation**: 
- `backend/agents/control_plane.py`
- `backend/agents/execution_orchestrator.py`
- `backend/agents/terraform_agent.py`
- `backend/agents/ansible_agent.py`
- `backend/agents/code_agent.py`

### 3. Swarm Intelligence Pattern (RAG)

```
    ┌─────────────────────────────────────────────────────┐
    │              SWARM RAG RETRIEVAL                    │
    │                                                     │
    │  ┌─────────┐  ┌─────────┐  ┌─────────┐            │
    │  │ Agent 1 │  │ Agent 2 │  │ Agent 3 │  ...       │
    │  │ Vector  │  │ Keyword │  │  Graph  │            │
    │  └────┬────┘  └────┬────┘  └────┬────┘            │
    │       │            │            │                  │
    │       └────────────┼────────────┘                  │
    │                    ▼                               │
    │           ┌─────────────────┐                      │
    │           │    CONSENSUS    │                      │
    │           │   (A2A Voting)  │                      │
    │           └────────┬────────┘                      │
    │                    ▼                               │
    │           Best Script (.tf/.yml)                   │
    └─────────────────────────────────────────────────────┘
```

**Implementation**: `backend/rag/swarm_retriever.py`

### 4. Event-Driven Pattern

**Implementation**: `backend/streaming/kafka_consumer.py`, `backend/streaming/kafka_producer.py`

---

## Design Patterns

### 1. ReAct Pattern (Reasoning + Acting)

Used in context enrichment nodes for iterative reasoning.

```python
# backend/orchestrator/nodes/enrich_context.py
class EnrichContextNode:
    """
    ReAct Pattern: Reason about incident, Act to gather more context, Repeat
    """
    async def execute(self, state: IncidentState) -> IncidentState:
        for iteration in range(self.max_iterations):
            # REASON: Analyze what context is missing
            thought = await self.llm.reason(state.incident, state.context)
            
            # ACT: Gather missing context
            if thought.needs_more_context:
                action_result = await self.execute_action(thought.action)
                state.context.append(action_result)
            else:
                break
        
        return state
```

### 2. Chain-of-Thought Pattern

Used in LLM reasoning for step-by-step problem solving.

```python
# backend/orchestrator/llm_intelligence.py
class LLMIntelligence:
    """
    Chain-of-Thought: Break down complex reasoning into steps
    """
    async def analyze_incident(self, incident: Incident) -> Analysis:
        prompt = f"""
        Analyze this incident step by step:
        
        Step 1: Identify the affected service
        Step 2: Determine the root cause category
        Step 3: Assess severity and blast radius
        Step 4: Recommend remediation approach
        
        Incident: {incident.description}
        
        Think through each step:
        """
        return await self.llm.complete(prompt)
```

### 3. Plan-Execute Pattern

Used in Decision Orchestrator for generating and executing remediation plans.

```python
# backend/orchestrator/decision_orchestrator.py
class DecisionOrchestrator:
    """
    Plan-Execute: First generate complete plan, then execute steps
    """
    async def process(self, state: IncidentState) -> IncidentState:
        # PLAN PHASE
        plan = await self.generate_plan(state)
        state.execution_plan = plan
        
        # EXECUTE PHASE (after approval)
        if state.approved:
            for step in plan.steps:
                result = await self.execute_step(step)
                state.execution_results.append(result)
        
        return state
```

### 4. Self-Reflection Pattern (LLM-as-Judge)

Used for quality evaluation by a separate model.

```python
# backend/orchestrator/llm_judge.py
class LLMJudge:
    """
    Self-Reflection: Separate model evaluates primary model's output
    
    Uses different model family to avoid shared biases:
    - Primary: GPT-4 (generation)
    - Judge: Claude (evaluation)
    """
    def __init__(self):
        self.judge_model = "claude-3-sonnet"  # Different from primary
    
    async def evaluate(self, plan: ExecutionPlan, context: dict) -> JudgeScore:
        scores = {
            "quality": await self._score_quality(plan),      # 1-10
            "safety": await self._validate_safety(plan),     # Pass/Fail
            "factual": await self._check_factual(plan, context),  # 1-10
            "feasibility": await self._assess_feasibility(plan),  # 1-10
            "risk": await self._assess_risk(plan)            # Low/Med/High
        }
        return JudgeScore(**scores)
```

### 5. Circuit Breaker Pattern

Prevents cascading failures when external services fail.

```python
# backend/utils/circuit_breaker.py
class CircuitBreaker:
    """
    States: CLOSED (normal) → OPEN (blocking) → HALF_OPEN (testing)
    """
    def __init__(self, failure_threshold=5, recovery_timeout=30):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.state = CircuitState.CLOSED
        self.failure_count = 0
    
    async def call(self, func, *args, **kwargs):
        if self.state == CircuitState.OPEN:
            if self._should_attempt_reset():
                self.state = CircuitState.HALF_OPEN
            else:
                raise CircuitOpenError()
        
        try:
            result = await func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise
```

---

## Protocol Layers

### Protocol Decision Matrix

| Use Case | Protocol | Reason | Implementation |
|----------|----------|--------|----------------|
| ServiceNow → System | **Kafka** | External system, audit trail | `incident_consumer.py` |
| Swarm agents coordinating | **A2A** | Real-time consensus | `swarm_retriever.py` |
| Agent calling RAG tool | **MCP** | Tool invocation standard | `mcp_client.py` |
| Plan approved → Execute | **Kafka** | State transition, replay | `kafka_producer.py` |
| Terraform ↔ Ansible coord | **A2A** | Agent coordination | `a2a_mesh.py` |
| Agent → GitHub Actions | **REST** | workflow_dispatch API | `github_actions.py` |
| Judge evaluating output | **A2A** | Real-time evaluation | `llm_judge.py` |
| Incident closed → SNOW | **Kafka** | External system, audit | `kafka_producer.py` |
| Redis/PG memory access | **SDK** | Direct, low latency | Direct calls |

### Protocol Layer Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                     PROTOCOL LAYERS                                     │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  🔴 KAFKA (External + Audit + State)                                   │
│  ────────────────────────────────────                                  │
│  Topics:                                                                │
│  • incident.created      - New incidents from external systems         │
│  • incident.enriched     - After context enrichment                    │
│  • plan.generated        - After LLM creates plan                      │
│  • incident.approved     - After human/auto approval                   │
│  • incident.executed     - After execution completes                   │
│  • incident.verified     - After verification passes                   │
│  • incident.closed       - Final state                                 │
│  • *.DLQ                 - Dead letter queues for each topic          │
│                                                                         │
│  🔵 A2A (Agent-to-Agent Real-time)                                     │
│  ────────────────────────────────────                                  │
│  Messages:                                                              │
│  • swarm.query           - RAG swarm search request                    │
│  • swarm.vote            - Agent voting on script selection            │
│  • judge.evaluate        - Request judge evaluation                    │
│  • judge.score           - Judge score response                        │
│  • agent.coordinate      - Inter-agent coordination                    │
│  • agent.capability      - Capability negotiation                      │
│                                                                         │
│  🟣 MCP (Agent-to-Tool)                                                │
│  ────────────────────────────────────                                  │
│  Servers:                                                               │
│  • servicenow-mcp        - ServiceNow operations                       │
│  • rag-mcp               - RAG search and update                       │
│  • github-mcp            - GitHub operations                           │
│  • k8s-mcp               - Kubernetes operations                       │
│  • terraform-mcp         - Terraform operations                        │
│                                                                         │
│  🟠 REST/Webhooks (External APIs)                                      │
│  ────────────────────────────────────                                  │
│  Endpoints:                                                             │
│  • GitHub Actions        - workflow_dispatch                           │
│  • Jira                  - Issue creation                              │
│  • Slack/Teams           - Notifications                               │
│  • PagerDuty             - Escalations                                 │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Kafka Topics Schema

```python
# backend/streaming/schemas.py
from dataclasses import dataclass
from enum import Enum

class IncidentState(Enum):
    NEW = "new"
    RECEIVED = "received"
    ENRICHED = "enriched"
    JUDGED = "judged"
    PLAN_GENERATED = "plan_generated"
    APPROVED = "approved"
    REJECTED = "rejected"
    EXECUTED = "executed"
    VERIFIED = "verified"
    CLOSED = "closed"
    FAILED = "failed"
    ROLLBACK = "rollback"
    MANUAL = "manual"

@dataclass
class IncidentEvent:
    incident_id: str
    state: IncidentState
    timestamp: str
    source: str  # servicenow, gcp, datadog, etc.
    payload: dict
    trace_id: str  # OpenTelemetry trace ID
    
@dataclass
class PlanGeneratedEvent:
    incident_id: str
    plan_id: str
    script_id: str
    script_type: str  # terraform, ansible, shell
    confidence: float
    judge_score: dict
    risk_level: str
    requires_approval: bool
```

### A2A Message Types

```python
# backend/agents/a2a/messages.py
from dataclasses import dataclass
from typing import List, Optional

@dataclass
class SwarmQueryMessage:
    """Request to swarm for script retrieval"""
    query_id: str
    incident_id: str
    query: str
    incident_type: str
    severity: str
    context: dict

@dataclass
class SwarmVoteMessage:
    """Agent's vote for script selection"""
    query_id: str
    agent_id: str
    script_id: str
    confidence: float
    reasoning: str

@dataclass
class JudgeEvaluateMessage:
    """Request for judge evaluation"""
    evaluation_id: str
    incident_id: str
    plan: dict
    context: dict
    
@dataclass
class JudgeScoreMessage:
    """Judge's evaluation response"""
    evaluation_id: str
    quality_score: float  # 1-10
    safety_passed: bool
    factual_score: float  # 1-10
    feasibility_score: float  # 1-10
    risk_level: str  # low, medium, high
    reasoning: str
    recommendations: List[str]
```

### MCP Server Definitions

```python
# backend/mcp/servers/rag_server.py
from mcp import MCPServer, tool

class RAGMCPServer(MCPServer):
    """MCP Server for RAG operations"""
    
    @tool(name="search_runbooks")
    async def search_runbooks(
        self,
        query: str,
        incident_type: str,
        top_k: int = 5
    ) -> List[dict]:
        """Search runbooks using hybrid search"""
        pass
    
    @tool(name="find_terraform")
    async def find_terraform(
        self,
        service: str,
        action: str,
        cloud_provider: str
    ) -> List[dict]:
        """Find Terraform scripts for remediation"""
        pass
    
    @tool(name="find_ansible")
    async def find_ansible(
        self,
        service: str,
        action: str,
        environment: str
    ) -> List[dict]:
        """Find Ansible playbooks for remediation"""
        pass
    
    @tool(name="update_success")
    async def update_success(
        self,
        script_id: str,
        incident_id: str,
        success: bool,
        resolution_time: float
    ) -> bool:
        """Update script success metrics after execution"""
        pass
```

---

## System Architecture

### High-Level Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              EXTERNAL SYSTEMS                                    │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐      │
│  │ServiceNow│   │   GCP    │   │ Datadog  │   │  GitHub  │   │  Slack   │      │
│  │   API    │   │ Pub/Sub  │   │ Webhook  │   │ Actions  │   │  Notify  │      │
│  └────┬─────┘   └────┬─────┘   └────┬─────┘   └────┬─────┘   └────┬─────┘      │
└───────┼──────────────┼──────────────┼──────────────┼──────────────┼─────────────┘
        │              │              │              │              │
        │         (Kafka)        (Kafka)        (REST)        (REST)
        ▼              ▼              ▼              ▲              ▲
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           INGESTION ZONE                                         │
│  ┌───────────────────────────────────────────────────────────────────────────┐  │
│  │                    SNOW MCP Server (Protocol: MCP)                         │  │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐           │  │
│  │  │  get_incident() │  │ update_status() │  │ close_ticket()  │           │  │
│  │  └─────────────────┘  └─────────────────┘  └─────────────────┘           │  │
│  └───────────────────────────────────────────────────────────────────────────┘  │
│                                      │                                           │
│                                      ▼                                           │
│  ┌───────────────────────────────────────────────────────────────────────────┐  │
│  │                    AIOps Correlator (-94% noise)                          │  │
│  │  • Deduplication  • Event correlation  • Severity enrichment              │  │
│  └───────────────────────────────────────────────────────────────────────────┘  │
│                                      │                                           │
│                                      ▼                                           │
│                         [incident.created] → Kafka                              │
└─────────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    KAFKA EVENT BUS (Protocol: Avro/Protobuf)                    │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐              │
│  │  incident.  │ │   plan.     │ │  incident.  │ │  incident.  │              │
│  │  created    │ │  generated  │ │  approved   │ │  executed   │  ...        │
│  └─────────────┘ └─────────────┘ └─────────────┘ └─────────────┘              │
└─────────────────────────────────────────────────────────────────────────────────┘
                                       │
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    A2A AGENT MESH (Protocol: A2A/JSON-RPC)                      │
│  Real-time agent communication • Swarm coordination • Judge evaluation          │
└─────────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          INTELLIGENCE ZONE                                       │
│                                                                                  │
│  ┌─────────────────────┐  ┌─────────────────────┐  ┌─────────────────────┐     │
│  │  Event Orchestrator │  │ Decision Orchestrator│  │   LLM Reasoning    │     │
│  │       (HUB)         │  │   (Plan-Execute)     │  │  (Chain-of-Thought)│     │
│  │  Kafka Consumer     │  │   MCP Host           │  │                    │     │
│  └─────────────────────┘  └─────────────────────┘  └─────────────────────┘     │
│            │                        │                        │                  │
│            │              ┌─────────┴─────────┐              │                  │
│            │              ▼                   ▼              │                  │
│            │   ┌─────────────────┐  ┌─────────────────┐     │                  │
│            │   │ Direct Memory   │  │  SWARM RAG MCP  │     │                  │
│            │   │ Redis ~1ms      │  │  (A2A + MCP)    │     │                  │
│            │   │ PostgreSQL ~10ms│  │                 │     │                  │
│            │   │ (No MCP - SDK)  │  │ Vector + Graph  │     │                  │
│            │   └─────────────────┘  │ find_terraform()│     │                  │
│            │                        │ find_ansible()  │     │                  │
│            │                        └─────────────────┘     │                  │
│            │                                                │                  │
│            │                        ┌───────────────────────┘                  │
│            │                        ▼                                          │
│            │              ┌─────────────────────┐                              │
│            │              │   LLM-as-Judge      │                              │
│            │              │  (Self-Reflection)  │                              │
│            │              │  Separate Model     │                              │
│            │              │  Protocol: A2A      │                              │
│            │              └─────────────────────┘                              │
│            │                        │                                          │
│            │                        ▼                                          │
│            │              [plan.generated] → Kafka                             │
└────────────┼────────────────────────────────────────────────────────────────────┘
             │                        │
             │                        ▼
┌────────────┼────────────────────────────────────────────────────────────────────┐
│            │              GOVERNANCE ZONE                                        │
│            │                                                                     │
│            │   ┌─────────────────────────────────────────────────────────────┐  │
│            │   │              Control Plane (Hierarchical)                    │  │
│            │   │  Policy Engine + Risk Assessment + Judge Score Input        │  │
│            │   │                                                              │  │
│            │   │  ┌─────────┐  ┌─────────┐  ┌─────────┐                     │  │
│            │   │  │ LOW     │  │ MEDIUM  │  │  HIGH   │                     │  │
│            │   │  │ → Auto  │  │ → Async │  │ → Manual│                     │  │
│            │   │  └─────────┘  └─────────┘  └─────────┘                     │  │
│            │   └─────────────────────────────────────────────────────────────┘  │
│            │                        │                                           │
│            │   ┌─────────────────────────────────────────────────────────────┐  │
│            │   │           Human-in-the-Loop (HITL)                          │  │
│            │   │  • Slack/Teams approval buttons                             │  │
│            │   │  • Timeout escalation: 15m → 30m → page                    │  │
│            │   │  • Override reason required + audit                         │  │
│            │   └─────────────────────────────────────────────────────────────┘  │
│            │                        │                                           │
│            │              [incident.approved] or [incident.rejected] → Kafka   │
└────────────┼────────────────────────────────────────────────────────────────────┘
             │                        │
             │                        ▼
┌────────────┼────────────────────────────────────────────────────────────────────┐
│            │              EXECUTION ZONE                                         │
│            │                                                                     │
│            │   ┌─────────────────────────────────────────────────────────────┐  │
│            │   │         Execution Orchestrator (Hierarchical)                │  │
│            │   │  🚫 No LLM • ✅ Execute Only • Token Auth • Allowlist       │  │
│            │   │  Protocol: A2A to agents, REST to GitHub Actions            │  │
│            │   └─────────────────────────────────────────────────────────────┘  │
│            │              │              │              │                       │
│            │              ▼              ▼              ▼                       │
│            │   ┌──────────────┐ ┌──────────────┐ ┌──────────────┐             │
│            │   │  Terraform   │ │   Ansible    │ │    Code      │             │
│            │   │    Agent     │ │    Agent     │ │   Agent      │             │
│            │   │ MCP: K8s/TF  │ │ MCP: Config  │ │ MCP: GitHub  │             │
│            │   │ REST→GH Act  │ │ REST→GH Act  │ │ REST→GH Act  │             │
│            │   └──────────────┘ └──────────────┘ └──────────────┘             │
│            │                        │                                          │
│            │   ┌─────────────────────────────────────────────────────────────┐ │
│            │   │  Scripts from Swarm RAG:                                    │ │
│            │   │  Terraform .tf • Ansible .yml • K8s manifests              │ │
│            │   │  → Fetched by Swarm → Executed via GitHub Actions          │ │
│            │   └─────────────────────────────────────────────────────────────┘ │
│            │                        │                                          │
│            │              [incident.executed] → Kafka                          │
└────────────┼────────────────────────────────────────────────────────────────────┘
             │                        │
             │                        ▼
┌────────────┼────────────────────────────────────────────────────────────────────┐
│            │              FEEDBACK ZONE                                          │
│            │                                                                     │
│            │   ┌──────────────┐ ┌──────────────┐ ┌──────────────┐             │
│            │   │ Verification │ │   Learning   │ │Ticket Closer │             │
│            │   │   Engine     │ │   Engine     │ │ (MCP→SNOW)   │             │
│            │   │ +Judge Valid │ │ Update RAG   │ │              │             │
│            │   └──────────────┘ └──────────────┘ └──────────────┘             │
│            │                        │                                          │
│            │   ┌─────────────────────────────────────────────────────────────┐ │
│            │   │              Swarm RAG Feedback                             │ │
│            │   │  • Index successful scripts                                 │ │
│            │   │  • Boost ranking scores                                     │ │
│            │   │  • Avoid blast radius patterns                              │ │
│            │   └─────────────────────────────────────────────────────────────┘ │
│            │                        │                                          │
│            │   ┌──────────────────────────────────────────────────────────────┐│
│            │   │  Auto-Postmortem: Timeline • RCA • Jira • Judge review      ││
│            │   └──────────────────────────────────────────────────────────────┘│
│            │                        │                                          │
│            │              [incident.closed] → Kafka                            │
└────────────┴────────────────────────────────────────────────────────────────────┘
```

### Data Layer

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                            DATA LAYER                                            │
│                                                                                  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐        │
│  │  PostgreSQL  │  │    Redis     │  │   Weaviate   │  │    Neo4j     │        │
│  │              │  │              │  │              │  │              │        │
│  │ • Incidents  │  │ • Cache      │  │ • Vectors    │  │ • FIXED_BY   │        │
│  │ • Audit Log  │  │ • Sessions   │  │ • Embeddings │  │ • Relations  │        │
│  │ • State      │  │ • Short-term │  │ • Runbooks   │  │ • Patterns   │        │
│  │              │  │   Memory     │  │              │  │              │        │
│  │ Access: SDK  │  │ Access: SDK  │  │ Access: MCP  │  │ Access: MCP  │        │
│  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘        │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## Component Details

### 1. Event Orchestrator (Hub)

**File**: `backend/orchestrator/event_orchestrator.py`

```python
class EventOrchestrator:
    """
    Central hub that consumes from Kafka and coordinates all processing.
    
    Pattern: Hub & Spoke
    Protocol: Kafka (consume), A2A (coordinate)
    """
    
    def __init__(self):
        self.kafka_consumer = KafkaConsumer(topics=["incident.created"])
        self.decision_orch = DecisionOrchestrator()
        self.a2a_client = A2AClient()
    
    async def run(self):
        async for message in self.kafka_consumer:
            incident = IncidentEvent.from_kafka(message)
            
            # Coordinate via A2A
            await self.a2a_client.send(
                "decision_orchestrator",
                ProcessIncidentMessage(incident_id=incident.id)
            )
```

### 2. Decision Orchestrator (MCP Host)

**File**: `backend/orchestrator/decision_orchestrator.py`

```python
class DecisionOrchestrator:
    """
    MCP Host that coordinates LLM reasoning and RAG retrieval.
    
    Pattern: Plan-Execute
    Protocol: MCP (to tools), A2A (to judge)
    """
    
    def __init__(self):
        self.mcp_client = MCPClient()
        self.a2a_client = A2AClient()
        self.llm = LLMIntelligence()
    
    async def process(self, incident_id: str) -> ExecutionPlan:
        # 1. Get incident from MCP
        incident = await self.mcp_client.call(
            "servicenow-mcp", 
            "get_incident", 
            {"incident_id": incident_id}
        )
        
        # 2. Enrich context via MCP (ReAct pattern)
        context = await self.enrich_context(incident)
        
        # 3. Search scripts via Swarm RAG (A2A + MCP)
        scripts = await self.swarm_search(incident, context)
        
        # 4. Generate plan (Chain-of-Thought)
        plan = await self.llm.generate_plan(incident, context, scripts)
        
        # 5. Get Judge evaluation (A2A)
        judge_score = await self.a2a_client.request(
            "llm_judge",
            JudgeEvaluateMessage(plan=plan, context=context)
        )
        
        plan.judge_score = judge_score
        return plan
```

### 3. Swarm RAG System

**File**: `backend/rag/swarm_retriever.py`

```python
class SwarmRetriever:
    """
    Multi-agent swarm for script retrieval with consensus.
    
    Pattern: Swarm Intelligence
    Protocol: A2A (coordination), MCP (tool calls)
    
    Agents:
    - VectorAgent: Semantic similarity search
    - KeywordAgent: TF-IDF exact matching
    - GraphAgent: Neo4j FIXED_BY relationships
    - MetadataAgent: Exact field matching
    """
    
    def __init__(self):
        self.a2a_client = A2AClient()
        self.agents = [
            VectorAgent(),    # Semantic similarity
            KeywordAgent(),   # TF-IDF exact match
            GraphAgent(),     # Neo4j FIXED_BY
            MetadataAgent()   # Exact field match
        ]
        self.rrf_k = 60  # Industry standard RRF constant

    async def search(self, query: SwarmQueryMessage) -> List[Script]:
        # 1. Broadcast query to all agents (A2A)
        agent_results = []
        for agent in self.agents:
            results = await self.a2a_client.request(
                agent.id,
                query
            )
            agent_results.append((agent.type, results))

        # 2. RRF Fusion (weight-free, rank-based)
        fused = self.apply_rrf_fusion(agent_results)

        # 3. Cross-encoder reranking
        reranked = await self.reranker.rerank(query, fused[:20])

        # 4. Return top scripts
        return reranked[:5]

    def apply_rrf_fusion(self, agent_results: List) -> List:
        """
        Reciprocal Rank Fusion (RRF) - Industry standard fusion.

        RRF Formula: Score = Σ (1 / (k + rank_i)) for each agent
        k = 60 (standard constant used by Google, Bing, Elasticsearch)

        Advantages over weighted consensus:
        - No manual weight tuning required
        - Scale-invariant (works regardless of raw score magnitudes)
        - Fair fusion based on relative ranking
        """
        doc_scores = defaultdict(float)

        for agent_type, results in agent_results:
            for rank, result in enumerate(results, start=1):
                # RRF contribution: 1 / (k + rank)
                rrf_score = 1.0 / (self.rrf_k + rank)
                doc_scores[result.id] += rrf_score

        # Sort by RRF score
        ranked = sorted(doc_scores.items(), key=lambda x: x[1], reverse=True)
        return ranked
```

### 4. LLM-as-Judge

**File**: `backend/orchestrator/llm_judge.py`

```python
class LLMJudge:
    """
    Separate evaluation model for quality assurance.
    
    Pattern: Self-Reflection
    Protocol: A2A (receive requests, send scores)
    
    Evaluations:
    1. Quality (1-10): Plan structure, completeness
    2. Safety (Pass/Fail): Guardrail compliance
    3. Factual (1-10): RAG context match
    4. Feasibility (1-10): Executability
    5. Risk (Low/Med/High): Blast radius
    """
    
    def __init__(self):
        # Use DIFFERENT model family than primary LLM
        self.model = "claude-3-sonnet"  # Primary uses GPT-4
        self.a2a_server = A2AServer()
    
    async def start(self):
        """Listen for evaluation requests via A2A"""
        async for message in self.a2a_server.listen("judge.evaluate"):
            score = await self.evaluate(message)
            await self.a2a_server.respond(
                message.reply_to,
                JudgeScoreMessage(**score)
            )
    
    async def evaluate(self, request: JudgeEvaluateMessage) -> dict:
        plan = request.plan
        context = request.context
        
        # Parallel evaluation
        quality, safety, factual, feasibility, risk = await asyncio.gather(
            self._score_quality(plan),
            self._validate_safety(plan),
            self._check_factual(plan, context),
            self._assess_feasibility(plan),
            self._assess_risk(plan)
        )
        
        return {
            "quality_score": quality,
            "safety_passed": safety,
            "factual_score": factual,
            "feasibility_score": feasibility,
            "risk_level": risk,
            "overall_pass": safety and quality > 6 and factual > 7
        }
```

### 5. Control Plane

**File**: `backend/agents/control_plane.py`

```python
class ControlPlane:
    """
    Policy enforcement and approval routing.
    
    Pattern: Hierarchical
    Protocol: Kafka (state), A2A (judge score)
    """
    
    def __init__(self):
        self.policy_engine = PolicyEngine()
        self.kafka_producer = KafkaProducer()
    
    async def evaluate(self, plan: ExecutionPlan) -> ApprovalDecision:
        # Get judge score from plan
        judge_score = plan.judge_score
        
        # Evaluate risk
        risk = self.policy_engine.evaluate_risk(plan)
        
        # Determine approval route
        if not judge_score.safety_passed:
            return ApprovalDecision.REJECTED
        
        if risk == "low" and judge_score.quality_score > 8:
            # Auto-approve
            await self.kafka_producer.send(
                "incident.approved",
                ApprovedEvent(plan_id=plan.id, auto=True)
            )
            return ApprovalDecision.AUTO_APPROVED
        
        elif risk == "medium":
            # Async approval (Slack notification)
            await self.request_async_approval(plan)
            return ApprovalDecision.PENDING_ASYNC
        
        else:  # high risk
            # Require manual approval
            await self.request_manual_approval(plan)
            return ApprovalDecision.PENDING_MANUAL
```

### 6. Execution Orchestrator

**File**: `backend/agents/execution_orchestrator.py`

```python
class ExecutionOrchestrator:
    """
    Executes approved plans via specialized agents.
    
    Pattern: Hierarchical
    Protocol: A2A (to agents), REST (to GitHub Actions)
    
    IMPORTANT: No LLM in execution path - only deterministic execution.
    """
    
    def __init__(self):
        self.a2a_client = A2AClient()
        self.github_client = GitHubActionsClient()
        self.agents = {
            "terraform": TerraformAgent(),
            "ansible": AnsibleAgent(),
            "code": CodeAgent()
        }
    
    async def execute(self, plan: ExecutionPlan) -> ExecutionResult:
        # 1. Validate plan (no LLM - deterministic)
        if not self.validate_plan(plan):
            raise ValidationError("Plan validation failed")
        
        # 2. Select agent based on script type
        agent = self.agents[plan.script_type]
        
        # 3. Coordinate with agent (A2A)
        await self.a2a_client.send(
            agent.id,
            ExecuteMessage(plan=plan)
        )
        
        # 4. Agent triggers GitHub Actions (REST)
        result = await agent.execute(plan)
        
        # 5. Publish result to Kafka
        await self.kafka_producer.send(
            "incident.executed",
            ExecutedEvent(plan_id=plan.id, result=result)
        )
        
        return result
```

### 7. Terraform Agent

**File**: `backend/agents/terraform_agent.py`

```python
class TerraformAgent:
    """
    Executes Terraform scripts via GitHub Actions.
    
    Protocol: MCP (tools), REST (GitHub Actions)
    """
    
    def __init__(self):
        self.mcp_client = MCPClient()
        self.github_client = GitHubActionsClient()
    
    async def execute(self, plan: ExecutionPlan) -> ExecutionResult:
        script = plan.script
        
        # 1. Validate script is in allowlist
        if not self.validate_script(script):
            raise SecurityError("Script not in allowlist")
        
        # 2. Generate rollback plan
        rollback = await self.generate_rollback(script)
        
        # 3. Trigger GitHub Actions workflow
        run_id = await self.github_client.trigger_workflow(
            workflow="terraform-apply.yml",
            inputs={
                "script_path": script.path,
                "environment": plan.environment,
                "dry_run": plan.dry_run,
                "rollback_script": rollback.path
            }
        )
        
        # 4. Wait for completion
        result = await self.github_client.wait_for_completion(run_id)
        
        return ExecutionResult(
            success=result.conclusion == "success",
            output=result.logs,
            run_id=run_id
        )
```

### 8. Ansible Agent

**File**: `backend/agents/ansible_agent.py`

```python
class AnsibleAgent:
    """
    Executes Ansible playbooks via GitHub Actions.
    
    Protocol: MCP (tools), REST (GitHub Actions)
    """
    
    async def execute(self, plan: ExecutionPlan) -> ExecutionResult:
        playbook = plan.script
        
        # Trigger GitHub Actions
        run_id = await self.github_client.trigger_workflow(
            workflow="ansible-run.yml",
            inputs={
                "playbook_path": playbook.path,
                "inventory": plan.inventory,
                "extra_vars": json.dumps(plan.variables),
                "check_mode": plan.dry_run
            }
        )
        
        result = await self.github_client.wait_for_completion(run_id)
        return ExecutionResult(success=result.conclusion == "success")
```

---

## Workflow (14 Steps + Judge)

### State Machine

```
NEW → RECEIVED → ENRICHED → JUDGED → PLAN_GENERATED → APPROVED → EXECUTED → VERIFIED → CLOSED
                                            │
                                            ├── REJECTED → END
                                            │
                                            └── (on failure) → ROLLBACK → MANUAL
```

### Detailed Workflow

| Step | Name | Zone | Pattern | Protocol | Description |
|------|------|------|---------|----------|-------------|
| 1 | ServiceNow Ingest | Ingestion | - | MCP | Receive incident from ServiceNow |
| 2 | MCP Server | Ingestion | - | MCP | Transform to internal format |
| 3 | Event Orchestrator | Intelligence | Hub & Spoke | Kafka (consume) | Central hub receives event |
| 4 | Decision Orchestrator | Intelligence | Plan-Execute | A2A, MCP | Coordinate enrichment and planning |
| 5 | LLM Reasoning | Intelligence | Chain-of-Thought | Internal | Generate remediation plan |
| 5b | **LLM-as-Judge** | Intelligence | **Self-Reflection** | **A2A** | **Evaluate plan quality/safety** |
| 6 | Control Plane | Governance | Hierarchical | Kafka, A2A | Apply policy, route approval |
| 7 | Execution Orchestrator | Execution | Hierarchical | A2A | Coordinate agent execution |
| 8 | Terraform Agent | Execution | - | MCP, REST | Execute Terraform via GH Actions |
| 9 | Ansible Agent | Execution | - | MCP, REST | Execute Ansible via GH Actions |
| 10 | Code Agent | Execution | - | MCP, REST | Create PR/hotfix |
| 11 | Verification Engine | Feedback | - | Internal | Verify fix + Judge validation |
| 12 | Learning Engine | Feedback | - | MCP | Update Swarm RAG |
| 13 | Ticket Closer | Feedback | - | MCP | Close ServiceNow ticket |
| 14 | Auto-Postmortem | Feedback | - | REST | Generate postmortem, create Jira |

### LangGraph Implementation

```python
# backend/orchestrator/langgraph_workflow.py
from langgraph.graph import StateGraph, END

class IncidentWorkflow:
    def __init__(self):
        self.graph = StateGraph(IncidentState)
        self._build_graph()
    
    def _build_graph(self):
        # Add nodes
        self.graph.add_node("ingest", self.ingest_node)
        self.graph.add_node("event_orchestrator", self.event_orch_node)
        self.graph.add_node("decision_orchestrator", self.decision_orch_node)
        self.graph.add_node("llm_reasoning", self.llm_reasoning_node)
        self.graph.add_node("llm_judge", self.llm_judge_node)  # Step 5b
        self.graph.add_node("control_plane", self.control_plane_node)
        self.graph.add_node("execution_orchestrator", self.execution_orch_node)
        self.graph.add_node("verification", self.verification_node)
        self.graph.add_node("learning", self.learning_node)
        self.graph.add_node("ticket_closer", self.ticket_closer_node)
        self.graph.add_node("postmortem", self.postmortem_node)
        
        # Add edges
        self.graph.add_edge("ingest", "event_orchestrator")
        self.graph.add_edge("event_orchestrator", "decision_orchestrator")
        self.graph.add_edge("decision_orchestrator", "llm_reasoning")
        self.graph.add_edge("llm_reasoning", "llm_judge")  # Always go to judge
        
        # Conditional edge based on judge score
        self.graph.add_conditional_edges(
            "llm_judge",
            self.judge_router,
            {
                "approved": "control_plane",
                "rejected": END,
                "needs_revision": "llm_reasoning"  # Loop back
            }
        )
        
        self.graph.add_conditional_edges(
            "control_plane",
            self.approval_router,
            {
                "auto_approved": "execution_orchestrator",
                "pending": "wait_approval",  # External wait
                "rejected": END
            }
        )
        
        self.graph.add_edge("execution_orchestrator", "verification")
        self.graph.add_conditional_edges(
            "verification",
            self.verification_router,
            {
                "success": "learning",
                "failure": "rollback"
            }
        )
        self.graph.add_edge("learning", "ticket_closer")
        self.graph.add_edge("ticket_closer", "postmortem")
        self.graph.add_edge("postmortem", END)
        
        # Set entry point
        self.graph.set_entry_point("ingest")
    
    def judge_router(self, state: IncidentState) -> str:
        """Route based on judge evaluation"""
        judge_score = state.judge_score
        
        if not judge_score.safety_passed:
            return "rejected"
        if judge_score.quality_score < 6:
            return "needs_revision"
        return "approved"
```

---

## Swarm RAG System

### Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         SWARM RAG SYSTEM v4.0                                    │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│   Query: "VM instance test-vm-01 is down in us-central1-a"                      │
│                                                                                  │
│   ┌─────────────────────────────────────────────────────────────────────────┐   │
│   │                    QUERY UNDERSTANDING (LLM + Rules)                     │   │
│   │                                                                          │   │
│   │  Intent: RESTART                                                         │   │
│   │  Entities: {instance: "test-vm-01", zone: "us-central1-a"}              │   │
│   │  Service: GCP                                                            │   │
│   │  Expanded: "VM down restart recover fix compute instance GCP"            │   │
│   └─────────────────────────────────────────────────────────────────────────┘   │
│                                      │                                           │
│                                      ▼                                           │
│   ┌─────────────────────────────────────────────────────────────────────────┐   │
│   │                    SWARM AGENTS (A2A Coordination)                       │   │
│   │                                                                          │   │
│   │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌────────────┐  │   │
│   │  │ VectorAgent  │  │ KeywordAgent │  │  GraphAgent  │  │MetadataAgent│ │   │
│   │  │  Rank 1      │  │   Rank 3     │  │   Rank 1     │  │  Rank 2    │  │   │
│   │  │              │  │              │  │              │  │            │  │   │
│   │  │ all-MiniLM   │  │   TF-IDF     │  │  FIXED_BY    │  │   Exact    │  │   │
│   │  │ Semantic     │  │   Bigrams    │  │  Neo4j       │  │   Match    │  │   │
│   │  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └─────┬──────┘  │   │
│   │         │                 │                 │                │         │   │
│   │         └─────────────────┼─────────────────┼────────────────┘         │   │
│   │                           ▼                 ▼                          │   │
│   │                  ┌─────────────────────────────────┐                   │   │
│   │                  │      RRF FUSION (v5.0)          │                   │   │
│   │                  │                                 │                   │   │
│   │                  │  Score = Σ 1/(k + rank)         │                   │
│   │                  │  k = 60 (industry standard)     │                   │   │
│   │                  └─────────────────────────────────┘                   │   │
│   └─────────────────────────────────────────────────────────────────────────┘   │
│                                      │                                           │
│                                      ▼                                           │
│   ┌─────────────────────────────────────────────────────────────────────────┐   │
│   │                    CROSS-ENCODER RERANKING                               │   │
│   │                    ms-marco-MiniLM-L-6-v2                                │   │
│   │                    +20-30% precision improvement                         │   │
│   └─────────────────────────────────────────────────────────────────────────┘   │
│                                      │                                           │
│                                      ▼                                           │
│   ┌─────────────────────────────────────────────────────────────────────────┐   │
│   │                    TOP RESULTS                                           │   │
│   │                                                                          │   │
│   │  1. terraform-gcp-restart-vm.tf    (score: 0.92, type: terraform)       │   │
│   │  2. ansible-gcp-vm-recovery.yml    (score: 0.87, type: ansible)         │   │
│   │  3. shell-gcp-instance-restart.sh  (score: 0.81, type: shell)           │   │
│   └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### RRF Scoring Formula (v5.0)

```
RRF_Score = Σ (1 / (k + rank_i)) for each agent i
k = 60 (industry standard constant)

Example:
- Vector Agent:   Rank 1  → 1/(60+1) = 0.0164
- Keyword Agent:  Rank 3  → 1/(60+3) = 0.0159
- Graph Agent:    Rank 1  → 1/(60+1) = 0.0164
- Metadata Agent: Rank 2  → 1/(60+2) = 0.0161

RRF Score = 0.0164 + 0.0159 + 0.0164 + 0.0161 = 0.0648

Advantages over weighted scoring:
- No manual weight tuning required
- Scale-invariant (works regardless of raw score magnitudes)
- Industry standard (Google, Bing, OpenAI, Elasticsearch)
```

### Graph Scoring (Neo4j)

```cypher
// Query: Get scripts that have successfully fixed similar incidents
MATCH (i:Incident {service: $service})-[r:FIXED_BY]->(s:Script)
WHERE r.success = true
RETURN s.id, 
       COUNT(r) as fixed_count,
       AVG(r.success_rate) as avg_success,
       AVG(r.resolution_time) as avg_time
ORDER BY fixed_count DESC, avg_success DESC
```

### Implementation

```python
# backend/rag/swarm_retriever.py
class SwarmRetriever:
    """
    Swarm RAG with multi-agent consensus.
    """
    
    async def search(self, query: str, context: dict) -> List[Script]:
        # 1. Query understanding
        understood = await self.query_understanding.analyze(query, context)
        
        # 2. Parallel agent search (A2A)
        tasks = [
            self.vector_agent.search(understood),
            self.keyword_agent.search(understood),
            self.graph_agent.search(understood),
            self.metadata_agent.search(understood)
        ]
        results = await asyncio.gather(*tasks)
        
        # 3. Consensus voting
        consensus = self.calculate_consensus(results)
        
        # 4. Cross-encoder reranking
        reranked = await self.reranker.rerank(query, consensus.candidates)
        
        # 5. Blast radius check
        safe_scripts = self.filter_blast_radius(reranked, context)
        
        return safe_scripts[:5]
    
    def filter_blast_radius(self, scripts: List[Script], context: dict) -> List[Script]:
        """
        Filter out scripts that might cause excessive blast radius.
        """
        safe = []
        for script in scripts:
            risk = self.assess_blast_radius(script, context)
            if risk.level != "critical":
                script.risk_assessment = risk
                safe.append(script)
        return safe
```

---

## Execution via GitHub Actions

### Workflow Dispatch Pattern

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    GITHUB ACTIONS EXECUTION FLOW                                 │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  1. Agent receives approved plan                                                │
│                     │                                                           │
│                     ▼                                                           │
│  2. Validate script in allowlist                                                │
│                     │                                                           │
│                     ▼                                                           │
│  3. Generate rollback script                                                    │
│                     │                                                           │
│                     ▼                                                           │
│  4. REST: POST /repos/{owner}/{repo}/actions/workflows/{workflow}/dispatches   │
│     {                                                                           │
│       "ref": "main",                                                            │
│       "inputs": {                                                               │
│         "script_path": "runbooks/terraform/gcp-restart-vm.tf",                 │
│         "environment": "production",                                            │
│         "incident_id": "INC001234",                                            │
│         "dry_run": "false",                                                     │
│         "rollback_script": "runbooks/terraform/gcp-restart-vm-rollback.tf"     │
│       }                                                                         │
│     }                                                                           │
│                     │                                                           │
│                     ▼                                                           │
│  5. Poll for workflow run status                                                │
│                     │                                                           │
│                     ▼                                                           │
│  6. Return execution result                                                     │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Terraform Workflow

```yaml
# .github/workflows/terraform-apply.yml
name: Terraform Apply

on:
  workflow_dispatch:
    inputs:
      script_path:
        description: 'Path to Terraform script'
        required: true
      environment:
        description: 'Target environment'
        required: true
        type: choice
        options:
          - production
          - staging
          - development
      incident_id:
        description: 'Incident ID for tracking'
        required: true
      dry_run:
        description: 'Run in plan-only mode'
        required: true
        default: 'true'
      rollback_script:
        description: 'Path to rollback script'
        required: true

jobs:
  terraform:
    runs-on: ubuntu-latest
    environment: ${{ inputs.environment }}
    
    steps:
      - uses: actions/checkout@v4
      
      - uses: hashicorp/setup-terraform@v3
        with:
          terraform_version: 1.6.0
      
      - name: Terraform Init
        run: terraform init
        working-directory: ${{ inputs.script_path }}
      
      - name: Terraform Plan
        id: plan
        run: terraform plan -out=tfplan
        working-directory: ${{ inputs.script_path }}
      
      - name: Terraform Apply
        if: inputs.dry_run == 'false'
        run: terraform apply -auto-approve tfplan
        working-directory: ${{ inputs.script_path }}
      
      - name: Store Rollback Info
        if: inputs.dry_run == 'false'
        run: |
          echo "ROLLBACK_SCRIPT=${{ inputs.rollback_script }}" >> $GITHUB_ENV
          terraform show -json tfplan > state-backup.json
      
      - name: Report to Platform
        if: always()
        uses: actions/github-script@v7
        with:
          script: |
            const result = {
              incident_id: '${{ inputs.incident_id }}',
              status: '${{ job.status }}',
              run_id: '${{ github.run_id }}',
              environment: '${{ inputs.environment }}'
            };
            // Webhook back to platform
            await fetch('${{ secrets.PLATFORM_WEBHOOK }}', {
              method: 'POST',
              body: JSON.stringify(result)
            });
```

### Ansible Workflow

```yaml
# .github/workflows/ansible-run.yml
name: Ansible Run

on:
  workflow_dispatch:
    inputs:
      playbook_path:
        description: 'Path to Ansible playbook'
        required: true
      inventory:
        description: 'Inventory file or dynamic inventory'
        required: true
      extra_vars:
        description: 'Extra variables as JSON'
        required: false
        default: '{}'
      check_mode:
        description: 'Run in check mode (dry run)'
        required: true
        default: 'true'
      incident_id:
        description: 'Incident ID for tracking'
        required: true

jobs:
  ansible:
    runs-on: ubuntu-latest
    
    steps:
      - uses: actions/checkout@v4
      
      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      
      - name: Install Ansible
        run: pip install ansible ansible-lint
      
      - name: Run Ansible Playbook
        run: |
          ansible-playbook \
            -i ${{ inputs.inventory }} \
            ${{ inputs.playbook_path }} \
            --extra-vars '${{ inputs.extra_vars }}' \
            ${{ inputs.check_mode == 'true' && '--check' || '' }}
        env:
          ANSIBLE_HOST_KEY_CHECKING: 'false'
      
      - name: Report to Platform
        if: always()
        run: |
          curl -X POST ${{ secrets.PLATFORM_WEBHOOK }} \
            -H "Content-Type: application/json" \
            -d '{"incident_id": "${{ inputs.incident_id }}", "status": "${{ job.status }}"}'
```

### GitHub Actions Client

```python
# backend/utils/github_actions.py
import aiohttp
from typing import Optional

class GitHubActionsClient:
    """
    Client for triggering and monitoring GitHub Actions workflows.
    
    Protocol: REST
    """
    
    def __init__(self, token: str, owner: str, repo: str):
        self.token = token
        self.owner = owner
        self.repo = repo
        self.base_url = f"https://api.github.com/repos/{owner}/{repo}"
    
    async def trigger_workflow(
        self,
        workflow: str,
        inputs: dict,
        ref: str = "main"
    ) -> str:
        """
        Trigger a workflow via workflow_dispatch.
        
        Returns: run_id
        """
        url = f"{self.base_url}/actions/workflows/{workflow}/dispatches"
        
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url,
                json={"ref": ref, "inputs": inputs},
                headers={
                    "Authorization": f"token {self.token}",
                    "Accept": "application/vnd.github.v3+json"
                }
            ) as resp:
                if resp.status != 204:
                    raise GitHubActionsError(f"Failed to trigger workflow: {await resp.text()}")
        
        # Get the run ID (workflow_dispatch doesn't return it directly)
        run_id = await self._get_latest_run_id(workflow)
        return run_id
    
    async def wait_for_completion(
        self,
        run_id: str,
        timeout: int = 600,
        poll_interval: int = 10
    ) -> WorkflowResult:
        """
        Poll for workflow completion.
        """
        url = f"{self.base_url}/actions/runs/{run_id}"
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url,
                    headers={"Authorization": f"token {self.token}"}
                ) as resp:
                    data = await resp.json()
                    
                    if data["status"] == "completed":
                        return WorkflowResult(
                            run_id=run_id,
                            conclusion=data["conclusion"],
                            logs_url=data["logs_url"]
                        )
            
            await asyncio.sleep(poll_interval)
        
        raise TimeoutError(f"Workflow {run_id} did not complete within {timeout}s")
```

---

## Quick Start

### Prerequisites

- Python 3.11+
- Node.js 18+
- Docker & Docker Compose
- OpenAI API key
- GitHub token (for Actions)
- ServiceNow instance (optional)

### 1. Clone Repository

```bash
git clone https://github.com/sam2881/test_01.git
cd ai_agent_app
```

### 2. Start Infrastructure

```bash
cd deployment
docker compose up -d
```

This starts:
- PostgreSQL (port 5432)
- Redis (port 6379)
- Kafka (port 29092)
- Weaviate (port 8080)
- Neo4j (port 7474)

### 3. Configure Environment

```bash
cat > .env << 'EOF'
# OpenAI (Primary LLM)
OPENAI_API_KEY=sk-your-key-here

# Anthropic (Judge LLM - different model family)
ANTHROPIC_API_KEY=sk-ant-your-key-here

# ServiceNow
SNOW_INSTANCE_URL=https://your-instance.service-now.com
SNOW_USERNAME=admin
SNOW_PASSWORD=your-password

# GitHub Actions
GITHUB_TOKEN=ghp_your-token-here
GITHUB_OWNER=your-org
GITHUB_REPO=your-repo

# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:29092

# Database
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/ai_agent
REDIS_URL=redis://localhost:6379

# Neo4j
NEO4J_URI=bolt://localhost:7687
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=password123

# Weaviate
WEAVIATE_URL=http://localhost:8080

# A2A Mesh
A2A_MESH_HOST=localhost
A2A_MESH_PORT=9000

# MCP Servers
MCP_SERVICENOW_PORT=9001
MCP_RAG_PORT=9002
MCP_GITHUB_PORT=9003
EOF
```

### 4. Start Backend Services

```bash
# Terminal 1: Start A2A Mesh
cd backend
python -m agents.a2a.mesh

# Terminal 2: Start MCP Servers
python -m mcp.servers.start_all

# Terminal 3: Start Main Orchestrator
python orchestrator/main.py

# Terminal 4: Start LLM Judge
python orchestrator/llm_judge.py
```

### 5. Start Frontend

```bash
cd frontend
npm install
npm run dev
```

### 6. Access Services

| Service | URL |
|---------|-----|
| Frontend | http://localhost:3002 |
| Backend API | http://localhost:8000 |
| API Docs | http://localhost:8000/docs |
| Neo4j Browser | http://localhost:7474 |
| A2A Mesh | http://localhost:9000 |

---

## API Reference

### Core APIs

| Method | Endpoint | Description | Protocol |
|--------|----------|-------------|----------|
| POST | `/api/incidents` | Create incident | REST |
| GET | `/api/incidents/{id}` | Get incident | REST |
| POST | `/api/langgraph/run` | Execute workflow | REST |
| POST | `/api/approvals/{id}/approve` | Approve plan | REST |
| POST | `/api/approvals/{id}/reject` | Reject plan | REST |

### RAG APIs

| Method | Endpoint | Description | Protocol |
|--------|----------|-------------|----------|
| POST | `/api/rag/search` | Swarm RAG search | REST→MCP |
| POST | `/api/rag/feedback` | Record feedback | REST→MCP |
| GET | `/api/rag/stats` | Get RAG stats | REST |

### Execution APIs

| Method | Endpoint | Description | Protocol |
|--------|----------|-------------|----------|
| POST | `/api/execute/terraform` | Execute Terraform | REST→GH Actions |
| POST | `/api/execute/ansible` | Execute Ansible | REST→GH Actions |
| GET | `/api/execute/status/{run_id}` | Get execution status | REST |

### Webhook Endpoints

| Method | Endpoint | Description | Source |
|--------|----------|-------------|--------|
| POST | `/api/webhooks/servicenow` | ServiceNow incidents | ServiceNow |
| POST | `/api/webhooks/gcp` | GCP alerts | GCP Pub/Sub |
| POST | `/api/webhooks/datadog` | Datadog alerts | Datadog |
| POST | `/api/webhooks/github` | GitHub Actions results | GitHub |

---

## Project Structure

```
ai_agent_app/
├── backend/
│   ├── orchestrator/                  # LangGraph workflow
│   │   ├── main.py                    # FastAPI application
│   │   ├── event_orchestrator.py      # Hub (Kafka consumer)
│   │   ├── decision_orchestrator.py   # MCP Host
│   │   ├── llm_intelligence.py        # LLM reasoning
│   │   ├── llm_judge.py               # LLM-as-Judge (A2A)
│   │   ├── langgraph_workflow.py      # Workflow definition
│   │   ├── rollback_generator.py      # Rollback plans
│   │   └── nodes/                     # LangGraph nodes
│   │       ├── ingest.py
│   │       ├── enrich_context.py      # ReAct pattern
│   │       ├── llm_reasoning.py       # Chain-of-Thought
│   │       ├── verification.py
│   │       └── learning.py
│   │
│   ├── agents/                        # Hierarchical agents
│   │   ├── base_agent.py              # Agent interface
│   │   ├── control_plane.py           # Policy + Risk
│   │   ├── execution_orchestrator.py  # Agent coordinator
│   │   ├── terraform_agent.py         # Terraform execution
│   │   ├── ansible_agent.py           # Ansible execution
│   │   ├── code_agent.py              # Code/PR agent
│   │   └── a2a/                       # A2A protocol
│   │       ├── mesh.py                # A2A mesh server
│   │       ├── client.py              # A2A client
│   │       └── messages.py            # Message types
│   │
│   ├── mcp/                           # MCP servers
│   │   ├── client.py                  # MCP client
│   │   ├── servers/
│   │   │   ├── servicenow_server.py   # ServiceNow MCP
│   │   │   ├── rag_server.py          # RAG MCP
│   │   │   ├── github_server.py       # GitHub MCP
│   │   │   ├── k8s_server.py          # Kubernetes MCP
│   │   │   └── terraform_server.py    # Terraform MCP
│   │   └── start_all.py               # Start all servers
│   │
│   ├── rag/                           # Swarm RAG v4.0
│   │   ├── swarm_retriever.py         # Swarm coordinator
│   │   ├── query_understanding.py     # Intent/entity extraction
│   │   ├── agents/                    # RAG agents
│   │   │   ├── vector_agent.py        # Semantic search
│   │   │   ├── keyword_agent.py       # TF-IDF search
│   │   │   ├── graph_agent.py         # Neo4j FIXED_BY
│   │   │   └── metadata_agent.py      # Exact matching
│   │   ├── cross_encoder_reranker.py  # Re-ranking
│   │   ├── smart_chunker.py           # Script chunking
│   │   ├── embedding_service.py       # Embeddings
│   │   └── feedback_optimizer.py      # Weight learning
│   │
│   ├── streaming/                     # Kafka
│   │   ├── kafka_consumer.py          # Consumer
│   │   ├── kafka_producer.py          # Producer
│   │   ├── schemas.py                 # Avro schemas
│   │   └── incident_sources.py        # Multi-source
│   │
│   ├── utils/                         # Utilities
│   │   ├── circuit_breaker.py         # Circuit breaker
│   │   ├── github_actions.py          # GH Actions client
│   │   ├── redis_client.py            # Redis
│   │   └── metrics.py                 # Observability
│   │
│   └── runbooks/                      # Resolution scripts
│       ├── terraform/
│       │   ├── gcp-restart-vm.tf
│       │   ├── gcp-scale-gke.tf
│       │   └── aws-restart-ec2.tf
│       ├── ansible/
│       │   ├── restart-service.yml
│       │   ├── clear-disk-space.yml
│       │   └── rotate-credentials.yml
│       └── shell/
│           ├── restart-pod.sh
│           └── clear-logs.sh
│
├── frontend/                          # Next.js UI
│   ├── app/
│   │   ├── incidents/
│   │   ├── workflows/
│   │   └── approvals/
│   └── components/
│
├── deployment/                        # Docker configs
│   ├── docker-compose.yml
│   ├── kafka/
│   ├── neo4j/
│   └── weaviate/
│
├── .github/workflows/                 # GitHub Actions
│   ├── terraform-apply.yml
│   ├── ansible-run.yml
│   └── ci.yml
│
├── docs/
│   ├── ARCHITECTURE.md
│   ├── ENHANCED_RAG_FEATURES.md
│   ├── PROTOCOL_GUIDE.md
│   └── compliance/
│
├── tests/
│   ├── unit/
│   ├── integration/
│   └── e2e/
│
├── .env.example
├── requirements.txt
└── README.md
```

---

## Configuration

### Environment Variables

```bash
# LLM Configuration
OPENAI_API_KEY=                    # Primary LLM (GPT-4)
ANTHROPIC_API_KEY=                 # Judge LLM (Claude)
LLM_PRIMARY_MODEL=gpt-4-turbo
LLM_JUDGE_MODEL=claude-3-sonnet

# Protocol Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:29092
A2A_MESH_HOST=localhost
A2A_MESH_PORT=9000
MCP_TIMEOUT=30

# Database
DATABASE_URL=postgresql://...
REDIS_URL=redis://...
NEO4J_URI=bolt://...
WEAVIATE_URL=http://...

# External Services
SNOW_INSTANCE_URL=
GITHUB_TOKEN=
GITHUB_OWNER=
GITHUB_REPO=

# Safety
MAX_BLAST_RADIUS=medium            # low, medium, high
AUTO_APPROVE_THRESHOLD=8           # Judge score threshold
CIRCUIT_BREAKER_THRESHOLD=5
CIRCUIT_BREAKER_TIMEOUT=30
```

### RRF Configuration (v5.0)

```python
# backend/rag/hybrid_search_engine.py
from dataclasses import dataclass

@dataclass
class RRFConfig:
    """RRF (Reciprocal Rank Fusion) configuration - weight-free fusion"""
    k: int = 60                      # Industry standard constant
    min_agents_required: int = 2     # Minimum agents for valid consensus
    top_candidates_for_rerank: int = 20  # Send to cross-encoder
    final_results: int = 5           # Return after reranking

# Usage
config = RRFConfig(k=60)
results = hybrid_search_engine.search_rrf(
    query="VM instance test-vm-01 is down",
    enable_rerank=True
)

# Each result includes:
# - rrf_score: Raw RRF fusion score
# - rerank_score: Cross-encoder score
# - agent_ranks: {"vector": 1, "keyword": 3, "graph": 1, "metadata": 2}
# - sources: ["vector", "keyword", "graph", "metadata"]
```

---

## Compliance & Safety

### AI Guardrails (P0)

| Guardrail | Description | Implementation |
|-----------|-------------|----------------|
| Prompt Injection | Detect and block injection attempts | `backend/utils/guardrails.py` |
| PII Redaction | Remove sensitive data from logs | `backend/utils/pii_filter.py` |
| Output Validation | Validate LLM outputs | `backend/utils/output_validator.py` |
| Jailbreak Block | Prevent jailbreak attempts | `backend/utils/guardrails.py` |
| Content Safety | Filter harmful content | `backend/utils/content_filter.py` |
| Hallucination Check | Verify against RAG context | `llm_judge.py` |
| LLM-as-Judge | Independent evaluation | `llm_judge.py` |

### Circuit Breakers

| Service | Threshold | Timeout | Implementation |
|---------|-----------|---------|----------------|
| OpenAI | 5 failures | 30s | `circuit_breaker.py` |
| ServiceNow | 5 failures | 30s | `circuit_breaker.py` |
| GitHub | 3 failures | 60s | `circuit_breaker.py` |
| Neo4j | 5 failures | 30s | `circuit_breaker.py` |
| Weaviate | 5 failures | 30s | `circuit_breaker.py` |

### Observability

| Component | Tool | Purpose |
|-----------|------|---------|
| Logging | Structured JSON | Audit trail |
| Tracing | OpenTelemetry | Request tracing |
| Metrics | Prometheus | Performance |
| LLM Tracking | LangSmith | LLM observability |
| Workflow | LangGraph Studio | Workflow debugging |

### Key Metrics

```python
# backend/utils/metrics.py
METRICS = {
    "mttr_seconds": "Mean time to resolution",
    "auto_resolution_percent": "Auto-resolved incidents",
    "human_override_percent": "Human overrides",
    "token_usage_per_incident": "LLM token usage",
    "agent_success_rate": "Agent execution success",
    "circuit_breaker_trips": "Circuit breaker activations",
    "guardrail_blocks": "Guardrail blocked requests",
    "judge_score_avg": "Average judge score",
    "cost_per_incident": "Cost per incident",
    "slo_budget_remaining": "SLO error budget",
    "swarm_retrieval_accuracy": "RAG accuracy",
    "terraform_apply_rate": "Terraform success rate",
    "ansible_playbook_rate": "Ansible success rate",
    "gh_actions_success": "GitHub Actions success",
    "blast_radius_avoided": "High-risk actions avoided"
}
```

---

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## License

MIT License - see [LICENSE](LICENSE) for details.

---

## Contact

- **Repository**: [https://github.com/sam2881/test_01](https://github.com/sam2881/test_01)
- **Issues**: [GitHub Issues](https://github.com/sam2881/test_01/issues)

---

*Version 5.0.0 - December 2024*
*Hybrid Protocol Architecture: Kafka + A2A + MCP + REST*
