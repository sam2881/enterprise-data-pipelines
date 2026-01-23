# Architecture Guide v5.0

## Hybrid Protocol Architecture

This document provides detailed architecture diagrams for the AI Incident Management Platform v5.0.

---

## 1. Protocol Layer Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              PROTOCOL LAYERS                                     │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  ┌───────────────────────────────────────────────────────────────────────────┐  │
│  │  LAYER 1: KAFKA (External Integration + Audit + State)                    │  │
│  │  ═══════════════════════════════════════════════════                      │  │
│  │                                                                            │  │
│  │  USE FOR:                                                                  │  │
│  │  • External system integration (ServiceNow, Jira, PagerDuty)             │  │
│  │  • State transitions (incident.created → approved → executed)            │  │
│  │  • Audit trail (compliance, replay capability)                           │  │
│  │  • Cross-system events                                                    │  │
│  │                                                                            │  │
│  │  TOPICS:                                                                   │  │
│  │  ┌────────────────┐ ┌────────────────┐ ┌────────────────┐                │  │
│  │  │incident.created│ │ plan.generated │ │incident.approved│                │  │
│  │  └────────────────┘ └────────────────┘ └────────────────┘                │  │
│  │  ┌────────────────┐ ┌────────────────┐ ┌────────────────┐                │  │
│  │  │incident.executed│ │incident.verified│ │ incident.closed│               │  │
│  │  └────────────────┘ └────────────────┘ └────────────────┘                │  │
│  │                                                                            │  │
│  │  PROTOCOL: Avro/Protobuf schemas, Schema Registry                        │  │
│  │  GUARANTEES: At-least-once delivery, ordering per partition              │  │
│  └───────────────────────────────────────────────────────────────────────────┘  │
│                                                                                  │
│  ┌───────────────────────────────────────────────────────────────────────────┐  │
│  │  LAYER 2: A2A (Agent-to-Agent Real-time Communication)                    │  │
│  │  ═══════════════════════════════════════════════════════                  │  │
│  │                                                                            │  │
│  │  USE FOR:                                                                  │  │
│  │  • Agent-to-Agent coordination                                            │  │
│  │  • Swarm RAG consensus                                                    │  │
│  │  • LLM-as-Judge evaluation requests                                       │  │
│  │  • Real-time messaging                                                    │  │
│  │  • Agent discovery and capability negotiation                             │  │
│  │                                                                            │  │
│  │  MESSAGES:                                                                 │  │
│  │  ┌────────────────┐ ┌────────────────┐ ┌────────────────┐                │  │
│  │  │  swarm.query   │ │   swarm.vote   │ │ judge.evaluate │                │  │
│  │  └────────────────┘ └────────────────┘ └────────────────┘                │  │
│  │  ┌────────────────┐ ┌────────────────┐ ┌────────────────┐                │  │
│  │  │  judge.score   │ │agent.coordinate│ │agent.capability│                │  │
│  │  └────────────────┘ └────────────────┘ └────────────────┘                │  │
│  │                                                                            │  │
│  │  PROTOCOL: A2A (Google), JSON-RPC 2.0                                    │  │
│  │  GUARANTEES: Best-effort, real-time                                      │  │
│  └───────────────────────────────────────────────────────────────────────────┘  │
│                                                                                  │
│  ┌───────────────────────────────────────────────────────────────────────────┐  │
│  │  LAYER 3: MCP (Agent-to-Tool Invocation)                                  │  │
│  │  ═══════════════════════════════════════                                  │  │
│  │                                                                            │  │
│  │  USE FOR:                                                                  │  │
│  │  • Agent → Tool invocation                                                │  │
│  │  • RAG search and update                                                  │  │
│  │  • ServiceNow operations                                                  │  │
│  │  • GitHub operations                                                      │  │
│  │  • Kubernetes/Terraform tools                                             │  │
│  │                                                                            │  │
│  │  SERVERS:                                                                  │  │
│  │  ┌────────────────┐ ┌────────────────┐ ┌────────────────┐                │  │
│  │  │servicenow-mcp  │ │    rag-mcp     │ │   github-mcp   │                │  │
│  │  └────────────────┘ └────────────────┘ └────────────────┘                │  │
│  │  ┌────────────────┐ ┌────────────────┐                                   │  │
│  │  │    k8s-mcp     │ │ terraform-mcp  │                                   │  │
│  │  └────────────────┘ └────────────────┘                                   │  │
│  │                                                                            │  │
│  │  PROTOCOL: MCP (Anthropic), JSON-RPC 2.0 over stdio                      │  │
│  │  GUARANTEES: Request-response, tool schemas                              │  │
│  └───────────────────────────────────────────────────────────────────────────┘  │
│                                                                                  │
│  ┌───────────────────────────────────────────────────────────────────────────┐  │
│  │  LAYER 4: REST/Webhooks (External APIs)                                   │  │
│  │  ═════════════════════════════════════                                    │  │
│  │                                                                            │  │
│  │  USE FOR:                                                                  │  │
│  │  • GitHub Actions (workflow_dispatch)                                     │  │
│  │  • Jira integration                                                       │  │
│  │  • Slack/Teams notifications                                              │  │
│  │  • PagerDuty escalations                                                  │  │
│  │  • Webhook callbacks                                                      │  │
│  │                                                                            │  │
│  │  ENDPOINTS:                                                                │  │
│  │  • POST /repos/{owner}/{repo}/actions/workflows/{id}/dispatches          │  │
│  │  • POST /rest/api/3/issue (Jira)                                         │  │
│  │  • POST /api/chat.postMessage (Slack)                                    │  │
│  │                                                                            │  │
│  │  PROTOCOL: HTTP/HTTPS, JSON                                              │  │
│  │  GUARANTEES: Request-response, retries                                   │  │
│  └───────────────────────────────────────────────────────────────────────────┘  │
│                                                                                  │
│  ┌───────────────────────────────────────────────────────────────────────────┐  │
│  │  LAYER 5: Direct SDK (Internal Low-Latency)                               │  │
│  │  ══════════════════════════════════════                                   │  │
│  │                                                                            │  │
│  │  USE FOR:                                                                  │  │
│  │  • Redis cache (~1ms)                                                     │  │
│  │  • PostgreSQL state (~10ms)                                               │  │
│  │  • In-process calls                                                       │  │
│  │                                                                            │  │
│  │  NO PROTOCOL OVERHEAD - Direct library calls                             │  │
│  └───────────────────────────────────────────────────────────────────────────┘  │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Component Interaction Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                      COMPONENT INTERACTION (by Protocol)                         │
└─────────────────────────────────────────────────────────────────────────────────┘

                              EXTERNAL SYSTEMS
    ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐
    │ServiceNow│  │   GCP   │  │ Datadog │  │ GitHub  │  │  Slack  │
    └────┬────┘  └────┬────┘  └────┬────┘  └────┬────┘  └────┬────┘
         │            │            │            │            │
         │ (MCP)      │ (Kafka)    │ (Kafka)    │ (REST)     │ (REST)
         ▼            ▼            ▼            ▲            ▲
┌─────────────────────────────────────────────────────────────────────────────────┐
│                                                                                  │
│    ┌─────────────────────────────────────────────────────────────────────────┐  │
│    │                     KAFKA EVENT BUS                                      │  │
│    │   [incident.created] [plan.generated] [incident.approved] [...]         │  │
│    └─────────────────────────────────────────────────────────────────────────┘  │
│         │ (consume)                                          │ (publish)        │
│         ▼                                                    ▲                  │
│    ┌─────────────────┐                              ┌─────────────────┐        │
│    │Event Orchestrator│                              │  Kafka Producer │        │
│    │     (HUB)        │                              │   (all zones)   │        │
│    └────────┬─────────┘                              └─────────────────┘        │
│             │ (A2A)                                                             │
│    ┌────────┴────────────────────────────────────────────────────────┐         │
│    │                      A2A AGENT MESH                              │         │
│    └────────┬────────────────┬─────────────────┬─────────────────────┘         │
│             │                │                 │                                │
│             ▼                ▼                 ▼                                │
│    ┌─────────────────┐ ┌─────────────┐ ┌─────────────────┐                     │
│    │    Decision     │ │  LLM-as-    │ │   Execution     │                     │
│    │  Orchestrator   │ │   Judge     │ │  Orchestrator   │                     │
│    │   (MCP Host)    │ │   (A2A)     │ │  (Hierarchical) │                     │
│    └────────┬────────┘ └─────────────┘ └────────┬────────┘                     │
│             │ (MCP)                              │ (A2A)                        │
│    ┌────────┴────────────────────────┐          │                              │
│    │                                 │          ▼                              │
│    ▼                                 ▼    ┌─────────────────────────────────┐  │
│  ┌──────────────┐             ┌──────────┐│    Terraform │ Ansible │ Code  │  │
│  │ SWARM RAG    │             │  Memory  ││      Agent   │  Agent  │ Agent │  │
│  │  MCP Server  │             │  (SDK)   │└──────┬───────┴────┬────┴───┬───┘  │
│  │              │             │          │       │            │        │       │
│  │ ┌──────────┐ │             │ ┌──────┐ │       │ (MCP)      │ (MCP)  │(MCP) │
│  │ │ Vector DB│ │             │ │Redis │ │       ▼            ▼        ▼       │
│  │ │(Weaviate)│ │             │ └──────┘ │  ┌─────────┐  ┌─────────┐  ┌─────┐ │
│  │ └──────────┘ │             │ ┌──────┐ │  │   K8s   │  │ Config  │  │GitHub│ │
│  │ ┌──────────┐ │             │ │  PG  │ │  │   MCP   │  │   MCP   │  │ MCP │ │
│  │ │ Graph DB │ │             │ └──────┘ │  └────┬────┘  └────┬────┘  └──┬──┘ │
│  │ │ (Neo4j)  │ │             └──────────┘       │            │         │     │
│  │ └──────────┘ │                                │ (REST)     │ (REST)  │(REST│
│  └──────────────┘                                ▼            ▼         ▼     │
│                                            ┌─────────────────────────────────┐ │
│                                            │      GITHUB ACTIONS             │ │
│                                            │  terraform-apply.yml            │ │
│                                            │  ansible-run.yml                │ │
│                                            └─────────────────────────────────┘ │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Swarm RAG Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         SWARM RAG ARCHITECTURE                                   │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│   ┌─────────────────────────────────────────────────────────────────────────┐   │
│   │                    DECISION ORCHESTRATOR (MCP Host)                      │   │
│   │                                                                          │   │
│   │   1. Receive incident from Event Orchestrator (A2A)                     │   │
│   │   2. Call Swarm RAG for script retrieval                                │   │
│   │   3. Send to LLM for plan generation                                    │   │
│   │   4. Request Judge evaluation (A2A)                                     │   │
│   └───────────────────────────────────┬─────────────────────────────────────┘   │
│                                       │                                          │
│                                       │ A2A: swarm.query                        │
│                                       ▼                                          │
│   ┌─────────────────────────────────────────────────────────────────────────┐   │
│   │                    SWARM COORDINATOR (A2A Server)                        │   │
│   │                                                                          │   │
│   │   Protocol: A2A for coordination, MCP for tool calls                    │   │
│   │                                                                          │   │
│   │   ┌─────────────────────────────────────────────────────────────────┐   │   │
│   │   │                    QUERY UNDERSTANDING                           │   │   │
│   │   │                                                                  │   │   │
│   │   │   Input: "VM instance test-vm-01 is down in us-central1-a"      │   │   │
│   │   │                                                                  │   │   │
│   │   │   Output:                                                        │   │   │
│   │   │   - intent: RESTART                                              │   │   │
│   │   │   - entities: {instance: "test-vm-01", zone: "us-central1-a"}   │   │   │
│   │   │   - service: GCP                                                 │   │   │
│   │   │   - keywords: ["vm", "down", "restart", "gcp", "compute"]       │   │   │
│   │   │   - expanded: "VM down restart recover fix compute GCP"         │   │   │
│   │   └─────────────────────────────────────────────────────────────────┘   │   │
│   │                                       │                                  │   │
│   │                                       ▼                                  │   │
│   │   ┌─────────────────────────────────────────────────────────────────┐   │   │
│   │   │              PARALLEL AGENT SEARCH (A2A Broadcast)               │   │   │
│   │   │                                                                  │   │   │
│   │   │   ┌────────────┐  ┌────────────┐  ┌────────────┐  ┌──────────┐ │   │   │
│   │   │   │   VECTOR   │  │  KEYWORD   │  │   GRAPH    │  │ METADATA │ │   │   │
│   │   │   │   AGENT    │  │   AGENT    │  │   AGENT    │  │  AGENT   │ │   │   │
│   │   │   │            │  │            │  │            │  │          │ │   │   │
│   │   │   │ Weight:0.40│  │ Weight:0.25│  │ Weight:0.25│  │Weight:0.10│   │   │
│   │   │   │            │  │            │  │            │  │          │ │   │   │
│   │   │   │ all-MiniLM │  │  TF-IDF    │  │  FIXED_BY  │  │  Exact   │ │   │   │
│   │   │   │ Semantic   │  │  Bigrams   │  │   Neo4j    │  │  Match   │ │   │   │
│   │   │   │            │  │            │  │            │  │          │ │   │   │
│   │   │   │ MCP:       │  │ Internal   │  │ MCP:       │  │ Internal │ │   │   │
│   │   │   │ Weaviate   │  │            │  │ Neo4j      │  │          │ │   │   │
│   │   │   └─────┬──────┘  └─────┬──────┘  └─────┬──────┘  └────┬─────┘ │   │   │
│   │   │         │               │               │               │       │   │   │
│   │   │         │    A2A: swarm.vote           │               │       │   │   │
│   │   │         └───────────────┼───────────────┼───────────────┘       │   │   │
│   │   │                         ▼               ▼                       │   │   │
│   │   └─────────────────────────────────────────────────────────────────┘   │   │
│   │                                       │                                  │   │
│   │                                       ▼                                  │   │
│   │   ┌─────────────────────────────────────────────────────────────────┐   │   │
│   │   │                    CONSENSUS ALGORITHM                           │   │   │
│   │   │                                                                  │   │   │
│   │   │   For each script:                                               │   │   │
│   │   │     score = Σ(agent_weight × vote_confidence × historical_acc)  │   │   │
│   │   │                                                                  │   │   │
│   │   │   Rank by score, select top candidates                          │   │   │
│   │   └─────────────────────────────────────────────────────────────────┘   │   │
│   │                                       │                                  │   │
│   │                                       ▼                                  │   │
│   │   ┌─────────────────────────────────────────────────────────────────┐   │   │
│   │   │                    CROSS-ENCODER RERANKING                       │   │   │
│   │   │                                                                  │   │   │
│   │   │   Model: ms-marco-MiniLM-L-6-v2                                 │   │   │
│   │   │   Improvement: +20-30% precision                                 │   │   │
│   │   │                                                                  │   │   │
│   │   │   Input: Top 20 candidates                                       │   │   │
│   │   │   Output: Top 5 reranked                                        │   │   │
│   │   └─────────────────────────────────────────────────────────────────┘   │   │
│   │                                       │                                  │   │
│   │                                       ▼                                  │   │
│   │   ┌─────────────────────────────────────────────────────────────────┐   │   │
│   │   │                    BLAST RADIUS FILTER                           │   │   │
│   │   │                                                                  │   │   │
│   │   │   For each script:                                               │   │   │
│   │   │     risk = assess_blast_radius(script, context)                 │   │   │
│   │   │     if risk.level == "critical":                                │   │   │
│   │   │       filter_out(script)                                        │   │   │
│   │   │     else:                                                        │   │   │
│   │   │       attach_risk_assessment(script, risk)                      │   │   │
│   │   └─────────────────────────────────────────────────────────────────┘   │   │
│   │                                       │                                  │   │
│   └───────────────────────────────────────┼─────────────────────────────────┘   │
│                                           │                                      │
│                                           ▼                                      │
│   ┌─────────────────────────────────────────────────────────────────────────┐   │
│   │                         FINAL RESULTS                                    │   │
│   │                                                                          │   │
│   │   1. terraform-gcp-restart-vm.tf    (score: 0.92, risk: low)            │   │
│   │   2. ansible-gcp-vm-recovery.yml    (score: 0.87, risk: low)            │   │
│   │   3. shell-gcp-instance-restart.sh  (score: 0.81, risk: medium)         │   │
│   │                                                                          │   │
│   │   Each result includes:                                                  │   │
│   │   - final_score, vector_score, keyword_score, graph_score, metadata_score│   │
│   │   - match_reasons, risk_level, historical_success_count                 │   │
│   │   - requires_approval, avg_resolution_time                              │   │
│   └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Execution Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         EXECUTION FLOW (Terraform/Ansible)                       │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│   ┌─────────────────────────────────────────────────────────────────────────┐   │
│   │  STEP 1: Receive Approved Plan (Kafka consume)                          │   │
│   │                                                                          │   │
│   │  Execution Orchestrator consumes from incident.approved topic           │   │
│   └───────────────────────────────────────────┬─────────────────────────────┘   │
│                                               │                                  │
│                                               ▼                                  │
│   ┌─────────────────────────────────────────────────────────────────────────┐   │
│   │  STEP 2: Validate Plan (No LLM - Deterministic)                         │   │
│   │                                                                          │   │
│   │  - Check script in allowlist                                            │   │
│   │  - Verify parameters match schema                                       │   │
│   │  - Confirm environment authorization                                    │   │
│   └───────────────────────────────────────────┬─────────────────────────────┘   │
│                                               │                                  │
│                                               ▼                                  │
│   ┌─────────────────────────────────────────────────────────────────────────┐   │
│   │  STEP 3: Select Agent (A2A coordination)                                │   │
│   │                                                                          │   │
│   │  Based on script_type:                                                  │   │
│   │  - terraform → TerraformAgent                                          │   │
│   │  - ansible → AnsibleAgent                                               │   │
│   │  - shell → CodeAgent                                                    │   │
│   └───────────────────────────────────────────┬─────────────────────────────┘   │
│                                               │                                  │
│                                               ▼                                  │
│   ┌─────────────────────────────────────────────────────────────────────────┐   │
│   │  STEP 4: Generate Rollback Plan                                         │   │
│   │                                                                          │   │
│   │  TerraformAgent:                                                        │   │
│   │    - Parse .tf file for resources                                       │   │
│   │    - Generate inverse operations                                        │   │
│   │    - Store state backup                                                 │   │
│   │                                                                          │   │
│   │  AnsibleAgent:                                                          │   │
│   │    - Parse playbook for tasks                                           │   │
│   │    - Generate rollback playbook                                         │   │
│   │    - Store configuration backup                                         │   │
│   └───────────────────────────────────────────┬─────────────────────────────┘   │
│                                               │                                  │
│                                               ▼                                  │
│   ┌─────────────────────────────────────────────────────────────────────────┐   │
│   │  STEP 5: Trigger GitHub Actions (REST)                                  │   │
│   │                                                                          │   │
│   │  POST /repos/{owner}/{repo}/actions/workflows/{workflow}/dispatches    │   │
│   │  {                                                                       │   │
│   │    "ref": "main",                                                        │   │
│   │    "inputs": {                                                           │   │
│   │      "script_path": "runbooks/terraform/gcp-restart-vm.tf",            │   │
│   │      "environment": "production",                                        │   │
│   │      "incident_id": "INC001234",                                        │   │
│   │      "dry_run": "false",                                                 │   │
│   │      "rollback_script": "runbooks/terraform/gcp-restart-vm-rollback.tf"│   │
│   │    }                                                                     │   │
│   │  }                                                                       │   │
│   └───────────────────────────────────────────┬─────────────────────────────┘   │
│                                               │                                  │
│                                               ▼                                  │
│   ┌─────────────────────────────────────────────────────────────────────────┐   │
│   │  STEP 6: Poll for Completion (REST)                                     │   │
│   │                                                                          │   │
│   │  GET /repos/{owner}/{repo}/actions/runs/{run_id}                       │   │
│   │                                                                          │   │
│   │  Poll every 10s until:                                                  │   │
│   │  - status == "completed"                                                │   │
│   │  - OR timeout (600s)                                                    │   │
│   └───────────────────────────────────────────┬─────────────────────────────┘   │
│                                               │                                  │
│                                               ▼                                  │
│   ┌─────────────────────────────────────────────────────────────────────────┐   │
│   │  STEP 7: Handle Result                                                  │   │
│   │                                                                          │   │
│   │  IF conclusion == "success":                                            │   │
│   │    → Publish to incident.executed (Kafka)                              │   │
│   │    → Proceed to Verification                                            │   │
│   │                                                                          │   │
│   │  IF conclusion == "failure":                                            │   │
│   │    → Trigger rollback workflow                                          │   │
│   │    → Publish to exec.failed (Kafka)                                    │   │
│   │    → Escalate to human                                                  │   │
│   └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 5. LLM-as-Judge Flow

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         LLM-AS-JUDGE EVALUATION FLOW                             │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│   ┌─────────────────────────────────────────────────────────────────────────┐   │
│   │  PRIMARY LLM (GPT-4)                                                     │   │
│   │                                                                          │   │
│   │  Generates execution plan with:                                         │   │
│   │  - Selected script from Swarm RAG                                       │   │
│   │  - Filled parameters                                                    │   │
│   │  - Risk assessment                                                       │   │
│   │  - Rollback plan                                                        │   │
│   └───────────────────────────────────────────┬─────────────────────────────┘   │
│                                               │                                  │
│                                               │ A2A: judge.evaluate             │
│                                               ▼                                  │
│   ┌─────────────────────────────────────────────────────────────────────────┐   │
│   │  LLM-AS-JUDGE (Claude - Different Model Family)                         │   │
│   │                                                                          │   │
│   │  Why different model:                                                   │   │
│   │  - Avoids shared biases                                                 │   │
│   │  - Independent perspective                                              │   │
│   │  - Prevents confirmation bias                                           │   │
│   │                                                                          │   │
│   │  ┌─────────────────────────────────────────────────────────────────┐   │   │
│   │  │  PARALLEL EVALUATIONS                                            │   │   │
│   │  │                                                                  │   │   │
│   │  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐             │   │   │
│   │  │  │   QUALITY   │  │   SAFETY    │  │  FACTUAL    │             │   │   │
│   │  │  │   (1-10)    │  │ (Pass/Fail) │  │   (1-10)    │             │   │   │
│   │  │  │             │  │             │  │             │             │   │   │
│   │  │  │ - Structure │  │ - Guardrails│  │ - RAG match │             │   │   │
│   │  │  │ - Complete  │  │ - No harmful│  │ - No halluc │             │   │   │
│   │  │  │ - Logical   │  │ - Blast rad │  │ - Verified  │             │   │   │
│   │  │  └─────────────┘  └─────────────┘  └─────────────┘             │   │   │
│   │  │                                                                  │   │   │
│   │  │  ┌─────────────┐  ┌─────────────┐                              │   │   │
│   │  │  │ FEASIBILITY │  │    RISK     │                              │   │   │
│   │  │  │   (1-10)    │  │ (Low/Med/Hi)│                              │   │   │
│   │  │  │             │  │             │                              │   │   │
│   │  │  │ - Executable│  │ - Blast rad │                              │   │   │
│   │  │  │ - Resources │  │ - Rollback  │                              │   │   │
│   │  │  │ - Timeout   │  │ - Impact    │                              │   │   │
│   │  │  └─────────────┘  └─────────────┘                              │   │   │
│   │  └─────────────────────────────────────────────────────────────────┘   │   │
│   │                                                                          │   │
│   └───────────────────────────────────────────┬─────────────────────────────┘   │
│                                               │                                  │
│                                               │ A2A: judge.score                │
│                                               ▼                                  │
│   ┌─────────────────────────────────────────────────────────────────────────┐   │
│   │  ROUTING DECISION                                                        │   │
│   │                                                                          │   │
│   │  IF safety_passed == False:                                             │   │
│   │    → REJECTED (immediate)                                               │   │
│   │                                                                          │   │
│   │  ELSE IF quality_score < 6:                                             │   │
│   │    → NEEDS_REVISION (loop back to LLM)                                  │   │
│   │                                                                          │   │
│   │  ELSE:                                                                   │   │
│   │    → APPROVED (send to Control Plane)                                   │   │
│   │    → Include judge_score in plan                                        │   │
│   │    → Control Plane uses score for auto-approve threshold                │   │
│   └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 6. Feedback Learning Loop

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         FEEDBACK LEARNING LOOP                                   │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│   ┌─────────────────────────────────────────────────────────────────────────┐   │
│   │  VERIFICATION ENGINE                                                     │   │
│   │                                                                          │   │
│   │  1. Health checks on affected service                                   │   │
│   │  2. Metric validation (error rates, latency)                           │   │
│   │  3. Regression tests                                                     │   │
│   │  4. Judge validation of outcome (A2A)                                   │   │
│   └───────────────────────────────────────────┬─────────────────────────────┘   │
│                                               │                                  │
│                           ┌───────────────────┴───────────────────┐             │
│                           ▼                                       ▼             │
│   ┌─────────────────────────────────────┐   ┌─────────────────────────────────┐│
│   │  SUCCESS PATH                        │   │  FAILURE PATH                   ││
│   │                                      │   │                                 ││
│   │  → Publish incident.verified         │   │  → Trigger rollback            ││
│   │  → Proceed to Learning Engine        │   │  → Publish verification.failed ││
│   │                                      │   │  → Escalate to human           ││
│   └───────────────────────────────────────┘   └─────────────────────────────────┘│
│                           │                                                      │
│                           ▼                                                      │
│   ┌─────────────────────────────────────────────────────────────────────────┐   │
│   │  LEARNING ENGINE (Only on SUCCESS)                                       │   │
│   │                                                                          │   │
│   │  🚫 NEVER update RAG on failure - prevents learning bad patterns        │   │
│   │                                                                          │   │
│   │  Updates via MCP to RAG:                                                │   │
│   │  1. Index successful script                                             │   │
│   │  2. Create/update FIXED_BY relationship in Neo4j                       │   │
│   │  3. Boost ranking score for this script                                │   │
│   │  4. Record metadata:                                                    │   │
│   │     - success: true                                                     │   │
│   │     - script_id                                                         │   │
│   │     - incident_type                                                     │   │
│   │     - resolution_time                                                   │   │
│   │     - confidence_delta                                                  │   │
│   │     - human_override (if any)                                          │   │
│   │     - token_cost                                                        │   │
│   └───────────────────────────────────────────┬─────────────────────────────┘   │
│                                               │                                  │
│                                               ▼                                  │
│   ┌─────────────────────────────────────────────────────────────────────────┐   │
│   │  NEO4J GRAPH UPDATE                                                      │   │
│   │                                                                          │   │
│   │  CREATE (i:Incident {id: $inc_id, type: $type})                        │   │
│   │  CREATE (s:Script {id: $script_id})                                    │   │
│   │  CREATE (i)-[:FIXED_BY {                                               │   │
│   │    success: true,                                                       │   │
│   │    resolution_time: $time,                                              │   │
│   │    timestamp: datetime()                                                │   │
│   │  }]->(s)                                                                │   │
│   │                                                                          │   │
│   │  // Update success rate                                                 │   │
│   │  MATCH (s:Script {id: $script_id})                                     │   │
│   │  SET s.success_count = s.success_count + 1,                            │   │
│   │      s.avg_resolution_time = (s.avg_resolution_time * s.total_count   │   │
│   │                                + $time) / (s.total_count + 1)          │   │
│   └─────────────────────────────────────────────────────────────────────────┘   │
│                                               │                                  │
│                                               ▼                                  │
│   ┌─────────────────────────────────────────────────────────────────────────┐   │
│   │  WEIGHT OPTIMIZATION                                                     │   │
│   │                                                                          │   │
│   │  Feedback Optimizer adjusts swarm weights based on outcomes:            │   │
│   │                                                                          │   │
│   │  For incident_type "infrastructure":                                    │   │
│   │    IF vector_agent recommended winning script:                          │   │
│   │      vector_weight += 0.01                                              │   │
│   │    IF graph_agent recommended winning script:                           │   │
│   │      graph_weight += 0.01                                               │   │
│   │    Normalize weights to sum to 1.0                                      │   │
│   │                                                                          │   │
│   │  Stored per incident_type for future searches                          │   │
│   └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 7. Circuit Breaker States

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         CIRCUIT BREAKER PATTERN                                  │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│   ┌─────────────────────────────────────────────────────────────────────────┐   │
│   │                                                                          │   │
│   │    ┌──────────────┐                         ┌──────────────┐            │   │
│   │    │              │                         │              │            │   │
│   │    │    CLOSED    │─────────────────────────│    OPEN      │            │   │
│   │    │   (Normal)   │     5 consecutive       │   (Block)    │            │   │
│   │    │              │        failures         │              │            │   │
│   │    └──────────────┘                         └──────────────┘            │   │
│   │           ▲                                        │                    │   │
│   │           │                                        │                    │   │
│   │           │                                        │ 30s timeout        │   │
│   │           │                                        │                    │   │
│   │           │                                        ▼                    │   │
│   │           │                                 ┌──────────────┐            │   │
│   │           │                                 │              │            │   │
│   │           │           success               │  HALF-OPEN   │            │   │
│   │           └─────────────────────────────────│   (Test)     │            │   │
│   │                                             │              │            │   │
│   │                        failure              └──────────────┘            │   │
│   │                        ─────────────────────────────│                   │   │
│   │                                                     │                   │   │
│   │                                                     ▼                   │   │
│   │                                             ┌──────────────┐            │   │
│   │                                             │    OPEN      │            │   │
│   │                                             │  (restart    │            │   │
│   │                                             │   timeout)   │            │   │
│   │                                             └──────────────┘            │   │
│   │                                                                          │   │
│   └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│   PROTECTED SERVICES:                                                           │
│   ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐          │
│   │   OpenAI    │  │ ServiceNow  │  │   GitHub    │  │   Neo4j     │          │
│   │ thresh: 5   │  │ thresh: 5   │  │ thresh: 3   │  │ thresh: 5   │          │
│   │ timeout: 30s│  │ timeout: 30s│  │ timeout: 60s│  │ timeout: 30s│          │
│   └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘          │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

*Document Version: 5.0.0*
*Last Updated: December 2024*
