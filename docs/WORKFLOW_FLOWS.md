# Workflow Flows - v6.0 Event-Driven Architecture

## Overview

This document provides ASCII diagrams for both workflow systems in the Enterprise Agentic Platform.

---

## 1. Incident Management Workflow

### High-Level Flow

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        INCIDENT MANAGEMENT FLOW                                  │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  ┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐   │
│  │  ServiceNow │────▶│    Kafka    │────▶│ LangGraph   │────▶│  GitHub     │   │
│  │     MCP     │     │   Events    │     │  Workflow   │     │  Actions    │   │
│  └─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘   │
│        │                    │                   │                   │           │
│        ▼                    ▼                   ▼                   ▼           │
│  ┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐   │
│  │    Poll     │     │   System    │     │    12       │     │  Execute    │   │
│  │  Incidents  │     │  of Record  │     │   Nodes     │     │   Scripts   │   │
│  └─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### LangGraph Node Sequence

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        LANGGRAPH 12-NODE WORKFLOW                                │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  START                                                                           │
│    │                                                                             │
│    ▼                                                                             │
│  ┌─────────────┐                                                                 │
│  │   ingest    │──▶ Kafka: incident.received                                    │
│  └─────────────┘                                                                 │
│         │                                                                        │
│         ▼                                                                        │
│  ┌─────────────┐                                                                 │
│  │    parse    │  Extract structured data from raw incident                     │
│  └─────────────┘                                                                 │
│         │                                                                        │
│         ▼                                                                        │
│  ┌─────────────┐                                                                 │
│  │  classify   │──▶ Kafka: incident.enriched                                    │
│  └─────────────┘  LLM classifies incident type (database, network, k8s, etc.)  │
│         │                                                                        │
│         ▼                                                                        │
│  ┌─────────────┐                                                                 │
│  │ swarm_rag   │  4 RAG agents vote on best runbook (RRF fusion)               │
│  └─────────────┘                                                                 │
│         │                                                                        │
│         ▼                                                                        │
│  ┌─────────────┐                                                                 │
│  │generate_plan│──▶ Kafka: incident.plan_generated                              │
│  └─────────────┘  LLM generates remediation plan with steps                     │
│         │                                                                        │
│         ▼                                                                        │
│  ┌─────────────┐       ┌─────────────┐                                          │
│  │   judge     │──────▶│generate_plan│  Retry loop (max 2)                      │
│  │ evaluation  │ FAIL  └─────────────┘                                          │
│  └─────────────┘                                                                 │
│         │ PASS                                                                   │
│         ▼                                                                        │
│  ┌─────────────┐                                                                 │
│  │control_plane│  Policy engine determines approval route                       │
│  └─────────────┘                                                                 │
│         │                                                                        │
│    ┌────┴────┐                                                                   │
│    ▼         ▼                                                                   │
│  ┌─────────────┐  ┌─────────────┐                                               │
│  │auto_approve │  │await_approval│──▶ Kafka: incident.requires_approval         │
│  └─────────────┘  └─────────────┘    [WORKFLOW PAUSES]                          │
│         │               │                                                        │
│         │               │◀───── Kafka: incident.approved (from UI/Slack)        │
│         │               ▼                                                        │
│         │        ┌─────────────┐                                                 │
│         │        │process_     │                                                 │
│         │        │approval     │                                                 │
│         │        └─────────────┘                                                 │
│         │               │                                                        │
│         └───────┬───────┘                                                        │
│                 ▼                                                                 │
│          ┌─────────────┐                                                         │
│          │   execute   │──▶ Kafka: remediation.started / remediation.executed   │
│          └─────────────┘  Trigger GitHub Actions workflow                       │
│                 │                                                                 │
│                 ▼                                                                 │
│          ┌─────────────┐                                                         │
│          │   verify    │──▶ Kafka: incident.verified                            │
│          └─────────────┘  Verify fix was successful                             │
│                 │                                                                 │
│                 ▼                                                                 │
│          ┌─────────────┐                                                         │
│          │close_ticket │──▶ Kafka: incident.close_execute                       │
│          └─────────────┘  [MCP consumes and closes ServiceNow ticket]           │
│                 │                                                                 │
│                 ▼                                                                 │
│          ┌─────────────┐                                                         │
│          │feedback_loop│──▶ Kafka: incident.closed                              │
│          └─────────────┘  Update Neo4j, feedback optimizer                      │
│                 │                                                                 │
│                 ▼                                                                 │
│               END                                                                │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Event Flow Sequence

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        EVENT FLOW TIMELINE                                       │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Time ──────────────────────────────────────────────────────────────────────▶   │
│                                                                                  │
│  ServiceNow MCP:                                                                 │
│    │                                                                             │
│    └──▶ incident.created ─────────────────────────────────────────────────────▶ │
│                                                                                  │
│  LangGraph:                                                                      │
│              │                                                                   │
│              └──▶ incident.received ─▶ incident.enriched ─▶ plan_generated ──▶  │
│                                                                                  │
│                                         │                                        │
│                                         └──▶ incident.requires_approval ──────▶ │
│                                                                                  │
│  FastAPI (Human):                        [PAUSE]                                 │
│                                                │                                 │
│                                                └──▶ incident.approved ─────────▶│
│                                                                                  │
│  LangGraph (resumed):                                                            │
│                                                           │                      │
│              └──▶ remediation.started ─▶ remediation.executed ─▶ verified ────▶ │
│                                                                                  │
│                                                                          │       │
│                                                                          └──▶    │
│                                                                 incident.close_execute
│  ServiceNow MCP:                                                                 │
│                                                                              │   │
│                                                                              └──▶│
│                                                                    incident.closed
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Data Pipeline Workflow

### High-Level Flow

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        DATA PIPELINE FLOW                                        │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  ┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐   │
│  │  Jira MCP   │────▶│    Kafka    │────▶│ Data Agent  │────▶│   Airflow   │   │
│  │  (or UI)    │     │   Events    │     │  LangGraph  │     │   Deploy    │   │
│  └─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘   │
│        │                    │                   │                   │           │
│        ▼                    ▼                   ▼                   ▼           │
│  ┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐   │
│  │ Poll Jira   │     │   System    │     │   5 Agent   │     │   DAG +     │   │
│  │  Tickets    │     │  of Record  │     │   Swarm     │     │ Spark Jobs  │   │
│  └─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Data Agent Node Sequence

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        DATA AGENT 5-AGENT WORKFLOW                               │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  START                                                                           │
│    │                                                                             │
│    ▼                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                         SUPERVISOR AGENT                                 │    │
│  │  Coordinates workflow, handles errors, routes to specialists             │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
│         │                                                                        │
│         ▼                                                                        │
│  ┌─────────────┐                                                                 │
│  │  PLANNER    │──▶ Kafka: pipeline.planned                                     │
│  │   AGENT     │  Determines: create/modify/upgrade/no_change                   │
│  └─────────────┘  Selects templates, identifies schema changes                  │
│         │                                                                        │
│         ▼                                                                        │
│  ┌─────────────┐                                                                 │
│  │ GENERATOR   │──▶ Kafka: pipeline.generated                                   │
│  │   AGENT     │  Renders Jinja2 templates: DAG + Spark jobs                    │
│  └─────────────┘  Creates BigQuery schema DDL                                   │
│         │                                                                        │
│         ▼                                                                        │
│  ┌─────────────┐       ┌─────────────┐                                          │
│  │ VALIDATOR   │──────▶│ GENERATOR   │  Retry loop (fix errors)                 │
│  │   AGENT     │ FAIL  └─────────────┘                                          │
│  └─────────────┘──▶ Kafka: pipeline.validated                                   │
│         │ PASS      Syntax check, business rules, dry run                       │
│         ▼                                                                        │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                         APPROVAL CHECK                                   │    │
│  │  if environment == "PROD" or schema_change:                              │    │
│  │      publish: pipeline.requires_approval                                 │    │
│  │      [WORKFLOW PAUSES]                                                   │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
│         │                                                                        │
│         │◀───── Kafka: pipeline.approved (from UI)                              │
│         ▼                                                                        │
│  ┌─────────────┐                                                                 │
│  │ DEPLOYER    │──▶ Kafka: pipeline.deploy_execute                              │
│  │   AGENT     │  Creates PR, triggers CI/CD                                    │
│  └─────────────┘  [Airflow MCP consumes and deploys]                            │
│         │                                                                        │
│         ▼                                                                        │
│       END ──▶ Kafka: pipeline.deployed                                          │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Template Generation Flow

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        TEMPLATE GENERATION                                       │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Intent JSON                                                                     │
│      │                                                                           │
│      ▼                                                                           │
│  ┌─────────────┐     ┌─────────────┐     ┌─────────────┐                        │
│  │ Pipeline    │────▶│  Source     │────▶│  Schema     │                        │
│  │ Identity    │     │  Config     │     │ Definition  │                        │
│  └─────────────┘     └─────────────┘     └─────────────┘                        │
│      │                    │                    │                                 │
│      └────────────────────┼────────────────────┘                                 │
│                           ▼                                                      │
│                   ┌─────────────┐                                                │
│                   │  Template   │                                                │
│                   │  Selection  │                                                │
│                   └─────────────┘                                                │
│                           │                                                      │
│              ┌────────────┼────────────┐                                         │
│              ▼            ▼            ▼                                         │
│       ┌───────────┐ ┌───────────┐ ┌───────────┐                                 │
│       │ DAG.py    │ │ Spark     │ │ BigQuery  │                                 │
│       │ Template  │ │ Templates │ │ DDL       │                                 │
│       │ (Jinja2)  │ │ (Jinja2)  │ │ (Jinja2)  │                                 │
│       └───────────┘ └───────────┘ └───────────┘                                 │
│              │            │            │                                         │
│              └────────────┼────────────┘                                         │
│                           ▼                                                      │
│                   ┌─────────────┐                                                │
│                   │  Rendered   │                                                │
│                   │  Artifacts  │                                                │
│                   └─────────────┘                                                │
│                           │                                                      │
│                           ▼                                                      │
│                   ┌─────────────┐                                                │
│                   │ Validation  │ → Python AST, dry-run, lint                   │
│                   └─────────────┘                                                │
│                           │                                                      │
│                           ▼                                                      │
│                   ┌─────────────┐                                                │
│                   │  Git Push   │ → GitHub PR                                   │
│                   │  + CI/CD    │                                                │
│                   └─────────────┘                                                │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Approval Flow (Both Systems)

### Human-in-the-Loop Pattern

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        APPROVAL FLOW                                             │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  LangGraph Workflow                                                              │
│        │                                                                         │
│        ▼                                                                         │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                    NODE: await_approval                                  │    │
│  │  1. Generate approval_token (UUID)                                       │    │
│  │  2. Publish: incident.requires_approval (with token, plan, judge_score)  │    │
│  │  3. Publish: Slack notification (optional)                               │    │
│  │  4. WORKFLOW CHECKPOINTS AND PAUSES                                      │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
│                                                                                  │
│                                      │                                           │
│                                      ▼                                           │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                         UI / SLACK                                       │    │
│  │                                                                          │    │
│  │   ┌─────────────────────────────────────────────────────────────────┐   │    │
│  │   │  Approval Request                                                │   │    │
│  │   │  ─────────────────────────                                       │   │    │
│  │   │  Incident: INC0001234                                            │   │    │
│  │   │  Plan: Restart MySQL service                                     │   │    │
│  │   │  Risk: Medium                                                    │   │    │
│  │   │  Judge Score: 8.5/10                                             │   │    │
│  │   │                                                                  │   │    │
│  │   │  [APPROVE]  [REJECT]                                             │   │    │
│  │   └─────────────────────────────────────────────────────────────────┘   │    │
│  │                                                                          │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
│                                                                                  │
│                        │ Human clicks                                            │
│                        ▼                                                         │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                    FASTAPI: /api/v1/incidents/{id}/approve              │    │
│  │  1. Validate approval_token                                              │    │
│  │  2. Publish: incident.approved (to Kafka)                                │    │
│  │     - incident_id                                                        │    │
│  │     - approval_token                                                     │    │
│  │     - approved_by: "user@company.com"                                    │    │
│  │     - approved: true                                                     │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
│                                                                                  │
│                                      │                                           │
│                                      ▼                                           │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                    EVENT ORCHESTRATOR                                    │    │
│  │  1. Consume: incident.approved                                           │    │
│  │  2. Lookup thread_id for incident_id                                     │    │
│  │  3. Call: workflow_orchestrator.resume(incident_id, approval_decision)   │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
│                                                                                  │
│                                      │                                           │
│                                      ▼                                           │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                    LANGGRAPH WORKFLOW RESUMES                            │    │
│  │  1. Load checkpoint from MemorySaver                                     │    │
│  │  2. Inject approval_decision into state                                  │    │
│  │  3. Continue from process_approval_decision node                         │    │
│  │  4. Proceed to execute node                                              │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 4. CQRS Pattern

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        CQRS ARCHITECTURE                                         │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  COMMAND SIDE (Writes)                   QUERY SIDE (Reads)                      │
│  ════════════════════                    ═══════════════════                     │
│                                                                                  │
│  ┌─────────────┐                         ┌─────────────┐                         │
│  │   FastAPI   │                         │   FastAPI   │                         │
│  │  Approval   │                         │     UI      │                         │
│  │  Endpoints  │                         │  Endpoints  │                         │
│  └──────┬──────┘                         └──────┬──────┘                         │
│         │                                       │                                │
│         ▼                                       ▼                                │
│  ┌─────────────┐                         ┌─────────────┐                         │
│  │    Kafka    │                         │   Redis /   │                         │
│  │   Topics    │                         │  Postgres   │                         │
│  └─────────────┘                         └─────────────┘                         │
│         │                                       ▲                                │
│         │                                       │                                │
│         └───────────────────────────────────────┘                                │
│                    State Projector Consumer                                      │
│                    (updates read models)                                         │
│                                                                                  │
│  Commands go to Kafka                    Queries read from Redis/Postgres        │
│  (system of record)                      (eventually consistent)                 │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## See Also

- [ARCHITECTURE_V6_EVENT_DRIVEN.md](ARCHITECTURE_V6_EVENT_DRIVEN.md) - Full architecture
- [KAFKA_TOPICS.md](KAFKA_TOPICS.md) - Topic reference
- [RESPONSIBILITY_MATRIX.md](RESPONSIBILITY_MATRIX.md) - Component responsibilities
