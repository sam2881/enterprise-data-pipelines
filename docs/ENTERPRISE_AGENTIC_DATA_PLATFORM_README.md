# Enterprise Agentic Data Engineering Platform

## Metadata-Driven DAG & PySpark Generation | GCP-Native | Production-Grade

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Platform Philosophy: Compiler, Not ETL Tool](#2-platform-philosophy-compiler-not-etl-tool)
3. [Project Purpose & Scope](#3-project-purpose--scope)
4. [Multi-Agent System Architecture](#4-multi-agent-system-architecture)
5. [UI Control Plane Specification](#5-ui-control-plane-specification)
6. [Core Design Principles](#6-core-design-principles)
7. [High-Level Architecture](#7-high-level-architecture)
8. [GCP Services Integration (Cost-Optimized)](#8-gcp-services-integration)
9. [Medallion Architecture Contract](#9-medallion-architecture-contract)
10. [Source Types Supported](#10-source-types-supported)
11. [Metadata Model Specification](#11-metadata-model-specification)
12. [Agent Responsibilities & Lifecycle](#12-agent-responsibilities--lifecycle)
13. [DAG Template Strategy](#13-dag-template-strategy)
14. [PySpark Template Strategy](#14-pyspark-template-strategy)
15. [Change Data Capture (CDC) Patterns](#15-change-data-capture-cdc-patterns)
16. [Data Quality Framework](#16-data-quality-framework)
17. [CI/CD Pipeline](#17-cicd-pipeline)
18. [Environment Configuration & Bootstrapping](#18-environment-configuration)
19. [Security & Governance](#19-security--governance)
20. [Observability & Monitoring](#20-observability--monitoring)
21. [Error Handling & Recovery](#21-error-handling--recovery)
22. [Repository Structure](#22-repository-structure)
23. [DDL SQL Schema (Split Files)](#23-ddl-sql-schema)
24. [Agent System Prompts (Materialized)](#24-agent-system-prompts)
25. [Deployment Repository Structure](#25-deployment-repository-structure)
26. [CI/CD YAML Configurations](#26-cicd-yaml-configurations)
27. [API Contracts](#27-api-contracts)
28. [Implementation Roadmap](#28-implementation-roadmap)
29. [Appendix](#29-appendix)

---

## 1. Executive Summary

This repository defines and implements an **enterprise-grade, metadata-driven, agentic data engineering platform** built on Google Cloud Platform (GCP). The platform leverages AI agents (powered by LangGraph) to automate the entire data pipeline lifecycle—from intent capture to production deployment.

### Key Capabilities

- **Automated Pipeline Generation**: AI agents dynamically generate Airflow DAGs and PySpark jobs from structured metadata
- **Multi-Source Ingestion**: Support for files, databases, streaming, APIs, and legacy systems
- **Medallion Architecture**: Enforced data lakehouse pattern (Bronze → Silver → Gold) using Apache Iceberg
- **Zero-Touch Deployment**: GitOps-driven CI/CD with no manual DAG deployment
- **Enterprise Governance**: Full lineage tracking, data quality enforcement, and audit trails
- **Multi-Agent Orchestration**: Specialized agents for planning, generation, validation, and deployment
- **Cost-Optimized**: Minimal GCP resource footprint with serverless-first approach

### Multi-Agent Ecosystem

This platform operates as part of a larger multi-agent ecosystem:

| Agent | Trigger Source | Responsibility |
|-------|---------------|----------------|
| **Data Engineering Agent** (This Platform) | Jira Requests | Pipeline generation, DAG creation, Spark job generation |
| **ITSM Orchestration Agent** (Separate Service) | ServiceNow Requests | Incident management, change management, service requests |

Both agents share common infrastructure but operate independently based on their trigger sources.

---

## 2. Platform Philosophy: Compiler, Not ETL Tool

### Core Philosophy

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    THIS PLATFORM IS A COMPILER                          │
│                      NOT AN ETL TOOL                                    │
└─────────────────────────────────────────────────────────────────────────┘

Traditional ETL:                    This Platform:
─────────────────                   ──────────────
• Hand-coded pipelines              • Intent-driven generation
• Manual DAG creation               • Automated DAG compilation
• Free-text requirements            • Structured metadata input
• Ad-hoc modifications              • Version-controlled evolution
• Tribal knowledge                  • Codified contracts
```

### The Compiler Model

Just as a programming language compiler transforms source code into executable binaries, this platform transforms **structured intent** into **executable data pipelines**.

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   INTENT    │ →  │   COMPILE   │ →  │  VALIDATE   │ →  │   DEPLOY    │
│   (JSON)    │    │  (Agent)    │    │  (Agent)    │    │  (GitOps)   │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
      │                  │                  │                  │
      ▼                  ▼                  ▼                  ▼
  UI captures       Agent generates    Agent validates    CI/CD deploys
  structured        DAGs, Spark,       syntax, schema,    to Composer
  parameters        metadata SQL       security           automatically
```

### Why This Approach?

| Traditional Approach | Problem | Our Solution |
|---------------------|---------|--------------|
| Manual DAG coding | Error-prone, inconsistent | Agent-generated from templates |
| Free-text Jira parsing | Ambiguous, incomplete | Structured UI with validation |
| Ad-hoc schema changes | Breaking changes | Versioned, immutable metadata |
| Copy-paste pipelines | Technical debt | Parameterized templates |
| Tribal knowledge | Bus factor = 1 | Codified in metadata |

### Five Pillars of the Platform

#### 1. Intent is King
```
The UI captures INTENT, not implementation details.
The agent decides HOW to implement the intent.
Humans specify WHAT, AI determines HOW.
```

#### 2. No Manual DAGs
```
Manual DAG creation is PROHIBITED.
All DAGs are generated by the agent.
Templates are frozen and versioned.
Customization happens via metadata, not code.
```

#### 3. No Free-Text Parsing
```
The agent does NOT parse:
  ❌ Jira descriptions
  ❌ Email requests
  ❌ Slack messages
  ❌ Natural language requirements

The agent ONLY accepts:
  ✅ Validated JSON from UI
  ✅ Structured metadata from PostgreSQL
```

#### 4. Metadata is Immutable
```
Once created, metadata versions are NEVER modified.
Changes create NEW versions.
This enables:
  • Full audit trail
  • Rollback capability
  • Reproducibility
  • Compliance
```

#### 5. Fail Fast, Fail Loud
```
The agent STOPS on:
  • Missing required fields
  • Validation failures
  • Schema incompatibility
  • Security violations

The agent does NOT:
  • Guess missing values
  • Auto-fix errors
  • Proceed with warnings
```

### What This Means for Stakeholders

| Role | Implication |
|------|-------------|
| **Data Engineers** | Focus on metadata design, not DAG coding |
| **Business Analysts** | Self-service pipeline creation via UI |
| **Platform Team** | Maintain templates and agent logic |
| **Security Team** | Audit via metadata, not code review |
| **Management** | Predictable delivery, reduced risk |

### Cost Philosophy

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    COST-CONSCIOUS BY DESIGN                             │
└─────────────────────────────────────────────────────────────────────────┘

• Serverless-first: Pay only for what you use
• Minimal always-on resources: Small Composer, no persistent Dataproc
• Right-sized for growth: Start small, scale as needed
• Avoid over-provisioning: DEV/QA use minimal resources
```

---

## 3. Project Purpose & Scope

### What This Platform Does

1. **Accepts structured intent** from a UI/API in the form of validated JSON
2. **Processes requests** triggered by Jira tickets for data pipeline creation/modification
3. **Generates pipelines** using AI agents that understand metadata schemas
4. **Deploys automatically** via GitOps to Cloud Composer (Airflow)
5. **Executes workloads** on Dataproc Serverless (PySpark)
6. **Stores data** in Apache Iceberg tables on GCS (Bronze/Silver) and BigQuery (Gold)

### What This Platform Is NOT

- ❌ A one-off ingestion framework
- ❌ A static DAG repository
- ❌ A hand-coded pipeline solution
- ❌ A replacement for data governance tooling (works alongside Dataplex)
- ❌ A ServiceNow integration (handled by separate ITSM Agent)

### Source of Truth

This README serves as the **single source of truth** for:

- Metadata model contracts
- DAG and Spark template specifications
- Agent behavior and lifecycle
- CI/CD deployment procedures
- Security and governance requirements

---

## 4. Multi-Agent System Architecture

### Agent Framework: LangGraph

The platform uses **LangGraph** for multi-agent orchestration, providing:

- **Stateful Graph Workflows**: Model agent interactions as nodes and edges
- **Cyclic Execution**: Support for iterative refinement and feedback loops
- **State Management**: Persistent checkpointing for recovery
- **Human-in-the-Loop**: Built-in approval gates for high-risk operations

### Agent Topology

```
┌─────────────────────────────────────────────────────────────────────┐
│                      SUPERVISOR AGENT                                │
│                  (Orchestration & Routing)                          │
└─────────────────────────────────────────────────────────────────────┘
                                │
         ┌──────────────────────┼──────────────────────┐
         │                      │                      │
         ▼                      ▼                      ▼
┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐
│  PLANNER AGENT  │   │ GENERATOR AGENT │   │ VALIDATOR AGENT │
│                 │   │                 │   │                 │
│ • Intent Parse  │   │ • DAG Gen       │   │ • SQL Validate  │
│ • Schema Check  │   │ • Spark Gen     │   │ • DAG Import    │
│ • Strategy Pick │   │ • Metadata SQL  │   │ • Schema Check  │
└─────────────────┘   └─────────────────┘   └─────────────────┘
         │                      │                      │
         └──────────────────────┼──────────────────────┘
                                │
                                ▼
                  ┌─────────────────────────┐
                  │    DEPLOYER AGENT       │
                  │                         │
                  │ • Git Commit            │
                  │ • CI/CD Trigger         │
                  │ • Status Verification   │
                  └─────────────────────────┘
```

### Agent Communication Flow

```python
# LangGraph State Definition
from typing import TypedDict, Annotated, List
from langgraph.graph import StateGraph, add_messages

class AgentState(TypedDict):
    messages: Annotated[List, add_messages]
    intent_json: dict
    metadata_context: dict
    generated_artifacts: dict
    validation_results: dict
    deployment_status: str
    human_approval_required: bool
    error_state: str | None
```

### Integration with ITSM Agent

The Data Engineering Agent and ITSM Agent operate as peers:

```
                    ┌─────────────────────┐
                    │   EVENT BUS         │
                    │   (Pub/Sub/Kafka)   │
                    └─────────────────────┘
                              │
           ┌──────────────────┴──────────────────┐
           │                                     │
           ▼                                     ▼
┌──────────────────────┐            ┌──────────────────────┐
│  JIRA WEBHOOK        │            │  SERVICENOW WEBHOOK  │
│  ↓                   │            │  ↓                   │
│  Data Engineering    │            │  ITSM Orchestration  │
│  Agent Platform      │            │  Agent Platform      │
│  (This Repository)   │            │  (Separate Repo)     │
└──────────────────────┘            └──────────────────────┘
```

---

## 5. UI Control Plane Specification

### 4.1 Purpose of the UI

The UI acts as the **Control Plane** for the Agentic Data Engineering Platform. It is NOT a simple form—it is a **compiler front-end** that captures structured intent for the AI agent.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         UI CONTROL PLANE                                 │
│                    (Compiler Front-End)                                  │
│                                                                          │
│   • Collects all pipeline parameters in structured manner               │
│   • Validates user input against business rules                         │
│   • Produces a SINGLE canonical intent object                           │
│   • Sends intent to Kafka / PubSub                                      │
│   • Acts as the ONLY input source for the agent                         │
└─────────────────────────────────────────────────────────────────────────┘
```

**CRITICAL RULE**: The agent MUST NOT parse free-text Jira stories. All structured input comes from the UI.

### 4.2 UI Position in Architecture

```
User
  ↓
UI (Control Plane)
  ↓
Validated Intent JSON
  ↓
Kafka / PubSub
  ↓
Agentic AI
  ↓
Metadata + DAG + Code
```

**Jira Integration**: Jira is used ONLY for:
- Approval workflows
- Progress tracking
- Audit trail
- Human governance gates

Jira is **NOT** parsed by the agent for pipeline configuration.

### 4.3 UI Sections (Mandatory)

The UI MUST be divided into the following sections, each capturing specific aspects of pipeline configuration.

#### Section 1: Pipeline Identity & Governance

**Purpose**: Identify and govern the pipeline.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| Pipeline Name | string | Yes | Unique identifier (snake_case) |
| Domain | enum | Yes | Business domain (finance, sales, ops, marketing, etc.) |
| Business Owner | email | Yes | Business stakeholder email |
| Technical Owner | email | Yes | Engineering owner email |
| Environment | enum | Yes | Target environment (dev / qa / prod) |
| Data Sensitivity | enum | Yes | Classification (public / internal / confidential / pii) |
| SLA Hours | integer | No | Expected completion deadline |
| Tags | key-value | No | Custom metadata tags |

**Validation Rules**:
- Pipeline name must be unique per environment
- Pipeline name must match pattern: `^[a-z][a-z0-9_]*$`
- Owners must be valid corporate email addresses
- PROD environment requires additional approval fields

#### Section 2: Source Configuration

**Purpose**: Define where data comes from.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| Source Type | enum | Yes | file / database / streaming / api |
| Source System | string | Yes | Source identifier (oracle, salesforce, vendor_x) |
| Connection Type | enum | Yes | gcs / jdbc / kafka / pubsub / rest |
| Landing Location | string | Yes | GCS path, JDBC URL, topic name, API endpoint |
| File Pattern | string | Conditional | Glob pattern for files (e.g., `*.csv`, `orders_*.json`) |
| File Format | enum | Conditional | csv / json / parquet / avro / xml / fixed / excel |
| Encoding | enum | No | UTF-8 (default), UTF-16, EBCDIC, ISO-8859-1 |
| Compression | enum | No | none / gzip / snappy / zstd / zip / bzip2 |
| Arrival Pattern | cron | No | Expected arrival schedule |
| Multi-File Expected | boolean | No | Whether multiple files per batch |
| CDC Enabled | boolean | No | Enable change data capture |
| CDC Mode | enum | Conditional | debezium / datastream / goldengate |

**Validation Rules**:
- Landing location must be accessible (validated via API)
- File format required for file-based sources
- CDC mode required if CDC enabled
- JDBC URL must reference Secret Manager (no plain-text credentials)

#### Section 3: Schema & Metadata

**Purpose**: Define structure and evolution tracking.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| Schema Source | enum | Yes | upload / infer / reference |
| Schema Definition | JSON/file | Conditional | Column definitions with types |
| Primary Keys | array | Yes | Columns that uniquely identify records |
| Natural Keys | array | No | Business keys (if different from primary) |
| Partition Columns | array | No | Columns for table partitioning |
| Clustering Columns | array | No | Columns for clustering (max 4) |
| Schema Evolution Allowed | boolean | No | Allow automatic schema changes |
| Schema Drift Policy | enum | No | reject / evolve / quarantine |

**Schema Definition Format**:
```json
{
  "columns": [
    {"name": "customer_id", "type": "STRING", "nullable": false, "description": "Unique customer identifier"},
    {"name": "email", "type": "STRING", "nullable": true, "pii": true, "description": "Customer email"},
    {"name": "order_date", "type": "DATE", "nullable": false, "format": "yyyy-MM-dd"},
    {"name": "amount", "type": "DECIMAL(15,2)", "nullable": false}
  ]
}
```

**Validation Rules**:
- Primary keys must exist in schema definition
- PII columns must be flagged for masking
- At least one column required in schema

#### Section 4: Parsing Rules

**Purpose**: Define how to read and interpret the data.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| Parser Type | enum | Yes | spark_csv / spark_json / fixed_width / copybook / custom |
| Has Header | boolean | Conditional | First row is header (CSV) |
| Delimiter | string | Conditional | Field delimiter (CSV) |
| Quote Character | string | No | Quote character for strings |
| Escape Character | string | No | Escape character |
| Date Format | string | No | Default date format (e.g., yyyy-MM-dd) |
| Timestamp Format | string | No | Default timestamp format |
| Null Values | array | No | Strings to treat as NULL |
| Multi-Line | boolean | No | Records span multiple lines (JSON) |
| Copybook File | file | Conditional | COBOL copybook for mainframe data |
| Position Definitions | JSON | Conditional | Column positions for fixed-width |

**Fixed-Width Position Definition**:
```json
{
  "positions": [
    {"name": "account_id", "start": 0, "length": 10, "type": "STRING"},
    {"name": "amount", "start": 10, "length": 15, "type": "DECIMAL(15,2)", "implied_decimals": 2},
    {"name": "date", "start": 25, "length": 8, "type": "DATE", "format": "yyyyMMdd"}
  ]
}
```

**Validation Rules**:
- Copybook required for EBCDIC/mainframe data
- Position definitions required for fixed-width without copybook
- Delimiter required for CSV format

#### Section 5: Transformation & Business Logic

**Purpose**: Capture transformation intent as metadata.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| Logic Type | enum | No | sql / expression / dbt / none |
| Bronze to Silver Rules | JSON | No | Column-level transformation rules |
| Derived Columns | array | No | Calculated column definitions |
| Lookup References | array | No | Reference tables for enrichment |
| Filter Conditions | string | No | SQL WHERE clause for filtering |
| Deduplication Strategy | enum | No | none / keep_first / keep_last / keep_all |

**Transformation Rules Format**:
```json
{
  "column_rules": {
    "customer_id": "TRIM(customer_id)",
    "email": "LOWER(TRIM(email))",
    "full_name": "CONCAT(first_name, ' ', last_name)",
    "order_date": "TO_DATE(order_date_str, 'yyyy-MM-dd')",
    "amount": "CAST(amount_str AS DECIMAL(15,2))"
  },
  "filters": "amount > 0 AND status != 'CANCELLED'",
  "dedup_keys": ["order_id"],
  "dedup_order_by": "updated_at DESC"
}
```

**Important Rule**: Logic is stored as metadata and applied via views/transforms. NO direct table rewrites.

#### Section 6: CDC / SCD & Modeling

**Purpose**: Control history tracking and data modeling strategy.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| CDC Strategy | enum | Conditional | hash_compare / operation_code / timestamp |
| SCD Type | enum | No | none / type1 / type2 |
| Effective Date Column | string | Conditional | Column for SCD2 versioning |
| End Date Column | string | Conditional | Column for SCD2 closure |
| Hash Columns | array | Conditional | Columns for change detection |
| Late Arriving Handling | enum | No | reject / reprocess / merge |
| Modeling Strategy | enum | No | none / data_vault / star_schema / flat |
| Hub Tables | array | Conditional | Hub definitions (Data Vault) |
| Link Tables | array | Conditional | Link definitions (Data Vault) |
| Fact Tables | array | Conditional | Fact definitions (Star Schema) |
| Dimension Tables | array | Conditional | Dimension definitions (Star Schema) |

**SCD Type 2 Configuration**:
```json
{
  "scd_type": "type2",
  "merge_keys": ["customer_id"],
  "tracked_columns": ["email", "address", "phone"],
  "effective_from_column": "__effective_from",
  "effective_to_column": "__effective_to",
  "is_current_column": "__is_current",
  "late_arriving_policy": "reprocess"
}
```

**Validation Rules**:
- SCD Type 2 requires effective date column
- CDC requires primary keys defined
- Data Vault requires at least one hub definition
- Star Schema requires at least one fact or dimension

#### Section 7: Target & Execution Controls

**Purpose**: Define output destination and execution behavior.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| Target Platform | enum | Yes | bigquery / iceberg / both |
| Project ID | string | Yes | GCP project for BigQuery |
| Dataset | string | Yes | BigQuery dataset name |
| Table Name | string | Yes | Target table name |
| Load Mode | enum | Yes | append / overwrite / merge / scd2 |
| Partition Column | string | No | Column for table partitioning |
| Partition Type | enum | Conditional | DAY / MONTH / YEAR / HOUR |
| Cluster Columns | array | No | Columns for clustering (max 4) |
| Partition Expiration Days | integer | No | Auto-delete partitions older than N days |
| Backfill Allowed | boolean | No | Allow historical reprocessing |
| Reprocessing Allowed | boolean | No | Allow re-running for same date |
| Schedule | cron | No | Pipeline execution schedule |
| Retry Count | integer | No | Number of retry attempts (default: 3) |
| Timeout Minutes | integer | No | Task timeout (default: 60) |
| Human Approval Required | boolean | No | Require approval before execution |

**Validation Rules**:
- Partition column required if partition type specified
- Cluster columns must exist in schema
- PROD environment requires human approval by default

### 4.4 UI Output Contract

The UI MUST produce exactly **one canonical intent object** in JSON format.

**Complete Intent Object Structure**:

```json
{
  "intent_version": "1.0.0",
  "created_at": "2025-01-18T10:30:00Z",
  "created_by": "user@company.com",
  "jira_ticket": "DATA-1234",
  
  "pipeline_identity": {
    "pipeline_name": "customer_orders_daily",
    "domain": "sales",
    "business_owner": "business@company.com",
    "technical_owner": "engineer@company.com",
    "environment": "dev",
    "data_sensitivity": "pii",
    "sla_hours": 4,
    "tags": {"team": "revenue", "priority": "high"}
  },
  
  "source_config": {
    "source_type": "file",
    "source_system": "erp_oracle",
    "connection_type": "gcs",
    "landing_path": "gs://data-landing-dev/erp/orders/",
    "file_pattern": "orders_*.csv",
    "file_format": "csv",
    "encoding": "UTF-8",
    "compression": "gzip",
    "arrival_pattern": "0 6 * * *",
    "multi_file": true,
    "cdc_enabled": false
  },
  
  "schema": {
    "schema_source": "upload",
    "columns": [
      {"name": "order_id", "type": "STRING", "nullable": false},
      {"name": "customer_id", "type": "STRING", "nullable": false},
      {"name": "order_date", "type": "DATE", "nullable": false},
      {"name": "amount", "type": "DECIMAL(15,2)", "nullable": false}
    ],
    "primary_keys": ["order_id"],
    "partition_columns": ["order_date"],
    "schema_drift_policy": "reject"
  },
  
  "parsing_rules": {
    "parser_type": "spark_csv",
    "has_header": true,
    "delimiter": ",",
    "quote_char": "\"",
    "date_format": "yyyy-MM-dd",
    "null_values": ["", "NULL", "null"]
  },
  
  "transformation_logic": {
    "logic_type": "expression",
    "column_rules": {
      "order_id": "TRIM(order_id)",
      "customer_id": "TRIM(customer_id)",
      "amount": "CAST(amount AS DECIMAL(15,2))"
    },
    "filters": "amount > 0",
    "deduplication": {
      "enabled": true,
      "keys": ["order_id"],
      "order_by": "ingestion_ts DESC"
    }
  },
  
  "cdc_scd": {
    "cdc_enabled": false,
    "scd_type": "none",
    "modeling_strategy": "flat"
  },
  
  "target_config": {
    "target_platform": "bigquery",
    "project_id": "project-data-platform-dev",
    "dataset": "sales",
    "table_name": "customer_orders",
    "load_mode": "merge",
    "partition_column": "order_date",
    "partition_type": "DAY",
    "cluster_columns": ["customer_id"]
  },
  
  "execution_policy": {
    "schedule": "0 8 * * *",
    "retry_count": 3,
    "timeout_minutes": 60,
    "backfill_allowed": true,
    "reprocessing_allowed": true,
    "human_approval_required": false,
    "alert_on_failure": true,
    "alert_channels": ["slack"],
    "alert_recipients": ["team-data@company.com"]
  }
}
```

**Intent Object Properties**:
- **Immutable**: Once created, intent cannot be modified (new version required)
- **Versioned**: Every intent has a version number
- **Stored**: Persisted to GCS for audit trail
- **Published**: Sent to Kafka/PubSub for agent consumption
- **Authoritative**: Used by agent as SOLE input source

### 4.5 UI → Agent Contract

| Rule | Description |
|------|-------------|
| **Trust UI Input** | Agent MUST trust validated UI input without re-validation |
| **No Inference** | Agent MUST NOT infer missing values |
| **Fail on Missing** | Missing mandatory fields = STOP execution |
| **Version Tracking** | UI version is stored with all generated metadata |
| **No Free-Text Parsing** | Agent MUST NOT parse Jira descriptions or comments |

**Agent Input Validation**:
```python
def validate_intent(intent: dict) -> bool:
    """
    Agent's first action: validate intent structure.
    Returns True if valid, raises AgentStopException if invalid.
    """
    required_sections = [
        "pipeline_identity",
        "source_config", 
        "schema",
        "parsing_rules",
        "target_config"
    ]
    
    for section in required_sections:
        if section not in intent:
            raise AgentStopException(f"Missing required section: {section}")
    
    if not intent["pipeline_identity"].get("pipeline_name"):
        raise AgentStopException("Missing pipeline_name")
    
    if not intent["schema"].get("primary_keys"):
        raise AgentStopException("Missing primary_keys")
    
    return True
```

### 4.6 UI → Jira Integration

The UI creates/updates Jira tickets with:

| Field | Source | Purpose |
|-------|--------|---------|
| Pipeline ID | Generated | Unique tracking identifier |
| Intent Version | UI | Version of configuration |
| Intent GCS Path | UI | Link to full intent JSON |
| Approval Status | Workflow | pending / approved / rejected |
| Execution Status | Agent | queued / running / success / failed |
| Environment | UI | dev / qa / prod |
| Requester | UI | User who submitted request |

**Jira Ticket Structure**:
```
Title: [DATA-PIPELINE] {pipeline_name} - {environment}
Description: 
  Pipeline: {pipeline_name}
  Domain: {domain}
  Source: {source_type} - {source_system}
  Target: {dataset}.{table_name}
  
  Intent: gs://intents-bucket/{intent_id}.json
  
Labels: data-pipeline, {domain}, {environment}
```

### 4.7 UI Security & Governance

| Control | Implementation |
|---------|----------------|
| **Authentication** | SSO via corporate identity provider |
| **Authorization** | Role-based access (Viewer, Editor, Admin) |
| **Environment Restrictions** | PROD access requires elevated permissions |
| **Approval Gates** | PROD deployments require manager approval |
| **Audit Trail** | All UI actions logged with user, timestamp, changes |
| **Data Classification** | PII pipelines require additional review |
| **Secret Handling** | No credentials in UI - reference Secret Manager only |

**Role Permissions**:

| Role | DEV | QA | PROD |
|------|-----|-----|------|
| Viewer | Read | Read | Read |
| Editor | Create/Edit | Create/Edit | Request only |
| Admin | Full | Full | Full (with approval) |

### 4.8 UI Implementation Guidelines

**Technology Stack** (Recommended):
- Frontend: React/Angular with TypeScript
- State Management: Redux/NgRx
- Form Validation: Zod/Yup schemas
- API: REST/GraphQL to backend service
- Backend: Python FastAPI / Node.js Express
- Message Queue: Kafka / Cloud Pub/Sub

**Form Validation Schema** (Zod Example):
```typescript
import { z } from 'zod';

const PipelineIdentitySchema = z.object({
  pipeline_name: z.string()
    .min(3)
    .max(100)
    .regex(/^[a-z][a-z0-9_]*$/, "Must be lowercase with underscores"),
  domain: z.enum(['finance', 'sales', 'marketing', 'operations', 'hr']),
  business_owner: z.string().email(),
  technical_owner: z.string().email(),
  environment: z.enum(['dev', 'qa', 'prod']),
  data_sensitivity: z.enum(['public', 'internal', 'confidential', 'pii']),
  sla_hours: z.number().min(1).max(72).optional(),
});

const SourceConfigSchema = z.object({
  source_type: z.enum(['file', 'database', 'streaming', 'api']),
  source_system: z.string().min(1),
  connection_type: z.enum(['gcs', 'jdbc', 'kafka', 'pubsub', 'rest']),
  landing_path: z.string().min(1),
  file_format: z.enum(['csv', 'json', 'parquet', 'avro', 'xml', 'fixed']).optional(),
  cdc_enabled: z.boolean().default(false),
});

const FullIntentSchema = z.object({
  intent_version: z.string(),
  pipeline_identity: PipelineIdentitySchema,
  source_config: SourceConfigSchema,
  schema: SchemaDefinitionSchema,
  parsing_rules: ParsingRulesSchema,
  transformation_logic: TransformationLogicSchema.optional(),
  cdc_scd: CdcScdSchema.optional(),
  target_config: TargetConfigSchema,
  execution_policy: ExecutionPolicySchema.optional(),
});
```

### 4.9 Critical UI Rules

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    CRITICAL UI RULES FOR AGENT                          │
└─────────────────────────────────────────────────────────────────────────┘

1. If a parameter is NOT in the UI intent, the agent MUST assume it 
   does not exist. No defaults, no inference.

2. The UI intent JSON is the ONLY source of truth for pipeline 
   configuration. Not Jira, not email, not Slack.

3. Agent MUST validate intent structure before processing.
   Invalid intent = STOP immediately.

4. All mandatory fields must be present. Missing = STOP.

5. UI version must match agent's expected version.
   Version mismatch = STOP.

6. Agent must log the full intent JSON in audit trail before 
   any processing begins.

7. If agent needs information not in intent, it must:
   - NOT guess or infer
   - NOT proceed with defaults
   - STOP and report missing information back to UI/user
```

---

## 6. Core Design Principles

### Non-Negotiable Principles

| # | Principle | Description | Enforcement |
|---|-----------|-------------|-------------|
| 1 | **Metadata-First** | No hard-coded logic, schemas, or paths | Agent validation |
| 2 | **Agentic-AI Driven** | Agents plan, decide, and generate; templates are tools | LangGraph orchestration |
| 3 | **Medallion Architecture** | SOURCE → RAW → BRONZE → SILVER → MODELING → GOLD | Schema enforcement |
| 4 | **Backward Compatibility** | New logic must NEVER break existing pipelines | Version-controlled schemas |
| 5 | **GitOps Only** | No manual DAG deployment; everything flows via Git | CI/CD gates |
| 6 | **Immutable Audit Trail** | All decisions logged with full traceability | Cloud Logging + BigQuery |
| 7 | **Fail-Safe Operations** | Agent stops on validation failure; no auto-fix | Circuit breakers |

### Agent Behavioral Rules

```yaml
agent_rules:
  must:
    - Read metadata from PostgreSQL before any generation
    - Validate all outputs before commit
    - Log every decision with reasoning
    - Support rollback for any operation
    - Request human approval for PROD deployments
  
  must_not:
    - Invent schemas or logic not in metadata
    - Modify frozen templates
    - Bypass CI/CD pipeline
    - Auto-fix data quality issues
    - Deploy without validation passing
```

---

## 7. High-Level Architecture

### System Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           USER INTERFACE                                 │
│                    (React/Angular Web Application)                       │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
                    ┌───────────────────────────────┐
                    │     STRUCTURED INTENT         │
                    │     (Validated JSON)          │
                    └───────────────────────────────┘
                                    │
                    ┌───────────────┴───────────────┐
                    │                               │
                    ▼                               ▼
         ┌─────────────────┐             ┌─────────────────┐
         │  JIRA TICKET    │             │  KAFKA/PUBSUB   │
         │  (Trigger)      │             │  EVENT QUEUE    │
         └─────────────────┘             └─────────────────┘
                    │                               │
                    └───────────────┬───────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                        AGENTIC AI LAYER                                  │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐    │
│  │  Supervisor │→ │   Planner   │→ │  Generator  │→ │  Validator  │    │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘    │
│                                                              │          │
│                                                              ▼          │
│                                                    ┌─────────────┐      │
│                                                    │  Deployer   │      │
│                                                    └─────────────┘      │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                    ┌───────────────┴───────────────┐
                    │                               │
                    ▼                               ▼
         ┌─────────────────┐             ┌─────────────────┐
         │  METADATA SQL   │             │  GENERATED CODE │
         │  (PostgreSQL)   │             │  (DAG + Spark)  │
         └─────────────────┘             └─────────────────┘
                                                │
                                                ▼
                              ┌───────────────────────────────┐
                              │        GIT REPOSITORY         │
                              │     (Deployment Repo)         │
                              └───────────────────────────────┘
                                                │
                                                ▼
                              ┌───────────────────────────────┐
                              │        CI/CD PIPELINE         │
                              │     (Cloud Build/GitHub)      │
                              └───────────────────────────────┘
                                                │
                    ┌───────────────────────────┴───────────────────────┐
                    │                                                   │
                    ▼                                                   ▼
         ┌─────────────────────┐                          ┌─────────────────────┐
         │   CLOUD COMPOSER    │                          │ DATAPROC SERVERLESS │
         │   (Airflow DAGs)    │ ─────────────────────────│     (PySpark)       │
         └─────────────────────┘                          └─────────────────────┘
                    │                                                   │
                    └───────────────────────────┬───────────────────────┘
                                                │
                    ┌───────────────────────────┴───────────────────────┐
                    │                                                   │
                    ▼                                                   ▼
         ┌─────────────────────┐                          ┌─────────────────────┐
         │  GOOGLE CLOUD       │                          │    APACHE ICEBERG   │
         │  STORAGE (GCS)      │                          │    (Bronze/Silver)  │
         └─────────────────────┘                          └─────────────────────┘
                                                │
                                                ▼
                              ┌───────────────────────────────┐
                              │          BIGQUERY             │
                              │       (Gold / Serving)        │
                              └───────────────────────────────┘
```

---

## 8. GCP Services Integration

### Cost-Conscious Resource Strategy

```
┌─────────────────────────────────────────────────────────────────────────┐
│              MINIMAL RESOURCE FOOTPRINT - START SMALL                   │
└─────────────────────────────────────────────────────────────────────────┘

DEV Environment:
  • Composer: Small (scheduler: 0.5 vCPU, web: 0.5 vCPU)
  • Dataproc: Serverless only (no persistent clusters)
  • BigQuery: On-demand pricing (no slots reservation)
  • Cloud SQL: db-f1-micro (shared core)

QA Environment:
  • Composer: Small (same as DEV)
  • Dataproc: Serverless only
  • BigQuery: On-demand pricing
  • Cloud SQL: db-g1-small

PROD Environment:
  • Composer: Medium (scale up as needed)
  • Dataproc: Serverless + auto-scaling
  • BigQuery: Flex slots (100 slots minimum, scale as needed)
  • Cloud SQL: db-custom-2-4096 (HA optional)
```

### Core Services (Minimal Configuration)

| Service | Purpose | DEV Config | PROD Config | Billing Model |
|---------|---------|------------|-------------|---------------|
| **Cloud Composer 3** | Airflow orchestration | Small env | Medium env | Per environment-hour |
| **Dataproc Serverless** | PySpark execution | On-demand | On-demand | Per DCU-second |
| **BigQuery** | Gold layer, analytics | On-demand | Flex slots | Per TB scanned / slot-hour |
| **Cloud Storage** | Data lake (Bronze/Silver) | Standard | Standard | Per GB-month |
| **BigLake Metastore** | Iceberg catalog | Serverless | Serverless | Per request |
| **Pub/Sub** | Event streaming | On-demand | On-demand | Per message |
| **Cloud SQL (PostgreSQL)** | Metadata store | db-f1-micro | db-custom-2-4096 | Per instance-hour |
| **Secret Manager** | Credentials | On-demand | On-demand | Per secret version |

### Estimated Monthly Costs (Starting Configuration)

| Environment | Composer | Dataproc | BigQuery | Storage | Cloud SQL | Total |
|-------------|----------|----------|----------|---------|-----------|-------|
| **DEV** | ~$300 | ~$50 | ~$20 | ~$10 | ~$10 | **~$390** |
| **QA** | ~$300 | ~$100 | ~$50 | ~$20 | ~$25 | **~$495** |
| **PROD** | ~$600 | ~$500 | ~$200 | ~$100 | ~$100 | **~$1,500** |

*Costs are estimates and will vary based on actual usage.*

### Cloud Composer Configuration (Cost-Optimized)

```yaml
# DEV/QA Composer Configuration
composer_config:
  environment_size: SMALL
  
  workloads_config:
    scheduler:
      cpu: 0.5
      memory_gb: 1.875
      storage_gb: 1
      count: 1
    web_server:
      cpu: 0.5
      memory_gb: 1.875
      storage_gb: 1
    worker:
      cpu: 0.5
      memory_gb: 1.875
      storage_gb: 1
      min_count: 1
      max_count: 3
    triggerer:
      cpu: 0.5
      memory_gb: 0.5
      count: 1
  
  software_config:
    airflow_config_overrides:
      core-parallelism: "16"
      core-dag_concurrency: "8"
      celery-worker_concurrency: "4"
    pypi_packages:
      apache-airflow-providers-google: ">=10.0.0"
      
  environment_config:
    resilience_mode: STANDARD_RESILIENCE  # Not HA for DEV
```

```yaml
# PROD Composer Configuration (scale up from here)
composer_config:
  environment_size: MEDIUM
  
  workloads_config:
    scheduler:
      cpu: 2
      memory_gb: 7.5
      storage_gb: 5
      count: 2  # HA
    web_server:
      cpu: 2
      memory_gb: 7.5
      storage_gb: 5
    worker:
      cpu: 2
      memory_gb: 7.5
      storage_gb: 5
      min_count: 2
      max_count: 6
    triggerer:
      cpu: 1
      memory_gb: 1
      count: 2
  
  environment_config:
    resilience_mode: HIGH_RESILIENCE
```

### Dataproc Serverless Configuration

```python
# Cost-optimized Dataproc Serverless batch configuration
batch_config = {
    "pyspark_batch": {
        "main_python_file_uri": "gs://bucket/job.py",
    },
    "runtime_config": {
        "version": "2.2",  # Latest stable
        "properties": {
            # Memory optimization
            "spark.executor.memory": "4g",
            "spark.driver.memory": "2g",
            "spark.executor.memoryOverhead": "512m",
            
            # Cost optimization
            "spark.dynamicAllocation.enabled": "true",
            "spark.dynamicAllocation.minExecutors": "2",
            "spark.dynamicAllocation.maxExecutors": "10",
            "spark.dynamicAllocation.executorIdleTimeout": "60s",
            
            # Performance with cost balance
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
        }
    },
    "environment_config": {
        "execution_config": {
            "subnetwork_uri": "projects/{project}/regions/{region}/subnetworks/{subnet}",
            # Use preemptible/spot for cost savings (DEV/QA only)
            # "ttl": "3600s"  # Auto-terminate after 1 hour
        }
    }
}
```

### BigQuery Configuration (Cost-Optimized)

```sql
-- Cost-optimized table creation
CREATE TABLE `project.dataset.table_name`
(
  -- columns
)
PARTITION BY DATE(reporting_date)  -- Always partition for cost savings
CLUSTER BY customer_id, source_system  -- Cluster frequently filtered columns
OPTIONS (
  partition_expiration_days = 365,  -- Auto-delete old partitions
  require_partition_filter = true,   -- Force partition pruning
  description = "Generated by Agentic Platform"
);

-- Use materialized views for frequently accessed aggregations
CREATE MATERIALIZED VIEW `project.dataset.mv_daily_summary`
OPTIONS (
  enable_refresh = true,
  refresh_interval_minutes = 60
)
AS SELECT ...;
```

| Service | Purpose | Configuration |
|---------|---------|---------------|
| **Cloud Composer 3** | Airflow DAG orchestration | Environment: Large, Airflow 2.10+ |
| **Dataproc Serverless** | PySpark job execution | Auto-scaling, per-second billing |
| **BigQuery** | Gold layer serving, analytics | Partitioned, clustered tables |
| **Cloud Storage (GCS)** | Data lake storage (Bronze/Silver) | Multi-regional, lifecycle policies |
| **BigLake Metastore** | Iceberg catalog management | Serverless, unified governance |
| **Pub/Sub** | Event streaming, CDC delivery | At-least-once delivery |
| **Cloud SQL (PostgreSQL)** | Metadata store | HA configuration, automated backups |
| **Datastream** | CDC from source databases | Log-based replication |
| **Secret Manager** | Credential management | Auto-rotation enabled |
| **Cloud Logging** | Centralized logging | 30-day retention, BigQuery sink |
| **Cloud Monitoring** | Metrics and alerting | Custom dashboards, PagerDuty integration |

### Apache Iceberg on GCP

The platform uses Apache Iceberg for the Bronze and Silver layers, providing:

- **ACID Transactions**: Multi-file atomic operations
- **Time Travel**: Unlimited snapshots (vs BigQuery's 7-day limit)
- **Schema Evolution**: Add, rename, delete columns without rewrite
- **Partition Evolution**: Change partitioning strategy without data movement
- **Engine Agnostic**: Query from Spark, Trino, Flink, or BigQuery

```python
# Iceberg table creation via Spark on Dataproc
spark.sql("""
    CREATE TABLE IF NOT EXISTS bronze.{table_name} (
        {schema_columns},
        run_id STRING,
        record_uuid STRING,
        reporting_date DATE,
        ingestion_ts TIMESTAMP,
        source_system STRING
    )
    USING iceberg
    PARTITIONED BY (reporting_date)
    LOCATION 'gs://{bucket}/bronze/{table_name}'
    TBLPROPERTIES (
        'write.format.default' = 'parquet',
        'write.parquet.compression-codec' = 'zstd'
    )
""")
```

### BigLake Integration

```python
# Register Iceberg table with BigLake Metastore
spark.conf.set("spark.sql.catalog.biglake", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.biglake.catalog-impl", "org.apache.iceberg.gcp.biglake.BigLakeCatalog")
spark.conf.set("spark.sql.catalog.biglake.gcp_project", "{project_id}")
spark.conf.set("spark.sql.catalog.biglake.gcp_location", "us-central1")
spark.conf.set("spark.sql.catalog.biglake.warehouse", "gs://{bucket}/warehouse")
```

---

## 9. Medallion Architecture Contract

### Layer Definitions

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              SOURCE                                      │
│                     (External Systems)                                   │
│   Files | Databases | APIs | Streaming | Legacy                         │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                               RAW                                        │
│                      (Immutable Landing)                                 │
│   • Exact copy of source data                                           │
│   • No transformations                                                  │
│   • Stored in GCS with timestamp partitioning                          │
│   • Format: Original (CSV, JSON, Parquet, etc.)                        │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                              BRONZE                                      │
│                    (Iceberg - All STRING)                               │
│   • All columns cast to STRING (schema-on-read)                        │
│   • System columns added                                                │
│   • Deduplication applied                                               │
│   • Format: Apache Iceberg on GCS                                      │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                              SILVER                                      │
│                  (Typed, Cleansed, Standardized)                        │
│   • Data type enforcement                                               │
│   • Null handling and defaults                                          │
│   • Business rule validation                                            │
│   • Standardization (dates, codes, names)                              │
│   • Format: Apache Iceberg on GCS                                      │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                            MODELING                                      │
│                    (DV2 / Star / Flat)                                  │
│   • Data Vault 2.0 (Hub, Link, Satellite)                              │
│   • Star Schema (Fact, Dimension)                                       │
│   • Flat/Wide Tables                                                    │
│   • SCD Type 2 history tracking                                        │
│   • Format: Apache Iceberg on GCS                                      │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                               GOLD                                       │
│                       (BigQuery Serving)                                │
│   • Optimized for analytics                                             │
│   • Partitioned and clustered                                          │
│   • Row-level security applied                                          │
│   • Aggregations and KPIs                                              │
│   • Format: BigQuery Native Tables                                     │
└─────────────────────────────────────────────────────────────────────────┘
```

### Mandatory System Columns

Every table at Bronze layer and above MUST include these system columns:

| Column | Type | Description | Populated By |
|--------|------|-------------|--------------|
| `run_id` | STRING | Unique pipeline execution ID | Airflow |
| `record_uuid` | STRING | Unique record identifier (UUID v4) | Spark |
| `reporting_date` | DATE | Business date of the data | Source/Config |
| `ingestion_ts` | TIMESTAMP | When record was ingested | Spark |
| `source_system` | STRING | Source system identifier | Metadata |
| `__row_hash` | STRING | MD5 hash for deduplication | Spark |
| `__is_deleted` | BOOLEAN | Soft delete flag (CDC) | CDC Logic |
| `__effective_from` | TIMESTAMP | SCD2 start timestamp | CDC Logic |
| `__effective_to` | TIMESTAMP | SCD2 end timestamp | CDC Logic |

---

## 10. Source Types Supported

### 11.1 File-Based Sources

| Format | Extensions | Parser | Special Handling |
|--------|------------|--------|------------------|
| CSV | .csv, .txt | Spark CSV | Header detection, delimiter config |
| Excel | .xlsx, .xls | openpyxl/xlrd | Sheet selection, range parsing |
| JSON | .json, .jsonl | Spark JSON | Nested structure flattening |
| Parquet | .parquet | Spark Parquet | Schema evolution |
| Avro | .avro | Spark Avro | Schema registry integration |
| XML | .xml | spark-xml | XPath extraction |
| Fixed-Width | .txt, .dat | Custom parser | Position-based parsing |
| EBCDIC | .dat | Copybook parser | Mainframe cobol copybook |
| Compressed | .gz, .zip, .tar | Auto-detect | In-flight decompression |

### 11.2 Database Sources

| Database | Connection | Ingestion Modes |
|----------|------------|-----------------|
| Oracle | JDBC (ojdbc8) | Snapshot, Incremental, CDC |
| PostgreSQL | JDBC (postgresql) | Snapshot, Incremental, CDC |
| SQL Server | JDBC (mssql-jdbc) | Snapshot, Incremental, CDC |
| MySQL | JDBC (mysql-connector) | Snapshot, Incremental, CDC |
| DB2 | JDBC (db2jcc) | Snapshot, Incremental, CDC |
| AlloyDB | Native connector | Snapshot, Incremental, CDC |
| Cloud SQL | Cloud SQL connector | Snapshot, Incremental, CDC |

### 11.3 Streaming Sources

| Source | Protocol | Processing |
|--------|----------|------------|
| Apache Kafka | Kafka Protocol | Structured Streaming |
| Cloud Pub/Sub | gRPC | Dataflow/Spark Streaming |
| Debezium CDC | Kafka + Avro | Change event processing |
| Oracle GoldenGate | Kafka/Files | CDC event processing |
| Datastream | Native | Log-based CDC |

### 11.4 API Sources

| Type | Protocol | Authentication |
|------|----------|----------------|
| REST | HTTP/HTTPS | OAuth2, API Key, Basic |
| SOAP | HTTP/HTTPS | WS-Security, Basic |
| GraphQL | HTTP/HTTPS | Bearer Token, API Key |
| SaaS (Salesforce) | REST/SOAP | OAuth2 |
| SaaS (SAP) | OData/RFC | SAP Logon |

---

## 11. Metadata Model Specification

### Entity Relationship Diagram

```
┌─────────────────┐       ┌─────────────────┐       ┌─────────────────┐
│    pipeline     │       │  source_config  │       │ schema_version  │
│─────────────────│       │─────────────────│       │─────────────────│
│ pipeline_id (PK)│◄──────│ pipeline_id(FK) │       │ pipeline_id(FK) │
│ pipeline_name   │       │ source_system   │       │ schema_version  │
│ domain          │       │ landing_path    │       │ schema_json     │
│ source_type     │       │ file_pattern    │       │ primary_keys    │
│ processing_mode │       │ file_format     │       │ partition_cols  │
│ modeling_strat  │       │ encoding        │       │ drift_policy    │
│ target_platform │       │ compression     │       │ effective_from  │
│ is_active       │       │ arrival_pattern │       └─────────────────┘
│ created_at      │       │ is_multi_file   │
│ updated_at      │       │ cdc_enabled     │
└─────────────────┘       └─────────────────┘
         │
         │       ┌─────────────────┐       ┌─────────────────┐
         │       │  parsing_rules  │       │transform_logic  │
         │       │─────────────────│       │─────────────────│
         ├──────►│ pipeline_id(FK) │       │ pipeline_id(FK) │
         │       │ parser_type     │       │ layer           │
         │       │ parser_config   │       │ logic_version   │
         │       │ reuse_existing  │       │ logic_type      │
         │       └─────────────────┘       │ logic_definition│
         │                                 └─────────────────┘
         │
         │       ┌─────────────────┐       ┌─────────────────┐
         │       │  dq_rules       │       │  target_config  │
         │       │─────────────────│       │─────────────────│
         ├──────►│ pipeline_id(FK) │       │ pipeline_id(FK) │
         │       │ layer           │       │ dataset         │
         │       │ rule_type       │       │ table_name      │
         │       │ rule_config     │       │ load_mode       │
         │       │ severity        │       │ partition_by    │
         │       └─────────────────┘       │ cluster_by      │
         │                                 └─────────────────┘
         │
         │       ┌─────────────────┐
         │       │execution_policy │
         │       │─────────────────│
         └──────►│ pipeline_id(FK) │
                 │ retry_count     │
                 │ timeout_seconds │
                 │ human_approval  │
                 │ env_overrides   │
                 └─────────────────┘
```

### 11.1 pipeline

Defines the **pipeline intent and identity**.

```yaml
pipeline:
  pipeline_id: integer (PK, auto-increment)
  pipeline_name: string (unique, snake_case)
  domain: string (e.g., "finance", "marketing", "operations")
  source_type: enum ["file", "database", "streaming", "api"]
  processing_mode: enum ["batch", "micro_batch", "streaming"]
  modeling_strategy: enum ["dv2", "star", "flat", "none"]
  target_platform: enum ["bigquery", "iceberg", "both"]
  is_active: boolean (default: true)
  created_at: timestamp
  updated_at: timestamp
  created_by: string
  tags: jsonb (key-value pairs for filtering)
```

### 11.2 source_config

Defines **where and how data comes from**.

```yaml
source_config:
  config_id: integer (PK)
  pipeline_id: integer (FK → pipeline)
  source_system: string (unique identifier)
  connection_type: enum ["gcs", "jdbc", "kafka", "pubsub", "api"]
  
  # File-specific
  landing_path: string (GCS path pattern)
  file_pattern: string (glob pattern, e.g., "*.csv")
  file_format: enum ["csv", "json", "parquet", "avro", "xml", "fixed"]
  encoding: string (default: "UTF-8")
  compression: enum ["none", "gzip", "snappy", "zstd"]
  has_header: boolean
  delimiter: string
  quote_char: string
  escape_char: string
  null_values: array[string]
  
  # Database-specific
  jdbc_url: string (encrypted reference)
  query_template: string (SQL with placeholders)
  watermark_column: string (for incremental)
  partition_column: string (for parallel reads)
  fetch_size: integer
  
  # Streaming-specific
  topic_name: string
  consumer_group: string
  starting_offset: enum ["earliest", "latest", "timestamp"]
  
  # Common
  arrival_pattern: cron expression
  is_multi_file: boolean
  cdc_enabled: boolean
  cdc_mode: enum ["debezium", "datastream", "goldengate"]
```

### 11.3 schema_version

Defines **structure and evolution tracking**.

```yaml
schema_version:
  version_id: integer (PK)
  pipeline_id: integer (FK → pipeline)
  schema_version: integer (incrementing)
  schema_json: jsonb
    # Example structure:
    # {
    #   "columns": [
    #     {"name": "customer_id", "type": "string", "nullable": false},
    #     {"name": "email", "type": "string", "nullable": true, "pii": true},
    #     {"name": "created_date", "type": "date", "format": "yyyy-MM-dd"}
    #   ]
    # }
  primary_keys: array[string]
  partition_columns: array[string]
  clustering_columns: array[string]
  schema_drift_policy: enum ["reject", "evolve", "quarantine"]
  effective_from: timestamp
  effective_to: timestamp (null for current)
  is_current: boolean
```

### 11.4 parsing_rules

Defines **how to read and interpret data**.

```yaml
parsing_rules:
  rule_id: integer (PK)
  pipeline_id: integer (FK → pipeline)
  parser_type: enum ["spark", "pandas", "custom"]
  parser_config_json: jsonb
    # CSV example:
    # {
    #   "header": true,
    #   "inferSchema": false,
    #   "dateFormat": "yyyy-MM-dd",
    #   "timestampFormat": "yyyy-MM-dd HH:mm:ss",
    #   "multiLine": false,
    #   "mode": "PERMISSIVE",
    #   "columnNameOfCorruptRecord": "_corrupt_record"
    # }
    
    # Fixed-width example:
    # {
    #   "positions": [
    #     {"name": "account_id", "start": 0, "length": 10},
    #     {"name": "amount", "start": 10, "length": 15, "type": "decimal(15,2)"}
    #   ]
    # }
  reuse_existing: boolean (use existing parser if available)
  validation_sample_size: integer (rows to validate before full load)
```

### 11.5 transformation_logic

Defines **transformation rules as metadata**.

```yaml
transformation_logic:
  logic_id: integer (PK)
  pipeline_id: integer (FK → pipeline)
  layer: enum ["bronze", "silver", "modeling", "gold"]
  logic_version: integer
  logic_type: enum ["sql", "pyspark", "dbt", "expression"]
  logic_definition: text
    # SQL example:
    # "SELECT 
    #    TRIM(customer_id) as customer_id,
    #    UPPER(email) as email,
    #    CAST(amount AS DECIMAL(15,2)) as amount,
    #    PARSE_DATE('%Y-%m-%d', date_str) as transaction_date
    #  FROM bronze.{table_name}
    #  WHERE _corrupt_record IS NULL"
    
    # Expression example (column-level):
    # {
    #   "customer_id": "TRIM(customer_id)",
    #   "email": "LOWER(TRIM(email))",
    #   "full_name": "CONCAT(first_name, ' ', last_name)"
    # }
  dependencies: array[string] (upstream tables)
  effective_from: timestamp
  is_current: boolean
```

### 11.6 data_quality_rules

Defines **validation rules and thresholds**.

```yaml
data_quality_rules:
  rule_id: integer (PK)
  pipeline_id: integer (FK → pipeline)
  layer: enum ["bronze", "silver", "gold"]
  rule_name: string
  rule_type: enum [
    "not_null",
    "unique",
    "referential",
    "range",
    "regex",
    "custom_sql",
    "freshness",
    "volume"
  ]
  rule_config_json: jsonb
    # Examples:
    # not_null: {"columns": ["customer_id", "email"]}
    # unique: {"columns": ["customer_id"], "scope": "table"}
    # range: {"column": "amount", "min": 0, "max": 1000000}
    # regex: {"column": "email", "pattern": "^[a-zA-Z0-9+_.-]+@[a-zA-Z0-9.-]+$"}
    # freshness: {"column": "ingestion_ts", "max_age_hours": 24}
    # volume: {"min_records": 1000, "max_variance_pct": 20}
  severity: enum ["info", "warning", "error", "critical"]
  action_on_failure: enum ["log", "quarantine", "fail", "alert"]
  threshold_pct: decimal (acceptable failure percentage)
  is_active: boolean
```

### 11.7 target_config

Defines **BigQuery/Iceberg target configuration**.

```yaml
target_config:
  config_id: integer (PK)
  pipeline_id: integer (FK → pipeline)
  target_type: enum ["bigquery", "iceberg"]
  
  # BigQuery specific
  project_id: string
  dataset: string
  table_name: string
  load_mode: enum ["append", "overwrite", "merge", "scd2"]
  write_disposition: enum ["WRITE_APPEND", "WRITE_TRUNCATE", "WRITE_EMPTY"]
  partition_by: string (column name)
  partition_type: enum ["DAY", "MONTH", "YEAR", "HOUR"]
  partition_expiration_days: integer
  cluster_by: array[string] (max 4 columns)
  
  # Merge/SCD2 specific
  merge_keys: array[string]
  update_columns: array[string] (null = all non-key columns)
  soft_delete_column: string
  
  # Iceberg specific
  iceberg_catalog: string
  iceberg_namespace: string
  iceberg_table: string
  
  # Common
  table_description: string
  column_descriptions: jsonb
  labels: jsonb
```

### 11.8 execution_policy

Defines **agent safety controls and runtime behavior**.

```yaml
execution_policy:
  policy_id: integer (PK)
  pipeline_id: integer (FK → pipeline)
  
  # Retry configuration
  retry_count: integer (default: 3)
  retry_delay_seconds: integer (default: 300)
  retry_exponential_backoff: boolean (default: true)
  
  # Timeout configuration
  task_timeout_seconds: integer (default: 3600)
  dag_timeout_seconds: integer (default: 86400)
  
  # Approval gates
  human_approval_required: boolean (default: false for DEV)
  approval_timeout_hours: integer (default: 24)
  approvers: array[string] (email addresses)
  
  # Resource allocation
  spark_driver_memory: string (default: "4g")
  spark_executor_memory: string (default: "8g")
  spark_executor_cores: integer (default: 4)
  spark_num_executors: integer (default: 2)
  spark_dynamic_allocation: boolean (default: true)
  
  # Environment overrides
  env_overrides: jsonb
    # {
    #   "DEV": {"spark_num_executors": 1},
    #   "QA": {"human_approval_required": true},
    #   "PROD": {"human_approval_required": true, "spark_num_executors": 10}
    # }
  
  # Alerting
  alert_on_failure: boolean (default: true)
  alert_channels: array[string] (slack, email, pagerduty)
  alert_recipients: array[string]
  
  # SLA
  sla_deadline_hour: integer (e.g., 6 for 6 AM)
  sla_miss_action: enum ["alert", "escalate", "fail"]
```

---

## 12. Agent Responsibilities & Lifecycle

### Agent Lifecycle Phases

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        AGENT LIFECYCLE                                   │
└─────────────────────────────────────────────────────────────────────────┘

Phase 1: PERCEPTION
├── Read intent JSON from event queue
├── Query existing metadata from PostgreSQL
├── Load current schema versions
├── Retrieve template configurations
└── Establish execution context

Phase 2: PLANNING
├── Determine: New pipeline vs. modification
├── Detect schema evolution requirements
├── Select parsing strategy
├── Choose DAG template(s)
├── Plan resource allocation
└── Identify approval requirements

Phase 3: GENERATION
├── Generate insert_metadata.sql
├── Generate update_metadata.sql
├── Generate PySpark jobs from templates
├── Generate Airflow DAGs from templates
├── Generate data quality rules
└── Generate documentation

Phase 4: VALIDATION
├── SQL syntax validation
├── DAG import validation (dry run)
├── Spark syntax validation
├── Schema compatibility check
├── Metadata consistency check
└── Security policy validation

Phase 5: DEPLOYMENT
├── Create Git branch
├── Commit generated artifacts
├── Create Pull Request
├── Trigger CI/CD pipeline
├── Wait for pipeline completion
└── Merge on success

Phase 6: VERIFICATION
├── Confirm DAG appears in Airflow
├── Trigger test execution (DEV)
├── Validate data landed correctly
├── Check data quality results
├── Update Jira ticket status
└── Send completion notification
```

### Agent Decision Matrix

```yaml
decisions:
  new_vs_existing:
    condition: "pipeline_id exists in metadata"
    new_pipeline:
      - Create new metadata records
      - Generate full DAG
      - Generate all Spark jobs
    existing_pipeline:
      - Compare schema versions
      - Generate delta updates
      - Preserve backward compatibility
  
  schema_evolution:
    condition: "schema_json differs from current"
    actions:
      - Create new schema_version record
      - Set effective_from = now()
      - Update previous version effective_to
      - Apply drift_policy rules
  
  template_selection:
    file_batch: "file_ingest_dag.py"
    file_streaming: "streaming_ingest_dag.py"
    database_snapshot: "db_snapshot_dag.py"
    database_incremental: "db_incremental_dag.py"
    database_cdc: "cdc_ingest_dag.py"
    api_rest: "api_ingest_dag.py"
    modeling: "modeling_dag.py"
  
  approval_gates:
    DEV: false
    QA: true (if schema change)
    PROD: true (always)
```

### Agent State Machine

```python
from enum import Enum
from typing import Optional
from pydantic import BaseModel

class AgentStatus(Enum):
    IDLE = "idle"
    PERCEIVING = "perceiving"
    PLANNING = "planning"
    GENERATING = "generating"
    VALIDATING = "validating"
    AWAITING_APPROVAL = "awaiting_approval"
    DEPLOYING = "deploying"
    VERIFYING = "verifying"
    COMPLETED = "completed"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"

class AgentContext(BaseModel):
    run_id: str
    pipeline_id: Optional[int]
    status: AgentStatus
    current_phase: str
    intent_json: dict
    metadata_snapshot: dict
    generated_artifacts: dict
    validation_results: dict
    deployment_url: Optional[str]
    error_message: Optional[str]
    started_at: datetime
    completed_at: Optional[datetime]
    audit_log: list[dict]
```

---

## 13. DAG Template Strategy

### Template Architecture

The platform uses **composable, parameterized DAG templates** rather than monolithic DAGs. Templates are versioned and frozen once deployed.

```
dag_templates/
├── base/
│   ├── base_dag.py              # Common DAG configuration
│   ├── task_groups.py           # Reusable task groups
│   └── operators.py             # Custom operator wrappers
├── ingestion/
│   ├── file_ingest_dag.py       # File-based ingestion
│   ├── db_snapshot_dag.py       # Database full snapshot
│   ├── db_incremental_dag.py    # Database incremental load
│   ├── cdc_ingest_dag.py        # CDC stream processing
│   ├── streaming_ingest_dag.py  # Real-time streaming
│   └── api_ingest_dag.py        # API data extraction
├── transformation/
│   ├── bronze_to_silver_dag.py  # Bronze → Silver transformation
│   ├── silver_to_modeling_dag.py # Silver → Modeling
│   └── modeling_to_gold_dag.py  # Modeling → Gold (BigQuery)
└── maintenance/
    ├── compaction_dag.py        # Iceberg table compaction
    ├── vacuum_dag.py            # Expired snapshot cleanup
    └── quality_dag.py           # Data quality validation
```

### Base DAG Template

```python
# dag_templates/base/base_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup

def create_base_dag(
    dag_id: str,
    pipeline_config: dict,
    schedule_interval: str = None,
    tags: list = None,
    **kwargs
) -> DAG:
    """
    Creates a base DAG with standard configuration.
    
    Args:
        dag_id: Unique DAG identifier
        pipeline_config: Metadata configuration from PostgreSQL
        schedule_interval: Cron expression or preset
        tags: DAG tags for filtering
    """
    
    default_args = {
        'owner': pipeline_config.get('domain', 'data-engineering'),
        'depends_on_past': False,
        'email': pipeline_config.get('alert_recipients', []),
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': pipeline_config.get('retry_count', 3),
        'retry_delay': timedelta(seconds=pipeline_config.get('retry_delay_seconds', 300)),
        'execution_timeout': timedelta(seconds=pipeline_config.get('task_timeout_seconds', 3600)),
        'sla': timedelta(hours=pipeline_config.get('sla_hours', 4)) if pipeline_config.get('sla_hours') else None,
    }
    
    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        description=f"Pipeline: {pipeline_config.get('pipeline_name', dag_id)}",
        schedule_interval=schedule_interval,
        start_date=datetime(2024, 1, 1),
        catchup=False,
        max_active_runs=1,
        tags=tags or [pipeline_config.get('domain', 'default')],
        doc_md=f"""
        ## {pipeline_config.get('pipeline_name', dag_id)}
        
        **Domain**: {pipeline_config.get('domain')}
        **Source Type**: {pipeline_config.get('source_type')}
        **Processing Mode**: {pipeline_config.get('processing_mode')}
        
        Generated by Agentic Data Engineering Platform
        """,
        **kwargs
    )
    
    return dag
```

### File Ingestion DAG Template

```python
# dag_templates/ingestion/file_ingest_dag.py
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitPySparkJobOperator,
    DataprocCreateBatchOperator
)
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.utils.task_group import TaskGroup

def create_file_ingest_dag(pipeline_config: dict, metadata: dict) -> DAG:
    """
    Creates a file ingestion DAG with the following task groups:
    
    1. sensor_group: Wait for source files
    2. ingest_group: Raw → Bronze ingestion
    3. transform_group: Bronze → Silver transformation
    4. quality_group: Data quality validation
    5. notify_group: Notifications and cleanup
    """
    
    dag_id = f"{pipeline_config['pipeline_name']}_file_ingest"
    
    with create_base_dag(dag_id, pipeline_config) as dag:
        
        # Task Group 1: File Sensors
        with TaskGroup(group_id='sensor_group') as sensor_group:
            file_sensor = GCSObjectsWithPrefixExistenceSensor(
                task_id='wait_for_files',
                bucket=metadata['source_config']['landing_bucket'],
                prefix=metadata['source_config']['landing_path'],
                google_cloud_conn_id='google_cloud_default',
                mode='poke',
                poke_interval=300,
                timeout=3600
            )
        
        # Task Group 2: Bronze Ingestion
        with TaskGroup(group_id='ingest_group') as ingest_group:
            bronze_ingest = DataprocCreateBatchOperator(
                task_id='bronze_ingest',
                project_id=Variable.get('gcp_project'),
                region=Variable.get('gcp_region'),
                batch_id=f"{dag_id}-bronze-{{{{ ts_nodash }}}}",
                batch={
                    'pyspark_batch': {
                        'main_python_file_uri': f"gs://{Variable.get('spark_scripts_bucket')}/bronze_ingest.py",
                        'args': [
                            '--pipeline_id', str(pipeline_config['pipeline_id']),
                            '--run_id', '{{ run_id }}',
                            '--reporting_date', '{{ ds }}'
                        ],
                        'python_file_uris': [
                            f"gs://{Variable.get('spark_scripts_bucket')}/utils/*.py"
                        ]
                    },
                    'runtime_config': {
                        'version': '2.1',
                        'properties': {
                            'spark.executor.memory': metadata['execution_policy']['spark_executor_memory'],
                            'spark.driver.memory': metadata['execution_policy']['spark_driver_memory']
                        }
                    },
                    'environment_config': {
                        'execution_config': {
                            'service_account': Variable.get('dataproc_service_account'),
                            'subnetwork_uri': Variable.get('dataproc_subnet')
                        }
                    }
                }
            )
        
        # Task Group 3: Silver Transformation
        with TaskGroup(group_id='transform_group') as transform_group:
            silver_transform = DataprocCreateBatchOperator(
                task_id='silver_transform',
                # ... similar configuration
            )
        
        # Task Group 4: Data Quality
        with TaskGroup(group_id='quality_group') as quality_group:
            run_dq_checks = DataprocCreateBatchOperator(
                task_id='run_dq_checks',
                # ... data quality spark job
            )
        
        # Define task dependencies
        sensor_group >> ingest_group >> transform_group >> quality_group
    
    return dag
```

### CDC Ingestion DAG Template

```python
# dag_templates/ingestion/cdc_ingest_dag.py
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor
from airflow.operators.python import BranchPythonOperator

def create_cdc_ingest_dag(pipeline_config: dict, metadata: dict) -> DAG:
    """
    Creates a CDC ingestion DAG supporting:
    - Debezium CDC events
    - Google Datastream
    - Oracle GoldenGate
    
    Implements SCD Type 2 history tracking in Silver layer.
    """
    
    dag_id = f"{pipeline_config['pipeline_name']}_cdc_ingest"
    
    with create_base_dag(
        dag_id, 
        pipeline_config,
        schedule_interval='*/5 * * * *'  # Every 5 minutes for near-real-time
    ) as dag:
        
        # Check for pending CDC events
        check_events = PubSubPullSensor(
            task_id='check_cdc_events',
            project_id=Variable.get('gcp_project'),
            subscription=metadata['source_config']['pubsub_subscription'],
            max_messages=1,
            mode='reschedule',
            poke_interval=60
        )
        
        # Branch based on event count
        def _check_event_volume(**context):
            message_count = context['ti'].xcom_pull(task_ids='check_cdc_events')
            if message_count and message_count > 0:
                return 'process_cdc'
            return 'skip_processing'
        
        branch = BranchPythonOperator(
            task_id='branch_on_events',
            python_callable=_check_event_volume
        )
        
        # Process CDC events with SCD2
        process_cdc = DataprocCreateBatchOperator(
            task_id='process_cdc',
            batch={
                'pyspark_batch': {
                    'main_python_file_uri': f"gs://{Variable.get('spark_scripts_bucket')}/cdc_merge.py",
                    'args': [
                        '--pipeline_id', str(pipeline_config['pipeline_id']),
                        '--cdc_mode', metadata['source_config']['cdc_mode'],
                        '--enable_scd2', str(pipeline_config.get('enable_scd2', True))
                    ]
                }
            }
        )
        
        # Apply SCD2 to Silver
        apply_scd2 = DataprocCreateBatchOperator(
            task_id='apply_scd2',
            batch={
                'pyspark_batch': {
                    'main_python_file_uri': f"gs://{Variable.get('spark_scripts_bucket')}/scd2_apply.py",
                    'args': [
                        '--pipeline_id', str(pipeline_config['pipeline_id']),
                        '--merge_keys', ','.join(metadata['target_config']['merge_keys'])
                    ]
                }
            }
        )
        
        check_events >> branch >> [process_cdc, skip_processing]
        process_cdc >> apply_scd2
    
    return dag
```

---

## 14. PySpark Template Strategy

### Template Organization

```
spark_templates/
├── common/
│   ├── spark_session.py         # Session initialization
│   ├── iceberg_utils.py         # Iceberg helper functions
│   ├── bigquery_utils.py        # BigQuery helper functions
│   ├── schema_utils.py          # Schema handling
│   ├── dq_utils.py              # Data quality utilities
│   └── logging_utils.py         # Structured logging
├── ingestion/
│   ├── bronze_ingest.py         # Raw → Bronze
│   ├── file_parsers/
│   │   ├── csv_parser.py
│   │   ├── json_parser.py
│   │   ├── fixed_width_parser.py
│   │   └── copybook_parser.py
│   └── source_readers/
│       ├── gcs_reader.py
│       ├── jdbc_reader.py
│       └── kafka_reader.py
├── transformation/
│   ├── silver_transform.py      # Bronze → Silver
│   ├── type_casting.py          # Data type enforcement
│   └── standardization.py       # Business rule application
├── cdc/
│   ├── cdc_merge.py             # CDC event processing
│   ├── scd2_apply.py            # SCD Type 2 implementation
│   └── debezium_parser.py       # Debezium format parsing
├── modeling/
│   ├── data_vault/
│   │   ├── hub_builder.py
│   │   ├── link_builder.py
│   │   └── satellite_builder.py
│   └── star_schema/
│       ├── fact_builder.py
│       └── dimension_builder.py
└── gold/
    ├── gold_load_bq.py          # Silver/Modeling → BigQuery
    └── aggregation_builder.py   # KPI/Aggregation generation
```

### Common Spark Session Setup

```python
# spark_templates/common/spark_session.py
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import logging

def create_spark_session(
    app_name: str,
    pipeline_config: dict,
    enable_iceberg: bool = True,
    enable_bigquery: bool = False
) -> SparkSession:
    """
    Creates a configured Spark session for Dataproc Serverless.
    
    Args:
        app_name: Application name for Spark UI
        pipeline_config: Metadata configuration
        enable_iceberg: Enable Iceberg catalog
        enable_bigquery: Enable BigQuery connector
    """
    
    conf = SparkConf()
    
    # Base configuration
    conf.set("spark.app.name", app_name)
    conf.set("spark.sql.adaptive.enabled", "true")
    conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    conf.set("spark.sql.shuffle.partitions", "auto")
    
    # Iceberg configuration
    if enable_iceberg:
        conf.set("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        conf.set("spark.sql.catalog.iceberg.catalog-impl", "org.apache.iceberg.gcp.biglake.BigLakeCatalog")
        conf.set("spark.sql.catalog.iceberg.gcp_project", pipeline_config['project_id'])
        conf.set("spark.sql.catalog.iceberg.gcp_location", pipeline_config['region'])
        conf.set("spark.sql.catalog.iceberg.warehouse", f"gs://{pipeline_config['warehouse_bucket']}/iceberg")
        conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        
        # Iceberg write optimization
        conf.set("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.gcp.gcs.GCSFileIO")
        conf.set("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
    
    # BigQuery configuration
    if enable_bigquery:
        conf.set("spark.datasource.bigquery.materializationDataset", pipeline_config['staging_dataset'])
        conf.set("spark.datasource.bigquery.project", pipeline_config['project_id'])
    
    # Memory and performance tuning
    conf.set("spark.sql.files.maxPartitionBytes", "134217728")  # 128MB
    conf.set("spark.sql.broadcastTimeout", "600")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    
    # Configure logging
    spark.sparkContext.setLogLevel("WARN")
    logger = logging.getLogger(__name__)
    logger.info(f"Spark session created: {app_name}")
    
    return spark
```

### Bronze Ingestion Template

```python
# spark_templates/ingestion/bronze_ingest.py
import argparse
import json
import uuid
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, input_file_name,
    md5, concat_ws, when, coalesce
)
from pyspark.sql.types import StringType
from common.spark_session import create_spark_session
from common.logging_utils import setup_logging, log_metrics

def parse_args():
    parser = argparse.ArgumentParser(description='Bronze layer ingestion')
    parser.add_argument('--pipeline_id', required=True, type=int)
    parser.add_argument('--run_id', required=True, type=str)
    parser.add_argument('--reporting_date', required=True, type=str)
    parser.add_argument('--metadata_json', required=True, type=str)
    return parser.parse_args()

def add_system_columns(df: DataFrame, run_id: str, reporting_date: str, source_system: str) -> DataFrame:
    """Add mandatory system columns to DataFrame."""
    
    # Generate record UUID
    df = df.withColumn("record_uuid", lit(str(uuid.uuid4())))
    
    # Add system columns
    df = df.withColumn("run_id", lit(run_id))
    df = df.withColumn("reporting_date", lit(reporting_date).cast("date"))
    df = df.withColumn("ingestion_ts", current_timestamp())
    df = df.withColumn("source_system", lit(source_system))
    df = df.withColumn("__source_file", input_file_name())
    
    # Generate row hash for deduplication (excluding system columns)
    business_columns = [c for c in df.columns if not c.startswith("__") and c not in 
                        ["run_id", "record_uuid", "reporting_date", "ingestion_ts", "source_system"]]
    df = df.withColumn("__row_hash", md5(concat_ws("||", *[coalesce(col(c).cast("string"), lit("NULL")) for c in business_columns])))
    
    return df

def cast_all_to_string(df: DataFrame) -> DataFrame:
    """Cast all columns to STRING type for Bronze layer (schema-on-read)."""
    for column in df.columns:
        if not column.startswith("__") and column not in ["reporting_date", "ingestion_ts"]:
            df = df.withColumn(column, col(column).cast(StringType()))
    return df

def deduplicate(df: DataFrame, primary_keys: list) -> DataFrame:
    """Remove duplicates based on primary keys, keeping the latest record."""
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number, desc
    
    if not primary_keys:
        return df.dropDuplicates(["__row_hash"])
    
    window = Window.partitionBy(*primary_keys).orderBy(desc("ingestion_ts"))
    df = df.withColumn("__row_num", row_number().over(window))
    df = df.filter(col("__row_num") == 1).drop("__row_num")
    
    return df

def write_to_iceberg(df: DataFrame, spark: SparkSession, table_name: str, partition_cols: list):
    """Write DataFrame to Iceberg table with merge-on-read optimization."""
    
    # Check if table exists
    table_exists = spark.catalog.tableExists(f"iceberg.bronze.{table_name}")
    
    if table_exists:
        # Merge new data with existing
        df.createOrReplaceTempView("new_data")
        
        merge_sql = f"""
        MERGE INTO iceberg.bronze.{table_name} AS target
        USING new_data AS source
        ON target.__row_hash = source.__row_hash
        WHEN NOT MATCHED THEN INSERT *
        """
        spark.sql(merge_sql)
    else:
        # Create new table
        df.writeTo(f"iceberg.bronze.{table_name}") \
            .partitionedBy(*partition_cols) \
            .tableProperty("write.format.default", "parquet") \
            .tableProperty("write.parquet.compression-codec", "zstd") \
            .tableProperty("write.metadata.delete-after-commit.enabled", "true") \
            .tableProperty("write.metadata.previous-versions-max", "100") \
            .createOrReplace()

def main():
    args = parse_args()
    logger = setup_logging(f"bronze_ingest_{args.pipeline_id}")
    
    # Load metadata
    metadata = json.loads(args.metadata_json)
    
    # Create Spark session
    spark = create_spark_session(
        app_name=f"bronze_ingest_{metadata['pipeline_name']}",
        pipeline_config=metadata,
        enable_iceberg=True
    )
    
    try:
        # Read source data
        source_config = metadata['source_config']
        
        if source_config['file_format'] == 'csv':
            df = spark.read.option("header", source_config.get('has_header', True)) \
                           .option("delimiter", source_config.get('delimiter', ',')) \
                           .option("encoding", source_config.get('encoding', 'UTF-8')) \
                           .csv(source_config['landing_path'])
        elif source_config['file_format'] == 'json':
            df = spark.read.option("multiLine", source_config.get('multi_line', False)) \
                           .json(source_config['landing_path'])
        elif source_config['file_format'] == 'parquet':
            df = spark.read.parquet(source_config['landing_path'])
        else:
            raise ValueError(f"Unsupported format: {source_config['file_format']}")
        
        # Log input metrics
        input_count = df.count()
        log_metrics(logger, "input_records", input_count)
        
        # Apply transformations
        df = cast_all_to_string(df)
        df = add_system_columns(df, args.run_id, args.reporting_date, source_config['source_system'])
        df = deduplicate(df, metadata.get('schema_version', {}).get('primary_keys', []))
        
        # Write to Iceberg
        write_to_iceberg(
            df=df,
            spark=spark,
            table_name=metadata['pipeline_name'],
            partition_cols=["reporting_date"]
        )
        
        # Log output metrics
        output_count = spark.table(f"iceberg.bronze.{metadata['pipeline_name']}").count()
        log_metrics(logger, "output_records", output_count)
        
        logger.info(f"Bronze ingestion completed: {input_count} records processed, {output_count} records in table")
        
    except Exception as e:
        logger.error(f"Bronze ingestion failed: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
```

### CDC Merge Template

```python
# spark_templates/cdc/cdc_merge.py
import argparse
import json
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, when, coalesce, current_timestamp,
    from_json, get_json_object, window, max as spark_max
)
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

def parse_debezium_event(df: DataFrame, schema: StructType) -> DataFrame:
    """
    Parse Debezium CDC events from Kafka/Pub/Sub.
    
    Debezium event structure:
    {
        "payload": {
            "before": {...},  // Previous state (null for INSERT)
            "after": {...},   // Current state (null for DELETE)
            "op": "c|u|d|r",  // Operation: create, update, delete, read(snapshot)
            "ts_ms": 1234567890,
            "source": {
                "table": "...",
                "lsn": 12345
            }
        }
    }
    """
    
    return df.select(
        get_json_object(col("value"), "$.payload.op").alias("__cdc_operation"),
        from_json(get_json_object(col("value"), "$.payload.before"), schema).alias("before"),
        from_json(get_json_object(col("value"), "$.payload.after"), schema).alias("after"),
        get_json_object(col("value"), "$.payload.ts_ms").cast("long").alias("__cdc_timestamp_ms"),
        get_json_object(col("value"), "$.payload.source.lsn").cast("long").alias("__cdc_lsn")
    ).select(
        # Use 'after' for INSERT/UPDATE, 'before' for DELETE
        when(col("__cdc_operation") == "d", col("before")).otherwise(col("after")).alias("data"),
        col("__cdc_operation"),
        col("__cdc_timestamp_ms"),
        col("__cdc_lsn"),
        when(col("__cdc_operation") == "d", lit(True)).otherwise(lit(False)).alias("__is_deleted")
    ).select(
        col("data.*"),
        col("__cdc_operation"),
        col("__cdc_timestamp_ms"),
        col("__cdc_lsn"),
        col("__is_deleted")
    )

def apply_cdc_changes(
    spark: SparkSession,
    cdc_df: DataFrame,
    target_table: str,
    merge_keys: list,
    enable_scd2: bool = True
) -> None:
    """
    Apply CDC changes to target Iceberg table.
    
    Args:
        spark: Spark session
        cdc_df: DataFrame with CDC events
        target_table: Target Iceberg table name
        merge_keys: Columns to use for merge
        enable_scd2: Enable SCD Type 2 history tracking
    """
    
    cdc_df.createOrReplaceTempView("cdc_changes")
    
    if enable_scd2:
        # SCD Type 2: Close existing records and insert new versions
        spark.sql(f"""
        MERGE INTO iceberg.silver.{target_table} AS target
        USING (
            SELECT *, 
                   ROW_NUMBER() OVER (
                       PARTITION BY {', '.join(merge_keys)} 
                       ORDER BY __cdc_lsn DESC
                   ) as __rn
            FROM cdc_changes
        ) AS source
        ON {' AND '.join([f'target.{k} = source.{k}' for k in merge_keys])}
           AND target.__effective_to IS NULL
           AND source.__rn = 1
        
        -- Close existing record
        WHEN MATCHED AND source.__is_deleted = false THEN UPDATE SET
            __effective_to = current_timestamp(),
            __is_current = false
        
        -- Mark as deleted
        WHEN MATCHED AND source.__is_deleted = true THEN UPDATE SET
            __effective_to = current_timestamp(),
            __is_current = false,
            __is_deleted = true
        """)
        
        # Insert new versions for non-deletes
        spark.sql(f"""
        INSERT INTO iceberg.silver.{target_table}
        SELECT 
            source.*,
            current_timestamp() as __effective_from,
            NULL as __effective_to,
            true as __is_current
        FROM (
            SELECT *, 
                   ROW_NUMBER() OVER (
                       PARTITION BY {', '.join(merge_keys)} 
                       ORDER BY __cdc_lsn DESC
                   ) as __rn
            FROM cdc_changes
            WHERE __is_deleted = false
        ) source
        WHERE source.__rn = 1
        """)
    else:
        # Simple MERGE without SCD2
        merge_condition = ' AND '.join([f'target.{k} = source.{k}' for k in merge_keys])
        
        # Get all columns except system columns
        update_cols = [c for c in cdc_df.columns if not c.startswith("__cdc")]
        update_set = ', '.join([f'target.{c} = source.{c}' for c in update_cols])
        
        spark.sql(f"""
        MERGE INTO iceberg.silver.{target_table} AS target
        USING cdc_changes AS source
        ON {merge_condition}
        WHEN MATCHED AND source.__is_deleted = true THEN DELETE
        WHEN MATCHED AND source.__is_deleted = false THEN UPDATE SET {update_set}
        WHEN NOT MATCHED AND source.__is_deleted = false THEN INSERT *
        """)
```

---

## 15. Change Data Capture (CDC) Patterns

### Supported CDC Sources

| Source | Tool | Protocol | Latency |
|--------|------|----------|---------|
| MySQL | Debezium | Binlog → Kafka | Near real-time |
| PostgreSQL | Debezium | WAL → Kafka | Near real-time |
| Oracle | Debezium/GoldenGate | LogMiner/Redo → Kafka | Near real-time |
| SQL Server | Debezium | CT → Kafka | Near real-time |
| Cloud SQL | Datastream | Log-based → Pub/Sub | Near real-time |
| AlloyDB | Datastream | Log-based → Pub/Sub | Near real-time |

### CDC Event Schema (Debezium Format)

```json
{
  "schema": {...},
  "payload": {
    "before": {
      "customer_id": 1001,
      "email": "old@email.com",
      "name": "John Doe"
    },
    "after": {
      "customer_id": 1001,
      "email": "new@email.com",
      "name": "John Doe"
    },
    "source": {
      "version": "2.4.0.Final",
      "connector": "postgresql",
      "name": "dbserver1",
      "ts_ms": 1704067200000,
      "snapshot": "false",
      "db": "customers_db",
      "sequence": "[\"26192584\",\"26192584\"]",
      "schema": "public",
      "table": "customers",
      "txId": 555,
      "lsn": 26192584,
      "xmin": null
    },
    "op": "u",
    "ts_ms": 1704067200123,
    "transaction": null
  }
}
```

### SCD Type 2 Implementation

```python
def apply_scd2(
    current_df: DataFrame,
    incoming_df: DataFrame,
    key_columns: list,
    tracked_columns: list
) -> DataFrame:
    """
    Apply SCD Type 2 logic to track history.
    
    Columns added:
    - __effective_from: When this version became active
    - __effective_to: When this version was superseded (NULL for current)
    - __is_current: Boolean flag for current version
    - __version: Incrementing version number
    """
    
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number, desc, max as spark_max
    
    # Get current version numbers
    current_versions = current_df.filter(col("__is_current") == True) \
        .select(*key_columns, "__version")
    
    # Join incoming with current versions
    incoming_with_version = incoming_df.join(
        current_versions,
        key_columns,
        "left"
    ).withColumn(
        "__new_version",
        coalesce(col("__version"), lit(0)) + 1
    )
    
    # Detect actual changes
    change_detection_cols = [
        when(
            col(f"incoming.{c}") != col(f"current.{c}"),
            lit(1)
        ).otherwise(lit(0))
        for c in tracked_columns
    ]
    
    changed_records = incoming_with_version.withColumn(
        "__has_changes",
        sum(*change_detection_cols) > 0
    ).filter(col("__has_changes") == True)
    
    # Close existing records
    closed_records = current_df.filter(col("__is_current") == True) \
        .join(changed_records.select(*key_columns), key_columns, "inner") \
        .withColumn("__effective_to", current_timestamp()) \
        .withColumn("__is_current", lit(False))
    
    # Create new versions
    new_versions = changed_records.select(
        *[col(c) for c in incoming_df.columns],
        current_timestamp().alias("__effective_from"),
        lit(None).cast(TimestampType()).alias("__effective_to"),
        lit(True).alias("__is_current"),
        col("__new_version").alias("__version")
    )
    
    # Combine: unchanged current + closed + new versions
    unchanged = current_df.join(
        changed_records.select(*key_columns),
        key_columns,
        "left_anti"
    )
    
    return unchanged.union(closed_records).union(new_versions)
```

---

## 16. Data Quality Framework

### Quality Rule Types

| Rule Type | Description | Example |
|-----------|-------------|---------|
| `not_null` | Column cannot be null | `customer_id NOT NULL` |
| `unique` | Column values must be unique | `email UNIQUE` |
| `referential` | FK exists in parent table | `product_id IN products.id` |
| `range` | Numeric value within bounds | `age BETWEEN 0 AND 150` |
| `regex` | String matches pattern | `email LIKE '%@%'` |
| `custom_sql` | Arbitrary SQL expression | `total = subtotal + tax` |
| `freshness` | Data not older than threshold | `MAX(updated_at) > NOW() - 24h` |
| `volume` | Row count within expected range | `COUNT(*) > 1000` |
| `completeness` | Percentage of non-null values | `COUNT(email)/COUNT(*) > 0.95` |

### Quality Check Implementation

```python
# spark_templates/common/dq_utils.py
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, when, sum as spark_sum, max as spark_max
from typing import List, Dict
import json

class DataQualityChecker:
    """
    Data quality validation framework.
    
    Executes rules defined in metadata and produces:
    1. Quality metrics for each rule
    2. Failed records for quarantine
    3. Summary statistics
    """
    
    def __init__(self, spark: SparkSession, pipeline_id: int, rules: List[Dict]):
        self.spark = spark
        self.pipeline_id = pipeline_id
        self.rules = rules
        self.results = []
    
    def run_checks(self, df: DataFrame, layer: str) -> Dict:
        """
        Run all quality checks for the specified layer.
        
        Returns:
            {
                "passed": bool,
                "total_rules": int,
                "passed_rules": int,
                "failed_rules": int,
                "critical_failures": int,
                "results": [...]
            }
        """
        layer_rules = [r for r in self.rules if r['layer'] == layer]
        
        for rule in layer_rules:
            result = self._execute_rule(df, rule)
            self.results.append(result)
        
        critical_failures = len([r for r in self.results 
                                  if r['severity'] == 'critical' and not r['passed']])
        
        return {
            "passed": critical_failures == 0,
            "total_rules": len(layer_rules),
            "passed_rules": len([r for r in self.results if r['passed']]),
            "failed_rules": len([r for r in self.results if not r['passed']]),
            "critical_failures": critical_failures,
            "results": self.results
        }
    
    def _execute_rule(self, df: DataFrame, rule: Dict) -> Dict:
        """Execute a single quality rule."""
        
        rule_type = rule['rule_type']
        config = rule['rule_config_json']
        threshold = rule.get('threshold_pct', 100)
        
        if rule_type == 'not_null':
            return self._check_not_null(df, config, rule, threshold)
        elif rule_type == 'unique':
            return self._check_unique(df, config, rule, threshold)
        elif rule_type == 'range':
            return self._check_range(df, config, rule, threshold)
        elif rule_type == 'regex':
            return self._check_regex(df, config, rule, threshold)
        elif rule_type == 'custom_sql':
            return self._check_custom_sql(df, config, rule, threshold)
        elif rule_type == 'freshness':
            return self._check_freshness(df, config, rule)
        elif rule_type == 'volume':
            return self._check_volume(df, config, rule)
        else:
            raise ValueError(f"Unknown rule type: {rule_type}")
    
    def _check_not_null(self, df: DataFrame, config: Dict, rule: Dict, threshold: float) -> Dict:
        """Check columns for null values."""
        columns = config['columns']
        total_rows = df.count()
        
        null_counts = {}
        for col_name in columns:
            null_count = df.filter(col(col_name).isNull()).count()
            null_counts[col_name] = null_count
        
        total_nulls = sum(null_counts.values())
        pass_rate = ((total_rows * len(columns) - total_nulls) / (total_rows * len(columns))) * 100
        
        return {
            "rule_id": rule['rule_id'],
            "rule_name": rule['rule_name'],
            "rule_type": "not_null",
            "passed": pass_rate >= threshold,
            "pass_rate": pass_rate,
            "threshold": threshold,
            "severity": rule['severity'],
            "details": null_counts
        }
    
    def _check_unique(self, df: DataFrame, config: Dict, rule: Dict, threshold: float) -> Dict:
        """Check column uniqueness."""
        columns = config['columns']
        total_rows = df.count()
        distinct_rows = df.select(*columns).distinct().count()
        
        uniqueness_rate = (distinct_rows / total_rows) * 100 if total_rows > 0 else 100
        
        return {
            "rule_id": rule['rule_id'],
            "rule_name": rule['rule_name'],
            "rule_type": "unique",
            "passed": uniqueness_rate >= threshold,
            "pass_rate": uniqueness_rate,
            "threshold": threshold,
            "severity": rule['severity'],
            "details": {
                "total_rows": total_rows,
                "distinct_rows": distinct_rows,
                "duplicate_rows": total_rows - distinct_rows
            }
        }
    
    def _check_range(self, df: DataFrame, config: Dict, rule: Dict, threshold: float) -> Dict:
        """Check numeric values within range."""
        column = config['column']
        min_val = config.get('min')
        max_val = config.get('max')
        
        total_rows = df.count()
        
        condition = col(column).isNotNull()
        if min_val is not None:
            condition = condition & (col(column) >= min_val)
        if max_val is not None:
            condition = condition & (col(column) <= max_val)
        
        valid_rows = df.filter(condition).count()
        pass_rate = (valid_rows / total_rows) * 100 if total_rows > 0 else 100
        
        return {
            "rule_id": rule['rule_id'],
            "rule_name": rule['rule_name'],
            "rule_type": "range",
            "passed": pass_rate >= threshold,
            "pass_rate": pass_rate,
            "threshold": threshold,
            "severity": rule['severity'],
            "details": {
                "column": column,
                "min": min_val,
                "max": max_val,
                "valid_rows": valid_rows,
                "total_rows": total_rows
            }
        }
    
    def get_failed_records(self, df: DataFrame, rule: Dict) -> DataFrame:
        """Extract records that failed a specific rule for quarantine."""
        # Implementation depends on rule type
        pass
    
    def write_results_to_bigquery(self, project_id: str, dataset: str):
        """Persist quality results to BigQuery for dashboarding."""
        results_df = self.spark.createDataFrame(self.results)
        results_df.write \
            .format("bigquery") \
            .option("table", f"{project_id}.{dataset}.dq_results") \
            .option("temporaryGcsBucket", f"{project_id}-temp") \
            .mode("append") \
            .save()
```

---

## 17. CI/CD Pipeline

### Pipeline Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         SOURCE REPOSITORY                                │
│                    (This Repository)                                     │
│   • Templates (DAG, Spark)                                              │
│   • Metadata Model                                                       │
│   • Agent Logic                                                          │
│   • Unit Tests                                                           │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                            Agent Commits
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                       DEPLOYMENT REPOSITORY                              │
│                   (Generated Artifacts)                                  │
│   • Generated DAGs                                                       │
│   • Generated Spark Jobs                                                │
│   • Metadata SQL                                                         │
│   • Configuration Files                                                  │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                            CI/CD Trigger
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                        CI/CD PIPELINE                                    │
│                      (Cloud Build)                                       │
│                                                                          │
│   ┌─────────────┐   ┌─────────────┐   ┌─────────────┐   ┌──────────┐  │
│   │   Lint &    │ → │  Unit       │ → │  DAG        │ → │  Deploy  │  │
│   │   Validate  │   │  Tests      │   │  Import     │   │  to      │  │
│   │             │   │             │   │  Test       │   │  Composer│  │
│   └─────────────┘   └─────────────┘   └─────────────┘   └──────────┘  │
│                                                                          │
│   ┌─────────────┐   ┌─────────────┐   ┌─────────────┐                   │
│   │  Security   │ → │  Notify     │ → │  Update     │                   │
│   │  Scan       │   │  Jira       │   │  Metadata   │                   │
│   └─────────────┘   └─────────────┘   └─────────────┘                   │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                            Deployment
                                    │
               ┌────────────────────┼────────────────────┐
               │                    │                    │
               ▼                    ▼                    ▼
        ┌───────────┐        ┌───────────┐        ┌───────────┐
        │    DEV    │   →    │    QA     │   →    │   PROD    │
        │ (Auto)    │        │(Approval) │        │(Approval) │
        └───────────┘        └───────────┘        └───────────┘
```

### CI/CD Stages

| Stage | Description | Failure Action |
|-------|-------------|----------------|
| **Lint** | Python syntax, style checks | Block deployment |
| **Unit Test** | Test transformation logic | Block deployment |
| **DAG Import** | Validate DAG syntax | Block deployment |
| **Security Scan** | Check for secrets, vulnerabilities | Block deployment |
| **Deploy DEV** | Automatic deployment to DEV | Alert team |
| **Integration Test** | Run test data through pipeline | Block QA promotion |
| **Deploy QA** | Requires approval for schema changes | Alert team |
| **Deploy PROD** | Always requires approval | Escalate |

---

## 18. Environment Configuration

### Environment Bootstrapping Guide

This section provides step-by-step instructions for bootstrapping each environment from scratch.

#### Prerequisites

```bash
# Required tools
gcloud version >= 450.0.0
terraform version >= 1.5.0
kubectl version >= 1.28

# Required permissions
roles/owner OR combination of:
  - roles/composer.admin
  - roles/dataproc.admin
  - roles/bigquery.admin
  - roles/storage.admin
  - roles/cloudsql.admin
  - roles/iam.securityAdmin
  - roles/secretmanager.admin
```

#### GCP Project Structure

```
Organization: company.com
└── Folder: Data Platform
    ├── Project: data-platform-dev
    │   ├── Composer (DEV)
    │   ├── Cloud SQL (metadata-dev)
    │   └── GCS Buckets (dev)
    │
    ├── Project: data-platform-qa
    │   ├── Composer (QA)
    │   ├── Cloud SQL (metadata-qa)
    │   └── GCS Buckets (qa)
    │
    └── Project: data-platform-prod
        ├── Composer (PROD)
        ├── Cloud SQL (metadata-prod)
        └── GCS Buckets (prod)
```

#### Step 1: Create GCP Projects

```bash
#!/bin/bash
# bootstrap/01_create_projects.sh

BILLING_ACCOUNT="your-billing-account-id"
ORG_ID="your-org-id"
FOLDER_ID="your-folder-id"

# Create projects
for ENV in dev qa prod; do
  gcloud projects create "data-platform-${ENV}" \
    --folder="${FOLDER_ID}" \
    --name="Data Platform ${ENV^^}"
  
  gcloud billing projects link "data-platform-${ENV}" \
    --billing-account="${BILLING_ACCOUNT}"
done
```

#### Step 2: Enable Required APIs

```bash
#!/bin/bash
# bootstrap/02_enable_apis.sh

APIS=(
  "composer.googleapis.com"
  "dataproc.googleapis.com"
  "bigquery.googleapis.com"
  "storage.googleapis.com"
  "sqladmin.googleapis.com"
  "secretmanager.googleapis.com"
  "pubsub.googleapis.com"
  "cloudbuild.googleapis.com"
  "artifactregistry.googleapis.com"
  "dataplex.googleapis.com"
  "biglake.googleapis.com"
  "datastream.googleapis.com"
)

for ENV in dev qa prod; do
  PROJECT="data-platform-${ENV}"
  for API in "${APIS[@]}"; do
    gcloud services enable "${API}" --project="${PROJECT}"
  done
done
```

#### Step 3: Create Service Accounts

```bash
#!/bin/bash
# bootstrap/03_create_service_accounts.sh

for ENV in dev qa prod; do
  PROJECT="data-platform-${ENV}"
  
  # Agent Service Account
  gcloud iam service-accounts create "agent-sa" \
    --project="${PROJECT}" \
    --display-name="Data Engineering Agent"
  
  # Dataproc Service Account  
  gcloud iam service-accounts create "dataproc-sa" \
    --project="${PROJECT}" \
    --display-name="Dataproc Workers"
  
  # Composer Service Account (auto-created, but set permissions)
  
  # Assign roles
  gcloud projects add-iam-policy-binding "${PROJECT}" \
    --member="serviceAccount:agent-sa@${PROJECT}.iam.gserviceaccount.com" \
    --role="roles/cloudsql.client"
  
  gcloud projects add-iam-policy-binding "${PROJECT}" \
    --member="serviceAccount:agent-sa@${PROJECT}.iam.gserviceaccount.com" \
    --role="roles/pubsub.subscriber"
  
  gcloud projects add-iam-policy-binding "${PROJECT}" \
    --member="serviceAccount:dataproc-sa@${PROJECT}.iam.gserviceaccount.com" \
    --role="roles/dataproc.worker"
  
  gcloud projects add-iam-policy-binding "${PROJECT}" \
    --member="serviceAccount:dataproc-sa@${PROJECT}.iam.gserviceaccount.com" \
    --role="roles/bigquery.dataEditor"
done
```

#### Step 4: Create GCS Buckets

```bash
#!/bin/bash
# bootstrap/04_create_buckets.sh

REGION="us-central1"

for ENV in dev qa prod; do
  PROJECT="data-platform-${ENV}"
  
  # Bucket naming: {project}-{purpose}-{env}
  BUCKETS=(
    "${PROJECT}-raw"
    "${PROJECT}-bronze"
    "${PROJECT}-silver"
    "${PROJECT}-gold-staging"
    "${PROJECT}-spark-scripts"
    "${PROJECT}-spark-temp"
    "${PROJECT}-composer-dags"
    "${PROJECT}-intents"
  )
  
  for BUCKET in "${BUCKETS[@]}"; do
    gcloud storage buckets create "gs://${BUCKET}" \
      --project="${PROJECT}" \
      --location="${REGION}" \
      --uniform-bucket-level-access
    
    # Set lifecycle for temp buckets
    if [[ "${BUCKET}" == *"-temp"* ]]; then
      gsutil lifecycle set lifecycle_7days.json "gs://${BUCKET}"
    fi
  done
done
```

#### Step 5: Create Cloud SQL (Metadata Database)

```bash
#!/bin/bash
# bootstrap/05_create_cloudsql.sh

REGION="us-central1"

# DEV - Minimal (shared CPU)
gcloud sql instances create metadata-db-dev \
  --project="data-platform-dev" \
  --database-version=POSTGRES_15 \
  --tier=db-f1-micro \
  --region="${REGION}" \
  --storage-size=10GB \
  --storage-type=SSD \
  --no-backup

# QA - Small dedicated
gcloud sql instances create metadata-db-qa \
  --project="data-platform-qa" \
  --database-version=POSTGRES_15 \
  --tier=db-g1-small \
  --region="${REGION}" \
  --storage-size=20GB \
  --storage-type=SSD \
  --backup-start-time="02:00"

# PROD - Production grade
gcloud sql instances create metadata-db-prod \
  --project="data-platform-prod" \
  --database-version=POSTGRES_15 \
  --tier=db-custom-2-4096 \
  --region="${REGION}" \
  --storage-size=50GB \
  --storage-type=SSD \
  --availability-type=REGIONAL \
  --backup-start-time="02:00" \
  --enable-point-in-time-recovery

# Create database and user
for ENV in dev qa prod; do
  gcloud sql databases create metadata \
    --instance="metadata-db-${ENV}" \
    --project="data-platform-${ENV}"
  
  gcloud sql users create metadata_user \
    --instance="metadata-db-${ENV}" \
    --project="data-platform-${ENV}" \
    --password="$(openssl rand -base64 24)"
done
```

#### Step 6: Create Cloud Composer Environment

```bash
#!/bin/bash
# bootstrap/06_create_composer.sh

REGION="us-central1"

# DEV Composer (minimal)
gcloud composer environments create composer-dev \
  --project="data-platform-dev" \
  --location="${REGION}" \
  --environment-size=small \
  --image-version="composer-3-airflow-2.10.2" \
  --scheduler-cpu=0.5 \
  --scheduler-memory=2 \
  --scheduler-storage=1 \
  --scheduler-count=1 \
  --web-server-cpu=0.5 \
  --web-server-memory=2 \
  --web-server-storage=1 \
  --worker-cpu=0.5 \
  --worker-memory=2 \
  --worker-storage=1 \
  --min-workers=1 \
  --max-workers=3 \
  --triggerer-cpu=0.5 \
  --triggerer-memory=0.5 \
  --triggerer-count=1

# QA Composer (same as DEV)
gcloud composer environments create composer-qa \
  --project="data-platform-qa" \
  --location="${REGION}" \
  --environment-size=small \
  --image-version="composer-3-airflow-2.10.2" \
  # ... same as DEV

# PROD Composer (scaled up)
gcloud composer environments create composer-prod \
  --project="data-platform-prod" \
  --location="${REGION}" \
  --environment-size=medium \
  --image-version="composer-3-airflow-2.10.2" \
  --scheduler-cpu=2 \
  --scheduler-memory=8 \
  --scheduler-storage=5 \
  --scheduler-count=2 \
  --web-server-cpu=2 \
  --web-server-memory=8 \
  --web-server-storage=5 \
  --worker-cpu=2 \
  --worker-memory=8 \
  --worker-storage=5 \
  --min-workers=2 \
  --max-workers=6 \
  --triggerer-cpu=1 \
  --triggerer-memory=1 \
  --triggerer-count=2 \
  --enable-high-resilience
```

#### Step 7: Create BigQuery Datasets

```bash
#!/bin/bash
# bootstrap/07_create_bigquery.sh

for ENV in dev qa prod; do
  PROJECT="data-platform-${ENV}"
  
  # Create datasets for each domain
  DATASETS=(
    "bronze"
    "silver"
    "gold"
    "staging"
    "monitoring"
  )
  
  for DATASET in "${DATASETS[@]}"; do
    bq mk --project_id="${PROJECT}" \
      --dataset \
      --location=US \
      --description="Data Platform ${DATASET} layer" \
      "${PROJECT}:${DATASET}"
  done
done
```

#### Step 8: Deploy Metadata Schema

```bash
#!/bin/bash
# bootstrap/08_deploy_metadata.sh

for ENV in dev qa prod; do
  PROJECT="data-platform-${ENV}"
  INSTANCE="metadata-db-${ENV}"
  
  # Get Cloud SQL connection name
  CONNECTION=$(gcloud sql instances describe "${INSTANCE}" \
    --project="${PROJECT}" \
    --format='get(connectionName)')
  
  # Deploy DDL files in order
  for DDL_FILE in ddl/*.sql; do
    cloud-sql-proxy "${CONNECTION}" &
    PROXY_PID=$!
    sleep 5
    
    PGPASSWORD="${DB_PASSWORD}" psql \
      -h 127.0.0.1 \
      -U metadata_user \
      -d metadata \
      -f "${DDL_FILE}"
    
    kill $PROXY_PID
  done
done
```

#### Step 9: Create Pub/Sub Topics

```bash
#!/bin/bash
# bootstrap/09_create_pubsub.sh

for ENV in dev qa prod; do
  PROJECT="data-platform-${ENV}"
  
  # Intent events topic
  gcloud pubsub topics create pipeline-intents \
    --project="${PROJECT}"
  
  gcloud pubsub subscriptions create pipeline-intents-sub \
    --project="${PROJECT}" \
    --topic=pipeline-intents \
    --ack-deadline=600
  
  # Status events topic
  gcloud pubsub topics create pipeline-status \
    --project="${PROJECT}"
done
```

#### Step 10: Store Secrets

```bash
#!/bin/bash
# bootstrap/10_create_secrets.sh

for ENV in dev qa prod; do
  PROJECT="data-platform-${ENV}"
  
  # Database password
  echo -n "${DB_PASSWORD}" | gcloud secrets create metadata-db-password \
    --project="${PROJECT}" \
    --replication-policy="automatic" \
    --data-file=-
  
  # Jira API token
  echo -n "${JIRA_TOKEN}" | gcloud secrets create jira-api-token \
    --project="${PROJECT}" \
    --replication-policy="automatic" \
    --data-file=-
  
  # Grant access to service accounts
  gcloud secrets add-iam-policy-binding metadata-db-password \
    --project="${PROJECT}" \
    --member="serviceAccount:agent-sa@${PROJECT}.iam.gserviceaccount.com" \
    --role="roles/secretmanager.secretAccessor"
done
```

### Environment Matrix

| Environment | Purpose | Deployment | Approval | Data |
|-------------|---------|------------|----------|------|
| **DEV** | Development & testing | Automatic | None | Synthetic |
| **QA** | Quality assurance | On merge to `release/*` | Tech lead | Masked production |
| **PROD** | Production | On merge to `main` | Manager + 2nd reviewer | Real |

### Environment-Specific Configuration

```yaml
# config/environments/dev.yaml
environment: dev
gcp:
  project_id: "project-data-platform-dev"
  region: "us-central1"
  composer:
    environment_name: "composer-dev"
    dag_bucket: "gs://composer-dev-dags"
  dataproc:
    subnet: "projects/project-dev/regions/us-central1/subnetworks/dataproc-subnet"
    service_account: "dataproc-sa@project-dev.iam.gserviceaccount.com"
  bigquery:
    dataset_prefix: "dev_"
    location: "US"
  storage:
    raw_bucket: "data-platform-raw-dev"
    bronze_bucket: "data-platform-bronze-dev"
    silver_bucket: "data-platform-silver-dev"

agent:
  human_approval_required: false
  auto_deploy: true
  max_concurrent_pipelines: 5

spark:
  driver_memory: "2g"
  executor_memory: "4g"
  executor_cores: 2
  num_executors: 2

logging:
  level: "DEBUG"
  sink: "cloud_logging"
```

```yaml
# config/environments/prod.yaml
environment: prod
gcp:
  project_id: "project-data-platform-prod"
  region: "us-central1"
  composer:
    environment_name: "composer-prod"
    dag_bucket: "gs://composer-prod-dags"
  dataproc:
    subnet: "projects/project-prod/regions/us-central1/subnetworks/dataproc-subnet"
    service_account: "dataproc-sa@project-prod.iam.gserviceaccount.com"
  bigquery:
    dataset_prefix: ""
    location: "US"
  storage:
    raw_bucket: "data-platform-raw-prod"
    bronze_bucket: "data-platform-bronze-prod"
    silver_bucket: "data-platform-silver-prod"

agent:
  human_approval_required: true
  auto_deploy: false
  max_concurrent_pipelines: 20

spark:
  driver_memory: "8g"
  executor_memory: "16g"
  executor_cores: 4
  num_executors: 10
  dynamic_allocation: true

logging:
  level: "INFO"
  sink: "cloud_logging"
```

---

## 19. Security & Governance

### Security Controls

| Control | Implementation | Enforcement |
|---------|----------------|-------------|
| **Authentication** | Service accounts with minimal permissions | IAM policies |
| **Authorization** | RBAC via Dataplex policies | Row-level security in BigQuery |
| **Encryption at Rest** | CMEK for GCS, BigQuery | Organization policy |
| **Encryption in Transit** | TLS 1.3 for all connections | VPC Service Controls |
| **Secret Management** | Secret Manager with auto-rotation | Agent fetches at runtime |
| **Audit Logging** | All data access logged | Cloud Audit Logs |
| **Data Masking** | PII columns masked in non-PROD | DLP API integration |
| **Network Security** | Private IP for all services | VPC Service Controls |

### IAM Roles

```yaml
# Minimum required roles per service account

# Agent Service Account
agent-sa@project.iam.gserviceaccount.com:
  - roles/cloudsql.client        # Metadata database access
  - roles/pubsub.subscriber      # Event consumption
  - roles/source.reader          # Git repository access
  - roles/secretmanager.accessor # Secret retrieval
  - roles/logging.logWriter      # Audit logging

# Dataproc Service Account
dataproc-sa@project.iam.gserviceaccount.com:
  - roles/dataproc.worker                # Dataproc operations
  - roles/storage.objectAdmin            # GCS read/write
  - roles/bigquery.dataEditor            # BigQuery write
  - roles/bigquery.jobUser               # BigQuery jobs
  - roles/biglake.admin                  # Iceberg catalog

# Composer Service Account
composer-sa@project.iam.gserviceaccount.com:
  - roles/composer.worker                # Composer operations
  - roles/dataproc.editor                # Submit Spark jobs
  - roles/storage.objectViewer           # DAG bucket access
```

### Data Lineage

```python
# Lineage tracking integration with Dataplex
from google.cloud import dataplex_v1

def record_lineage(
    pipeline_id: str,
    source_tables: list,
    target_table: str,
    transformation_logic: str
):
    """Record data lineage in Dataplex Catalog."""
    
    client = dataplex_v1.DataplexServiceClient()
    
    lineage_event = {
        "start_time": datetime.utcnow().isoformat(),
        "end_time": datetime.utcnow().isoformat(),
        "links": [
            {
                "source": {"fully_qualified_name": f"bigquery:{src}"}
                for src in source_tables
            } + [{
                "target": {"fully_qualified_name": f"bigquery:{target_table}"}
            }]
        ],
        "process": {
            "name": f"pipeline/{pipeline_id}",
            "attributes": {
                "transformation": transformation_logic
            }
        }
    }
    
    # Record lineage event
    client.create_lineage_event(parent=f"projects/{project}/locations/{region}", lineage_event=lineage_event)
```

---

## 20. Observability & Monitoring

### Metrics Collection

| Metric | Source | Alert Threshold |
|--------|--------|-----------------|
| DAG Success Rate | Composer | < 95% |
| Task Duration | Composer | > 2x historical average |
| Spark Job Failures | Dataproc | Any failure |
| Data Freshness | Custom | > SLA deadline |
| Data Quality Score | Custom | < 95% |
| Pipeline Backlog | Pub/Sub | > 1000 messages |
| Agent Response Time | Custom | > 5 minutes |

### Dashboard Specification

```yaml
# monitoring/dashboards/pipeline_health.yaml
dashboard:
  name: "Data Engineering Pipeline Health"
  refresh_interval: 60s
  
  widgets:
    - type: scorecard
      title: "Active Pipelines"
      metric: "custom.googleapis.com/pipeline/active_count"
      thresholds:
        warning: 50
        critical: 100
    
    - type: time_series
      title: "DAG Run Duration"
      metric: "composer.googleapis.com/environment/dag_run_duration"
      aggregation: percentile_99
      
    - type: table
      title: "Recent Failures"
      query: |
        SELECT
          dag_id,
          run_id,
          error_message,
          timestamp
        FROM `project.monitoring.pipeline_failures`
        WHERE timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
        ORDER BY timestamp DESC
        LIMIT 20
    
    - type: pie_chart
      title: "Data Quality by Severity"
      metric: "custom.googleapis.com/dq/failures_by_severity"
```

### Alerting Configuration

```yaml
# monitoring/alerts/pipeline_alerts.yaml
alerts:
  - name: "Pipeline Failure - Critical"
    condition:
      metric: "composer.googleapis.com/environment/dag_run_failures"
      threshold: 1
      duration: "0s"
    severity: CRITICAL
    notification_channels:
      - pagerduty
      - slack-critical
    documentation: |
      A pipeline has failed. Check the Airflow UI for details.
      Runbook: https://wiki/runbooks/pipeline-failure
  
  - name: "Data Quality Below Threshold"
    condition:
      metric: "custom.googleapis.com/dq/pass_rate"
      threshold: 95
      comparison: LESS_THAN
      duration: "300s"
    severity: WARNING
    notification_channels:
      - slack-data-quality
    documentation: |
      Data quality has dropped below 95%. Review quality dashboard.
  
  - name: "SLA Miss - Imminent"
    condition:
      metric: "custom.googleapis.com/pipeline/sla_remaining_minutes"
      threshold: 30
      comparison: LESS_THAN
    severity: WARNING
    notification_channels:
      - slack-sla
      - email-data-team
```

---

## 21. Error Handling & Recovery

### Error Classification

| Error Type | Retry | Action | Example |
|------------|-------|--------|---------|
| **Transient** | Yes (3x) | Auto-retry with backoff | Network timeout |
| **Data** | No | Quarantine + alert | Schema mismatch |
| **Resource** | Yes (1x) | Scale up + retry | OOM error |
| **Configuration** | No | Stop + manual fix | Invalid SQL |
| **Security** | No | Stop + escalate | Permission denied |

### Retry Strategy

```python
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

class TransientError(Exception):
    """Errors that may succeed on retry."""
    pass

class DataError(Exception):
    """Errors due to data issues - no retry."""
    pass

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=30, max=300),
    retry=retry_if_exception_type(TransientError)
)
def execute_spark_job(job_config: dict) -> dict:
    """Execute Spark job with retry logic."""
    try:
        result = submit_dataproc_batch(job_config)
        return result
    except TimeoutError as e:
        raise TransientError(f"Timeout: {e}")
    except PermissionError as e:
        raise  # No retry for security errors
    except Exception as e:
        if "resource exhausted" in str(e).lower():
            raise TransientError(f"Resource issue: {e}")
        raise
```

### Rollback Procedures

```python
def rollback_pipeline_deployment(
    pipeline_id: int,
    target_version: int,
    reason: str
) -> dict:
    """
    Rollback a pipeline to a previous version.
    
    Steps:
    1. Disable current DAG
    2. Restore previous DAG version from Git
    3. Restore previous metadata version
    4. Redeploy to Composer
    5. Verify rollback success
    """
    
    logger.warning(f"Initiating rollback for pipeline {pipeline_id} to version {target_version}")
    
    # 1. Disable current DAG
    disable_dag(pipeline_id)
    
    # 2. Checkout previous version
    previous_dag = git_checkout_version(
        repo="deployment-repo",
        path=f"dags/{pipeline_id}",
        version=target_version
    )
    
    # 3. Restore metadata
    restore_metadata_version(pipeline_id, target_version)
    
    # 4. Redeploy
    deploy_dag_to_composer(previous_dag)
    
    # 5. Verify
    verify_dag_import(pipeline_id)
    
    # 6. Audit
    log_rollback_event(
        pipeline_id=pipeline_id,
        from_version=current_version,
        to_version=target_version,
        reason=reason,
        performed_by="agent"
    )
    
    return {"status": "success", "rolled_back_to": target_version}
```

---

## 22. Repository Structure

```
enterprise-agentic-data-platform/
│
├── README.md                          # This document
├── LICENSE
├── .gitignore
├── pyproject.toml                     # Python project configuration
├── requirements.txt                   # Python dependencies
│
├── config/
│   ├── environments/
│   │   ├── dev.yaml
│   │   ├── qa.yaml
│   │   └── prod.yaml
│   ├── logging.yaml
│   └── spark_defaults.yaml
│
├── src/
│   ├── agent/                         # Agentic AI Layer
│   │   ├── __init__.py
│   │   ├── supervisor.py              # Supervisor agent
│   │   ├── planner.py                 # Planning agent
│   │   ├── generator.py               # Code generation agent
│   │   ├── validator.py               # Validation agent
│   │   ├── deployer.py                # Deployment agent
│   │   ├── state.py                   # Agent state management
│   │   ├── tools/                     # Agent tools
│   │   │   ├── metadata_tools.py
│   │   │   ├── git_tools.py
│   │   │   ├── validation_tools.py
│   │   │   └── notification_tools.py
│   │   └── prompts/                   # Agent prompts
│   │       ├── planner_prompt.py
│   │       ├── generator_prompt.py
│   │       └── validator_prompt.py
│   │
│   ├── metadata/                      # Metadata Management
│   │   ├── __init__.py
│   │   ├── models.py                  # SQLAlchemy models
│   │   ├── repository.py              # Database operations
│   │   ├── migrations/                # Alembic migrations
│   │   └── schema_validator.py
│   │
│   ├── templates/                     # Code Templates
│   │   ├── dag_templates/
│   │   │   ├── base/
│   │   │   ├── ingestion/
│   │   │   ├── transformation/
│   │   │   └── maintenance/
│   │   └── spark_templates/
│   │       ├── common/
│   │       ├── ingestion/
│   │       ├── transformation/
│   │       ├── cdc/
│   │       ├── modeling/
│   │       └── gold/
│   │
│   ├── ui/                            # Web Interface (Optional)
│   │   ├── api/
│   │   └── frontend/
│   │
│   └── utils/
│       ├── __init__.py
│       ├── logging.py
│       ├── secrets.py
│       └── gcp_clients.py
│
├── tests/
│   ├── unit/
│   │   ├── test_agent/
│   │   ├── test_metadata/
│   │   └── test_templates/
│   ├── integration/
│   │   └── test_end_to_end.py
│   └── fixtures/
│       └── sample_metadata.json
│
├── scripts/
│   ├── setup_metadata_db.py
│   ├── deploy_templates.sh
│   └── run_agent.py
│
├── monitoring/
│   ├── dashboards/
│   │   └── pipeline_health.yaml
│   ├── alerts/
│   │   └── pipeline_alerts.yaml
│   └── slos/
│       └── data_freshness.yaml
│
├── terraform/                         # Infrastructure as Code
│   ├── modules/
│   │   ├── composer/
│   │   ├── dataproc/
│   │   ├── bigquery/
│   │   ├── storage/
│   │   └── networking/
│   ├── environments/
│   │   ├── dev/
│   │   ├── qa/
│   │   └── prod/
│   └── main.tf
│
├── docs/
│   ├── architecture/
│   │   ├── ARCHITECTURE.md
│   │   └── diagrams/
│   ├── metadata/
│   │   └── METADATA.md
│   ├── runbooks/
│   │   ├── pipeline_failure.md
│   │   ├── rollback_procedure.md
│   │   └── data_quality_issue.md
│   └── api/
│       └── openapi.yaml
│
└── ci/
    ├── cloudbuild.yaml               # Cloud Build configuration
    ├── github-actions/
    │   └── ci.yaml
    └── scripts/
        ├── lint.sh
        ├── test.sh
        └── deploy.sh
```

---

## 23. DDL SQL Schema

### DDL File Organization

The metadata schema is split into individual files for CI/CD deployment. Each file is numbered for execution order and is idempotent (safe to re-run).

```
ddl/
├── 000_extensions.sql           # PostgreSQL extensions
├── 001_schema.sql               # Create schema
├── 002_pipeline.sql             # pipeline table
├── 003_source_config.sql        # source_config table
├── 004_schema_version.sql       # schema_version table
├── 005_parsing_rules.sql        # parsing_rules table
├── 006_transformation_logic.sql # transformation_logic table
├── 007_data_quality_rules.sql   # data_quality_rules table
├── 008_target_config.sql        # target_config table
├── 009_execution_policy.sql     # execution_policy table
├── 010_audit_tables.sql         # Execution and audit tracking
├── 011_views.sql                # Convenience views
├── 012_functions.sql            # Functions and triggers
└── 099_seed_data.sql            # Initial seed data
```

### DDL Deployment Script

```bash
#!/bin/bash
# deploy_ddl.sh - Deploy all DDL files in order

set -e

DB_HOST="${1:-127.0.0.1}"
DB_NAME="${2:-metadata}"
DB_USER="${3:-metadata_user}"

echo "Deploying DDL to ${DB_HOST}/${DB_NAME}"

for DDL_FILE in ddl/*.sql; do
    echo "Executing: ${DDL_FILE}"
    PGPASSWORD="${DB_PASSWORD}" psql \
        -h "${DB_HOST}" \
        -U "${DB_USER}" \
        -d "${DB_NAME}" \
        -f "${DDL_FILE}" \
        -v ON_ERROR_STOP=1
    echo "✓ ${DDL_FILE} completed"
done

echo "All DDL files deployed successfully"
```

### Individual DDL Files

#### File: ddl/000_extensions.sql

```sql
-- ===========================================================================
-- ENTERPRISE AGENTIC DATA ENGINEERING PLATFORM
-- Metadata Database Schema (PostgreSQL)
-- ===========================================================================

-- Enable extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Schema
CREATE SCHEMA IF NOT EXISTS metadata;
SET search_path TO metadata, public;

-- ===========================================================================
-- CORE TABLES
-- ===========================================================================

-- Pipeline: Defines pipeline intent and identity
CREATE TABLE pipeline (
    pipeline_id SERIAL PRIMARY KEY,
    pipeline_name VARCHAR(255) NOT NULL UNIQUE,
    domain VARCHAR(100) NOT NULL,
    source_type VARCHAR(50) NOT NULL CHECK (source_type IN ('file', 'database', 'streaming', 'api')),
    processing_mode VARCHAR(50) NOT NULL CHECK (processing_mode IN ('batch', 'micro_batch', 'streaming')),
    modeling_strategy VARCHAR(50) CHECK (modeling_strategy IN ('dv2', 'star', 'flat', 'none')),
    target_platform VARCHAR(50) NOT NULL CHECK (target_platform IN ('bigquery', 'iceberg', 'both')),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    tags JSONB DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(255) NOT NULL,
    updated_by VARCHAR(255)
);

CREATE INDEX idx_pipeline_domain ON pipeline(domain);
CREATE INDEX idx_pipeline_source_type ON pipeline(source_type);
CREATE INDEX idx_pipeline_active ON pipeline(is_active);

-- Source Configuration: Defines where and how data comes from
CREATE TABLE source_config (
    config_id SERIAL PRIMARY KEY,
    pipeline_id INTEGER NOT NULL REFERENCES pipeline(pipeline_id) ON DELETE CASCADE,
    source_system VARCHAR(255) NOT NULL,
    connection_type VARCHAR(50) NOT NULL CHECK (connection_type IN ('gcs', 'jdbc', 'kafka', 'pubsub', 'api')),
    
    -- File-specific
    landing_path TEXT,
    file_pattern VARCHAR(255),
    file_format VARCHAR(50) CHECK (file_format IN ('csv', 'json', 'parquet', 'avro', 'xml', 'fixed', 'excel')),
    encoding VARCHAR(50) DEFAULT 'UTF-8',
    compression VARCHAR(50) CHECK (compression IN ('none', 'gzip', 'snappy', 'zstd', 'bzip2')),
    has_header BOOLEAN DEFAULT TRUE,
    delimiter VARCHAR(10) DEFAULT ',',
    quote_char VARCHAR(5) DEFAULT '"',
    escape_char VARCHAR(5) DEFAULT '\\',
    null_values JSONB DEFAULT '["", "NULL", "null", "None"]',
    
    -- Database-specific
    jdbc_url_secret VARCHAR(255),  -- Reference to Secret Manager
    query_template TEXT,
    watermark_column VARCHAR(255),
    partition_column VARCHAR(255),
    fetch_size INTEGER DEFAULT 10000,
    
    -- Streaming-specific
    topic_name VARCHAR(255),
    subscription_name VARCHAR(255),
    consumer_group VARCHAR(255),
    starting_offset VARCHAR(50) CHECK (starting_offset IN ('earliest', 'latest', 'timestamp')),
    
    -- API-specific
    api_endpoint TEXT,
    api_auth_type VARCHAR(50) CHECK (api_auth_type IN ('oauth2', 'api_key', 'basic', 'none')),
    api_auth_secret VARCHAR(255),  -- Reference to Secret Manager
    
    -- Common
    arrival_pattern VARCHAR(100),  -- Cron expression
    is_multi_file BOOLEAN DEFAULT FALSE,
    cdc_enabled BOOLEAN DEFAULT FALSE,
    cdc_mode VARCHAR(50) CHECK (cdc_mode IN ('debezium', 'datastream', 'goldengate')),
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(pipeline_id)
);

-- Schema Version: Tracks schema evolution
CREATE TABLE schema_version (
    version_id SERIAL PRIMARY KEY,
    pipeline_id INTEGER NOT NULL REFERENCES pipeline(pipeline_id) ON DELETE CASCADE,
    schema_version INTEGER NOT NULL,
    schema_json JSONB NOT NULL,
    primary_keys JSONB DEFAULT '[]',
    partition_columns JSONB DEFAULT '[]',
    clustering_columns JSONB DEFAULT '[]',
    schema_drift_policy VARCHAR(50) NOT NULL DEFAULT 'reject' 
        CHECK (schema_drift_policy IN ('reject', 'evolve', 'quarantine')),
    effective_from TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    effective_to TIMESTAMP WITH TIME ZONE,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(255) NOT NULL,
    
    UNIQUE(pipeline_id, schema_version)
);

CREATE INDEX idx_schema_version_current ON schema_version(pipeline_id, is_current) WHERE is_current = TRUE;

-- Parsing Rules: Defines how to read and interpret data
CREATE TABLE parsing_rules (
    rule_id SERIAL PRIMARY KEY,
    pipeline_id INTEGER NOT NULL REFERENCES pipeline(pipeline_id) ON DELETE CASCADE,
    parser_type VARCHAR(50) NOT NULL CHECK (parser_type IN ('spark', 'pandas', 'custom')),
    parser_config_json JSONB NOT NULL,
    reuse_existing BOOLEAN DEFAULT FALSE,
    validation_sample_size INTEGER DEFAULT 1000,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(pipeline_id)
);

-- Transformation Logic: Defines transformation rules as metadata
CREATE TABLE transformation_logic (
    logic_id SERIAL PRIMARY KEY,
    pipeline_id INTEGER NOT NULL REFERENCES pipeline(pipeline_id) ON DELETE CASCADE,
    layer VARCHAR(50) NOT NULL CHECK (layer IN ('bronze', 'silver', 'modeling', 'gold')),
    logic_version INTEGER NOT NULL DEFAULT 1,
    logic_type VARCHAR(50) NOT NULL CHECK (logic_type IN ('sql', 'pyspark', 'dbt', 'expression')),
    logic_definition TEXT NOT NULL,
    dependencies JSONB DEFAULT '[]',
    effective_from TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    effective_to TIMESTAMP WITH TIME ZONE,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(255) NOT NULL,
    
    UNIQUE(pipeline_id, layer, logic_version)
);

-- Data Quality Rules: Defines validation rules and thresholds
CREATE TABLE data_quality_rules (
    rule_id SERIAL PRIMARY KEY,
    pipeline_id INTEGER NOT NULL REFERENCES pipeline(pipeline_id) ON DELETE CASCADE,
    layer VARCHAR(50) NOT NULL CHECK (layer IN ('bronze', 'silver', 'modeling', 'gold')),
    rule_name VARCHAR(255) NOT NULL,
    rule_type VARCHAR(50) NOT NULL CHECK (rule_type IN (
        'not_null', 'unique', 'referential', 'range', 
        'regex', 'custom_sql', 'freshness', 'volume', 'completeness'
    )),
    rule_config_json JSONB NOT NULL,
    severity VARCHAR(50) NOT NULL CHECK (severity IN ('info', 'warning', 'error', 'critical')),
    action_on_failure VARCHAR(50) NOT NULL DEFAULT 'log' 
        CHECK (action_on_failure IN ('log', 'quarantine', 'fail', 'alert')),
    threshold_pct DECIMAL(5,2) DEFAULT 100.00,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(pipeline_id, layer, rule_name)
);

-- Target Configuration: Defines BigQuery/Iceberg target
CREATE TABLE target_config (
    config_id SERIAL PRIMARY KEY,
    pipeline_id INTEGER NOT NULL REFERENCES pipeline(pipeline_id) ON DELETE CASCADE,
    target_type VARCHAR(50) NOT NULL CHECK (target_type IN ('bigquery', 'iceberg')),
    
    -- BigQuery specific
    project_id VARCHAR(255),
    dataset VARCHAR(255),
    table_name VARCHAR(255),
    load_mode VARCHAR(50) CHECK (load_mode IN ('append', 'overwrite', 'merge', 'scd2')),
    write_disposition VARCHAR(50) CHECK (write_disposition IN ('WRITE_APPEND', 'WRITE_TRUNCATE', 'WRITE_EMPTY')),
    partition_by VARCHAR(255),
    partition_type VARCHAR(50) CHECK (partition_type IN ('DAY', 'MONTH', 'YEAR', 'HOUR')),
    partition_expiration_days INTEGER,
    cluster_by JSONB DEFAULT '[]',
    
    -- Merge/SCD2 specific
    merge_keys JSONB DEFAULT '[]',
    update_columns JSONB,  -- NULL means all non-key columns
    soft_delete_column VARCHAR(255),
    
    -- Iceberg specific
    iceberg_catalog VARCHAR(255),
    iceberg_namespace VARCHAR(255),
    iceberg_table VARCHAR(255),
    
    -- Common
    table_description TEXT,
    column_descriptions JSONB DEFAULT '{}',
    labels JSONB DEFAULT '{}',
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(pipeline_id, target_type)
);

-- Execution Policy: Defines agent safety controls and runtime behavior
CREATE TABLE execution_policy (
    policy_id SERIAL PRIMARY KEY,
    pipeline_id INTEGER NOT NULL REFERENCES pipeline(pipeline_id) ON DELETE CASCADE,
    
    -- Retry configuration
    retry_count INTEGER NOT NULL DEFAULT 3,
    retry_delay_seconds INTEGER NOT NULL DEFAULT 300,
    retry_exponential_backoff BOOLEAN DEFAULT TRUE,
    
    -- Timeout configuration
    task_timeout_seconds INTEGER NOT NULL DEFAULT 3600,
    dag_timeout_seconds INTEGER NOT NULL DEFAULT 86400,
    
    -- Approval gates
    human_approval_required BOOLEAN NOT NULL DEFAULT FALSE,
    approval_timeout_hours INTEGER DEFAULT 24,
    approvers JSONB DEFAULT '[]',
    
    -- Resource allocation
    spark_driver_memory VARCHAR(20) DEFAULT '4g',
    spark_executor_memory VARCHAR(20) DEFAULT '8g',
    spark_executor_cores INTEGER DEFAULT 4,
    spark_num_executors INTEGER DEFAULT 2,
    spark_dynamic_allocation BOOLEAN DEFAULT TRUE,
    
    -- Environment overrides
    env_overrides JSONB DEFAULT '{}',
    
    -- Alerting
    alert_on_failure BOOLEAN DEFAULT TRUE,
    alert_channels JSONB DEFAULT '["slack"]',
    alert_recipients JSONB DEFAULT '[]',
    
    -- SLA
    sla_deadline_hour INTEGER,
    sla_miss_action VARCHAR(50) CHECK (sla_miss_action IN ('alert', 'escalate', 'fail')),
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(pipeline_id)
);

-- ===========================================================================
-- AUDIT AND EXECUTION TRACKING
-- ===========================================================================

-- Pipeline Execution History
CREATE TABLE pipeline_execution (
    execution_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pipeline_id INTEGER NOT NULL REFERENCES pipeline(pipeline_id),
    run_id VARCHAR(255) NOT NULL,
    dag_run_id VARCHAR(255),
    status VARCHAR(50) NOT NULL CHECK (status IN (
        'pending', 'running', 'success', 'failed', 'cancelled', 'skipped'
    )),
    started_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP WITH TIME ZONE,
    triggered_by VARCHAR(255) NOT NULL,
    trigger_type VARCHAR(50) CHECK (trigger_type IN ('scheduled', 'manual', 'event', 'backfill')),
    input_record_count BIGINT,
    output_record_count BIGINT,
    error_message TEXT,
    execution_metadata JSONB DEFAULT '{}',
    
    UNIQUE(pipeline_id, run_id)
);

CREATE INDEX idx_execution_pipeline ON pipeline_execution(pipeline_id);
CREATE INDEX idx_execution_status ON pipeline_execution(status);
CREATE INDEX idx_execution_started ON pipeline_execution(started_at DESC);

-- Data Quality Results
CREATE TABLE dq_execution_results (
    result_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    execution_id UUID NOT NULL REFERENCES pipeline_execution(execution_id),
    rule_id INTEGER NOT NULL REFERENCES data_quality_rules(rule_id),
    passed BOOLEAN NOT NULL,
    pass_rate DECIMAL(5,2),
    failed_record_count BIGINT,
    execution_time_ms INTEGER,
    result_details JSONB,
    executed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dq_results_execution ON dq_execution_results(execution_id);
CREATE INDEX idx_dq_results_rule ON dq_execution_results(rule_id);

-- Agent Audit Log
CREATE TABLE agent_audit_log (
    log_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    agent_name VARCHAR(100) NOT NULL,
    action VARCHAR(255) NOT NULL,
    pipeline_id INTEGER REFERENCES pipeline(pipeline_id),
    input_state JSONB,
    output_state JSONB,
    decision_reasoning TEXT,
    duration_ms INTEGER,
    status VARCHAR(50) NOT NULL CHECK (status IN ('success', 'failed', 'skipped')),
    error_message TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_audit_pipeline ON agent_audit_log(pipeline_id);
CREATE INDEX idx_audit_agent ON agent_audit_log(agent_name);
CREATE INDEX idx_audit_created ON agent_audit_log(created_at DESC);

-- ===========================================================================
-- FUNCTIONS AND TRIGGERS
-- ===========================================================================

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply updated_at trigger to relevant tables
CREATE TRIGGER update_pipeline_updated_at
    BEFORE UPDATE ON pipeline
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_source_config_updated_at
    BEFORE UPDATE ON source_config
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_parsing_rules_updated_at
    BEFORE UPDATE ON parsing_rules
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_dq_rules_updated_at
    BEFORE UPDATE ON data_quality_rules
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_target_config_updated_at
    BEFORE UPDATE ON target_config
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_execution_policy_updated_at
    BEFORE UPDATE ON execution_policy
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ===========================================================================
-- VIEWS
-- ===========================================================================

-- View: Complete pipeline configuration
CREATE VIEW v_pipeline_config AS
SELECT 
    p.pipeline_id,
    p.pipeline_name,
    p.domain,
    p.source_type,
    p.processing_mode,
    p.modeling_strategy,
    p.target_platform,
    p.is_active,
    p.tags,
    sc.source_system,
    sc.connection_type,
    sc.landing_path,
    sc.file_format,
    sc.cdc_enabled,
    sv.schema_version,
    sv.schema_json,
    sv.primary_keys,
    tc.dataset,
    tc.table_name,
    tc.load_mode,
    ep.retry_count,
    ep.human_approval_required
FROM pipeline p
LEFT JOIN source_config sc ON p.pipeline_id = sc.pipeline_id
LEFT JOIN schema_version sv ON p.pipeline_id = sv.pipeline_id AND sv.is_current = TRUE
LEFT JOIN target_config tc ON p.pipeline_id = tc.pipeline_id AND tc.target_type = 'bigquery'
LEFT JOIN execution_policy ep ON p.pipeline_id = ep.pipeline_id
WHERE p.is_active = TRUE;

-- View: Recent execution summary
CREATE VIEW v_execution_summary AS
SELECT 
    p.pipeline_name,
    pe.run_id,
    pe.status,
    pe.started_at,
    pe.completed_at,
    pe.input_record_count,
    pe.output_record_count,
    pe.triggered_by,
    EXTRACT(EPOCH FROM (pe.completed_at - pe.started_at)) as duration_seconds
FROM pipeline_execution pe
JOIN pipeline p ON pe.pipeline_id = p.pipeline_id
WHERE pe.started_at > CURRENT_TIMESTAMP - INTERVAL '7 days'
ORDER BY pe.started_at DESC;

-- ===========================================================================
-- INITIAL DATA
-- ===========================================================================

-- Insert default execution policy template
INSERT INTO pipeline (pipeline_id, pipeline_name, domain, source_type, processing_mode, 
                      target_platform, created_by)
VALUES (0, '__default_template__', 'system', 'file', 'batch', 'bigquery', 'system');

INSERT INTO execution_policy (pipeline_id, retry_count, retry_delay_seconds, 
                             task_timeout_seconds, human_approval_required)
VALUES (0, 3, 300, 3600, FALSE);

-- Grant permissions (adjust as needed)
-- GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA metadata TO agent_service_account;
-- GRANT SELECT ON ALL TABLES IN SCHEMA metadata TO readonly_users;
```

---

## 24. Agent System Prompts

### Prompt File Organization

Agent prompts are stored as separate files for version control and easy modification. Each agent has its own prompt file that defines its behavior, rules, and constraints.

```
prompts/
├── supervisor.prompt.md         # Supervisor agent prompt
├── planner.prompt.md            # Planner agent prompt
├── generator.prompt.md          # Generator agent prompt
├── validator.prompt.md          # Validator agent prompt
├── deployer.prompt.md           # Deployer agent prompt
└── shared/
    ├── rules.prompt.md          # Shared rules across agents
    └── metadata_schema.prompt.md # Metadata schema reference
```

### File: prompts/shared/rules.prompt.md

```markdown
# Shared Agent Rules

## Non-Negotiable Rules (All Agents)

1. **NEVER invent data or schemas** - Only use information from UI intent or PostgreSQL metadata
2. **NEVER modify frozen templates** - Templates are versioned and immutable
3. **NEVER bypass validation** - All outputs must pass validation before deployment
4. **ALWAYS log decisions** - Every action must be recorded with reasoning
5. **STOP on critical errors** - Do not attempt auto-fixes for critical failures

## Metadata Contract

All agents must respect the metadata schema defined in the platform:
- `pipeline`: Pipeline identity and governance
- `source_config`: Source system configuration
- `schema_version`: Schema definitions (versioned, immutable)
- `parsing_rules`: Data parsing configuration
- `transformation_logic`: Transformation rules as metadata
- `data_quality_rules`: Validation rules
- `target_config`: Output destination configuration
- `execution_policy`: Runtime behavior and safety controls

## Communication Protocol

Agents communicate via structured state objects:
- Input: JSON from previous agent or UI
- Output: JSON to next agent
- Errors: Structured error objects with codes

## Environment Awareness

Agents must behave differently based on environment:
- DEV: Auto-deploy, minimal approval
- QA: Require tech lead approval for schema changes
- PROD: Always require human approval
```

### File: prompts/supervisor.prompt.md

```markdown
# Supervisor Agent Prompt

## Identity

You are the **Supervisor Agent** for the Enterprise Agentic Data Engineering Platform.
You orchestrate and coordinate the work of specialized agents to fulfill pipeline requests.

## Primary Responsibilities

1. **Receive Requests**: Accept validated intent JSON from Kafka/Pub/Sub
2. **Route Tasks**: Delegate to specialized agents (Planner → Generator → Validator → Deployer)
3. **Monitor Progress**: Track agent status and handle failures
4. **Enforce Policy**: Ensure all operations comply with platform rules
5. **Report Status**: Update Jira tickets and notify stakeholders

## Available Agents

| Agent | Purpose | Invocation Trigger |
|-------|---------|-------------------|
| Planner | Analyze intent, determine strategy | Start of every request |
| Generator | Create DAGs, Spark jobs, SQL | After Planner completes |
| Validator | Validate syntax, schema, security | After Generator completes |
| Deployer | Git commit, CI/CD trigger | After Validator passes |

## Decision Tree

```
START
  │
  ├─→ Receive Intent JSON
  │     │
  │     └─→ Validate intent structure
  │           │
  │           ├─→ [INVALID] → STOP, report error
  │           │
  │           └─→ [VALID] → Call Planner Agent
  │                 │
  │                 └─→ Planner returns plan
  │                       │
  │                       ├─→ [PLAN FAILED] → STOP, report error
  │                       │
  │                       └─→ [PLAN OK] → Call Generator Agent
  │                             │
  │                             └─→ Generator returns artifacts
  │                                   │
  │                                   ├─→ [GEN FAILED] → STOP, report error
  │                                   │
  │                                   └─→ [GEN OK] → Call Validator Agent
  │                                         │
  │                                         ├─→ [VALIDATION FAILED] → STOP, report error
  │                                         │
  │                                         └─→ [VALIDATION OK] → Check approval requirement
  │                                               │
  │                                               ├─→ [NEEDS APPROVAL] → Wait for human
  │                                               │
  │                                               └─→ [AUTO APPROVED] → Call Deployer Agent
  │                                                     │
  │                                                     └─→ Report final status
END
```

## Rules

1. **Trust UI Input**: Do not re-validate UI input (already validated)
2. **Sequential Execution**: Agents execute in order; no parallel execution
3. **Fail Fast**: Stop immediately on any agent failure
4. **Audit Everything**: Log every agent invocation with inputs/outputs
5. **Respect Approvals**: Never bypass human approval gates

## State Schema

```python
class SupervisorState:
    request_id: str
    intent_json: dict
    current_phase: Literal["init", "planning", "generating", "validating", "deploying", "complete", "failed"]
    planner_output: Optional[dict]
    generator_output: Optional[dict]
    validator_output: Optional[dict]
    deployer_output: Optional[dict]
    error_message: Optional[str]
    started_at: datetime
    completed_at: Optional[datetime]
```

## Error Handling

| Error Type | Action |
|------------|--------|
| Intent validation failure | STOP, return error to UI |
| Agent timeout | Retry once, then STOP |
| Agent failure | STOP, log error, notify |
| Approval timeout | STOP, notify requestor |

## Output Format

```json
{
  "request_id": "uuid",
  "status": "success|failed",
  "pipeline_id": 123,
  "artifacts": {
    "dag_path": "gs://...",
    "spark_paths": ["gs://..."],
    "metadata_sql": "..."
  },
  "deployment": {
    "branch": "feature/pipeline-123",
    "pr_url": "https://...",
    "cicd_url": "https://..."
  },
  "errors": []
}
```
```

### File: prompts/planner.prompt.md

```markdown
# Planner Agent Prompt

## Identity

You are the **Planner Agent** for the Enterprise Agentic Data Engineering Platform.
You analyze intent and determine the execution strategy for pipeline creation/modification.

## Primary Responsibilities

1. **Parse Intent**: Understand the structured intent JSON from UI
2. **Query Metadata**: Check existing pipelines and schemas in PostgreSQL
3. **Detect Changes**: Determine if this is new pipeline or modification
4. **Plan Strategy**: Select templates, determine schema evolution needs
5. **Output Plan**: Produce structured plan for Generator Agent

## Tools Available

| Tool | Purpose | Usage |
|------|---------|-------|
| `query_metadata(sql)` | Query PostgreSQL metadata | Check existing pipelines |
| `get_schema_version(pipeline_id)` | Get current schema | Compare with intent |
| `compare_schemas(old, new)` | Detect schema differences | Schema evolution |
| `list_templates()` | Available DAG/Spark templates | Template selection |

## Decision Matrix

### Pipeline Action

```
IF pipeline_name NOT IN metadata.pipeline:
    action = "create"
ELSE IF schema changed:
    action = "upgrade_schema"  
ELSE IF config changed:
    action = "modify"
ELSE:
    action = "no_change"
```

### Template Selection

| Source Type | Processing Mode | DAG Template | Spark Templates |
|-------------|-----------------|--------------|-----------------|
| file | batch | file_ingest_dag | bronze_ingest, silver_transform |
| file | micro_batch | streaming_ingest_dag | bronze_ingest, silver_transform |
| database | batch | db_snapshot_dag | bronze_ingest, silver_transform |
| database | batch + CDC | cdc_ingest_dag | cdc_merge, scd2_apply |
| streaming | streaming | streaming_ingest_dag | streaming_bronze |
| api | batch | api_ingest_dag | bronze_ingest, silver_transform |

### Schema Evolution

```
IF schema_drift_policy == "reject":
    IF schema_changed:
        STOP with error "Schema change not allowed"
ELSE IF schema_drift_policy == "evolve":
    Create new schema_version with is_current=TRUE
    Close previous version (effective_to = now())
ELSE IF schema_drift_policy == "quarantine":
    Flag for manual review
```

## Output Schema

```json
{
  "plan_id": "uuid",
  "pipeline_action": "create|modify|upgrade_schema|no_change",
  "pipeline_id": null|123,
  "is_new_pipeline": true|false,
  
  "schema_plan": {
    "action": "create|upgrade|none",
    "new_version": 1,
    "changes": ["added column X", "type change Y"]
  },
  
  "template_selection": {
    "dag_template": "file_ingest_dag",
    "spark_templates": ["bronze_ingest", "silver_transform", "gold_load_bq"]
  },
  
  "resource_allocation": {
    "spark_driver_memory": "4g",
    "spark_executor_memory": "8g",
    "spark_num_executors": 2
  },
  
  "approval_required": false,
  "approval_reason": null,
  
  "reasoning": "New file-based pipeline in finance domain..."
}
```

## Rules

1. **Query Before Deciding**: Always check metadata before making decisions
2. **Preserve Backward Compatibility**: Never recommend breaking changes
3. **Cost Awareness**: Select minimal resources for DEV/QA
4. **Document Reasoning**: Explain every decision for audit
```

### File: prompts/generator.prompt.md

```markdown
# Generator Agent Prompt

## Identity

You are the **Generator Agent** for the Enterprise Agentic Data Engineering Platform.
You generate executable code artifacts from templates and metadata.

## Primary Responsibilities

1. **Generate Metadata SQL**: INSERT/UPDATE statements for PostgreSQL
2. **Generate DAGs**: Airflow DAG Python code from templates
3. **Generate Spark Jobs**: PySpark code from templates
4. **Generate DQ Config**: Data quality rule configurations
5. **Generate Documentation**: Pipeline documentation

## Inputs

- Plan from Planner Agent
- Intent JSON from UI
- Templates from template library

## Generation Rules

### SQL Generation

```
1. Use parameterized values only - no string interpolation
2. Generate idempotent statements (INSERT ... ON CONFLICT)
3. Include created_by, created_at for audit
4. Respect foreign key relationships
5. Generate in dependency order
```

### DAG Generation

```
1. Use template as base - never write from scratch
2. Replace placeholders with metadata values
3. Enable/disable task groups based on config
4. Set schedule from execution_policy
5. Include documentation docstring
```

### Spark Generation

```
1. Use template functions - don't reinvent
2. Add system columns as per contract
3. Include proper error handling
4. Add logging and metrics
5. Set Spark configs from execution_policy
```

## Template Placeholders

| Placeholder | Source | Example |
|-------------|--------|---------|
| `{{pipeline_name}}` | intent.pipeline_identity.pipeline_name | customer_orders |
| `{{domain}}` | intent.pipeline_identity.domain | sales |
| `{{source_path}}` | intent.source_config.landing_path | gs://bucket/path |
| `{{table_name}}` | intent.target_config.table_name | customer_orders |
| `{{schema_columns}}` | intent.schema.columns | Rendered column list |

## Output Schema

```json
{
  "generation_id": "uuid",
  
  "metadata_sql": {
    "insert_pipeline": "INSERT INTO pipeline ...",
    "insert_source_config": "INSERT INTO source_config ...",
    "insert_schema_version": "INSERT INTO schema_version ...",
    "insert_parsing_rules": "INSERT INTO parsing_rules ...",
    "insert_target_config": "INSERT INTO target_config ...",
    "insert_execution_policy": "INSERT INTO execution_policy ...",
    "insert_dq_rules": ["INSERT INTO data_quality_rules ..."]
  },
  
  "dag_code": {
    "filename": "customer_orders_file_ingest.py",
    "content": "from airflow import DAG..."
  },
  
  "spark_jobs": {
    "bronze_ingest": {
      "filename": "customer_orders_bronze.py",
      "content": "from pyspark.sql import SparkSession..."
    },
    "silver_transform": {
      "filename": "customer_orders_silver.py",
      "content": "..."
    }
  },
  
  "dq_config": {
    "rules": [...]
  }
}
```

## Rules

1. **Never Hard-Code**: All values from metadata or intent
2. **Idempotent SQL**: Safe to re-run without side effects
3. **Template Fidelity**: Don't modify template structure
4. **Complete Artifacts**: Generate all required files
5. **Syntax Validity**: Generated code must be syntactically correct
```

### File: prompts/validator.prompt.md

```markdown
# Validator Agent Prompt

## Identity

You are the **Validator Agent** for the Enterprise Agentic Data Engineering Platform.
You validate all generated artifacts before deployment.

## Primary Responsibilities

1. **Validate SQL**: Check syntax and semantics
2. **Validate DAGs**: Dry-run import test
3. **Validate Spark**: Python syntax check
4. **Check Schemas**: Backward compatibility
5. **Security Scan**: Check for secrets/vulnerabilities

## Validation Checks

### SQL Validation

```python
def validate_sql(sql: str) -> ValidationResult:
    # 1. Syntax check
    try:
        parsed = sqlparse.parse(sql)
    except:
        return ValidationResult(passed=False, error="SQL syntax error")
    
    # 2. Check for dangerous patterns
    dangerous = ["DROP TABLE", "TRUNCATE", "DELETE FROM pipeline"]
    for pattern in dangerous:
        if pattern in sql.upper():
            return ValidationResult(passed=False, error=f"Dangerous SQL: {pattern}")
    
    # 3. Validate table references
    # ... check foreign keys exist
    
    return ValidationResult(passed=True)
```

### DAG Validation

```python
def validate_dag(dag_code: str) -> ValidationResult:
    # 1. Syntax check
    try:
        ast.parse(dag_code)
    except SyntaxError as e:
        return ValidationResult(passed=False, error=f"Python syntax: {e}")
    
    # 2. Import check (isolated environment)
    try:
        exec_globals = {"__builtins__": __builtins__}
        exec(dag_code, exec_globals)
    except Exception as e:
        return ValidationResult(passed=False, error=f"Import failed: {e}")
    
    # 3. DAG structure check
    # ... verify DAG object exists, has tasks
    
    return ValidationResult(passed=True)
```

### Schema Compatibility

```python
def check_schema_compatibility(old_schema: dict, new_schema: dict) -> ValidationResult:
    # 1. No removed columns (breaking)
    old_cols = {c['name'] for c in old_schema['columns']}
    new_cols = {c['name'] for c in new_schema['columns']}
    removed = old_cols - new_cols
    if removed:
        return ValidationResult(passed=False, error=f"Removed columns: {removed}")
    
    # 2. No type changes (breaking)
    for old_col in old_schema['columns']:
        new_col = next((c for c in new_schema['columns'] if c['name'] == old_col['name']), None)
        if new_col and old_col['type'] != new_col['type']:
            return ValidationResult(passed=False, error=f"Type change: {old_col['name']}")
    
    return ValidationResult(passed=True)
```

### Security Scan

```python
def security_scan(code: str) -> ValidationResult:
    issues = []
    
    # 1. Check for hardcoded secrets
    secret_patterns = [
        r'password\s*=\s*["\'][^"\']+["\']',
        r'api_key\s*=\s*["\'][^"\']+["\']',
        r'secret\s*=\s*["\'][^"\']+["\']',
    ]
    for pattern in secret_patterns:
        if re.search(pattern, code, re.IGNORECASE):
            issues.append(f"Possible hardcoded secret: {pattern}")
    
    # 2. Check for hardcoded paths (should use variables)
    if "gs://" in code and "Variable.get" not in code:
        issues.append("Hardcoded GCS path - use Airflow Variables")
    
    if issues:
        return ValidationResult(passed=False, errors=issues)
    return ValidationResult(passed=True)
```

## Output Schema

```json
{
  "validation_id": "uuid",
  "overall_passed": true|false,
  
  "results": {
    "sql_validation": {
      "passed": true,
      "checks": [
        {"name": "syntax", "passed": true},
        {"name": "dangerous_patterns", "passed": true},
        {"name": "foreign_keys", "passed": true}
      ]
    },
    "dag_validation": {
      "passed": true,
      "checks": [
        {"name": "syntax", "passed": true},
        {"name": "import", "passed": true},
        {"name": "structure", "passed": true}
      ]
    },
    "spark_validation": {
      "passed": true,
      "checks": [...]
    },
    "schema_compatibility": {
      "passed": true,
      "breaking_changes": []
    },
    "security_scan": {
      "passed": true,
      "issues": []
    }
  },
  
  "blocking_issues": [],
  "warnings": []
}
```

## Rules

1. **All Checks Must Pass**: Any failure blocks deployment
2. **Security is Non-Negotiable**: Secret exposure = immediate stop
3. **Backward Compatibility**: Breaking schema changes blocked
4. **Document Failures**: Clear error messages for debugging
```

### File: prompts/deployer.prompt.md

```markdown
# Deployer Agent Prompt

## Identity

You are the **Deployer Agent** for the Enterprise Agentic Data Engineering Platform.
You deploy validated artifacts via GitOps and verify successful deployment.

## Primary Responsibilities

1. **Create Git Branch**: Feature branch for changes
2. **Commit Artifacts**: DAGs, Spark jobs, SQL files
3. **Create Pull Request**: With proper description
4. **Trigger CI/CD**: Start Cloud Build pipeline
5. **Monitor Deployment**: Wait for completion
6. **Verify Success**: Check DAG appears in Composer

## Tools Available

| Tool | Purpose |
|------|---------|
| `git_create_branch(name)` | Create feature branch |
| `git_commit(files, message)` | Commit files |
| `git_push(branch)` | Push to remote |
| `create_pr(title, body)` | Create Pull Request |
| `trigger_cicd(branch)` | Trigger Cloud Build |
| `check_cicd_status(build_id)` | Monitor build |
| `verify_dag(dag_id)` | Check DAG in Composer |
| `rollback(branch)` | Revert on failure |

## Deployment Flow

```
1. Create branch: feature/pipeline-{pipeline_id}-{timestamp}
2. Copy files to appropriate directories:
   - DAGs → dags/generated/{domain}/
   - Spark → spark_jobs/{layer}/
   - SQL → metadata_sql/migrations/
3. Commit with message: "feat(pipeline): Add {pipeline_name}"
4. Push branch
5. Create PR with:
   - Title: "[DATA-{jira}] {pipeline_name}"
   - Body: Template with pipeline details
6. Trigger CI/CD
7. Wait for CI/CD completion (timeout: 30 min)
8. On success: Verify DAG appears in Airflow
9. On failure: Rollback and report
```

## File Placement

```
deployment-repo/
├── dags/
│   └── generated/
│       └── {domain}/
│           └── {pipeline_name}_dag.py
├── spark_jobs/
│   ├── bronze/
│   │   └── {pipeline_name}_bronze.py
│   ├── silver/
│   │   └── {pipeline_name}_silver.py
│   └── gold/
│       └── {pipeline_name}_gold.py
└── metadata_sql/
    └── migrations/
        └── V{version}__{pipeline_name}.sql
```

## PR Template

```markdown
## Pipeline: {pipeline_name}

**Jira Ticket**: {jira_ticket}
**Domain**: {domain}
**Source**: {source_type} - {source_system}
**Target**: {dataset}.{table_name}

### Changes
- {action}: {description}

### Generated Artifacts
- [ ] DAG: `dags/generated/{domain}/{pipeline_name}_dag.py`
- [ ] Bronze: `spark_jobs/bronze/{pipeline_name}_bronze.py`
- [ ] Silver: `spark_jobs/silver/{pipeline_name}_silver.py`
- [ ] SQL: `metadata_sql/migrations/V{version}__{pipeline_name}.sql`

### Validation Results
- SQL: ✅ Passed
- DAG: ✅ Passed
- Spark: ✅ Passed
- Security: ✅ Passed

### Rollback
To rollback: `git revert {commit_sha}`
```

## Output Schema

```json
{
  "deployment_id": "uuid",
  "status": "success|failed|rolled_back",
  
  "git": {
    "branch": "feature/pipeline-123-20250118",
    "commit_sha": "abc123",
    "pr_number": 456,
    "pr_url": "https://github.com/..."
  },
  
  "cicd": {
    "build_id": "xyz789",
    "build_url": "https://console.cloud.google.com/...",
    "status": "SUCCESS",
    "duration_seconds": 180
  },
  
  "verification": {
    "dag_found": true,
    "dag_url": "https://composer.../dags/{dag_id}"
  },
  
  "error": null
}
```

## Rollback Procedure

```python
def rollback(deployment_id: str, reason: str):
    # 1. Get deployment details
    deployment = get_deployment(deployment_id)
    
    # 2. Create revert commit
    git_revert(deployment.git.commit_sha)
    
    # 3. Push revert
    git_push(deployment.git.branch)
    
    # 4. Trigger CI/CD to remove DAG
    trigger_cicd(deployment.git.branch)
    
    # 5. Log rollback
    log_rollback(deployment_id, reason)
    
    # 6. Notify stakeholders
    notify(f"Pipeline {deployment.pipeline_name} rolled back: {reason}")
```

## Rules

1. **Never Force Push**: Always create new commits
2. **Always Verify**: Don't report success without verification
3. **Timeout Handling**: Fail after 30 min, don't wait forever
4. **Rollback Ready**: Be prepared to rollback any deployment
5. **Audit Trail**: Log every Git operation
```

```python
SUPERVISOR_SYSTEM_PROMPT = """
You are the Supervisor Agent for the Enterprise Agentic Data Engineering Platform.

Your responsibilities:
1. Receive and validate incoming pipeline requests from Jira tickets
2. Route tasks to specialized agents (Planner, Generator, Validator, Deployer)
3. Monitor agent progress and handle failures
4. Ensure all operations comply with platform policies
5. Report status back to Jira and stakeholders

Rules you MUST follow:
- NEVER skip validation steps
- ALWAYS log decisions with reasoning
- STOP immediately on critical failures
- REQUEST human approval for PROD deployments
- MAINTAIN audit trail for all operations

Available agents:
- Planner: Analyzes intent, determines strategy
- Generator: Creates DAGs, Spark jobs, metadata SQL
- Validator: Validates syntax, schemas, security
- Deployer: Commits to Git, triggers CI/CD

Current environment: {environment}
Human approval required: {human_approval_required}
"""
```

### Planner Agent Prompt

```python
PLANNER_SYSTEM_PROMPT = """
You are the Planner Agent for the Enterprise Agentic Data Engineering Platform.

Your responsibilities:
1. Parse and understand the structured intent JSON
2. Query existing metadata to understand current state
3. Determine if this is a new pipeline or modification
4. Detect schema evolution requirements
5. Select appropriate DAG and Spark templates
6. Plan resource allocation

You have access to these tools:
- query_metadata: Query PostgreSQL metadata database
- get_schema_version: Retrieve current schema for a pipeline
- compare_schemas: Compare two schema versions
- select_template: Choose appropriate template based on source type

Planning rules:
- New pipeline: Generate full metadata and code
- Existing pipeline: Generate delta changes only
- Schema change: Create new schema_version, preserve backward compatibility
- CDC enabled: Use CDC-specific templates
- PROD changes: Flag for human approval

Output your plan as structured JSON with:
- pipeline_action: "create" | "modify" | "upgrade_schema"
- template_selections: {dag_template, spark_templates}
- resource_allocation: {spark_config}
- approval_required: boolean
- reasoning: string explaining decisions
"""
```

### Generator Agent Prompt

```python
GENERATOR_SYSTEM_PROMPT = """
You are the Generator Agent for the Enterprise Agentic Data Engineering Platform.

Your responsibilities:
1. Generate metadata SQL (INSERT/UPDATE statements)
2. Generate Airflow DAGs from templates
3. Generate PySpark jobs from templates
4. Generate data quality rule configurations
5. Generate documentation

Code generation rules:
- NEVER invent schemas or logic not in metadata
- NEVER modify frozen templates
- ALWAYS use parameterized templates
- ALWAYS include proper error handling
- ALWAYS add logging and metrics

Templates available:
- DAG: file_ingest, db_snapshot, db_incremental, cdc_ingest, streaming_ingest, api_ingest
- Spark: bronze_ingest, silver_transform, cdc_merge, scd2_apply, gold_load_bq

Output format:
- metadata_sql: List of SQL statements
- dag_code: Python code for DAG
- spark_jobs: Dict of {job_name: python_code}
- dq_config: Data quality configuration
"""
```

### Validator Agent Prompt

```python
VALIDATOR_SYSTEM_PROMPT = """
You are the Validator Agent for the Enterprise Agentic Data Engineering Platform.

Your responsibilities:
1. Validate SQL syntax and semantics
2. Validate DAG syntax via dry-run import
3. Validate Spark code syntax
4. Check schema compatibility
5. Verify security policies
6. Ensure metadata consistency

Validation rules:
- SQL must be valid PostgreSQL
- DAGs must import without errors
- Spark code must parse correctly
- New schemas must be backward compatible
- No secrets in code
- No hard-coded paths

Validation tools:
- validate_sql: Check SQL syntax
- import_dag: Dry-run DAG import
- parse_spark: Validate PySpark syntax
- check_schema_compatibility: Compare schemas
- scan_secrets: Check for exposed secrets

Output format:
- validation_passed: boolean
- validation_results: List of {check, status, message}
- blocking_issues: List of critical failures
- warnings: List of non-blocking issues
"""
```

### Deployer Agent Prompt

```python
DEPLOYER_SYSTEM_PROMPT = """
You are the Deployer Agent for the Enterprise Agentic Data Engineering Platform.

Your responsibilities:
1. Create Git branch for deployment
2. Commit generated artifacts
3. Create Pull Request
4. Trigger CI/CD pipeline
5. Monitor deployment status
6. Verify successful deployment
7. Update Jira ticket status

Deployment rules:
- ALWAYS create branch from main
- ALWAYS include descriptive commit messages
- NEVER force push
- WAIT for CI/CD completion
- VERIFY DAG appears in Composer
- ROLLBACK on failure

Deployment tools:
- git_create_branch: Create new branch
- git_commit: Commit files
- git_push: Push to remote
- create_pr: Create Pull Request
- trigger_cicd: Trigger Cloud Build
- check_cicd_status: Monitor build status
- verify_dag: Check DAG in Composer
- rollback: Revert deployment

Output format:
- deployment_status: "success" | "failed" | "rolled_back"
- branch_name: string
- pr_url: string
- cicd_url: string
- dag_url: string
- error_message: string (if failed)
"""
```

---

## 25. Deployment Repository Structure

```
enterprise-data-pipelines-deploy/
│
├── README.md
├── .gitignore
│
├── dags/
│   ├── generated/                     # Agent-generated DAGs
│   │   ├── domain_1/
│   │   │   ├── pipeline_a_file_ingest.py
│   │   │   └── pipeline_b_cdc_ingest.py
│   │   └── domain_2/
│   │       └── pipeline_c_api_ingest.py
│   └── common/                        # Shared DAG utilities
│       ├── __init__.py
│       ├── base_dag.py
│       └── task_groups.py
│
├── spark_jobs/
│   ├── bronze/
│   │   ├── pipeline_a_bronze.py
│   │   └── pipeline_b_bronze.py
│   ├── silver/
│   │   ├── pipeline_a_silver.py
│   │   └── pipeline_b_silver.py
│   ├── cdc/
│   │   └── pipeline_b_cdc_merge.py
│   ├── modeling/
│   │   └── pipeline_a_dv2.py
│   └── gold/
│       ├── pipeline_a_gold.py
│       └── pipeline_b_gold.py
│
├── metadata_sql/
│   ├── migrations/
│   │   ├── V001__initial_schema.sql
│   │   ├── V002__add_pipeline_a.sql
│   │   └── V003__add_pipeline_b.sql
│   └── rollback/
│       ├── R001__rollback_pipeline_a.sql
│       └── R002__rollback_pipeline_b.sql
│
├── config/
│   ├── variables/
│   │   ├── dev.json
│   │   ├── qa.json
│   │   └── prod.json
│   └── connections/
│       ├── dev.json
│       └── prod.json
│
├── tests/
│   ├── dag_tests/
│   │   └── test_dag_import.py
│   ├── spark_tests/
│   │   └── test_transformations.py
│   └── integration/
│       └── test_pipeline_e2e.py
│
└── ci/
    ├── cloudbuild.yaml
    └── scripts/
        ├── validate_dags.sh
        ├── deploy_to_composer.sh
        └── run_tests.sh
```

---

## 26. CI/CD YAML Configurations

### Cloud Build Configuration

```yaml
# ci/cloudbuild.yaml
steps:
  # Step 1: Lint Python code
  - name: 'python:3.11'
    id: 'lint'
    entrypoint: 'bash'
    args:
      - '-c'
      - |
        pip install ruff black
        ruff check dags/ spark_jobs/
        black --check dags/ spark_jobs/

  # Step 2: Run unit tests
  - name: 'python:3.11'
    id: 'unit-tests'
    entrypoint: 'bash'
    args:
      - '-c'
      - |
        pip install pytest apache-airflow pyspark
        pytest tests/dag_tests/ tests/spark_tests/ -v --junitxml=test-results.xml
    waitFor: ['lint']

  # Step 3: Validate DAG imports
  - name: 'gcr.io/cloud-builders/gcloud'
    id: 'validate-dags'
    entrypoint: 'bash'
    args:
      - '-c'
      - |
        # Use Composer environment to validate DAGs
        gcloud composer environments run ${_COMPOSER_ENV} \
          --location ${_REGION} \
          dags list
        
        # Check for import errors
        for dag_file in dags/generated/**/*.py; do
          python -c "import ast; ast.parse(open('$dag_file').read())"
        done
    waitFor: ['unit-tests']

  # Step 4: Security scan
  - name: 'gcr.io/cloud-builders/gcloud'
    id: 'security-scan'
    entrypoint: 'bash'
    args:
      - '-c'
      - |
        pip install bandit detect-secrets
        bandit -r dags/ spark_jobs/ -ll
        detect-secrets scan --all-files
    waitFor: ['lint']

  # Step 5: Deploy DAGs to Composer
  - name: 'gcr.io/cloud-builders/gsutil'
    id: 'deploy-dags'
    entrypoint: 'bash'
    args:
      - '-c'
      - |
        # Get Composer DAG bucket
        DAG_BUCKET=$(gcloud composer environments describe ${_COMPOSER_ENV} \
          --location ${_REGION} \
          --format='get(config.dagGcsPrefix)')
        
        # Sync DAGs
        gsutil -m rsync -r -d dags/generated/ ${DAG_BUCKET}/generated/
        gsutil -m rsync -r dags/common/ ${DAG_BUCKET}/common/
    waitFor: ['validate-dags', 'security-scan']

  # Step 6: Deploy Spark jobs to GCS
  - name: 'gcr.io/cloud-builders/gsutil'
    id: 'deploy-spark'
    entrypoint: 'bash'
    args:
      - '-c'
      - |
        gsutil -m rsync -r spark_jobs/ gs://${_SPARK_BUCKET}/spark_jobs/
    waitFor: ['security-scan']

  # Step 7: Apply metadata migrations
  - name: 'gcr.io/cloud-builders/gcloud'
    id: 'apply-migrations'
    entrypoint: 'bash'
    args:
      - '-c'
      - |
        # Connect to Cloud SQL and apply migrations
        gcloud sql connect ${_METADATA_DB} --user=postgres << EOF
        \i metadata_sql/migrations/*.sql
        EOF
    waitFor: ['deploy-dags', 'deploy-spark']

  # Step 8: Notify on completion
  - name: 'gcr.io/cloud-builders/gcloud'
    id: 'notify'
    entrypoint: 'bash'
    args:
      - '-c'
      - |
        curl -X POST "${_SLACK_WEBHOOK}" \
          -H "Content-Type: application/json" \
          -d '{
            "text": "✅ Pipeline deployment completed",
            "blocks": [
              {
                "type": "section",
                "text": {
                  "type": "mrkdwn",
                  "text": "*Deployment Successful*\n\nEnvironment: `${_ENV}`\nBuild: `${BUILD_ID}`\nCommit: `${SHORT_SHA}`"
                }
              }
            ]
          }'
    waitFor: ['apply-migrations']

substitutions:
  _ENV: 'dev'
  _REGION: 'us-central1'
  _COMPOSER_ENV: 'composer-dev'
  _SPARK_BUCKET: 'data-platform-spark-dev'
  _METADATA_DB: 'metadata-db-dev'
  _SLACK_WEBHOOK: 'https://hooks.slack.com/services/xxx'

options:
  logging: CLOUD_LOGGING_ONLY
  machineType: 'E2_HIGHCPU_8'

timeout: '1800s'

artifacts:
  objects:
    location: 'gs://${_SPARK_BUCKET}/build-artifacts/${BUILD_ID}/'
    paths:
      - 'test-results.xml'
```

### GitHub Actions Configuration

```yaml
# .github/workflows/ci.yaml
name: CI/CD Pipeline

on:
  push:
    branches: [main, 'release/*']
  pull_request:
    branches: [main]

env:
  GCP_PROJECT_DEV: ${{ secrets.GCP_PROJECT_DEV }}
  GCP_PROJECT_PROD: ${{ secrets.GCP_PROJECT_PROD }}
  GCP_REGION: us-central1

jobs:
  lint-and-test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      
      - name: Install dependencies
        run: |
          pip install ruff black pytest apache-airflow pyspark
      
      - name: Lint with ruff
        run: ruff check dags/ spark_jobs/
      
      - name: Check formatting with black
        run: black --check dags/ spark_jobs/
      
      - name: Run unit tests
        run: pytest tests/ -v --junitxml=test-results.xml
      
      - name: Upload test results
        uses: actions/upload-artifact@v4
        with:
          name: test-results
          path: test-results.xml

  validate-dags:
    runs-on: ubuntu-latest
    needs: lint-and-test
    steps:
      - uses: actions/checkout@v4
      
      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      
      - name: Install Airflow
        run: pip install apache-airflow
      
      - name: Validate DAG syntax
        run: |
          export AIRFLOW_HOME=$(pwd)/airflow_home
          airflow db init
          for dag in dags/generated/**/*.py; do
            python -c "import ast; ast.parse(open('$dag').read())"
            echo "✓ $dag valid"
          done

  security-scan:
    runs-on: ubuntu-latest
    needs: lint-and-test
    steps:
      - uses: actions/checkout@v4
      
      - name: Run Bandit security scan
        run: |
          pip install bandit
          bandit -r dags/ spark_jobs/ -ll -f json -o bandit-report.json
      
      - name: Check for secrets
        uses: trufflesecurity/trufflehog@main
        with:
          path: ./
          base: ${{ github.event.pull_request.base.sha }}
          head: ${{ github.event.pull_request.head.sha }}

  deploy-dev:
    runs-on: ubuntu-latest
    needs: [validate-dags, security-scan]
    if: github.ref == 'refs/heads/main'
    environment: dev
    steps:
      - uses: actions/checkout@v4
      
      - name: Authenticate to GCP
        uses: google-github-actions/auth@v2
        with:
          credentials_json: ${{ secrets.GCP_SA_KEY_DEV }}
      
      - name: Set up Cloud SDK
        uses: google-github-actions/setup-gcloud@v2
      
      - name: Deploy to DEV
        run: |
          gcloud builds submit \
            --config=ci/cloudbuild.yaml \
            --substitutions=_ENV=dev,_COMPOSER_ENV=composer-dev

  deploy-prod:
    runs-on: ubuntu-latest
    needs: deploy-dev
    if: startsWith(github.ref, 'refs/heads/release/')
    environment: production
    steps:
      - uses: actions/checkout@v4
      
      - name: Authenticate to GCP
        uses: google-github-actions/auth@v2
        with:
          credentials_json: ${{ secrets.GCP_SA_KEY_PROD }}
      
      - name: Set up Cloud SDK
        uses: google-github-actions/setup-gcloud@v2
      
      - name: Deploy to PROD
        run: |
          gcloud builds submit \
            --config=ci/cloudbuild.yaml \
            --substitutions=_ENV=prod,_COMPOSER_ENV=composer-prod
```

---

## 27. API Contracts

### Intent API (Pipeline Creation Request)

```yaml
openapi: 3.0.3
info:
  title: Data Engineering Platform API
  version: 1.0.0
  description: API for submitting pipeline creation/modification requests

paths:
  /api/v1/pipelines:
    post:
      summary: Create or modify a pipeline
      description: Submit a structured intent for pipeline generation
      operationId: createPipeline
      tags:
        - Pipelines
      requestBody:
        required: true
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/PipelineIntent'
      responses:
        '202':
          description: Request accepted for processing
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/PipelineResponse'
        '400':
          description: Invalid request
        '401':
          description: Unauthorized

components:
  schemas:
    PipelineIntent:
      type: object
      required:
        - pipeline_name
        - domain
        - source_type
        - source_config
        - schema_definition
      properties:
        pipeline_name:
          type: string
          pattern: '^[a-z][a-z0-9_]*$'
          example: "customer_orders_daily"
        domain:
          type: string
          example: "sales"
        source_type:
          type: string
          enum: [file, database, streaming, api]
        processing_mode:
          type: string
          enum: [batch, micro_batch, streaming]
          default: batch
        modeling_strategy:
          type: string
          enum: [dv2, star, flat, none]
        source_config:
          $ref: '#/components/schemas/SourceConfig'
        schema_definition:
          $ref: '#/components/schemas/SchemaDefinition'
        transformation_rules:
          type: array
          items:
            $ref: '#/components/schemas/TransformationRule'
        data_quality_rules:
          type: array
          items:
            $ref: '#/components/schemas/DataQualityRule'
        target_config:
          $ref: '#/components/schemas/TargetConfig'
        execution_policy:
          $ref: '#/components/schemas/ExecutionPolicy'
        jira_ticket:
          type: string
          example: "DATA-1234"
    
    SourceConfig:
      type: object
      properties:
        source_system:
          type: string
        connection_type:
          type: string
          enum: [gcs, jdbc, kafka, pubsub, api]
        landing_path:
          type: string
        file_format:
          type: string
        cdc_enabled:
          type: boolean
          default: false
    
    SchemaDefinition:
      type: object
      properties:
        columns:
          type: array
          items:
            type: object
            properties:
              name:
                type: string
              type:
                type: string
              nullable:
                type: boolean
              pii:
                type: boolean
        primary_keys:
          type: array
          items:
            type: string
    
    PipelineResponse:
      type: object
      properties:
        request_id:
          type: string
          format: uuid
        status:
          type: string
          enum: [accepted, processing, completed, failed]
        pipeline_id:
          type: integer
        tracking_url:
          type: string
          format: uri
```

---

## 28. Implementation Roadmap

### Phase 1: Foundation (Weeks 1-4)

| Task | Description | Owner |
|------|-------------|-------|
| Infrastructure setup | Terraform for Composer, Dataproc, BigQuery | Platform Team |
| Metadata database | PostgreSQL schema deployment | Data Team |
| Base templates | DAG and Spark template library | Data Team |
| CI/CD pipeline | Cloud Build configuration | DevOps Team |

### Phase 2: Agent Development (Weeks 5-8)

| Task | Description | Owner |
|------|-------------|-------|
| Supervisor agent | LangGraph orchestration layer | AI Team |
| Planner agent | Intent parsing and strategy | AI Team |
| Generator agent | Code generation logic | AI Team |
| Validator agent | Validation framework | AI Team |
| Deployer agent | Git and CI/CD integration | AI Team |

### Phase 3: Integration (Weeks 9-12)

| Task | Description | Owner |
|------|-------------|-------|
| Jira integration | Webhook and ticket management | Platform Team |
| UI development | Self-service portal | Frontend Team |
| Monitoring setup | Dashboards and alerts | SRE Team |
| Security hardening | IAM, VPC, encryption | Security Team |

### Phase 4: Production Readiness (Weeks 13-16)

| Task | Description | Owner |
|------|-------------|-------|
| Load testing | Performance validation | QA Team |
| DR testing | Disaster recovery validation | SRE Team |
| Documentation | Runbooks and training | All Teams |
| Production deployment | Phased rollout | Platform Team |

---

## 29. Appendix

### A. Glossary

| Term | Definition |
|------|------------|
| **Agent** | AI-powered component that performs specific tasks autonomously |
| **DAG** | Directed Acyclic Graph - Airflow workflow definition |
| **CDC** | Change Data Capture - streaming database changes |
| **SCD2** | Slowly Changing Dimension Type 2 - history tracking pattern |
| **Iceberg** | Open table format for large analytical datasets |
| **Medallion** | Data architecture pattern (Bronze/Silver/Gold layers) |
| **DV2** | Data Vault 2.0 - data modeling methodology |
| **GitOps** | Infrastructure and deployment managed via Git |

### B. Reference Links

- [Apache Iceberg Documentation](https://iceberg.apache.org/docs/latest/)
- [Cloud Composer Documentation](https://cloud.google.com/composer/docs)
- [Dataproc Serverless Documentation](https://cloud.google.com/dataproc-serverless/docs)
- [BigQuery Documentation](https://cloud.google.com/bigquery/docs)
- [LangGraph Documentation](https://langchain-ai.github.io/langgraph/)
- [Debezium Documentation](https://debezium.io/documentation/)

### C. Contact Information

| Role | Contact |
|------|---------|
| Platform Owner | data-platform@company.com |
| On-Call Support | #data-platform-oncall (Slack) |
| Security Team | security@company.com |

---

## Important Notes for Claude Code / LLM Implementation

When implementing this platform, Claude (or any LLM) MUST:

1. **Follow this README exactly** - Do not invent new patterns or deviate from specifications
2. **Respect metadata contracts** - All schemas and structures defined here are authoritative
3. **Generate deterministic outputs** - Same input should produce same output
4. **Never hard-code logic** - All business logic must be metadata-driven
5. **Preserve backward compatibility** - Existing pipelines must not break
6. **Log all decisions** - Every agent action must be auditable
7. **Stop on validation failure** - Never auto-fix or proceed with invalid state

---

**END OF DOCUMENT**

*Version: 1.0.0*
*Last Updated: January 2026*
*Maintained by: Enterprise Data Platform Team*