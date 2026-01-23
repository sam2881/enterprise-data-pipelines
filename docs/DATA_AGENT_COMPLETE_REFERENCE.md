# AI Agent Platform - Complete Reference Documentation

## Executive Summary

This document provides a comprehensive reference for the AI Agent Platform, covering all four major components:
1. **Backend** - IT Service Management agents (ServiceNow, Jira, Infrastructure)
2. **Data Agent** - Data Pipeline generation with Medallion Architecture
3. **Frontend** - Next.js dashboard for incident management
4. **Infrastructure** - Hybrid deployment (GCP Services + Docker)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                     AI AGENT PLATFORM (Hybrid Architecture)                      │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐              │
│  │    FRONTEND     │    │     BACKEND     │    │   DATA AGENT    │              │
│  │   (Next.js)     │◄──►│   (FastAPI)     │◄──►│   (FastAPI)     │              │
│  │   Port: 3000    │    │   Port: 8000    │    │   Port: 8001    │              │
│  └─────────────────┘    └────────┬────────┘    └────────┬────────┘              │
│                                  │                      │                        │
│  ┌───────────────────────────────┴──────────────────────┴───────────────────┐   │
│  │                    DOCKER INFRASTRUCTURE (Local)                          │   │
│  │  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐             │   │
│  │  │ Kafka   │ │ Redis   │ │Postgres │ │Weaviate │ │  Neo4j  │             │   │
│  │  │(Events) │ │(Cache)  │ │(State)  │ │(Vector) │ │(Graph)  │             │   │
│  │  └─────────┘ └─────────┘ └─────────┘ └─────────┘ └─────────┘             │   │
│  │  ┌─────────┐ ┌─────────┐                                                  │   │
│  │  │ Airflow │ │  Trino  │  (Orchestration & Query Engine)                  │   │
│  │  └─────────┘ └─────────┘                                                  │   │
│  └──────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  ┌──────────────────────────────────────────────────────────────────────────┐   │
│  │                    GCP INFRASTRUCTURE (Managed)                           │   │
│  │  ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐             │   │
│  │  │      GCS        │ │    BigQuery     │ │    Dataproc     │             │   │
│  │  │ bronze/silver/  │ │  (Analytics)    │ │  (Spark Jobs)   │             │   │
│  │  │     gold        │ │                 │ │   on-demand     │             │   │
│  │  └─────────────────┘ └─────────────────┘ └─────────────────┘             │   │
│  └──────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## Component 1: BACKEND (IT Service Management)

### Purpose
Handles IT service incidents from ServiceNow, processes Jira stories, and executes infrastructure remediation.

### Key Files (84 Python files, ~30,441 LOC)

#### 1.1 Agents (`backend/agents/`)

| File | Lines | Purpose |
|------|-------|---------|
| `base_agent.py` | 154 | Abstract base class with LangFuse observability |
| `servicenow/agent.py` | 330 | ServiceNow incident triage and resolution |
| `jira/agent.py` | 204 | Jira story processing and task breakdown |
| `infra/agent.py` | 125 | Terraform/Ansible automation |
| `infra/autonomous_vm_recovery.py` | 415 | Specialized VM failure recovery |
| `data/agent.py` | 574 | Data pipeline generation (Two-step PII protection) |
| `remediation/agent.py` | 1,175 | Script matching and execution workflow |
| `control_plane.py` | 100+ | Policy-based approval routing |

#### 1.2 Orchestration (`backend/orchestrator/`)

| File | Lines | Purpose |
|------|-------|---------|
| `main.py` | 1,258 | REST API with 40+ endpoints |
| `langgraph_workflow.py` | 1,131 | 14-step LangGraph workflow |
| `llm_intelligence.py` | 785 | LLM-powered analysis engine |
| `llm_judge.py` | 506 | GPT-4 plan validation |
| `rollback_generator.py` | 556 | Rollback plan generation |
| `metrics.py` | 543 | Prometheus metrics |

#### 1.3 RAG System (`backend/rag/`)

| File | Lines | Purpose |
|------|-------|---------|
| `hybrid_search_engine.py` | 1,087 | RRF multi-agent fusion search |
| `intelligent_retriever.py` | 950 | Full RAG pipeline orchestrator |
| `swarm_retriever.py` | 529 | Swarm consensus retrieval |
| `embedding_service.py` | 532 | OpenAI embeddings with Redis cache |
| `graph_scorer.py` | 598 | Neo4j historical success scoring |
| `query_understanding.py` | 540 | Intent/entity extraction |

**4-Agent RAG Architecture:**
```
Query → [Vector Agent] ──┐
      → [Graph Agent]  ──┼──► RRF Fusion → Cross-Encoder → Results
      → [Keyword Agent]──┤
      → [Metadata Agent]─┘
```

#### 1.4 API Endpoints Summary

```
# Incidents
GET  /api/incidents              - List ServiceNow incidents
GET  /api/incidents/{id}         - Get incident details
POST /api/incidents/{id}/close   - Close incident

# Script Matching
GET  /api/scripts                - List remediation scripts
POST /api/scripts/match          - Match incident to scripts

# Execution
POST /api/execute                - Execute with approval workflow
GET  /api/execute/{id}           - Get execution status
POST /api/approvals/{id}/approve - Approve execution
POST /api/approvals/{id}/reject  - Reject execution

# LangGraph Workflow
POST /api/langgraph/run          - Run full 14-step workflow

# Data Pipelines
POST /api/pipelines              - Create data pipeline
GET  /api/pipelines/{id}/spark   - Get generated Spark code
GET  /api/pipelines/{id}/dag     - Get generated Airflow DAG
```

---

## Component 2: DATA AGENT (Data Pipeline Generation)

### Purpose
Generate metadata-driven data pipelines with Medallion Architecture (Bronze → Silver → Gold) using GCP services.

### Key Files (100 Python files)

#### 2.1 Pipeline Agents (`data_agent/src/agents/`)

| File | Purpose |
|------|---------|
| `base_agent.py` | Base class for data agents |
| `analysis_agent.py` | Source schema analysis |
| `planning_agent.py` | Pipeline planning |
| `spark_generator_agent.py` | PySpark code generation |
| `dag_generator_agent.py` | Airflow DAG generation |
| `dq_generator_agent.py` | Great Expectations rules |
| `ssis_migration_agent.py` | SSIS to Spark migration |

#### 2.2 Metadata-Driven Pipelines (`data_agent/pipelines/`)

**Architecture:**
```
pipelines/
├── sql/ddl/
│   └── create_metadata_tables.sql    # 7 metadata tables
├── common/
│   ├── utils/
│   │   ├── metadata_reader.py        # Query metadata at runtime
│   │   ├── schema_builder.py         # Build Spark schemas
│   │   ├── file_validator.py         # DQ validation
│   │   ├── audit_logger.py           # Execution audit
│   │   └── xcom_utils.py             # Airflow XCom helpers
│   ├── bronze/
│   │   └── bronze_loader.py          # Generic Bronze loader
│   ├── silver/
│   │   └── silver_transformer.py     # Generic Silver transformer
│   └── gold/
│       └── gold_aggregator.py        # Generic Gold aggregator
├── customer_transactions/            # Example feed
│   ├── dags/
│   │   └── customer_transactions_dag.py
│   ├── metadata/
│   │   └── insert_metadata.sql
│   └── tests/
│       └── test_e2e_pipeline.py
└── _templates/
    ├── dag_template.py
    └── insert_metadata_template.sql
```

#### 2.3 Metadata Tables

| Table | Purpose |
|-------|---------|
| `feed_registry` | Master feed configuration (schedule, owner, pattern) |
| `feed_columns` | Column definitions (name, type, transforms) |
| `feed_validation` | DQ rules (not_null, range, pattern) |
| `feed_targets` | Target tables per layer (Bronze/Silver/Gold) |
| `feed_aggregations` | Gold layer aggregation definitions |
| `feed_state` | Execution state per posting_date |
| `feed_audit_log` | Detailed execution audit trail |

#### 2.4 Medallion Architecture Rules

| Layer | Data Types | Storage | Transformations |
|-------|------------|---------|-----------------|
| **Bronze** | ALL STRING | gs://{project}-bronze | None - raw ingestion |
| **Silver** | From metadata | gs://{project}-silver | Type casting, validation, cleansing |
| **Gold** | Aggregated | gs://{project}-gold / BigQuery | Business aggregations |

---

## Component 3: FRONTEND (Dashboard)

### Purpose
Next.js dashboard for incident management, workflow visualization, and approvals.

### Key Files (`frontend/src/`)

| Directory | Purpose |
|-----------|---------|
| `app/` | Next.js 14 App Router pages |
| `components/chat/` | Chat interface for incidents |
| `components/incidents/` | Incident detail, workflow, remediation panels |
| `components/workflow/` | LangGraph workflow visualization |
| `components/layout/` | Sidebar, navigation |

### Key Components

| Component | Purpose |
|-----------|---------|
| `IncidentChat.tsx` | Chat interface for incident resolution |
| `IncidentWorkflow.tsx` | 14-step workflow progress |
| `RemediationPanel.tsx` | Script matching and execution |
| `WorkflowVisualization.tsx` | D3.js workflow graph |
| `ChatWrapper.tsx` | Real-time chat with WebSocket |

---

## Component 4: INFRASTRUCTURE (Hybrid: GCP + Docker)

### Purpose
Cost-optimized hybrid deployment using GCP managed services for data processing and Docker for orchestration.

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    HYBRID INFRASTRUCTURE                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  GCP SERVICES (Pay-per-use):                                    │
│  ├── GCS Buckets (Storage)                                      │
│  │   ├── gs://{project}-bronze     Raw data                     │
│  │   ├── gs://{project}-silver     Cleaned data                 │
│  │   ├── gs://{project}-gold       Aggregated data              │
│  │   └── gs://{project}-temp       Temporary (7-day lifecycle)  │
│  ├── BigQuery (Analytics)                                       │
│  │   └── data_warehouse dataset                                 │
│  └── Dataproc (Spark - On-demand)                               │
│      └── ai-agent-spark cluster                                 │
│                                                                  │
│  DOCKER SERVICES (Local):                                       │
│  ├── Core: Kafka, Redis, PostgreSQL                             │
│  ├── Vector/Graph: Weaviate, Neo4j                              │
│  ├── Orchestration: Airflow (submits to Dataproc)               │
│  ├── Query: Trino (queries GCS + BigQuery)                      │
│  ├── Apps: Backend, Data Agent, Frontend                        │
│  └── Observability: Prometheus, Grafana, LangFuse               │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Key Files (`infrastructure/`)

```
infrastructure/
├── docker-compose.yml        # Docker services configuration
├── init-postgres.sql         # Database initialization
├── start-all.sh              # Start platform (validates GCP config)
├── stop-all.sh               # Stop all services
├── trino/
│   └── catalog/
│       ├── bigquery.properties   # Trino → BigQuery
│       ├── gcs.properties        # Trino → GCS
│       └── postgresql.properties # Trino → PostgreSQL
└── scripts/
    ├── setup-gcp.sh          # Create GCP resources
    └── test-local-e2e.sh     # E2E testing
```

### Service Ports

| Service | Port | URL |
|---------|------|-----|
| **Application** | | |
| Frontend | 3000 | http://localhost:3000 |
| Backend API | 8000 | http://localhost:8000 |
| Data Agent API | 8001 | http://localhost:8001 |
| **Data Infrastructure** | | |
| Airflow | 8083 | http://localhost:8083 |
| Trino | 8084 | http://localhost:8084 |
| **Observability** | | |
| Grafana | 3001 | http://localhost:3001 |
| LangFuse | 3002 | http://localhost:3002 |
| Prometheus | 9090 | http://localhost:9090 |
| Kafka UI | 8085 | http://localhost:8085 |
| **Databases** | | |
| Weaviate | 8081 | http://localhost:8081 |
| Neo4j | 7474 | http://localhost:7474 |
| PostgreSQL | 5432 | - |
| Redis | 6379 | - |

### GCP Services

#### Google Cloud Storage (GCS)
- **Purpose**: Object storage for Medallion Architecture
- **Buckets**:
  - `gs://{project}-bronze` - Raw data (ALL STRING)
  - `gs://{project}-silver` - Cleaned & typed data
  - `gs://{project}-gold` - Aggregated analytics
  - `gs://{project}-temp` - Temporary files (auto-delete 7 days)

#### BigQuery
- **Purpose**: Analytics data warehouse
- **Dataset**: `data_warehouse`
- **Tables**: `pipeline_runs`, `data_quality_results`
- **Pricing**: On-demand (pay per query)

#### Dataproc (Spark)
- **Purpose**: Distributed data processing
- **Cluster**: Created on-demand by Airflow
- **Cost Optimization**: Uses preemptible VMs, auto-deletes after job

### Cost Optimization

| Service | Strategy | Estimated Cost |
|---------|----------|----------------|
| GCS | Standard storage class | ~$5-10/month |
| BigQuery | On-demand pricing | ~$5-20/month |
| Dataproc | On-demand clusters, preemptible VMs | ~$0 idle, $2-5/hr running |

---

## GCP Setup Guide

### Prerequisites
- GCP account with billing enabled
- `gcloud` CLI installed and authenticated
- `gsutil` and `bq` CLI tools

### Quick Setup

```bash
# 1. Set your project ID
export GCP_PROJECT_ID=your-project-id

# 2. Run setup script
cd infrastructure/scripts
./setup-gcp.sh

# 3. Copy GCP settings to .env
cat .env.gcp >> ../.env

# 4. Start the platform
cd ..
./start-all.sh
```

### Manual Setup (Alternative)

```bash
# Enable APIs
gcloud services enable storage.googleapis.com bigquery.googleapis.com dataproc.googleapis.com

# Create GCS buckets
gsutil mb -l us-central1 gs://${GCP_PROJECT_ID}-bronze
gsutil mb -l us-central1 gs://${GCP_PROJECT_ID}-silver
gsutil mb -l us-central1 gs://${GCP_PROJECT_ID}-gold
gsutil mb -l us-central1 gs://${GCP_PROJECT_ID}-temp

# Create BigQuery dataset
bq mk --dataset ${GCP_PROJECT_ID}:data_warehouse

# Create service account
gcloud iam service-accounts create ai-agent-sa
gcloud iam service-accounts keys create gcp-key.json --iam-account=ai-agent-sa@${GCP_PROJECT_ID}.iam.gserviceaccount.com

# Grant permissions
gcloud projects add-iam-policy-binding ${GCP_PROJECT_ID} --member="serviceAccount:ai-agent-sa@${GCP_PROJECT_ID}.iam.gserviceaccount.com" --role="roles/storage.objectAdmin"
gcloud projects add-iam-policy-binding ${GCP_PROJECT_ID} --member="serviceAccount:ai-agent-sa@${GCP_PROJECT_ID}.iam.gserviceaccount.com" --role="roles/bigquery.dataEditor"
gcloud projects add-iam-policy-binding ${GCP_PROJECT_ID} --member="serviceAccount:ai-agent-sa@${GCP_PROJECT_ID}.iam.gserviceaccount.com" --role="roles/bigquery.jobUser"
gcloud projects add-iam-policy-binding ${GCP_PROJECT_ID} --member="serviceAccount:ai-agent-sa@${GCP_PROJECT_ID}.iam.gserviceaccount.com" --role="roles/dataproc.editor"
```

---

## Environment Variables Required

Create `.env` file in `infrastructure/` directory:

```bash
# =============================================================================
# AI Agent Platform - Environment Variables
# =============================================================================

# LLM API Keys (required)
OPENAI_API_KEY=your-openai-key
ANTHROPIC_API_KEY=your-anthropic-key

# =============================================================================
# GCP CONFIGURATION (REQUIRED)
# =============================================================================

GCP_PROJECT_ID=your-gcp-project-id
GCP_REGION=us-central1
GCP_ZONE=us-central1-a

# Service Account Key
GOOGLE_APPLICATION_CREDENTIALS=./gcp-key.json

# GCS Buckets (Medallion Architecture)
GCS_BUCKET_BRONZE=your-project-bronze
GCS_BUCKET_SILVER=your-project-silver
GCS_BUCKET_GOLD=your-project-gold
GCS_BUCKET_TEMP=your-project-temp

# BigQuery
BIGQUERY_DATASET=data_warehouse
BIGQUERY_LOCATION=US

# Dataproc
DATAPROC_CLUSTER=ai-agent-spark
DATAPROC_REGION=us-central1

# =============================================================================
# DOCKER SERVICES
# =============================================================================

# Airflow
AIRFLOW_FERNET_KEY=81HqDtbqAywKSOumSha3BhWNOdQ26slT6K0YaZeZyPs=
AIRFLOW_SECRET_KEY=change-this-airflow-secret-key

# LangFuse
LANGFUSE_PUBLIC_KEY=
LANGFUSE_SECRET_KEY=
NEXTAUTH_SECRET=change-this-secret
SALT=change-this-salt

# =============================================================================
# OPTIONAL INTEGRATIONS
# =============================================================================

# ServiceNow
SNOW_INSTANCE_URL=
SNOW_USERNAME=
SNOW_PASSWORD=

# GitHub
GITHUB_TOKEN=
GITHUB_OWNER=
GITHUB_REPO=
GITHUB_DATA_REPO=data-pipelines

# Slack
SLACK_WEBHOOK_URL=
SLACK_CHANNEL=
```

---

## Quick Start Commands

### Start Platform

```bash
cd infrastructure

# First time: Setup GCP resources
./scripts/setup-gcp.sh

# Start all services
./start-all.sh

# Start with rebuild
./start-all.sh --build

# Start only infrastructure
./start-all.sh --infra-only
```

### Stop Platform

```bash
cd infrastructure
./stop-all.sh
```

### View Logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f data-agent
docker-compose logs -f airflow-scheduler
```

### Query Data

```bash
# Trino CLI (query GCS + BigQuery)
docker exec -it ai-agent-trino trino

# Query BigQuery via Trino
docker exec -it ai-agent-trino trino --catalog bigquery --schema data_warehouse

# List GCS files
gsutil ls gs://${GCS_BUCKET_BRONZE}/
```

---

## End-to-End Flow Examples

### Flow 1: ServiceNow Incident → Remediation

```
1. ServiceNow Incident Created
   ↓
2. Kafka Consumer (servicenow.incidents topic)
   ↓
3. LangGraph Workflow (14 steps):
   - Ingest → Parse → Classify → Swarm RAG → Generate Plan
   - Judge Evaluation → Control Plane → Await Approval
   - Execute (GitHub Actions) → Verify → Close Ticket → Feedback Loop
   ↓
4. Incident Resolved
```

### Flow 2: Jira Story → Data Pipeline (GCP)

```
1. Jira Story Created (Data Pipeline Request)
   ↓
2. Kafka Consumer (jira.stories topic)
   ↓
3. Data Pipeline Agent:
   - Analyze Source (GCS schema inference)
   - Generate IR (intermediate representation)
   - Generate PySpark Code (for Dataproc)
   - Generate Airflow DAG
   - Generate DQ Rules
   ↓
4. GitHub MR Created with generated code
   ↓
5. Airflow DAG deployed → Triggers Dataproc job
```

### Flow 3: Pipeline Execution (GCP)

```
1. Airflow DAG triggered
   ↓
2. Bronze Task (GCS):
   - Read from gs://{project}-raw-data/
   - Load as ALL STRING
   - Write to gs://{project}-bronze/
   ↓
3. Silver Task (Dataproc Spark):
   - Create Dataproc cluster (on-demand)
   - Read from gs://{project}-bronze/
   - Type casting, validation
   - Write to gs://{project}-silver/
   - Delete cluster
   ↓
4. Gold Task (BigQuery):
   - Read from gs://{project}-silver/
   - Aggregations
   - Write to BigQuery data_warehouse
```

---

## Testing Commands

```bash
# Backend unit tests
pytest backend/tests/unit/ -v

# Data Agent unit tests
pytest data_agent/tests/unit/ -v

# E2E pipeline test
python data_agent/pipelines/customer_transactions/tests/test_e2e_pipeline.py

# Integration tests
pytest tests/integration/ -v
```

---

## File Statistics Summary

| Component | Python Files | Lines of Code | Key Directories |
|-----------|--------------|---------------|-----------------|
| Backend | 84 | ~30,441 | agents/, orchestrator/, rag/, governance/ |
| Data Agent | 100 | ~15,000 | agents/, pipelines/, mcp_servers/ |
| Frontend | - | ~5,000 | app/, components/, types/ |
| Infrastructure | - | ~1,500 | docker-compose.yml, scripts/ |
| **Total** | **184+** | **~52,000** | - |

---

## Database Credentials

| Database | Host | Port | User | Password |
|----------|------|------|------|----------|
| PostgreSQL | localhost | 5432 | admin | admin123 |
| Redis | localhost | 6379 | - | - |
| Neo4j | localhost | 7474/7687 | neo4j | adminadmin |
| Weaviate | localhost | 8081 | - | - |
| Airflow | localhost | 8083 | admin | admin123 |
| Grafana | localhost | 3001 | admin | admin |

---

## Troubleshooting

### GCP Connection Issues

```bash
# Verify service account
gcloud auth activate-service-account --key-file=gcp-key.json

# Test GCS access
gsutil ls gs://${GCS_BUCKET_BRONZE}/

# Test BigQuery access
bq ls ${GCP_PROJECT_ID}:data_warehouse
```

### Docker Service Issues

```bash
# Check service health
docker-compose ps

# View logs for specific service
docker-compose logs -f airflow-scheduler

# Restart a service
docker-compose restart data-agent
```

### Reset Everything

```bash
cd infrastructure
docker-compose down -v  # Remove containers and volumes
./start-all.sh --build  # Rebuild and start fresh
```
