# Project Setup and Run Guide

## Overview

This guide covers how to run the **Enterprise Agentic Platform** with minimal resources.

### Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                    LOCAL DEVELOPMENT (Docker)                        │
├─────────────────────────────────────────────────────────────────────┤
│  Kafka ─────────► Event Bus (System of Record)                      │
│  Airflow ───────► Local DAG Orchestration (REST API)                │
│  PostgreSQL ────► State Persistence                                  │
│  Redis ─────────► Caching & Short-term Memory                       │
│  Weaviate ──────► Vector Store (RAG)                                │
│  Neo4j ─────────► Knowledge Graph                                    │
│  Orchestrator ──► Main Backend API (LangGraph)                      │
│  Data Agent ────► Pipeline Generation (LangGraph)                   │
│  MCP Servers ───► ServiceNow & Jira Event Polling                   │
│  Frontend ──────► Next.js UI                                         │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                    GCP PRODUCTION                                    │
├─────────────────────────────────────────────────────────────────────┤
│  Cloud Composer ► Managed Airflow (DAGs synced via GitHub Actions)  │
│  GCS Buckets ───► Medallion Architecture (bronze/silver/gold)       │
│  BigQuery ──────► Data Warehouse                                     │
│  Dataproc ──────► Spark Jobs                                         │
└─────────────────────────────────────────────────────────────────────┘
```

### Key Changes (v2.0)

- **Unified Dockerfile**: Single Dockerfile for all Python services
- **Local Airflow**: Replaces Cloud Composer for local development
- **MCP Servers in Docker**: ServiceNow and Jira polling services
- **Kafka as System of Record**: All events flow through Kafka

---

## Prerequisites

1. **Docker & Docker Compose** (v2.0+)
2. **Python 3.11+**
3. **Node.js 18+** (for frontend)
4. **API Keys**: OpenAI and/or Anthropic
5. **Optional - GCP Account** (for production deployment only)
6. **Optional - ServiceNow/Jira** (for MCP integrations)

---

## Step 1: Clone and Configure

```bash
# Clone the repository
git clone <your-repo-url>
cd ai_agent_app

# Copy environment template
cp .env.example .env
```

### Configure `.env` file:

```bash
# =============================================================================
# Required - LLM APIs (at least one)
# =============================================================================
OPENAI_API_KEY=sk-your-openai-key
ANTHROPIC_API_KEY=sk-ant-your-anthropic-key

# =============================================================================
# Environment
# =============================================================================
ENVIRONMENT=dev

# =============================================================================
# Database (defaults work for local Docker)
# =============================================================================
POSTGRES_USER=admin
POSTGRES_PASSWORD=admin123
POSTGRES_DB=agentdb

# =============================================================================
# Airflow (local - auto-configured by Docker)
# =============================================================================
AIRFLOW_FERNET_KEY=81HqDtbqAywKSOumSha3BhWNOdQ26slT6K0YaZeZyPs=

# =============================================================================
# Optional - ServiceNow (for incident management)
# =============================================================================
SNOW_INSTANCE_URL=https://your-instance.service-now.com
SNOW_USERNAME=your-username
SNOW_PASSWORD=your-password
SNOW_POLL_INTERVAL=60

# =============================================================================
# Optional - Jira (for pipeline requests)
# =============================================================================
JIRA_URL=https://your-org.atlassian.net
JIRA_USERNAME=your-email@company.com
JIRA_API_TOKEN=your-jira-token
JIRA_PROJECT_KEY=DATA
JIRA_POLL_INTERVAL=120

# =============================================================================
# Optional - GCP (for production deployment)
# =============================================================================
GCP_PROJECT_ID=your-gcp-project-id
GCP_REGION=us-central1
GCS_BUCKET_RAW=${GCP_PROJECT_ID}-raw
GCS_BUCKET_BRONZE=${GCP_PROJECT_ID}-bronze
GCS_BUCKET_SILVER=${GCP_PROJECT_ID}-silver
GCS_BUCKET_GOLD=${GCP_PROJECT_ID}-gold
```

---

## Step 2: Start Local Services (Docker)

### Option A: Full Stack (Recommended)

```bash
cd deployment
docker-compose up -d
```

This starts:
- **Infrastructure**: Kafka + Zookeeper + Kafka UI, PostgreSQL, Redis, Weaviate, Neo4j
- **Orchestration**: Airflow (local DAG execution)
- **Observability**: Prometheus, Grafana, Tempo, LangFuse
- **Services**: Orchestrator, Data Agent, ServiceNow MCP, Jira MCP
- **Frontend**: Next.js UI

### Option B: Minimal Stack (Development)

```bash
cd deployment
docker-compose up -d kafka zookeeper postgres redis airflow
```

Then run services locally:

```bash
# Terminal 1: Backend Orchestrator
cd backend
pip install -r ../requirements.txt
uvicorn app:app --host 0.0.0.0 --port 8000 --reload

# Terminal 2: Data Agent
cd agents/data_agent
pip install -r requirements.txt
uvicorn src.api.main:app --host 0.0.0.0 --port 8001 --reload

# Terminal 3: Frontend
cd frontend
npm install
npm run dev
```

---

## Step 3: Verify Setup

### 3.1 Check Services

```bash
# Check Docker services
cd deployment
docker-compose ps

# Check service health
curl http://localhost:8000/health  # Orchestrator
curl http://localhost:8001/health  # Data Agent
curl http://localhost:8083/health  # Airflow
curl http://localhost:8085         # Kafka UI
```

### 3.2 Test Pipeline Generation

```bash
# Create a test pipeline via API
curl -X POST http://localhost:8001/pipelines \
  -H "Content-Type: application/json" \
  -d '{
    "pipeline_identity": {
      "pipeline_name": "test_pipeline",
      "project_name": "test-project",
      "team": "data-engineering"
    },
    "source_config": {
      "source_type": "postgres",
      "connection_id": "postgres-main",
      "schema_name": "public",
      "table_name": "users"
    },
    "target_config": {
      "target_type": "bigquery",
      "dataset": "bronze",
      "table": "users_raw"
    },
    "schema_definition": {
      "columns": [
        {"name": "id", "type": "INT64", "mode": "REQUIRED"},
        {"name": "email", "type": "STRING", "mode": "REQUIRED"},
        {"name": "created_at", "type": "TIMESTAMP", "mode": "NULLABLE"}
      ]
    },
    "execution_policy": {
      "schedule": "0 2 * * *",
      "environment": "dev"
    }
  }'

# Check status (replace <request_id> with actual ID)
curl http://localhost:8001/pipelines/<request_id>
```

### 3.3 View Generated DAGs in Airflow

1. Open http://localhost:8083
2. Login with `admin` / `admin123`
3. Check DAGs list for your generated pipeline

---

## Step 4: GCP Production Setup (Optional)

### 4.1 Create GCS Buckets

```bash
gsutil mb -l us-central1 gs://${GCP_PROJECT_ID}-raw
gsutil mb -l us-central1 gs://${GCP_PROJECT_ID}-bronze
gsutil mb -l us-central1 gs://${GCP_PROJECT_ID}-silver
gsutil mb -l us-central1 gs://${GCP_PROJECT_ID}-gold
```

### 4.2 Create Cloud Composer

```bash
gcloud composer environments create data-agent-composer \
  --location=us-central1 \
  --image-version=composer-2.9.7-airflow-2.9.3 \
  --environment-size=small
```

### 4.3 Set Up GitHub Actions for DAG Sync

The workflow at `.github/workflows/deploy-dags.yml` automatically syncs DAGs to Cloud Composer on merge to main. Add these secrets to your repo:

- `GCP_PROJECT_ID` - Your GCP project ID
- `GCP_SA_KEY` - Service account JSON key
- `COMPOSER_LOCATION` - e.g., `us-central1`

---

## Quick Reference

### Service URLs (Local)

| Service | URL | Credentials |
|---------|-----|-------------|
| Backend API | http://localhost:8000 | - |
| Data Agent | http://localhost:8001 | - |
| Airflow | http://localhost:8083 | admin / admin123 |
| Frontend | http://localhost:3002 | - |
| Kafka UI | http://localhost:8085 | - |
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | - |
| LangFuse | http://localhost:3001 | - |
| Neo4j Browser | http://localhost:7474 | neo4j / adminadmin |

### Common Commands

```bash
# Start all services
cd deployment && docker-compose up -d

# View logs
docker-compose logs -f orchestrator
docker-compose logs -f data_agent
docker-compose logs -f airflow

# Stop all services
docker-compose down

# Reset data (removes all volumes)
docker-compose down -v

# Rebuild after code changes
docker-compose up -d --build orchestrator data_agent

# Run tests
pytest tests/

# Validate DAGs locally
python -c "from airflow.models import DagBag; DagBag('/opt/airflow/dags')"
```

### Kafka Topics

| Topic | Producer | Consumer |
|-------|----------|----------|
| `incident.created` | ServiceNow MCP | Orchestrator |
| `incident.close_execute` | Orchestrator | ServiceNow MCP |
| `incident.approved` | FastAPI | Orchestrator |
| `pipeline.requested` | Jira MCP / API | Data Agent |
| `pipeline.completed` | Data Agent | Jira MCP |
| `pipeline.failed` | Data Agent | Jira MCP |

### Unified Dockerfile Services

The platform uses a single Dockerfile with the `SERVICE` environment variable:

| SERVICE Value | Description | Port |
|---------------|-------------|------|
| `orchestrator` | Backend API | 8000 |
| `data-agent` | Data Pipeline Agent | 8001 |
| `servicenow-mcp` | ServiceNow MCP Server | - |
| `jira-mcp` | Jira MCP Server | - |

---

## Troubleshooting

### Kafka Connection Issues

```bash
# Check Kafka health
docker-compose ps kafka
docker-compose logs kafka

# List topics
docker exec -it deployment-kafka-1 kafka-topics --list --bootstrap-server localhost:9092
```

### Airflow Issues

```bash
# Check Airflow logs
docker-compose logs airflow

# Restart Airflow
docker-compose restart airflow

# Access Airflow CLI
docker exec -it deployment-airflow-1 airflow dags list
```

### Database Issues

```bash
# Check PostgreSQL
docker-compose exec postgres psql -U admin -d agentdb -c "SELECT 1"

# Reset database
docker-compose down postgres
docker volume rm deployment_postgres_data
docker-compose up -d postgres
```

### Service Not Starting

```bash
# Check service logs
docker-compose logs <service_name>

# Rebuild specific service
docker-compose up -d --build <service_name>

# Check health endpoints
curl http://localhost:8000/health
curl http://localhost:8001/health
```

---

## Next Steps

1. **Test pipeline generation** via the API or Frontend
2. **Configure ServiceNow/Jira** MCP servers for event-driven workflows
3. **Customize templates** in `agents/data_agent/src/templates/`
4. **Set up GCP** for production deployment (optional)
5. **Configure monitoring alerts** in Grafana

For more details, see:
- [docs/ARCHITECTURE_V6_EVENT_DRIVEN.md](docs/ARCHITECTURE_V6_EVENT_DRIVEN.md)
- [docs/KAFKA_TOPICS.md](docs/KAFKA_TOPICS.md)
- [docs/PATTERNS.md](docs/PATTERNS.md)
