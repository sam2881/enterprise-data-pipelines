# AI Agent Platform - Production Architecture v2.0

## Overview

This document describes the production-ready architecture of the AI Agent Platform, a comprehensive Agentic AI Enterprise Data Engineering & Migration Platform.

## Architecture Principles

### 1. Separation of Concerns

The platform is organized into distinct layers:

```
┌─────────────────────────────────────────────────────────────────┐
│                         Frontend (UI)                            │
│                     Next.js / React Dashboard                    │
└─────────────────────────────────────────────────────────────────┘
                                │
┌─────────────────────────────────────────────────────────────────┐
│                      API Gateway Layer                           │
│                   FastAPI REST Endpoints                         │
│                   /api/v2/workflows                              │
│                   /api/v2/incidents                              │
│                   /api/v2/pipelines                              │
└─────────────────────────────────────────────────────────────────┘
                                │
┌─────────────────────────────────────────────────────────────────┐
│                     Control Plane (MCP)                          │
│    ┌─────────────────────────────────────────────────────────┐  │
│    │              ControlPlaneOrchestrator                    │  │
│    │  - Workflow state management                             │  │
│    │  - Approval routing                                      │  │
│    │  - Policy enforcement                                    │  │
│    │  - Audit logging                                         │  │
│    └─────────────────────────────────────────────────────────┘  │
│                              │                                   │
│    ┌───────────────┐  ┌───────────────┐  ┌───────────────┐      │
│    │ PolicyEngine  │  │  Handlers     │  │ EventPublisher│      │
│    └───────────────┘  └───────────────┘  └───────────────┘      │
└─────────────────────────────────────────────────────────────────┘
                                │
┌─────────────────────────────────────────────────────────────────┐
│                      Agent Layer                                 │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │ITService    │  │DataPipeline │  │Infrastructure           │  │
│  │Handler      │  │Handler      │  │Handler                  │  │
│  │             │  │             │  │                         │  │
│  │- ServiceNow │  │- Jira       │  │- GCP VM Management      │  │
│  │- Incidents  │  │- Pipelines  │  │- Dataproc              │  │
│  │- Remediation│  │- Spark/DAG  │  │                         │  │
│  └─────────────┘  └─────────────┘  └─────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                                │
┌─────────────────────────────────────────────────────────────────┐
│                   Infrastructure Layer                           │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐              │
│  │DataprocClient│ │GCS Client   │  │BigQuery     │              │
│  │(Spark Jobs)  │ │(Storage)    │  │(Warehouse)  │              │
│  └─────────────┘  └─────────────┘  └─────────────┘              │
│                                                                  │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐              │
│  │Kafka        │  │Redis        │  │PostgreSQL   │              │
│  │(Events)     │  │(Cache)      │  │(Metadata)   │              │
│  └─────────────┘  └─────────────┘  └─────────────┘              │
└─────────────────────────────────────────────────────────────────┘
```

### 2. Clear Ownership Boundaries

| Component | Responsibility | Location |
|-----------|----------------|----------|
| **Control Plane** | Workflow orchestration, approvals | `/backend/control_plane/` |
| **IT Service Handler** | ServiceNow incidents | `/backend/control_plane/handlers/it_service_handler.py` |
| **Data Pipeline Handler** | Jira pipelines | `/backend/control_plane/handlers/data_pipeline_handler.py` |
| **Infrastructure** | GCP clients (Dataproc, GCS, BigQuery) | `/backend/infrastructure/` |
| **Streaming** | Kafka event publishing | `/backend/streaming/` |
| **Config** | Unified settings | `/backend/config/` |
| **Secrets** | GCP Secret Manager | `/backend/secrets/` |

### 3. Explicit Interfaces

Each handler implements the `WorkflowHandler` interface:

```python
class WorkflowHandler(ABC):
    @abstractmethod
    async def analyze(self, context: WorkflowContext) -> Dict[str, Any]:
        """Analyze the incoming request."""
        pass

    @abstractmethod
    async def generate_plan(self, context: WorkflowContext) -> Dict[str, Any]:
        """Generate execution plan."""
        pass

    @abstractmethod
    async def execute(self, context: WorkflowContext) -> Dict[str, Any]:
        """Execute the approved plan."""
        pass

    @abstractmethod
    async def verify(self, context: WorkflowContext) -> Dict[str, Any]:
        """Verify execution results."""
        pass

    @abstractmethod
    async def rollback(self, context: WorkflowContext) -> None:
        """Rollback on failure (compensation)."""
        pass
```

## Workflow Patterns

### IT Service Workflow (ServiceNow)

```
ServiceNow Incident → Kafka → Control Plane → ITServiceHandler
                                    │
                        ┌───────────┴───────────┐
                        │                       │
                    Analyze                 Search RAG
                        │                       │
                        └───────────┬───────────┘
                                    │
                            Generate Plan
                                    │
                            Policy Evaluation
                                    │
                    ┌───────────────┼───────────────┐
                    │               │               │
                AUTO_APPROVE       HITL          SENIOR
                    │               │               │
                    └───────────────┼───────────────┘
                                    │
                                Execute
                                    │
                            GitHub Actions
                                    │
                                Verify
                                    │
                        Update ServiceNow
```

### Data Pipeline Workflow (Jira)

```
Jira Ticket → Kafka → Control Plane → DataPipelineHandler
                            │
                ┌───────────┴───────────┐
                │                       │
        Parse Requirements       Analyze Source
                │                   (GCS)
                └───────────┬───────────┘
                            │
                    Generate Code
                        │
            ┌───────────┼───────────┐
            │           │           │
        PySpark       DAG         DQ Rules
            │           │           │
            └───────────┼───────────┘
                        │
                Policy Evaluation
                        │
                    Approval
                        │
                    Deploy
                        │
            ┌───────────┼───────────┐
            │           │           │
        GCS         Git/CI      Airflow
        (code)      (PR)        (DAG)
            │           │           │
            └───────────┼───────────┘
                        │
                Dataproc Execution
                        │
                    Verify
                        │
                Update Jira
```

## Configuration Management

### Hierarchy

1. **Default Values**: Defined in `config/settings.py`
2. **Environment Variables**: Override defaults
3. **Secrets**: Loaded from GCP Secret Manager at runtime

### Environment-Specific Configuration

```python
# config/settings.py
class Settings(BaseSettings):
    environment: Environment = Environment.LOCAL

    kafka: KafkaConfig = KafkaConfig()
    redis: RedisConfig = RedisConfig()
    dataproc: DataprocConfig = DataprocConfig()
    # ...
```

### Secret Categories

| Category | Secrets |
|----------|---------|
| **LLM** | `OPENAI_API_KEY`, `ANTHROPIC_API_KEY` |
| **ServiceNow** | `SNOW_INSTANCE_URL`, `SNOW_USERNAME`, `SNOW_PASSWORD` |
| **Jira** | `JIRA_URL`, `JIRA_USERNAME`, `JIRA_API_TOKEN` |
| **GitHub** | `GITHUB_TOKEN`, `GITHUB_ORG`, `GITHUB_REPO` |
| **Database** | `POSTGRES_PASSWORD`, `NEO4J_PASSWORD` |
| **Slack** | `SLACK_BOT_TOKEN`, `SLACK_CHANNEL` |

## GCP Integration

### Dataproc (Spark)

Configuration in `config/settings.py`:

```python
class DataprocConfig(BaseModel):
    project_id: str = "agent-ai-test-461120"
    region: str = "us-central1"
    cluster_name: str = "ai-agent-spark"

    # Minimal sizing for cost optimization
    master_machine_type: str = "n1-standard-2"
    worker_machine_type: str = "n1-standard-2"
    num_workers: int = 2

    # Auto-delete after 30 min idle
    idle_delete_ttl: str = "1800s"
```

Usage:

```python
from infrastructure import get_dataproc_client

client = get_dataproc_client()

# Submit a Spark job
job_id = await client.submit_pyspark_job(
    script_uri="gs://bucket/scripts/transform.py",
    args=["--input", "gs://bronze/data", "--output", "gs://silver/data"]
)

# Wait for completion
result = await client.wait_for_job(job_id)
```

### GCS (Medallion Architecture)

```
gs://project-raw-data/     # Landing zone
gs://project-bronze/       # Raw data (as-is)
gs://project-silver/       # Cleaned/transformed
gs://project-gold/         # Aggregated/analytics
gs://project-temp/         # Temporary files (7-day lifecycle)
```

### BigQuery

Dataset: `data_warehouse`

Tables:
- `pipeline_runs` - Pipeline execution history
- `data_quality_results` - DQ check results

## Kafka Event Flow

### Topics

| Topic | Purpose | Publishers | Consumers |
|-------|---------|------------|-----------|
| `workflow.events` | Workflow lifecycle | Control Plane | Dashboard, Audit |
| `workflow.approvals` | Pending approvals | Control Plane | HITL Service |
| `incident.created` | New incidents | ServiceNow | IT Service Agent |
| `incident.resolved` | Resolved incidents | IT Service Agent | Dashboard |
| `pipeline.requested` | New pipelines | Jira | Data Agent |
| `pipeline.completed` | Completed pipelines | Data Agent | Dashboard |

### Event Schema

```json
{
  "event_id": "uuid",
  "event_type": "workflow.started",
  "event_category": "workflow",
  "timestamp": "2024-01-01T00:00:00Z",
  "source": "ai-agent-platform",
  "version": "1.0",
  "correlation_id": "workflow-123",
  "data": {
    "workflow_id": "it_service-abc123",
    "source_id": "INC001"
  }
}
```

## API Endpoints

### Workflow API (v2)

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/v2/workflows` | Start new workflow |
| GET | `/api/v2/workflows/{id}` | Get workflow status |
| GET | `/api/v2/workflows/approvals/pending` | List pending approvals |
| POST | `/api/v2/workflows/{id}/approve` | Approve workflow |
| POST | `/api/v2/workflows/{id}/reject` | Reject workflow |

### Incident API

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/v2/incidents` | Create incident workflow |
| POST | `/api/v2/webhooks/servicenow` | ServiceNow webhook |

### Pipeline API

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/v2/pipelines` | Create pipeline workflow |
| POST | `/api/v2/webhooks/jira` | Jira webhook |

### Dataproc API

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v2/dataproc/cluster/status` | Get cluster status |
| POST | `/api/v2/dataproc/jobs/submit` | Submit Spark job |
| GET | `/api/v2/dataproc/jobs/{id}` | Get job status |

## Deployment

### Local Development

```bash
# Start infrastructure
cd infrastructure
docker compose up -d postgres redis kafka zookeeper

# Run backend
cd backend
python app.py
```

### Production (Docker)

```bash
# Start all services
cd infrastructure
./start-all.sh

# Or with Dataproc
./scripts/setup-dataproc.sh create
```

### GCP Setup

```bash
# 1. Setup GCP resources
./scripts/setup-gcp.sh

# 2. Setup secrets
./scripts/setup-secrets.sh

# 3. Create Dataproc cluster (on-demand)
./scripts/setup-dataproc.sh create
```

## Monitoring & Observability

- **Metrics**: Prometheus + Grafana (port 3001)
- **Tracing**: OpenTelemetry + Tempo
- **LLM Observability**: Langfuse
- **Logs**: Structured JSON logging

## Security

### Secrets Management

All secrets stored in GCP Secret Manager:

```python
from secrets import get_secret

api_key = get_secret("OPENAI_API_KEY")
```

### Policy Enforcement

```python
# Risk-based approval routing
if risk_level == RiskLevel.CRITICAL:
    return ApprovalRoute.SENIOR
elif risk_level == RiskLevel.HIGH:
    return ApprovalRoute.HITL
elif confidence >= 0.75 and risk_level == RiskLevel.LOW:
    return ApprovalRoute.AUTO_APPROVE
```

### Audit Logging

All workflow actions logged with:
- Event type
- User/system actor
- Timestamp
- AI decision explanation
- Human oversight applied

## Cost Optimization

1. **Dataproc**: On-demand clusters with auto-delete (30 min idle)
2. **BigQuery**: On-demand pricing (pay per query)
3. **GCS**: Standard storage class
4. **Preemptible VMs**: Available for Dataproc workers (60-80% savings)

## Next Steps

1. [ ] Add Infrastructure Handler for GCP VM management
2. [ ] Implement RAG integration for script matching
3. [ ] Add real ServiceNow/Jira client integration
4. [ ] Set up CI/CD for DAG deployment
5. [ ] Add comprehensive test coverage
