# Kafka Topics Reference - v6.0 Event-Driven Architecture

## Overview

Kafka is the **system of record** for the Enterprise Agentic Platform. All state transitions flow through Kafka topics. This document provides a comprehensive reference for all topics and their Pydantic schemas.

---

## Architecture Principle

```
MCPs sense → Kafka remembers → Orchestrator routes → LangGraph reasons & acts → FastAPI governs
```

---

## Topic Naming Convention

```
{domain}.{event_type}
```

Examples:
- `incident.created` - Incident domain, created event
- `pipeline.approved` - Pipeline domain, approved event
- `mcp.servicenow.commands` - MCP commands for ServiceNow

---

## Incident Lifecycle Topics

| Topic | Publisher | Consumer | Description |
|-------|-----------|----------|-------------|
| `incident.created` | ServiceNow MCP | EventOrchestrator | New incident detected |
| `incident.received` | LangGraph | State consumers | LangGraph started processing |
| `incident.enriched` | LangGraph | State consumers | Classification complete |
| `incident.plan_generated` | LangGraph | State consumers | Remediation plan ready |
| `incident.requires_approval` | LangGraph | UI/Slack | Human approval required |
| `incident.approved` | FastAPI | EventOrchestrator | Human approved |
| `incident.rejected` | FastAPI | EventOrchestrator | Human rejected |
| `incident.executed` | LangGraph | State consumers | Execution complete |
| `incident.verified` | LangGraph | State consumers | Fix verified |
| `incident.close_execute` | LangGraph | ServiceNow MCP | Command to close ticket |
| `incident.closed` | ServiceNow MCP | State consumers | Ticket closed |
| `incident.failed` | LangGraph | Alerting | Workflow failed |

### Incident Event Schemas

```python
from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional
from datetime import datetime
import uuid

class IncidentCreatedEvent(BaseModel):
    """Published by ServiceNow MCP when new incident detected"""
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str = "incident.created"
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    correlation_id: str
    incident_id: str
    short_description: str
    description: str
    priority: str
    service: str
    source_system: str = "servicenow"
    raw_data: Dict[str, Any] = Field(default_factory=dict)

class IncidentEnrichedEvent(BaseModel):
    """Published by LangGraph after classification and RAG enrichment"""
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str = "incident.enriched"
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    correlation_id: str
    incident_id: str
    classification: str
    severity: str
    service: str
    rag_results_count: int
    rag_confidence: float
    parsed_context: Dict[str, Any]

class IncidentRequiresApprovalEvent(BaseModel):
    """Published by LangGraph when human approval is required"""
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str = "incident.requires_approval"
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    correlation_id: str
    incident_id: str
    plan_id: str
    approval_route: str  # "auto", "async", "manual"
    approval_token: str  # Unique token for this approval request
    risk_level: str
    script_id: str
    script_path: str
    judge_score: Optional[Dict[str, Any]] = None
    approval_timeout_seconds: int = 3600

class IncidentApprovedEvent(BaseModel):
    """Published by FastAPI when human approves"""
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str = "incident.approved"
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    correlation_id: str
    incident_id: str
    approval_token: str
    approved_by: str
    conditions: List[str] = Field(default_factory=list)

class IncidentCloseExecuteEvent(BaseModel):
    """Command for ServiceNow MCP to close ticket"""
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str = "incident.close_execute"
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    correlation_id: str
    incident_id: str
    servicenow_sys_id: str
    resolution_code: str
    resolution_notes: str
    close_code: str = "Resolved"
```

---

## Pipeline Lifecycle Topics

| Topic | Publisher | Consumer | Description |
|-------|-----------|----------|-------------|
| `pipeline.requested` | Jira MCP / FastAPI | EventOrchestrator | New pipeline request |
| `pipeline.planned` | Data Agent | State consumers | Planning complete |
| `pipeline.generated` | Data Agent | State consumers | Code generated |
| `pipeline.validated` | Data Agent | State consumers | Validation passed |
| `pipeline.requires_approval` | Data Agent | UI | PROD approval required |
| `pipeline.approved` | FastAPI | EventOrchestrator | Human approved |
| `pipeline.rejected` | FastAPI | EventOrchestrator | Human rejected |
| `pipeline.deploy_execute` | Data Agent | Airflow MCP | Command to deploy |
| `pipeline.deployed` | Airflow MCP | State consumers | Deployment complete |
| `pipeline.failed` | Data Agent | Alerting | Any stage failed |

### Pipeline Event Schemas

```python
class PipelineRequestedEvent(BaseModel):
    """Published by Jira MCP when pipeline request received"""
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str = "pipeline.requested"
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    correlation_id: str
    request_id: str
    jira_key: str
    pipeline_identity: Dict[str, Any]
    source_config: Dict[str, Any]
    target_config: Dict[str, Any]
    schema_definition: Dict[str, Any]
    execution_policy: Dict[str, Any] = Field(default_factory=dict)

class PipelineRequiresApprovalEvent(BaseModel):
    """Published when PROD deployment needs approval"""
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str = "pipeline.requires_approval"
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    correlation_id: str
    request_id: str
    jira_key: str
    environment: str
    approval_reason: str  # "prod_deployment", "schema_change"
    risk_level: str
    artifacts_preview: Dict[str, Any] = Field(default_factory=dict)
    approval_timeout_seconds: int = 86400

class PipelineDeployExecuteEvent(BaseModel):
    """Command for Airflow MCP to deploy"""
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str = "pipeline.deploy_execute"
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    correlation_id: str
    request_id: str
    jira_key: str
    dag_path: str
    spark_job_paths: Dict[str, str] = Field(default_factory=dict)
    environment: str
    pr_merged: bool = False
```

---

## Remediation Topics

| Topic | Publisher | Consumer | Description |
|-------|-----------|----------|-------------|
| `remediation.started` | LangGraph | Monitoring | Execution began |
| `remediation.executed` | LangGraph | State consumers | Execution complete |
| `remediation.failed` | LangGraph | Alerting | Execution failed |
| `remediation.rollback` | LangGraph | Alerting | Rollback triggered |

---

## MCP Command Topics

| Topic | Publisher | Consumer | Description |
|-------|-----------|----------|-------------|
| `mcp.servicenow.commands` | FastAPI/Orchestrator | ServiceNow MCP | Close/update tickets |
| `mcp.github.commands` | FastAPI/Orchestrator | GitHub MCP | Trigger workflows |
| `mcp.airflow.commands` | FastAPI/Orchestrator | Airflow MCP | Trigger DAGs |

---

## External Integration Topics

| Topic | Publisher | Consumer | Description |
|-------|-----------|----------|-------------|
| `servicenow.incidents` | ServiceNow MCP | EventOrchestrator | Raw ServiceNow events |
| `jira.tickets` | Jira MCP | EventOrchestrator | Raw Jira events |
| `gcp.alerts` | GCP Pub/Sub | EventOrchestrator | GCP monitoring alerts |

---

## Topic Configuration

### Partitioning Strategy

- **Incident topics**: Partition by `incident_id` for ordering
- **Pipeline topics**: Partition by `request_id` for ordering
- **Command topics**: Partition by `target_id` for ordering

### Retention Policy

| Topic Type | Retention | Compaction |
|------------|-----------|------------|
| Lifecycle events | 7 days | No |
| Command events | 1 day | No |
| Audit events | 30 days | No |

### Consumer Groups

| Consumer Group | Topics | Purpose |
|----------------|--------|---------|
| `event-orchestrator` | All lifecycle | Route to workflows |
| `state-projector` | All lifecycle | Update Redis/Postgres |
| `audit-logger` | All | Compliance logging |
| `servicenow-mcp` | `incident.close_execute` | Close tickets |
| `airflow-mcp` | `pipeline.deploy_execute` | Deploy pipelines |

---

## Creating Topics (Manual)

```bash
# Create incident topics
kafka-topics.sh --create --topic incident.created --partitions 6 --replication-factor 3
kafka-topics.sh --create --topic incident.enriched --partitions 6 --replication-factor 3
kafka-topics.sh --create --topic incident.requires_approval --partitions 6 --replication-factor 3
kafka-topics.sh --create --topic incident.approved --partitions 6 --replication-factor 3
kafka-topics.sh --create --topic incident.close_execute --partitions 6 --replication-factor 3
kafka-topics.sh --create --topic incident.closed --partitions 6 --replication-factor 3

# Create pipeline topics
kafka-topics.sh --create --topic pipeline.requested --partitions 6 --replication-factor 3
kafka-topics.sh --create --topic pipeline.requires_approval --partitions 6 --replication-factor 3
kafka-topics.sh --create --topic pipeline.approved --partitions 6 --replication-factor 3
kafka-topics.sh --create --topic pipeline.deploy_execute --partitions 6 --replication-factor 3
kafka-topics.sh --create --topic pipeline.deployed --partitions 6 --replication-factor 3
```

---

## See Also

- [ARCHITECTURE_V6_EVENT_DRIVEN.md](ARCHITECTURE_V6_EVENT_DRIVEN.md) - Full architecture
- [EVENT_MODEL.md](EVENT_MODEL.md) - Event schema details
- [WORKFLOW_FLOWS.md](WORKFLOW_FLOWS.md) - Flow diagrams
