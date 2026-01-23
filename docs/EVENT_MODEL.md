# Event Model - v6.0 Event-Driven Architecture

## Overview

This document describes the event model for the Enterprise Agentic Platform. All events follow a consistent structure and are validated using Pydantic schemas.

---

## Event Base Structure

Every event in the platform includes these base fields:

```python
class EventBase(BaseModel):
    """Base event with common fields"""
    event_id: str          # UUID - unique identifier for this event
    event_type: str        # Event type (e.g., "incident.created")
    timestamp: str         # ISO 8601 timestamp
    correlation_id: str    # Links related events across the workflow
    source: str            # System that published the event
    version: str           # Schema version (e.g., "6.0")
```

### Example Base Event

```json
{
  "event_id": "550e8400-e29b-41d4-a716-446655440000",
  "event_type": "incident.created",
  "timestamp": "2024-01-15T10:30:00.000Z",
  "correlation_id": "corr-abc-123",
  "source": "servicenow-mcp",
  "version": "6.0"
}
```

---

## Idempotency Key Pattern

All events include an idempotency key for deduplication:

```python
idempotency_key: str = f"{incident_id}:{event_type}:{timestamp}"
```

Consumers use this key to detect and skip duplicate events:

```python
def process_event(event: dict):
    idempotency_key = event.get("idempotency_key")
    if redis.exists(f"processed:{idempotency_key}"):
        logger.info("Skipping duplicate event")
        return

    # Process event...

    # Mark as processed (TTL: 24 hours)
    redis.setex(f"processed:{idempotency_key}", 86400, "1")
```

---

## Event Categories

### 1. Lifecycle Events (State Transitions)

Published by LangGraph workflows when state changes:

| Event Type | Purpose |
|------------|---------|
| `incident.received` | Workflow started processing |
| `incident.enriched` | Classification complete |
| `incident.plan_generated` | Remediation plan ready |
| `incident.verified` | Fix verified |
| `incident.closed` | Workflow complete |

### 2. Control Events (Human Interactions)

Published by FastAPI when humans make decisions:

| Event Type | Purpose |
|------------|---------|
| `incident.approved` | Human approved plan |
| `incident.rejected` | Human rejected plan |
| `incident.close_requested` | Human requested closure |

### 3. Command Events (Actions)

Published to trigger actions by MCPs:

| Event Type | Consumer | Action |
|------------|----------|--------|
| `incident.close_execute` | ServiceNow MCP | Close ticket |
| `pipeline.deploy_execute` | Airflow MCP | Deploy pipeline |
| `mcp.github.command` | GitHub MCP | Trigger workflow |

---

## Complete Event Examples

### incident.created

Published by ServiceNow MCP when new incident detected.

```json
{
  "event_id": "550e8400-e29b-41d4-a716-446655440001",
  "event_type": "incident.created",
  "timestamp": "2024-01-15T10:30:00.000Z",
  "correlation_id": "corr-INC0001234",
  "source": "servicenow-mcp",
  "version": "6.0",

  "incident_id": "INC0001234",
  "short_description": "Database connection timeout on prod-db-01",
  "description": "Users reporting slow page loads. Database shows high connection count.",
  "priority": "2",
  "service": "database",
  "source_system": "servicenow",
  "raw_data": {
    "sys_id": "abc123",
    "number": "INC0001234",
    "state": "1",
    "urgency": "2",
    "impact": "2",
    "category": "software",
    "subcategory": "database",
    "assignment_group": "db-admins",
    "sys_created_on": "2024-01-15 10:29:45",
    "opened_by": "jsmith"
  }
}
```

### incident.enriched

Published by LangGraph after classification and RAG enrichment.

```json
{
  "event_id": "550e8400-e29b-41d4-a716-446655440002",
  "event_type": "incident.enriched",
  "timestamp": "2024-01-15T10:30:15.000Z",
  "correlation_id": "corr-INC0001234",
  "source": "langgraph-workflow",
  "version": "6.0",

  "incident_id": "INC0001234",
  "classification": "database",
  "severity": "2",
  "service": "prod-db-01",
  "rag_results_count": 5,
  "rag_confidence": 0.92,
  "parsed_context": {
    "description": "Database connection timeout on prod-db-01",
    "service": "database",
    "severity": "2",
    "error_message": "Connection pool exhausted",
    "affected_resources": ["prod-db-01", "prod-db-02"],
    "environment": "production"
  }
}
```

### incident.plan_generated

Published by LangGraph when remediation plan is created.

```json
{
  "event_id": "550e8400-e29b-41d4-a716-446655440003",
  "event_type": "incident.plan_generated",
  "timestamp": "2024-01-15T10:30:25.000Z",
  "correlation_id": "corr-INC0001234",
  "source": "langgraph-workflow",
  "version": "6.0",

  "incident_id": "INC0001234",
  "plan_id": "plan-001",
  "action_type": "shell",
  "script_id": "mysql-connection-reset",
  "script_path": "scripts/database/mysql_connection_reset.sh",
  "workflow_name": "shell-execute.yml",
  "steps_count": 3,
  "has_rollback": true,
  "confidence": 0.92,
  "plan": {
    "action_type": "shell",
    "script_id": "mysql-connection-reset",
    "script_path": "scripts/database/mysql_connection_reset.sh",
    "workflow_name": "shell-execute.yml",
    "steps": [
      {"step": 1, "action": "Validate target system accessibility", "timeout": 30},
      {"step": 2, "action": "Reset MySQL connection pool", "timeout": 120},
      {"step": 3, "action": "Verify fix applied successfully", "timeout": 60}
    ],
    "rollback_plan": {
      "steps": [{"step": 1, "action": "Restart MySQL service"}],
      "script_path": "scripts/database/mysql_restart.sh"
    },
    "affected_resources": ["prod-db-01"],
    "target_service": "prod-db-01",
    "environment": "production",
    "dry_run": true,
    "confidence": 0.92,
    "estimated_duration_seconds": 210,
    "risk_assessment": "medium"
  }
}
```

### incident.requires_approval

Published by LangGraph when human approval is required.

```json
{
  "event_id": "550e8400-e29b-41d4-a716-446655440004",
  "event_type": "incident.requires_approval",
  "timestamp": "2024-01-15T10:30:30.000Z",
  "correlation_id": "corr-INC0001234",
  "source": "langgraph-workflow",
  "version": "6.0",

  "incident_id": "INC0001234",
  "plan_id": "plan-001",
  "approval_route": "manual_approve",
  "approval_token": "550e8400-e29b-41d4-a716-446655440099",
  "risk_level": "medium",
  "script_id": "mysql-connection-reset",
  "script_path": "scripts/database/mysql_connection_reset.sh",
  "rollback_available": true,
  "approval_timeout_seconds": 3600,
  "callback_topic": "incident.approved",
  "judge_score": {
    "quality_score": 8.5,
    "safety_passed": true,
    "factual_score": 8.0,
    "feasibility_score": 9.0,
    "risk_level": "medium",
    "reasoning": "Plan uses well-tested script with rollback capability."
  }
}
```

### incident.approved

Published by FastAPI when human approves.

```json
{
  "event_id": "550e8400-e29b-41d4-a716-446655440005",
  "event_type": "incident.approved",
  "timestamp": "2024-01-15T10:35:00.000Z",
  "correlation_id": "corr-INC0001234",
  "source": "fastapi-control-plane",
  "version": "6.0",

  "incident_id": "INC0001234",
  "plan_id": "plan-001",
  "approval_route": "manual_approve",
  "approved_by": "admin@company.com",
  "approval_token": "550e8400-e29b-41d4-a716-446655440099",
  "conditions": [],
  "risk_level": "medium"
}
```

### remediation.started

Published by LangGraph when execution begins.

```json
{
  "event_id": "550e8400-e29b-41d4-a716-446655440006",
  "event_type": "remediation.started",
  "timestamp": "2024-01-15T10:35:05.000Z",
  "correlation_id": "corr-INC0001234",
  "source": "langgraph-workflow",
  "version": "6.0",

  "incident_id": "INC0001234",
  "plan_id": "plan-001",
  "script_id": "mysql-connection-reset",
  "github_workflow_id": "shell-execute.yml",
  "expected_duration_seconds": 210
}
```

### remediation.executed

Published by LangGraph after execution completes.

```json
{
  "event_id": "550e8400-e29b-41d4-a716-446655440007",
  "event_type": "remediation.executed",
  "timestamp": "2024-01-15T10:37:30.000Z",
  "correlation_id": "corr-INC0001234",
  "source": "langgraph-workflow",
  "version": "6.0",

  "incident_id": "INC0001234",
  "plan_id": "plan-001",
  "success": true,
  "execution_time_seconds": 145,
  "github_run_id": 12345678,
  "github_run_url": "https://github.com/org/repo/actions/runs/12345678",
  "output": {
    "status": "success",
    "conclusion": "success",
    "exit_code": 0,
    "output": "Connection pool reset successfully. Active connections: 45"
  }
}
```

### incident.close_execute

Command for ServiceNow MCP to close ticket.

```json
{
  "event_id": "550e8400-e29b-41d4-a716-446655440008",
  "event_type": "incident.close_execute",
  "timestamp": "2024-01-15T10:38:00.000Z",
  "correlation_id": "corr-INC0001234",
  "source": "langgraph-workflow",
  "version": "6.0",

  "incident_id": "INC0001234",
  "servicenow_sys_id": "abc123",
  "resolution_code": "Solved Remotely (Permanently)",
  "resolution_notes": "Automatically resolved by AI Agent Platform.\n\nScript: mysql-connection-reset\nGitHub Actions Run: 12345678\nExecution Duration: 145 seconds\nConfidence Score: 92.0%\nJudge Score: 8.5/10",
  "close_code": "Solved (Permanently)"
}
```

### incident.closed

Published by ServiceNow MCP after ticket closure.

```json
{
  "event_id": "550e8400-e29b-41d4-a716-446655440009",
  "event_type": "incident.closed",
  "timestamp": "2024-01-15T10:38:10.000Z",
  "correlation_id": "corr-INC0001234",
  "source": "servicenow-mcp",
  "version": "6.0",

  "incident_id": "INC0001234",
  "resolution_summary": "Automatically resolved by executing mysql-connection-reset",
  "total_duration_seconds": 490,
  "automation_level": "full",
  "script_used": "mysql-connection-reset",
  "feedback_recorded": true
}
```

---

## Pipeline Event Examples

### pipeline.requested

Published by Jira MCP when new pipeline request received.

```json
{
  "event_id": "660e8400-e29b-41d4-a716-446655440001",
  "event_type": "pipeline.requested",
  "timestamp": "2024-01-15T11:00:00.000Z",
  "correlation_id": "corr-DATA-1234",
  "source": "jira-mcp",
  "version": "6.0",

  "request_id": "req-001",
  "jira_key": "DATA-1234",
  "pipeline_identity": {
    "pipeline_name": "customer_transactions",
    "project_name": "data-platform",
    "team": "analytics"
  },
  "source_config": {
    "source_type": "postgres",
    "connection_id": "postgres-prod",
    "schema": "public",
    "table": "transactions"
  },
  "target_config": {
    "target_type": "bigquery",
    "dataset": "analytics",
    "table": "customer_transactions"
  },
  "schema_definition": {
    "columns": [
      {"name": "id", "type": "INT64", "mode": "REQUIRED"},
      {"name": "customer_id", "type": "STRING", "mode": "REQUIRED"},
      {"name": "amount", "type": "FLOAT64", "mode": "NULLABLE"},
      {"name": "created_at", "type": "TIMESTAMP", "mode": "REQUIRED"}
    ]
  },
  "execution_policy": {
    "schedule": "0 2 * * *",
    "retry_count": 3,
    "environment": "production"
  }
}
```

### pipeline.requires_approval

Published when PROD deployment needs approval.

```json
{
  "event_id": "660e8400-e29b-41d4-a716-446655440002",
  "event_type": "pipeline.requires_approval",
  "timestamp": "2024-01-15T11:05:00.000Z",
  "correlation_id": "corr-DATA-1234",
  "source": "data-agent",
  "version": "6.0",

  "request_id": "req-001",
  "jira_key": "DATA-1234",
  "environment": "production",
  "approval_reason": "prod_deployment",
  "risk_level": "medium",
  "artifacts_preview": {
    "dag_path": "dags/customer_transactions_dag.py",
    "spark_jobs": ["spark/customer_transactions_extract.py"],
    "schema_ddl": "CREATE TABLE IF NOT EXISTS..."
  },
  "approval_timeout_seconds": 86400
}
```

---

## Event Serialization

### Publishing Events

```python
from backend.streaming.kafka_producer import get_producer
from backend.streaming.schemas import Topics, IncidentCreatedEvent

async def publish_incident_created(incident_data: dict):
    producer = get_producer()

    event = IncidentCreatedEvent(
        incident_id=incident_data["number"],
        correlation_id=f"corr-{incident_data['number']}",
        short_description=incident_data["short_description"],
        description=incident_data["description"],
        priority=incident_data["priority"],
        service=incident_data["assignment_group"],
        raw_data=incident_data
    )

    await producer.publish_event(
        topic=Topics.INCIDENT_CREATED,
        event=event.model_dump(),
        key=incident_data["number"]
    )
```

### Consuming Events

```python
from backend.streaming.schemas import event_from_json

def process_message(message):
    event_type = message.value.get("event_type")
    event = event_from_json(json.dumps(message.value), event_type)

    if event_type == "incident.created":
        handle_incident_created(event)
    elif event_type == "incident.approved":
        handle_incident_approved(event)
```

---

## See Also

- [KAFKA_TOPICS.md](KAFKA_TOPICS.md) - Complete topic reference
- [WORKFLOW_FLOWS.md](WORKFLOW_FLOWS.md) - Flow diagrams
- [RESPONSIBILITY_MATRIX.md](RESPONSIBILITY_MATRIX.md) - Who publishes what
