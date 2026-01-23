# MCP Servers Module Reference

> **Last Updated**: 2026-01-19
> **Purpose**: Model Context Protocol (MCP) servers for external system integrations

## Overview

MCP servers provide standardized interfaces to external systems. They are called by the LangGraph workflow via Kafka events.

**Architecture**:
```
LangGraph Workflow → Kafka Event → MCP Server → External System → Kafka Response
```

---

## Quick Navigation

| Server | Purpose | External System |
|--------|---------|-----------------|
| [servicenow-mcp](#servicenow-mcp) | Incident polling & updates | ServiceNow |
| [jira-mcp](#jira-mcp) | Story management | Jira |
| [github-mcp](#github-mcp) | Workflow dispatch & file commits | GitHub |
| [gcp-mcp](#gcp-mcp) | GCP operations | GCP (Compute, Logging) |
| [airflow-mcp](#airflow-mcp) | DAG management | Airflow/Composer |
| [shared/](#shared) | Common utilities | N/A |

---

## Server Details

### servicenow-mcp/
ServiceNow integration - polls incidents and publishes updates.

| File | Purpose |
|------|---------|
| `server.py` | **EVENT-DRIVEN** - Polls ServiceNow, publishes to Kafka |
| `requirements.txt` | Python dependencies |

**Kafka Topics**:
- Publishes: `incident.created`, `incident.updated`
- Consumes: `incident.close_execute`

**Tools Provided**:
- `list_incidents` - Get open incidents
- `get_incident` - Get incident details
- `update_incident` - Update incident status
- `close_incident` - Close with resolution

---

### jira-mcp/
Jira integration - manages stories for data pipeline requests.

| File | Purpose |
|------|---------|
| `server.py` | **EVENT-DRIVEN** - Polls Jira, publishes story events |
| `requirements.txt` | Python dependencies |

**Kafka Topics**:
- Publishes: `jira.story.created`, `jira.story.updated`
- Consumes: `jira.comment.add`

**Tools Provided**:
- `list_stories` - Get project stories
- `get_story` - Get story details
- `create_story` - Create new story
- `update_story` - Update story
- `add_comment` - Add comment to story
- `transition_story` - Change story status

---

### github-mcp/
GitHub integration - workflow dispatch and file management.

| File | Purpose |
|------|---------|
| `server.py` | GitHub Actions & file operations |
| `requirements.txt` | Python dependencies |

**Environment Variables**:
```bash
# For remediation workflows (test_01 repo)
GITHUB_REMEDIATION_TOKEN=ghp_xxx
GITHUB_REMEDIATION_OWNER=sam2881
GITHUB_REMEDIATION_REPO=test_01

# For pipeline artifacts (enterprise-data-pipelines repo)
GITHUB_PIPELINES_TOKEN=ghp_xxx
GITHUB_PIPELINES_OWNER=sam2881
GITHUB_PIPELINES_REPO=enterprise-data-pipelines
```

**Kafka Topics**:
- Consumes: `github.trigger_workflow`, `github.commit_file`
- Publishes: `github.workflow_completed`, `github.workflow_failed`

**Tools Provided**:
- `trigger_workflow` - Dispatch GitHub Actions workflow
- `get_workflow_run` - Get workflow run status
- `list_workflow_runs` - List recent workflow runs
- `commit_file` - Commit file to repository
- `create_branch` - Create new branch
- `create_pull_request` - Create PR

---

### gcp-mcp/
GCP integration - compute, logging, and monitoring.

| File | Purpose |
|------|---------|
| `server.py` | GCP operations |
| `requirements.txt` | Python dependencies |

**Tools Provided**:
- `list_vms` - List Compute Engine VMs
- `get_vm_status` - Get VM status
- `start_vm` - Start VM instance
- `stop_vm` - Stop VM instance
- `restart_vm` - Restart VM instance
- `get_logs` - Get Cloud Logging logs
- `get_metrics` - Get Cloud Monitoring metrics

---

### airflow-mcp/
Airflow/Composer integration - DAG management.

| File | Purpose |
|------|---------|
| `__init__.py` | Package init |
| `server.py` | Airflow operations via REST API |

**Tools Provided**:
- `list_dags` - List available DAGs
- `get_dag` - Get DAG details
- `trigger_dag` - Trigger DAG run
- `get_dag_runs` - Get DAG run history
- `pause_dag` - Pause DAG
- `unpause_dag` - Unpause DAG
- `get_task_logs` - Get task logs
- `clear_task` - Clear task for retry

---

### shared/
Common utilities for all MCP servers.

| File | Purpose |
|------|---------|
| `__init__.py` | Shared exports |
| `metrics.py` | Prometheus metrics for MCP servers |

---

## Event-Driven Architecture

All MCP servers follow the event-driven pattern:

```
┌─────────────────┐     ┌───────────┐     ┌─────────────────┐
│  LangGraph      │────>│   Kafka   │────>│  MCP Server     │
│  Workflow       │<────│   Topics  │<────│  (ServiceNow)   │
└─────────────────┘     └───────────┘     └─────────────────┘
                                                   │
                                                   v
                                          ┌─────────────────┐
                                          │  External API   │
                                          │  (ServiceNow)   │
                                          └─────────────────┘
```

---

## Running MCP Servers

### Individual Server
```bash
# ServiceNow MCP
python mcp-servers/servicenow-mcp/server.py

# Jira MCP
python mcp-servers/jira-mcp/server.py

# GitHub MCP
python mcp-servers/github-mcp/server.py

# GCP MCP
python mcp-servers/gcp-mcp/server.py

# Airflow MCP
python mcp-servers/airflow-mcp/server.py
```

### Docker Compose
All MCP servers can be started via docker-compose:
```bash
docker-compose up servicenow-mcp jira-mcp github-mcp gcp-mcp airflow-mcp
```

---

## Configuration

Each server reads configuration from environment variables. See `.env.example` for full list.

**Common Variables**:
```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
LOG_LEVEL=INFO
```

**Per-Server Variables**:
```bash
# ServiceNow
SNOW_INSTANCE_URL=https://dev12345.service-now.com
SNOW_USERNAME=admin
SNOW_PASSWORD=xxx

# Jira
JIRA_URL=https://company.atlassian.net
JIRA_EMAIL=user@company.com
JIRA_API_TOKEN=xxx

# GitHub (see github-mcp section above)

# GCP
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
GCP_PROJECT_ID=my-project

# Airflow
AIRFLOW_URL=https://composer-url/api/v1
```

---

## Cleanup Log

| Date | Action | Reason |
|------|--------|--------|
| 2026-01-19 | Removed `jira-mcp/server.py` (old) | Replaced by event_driven_server.py |
| 2026-01-19 | Removed `servicenow-mcp/server.py` (old) | Replaced by event_driven_server.py |
| 2026-01-19 | Renamed `event_driven_server.py` → `server.py` | Standardization |
