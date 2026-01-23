# Platform Services Module Reference

> **Last Updated**: 2026-01-19
> **Purpose**: Shared platform infrastructure, protocols, and runbooks

## Overview

Platform services provide the foundational infrastructure that is shared across all agents and backend services.

---

## Quick Navigation

| Folder | Purpose | Key Files |
|--------|---------|-----------|
| [infrastructure_clients/](#infrastructure_clients) | Database/messaging clients | `redis_client.py`, `kafka_client.py` |
| [protocols/](#protocols) | Communication protocols | `a2a/` (Agent-to-Agent) |
| [runbooks/](#runbooks) | Remediation scripts | Ansible, Terraform, Shell |
| [metadata/](#metadata) | Metadata utilities | `__init__.py` |
| [utils/](#utils) | Shared utilities | `__init__.py` |

---

## Folder Details

### infrastructure_clients/
Clients for external infrastructure services.

| File | Purpose |
|------|---------|
| `__init__.py` | Exports all clients |
| `redis_client.py` | Redis connection and caching |
| `kafka_client.py` | Generic Kafka producer/consumer |
| `postgres_client.py` | PostgreSQL connection |
| `dataproc_client.py` | GCP Dataproc job management |
| `circuit_breaker.py` | Circuit breaker pattern implementation |

**Usage**:
```python
from platform_services.infrastructure_clients import (
    redis_client,
    kafka_client,
    postgres_client,
    get_dataproc_client,
    CircuitBreaker,
)

# Redis
redis_client.set("key", "value")
value = redis_client.get("key")

# Kafka
kafka_client.publish_event("topic", {"data": "value"})

# PostgreSQL
with postgres_client.get_connection() as conn:
    conn.execute(query)

# Dataproc
client = get_dataproc_client()
job_result = await client.submit_pyspark_job(...)

# Circuit Breaker
breaker = CircuitBreaker(name="external_api", failure_threshold=3)
if breaker.can_execute():
    result = call_external_api()
    breaker.record_success()
```

---

### protocols/
Communication protocol implementations.

#### protocols/a2a/
Agent-to-Agent (A2A) protocol for inter-agent communication.

| File | Purpose |
|------|---------|
| `__init__.py` | Exports A2A components |
| `client.py` | A2A client for sending messages |
| `mesh.py` | Agent mesh discovery and routing |
| `messages.py` | Message types and serialization |

**Usage**:
```python
from platform_services.protocols.a2a import A2AClient, AgentMessage

client = A2AClient()
await client.send_message(
    target_agent="remediation-agent",
    message=AgentMessage(type="execute", payload={...})
)
```

---

### runbooks/
Remediation scripts and playbooks organized by type.

```
runbooks/
├── __init__.py        # Registry loader
├── registry.json      # Script metadata registry
├── ansible/           # Ansible playbooks
├── kubernetes/        # K8s manifests
├── pipelines/         # Pipeline YAML
├── scripts/           # Shell scripts
└── terraform/         # Terraform configs
```

#### runbooks/ansible/
Ansible playbooks for common remediation tasks.

| File | Purpose |
|------|---------|
| `fix_database_cpu.yml` | Fix high CPU on database |
| `fix_memory_leak.yml` | Fix memory leak |
| `fix_nginx_502.yml` | Fix Nginx 502 errors |
| `restart_airflow_scheduler.yml` | Restart Airflow scheduler |
| `restart_kubernetes_pod.yml` | Restart K8s pod |

#### runbooks/kubernetes/
Kubernetes manifests.

| File | Purpose |
|------|---------|
| `restart_deployment.yaml` | Deployment restart |
| `scale_deployment.yaml` | Scale deployment |

#### runbooks/pipelines/
Pipeline definitions.

| File | Purpose |
|------|---------|
| `restart_airflow_dag.yaml` | DAG restart config |

#### runbooks/scripts/
Shell scripts for remediation.

| File | Purpose |
|------|---------|
| `check_and_restart_service.sh` | Service health check and restart |
| `clear_disk_space.sh` | Disk cleanup |
| `start_gcp_instance.sh` | Start GCP VM instance |

#### runbooks/terraform/
Terraform configurations.

| File | Purpose |
|------|---------|
| `scale_gcp_instance.tf` | Scale GCP instance |

---

### metadata/
Metadata handling utilities.

| File | Purpose |
|------|---------|
| `__init__.py` | Metadata utilities and types |

---

### utils/
Shared utility functions.

| File | Purpose |
|------|---------|
| `__init__.py` | Common utilities (logging, timing, etc.) |

---

## Import Guidelines

```python
# Preferred: Direct imports from platform_services
from platform_services.infrastructure_clients import redis_client, kafka_client
from platform_services.protocols.a2a import A2AClient

# Backward compatible: Import via backend
from backend.infrastructure import redis_client  # Re-exports from here
```

---

## Registry Schema

The `runbooks/registry.json` contains metadata for all scripts:

```json
{
  "scripts": [
    {
      "id": "fix-nginx-502",
      "name": "Fix Nginx 502 Error",
      "description": "Restarts Nginx when 502 errors detected",
      "path": "ansible/fix_nginx_502.yml",
      "type": "ansible",
      "service": "nginx",
      "action": "restart",
      "risk_level": "low",
      "auto_approve": true,
      "keywords": ["nginx", "502", "gateway"],
      "error_patterns": ["502 Bad Gateway"],
      "required_inputs": ["target_host"]
    }
  ]
}
```

---

## Testing

```bash
# Test infrastructure clients
pytest tests/unit/test_utils.py -v -k "redis or kafka"

# Test circuit breaker
pytest tests/unit/test_utils.py -v -k "circuit"
```
