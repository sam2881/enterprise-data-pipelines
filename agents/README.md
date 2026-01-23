# Agents Module Reference

> **Last Updated**: 2026-01-19
> **Purpose**: Agent implementations for IT Service Management and Data Pipeline Generation

## Quick Navigation

| Folder | Purpose | Key Files |
|--------|---------|-----------|
| [shared/](#shared) | Common agent interfaces & base classes | `base.py`, `interfaces.py`, `types.py` |
| [servicenow_agent/](#servicenow_agent) | IT Service incident handling | `service.py`, `it_service/`, `remediation/` |
| [data_agent/](#data_agent) | Data pipeline generation | `src/agents/`, `src/graphs/`, `src/templates/` |

---

## Architecture Overview

```
agents/
├── __init__.py          # Main exports (BaseAgent, IAgent*, registry)
├── registry.py          # Agent registry management
├── shared/              # Common interfaces
├── servicenow_agent/    # IT Service Agent (ServiceNow incidents)
└── data_agent/          # Data Pipeline Agent (Jira stories)
```

---

## Folder Details

### shared/
Common agent interfaces, base classes, and types used by all agents.

| File | Purpose |
|------|---------|
| `__init__.py` | Exports shared components |
| `base.py` | `BaseAgent` - Abstract base class for all agents |
| `interfaces.py` | `IAgentService`, `IAgentTask`, `IAgentResult` interfaces |
| `types.py` | Common type definitions |
| `config.py` | Shared agent configuration |

**Key Classes**:
```python
from agents.shared import BaseAgent, IAgentService, IAgentTask, IAgentResult
```

---

### servicenow_agent/
IT Service Agent for handling ServiceNow incidents.

#### Structure
```
servicenow_agent/
├── __init__.py           # Exports ServiceNowAgentService
├── service.py            # Main service class
├── it_service/           # IT Service sub-agents
│   ├── __init__.py
│   ├── incident_agent.py  # Incident analysis agent
│   └── matcher_agent.py   # Script matching agent
└── remediation/          # Remediation sub-agents
    ├── __init__.py
    └── remediation_agent.py  # Remediation execution
```

| File | Purpose |
|------|---------|
| `service.py` | `ServiceNowAgentService` - Main entry point |
| `it_service/incident_agent.py` | `IncidentAgent` - Analyzes and classifies incidents |
| `it_service/matcher_agent.py` | `ScriptMatcherAgent` - Matches incidents to scripts |
| `remediation/remediation_agent.py` | `RemediationAgent` - Executes remediation |

**Usage**:
```python
from agents.servicenow_agent import ServiceNowAgentService

service = ServiceNowAgentService()
result = await service.triage_incident(incident_data)
```

---

### data_agent/
Data Pipeline Agent for generating data pipelines from Jira stories.

#### Structure
```
data_agent/
├── Dockerfile            # Container build
├── requirements.txt      # Dependencies
├── pyproject.toml        # Package config
├── ddl/                  # DDL templates
├── prompts/              # LLM prompts
├── deployment/           # Deployment configs
├── terraform/            # IaC templates
├── tests/                # Agent tests
└── src/                  # Main source code
    ├── __init__.py
    ├── agents/           # LangGraph agents
    ├── api/              # FastAPI endpoints
    ├── config/           # Settings
    ├── deployers/        # Deployment clients
    ├── generators/       # Code generators
    ├── graphs/           # LangGraph workflow
    ├── integrations/     # External integrations
    ├── metadata/         # Metadata handling
    ├── state/            # State management
    ├── templates/        # Jinja2 templates
    ├── utils/            # Utilities
    └── validators/       # Validation logic
```

#### src/agents/ - LangGraph Agents
| File | Purpose |
|------|---------|
| `base_agent.py` | Base agent class for data agents |
| `supervisor_agent.py` | Orchestrates other agents |
| `planner_agent.py` | Creates pipeline execution plan |
| `generator_agent.py` | Generates Spark/DAG/SQL code |
| `validator_agent.py` | Validates generated code |
| `deployer_agent.py` | Deploys to GCP Composer |

#### src/graphs/ - LangGraph Workflow
| File | Purpose |
|------|---------|
| `__init__.py` | Graph exports |
| `main_graph.py` | **CORE** - 5-node StateGraph workflow |
| `nodes.py` | Node implementations |
| `edges.py` | Conditional edge logic |

#### src/generators/ - Code Generators
| File | Purpose |
|------|---------|
| `dag_generator.py` | Airflow DAG generation |
| `spark_generator.py` | PySpark code generation |
| `metadata_generator.py` | Metadata file generation |

#### src/templates/ - Jinja2 Templates
| Folder | Purpose |
|--------|---------|
| `dag/` | Airflow DAG templates |
| `spark/` | PySpark job templates |
| `sql/` | SQL query templates |

#### src/validators/ - Validation
| File | Purpose |
|------|---------|
| `dag_validator.py` | DAG syntax validation |
| `sql_validator.py` | SQL validation |
| `schema_validator.py` | JSON schema validation |
| `security_validator.py` | Security checks |

#### src/deployers/ - Deployment
| File | Purpose |
|------|---------|
| `git_client.py` | GitHub operations (commit, PR) |
| `airflow_client.py` | Airflow REST API |
| `composer_client.py` | GCP Composer client |
| `cicd_trigger.py` | CI/CD trigger |

#### src/integrations/ - External Services
| File | Purpose |
|------|---------|
| `kafka_client.py` | Pipeline Kafka producer/consumer |
| `jira_client.py` | Jira API client |
| `pubsub_client.py` | GCP Pub/Sub client |
| `secret_manager.py` | GCP Secret Manager |

#### src/state/ - State Management
| File | Purpose |
|------|---------|
| `pipeline_state.py` | Pipeline state TypedDict |
| `agent_state.py` | Agent state management |
| `execution_context.py` | Execution context |

#### src/config/
| File | Purpose |
|------|---------|
| `settings.py` | Pydantic settings with GitHub config |

---

## Registry

The agent registry (`registry.py`) manages all available agents.

```python
from agents import get_agent_registry

registry = get_agent_registry()
agent = registry.get_agent("servicenow")
```

---

## Import Guidelines

```python
# Root imports (preferred)
from agents import BaseAgent, IAgentService, IAgentTask, IAgentResult
from agents import get_agent_registry

# ServiceNow Agent
from agents.servicenow_agent import ServiceNowAgentService
from agents.servicenow_agent.it_service import IncidentAgent, ScriptMatcherAgent
from agents.servicenow_agent.remediation import RemediationAgent

# Data Agent
from agents.data_agent.src.graphs import create_pipeline_graph
from agents.data_agent.src.agents import PlannerAgent, GeneratorAgent
from agents.data_agent.src.config.settings import get_settings
```

---

## Entry Points

1. **ServiceNow Agent Service**: Used by backend orchestrator
   ```python
   from agents.servicenow_agent import ServiceNowAgentService
   service = ServiceNowAgentService()
   ```

2. **Data Agent Graph**: LangGraph workflow
   ```python
   from agents.data_agent.src.graphs import create_pipeline_graph
   graph = create_pipeline_graph()
   result = await graph.ainvoke(initial_state)
   ```

3. **Data Agent API**: FastAPI server
   ```bash
   cd agents/data_agent
   python -m src.api.main
   ```

---

## Workflow Diagrams

### ServiceNow Agent Flow
```
ServiceNow Incident → IncidentAgent (analyze) → ScriptMatcherAgent (match) → RemediationAgent (execute)
```

### Data Agent Flow (LangGraph)
```
Jira Story → Supervisor → Planner → Generator → Validator → Deployer → GitHub PR
```

---

## Testing

```bash
# ServiceNow Agent tests
pytest tests/unit/test_agents.py -v

# Data Agent tests
cd agents/data_agent
pytest tests/ -v
```
