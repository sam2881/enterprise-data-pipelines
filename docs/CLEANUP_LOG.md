# Codebase Cleanup Log

## Cleanup Session: 2026-01-16

### Objectives
1. Remove empty folders
2. Remove duplicate code
3. Remove unused/dead code
4. Ensure single source of truth for shared components
5. Clean up deprecated files
6. Remove sensitive files from repository

### Architecture Reference
```
agents/                      # ALL agent logic (SINGLE LOCATION)
├── shared/                  # Base classes, interfaces
├── data_agent/             # Data Engineering Agent
└── servicenow_agent/       # ServiceNow Platform Agent

platform_services/           # Shared platform capabilities (SINGLE SOURCE)
├── infrastructure_clients/  # Kafka, Redis, Postgres, Dataproc
├── protocols/a2a/          # A2A protocol
├── runbooks/               # Scripts + registry
├── metadata/               # Shared models
└── utils/                  # Common utilities

backend/                    # Runtime only (NO agent logic)
├── infrastructure/         # RE-EXPORTS from platform_services
├── utils/                  # RE-EXPORTS + unique utils
└── ...                     # APIs, orchestrator, RAG, etc.
```

---

## Scan Progress

### 1. agents/ folder
- [x] agents/__init__.py - Clean
- [x] agents/registry.py - Clean
- [x] agents/shared/ - Clean (base.py, interfaces.py, config.py, types.py)
- [x] agents/servicenow_agent/ - Clean (service.py, it_service/, remediation/)
- [x] agents/data_agent/ - Clean (109 files, well-structured)

**Notes:**
- data_agent has its own utils (logging.py, hashing.py, config.py) - these are specific to data_agent
- data_agent has its own a2a/ implementation (HTTP-based for Google A2A protocol) - different from platform_services WebSocket A2A
- data_agent has mcp_servers/ - these are development/local servers, separate from backend/mcp/

### 2. platform_services/ folder
- [x] platform_services/__init__.py - Clean
- [x] platform_services/infrastructure_clients/ - Clean (5 client files)
- [x] platform_services/protocols/ - Clean (a2a/ with mesh, client, messages)
- [x] platform_services/runbooks/ - Clean (registry.json + scripts)
- [x] platform_services/metadata/ - Clean (IncidentMetadata, PipelineMetadata, ScriptMetadata)
- [x] platform_services/utils/ - Clean (get_logger, compute_hash, get_env, get_secret)

### 3. backend/ folder
- [x] backend/app.py - Clean
- [x] backend/agents/ - Only __init__.py (deprecated, re-exports from agents/)
- [x] backend/config/ - Clean (settings.py, thresholds.py)
- [x] backend/control_plane/ - Clean
- [x] backend/governance/ - Clean
- [x] backend/guardrails/ - Clean
- [x] backend/infrastructure/ - Only __init__.py (re-exports from platform_services)
- [x] backend/mcp/ - Clean (client.py, servers/)
- [x] backend/observability/ - Clean
- [x] backend/orchestrator/ - Clean (main.py, llm_judge.py, langgraph_workflow.py, etc.)
- [x] backend/rag/ - Clean (18 files for RAG pipeline)
- [x] backend/secrets/ - Clean
- [x] backend/services/ - Clean (agent_service.py)
- [x] backend/streaming/ - Clean (consumers/ subfolder, event_publisher.py)
- [x] backend/utils/ - Clean (5 unique utils, re-exports infrastructure from platform_services)

**Notes:**
- backend/utils/__init__.py now re-exports from platform_services.infrastructure_clients
- Removed duplicate infrastructure clients (kafka_client.py, redis_client.py, postgres_client.py, circuit_breaker.py)
- Removed duplicate streaming consumers (kept in consumers/ subfolder)

### 4. Other folders
- [x] frontend/ - Clean (66 TS/TSX files)
- [x] mcp-servers/ - Clean (4 standalone servers + shared/)
- [x] tests/ - Clean (34 test files)
- [x] scripts/ - Clean (20 utility scripts)
- [x] infrastructure/ - Cleaned (removed .env and gcp-key.json)
- [x] monitoring/ - Clean
- [x] deployment/ - Cleaned (removed .env)
- [x] docs/ - Clean

---

## Actions Taken

### Security Fixes (CRITICAL)
1. **REMOVED**: `infrastructure/gcp-key.json` - Contained actual GCP service account private key
2. **REMOVED**: `infrastructure/.env` - Contained actual configuration values
3. **REMOVED**: `deployment/.env` - Contained actual configuration values

### Cache/Temporary Files Removed
- 37 `__pycache__` directories
- 2 `.pytest_cache` directories
- All `.pyc` compiled Python files
- All `.DS_Store` files
- All log files in `./logs/`

### Empty Directories Removed
- `deployment/k8s`
- `agents/servicenow_agent/infrastructure`
- `agents/data_agent/src/a2a/agent_cards`
- `backend/config/environments`
- `tests/dspy_evals`
- `frontend/src/components/data-pipelines`
- `logs/mcp`
- `deployment/monitoring/grafana/dashboards`
- `agents/data_agent/pipelines/common/operators`
- `backend/data/embeddings_cache`
- `agents/data_agent/demo/samples/08_ssis_data_vault/input`

### Duplicate Files Removed

**From `backend/utils/`** (now re-exports from `platform_services`):
- `kafka_client.py`
- `redis_client.py`
- `postgres_client.py`
- `circuit_breaker.py`

**From `backend/streaming/`** (kept in `consumers/` subfolder):
- `incident_consumer.py`
- `jira_consumer.py`
- `jira_data_consumer.py`
- `data_pipeline_consumer.py`

### Files Updated
- `backend/utils/__init__.py` - Now re-exports from platform_services with deprecation warning

---

## File Count Summary

| Folder | Python Files | Other Files | Notes |
|--------|-------------|-------------|-------|
| agents/ | 109 | - | Includes data_agent + servicenow_agent |
| platform_services/ | 15 | 14 | Infrastructure + protocols + runbooks |
| backend/ | 80 | - | Runtime, APIs, RAG |
| frontend/ | 66 (TS/TSX) | - | Next.js UI |
| mcp-servers/ | 6 | 4 | Standalone MCP servers |
| tests/ | 34 | 4 | Unit, integration, e2e tests |
| scripts/ | 6 | 14 | Utility scripts |

---

## Recommendations

1. **Rotate GCP Service Account Key** - The key was exposed in git history
2. ~~**Use GCP Secret Manager**~~ - **DONE** (see Security Enhancements below)
3. ~~**Add pre-commit hooks**~~ - **DONE** (see Security Enhancements below)
4. ~~**Consider consolidating MCP servers**~~ - **DONE** (see below)

---

## Security Enhancements (Completed)

### Pre-commit Hooks Added
Created `.pre-commit-config.yaml` with:
- **detect-secrets** - Scans for high-entropy strings, API keys
- **gitleaks** - Scans for known secret patterns
- **custom hook** - Blocks sensitive file patterns (*.pem, *.key, *-key.json, etc.)
- **black** - Python code formatting
- **isort** - Python import sorting
- **flake8** - Python linting
- **mypy** - Python type checking
- **eslint** - TypeScript linting
- **markdownlint** - Markdown linting

### Files Created
| File | Purpose |
|------|---------|
| `.pre-commit-config.yaml` | Pre-commit hook configuration |
| `.secrets.baseline` | detect-secrets baseline for false positives |
| `scripts/setup-pre-commit.sh` | Automated setup script |
| `docs/SECRETS_MANAGEMENT.md` | Complete secrets management guide |

### .gitignore Enhanced
Added comprehensive security patterns:
```
# GCP Service Account Keys
gcp-service-account-key.json
gcp-key.json
*-key.json
service-account*.json
credentials*.json

# Private Keys
*.pem
*.key
*.p12
*.pfx

# Environment files
.env
.env.local
.env.*.local

# Secret files
secrets.yaml
**/secrets/
!backend/secrets/__init__.py
!backend/secrets/manager.py
```

### GCP Secret Manager Integration
Existing `backend/secrets/manager.py` provides:
- Singleton SecretManager with 1-hour TTL caching
- Environment-based secret prefixes (dev-, staging-, prod-)
- Fallback to environment variables for local development
- Helper function `get_secret(name)` for easy access

### Setup Instructions
```bash
# Install and configure pre-commit hooks
./scripts/setup-pre-commit.sh

# Hooks run automatically on every commit
# To skip (emergency only): git commit --no-verify
```

---

## MCP Server Consolidation (Completed)

### Action Taken
- **REMOVED**: `agents/data_agent/src/mcp_servers/` (duplicate)
- **KEPT**: `backend/mcp/servers/` as SINGLE SOURCE OF TRUTH

### MCP Server Architecture
```
backend/mcp/servers/           # SINGLE SOURCE for all MCP servers
├── __init__.py               # Exports all server factories
├── rag_server.py             # RAG search (port 8010)
├── gcs_server.py             # GCS operations (port 8011)
├── iceberg_server.py         # Iceberg catalog (port 8012)
├── llm_server.py             # LLM operations (port 8013)
└── start_all.py              # Launcher for all servers
```

### Usage
All agents use the shared MCP servers:
- ServiceNow Agent: RAG, GCS (logs), LLM
- Data Agent: GCS (data lake), Iceberg, LLM

---

## Clean Architecture Verified

```
ai_agent_app/
├── agents/                    # ALL agent logic
│   ├── shared/               # Base classes
│   ├── data_agent/           # Data Agent (no mcp_servers/)
│   └── servicenow_agent/     # ServiceNow Agent
├── platform_services/         # SINGLE SOURCE for shared capabilities
│   ├── infrastructure_clients/
│   ├── protocols/a2a/
│   ├── runbooks/
│   ├── metadata/
│   └── utils/
├── backend/                   # Runtime (NO agent logic)
│   ├── agents/               # DEPRECATED - re-exports only
│   ├── infrastructure/       # Re-exports from platform_services
│   ├── mcp/servers/          # SINGLE SOURCE for MCP servers
│   └── ...
├── frontend/                  # UI
├── mcp-servers/              # Standalone external MCP servers
├── tests/                    # Test suites
└── scripts/                  # Utilities
```
