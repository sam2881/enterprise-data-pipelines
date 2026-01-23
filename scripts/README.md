# Scripts Module Reference

> **Last Updated**: 2026-01-19
> **Purpose**: Utility scripts for setup, testing, and operations

## Quick Navigation

| Category | Scripts |
|----------|---------|
| [Setup](#setup-scripts) | `setup.sh`, `setup-pre-commit.sh` |
| [System Management](#system-management) | `start_system.sh`, `stop_system.sh`, `health_check.sh` |
| [Testing](#testing-scripts) | `e2e_validator.py`, `test_e2e.sh`, `verify_system.sh` |
| [Data Population](#data-population) | `populate_rag_data.py`, `seed_historical_incidents.py` |
| [GitHub/Git](#github-scripts) | `push_to_github.sh`, `push_to_enterprise_repo.sh` |
| [Compliance](#compliance-scripts) | `compliance_scanner.py`, `run_compliance_check.sh` |
| [Infrastructure](#infrastructure-scripts) | `start_gcp_instance.sh`, `stop_gcp_instance.sh` |

---

## Script Details

### Setup Scripts

| File | Purpose | Usage |
|------|---------|-------|
| `setup.sh` | Initial project setup | `./scripts/setup.sh` |
| `setup-pre-commit.sh` | Install pre-commit hooks | `./scripts/setup-pre-commit.sh` |

---

### System Management

| File | Purpose | Usage |
|------|---------|-------|
| `start_system.sh` | Start all services (Docker Compose) | `./scripts/start_system.sh` |
| `stop_system.sh` | Stop all services | `./scripts/stop_system.sh` |
| `health_check.sh` | Check health of all services | `./scripts/health_check.sh` |
| `verify_system.sh` | Verify system is working | `./scripts/verify_system.sh` |

---

### Testing Scripts

| File | Purpose | Usage |
|------|---------|-------|
| `e2e_validator.py` | **MAIN** - End-to-end validation | `python scripts/e2e_validator.py --all` |
| `test_e2e.sh` | Shell wrapper for E2E tests | `./scripts/test_e2e.sh` |
| `verify_system.sh` | System verification | `./scripts/verify_system.sh` |
| `run_full_demo.sh` | Full demo scenario | `./scripts/run_full_demo.sh` |

**E2E Validator Options**:
```bash
# Full validation
python scripts/e2e_validator.py --all

# Health checks only
python scripts/e2e_validator.py --health

# Specific components
python scripts/e2e_validator.py --kafka
python scripts/e2e_validator.py --rag
python scripts/e2e_validator.py --workflow
```

---

### Data Population

| File | Purpose | Usage |
|------|---------|-------|
| `populate_rag_data.py` | Index scripts into RAG | `python scripts/populate_rag_data.py` |
| `populate_all_data.py` | Populate all data stores | `python scripts/populate_all_data.py` |
| `seed_historical_incidents.py` | Seed test incidents | `python scripts/seed_historical_incidents.py` |
| `sync_servicenow_incidents.py` | Sync from ServiceNow | `python scripts/sync_servicenow_incidents.py` |
| `view_incidents.py` | View incidents | `python scripts/view_incidents.py` |

---

### GitHub Scripts

| File | Purpose | Usage |
|------|---------|-------|
| `push_to_github.sh` | Push changes to GitHub | `./scripts/push_to_github.sh` |
| `push_to_enterprise_repo.sh` | Push to enterprise-data-pipelines | `./scripts/push_to_enterprise_repo.sh` |
| `create_github_pr.py` | Create GitHub PR programmatically | `python scripts/create_github_pr.py` |

---

### Compliance Scripts

| File | Purpose | Usage |
|------|---------|-------|
| `compliance_scanner.py` | Scan for compliance issues | `python scripts/compliance_scanner.py` |
| `run_compliance_check.sh` | Run compliance checks | `./scripts/run_compliance_check.sh` |

---

### Infrastructure Scripts

| File | Purpose | Usage |
|------|---------|-------|
| `start_gcp_instance.sh` | Start GCP VM instance | `./scripts/start_gcp_instance.sh <instance>` |
| `stop_gcp_instance.sh` | Stop GCP VM instance | `./scripts/stop_gcp_instance.sh <instance>` |
| `clear_disk_space.sh` | Clear disk space | `./scripts/clear_disk_space.sh` |

---

### Workflow Scripts

| File | Purpose | Usage |
|------|---------|-------|
| `agentic_workflow.py` | Agentic workflow testing | `python scripts/agentic_workflow.py` |

---

## Common Usage Patterns

### Initial Setup
```bash
# 1. Run setup
./scripts/setup.sh

# 2. Install pre-commit hooks
./scripts/setup-pre-commit.sh

# 3. Start services
./scripts/start_system.sh

# 4. Populate RAG data
python scripts/populate_rag_data.py

# 5. Verify system
./scripts/verify_system.sh
```

### Daily Development
```bash
# Start services
./scripts/start_system.sh

# Run tests
python scripts/e2e_validator.py --health

# Stop services
./scripts/stop_system.sh
```

### Before Commit
```bash
# Run compliance check
./scripts/run_compliance_check.sh

# Run E2E tests
python scripts/e2e_validator.py --all
```

---

## Environment Requirements

Most scripts require:
- Python 3.10+
- Docker and Docker Compose
- Environment variables from `.env`

```bash
# Load environment
source .env

# Run script
python scripts/e2e_validator.py --all
```
