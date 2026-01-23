# AI Agent Platform - Compliance Matrix v5.0

## Overview

This document provides detailed compliance testing criteria for all supported regulatory frameworks.

| Standard | Total Checks | Description |
|----------|--------------|-------------|
| SOC 2 Type II | 7 | Security, Availability, Processing Integrity, Confidentiality |
| ISO 42001 | 8 | AI Management System (context, policy, risk, lifecycle, data) |
| NIST AI RMF | 8 | GOVERN, MAP, MEASURE, MANAGE functions |
| EU AI Act | 7 | Articles 9-15 for High-Risk AI systems |
| MITRE ATLAS | 7 | Adversarial Threat Landscape (injection, evasion, poisoning) |
| **Observability** | **8** | **Metrics, Tracing, Logging, Alerting (LMT Stack)** |

---

## 1. EU AI Act (Articles 9-15)

| Art. | Requirement | Why Test? | What We Search | Files Tested | Pass If |
|------|-------------|-----------|----------------|--------------|---------|
| 9 | Risk Management | Prevent AI harm | `circuit_breaker`, `try/except`, `risk_level`, `RiskLevel` | `circuit_breaker.py`, `llm_guardrails.py`, `eu_ai_act_compliance.py` | ≥2 controls exist |
| 10 | Data Governance | Protect data quality & privacy | `backend/rag/` dir, `pii.*detect`, `retention` | `backend/rag/`, `data_retention.py`, `llm_guardrails.py` | RAG + retention exist |
| 11 | Technical Docs | Anyone can understand system | File existence check | `ARCHITECTURE*.md`, `README.md`, `PROTOCOL_GUIDE.md` | ≥3 docs exist |
| 12 | Record-Keeping | Track all AI decisions | `audit_logger`, `log_event`, `AuditEvent` | `audit_logger.py`, `main.py` | Audit logger used |
| 13 | Transparency | AI explains decisions | `explanation`, `reasoning`, `root_cause` | All `.py` files | Found in ≥3 files |
| 14 | Human Oversight | Humans can override AI | `hitl`, `approval`, `human.*oversight`, `reject` | `main.py`, `control_plane.py` | HITL patterns found |
| 15 | Accuracy/Security | AI is reliable & secure | `confidence.*threshold`, `validate`, `injection` | `llm_guardrails.py`, `intelligent_retriever.py` | Validation exists |

---

## 2. SOC 2 Type II

| Control | Requirement | Why Test? | What We Search | Files Tested | Pass If |
|---------|-------------|-----------|----------------|--------------|---------|
| CC1.1 | Access Control | Restrict unauthorized access | `authorization`, `authenticate`, `token`, `api_key` | `main.py`, `audit_logger.py` | ≥2 auth patterns |
| CC2.1 | Risk Assessment | Identify & manage risks | `risk_level`, `RiskLevel`, `assess_risk` | `eu_ai_act_compliance.py`, `control_plane.py` | Risk classification exists |
| CC3.1 | Change Management | Control system changes | `.git` dir, `github_actions`, `SYSTEM_CONFIG_CHANGE` | `github_actions.py`, `audit_logger.py` | Git + audit logging |
| CC4.1 | Monitoring | Monitor system operations | `prometheus`, `structlog`, `langfuse`, `metrics` | `metrics.py`, `requirements.txt` | ≥3 monitoring tools |
| CC5.1 | Data Protection | Protect confidential data | `PII`, `detect_pii`, `anonymize`, `retention` | `llm_guardrails.py`, `data_retention.py` | PII detection + retention |
| A1.1 | Availability | Meet availability commitments | `/health`, `circuit_breaker`, `redis` | `main.py`, `circuit_breaker.py`, `redis_client.py` | Health + resilience |
| PI1.1 | Processing Integrity | Ensure accurate processing | `validate`, `confidence`, `LLM.*Judge` | `llm_guardrails.py`, `llm_judge.py` | Validation + quality check |

---

## 3. ISO 42001 (AI Management System)

| Clause | Requirement | Why Test? | What We Search | Files Tested | Pass If |
|--------|-------------|-----------|----------------|--------------|---------|
| 4.1 | Organization Context | Understand AI system context | `docs/` dir, `ARCHITECTURE` | `docs/ARCHITECTURE*.md`, `docs/README.md` | ≥1 architecture doc |
| 5.2 | AI Policy | Establish AI governance policy | `governance/` dir, `compliance`, `policy` | `backend/governance/`, `EU_AI_ACT_COMPLIANCE_GUIDE.md` | Governance module exists |
| 6.1 | Risk Management | Address AI-specific risks | `RiskLevel`, `risk_assessment`, `guardrails` | `eu_ai_act_compliance.py`, `llm_guardrails.py`, `control_plane.py` | ≥2 risk controls |
| 7.2 | Competence | Ensure personnel competence | `docs/` count, code documentation | `docs/*.md` | ≥3 documentation files |
| 8.2 | AI Lifecycle | Manage AI system lifecycle | `workflow`, `execution`, `feedback` | `langgraph_workflow.py`, `execution_orchestrator.py`, `feedback_optimizer.py` | Lifecycle components exist |
| 8.4 | Data Management | Manage AI training/inference data | `rag/` dir, `embedding`, `retention` | `backend/rag/`, `embedding_service.py`, `data_retention.py` | RAG + embedding + retention |
| 9.1 | Performance Eval | Monitor AI performance | `metrics`, `prometheus`, `langfuse` | `metrics.py`, `requirements.txt` | Metrics collection exists |
| 10.1 | Improvement | Continuously improve AI | `feedback`, `optimizer`, `llm_judge` | `feedback_optimizer.py`, `llm_judge.py` | Feedback loop exists |

---

## 4. NIST AI RMF

| Function | Requirement | Why Test? | What We Search | Files Tested | Pass If |
|----------|-------------|-----------|----------------|--------------|---------|
| GOVERN-1 | Policies | Establish AI risk policies | `governance/` dir, `policy`, `compliance` | `backend/governance/`, `control_plane.py` | Governance module exists |
| GOVERN-2 | Accountability | Establish accountability | `audit`, `user_id`, `actor`, `HUMAN_APPROVAL` | `audit_logger.py` | User attribution in logs |
| MAP-1 | Context | Establish AI system context | `query_understanding`, `ARCHITECTURE` | `query_understanding.py`, `docs/ARCHITECTURE*.md` | Context docs exist |
| MAP-2 | Risk Categories | Categorize AI risks | `RiskLevel`, `CRITICAL`, `HIGH`, `MEDIUM`, `LOW` | `eu_ai_act_compliance.py`, `control_plane.py` | Risk levels defined |
| MEASURE-1 | Metrics | Define AI risk metrics | `metrics`, `prometheus`, `confidence` | `metrics.py`, `intelligent_retriever.py` | Metrics + confidence scoring |
| MEASURE-2 | Testing | Test AI systems | `tests/` dir, `test_*.py` files | `tests/unit/`, `tests/security/`, `tests/chaos/` | ≥2 test directories |
| MANAGE-1 | Prioritization | Prioritize AI risks | `priority`, `threshold`, `auto_approve` | `control_plane.py` | Risk-based routing exists |
| MANAGE-2 | Response | Respond to AI risks | `circuit_breaker`, `guardrails`, `escalate` | `circuit_breaker.py`, `llm_guardrails.py`, `control_plane.py` | ≥2 response mechanisms |

---

## 5. MITRE ATLAS (Adversarial Threats)

| Technique | Requirement | Why Test? | What We Search | Files Tested | Pass If |
|-----------|-------------|-----------|----------------|--------------|---------|
| AML.T0051 | Prompt Injection | Prevent prompt manipulation | `prompt_injection`, `INJECTION_PATTERNS`, `jailbreak` | `llm_guardrails.py` | Injection detection exists |
| AML.T0015 | Model Evasion | Prevent adversarial inputs | `validate`, `sanitize`, `confidence.*threshold` | `llm_guardrails.py`, `intelligent_retriever.py` | Input validation exists |
| AML.T0020 | Data Poisoning | Prevent malicious data | `validate`, `quality`, `human.*review` | `smart_chunker.py`, `feedback_optimizer.py`, `control_plane.py` | Data validation exists |
| AML.T0024 | Model Theft | Prevent model extraction | `rate_limit`, `throttle`, `DATA_ACCESS` | `llm_guardrails.py`, `audit_logger.py` | Rate limiting + access logs |
| AML.T0010 | Supply Chain | Prevent supply chain attacks | `requirements.txt`, `>=`, `==` version pins | `requirements.txt` | Dependencies documented |
| AML.T0043 | Output Integrity | Verify output integrity | `validate_output`, `LLM.*Judge`, `checksum` | `llm_guardrails.py`, `llm_judge.py`, `audit_logger.py` | Output validation exists |
| AML.T0048 | Inference Manipulation | Prevent API manipulation | `sanitize`, `escape`, `cors`, `authentication` | `llm_guardrails.py`, `main.py` | API security exists |

---

## 6. Observability Stack (LMT - Logs, Metrics, Traces)

| Component | Requirement | Why Test? | What We Search | Files Tested | Pass If |
|-----------|-------------|-----------|----------------|--------------|---------|
| OBS-1 | Prometheus Metrics | Export metrics for monitoring | `prometheus_client`, `Counter`, `Histogram`, `Gauge` | `metrics.py`, `cost_tracker.py`, `llm_judge.py` | ≥60 metrics defined |
| OBS-2 | Grafana Dashboards | Visualize metrics | `monitoring/grafana/dashboards/*.json` | `ai_agent_dashboard.json` | Dashboard exists |
| OBS-3 | Alert Rules | Proactive alerting | `monitoring/alerts/*.yml` | `ai_agent_alerts.yml` | ≥20 alert rules |
| OBS-4 | Langfuse LLM Tracing | Track LLM calls | `langfuse`, `create_trace`, `create_span` | `llm_intelligence.py`, `base_agent.py` | Langfuse integrated |
| OBS-5 | OpenTelemetry | Distributed tracing | `opentelemetry`, `setup_tracing`, `traced` | `otel_tracing.py`, `requirements.txt` | OTEL configured |
| OBS-6 | Structured Logging | Consistent log format | `structlog`, `logger.info`, `logger.warning` | All `.py` files | structlog in ≥20 files |
| OBS-7 | Health Checks | Service health endpoints | `/health`, `status.*healthy` | `main.py`, all agent files | ≥5 health endpoints |
| OBS-8 | Audit Logging | Track all decisions | `AuditLogger`, `log_ai_decision`, `checksum` | `audit_logger.py` | EU AI Act compliant audit |

### Observability Files Summary

| Category | Files | Metrics/Endpoints |
|----------|-------|-------------------|
| **Prometheus Metrics** | `backend/orchestrator/metrics.py` | 60+ metrics (request, workflow, LLM, RAG, MCP) |
| | `backend/utils/cost_tracker.py` | Cost metrics (llm_cost_total, budget_remaining) |
| | `backend/orchestrator/llm_judge.py` | Judge metrics (evaluations, quality_score) |
| | `mcp-servers/shared/metrics.py` | MCP metrics (tool_calls, tool_latency) |
| **Grafana** | `monitoring/grafana/dashboards/ai_agent_dashboard.json` | 9 visualization panels |
| **Alerts** | `monitoring/alerts/ai_agent_alerts.yml` | 35+ alert rules across 8 categories |
| **Langfuse** | `backend/orchestrator/llm_intelligence.py` | LLM trace + span creation |
| | `backend/agents/base_agent.py` | Agent trace integration |
| **OpenTelemetry** | `backend/utils/otel_tracing.py` | Distributed tracing setup |
| **Audit Logs** | `backend/governance/audit_logger.py` | EU AI Act compliant auditing |

### Alert Categories

| Category | Alert Count | Examples |
|----------|-------------|----------|
| System Health | 8 | ServiceDown, CircuitBreakerOpen, HighErrorRate |
| LLM/AI Performance | 5 | LLMHighLatency, LLMHighErrorRate, LowConfidenceDecisions |
| RAG System | 5 | RAGHighLatency, WeaviateConnectionFailed, Neo4jConnectionFailed |
| Workflow/Incidents | 6 | HighPendingApprovals, RemediationFailureRate, WorkflowNodeStuck |
| Cost Management | 4 | DailyBudgetExceeded, BudgetDepleted, CostSpike |
| Security/Compliance | 5 | HighRiskActionWithoutApproval, GuardrailTriggered, PIIDetected |
| MCP Servers | 3 | MCPServerDown, MCPToolHighErrorRate |
| Integrations | 4 | ServiceNowAuthFailure, GitHubActionsHighFailureRate |

---

## Running Compliance Checks

```bash
# Run all compliance checks
python3 tests/compliance/compliance_checker.py

# Run specific framework
python3 tests/compliance/compliance_checker.py --standard EU-AI-Act
python3 tests/compliance/compliance_checker.py --standard SOC2
python3 tests/compliance/compliance_checker.py --standard ISO42001
python3 tests/compliance/compliance_checker.py --standard NIST-AI-RMF
python3 tests/compliance/compliance_checker.py --standard MITRE-ATLAS

# Verbose output with evidence
python3 tests/compliance/compliance_checker.py --verbose

# JSON output for CI/CD
python3 tests/compliance/compliance_checker.py --json --output reports/compliance.json
```

---

## Evidence Collection

The compliance checker automatically scans the codebase and collects evidence by:

1. **File Existence Checks** - Verifying required files/directories exist
2. **Pattern Matching** - Searching for specific code patterns using regex
3. **Import Verification** - Testing that modules can be imported
4. **Configuration Validation** - Checking environment variables and configs

Each check produces:
- **Status**: PASS, FAIL, PARTIAL, MANUAL_REVIEW
- **Evidence**: List of findings that support the status
- **Recommendations**: Suggested improvements if not fully compliant
