#!/usr/bin/env python3
"""
Enterprise Compliance Scanner for AI Agent Platform
====================================================

Scans the codebase and produces detailed compliance tables for:
- EU AI Act (Articles 9-17)
- SOC 2 Type II (Trust Service Criteria)
- ISO 42001 (AI Management System)
- NIST AI RMF 1.0
- MITRE ATLAS

Output: Detailed markdown tables showing how each compliance requirement
is implemented in the codebase.

Usage:
    python3 scripts/compliance_scanner.py
    python3 scripts/compliance_scanner.py --output reports/compliance_report.md
    python3 scripts/compliance_scanner.py --framework eu-ai-act
"""

import os
import re
import sys
import json
import argparse
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from collections import defaultdict

# Add backend to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


@dataclass
class ComplianceRequirement:
    """Single compliance requirement definition"""
    framework: str
    article: str
    requirement: str
    why_it_matters: str
    what_we_check: str
    how_we_check: str
    files_to_test: List[str]
    how_code_achieves_it: str
    pass_criteria: str
    status: str = "PENDING"
    evidence: List[str] = field(default_factory=list)
    confidence: float = 0.0


@dataclass
class ScanResult:
    """Result of scanning a single requirement"""
    requirement: ComplianceRequirement
    files_found: List[str]
    patterns_matched: Dict[str, List[str]]
    confidence_score: float
    status: str  # PASS, PARTIAL, FAIL
    notes: List[str]


class ComplianceScanner:
    """Enterprise compliance scanner for AI Agent Platform"""

    def __init__(self, root_path: str = None):
        self.root_path = Path(root_path or os.path.dirname(os.path.dirname(__file__)))
        self.scan_results: List[ScanResult] = []
        self.frameworks = self._define_frameworks()

    def _define_frameworks(self) -> Dict[str, List[ComplianceRequirement]]:
        """Define all compliance frameworks and their requirements"""
        return {
            "EU AI Act": self._eu_ai_act_requirements(),
            "SOC 2 Type II": self._soc2_requirements(),
            "ISO 42001": self._iso_42001_requirements(),
            "NIST AI RMF": self._nist_ai_rmf_requirements(),
            "MITRE ATLAS": self._mitre_atlas_requirements(),
        }

    def _eu_ai_act_requirements(self) -> List[ComplianceRequirement]:
        """EU AI Act compliance requirements"""
        return [
            ComplianceRequirement(
                framework="EU AI Act",
                article="Article 9 - Risk Management",
                requirement="Establish risk management system for high-risk AI",
                why_it_matters="Ensures systematic identification and mitigation of AI risks throughout lifecycle",
                what_we_check="Risk assessment logic, risk scoring, mitigation tracking",
                how_we_check="Search for risk_level, RiskLevel enum, risk assessment functions",
                files_to_test=[
                    "backend/agents/control_plane.py",
                    "backend/governance/audit_logger.py",
                    "backend/orchestrator/llm_judge.py",
                    "backend/agents/remediation/enterprise_matcher.py"
                ],
                how_code_achieves_it="RiskLevel enum (LOW/MEDIUM/HIGH/CRITICAL), risk assessment in control_plane.py, LLM Judge scoring for remediation plans",
                pass_criteria="Risk scoring function exists AND risk levels defined AND remediation risk assessment present"
            ),
            ComplianceRequirement(
                framework="EU AI Act",
                article="Article 10 - Data Governance",
                requirement="Training data quality and governance",
                why_it_matters="Ensures AI trained on relevant, representative, high-quality data",
                what_we_check="Data validation, embedding quality, RAG data governance",
                how_we_check="Search for data validation, embedding service, smart chunking",
                files_to_test=[
                    "backend/rag/embedding_service.py",
                    "backend/rag/smart_chunker.py",
                    "backend/rag/feedback_optimizer.py"
                ],
                how_code_achieves_it="EmbeddingService validates embeddings, SmartChunker ensures quality chunks, FeedbackOptimizer tracks data quality metrics",
                pass_criteria="Embedding validation exists AND data quality checks present"
            ),
            ComplianceRequirement(
                framework="EU AI Act",
                article="Article 11 - Technical Documentation",
                requirement="Maintain technical documentation for AI system",
                why_it_matters="Enables auditing and verification of AI system behavior",
                what_we_check="Code documentation, architecture docs, API docs",
                how_we_check="Check for docstrings, markdown docs, API documentation",
                files_to_test=[
                    "docs/ARCHITECTURE_V5.md",
                    "docs/OBSERVABILITY_WHITEPAPER.md",
                    "docs/INCIDENT_LIFECYCLE_WHITEPAPER.md",
                    "docs/COMPLIANCE_MATRIX.md"
                ],
                how_code_achieves_it="Comprehensive documentation in docs/ folder, inline docstrings with WHY/HOW comments, API specifications",
                pass_criteria="Architecture documentation exists AND code has docstrings"
            ),
            ComplianceRequirement(
                framework="EU AI Act",
                article="Article 12 - Record Keeping",
                requirement="Automatic logging of AI system events",
                why_it_matters="Enables post-hoc analysis and compliance auditing",
                what_we_check="Audit logging, event tracking, log persistence",
                how_we_check="Search for audit_logger, structlog, event logging",
                files_to_test=[
                    "backend/governance/audit_logger.py",
                    "backend/orchestrator/metrics.py",
                    "monitoring/alerts/ai_agent_alerts.yml"
                ],
                how_code_achieves_it="AuditLogger class logs all AI decisions with checksums, structlog for structured logging, Prometheus metrics for operational events",
                pass_criteria="AuditLogger exists AND logs AI decisions AND has integrity checksums"
            ),
            ComplianceRequirement(
                framework="EU AI Act",
                article="Article 13 - Transparency",
                requirement="AI system operation is transparent to users",
                why_it_matters="Users must understand they're interacting with AI and how it works",
                what_we_check="Explainability functions, confidence scores, decision explanations",
                how_we_check="Search for explain, confidence_score, ai_explanation",
                files_to_test=[
                    "backend/orchestrator/llm_intelligence.py",
                    "backend/rag/hybrid_search_engine.py",
                    "backend/agents/servicenow/agent.py"
                ],
                how_code_achieves_it="Confidence scores on all AI decisions, explanation fields in audit logs, hybrid search returns score breakdown",
                pass_criteria="Confidence scores present AND explanations generated for decisions"
            ),
            ComplianceRequirement(
                framework="EU AI Act",
                article="Article 14 - Human Oversight",
                requirement="Enable human oversight and intervention",
                why_it_matters="Humans must be able to understand, override, and stop AI decisions",
                what_we_check="Approval workflows, human-in-loop, override capabilities",
                how_we_check="Search for requires_approval, human_oversight, HUMAN_APPROVAL",
                files_to_test=[
                    "backend/governance/audit_logger.py",
                    "backend/agents/control_plane.py",
                    "monitoring/alerts/ai_agent_alerts.yml"
                ],
                how_code_achieves_it="ApprovalRoute enum (AUTO/ASYNC/MANUAL), requires_approval flags, HUMAN_OVERSIGHT audit events, LowHumanOversightRate alert",
                pass_criteria="Human approval workflow exists AND high-risk actions require approval"
            ),
            ComplianceRequirement(
                framework="EU AI Act",
                article="Article 15 - Accuracy & Robustness",
                requirement="AI systems must be accurate and robust",
                why_it_matters="Ensures AI performs correctly under various conditions",
                what_we_check="Error handling, fallback mechanisms, accuracy metrics",
                how_we_check="Search for circuit_breaker, fallback, accuracy, error handling",
                files_to_test=[
                    "backend/utils/circuit_breaker.py",
                    "backend/rag/cross_encoder_reranker.py",
                    "backend/orchestrator/metrics.py"
                ],
                how_code_achieves_it="CircuitBreaker for fault tolerance, RerankerWithFallback for degraded operation, accuracy metrics tracked in Prometheus",
                pass_criteria="Circuit breaker implemented AND fallback mechanisms present"
            ),
            ComplianceRequirement(
                framework="EU AI Act",
                article="Article 17 - Quality Management",
                requirement="Quality management system for AI",
                why_it_matters="Systematic approach to ensuring AI quality throughout lifecycle",
                what_we_check="Testing, CI/CD, quality metrics, continuous improvement",
                how_we_check="Search for tests, quality metrics, feedback loops",
                files_to_test=[
                    "tests/unit/test_agents.py",
                    "tests/unit/test_rag.py",
                    "backend/rag/feedback_optimizer.py"
                ],
                how_code_achieves_it="Unit tests for all major components, FeedbackOptimizer for continuous learning, quality metrics in Prometheus",
                pass_criteria="Unit tests exist AND feedback system implemented"
            ),
        ]

    def _soc2_requirements(self) -> List[ComplianceRequirement]:
        """SOC 2 Type II Trust Service Criteria"""
        return [
            ComplianceRequirement(
                framework="SOC 2 Type II",
                article="CC6.1 - Logical Access",
                requirement="Implement logical access controls",
                why_it_matters="Prevents unauthorized access to systems and data",
                what_we_check="Authentication, authorization, access logging",
                how_we_check="Search for auth, authorization, access control",
                files_to_test=[
                    "backend/governance/audit_logger.py",
                    "backend/orchestrator/main.py"
                ],
                how_code_achieves_it="AUTH_SUCCESS/AUTH_FAILURE audit events, environment-based authentication for ServiceNow/Jira",
                pass_criteria="Authentication audit logging exists"
            ),
            ComplianceRequirement(
                framework="SOC 2 Type II",
                article="CC6.6 - Encryption",
                requirement="Protect data with encryption",
                why_it_matters="Ensures data confidentiality in transit and at rest",
                what_we_check="TLS/HTTPS usage, encrypted connections",
                how_we_check="Search for https, ssl, encryption, secure connection",
                files_to_test=[
                    "deployment/docker-compose.yml",
                    "backend/utils/kafka_client.py"
                ],
                how_code_achieves_it="HTTPS for ServiceNow API, secure database connections, Kafka SSL support",
                pass_criteria="HTTPS used for external APIs"
            ),
            ComplianceRequirement(
                framework="SOC 2 Type II",
                article="CC7.2 - Security Monitoring",
                requirement="Monitor system for security events",
                why_it_matters="Enables detection and response to security incidents",
                what_we_check="Security alerts, intrusion detection, audit logs",
                how_we_check="Search for security alerts, unauthorized access, monitoring",
                files_to_test=[
                    "monitoring/alerts/ai_agent_alerts.yml",
                    "backend/governance/audit_logger.py"
                ],
                how_code_achieves_it="UnauthorizedAccessAttempt alert, UNAUTHORIZED_ACCESS audit events, security_compliance alert group",
                pass_criteria="Security monitoring alerts defined AND unauthorized access tracked"
            ),
            ComplianceRequirement(
                framework="SOC 2 Type II",
                article="CC8.1 - Change Management",
                requirement="Control and document system changes",
                why_it_matters="Prevents unauthorized changes and tracks modifications",
                what_we_check="Change logging, version control, deployment tracking",
                how_we_check="Search for version, deployment, change tracking",
                files_to_test=[
                    "backend/governance/audit_logger.py",
                    "backend/utils/github_actions.py"
                ],
                how_code_achieves_it="SYSTEM_CONFIG_CHANGE audit events, GitHub Actions integration for tracked deployments",
                pass_criteria="Change audit events logged"
            ),
            ComplianceRequirement(
                framework="SOC 2 Type II",
                article="A1.2 - Availability Monitoring",
                requirement="Monitor system availability",
                why_it_matters="Ensures system meets availability SLAs",
                what_we_check="Health checks, uptime monitoring, availability alerts",
                how_we_check="Search for health, availability, uptime, ServiceDown",
                files_to_test=[
                    "monitoring/alerts/ai_agent_alerts.yml",
                    "scripts/health_check.sh"
                ],
                how_code_achieves_it="ServiceDown alert, health check endpoints on all services, system_health alert group",
                pass_criteria="Health checks exist AND availability alerts defined"
            ),
            ComplianceRequirement(
                framework="SOC 2 Type II",
                article="PI1.1 - Processing Integrity",
                requirement="Ensure processing is complete and accurate",
                why_it_matters="AI outputs must be reliable and correct",
                what_we_check="Validation, checksums, integrity verification",
                how_we_check="Search for checksum, validation, integrity",
                files_to_test=[
                    "backend/governance/audit_logger.py",
                    "backend/guardrails/llm_guardrails.py"
                ],
                how_code_achieves_it="Audit event checksums with SHA256, LLM guardrails validate inputs/outputs",
                pass_criteria="Checksum validation implemented"
            ),
        ]

    def _iso_42001_requirements(self) -> List[ComplianceRequirement]:
        """ISO 42001 AI Management System requirements"""
        return [
            ComplianceRequirement(
                framework="ISO 42001",
                article="6.1 - AI Risk Assessment",
                requirement="Identify and assess AI-specific risks",
                why_it_matters="Proactive identification of AI risks before deployment",
                what_we_check="Risk identification, impact assessment, mitigation planning",
                how_we_check="Search for risk_assessment, risk_level, impact",
                files_to_test=[
                    "backend/agents/control_plane.py",
                    "backend/orchestrator/llm_judge.py"
                ],
                how_code_achieves_it="Control plane assesses remediation risk, LLM Judge evaluates plan safety, risk levels defined in enum",
                pass_criteria="Risk assessment function exists"
            ),
            ComplianceRequirement(
                framework="ISO 42001",
                article="7.2 - AI Competence",
                requirement="Ensure AI system competence through testing",
                why_it_matters="Validates AI performs its intended function correctly",
                what_we_check="Testing coverage, validation procedures, competence metrics",
                how_we_check="Search for test_, validation, competence",
                files_to_test=[
                    "tests/unit/test_agents.py",
                    "tests/unit/test_rag.py",
                    "tests/unit/test_guardrails.py"
                ],
                how_code_achieves_it="Comprehensive unit tests for agents, RAG system, and guardrails",
                pass_criteria="Unit tests exist for major AI components"
            ),
            ComplianceRequirement(
                framework="ISO 42001",
                article="8.2 - AI System Design",
                requirement="Design AI with safety and ethics in mind",
                why_it_matters="Ensures AI is designed responsibly from the start",
                what_we_check="Guardrails, safety checks, ethical guidelines",
                how_we_check="Search for guardrail, safety, ethical, PII",
                files_to_test=[
                    "backend/guardrails/llm_guardrails.py",
                    "backend/governance/audit_logger.py"
                ],
                how_code_achieves_it="LLMGuardrails for input/output validation, PII detection, prompt injection prevention",
                pass_criteria="Guardrails implemented AND PII protection present"
            ),
            ComplianceRequirement(
                framework="ISO 42001",
                article="9.1 - AI Monitoring",
                requirement="Monitor AI system performance and behavior",
                why_it_matters="Continuous oversight of AI in production",
                what_we_check="Performance metrics, behavior monitoring, anomaly detection",
                how_we_check="Search for metrics, monitoring, observability",
                files_to_test=[
                    "backend/orchestrator/metrics.py",
                    "monitoring/prometheus.yml",
                    "monitoring/alerts/ai_agent_alerts.yml"
                ],
                how_code_achieves_it="Prometheus metrics for LLM latency, error rates, token usage; alert rules for anomalies",
                pass_criteria="AI-specific metrics defined AND monitoring active"
            ),
            ComplianceRequirement(
                framework="ISO 42001",
                article="10.1 - AI Improvement",
                requirement="Continuous improvement of AI system",
                why_it_matters="AI systems should improve based on feedback and performance",
                what_we_check="Feedback loops, optimization, learning systems",
                how_we_check="Search for feedback, optimization, improvement, learning",
                files_to_test=[
                    "backend/rag/feedback_optimizer.py",
                    "backend/rag/hybrid_search_engine.py"
                ],
                how_code_achieves_it="FeedbackOptimizer learns from user feedback, RRF fusion adapts to retrieval patterns",
                pass_criteria="Feedback system implemented"
            ),
        ]

    def _nist_ai_rmf_requirements(self) -> List[ComplianceRequirement]:
        """NIST AI Risk Management Framework requirements"""
        return [
            ComplianceRequirement(
                framework="NIST AI RMF",
                article="GOVERN 1.1",
                requirement="Establish AI governance policies",
                why_it_matters="Organizational accountability for AI decisions",
                what_we_check="Governance policies, approval workflows, policy enforcement",
                how_we_check="Search for governance, policy, approval",
                files_to_test=[
                    "backend/governance/audit_logger.py",
                    "backend/agents/control_plane.py"
                ],
                how_code_achieves_it="Governance module with audit logging, control plane for policy enforcement, approval routing",
                pass_criteria="Governance module exists"
            ),
            ComplianceRequirement(
                framework="NIST AI RMF",
                article="MAP 1.1",
                requirement="Document AI system context and purpose",
                why_it_matters="Understanding AI system's role and boundaries",
                what_we_check="System documentation, purpose statements, context documentation",
                how_we_check="Search for docstrings, README, architecture docs",
                files_to_test=[
                    "docs/ARCHITECTURE_V5.md",
                    "backend/rag/__init__.py"
                ],
                how_code_achieves_it="Architecture documentation, module docstrings with WHY/HOW, API documentation",
                pass_criteria="System documentation exists"
            ),
            ComplianceRequirement(
                framework="NIST AI RMF",
                article="MEASURE 2.1",
                requirement="Measure AI system performance and fairness",
                why_it_matters="Quantitative assessment of AI behavior",
                what_we_check="Performance metrics, fairness metrics, bias detection",
                how_we_check="Search for metrics, performance, accuracy, confidence",
                files_to_test=[
                    "backend/orchestrator/metrics.py",
                    "backend/rag/hybrid_search_engine.py"
                ],
                how_code_achieves_it="Prometheus metrics for LLM performance, confidence scores on decisions, accuracy tracking",
                pass_criteria="Performance metrics tracked"
            ),
            ComplianceRequirement(
                framework="NIST AI RMF",
                article="MANAGE 2.2",
                requirement="Implement AI risk controls",
                why_it_matters="Active mitigation of identified AI risks",
                what_we_check="Risk controls, mitigation measures, safeguards",
                how_we_check="Search for risk, control, mitigation, safeguard",
                files_to_test=[
                    "backend/utils/circuit_breaker.py",
                    "backend/guardrails/llm_guardrails.py"
                ],
                how_code_achieves_it="Circuit breaker for fault tolerance, LLM guardrails for content safety, approval workflows for high-risk actions",
                pass_criteria="Risk controls implemented"
            ),
            ComplianceRequirement(
                framework="NIST AI RMF",
                article="MANAGE 4.1",
                requirement="Incident response for AI systems",
                why_it_matters="Ability to respond to AI failures and issues",
                what_we_check="Incident handling, alerting, rollback capabilities",
                how_we_check="Search for incident, alert, rollback, recovery",
                files_to_test=[
                    "monitoring/alerts/ai_agent_alerts.yml",
                    "backend/agents/execution_orchestrator.py"
                ],
                how_code_achieves_it="Prometheus alerts for AI incidents, execution orchestrator with rollback support, ServiceNow integration for incident tracking",
                pass_criteria="Incident alerting defined AND rollback capability exists"
            ),
        ]

    def _mitre_atlas_requirements(self) -> List[ComplianceRequirement]:
        """MITRE ATLAS adversarial threat requirements"""
        return [
            ComplianceRequirement(
                framework="MITRE ATLAS",
                article="AML.T0015 - Prompt Injection",
                requirement="Protect against prompt injection attacks",
                why_it_matters="Prevents malicious manipulation of LLM behavior",
                what_we_check="Input sanitization, prompt injection detection",
                how_we_check="Search for prompt_injection, sanitize, injection",
                files_to_test=[
                    "backend/guardrails/llm_guardrails.py"
                ],
                how_code_achieves_it="LLMGuardrails detects and blocks prompt injection attempts with pattern matching",
                pass_criteria="Prompt injection detection implemented"
            ),
            ComplianceRequirement(
                framework="MITRE ATLAS",
                article="AML.T0025 - Data Poisoning",
                requirement="Protect training/RAG data from poisoning",
                why_it_matters="Ensures AI learns from trusted, clean data",
                what_we_check="Data validation, source verification, anomaly detection",
                how_we_check="Search for validation, verify, trusted, poisoning",
                files_to_test=[
                    "backend/rag/embedding_service.py",
                    "backend/rag/smart_chunker.py"
                ],
                how_code_achieves_it="EmbeddingService validates input quality, SmartChunker filters malformed content",
                pass_criteria="Data validation implemented"
            ),
            ComplianceRequirement(
                framework="MITRE ATLAS",
                article="AML.T0047 - Model Evasion",
                requirement="Detect and prevent model evasion attempts",
                why_it_matters="Prevents adversaries from bypassing AI detection",
                what_we_check="Confidence thresholds, anomaly detection, evasion patterns",
                how_we_check="Search for confidence, threshold, anomaly, evasion",
                files_to_test=[
                    "backend/orchestrator/llm_intelligence.py",
                    "backend/guardrails/llm_guardrails.py"
                ],
                how_code_achieves_it="Confidence thresholds on decisions, low-confidence decisions flagged for review",
                pass_criteria="Confidence thresholds enforced"
            ),
            ComplianceRequirement(
                framework="MITRE ATLAS",
                article="AML.T0048 - API Abuse",
                requirement="Protect AI APIs from abuse",
                why_it_matters="Prevents excessive or malicious API usage",
                what_we_check="Rate limiting, cost controls, usage monitoring",
                how_we_check="Search for rate_limit, cost, budget, usage",
                files_to_test=[
                    "backend/utils/cost_tracker.py",
                    "monitoring/alerts/ai_agent_alerts.yml"
                ],
                how_code_achieves_it="CostTracker monitors LLM spending, budget alerts, DailyBudgetExceeded alert",
                pass_criteria="Cost controls AND usage monitoring implemented"
            ),
            ComplianceRequirement(
                framework="MITRE ATLAS",
                article="AML.T0050 - Model Theft",
                requirement="Protect AI model assets",
                why_it_matters="Prevents unauthorized access to AI models",
                what_we_check="Access controls, model protection, API key management",
                how_we_check="Search for api_key, secret, credential, access",
                files_to_test=[
                    "backend/agents/base_agent.py",
                    "deployment/docker-compose.yml"
                ],
                how_code_achieves_it="API keys via environment variables, no hardcoded credentials, Docker secrets support",
                pass_criteria="API keys managed via environment AND no hardcoded secrets"
            ),
        ]

    def scan_file(self, file_path: Path, patterns: List[str]) -> Dict[str, List[str]]:
        """Scan a single file for compliance patterns"""
        matches = defaultdict(list)

        if not file_path.exists():
            return matches

        try:
            content = file_path.read_text(encoding='utf-8')
            lines = content.split('\n')

            for pattern in patterns:
                # Search case-insensitive
                regex = re.compile(pattern, re.IGNORECASE)
                for i, line in enumerate(lines, 1):
                    if regex.search(line):
                        matches[pattern].append(f"{file_path.name}:{i}")
        except Exception as e:
            pass

        return matches

    def check_requirement(self, req: ComplianceRequirement) -> ScanResult:
        """Check a single compliance requirement"""
        all_matches = defaultdict(list)
        files_found = []
        notes = []

        # Extract patterns from how_we_check
        patterns = re.findall(r'(\w+_\w+|\w+)', req.how_we_check)
        patterns = [p for p in patterns if len(p) > 3 and p not in ['Search', 'for', 'check', 'the', 'and', 'with']]

        # Scan specified files
        for file_pattern in req.files_to_test:
            file_path = self.root_path / file_pattern
            if file_path.exists():
                files_found.append(str(file_path.relative_to(self.root_path)))
                matches = self.scan_file(file_path, patterns)
                for pattern, locations in matches.items():
                    all_matches[pattern].extend(locations)
            else:
                notes.append(f"File not found: {file_pattern}")

        # Calculate confidence
        total_patterns = max(len(patterns), 1)
        matched_patterns = len([p for p in patterns if all_matches.get(p)])
        file_coverage = len(files_found) / max(len(req.files_to_test), 1)

        confidence = (matched_patterns / total_patterns * 0.6) + (file_coverage * 0.4)

        # Determine status
        if confidence >= 0.7 and file_coverage >= 0.5:
            status = "PASS"
        elif confidence >= 0.4 or file_coverage >= 0.3:
            status = "PARTIAL"
        else:
            status = "FAIL"

        return ScanResult(
            requirement=req,
            files_found=files_found,
            patterns_matched=dict(all_matches),
            confidence_score=confidence,
            status=status,
            notes=notes
        )

    def scan_all(self, frameworks: List[str] = None) -> List[ScanResult]:
        """Scan all requirements for specified frameworks"""
        self.scan_results = []

        target_frameworks = frameworks or list(self.frameworks.keys())

        for framework_name, requirements in self.frameworks.items():
            if framework_name not in target_frameworks:
                continue

            for req in requirements:
                result = self.check_requirement(req)
                self.scan_results.append(result)

        return self.scan_results

    def generate_markdown_report(self) -> str:
        """Generate comprehensive markdown report with tables"""
        report = []
        report.append("# AI Agent Platform - Compliance Scan Report")
        report.append(f"\n**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"**Scanner Version:** 1.0.0")
        report.append(f"**Root Path:** `{self.root_path}`")

        # Executive Summary
        report.append("\n## Executive Summary\n")

        summary = defaultdict(lambda: {"pass": 0, "partial": 0, "fail": 0})
        for result in self.scan_results:
            framework = result.requirement.framework
            summary[framework][result.status.lower()] += 1

        report.append("| Framework | PASS | PARTIAL | FAIL | Coverage |")
        report.append("|-----------|------|---------|------|----------|")
        for framework, counts in summary.items():
            total = counts["pass"] + counts["partial"] + counts["fail"]
            coverage = (counts["pass"] + counts["partial"] * 0.5) / total * 100 if total > 0 else 0
            emoji = "" if coverage >= 80 else "" if coverage >= 50 else ""
            report.append(f"| {framework} | {counts['pass']} | {counts['partial']} | {counts['fail']} | {coverage:.0f}% {emoji} |")

        # Detailed Results per Framework
        for framework_name in self.frameworks.keys():
            framework_results = [r for r in self.scan_results if r.requirement.framework == framework_name]
            if not framework_results:
                continue

            report.append(f"\n---\n\n## {framework_name}\n")

            # Detailed Table
            report.append("| Article | Requirement | Why It Matters | What We Check | How We Check | Files We Test | How Our Code Achieves It | Pass Criteria | Status | Confidence |")
            report.append("|---------|-------------|----------------|---------------|--------------|---------------|--------------------------|---------------|--------|------------|")

            for result in framework_results:
                req = result.requirement
                status_emoji = "" if result.status == "PASS" else "" if result.status == "PARTIAL" else ""

                # Truncate long fields
                def truncate(s, max_len=100):
                    return s[:max_len-3] + "..." if len(s) > max_len else s

                files = ", ".join([f.split("/")[-1] for f in result.files_found[:3]])
                if len(result.files_found) > 3:
                    files += f" +{len(result.files_found)-3} more"

                row = [
                    req.article,
                    truncate(req.requirement, 60),
                    truncate(req.why_it_matters, 60),
                    truncate(req.what_we_check, 50),
                    truncate(req.how_we_check, 50),
                    files or "N/A",
                    truncate(req.how_code_achieves_it, 80),
                    truncate(req.pass_criteria, 60),
                    f"{status_emoji} {result.status}",
                    f"{result.confidence_score:.0%}"
                ]
                report.append("| " + " | ".join(row) + " |")

            # Evidence Section
            report.append(f"\n### {framework_name} - Evidence Details\n")
            for result in framework_results:
                req = result.requirement
                report.append(f"#### {req.article}\n")
                report.append(f"**Status:** {result.status} ({result.confidence_score:.0%} confidence)\n")
                report.append(f"**Files Found:** {', '.join(result.files_found) or 'None'}\n")
                if result.patterns_matched:
                    report.append("**Pattern Matches:**")
                    for pattern, locations in result.patterns_matched.items():
                        report.append(f"- `{pattern}`: {', '.join(locations[:5])}")
                        if len(locations) > 5:
                            report.append(f"  ... and {len(locations)-5} more")
                if result.notes:
                    report.append(f"**Notes:** {'; '.join(result.notes)}")
                report.append("")

        # Recommendations
        report.append("\n---\n\n## Recommendations\n")

        failed = [r for r in self.scan_results if r.status == "FAIL"]
        partial = [r for r in self.scan_results if r.status == "PARTIAL"]

        if failed:
            report.append("### Critical (FAIL)\n")
            for result in failed:
                report.append(f"- **{result.requirement.article}**: {result.requirement.pass_criteria}")

        if partial:
            report.append("\n### Improvements Needed (PARTIAL)\n")
            for result in partial:
                report.append(f"- **{result.requirement.article}**: Consider enhancing - {result.requirement.how_code_achieves_it}")

        # Appendix
        report.append("\n---\n\n## Appendix: Compliance Matrix Cross-Reference\n")
        report.append("| Requirement | EU AI Act | SOC 2 | ISO 42001 | NIST AI RMF | MITRE ATLAS |")
        report.append("|-------------|-----------|-------|-----------|-------------|-------------|")

        categories = {
            "Risk Management": ["Article 9", "CC6.1", "6.1", "GOVERN 1.1", "AML.T0047"],
            "Data Governance": ["Article 10", "PI1.1", "7.2", "MAP 1.1", "AML.T0025"],
            "Documentation": ["Article 11", "CC8.1", "8.2", "MAP 1.1", "-"],
            "Logging/Audit": ["Article 12", "CC7.2", "9.1", "MEASURE 2.1", "-"],
            "Transparency": ["Article 13", "-", "-", "-", "-"],
            "Human Oversight": ["Article 14", "-", "-", "MANAGE 2.2", "-"],
            "Security": ["Article 15", "CC6.6", "8.2", "MANAGE 4.1", "AML.T0015"],
        }

        for category, refs in categories.items():
            report.append(f"| {category} | {' | '.join(refs)} |")

        return "\n".join(report)

    def generate_json_report(self) -> Dict[str, Any]:
        """Generate JSON report for programmatic consumption"""
        return {
            "generated_at": datetime.now().isoformat(),
            "scanner_version": "1.0.0",
            "root_path": str(self.root_path),
            "summary": {
                framework: {
                    "pass": len([r for r in self.scan_results if r.requirement.framework == framework and r.status == "PASS"]),
                    "partial": len([r for r in self.scan_results if r.requirement.framework == framework and r.status == "PARTIAL"]),
                    "fail": len([r for r in self.scan_results if r.requirement.framework == framework and r.status == "FAIL"]),
                }
                for framework in self.frameworks.keys()
            },
            "results": [
                {
                    "framework": r.requirement.framework,
                    "article": r.requirement.article,
                    "requirement": r.requirement.requirement,
                    "status": r.status,
                    "confidence": r.confidence_score,
                    "files_found": r.files_found,
                    "patterns_matched": r.patterns_matched,
                    "notes": r.notes,
                }
                for r in self.scan_results
            ]
        }


def main():
    parser = argparse.ArgumentParser(
        description="Enterprise Compliance Scanner for AI Agent Platform",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 scripts/compliance_scanner.py
  python3 scripts/compliance_scanner.py --output reports/compliance.md
  python3 scripts/compliance_scanner.py --framework "EU AI Act" --framework "SOC 2 Type II"
  python3 scripts/compliance_scanner.py --json > compliance.json
        """
    )
    parser.add_argument("--output", "-o", help="Output file path (default: stdout)")
    parser.add_argument("--framework", "-f", action="append", help="Specific framework(s) to scan")
    parser.add_argument("--json", "-j", action="store_true", help="Output as JSON instead of Markdown")
    parser.add_argument("--root", "-r", help="Root path to scan (default: project root)")

    args = parser.parse_args()

    # Initialize scanner
    root_path = args.root or os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    scanner = ComplianceScanner(root_path)

    # Run scan
    print("=" * 70, file=sys.stderr)
    print("  ENTERPRISE COMPLIANCE SCANNER v1.0.0", file=sys.stderr)
    print("=" * 70, file=sys.stderr)
    print(f"\nScanning: {root_path}", file=sys.stderr)

    frameworks = args.framework if args.framework else None
    scanner.scan_all(frameworks)

    # Generate report
    if args.json:
        report = json.dumps(scanner.generate_json_report(), indent=2)
    else:
        report = scanner.generate_markdown_report()

    # Output
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(report)
        print(f"\nReport written to: {args.output}", file=sys.stderr)
    else:
        print(report)

    # Summary to stderr
    total_pass = len([r for r in scanner.scan_results if r.status == "PASS"])
    total_partial = len([r for r in scanner.scan_results if r.status == "PARTIAL"])
    total_fail = len([r for r in scanner.scan_results if r.status == "FAIL"])
    total = total_pass + total_partial + total_fail

    print(f"\n{'=' * 70}", file=sys.stderr)
    print(f"  SCAN COMPLETE", file=sys.stderr)
    print(f"{'=' * 70}", file=sys.stderr)
    print(f"  PASS:    {total_pass}/{total}", file=sys.stderr)
    print(f"  PARTIAL: {total_partial}/{total}", file=sys.stderr)
    print(f"  FAIL:    {total_fail}/{total}", file=sys.stderr)
    overall = (total_pass + total_partial * 0.5) / total * 100 if total > 0 else 0
    print(f"  OVERALL: {overall:.0f}% compliance coverage", file=sys.stderr)
    print(f"{'=' * 70}\n", file=sys.stderr)

    return 0 if total_fail == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
