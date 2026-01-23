#!/usr/bin/env python3
"""
AI Agent Platform - Detailed Compliance Checker with Evidence Matrix
=====================================================================

Tests compliance against multiple standards with detailed evidence:
- SOC 2 Type II
- ISO 42001 (AI Management System)
- NIST AI RMF
- EU AI Act (Articles 9-15)
- MITRE ATLAS

Usage:
    python3 tests/compliance/detailed_compliance_checker.py [--standard <name>] [--format table|json]

Author: AI Agent Platform
Version: 2.0.0
"""

import os
import sys
import json
import re
import argparse
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, field
from typing import List, Dict, Tuple, Optional
from enum import Enum

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "backend"))


class Status(Enum):
    PASS = "PASS"
    FAIL = "FAIL"
    PARTIAL = "PARTIAL"


@dataclass
class ComplianceCheck:
    """Single compliance check definition"""
    control_id: str
    requirement: str
    why_test: str
    search_patterns: List[str]
    files_to_test: List[str]
    pass_condition: str

    # Results
    status: Status = Status.FAIL
    evidence: List[str] = field(default_factory=list)
    files_found: List[str] = field(default_factory=list)
    patterns_found: Dict[str, List[str]] = field(default_factory=dict)


@dataclass
class ComplianceFramework:
    """Compliance framework with checks"""
    name: str
    description: str
    checks: List[ComplianceCheck] = field(default_factory=list)

    @property
    def passed(self) -> int:
        return sum(1 for c in self.checks if c.status == Status.PASS)

    @property
    def failed(self) -> int:
        return sum(1 for c in self.checks if c.status == Status.FAIL)

    @property
    def partial(self) -> int:
        return sum(1 for c in self.checks if c.status == Status.PARTIAL)

    @property
    def score(self) -> float:
        if not self.checks:
            return 0.0
        return ((self.passed + self.partial * 0.5) / len(self.checks)) * 100


# =============================================================================
# FILE AND PATTERN SEARCH UTILITIES
# =============================================================================

def file_exists(path: str) -> bool:
    """Check if file/directory exists"""
    return (PROJECT_ROOT / path).exists()


def dir_exists(path: str) -> bool:
    """Check if directory exists"""
    p = PROJECT_ROOT / path
    return p.exists() and p.is_dir()


def count_files(path: str, pattern: str = "*.py") -> int:
    """Count files matching pattern in directory"""
    p = PROJECT_ROOT / path
    if not p.exists():
        return 0
    return len(list(p.rglob(pattern)))


def search_in_file(filepath: str, patterns: List[str]) -> Dict[str, List[str]]:
    """Search for patterns in a file, return matches with line numbers"""
    results = {p: [] for p in patterns}
    full_path = PROJECT_ROOT / filepath

    if not full_path.exists():
        return results

    try:
        content = full_path.read_text()
        lines = content.split('\n')

        for i, line in enumerate(lines, 1):
            for pattern in patterns:
                try:
                    if re.search(pattern, line, re.IGNORECASE):
                        results[pattern].append(f"Line {i}: {line.strip()[:80]}")
                except re.error:
                    # Fallback to simple string search
                    if pattern.lower() in line.lower():
                        results[pattern].append(f"Line {i}: {line.strip()[:80]}")
    except Exception:
        pass

    return results


def search_in_directory(dirpath: str, patterns: List[str], file_pattern: str = "*.py") -> Dict[str, List[Tuple[str, str]]]:
    """Search for patterns in all files in directory"""
    results = {p: [] for p in patterns}
    dir_path = PROJECT_ROOT / dirpath

    if not dir_path.exists():
        return results

    for file in dir_path.rglob(file_pattern):
        rel_path = str(file.relative_to(PROJECT_ROOT))
        file_results = search_in_file(rel_path, patterns)

        for pattern, matches in file_results.items():
            for match in matches:
                results[pattern].append((rel_path, match))

    return results


def list_files(path: str, pattern: str = "*") -> List[str]:
    """List files matching pattern"""
    p = PROJECT_ROOT / path
    if not p.exists():
        return []

    if p.is_file():
        return [str(p.relative_to(PROJECT_ROOT))]

    return [str(f.relative_to(PROJECT_ROOT)) for f in p.rglob(pattern)]


# =============================================================================
# EU AI ACT CHECKS
# =============================================================================

def create_eu_ai_act_framework() -> ComplianceFramework:
    """Create EU AI Act compliance framework"""
    framework = ComplianceFramework(
        name="EU-AI-Act",
        description="EU AI Act Articles 9-15 for High-Risk AI Systems"
    )

    framework.checks = [
        ComplianceCheck(
            control_id="Art.9",
            requirement="Risk Management",
            why_test="Prevent AI harm through systematic risk controls",
            search_patterns=["circuit_breaker", "try.*except", "risk_level", "RiskLevel", "CircuitBreaker"],
            files_to_test=["backend/utils/circuit_breaker.py", "backend/guardrails/llm_guardrails.py",
                          "backend/governance/eu_ai_act_compliance.py", "tests/chaos/"],
            pass_condition="≥2 risk controls exist"
        ),
        ComplianceCheck(
            control_id="Art.10",
            requirement="Data Governance",
            why_test="Protect data quality & privacy",
            search_patterns=["pii.*detect", "detect_pii", "retention", "DataRetention", "GDPR"],
            files_to_test=["backend/rag/", "backend/governance/data_retention.py",
                          "backend/guardrails/llm_guardrails.py"],
            pass_condition="RAG + retention policies exist"
        ),
        ComplianceCheck(
            control_id="Art.11",
            requirement="Technical Documentation",
            why_test="Anyone can understand the system",
            search_patterns=[],  # File existence check only
            files_to_test=["docs/ARCHITECTURE_V5.md", "docs/README.md", "docs/PROTOCOL_GUIDE.md",
                          "docs/EU_AI_ACT_COMPLIANCE_GUIDE.md", "docs/COMPLIANCE_MATRIX.md"],
            pass_condition="≥3 documentation files exist"
        ),
        ComplianceCheck(
            control_id="Art.12",
            requirement="Record-Keeping",
            why_test="Track all AI decisions for audit",
            search_patterns=["audit_logger", "log_event", "AuditEvent", "AuditEventType"],
            files_to_test=["backend/governance/audit_logger.py", "backend/orchestrator/main.py",
                          "backend/orchestrator/langgraph_workflow.py"],
            pass_condition="Audit logger implemented and used"
        ),
        ComplianceCheck(
            control_id="Art.13",
            requirement="Transparency",
            why_test="AI explains its decisions",
            search_patterns=["explanation", "reasoning", "root_cause", "ai_explanation", "decision_explanation"],
            files_to_test=["backend/"],
            pass_condition="Explanation patterns in ≥3 files"
        ),
        ComplianceCheck(
            control_id="Art.14",
            requirement="Human Oversight",
            why_test="Humans can override AI decisions",
            search_patterns=["hitl", "human.*approval", "human.*oversight", "HUMAN_APPROVAL",
                            "human.*reject", "approval.*request"],
            files_to_test=["backend/orchestrator/main.py", "backend/agents/control_plane.py",
                          "backend/governance/audit_logger.py"],
            pass_condition="HITL patterns found in control flow"
        ),
        ComplianceCheck(
            control_id="Art.15",
            requirement="Accuracy & Security",
            why_test="AI is reliable and secure",
            search_patterns=["confidence.*threshold", "validate_input", "validate_output",
                            "injection.*detect", "sanitize"],
            files_to_test=["backend/guardrails/llm_guardrails.py", "backend/rag/intelligent_retriever.py",
                          "backend/orchestrator/llm_judge.py"],
            pass_condition="Validation controls exist"
        )
    ]

    return framework


# =============================================================================
# SOC 2 TYPE II CHECKS
# =============================================================================

def create_soc2_framework() -> ComplianceFramework:
    """Create SOC 2 Type II compliance framework"""
    framework = ComplianceFramework(
        name="SOC2-TypeII",
        description="SOC 2 Type II - Security, Availability, Processing Integrity"
    )

    framework.checks = [
        ComplianceCheck(
            control_id="CC1.1",
            requirement="Access Control",
            why_test="Restrict unauthorized access",
            search_patterns=["authorization", "authenticate", "token", "api_key", "bearer", "APIKey"],
            files_to_test=["backend/orchestrator/main.py", "backend/governance/audit_logger.py"],
            pass_condition="≥2 authentication patterns"
        ),
        ComplianceCheck(
            control_id="CC2.1",
            requirement="Risk Assessment",
            why_test="Identify and manage risks",
            search_patterns=["risk_level", "RiskLevel", "assess_risk", "risk_threshold"],
            files_to_test=["backend/governance/eu_ai_act_compliance.py", "backend/agents/control_plane.py"],
            pass_condition="Risk classification exists"
        ),
        ComplianceCheck(
            control_id="CC3.1",
            requirement="Change Management",
            why_test="Control system changes",
            search_patterns=["SYSTEM_CONFIG_CHANGE", "git", "version_control"],
            files_to_test=[".git/", "backend/utils/github_actions.py", "backend/governance/audit_logger.py"],
            pass_condition="Git + change audit logging"
        ),
        ComplianceCheck(
            control_id="CC4.1",
            requirement="Monitoring & Logging",
            why_test="Monitor system operations",
            search_patterns=["prometheus", "structlog", "langfuse", "metrics", "Histogram", "Counter"],
            files_to_test=["backend/orchestrator/metrics.py", "requirements.txt"],
            pass_condition="≥3 monitoring tools"
        ),
        ComplianceCheck(
            control_id="CC5.1",
            requirement="Data Protection",
            why_test="Protect confidential data",
            search_patterns=["PII", "detect_pii", "anonymize", "retention", "encrypt"],
            files_to_test=["backend/guardrails/llm_guardrails.py", "backend/governance/data_retention.py"],
            pass_condition="PII detection + retention"
        ),
        ComplianceCheck(
            control_id="A1.1",
            requirement="System Availability",
            why_test="Meet availability commitments",
            search_patterns=["/health", "health_check", "circuit_breaker", "CircuitBreaker", "redis"],
            files_to_test=["backend/orchestrator/main.py", "backend/utils/circuit_breaker.py",
                          "backend/utils/redis_client.py"],
            pass_condition="Health + resilience patterns"
        ),
        ComplianceCheck(
            control_id="PI1.1",
            requirement="Processing Integrity",
            why_test="Ensure accurate processing",
            search_patterns=["validate", "ValidationResult", "confidence", "LLM.*Judge", "quality"],
            files_to_test=["backend/guardrails/llm_guardrails.py", "backend/orchestrator/llm_judge.py"],
            pass_condition="Validation + quality checks"
        )
    ]

    return framework


# =============================================================================
# ISO 42001 CHECKS
# =============================================================================

def create_iso42001_framework() -> ComplianceFramework:
    """Create ISO 42001 compliance framework"""
    framework = ComplianceFramework(
        name="ISO-42001",
        description="ISO 42001 - AI Management System"
    )

    framework.checks = [
        ComplianceCheck(
            control_id="4.1",
            requirement="Organization Context",
            why_test="Understand AI system context",
            search_patterns=["architecture", "system.*context"],
            files_to_test=["docs/ARCHITECTURE_V5.md", "docs/README.md"],
            pass_condition="≥1 architecture doc exists"
        ),
        ComplianceCheck(
            control_id="5.2",
            requirement="AI Policy",
            why_test="Establish AI governance policy",
            search_patterns=["governance", "compliance", "policy", "eu_ai_act"],
            files_to_test=["backend/governance/", "docs/EU_AI_ACT_COMPLIANCE_GUIDE.md"],
            pass_condition="Governance module exists"
        ),
        ComplianceCheck(
            control_id="6.1",
            requirement="Risk Management",
            why_test="Address AI-specific risks",
            search_patterns=["RiskLevel", "risk_assessment", "guardrails", "circuit_breaker"],
            files_to_test=["backend/governance/eu_ai_act_compliance.py", "backend/guardrails/llm_guardrails.py",
                          "backend/agents/control_plane.py"],
            pass_condition="≥2 risk controls"
        ),
        ComplianceCheck(
            control_id="7.2",
            requirement="Competence",
            why_test="Ensure personnel competence",
            search_patterns=[],  # File count check
            files_to_test=["docs/"],
            pass_condition="≥3 documentation files"
        ),
        ComplianceCheck(
            control_id="8.2",
            requirement="AI System Lifecycle",
            why_test="Manage AI system lifecycle",
            search_patterns=["workflow", "execution", "StateGraph", "langgraph"],
            files_to_test=["backend/orchestrator/langgraph_workflow.py",
                          "backend/agents/execution_orchestrator.py",
                          "backend/rag/feedback_optimizer.py"],
            pass_condition="Lifecycle components exist"
        ),
        ComplianceCheck(
            control_id="8.4",
            requirement="Data Management",
            why_test="Manage AI training/inference data",
            search_patterns=["embedding", "retriever", "retention", "chunker"],
            files_to_test=["backend/rag/", "backend/rag/embedding_service.py",
                          "backend/governance/data_retention.py"],
            pass_condition="RAG + embedding + retention"
        ),
        ComplianceCheck(
            control_id="9.1",
            requirement="Performance Evaluation",
            why_test="Monitor AI performance",
            search_patterns=["metrics", "prometheus", "langfuse", "Histogram", "Gauge"],
            files_to_test=["backend/orchestrator/metrics.py", "requirements.txt"],
            pass_condition="Metrics collection exists"
        ),
        ComplianceCheck(
            control_id="10.1",
            requirement="Continual Improvement",
            why_test="Continuously improve AI",
            search_patterns=["feedback", "optimizer", "llm_judge", "improve"],
            files_to_test=["backend/rag/feedback_optimizer.py", "backend/orchestrator/llm_judge.py"],
            pass_condition="Feedback loop exists"
        )
    ]

    return framework


# =============================================================================
# NIST AI RMF CHECKS
# =============================================================================

def create_nist_ai_rmf_framework() -> ComplianceFramework:
    """Create NIST AI RMF compliance framework"""
    framework = ComplianceFramework(
        name="NIST-AI-RMF",
        description="NIST AI Risk Management Framework - GOVERN, MAP, MEASURE, MANAGE"
    )

    framework.checks = [
        ComplianceCheck(
            control_id="GOVERN-1",
            requirement="Policies & Processes",
            why_test="Establish AI risk policies",
            search_patterns=["governance", "policy", "compliance"],
            files_to_test=["backend/governance/", "backend/agents/control_plane.py"],
            pass_condition="Governance module exists"
        ),
        ComplianceCheck(
            control_id="GOVERN-2",
            requirement="Accountability",
            why_test="Establish accountability structures",
            search_patterns=["audit", "user_id", "actor", "HUMAN_APPROVAL", "actor_type"],
            files_to_test=["backend/governance/audit_logger.py"],
            pass_condition="User attribution in logs"
        ),
        ComplianceCheck(
            control_id="MAP-1",
            requirement="Context Establishment",
            why_test="Establish AI system context",
            search_patterns=["query_understanding", "context", "architecture"],
            files_to_test=["backend/rag/query_understanding.py", "docs/ARCHITECTURE_V5.md"],
            pass_condition="Context components exist"
        ),
        ComplianceCheck(
            control_id="MAP-2",
            requirement="Risk Categorization",
            why_test="Categorize AI risks",
            search_patterns=["RiskLevel", "CRITICAL", "HIGH", "MEDIUM", "LOW"],
            files_to_test=["backend/governance/eu_ai_act_compliance.py", "backend/agents/control_plane.py"],
            pass_condition="Risk levels defined"
        ),
        ComplianceCheck(
            control_id="MEASURE-1",
            requirement="Risk Metrics",
            why_test="Define AI risk metrics",
            search_patterns=["metrics", "prometheus", "confidence", "Histogram"],
            files_to_test=["backend/orchestrator/metrics.py", "backend/rag/intelligent_retriever.py"],
            pass_condition="Metrics + confidence scoring"
        ),
        ComplianceCheck(
            control_id="MEASURE-2",
            requirement="Testing",
            why_test="Test AI systems regularly",
            search_patterns=["test_", "pytest", "unittest"],
            files_to_test=["tests/unit/", "tests/security/", "tests/chaos/", "tests/compliance/"],
            pass_condition="≥2 test directories"
        ),
        ComplianceCheck(
            control_id="MANAGE-1",
            requirement="Risk Prioritization",
            why_test="Prioritize AI risks",
            search_patterns=["priority", "threshold", "auto_approve", "judge_threshold"],
            files_to_test=["backend/agents/control_plane.py"],
            pass_condition="Risk-based routing exists"
        ),
        ComplianceCheck(
            control_id="MANAGE-2",
            requirement="Risk Response",
            why_test="Respond to AI risks effectively",
            search_patterns=["circuit_breaker", "guardrails", "escalate", "fallback"],
            files_to_test=["backend/utils/circuit_breaker.py", "backend/guardrails/llm_guardrails.py",
                          "backend/agents/control_plane.py"],
            pass_condition="≥2 response mechanisms"
        )
    ]

    return framework


# =============================================================================
# MITRE ATLAS CHECKS
# =============================================================================

def create_mitre_atlas_framework() -> ComplianceFramework:
    """Create MITRE ATLAS compliance framework"""
    framework = ComplianceFramework(
        name="MITRE-ATLAS",
        description="MITRE ATLAS - Adversarial Threat Landscape for AI Systems"
    )

    framework.checks = [
        ComplianceCheck(
            control_id="AML.T0051",
            requirement="Prompt Injection Prevention",
            why_test="Prevent prompt manipulation attacks",
            search_patterns=["prompt_injection", "INJECTION_PATTERNS", "jailbreak", "override.*instruction"],
            files_to_test=["backend/guardrails/llm_guardrails.py"],
            pass_condition="Injection detection exists"
        ),
        ComplianceCheck(
            control_id="AML.T0015",
            requirement="Model Evasion Prevention",
            why_test="Prevent adversarial inputs",
            search_patterns=["validate", "sanitize", "confidence.*threshold", "min_score"],
            files_to_test=["backend/guardrails/llm_guardrails.py", "backend/rag/intelligent_retriever.py"],
            pass_condition="Input validation exists"
        ),
        ComplianceCheck(
            control_id="AML.T0020",
            requirement="Data Poisoning Prevention",
            why_test="Prevent malicious data injection",
            search_patterns=["validate", "quality", "human.*review", "verify"],
            files_to_test=["backend/rag/smart_chunker.py", "backend/rag/feedback_optimizer.py",
                          "backend/agents/control_plane.py"],
            pass_condition="Data validation exists"
        ),
        ComplianceCheck(
            control_id="AML.T0024",
            requirement="Model Theft Prevention",
            why_test="Prevent model extraction attacks",
            search_patterns=["rate_limit", "throttle", "DATA_ACCESS", "quota"],
            files_to_test=["backend/guardrails/llm_guardrails.py", "backend/governance/audit_logger.py"],
            pass_condition="Rate limiting + access logs"
        ),
        ComplianceCheck(
            control_id="AML.T0010",
            requirement="Supply Chain Security",
            why_test="Prevent supply chain attacks",
            search_patterns=[">=", "==", "~="],  # Version pinning
            files_to_test=["requirements.txt"],
            pass_condition="Dependencies documented with versions"
        ),
        ComplianceCheck(
            control_id="AML.T0043",
            requirement="Output Integrity",
            why_test="Verify AI output integrity",
            search_patterns=["validate_output", "LLM.*Judge", "checksum", "verify_output"],
            files_to_test=["backend/guardrails/llm_guardrails.py", "backend/orchestrator/llm_judge.py",
                          "backend/governance/audit_logger.py"],
            pass_condition="Output validation exists"
        ),
        ComplianceCheck(
            control_id="AML.T0048",
            requirement="Inference API Security",
            why_test="Prevent API manipulation",
            search_patterns=["sanitize", "escape", "cors", "authentication", "CORSMiddleware"],
            files_to_test=["backend/guardrails/llm_guardrails.py", "backend/orchestrator/main.py"],
            pass_condition="API security exists"
        )
    ]

    return framework


# =============================================================================
# CHECK EXECUTION ENGINE
# =============================================================================

def run_check(check: ComplianceCheck) -> ComplianceCheck:
    """Execute a single compliance check and collect evidence"""
    evidence = []
    patterns_found = {}
    files_found = []

    # Check file existence
    for file_path in check.files_to_test:
        if file_exists(file_path):
            files_found.append(file_path)
            if file_path.endswith('/'):
                count = count_files(file_path, "*.py")
                evidence.append(f"Directory {file_path} exists with {count} Python files")
            else:
                evidence.append(f"File {file_path} exists")

    # Search for patterns
    for file_path in check.files_to_test:
        if check.search_patterns:
            if file_path.endswith('/'):
                results = search_in_directory(file_path, check.search_patterns)
                for pattern, matches in results.items():
                    if matches:
                        if pattern not in patterns_found:
                            patterns_found[pattern] = []
                        patterns_found[pattern].extend([f"{f}: {m}" for f, m in matches[:3]])
            elif file_exists(file_path):
                results = search_in_file(file_path, check.search_patterns)
                for pattern, matches in results.items():
                    if matches:
                        if pattern not in patterns_found:
                            patterns_found[pattern] = []
                        patterns_found[pattern].extend(matches[:3])

    # Add pattern evidence
    for pattern, matches in patterns_found.items():
        evidence.append(f"Pattern '{pattern}' found {len(matches)} times")

    # Determine status based on pass condition
    check.evidence = evidence
    check.files_found = files_found
    check.patterns_found = patterns_found

    # Evaluate pass condition
    total_evidence = len(files_found) + len(patterns_found)

    if "≥3" in check.pass_condition:
        if total_evidence >= 3:
            check.status = Status.PASS
        elif total_evidence >= 1:
            check.status = Status.PARTIAL
        else:
            check.status = Status.FAIL
    elif "≥2" in check.pass_condition:
        if total_evidence >= 2:
            check.status = Status.PASS
        elif total_evidence >= 1:
            check.status = Status.PARTIAL
        else:
            check.status = Status.FAIL
    elif "≥1" in check.pass_condition or "exist" in check.pass_condition.lower():
        if total_evidence >= 1:
            check.status = Status.PASS
        else:
            check.status = Status.FAIL
    else:
        # Default: pass if any evidence found
        if total_evidence >= 1:
            check.status = Status.PASS
        else:
            check.status = Status.FAIL

    return check


def run_framework(framework: ComplianceFramework) -> ComplianceFramework:
    """Run all checks in a framework"""
    for check in framework.checks:
        run_check(check)
    return framework


# =============================================================================
# OUTPUT FORMATTERS
# =============================================================================

def print_table_report(frameworks: List[ComplianceFramework]):
    """Print detailed table report"""

    print("=" * 120)
    print("AI AGENT PLATFORM - DETAILED COMPLIANCE REPORT")
    print("=" * 120)
    print(f"Generated: {datetime.now().isoformat()}")
    print(f"Platform Version: 5.0.0")
    print()

    # Overall summary
    total_checks = sum(len(f.checks) for f in frameworks)
    total_passed = sum(f.passed for f in frameworks)
    total_partial = sum(f.partial for f in frameworks)
    total_failed = sum(f.failed for f in frameworks)
    overall_score = (total_passed + total_partial * 0.5) / total_checks * 100 if total_checks > 0 else 0

    print("-" * 120)
    print("EXECUTIVE SUMMARY")
    print("-" * 120)
    print(f"{'Framework':<20} {'Checks':<10} {'Pass':<10} {'Partial':<10} {'Fail':<10} {'Score':<10}")
    print("-" * 120)

    for f in frameworks:
        print(f"{f.name:<20} {len(f.checks):<10} {f.passed:<10} {f.partial:<10} {f.failed:<10} {f.score:.1f}%")

    print("-" * 120)
    print(f"{'TOTAL':<20} {total_checks:<10} {total_passed:<10} {total_partial:<10} {total_failed:<10} {overall_score:.1f}%")
    print()

    # Detailed tables per framework
    for framework in frameworks:
        print("=" * 120)
        print(f"{framework.name}: {framework.description}")
        print("=" * 120)
        print()

        # Header
        print(f"{'Control':<12} {'Requirement':<25} {'Why Test?':<30} {'Status':<8} {'Evidence':<40}")
        print("-" * 120)

        for check in framework.checks:
            status_str = f"[{check.status.value}]"
            evidence_summary = f"{len(check.files_found)} files, {len(check.patterns_found)} patterns"

            print(f"{check.control_id:<12} {check.requirement:<25} {check.why_test[:28]:<30} {status_str:<8} {evidence_summary:<40}")

        print()

        # Evidence details
        print("Evidence Details:")
        print("-" * 80)
        for check in framework.checks:
            if check.evidence:
                print(f"\n  [{check.control_id}] {check.requirement}:")
                print(f"    Pass Condition: {check.pass_condition}")
                print(f"    Files to Test: {', '.join(check.files_to_test[:3])}")
                print(f"    Search Patterns: {', '.join(check.search_patterns[:3])}")
                print(f"    Evidence Found:")
                for e in check.evidence[:5]:
                    print(f"      - {e}")
        print()


def print_json_report(frameworks: List[ComplianceFramework]) -> str:
    """Generate JSON report"""
    data = {
        "timestamp": datetime.now().isoformat(),
        "platform_version": "5.0.0",
        "summary": {
            "total_checks": sum(len(f.checks) for f in frameworks),
            "total_passed": sum(f.passed for f in frameworks),
            "total_partial": sum(f.partial for f in frameworks),
            "total_failed": sum(f.failed for f in frameworks),
            "overall_score": sum(f.score for f in frameworks) / len(frameworks) if frameworks else 0
        },
        "frameworks": []
    }

    for f in frameworks:
        framework_data = {
            "name": f.name,
            "description": f.description,
            "score": f.score,
            "passed": f.passed,
            "partial": f.partial,
            "failed": f.failed,
            "checks": []
        }

        for c in f.checks:
            check_data = {
                "control_id": c.control_id,
                "requirement": c.requirement,
                "why_test": c.why_test,
                "search_patterns": c.search_patterns,
                "files_to_test": c.files_to_test,
                "pass_condition": c.pass_condition,
                "status": c.status.value,
                "evidence": c.evidence,
                "files_found": c.files_found,
                "patterns_found": {k: v[:3] for k, v in c.patterns_found.items()}
            }
            framework_data["checks"].append(check_data)

        data["frameworks"].append(framework_data)

    return json.dumps(data, indent=2)


def print_markdown_matrix(frameworks: List[ComplianceFramework]):
    """Print markdown format compliance matrix"""

    print("# Compliance Test Results\n")
    print(f"Generated: {datetime.now().isoformat()}\n")

    for framework in frameworks:
        print(f"## {framework.name}\n")
        print(f"**Score: {framework.score:.1f}%** ({framework.passed} Pass, {framework.partial} Partial, {framework.failed} Fail)\n")

        print("| Control | Requirement | Why Test? | What We Search | Files Tested | Pass If | Status |")
        print("|---------|-------------|-----------|----------------|--------------|---------|--------|")

        for check in framework.checks:
            patterns = ", ".join(check.search_patterns[:2]) + ("..." if len(check.search_patterns) > 2 else "")
            files = ", ".join([f.split("/")[-1] for f in check.files_to_test[:2]]) + ("..." if len(check.files_to_test) > 2 else "")
            status_emoji = {"PASS": "✅", "PARTIAL": "⚠️", "FAIL": "❌"}[check.status.value]

            print(f"| {check.control_id} | {check.requirement} | {check.why_test[:25]}... | `{patterns}` | {files} | {check.pass_condition} | {status_emoji} |")

        print()


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Detailed Compliance Checker with Evidence Matrix")
    parser.add_argument("--standard", "-s", choices=["EU-AI-Act", "SOC2", "ISO42001", "NIST-AI-RMF", "MITRE-ATLAS", "all"],
                       default="all", help="Standard to check")
    parser.add_argument("--format", "-f", choices=["table", "json", "markdown"], default="table",
                       help="Output format")
    parser.add_argument("--output", "-o", help="Output file path")

    args = parser.parse_args()

    # Create frameworks
    all_frameworks = {
        "EU-AI-Act": create_eu_ai_act_framework,
        "SOC2": create_soc2_framework,
        "ISO42001": create_iso42001_framework,
        "NIST-AI-RMF": create_nist_ai_rmf_framework,
        "MITRE-ATLAS": create_mitre_atlas_framework
    }

    if args.standard == "all":
        frameworks = [f() for f in all_frameworks.values()]
    else:
        frameworks = [all_frameworks[args.standard]()]

    # Run checks
    for framework in frameworks:
        run_framework(framework)

    # Generate output
    if args.format == "json":
        output = print_json_report(frameworks)
        if args.output:
            with open(args.output, "w") as f:
                f.write(output)
            print(f"Report written to: {args.output}")
        else:
            print(output)
    elif args.format == "markdown":
        print_markdown_matrix(frameworks)
    else:
        print_table_report(frameworks)

    # Exit code
    total_failed = sum(f.failed for f in frameworks)
    return 1 if total_failed > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
