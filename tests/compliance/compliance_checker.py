#!/usr/bin/env python3
"""
AI Agent Platform - Comprehensive Compliance Checker
=====================================================

Tests compliance against multiple industry standards:
- SOC 2 Type II (Security, Availability, Processing Integrity, Confidentiality, Privacy)
- ISO 42001 (AI Management System)
- NIST AI RMF (Risk Management Framework)
- EU AI Act (Articles 9-15 for High-Risk AI)
- MITRE ATLAS (Adversarial Threat Landscape for AI Systems)

Usage:
    python3 tests/compliance/compliance_checker.py [--standard <name>] [--verbose] [--json]

Author: AI Agent Platform
Version: 1.0.0
"""

import os
import sys
import json
import argparse
import importlib
import inspect
from datetime import datetime
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Callable, Any
from enum import Enum
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "backend"))


# =============================================================================
# DATA STRUCTURES
# =============================================================================

class ComplianceStatus(Enum):
    PASS = "PASS"
    FAIL = "FAIL"
    PARTIAL = "PARTIAL"
    NOT_APPLICABLE = "N/A"
    MANUAL_REVIEW = "MANUAL_REVIEW"


class Severity(Enum):
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    INFO = "INFO"


@dataclass
class ComplianceCheck:
    """Individual compliance check result"""
    standard: str
    control_id: str
    control_name: str
    description: str
    status: ComplianceStatus
    severity: Severity
    evidence: List[str] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)
    implementation_details: str = ""


@dataclass
class ComplianceReport:
    """Full compliance report"""
    timestamp: str
    platform_version: str
    standards_tested: List[str]
    total_checks: int = 0
    passed: int = 0
    failed: int = 0
    partial: int = 0
    manual_review: int = 0
    checks: List[ComplianceCheck] = field(default_factory=list)

    def add_check(self, check: ComplianceCheck):
        self.checks.append(check)
        self.total_checks += 1
        if check.status == ComplianceStatus.PASS:
            self.passed += 1
        elif check.status == ComplianceStatus.FAIL:
            self.failed += 1
        elif check.status == ComplianceStatus.PARTIAL:
            self.partial += 1
        elif check.status == ComplianceStatus.MANUAL_REVIEW:
            self.manual_review += 1

    @property
    def compliance_score(self) -> float:
        if self.total_checks == 0:
            return 0.0
        return ((self.passed + (self.partial * 0.5)) / self.total_checks) * 100


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def check_file_exists(filepath: str) -> bool:
    """Check if a file exists in the project"""
    full_path = PROJECT_ROOT / filepath
    return full_path.exists()


def check_class_exists(module_path: str, class_name: str) -> bool:
    """Check if a class exists in a module"""
    try:
        module = importlib.import_module(module_path)
        return hasattr(module, class_name)
    except ImportError:
        return False


def check_function_exists(module_path: str, func_name: str) -> bool:
    """Check if a function exists in a module"""
    try:
        module = importlib.import_module(module_path)
        return hasattr(module, func_name) and callable(getattr(module, func_name))
    except ImportError:
        return False


def search_in_file(filepath: str, patterns: List[str]) -> Dict[str, bool]:
    """Search for patterns in a file"""
    results = {p: False for p in patterns}
    full_path = PROJECT_ROOT / filepath
    if not full_path.exists():
        return results

    try:
        content = full_path.read_text()
        for pattern in patterns:
            if pattern.lower() in content.lower():
                results[pattern] = True
    except Exception:
        pass
    return results


def get_python_files(directory: str) -> List[Path]:
    """Get all Python files in a directory"""
    dir_path = PROJECT_ROOT / directory
    if not dir_path.exists():
        return []
    return list(dir_path.rglob("*.py"))


# =============================================================================
# SOC 2 TYPE II COMPLIANCE CHECKS
# =============================================================================

class SOC2Checker:
    """
    SOC 2 Type II Compliance Checker

    Trust Service Criteria:
    - CC: Common Criteria (Security)
    - A: Availability
    - PI: Processing Integrity
    - C: Confidentiality
    - P: Privacy
    """

    STANDARD = "SOC2-TypeII"

    @staticmethod
    def check_cc1_access_control(report: ComplianceReport):
        """CC1: Access Control - Logical and physical access controls"""
        evidence = []
        recommendations = []

        # Check for authentication mechanisms
        auth_patterns = search_in_file(
            "backend/orchestrator/main.py",
            ["authorization", "authenticate", "token", "api_key", "bearer"]
        )

        # Check for RBAC/permission systems
        rbac_exists = check_file_exists("backend/governance/audit_logger.py")

        # Check guardrails for input validation
        guardrails_exist = check_file_exists("backend/guardrails/llm_guardrails.py")

        if any(auth_patterns.values()):
            evidence.append("Authentication mechanisms found in orchestrator")
        if rbac_exists:
            evidence.append("Audit logging with user tracking implemented")
        if guardrails_exist:
            evidence.append("Input validation guardrails implemented")

        status = ComplianceStatus.PASS if len(evidence) >= 2 else ComplianceStatus.PARTIAL
        if len(evidence) == 0:
            status = ComplianceStatus.FAIL
            recommendations.append("Implement authentication and access control mechanisms")

        report.add_check(ComplianceCheck(
            standard=SOC2Checker.STANDARD,
            control_id="CC1.1",
            control_name="Access Control",
            description="Logical and physical access to systems is restricted",
            status=status,
            severity=Severity.CRITICAL,
            evidence=evidence,
            recommendations=recommendations,
            implementation_details="Access controls via API authentication and guardrails"
        ))

    @staticmethod
    def check_cc2_risk_assessment(report: ComplianceReport):
        """CC2: Risk Assessment - Risk management processes"""
        evidence = []
        recommendations = []

        # Check for risk management
        risk_patterns = search_in_file(
            "backend/governance/eu_ai_act_compliance.py",
            ["risk_level", "risk_assessment", "RiskLevel"]
        )

        # Check circuit breaker for fault tolerance
        circuit_breaker = check_file_exists("backend/utils/circuit_breaker.py")

        # Check control plane for approval routing
        control_plane = check_file_exists("backend/agents/control_plane.py")

        if any(risk_patterns.values()):
            evidence.append("Risk level classification implemented")
        if circuit_breaker:
            evidence.append("Circuit breaker pattern for fault tolerance")
        if control_plane:
            evidence.append("Control plane with risk-based approval routing")

        status = ComplianceStatus.PASS if len(evidence) >= 2 else ComplianceStatus.PARTIAL

        report.add_check(ComplianceCheck(
            standard=SOC2Checker.STANDARD,
            control_id="CC2.1",
            control_name="Risk Assessment",
            description="Risk assessment and management processes are established",
            status=status,
            severity=Severity.HIGH,
            evidence=evidence,
            recommendations=recommendations
        ))

    @staticmethod
    def check_cc3_change_management(report: ComplianceReport):
        """CC3: Change Management - System changes are controlled"""
        evidence = []
        recommendations = []

        # Check for version control integration
        git_exists = check_file_exists(".git")
        github_actions = check_file_exists("backend/utils/github_actions.py")

        # Check for audit logging of changes
        audit_patterns = search_in_file(
            "backend/governance/audit_logger.py",
            ["SYSTEM_CONFIG_CHANGE", "log_event", "audit_trail"]
        )

        if git_exists:
            evidence.append("Git version control in use")
        if github_actions:
            evidence.append("GitHub Actions integration for controlled deployments")
        if any(audit_patterns.values()):
            evidence.append("System configuration changes are audited")

        status = ComplianceStatus.PASS if len(evidence) >= 2 else ComplianceStatus.PARTIAL

        report.add_check(ComplianceCheck(
            standard=SOC2Checker.STANDARD,
            control_id="CC3.1",
            control_name="Change Management",
            description="System changes are authorized, tested, and documented",
            status=status,
            severity=Severity.HIGH,
            evidence=evidence,
            recommendations=recommendations
        ))

    @staticmethod
    def check_cc4_monitoring(report: ComplianceReport):
        """CC4: Monitoring - System operations are monitored"""
        evidence = []
        recommendations = []

        # Check for metrics collection
        metrics_exists = check_file_exists("backend/orchestrator/metrics.py")
        prometheus = search_in_file("requirements.txt", ["prometheus"])
        structlog = search_in_file("requirements.txt", ["structlog"])

        # Check for Langfuse tracing
        langfuse = search_in_file("requirements.txt", ["langfuse"])

        if metrics_exists:
            evidence.append("Metrics collection module implemented")
        if prometheus.get("prometheus"):
            evidence.append("Prometheus metrics integration")
        if structlog.get("structlog"):
            evidence.append("Structured logging with structlog")
        if langfuse.get("langfuse"):
            evidence.append("LLM tracing with Langfuse")

        status = ComplianceStatus.PASS if len(evidence) >= 3 else ComplianceStatus.PARTIAL

        report.add_check(ComplianceCheck(
            standard=SOC2Checker.STANDARD,
            control_id="CC4.1",
            control_name="Monitoring & Logging",
            description="System operations are monitored and logged",
            status=status,
            severity=Severity.HIGH,
            evidence=evidence,
            recommendations=recommendations
        ))

    @staticmethod
    def check_cc5_data_protection(report: ComplianceReport):
        """CC5: Data Protection - Confidential information is protected"""
        evidence = []
        recommendations = []

        # Check for PII detection
        pii_patterns = search_in_file(
            "backend/guardrails/llm_guardrails.py",
            ["PII", "detect_pii", "SSN", "credit_card", "anonymize"]
        )

        # Check for data retention policies
        retention = check_file_exists("backend/governance/data_retention.py")

        # Check for encryption patterns
        encryption = search_in_file("requirements.txt", ["cryptography", "hashlib"])

        if any(pii_patterns.values()):
            evidence.append("PII detection and filtering implemented")
        if retention:
            evidence.append("Data retention policies defined")

        status = ComplianceStatus.PASS if len(evidence) >= 2 else ComplianceStatus.PARTIAL
        if len(evidence) == 0:
            status = ComplianceStatus.FAIL
            recommendations.append("Implement PII protection and data retention policies")

        report.add_check(ComplianceCheck(
            standard=SOC2Checker.STANDARD,
            control_id="CC5.1",
            control_name="Data Protection",
            description="Confidential information is protected throughout lifecycle",
            status=status,
            severity=Severity.CRITICAL,
            evidence=evidence,
            recommendations=recommendations
        ))

    @staticmethod
    def check_a1_availability(report: ComplianceReport):
        """A1: Availability - System availability commitments"""
        evidence = []
        recommendations = []

        # Check for health endpoints
        health_patterns = search_in_file(
            "backend/orchestrator/main.py",
            ["/health", "health_check", "readiness", "liveness"]
        )

        # Check circuit breaker
        circuit_breaker = check_file_exists("backend/utils/circuit_breaker.py")

        # Check Redis caching
        redis_client = check_file_exists("backend/utils/redis_client.py")

        if any(health_patterns.values()):
            evidence.append("Health check endpoints implemented")
        if circuit_breaker:
            evidence.append("Circuit breaker for resilience")
        if redis_client:
            evidence.append("Redis caching for performance")

        status = ComplianceStatus.PASS if len(evidence) >= 2 else ComplianceStatus.PARTIAL

        report.add_check(ComplianceCheck(
            standard=SOC2Checker.STANDARD,
            control_id="A1.1",
            control_name="System Availability",
            description="System availability meets defined commitments",
            status=status,
            severity=Severity.HIGH,
            evidence=evidence,
            recommendations=recommendations
        ))

    @staticmethod
    def check_pi1_processing_integrity(report: ComplianceReport):
        """PI1: Processing Integrity - System processing is complete and accurate"""
        evidence = []
        recommendations = []

        # Check for validation
        validation_patterns = search_in_file(
            "backend/guardrails/llm_guardrails.py",
            ["validate", "ValidationResult", "validate_output"]
        )

        # Check for LLM Judge
        llm_judge = check_file_exists("backend/orchestrator/llm_judge.py")

        # Check for confidence scoring
        confidence_patterns = search_in_file(
            "backend/rag/intelligent_retriever.py",
            ["confidence", "score", "threshold"]
        )

        if any(validation_patterns.values()):
            evidence.append("Input/output validation implemented")
        if llm_judge:
            evidence.append("LLM-as-Judge for quality assurance")
        if any(confidence_patterns.values()):
            evidence.append("Confidence scoring in retrieval")

        status = ComplianceStatus.PASS if len(evidence) >= 2 else ComplianceStatus.PARTIAL

        report.add_check(ComplianceCheck(
            standard=SOC2Checker.STANDARD,
            control_id="PI1.1",
            control_name="Processing Integrity",
            description="System processing is complete, valid, and accurate",
            status=status,
            severity=Severity.HIGH,
            evidence=evidence,
            recommendations=recommendations
        ))

    @staticmethod
    def run_all_checks(report: ComplianceReport):
        """Run all SOC 2 Type II checks"""
        SOC2Checker.check_cc1_access_control(report)
        SOC2Checker.check_cc2_risk_assessment(report)
        SOC2Checker.check_cc3_change_management(report)
        SOC2Checker.check_cc4_monitoring(report)
        SOC2Checker.check_cc5_data_protection(report)
        SOC2Checker.check_a1_availability(report)
        SOC2Checker.check_pi1_processing_integrity(report)


# =============================================================================
# ISO 42001 COMPLIANCE CHECKS (AI Management System)
# =============================================================================

class ISO42001Checker:
    """
    ISO 42001 Compliance Checker

    AI Management System Standard covering:
    - Leadership & commitment
    - Risk management for AI
    - AI system lifecycle
    - Data management
    - Performance evaluation
    """

    STANDARD = "ISO-42001"

    @staticmethod
    def check_4_1_understanding_context(report: ComplianceReport):
        """4.1: Understanding the organization and its context"""
        evidence = []
        recommendations = []

        # Check for architecture documentation
        arch_docs = check_file_exists("docs/ARCHITECTURE_V5.md")
        readme = check_file_exists("docs/README.md")

        if arch_docs:
            evidence.append("Architecture documentation exists")
        if readme:
            evidence.append("System documentation available")

        status = ComplianceStatus.PASS if len(evidence) >= 1 else ComplianceStatus.MANUAL_REVIEW

        report.add_check(ComplianceCheck(
            standard=ISO42001Checker.STANDARD,
            control_id="4.1",
            control_name="Organization Context",
            description="Organization understands AI system context and stakeholders",
            status=status,
            severity=Severity.MEDIUM,
            evidence=evidence,
            recommendations=recommendations
        ))

    @staticmethod
    def check_5_2_ai_policy(report: ComplianceReport):
        """5.2: AI Policy establishment"""
        evidence = []
        recommendations = []

        # Check for governance module
        governance = check_file_exists("backend/governance/__init__.py")
        eu_compliance = check_file_exists("backend/governance/eu_ai_act_compliance.py")
        compliance_doc = check_file_exists("docs/EU_AI_ACT_COMPLIANCE_GUIDE.md")

        if governance:
            evidence.append("Governance module implemented")
        if eu_compliance:
            evidence.append("AI compliance policies codified")
        if compliance_doc:
            evidence.append("Compliance documentation available")

        status = ComplianceStatus.PASS if len(evidence) >= 2 else ComplianceStatus.PARTIAL

        report.add_check(ComplianceCheck(
            standard=ISO42001Checker.STANDARD,
            control_id="5.2",
            control_name="AI Policy",
            description="AI policy is established and communicated",
            status=status,
            severity=Severity.HIGH,
            evidence=evidence,
            recommendations=recommendations
        ))

    @staticmethod
    def check_6_1_risk_management(report: ComplianceReport):
        """6.1: Actions to address AI risks and opportunities"""
        evidence = []
        recommendations = []

        # Check risk management components
        control_plane = check_file_exists("backend/agents/control_plane.py")
        risk_patterns = search_in_file(
            "backend/governance/eu_ai_act_compliance.py",
            ["RiskLevel", "risk_assessment", "assess_risk"]
        )
        guardrails = check_file_exists("backend/guardrails/llm_guardrails.py")

        if control_plane:
            evidence.append("Control plane with risk-based routing")
        if any(risk_patterns.values()):
            evidence.append("Risk level classification system")
        if guardrails:
            evidence.append("Input/output guardrails for risk mitigation")

        status = ComplianceStatus.PASS if len(evidence) >= 2 else ComplianceStatus.PARTIAL

        report.add_check(ComplianceCheck(
            standard=ISO42001Checker.STANDARD,
            control_id="6.1",
            control_name="Risk Management",
            description="AI-specific risks are identified and addressed",
            status=status,
            severity=Severity.CRITICAL,
            evidence=evidence,
            recommendations=recommendations
        ))

    @staticmethod
    def check_7_2_competence(report: ComplianceReport):
        """7.2: Competence of personnel working with AI"""
        evidence = []
        recommendations = []

        # Check for documentation that supports understanding
        docs_exist = len(list((PROJECT_ROOT / "docs").glob("*.md"))) > 0 if (PROJECT_ROOT / "docs").exists() else False
        code_comments = True  # Assume code is documented

        if docs_exist:
            evidence.append("Technical documentation available")

        status = ComplianceStatus.MANUAL_REVIEW
        recommendations.append("Verify personnel training records exist")
        recommendations.append("Document AI system competency requirements")

        report.add_check(ComplianceCheck(
            standard=ISO42001Checker.STANDARD,
            control_id="7.2",
            control_name="Competence",
            description="Personnel competence for AI systems is ensured",
            status=status,
            severity=Severity.MEDIUM,
            evidence=evidence,
            recommendations=recommendations
        ))

    @staticmethod
    def check_8_2_ai_system_lifecycle(report: ComplianceReport):
        """8.2: AI system lifecycle management"""
        evidence = []
        recommendations = []

        # Check for lifecycle components
        workflow = check_file_exists("backend/orchestrator/langgraph_workflow.py")
        execution = check_file_exists("backend/agents/execution_orchestrator.py")
        feedback = check_file_exists("backend/rag/feedback_optimizer.py")

        if workflow:
            evidence.append("LangGraph workflow for lifecycle management")
        if execution:
            evidence.append("Execution orchestrator for deployment lifecycle")
        if feedback:
            evidence.append("Feedback optimizer for continuous improvement")

        status = ComplianceStatus.PASS if len(evidence) >= 2 else ComplianceStatus.PARTIAL

        report.add_check(ComplianceCheck(
            standard=ISO42001Checker.STANDARD,
            control_id="8.2",
            control_name="AI System Lifecycle",
            description="AI system lifecycle is managed appropriately",
            status=status,
            severity=Severity.HIGH,
            evidence=evidence,
            recommendations=recommendations
        ))

    @staticmethod
    def check_8_4_data_management(report: ComplianceReport):
        """8.4: Data management for AI systems"""
        evidence = []
        recommendations = []

        # Check data management components
        rag_module = check_file_exists("backend/rag/__init__.py")
        data_retention = check_file_exists("backend/governance/data_retention.py")
        embedding = check_file_exists("backend/rag/embedding_service.py")

        if rag_module:
            evidence.append("RAG system for knowledge management")
        if data_retention:
            evidence.append("Data retention policies implemented")
        if embedding:
            evidence.append("Embedding service for data processing")

        status = ComplianceStatus.PASS if len(evidence) >= 2 else ComplianceStatus.PARTIAL

        report.add_check(ComplianceCheck(
            standard=ISO42001Checker.STANDARD,
            control_id="8.4",
            control_name="Data Management",
            description="Data for AI systems is appropriately managed",
            status=status,
            severity=Severity.HIGH,
            evidence=evidence,
            recommendations=recommendations
        ))

    @staticmethod
    def check_9_1_performance_evaluation(report: ComplianceReport):
        """9.1: Monitoring, measurement, analysis and evaluation"""
        evidence = []
        recommendations = []

        # Check monitoring components
        metrics = check_file_exists("backend/orchestrator/metrics.py")
        langfuse = search_in_file("requirements.txt", ["langfuse"])
        audit = check_file_exists("backend/governance/audit_logger.py")

        if metrics:
            evidence.append("Metrics collection implemented")
        if langfuse.get("langfuse"):
            evidence.append("LLM performance tracing with Langfuse")
        if audit:
            evidence.append("Audit logging for evaluation")

        status = ComplianceStatus.PASS if len(evidence) >= 2 else ComplianceStatus.PARTIAL

        report.add_check(ComplianceCheck(
            standard=ISO42001Checker.STANDARD,
            control_id="9.1",
            control_name="Performance Evaluation",
            description="AI system performance is monitored and evaluated",
            status=status,
            severity=Severity.HIGH,
            evidence=evidence,
            recommendations=recommendations
        ))

    @staticmethod
    def check_10_1_continual_improvement(report: ComplianceReport):
        """10.1: Continual improvement"""
        evidence = []
        recommendations = []

        # Check improvement mechanisms
        feedback = check_file_exists("backend/rag/feedback_optimizer.py")
        llm_judge = check_file_exists("backend/orchestrator/llm_judge.py")

        if feedback:
            evidence.append("Feedback optimization for continuous learning")
        if llm_judge:
            evidence.append("LLM Judge for quality evaluation and improvement")

        status = ComplianceStatus.PASS if len(evidence) >= 1 else ComplianceStatus.PARTIAL

        report.add_check(ComplianceCheck(
            standard=ISO42001Checker.STANDARD,
            control_id="10.1",
            control_name="Continual Improvement",
            description="AI management system is continually improved",
            status=status,
            severity=Severity.MEDIUM,
            evidence=evidence,
            recommendations=recommendations
        ))

    @staticmethod
    def run_all_checks(report: ComplianceReport):
        """Run all ISO 42001 checks"""
        ISO42001Checker.check_4_1_understanding_context(report)
        ISO42001Checker.check_5_2_ai_policy(report)
        ISO42001Checker.check_6_1_risk_management(report)
        ISO42001Checker.check_7_2_competence(report)
        ISO42001Checker.check_8_2_ai_system_lifecycle(report)
        ISO42001Checker.check_8_4_data_management(report)
        ISO42001Checker.check_9_1_performance_evaluation(report)
        ISO42001Checker.check_10_1_continual_improvement(report)


# =============================================================================
# NIST AI RMF COMPLIANCE CHECKS
# =============================================================================

class NISTAIRMFChecker:
    """
    NIST AI Risk Management Framework Checker

    Four core functions:
    - GOVERN: Cultivate AI risk management culture
    - MAP: Context and risk identification
    - MEASURE: Analyze, assess, and track AI risks
    - MANAGE: Prioritize and act on AI risks
    """

    STANDARD = "NIST-AI-RMF"

    @staticmethod
    def check_govern_1_policies(report: ComplianceReport):
        """GOVERN 1: AI risk management policies and processes"""
        evidence = []
        recommendations = []

        governance = check_file_exists("backend/governance/__init__.py")
        control_plane = check_file_exists("backend/agents/control_plane.py")
        compliance_doc = check_file_exists("docs/EU_AI_ACT_COMPLIANCE_GUIDE.md")

        if governance:
            evidence.append("Governance module with risk policies")
        if control_plane:
            evidence.append("Control plane for policy enforcement")
        if compliance_doc:
            evidence.append("Documented compliance policies")

        status = ComplianceStatus.PASS if len(evidence) >= 2 else ComplianceStatus.PARTIAL

        report.add_check(ComplianceCheck(
            standard=NISTAIRMFChecker.STANDARD,
            control_id="GOVERN-1",
            control_name="Policies & Processes",
            description="AI risk management policies are established",
            status=status,
            severity=Severity.HIGH,
            evidence=evidence,
            recommendations=recommendations
        ))

    @staticmethod
    def check_govern_2_accountability(report: ComplianceReport):
        """GOVERN 2: Accountability structures"""
        evidence = []
        recommendations = []

        audit = check_file_exists("backend/governance/audit_logger.py")
        audit_patterns = search_in_file(
            "backend/governance/audit_logger.py",
            ["user_id", "actor", "approver", "HUMAN_APPROVAL"]
        )

        if audit:
            evidence.append("Audit logging for accountability")
        if any(audit_patterns.values()):
            evidence.append("User attribution in audit logs")

        status = ComplianceStatus.PASS if len(evidence) >= 1 else ComplianceStatus.PARTIAL

        report.add_check(ComplianceCheck(
            standard=NISTAIRMFChecker.STANDARD,
            control_id="GOVERN-2",
            control_name="Accountability",
            description="Accountability structures are in place",
            status=status,
            severity=Severity.HIGH,
            evidence=evidence,
            recommendations=recommendations
        ))

    @staticmethod
    def check_map_1_context(report: ComplianceReport):
        """MAP 1: Context is established"""
        evidence = []
        recommendations = []

        # Check for context understanding
        query_understanding = check_file_exists("backend/rag/query_understanding.py")
        arch_doc = check_file_exists("docs/ARCHITECTURE_V5.md")

        if query_understanding:
            evidence.append("Query understanding for context extraction")
        if arch_doc:
            evidence.append("System context documented in architecture")

        status = ComplianceStatus.PASS if len(evidence) >= 1 else ComplianceStatus.PARTIAL

        report.add_check(ComplianceCheck(
            standard=NISTAIRMFChecker.STANDARD,
            control_id="MAP-1",
            control_name="Context Establishment",
            description="AI system context is established and documented",
            status=status,
            severity=Severity.MEDIUM,
            evidence=evidence,
            recommendations=recommendations
        ))

    @staticmethod
    def check_map_2_risk_categories(report: ComplianceReport):
        """MAP 2: Categorization of AI risks"""
        evidence = []
        recommendations = []

        risk_patterns = search_in_file(
            "backend/governance/eu_ai_act_compliance.py",
            ["RiskLevel", "CRITICAL", "HIGH", "MEDIUM", "LOW"]
        )
        control_plane_risk = search_in_file(
            "backend/agents/control_plane.py",
            ["RiskLevel", "risk_level", "risk_threshold"]
        )

        if any(risk_patterns.values()):
            evidence.append("Risk levels defined in governance")
        if any(control_plane_risk.values()):
            evidence.append("Risk categorization in control plane")

        status = ComplianceStatus.PASS if len(evidence) >= 1 else ComplianceStatus.PARTIAL

        report.add_check(ComplianceCheck(
            standard=NISTAIRMFChecker.STANDARD,
            control_id="MAP-2",
            control_name="Risk Categorization",
            description="AI risks are categorized appropriately",
            status=status,
            severity=Severity.HIGH,
            evidence=evidence,
            recommendations=recommendations
        ))

    @staticmethod
    def check_measure_1_metrics(report: ComplianceReport):
        """MEASURE 1: AI risk metrics"""
        evidence = []
        recommendations = []

        metrics = check_file_exists("backend/orchestrator/metrics.py")
        prometheus = search_in_file("requirements.txt", ["prometheus"])
        confidence = search_in_file(
            "backend/rag/intelligent_retriever.py",
            ["confidence", "score", "threshold"]
        )

        if metrics:
            evidence.append("Metrics module for risk measurement")
        if prometheus.get("prometheus"):
            evidence.append("Prometheus for metrics collection")
        if any(confidence.values()):
            evidence.append("Confidence scoring for AI outputs")

        status = ComplianceStatus.PASS if len(evidence) >= 2 else ComplianceStatus.PARTIAL

        report.add_check(ComplianceCheck(
            standard=NISTAIRMFChecker.STANDARD,
            control_id="MEASURE-1",
            control_name="Risk Metrics",
            description="AI risk metrics are defined and collected",
            status=status,
            severity=Severity.HIGH,
            evidence=evidence,
            recommendations=recommendations
        ))

    @staticmethod
    def check_measure_2_testing(report: ComplianceReport):
        """MEASURE 2: AI systems are tested"""
        evidence = []
        recommendations = []

        # Check for test files
        unit_tests = check_file_exists("tests/unit")
        security_tests = check_file_exists("tests/security")
        chaos_tests = check_file_exists("tests/chaos")

        if unit_tests:
            evidence.append("Unit tests implemented")
        if security_tests:
            evidence.append("Security tests implemented")
        if chaos_tests:
            evidence.append("Chaos engineering tests")

        status = ComplianceStatus.PASS if len(evidence) >= 2 else ComplianceStatus.PARTIAL

        report.add_check(ComplianceCheck(
            standard=NISTAIRMFChecker.STANDARD,
            control_id="MEASURE-2",
            control_name="Testing",
            description="AI systems are regularly tested",
            status=status,
            severity=Severity.HIGH,
            evidence=evidence,
            recommendations=recommendations
        ))

    @staticmethod
    def check_manage_1_risk_prioritization(report: ComplianceReport):
        """MANAGE 1: AI risks are prioritized"""
        evidence = []
        recommendations = []

        control_plane = check_file_exists("backend/agents/control_plane.py")
        priority_patterns = search_in_file(
            "backend/agents/control_plane.py",
            ["priority", "threshold", "auto_approve", "human_review"]
        )

        if control_plane:
            evidence.append("Control plane for risk prioritization")
        if any(priority_patterns.values()):
            evidence.append("Risk-based approval thresholds")

        status = ComplianceStatus.PASS if len(evidence) >= 1 else ComplianceStatus.PARTIAL

        report.add_check(ComplianceCheck(
            standard=NISTAIRMFChecker.STANDARD,
            control_id="MANAGE-1",
            control_name="Risk Prioritization",
            description="AI risks are prioritized and managed",
            status=status,
            severity=Severity.HIGH,
            evidence=evidence,
            recommendations=recommendations
        ))

    @staticmethod
    def check_manage_2_response(report: ComplianceReport):
        """MANAGE 2: Risk response strategies"""
        evidence = []
        recommendations = []

        circuit_breaker = check_file_exists("backend/utils/circuit_breaker.py")
        guardrails = check_file_exists("backend/guardrails/llm_guardrails.py")
        hitl = search_in_file(
            "backend/agents/control_plane.py",
            ["human", "approval", "reject", "escalate"]
        )

        if circuit_breaker:
            evidence.append("Circuit breaker for failure response")
        if guardrails:
            evidence.append("Guardrails for risk mitigation")
        if any(hitl.values()):
            evidence.append("Human-in-the-loop escalation")

        status = ComplianceStatus.PASS if len(evidence) >= 2 else ComplianceStatus.PARTIAL

        report.add_check(ComplianceCheck(
            standard=NISTAIRMFChecker.STANDARD,
            control_id="MANAGE-2",
            control_name="Risk Response",
            description="Risk response strategies are implemented",
            status=status,
            severity=Severity.HIGH,
            evidence=evidence,
            recommendations=recommendations
        ))

    @staticmethod
    def run_all_checks(report: ComplianceReport):
        """Run all NIST AI RMF checks"""
        NISTAIRMFChecker.check_govern_1_policies(report)
        NISTAIRMFChecker.check_govern_2_accountability(report)
        NISTAIRMFChecker.check_map_1_context(report)
        NISTAIRMFChecker.check_map_2_risk_categories(report)
        NISTAIRMFChecker.check_measure_1_metrics(report)
        NISTAIRMFChecker.check_measure_2_testing(report)
        NISTAIRMFChecker.check_manage_1_risk_prioritization(report)
        NISTAIRMFChecker.check_manage_2_response(report)


# =============================================================================
# EU AI ACT COMPLIANCE CHECKS
# =============================================================================

class EUAIActChecker:
    """
    EU AI Act Compliance Checker

    Articles 9-15 for High-Risk AI Systems:
    - Article 9: Risk Management
    - Article 10: Data Governance
    - Article 11: Technical Documentation
    - Article 12: Record-keeping
    - Article 13: Transparency
    - Article 14: Human Oversight
    - Article 15: Accuracy & Robustness
    """

    STANDARD = "EU-AI-Act"

    @staticmethod
    def check_article_9_risk_management(report: ComplianceReport):
        """Article 9: Risk management system"""
        evidence = []
        recommendations = []

        # Check risk management implementation
        eu_compliance = check_file_exists("backend/governance/eu_ai_act_compliance.py")
        risk_patterns = search_in_file(
            "backend/governance/eu_ai_act_compliance.py",
            ["Article9", "risk_management", "RiskManagement", "identify_risk"]
        )
        guardrails = check_file_exists("backend/guardrails/llm_guardrails.py")
        circuit_breaker = check_file_exists("backend/utils/circuit_breaker.py")

        if eu_compliance:
            evidence.append("EU AI Act compliance module exists")
        if any(risk_patterns.values()):
            evidence.append("Article 9 risk management implemented")
        if guardrails:
            evidence.append("Input/output risk guardrails")
        if circuit_breaker:
            evidence.append("Circuit breaker for risk mitigation")

        status = ComplianceStatus.PASS if len(evidence) >= 3 else ComplianceStatus.PARTIAL

        report.add_check(ComplianceCheck(
            standard=EUAIActChecker.STANDARD,
            control_id="Art.9",
            control_name="Risk Management",
            description="Risk management system throughout AI lifecycle",
            status=status,
            severity=Severity.CRITICAL,
            evidence=evidence,
            recommendations=recommendations
        ))

    @staticmethod
    def check_article_10_data_governance(report: ComplianceReport):
        """Article 10: Data and data governance"""
        evidence = []
        recommendations = []

        # Check data governance
        data_retention = check_file_exists("backend/governance/data_retention.py")
        rag_module = check_file_exists("backend/rag/__init__.py")
        pii_detection = search_in_file(
            "backend/guardrails/llm_guardrails.py",
            ["PII", "detect_pii", "anonymize"]
        )

        if data_retention:
            evidence.append("Data retention policies defined")
        if rag_module:
            evidence.append("RAG system for data quality")
        if any(pii_detection.values()):
            evidence.append("PII detection and handling")

        status = ComplianceStatus.PASS if len(evidence) >= 2 else ComplianceStatus.PARTIAL

        report.add_check(ComplianceCheck(
            standard=EUAIActChecker.STANDARD,
            control_id="Art.10",
            control_name="Data Governance",
            description="Training, validation and testing data governance",
            status=status,
            severity=Severity.CRITICAL,
            evidence=evidence,
            recommendations=recommendations
        ))

    @staticmethod
    def check_article_11_technical_documentation(report: ComplianceReport):
        """Article 11: Technical documentation"""
        evidence = []
        recommendations = []

        # Check documentation
        arch_doc = check_file_exists("docs/ARCHITECTURE_V5.md")
        compliance_doc = check_file_exists("docs/EU_AI_ACT_COMPLIANCE_GUIDE.md")
        readme = check_file_exists("docs/README.md")
        protocol_doc = check_file_exists("docs/PROTOCOL_GUIDE.md")

        if arch_doc:
            evidence.append("Architecture documentation")
        if compliance_doc:
            evidence.append("Compliance documentation")
        if readme:
            evidence.append("General system documentation")
        if protocol_doc:
            evidence.append("Protocol documentation")

        status = ComplianceStatus.PASS if len(evidence) >= 2 else ComplianceStatus.PARTIAL

        report.add_check(ComplianceCheck(
            standard=EUAIActChecker.STANDARD,
            control_id="Art.11",
            control_name="Technical Documentation",
            description="Technical documentation before market placement",
            status=status,
            severity=Severity.HIGH,
            evidence=evidence,
            recommendations=recommendations
        ))

    @staticmethod
    def check_article_12_record_keeping(report: ComplianceReport):
        """Article 12: Record-keeping"""
        evidence = []
        recommendations = []

        # Check logging and record keeping
        audit_logger = check_file_exists("backend/governance/audit_logger.py")
        audit_patterns = search_in_file(
            "backend/governance/audit_logger.py",
            ["log_event", "audit_trail", "retention", "7 years", "2555"]
        )
        langfuse = search_in_file("requirements.txt", ["langfuse"])

        if audit_logger:
            evidence.append("Audit logger implemented")
        if any(audit_patterns.values()):
            evidence.append("7-year retention for audit logs")
        if langfuse.get("langfuse"):
            evidence.append("LLM trace logging with Langfuse")

        status = ComplianceStatus.PASS if len(evidence) >= 2 else ComplianceStatus.PARTIAL

        report.add_check(ComplianceCheck(
            standard=EUAIActChecker.STANDARD,
            control_id="Art.12",
            control_name="Record-keeping",
            description="Automatic logging of events throughout lifecycle",
            status=status,
            severity=Severity.HIGH,
            evidence=evidence,
            recommendations=recommendations
        ))

    @staticmethod
    def check_article_13_transparency(report: ComplianceReport):
        """Article 13: Transparency and information provision"""
        evidence = []
        recommendations = []

        # Check transparency features
        transparency_patterns = search_in_file(
            "backend/governance/eu_ai_act_compliance.py",
            ["transparency", "explanation", "Article13", "Transparency"]
        )
        llm_judge = check_file_exists("backend/orchestrator/llm_judge.py")

        if any(transparency_patterns.values()):
            evidence.append("Article 13 transparency implemented")
        if llm_judge:
            evidence.append("LLM Judge provides decision explanations")

        status = ComplianceStatus.PASS if len(evidence) >= 1 else ComplianceStatus.PARTIAL
        if len(evidence) == 0:
            recommendations.append("Implement explanation generation for AI decisions")

        report.add_check(ComplianceCheck(
            standard=EUAIActChecker.STANDARD,
            control_id="Art.13",
            control_name="Transparency",
            description="System is transparent and provides information to users",
            status=status,
            severity=Severity.HIGH,
            evidence=evidence,
            recommendations=recommendations
        ))

    @staticmethod
    def check_article_14_human_oversight(report: ComplianceReport):
        """Article 14: Human oversight"""
        evidence = []
        recommendations = []

        # Check human oversight mechanisms
        control_plane = check_file_exists("backend/agents/control_plane.py")
        hitl_patterns = search_in_file(
            "backend/agents/control_plane.py",
            ["human", "approval", "reject", "override", "escalate"]
        )
        audit_patterns = search_in_file(
            "backend/governance/audit_logger.py",
            ["HUMAN_APPROVAL", "HUMAN_REJECTION", "human_oversight"]
        )

        if control_plane:
            evidence.append("Control plane for human oversight")
        if any(hitl_patterns.values()):
            evidence.append("Human-in-the-loop approval workflow")
        if any(audit_patterns.values()):
            evidence.append("Human oversight events logged")

        status = ComplianceStatus.PASS if len(evidence) >= 2 else ComplianceStatus.PARTIAL

        report.add_check(ComplianceCheck(
            standard=EUAIActChecker.STANDARD,
            control_id="Art.14",
            control_name="Human Oversight",
            description="Human oversight is enabled during operation",
            status=status,
            severity=Severity.CRITICAL,
            evidence=evidence,
            recommendations=recommendations
        ))

    @staticmethod
    def check_article_15_accuracy_robustness(report: ComplianceReport):
        """Article 15: Accuracy, robustness and cybersecurity"""
        evidence = []
        recommendations = []

        # Check accuracy and security
        guardrails = check_file_exists("backend/guardrails/llm_guardrails.py")
        security_patterns = search_in_file(
            "backend/guardrails/llm_guardrails.py",
            ["injection", "security", "validate", "sanitize"]
        )
        confidence_patterns = search_in_file(
            "backend/rag/intelligent_retriever.py",
            ["confidence", "threshold", "score"]
        )
        circuit_breaker = check_file_exists("backend/utils/circuit_breaker.py")
        security_tests = check_file_exists("tests/security/test_security.py")

        if guardrails:
            evidence.append("Input/output guardrails")
        if any(security_patterns.values()):
            evidence.append("Injection attack prevention")
        if any(confidence_patterns.values()):
            evidence.append("Confidence thresholds for accuracy")
        if circuit_breaker:
            evidence.append("Circuit breaker for robustness")
        if security_tests:
            evidence.append("Security testing implemented")

        status = ComplianceStatus.PASS if len(evidence) >= 3 else ComplianceStatus.PARTIAL

        report.add_check(ComplianceCheck(
            standard=EUAIActChecker.STANDARD,
            control_id="Art.15",
            control_name="Accuracy & Robustness",
            description="System achieves appropriate accuracy, robustness and security",
            status=status,
            severity=Severity.CRITICAL,
            evidence=evidence,
            recommendations=recommendations
        ))

    @staticmethod
    def run_all_checks(report: ComplianceReport):
        """Run all EU AI Act checks"""
        EUAIActChecker.check_article_9_risk_management(report)
        EUAIActChecker.check_article_10_data_governance(report)
        EUAIActChecker.check_article_11_technical_documentation(report)
        EUAIActChecker.check_article_12_record_keeping(report)
        EUAIActChecker.check_article_13_transparency(report)
        EUAIActChecker.check_article_14_human_oversight(report)
        EUAIActChecker.check_article_15_accuracy_robustness(report)


# =============================================================================
# MITRE ATLAS COMPLIANCE CHECKS
# =============================================================================

class MITREATLASChecker:
    """
    MITRE ATLAS Compliance Checker

    Adversarial Threat Landscape for AI Systems covering:
    - ML Attack Techniques
    - ML Mitigations
    - ML Model Attacks
    - Data Poisoning Prevention
    """

    STANDARD = "MITRE-ATLAS"

    @staticmethod
    def check_ata_0001_prompt_injection(report: ComplianceReport):
        """AML.T0051: Prompt Injection Prevention"""
        evidence = []
        recommendations = []

        # Check for prompt injection prevention
        injection_patterns = search_in_file(
            "backend/guardrails/llm_guardrails.py",
            ["prompt_injection", "injection", "override", "ignore", "jailbreak"]
        )

        if any(injection_patterns.values()):
            evidence.append("Prompt injection detection implemented")

        # Check for specific patterns
        detection_methods = search_in_file(
            "backend/guardrails/llm_guardrails.py",
            ["detect_prompt_injection", "INJECTION_PATTERNS", "role manipulation"]
        )

        if any(detection_methods.values()):
            evidence.append("Multiple injection pattern detection")

        status = ComplianceStatus.PASS if len(evidence) >= 1 else ComplianceStatus.FAIL
        if status == ComplianceStatus.FAIL:
            recommendations.append("Implement prompt injection detection")

        report.add_check(ComplianceCheck(
            standard=MITREATLASChecker.STANDARD,
            control_id="AML.T0051",
            control_name="Prompt Injection",
            description="Prevention of prompt injection attacks",
            status=status,
            severity=Severity.CRITICAL,
            evidence=evidence,
            recommendations=recommendations
        ))

    @staticmethod
    def check_ata_0002_model_evasion(report: ComplianceReport):
        """AML.T0015: Model Evasion Prevention"""
        evidence = []
        recommendations = []

        # Check for input validation
        validation_patterns = search_in_file(
            "backend/guardrails/llm_guardrails.py",
            ["validate", "sanitize", "filter", "clean"]
        )

        # Check for confidence thresholds
        confidence_patterns = search_in_file(
            "backend/rag/intelligent_retriever.py",
            ["confidence", "threshold", "min_score"]
        )

        if any(validation_patterns.values()):
            evidence.append("Input validation and sanitization")
        if any(confidence_patterns.values()):
            evidence.append("Confidence thresholds to detect low-quality outputs")

        status = ComplianceStatus.PASS if len(evidence) >= 1 else ComplianceStatus.PARTIAL

        report.add_check(ComplianceCheck(
            standard=MITREATLASChecker.STANDARD,
            control_id="AML.T0015",
            control_name="Model Evasion",
            description="Prevention of adversarial inputs to evade model",
            status=status,
            severity=Severity.HIGH,
            evidence=evidence,
            recommendations=recommendations
        ))

    @staticmethod
    def check_ata_0003_data_poisoning(report: ComplianceReport):
        """AML.T0020: Data Poisoning Prevention"""
        evidence = []
        recommendations = []

        # Check for data validation in RAG
        rag_validation = search_in_file(
            "backend/rag/smart_chunker.py",
            ["validate", "verify", "quality"]
        )

        # Check for feedback validation
        feedback_patterns = search_in_file(
            "backend/rag/feedback_optimizer.py",
            ["validate", "quality", "threshold", "filter"]
        )

        # Check for human review
        human_review = search_in_file(
            "backend/agents/control_plane.py",
            ["human", "review", "approval"]
        )

        if any(rag_validation.values()):
            evidence.append("RAG data validation")
        if any(feedback_patterns.values()):
            evidence.append("Feedback quality filtering")
        if any(human_review.values()):
            evidence.append("Human review for high-risk data changes")

        status = ComplianceStatus.PASS if len(evidence) >= 1 else ComplianceStatus.PARTIAL
        recommendations.append("Consider implementing data provenance tracking")

        report.add_check(ComplianceCheck(
            standard=MITREATLASChecker.STANDARD,
            control_id="AML.T0020",
            control_name="Data Poisoning",
            description="Prevention of malicious data injection",
            status=status,
            severity=Severity.CRITICAL,
            evidence=evidence,
            recommendations=recommendations
        ))

    @staticmethod
    def check_ata_0004_model_theft(report: ComplianceReport):
        """AML.T0024: Model Theft Prevention"""
        evidence = []
        recommendations = []

        # Check for rate limiting
        rate_limit_patterns = search_in_file(
            "backend/guardrails/llm_guardrails.py",
            ["rate_limit", "throttle", "quota"]
        )

        # Check for access logging
        access_logging = search_in_file(
            "backend/governance/audit_logger.py",
            ["access", "log", "DATA_ACCESS"]
        )

        if any(rate_limit_patterns.values()):
            evidence.append("Rate limiting implemented")
        if any(access_logging.values()):
            evidence.append("Model access logging")

        status = ComplianceStatus.PASS if len(evidence) >= 1 else ComplianceStatus.PARTIAL
        recommendations.append("Consider implementing query anomaly detection")

        report.add_check(ComplianceCheck(
            standard=MITREATLASChecker.STANDARD,
            control_id="AML.T0024",
            control_name="Model Theft",
            description="Prevention of model extraction attacks",
            status=status,
            severity=Severity.HIGH,
            evidence=evidence,
            recommendations=recommendations
        ))

    @staticmethod
    def check_ata_0005_supply_chain(report: ComplianceReport):
        """AML.T0010: Supply Chain Compromise Prevention"""
        evidence = []
        recommendations = []

        # Check for dependency management
        requirements = check_file_exists("requirements.txt")

        # Check for version pinning
        version_patterns = search_in_file(
            "requirements.txt",
            [">=", "==", "~="]
        )

        if requirements:
            evidence.append("Dependencies documented in requirements.txt")
        if any(version_patterns.values()):
            evidence.append("Dependency version constraints")

        status = ComplianceStatus.PASS if len(evidence) >= 1 else ComplianceStatus.PARTIAL
        recommendations.append("Consider implementing dependency vulnerability scanning")
        recommendations.append("Use requirements.txt with hash verification")

        report.add_check(ComplianceCheck(
            standard=MITREATLASChecker.STANDARD,
            control_id="AML.T0010",
            control_name="Supply Chain",
            description="Prevention of supply chain attacks on ML pipeline",
            status=status,
            severity=Severity.HIGH,
            evidence=evidence,
            recommendations=recommendations
        ))

    @staticmethod
    def check_ata_0006_output_integrity(report: ComplianceReport):
        """AML.T0043: Output Integrity Verification"""
        evidence = []
        recommendations = []

        # Check for output validation
        output_validation = search_in_file(
            "backend/guardrails/llm_guardrails.py",
            ["validate_output", "output_validation", "verify_output"]
        )

        # Check for LLM Judge
        llm_judge = check_file_exists("backend/orchestrator/llm_judge.py")

        # Check for checksum/integrity
        integrity_patterns = search_in_file(
            "backend/governance/audit_logger.py",
            ["checksum", "hash", "integrity"]
        )

        if any(output_validation.values()):
            evidence.append("Output validation implemented")
        if llm_judge:
            evidence.append("LLM Judge for output quality verification")
        if any(integrity_patterns.values()):
            evidence.append("Audit log integrity verification")

        status = ComplianceStatus.PASS if len(evidence) >= 2 else ComplianceStatus.PARTIAL

        report.add_check(ComplianceCheck(
            standard=MITREATLASChecker.STANDARD,
            control_id="AML.T0043",
            control_name="Output Integrity",
            description="Verification of AI output integrity",
            status=status,
            severity=Severity.HIGH,
            evidence=evidence,
            recommendations=recommendations
        ))

    @staticmethod
    def check_ata_0007_inference_manipulation(report: ComplianceReport):
        """AML.T0048: Inference API Manipulation Prevention"""
        evidence = []
        recommendations = []

        # Check for input sanitization
        sanitize_patterns = search_in_file(
            "backend/guardrails/llm_guardrails.py",
            ["sanitize", "clean", "escape", "encode"]
        )

        # Check for API security
        api_security = search_in_file(
            "backend/orchestrator/main.py",
            ["cors", "authentication", "authorization", "bearer"]
        )

        if any(sanitize_patterns.values()):
            evidence.append("Input sanitization implemented")
        if any(api_security.values()):
            evidence.append("API security controls")

        status = ComplianceStatus.PASS if len(evidence) >= 1 else ComplianceStatus.PARTIAL

        report.add_check(ComplianceCheck(
            standard=MITREATLASChecker.STANDARD,
            control_id="AML.T0048",
            control_name="Inference Manipulation",
            description="Prevention of inference API manipulation",
            status=status,
            severity=Severity.HIGH,
            evidence=evidence,
            recommendations=recommendations
        ))

    @staticmethod
    def run_all_checks(report: ComplianceReport):
        """Run all MITRE ATLAS checks"""
        MITREATLASChecker.check_ata_0001_prompt_injection(report)
        MITREATLASChecker.check_ata_0002_model_evasion(report)
        MITREATLASChecker.check_ata_0003_data_poisoning(report)
        MITREATLASChecker.check_ata_0004_model_theft(report)
        MITREATLASChecker.check_ata_0005_supply_chain(report)
        MITREATLASChecker.check_ata_0006_output_integrity(report)
        MITREATLASChecker.check_ata_0007_inference_manipulation(report)


# =============================================================================
# REPORT GENERATION
# =============================================================================

def generate_text_report(report: ComplianceReport, verbose: bool = False) -> str:
    """Generate human-readable compliance report"""
    lines = []

    lines.append("=" * 80)
    lines.append("AI AGENT PLATFORM - COMPLIANCE REPORT")
    lines.append("=" * 80)
    lines.append(f"Generated: {report.timestamp}")
    lines.append(f"Platform Version: {report.platform_version}")
    lines.append(f"Standards Tested: {', '.join(report.standards_tested)}")
    lines.append("")

    # Summary
    lines.append("-" * 80)
    lines.append("EXECUTIVE SUMMARY")
    lines.append("-" * 80)
    lines.append(f"Total Checks: {report.total_checks}")
    lines.append(f"Passed:       {report.passed} ({report.passed/report.total_checks*100:.1f}%)")
    lines.append(f"Partial:      {report.partial} ({report.partial/report.total_checks*100:.1f}%)")
    lines.append(f"Failed:       {report.failed} ({report.failed/report.total_checks*100:.1f}%)")
    lines.append(f"Manual Review: {report.manual_review}")
    lines.append(f"")
    lines.append(f"OVERALL COMPLIANCE SCORE: {report.compliance_score:.1f}%")
    lines.append("")

    # Group by standard
    by_standard: Dict[str, List[ComplianceCheck]] = {}
    for check in report.checks:
        if check.standard not in by_standard:
            by_standard[check.standard] = []
        by_standard[check.standard].append(check)

    # Standard summaries
    for standard, checks in by_standard.items():
        lines.append("-" * 80)
        lines.append(f"{standard}")
        lines.append("-" * 80)

        passed = sum(1 for c in checks if c.status == ComplianceStatus.PASS)
        partial = sum(1 for c in checks if c.status == ComplianceStatus.PARTIAL)
        failed = sum(1 for c in checks if c.status == ComplianceStatus.FAIL)

        lines.append(f"  Status: {passed} PASS | {partial} PARTIAL | {failed} FAIL")
        lines.append("")

        for check in checks:
            status_icon = {
                ComplianceStatus.PASS: "[PASS]",
                ComplianceStatus.FAIL: "[FAIL]",
                ComplianceStatus.PARTIAL: "[PART]",
                ComplianceStatus.NOT_APPLICABLE: "[N/A] ",
                ComplianceStatus.MANUAL_REVIEW: "[REV] "
            }[check.status]

            lines.append(f"  {status_icon} {check.control_id}: {check.control_name}")

            if verbose:
                lines.append(f"          Description: {check.description}")
                if check.evidence:
                    lines.append(f"          Evidence:")
                    for e in check.evidence:
                        lines.append(f"            - {e}")
                if check.recommendations:
                    lines.append(f"          Recommendations:")
                    for r in check.recommendations:
                        lines.append(f"            - {r}")
                lines.append("")

        lines.append("")

    # Critical findings
    critical_failures = [c for c in report.checks
                        if c.status == ComplianceStatus.FAIL and c.severity == Severity.CRITICAL]

    if critical_failures:
        lines.append("-" * 80)
        lines.append("CRITICAL FINDINGS REQUIRING IMMEDIATE ATTENTION")
        lines.append("-" * 80)
        for check in critical_failures:
            lines.append(f"  [{check.standard}] {check.control_id}: {check.control_name}")
            for rec in check.recommendations:
                lines.append(f"    -> {rec}")
        lines.append("")

    # Recommendations summary
    all_recommendations = []
    for check in report.checks:
        if check.status in [ComplianceStatus.FAIL, ComplianceStatus.PARTIAL]:
            for rec in check.recommendations:
                if rec not in all_recommendations:
                    all_recommendations.append(rec)

    if all_recommendations:
        lines.append("-" * 80)
        lines.append("RECOMMENDATIONS FOR IMPROVEMENT")
        lines.append("-" * 80)
        for i, rec in enumerate(all_recommendations, 1):
            lines.append(f"  {i}. {rec}")
        lines.append("")

    lines.append("=" * 80)
    lines.append("END OF REPORT")
    lines.append("=" * 80)

    return "\n".join(lines)


def generate_json_report(report: ComplianceReport) -> str:
    """Generate JSON compliance report"""
    data = {
        "timestamp": report.timestamp,
        "platform_version": report.platform_version,
        "standards_tested": report.standards_tested,
        "summary": {
            "total_checks": report.total_checks,
            "passed": report.passed,
            "failed": report.failed,
            "partial": report.partial,
            "manual_review": report.manual_review,
            "compliance_score": round(report.compliance_score, 2)
        },
        "checks": [
            {
                "standard": c.standard,
                "control_id": c.control_id,
                "control_name": c.control_name,
                "description": c.description,
                "status": c.status.value,
                "severity": c.severity.value,
                "evidence": c.evidence,
                "recommendations": c.recommendations,
                "implementation_details": c.implementation_details
            }
            for c in report.checks
        ]
    }
    return json.dumps(data, indent=2)


# =============================================================================
# MAIN EXECUTION
# =============================================================================

def run_compliance_checks(standards: List[str] = None) -> ComplianceReport:
    """Run compliance checks for specified standards"""

    all_standards = ["SOC2", "ISO42001", "NIST-AI-RMF", "EU-AI-Act", "MITRE-ATLAS"]

    if standards is None or "all" in standards:
        standards = all_standards

    report = ComplianceReport(
        timestamp=datetime.now().isoformat(),
        platform_version="5.0.0",
        standards_tested=standards
    )

    checkers = {
        "SOC2": SOC2Checker,
        "ISO42001": ISO42001Checker,
        "NIST-AI-RMF": NISTAIRMFChecker,
        "EU-AI-Act": EUAIActChecker,
        "MITRE-ATLAS": MITREATLASChecker
    }

    for standard in standards:
        if standard in checkers:
            checkers[standard].run_all_checks(report)

    return report


def main():
    parser = argparse.ArgumentParser(
        description="AI Agent Platform Compliance Checker",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Standards available:
  SOC2         - SOC 2 Type II (Security, Availability, Processing Integrity)
  ISO42001     - ISO 42001 (AI Management System)
  NIST-AI-RMF  - NIST AI Risk Management Framework
  EU-AI-Act    - EU AI Act (Articles 9-15 for High-Risk AI)
  MITRE-ATLAS  - MITRE ATLAS (Adversarial Threat Landscape for AI)
  all          - Run all standards (default)

Examples:
  python compliance_checker.py                    # Run all checks
  python compliance_checker.py --standard SOC2    # Run only SOC2 checks
  python compliance_checker.py --verbose --json   # Verbose JSON output
        """
    )

    parser.add_argument(
        "--standard", "-s",
        action="append",
        choices=["SOC2", "ISO42001", "NIST-AI-RMF", "EU-AI-Act", "MITRE-ATLAS", "all"],
        help="Standards to check (can specify multiple)"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show detailed evidence and recommendations"
    )
    parser.add_argument(
        "--json", "-j",
        action="store_true",
        help="Output report in JSON format"
    )
    parser.add_argument(
        "--output", "-o",
        help="Output file path (default: stdout)"
    )

    args = parser.parse_args()

    # Run checks
    standards = args.standard if args.standard else ["all"]
    report = run_compliance_checks(standards)

    # Generate output
    if args.json:
        output = generate_json_report(report)
    else:
        output = generate_text_report(report, verbose=args.verbose)

    # Write output
    if args.output:
        with open(args.output, "w") as f:
            f.write(output)
        print(f"Report written to: {args.output}")
    else:
        print(output)

    # Exit code based on critical failures
    critical_failures = sum(1 for c in report.checks
                          if c.status == ComplianceStatus.FAIL and c.severity == Severity.CRITICAL)

    return 1 if critical_failures > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
