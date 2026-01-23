"""
Comprehensive Project Validator
Scans ALL Python files and validates against EU AI Act governance requirements
"""
import os
import re
import ast
from pathlib import Path
from typing import Dict, List, Set, Tuple
from dataclasses import dataclass, field
from collections import defaultdict

@dataclass
class ValidationResult:
    article: str
    requirement: str
    status: str  # PASS, FAIL, PARTIAL
    score: float  # 0.0 - 1.0
    evidence: List[str] = field(default_factory=list)
    missing: List[str] = field(default_factory=list)
    recommendation: str = ""

@dataclass
class CodeScanResult:
    total_files: int = 0
    files_scanned: List[str] = field(default_factory=list)
    imports_found: Dict[str, List[str]] = field(default_factory=dict)
    function_calls: Dict[str, List[str]] = field(default_factory=dict)
    patterns_found: Dict[str, List[Tuple[str, int]]] = field(default_factory=dict)

class ComprehensiveProjectValidator:
    """Scans ALL Python files and validates governance compliance"""

    def __init__(self, project_root: str = "/home/samrattidke600/ai_agent_app"):
        self.root = Path(project_root)
        self.scan_result = CodeScanResult()
        self.results: List[ValidationResult] = []

        # Directories to skip
        self.skip_dirs = {'.git', '__pycache__', 'node_modules', '.venv', 'venv', 'env'}

        # Governance patterns to search for
        self.governance_patterns = {
            "audit_logging": [
                r"audit_logger\.log",
                r"audit_logger\.log_ai_decision",
                r"audit_logger\.log_human_oversight",
                r"AuditEventType\.",
            ],
            "human_oversight": [
                r"hitl",
                r"human.*oversight",
                r"approval.*workflow",
                r"human.?in.?the.?loop",
                r"requires_approval",
                r"pending.*approval",
            ],
            "confidence_threshold": [
                r"confidence.*threshold",
                r"confidence.*score",
                r"confidence.*[<>=].*0\.\d",
                r"if.*confidence",
            ],
            "input_validation": [
                r"validate_input",
                r"validate_incident",
                r"InputValidator",
                r"guardrails\.validate",
                r"sanitize",
            ],
            "output_validation": [
                r"validate_output",
                r"OutputValidator",
                r"pii.*detect",
                r"secrets.*detect",
            ],
            "error_handling": [
                r"try\s*:",
                r"except\s+\w+",
                r"circuit_breaker",
                r"CircuitBreaker",
                r"retry",
                r"fallback",
            ],
            "explainability": [
                r"explanation",
                r"reasoning",
                r"root_cause",
                r"ai_decision_explanation",
            ],
            "logging": [
                r"logger\.(info|warning|error|debug)",
                r"structlog",
                r"langfuse",
                r"_track_llm_call",
            ],
            "risk_assessment": [
                r"risk_level",
                r"RiskLevel\.",
                r"safety_score",
                r"risk_assessment",
            ],
        }

    def scan_all_python_files(self) -> CodeScanResult:
        """Scan ALL Python files in the project"""
        python_files = []

        for root, dirs, files in os.walk(self.root):
            # Skip unwanted directories
            dirs[:] = [d for d in dirs if d not in self.skip_dirs]

            for file in files:
                if file.endswith('.py'):
                    rel_path = os.path.relpath(os.path.join(root, file), self.root)
                    python_files.append(rel_path)

        self.scan_result.total_files = len(python_files)
        self.scan_result.files_scanned = python_files

        # Scan each file
        for file_path in python_files:
            self._scan_file(file_path)

        return self.scan_result

    def _scan_file(self, file_path: str):
        """Scan a single Python file for governance patterns"""
        full_path = self.root / file_path
        try:
            content = full_path.read_text(errors='ignore')

            # Check for imports
            self._extract_imports(content, file_path)

            # Check for governance patterns
            for pattern_name, patterns in self.governance_patterns.items():
                for pattern in patterns:
                    matches = list(re.finditer(pattern, content, re.IGNORECASE))
                    if matches:
                        if pattern_name not in self.scan_result.patterns_found:
                            self.scan_result.patterns_found[pattern_name] = []
                        for match in matches:
                            line_num = content[:match.start()].count('\n') + 1
                            self.scan_result.patterns_found[pattern_name].append((file_path, line_num))
                        break  # Found at least one pattern in this category

        except Exception as e:
            pass  # Skip files that can't be read

    def _extract_imports(self, content: str, file_path: str):
        """Extract import statements"""
        import_patterns = [
            r"from\s+([\w.]+)\s+import",
            r"import\s+([\w.]+)",
        ]

        for pattern in import_patterns:
            matches = re.findall(pattern, content)
            for match in matches:
                if match not in self.scan_result.imports_found:
                    self.scan_result.imports_found[match] = []
                self.scan_result.imports_found[match].append(file_path)

    def validate_all(self) -> Dict:
        """Run comprehensive validation"""
        # First scan all files
        self.scan_all_python_files()

        # Run all validations
        checks = [
            self._check_article_9_risk_management(),
            self._check_article_10_data_governance(),
            self._check_article_11_documentation(),
            self._check_article_12_record_keeping(),
            self._check_article_13_transparency(),
            self._check_article_14_human_oversight(),
            self._check_article_15_accuracy_security(),
            self._check_audit_integration(),
            self._check_critical_paths(),
        ]

        total_score = sum(c.score for c in checks) / len(checks)
        passed = sum(1 for c in checks if c.status == "PASS")
        partial = sum(1 for c in checks if c.status == "PARTIAL")

        return {
            "scan_summary": {
                "total_python_files": self.scan_result.total_files,
                "files_with_governance": len(set(
                    f for patterns in self.scan_result.patterns_found.values()
                    for f, _ in patterns
                )),
            },
            "compliance_score": f"{total_score*100:.1f}%",
            "passed": passed,
            "partial": partial,
            "failed": len(checks) - passed - partial,
            "results": [
                {
                    "article": c.article,
                    "requirement": c.requirement,
                    "status": c.status,
                    "score": f"{c.score*100:.0f}%",
                    "evidence": c.evidence[:5],  # Limit to 5
                    "missing": c.missing[:5],
                    "recommendation": c.recommendation
                }
                for c in checks
            ]
        }

    def _get_pattern_files(self, pattern_name: str) -> List[str]:
        """Get unique files where pattern was found"""
        if pattern_name in self.scan_result.patterns_found:
            return list(set(f for f, _ in self.scan_result.patterns_found[pattern_name]))
        return []

    def _get_pattern_locations(self, pattern_name: str) -> List[str]:
        """Get file:line locations where pattern was found"""
        if pattern_name in self.scan_result.patterns_found:
            return [f"{f}:{line}" for f, line in self.scan_result.patterns_found[pattern_name][:10]]
        return []

    def _check_article_9_risk_management(self) -> ValidationResult:
        """Article 9: Risk Management System"""
        error_files = self._get_pattern_files("error_handling")
        risk_files = self._get_pattern_files("risk_assessment")

        # Check for circuit breaker
        circuit_breaker_exists = (self.root / "backend/utils/circuit_breaker.py").exists()
        guardrails_exists = (self.root / "backend/guardrails/llm_guardrails.py").exists()
        chaos_exists = (self.root / "tests/chaos/chaos_engineering.py").exists()

        evidence = []
        if circuit_breaker_exists: evidence.append("backend/utils/circuit_breaker.py")
        if guardrails_exists: evidence.append("backend/guardrails/llm_guardrails.py")
        if chaos_exists: evidence.append("tests/chaos/chaos_engineering.py")
        evidence.extend(self._get_pattern_locations("risk_assessment")[:3])

        score = (len(error_files) > 5) * 0.3 + circuit_breaker_exists * 0.3 + guardrails_exists * 0.2 + chaos_exists * 0.2

        return ValidationResult(
            article="Article 9",
            requirement="Risk Management System",
            status="PASS" if score >= 0.8 else "PARTIAL" if score >= 0.5 else "FAIL",
            score=score,
            evidence=evidence,
            missing=["chaos testing"] if not chaos_exists else [],
            recommendation="" if score >= 0.8 else "Add circuit breakers and chaos testing"
        )

    def _check_article_10_data_governance(self) -> ValidationResult:
        """Article 10: Data Governance"""
        # Check for RAG/data quality
        rag_exists = (self.root / "backend/rag").exists()
        retention_exists = (self.root / "backend/governance/data_retention.py").exists()

        # Check for PII handling
        pii_patterns = self._get_pattern_files("output_validation")

        evidence = []
        if rag_exists: evidence.append("backend/rag/ (data quality)")
        if retention_exists: evidence.append("backend/governance/data_retention.py")
        evidence.extend(self._get_pattern_locations("output_validation")[:3])

        score = rag_exists * 0.4 + retention_exists * 0.3 + (len(pii_patterns) > 0) * 0.3

        return ValidationResult(
            article="Article 10",
            requirement="Data Governance & Quality",
            status="PASS" if score >= 0.8 else "PARTIAL" if score >= 0.5 else "FAIL",
            score=score,
            evidence=evidence,
            missing=["data retention policy"] if not retention_exists else [],
            recommendation="" if score >= 0.8 else "Add data retention and PII handling"
        )

    def _check_article_11_documentation(self) -> ValidationResult:
        """Article 11: Technical Documentation"""
        docs = [
            "docs/ARCHITECTURE.md",
            "README.md",
            "CODE_DOCUMENTATION.md",
            "INCIDENT_LIFECYCLE_AND_RAG.txt",
            "docs/AI_AGENT_PLATFORM_PPT.md"
        ]
        found = [d for d in docs if (self.root / d).exists()]

        score = min(len(found) / 3, 1.0)  # Need at least 3 docs for full score

        return ValidationResult(
            article="Article 11",
            requirement="Technical Documentation",
            status="PASS" if score >= 0.8 else "PARTIAL" if score >= 0.5 else "FAIL",
            score=score,
            evidence=found,
            missing=[d for d in docs[:3] if d not in found],
            recommendation="" if score >= 0.8 else "Add architecture documentation"
        )

    def _check_article_12_record_keeping(self) -> ValidationResult:
        """Article 12: Record-Keeping & Audit Logging"""
        audit_exists = (self.root / "backend/governance/audit_logger.py").exists()
        logging_files = self._get_pattern_files("logging")
        audit_usage = self._get_pattern_files("audit_logging")

        evidence = []
        if audit_exists: evidence.append("backend/governance/audit_logger.py")
        evidence.extend(self._get_pattern_locations("audit_logging")[:5])
        evidence.extend(self._get_pattern_locations("logging")[:3])

        # Score: audit module exists + actually used + general logging
        score = audit_exists * 0.3 + (len(audit_usage) > 0) * 0.4 + (len(logging_files) > 5) * 0.3

        missing = []
        if not audit_exists: missing.append("audit_logger.py module")
        if len(audit_usage) == 0: missing.append("audit logging not used in code")

        return ValidationResult(
            article="Article 12",
            requirement="Record-Keeping & Audit Trail",
            status="PASS" if score >= 0.8 else "PARTIAL" if score >= 0.5 else "FAIL",
            score=score,
            evidence=evidence,
            missing=missing,
            recommendation="" if score >= 0.8 else "Integrate audit_logger in LangGraph nodes"
        )

    def _check_article_13_transparency(self) -> ValidationResult:
        """Article 13: Transparency & Explanations"""
        explain_files = self._get_pattern_files("explainability")

        evidence = self._get_pattern_locations("explainability")[:10]

        score = min(len(explain_files) / 3, 1.0)  # Need explanations in at least 3 files

        return ValidationResult(
            article="Article 13",
            requirement="Transparency & Explainability",
            status="PASS" if score >= 0.8 else "PARTIAL" if score >= 0.5 else "FAIL",
            score=score,
            evidence=evidence,
            missing=["Add reasoning/explanation fields"] if score < 0.8 else [],
            recommendation="" if score >= 0.8 else "Add explanation fields to AI decisions"
        )

    def _check_article_14_human_oversight(self) -> ValidationResult:
        """Article 14: Human Oversight"""
        hitl_files = self._get_pattern_files("human_oversight")

        # Check specific HITL implementation
        main_has_hitl = "backend/orchestrator/main.py" in hitl_files
        langgraph_has_hitl = "backend/orchestrator/langgraph_orchestrator.py" in hitl_files

        evidence = self._get_pattern_locations("human_oversight")[:10]

        score = (len(hitl_files) > 0) * 0.4 + main_has_hitl * 0.3 + langgraph_has_hitl * 0.3

        return ValidationResult(
            article="Article 14",
            requirement="Human Oversight (HITL)",
            status="PASS" if score >= 0.8 else "PARTIAL" if score >= 0.5 else "FAIL",
            score=score,
            evidence=evidence,
            missing=["HITL in LangGraph"] if not langgraph_has_hitl else [],
            recommendation="" if score >= 0.8 else "Add human approval workflow"
        )

    def _check_article_15_accuracy_security(self) -> ValidationResult:
        """Article 15: Accuracy, Robustness, Security"""
        confidence_files = self._get_pattern_files("confidence_threshold")
        input_val_files = self._get_pattern_files("input_validation")
        output_val_files = self._get_pattern_files("output_validation")

        evidence = []
        evidence.extend(self._get_pattern_locations("confidence_threshold")[:3])
        evidence.extend(self._get_pattern_locations("input_validation")[:3])
        evidence.extend(self._get_pattern_locations("output_validation")[:3])

        score = (len(confidence_files) > 0) * 0.4 + (len(input_val_files) > 0) * 0.3 + (len(output_val_files) > 0) * 0.3

        missing = []
        if len(confidence_files) == 0: missing.append("confidence thresholds")
        if len(input_val_files) == 0: missing.append("input validation")

        return ValidationResult(
            article="Article 15",
            requirement="Accuracy & Security",
            status="PASS" if score >= 0.8 else "PARTIAL" if score >= 0.5 else "FAIL",
            score=score,
            evidence=evidence,
            missing=missing,
            recommendation="" if score >= 0.8 else "Add confidence thresholds and input validation"
        )

    def _check_audit_integration(self) -> ValidationResult:
        """Check if audit logging is integrated in critical components"""
        audit_usage = self.scan_result.patterns_found.get("audit_logging", [])

        critical_files = [
            "backend/orchestrator/main.py",
            "backend/orchestrator/langgraph_orchestrator.py",
            "backend/orchestrator/llm_intelligence.py",
            "backend/agents/servicenow/agent.py",
            "backend/agents/jira/agent.py",
        ]

        files_with_audit = set(f for f, _ in audit_usage)
        integrated = [f for f in critical_files if f in files_with_audit]
        missing = [f for f in critical_files if f not in files_with_audit and (self.root / f).exists()]

        score = len(integrated) / max(len([f for f in critical_files if (self.root / f).exists()]), 1)

        return ValidationResult(
            article="Integration",
            requirement="Audit Logging in Critical Paths",
            status="PASS" if score >= 0.8 else "PARTIAL" if score >= 0.5 else "FAIL",
            score=score,
            evidence=[f"{f} (has audit logging)" for f in integrated],
            missing=missing,
            recommendation="" if score >= 0.8 else f"Add audit_logger to: {', '.join(missing[:3])}"
        )

    def _check_critical_paths(self) -> ValidationResult:
        """Check governance in LangGraph workflow nodes"""
        langgraph_file = self.root / "backend/orchestrator/langgraph_orchestrator.py"

        if not langgraph_file.exists():
            return ValidationResult(
                article="Critical Paths",
                requirement="Governance in LangGraph Nodes",
                status="FAIL",
                score=0.0,
                evidence=[],
                missing=["langgraph_orchestrator.py not found"],
                recommendation="Create LangGraph orchestrator"
            )

        content = langgraph_file.read_text(errors='ignore')

        # Check for governance patterns in LangGraph
        checks = {
            "judge_nodes": len(re.findall(r"def node_\d+_judge|judge_\w+", content)),
            "confidence_checks": len(re.findall(r"confidence|score", content, re.I)),
            "logging": len(re.findall(r"logger\.(info|warning|error)", content)),
            "error_handling": len(re.findall(r"try:|except", content)),
        }

        evidence = [f"{k}: {v} occurrences" for k, v in checks.items() if v > 0]

        score = min(sum(1 for v in checks.values() if v > 2) / 4, 1.0)

        return ValidationResult(
            article="Critical Paths",
            requirement="Governance in LangGraph Workflow",
            status="PASS" if score >= 0.8 else "PARTIAL" if score >= 0.5 else "FAIL",
            score=score,
            evidence=evidence,
            missing=[k for k, v in checks.items() if v < 2],
            recommendation="" if score >= 0.8 else "Add judge nodes and logging to workflow"
        )


def validate_project(verbose: bool = False) -> Dict:
    """Main validation function"""
    validator = ComprehensiveProjectValidator()
    result = validator.validate_all()

    if verbose:
        result["files_scanned"] = validator.scan_result.files_scanned
        result["patterns_summary"] = {
            k: len(v) for k, v in validator.scan_result.patterns_found.items()
        }

    return result


if __name__ == "__main__":
    import json
    result = validate_project(verbose=True)
    print(json.dumps(result, indent=2))
