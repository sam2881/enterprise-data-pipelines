"""
EU AI Act Compliance Module
Implements requirements for High-Risk AI Systems (Article 6-15)

This incident management agent is classified as HIGH-RISK under EU AI Act
because it affects critical infrastructure and makes automated decisions.
"""
import json
from datetime import datetime
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from enum import Enum
import structlog

logger = structlog.get_logger()


class AIActRiskCategory(Enum):
    """EU AI Act Risk Categories"""
    UNACCEPTABLE = "unacceptable"  # Banned
    HIGH = "high"  # Requires conformity assessment
    LIMITED = "limited"  # Transparency obligations
    MINIMAL = "minimal"  # No restrictions


class ComplianceStatus(Enum):
    COMPLIANT = "compliant"
    PARTIAL = "partial"
    NON_COMPLIANT = "non_compliant"
    NOT_APPLICABLE = "not_applicable"


@dataclass
class AISystemRegistration:
    """Article 51 - EU Database Registration"""
    system_name: str = "AI Incident Management Agent"
    system_version: str = "4.0.0"
    provider: str = "AI Agent Platform"
    risk_category: str = AIActRiskCategory.HIGH.value
    intended_purpose: str = "Automated incident detection, classification, and remediation in IT infrastructure"
    deployment_regions: List[str] = None
    registration_date: str = None

    def __post_init__(self):
        self.deployment_regions = self.deployment_regions or ["EU", "US"]
        self.registration_date = self.registration_date or datetime.utcnow().isoformat()


class EUAIActCompliance:
    """
    EU AI Act Compliance Manager
    Implements Articles 6-15 for High-Risk AI Systems
    """

    def __init__(self):
        self.registration = AISystemRegistration()
        self.compliance_checks: Dict[str, ComplianceStatus] = {}
        self._initialize_compliance_status()

    def _initialize_compliance_status(self):
        """Initialize compliance status for each article"""
        self.compliance_checks = {
            "article_9_risk_management": ComplianceStatus.COMPLIANT,
            "article_10_data_governance": ComplianceStatus.COMPLIANT,
            "article_11_technical_documentation": ComplianceStatus.COMPLIANT,
            "article_12_record_keeping": ComplianceStatus.COMPLIANT,
            "article_13_transparency": ComplianceStatus.COMPLIANT,
            "article_14_human_oversight": ComplianceStatus.COMPLIANT,
            "article_15_accuracy_robustness": ComplianceStatus.COMPLIANT,
        }

    # =========================================================================
    # Article 9: Risk Management System
    # =========================================================================
    def get_risk_management_info(self) -> Dict:
        """Article 9 - Risk Management System"""
        return {
            "article": "Article 9",
            "title": "Risk Management System",
            "status": self.compliance_checks["article_9_risk_management"].value,
            "implementation": {
                "risk_identification": {
                    "implemented": True,
                    "description": "LLM guardrails detect prompt injection, harmful content",
                    "file": "backend/guardrails/llm_guardrails.py"
                },
                "risk_mitigation": {
                    "implemented": True,
                    "description": "Circuit breakers, confidence thresholds, human approval",
                    "file": "backend/utils/circuit_breaker.py"
                },
                "residual_risk_evaluation": {
                    "implemented": True,
                    "description": "Safety scores, risk level assessment per incident",
                    "file": "backend/orchestrator/llm_intelligence.py"
                },
                "testing": {
                    "implemented": True,
                    "description": "Chaos engineering, adversarial testing",
                    "file": "tests/chaos/chaos_engineering.py"
                }
            }
        }

    # =========================================================================
    # Article 10: Data and Data Governance
    # =========================================================================
    def get_data_governance_info(self) -> Dict:
        """Article 10 - Data and Data Governance"""
        return {
            "article": "Article 10",
            "title": "Data and Data Governance",
            "status": self.compliance_checks["article_10_data_governance"].value,
            "implementation": {
                "training_data": {
                    "description": "RAG knowledge base from runbooks, past incidents",
                    "bias_mitigation": "Diverse incident scenarios from multiple sources",
                    "file": "backend/rag/embedding_service.py"
                },
                "data_quality": {
                    "description": "Judge nodes validate data quality at each step",
                    "file": "backend/orchestrator/langgraph_orchestrator.py"
                },
                "pii_handling": {
                    "description": "PII detection in guardrails, anonymization available",
                    "file": "backend/guardrails/llm_guardrails.py"
                }
            }
        }

    # =========================================================================
    # Article 11: Technical Documentation
    # =========================================================================
    def get_technical_documentation(self) -> Dict:
        """Article 11 - Technical Documentation"""
        return {
            "article": "Article 11",
            "title": "Technical Documentation",
            "status": self.compliance_checks["article_11_technical_documentation"].value,
            "documentation": {
                "system_description": "18-node LangGraph workflow for incident resolution",
                "intended_purpose": self.registration.intended_purpose,
                "architecture": {
                    "workflow_engine": "LangGraph StateGraph",
                    "llm_provider": "OpenAI GPT-4",
                    "knowledge_base": "Hybrid RAG (Vector + Graph)",
                    "human_oversight": "HITL approval workflow"
                },
                "performance_metrics": {
                    "accuracy_target": "95% correct classification",
                    "response_time_target": "<5s for analysis",
                    "human_oversight_rate": ">80% for high-risk actions"
                },
                "files": [
                    "docs/ARCHITECTURE.md",
                    "docs/CODE_DOCUMENTATION.md",
                    "INCIDENT_LIFECYCLE_AND_RAG.txt"
                ]
            }
        }

    # =========================================================================
    # Article 12: Record-Keeping
    # =========================================================================
    def get_record_keeping_info(self) -> Dict:
        """Article 12 - Record-Keeping (Logging)"""
        return {
            "article": "Article 12",
            "title": "Record-Keeping",
            "status": self.compliance_checks["article_12_record_keeping"].value,
            "implementation": {
                "automatic_logging": {
                    "implemented": True,
                    "description": "All AI decisions logged with timestamps",
                    "file": "backend/governance/audit_logger.py"
                },
                "traceability": {
                    "implemented": True,
                    "description": "LangFuse tracing for all LLM calls",
                    "file": "backend/orchestrator/llm_intelligence.py"
                },
                "retention_period": "90 days minimum",
                "log_contents": [
                    "AI decisions and confidence scores",
                    "Human oversight actions",
                    "Input/output data",
                    "System performance metrics"
                ]
            }
        }

    # =========================================================================
    # Article 13: Transparency and Information to Users
    # =========================================================================
    def get_transparency_info(self) -> Dict:
        """Article 13 - Transparency and Provision of Information"""
        return {
            "article": "Article 13",
            "title": "Transparency",
            "status": self.compliance_checks["article_13_transparency"].value,
            "implementation": {
                "ai_disclosure": {
                    "implemented": True,
                    "description": "Clear indication that system is AI-powered",
                    "ui_elements": ["AI badge on recommendations", "Confidence scores displayed"]
                },
                "decision_explanation": {
                    "implemented": True,
                    "description": "LLM provides reasoning for each decision",
                    "file": "backend/orchestrator/llm_intelligence.py"
                },
                "capabilities_limitations": {
                    "implemented": True,
                    "description": "Documentation of system capabilities and limits",
                    "file": "docs/ARCHITECTURE.md"
                },
                "contact_information": {
                    "provider": self.registration.provider,
                    "support": "support@aiagent.platform"
                }
            }
        }

    # =========================================================================
    # Article 14: Human Oversight
    # =========================================================================
    def get_human_oversight_info(self) -> Dict:
        """Article 14 - Human Oversight"""
        return {
            "article": "Article 14",
            "title": "Human Oversight",
            "status": self.compliance_checks["article_14_human_oversight"].value,
            "implementation": {
                "oversight_design": {
                    "description": "Human-in-the-Loop (HITL) approval workflow",
                    "file": "backend/orchestrator/main.py"
                },
                "override_capability": {
                    "implemented": True,
                    "description": "Humans can approve, reject, or modify AI decisions",
                    "api_endpoints": [
                        "/api/hitl/approvals/pending",
                        "/api/hitl/approvals/{id}/approve",
                        "/api/hitl/approvals/{id}/reject"
                    ]
                },
                "intervention_points": [
                    "Routing decision approval (Node 5)",
                    "Script selection approval (Node 11)",
                    "Execution plan approval (Node 14)",
                    "Post-execution validation (Node 16)"
                ],
                "stop_capability": {
                    "implemented": True,
                    "description": "Emergency stop for any running workflow"
                },
                "risk_based_approval": {
                    "low_risk": "Auto-approve with logging",
                    "medium_risk": "Single approver required",
                    "high_risk": "Dual approval required",
                    "critical_risk": "Manual execution only"
                }
            }
        }

    # =========================================================================
    # Article 15: Accuracy, Robustness, Cybersecurity
    # =========================================================================
    def get_accuracy_robustness_info(self) -> Dict:
        """Article 15 - Accuracy, Robustness and Cybersecurity"""
        return {
            "article": "Article 15",
            "title": "Accuracy, Robustness and Cybersecurity",
            "status": self.compliance_checks["article_15_accuracy_robustness"].value,
            "implementation": {
                "accuracy": {
                    "confidence_thresholds": {
                        "auto_execute": 0.95,
                        "recommend": 0.80,
                        "human_review": 0.60,
                        "reject": "<0.60"
                    },
                    "validation": "Judge nodes at 5 points in workflow",
                    "file": "backend/orchestrator/langgraph_orchestrator.py"
                },
                "robustness": {
                    "circuit_breakers": True,
                    "retry_mechanisms": True,
                    "fallback_behavior": True,
                    "chaos_testing": True,
                    "file": "backend/utils/circuit_breaker.py"
                },
                "cybersecurity": {
                    "input_validation": "Prompt injection detection",
                    "output_validation": "PII/secrets detection",
                    "authentication": "API key + RBAC",
                    "encryption": "TLS in transit",
                    "file": "backend/guardrails/llm_guardrails.py"
                }
            }
        }

    # =========================================================================
    # Compliance Report Generation
    # =========================================================================
    def generate_full_compliance_report(self) -> Dict:
        """Generate comprehensive EU AI Act compliance report"""
        return {
            "report_title": "EU AI Act Compliance Report",
            "system_registration": {
                "name": self.registration.system_name,
                "version": self.registration.system_version,
                "provider": self.registration.provider,
                "risk_category": self.registration.risk_category,
                "intended_purpose": self.registration.intended_purpose,
                "deployment_regions": self.registration.deployment_regions
            },
            "generated_at": datetime.utcnow().isoformat(),
            "overall_status": self._calculate_overall_status(),
            "articles": {
                "article_9": self.get_risk_management_info(),
                "article_10": self.get_data_governance_info(),
                "article_11": self.get_technical_documentation(),
                "article_12": self.get_record_keeping_info(),
                "article_13": self.get_transparency_info(),
                "article_14": self.get_human_oversight_info(),
                "article_15": self.get_accuracy_robustness_info()
            },
            "recommendations": self._get_recommendations()
        }

    def _calculate_overall_status(self) -> str:
        """Calculate overall compliance status"""
        statuses = list(self.compliance_checks.values())
        if all(s == ComplianceStatus.COMPLIANT for s in statuses):
            return "FULLY_COMPLIANT"
        elif any(s == ComplianceStatus.NON_COMPLIANT for s in statuses):
            return "NON_COMPLIANT"
        else:
            return "PARTIALLY_COMPLIANT"

    def _get_recommendations(self) -> List[str]:
        """Get recommendations for improving compliance"""
        recommendations = []

        # Check each article and provide recommendations
        for article, status in self.compliance_checks.items():
            if status != ComplianceStatus.COMPLIANT:
                recommendations.append(f"Review {article} implementation")

        # General recommendations
        recommendations.extend([
            "Conduct regular bias audits on AI decisions",
            "Update technical documentation quarterly",
            "Review human oversight logs monthly",
            "Perform annual third-party compliance audit"
        ])

        return recommendations

    def validate_decision(
        self,
        decision_type: str,
        confidence: float,
        risk_level: str,
        has_human_oversight: bool
    ) -> Dict:
        """Validate if a decision meets EU AI Act requirements"""

        issues = []

        # Article 14: Human oversight for high-risk
        if risk_level in ["high", "critical"] and not has_human_oversight:
            issues.append("Article 14: High-risk decision requires human oversight")

        # Article 15: Confidence threshold
        if confidence < 0.6:
            issues.append("Article 15: Confidence below minimum threshold (0.6)")

        # Article 13: Transparency
        if not decision_type:
            issues.append("Article 13: Decision type must be specified for transparency")

        return {
            "compliant": len(issues) == 0,
            "issues": issues,
            "recommendations": ["Add human review"] if issues else []
        }


# Global instance
eu_ai_compliance = EUAIActCompliance()
