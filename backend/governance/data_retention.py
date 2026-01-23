"""
Data Retention Policy Module
GDPR Article 5(1)(e) - Storage Limitation
EU AI Act Article 12 - Record Keeping
"""
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from dataclasses import dataclass
from enum import Enum
import structlog

logger = structlog.get_logger()


class DataCategory(Enum):
    """Data categories with different retention requirements"""
    AUDIT_LOGS = "audit_logs"
    AI_DECISIONS = "ai_decisions"
    INCIDENT_DATA = "incident_data"
    PII_DATA = "pii_data"
    METRICS = "metrics"
    LLM_TRACES = "llm_traces"
    EMBEDDINGS = "embeddings"


@dataclass
class RetentionPolicy:
    category: DataCategory
    retention_days: int
    legal_basis: str
    deletion_method: str
    archive_before_delete: bool = True


class DataRetentionManager:
    """Manages data retention policies for GDPR/EU AI Act compliance"""

    def __init__(self):
        self.policies = self._initialize_policies()

    def _initialize_policies(self) -> Dict[DataCategory, RetentionPolicy]:
        """Initialize retention policies per data category"""
        return {
            DataCategory.AUDIT_LOGS: RetentionPolicy(
                category=DataCategory.AUDIT_LOGS,
                retention_days=2555,  # 7 years for compliance
                legal_basis="EU AI Act Article 12, SOC2",
                deletion_method="secure_delete",
                archive_before_delete=True
            ),
            DataCategory.AI_DECISIONS: RetentionPolicy(
                category=DataCategory.AI_DECISIONS,
                retention_days=365,  # 1 year
                legal_basis="EU AI Act Article 12 - Record Keeping",
                deletion_method="secure_delete",
                archive_before_delete=True
            ),
            DataCategory.INCIDENT_DATA: RetentionPolicy(
                category=DataCategory.INCIDENT_DATA,
                retention_days=365,
                legal_basis="Business requirement, EU AI Act",
                deletion_method="anonymize_then_delete",
                archive_before_delete=True
            ),
            DataCategory.PII_DATA: RetentionPolicy(
                category=DataCategory.PII_DATA,
                retention_days=90,  # Minimize PII retention
                legal_basis="GDPR Article 5(1)(e)",
                deletion_method="secure_delete",
                archive_before_delete=False  # No archive for PII
            ),
            DataCategory.METRICS: RetentionPolicy(
                category=DataCategory.METRICS,
                retention_days=180,
                legal_basis="Operational requirement",
                deletion_method="aggregate_then_delete",
                archive_before_delete=False
            ),
            DataCategory.LLM_TRACES: RetentionPolicy(
                category=DataCategory.LLM_TRACES,
                retention_days=90,
                legal_basis="EU AI Act Article 12",
                deletion_method="secure_delete",
                archive_before_delete=True
            ),
            DataCategory.EMBEDDINGS: RetentionPolicy(
                category=DataCategory.EMBEDDINGS,
                retention_days=365,
                legal_basis="RAG knowledge base maintenance",
                deletion_method="recompute_without_source",
                archive_before_delete=False
            )
        }

    def get_policy(self, category: DataCategory) -> RetentionPolicy:
        """Get retention policy for a data category"""
        return self.policies.get(category)

    def get_retention_days(self, category: DataCategory) -> int:
        """Get retention period in days"""
        policy = self.policies.get(category)
        return policy.retention_days if policy else 90

    def is_expired(self, category: DataCategory, created_at: datetime) -> bool:
        """Check if data has exceeded retention period"""
        policy = self.policies.get(category)
        if not policy:
            return False

        expiry_date = created_at + timedelta(days=policy.retention_days)
        return datetime.utcnow() > expiry_date

    def get_data_for_deletion(self, category: DataCategory) -> Dict:
        """Identify data eligible for deletion"""
        policy = self.policies.get(category)
        cutoff_date = datetime.utcnow() - timedelta(days=policy.retention_days)

        return {
            "category": category.value,
            "cutoff_date": cutoff_date.isoformat(),
            "deletion_method": policy.deletion_method,
            "archive_first": policy.archive_before_delete
        }

    def execute_retention_cleanup(self) -> Dict:
        """Execute retention policy cleanup (dry run by default)"""
        results = {
            "executed_at": datetime.utcnow().isoformat(),
            "categories_processed": [],
            "records_identified": 0,
            "dry_run": True  # Safety: always dry run first
        }

        for category, policy in self.policies.items():
            deletion_info = self.get_data_for_deletion(category)
            results["categories_processed"].append({
                "category": category.value,
                "retention_days": policy.retention_days,
                "cutoff_date": deletion_info["cutoff_date"],
                "method": deletion_info["deletion_method"]
            })

            logger.info(
                "retention_cleanup_identified",
                category=category.value,
                cutoff_date=deletion_info["cutoff_date"]
            )

        return results

    def handle_deletion_request(self, user_id: str, request_type: str = "gdpr") -> Dict:
        """Handle GDPR Article 17 - Right to Erasure"""

        # Categories that must be deleted for GDPR requests
        gdpr_deletable = [
            DataCategory.PII_DATA,
            DataCategory.INCIDENT_DATA,  # If contains user data
        ]

        # Categories that must be retained for legal compliance
        legally_retained = [
            DataCategory.AUDIT_LOGS,  # Required for EU AI Act
            DataCategory.AI_DECISIONS,  # Required for accountability
        ]

        return {
            "request_id": f"DEL-{int(datetime.utcnow().timestamp())}",
            "user_id": user_id,
            "request_type": request_type,
            "received_at": datetime.utcnow().isoformat(),
            "deadline": (datetime.utcnow() + timedelta(days=30)).isoformat(),
            "actions": {
                "will_delete": [c.value for c in gdpr_deletable],
                "legally_retained": [c.value for c in legally_retained],
                "retention_reason": "EU AI Act Article 12 requires record keeping"
            },
            "status": "pending_execution"
        }

    def generate_retention_report(self) -> Dict:
        """Generate data retention compliance report"""
        return {
            "report_title": "Data Retention Compliance Report",
            "generated_at": datetime.utcnow().isoformat(),
            "legal_framework": ["GDPR", "EU AI Act", "SOC2"],
            "policies": [
                {
                    "category": cat.value,
                    "retention_days": pol.retention_days,
                    "legal_basis": pol.legal_basis,
                    "deletion_method": pol.deletion_method,
                    "archive": pol.archive_before_delete
                }
                for cat, pol in self.policies.items()
            ],
            "compliance_status": "COMPLIANT",
            "next_review_date": (datetime.utcnow() + timedelta(days=90)).isoformat()
        }


# Global instance
retention_manager = DataRetentionManager()
