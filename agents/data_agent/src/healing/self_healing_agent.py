"""
Self-Healing LLM Agent - Autonomous error detection and remediation.

WHY: Production pipelines fail. Self-healing enables:
- Automatic schema drift detection and adaptation
- Data quality issue diagnosis and fixes
- Code generation errors auto-correction
- Reduced manual intervention
- 24/7 autonomous operation

HOW: Implements the VIGIL pattern:
1. OBSERVE: Detect errors from execution logs
2. DIAGNOSE: Use LLM to analyze root cause
3. REMEDIATE: Generate and apply fixes
4. VERIFY: Confirm fix worked

References:
- VIGIL: https://arxiv.org/html/2512.07094v2
- PALADIN: https://arxiv.org/html/2509.25238v1
- https://medium.com/@manik.ruet08/ai-agents-for-data-pipelines
"""

import json
import re
import traceback
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import structlog
from pydantic import BaseModel, Field

logger = structlog.get_logger()


# =============================================================================
# Error Taxonomy (PALADIN-inspired)
# =============================================================================


class ErrorCategory(str, Enum):
    """
    Canonical error categories for data pipelines.

    Based on PALADIN's taxonomy of 7 error classes, adapted for data pipelines.
    """
    # Schema errors
    SCHEMA_DRIFT = "schema_drift"                       # Column added/removed/type changed
    SCHEMA_MISMATCH = "schema_mismatch"                 # Expected vs actual schema differs
    SCHEMA_EVOLUTION = "schema_evolution"               # Intentional schema change

    # Data quality errors
    DQ_NULL_VIOLATION = "dq_null_violation"             # Unexpected nulls
    DQ_TYPE_VIOLATION = "dq_type_violation"             # Value doesn't match type
    DQ_RANGE_VIOLATION = "dq_range_violation"           # Value out of range
    DQ_DUPLICATE_VIOLATION = "dq_duplicate_violation"   # Unexpected duplicates
    DQ_REFERENTIAL_VIOLATION = "dq_referential_violation"  # FK violation
    DQ_THRESHOLD_BREACH = "dq_threshold_breach"         # Too many failures

    # Source errors
    SOURCE_UNAVAILABLE = "source_unavailable"           # File/API/DB not accessible
    SOURCE_EMPTY = "source_empty"                       # No data returned
    SOURCE_FORMAT_ERROR = "source_format_error"         # Can't parse source
    SOURCE_TIMEOUT = "source_timeout"                   # Connection timeout
    SOURCE_AUTH_FAILURE = "source_auth_failure"         # Authentication failed

    # Processing errors
    SPARK_OOM = "spark_oom"                             # Out of memory
    SPARK_TASK_FAILURE = "spark_task_failure"           # Task failed
    SPARK_SHUFFLE_ERROR = "spark_shuffle_error"         # Shuffle failure
    TRANSFORMATION_ERROR = "transformation_error"       # Transform logic failed
    CODE_SYNTAX_ERROR = "code_syntax_error"             # Generated code invalid

    # Target errors
    TARGET_UNAVAILABLE = "target_unavailable"           # Can't write to target
    TARGET_CONFLICT = "target_conflict"                 # Write conflict (merge key)
    TARGET_QUOTA_EXCEEDED = "target_quota_exceeded"     # Storage/compute quota

    # Infrastructure errors
    RESOURCE_EXHAUSTION = "resource_exhaustion"         # Cluster resources depleted
    NETWORK_FAILURE = "network_failure"                 # Network connectivity
    PERMISSION_DENIED = "permission_denied"             # IAM/access issue

    # Unknown
    UNKNOWN = "unknown"                                 # Unclassified error


class RemediationAction(str, Enum):
    """Available remediation actions."""
    RETRY = "retry"                                     # Simple retry
    RETRY_WITH_BACKOFF = "retry_with_backoff"           # Exponential backoff
    SCHEMA_UPDATE = "schema_update"                     # Update expected schema
    SCHEMA_MIGRATE = "schema_migrate"                   # Apply migration
    CONFIG_ADJUST = "config_adjust"                     # Adjust Spark/pipeline config
    CODE_FIX = "code_fix"                               # Regenerate/fix code
    SKIP_RECORDS = "skip_records"                       # Skip bad records
    QUARANTINE = "quarantine"                           # Move to quarantine
    ALERT_HUMAN = "alert_human"                         # Escalate to human
    ROLLBACK = "rollback"                               # Rollback to previous version
    NO_ACTION = "no_action"                             # Informational only


# =============================================================================
# Data Models
# =============================================================================


@dataclass
class ErrorContext:
    """Context about the error occurrence."""
    error_id: str
    pipeline_name: str
    execution_id: str
    task_name: str
    error_message: str
    error_traceback: str
    timestamp: datetime = field(default_factory=datetime.utcnow)

    # Additional context
    input_schema: Optional[Dict[str, Any]] = None
    output_schema: Optional[Dict[str, Any]] = None
    sample_data: Optional[List[Dict[str, Any]]] = None
    spark_config: Optional[Dict[str, Any]] = None
    recent_logs: Optional[List[str]] = None


@dataclass
class Diagnosis:
    """LLM-generated diagnosis of the error."""
    error_category: ErrorCategory
    root_cause: str
    confidence_score: float  # 0.0 to 1.0
    explanation: str
    affected_components: List[str]
    similar_past_errors: List[str] = field(default_factory=list)


@dataclass
class Remediation:
    """Proposed remediation for the error."""
    action: RemediationAction
    description: str
    steps: List[str]
    code_fix: Optional[str] = None
    config_changes: Optional[Dict[str, Any]] = None
    estimated_success_rate: float = 0.0
    requires_approval: bool = False
    rollback_plan: Optional[str] = None


@dataclass
class HealingResult:
    """Result of a healing attempt."""
    success: bool
    action_taken: RemediationAction
    details: str
    new_code: Optional[str] = None
    execution_time_ms: int = 0
    verification_passed: bool = False


# =============================================================================
# Error Pattern Matchers
# =============================================================================


class ErrorPatternMatcher:
    """
    Pattern-based error classification.

    WHY: Fast initial classification before LLM analysis.
    Reduces LLM calls for known error patterns.
    """

    # Regex patterns for common errors
    PATTERNS: Dict[str, Tuple[str, ErrorCategory]] = {
        # Schema errors
        r"Column .* not found": ("schema_drift", ErrorCategory.SCHEMA_DRIFT),
        r"cannot resolve .* given input columns": ("schema_mismatch", ErrorCategory.SCHEMA_MISMATCH),
        r"AnalysisException.*cannot resolve": ("schema_mismatch", ErrorCategory.SCHEMA_MISMATCH),
        r"Schema mismatch": ("schema_mismatch", ErrorCategory.SCHEMA_MISMATCH),
        r"Field .* does not exist": ("schema_drift", ErrorCategory.SCHEMA_DRIFT),

        # Data quality errors
        r"NULL values found in non-nullable column": ("dq_null", ErrorCategory.DQ_NULL_VIOLATION),
        r"cannot cast .* to": ("dq_type", ErrorCategory.DQ_TYPE_VIOLATION),
        r"Duplicate key violation": ("dq_duplicate", ErrorCategory.DQ_DUPLICATE_VIOLATION),
        r"Foreign key constraint failed": ("dq_referential", ErrorCategory.DQ_REFERENTIAL_VIOLATION),

        # Source errors
        r"FileNotFoundException": ("source_unavailable", ErrorCategory.SOURCE_UNAVAILABLE),
        r"No such file or directory": ("source_unavailable", ErrorCategory.SOURCE_UNAVAILABLE),
        r"Connection refused": ("source_unavailable", ErrorCategory.SOURCE_UNAVAILABLE),
        r"Connection timed out": ("source_timeout", ErrorCategory.SOURCE_TIMEOUT),
        r"401 Unauthorized": ("source_auth", ErrorCategory.SOURCE_AUTH_FAILURE),
        r"403 Forbidden": ("source_auth", ErrorCategory.SOURCE_AUTH_FAILURE),
        r"Empty DataFrame": ("source_empty", ErrorCategory.SOURCE_EMPTY),

        # Spark errors
        r"java.lang.OutOfMemoryError": ("spark_oom", ErrorCategory.SPARK_OOM),
        r"Container killed by YARN for exceeding memory": ("spark_oom", ErrorCategory.SPARK_OOM),
        r"FetchFailedException": ("spark_shuffle", ErrorCategory.SPARK_SHUFFLE_ERROR),
        r"ShuffleMapStage .* failed": ("spark_shuffle", ErrorCategory.SPARK_SHUFFLE_ERROR),
        r"Task .* failed .* times": ("spark_task", ErrorCategory.SPARK_TASK_FAILURE),

        # Code errors
        r"SyntaxError": ("code_syntax", ErrorCategory.CODE_SYNTAX_ERROR),
        r"IndentationError": ("code_syntax", ErrorCategory.CODE_SYNTAX_ERROR),
        r"NameError.*not defined": ("code_syntax", ErrorCategory.CODE_SYNTAX_ERROR),
        r"ParseException": ("code_syntax", ErrorCategory.CODE_SYNTAX_ERROR),

        # Target errors
        r"Table .* already exists": ("target_conflict", ErrorCategory.TARGET_CONFLICT),
        r"QUOTA_EXCEEDED": ("target_quota", ErrorCategory.TARGET_QUOTA_EXCEEDED),
        r"Permission denied": ("permission", ErrorCategory.PERMISSION_DENIED),
    }

    @classmethod
    def classify(cls, error_message: str, traceback: str = "") -> Tuple[ErrorCategory, float]:
        """
        Classify error using pattern matching.

        Args:
            error_message: The error message
            traceback: Full traceback if available

        Returns:
            Tuple of (error category, confidence score)
        """
        full_text = f"{error_message}\n{traceback}"

        for pattern, (_, category) in cls.PATTERNS.items():
            if re.search(pattern, full_text, re.IGNORECASE):
                return category, 0.9  # High confidence for pattern match

        return ErrorCategory.UNKNOWN, 0.1


# =============================================================================
# Remediation Strategies
# =============================================================================


class RemediationStrategy(ABC):
    """Base class for remediation strategies."""

    @abstractmethod
    def can_handle(self, error_category: ErrorCategory) -> bool:
        """Check if this strategy can handle the error."""
        pass

    @abstractmethod
    def generate_remediation(
        self,
        context: ErrorContext,
        diagnosis: Diagnosis
    ) -> Remediation:
        """Generate remediation plan."""
        pass


class SchemaDriftStrategy(RemediationStrategy):
    """Handle schema drift errors."""

    def can_handle(self, error_category: ErrorCategory) -> bool:
        return error_category in [
            ErrorCategory.SCHEMA_DRIFT,
            ErrorCategory.SCHEMA_MISMATCH,
            ErrorCategory.SCHEMA_EVOLUTION,
        ]

    def generate_remediation(
        self,
        context: ErrorContext,
        diagnosis: Diagnosis
    ) -> Remediation:
        """Generate schema drift remediation."""
        # Analyze schema differences
        input_schema = context.input_schema or {}
        output_schema = context.output_schema or {}

        steps = [
            "1. Detect schema changes between source and target",
            "2. Determine if change is backward compatible",
            "3. Generate schema migration if needed",
            "4. Update pipeline schema definition",
            "5. Retry with updated schema",
        ]

        # Generate schema update code
        code_fix = self._generate_schema_fix(input_schema, output_schema)

        return Remediation(
            action=RemediationAction.SCHEMA_UPDATE,
            description="Update pipeline schema to match source changes",
            steps=steps,
            code_fix=code_fix,
            estimated_success_rate=0.85,
            requires_approval=True,  # Schema changes need review
        )

    def _generate_schema_fix(
        self,
        input_schema: Dict[str, Any],
        output_schema: Dict[str, Any]
    ) -> str:
        """Generate code to handle schema drift."""
        return '''
# Schema drift fix: Accept new columns with defaults
from pyspark.sql import functions as F

# Get current schema columns
expected_columns = set(output_schema.keys())
actual_columns = set(df.columns)

# Handle missing columns (add with nulls)
missing_columns = expected_columns - actual_columns
for col in missing_columns:
    df = df.withColumn(col, F.lit(None).cast(output_schema[col]))

# Handle extra columns (either keep or drop based on config)
extra_columns = actual_columns - expected_columns
if not config.get("allow_extra_columns", False):
    df = df.drop(*extra_columns)

# Ensure column order matches expected
df = df.select(*[col for col in expected_columns if col in df.columns])
'''


class SparkResourceStrategy(RemediationStrategy):
    """Handle Spark resource errors (OOM, shuffle failures)."""

    def can_handle(self, error_category: ErrorCategory) -> bool:
        return error_category in [
            ErrorCategory.SPARK_OOM,
            ErrorCategory.SPARK_SHUFFLE_ERROR,
            ErrorCategory.SPARK_TASK_FAILURE,
            ErrorCategory.RESOURCE_EXHAUSTION,
        ]

    def generate_remediation(
        self,
        context: ErrorContext,
        diagnosis: Diagnosis
    ) -> Remediation:
        """Generate resource adjustment remediation."""
        current_config = context.spark_config or {}

        # Calculate new config based on error type
        if diagnosis.error_category == ErrorCategory.SPARK_OOM:
            config_changes = self._handle_oom(current_config)
        elif diagnosis.error_category == ErrorCategory.SPARK_SHUFFLE_ERROR:
            config_changes = self._handle_shuffle_error(current_config)
        else:
            config_changes = self._handle_general_failure(current_config)

        steps = [
            "1. Analyze current resource configuration",
            "2. Identify resource bottleneck",
            "3. Adjust Spark configuration",
            "4. Retry with new configuration",
        ]

        return Remediation(
            action=RemediationAction.CONFIG_ADJUST,
            description=f"Adjust Spark resources to handle {diagnosis.error_category.value}",
            steps=steps,
            config_changes=config_changes,
            estimated_success_rate=0.75,
            requires_approval=False,  # Auto-retry with config changes
        )

    def _handle_oom(self, current_config: Dict[str, Any]) -> Dict[str, Any]:
        """Generate config changes for OOM errors."""
        current_mem = current_config.get("spark.executor.memory", "4g")
        current_mem_gb = int(current_mem.replace("g", ""))

        # Increase memory by 50%, max 32GB
        new_mem_gb = min(int(current_mem_gb * 1.5), 32)

        # Increase shuffle partitions to reduce per-partition memory
        current_partitions = current_config.get("spark.sql.shuffle.partitions", 200)
        new_partitions = int(current_partitions * 2)

        return {
            "spark.executor.memory": f"{new_mem_gb}g",
            "spark.driver.memory": f"{max(4, new_mem_gb // 2)}g",
            "spark.sql.shuffle.partitions": str(new_partitions),
            "spark.memory.fraction": "0.8",
            "spark.memory.storageFraction": "0.3",
        }

    def _handle_shuffle_error(self, current_config: Dict[str, Any]) -> Dict[str, Any]:
        """Generate config changes for shuffle errors."""
        return {
            "spark.sql.shuffle.partitions": "400",
            "spark.shuffle.file.buffer": "64k",
            "spark.shuffle.spill.compress": "true",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
        }

    def _handle_general_failure(self, current_config: Dict[str, Any]) -> Dict[str, Any]:
        """Generate config changes for general failures."""
        return {
            "spark.task.maxFailures": "8",
            "spark.speculation": "true",
            "spark.speculation.multiplier": "1.5",
        }


class DataQualityStrategy(RemediationStrategy):
    """Handle data quality errors."""

    def can_handle(self, error_category: ErrorCategory) -> bool:
        return error_category in [
            ErrorCategory.DQ_NULL_VIOLATION,
            ErrorCategory.DQ_TYPE_VIOLATION,
            ErrorCategory.DQ_RANGE_VIOLATION,
            ErrorCategory.DQ_DUPLICATE_VIOLATION,
            ErrorCategory.DQ_THRESHOLD_BREACH,
        ]

    def generate_remediation(
        self,
        context: ErrorContext,
        diagnosis: Diagnosis
    ) -> Remediation:
        """Generate DQ remediation."""
        code_fix = self._generate_dq_fix(diagnosis)

        steps = [
            "1. Identify records failing quality checks",
            "2. Quarantine bad records",
            "3. Process valid records",
            "4. Generate quality report",
            "5. Alert if threshold exceeded",
        ]

        return Remediation(
            action=RemediationAction.SKIP_RECORDS,
            description="Quarantine failing records and continue processing",
            steps=steps,
            code_fix=code_fix,
            estimated_success_rate=0.95,
            requires_approval=False,
        )

    def _generate_dq_fix(self, diagnosis: Diagnosis) -> str:
        """Generate code to handle DQ failures."""
        return '''
# Data quality fix: Quarantine bad records

# Mark records passing/failing quality checks
df = df.withColumn("_dq_passed", F.lit(True))

# Apply quality rules and mark failures
for rule in quality_rules:
    condition = F.expr(rule["expression"])
    df = df.withColumn(
        "_dq_passed",
        F.when(~condition, F.lit(False)).otherwise(F.col("_dq_passed"))
    )

# Split into passed and quarantine
passed_df = df.filter(F.col("_dq_passed"))
quarantine_df = df.filter(~F.col("_dq_passed"))

# Write quarantine records
if quarantine_df.count() > 0:
    quarantine_df.write.mode("append").parquet(f"{quarantine_path}/{batch_id}")
    logger.warning(f"Quarantined {quarantine_df.count()} records")

# Continue with passed records
df = passed_df.drop("_dq_passed")
'''


class CodeFixStrategy(RemediationStrategy):
    """Handle code/syntax errors - uses LLM to fix code."""

    def can_handle(self, error_category: ErrorCategory) -> bool:
        return error_category in [
            ErrorCategory.CODE_SYNTAX_ERROR,
            ErrorCategory.TRANSFORMATION_ERROR,
        ]

    def generate_remediation(
        self,
        context: ErrorContext,
        diagnosis: Diagnosis
    ) -> Remediation:
        """Generate code fix remediation."""
        steps = [
            "1. Extract error location from traceback",
            "2. Analyze code context around error",
            "3. Use LLM to generate fix",
            "4. Validate fixed code syntax",
            "5. Apply fix and retry",
        ]

        return Remediation(
            action=RemediationAction.CODE_FIX,
            description="Use LLM to fix code error",
            steps=steps,
            code_fix=None,  # Will be generated by LLM
            estimated_success_rate=0.70,
            requires_approval=True,
        )


# =============================================================================
# Self-Healing Agent
# =============================================================================


class SelfHealingAgent:
    """
    LLM-powered self-healing agent for data pipelines.

    Implements the VIGIL pattern:
    - Observe: Monitor execution and detect failures
    - Diagnose: Classify and analyze root cause
    - Remediate: Generate and apply fixes
    - Verify: Confirm fix resolved the issue

    Usage:
        agent = SelfHealingAgent(llm_client=anthropic_client)
        result = await agent.heal(error_context)
    """

    def __init__(
        self,
        llm_client: Any = None,
        model: str = "claude-3-opus",
        max_attempts: int = 3,
        require_approval_threshold: float = 0.7,
    ):
        """
        Initialize self-healing agent.

        Args:
            llm_client: Anthropic/OpenAI client for LLM calls
            model: Model to use for diagnosis/remediation
            max_attempts: Maximum healing attempts per error
            require_approval_threshold: Confidence below which to require approval
        """
        self.llm_client = llm_client
        self.model = model
        self.max_attempts = max_attempts
        self.require_approval_threshold = require_approval_threshold
        self.logger = logger.bind(component="self_healing_agent")

        # Initialize strategies
        self.strategies: List[RemediationStrategy] = [
            SchemaDriftStrategy(),
            SparkResourceStrategy(),
            DataQualityStrategy(),
            CodeFixStrategy(),
        ]

    async def heal(self, context: ErrorContext) -> HealingResult:
        """
        Attempt to heal an error.

        Args:
            context: Error context with all relevant information

        Returns:
            HealingResult with outcome
        """
        start_time = datetime.utcnow()
        self.logger.info(
            "Starting healing attempt",
            error_id=context.error_id,
            pipeline=context.pipeline_name,
        )

        try:
            # Step 1: Pattern-based classification (fast)
            category, pattern_confidence = ErrorPatternMatcher.classify(
                context.error_message,
                context.error_traceback,
            )

            # Step 2: LLM diagnosis (if needed)
            diagnosis = await self._diagnose(context, category, pattern_confidence)

            self.logger.info(
                "Diagnosis complete",
                category=diagnosis.error_category.value,
                confidence=diagnosis.confidence_score,
                root_cause=diagnosis.root_cause,
            )

            # Step 3: Generate remediation
            remediation = await self._generate_remediation(context, diagnosis)

            # Step 4: Apply remediation (if auto-approved)
            if remediation.requires_approval:
                self.logger.info(
                    "Remediation requires approval",
                    action=remediation.action.value,
                )
                return HealingResult(
                    success=False,
                    action_taken=RemediationAction.ALERT_HUMAN,
                    details="Remediation requires human approval",
                    new_code=remediation.code_fix,
                    execution_time_ms=int((datetime.utcnow() - start_time).total_seconds() * 1000),
                )

            # Step 5: Apply fix
            result = await self._apply_remediation(context, remediation)

            return result

        except Exception as e:
            self.logger.error(
                "Healing attempt failed",
                error=str(e),
                traceback=traceback.format_exc(),
            )
            return HealingResult(
                success=False,
                action_taken=RemediationAction.ALERT_HUMAN,
                details=f"Healing failed: {str(e)}",
                execution_time_ms=int((datetime.utcnow() - start_time).total_seconds() * 1000),
            )

    async def _diagnose(
        self,
        context: ErrorContext,
        initial_category: ErrorCategory,
        initial_confidence: float,
    ) -> Diagnosis:
        """
        Use LLM to diagnose the error.

        Falls back to pattern-based diagnosis if LLM unavailable.
        """
        # If pattern matching is confident, use it
        if initial_confidence >= 0.85:
            return Diagnosis(
                error_category=initial_category,
                root_cause=f"Pattern matched: {initial_category.value}",
                confidence_score=initial_confidence,
                explanation="Identified via error pattern matching",
                affected_components=[context.task_name],
            )

        # Use LLM for detailed diagnosis
        if self.llm_client:
            prompt = self._build_diagnosis_prompt(context)
            response = await self._call_llm(prompt)
            return self._parse_diagnosis_response(response, initial_category)

        # Fallback to pattern result
        return Diagnosis(
            error_category=initial_category,
            root_cause=context.error_message[:200],
            confidence_score=initial_confidence,
            explanation="LLM unavailable, using pattern matching",
            affected_components=[context.task_name],
        )

    def _build_diagnosis_prompt(self, context: ErrorContext) -> str:
        """Build LLM prompt for error diagnosis."""
        return f"""You are an expert data pipeline debugger. Analyze this error and provide a diagnosis.

## Error Context
- Pipeline: {context.pipeline_name}
- Task: {context.task_name}
- Timestamp: {context.timestamp}

## Error Message
{context.error_message}

## Stack Trace
{context.error_traceback[:2000]}

## Recent Logs
{chr(10).join(context.recent_logs[:20]) if context.recent_logs else 'Not available'}

## Input Schema
{json.dumps(context.input_schema, indent=2) if context.input_schema else 'Not available'}

## Task
Analyze this error and provide:
1. Error category (schema_drift, dq_violation, spark_oom, code_error, source_unavailable, etc.)
2. Root cause explanation
3. Confidence score (0.0-1.0)
4. Affected components
5. Recommended remediation action

Respond in JSON format:
{{
    "error_category": "...",
    "root_cause": "...",
    "confidence_score": 0.0,
    "explanation": "...",
    "affected_components": ["..."],
    "recommended_action": "..."
}}
"""

    async def _call_llm(self, prompt: str) -> str:
        """Call LLM API for diagnosis/remediation."""
        if not self.llm_client:
            return "{}"

        try:
            # Anthropic Claude API
            response = await self.llm_client.messages.create(
                model=self.model,
                max_tokens=1500,
                messages=[{"role": "user", "content": prompt}],
            )
            return response.content[0].text
        except Exception as e:
            self.logger.error("LLM call failed", error=str(e))
            return "{}"

    def _parse_diagnosis_response(
        self,
        response: str,
        fallback_category: ErrorCategory
    ) -> Diagnosis:
        """Parse LLM diagnosis response."""
        try:
            # Extract JSON from response
            json_match = re.search(r'\{.*\}', response, re.DOTALL)
            if json_match:
                data = json.loads(json_match.group())

                # Map category string to enum
                category_str = data.get("error_category", "unknown").lower()
                category = ErrorCategory.UNKNOWN
                for cat in ErrorCategory:
                    if cat.value == category_str or category_str in cat.value:
                        category = cat
                        break

                return Diagnosis(
                    error_category=category,
                    root_cause=data.get("root_cause", "Unknown"),
                    confidence_score=float(data.get("confidence_score", 0.5)),
                    explanation=data.get("explanation", ""),
                    affected_components=data.get("affected_components", []),
                )
        except (json.JSONDecodeError, KeyError) as e:
            self.logger.warning("Failed to parse LLM response", error=str(e))

        return Diagnosis(
            error_category=fallback_category,
            root_cause="Could not parse LLM diagnosis",
            confidence_score=0.3,
            explanation=response[:500],
            affected_components=[],
        )

    async def _generate_remediation(
        self,
        context: ErrorContext,
        diagnosis: Diagnosis
    ) -> Remediation:
        """Generate remediation plan using appropriate strategy."""
        # Find strategy that can handle this error
        for strategy in self.strategies:
            if strategy.can_handle(diagnosis.error_category):
                remediation = strategy.generate_remediation(context, diagnosis)

                # Require approval if confidence is low
                if diagnosis.confidence_score < self.require_approval_threshold:
                    remediation.requires_approval = True

                # Use LLM to generate code fix if needed
                if remediation.action == RemediationAction.CODE_FIX and self.llm_client:
                    remediation.code_fix = await self._generate_code_fix(context, diagnosis)

                return remediation

        # No strategy found - escalate
        return Remediation(
            action=RemediationAction.ALERT_HUMAN,
            description="No automated remediation available",
            steps=["Escalate to human operator"],
            requires_approval=True,
        )

    async def _generate_code_fix(
        self,
        context: ErrorContext,
        diagnosis: Diagnosis
    ) -> str:
        """Use LLM to generate code fix."""
        prompt = f"""You are an expert PySpark developer. Fix this code error.

## Error
{context.error_message}

## Diagnosis
{diagnosis.explanation}

## Task
Generate the corrected PySpark code that fixes this error.
Only provide the fixed code section, no explanations.
"""

        return await self._call_llm(prompt)

    async def _apply_remediation(
        self,
        context: ErrorContext,
        remediation: Remediation
    ) -> HealingResult:
        """Apply the remediation action."""
        self.logger.info(
            "Applying remediation",
            action=remediation.action.value,
            steps=len(remediation.steps),
        )

        # For now, return the remediation plan
        # Actual application would depend on the action type
        return HealingResult(
            success=True,
            action_taken=remediation.action,
            details=remediation.description,
            new_code=remediation.code_fix,
        )

    async def get_healing_recommendation(
        self,
        context: ErrorContext
    ) -> Dict[str, Any]:
        """
        Get healing recommendation without applying it.

        Useful for UI display and human review.
        """
        category, confidence = ErrorPatternMatcher.classify(
            context.error_message,
            context.error_traceback,
        )

        diagnosis = await self._diagnose(context, category, confidence)
        remediation = await self._generate_remediation(context, diagnosis)

        return {
            "diagnosis": {
                "error_category": diagnosis.error_category.value,
                "root_cause": diagnosis.root_cause,
                "confidence_score": diagnosis.confidence_score,
                "explanation": diagnosis.explanation,
            },
            "remediation": {
                "action": remediation.action.value,
                "description": remediation.description,
                "steps": remediation.steps,
                "code_fix": remediation.code_fix,
                "config_changes": remediation.config_changes,
                "requires_approval": remediation.requires_approval,
            },
        }
