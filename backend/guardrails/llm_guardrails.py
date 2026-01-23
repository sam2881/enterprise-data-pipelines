"""
LLM Guardrails for AI Agent Platform

Provides comprehensive input/output validation for LLM calls:
- Prompt injection detection
- Jailbreak attempt detection
- Content moderation (harmful content, PII)
- Output format validation
- Safety scoring
- Rate limiting
"""

import re
import json
import hashlib
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import defaultdict
import structlog

logger = structlog.get_logger()


# =============================================================================
# Guardrail Result
# =============================================================================

@dataclass
class GuardrailResult:
    """Result of guardrail validation"""
    passed: bool
    score: float  # 0.0 = blocked, 1.0 = fully safe
    issues: List[str] = field(default_factory=list)
    sanitized_content: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "passed": self.passed,
            "score": self.score,
            "issues": self.issues,
            "sanitized_content": self.sanitized_content,
            "metadata": self.metadata
        }


# =============================================================================
# Input Validator
# =============================================================================

class InputValidator:
    """Validates and sanitizes LLM inputs"""

    # Prompt injection patterns
    INJECTION_PATTERNS = [
        # Direct instruction override
        r"ignore\s+(all\s+)?(previous|prior|above)\s+(instructions?|prompts?|rules?)",
        r"disregard\s+(all\s+)?(previous|prior|above)",
        r"forget\s+(everything|all)\s+(you|that)",
        r"new\s+instructions?:",
        r"override\s+(system|previous)\s+prompt",

        # Role manipulation
        r"you\s+are\s+(now|actually)\s+a",
        r"pretend\s+(to\s+be|you\s+are)",
        r"act\s+as\s+(if|though)",
        r"roleplay\s+as",
        r"switch\s+to\s+\w+\s+mode",

        # Jailbreak attempts
        r"do\s+anything\s+now",
        r"dan\s+mode",
        r"developer\s+mode\s+enabled",
        r"jailbreak",
        r"bypass\s+(safety|security|filters?)",

        # System prompt extraction
        r"(show|reveal|print|display|output)\s+(your|the)\s+(system\s+)?prompt",
        r"what\s+(is|are)\s+your\s+(instructions?|rules?|prompt)",
        r"repeat\s+(your|the)\s+(system\s+)?prompt",

        # Encoding tricks
        r"base64\s*:",
        r"decode\s+this:",
        r"hex\s*:",
    ]

    # Sensitive data patterns
    PII_PATTERNS = [
        (r"\b\d{3}[-.]?\d{2}[-.]?\d{4}\b", "SSN"),
        (r"\b\d{16}\b", "credit_card"),
        (r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b", "email"),
        (r"\b\d{3}[-.]?\d{3}[-.]?\d{4}\b", "phone"),
    ]

    # Command injection patterns (for incident descriptions)
    COMMAND_INJECTION_PATTERNS = [
        r";\s*(rm|wget|curl|nc|bash|sh|python|perl|cat|head|tail|less|more)",
        r"\|\s*(rm|wget|curl|nc|bash|sh|cat|head|tail|less|more|grep|awk|sed)",
        r"\$\([^)]+\)",
        r"`[^`]+`",
        r"&&\s*(rm|wget|curl|cat)",
        r"/etc/(passwd|shadow|hosts|sudoers)",
        r"--no-preserve-root",
    ]

    def __init__(self, max_input_length: int = 10000):
        self.max_input_length = max_input_length
        self._compile_patterns()

    def _compile_patterns(self):
        """Compile regex patterns for efficiency"""
        self.injection_regex = [re.compile(p, re.IGNORECASE) for p in self.INJECTION_PATTERNS]
        self.pii_regex = [(re.compile(p), name) for p, name in self.PII_PATTERNS]
        self.command_regex = [re.compile(p, re.IGNORECASE) for p in self.COMMAND_INJECTION_PATTERNS]

    def validate(self, content: str, context: str = "general") -> GuardrailResult:
        """
        Validate input content

        Args:
            content: The input text to validate
            context: Context type ('incident', 'user_query', 'general')

        Returns:
            GuardrailResult with validation status
        """
        issues = []
        score = 1.0
        sanitized = content

        # Length check
        if len(content) > self.max_input_length:
            issues.append(f"Input exceeds max length ({self.max_input_length})")
            score -= 0.2
            sanitized = content[:self.max_input_length]

        # Prompt injection detection
        injection_score = self._check_prompt_injection(content)
        if injection_score < 1.0:
            issues.append(f"Potential prompt injection detected (score: {injection_score:.2f})")
            score = min(score, injection_score)

        # Command injection (for incident context)
        if context == "incident":
            cmd_score = self._check_command_injection(content)
            if cmd_score < 1.0:
                issues.append("Potential command injection in incident description")
                score = min(score, cmd_score)

        # PII detection (just log, don't block)
        pii_found = self._detect_pii(content)
        if pii_found:
            issues.append(f"PII detected: {', '.join(pii_found)}")
            # Don't reduce score, just flag

        passed = score >= 0.5

        logger.info(
            "input_validation_completed",
            passed=passed,
            score=score,
            issues=issues,
            context=context
        )

        return GuardrailResult(
            passed=passed,
            score=score,
            issues=issues,
            sanitized_content=sanitized if sanitized != content else None,
            metadata={"context": context, "pii_detected": pii_found}
        )

    def _check_prompt_injection(self, content: str) -> float:
        """Check for prompt injection attempts"""
        matches = 0
        for pattern in self.injection_regex:
            if pattern.search(content):
                matches += 1

        if matches == 0:
            return 1.0
        elif matches == 1:
            return 0.6
        elif matches == 2:
            return 0.3
        else:
            return 0.1

    def _check_command_injection(self, content: str) -> float:
        """Check for command injection in incident descriptions"""
        for pattern in self.command_regex:
            if pattern.search(content):
                return 0.3
        return 1.0

    def _detect_pii(self, content: str) -> List[str]:
        """Detect PII in content"""
        found = []
        for pattern, pii_type in self.pii_regex:
            if pattern.search(content):
                found.append(pii_type)
        return found


# =============================================================================
# Output Validator
# =============================================================================

class OutputValidator:
    """Validates LLM outputs"""

    # Harmful content patterns
    HARMFUL_PATTERNS = [
        # Dangerous commands
        r"rm\s+-rf\s+/",
        r":(){ :|:& };:",  # Fork bomb
        r"mkfs\.",
        r"dd\s+if=.*of=/dev/",

        # Sensitive data exposure
        r"(password|api_key|secret|token)\s*[:=]\s*['\"][^'\"]+['\"]",

        # Privilege escalation
        r"chmod\s+777",
        r"sudo\s+su\s+-",
    ]

    # Expected output format patterns
    FORMAT_PATTERNS = {
        "json": r"^\s*[\[{]",
        "script": r"^(#!/|#\s|echo|gcloud|kubectl|ansible)",
    }

    def __init__(self):
        self.harmful_regex = [re.compile(p, re.IGNORECASE) for p in self.HARMFUL_PATTERNS]

    def validate(
        self,
        content: str,
        expected_format: Optional[str] = None,
        max_length: int = 50000
    ) -> GuardrailResult:
        """
        Validate LLM output

        Args:
            content: The output text to validate
            expected_format: Expected format ('json', 'script', None)
            max_length: Maximum allowed output length

        Returns:
            GuardrailResult with validation status
        """
        issues = []
        score = 1.0

        # Length check
        if len(content) > max_length:
            issues.append(f"Output exceeds max length ({max_length})")
            score -= 0.1

        # Harmful content check
        harmful_score = self._check_harmful_content(content)
        if harmful_score < 1.0:
            issues.append("Potentially harmful content detected")
            score = min(score, harmful_score)

        # Format validation
        if expected_format:
            format_valid = self._validate_format(content, expected_format)
            if not format_valid:
                issues.append(f"Output does not match expected format: {expected_format}")
                score -= 0.2

        # JSON validity check
        if expected_format == "json":
            try:
                json.loads(content)
            except json.JSONDecodeError as e:
                issues.append(f"Invalid JSON: {str(e)}")
                score -= 0.3

        passed = score >= 0.5

        logger.info(
            "output_validation_completed",
            passed=passed,
            score=score,
            issues=issues,
            expected_format=expected_format
        )

        return GuardrailResult(
            passed=passed,
            score=score,
            issues=issues,
            metadata={"expected_format": expected_format}
        )

    def _check_harmful_content(self, content: str) -> float:
        """Check for harmful content in output"""
        for pattern in self.harmful_regex:
            if pattern.search(content):
                return 0.2
        return 1.0

    def _validate_format(self, content: str, expected_format: str) -> bool:
        """Validate output format"""
        if expected_format not in self.FORMAT_PATTERNS:
            return True
        pattern = re.compile(self.FORMAT_PATTERNS[expected_format])
        return bool(pattern.search(content))


# =============================================================================
# Rate Limiter
# =============================================================================

class RateLimiter:
    """Rate limiting for LLM calls"""

    def __init__(
        self,
        max_requests_per_minute: int = 60,
        max_requests_per_hour: int = 500
    ):
        self.max_per_minute = max_requests_per_minute
        self.max_per_hour = max_requests_per_hour
        self.requests: Dict[str, List[datetime]] = defaultdict(list)

    def check(self, identifier: str = "default") -> Tuple[bool, str]:
        """
        Check if request is allowed

        Args:
            identifier: Unique identifier (user_id, ip, etc.)

        Returns:
            Tuple of (allowed, reason)
        """
        now = datetime.now()

        # Clean old requests
        minute_ago = now - timedelta(minutes=1)
        hour_ago = now - timedelta(hours=1)

        self.requests[identifier] = [
            t for t in self.requests[identifier]
            if t > hour_ago
        ]

        # Count recent requests
        recent_minute = sum(1 for t in self.requests[identifier] if t > minute_ago)
        recent_hour = len(self.requests[identifier])

        if recent_minute >= self.max_per_minute:
            return False, f"Rate limit exceeded: {self.max_per_minute}/minute"

        if recent_hour >= self.max_per_hour:
            return False, f"Rate limit exceeded: {self.max_per_hour}/hour"

        # Record this request
        self.requests[identifier].append(now)
        return True, "OK"


# =============================================================================
# Content Moderator
# =============================================================================

class ContentModerator:
    """Content moderation for incident-related content"""

    # Topics that should be blocked
    BLOCKED_TOPICS = [
        r"\b(hack|exploit|attack)\b.*\b(other|external|customer)\b",
        r"\bddos\b",
        r"\bransomware\b",
        r"\bmalware\b.*\b(create|deploy|spread)\b",
    ]

    # Topics that require extra review
    SENSITIVE_TOPICS = [
        r"\bproduction\b.*\b(database|db)\b",
        r"\bdelete\b.*\ball\b",
        r"\bdrop\b.*\btable\b",
        r"\btruncate\b",
    ]

    def __init__(self):
        self.blocked_regex = [re.compile(p, re.IGNORECASE) for p in self.BLOCKED_TOPICS]
        self.sensitive_regex = [re.compile(p, re.IGNORECASE) for p in self.SENSITIVE_TOPICS]

    def moderate(self, content: str) -> GuardrailResult:
        """
        Moderate content for appropriateness

        Args:
            content: Content to moderate

        Returns:
            GuardrailResult with moderation status
        """
        issues = []
        score = 1.0

        # Check blocked topics
        for pattern in self.blocked_regex:
            if pattern.search(content):
                issues.append("Content contains blocked topic")
                score = 0.0
                break

        # Check sensitive topics
        if score > 0:
            for pattern in self.sensitive_regex:
                if pattern.search(content):
                    issues.append("Content contains sensitive topic - requires review")
                    score = min(score, 0.7)

        passed = score > 0

        logger.info(
            "content_moderation_completed",
            passed=passed,
            score=score,
            issues=issues
        )

        return GuardrailResult(
            passed=passed,
            score=score,
            issues=issues,
            metadata={"moderation_level": "standard"}
        )


# =============================================================================
# Main Guardrails Class
# =============================================================================

class LLMGuardrails:
    """
    Main guardrails class that combines all validation components.

    Usage:
        guardrails = LLMGuardrails()

        # Validate input before LLM call
        input_result = guardrails.validate_input(user_query, context="incident")
        if not input_result.passed:
            return {"error": "Input validation failed", "issues": input_result.issues}

        # Make LLM call...
        llm_response = call_llm(input_result.sanitized_content or user_query)

        # Validate output
        output_result = guardrails.validate_output(llm_response, expected_format="json")
        if not output_result.passed:
            return {"error": "Output validation failed", "issues": output_result.issues}
    """

    def __init__(
        self,
        max_input_length: int = 10000,
        max_requests_per_minute: int = 60,
        max_requests_per_hour: int = 500
    ):
        self.input_validator = InputValidator(max_input_length)
        self.output_validator = OutputValidator()
        self.rate_limiter = RateLimiter(max_requests_per_minute, max_requests_per_hour)
        self.content_moderator = ContentModerator()

        logger.info(
            "llm_guardrails_initialized",
            max_input_length=max_input_length,
            rate_limit_minute=max_requests_per_minute,
            rate_limit_hour=max_requests_per_hour
        )

    def validate_input(
        self,
        content: str,
        context: str = "general",
        identifier: str = "default"
    ) -> GuardrailResult:
        """
        Validate input before LLM call

        Args:
            content: Input content to validate
            context: Context type ('incident', 'user_query', 'general')
            identifier: Rate limit identifier

        Returns:
            GuardrailResult with combined validation status
        """
        # Rate limit check
        allowed, reason = self.rate_limiter.check(identifier)
        if not allowed:
            return GuardrailResult(
                passed=False,
                score=0.0,
                issues=[reason],
                metadata={"rate_limited": True}
            )

        # Input validation
        input_result = self.input_validator.validate(content, context)

        # Content moderation
        moderation_result = self.content_moderator.moderate(content)

        # Combine results
        combined_score = min(input_result.score, moderation_result.score)
        combined_issues = input_result.issues + moderation_result.issues
        passed = input_result.passed and moderation_result.passed

        return GuardrailResult(
            passed=passed,
            score=combined_score,
            issues=combined_issues,
            sanitized_content=input_result.sanitized_content,
            metadata={
                **input_result.metadata,
                **moderation_result.metadata,
                "identifier": identifier
            }
        )

    def validate_output(
        self,
        content: str,
        expected_format: Optional[str] = None,
        max_length: int = 50000
    ) -> GuardrailResult:
        """
        Validate LLM output

        Args:
            content: Output content to validate
            expected_format: Expected format ('json', 'script', None)
            max_length: Maximum allowed length

        Returns:
            GuardrailResult with validation status
        """
        return self.output_validator.validate(content, expected_format, max_length)

    def validate_incident(self, incident: Dict[str, Any]) -> GuardrailResult:
        """
        Validate an incident object

        Args:
            incident: Incident dictionary to validate

        Returns:
            GuardrailResult with validation status
        """
        # Combine relevant fields for validation
        content = " ".join([
            str(incident.get("short_description", "")),
            str(incident.get("description", "")),
            str(incident.get("comments", ""))
        ])

        return self.validate_input(content, context="incident")

    def validate_script(self, script_content: str) -> GuardrailResult:
        """
        Validate a remediation script

        Args:
            script_content: Script content to validate

        Returns:
            GuardrailResult with validation status
        """
        return self.output_validator.validate(
            script_content,
            expected_format="script"
        )


# =============================================================================
# Global Instance
# =============================================================================

guardrails = LLMGuardrails()

# Flag to indicate guardrails are available
GUARDRAILS_ENABLED = True
