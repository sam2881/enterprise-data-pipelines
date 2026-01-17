"""
File Validator - Enterprise metadata-driven file discovery and validation.

WHY: Validation rules are defined in metadata, not code.
     Same validator works for ALL feeds - rules come from feed_validation table.
     Supports the full enterprise pipeline flow: discovery → dedup → staging → validation.

HOW:
1. Reads validation rules from metadata and applies them to DataFrames.
2. Provides file discovery, duplicate detection, and staging operations.
3. All PySpark operations use lazy imports (Airflow-safe).
4. Database connections are lazy-initialized (no parse-time connections).

DESIGN DECISIONS:
1. Rules are evaluated dynamically from metadata
2. Support multiple severity levels (warning, error, critical)
3. Return detailed results for audit trail
4. Compatible with Great Expectations patterns
5. LAZY IMPORTS: PySpark imported inside methods, not at module level
6. Thread-safe singleton pattern for shared connections

ENTERPRISE METHODS (for DAG orchestration, no PySpark needed):
- discover_files(): Find files matching pattern in GCS
- is_duplicate(): Check if file already processed via PostgreSQL
- move_to_transient(): Move file to staging area
- move_duplicate(): Move duplicate file to archive
- compute_file_hash(): Compute SHA256 hash of file

VALIDATION METHODS (PySpark-based, run on Dataproc):
- validate(): Run all validation rules
- validate_schema(): Validate DataFrame schema
- validate_semantic_rule(): Run semantic validation
- get_valid_rows(): Filter to valid rows only
- get_rejected_rows(): Get rejected rows for quarantine
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, TYPE_CHECKING
from datetime import datetime
import hashlib
import os
import logging
import threading

# Lazy imports for PySpark (only imported when methods are called)
# WHY: Airflow parses DAG files but doesn't have PySpark installed
_pyspark_imported = False
_DataFrame = None
_SparkSession = None
_F = None
_StringType = None


def _ensure_pyspark():
    """Lazily import PySpark modules when needed."""
    global _pyspark_imported, _DataFrame, _SparkSession, _F, _StringType
    if not _pyspark_imported:
        from pyspark.sql import DataFrame, SparkSession
        from pyspark.sql import functions as F
        from pyspark.sql.types import StringType
        _DataFrame = DataFrame
        _SparkSession = SparkSession
        _F = F
        _StringType = StringType
        _pyspark_imported = True


# Lazy imports for GCS (Google Cloud Storage)
_gcs_imported = False
_storage_client = None


def _ensure_gcs():
    """Lazily import GCS client when needed."""
    global _gcs_imported, _storage_client
    if not _gcs_imported:
        from google.cloud import storage
        _storage_client = storage.Client
        _gcs_imported = True


# Lazy imports for SQLAlchemy (database operations)
_sqlalchemy_imported = False
_create_engine = None
_text = None


def _ensure_sqlalchemy():
    """Lazily import SQLAlchemy when needed."""
    global _sqlalchemy_imported, _create_engine, _text
    if not _sqlalchemy_imported:
        from sqlalchemy import create_engine, text
        _create_engine = create_engine
        _text = text
        _sqlalchemy_imported = True


# TYPE_CHECKING block for IDE support without runtime import
if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from .metadata_reader import MetadataReader, ValidationRule

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Result of a single validation rule."""
    rule_id: int
    rule_name: str
    rule_type: str
    column_name: Optional[str]
    passed: bool
    total_rows: int
    failed_rows: int
    pass_rate: float
    severity: str
    action: str
    details: Dict[str, Any] = field(default_factory=dict)
    sample_failures: List[Dict] = field(default_factory=list)


@dataclass
class ValidationSummary:
    """Summary of all validation results for a feed."""
    feed_id: str
    layer: str
    posting_date: str
    timestamp: datetime
    total_rules: int
    passed_rules: int
    failed_rules: int
    warning_rules: int
    total_rows: int
    valid_rows: int
    rejected_rows: int
    overall_pass_rate: float
    results: List[ValidationResult] = field(default_factory=list)

    def is_valid(self, min_pass_rate: float = 0.95) -> bool:
        """Check if validation passed overall threshold."""
        # Fail if any critical rule failed
        critical_failures = [r for r in self.results if r.severity == "critical" and not r.passed]
        if critical_failures:
            return False
        return self.overall_pass_rate >= min_pass_rate


@dataclass
class FileInfo:
    """Information about a discovered file."""
    file_path: str
    file_name: str
    file_size: int
    file_hash: Optional[str] = None
    last_modified: Optional[datetime] = None
    posting_date: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "file_path": self.file_path,
            "file_name": self.file_name,
            "file_size": self.file_size,
            "file_hash": self.file_hash,
            "last_modified": self.last_modified.isoformat() if self.last_modified else None,
            "posting_date": self.posting_date,
        }


class FileValidator:
    """
    Enterprise file validator with discovery, deduplication, and validation.

    WHY: Single class handles the entire file validation lifecycle:
         1. File discovery (find files matching pattern)
         2. Duplicate detection (check if already processed)
         3. File staging (move to transient location)
         4. Schema validation (verify structure)
         5. Semantic validation (verify data quality)

    HOW:
    - Enterprise methods (discover_files, is_duplicate, etc.) use GCS + PostgreSQL
    - Validation methods use PySpark (run on Dataproc, not Airflow)
    - All imports are lazy to support Airflow DAG parsing

    Usage (in DAG task):
        validator = FileValidator()
        files = validator.discover_files("my_feed", "*.csv", "2024-01-15")
        for file in files:
            if not validator.is_duplicate("my_feed", file.file_path):
                transient_path = validator.move_to_transient(file.file_path, "my_feed", "run_123")

    Usage (in PySpark job):
        validator = FileValidator(metadata_reader)
        summary = validator.validate(df, "my_feed", "silver", "2024-01-15")
    """

    # Thread-safe singleton lock
    _lock = threading.Lock()
    _db_engine = None

    def __init__(self, metadata_reader: Optional["MetadataReader"] = None):
        """
        Initialize FileValidator.

        Args:
            metadata_reader: Optional MetadataReader for validation rules.
                           If None, created lazily when needed.
        """
        self._reader = metadata_reader
        self._gcs_client = None

    @property
    def reader(self) -> "MetadataReader":
        """Lazy-load MetadataReader when needed."""
        if self._reader is None:
            from .metadata_reader import MetadataReader
            self._reader = MetadataReader()
        return self._reader

    def _get_gcs_client(self):
        """Get or create GCS client (lazy initialization)."""
        if self._gcs_client is None:
            _ensure_gcs()
            self._gcs_client = _storage_client()
        return self._gcs_client

    def _get_db_engine(self):
        """Get or create database engine (thread-safe singleton)."""
        if FileValidator._db_engine is None:
            with FileValidator._lock:
                if FileValidator._db_engine is None:
                    _ensure_sqlalchemy()
                    db_url = os.environ.get(
                        "METADATA_DB_URL",
                        f"postgresql://{os.environ.get('POSTGRES_USER', 'admin')}:"
                        f"{os.environ.get('POSTGRES_PASSWORD', 'admin123')}@"
                        f"{os.environ.get('POSTGRES_HOST', 'localhost')}:"
                        f"{os.environ.get('POSTGRES_PORT', '5432')}/"
                        f"{os.environ.get('POSTGRES_DB', 'agentdb')}"
                    )
                    FileValidator._db_engine = _create_engine(db_url)
        return FileValidator._db_engine

    # =========================================================================
    # ENTERPRISE METHODS (No PySpark - Safe for Airflow)
    # =========================================================================

    def discover_files(
        self,
        feed_id: str,
        file_pattern: str,
        posting_date: str,
        source_bucket: Optional[str] = None,
    ) -> List[FileInfo]:
        """
        Discover files matching pattern in GCS.

        WHY: First step in enterprise pipeline - find files to process.

        Args:
            feed_id: Feed identifier (for logging)
            file_pattern: Glob pattern (e.g., "raw/customers/*.csv")
            posting_date: Processing date (YYYY-MM-DD)
            source_bucket: GCS bucket name (defaults to GCS_BUCKET_RAW env var)

        Returns:
            List of FileInfo objects for discovered files
        """
        bucket_name = source_bucket or os.environ.get("GCS_BUCKET_RAW", "")
        if not bucket_name:
            logger.warning(f"No source bucket configured for feed {feed_id}")
            return []

        client = self._get_gcs_client()
        bucket = client.bucket(bucket_name)

        # Convert glob pattern to prefix for listing
        # e.g., "raw/customers/*.csv" -> prefix="raw/customers/"
        prefix = file_pattern.rsplit("*", 1)[0] if "*" in file_pattern else file_pattern

        # Get file extension filter if specified
        extension = None
        if "*." in file_pattern:
            extension = "." + file_pattern.rsplit("*.", 1)[1]

        files = []
        for blob in bucket.list_blobs(prefix=prefix):
            # Skip if extension filter doesn't match
            if extension and not blob.name.endswith(extension):
                continue

            # Skip directories
            if blob.name.endswith("/"):
                continue

            file_info = FileInfo(
                file_path=f"gs://{bucket_name}/{blob.name}",
                file_name=os.path.basename(blob.name),
                file_size=blob.size or 0,
                last_modified=blob.updated,
                posting_date=posting_date,
            )
            files.append(file_info)

        logger.info(f"Discovered {len(files)} files for feed {feed_id} with pattern {file_pattern}")
        return files

    def compute_file_hash(self, file_path: str) -> str:
        """
        Compute SHA256 hash of a file.

        WHY: Used for duplicate detection - same hash = same file content.

        Args:
            file_path: GCS path (gs://bucket/path) or local path

        Returns:
            SHA256 hash string
        """
        if file_path.startswith("gs://"):
            # GCS file - download to memory and hash
            _ensure_gcs()
            parts = file_path[5:].split("/", 1)
            bucket_name, blob_name = parts[0], parts[1]

            client = self._get_gcs_client()
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(blob_name)

            content = blob.download_as_bytes()
            return hashlib.sha256(content).hexdigest()
        else:
            # Local file
            sha256 = hashlib.sha256()
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(8192), b""):
                    sha256.update(chunk)
            return sha256.hexdigest()

    def is_duplicate(
        self,
        feed_id: str,
        file_path: str,
        file_hash: Optional[str] = None,
    ) -> bool:
        """
        Check if file has already been processed.

        WHY: Prevents reprocessing same file twice. Uses file_tracking table.

        Args:
            feed_id: Feed identifier
            file_path: Path to file
            file_hash: Pre-computed hash (computed if not provided)

        Returns:
            True if file is a duplicate, False otherwise
        """
        _ensure_sqlalchemy()

        # Compute hash if not provided
        if file_hash is None:
            file_hash = self.compute_file_hash(file_path)

        engine = self._get_db_engine()

        query = _text("""
            SELECT COUNT(*) FROM file_tracking
            WHERE feed_id = :feed_id
            AND (file_hash = :file_hash OR file_path = :file_path)
            AND status IN ('processed', 'completed')
        """)

        with engine.connect() as conn:
            result = conn.execute(
                query,
                {"feed_id": feed_id, "file_hash": file_hash, "file_path": file_path}
            )
            count = result.scalar()

        is_dup = count > 0
        if is_dup:
            logger.info(f"Duplicate detected: {file_path} for feed {feed_id}")

        return is_dup

    def move_to_transient(
        self,
        source_path: str,
        feed_id: str,
        group_run_id: str,
        transient_bucket: Optional[str] = None,
    ) -> str:
        """
        Move file to transient staging area.

        WHY: Files are staged before processing to ensure atomicity.
             If processing fails, original file is not affected.

        Args:
            source_path: GCS path to source file
            feed_id: Feed identifier
            group_run_id: Run identifier for grouping
            transient_bucket: Target bucket (defaults to GCS_BUCKET_TRANSIENT)

        Returns:
            Path to file in transient location
        """
        _ensure_gcs()

        target_bucket = transient_bucket or os.environ.get("GCS_BUCKET_TRANSIENT", "")
        if not target_bucket:
            # Use same bucket with transient/ prefix
            parts = source_path[5:].split("/", 1)
            target_bucket = parts[0]
            target_prefix = "transient"
        else:
            target_prefix = ""

        # Parse source path
        source_parts = source_path[5:].split("/", 1)
        source_bucket_name, source_blob_name = source_parts[0], source_parts[1]
        file_name = os.path.basename(source_blob_name)

        # Build target path: transient/{feed_id}/{group_run_id}/{filename}
        if target_prefix:
            target_blob_name = f"{target_prefix}/{feed_id}/{group_run_id}/{file_name}"
        else:
            target_blob_name = f"{feed_id}/{group_run_id}/{file_name}"

        client = self._get_gcs_client()
        source_bucket = client.bucket(source_bucket_name)
        source_blob = source_bucket.blob(source_blob_name)

        dest_bucket = client.bucket(target_bucket)

        # Copy to transient location
        source_bucket.copy_blob(source_blob, dest_bucket, target_blob_name)

        transient_path = f"gs://{target_bucket}/{target_blob_name}"
        logger.info(f"Moved {source_path} to transient: {transient_path}")

        return transient_path

    def move_duplicate(
        self,
        file_path: str,
        feed_id: str,
        archive_bucket: Optional[str] = None,
    ) -> str:
        """
        Move duplicate file to archive location.

        WHY: Duplicates are archived (not deleted) for audit purposes.

        Args:
            file_path: GCS path to duplicate file
            feed_id: Feed identifier
            archive_bucket: Archive bucket (defaults to GCS_BUCKET_ARCHIVE)

        Returns:
            Path to archived file
        """
        _ensure_gcs()

        target_bucket = archive_bucket or os.environ.get("GCS_BUCKET_ARCHIVE", "")
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

        # Parse source path
        source_parts = file_path[5:].split("/", 1)
        source_bucket_name, source_blob_name = source_parts[0], source_parts[1]
        file_name = os.path.basename(source_blob_name)

        if not target_bucket:
            target_bucket = source_bucket_name
            target_blob_name = f"archive/duplicates/{feed_id}/{timestamp}_{file_name}"
        else:
            target_blob_name = f"duplicates/{feed_id}/{timestamp}_{file_name}"

        client = self._get_gcs_client()
        source_bucket = client.bucket(source_bucket_name)
        source_blob = source_bucket.blob(source_blob_name)

        dest_bucket = client.bucket(target_bucket)

        # Copy to archive
        source_bucket.copy_blob(source_blob, dest_bucket, target_blob_name)

        archive_path = f"gs://{target_bucket}/{target_blob_name}"
        logger.info(f"Archived duplicate {file_path} to {archive_path}")

        return archive_path

    def register_file(
        self,
        feed_id: str,
        file_path: str,
        file_hash: str,
        file_size: int,
        group_run_id: str,
        run_id: str,
        status: str = "discovered",
    ) -> int:
        """
        Register file in tracking table.

        WHY: Creates audit trail and enables duplicate detection.

        Args:
            feed_id: Feed identifier
            file_path: Path to file
            file_hash: SHA256 hash
            file_size: File size in bytes
            group_run_id: Group run identifier
            run_id: Individual run identifier
            status: Initial status (discovered, processing, processed, failed)

        Returns:
            File tracking ID
        """
        _ensure_sqlalchemy()
        engine = self._get_db_engine()

        query = _text("""
            INSERT INTO file_tracking (
                feed_id, file_path, file_name, file_hash, file_size_bytes,
                group_run_id, run_id, status, discovered_at
            ) VALUES (
                :feed_id, :file_path, :file_name, :file_hash, :file_size,
                :group_run_id, :run_id, :status, NOW()
            )
            RETURNING id
        """)

        with engine.connect() as conn:
            result = conn.execute(query, {
                "feed_id": feed_id,
                "file_path": file_path,
                "file_name": os.path.basename(file_path),
                "file_hash": file_hash,
                "file_size": file_size,
                "group_run_id": group_run_id,
                "run_id": run_id,
                "status": status,
            })
            file_id = result.scalar()
            conn.commit()

        logger.info(f"Registered file {file_path} with ID {file_id}")
        return file_id

    def update_file_status(
        self,
        file_id: int,
        status: str,
        error_message: Optional[str] = None,
    ) -> None:
        """
        Update file tracking status.

        Args:
            file_id: File tracking ID
            status: New status
            error_message: Optional error message for failed status
        """
        _ensure_sqlalchemy()
        engine = self._get_db_engine()

        if status == "processed":
            query = _text("""
                UPDATE file_tracking
                SET status = :status, processed_at = NOW()
                WHERE id = :file_id
            """)
        elif error_message:
            query = _text("""
                UPDATE file_tracking
                SET status = :status, error_message = :error_message
                WHERE id = :file_id
            """)
        else:
            query = _text("""
                UPDATE file_tracking
                SET status = :status
                WHERE id = :file_id
            """)

        params = {"file_id": file_id, "status": status}
        if error_message:
            params["error_message"] = error_message

        with engine.connect() as conn:
            conn.execute(query, params)
            conn.commit()

    # =========================================================================
    # SCHEMA VALIDATION (PySpark-based)
    # =========================================================================

    def validate_schema(
        self,
        df: "DataFrame",
        feed_id: str,
        layer: str,
    ) -> Dict[str, Any]:
        """
        Validate DataFrame schema against metadata.

        WHY: Ensures incoming data matches expected structure before processing.

        Args:
            df: PySpark DataFrame to validate
            feed_id: Feed identifier
            layer: Data layer (bronze, silver, gold)

        Returns:
            Dict with validation results
        """
        _ensure_pyspark()

        # Get expected columns from metadata
        expected_columns = self.reader.get_feed_columns(feed_id, layer)
        actual_columns = set(df.columns)
        expected_names = {col.column_name for col in expected_columns}

        # Find missing and extra columns
        missing = expected_names - actual_columns
        extra = actual_columns - expected_names

        # Check required columns
        required = {col.column_name for col in expected_columns if col.is_required}
        missing_required = required - actual_columns

        is_valid = len(missing_required) == 0

        return {
            "is_valid": is_valid,
            "expected_columns": list(expected_names),
            "actual_columns": list(actual_columns),
            "missing_columns": list(missing),
            "extra_columns": list(extra),
            "missing_required": list(missing_required),
        }

    def validate_semantic_rule(
        self,
        df: "DataFrame",
        rule_type: str,
        rule_config: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Run a single semantic validation rule.

        WHY: Semantic rules check data quality beyond schema (nulls, ranges, etc.)

        Args:
            df: PySpark DataFrame
            rule_type: Type of rule (not_null, unique, range, pattern, etc.)
            rule_config: Rule configuration parameters

        Returns:
            Dict with validation results
        """
        _ensure_pyspark()

        total_rows = df.count()

        if rule_type == "not_null":
            col_name = rule_config.get("column")
            failed_df = df.filter(_F.col(col_name).isNull())
            failed_count = failed_df.count()

        elif rule_type == "unique":
            col_name = rule_config.get("column")
            dup_df = df.groupBy(col_name).count().filter(_F.col("count") > 1)
            failed_count = dup_df.count()

        elif rule_type == "range":
            col_name = rule_config.get("column")
            min_val = rule_config.get("min")
            max_val = rule_config.get("max")
            conditions = []
            if min_val is not None:
                conditions.append(_F.col(col_name) < min_val)
            if max_val is not None:
                conditions.append(_F.col(col_name) > max_val)
            if conditions:
                combined = conditions[0]
                for c in conditions[1:]:
                    combined = combined | c
                failed_count = df.filter(combined).count()
            else:
                failed_count = 0

        elif rule_type == "pattern":
            col_name = rule_config.get("column")
            pattern = rule_config.get("pattern", ".*")
            failed_df = df.filter(~_F.col(col_name).rlike(pattern))
            failed_count = failed_df.count()

        elif rule_type == "custom":
            expression = rule_config.get("expression", "1=1")
            try:
                failed_df = df.filter(f"NOT ({expression})")
                failed_count = failed_df.count()
            except Exception as e:
                return {"is_valid": False, "error": str(e)}
        else:
            return {"is_valid": False, "error": f"Unknown rule type: {rule_type}"}

        pass_rate = 1.0 - (failed_count / total_rows) if total_rows > 0 else 1.0
        threshold = rule_config.get("threshold", 1.0)

        return {
            "is_valid": pass_rate >= threshold,
            "total_rows": total_rows,
            "failed_rows": failed_count,
            "pass_rate": pass_rate,
            "threshold": threshold,
        }

    # =========================================================================
    # EXISTING VALIDATION METHODS (PySpark-based)
    # =========================================================================

    def validate(
        self,
        df: "DataFrame",
        feed_id: str,
        layer: str,
        posting_date: str,
        sample_failures: int = 5,
    ) -> ValidationSummary:
        """
        Run all validation rules for a feed.

        Args:
            df: DataFrame to validate
            feed_id: Feed identifier
            layer: Layer being validated (bronze, silver, gold)
            posting_date: Processing date
            sample_failures: Number of sample failures to capture

        Returns:
            ValidationSummary with all results
        """
        rules = self.reader.get_validation_rules(feed_id, layer)
        total_rows = df.count()

        results = []
        for rule in rules:
            result = self._evaluate_rule(df, rule, total_rows, sample_failures)
            results.append(result)

        # Calculate summary statistics
        passed_rules = sum(1 for r in results if r.passed)
        failed_rules = sum(1 for r in results if not r.passed and r.severity == "error")
        warning_rules = sum(1 for r in results if not r.passed and r.severity == "warning")

        # Calculate rejected rows (failed any non-warning rule)
        rejection_conditions = []
        for rule in rules:
            if rule.severity != "warning" and rule.action_on_failure == "reject":
                cond = self._get_failure_condition(df, rule)
                if cond is not None:
                    rejection_conditions.append(cond)

        rejected_rows = 0
        if rejection_conditions:
            combined_condition = rejection_conditions[0]
            for cond in rejection_conditions[1:]:
                combined_condition = combined_condition | cond
            rejected_rows = df.filter(combined_condition).count()

        valid_rows = total_rows - rejected_rows
        overall_pass_rate = valid_rows / total_rows if total_rows > 0 else 1.0

        return ValidationSummary(
            feed_id=feed_id,
            layer=layer,
            posting_date=posting_date,
            timestamp=datetime.utcnow(),
            total_rules=len(rules),
            passed_rules=passed_rules,
            failed_rules=failed_rules,
            warning_rules=warning_rules,
            total_rows=total_rows,
            valid_rows=valid_rows,
            rejected_rows=rejected_rows,
            overall_pass_rate=overall_pass_rate,
            results=results,
        )

    def _evaluate_rule(
        self,
        df: "DataFrame",
        rule: "ValidationRule",
        total_rows: int,
        sample_failures: int,
    ) -> ValidationResult:
        """Evaluate a single validation rule."""
        rule_type = rule.rule_type.lower()

        # Dispatch to appropriate rule evaluator
        evaluators = {
            "not_null": self._check_not_null,
            "unique": self._check_unique,
            "range": self._check_range,
            "pattern": self._check_pattern,
            "in_set": self._check_in_set,
            "custom": self._check_custom,
            "row_count": self._check_row_count,
        }

        evaluator = evaluators.get(rule_type, self._check_custom)
        failed_count, details, samples = evaluator(df, rule, sample_failures)

        pass_rate = 1.0 - (failed_count / total_rows) if total_rows > 0 else 1.0
        passed = failed_count == 0

        return ValidationResult(
            rule_id=rule.rule_id,
            rule_name=rule.rule_name,
            rule_type=rule.rule_type,
            column_name=rule.column_name,
            passed=passed,
            total_rows=total_rows,
            failed_rows=failed_count,
            pass_rate=pass_rate,
            severity=rule.severity,
            action=rule.action_on_failure,
            details=details,
            sample_failures=samples,
        )

    def _get_failure_condition(self, df: "DataFrame", rule: "ValidationRule"):
        """Get the failure condition for a rule (for filtering)."""
        _ensure_pyspark()
        rule_type = rule.rule_type.lower()
        col_name = rule.column_name

        if rule_type == "not_null" and col_name:
            return _F.col(col_name).isNull()
        elif rule_type == "range" and col_name:
            params = rule.parameters
            min_val = params.get("min")
            max_val = params.get("max")
            conditions = []
            if min_val is not None:
                conditions.append(_F.col(col_name) < min_val)
            if max_val is not None:
                conditions.append(_F.col(col_name) > max_val)
            if conditions:
                return conditions[0] if len(conditions) == 1 else (conditions[0] | conditions[1])
        elif rule_type == "pattern" and col_name:
            pattern = rule.parameters.get("pattern", ".*")
            return ~_F.col(col_name).rlike(pattern)

        return None

    # =========================================================================
    # Rule Evaluators
    # =========================================================================

    def _check_not_null(
        self,
        df: "DataFrame",
        rule: "ValidationRule",
        sample_count: int
    ) -> tuple:
        """Check for NULL values in a column."""
        _ensure_pyspark()
        col_name = rule.column_name
        if not col_name:
            return 0, {"error": "Column name required"}, []

        null_df = df.filter(_F.col(col_name).isNull())
        failed_count = null_df.count()

        samples = []
        if failed_count > 0 and sample_count > 0:
            samples = [row.asDict() for row in null_df.limit(sample_count).collect()]

        return failed_count, {"column": col_name}, samples

    def _check_unique(
        self,
        df: "DataFrame",
        rule: "ValidationRule",
        sample_count: int
    ) -> tuple:
        """Check for duplicate values."""
        _ensure_pyspark()
        col_name = rule.column_name
        if not col_name:
            return 0, {"error": "Column name required"}, []

        # Find duplicates
        dup_df = df.groupBy(col_name).count().filter(_F.col("count") > 1)
        dup_values = dup_df.count()

        # Count total duplicate rows
        if dup_values > 0:
            dup_vals = [row[col_name] for row in dup_df.collect()]
            failed_count = df.filter(_F.col(col_name).isin(dup_vals)).count() - dup_values
        else:
            failed_count = 0

        samples = []
        if dup_values > 0 and sample_count > 0:
            samples = [row.asDict() for row in dup_df.limit(sample_count).collect()]

        return failed_count, {"column": col_name, "duplicate_values": dup_values}, samples

    def _check_range(
        self,
        df: "DataFrame",
        rule: "ValidationRule",
        sample_count: int
    ) -> tuple:
        """Check if values are within range."""
        _ensure_pyspark()
        col_name = rule.column_name
        params = rule.parameters
        min_val = params.get("min")
        max_val = params.get("max")

        if not col_name:
            return 0, {"error": "Column name required"}, []

        conditions = []
        if min_val is not None:
            conditions.append(_F.col(col_name) < min_val)
        if max_val is not None:
            conditions.append(_F.col(col_name) > max_val)

        if not conditions:
            return 0, {"error": "No min or max specified"}, []

        combined = conditions[0]
        for cond in conditions[1:]:
            combined = combined | cond

        failed_df = df.filter(combined)
        failed_count = failed_df.count()

        samples = []
        if failed_count > 0 and sample_count > 0:
            samples = [row.asDict() for row in failed_df.limit(sample_count).collect()]

        return failed_count, {"column": col_name, "min": min_val, "max": max_val}, samples

    def _check_pattern(
        self,
        df: "DataFrame",
        rule: "ValidationRule",
        sample_count: int
    ) -> tuple:
        """Check if values match regex pattern."""
        _ensure_pyspark()
        col_name = rule.column_name
        pattern = rule.parameters.get("pattern", ".*")

        if not col_name:
            return 0, {"error": "Column name required"}, []

        failed_df = df.filter(~_F.col(col_name).rlike(pattern))
        failed_count = failed_df.count()

        samples = []
        if failed_count > 0 and sample_count > 0:
            samples = [row.asDict() for row in failed_df.limit(sample_count).collect()]

        return failed_count, {"column": col_name, "pattern": pattern}, samples

    def _check_in_set(
        self,
        df: "DataFrame",
        rule: "ValidationRule",
        sample_count: int
    ) -> tuple:
        """Check if values are in allowed set."""
        _ensure_pyspark()
        col_name = rule.column_name
        allowed_values = rule.parameters.get("values", [])

        if not col_name:
            return 0, {"error": "Column name required"}, []

        failed_df = df.filter(~_F.col(col_name).isin(allowed_values))
        failed_count = failed_df.count()

        samples = []
        if failed_count > 0 and sample_count > 0:
            samples = [row.asDict() for row in failed_df.limit(sample_count).collect()]

        return failed_count, {"column": col_name, "allowed": allowed_values}, samples

    def _check_custom(
        self,
        df: "DataFrame",
        rule: "ValidationRule",
        sample_count: int
    ) -> tuple:
        """Evaluate custom SQL expression."""
        expression = rule.parameters.get("expression", "1=1")

        try:
            failed_df = df.filter(f"NOT ({expression})")
            failed_count = failed_df.count()

            samples = []
            if failed_count > 0 and sample_count > 0:
                samples = [row.asDict() for row in failed_df.limit(sample_count).collect()]

            return failed_count, {"expression": expression}, samples
        except Exception as e:
            return 0, {"error": str(e)}, []

    def _check_row_count(
        self,
        df: "DataFrame",
        rule: "ValidationRule",
        sample_count: int
    ) -> tuple:
        """Check total row count is within range."""
        params = rule.parameters
        min_rows = params.get("min", 0)
        max_rows = params.get("max", float('inf'))

        total = df.count()
        passed = min_rows <= total <= max_rows

        return (
            0 if passed else total,
            {"total_rows": total, "min": min_rows, "max": max_rows},
            []
        )

    # =========================================================================
    # Helper Methods
    # =========================================================================

    def get_valid_rows(
        self,
        df: "DataFrame",
        feed_id: str,
        layer: str,
    ) -> "DataFrame":
        """
        Filter DataFrame to only valid rows.

        Applies all rejection rules and returns only passing rows.
        """
        _ensure_pyspark()
        rules = self.reader.get_validation_rules(feed_id, layer)

        valid_df = df
        for rule in rules:
            if rule.severity != "warning" and rule.action_on_failure == "reject":
                condition = self._get_failure_condition(df, rule)
                if condition is not None:
                    valid_df = valid_df.filter(~condition)

        return valid_df

    def get_rejected_rows(
        self,
        df: "DataFrame",
        feed_id: str,
        layer: str,
    ) -> "DataFrame":
        """
        Get rejected rows for error handling/quarantine.
        """
        _ensure_pyspark()
        rules = self.reader.get_validation_rules(feed_id, layer)

        rejection_conditions = []
        for rule in rules:
            if rule.severity != "warning" and rule.action_on_failure == "reject":
                condition = self._get_failure_condition(df, rule)
                if condition is not None:
                    rejection_conditions.append(condition)

        if not rejection_conditions:
            return df.limit(0)  # Empty DataFrame

        combined = rejection_conditions[0]
        for cond in rejection_conditions[1:]:
            combined = combined | cond

        return df.filter(combined)
