"""
APEX Data Agent - Spark Job Submitter

Submits and monitors PySpark jobs for APEX pipelines.
Handles both Dataproc and local Spark cluster submissions.
"""

import os
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field

from dag_utilities.core.exceptions import SparkJobError
from dag_utilities.core.config_loader import ConfigLoader

# Default base path for spark job scripts
SPARK_JOBS_PATH = os.getenv("APEX_SPARK_JOBS_PATH", "/opt/airflow/spark_jobs")


@dataclass
class SparkJobResult:
    """Result of a Spark job execution."""

    job_name: str
    application_id: Optional[str] = None
    status: str = "UNKNOWN"
    start_time: datetime = field(default_factory=datetime.utcnow)
    end_time: Optional[datetime] = None
    duration_seconds: float = 0.0
    records_read: int = 0
    records_written: int = 0
    records_rejected: int = 0
    bytes_read: int = 0
    bytes_written: int = 0
    error_message: Optional[str] = None
    spark_history_url: Optional[str] = None


class SparkJobSubmitter:
    """
    Submits and monitors Spark jobs.

    Supports submission to:
    - Local Spark cluster (spark-submit)
    - Google Cloud Dataproc
    - Kubernetes (spark-on-k8s)
    """

    def __init__(self, submit_mode: str = "local"):
        """
        Initialize Spark job submitter.

        Args:
            submit_mode: "local", "dataproc", or "kubernetes"
        """
        self.submit_mode = submit_mode
        self.config = ConfigLoader()

    def submit_job(
        self,
        job_name: str,
        job_path: str,
        config: Any,  # SparkConfigData from metadata
        arguments: Dict[str, Any],
        wait_for_completion: bool = True,
    ) -> SparkJobResult:
        """
        Submit a Spark job.

        Args:
            job_name: Name of the job (e.g., "raw_to_bronze")
            job_path: Path to the PySpark script
            config: Spark configuration from metadata
            arguments: Job arguments (feed_id, contract_id, etc.)
            wait_for_completion: Whether to wait for job completion

        Returns:
            SparkJobResult with execution details
        """
        # Resolve relative spark job paths
        if not os.path.isabs(job_path) and not job_path.startswith("gs://"):
            basename = job_path
            if basename.startswith("spark_jobs/"):
                basename = basename[len("spark_jobs/"):]
            job_path = str(Path(SPARK_JOBS_PATH) / basename)

        result = SparkJobResult(job_name=job_name)

        try:
            if self.submit_mode == "local":
                result = self._submit_local(job_name, job_path, config, arguments)
            elif self.submit_mode == "dataproc":
                result = self._submit_dataproc(job_name, job_path, config, arguments)
            else:
                raise SparkJobError(f"Unknown submit mode: {self.submit_mode}")

            if wait_for_completion:
                result = self._wait_for_completion(result)

        except Exception as e:
            result.status = "FAILED"
            result.error_message = str(e)
            result.end_time = datetime.utcnow()

        return result

    def _submit_local(
        self,
        job_name: str,
        job_path: str,
        config: Any,
        arguments: Dict[str, Any],
    ) -> SparkJobResult:
        """Submit job to local Spark cluster."""
        result = SparkJobResult(job_name=job_name)

        # Build spark-submit command
        cmd = self._build_spark_submit_command(job_path, config, arguments)

        # Execute
        try:
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )

            # Parse application ID from output
            for line in process.stderr:
                if "Application report" in line or "application_" in line:
                    # Extract application ID
                    import re
                    match = re.search(r"application_\d+_\d+", line)
                    if match:
                        result.application_id = match.group()
                        break

            stdout, stderr = process.communicate()

            if process.returncode == 0:
                result.status = "SUCCESS"
            else:
                result.status = "FAILED"
                result.error_message = stderr

            result.end_time = datetime.utcnow()
            result.duration_seconds = (
                result.end_time - result.start_time
            ).total_seconds()

        except Exception as e:
            result.status = "FAILED"
            result.error_message = str(e)
            result.end_time = datetime.utcnow()

        return result

    def _submit_dataproc(
        self,
        job_name: str,
        job_path: str,
        config: Any,
        arguments: Dict[str, Any],
    ) -> SparkJobResult:
        """Submit job to Google Cloud Dataproc."""
        import os

        result = SparkJobResult(job_name=job_name)

        project = os.getenv("GCP_PROJECT", "enterprise-data-lake")
        region = os.getenv("DATAPROC_REGION", "us-central1")
        cluster = os.getenv("DATAPROC_CLUSTER", "apex-cluster")

        # Build gcloud command
        cmd = [
            "gcloud", "dataproc", "jobs", "submit", "pyspark",
            job_path,
            f"--cluster={cluster}",
            f"--region={region}",
            f"--project={project}",
            f"--properties=spark.executor.instances={config.executor_instances},"
            f"spark.executor.memory={config.executor_memory},"
            f"spark.executor.cores={config.executor_cores},"
            f"spark.driver.memory={config.driver_memory}",
            "--",
        ]

        # Add job arguments
        for key, value in arguments.items():
            cmd.extend([f"--{key}", str(value)])

        try:
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )

            stdout, stderr = process.communicate()

            if process.returncode == 0:
                result.status = "SUCCESS"
            else:
                result.status = "FAILED"
                result.error_message = stderr

            result.end_time = datetime.utcnow()
            result.duration_seconds = (
                result.end_time - result.start_time
            ).total_seconds()

        except Exception as e:
            result.status = "FAILED"
            result.error_message = str(e)

        return result

    def _build_spark_submit_command(
        self,
        job_path: str,
        config: Any,
        arguments: Dict[str, Any],
    ) -> List[str]:
        """Build spark-submit command."""
        spark_home = self.config.get("spark_home", "/opt/spark")

        cmd = [
            f"{spark_home}/bin/spark-submit",
            "--master", self.config.get("spark_master", "yarn"),
            "--deploy-mode", "cluster",
            "--num-executors", str(config.executor_instances),
            "--executor-memory", config.executor_memory,
            "--executor-cores", str(config.executor_cores),
            "--driver-memory", config.driver_memory,
            "--conf", f"spark.sql.shuffle.partitions={config.shuffle_partitions}",
        ]

        # Add adaptive query execution if enabled
        if config.adaptive_enabled:
            cmd.extend([
                "--conf", "spark.sql.adaptive.enabled=true",
                "--conf", "spark.sql.adaptive.coalescePartitions.enabled=true",
            ])

        # Add extra configurations
        for key, value in config.extra_conf.items():
            cmd.extend(["--conf", f"{key}={value}"])

        # Add the job script
        cmd.append(job_path)

        # Add job arguments
        for key, value in arguments.items():
            cmd.extend([f"--{key}", str(value)])

        return cmd

    def _wait_for_completion(
        self,
        result: SparkJobResult,
        timeout_seconds: int = 7200,
        poll_interval: int = 30,
    ) -> SparkJobResult:
        """Wait for job completion."""
        if result.status in ["SUCCESS", "FAILED"]:
            return result

        start = time.time()
        while time.time() - start < timeout_seconds:
            # Poll for status
            # This is a placeholder - actual implementation depends on cluster type
            time.sleep(poll_interval)

            # Check if job completed
            # result.status = self._get_job_status(result.application_id)

            if result.status in ["SUCCESS", "FAILED"]:
                break

        if result.status not in ["SUCCESS", "FAILED"]:
            result.status = "TIMEOUT"
            result.error_message = f"Job timed out after {timeout_seconds} seconds"

        return result

    def get_job_metrics(self, application_id: str) -> Dict[str, Any]:
        """Get metrics for a completed job."""
        # Placeholder - would query Spark History Server
        return {
            "records_read": 0,
            "records_written": 0,
            "bytes_read": 0,
            "bytes_written": 0,
        }
