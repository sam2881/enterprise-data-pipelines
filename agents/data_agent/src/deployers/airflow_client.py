"""
Local Airflow REST API client.

WHY: Syncs DAGs to local Airflow (Docker) instead of Cloud Composer.
HOW: Uses Airflow REST API for DAG management and triggering.

Architecture:
- DAGs are deployed by writing to the mounted DAGs folder
- DAG runs are triggered via Airflow REST API
- Status is checked via Airflow REST API

This replaces ComposerClient for local/Docker deployments.
"""

import ast
import os
import shutil
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
import structlog

logger = structlog.get_logger()


class AirflowClient:
    """
    Local Airflow REST API client for DAG management.

    This client handles:
    - Uploading DAGs to the local DAGs folder
    - Triggering DAG runs via REST API
    - Getting DAG run status
    - Listing available DAGs
    """

    def __init__(
        self,
        api_url: str = "http://localhost:8083",
        username: str = "admin",
        password: str = "admin123",
        dags_folder: str = "/opt/airflow/dags",
    ) -> None:
        """
        Initialize Airflow client.

        Args:
            api_url: Airflow webserver URL (default: http://localhost:8083)
            username: Airflow username (default: admin)
            password: Airflow password (default: admin123)
            dags_folder: Local path to DAGs folder (mounted volume)
        """
        self.api_url = api_url.rstrip("/")
        self.username = username
        self.password = password
        self.dags_folder = Path(dags_folder)
        self._session: Optional[requests.Session] = None

        logger.info(
            "AirflowClient initialized",
            api_url=self.api_url,
            dags_folder=str(self.dags_folder),
        )

    def _get_session(self) -> requests.Session:
        """Get or create authenticated session."""
        if self._session is None:
            self._session = requests.Session()
            self._session.auth = (self.username, self.password)
            self._session.headers.update({
                "Content-Type": "application/json",
                "Accept": "application/json",
            })
        return self._session

    def _api_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict] = None,
        params: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """
        Make authenticated API request to Airflow.

        Args:
            method: HTTP method (GET, POST, PATCH, DELETE)
            endpoint: API endpoint (e.g., "/api/v1/dags")
            data: Request body (for POST/PATCH)
            params: Query parameters

        Returns:
            Response JSON
        """
        session = self._get_session()
        url = f"{self.api_url}{endpoint}"

        try:
            response = session.request(
                method=method,
                url=url,
                json=data,
                params=params,
                timeout=30,
            )
            response.raise_for_status()
            return response.json() if response.content else {}

        except requests.exceptions.ConnectionError:
            logger.error("Cannot connect to Airflow", url=url)
            raise ConnectionError(
                f"Cannot connect to Airflow at {self.api_url}. "
                "Make sure Airflow is running."
            )
        except requests.exceptions.HTTPError as e:
            logger.error(
                "Airflow API error",
                status_code=e.response.status_code,
                response=e.response.text,
            )
            raise

    def health_check(self) -> bool:
        """
        Check if Airflow is healthy and accessible.

        Returns:
            True if healthy, False otherwise
        """
        try:
            response = self._api_request("GET", "/api/v1/health")
            return response.get("metadatabase", {}).get("status") == "healthy"
        except Exception as e:
            logger.warning("Airflow health check failed", error=str(e))
            return False

    def validate_dag_syntax(self, dag_content: str) -> Tuple[bool, Optional[str]]:
        """
        Validate DAG Python syntax.

        Args:
            dag_content: DAG Python code

        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            # Parse Python syntax
            ast.parse(dag_content)

            # Optional: Try to import and validate with Airflow
            try:
                with tempfile.NamedTemporaryFile(
                    mode='w', suffix='.py', delete=False
                ) as f:
                    f.write(dag_content)
                    temp_path = f.name

                try:
                    import logging
                    logging.disable(logging.WARNING)

                    from airflow.models import DagBag

                    dag_bag = DagBag(
                        dag_folder=os.path.dirname(temp_path),
                        include_examples=False,
                    )

                    logging.disable(logging.NOTSET)

                    if dag_bag.import_errors:
                        errors = list(dag_bag.import_errors.values())
                        return False, f"Airflow import error: {errors[0]}"

                    return True, None

                finally:
                    os.unlink(temp_path)

            except ImportError:
                logger.debug("Airflow not installed locally, using syntax check only")
                return True, None

        except SyntaxError as e:
            return False, f"Syntax error at line {e.lineno}: {e.msg}"
        except Exception as e:
            return False, f"Validation error: {str(e)}"

    def upload_dag(
        self,
        dag_path: str,
        dag_content: str,
        validate: bool = True,
    ) -> str:
        """
        Upload a DAG to the local Airflow DAGs folder.

        Args:
            dag_path: Relative path within DAGs folder (e.g., "my_dag.py")
            dag_content: DAG Python code
            validate: Whether to validate syntax before upload

        Returns:
            Full path to the uploaded DAG file
        """
        if validate:
            is_valid, error = self.validate_dag_syntax(dag_content)
            if not is_valid:
                raise ValueError(f"DAG validation failed: {error}")

        # Ensure dag_path doesn't start with /
        dag_path = dag_path.lstrip("/")

        # Full file path
        full_path = self.dags_folder / dag_path

        # Create parent directories if needed
        full_path.parent.mkdir(parents=True, exist_ok=True)

        # Write DAG file
        full_path.write_text(dag_content)

        logger.info(
            "DAG uploaded to local folder",
            dag_path=dag_path,
            full_path=str(full_path),
        )

        return str(full_path)

    def upload_dags_batch(
        self,
        dags: Dict[str, str],
        validate: bool = True,
    ) -> Dict[str, str]:
        """
        Upload multiple DAGs to the local folder.

        Args:
            dags: Dict of dag_path -> dag_content
            validate: Whether to validate syntax before upload

        Returns:
            Dict of dag_path -> full_path (or error message)
        """
        results = {}

        for dag_path, dag_content in dags.items():
            try:
                full_path = self.upload_dag(dag_path, dag_content, validate)
                results[dag_path] = full_path
            except Exception as e:
                logger.error("Failed to upload DAG", dag_path=dag_path, error=str(e))
                results[dag_path] = f"ERROR: {str(e)}"

        return results

    def delete_dag(self, dag_path: str) -> None:
        """
        Delete a DAG from the local folder.

        Args:
            dag_path: Relative path within DAGs folder
        """
        dag_path = dag_path.lstrip("/")
        full_path = self.dags_folder / dag_path

        if full_path.exists():
            full_path.unlink()
            logger.info("DAG deleted", dag_path=dag_path)
        else:
            logger.warning("DAG file not found", dag_path=dag_path)

    def list_dags(self, prefix: Optional[str] = None) -> List[str]:
        """
        List DAGs from the local folder.

        Args:
            prefix: Optional prefix filter

        Returns:
            List of DAG file paths (relative to dags_folder)
        """
        dag_paths = []

        search_path = self.dags_folder
        if prefix:
            search_path = self.dags_folder / prefix.lstrip("/")

        if search_path.exists():
            for dag_file in search_path.rglob("*.py"):
                if not dag_file.name.startswith("__"):
                    relative_path = dag_file.relative_to(self.dags_folder)
                    dag_paths.append(str(relative_path))

        logger.info("Listed local DAGs", count=len(dag_paths), prefix=prefix)
        return dag_paths

    def list_dags_from_api(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        List DAGs from Airflow API (registered DAGs).

        Args:
            limit: Maximum number of DAGs to return

        Returns:
            List of DAG info dicts
        """
        response = self._api_request(
            "GET",
            "/api/v1/dags",
            params={"limit": limit},
        )
        return response.get("dags", [])

    def get_dag(self, dag_id: str) -> Dict[str, Any]:
        """
        Get DAG details from Airflow API.

        Args:
            dag_id: DAG ID

        Returns:
            DAG info dict
        """
        return self._api_request("GET", f"/api/v1/dags/{dag_id}")

    def trigger_dag(
        self,
        dag_id: str,
        conf: Optional[Dict[str, Any]] = None,
        run_id: Optional[str] = None,
        logical_date: Optional[str] = None,
    ) -> str:
        """
        Trigger a DAG run via Airflow REST API.

        Args:
            dag_id: ID of DAG to trigger
            conf: Optional DAG run configuration
            run_id: Optional run ID (auto-generated if not provided)
            logical_date: Optional logical date (ISO format)

        Returns:
            DAG run ID
        """
        if run_id is None:
            run_id = f"manual__{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}"

        payload = {
            "dag_run_id": run_id,
            "conf": conf or {},
        }

        if logical_date:
            payload["logical_date"] = logical_date

        response = self._api_request(
            "POST",
            f"/api/v1/dags/{dag_id}/dagRuns",
            data=payload,
        )

        dag_run_id = response.get("dag_run_id", run_id)

        logger.info(
            "DAG triggered",
            dag_id=dag_id,
            run_id=dag_run_id,
        )

        return dag_run_id

    def get_dag_run_status(self, dag_id: str, run_id: str) -> Dict[str, Any]:
        """
        Get the status of a DAG run.

        Args:
            dag_id: DAG ID
            run_id: DAG run ID

        Returns:
            Dict with run status info
        """
        return self._api_request(
            "GET",
            f"/api/v1/dags/{dag_id}/dagRuns/{run_id}",
        )

    def wait_for_dag_run(
        self,
        dag_id: str,
        run_id: str,
        timeout_seconds: int = 300,
        poll_interval: int = 10,
    ) -> Dict[str, Any]:
        """
        Wait for a DAG run to complete.

        Args:
            dag_id: DAG ID
            run_id: DAG run ID
            timeout_seconds: Maximum wait time
            poll_interval: Seconds between status checks

        Returns:
            Final DAG run status
        """
        import time

        start_time = time.time()
        terminal_states = {"success", "failed", "upstream_failed"}

        while time.time() - start_time < timeout_seconds:
            status = self.get_dag_run_status(dag_id, run_id)
            state = status.get("state", "").lower()

            if state in terminal_states:
                logger.info(
                    "DAG run completed",
                    dag_id=dag_id,
                    run_id=run_id,
                    state=state,
                )
                return status

            logger.debug(
                "DAG run in progress",
                dag_id=dag_id,
                run_id=run_id,
                state=state,
            )
            time.sleep(poll_interval)

        raise TimeoutError(
            f"DAG run {run_id} did not complete within {timeout_seconds} seconds"
        )

    def pause_dag(self, dag_id: str) -> Dict[str, Any]:
        """
        Pause a DAG.

        Args:
            dag_id: DAG ID

        Returns:
            Updated DAG info
        """
        return self._api_request(
            "PATCH",
            f"/api/v1/dags/{dag_id}",
            data={"is_paused": True},
        )

    def unpause_dag(self, dag_id: str) -> Dict[str, Any]:
        """
        Unpause a DAG.

        Args:
            dag_id: DAG ID

        Returns:
            Updated DAG info
        """
        return self._api_request(
            "PATCH",
            f"/api/v1/dags/{dag_id}",
            data={"is_paused": False},
        )

    def get_task_instances(
        self,
        dag_id: str,
        run_id: str,
    ) -> List[Dict[str, Any]]:
        """
        Get task instances for a DAG run.

        Args:
            dag_id: DAG ID
            run_id: DAG run ID

        Returns:
            List of task instance info
        """
        response = self._api_request(
            "GET",
            f"/api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances",
        )
        return response.get("task_instances", [])

    def upload_spark_job(
        self,
        job_path: str,
        job_content: str,
        spark_jobs_folder: Optional[str] = None,
    ) -> str:
        """
        Upload a Spark job file to the designated folder.

        Args:
            job_path: Relative path for the Spark job
            job_content: Spark job Python code
            spark_jobs_folder: Folder for Spark jobs (default: dags_folder/spark)

        Returns:
            Full path to the uploaded Spark job
        """
        if spark_jobs_folder is None:
            spark_folder = self.dags_folder / "spark"
        else:
            spark_folder = Path(spark_jobs_folder)

        job_path = job_path.lstrip("/")
        full_path = spark_folder / job_path

        full_path.parent.mkdir(parents=True, exist_ok=True)
        full_path.write_text(job_content)

        logger.info(
            "Spark job uploaded",
            job_path=job_path,
            full_path=str(full_path),
        )

        return str(full_path)


# =============================================================================
# Factory function
# =============================================================================


def get_airflow_client(
    api_url: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    dags_folder: Optional[str] = None,
) -> AirflowClient:
    """
    Get an AirflowClient with settings from environment or config.

    Args:
        api_url: Airflow API URL (default: from env/config)
        username: Airflow username (default: from env/config)
        password: Airflow password (default: from env/config)
        dags_folder: DAGs folder path (default: from env/config)

    Returns:
        AirflowClient instance
    """
    # Try to get from settings first
    try:
        from src.config.settings import get_settings
        settings = get_settings()

        return AirflowClient(
            api_url=api_url or settings.airflow_api_url,
            username=username or settings.airflow_username,
            password=password or settings.airflow_password,
            dags_folder=dags_folder or settings.airflow_dags_folder,
        )
    except (ImportError, AttributeError):
        pass

    # Fallback to environment variables
    return AirflowClient(
        api_url=api_url or os.getenv("AIRFLOW_API_URL", "http://localhost:8083"),
        username=username or os.getenv("AIRFLOW_USERNAME", "admin"),
        password=password or os.getenv("AIRFLOW_PASSWORD", "admin123"),
        dags_folder=dags_folder or os.getenv("AIRFLOW_DAGS_FOLDER", "/opt/airflow/dags"),
    )
