"""
Cloud Composer DAG sync client.

WHY: Synchronizes generated DAGs to Cloud Composer (managed Airflow).
HOW: Uses GCS client to upload DAGs to Composer's bucket, Airflow API for triggering.

RULES:
1. Verify DAG syntax before upload
2. Use environment-specific buckets
3. Track sync status
4. Support rollback on failure
"""

import ast
import os
import tempfile
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import structlog

logger = structlog.get_logger()


class ComposerClient:
    """
    Cloud Composer client for DAG synchronization.

    This client handles:
    - Getting the DAGs bucket from Composer environment
    - Uploading/deleting DAGs from GCS
    - Triggering DAG runs via Airflow API
    - Validating DAG syntax before upload
    """

    def __init__(
        self,
        project_id: str,
        environment_name: str,
        region: str = "us-central1",
    ) -> None:
        """
        Initialize Composer client.

        Args:
            project_id: GCP project ID
            environment_name: Composer environment name
            region: GCP region (default: us-central1)
        """
        self.project_id = project_id
        self.environment_name = environment_name
        self.region = region

        # Lazy-loaded clients
        self._storage_client = None
        self._composer_client = None
        self._airflow_uri: Optional[str] = None
        self._dags_bucket: Optional[str] = None
        self._dags_prefix: Optional[str] = None

        logger.info(
            "ComposerClient initialized",
            project_id=project_id,
            environment=environment_name,
            region=region,
        )

    def _get_storage_client(self):
        """Get or create GCS storage client."""
        if self._storage_client is None:
            try:
                from google.cloud import storage
                self._storage_client = storage.Client(project=self.project_id)
            except ImportError:
                raise ImportError(
                    "google-cloud-storage is required. "
                    "Install with: pip install google-cloud-storage"
                )
        return self._storage_client

    def _get_composer_info(self) -> Tuple[str, str, str]:
        """
        Get Composer environment info (DAGs bucket, prefix, Airflow URI).

        Returns:
            Tuple of (dags_bucket, dags_prefix, airflow_uri)
        """
        if self._dags_bucket and self._dags_prefix:
            return self._dags_bucket, self._dags_prefix, self._airflow_uri

        try:
            from google.cloud.orchestration.airflow import service_v1
            from google.cloud.orchestration.airflow.service_v1 import types

            client = service_v1.EnvironmentsClient()

            environment_name = (
                f"projects/{self.project_id}/locations/{self.region}/"
                f"environments/{self.environment_name}"
            )

            env = client.get_environment(name=environment_name)

            # Parse DAGs GCS prefix: gs://bucket-name/dags
            dags_gcs_prefix = env.config.dag_gcs_prefix
            parts = dags_gcs_prefix.replace("gs://", "").split("/", 1)
            self._dags_bucket = parts[0]
            self._dags_prefix = parts[1] if len(parts) > 1 else "dags"
            self._airflow_uri = env.config.airflow_uri

            logger.info(
                "Got Composer environment info",
                dags_bucket=self._dags_bucket,
                dags_prefix=self._dags_prefix,
                airflow_uri=self._airflow_uri,
            )

            return self._dags_bucket, self._dags_prefix, self._airflow_uri

        except ImportError:
            raise ImportError(
                "google-cloud-orchestration-airflow is required. "
                "Install with: pip install google-cloud-orchestration-airflow"
            )
        except Exception as e:
            logger.error("Failed to get Composer environment info", error=str(e))
            raise

    def get_dags_bucket(self) -> str:
        """
        Get the DAGs bucket for the Composer environment.

        Returns:
            GCS bucket name (without gs:// prefix)
        """
        bucket, _, _ = self._get_composer_info()
        return bucket

    def get_dags_gcs_prefix(self) -> str:
        """
        Get the full GCS prefix for DAGs.

        Returns:
            Full GCS URI (gs://bucket/prefix)
        """
        bucket, prefix, _ = self._get_composer_info()
        return f"gs://{bucket}/{prefix}"

    def get_airflow_uri(self) -> str:
        """
        Get the Airflow web UI URI.

        Returns:
            Airflow web UI URI
        """
        _, _, uri = self._get_composer_info()
        return uri

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

            # Optional: Try to import and validate Airflow DAG
            # This requires Airflow to be installed
            try:
                with tempfile.NamedTemporaryFile(
                    mode='w', suffix='.py', delete=False
                ) as f:
                    f.write(dag_content)
                    temp_path = f.name

                try:
                    # Suppress Airflow logging during validation
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
                # Airflow not installed, just use syntax check
                logger.debug("Airflow not installed, using syntax check only")
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
        Upload a DAG to the Composer environment.

        Args:
            dag_path: Path within DAGs folder (e.g., "my_dag.py")
            dag_content: DAG Python code
            validate: Whether to validate syntax before upload (default: True)

        Returns:
            GCS URI of uploaded DAG
        """
        if validate:
            is_valid, error = self.validate_dag_syntax(dag_content)
            if not is_valid:
                raise ValueError(f"DAG validation failed: {error}")

        bucket_name, prefix, _ = self._get_composer_info()
        storage_client = self._get_storage_client()

        # Ensure dag_path doesn't start with /
        dag_path = dag_path.lstrip("/")

        # Full blob path
        blob_path = f"{prefix}/{dag_path}"

        # Upload to GCS
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        blob.upload_from_string(dag_content, content_type="text/x-python")

        gcs_uri = f"gs://{bucket_name}/{blob_path}"

        logger.info(
            "DAG uploaded",
            dag_path=dag_path,
            gcs_uri=gcs_uri,
        )

        return gcs_uri

    def upload_dags_batch(
        self,
        dags: Dict[str, str],
        validate: bool = True,
    ) -> Dict[str, str]:
        """
        Upload multiple DAGs to the Composer environment.

        Args:
            dags: Dict of dag_path -> dag_content
            validate: Whether to validate syntax before upload

        Returns:
            Dict of dag_path -> gcs_uri
        """
        results = {}

        for dag_path, dag_content in dags.items():
            try:
                gcs_uri = self.upload_dag(dag_path, dag_content, validate)
                results[dag_path] = gcs_uri
            except Exception as e:
                logger.error("Failed to upload DAG", dag_path=dag_path, error=str(e))
                results[dag_path] = f"ERROR: {str(e)}"

        return results

    def delete_dag(self, dag_path: str) -> None:
        """
        Delete a DAG from the Composer environment.

        Args:
            dag_path: Path within DAGs folder
        """
        bucket_name, prefix, _ = self._get_composer_info()
        storage_client = self._get_storage_client()

        dag_path = dag_path.lstrip("/")
        blob_path = f"{prefix}/{dag_path}"

        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_path)

        if blob.exists():
            blob.delete()
            logger.info("DAG deleted", dag_path=dag_path)
        else:
            logger.warning("DAG not found", dag_path=dag_path)

    def list_dags(self, prefix: Optional[str] = None) -> List[str]:
        """
        List DAGs in the Composer environment.

        Args:
            prefix: Optional prefix filter (within dags folder)

        Returns:
            List of DAG file paths
        """
        bucket_name, dags_prefix, _ = self._get_composer_info()
        storage_client = self._get_storage_client()

        # Build search prefix
        search_prefix = dags_prefix
        if prefix:
            search_prefix = f"{dags_prefix}/{prefix.lstrip('/')}"

        bucket = storage_client.bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=search_prefix)

        dag_paths = []
        for blob in blobs:
            if blob.name.endswith(".py"):
                # Remove the dags prefix to get relative path
                relative_path = blob.name[len(dags_prefix):].lstrip("/")
                dag_paths.append(relative_path)

        logger.info("Listed DAGs", count=len(dag_paths), prefix=prefix)
        return dag_paths

    def trigger_dag(
        self,
        dag_id: str,
        conf: Optional[Dict[str, Any]] = None,
        run_id: Optional[str] = None,
    ) -> str:
        """
        Trigger a DAG run via Airflow REST API.

        Args:
            dag_id: ID of DAG to trigger
            conf: Optional DAG run configuration
            run_id: Optional run ID (auto-generated if not provided)

        Returns:
            DAG run ID
        """
        _, _, airflow_uri = self._get_composer_info()

        if not airflow_uri:
            raise ValueError("Airflow URI not available")

        try:
            import requests
            from google.auth.transport.requests import Request
            from google.oauth2 import id_token

            # Get ID token for IAP authentication
            credentials_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
            if credentials_path:
                audience = airflow_uri.split("/")[2]  # Extract host
                token = id_token.fetch_id_token(Request(), audience)
            else:
                # Try default credentials
                from google.auth import default
                credentials, _ = default()
                credentials.refresh(Request())
                token = credentials.token

            # Build request
            if run_id is None:
                run_id = f"manual__{datetime.utcnow().isoformat()}"

            api_url = f"{airflow_uri}/api/v1/dags/{dag_id}/dagRuns"

            payload = {
                "dag_run_id": run_id,
                "conf": conf or {},
            }

            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }

            response = requests.post(api_url, json=payload, headers=headers)
            response.raise_for_status()

            result = response.json()
            logger.info(
                "DAG triggered",
                dag_id=dag_id,
                run_id=result.get("dag_run_id"),
            )

            return result.get("dag_run_id", run_id)

        except ImportError:
            raise ImportError(
                "requests and google-auth are required. "
                "Install with: pip install requests google-auth"
            )
        except Exception as e:
            logger.error("Failed to trigger DAG", dag_id=dag_id, error=str(e))
            raise

    def get_dag_run_status(self, dag_id: str, run_id: str) -> Dict[str, Any]:
        """
        Get the status of a DAG run.

        Args:
            dag_id: DAG ID
            run_id: DAG run ID

        Returns:
            Dict with run status info
        """
        _, _, airflow_uri = self._get_composer_info()

        if not airflow_uri:
            raise ValueError("Airflow URI not available")

        try:
            import requests
            from google.auth.transport.requests import Request
            from google.oauth2 import id_token

            # Get token
            credentials_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
            if credentials_path:
                audience = airflow_uri.split("/")[2]
                token = id_token.fetch_id_token(Request(), audience)
            else:
                from google.auth import default
                credentials, _ = default()
                credentials.refresh(Request())
                token = credentials.token

            api_url = f"{airflow_uri}/api/v1/dags/{dag_id}/dagRuns/{run_id}"

            headers = {"Authorization": f"Bearer {token}"}

            response = requests.get(api_url, headers=headers)
            response.raise_for_status()

            return response.json()

        except Exception as e:
            logger.error(
                "Failed to get DAG run status",
                dag_id=dag_id,
                run_id=run_id,
                error=str(e),
            )
            raise


# =============================================================================
# Factory function
# =============================================================================


def get_composer_client(
    project_id: Optional[str] = None,
    environment_name: Optional[str] = None,
    region: Optional[str] = None,
) -> ComposerClient:
    """
    Get a ComposerClient with settings from environment or config.

    Args:
        project_id: GCP project ID (default: from env)
        environment_name: Composer environment name (default: from env)
        region: GCP region (default: from env)

    Returns:
        ComposerClient instance
    """
    from src.config.settings import get_settings

    settings = get_settings()

    return ComposerClient(
        project_id=project_id or settings.gcp_project_id,
        environment_name=environment_name or settings.composer_environment,
        region=region or settings.gcp_region,
    )
