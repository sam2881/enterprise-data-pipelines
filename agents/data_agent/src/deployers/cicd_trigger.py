"""
CI/CD pipeline trigger for Google Cloud Build.

WHY: Triggers Cloud Build pipelines after artifact deployment.
     Provides build status tracking and wait functionality.

RULES:
1. Only trigger for approved changes
2. Track build status
3. Report failures immediately
4. Log all triggers for audit
"""

from typing import Any, Dict, Optional
import time
import structlog

from src.config.settings import get_settings
from src.utils.exceptions import CICDError

logger = structlog.get_logger()


class CICDTrigger:
    """
    CI/CD pipeline trigger for Cloud Build.

    Uses Google Cloud Build API to:
    - Trigger builds
    - Check build status
    - Wait for completion
    """

    def __init__(
        self,
        project_id: Optional[str] = None,
        region: str = "us-central1",
    ) -> None:
        """Initialize CI/CD trigger client."""
        settings = get_settings()

        self.project_id = project_id or settings.gcp_project_id
        self.region = region or settings.gcp_region

        self._client = None

    @property
    def client(self):
        """Get or create Cloud Build client."""
        if self._client is None:
            try:
                from google.cloud import cloudbuild_v1
                self._client = cloudbuild_v1.CloudBuildClient()
            except ImportError:
                logger.warning("google-cloud-build not available")
                self._client = None
        return self._client

    def trigger_build(
        self,
        branch: str,
        substitutions: Optional[Dict[str, str]] = None,
        trigger_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Trigger a Cloud Build pipeline.

        Args:
            branch: Git branch to build
            substitutions: Optional build substitutions
            trigger_id: Optional Cloud Build trigger ID

        Returns:
            Dict with build_id and status
        """
        logger.info(
            "Triggering Cloud Build",
            project=self.project_id,
            branch=branch,
            trigger_id=trigger_id,
        )

        if not self.client:
            # Return mock response if Cloud Build not available
            logger.warning("Cloud Build client not available, returning mock response")
            return {
                "build_id": f"mock-build-{int(time.time())}",
                "status": "QUEUED",
                "log_url": None,
            }

        try:
            from google.cloud.devtools.cloudbuild_v1 import Build, RepoSource

            if trigger_id:
                # Run trigger
                request = {
                    "project_id": self.project_id,
                    "trigger_id": trigger_id,
                    "source": {
                        "branch_name": branch,
                        "substitutions": substitutions or {},
                    },
                }
                operation = self.client.run_build_trigger(request=request)
                build = operation.result()

            else:
                # Create inline build
                build_config = Build(
                    source={"repo_source": RepoSource(branch_name=branch)},
                    substitutions=substitutions or {},
                    steps=[
                        {
                            "name": "gcr.io/cloud-builders/gcloud",
                            "args": ["version"],
                        }
                    ],
                )

                request = {
                    "project_id": self.project_id,
                    "build": build_config,
                }
                operation = self.client.create_build(request=request)
                build = operation.result()

            logger.info(
                "Build triggered",
                build_id=build.id,
                status=build.status.name,
            )

            return {
                "build_id": build.id,
                "status": build.status.name,
                "log_url": build.log_url,
            }

        except Exception as e:
            logger.error("Failed to trigger build", error=str(e))
            raise CICDError(f"Failed to trigger Cloud Build: {e}")

    def get_build_status(self, build_id: str) -> Dict[str, Any]:
        """
        Get status of a build.

        Args:
            build_id: Build ID to check

        Returns:
            Build status dict with status, log_url, etc.
        """
        logger.debug("Checking build status", build_id=build_id)

        if not self.client:
            return {
                "build_id": build_id,
                "status": "UNKNOWN",
                "log_url": None,
            }

        try:
            request = {
                "project_id": self.project_id,
                "id": build_id,
            }
            build = self.client.get_build(request=request)

            return {
                "build_id": build.id,
                "status": build.status.name,
                "log_url": build.log_url,
                "create_time": build.create_time.isoformat() if build.create_time else None,
                "finish_time": build.finish_time.isoformat() if build.finish_time else None,
            }

        except Exception as e:
            logger.error("Failed to get build status", build_id=build_id, error=str(e))
            raise CICDError(f"Failed to get build status: {e}")

    def wait_for_completion(
        self,
        build_id: str,
        timeout_seconds: int = 1800,
        poll_interval: int = 30,
    ) -> Dict[str, Any]:
        """
        Wait for build to complete.

        Args:
            build_id: Build ID to wait for
            timeout_seconds: Maximum wait time (default: 30 minutes)
            poll_interval: Seconds between status checks

        Returns:
            Final build status dict
        """
        logger.info(
            "Waiting for build completion",
            build_id=build_id,
            timeout=timeout_seconds,
        )

        terminal_statuses = {
            "SUCCESS",
            "FAILURE",
            "INTERNAL_ERROR",
            "TIMEOUT",
            "CANCELLED",
            "EXPIRED",
        }

        start_time = time.time()

        while time.time() - start_time < timeout_seconds:
            status = self.get_build_status(build_id)

            if status["status"] in terminal_statuses:
                logger.info(
                    "Build completed",
                    build_id=build_id,
                    status=status["status"],
                    duration=time.time() - start_time,
                )
                return status

            logger.debug(
                "Build in progress",
                build_id=build_id,
                status=status["status"],
            )

            time.sleep(poll_interval)

        # Timeout
        raise CICDError(f"Build {build_id} timed out after {timeout_seconds} seconds")

    def cancel_build(self, build_id: str) -> bool:
        """
        Cancel a running build.

        Args:
            build_id: Build ID to cancel

        Returns:
            True if cancelled successfully
        """
        logger.info("Cancelling build", build_id=build_id)

        if not self.client:
            return False

        try:
            request = {
                "project_id": self.project_id,
                "id": build_id,
            }
            self.client.cancel_build(request=request)

            logger.info("Build cancelled", build_id=build_id)
            return True

        except Exception as e:
            logger.error("Failed to cancel build", build_id=build_id, error=str(e))
            return False
