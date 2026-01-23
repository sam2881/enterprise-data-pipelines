"""
GitHub Actions Client - Trigger and Monitor Workflows

NOTE: For event-driven architecture, prefer using GitHub MCP Server via Kafka:
    - Use trigger_workflow_via_mcp() from backend/orchestrator/langgraph_workflow.py
    - Kafka topic: github.trigger_workflow
    - This ensures proper event sourcing and decoupling

This client can still be used for:
    - Direct API calls in non-event-driven contexts
    - Testing and debugging
    - Backward compatibility

WHY: Enables safe execution of infrastructure changes via GitHub Actions:
- Audit trail in GitHub
- Approval workflows built-in
- Secrets management handled by GitHub
- Rollback via Git history

HOW: Uses GitHub REST API to trigger workflow_dispatch events
and poll for completion status.

REPOSITORIES:
    - sam2881/test_01: Remediation scripts (Terraform, Ansible, Shell)
    - sam2881/enterprise-data-pipelines: Data Agent DAGs (use GitHub MCP for PRs)

Usage:
    # For direct API calls (non-event-driven):
    client = GitHubActionsClient()
    run_id = await client.trigger_workflow(
        "terraform-apply.yml",
        {"script_path": "scale_gcp_instance.tf", "environment": "prod"}
    )
    result = await client.wait_for_completion(run_id)

    # For event-driven (PREFERRED):
    from backend.orchestrator.langgraph_workflow import trigger_workflow_via_mcp
    await trigger_workflow_via_mcp("terraform-apply.yml", {"script_path": "..."})
"""
import asyncio
import os
from typing import Any, Dict, List, Optional
from dataclasses import dataclass
from datetime import datetime
import httpx
import structlog

logger = structlog.get_logger()


@dataclass
class WorkflowRun:
    """Represents a GitHub Actions workflow run"""
    id: int
    name: str
    status: str  # queued, in_progress, completed
    conclusion: Optional[str]  # success, failure, cancelled, etc.
    html_url: str
    created_at: str
    updated_at: str
    run_started_at: Optional[str] = None
    run_attempt: int = 1


@dataclass
class WorkflowResult:
    """Result of a completed workflow run"""
    run_id: int
    success: bool
    conclusion: str
    duration_seconds: float
    logs_url: str
    artifacts: List[Dict[str, Any]]
    outputs: Dict[str, Any]


class GitHubActionsClient:
    """
    Client for triggering and monitoring GitHub Actions workflows.

    WHY GitHub Actions:
    - Auditable execution history
    - Built-in secrets management
    - Approval gates via environments
    - Rollback via Git history
    - Integration with cloud providers
    """

    def __init__(
        self,
        owner: Optional[str] = None,
        repo: Optional[str] = None,
        token: Optional[str] = None,
        poll_interval: float = 5.0,
        timeout: float = 600.0  # 10 minutes
    ):
        # Use remediation repo by default (test_01)
        self.owner = owner or os.getenv("GITHUB_REMEDIATION_OWNER", os.getenv("GITHUB_OWNER", "sam2881"))
        self.repo = repo or os.getenv("GITHUB_REMEDIATION_REPO", os.getenv("GITHUB_REPO", "test_01"))
        self.token = token or os.getenv("GITHUB_REMEDIATION_TOKEN", os.getenv("GITHUB_TOKEN"))
        self.poll_interval = poll_interval
        self.timeout = timeout

        self.base_url = f"https://api.github.com/repos/{self.owner}/{self.repo}"
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/vnd.github.v3+json",
            "X-GitHub-Api-Version": "2022-11-28"
        }

        logger.info(
            "github_actions_client_initialized",
            owner=self.owner,
            repo=self.repo
        )

    async def trigger_workflow(
        self,
        workflow_file: str,
        inputs: Dict[str, Any],
        ref: str = "main"
    ) -> int:
        """
        Trigger a GitHub Actions workflow via workflow_dispatch.

        Args:
            workflow_file: Workflow filename (e.g., "terraform-apply.yml")
            inputs: Input parameters for the workflow
            ref: Git ref to run workflow on (default: main)

        Returns:
            Workflow run ID
        """
        url = f"{self.base_url}/actions/workflows/{workflow_file}/dispatches"

        payload = {
            "ref": ref,
            "inputs": {k: str(v) for k, v in inputs.items()}
        }

        async with httpx.AsyncClient() as client:
            response = await client.post(
                url,
                headers=self.headers,
                json=payload
            )

            if response.status_code not in [204, 200]:
                logger.error(
                    "workflow_trigger_failed",
                    status=response.status_code,
                    body=response.text
                )
                raise GitHubActionsError(f"Failed to trigger workflow: {response.text}")

        # Wait a moment for the run to be created
        await asyncio.sleep(2)

        # Get the latest run ID for this workflow
        run = await self._get_latest_run(workflow_file)

        logger.info(
            "workflow_triggered",
            workflow=workflow_file,
            run_id=run.id,
            inputs=inputs
        )

        return run.id

    async def _get_latest_run(self, workflow_file: str) -> WorkflowRun:
        """Get the most recent run for a workflow"""
        url = f"{self.base_url}/actions/workflows/{workflow_file}/runs"

        async with httpx.AsyncClient() as client:
            response = await client.get(
                url,
                headers=self.headers,
                params={"per_page": 1}
            )

            if response.status_code != 200:
                raise GitHubActionsError(f"Failed to get workflow runs: {response.text}")

            data = response.json()
            runs = data.get("workflow_runs", [])

            if not runs:
                raise GitHubActionsError("No workflow runs found")

            run = runs[0]
            return WorkflowRun(
                id=run["id"],
                name=run["name"],
                status=run["status"],
                conclusion=run.get("conclusion"),
                html_url=run["html_url"],
                created_at=run["created_at"],
                updated_at=run["updated_at"],
                run_started_at=run.get("run_started_at"),
                run_attempt=run.get("run_attempt", 1)
            )

    async def get_run_status(self, run_id: int) -> WorkflowRun:
        """Get current status of a workflow run"""
        url = f"{self.base_url}/actions/runs/{run_id}"

        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=self.headers)

            if response.status_code != 200:
                raise GitHubActionsError(f"Failed to get run status: {response.text}")

            run = response.json()
            return WorkflowRun(
                id=run["id"],
                name=run["name"],
                status=run["status"],
                conclusion=run.get("conclusion"),
                html_url=run["html_url"],
                created_at=run["created_at"],
                updated_at=run["updated_at"],
                run_started_at=run.get("run_started_at"),
                run_attempt=run.get("run_attempt", 1)
            )

    async def wait_for_completion(
        self,
        run_id: int,
        timeout: Optional[float] = None
    ) -> WorkflowResult:
        """
        Wait for a workflow run to complete.

        Args:
            run_id: Workflow run ID
            timeout: Override default timeout

        Returns:
            WorkflowResult with completion details
        """
        timeout = timeout or self.timeout
        start_time = datetime.now()

        while True:
            run = await self.get_run_status(run_id)

            if run.status == "completed":
                # Calculate duration
                duration = (datetime.now() - start_time).total_seconds()

                # Get artifacts
                artifacts = await self._get_artifacts(run_id)

                logger.info(
                    "workflow_completed",
                    run_id=run_id,
                    conclusion=run.conclusion,
                    duration_seconds=duration
                )

                return WorkflowResult(
                    run_id=run_id,
                    success=run.conclusion == "success",
                    conclusion=run.conclusion or "unknown",
                    duration_seconds=duration,
                    logs_url=f"{run.html_url}/logs",
                    artifacts=artifacts,
                    outputs={}  # Would need to parse job outputs
                )

            # Check timeout
            elapsed = (datetime.now() - start_time).total_seconds()
            if elapsed > timeout:
                logger.error("workflow_timeout", run_id=run_id, elapsed=elapsed)
                raise GitHubActionsError(f"Workflow run {run_id} timed out after {elapsed}s")

            logger.debug(
                "workflow_in_progress",
                run_id=run_id,
                status=run.status,
                elapsed=elapsed
            )

            await asyncio.sleep(self.poll_interval)

    async def _get_artifacts(self, run_id: int) -> List[Dict[str, Any]]:
        """Get artifacts from a workflow run"""
        url = f"{self.base_url}/actions/runs/{run_id}/artifacts"

        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=self.headers)

            if response.status_code != 200:
                return []

            data = response.json()
            return [
                {
                    "id": a["id"],
                    "name": a["name"],
                    "size_in_bytes": a["size_in_bytes"],
                    "expired": a["expired"]
                }
                for a in data.get("artifacts", [])
            ]

    async def cancel_run(self, run_id: int) -> bool:
        """Cancel a running workflow"""
        url = f"{self.base_url}/actions/runs/{run_id}/cancel"

        async with httpx.AsyncClient() as client:
            response = await client.post(url, headers=self.headers)

            if response.status_code == 202:
                logger.info("workflow_cancelled", run_id=run_id)
                return True
            else:
                logger.warning(
                    "workflow_cancel_failed",
                    run_id=run_id,
                    status=response.status_code
                )
                return False

    async def rerun_workflow(self, run_id: int) -> int:
        """Rerun a failed workflow"""
        url = f"{self.base_url}/actions/runs/{run_id}/rerun"

        async with httpx.AsyncClient() as client:
            response = await client.post(url, headers=self.headers)

            if response.status_code == 201:
                # Get the new run ID
                run = await self.get_run_status(run_id)
                logger.info("workflow_rerun", original_run_id=run_id, new_run_id=run.id)
                return run.id
            else:
                raise GitHubActionsError(f"Failed to rerun workflow: {response.text}")

    async def list_workflows(self) -> List[Dict[str, Any]]:
        """List all workflows in the repository"""
        url = f"{self.base_url}/actions/workflows"

        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=self.headers)

            if response.status_code != 200:
                raise GitHubActionsError(f"Failed to list workflows: {response.text}")

            data = response.json()
            return [
                {
                    "id": w["id"],
                    "name": w["name"],
                    "path": w["path"],
                    "state": w["state"]
                }
                for w in data.get("workflows", [])
            ]


class GitHubActionsError(Exception):
    """Error from GitHub Actions API"""
    pass


# Global instance
github_actions = GitHubActionsClient()
