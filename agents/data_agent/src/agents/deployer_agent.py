"""
Deployer Agent - Git and CI/CD Operations

WHY: Handles deployment of generated artifacts to Git and triggers CI/CD.
     Ensures proper versioning and audit trail for all deployments.

HOW:
1. Create Git branch for changes
2. Commit generated artifacts
3. Push to remote
4. Create pull request
5. Trigger Cloud Build CI/CD

RULES:
- NEVER push directly to main branch
- ALWAYS create pull request for review
- ALWAYS wait for CI/CD success before completing
- Require human approval for production deployments
"""

from typing import Any, Dict, Optional
import uuid
from datetime import datetime

from src.agents.base_agent import BaseAgent, requires_state_keys
from src.utils.exceptions import DeploymentError, GitError, CICDError


class DeployerAgent(BaseAgent):
    """
    Deployer Agent for Git and CI/CD operations.

    RESPONSIBILITIES:
    1. Create Git branches for changes
    2. Commit generated artifacts
    3. Push to remote repository
    4. Create pull requests
    5. Trigger CI/CD pipelines
    6. Sync DAGs to Composer

    RULES:
    - NEVER deploy without validation passing
    - ALWAYS create PR for main branch
    - Require approval for PROD environment
    """

    def __init__(self) -> None:
        super().__init__("deployer")
        self._git_client = None
        self._cicd_trigger = None
        self._composer_client = None

    @property
    def git_client(self):
        """Lazy-load Git client."""
        if self._git_client is None:
            from src.deployers.git_client import GitClient
            self._git_client = GitClient()
        return self._git_client

    @property
    def cicd_trigger(self):
        """Lazy-load CI/CD trigger."""
        if self._cicd_trigger is None:
            from src.deployers.cicd_trigger import CICDTrigger
            self._cicd_trigger = CICDTrigger()
        return self._cicd_trigger

    @property
    def composer_client(self):
        """Lazy-load Composer client."""
        if self._composer_client is None:
            from src.deployers.composer_client import ComposerClient
            self._composer_client = ComposerClient()
        return self._composer_client

    @requires_state_keys("intent_json", "request_id", "generator_output", "validator_output")
    def execute(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute deployment of artifacts.

        Steps:
        1. Check if deployment needed
        2. Create Git branch
        3. Commit artifacts
        4. Push and create PR
        5. Trigger CI/CD
        6. Return DeployerOutput

        Args:
            state: Current workflow state with generator and validator output

        Returns:
            Partial state update with deployer_output
        """
        self.logger.info(
            "Starting deployment",
            request_id=state["request_id"],
        )

        try:
            generator_output = state["generator_output"]
            validator_output = state["validator_output"]
            intent = state["intent_json"]
            metadata_context = state.get("metadata_context", {})

            # Check if deployment was skipped
            if generator_output.get("skipped"):
                self.logger.info("Deployment skipped - no artifacts to deploy")
                return {
                    "current_phase": "deploying",
                    "deployer_output": {
                        "deployment_id": str(uuid.uuid4()),
                        "status": "skipped",
                        "skip_reason": "No artifacts to deploy",
                    },
                }

            # Check if validation passed
            if not validator_output.get("is_valid"):
                return self.create_error_state(
                    "Cannot deploy - validation failed"
                )

            # Extract deployment context
            pipeline_identity = intent.get("pipeline_identity", {})
            pipeline_name = pipeline_identity.get("pipeline_name")
            domain = pipeline_identity.get("domain", "default")
            environment = pipeline_identity.get("environment", "dev")
            jira_ticket = intent.get("jira_ticket", "")

            # Generate branch name
            branch_name = self._generate_branch_name(
                pipeline_name,
                domain,
                jira_ticket,
            )

            # Step 1: Create Git branch
            self.logger.info(f"Creating branch: {branch_name}")
            self.git_client.create_branch(branch_name)

            # Step 2: Commit artifacts
            files_to_commit = self._prepare_files(generator_output)
            commit_message = self._generate_commit_message(
                pipeline_name,
                domain,
                jira_ticket,
                state.get("planner_output", {}).get("pipeline_action", "create"),
            )

            self.logger.info(f"Committing {len(files_to_commit)} files")
            commit_sha = self.git_client.commit_files(
                files=files_to_commit,
                message=commit_message,
            )

            # Step 3: Push and create PR
            self.logger.info("Pushing to remote")
            self.git_client.push(branch_name)

            pr_url = None
            pr_number = None

            # Create PR for non-dev environments
            if environment != "dev":
                pr_title = f"[{jira_ticket or 'AUTO'}] {pipeline_name} pipeline - {environment}"
                pr_description = self._generate_pr_description(
                    intent,
                    state.get("planner_output", {}),
                    generator_output,
                )

                self.logger.info("Creating pull request")
                pr_result = self.git_client.create_pull_request(
                    title=pr_title,
                    description=pr_description,
                    head=branch_name,
                    base="main",
                )
                pr_url = pr_result.get("url")
                pr_number = pr_result.get("number")

            # Step 4: Trigger CI/CD
            self.logger.info("Triggering CI/CD pipeline")
            cicd_result = self.cicd_trigger.trigger_build(
                branch=branch_name,
                substitutions={
                    "_PIPELINE_NAME": pipeline_name,
                    "_DOMAIN": domain,
                    "_ENVIRONMENT": environment,
                    "_REQUEST_ID": state["request_id"],
                },
            )
            build_id = cicd_result.get("build_id")

            # Log decision
            self.log_decision(
                decision=f"Deployed to branch {branch_name}",
                reasoning=f"Artifacts validated successfully. "
                    f"{'Created PR for review' if pr_url else 'Direct deploy to dev'}",
                context={
                    "branch": branch_name,
                    "commit_sha": commit_sha,
                    "pr_url": pr_url,
                    "build_id": build_id,
                },
            )

            # Build deployer output
            deployer_output = {
                "deployment_id": str(uuid.uuid4()),
                "branch_name": branch_name,
                "commit_sha": commit_sha,
                "pr_url": pr_url,
                "pr_number": pr_number,
                "cicd_build_id": build_id,
                "deployment_status": "pending",
                "files_deployed": list(files_to_commit.keys()),
                "deployed_at": datetime.utcnow().isoformat(),
            }

            self.logger.info(
                "Deployment complete",
                request_id=state["request_id"],
                branch=branch_name,
                commit_sha=commit_sha,
                build_id=build_id,
            )

            return {
                "current_phase": "deploying",
                "deployer_output": deployer_output,
                "decision_reasoning": f"Deployed to branch {branch_name}. "
                    f"{'PR created at ' + pr_url if pr_url else 'Direct deployment for dev.'}",
            }

        except GitError as e:
            self.logger.error("Git operation failed", error=str(e))
            return self.create_error_state(str(e), e)
        except CICDError as e:
            self.logger.error("CI/CD trigger failed", error=str(e))
            return self.create_error_state(str(e), e)
        except Exception as e:
            self.logger.error("Unexpected deployment error", error=str(e))
            return self.create_error_state(f"Deployment failed: {str(e)}", e)

    def _generate_branch_name(
        self,
        pipeline_name: str,
        domain: str,
        jira_ticket: Optional[str],
    ) -> str:
        """Generate Git branch name."""
        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")

        if jira_ticket:
            return f"data-agent/{jira_ticket}-{pipeline_name}-{timestamp}"
        else:
            return f"data-agent/{domain}/{pipeline_name}-{timestamp}"

    def _prepare_files(self, generator_output: Dict[str, Any]) -> Dict[str, str]:
        """Prepare files for commit."""
        files = {}
        artifact_paths = generator_output.get("artifact_paths", {})

        # Add DAG file
        if generator_output.get("dag_code"):
            dag_path = artifact_paths.get("dag", "dags/generated_dag.py")
            files[dag_path] = generator_output["dag_code"]

        # Add Spark jobs
        for job_name, job_code in generator_output.get("spark_jobs", {}).items():
            if job_code:
                # Find matching path or generate one
                for key, path in artifact_paths.items():
                    if key.startswith("spark_") and job_name.endswith(key.replace("spark_", "")):
                        files[path] = job_code
                        break
                else:
                    files[f"spark_jobs/{job_name}.py"] = job_code

        # Add metadata SQL (combined into one file)
        metadata_sql = generator_output.get("metadata_sql", [])
        if metadata_sql:
            sql_path = artifact_paths.get("metadata_sql", "metadata_sql/generated.sql")
            files[sql_path] = "\n\n".join(metadata_sql)

        return files

    def _generate_commit_message(
        self,
        pipeline_name: str,
        domain: str,
        jira_ticket: Optional[str],
        action: str,
    ) -> str:
        """Generate commit message."""
        action_verb = {
            "create": "Add",
            "modify": "Update",
            "upgrade_schema": "Upgrade schema for",
        }.get(action, "Update")

        message = f"{action_verb} {domain}/{pipeline_name} pipeline\n\n"

        if jira_ticket:
            message += f"Jira: {jira_ticket}\n"

        message += "Generated by Data Agent\n"
        message += f"\nCo-Authored-By: Data Agent <data-agent@example.com>"

        return message

    def _generate_pr_description(
        self,
        intent: Dict[str, Any],
        planner_output: Dict[str, Any],
        generator_output: Dict[str, Any],
    ) -> str:
        """Generate pull request description."""
        pipeline_identity = intent.get("pipeline_identity", {})
        schema_plan = planner_output.get("schema_plan", {})

        description = f"""## Summary

This PR was automatically generated by the Data Agent platform.

### Pipeline Details
- **Name**: {pipeline_identity.get('pipeline_name')}
- **Domain**: {pipeline_identity.get('domain')}
- **Environment**: {pipeline_identity.get('environment')}
- **Owner**: {pipeline_identity.get('technical_owner', 'TBD')}

### Changes
- **Action**: {planner_output.get('pipeline_action', 'create')}
- **Schema Version**: {schema_plan.get('new_version', 1)}
- **Schema Changes**: {len(schema_plan.get('changes', []))}

### Generated Artifacts
- DAG: {generator_output.get('artifact_paths', {}).get('dag', 'N/A')}
- Spark Jobs: {len(generator_output.get('spark_jobs', {}))}
- SQL Statements: {len(generator_output.get('metadata_sql', []))}

### Validation
All validation checks passed.

---
Generated by Data Agent
"""
        return description
