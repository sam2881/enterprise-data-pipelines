#!/usr/bin/env python3
"""
GitHub MCP Server
Provides MCP protocol access to GitHub APIs

WHY: Centralized GitHub operations for the AI Agent Platform.
     Used for TWO main purposes:
     1. Remediation: Trigger GitHub Actions workflows in test_01 repo
        to run Terraform/Ansible/shell scripts for incident resolution
     2. Data Pipelines: Manage PRs and branches in enterprise-data-pipelines
        for DAG deployment

ARCHITECTURE:
  ServiceNow Incident → RAG retrieves scripts → LangGraph →
    Kafka (github.trigger_workflow) → GitHub MCP →
    GitHub Actions (test_01 repo) → Script execution → Resolution

  Jira Pipeline Request → Data Agent → GitHub MCP →
    Create branch, commit files, create PR → enterprise-data-pipelines →
    CI/CD → GCP Composer

RULES:
  1. All GitHub operations go through this MCP server
  2. LangGraph never calls GitHub API directly
  3. Support both remediation (workflow dispatch) and pipelines (PR/branch)
"""
import os
import sys
import asyncio
import time
import json
from typing import Any, Dict, List, Optional
from github import Github
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent
import requests

# Add parent directory for shared modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.metrics import start_metrics_server, ToolMetrics, APIMetrics

# Server name and metrics port
SERVER_NAME = "github-mcp"
METRICS_PORT = int(os.getenv("METRICS_PORT", "8092"))

# Repository configurations from environment
REMEDIATION_OWNER = os.getenv("GITHUB_REMEDIATION_OWNER", "sam2881")
REMEDIATION_REPO = os.getenv("GITHUB_REMEDIATION_REPO", "test_01")
PIPELINES_OWNER = os.getenv("GITHUB_PIPELINES_OWNER", "sam2881")
PIPELINES_REPO = os.getenv("GITHUB_PIPELINES_REPO", "enterprise-data-pipelines")


class GitHubMCPServer:
    """MCP Server for GitHub integration"""

    def __init__(self):
        # Initialize GitHub client
        self.github_token = os.getenv("GITHUB_TOKEN")

        if not self.github_token:
            raise ValueError("GitHub token not configured in environment")

        self.client = Github(self.github_token)

        # Create server
        self.server = Server("github-mcp")

        # Register tools
        self._register_tools()

    def _register_tools(self):
        """Register all GitHub tools"""

        @self.server.list_tools()
        async def list_tools() -> List[Tool]:
            return [
                Tool(
                    name="get_repository",
                    description="Get repository information",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "owner": {
                                "type": "string",
                                "description": "Repository owner"
                            },
                            "repo": {
                                "type": "string",
                                "description": "Repository name"
                            }
                        },
                        "required": ["owner", "repo"]
                    }
                ),
                Tool(
                    name="create_pull_request",
                    description="Create a new pull request",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "owner": {"type": "string"},
                            "repo": {"type": "string"},
                            "title": {"type": "string"},
                            "body": {"type": "string"},
                            "head": {
                                "type": "string",
                                "description": "Source branch"
                            },
                            "base": {
                                "type": "string",
                                "description": "Target branch (default: main)"
                            }
                        },
                        "required": ["owner", "repo", "title", "head"]
                    }
                ),
                Tool(
                    name="create_issue",
                    description="Create a new GitHub issue",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "owner": {"type": "string"},
                            "repo": {"type": "string"},
                            "title": {"type": "string"},
                            "body": {"type": "string"},
                            "labels": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "Issue labels (optional)"
                            }
                        },
                        "required": ["owner", "repo", "title"]
                    }
                ),
                Tool(
                    name="get_pull_request",
                    description="Get pull request details",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "owner": {"type": "string"},
                            "repo": {"type": "string"},
                            "pr_number": {
                                "type": "integer",
                                "description": "PR number"
                            }
                        },
                        "required": ["owner", "repo", "pr_number"]
                    }
                ),
                Tool(
                    name="merge_pull_request",
                    description="Merge a pull request",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "owner": {"type": "string"},
                            "repo": {"type": "string"},
                            "pr_number": {"type": "integer"},
                            "merge_method": {
                                "type": "string",
                                "enum": ["merge", "squash", "rebase"],
                                "default": "merge"
                            }
                        },
                        "required": ["owner", "repo", "pr_number"]
                    }
                ),
                Tool(
                    name="list_pull_requests",
                    description="List repository pull requests",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "owner": {"type": "string"},
                            "repo": {"type": "string"},
                            "state": {
                                "type": "string",
                                "enum": ["open", "closed", "all"],
                                "default": "open"
                            },
                            "limit": {
                                "type": "integer",
                                "default": 10
                            }
                        },
                        "required": ["owner", "repo"]
                    }
                ),
                Tool(
                    name="create_branch",
                    description="Create a new branch",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "owner": {"type": "string"},
                            "repo": {"type": "string"},
                            "branch_name": {"type": "string"},
                            "from_branch": {
                                "type": "string",
                                "description": "Source branch (default: main)",
                                "default": "main"
                            }
                        },
                        "required": ["owner", "repo", "branch_name"]
                    }
                ),
                Tool(
                    name="trigger_workflow",
                    description="Trigger a GitHub Actions workflow (used for remediation scripts)",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "owner": {
                                "type": "string",
                                "description": f"Repository owner (default: {REMEDIATION_OWNER})"
                            },
                            "repo": {
                                "type": "string",
                                "description": f"Repository name (default: {REMEDIATION_REPO})"
                            },
                            "workflow_id": {
                                "type": "string",
                                "description": "Workflow file name or ID (e.g., 'remediation.yml')"
                            },
                            "ref": {
                                "type": "string",
                                "description": "Branch or tag to run workflow on (default: main)",
                                "default": "main"
                            },
                            "inputs": {
                                "type": "object",
                                "description": "Workflow input parameters",
                                "additionalProperties": {"type": "string"}
                            }
                        },
                        "required": ["workflow_id"]
                    }
                ),
                Tool(
                    name="get_workflow_run",
                    description="Get status of a workflow run",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "owner": {"type": "string"},
                            "repo": {"type": "string"},
                            "run_id": {
                                "type": "integer",
                                "description": "Workflow run ID"
                            }
                        },
                        "required": ["run_id"]
                    }
                ),
                Tool(
                    name="list_workflow_runs",
                    description="List recent workflow runs",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "owner": {"type": "string"},
                            "repo": {"type": "string"},
                            "workflow_id": {
                                "type": "string",
                                "description": "Filter by workflow file name (optional)"
                            },
                            "status": {
                                "type": "string",
                                "enum": ["queued", "in_progress", "completed"],
                                "description": "Filter by status (optional)"
                            },
                            "limit": {
                                "type": "integer",
                                "default": 10
                            }
                        }
                    }
                ),
                Tool(
                    name="commit_file",
                    description="Create or update a file in a repository (used for pipeline deployment)",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "owner": {
                                "type": "string",
                                "description": f"Repository owner (default: {PIPELINES_OWNER})"
                            },
                            "repo": {
                                "type": "string",
                                "description": f"Repository name (default: {PIPELINES_REPO})"
                            },
                            "path": {
                                "type": "string",
                                "description": "File path in repository"
                            },
                            "content": {
                                "type": "string",
                                "description": "File content"
                            },
                            "message": {
                                "type": "string",
                                "description": "Commit message"
                            },
                            "branch": {
                                "type": "string",
                                "description": "Target branch (default: main)",
                                "default": "main"
                            }
                        },
                        "required": ["path", "content", "message"]
                    }
                )
            ]

        @self.server.call_tool()
        async def call_tool(name: str, arguments: Dict[str, Any]) -> List[TextContent]:
            """Handle tool calls"""
            with ToolMetrics(SERVER_NAME, name):
                if name == "get_repository":
                    return await self._get_repository(
                        arguments["owner"],
                        arguments["repo"]
                    )

                elif name == "create_pull_request":
                    return await self._create_pull_request(
                        arguments["owner"],
                        arguments["repo"],
                        arguments["title"],
                        arguments.get("body", ""),
                        arguments["head"],
                        arguments.get("base", "main")
                    )

                elif name == "create_issue":
                    return await self._create_issue(
                        arguments["owner"],
                        arguments["repo"],
                        arguments["title"],
                        arguments.get("body", ""),
                        arguments.get("labels", [])
                    )

                elif name == "get_pull_request":
                    return await self._get_pull_request(
                        arguments["owner"],
                        arguments["repo"],
                        arguments["pr_number"]
                    )

                elif name == "merge_pull_request":
                    return await self._merge_pull_request(
                        arguments["owner"],
                        arguments["repo"],
                        arguments["pr_number"],
                        arguments.get("merge_method", "merge")
                    )

                elif name == "list_pull_requests":
                    return await self._list_pull_requests(
                        arguments["owner"],
                        arguments["repo"],
                        arguments.get("state", "open"),
                        arguments.get("limit", 10)
                    )

                elif name == "create_branch":
                    return await self._create_branch(
                        arguments["owner"],
                        arguments["repo"],
                        arguments["branch_name"],
                        arguments.get("from_branch", "main")
                    )

                elif name == "trigger_workflow":
                    return await self._trigger_workflow(
                        arguments.get("owner", REMEDIATION_OWNER),
                        arguments.get("repo", REMEDIATION_REPO),
                        arguments["workflow_id"],
                        arguments.get("ref", "main"),
                        arguments.get("inputs", {})
                    )

                elif name == "get_workflow_run":
                    return await self._get_workflow_run(
                        arguments.get("owner", REMEDIATION_OWNER),
                        arguments.get("repo", REMEDIATION_REPO),
                        arguments["run_id"]
                    )

                elif name == "list_workflow_runs":
                    return await self._list_workflow_runs(
                        arguments.get("owner", REMEDIATION_OWNER),
                        arguments.get("repo", REMEDIATION_REPO),
                        arguments.get("workflow_id"),
                        arguments.get("status"),
                        arguments.get("limit", 10)
                    )

                elif name == "commit_file":
                    return await self._commit_file(
                        arguments.get("owner", PIPELINES_OWNER),
                        arguments.get("repo", PIPELINES_REPO),
                        arguments["path"],
                        arguments["content"],
                        arguments["message"],
                        arguments.get("branch", "main")
                    )

                else:
                    raise ValueError(f"Unknown tool: {name}")

    async def _get_repository(self, owner: str, repo: str) -> List[TextContent]:
        """Get repository information"""
        try:
            repository = self.client.get_repo(f"{owner}/{repo}")

            text = f"""
Repository: {repository.full_name}
Description: {repository.description or 'None'}
Stars: {repository.stargazers_count}
Forks: {repository.forks_count}
Open Issues: {repository.open_issues_count}
Default Branch: {repository.default_branch}
Language: {repository.language or 'Unknown'}
Created: {repository.created_at}
URL: {repository.html_url}
            """.strip()

            return [TextContent(type="text", text=text)]

        except Exception as e:
            return [TextContent(type="text", text=f"Error: {str(e)}")]

    async def _create_pull_request(
        self,
        owner: str,
        repo: str,
        title: str,
        body: str,
        head: str,
        base: str = "main"
    ) -> List[TextContent]:
        """Create pull request"""
        try:
            repository = self.client.get_repo(f"{owner}/{repo}")

            pr = repository.create_pull(
                title=title,
                body=body,
                head=head,
                base=base
            )

            text = f"""
Created Pull Request #{pr.number}
Title: {pr.title}
Branch: {head} → {base}
URL: {pr.html_url}
            """.strip()

            return [TextContent(type="text", text=text)]

        except Exception as e:
            return [TextContent(type="text", text=f"Error: {str(e)}")]

    async def _create_issue(
        self,
        owner: str,
        repo: str,
        title: str,
        body: str = "",
        labels: List[str] = None
    ) -> List[TextContent]:
        """Create GitHub issue"""
        try:
            repository = self.client.get_repo(f"{owner}/{repo}")

            issue = repository.create_issue(
                title=title,
                body=body,
                labels=labels or []
            )

            text = f"""
Created Issue #{issue.number}
Title: {issue.title}
Labels: {', '.join(labels) if labels else 'None'}
URL: {issue.html_url}
            """.strip()

            return [TextContent(type="text", text=text)]

        except Exception as e:
            return [TextContent(type="text", text=f"Error: {str(e)}")]

    async def _get_pull_request(
        self,
        owner: str,
        repo: str,
        pr_number: int
    ) -> List[TextContent]:
        """Get PR details"""
        try:
            repository = self.client.get_repo(f"{owner}/{repo}")
            pr = repository.get_pull(pr_number)

            text = f"""
Pull Request #{pr.number}
Title: {pr.title}
State: {pr.state}
Branch: {pr.head.ref} → {pr.base.ref}
Author: {pr.user.login}
Created: {pr.created_at}
Mergeable: {pr.mergeable}
URL: {pr.html_url}
            """.strip()

            return [TextContent(type="text", text=text)]

        except Exception as e:
            return [TextContent(type="text", text=f"Error: {str(e)}")]

    async def _merge_pull_request(
        self,
        owner: str,
        repo: str,
        pr_number: int,
        merge_method: str = "merge"
    ) -> List[TextContent]:
        """Merge pull request"""
        try:
            repository = self.client.get_repo(f"{owner}/{repo}")
            pr = repository.get_pull(pr_number)

            result = pr.merge(merge_method=merge_method)

            if result.merged:
                text = f"Successfully merged PR #{pr_number} using {merge_method}"
            else:
                text = f"Failed to merge PR #{pr_number}: {result.message}"

            return [TextContent(type="text", text=text)]

        except Exception as e:
            return [TextContent(type="text", text=f"Error: {str(e)}")]

    async def _list_pull_requests(
        self,
        owner: str,
        repo: str,
        state: str = "open",
        limit: int = 10
    ) -> List[TextContent]:
        """List pull requests"""
        try:
            repository = self.client.get_repo(f"{owner}/{repo}")
            prs = repository.get_pulls(state=state)[:limit]

            if not prs:
                return [TextContent(type="text", text=f"No {state} pull requests found")]

            text_lines = [f"Pull Requests ({state}):\n"]

            for pr in prs:
                text_lines.append(
                    f"- #{pr.number}: {pr.title} [{pr.state}]"
                )

            return [TextContent(type="text", text="\n".join(text_lines))]

        except Exception as e:
            return [TextContent(type="text", text=f"Error: {str(e)}")]

    async def _create_branch(
        self,
        owner: str,
        repo: str,
        branch_name: str,
        from_branch: str = "main"
    ) -> List[TextContent]:
        """Create new branch"""
        try:
            repository = self.client.get_repo(f"{owner}/{repo}")

            # Get source branch SHA
            source = repository.get_branch(from_branch)
            sha = source.commit.sha

            # Create new branch
            repository.create_git_ref(
                ref=f"refs/heads/{branch_name}",
                sha=sha
            )

            text = f"Created branch '{branch_name}' from '{from_branch}'"

            return [TextContent(type="text", text=text)]

        except Exception as e:
            return [TextContent(type="text", text=f"Error: {str(e)}")]

    async def _trigger_workflow(
        self,
        owner: str,
        repo: str,
        workflow_id: str,
        ref: str = "main",
        inputs: Dict[str, str] = None
    ) -> List[TextContent]:
        """
        Trigger a GitHub Actions workflow dispatch.

        This is the PRIMARY method for executing remediation scripts.
        The test_01 repo contains workflows that run Terraform, Ansible,
        and shell scripts to resolve ServiceNow incidents.
        """
        try:
            # Use GitHub REST API for workflow dispatch
            # PyGithub's workflow dispatch has limitations
            url = f"https://api.github.com/repos/{owner}/{repo}/actions/workflows/{workflow_id}/dispatches"

            headers = {
                "Authorization": f"token {self.github_token}",
                "Accept": "application/vnd.github.v3+json"
            }

            payload = {
                "ref": ref,
                "inputs": inputs or {}
            }

            response = requests.post(url, headers=headers, json=payload)

            if response.status_code == 204:
                # Workflow triggered successfully, get the run ID
                # Need to fetch the most recent run
                await asyncio.sleep(2)  # Wait for workflow to be queued

                runs_url = f"https://api.github.com/repos/{owner}/{repo}/actions/runs"
                runs_response = requests.get(
                    runs_url,
                    headers=headers,
                    params={"per_page": 1}
                )

                run_id = None
                run_url = None
                if runs_response.status_code == 200:
                    runs = runs_response.json().get("workflow_runs", [])
                    if runs:
                        run_id = runs[0]["id"]
                        run_url = runs[0]["html_url"]

                text = f"""
Workflow triggered successfully!
Repository: {owner}/{repo}
Workflow: {workflow_id}
Branch: {ref}
Inputs: {json.dumps(inputs or {}, indent=2)}
Run ID: {run_id or 'Pending'}
URL: {run_url or 'Check GitHub Actions'}
                """.strip()

                return [TextContent(type="text", text=text)]
            else:
                error_msg = response.json().get("message", response.text)
                return [TextContent(
                    type="text",
                    text=f"Failed to trigger workflow: {response.status_code} - {error_msg}"
                )]

        except Exception as e:
            return [TextContent(type="text", text=f"Error: {str(e)}")]

    async def _get_workflow_run(
        self,
        owner: str,
        repo: str,
        run_id: int
    ) -> List[TextContent]:
        """Get status of a specific workflow run"""
        try:
            repository = self.client.get_repo(f"{owner}/{repo}")
            run = repository.get_workflow_run(run_id)

            text = f"""
Workflow Run #{run.id}
Name: {run.name}
Status: {run.status}
Conclusion: {run.conclusion or 'In Progress'}
Branch: {run.head_branch}
Event: {run.event}
Created: {run.created_at}
Updated: {run.updated_at}
URL: {run.html_url}
            """.strip()

            return [TextContent(type="text", text=text)]

        except Exception as e:
            return [TextContent(type="text", text=f"Error: {str(e)}")]

    async def _list_workflow_runs(
        self,
        owner: str,
        repo: str,
        workflow_id: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 10
    ) -> List[TextContent]:
        """List recent workflow runs"""
        try:
            repository = self.client.get_repo(f"{owner}/{repo}")

            if workflow_id:
                workflow = repository.get_workflow(workflow_id)
                runs = workflow.get_runs(status=status) if status else workflow.get_runs()
            else:
                runs = repository.get_workflow_runs(status=status) if status else repository.get_workflow_runs()

            runs_list = list(runs[:limit])

            if not runs_list:
                return [TextContent(type="text", text="No workflow runs found")]

            text_lines = [f"Recent Workflow Runs ({owner}/{repo}):\n"]

            for run in runs_list:
                conclusion = run.conclusion or "in_progress"
                text_lines.append(
                    f"- #{run.id}: {run.name} [{run.status}/{conclusion}] ({run.head_branch})"
                )

            return [TextContent(type="text", text="\n".join(text_lines))]

        except Exception as e:
            return [TextContent(type="text", text=f"Error: {str(e)}")]

    async def _commit_file(
        self,
        owner: str,
        repo: str,
        path: str,
        content: str,
        message: str,
        branch: str = "main"
    ) -> List[TextContent]:
        """
        Create or update a file in the repository.

        Used by Data Agent to commit generated DAGs and Spark jobs
        to the enterprise-data-pipelines repository.
        """
        try:
            import base64
            repository = self.client.get_repo(f"{owner}/{repo}")

            # Check if file exists
            try:
                existing_file = repository.get_contents(path, ref=branch)
                sha = existing_file.sha

                # Update file
                result = repository.update_file(
                    path=path,
                    message=message,
                    content=content,
                    sha=sha,
                    branch=branch
                )
                action = "Updated"
            except Exception:
                # Create new file
                result = repository.create_file(
                    path=path,
                    message=message,
                    content=content,
                    branch=branch
                )
                action = "Created"

            text = f"""
{action} file: {path}
Repository: {owner}/{repo}
Branch: {branch}
Commit: {result['commit'].sha[:8]}
Message: {message}
            """.strip()

            return [TextContent(type="text", text=text)]

        except Exception as e:
            return [TextContent(type="text", text=f"Error: {str(e)}")]

    async def run(self):
        """Run the MCP server"""
        async with stdio_server() as (read_stream, write_stream):
            await self.server.run(
                read_stream,
                write_stream,
                self.server.create_initialization_options()
            )


async def main():
    """Main entry point"""
    # Start metrics server in background
    start_metrics_server(METRICS_PORT, SERVER_NAME)
    print(f"GitHub MCP Server starting (metrics on port {METRICS_PORT})")

    server = GitHubMCPServer()
    await server.run()


if __name__ == "__main__":
    asyncio.run(main())
