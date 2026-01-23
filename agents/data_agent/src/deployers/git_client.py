"""
Git operations client for artifact deployment.

WHY: Manages Git operations for deploying generated artifacts.
     Provides version control and audit trail for all changes.

RULES:
1. Never force push
2. Always create branches for changes
3. Include meaningful commit messages
4. Track all operations for audit

IMPLEMENTATION:
- Prefers GitPython for local operations if available
- Falls back to GitHub REST API for remote operations
"""

from pathlib import Path
from typing import Dict, List, Optional
import os
import tempfile
import shutil
import base64
import requests
import structlog

from src.config.settings import get_settings
from src.utils.exceptions import GitError

logger = structlog.get_logger()


class GitClient:
    """
    Git operations for artifact deployment.

    Uses GitPython or subprocess for Git operations.
    Supports local repository or cloning remote.
    """

    def __init__(
        self,
        repo_url: Optional[str] = None,
        repo_path: Optional[str] = None,
        github_owner: Optional[str] = None,
        github_repo: Optional[str] = None,
        clear_before_push: Optional[bool] = None,
    ) -> None:
        """
        Initialize Git client.

        Args:
            repo_url: Remote repository URL (will clone if provided)
            repo_path: Local repository path
            github_owner: GitHub owner/organization
            github_repo: GitHub repository name
            clear_before_push: Whether to clear repo before pushing
        """
        settings = get_settings()

        # Build repo URL from GitHub owner/repo if not provided
        self.github_owner = github_owner or settings.github_owner
        self.github_repo = github_repo or settings.github_repo

        if repo_url:
            self.repo_url = repo_url
        elif self.github_owner and self.github_repo:
            self.repo_url = f"https://github.com/{self.github_owner}/{self.github_repo}.git"
        else:
            self.repo_url = settings.git_repo_url

        self.main_branch = settings.git_main_branch
        self.committer_name = settings.git_committer_name
        self.committer_email = settings.git_committer_email
        self.clear_before_push = clear_before_push if clear_before_push is not None else settings.git_clear_repo_before_push

        self._repo = None
        self._work_dir: Optional[Path] = None
        self._use_api = False  # Use GitHub API instead of GitPython
        # Try both GITHUB_TOKEN and GITHUB_PIPELINES_TOKEN from env
        self._github_token = (
            os.getenv("GITHUB_TOKEN") or
            os.getenv("GITHUB_PIPELINES_TOKEN") or
            (settings.github_token.get_secret_value() if settings.github_token else None)
        )
        self._current_branch = None
        self._committed_files: Dict[str, str] = {}  # Store files for batch commit

        if repo_path:
            self._work_dir = Path(repo_path)

    def _ensure_repo(self) -> None:
        """Ensure repository is available."""
        if self._repo is not None:
            return

        try:
            import git

            if self._work_dir and self._work_dir.exists():
                # Use existing local repo
                self._repo = git.Repo(self._work_dir)
                logger.info("Using existing repository", path=str(self._work_dir))
            elif self.repo_url:
                # Clone remote repo to temp directory
                self._work_dir = Path(tempfile.mkdtemp(prefix="data-agent-"))
                logger.info("Cloning repository", url=self.repo_url, path=str(self._work_dir))

                self._repo = git.Repo.clone_from(
                    self.repo_url,
                    self._work_dir,
                    branch=self.main_branch,
                )
            else:
                raise GitError("No repository URL or path provided", operation="init")

            # Configure committer
            with self._repo.config_writer() as config:
                config.set_value("user", "name", self.committer_name)
                config.set_value("user", "email", self.committer_email)

        except ImportError:
            logger.warning("GitPython not available, using GitHub API")
            self._use_api = True
            if not self._github_token:
                raise GitError("GITHUB_TOKEN required when GitPython is not available", operation="init")
        except Exception as e:
            raise GitError(f"Failed to initialize Git repository: {e}", operation="init")

    def _github_api_request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        """Make a GitHub API request."""
        url = f"https://api.github.com{endpoint}"
        headers = {
            "Authorization": f"token {self._github_token}",
            "Accept": "application/vnd.github.v3+json",
        }
        headers.update(kwargs.pop("headers", {}))
        response = requests.request(method, url, headers=headers, **kwargs)
        return response

    def clear_repo_contents(self, preserve: List[str] = None) -> int:
        """
        Delete all files in the repository except preserved files.

        This is used when git_clear_repo_before_push is True to ensure
        the repository only contains the newly generated artifacts.

        Args:
            preserve: List of file patterns to preserve (e.g., ['.git', 'README.md', '.gitignore'])
                     Defaults to ['.git', '.gitignore']

        Returns:
            Number of files deleted
        """
        self._ensure_repo()

        if preserve is None:
            preserve = ['.git', '.gitignore']

        try:
            deleted_count = 0

            # Get all files in repo root
            for item in self._work_dir.iterdir():
                # Skip preserved items
                if item.name in preserve:
                    continue

                if item.is_file():
                    item.unlink()
                    deleted_count += 1
                    logger.debug("Deleted file", file=item.name)
                elif item.is_dir():
                    shutil.rmtree(item)
                    # Count all files in deleted directory
                    deleted_count += 1
                    logger.debug("Deleted directory", dir=item.name)

            # Stage deletions
            if deleted_count > 0:
                self._repo.git.add(all=True)
                logger.info("Cleared repository contents", deleted_count=deleted_count, preserved=preserve)

            return deleted_count

        except Exception as e:
            raise GitError(f"Failed to clear repository contents: {e}", operation="clear")

    def create_branch(self, branch_name: str) -> str:
        """
        Create and checkout a new branch from main.

        Args:
            branch_name: Name of branch to create

        Returns:
            Branch name
        """
        self._ensure_repo()

        if self._use_api:
            return self._create_branch_api(branch_name)

        try:
            # Fetch latest from remote
            if self._repo.remotes:
                origin = self._repo.remotes.origin
                origin.fetch()

                # Checkout main and pull
                self._repo.heads[self.main_branch].checkout()
                origin.pull()

            # Create new branch
            new_branch = self._repo.create_head(branch_name)
            new_branch.checkout()

            logger.info("Created branch", branch=branch_name)
            return branch_name

        except Exception as e:
            raise GitError(f"Failed to create branch {branch_name}: {e}", operation="create_branch", branch=branch_name)

    def _create_branch_api(self, branch_name: str) -> str:
        """Create branch using GitHub API."""
        try:
            # Get the SHA of the main branch
            ref_resp = self._github_api_request(
                "GET",
                f"/repos/{self.github_owner}/{self.github_repo}/git/ref/heads/{self.main_branch}"
            )

            if ref_resp.status_code != 200:
                raise GitError(f"Failed to get main branch: {ref_resp.text}", operation="create_branch", branch=branch_name)

            main_sha = ref_resp.json()["object"]["sha"]

            # Create new branch
            create_resp = self._github_api_request(
                "POST",
                f"/repos/{self.github_owner}/{self.github_repo}/git/refs",
                json={"ref": f"refs/heads/{branch_name}", "sha": main_sha}
            )

            if create_resp.status_code == 201:
                logger.info("Created branch via API", branch=branch_name)
                self._current_branch = branch_name
                return branch_name
            elif create_resp.status_code == 422:
                # Branch already exists, use it
                logger.info("Branch already exists", branch=branch_name)
                self._current_branch = branch_name
                return branch_name
            else:
                raise GitError(f"Failed to create branch: {create_resp.text}", operation="create_branch", branch=branch_name)

        except GitError:
            raise
        except Exception as e:
            raise GitError(f"API error creating branch: {e}", operation="create_branch", branch=branch_name)

    def commit_files(
        self,
        files: Dict[str, str],
        message: str,
    ) -> str:
        """
        Write files and commit them.

        Args:
            files: Dict of file path -> content
            message: Commit message

        Returns:
            Commit SHA
        """
        self._ensure_repo()

        if self._use_api:
            return self._commit_files_api(files, message)

        try:
            # Write files
            for file_path, content in files.items():
                full_path = self._work_dir / file_path
                full_path.parent.mkdir(parents=True, exist_ok=True)

                with open(full_path, "w") as f:
                    f.write(content)

                # Stage file
                self._repo.index.add([file_path])

            logger.info("Staged files", count=len(files))

            # Commit
            commit = self._repo.index.commit(message)
            commit_sha = commit.hexsha

            logger.info("Committed changes", sha=commit_sha[:8])
            return commit_sha

        except Exception as e:
            raise GitError(f"Failed to commit files: {e}", operation="commit")

    def _commit_files_api(self, files: Dict[str, str], message: str) -> str:
        """Commit files using GitHub API."""
        try:
            branch = self._current_branch or self.main_branch

            # Get the latest commit SHA for the branch
            ref_resp = self._github_api_request(
                "GET",
                f"/repos/{self.github_owner}/{self.github_repo}/git/ref/heads/{branch}"
            )
            if ref_resp.status_code != 200:
                raise GitError(f"Failed to get branch ref: {ref_resp.text}", operation="commit")

            latest_commit_sha = ref_resp.json()["object"]["sha"]

            # Get the tree SHA of the latest commit
            commit_resp = self._github_api_request(
                "GET",
                f"/repos/{self.github_owner}/{self.github_repo}/git/commits/{latest_commit_sha}"
            )
            if commit_resp.status_code != 200:
                raise GitError(f"Failed to get commit: {commit_resp.text}", operation="commit")

            base_tree_sha = commit_resp.json()["tree"]["sha"]

            # Create blobs for each file
            tree_items = []
            for file_path, content in files.items():
                blob_resp = self._github_api_request(
                    "POST",
                    f"/repos/{self.github_owner}/{self.github_repo}/git/blobs",
                    json={"content": content, "encoding": "utf-8"}
                )
                if blob_resp.status_code != 201:
                    raise GitError(f"Failed to create blob: {blob_resp.text}", operation="commit")

                blob_sha = blob_resp.json()["sha"]
                tree_items.append({
                    "path": file_path,
                    "mode": "100644",
                    "type": "blob",
                    "sha": blob_sha
                })

            # Create new tree
            tree_resp = self._github_api_request(
                "POST",
                f"/repos/{self.github_owner}/{self.github_repo}/git/trees",
                json={"base_tree": base_tree_sha, "tree": tree_items}
            )
            if tree_resp.status_code != 201:
                raise GitError(f"Failed to create tree: {tree_resp.text}", operation="commit")

            new_tree_sha = tree_resp.json()["sha"]

            # Create commit
            commit_resp = self._github_api_request(
                "POST",
                f"/repos/{self.github_owner}/{self.github_repo}/git/commits",
                json={
                    "message": message,
                    "tree": new_tree_sha,
                    "parents": [latest_commit_sha],
                    "author": {
                        "name": self.committer_name,
                        "email": self.committer_email
                    }
                }
            )
            if commit_resp.status_code != 201:
                raise GitError(f"Failed to create commit: {commit_resp.text}", operation="commit")

            new_commit_sha = commit_resp.json()["sha"]

            # Update branch reference
            update_resp = self._github_api_request(
                "PATCH",
                f"/repos/{self.github_owner}/{self.github_repo}/git/refs/heads/{branch}",
                json={"sha": new_commit_sha}
            )
            if update_resp.status_code != 200:
                raise GitError(f"Failed to update branch: {update_resp.text}", operation="commit")

            logger.info("Committed files via API", sha=new_commit_sha[:8], file_count=len(files))
            return new_commit_sha

        except GitError:
            raise
        except Exception as e:
            raise GitError(f"API error committing files: {e}", operation="commit")

    def add_files(self, file_paths: List[str]) -> None:
        """
        Stage files for commit.

        Args:
            file_paths: List of file paths to add
        """
        self._ensure_repo()

        try:
            self._repo.index.add(file_paths)
            logger.info("Staged files", count=len(file_paths))

        except Exception as e:
            raise GitError(f"Failed to stage files: {e}", operation="stage")

    def commit(self, message: str, author: Optional[str] = None) -> str:
        """
        Commit staged changes.

        Args:
            message: Commit message
            author: Optional author override

        Returns:
            Commit SHA
        """
        self._ensure_repo()

        try:
            import git

            author_obj = None
            if author:
                author_obj = git.Actor(author, self.committer_email)

            commit = self._repo.index.commit(
                message,
                author=author_obj,
            )

            logger.info("Committed changes", sha=commit.hexsha[:8])
            return commit.hexsha

        except Exception as e:
            raise GitError(f"Failed to commit: {e}", operation="commit")

    def push(self, branch_name: str, set_upstream: bool = True) -> None:
        """
        Push branch to remote.

        Args:
            branch_name: Branch to push
            set_upstream: Whether to set upstream tracking
        """
        self._ensure_repo()

        if self._use_api:
            # When using API, commits are already pushed directly
            logger.info("Using API mode - push already completed via commit", branch=branch_name)
            return

        try:
            if not self._repo.remotes:
                logger.warning("No remote configured, skipping push")
                return

            origin = self._repo.remotes.origin

            if set_upstream:
                origin.push(branch_name, set_upstream=True)
            else:
                origin.push(branch_name)

            logger.info("Pushed to remote", branch=branch_name)

        except Exception as e:
            raise GitError(f"Failed to push branch {branch_name}: {e}", operation="push", branch=branch_name)

    def create_pull_request(
        self,
        title: str,
        description: str,
        head: str,
        base: str = "main",
    ) -> Dict[str, str]:
        """
        Create pull request via GitHub API.

        Args:
            title: PR title
            description: PR description
            head: Head branch
            base: Base branch (default: main)

        Returns:
            Dict with 'url' and 'number'
        """
        logger.info("Creating pull request", title=title, head=head, base=base)

        # Always use GitHub API for PR creation
        try:
            pr_resp = self._github_api_request(
                "POST",
                f"/repos/{self.github_owner}/{self.github_repo}/pulls",
                json={
                    "title": title,
                    "body": description,
                    "head": head,
                    "base": base
                }
            )

            if pr_resp.status_code == 201:
                pr_data = pr_resp.json()
                pr_url = pr_data["html_url"]
                pr_number = str(pr_data["number"])
                logger.info("Pull request created", url=pr_url, number=pr_number)
                return {"url": pr_url, "number": pr_number}
            else:
                error_msg = pr_resp.json().get("message", pr_resp.text)
                logger.warning("PR creation failed", error=error_msg)
                # Return a placeholder with error info for non-critical scenarios
                return {
                    "url": f"https://github.com/{self.github_owner}/{self.github_repo}/compare/{base}...{head}",
                    "number": "0",
                    "error": error_msg
                }

        except Exception as e:
            raise GitError(f"Failed to create pull request: {e}", operation="create_pr")
        except Exception as e:
            raise GitError(f"Failed to create pull request: {e}", operation="create_pr")

    def get_file_history(self, file_path: str, limit: int = 10) -> List[dict]:
        """
        Get commit history for a file.

        Args:
            file_path: Path to file
            limit: Maximum commits to return

        Returns:
            List of commit info dicts
        """
        self._ensure_repo()

        try:
            commits = []
            for commit in self._repo.iter_commits(paths=file_path, max_count=limit):
                commits.append({
                    "sha": commit.hexsha,
                    "message": commit.message.strip(),
                    "author": str(commit.author),
                    "date": commit.authored_datetime.isoformat(),
                })
            return commits

        except Exception as e:
            raise GitError(f"Failed to get file history: {e}", operation="get_history")

    def deploy_artifacts(
        self,
        files: Dict[str, str],
        commit_message: str,
        branch_name: Optional[str] = None,
        create_pr: bool = True,
        pr_title: Optional[str] = None,
        pr_description: Optional[str] = None,
    ) -> Dict[str, str]:
        """
        Full deployment workflow: clone, clear, write, commit, push, PR.

        This is the main method for deploying generated pipeline artifacts.
        If clear_before_push is True, all existing files are deleted first.

        Args:
            files: Dict of file path -> content to deploy
            commit_message: Commit message
            branch_name: Branch name (generated if not provided)
            create_pr: Whether to create a PR after pushing
            pr_title: PR title (uses commit_message if not provided)
            pr_description: PR description

        Returns:
            Dict with deployment results including commit_sha and pr_url
        """
        import uuid
        from datetime import datetime

        results = {
            "status": "pending",
            "repo": f"{self.github_owner}/{self.github_repo}",
            "branch": "",
            "commit_sha": "",
            "pr_url": "",
            "files_deployed": len(files),
            "cleared_existing": False,
        }

        try:
            # Clone the repository
            self._ensure_repo()

            # Generate branch name if not provided
            if not branch_name:
                timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
                branch_name = f"data-agent/{timestamp}-{uuid.uuid4().hex[:8]}"

            results["branch"] = branch_name

            # Create branch
            self.create_branch(branch_name)

            # Clear existing repo contents if configured
            if self.clear_before_push:
                deleted = self.clear_repo_contents(preserve=['.git', '.gitignore', 'README.md'])
                results["cleared_existing"] = deleted > 0
                logger.info("Cleared existing files before push", deleted_count=deleted)

            # Write and commit new files
            commit_sha = self.commit_files(files, commit_message)
            results["commit_sha"] = commit_sha

            # Push to remote
            self.push(branch_name)

            # Create PR if requested
            if create_pr:
                pr_result = self.create_pull_request(
                    title=pr_title or commit_message,
                    description=pr_description or f"Automated pipeline deployment by Data Agent.\n\nFiles: {len(files)}\nBranch: {branch_name}",
                    head=branch_name,
                    base=self.main_branch,
                )
                results["pr_url"] = pr_result.get("url", "")
                results["pr_number"] = pr_result.get("number", "")

            results["status"] = "success"
            logger.info(
                "Deployment completed",
                repo=results["repo"],
                branch=branch_name,
                commit=commit_sha[:8],
                pr_url=results.get("pr_url"),
            )

        except Exception as e:
            results["status"] = "failed"
            results["error"] = str(e)
            logger.error("Deployment failed", error=str(e))
            raise GitError(f"Deployment failed: {e}", operation="deploy")

        finally:
            self.cleanup()

        return results

    def cleanup(self) -> None:
        """Clean up temporary work directory."""
        if self._work_dir and self._work_dir.exists():
            if str(self._work_dir).startswith(tempfile.gettempdir()):
                shutil.rmtree(self._work_dir, ignore_errors=True)
                logger.info("Cleaned up work directory", path=str(self._work_dir))
