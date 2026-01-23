"""
Jira API client for issue tracking and status updates.

WHY: Integrates with Jira for ticket status updates and comments.
     Provides audit trail for pipeline generation workflow.

RULES:
1. NEVER parse free-text descriptions for configuration
2. Only use for status updates and linking
3. All configuration must come from validated JSON
4. Log all API calls for audit
"""

from typing import Any, Dict, List, Optional
import structlog
import httpx

from src.config.settings import get_settings

logger = structlog.get_logger()


class JiraClient:
    """
    Jira API client for issue tracking.

    Uses Jira REST API for:
    - Getting issue details
    - Updating issue status
    - Adding comments
    - Linking issues
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        username: Optional[str] = None,
        api_token: Optional[str] = None,
    ) -> None:
        """Initialize Jira client."""
        settings = get_settings()

        self.base_url = base_url or settings.jira_base_url
        self.username = username or settings.jira_username
        self.api_token = api_token

        if not self.api_token and settings.jira_api_token:
            self.api_token = settings.jira_api_token.get_secret_value()

        self._client: Optional[httpx.Client] = None

    @property
    def client(self) -> httpx.Client:
        """Get or create HTTP client."""
        if self._client is None:
            self._client = httpx.Client(
                base_url=f"{self.base_url}/rest/api/3",
                auth=(self.username, self.api_token) if self.api_token else None,
                headers={"Content-Type": "application/json"},
                timeout=30.0,
            )
        return self._client

    def get_issue(self, issue_key: str) -> Dict[str, Any]:
        """
        Get issue details.

        Args:
            issue_key: Jira issue key (e.g., DATA-123)

        Returns:
            Issue details dict
        """
        logger.info("Fetching Jira issue", issue_key=issue_key)

        try:
            response = self.client.get(f"/issue/{issue_key}")
            response.raise_for_status()

            data = response.json()
            logger.debug("Issue fetched", issue_key=issue_key)
            return data

        except httpx.HTTPError as e:
            logger.error("Failed to fetch Jira issue", issue_key=issue_key, error=str(e))
            raise

    def update_ticket_status(self, issue_key: str, status: str, comment: Optional[str] = None) -> None:
        """
        Update issue status with optional comment.

        Args:
            issue_key: Jira issue key
            status: New status name
            comment: Optional comment to add
        """
        logger.info("Updating Jira ticket status", issue_key=issue_key, status=status)

        # Get transition ID for status
        transition_id = self._get_transition_id(issue_key, status)

        if transition_id:
            self.transition_ticket(issue_key, transition_id)

        if comment:
            self.add_comment(issue_key, comment)

    def transition_ticket(self, issue_key: str, transition_name: str) -> None:
        """
        Transition ticket to new status.

        Args:
            issue_key: Jira issue key
            transition_name: Name of transition (e.g., "Done", "In Progress")
        """
        logger.info("Transitioning Jira ticket", issue_key=issue_key, transition=transition_name)

        try:
            # Get available transitions
            response = self.client.get(f"/issue/{issue_key}/transitions")
            response.raise_for_status()
            transitions = response.json().get("transitions", [])

            # Find matching transition
            transition_id = None
            for t in transitions:
                if t["name"].lower() == transition_name.lower():
                    transition_id = t["id"]
                    break

            if not transition_id:
                logger.warning(
                    "Transition not found",
                    issue_key=issue_key,
                    transition=transition_name,
                    available=[t["name"] for t in transitions],
                )
                return

            # Execute transition
            response = self.client.post(
                f"/issue/{issue_key}/transitions",
                json={"transition": {"id": transition_id}},
            )
            response.raise_for_status()

            logger.info("Ticket transitioned", issue_key=issue_key, transition=transition_name)

        except httpx.HTTPError as e:
            logger.error("Failed to transition ticket", issue_key=issue_key, error=str(e))

    def add_comment(self, issue_key: str, comment: str) -> Optional[str]:
        """
        Add comment to issue.

        Args:
            issue_key: Jira issue key
            comment: Comment text

        Returns:
            Comment ID if successful
        """
        logger.info("Adding comment to Jira issue", issue_key=issue_key)

        try:
            # Jira Cloud uses ADF (Atlassian Document Format) for comments
            body = {
                "body": {
                    "type": "doc",
                    "version": 1,
                    "content": [
                        {
                            "type": "paragraph",
                            "content": [{"type": "text", "text": comment}],
                        }
                    ],
                }
            }

            response = self.client.post(f"/issue/{issue_key}/comment", json=body)
            response.raise_for_status()

            result = response.json()
            comment_id = result.get("id")

            logger.debug("Comment added", issue_key=issue_key, comment_id=comment_id)
            return comment_id

        except httpx.HTTPError as e:
            logger.error("Failed to add comment", issue_key=issue_key, error=str(e))
            return None

    def link_issues(
        self,
        source_key: str,
        target_key: str,
        link_type: str = "Relates",
    ) -> None:
        """
        Create link between issues.

        Args:
            source_key: Source issue key
            target_key: Target issue key
            link_type: Type of link (e.g., "Relates", "Blocks")
        """
        logger.info("Linking Jira issues", source=source_key, target=target_key, type=link_type)

        try:
            body = {
                "type": {"name": link_type},
                "inwardIssue": {"key": source_key},
                "outwardIssue": {"key": target_key},
            }

            response = self.client.post("/issueLink", json=body)
            response.raise_for_status()

            logger.info("Issues linked", source=source_key, target=target_key)

        except httpx.HTTPError as e:
            logger.error("Failed to link issues", source=source_key, target=target_key, error=str(e))

    def get_custom_field(
        self,
        issue_key: str,
        field_name: str,
    ) -> Optional[Any]:
        """
        Get custom field value from issue.

        Args:
            issue_key: Jira issue key
            field_name: Custom field name

        Returns:
            Field value or None
        """
        issue = self.get_issue(issue_key)
        fields = issue.get("fields", {})

        # Check standard fields first
        if field_name in fields:
            return fields[field_name]

        # Check custom fields
        for key, value in fields.items():
            if key.startswith("customfield_"):
                # Would need field metadata to match name to ID
                pass

        return None

    def _get_transition_id(self, issue_key: str, status_name: str) -> Optional[str]:
        """Get transition ID for a status name."""
        try:
            response = self.client.get(f"/issue/{issue_key}/transitions")
            response.raise_for_status()
            transitions = response.json().get("transitions", [])

            for t in transitions:
                if t["name"].lower() == status_name.lower():
                    return t["id"]

            return None

        except httpx.HTTPError:
            return None

    def close(self) -> None:
        """Close HTTP client."""
        if self._client:
            self._client.close()
            self._client = None
