"""
APEX Data Agent - API Source Utilities

REST API polling, pagination, rate limiting, and response parsing.
"""

import json
import time
from typing import Any, Dict, List, Optional


def poll_api(
    endpoint: str,
    auth_type: str = "none",
    auth_config: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, str]] = None,
    headers: Optional[Dict[str, str]] = None,
    method: str = "GET",
    **context,
) -> Dict[str, Any]:
    """Poll REST API endpoint with authentication."""
    import requests

    auth_config = auth_config or {}
    headers = headers or {"Content-Type": "application/json"}
    params = params or {}

    # Auth
    if auth_type == "bearer":
        headers["Authorization"] = f"Bearer {auth_config.get('token', '')}"
    elif auth_type == "api_key":
        headers[auth_config.get("header_name", "X-API-Key")] = auth_config.get("key", "")
    elif auth_type == "oauth2":
        token = _get_oauth_token(auth_config)
        headers["Authorization"] = f"Bearer {token}"

    if method.upper() == "GET":
        response = requests.get(endpoint, headers=headers, params=params, timeout=60)
    else:
        response = requests.post(endpoint, headers=headers, json=params, timeout=60)

    response.raise_for_status()
    data = response.json()

    context["ti"].xcom_push(key="api_response_status", value=response.status_code)
    return data


def paginate(
    endpoint: str,
    pagination_type: str = "offset",
    page_size: int = 100,
    max_pages: int = 1000,
    auth_type: str = "none",
    auth_config: Optional[Dict[str, str]] = None,
    rate_limit_per_second: float = 10.0,
    jmespath_expr: Optional[str] = None,
    **context,
) -> List[Dict[str, Any]]:
    """Paginate through API with rate limiting."""
    import requests

    all_records = []
    headers = {"Content-Type": "application/json"}
    auth_config = auth_config or {}

    if auth_type == "bearer":
        headers["Authorization"] = f"Bearer {auth_config.get('token', '')}"

    delay = 1.0 / rate_limit_per_second if rate_limit_per_second > 0 else 0

    for page in range(max_pages):
        params = {}
        if pagination_type == "offset":
            params["offset"] = page * page_size
            params["limit"] = page_size
        elif pagination_type == "page":
            params["page"] = page + 1
            params["page_size"] = page_size
        elif pagination_type == "cursor":
            if page > 0:
                params["cursor"] = context.get("_next_cursor", "")

        response = requests.get(endpoint, headers=headers, params=params, timeout=60)
        response.raise_for_status()
        data = response.json()

        # Extract records
        records = parse_response(data, jmespath_expr) if jmespath_expr else data
        if isinstance(records, list):
            all_records.extend(records)
        elif isinstance(records, dict):
            all_records.append(records)

        # Check end of pagination
        if isinstance(records, list) and len(records) < page_size:
            break

        # Cursor-based pagination
        if pagination_type == "cursor":
            next_cursor = data.get("next_cursor") or data.get("cursor")
            if not next_cursor:
                break
            context["_next_cursor"] = next_cursor

        # Rate limiting
        if delay > 0:
            time.sleep(delay)

    context.get("ti", _DummyTi()).xcom_push(key="api_record_count", value=len(all_records))
    print(f"[api_source] Fetched {len(all_records)} records from {endpoint}")
    return all_records


def parse_response(data: Any, jmespath_expr: Optional[str] = None) -> Any:
    """Extract data from API response using JMESPath expression."""
    if not jmespath_expr:
        return data
    try:
        import jmespath
        return jmespath.search(jmespath_expr, data)
    except ImportError:
        # Fallback: simple dot-notation access
        parts = jmespath_expr.split(".")
        result = data
        for part in parts:
            if isinstance(result, dict):
                result = result.get(part, result)
            elif isinstance(result, list) and part == "[]":
                pass
        return result


def stage_api_response_to_gcs(records: List[Dict], target_path: str, **context) -> str:
    """Stage API response records to GCS as newline-delimited JSON."""
    from google.cloud import storage

    bucket_name = target_path.replace("gs://", "").split("/")[0]
    blob_path = "/".join(target_path.replace("gs://", "").split("/")[1:])

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)

    content = "\n".join(json.dumps(r) for r in records)
    blob.upload_from_string(content, content_type="application/json")

    print(f"[api_source] Staged {len(records)} records to {target_path}")
    return target_path


def _get_oauth_token(auth_config: Dict[str, str]) -> str:
    """Get OAuth2 access token."""
    import requests

    response = requests.post(
        auth_config.get("token_url", ""),
        data={
            "grant_type": "client_credentials",
            "client_id": auth_config.get("client_id", ""),
            "client_secret": auth_config.get("client_secret", ""),
            "scope": auth_config.get("scope", ""),
        },
        timeout=30,
    )
    response.raise_for_status()
    return response.json().get("access_token", "")


class _DummyTi:
    """Fallback when context has no task instance."""
    def xcom_push(self, **kwargs):
        pass
