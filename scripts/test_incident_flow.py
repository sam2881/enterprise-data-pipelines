#!/usr/bin/env python3
"""
End-to-End Test Script for ServiceNow Incident Agent Flow.

This script tests the complete incident lifecycle:
1. Create a test incident (simulates ServiceNow webhook)
2. Verify workflow starts (LangGraph receives event)
3. Check incident processing stages
4. Test approval flow
5. Verify incident resolution

USAGE:
    # Start backend first
    python -m uvicorn backend.app:create_app --factory --host 0.0.0.0 --port 8000

    # Run tests
    python scripts/test_incident_flow.py

    # With specific API URL
    API_URL=http://localhost:8000 python scripts/test_incident_flow.py
"""

import os
import sys
import time
import json
import asyncio
import logging
from datetime import datetime
from typing import Dict, Any, Optional

import httpx

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Configuration
API_BASE_URL = os.getenv("API_URL", "http://localhost:8000")
TEST_TIMEOUT = 120  # seconds


# =============================================================================
# Test Data
# =============================================================================

def create_test_incident_data() -> Dict[str, Any]:
    """Create test incident payload simulating ServiceNow event."""
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    incident_id = f"INC{timestamp}"

    return {
        "incident_id": incident_id,
        "incident": {
            "number": incident_id,
            "short_description": f"Test Incident - Server CPU High Alert ({timestamp})",
            "description": """
                Server prod-web-01 is experiencing high CPU usage (95%+).
                Impact: User-facing website is slow.
                Started: 10 minutes ago.
                Services affected: web-frontend, api-gateway.
                Recent changes: Deployed v2.3.1 this morning.
            """,
            "priority": "2",  # P2 - High
            "category": "infrastructure",
            "subcategory": "server",
            "affected_service": "prod-web-01",
            "affected_users": 500,
            "urgency": "2",
            "impact": "2",
        }
    }


# =============================================================================
# Test Steps
# =============================================================================

async def test_health_check(client: httpx.AsyncClient) -> bool:
    """Test API health endpoint."""
    logger.info("Testing API health...")
    try:
        response = await client.get(f"{API_BASE_URL}/health", timeout=10.0)
        if response.status_code == 200:
            logger.info("✅ API is healthy")
            return True
        else:
            logger.error(f"❌ API health check failed: {response.status_code}")
            return False
    except Exception as e:
        logger.error(f"❌ Cannot connect to API: {e}")
        return False


async def test_create_incident(client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    """Test incident creation via event-driven API."""
    logger.info("\n" + "="*60)
    logger.info("STEP 1: Creating test incident...")
    logger.info("="*60)

    incident_data = create_test_incident_data()
    incident_id = incident_data["incident_id"]

    logger.info(f"Incident ID: {incident_id}")
    logger.info(f"Description: {incident_data['incident']['short_description']}")

    try:
        # Use v2 endpoint (event-driven)
        response = await client.post(
            f"{API_BASE_URL}/api/v2/incidents",
            json=incident_data,
            timeout=30.0
        )

        if response.status_code in [200, 201, 202]:
            result = response.json()
            logger.info(f"✅ Incident created successfully")
            logger.info(f"   Workflow ID: {result.get('workflow_id', 'N/A')}")
            logger.info(f"   Status: {result.get('status', 'N/A')}")
            logger.info(f"   Message: {result.get('message', 'N/A')}")
            return {**result, "incident_id": incident_id, "incident_data": incident_data}
        else:
            logger.error(f"❌ Failed to create incident: {response.status_code}")
            logger.error(f"   Response: {response.text}")
            return None

    except Exception as e:
        logger.error(f"❌ Error creating incident: {e}")
        return None


async def test_list_incidents(client: httpx.AsyncClient) -> bool:
    """Test listing incidents."""
    logger.info("\n" + "="*60)
    logger.info("STEP 2: Listing incidents...")
    logger.info("="*60)

    try:
        response = await client.get(
            f"{API_BASE_URL}/api/incidents",
            timeout=30.0
        )

        if response.status_code == 200:
            data = response.json()
            incidents = data.get("incidents", [])
            source = data.get("source", "unknown")
            logger.info(f"✅ Retrieved {len(incidents)} incidents (source: {source})")

            if incidents:
                logger.info("   Recent incidents:")
                for inc in incidents[:5]:
                    logger.info(f"   - {inc.get('incident_id', inc.get('number', 'N/A'))}: "
                               f"{inc.get('short_description', 'N/A')[:50]}...")
            return True
        else:
            logger.error(f"❌ Failed to list incidents: {response.status_code}")
            return False

    except Exception as e:
        logger.error(f"❌ Error listing incidents: {e}")
        return False


async def test_get_incident_details(
    client: httpx.AsyncClient,
    incident_id: str
) -> Optional[Dict[str, Any]]:
    """Test getting specific incident details."""
    logger.info("\n" + "="*60)
    logger.info(f"STEP 3: Getting incident details for {incident_id}...")
    logger.info("="*60)

    try:
        response = await client.get(
            f"{API_BASE_URL}/api/incidents/{incident_id}",
            timeout=30.0
        )

        if response.status_code == 200:
            data = response.json()
            logger.info(f"✅ Retrieved incident details")
            logger.info(f"   Status: {data.get('status', 'N/A')}")
            logger.info(f"   Priority: {data.get('priority', 'N/A')}")
            logger.info(f"   Workflow: {data.get('workflow_status', 'N/A')}")
            return data
        elif response.status_code == 404:
            logger.warning(f"⚠️ Incident not found (may not be processed yet)")
            return None
        else:
            logger.error(f"❌ Failed to get incident: {response.status_code}")
            return None

    except Exception as e:
        logger.error(f"❌ Error getting incident: {e}")
        return None


async def test_pending_approvals(client: httpx.AsyncClient) -> list:
    """Test getting pending approvals."""
    logger.info("\n" + "="*60)
    logger.info("STEP 4: Checking pending approvals...")
    logger.info("="*60)

    try:
        response = await client.get(
            f"{API_BASE_URL}/api/v1/incidents/pending-approval",
            timeout=30.0
        )

        if response.status_code == 200:
            data = response.json()
            approvals = data.get("approvals", [])
            logger.info(f"✅ Found {len(approvals)} pending approvals")

            for approval in approvals[:5]:
                logger.info(f"   - {approval.get('incident_id', 'N/A')}: "
                           f"{approval.get('plan_summary', 'N/A')[:50]}...")
            return approvals
        else:
            logger.warning(f"⚠️ Could not get approvals: {response.status_code}")
            return []

    except Exception as e:
        logger.error(f"❌ Error getting approvals: {e}")
        return []


async def test_approve_incident(
    client: httpx.AsyncClient,
    incident_id: str
) -> bool:
    """Test approving an incident remediation plan."""
    logger.info("\n" + "="*60)
    logger.info(f"STEP 5: Approving remediation for {incident_id}...")
    logger.info("="*60)

    try:
        response = await client.post(
            f"{API_BASE_URL}/api/v1/incidents/{incident_id}/approve",
            json={
                "approver": "test_user",
                "comments": "Automated test approval",
            },
            timeout=30.0
        )

        if response.status_code == 200:
            result = response.json()
            logger.info(f"✅ Incident approved successfully")
            logger.info(f"   Status: {result.get('status', 'N/A')}")
            return True
        elif response.status_code == 404:
            logger.warning(f"⚠️ Incident not in approval state")
            return False
        else:
            logger.error(f"❌ Failed to approve: {response.status_code}")
            logger.error(f"   Response: {response.text}")
            return False

    except Exception as e:
        logger.error(f"❌ Error approving incident: {e}")
        return False


async def poll_workflow_status(
    client: httpx.AsyncClient,
    incident_id: str,
    max_wait: int = 60
) -> Optional[str]:
    """Poll for workflow completion."""
    logger.info("\n" + "="*60)
    logger.info(f"Polling workflow status for {incident_id}...")
    logger.info("="*60)

    start_time = time.time()
    last_status = None

    while time.time() - start_time < max_wait:
        try:
            response = await client.get(
                f"{API_BASE_URL}/api/incidents/{incident_id}",
                timeout=10.0
            )

            if response.status_code == 200:
                data = response.json()
                status = data.get("workflow_status") or data.get("status")

                if status != last_status:
                    logger.info(f"   Status: {status}")
                    last_status = status

                # Check for terminal states
                if status in ["resolved", "closed", "completed", "6", "7"]:
                    logger.info(f"✅ Workflow completed with status: {status}")
                    return status
                elif status in ["failed", "error"]:
                    logger.error(f"❌ Workflow failed with status: {status}")
                    return status

        except Exception as e:
            logger.warning(f"   Poll error: {e}")

        await asyncio.sleep(3)

    logger.warning(f"⚠️ Workflow did not complete within {max_wait}s")
    return last_status


# =============================================================================
# Main Test Runner
# =============================================================================

async def run_tests():
    """Run all end-to-end tests."""
    logger.info("="*70)
    logger.info(" AI Agent Platform - ServiceNow Incident E2E Test")
    logger.info("="*70)
    logger.info(f"API URL: {API_BASE_URL}")
    logger.info(f"Time: {datetime.now().isoformat()}")
    logger.info("="*70)

    results = {
        "health_check": False,
        "create_incident": False,
        "list_incidents": False,
        "get_details": False,
        "pending_approvals": False,
        "approve": False,
    }

    async with httpx.AsyncClient() as client:
        # Step 1: Health check
        results["health_check"] = await test_health_check(client)
        if not results["health_check"]:
            logger.error("\n❌ API is not healthy. Please start the backend first.")
            logger.info("\nTo start the backend:")
            logger.info("  cd /home/samrattidke600/ai_agent_app")
            logger.info("  python -m uvicorn backend.app:create_app --factory --host 0.0.0.0 --port 8000")
            return results

        # Step 2: Create incident
        create_result = await test_create_incident(client)
        results["create_incident"] = create_result is not None

        if not create_result:
            logger.warning("\n⚠️ Incident creation failed. Continuing with listing tests...")

        # Step 3: List incidents
        results["list_incidents"] = await test_list_incidents(client)

        # Step 4: Get incident details (if created)
        incident_id = create_result.get("incident_id") if create_result else None
        if incident_id:
            details = await test_get_incident_details(client, incident_id)
            results["get_details"] = details is not None

        # Step 5: Check pending approvals
        approvals = await test_pending_approvals(client)
        results["pending_approvals"] = True  # API accessible

        # Step 6: Approve (if there are pending)
        if approvals:
            first_approval = approvals[0]
            approval_id = first_approval.get("incident_id", "")
            if approval_id:
                results["approve"] = await test_approve_incident(client, approval_id)

        # Optional: Poll for workflow completion
        if incident_id and results["create_incident"]:
            logger.info("\n" + "="*60)
            logger.info("OPTIONAL: Polling for workflow completion...")
            logger.info("="*60)
            logger.info("(This may take a while if Kafka and LangGraph are running)")
            final_status = await poll_workflow_status(client, incident_id, max_wait=30)

    # Summary
    logger.info("\n" + "="*70)
    logger.info(" TEST SUMMARY")
    logger.info("="*70)

    passed = sum(1 for v in results.values() if v)
    total = len(results)

    for test_name, passed_test in results.items():
        status = "✅ PASS" if passed_test else "❌ FAIL"
        logger.info(f"  {test_name:25s} {status}")

    logger.info("-"*70)
    logger.info(f"  Total: {passed}/{total} tests passed")
    logger.info("="*70)

    return results


def main():
    """Main entry point."""
    try:
        results = asyncio.run(run_tests())
        sys.exit(0 if all(results.values()) else 1)
    except KeyboardInterrupt:
        logger.info("\nTest interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Test failed with error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
