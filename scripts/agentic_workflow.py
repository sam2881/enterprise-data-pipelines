#!/usr/bin/env python3
"""
End-to-End Agentic AI Workflow

Complete automated incident management workflow:
1. Monitor GCP VM status
2. Detect VM OFF state
3. Check/Create ServiceNow incident (idempotent)
4. Search RAG for remediation scripts
5. Use Swarm agents to select best script
6. Trigger GitHub Actions for remediation
7. Monitor execution and resolve incident

This implements a production-safe, idempotent agentic AI workflow.
"""
import os
import sys
import json
import time
import base64
import asyncio
import subprocess
from datetime import datetime
from typing import Optional, Dict, List, Tuple
from dataclasses import dataclass, asdict

import httpx
from dotenv import load_dotenv

# Add backend to path
sys.path.insert(0, "/home/samrattidke600/ai_agent_app/backend")

load_dotenv("/home/samrattidke600/ai_agent_app/.env")

# Import RAG components
from rag.script_ingestion import RAGIngestionService
from rag.swarm_script_selector import SwarmScriptSelector, SwarmDecision

# Configuration
VM_NAME = "test-incident-vm-01"
GCP_PROJECT = os.getenv("GCP_PROJECT_ID", "agent-ai-test-461120")
GCP_ZONE = "us-central1-a"

SNOW_INSTANCE = os.getenv("SNOW_INSTANCE_URL", "").rstrip("/")
SNOW_USER = os.getenv("SNOW_USERNAME", "")
SNOW_PASS = os.getenv("SNOW_PASSWORD", "")

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")
GITHUB_OWNER = "sam2881"
GITHUB_REPO = "test_01"

INCIDENT_MESSAGE = f"{VM_NAME} instance is off"
REGISTRY_PATH = "/home/samrattidke600/ai_agent_app/backend/data/registry.json"


@dataclass
class WorkflowState:
    """State of the agentic workflow"""
    started_at: str
    vm_name: str
    vm_status: str
    incident_sys_id: Optional[str] = None
    incident_number: Optional[str] = None
    script_selected: Optional[str] = None
    script_path: Optional[str] = None
    workflow_file: Optional[str] = None
    github_run_id: Optional[int] = None
    execution_status: Optional[str] = None
    final_vm_status: Optional[str] = None
    completed: bool = False
    success: bool = False
    error: Optional[str] = None


class AgenticWorkflow:
    """
    Complete end-to-end agentic workflow for incident management.

    Workflow Steps:
    1. Check VM status (GCP API)
    2. Create/find ServiceNow incident (idempotent)
    3. Search RAG for remediation scripts
    4. Use Swarm agents to select best script
    5. Trigger GitHub Actions
    6. Monitor execution
    7. Verify remediation
    8. Resolve incident
    """

    def __init__(self):
        self.state = WorkflowState(
            started_at=datetime.now().isoformat(),
            vm_name=VM_NAME,
            vm_status="UNKNOWN"
        )
        self.rag_service = RAGIngestionService()
        self.swarm_selector = SwarmScriptSelector()

    def _get_snow_headers(self) -> Dict[str, str]:
        """Get ServiceNow authentication headers"""
        auth_str = f"{SNOW_USER}:{SNOW_PASS}"
        auth_b64 = base64.b64encode(auth_str.encode("utf-8")).decode("utf-8")
        return {
            "Authorization": f"Basic {auth_b64}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }

    def _get_github_headers(self) -> Dict[str, str]:
        """Get GitHub authentication headers"""
        return {
            "Authorization": f"Bearer {GITHUB_TOKEN}",
            "Accept": "application/vnd.github.v3+json",
            "X-GitHub-Api-Version": "2022-11-28"
        }

    # =========================================================================
    # STEP 1: Check GCP VM Status
    # =========================================================================
    def check_vm_status(self) -> Tuple[bool, str]:
        """Check GCP VM status using gcloud CLI"""
        print("\n" + "=" * 70)
        print("STEP 1: CHECKING GCP VM STATUS")
        print("=" * 70)

        try:
            result = subprocess.run(
                [
                    "gcloud", "compute", "instances", "describe",
                    VM_NAME,
                    f"--project={GCP_PROJECT}",
                    f"--zone={GCP_ZONE}",
                    "--format=json"
                ],
                capture_output=True,
                text=True,
                timeout=30
            )

            if result.returncode != 0:
                print(f"  Error: {result.stderr}")
                return False, "ERROR"

            vm_data = json.loads(result.stdout)
            status = vm_data.get("status", "UNKNOWN")
            is_running = status == "RUNNING"

            self.state.vm_status = status
            print(f"  VM: {VM_NAME}")
            print(f"  Project: {GCP_PROJECT}")
            print(f"  Zone: {GCP_ZONE}")
            print(f"  Status: {status}")

            return is_running, status

        except Exception as e:
            print(f"  Exception: {e}")
            self.state.error = str(e)
            return False, "ERROR"

    # =========================================================================
    # STEP 2: ServiceNow Incident Management (Idempotent)
    # =========================================================================
    async def find_existing_incident(self) -> Optional[Dict]:
        """Find active incident for this VM (idempotent check)"""
        url = f"{SNOW_INSTANCE}/api/now/table/incident"
        params = {
            "sysparm_query": f"short_descriptionLIKE{VM_NAME}^state!=6^state!=7",
            "sysparm_limit": "1"
        }

        async with httpx.AsyncClient() as client:
            response = await client.get(
                url,
                headers=self._get_snow_headers(),
                params=params,
                timeout=30.0
            )

            if response.status_code == 200:
                results = response.json().get("result", [])
                if results:
                    return results[0]
        return None

    async def create_incident(self) -> Optional[Dict]:
        """Create ServiceNow incident for VM being OFF"""
        print("\n" + "=" * 70)
        print("STEP 2: SERVICENOW INCIDENT MANAGEMENT")
        print("=" * 70)

        # Idempotent check - find existing incident
        print("  Checking for existing active incident...")
        existing = await self.find_existing_incident()

        if existing:
            inc_number = existing.get("number")
            sys_id = existing.get("sys_id")
            print(f"  Found existing incident: {inc_number}")
            self.state.incident_sys_id = sys_id
            self.state.incident_number = inc_number
            return existing

        # Create new incident
        print("  Creating new incident...")
        url = f"{SNOW_INSTANCE}/api/now/table/incident"
        payload = {
            "short_description": INCIDENT_MESSAGE,
            "description": f"""GCP VM '{VM_NAME}' in project '{GCP_PROJECT}' is in {self.state.vm_status} state.

This incident was automatically created by the AI Agent Platform.

Workflow Details:
- VM Name: {VM_NAME}
- GCP Project: {GCP_PROJECT}
- GCP Zone: {GCP_ZONE}
- Detected Status: {self.state.vm_status}
- Detection Time: {self.state.started_at}

Automated remediation will be attempted using RAG-matched scripts and Swarm-based agent selection.""",
            "urgency": "2",
            "impact": "2",
            "category": "Infrastructure",
            "subcategory": "Cloud"
        }

        async with httpx.AsyncClient() as client:
            response = await client.post(
                url,
                headers=self._get_snow_headers(),
                json=payload,
                timeout=30.0
            )

            if response.status_code in [200, 201]:
                result = response.json().get("result", {})
                inc_number = result.get("number")
                sys_id = result.get("sys_id")
                print(f"  Created incident: {inc_number}")
                print(f"  sys_id: {sys_id}")
                self.state.incident_sys_id = sys_id
                self.state.incident_number = inc_number
                return result
            else:
                print(f"  Error creating incident: {response.status_code}")
                print(f"  Response: {response.text}")
                return None

    # =========================================================================
    # STEP 3: RAG Search for Remediation Scripts
    # =========================================================================
    async def search_remediation_scripts(self) -> List[Dict]:
        """Search RAG for matching remediation scripts"""
        print("\n" + "=" * 70)
        print("STEP 3: RAG SEARCH FOR REMEDIATION SCRIPTS")
        print("=" * 70)

        # Search query based on incident
        query = f"{INCIDENT_MESSAGE} {self.state.vm_status}"
        print(f"  Search query: '{query}'")

        results = await self.rag_service.search_scripts(query, limit=5)

        print(f"  Found {len(results)} candidate scripts:")
        for i, r in enumerate(results, 1):
            print(f"    {i}. {r.get('name', 'Unknown')} "
                  f"(vector: {r.get('vector_score', 0):.2f}, "
                  f"graph: {r.get('graph_score', 0)})")

        return results

    # =========================================================================
    # STEP 4: Swarm-based Script Selection
    # =========================================================================
    async def select_script_with_swarm(
        self,
        candidates: List[Dict]
    ) -> SwarmDecision:
        """Use Swarm agents to select the best script"""
        print("\n" + "=" * 70)
        print("STEP 4: SWARM-BASED SCRIPT SELECTION")
        print("=" * 70)

        incident_data = {
            "short_description": INCIDENT_MESSAGE,
            "description": f"GCP VM {VM_NAME} is {self.state.vm_status}",
            "priority": "2",
            "category": "Infrastructure"
        }

        decision = await self.swarm_selector.select_script(incident_data, candidates)

        self.state.script_selected = decision.selected_script.script_id
        self.state.script_path = decision.selected_script.path
        self.state.workflow_file = decision.selected_script.workflow

        print(f"\n  SWARM DECISION:")
        print(f"    Selected Script: {decision.selected_script.name}")
        print(f"    Path: {decision.selected_script.path}")
        print(f"    Workflow: {decision.selected_script.workflow}")
        print(f"    Final Score: {decision.final_score:.2f}")
        print(f"    Confidence: {decision.confidence:.2f}")
        print(f"    Auto-Approved: {decision.approved}")

        return decision

    # =========================================================================
    # STEP 5: Trigger GitHub Actions
    # =========================================================================
    async def trigger_github_actions(self, decision: SwarmDecision) -> Optional[int]:
        """Trigger GitHub Actions workflow to execute remediation"""
        print("\n" + "=" * 70)
        print("STEP 5: TRIGGERING GITHUB ACTIONS REMEDIATION")
        print("=" * 70)

        if not decision.approved and decision.requires_human_approval:
            print("  Script requires human approval - skipping auto-execution")
            self.state.execution_status = "PENDING_APPROVAL"
            return None

        url = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/actions/workflows/{decision.selected_script.workflow}/dispatches"

        # Prepare script arguments
        script_args = {
            "instance_name": VM_NAME,
            "project": GCP_PROJECT,
            "zone": GCP_ZONE
        }

        payload = {
            "ref": "main",
            "inputs": {
                "script_path": decision.selected_script.path,
                "script_args": json.dumps(script_args),
                "environment": "production",
                "incident_id": self.state.incident_sys_id or "N/A",
                "dry_run": "false"
            }
        }

        print(f"  Workflow: {decision.selected_script.workflow}")
        print(f"  Script: {decision.selected_script.path}")
        print(f"  Arguments: {script_args}")

        async with httpx.AsyncClient() as client:
            response = await client.post(
                url,
                headers=self._get_github_headers(),
                json=payload,
                timeout=30.0
            )

            if response.status_code in [200, 204]:
                print("  Successfully triggered workflow")
                self.state.execution_status = "TRIGGERED"

                # Wait and get run ID
                await asyncio.sleep(3)
                run_id = await self._get_latest_run_id(decision.selected_script.workflow)

                if run_id:
                    self.state.github_run_id = run_id
                    print(f"  Run ID: {run_id}")
                    print(f"  URL: https://github.com/{GITHUB_OWNER}/{GITHUB_REPO}/actions/runs/{run_id}")

                return run_id
            else:
                print(f"  Error triggering workflow: {response.status_code}")
                print(f"  Response: {response.text}")
                self.state.execution_status = "TRIGGER_FAILED"
                return None

    async def _get_latest_run_id(self, workflow_file: str) -> Optional[int]:
        """Get the latest workflow run ID"""
        url = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/actions/workflows/{workflow_file}/runs"

        async with httpx.AsyncClient() as client:
            response = await client.get(
                url,
                headers=self._get_github_headers(),
                params={"per_page": "1"},
                timeout=30.0
            )

            if response.status_code == 200:
                runs = response.json().get("workflow_runs", [])
                if runs:
                    return runs[0].get("id")
        return None

    # =========================================================================
    # STEP 6: Monitor Execution
    # =========================================================================
    async def wait_for_execution(self, run_id: int, timeout: int = 300) -> bool:
        """Wait for GitHub Actions workflow to complete"""
        print("\n" + "=" * 70)
        print("STEP 6: MONITORING EXECUTION")
        print("=" * 70)

        url = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/actions/runs/{run_id}"
        start_time = time.time()

        while time.time() - start_time < timeout:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    url,
                    headers=self._get_github_headers(),
                    timeout=30.0
                )

                if response.status_code == 200:
                    data = response.json()
                    status = data.get("status")
                    conclusion = data.get("conclusion")

                    elapsed = int(time.time() - start_time)
                    print(f"  [{elapsed}s] Status: {status}, Conclusion: {conclusion}")

                    if status == "completed":
                        self.state.execution_status = conclusion.upper() if conclusion else "UNKNOWN"

                        if conclusion == "success":
                            print("  Workflow completed successfully!")
                            return True
                        else:
                            print(f"  Workflow failed: {conclusion}")
                            return False

            await asyncio.sleep(10)

        print(f"  Timeout after {timeout}s")
        self.state.execution_status = "TIMEOUT"
        return False

    # =========================================================================
    # STEP 7: Verify Remediation
    # =========================================================================
    def verify_remediation(self) -> bool:
        """Verify VM is now running"""
        print("\n" + "=" * 70)
        print("STEP 7: VERIFYING REMEDIATION")
        print("=" * 70)

        # Wait for VM to fully start
        time.sleep(15)

        is_running, status = self.check_vm_status()
        self.state.final_vm_status = status

        if is_running:
            print(f"  VM {VM_NAME} is now RUNNING - Remediation successful!")
            return True
        else:
            print(f"  VM {VM_NAME} is still {status} - Remediation may have failed")
            return False

    # =========================================================================
    # STEP 8: Resolve Incident
    # =========================================================================
    async def resolve_incident(self, success: bool):
        """Update ServiceNow incident with resolution"""
        print("\n" + "=" * 70)
        print("STEP 8: RESOLVING SERVICENOW INCIDENT")
        print("=" * 70)

        if not self.state.incident_sys_id:
            print("  No incident to resolve")
            return

        url = f"{SNOW_INSTANCE}/api/now/table/incident/{self.state.incident_sys_id}"

        if success:
            state = "6"  # Resolved
            resolution_notes = f"""VM {VM_NAME} has been successfully started.

Remediation Details:
- Script Used: {self.state.script_selected}
- Script Path: {self.state.script_path}
- GitHub Run ID: {self.state.github_run_id}
- Final VM Status: {self.state.final_vm_status}

This incident was automatically resolved by the AI Agent Platform using:
- RAG-based script matching (Weaviate + Neo4j)
- Swarm-based multi-agent script selection
- GitHub Actions automated execution"""
        else:
            state = "2"  # In Progress
            resolution_notes = f"""Automated remediation attempted but requires attention.

Details:
- Script Attempted: {self.state.script_selected}
- Execution Status: {self.state.execution_status}
- Final VM Status: {self.state.final_vm_status}

Manual intervention may be required."""

        payload = {
            "state": state,
            "work_notes": resolution_notes
        }

        if success:
            payload["close_notes"] = resolution_notes
            payload["close_code"] = "Solved (Permanently)"

        async with httpx.AsyncClient() as client:
            response = await client.patch(
                url,
                headers=self._get_snow_headers(),
                json=payload,
                timeout=30.0
            )

            if response.status_code == 200:
                result = response.json().get("result", {})
                print(f"  Updated incident {self.state.incident_number}")
                print(f"  New state: {state} ({'Resolved' if state == '6' else 'In Progress'})")
            else:
                print(f"  Error updating incident: {response.status_code}")

    # =========================================================================
    # Main Workflow Execution
    # =========================================================================
    async def run(self):
        """Execute the complete agentic workflow"""
        print("=" * 70)
        print("AGENTIC AI WORKFLOW - AUTOMATED INCIDENT MANAGEMENT")
        print(f"Started: {self.state.started_at}")
        print("=" * 70)

        try:
            # Step 1: Check VM status
            is_running, status = self.check_vm_status()

            if is_running:
                print(f"\nVM {VM_NAME} is already RUNNING. No action needed.")
                self.state.completed = True
                self.state.success = True
                return self.state

            print(f"\nVM {VM_NAME} is {status} - Starting remediation workflow...")

            # Step 2: Create/find incident (idempotent)
            incident = await self.create_incident()
            if not incident:
                raise Exception("Failed to create/find ServiceNow incident")

            # Step 3: Search RAG for scripts
            candidates = await self.search_remediation_scripts()
            if not candidates:
                raise Exception("No remediation scripts found in RAG")

            # Step 4: Swarm script selection
            decision = await self.select_script_with_swarm(candidates)

            # Step 5: Trigger GitHub Actions
            run_id = await self.trigger_github_actions(decision)
            if not run_id:
                if self.state.execution_status == "PENDING_APPROVAL":
                    print("\nWorkflow paused - awaiting human approval")
                    self.state.completed = True
                    self.state.success = False
                    return self.state
                raise Exception("Failed to trigger GitHub Actions")

            # Step 6: Monitor execution
            execution_success = await self.wait_for_execution(run_id)

            # Step 7: Verify remediation
            remediation_success = self.verify_remediation() if execution_success else False

            # Step 8: Resolve incident
            await self.resolve_incident(remediation_success)

            # Final state
            self.state.completed = True
            self.state.success = remediation_success

        except Exception as e:
            print(f"\nERROR: {e}")
            self.state.error = str(e)
            self.state.completed = True
            self.state.success = False

            # Try to update incident with error
            if self.state.incident_sys_id:
                await self.resolve_incident(False)

        return self.state

    def print_summary(self):
        """Print workflow summary"""
        print("\n" + "=" * 70)
        print("WORKFLOW SUMMARY")
        print("=" * 70)
        print(f"  VM Name: {self.state.vm_name}")
        print(f"  Initial Status: {self.state.vm_status}")
        print(f"  Final Status: {self.state.final_vm_status or 'N/A'}")
        print(f"  Incident: {self.state.incident_number or 'N/A'}")
        print(f"  Script Selected: {self.state.script_selected or 'N/A'}")
        print(f"  GitHub Run ID: {self.state.github_run_id or 'N/A'}")
        print(f"  Execution Status: {self.state.execution_status or 'N/A'}")
        print(f"  Completed: {self.state.completed}")
        print(f"  Success: {self.state.success}")
        if self.state.error:
            print(f"  Error: {self.state.error}")
        print("=" * 70)


async def main():
    """Main entry point"""
    workflow = AgenticWorkflow()
    state = await workflow.run()
    workflow.print_summary()

    return 0 if state.success else 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
