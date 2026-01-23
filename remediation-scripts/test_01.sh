#!/bin/bash
#
# test_01.sh - Test remediation script for AI Agent Platform
#
# This script is triggered by GitHub Actions when the AI Agent
# approves a remediation plan for a ServiceNow incident.
#
# Environment variables (set by GitHub Actions):
#   INCIDENT_ID - ServiceNow incident number (e.g., INC0010009)
#   WORKFLOW_ID - LangGraph workflow ID
#   GCP_PROJECT - GCP project ID
#   GCP_ZONE    - GCP zone for VM operations
#

set -e

echo "========================================"
echo "AI Agent Remediation Script: test_01.sh"
echo "========================================"
echo ""

# Log environment
echo "Environment:"
echo "  INCIDENT_ID: ${INCIDENT_ID:-'not set'}"
echo "  WORKFLOW_ID: ${WORKFLOW_ID:-'not set'}"
echo "  GCP_PROJECT: ${GCP_PROJECT:-'not set'}"
echo "  GCP_ZONE: ${GCP_ZONE:-'not set'}"
echo ""

# Step 1: Check GCP authentication
echo "Step 1: Checking GCP authentication..."
if command -v gcloud &> /dev/null; then
    gcloud auth list 2>/dev/null || echo "  No active GCP credentials"
else
    echo "  gcloud CLI not available - skipping"
fi
echo "  [OK] Authentication check complete"
echo ""

# Step 2: Identify terminated VMs (simulated for test)
echo "Step 2: Identifying terminated VMs..."
echo "  Searching for VMs matching incident pattern..."
echo "  Found: dev-vscode (TERMINATED)"
echo "  [OK] VM identified"
echo ""

# Step 3: Start the VM (simulated for test)
echo "Step 3: Starting terminated VM..."
echo "  Command: gcloud compute instances start dev-vscode --zone=${GCP_ZONE:-us-central1-a}"
echo "  [SIMULATED] VM start command would execute here"
echo "  [OK] VM start initiated"
echo ""

# Step 4: Wait for VM to be running
echo "Step 4: Waiting for VM to be running..."
echo "  Checking VM status..."
sleep 2
echo "  Status: RUNNING"
echo "  [OK] VM is now running"
echo ""

# Step 5: Verify health
echo "Step 5: Verifying service health..."
echo "  Checking application endpoints..."
echo "  Health check: PASSED"
echo "  [OK] Service is healthy"
echo ""

# Summary
echo "========================================"
echo "Remediation Complete"
echo "========================================"
echo "  Incident: ${INCIDENT_ID:-'unknown'}"
echo "  Action: VM restart"
echo "  Result: SUCCESS"
echo "  Duration: ~5 seconds"
echo "========================================"

exit 0
