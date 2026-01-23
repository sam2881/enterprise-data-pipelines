#!/bin/bash
# Start GCP VM Instance Script
# This script starts a stopped/terminated GCP VM instance
#
# Usage: ./start_gcp_instance.sh <instance_name> <zone> [project]
#
# Environment variables required:
#   GOOGLE_APPLICATION_CREDENTIALS - Path to service account key

set -e

# Input parameters
INSTANCE_NAME="${1:-}"
ZONE="${2:-us-central1-a}"
PROJECT="${3:-$(gcloud config get-value project 2>/dev/null)}"

# Validate inputs
if [ -z "$INSTANCE_NAME" ]; then
    echo "ERROR: Instance name is required"
    echo "Usage: $0 <instance_name> <zone> [project]"
    exit 1
fi

echo "=========================================="
echo "GCP VM Instance Start Script"
echo "=========================================="
echo "Instance: $INSTANCE_NAME"
echo "Zone: $ZONE"
echo "Project: $PROJECT"
echo "=========================================="

# Pre-check: Verify instance exists and get current status
echo ""
echo "[PRE-CHECK 1] Verifying instance exists..."
CURRENT_STATUS=$(gcloud compute instances describe "$INSTANCE_NAME" \
    --zone="$ZONE" \
    --project="$PROJECT" \
    --format="value(status)" 2>&1) || {
    echo "ERROR: Instance '$INSTANCE_NAME' not found in zone '$ZONE'"
    exit 1
}
echo "Current status: $CURRENT_STATUS"

# Check if already running
if [ "$CURRENT_STATUS" = "RUNNING" ]; then
    echo ""
    echo "Instance is already RUNNING. No action needed."
    exit 0
fi

# Pre-check: Verify we have permissions
echo ""
echo "[PRE-CHECK 2] Verifying permissions..."
gcloud compute instances describe "$INSTANCE_NAME" \
    --zone="$ZONE" \
    --project="$PROJECT" \
    --format="value(name)" > /dev/null 2>&1 || {
    echo "ERROR: Insufficient permissions to manage instance"
    exit 1
}
echo "Permissions verified."

# Execute: Start the instance
echo ""
echo "[EXECUTION] Starting instance..."
gcloud compute instances start "$INSTANCE_NAME" \
    --zone="$ZONE" \
    --project="$PROJECT" \
    --quiet

# Wait for instance to be running
echo ""
echo "[WAITING] Waiting for instance to be RUNNING..."
MAX_WAIT=120
WAIT_INTERVAL=5
ELAPSED=0

while [ $ELAPSED -lt $MAX_WAIT ]; do
    STATUS=$(gcloud compute instances describe "$INSTANCE_NAME" \
        --zone="$ZONE" \
        --project="$PROJECT" \
        --format="value(status)" 2>/dev/null)

    if [ "$STATUS" = "RUNNING" ]; then
        echo "Instance is now RUNNING!"
        break
    fi

    echo "  Status: $STATUS (waiting...)"
    sleep $WAIT_INTERVAL
    ELAPSED=$((ELAPSED + WAIT_INTERVAL))
done

# Post-check: Verify instance is running
echo ""
echo "[POST-CHECK 1] Verifying instance status..."
FINAL_STATUS=$(gcloud compute instances describe "$INSTANCE_NAME" \
    --zone="$ZONE" \
    --project="$PROJECT" \
    --format="value(status)")

if [ "$FINAL_STATUS" != "RUNNING" ]; then
    echo "ERROR: Instance failed to start. Current status: $FINAL_STATUS"
    exit 1
fi
echo "Status: $FINAL_STATUS"

# Post-check: Get instance details
echo ""
echo "[POST-CHECK 2] Getting instance details..."
gcloud compute instances describe "$INSTANCE_NAME" \
    --zone="$ZONE" \
    --project="$PROJECT" \
    --format="table(name,zone.basename(),status,networkInterfaces[0].accessConfigs[0].natIP,machineType.basename())"

echo ""
echo "=========================================="
echo "SUCCESS: Instance '$INSTANCE_NAME' is now RUNNING"
echo "=========================================="
