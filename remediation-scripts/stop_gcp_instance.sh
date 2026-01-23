#!/bin/bash
#
# Stop GCP VM Instance
# This script stops a running GCP compute instance
#
# Usage: ./stop_gcp_instance.sh --instance-name <name> --zone <zone> [--project <project>]
#

set -e

# Default values
PROJECT="${GCP_PROJECT:-$(gcloud config get-value project 2>/dev/null)}"
TIMEOUT=300

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --instance-name|--instance_name)
            INSTANCE_NAME="$2"
            shift 2
            ;;
        --zone)
            ZONE="$2"
            shift 2
            ;;
        --project)
            PROJECT="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Validate required parameters
if [[ -z "$INSTANCE_NAME" ]]; then
    echo "ERROR: --instance-name is required"
    exit 1
fi

if [[ -z "$ZONE" ]]; then
    echo "ERROR: --zone is required"
    exit 1
fi

echo "=========================================="
echo "Stopping GCP VM Instance"
echo "=========================================="
echo "Instance: $INSTANCE_NAME"
echo "Zone: $ZONE"
echo "Project: $PROJECT"
echo "=========================================="

# Check current instance status
echo ""
echo "Checking current instance status..."
CURRENT_STATUS=$(gcloud compute instances describe "$INSTANCE_NAME" \
    --zone="$ZONE" \
    --project="$PROJECT" \
    --format="value(status)" 2>/dev/null || echo "NOT_FOUND")

echo "Current status: $CURRENT_STATUS"

if [[ "$CURRENT_STATUS" == "TERMINATED" ]] || [[ "$CURRENT_STATUS" == "STOPPED" ]]; then
    echo "Instance is already stopped!"
    exit 0
fi

if [[ "$CURRENT_STATUS" == "NOT_FOUND" ]]; then
    echo "ERROR: Instance not found"
    exit 1
fi

# Stop the instance
echo ""
echo "Stopping instance..."
gcloud compute instances stop "$INSTANCE_NAME" \
    --zone="$ZONE" \
    --project="$PROJECT"

# Wait for instance to be stopped
echo ""
echo "Waiting for instance to be TERMINATED..."

ELAPSED=0
while [[ $ELAPSED -lt $TIMEOUT ]]; do
    STATUS=$(gcloud compute instances describe "$INSTANCE_NAME" \
        --zone="$ZONE" \
        --project="$PROJECT" \
        --format="value(status)" 2>/dev/null)

    if [[ "$STATUS" == "TERMINATED" ]] || [[ "$STATUS" == "STOPPED" ]]; then
        echo "Instance is now TERMINATED!"
        break
    fi

    echo "  Status: $STATUS (waiting...)"
    sleep 10
    ELAPSED=$((ELAPSED + 10))
done

echo ""
echo "=========================================="
echo "SUCCESS: Instance stopped successfully"
echo "=========================================="
