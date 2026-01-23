#!/bin/bash
#
# Start GCP VM Instance
# This script starts a stopped/terminated GCP compute instance
#
# Usage: ./start_gcp_instance.sh --instance-name <name> --zone <zone> [--project <project>]
#

set -e

# Default values
PROJECT="${GCP_PROJECT:-$(gcloud config get-value project 2>/dev/null)}"
WAIT_FOR_RUNNING="true"
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
        --no-wait)
            WAIT_FOR_RUNNING="false"
            shift
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
echo "Starting GCP VM Instance"
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

if [[ "$CURRENT_STATUS" == "RUNNING" ]]; then
    echo "Instance is already running!"
    exit 0
fi

if [[ "$CURRENT_STATUS" == "NOT_FOUND" ]]; then
    echo "ERROR: Instance not found"
    exit 1
fi

# Start the instance
echo ""
echo "Starting instance..."
gcloud compute instances start "$INSTANCE_NAME" \
    --zone="$ZONE" \
    --project="$PROJECT"

# Wait for instance to be running
if [[ "$WAIT_FOR_RUNNING" == "true" ]]; then
    echo ""
    echo "Waiting for instance to be RUNNING..."

    ELAPSED=0
    while [[ $ELAPSED -lt $TIMEOUT ]]; do
        STATUS=$(gcloud compute instances describe "$INSTANCE_NAME" \
            --zone="$ZONE" \
            --project="$PROJECT" \
            --format="value(status)" 2>/dev/null)

        if [[ "$STATUS" == "RUNNING" ]]; then
            echo "Instance is now RUNNING!"
            break
        fi

        echo "  Status: $STATUS (waiting...)"
        sleep 10
        ELAPSED=$((ELAPSED + 10))
    done

    if [[ $ELAPSED -ge $TIMEOUT ]]; then
        echo "WARNING: Timeout waiting for instance to start"
        exit 1
    fi
fi

# Get instance details
echo ""
echo "Instance Details:"
gcloud compute instances describe "$INSTANCE_NAME" \
    --zone="$ZONE" \
    --project="$PROJECT" \
    --format="table(name,status,networkInterfaces[0].accessConfigs[0].natIP,networkInterfaces[0].networkIP)"

echo ""
echo "=========================================="
echo "SUCCESS: Instance started successfully"
echo "=========================================="
