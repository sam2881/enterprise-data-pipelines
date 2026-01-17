#!/bin/bash
# Deployment Script for enterprise-data-pipelines
#
# Usage: ./scripts/deploy.sh <environment>
#   environment: dev, staging, or prod

set -e

ENVIRONMENT=${1:-dev}
PROJECT_ID="ai-agent-platform"

case $ENVIRONMENT in
  dev)
    PROJECT_ID="${PROJECT_ID}-dev"
    ;;
  staging)
    PROJECT_ID="${PROJECT_ID}-stg"
    ;;
  prod)
    # Production uses base project ID
    ;;
  *)
    echo "Invalid environment: $ENVIRONMENT"
    echo "Usage: $0 <dev|staging|prod>"
    exit 1
    ;;
esac

echo "Deploying to $ENVIRONMENT ($PROJECT_ID)..."

# Get Composer bucket
COMPOSER_ENV="${PROJECT_ID}-composer"
BUCKET=$(gcloud composer environments describe $COMPOSER_ENV \
  --location=us-central1 \
  --project=$PROJECT_ID \
  --format='value(config.dagGcsPrefix)')

echo "Syncing DAGs to $BUCKET/dags/"
gsutil -m rsync -r -d dags/ $BUCKET/dags/

echo "Syncing common modules to $BUCKET/plugins/common/"
gsutil -m rsync -r -d common/ $BUCKET/plugins/common/

echo "Deployment complete!"
