#!/bin/bash
# =============================================================================
# Setup GitHub Repository Secrets for CI/CD
# =============================================================================
# This script configures the required secrets in your GitHub repositories
# for the CI/CD pipeline to deploy to GCP Cloud Composer.
#
# Prerequisites:
# - GitHub CLI (gh) installed and authenticated
# - GCP service account key file
# =============================================================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "=============================================="
echo "GitHub Secrets Setup for CI/CD"
echo "=============================================="

# Load environment variables
if [ -f .env ]; then
    source .env
fi

# Configuration
PIPELINES_REPO="${GITHUB_PIPELINES_OWNER}/${GITHUB_PIPELINES_REPO}"
REMEDIATION_REPO="${GITHUB_REMEDIATION_OWNER}/${GITHUB_REMEDIATION_REPO}"
GCP_KEY_FILE="./gcp-service-account-key.json"

echo ""
echo -e "${YELLOW}Configuration:${NC}"
echo "  Pipelines Repo: $PIPELINES_REPO"
echo "  Remediation Repo: $REMEDIATION_REPO"
echo "  GCP Key File: $GCP_KEY_FILE"
echo ""

# Check if gh is installed
if ! command -v gh &> /dev/null; then
    echo -e "${RED}Error: GitHub CLI (gh) is not installed${NC}"
    echo ""
    echo "Install it with:"
    echo "  curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg | sudo dd of=/usr/share/keyrings/githubcli-archive-keyring.gpg"
    echo "  echo \"deb [arch=\$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main\" | sudo tee /etc/apt/sources.list.d/github-cli.list > /dev/null"
    echo "  sudo apt update && sudo apt install gh"
    echo ""
    echo "Then authenticate with: gh auth login"
    exit 1
fi

# Check if authenticated
if ! gh auth status &> /dev/null; then
    echo -e "${RED}Error: Not authenticated with GitHub CLI${NC}"
    echo "Run: gh auth login"
    exit 1
fi

# Check if GCP key file exists
if [ ! -f "$GCP_KEY_FILE" ]; then
    echo -e "${RED}Error: GCP service account key not found at $GCP_KEY_FILE${NC}"
    exit 1
fi

# Read GCP key as single line for GitHub secret
GCP_SA_KEY=$(cat "$GCP_KEY_FILE" | tr -d '\n')
GCP_PROJECT_ID=$(cat "$GCP_KEY_FILE" | grep -o '"project_id": "[^"]*"' | cut -d'"' -f4)

echo -e "${GREEN}Setting up secrets for: $PIPELINES_REPO${NC}"
echo ""

# Set secrets for enterprise-data-pipelines repo
echo "Setting GCP_SA_KEY..."
echo "$GCP_SA_KEY" | gh secret set GCP_SA_KEY --repo "$PIPELINES_REPO"

echo "Setting GCP_PROJECT_ID..."
gh secret set GCP_PROJECT_ID --repo "$PIPELINES_REPO" --body "$GCP_PROJECT_ID"

echo "Setting COMPOSER_LOCATION..."
gh secret set COMPOSER_LOCATION --repo "$PIPELINES_REPO" --body "us-central1"

echo ""
echo -e "${GREEN}Setting up secrets for: $REMEDIATION_REPO${NC}"
echo ""

# Set secrets for test_01 repo (remediation)
echo "Setting GCP_SA_KEY..."
echo "$GCP_SA_KEY" | gh secret set GCP_SA_KEY --repo "$REMEDIATION_REPO"

echo "Setting GCP_PROJECT_ID..."
gh secret set GCP_PROJECT_ID --repo "$REMEDIATION_REPO" --body "$GCP_PROJECT_ID"

echo ""
echo "=============================================="
echo -e "${GREEN}GitHub Secrets Setup Complete!${NC}"
echo "=============================================="
echo ""
echo "Secrets configured:"
echo "  - GCP_SA_KEY (service account JSON)"
echo "  - GCP_PROJECT_ID ($GCP_PROJECT_ID)"
echo "  - COMPOSER_LOCATION (us-central1)"
echo ""
echo "Next steps:"
echo "1. Push code to $PIPELINES_REPO to trigger CI/CD"
echo "2. Check workflow runs at:"
echo "   https://github.com/$PIPELINES_REPO/actions"
echo ""
