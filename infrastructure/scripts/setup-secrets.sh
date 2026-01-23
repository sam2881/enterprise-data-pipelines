#!/bin/bash
# =============================================================================
# GCP Secret Manager Setup Script
# =============================================================================
# Creates all required secrets in GCP Secret Manager for the AI Agent Platform.
#
# USAGE:
#   export GCP_PROJECT_ID=your-project-id
#   ./setup-secrets.sh
#
# IMPORTANT: Update secret values before running in production!
# =============================================================================

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Configuration
GCP_PROJECT_ID=${GCP_PROJECT_ID:-agent-ai-test-461120}
ENVIRONMENT=${ENVIRONMENT:-dev}

echo -e "${BLUE}╔═══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║     GCP Secret Manager Setup for AI Agent Platform            ║${NC}"
echo -e "${BLUE}╚═══════════════════════════════════════════════════════════════╝${NC}"

echo -e "\n${YELLOW}Project: ${GCP_PROJECT_ID}${NC}"
echo -e "${YELLOW}Environment: ${ENVIRONMENT}${NC}"

# Enable Secret Manager API
echo -e "\n${BLUE}[1/3] Enabling Secret Manager API...${NC}"
gcloud services enable secretmanager.googleapis.com --project=$GCP_PROJECT_ID
echo -e "${GREEN}✓ API enabled${NC}"

# Function to create or update a secret
create_secret() {
    local secret_id=$1
    local secret_value=$2
    local description=$3

    # Check if secret exists
    if gcloud secrets describe $secret_id --project=$GCP_PROJECT_ID 2>/dev/null; then
        echo -e "  ${YELLOW}Secret $secret_id already exists, adding new version${NC}"
        echo -n "$secret_value" | gcloud secrets versions add $secret_id \
            --project=$GCP_PROJECT_ID \
            --data-file=-
    else
        echo -e "  ${GREEN}Creating secret: $secret_id${NC}"
        echo -n "$secret_value" | gcloud secrets create $secret_id \
            --project=$GCP_PROJECT_ID \
            --replication-policy="automatic" \
            --data-file=- \
            --labels="environment=${ENVIRONMENT},managed-by=ai-agent-platform"
    fi
}

# =============================================================================
# Create Secrets
# =============================================================================

echo -e "\n${BLUE}[2/3] Creating secrets...${NC}"
echo -e "${RED}IMPORTANT: Replace placeholder values with real credentials!${NC}\n"

# LLM Provider Secrets
create_secret "openai-api-key" "${OPENAI_API_KEY:-PLACEHOLDER_OPENAI_KEY}" "OpenAI API Key"
create_secret "anthropic-api-key" "${ANTHROPIC_API_KEY:-PLACEHOLDER_ANTHROPIC_KEY}" "Anthropic API Key"

# Observability Secrets
create_secret "langfuse-public-key" "${LANGFUSE_PUBLIC_KEY:-PLACEHOLDER}" "Langfuse Public Key"
create_secret "langfuse-secret-key" "${LANGFUSE_SECRET_KEY:-PLACEHOLDER}" "Langfuse Secret Key"

# ServiceNow Secrets
create_secret "servicenow-instance-url" "${SNOW_INSTANCE_URL:-https://dev.service-now.com}" "ServiceNow Instance URL"
create_secret "servicenow-username" "${SNOW_USERNAME:-admin}" "ServiceNow Username"
create_secret "servicenow-password" "${SNOW_PASSWORD:-PLACEHOLDER}" "ServiceNow Password"
create_secret "servicenow-api-key" "${SNOW_API_KEY:-PLACEHOLDER}" "ServiceNow API Key"
create_secret "servicenow-client-id" "${SNOW_CLIENT_ID:-PLACEHOLDER}" "ServiceNow OAuth Client ID"
create_secret "servicenow-client-secret" "${SNOW_CLIENT_SECRET:-PLACEHOLDER}" "ServiceNow OAuth Client Secret"

# Jira Secrets
create_secret "jira-url" "${JIRA_URL:-https://your-org.atlassian.net}" "Jira Instance URL"
create_secret "jira-username" "${JIRA_USERNAME:-user@example.com}" "Jira Username"
create_secret "jira-api-token" "${JIRA_API_TOKEN:-PLACEHOLDER}" "Jira API Token"

# GitHub Secrets
create_secret "github-token" "${GITHUB_TOKEN:-PLACEHOLDER}" "GitHub Personal Access Token"
create_secret "github-org" "${GITHUB_ORG:-your-org}" "GitHub Organization"
create_secret "github-repo" "${GITHUB_REPO:-ai-agent-platform}" "GitHub Repository"

# Database Secrets
create_secret "postgres-password" "${POSTGRES_PASSWORD:-admin123}" "PostgreSQL Password"
create_secret "neo4j-password" "${NEO4J_PASSWORD:-adminadmin}" "Neo4j Password"

# Slack Secrets (optional)
create_secret "slack-bot-token" "${SLACK_BOT_TOKEN:-PLACEHOLDER}" "Slack Bot Token"
create_secret "slack-channel" "${SLACK_CHANNEL:-#ai-agent-alerts}" "Slack Channel"

# =============================================================================
# Grant Service Account Access
# =============================================================================

echo -e "\n${BLUE}[3/3] Granting service account access to secrets...${NC}"

SA_EMAIL="ai-agent-sa@${GCP_PROJECT_ID}.iam.gserviceaccount.com"

# Check if service account exists
if gcloud iam service-accounts describe $SA_EMAIL --project=$GCP_PROJECT_ID 2>/dev/null; then
    echo -e "  Granting Secret Accessor role to $SA_EMAIL"

    gcloud projects add-iam-policy-binding $GCP_PROJECT_ID \
        --member="serviceAccount:$SA_EMAIL" \
        --role="roles/secretmanager.secretAccessor" \
        --condition=None --quiet

    echo -e "${GREEN}✓ Service account access granted${NC}"
else
    echo -e "${YELLOW}Service account $SA_EMAIL not found. Run setup-gcp.sh first.${NC}"
fi

# =============================================================================
# Summary
# =============================================================================

echo -e "\n${GREEN}╔═══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║     Secret Manager Setup Complete!                            ║${NC}"
echo -e "${GREEN}╚═══════════════════════════════════════════════════════════════╝${NC}"

echo -e "
${YELLOW}Secrets Created:${NC}
  LLM:
    - openai-api-key
    - anthropic-api-key

  Observability:
    - langfuse-public-key
    - langfuse-secret-key

  ServiceNow:
    - servicenow-instance-url
    - servicenow-username
    - servicenow-password
    - servicenow-api-key
    - servicenow-client-id
    - servicenow-client-secret

  Jira:
    - jira-url
    - jira-username
    - jira-api-token

  GitHub:
    - github-token
    - github-org
    - github-repo

  Database:
    - postgres-password
    - neo4j-password

  Slack:
    - slack-bot-token
    - slack-channel

${RED}IMPORTANT:${NC}
  1. Update placeholder values with real credentials:
     gcloud secrets versions add SECRET_ID --project=$GCP_PROJECT_ID --data-file=-

  2. Never commit real secrets to source control

  3. Rotate secrets regularly

${YELLOW}To access secrets in code:${NC}
  from secrets import get_secret
  api_key = get_secret('OPENAI_API_KEY')
"
