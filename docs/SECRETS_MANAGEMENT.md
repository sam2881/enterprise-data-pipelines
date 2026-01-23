# Secrets Management Guide

## Overview

This document describes how to securely manage secrets in the AI Agent Platform.

**Golden Rules:**
1. NEVER commit secrets to Git
2. ALWAYS use GCP Secret Manager in production
3. Use environment variables only for local development

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     SECRETS MANAGEMENT                               │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌─────────────────────┐     ┌─────────────────────┐                │
│  │   LOCAL DEV         │     │   PRODUCTION        │                │
│  │                     │     │                     │                │
│  │  .env.example       │     │  GCP Secret Manager │                │
│  │       ↓ copy        │     │       ↓             │                │
│  │  .env (gitignored)  │     │  SecretManager.get()│                │
│  │       ↓             │     │       ↓             │                │
│  │  os.environ         │     │  Cached in memory   │                │
│  └─────────────────────┘     └─────────────────────┘                │
│                                                                      │
│                    ┌─────────────────────┐                          │
│                    │   SecretManager     │                          │
│                    │   (Singleton)       │                          │
│                    │                     │                          │
│                    │  - Caching (1hr)    │                          │
│                    │  - Fallback logic   │                          │
│                    │  - Env-based prefix │                          │
│                    └─────────────────────┘                          │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

## Quick Start

### 1. Local Development

```bash
# Copy the example file
cp .env.example .env

# Edit with your values
vim .env

# The .env file is gitignored and will never be committed
```

### 2. Production (GCP)

```bash
# Create secrets in GCP Secret Manager
gcloud secrets create openai-api-key --data-file=-
# Enter your API key, then Ctrl+D

gcloud secrets create anthropic-api-key --data-file=-
# Enter your API key, then Ctrl+D

# Grant access to your service account
gcloud secrets add-iam-policy-binding openai-api-key \
    --member="serviceAccount:YOUR_SA@YOUR_PROJECT.iam.gserviceaccount.com" \
    --role="roles/secretmanager.secretAccessor"
```

### 3. Using Secrets in Code

```python
# Preferred method
from backend.secrets import get_secret

openai_key = get_secret("OPENAI_API_KEY")
anthropic_key = get_secret("ANTHROPIC_API_KEY")

# Or using the manager directly
from backend.secrets import SecretManager

secrets = SecretManager()
all_snow_secrets = secrets.get_all(prefix="SNOW_")
```

## Pre-commit Hooks

We use pre-commit hooks to prevent accidental secret commits.

### Installation

```bash
# Install pre-commit
pip install pre-commit

# Install the hooks
pre-commit install

# Run manually on all files
pre-commit run --all-files
```

### What Gets Checked

1. **detect-secrets** - Scans for high-entropy strings, API keys, passwords
2. **gitleaks** - Scans for known secret patterns
3. **custom hook** - Blocks files matching sensitive patterns:
   - `*.pem`, `*.key`
   - `*-key.json`, `gcp-key.json`
   - `service-account*.json`
   - `credentials*.json`
   - `.env`, `secrets.yaml`

### Bypassing (Emergency Only)

```bash
# ONLY if you're 100% sure it's safe
git commit --no-verify -m "your message"

# Better: Add to .secrets.baseline for known false positives
detect-secrets scan --update .secrets.baseline
```

## Secret Definitions

All secrets are defined in `backend/secrets/manager.py`:

| Secret Name | GCP Secret ID | Required | Description |
|------------|---------------|----------|-------------|
| `OPENAI_API_KEY` | `openai-api-key` | Yes | OpenAI API key |
| `ANTHROPIC_API_KEY` | `anthropic-api-key` | Yes | Anthropic API key |
| `SNOW_INSTANCE_URL` | `servicenow-instance-url` | Yes | ServiceNow instance |
| `SNOW_USERNAME` | `servicenow-username` | Yes | ServiceNow user |
| `SNOW_PASSWORD` | `servicenow-password` | Yes | ServiceNow password |
| `JIRA_URL` | `jira-url` | Yes | Jira instance URL |
| `JIRA_USERNAME` | `jira-username` | Yes | Jira username |
| `JIRA_API_TOKEN` | `jira-api-token` | Yes | Jira API token |
| `GITHUB_TOKEN` | `github-token` | Yes | GitHub PAT |
| `POSTGRES_PASSWORD` | `postgres-password` | Yes | PostgreSQL password |
| `SLACK_BOT_TOKEN` | `slack-bot-token` | No | Slack bot token |

## Environment-Based Prefixes

In production, secrets are prefixed by environment:

```
dev-openai-api-key
staging-openai-api-key
prod-openai-api-key
```

The `SecretManager` automatically adds the prefix based on the `ENVIRONMENT` variable.

## GCP Secret Manager Setup

### Create Secrets

```bash
# Set project
export PROJECT_ID=your-gcp-project

# Create secrets for each environment
for ENV in dev staging prod; do
    echo -n "sk-your-key" | gcloud secrets create ${ENV}-openai-api-key \
        --project=$PROJECT_ID \
        --data-file=-
done
```

### Grant Access

```bash
# Service account for your workload
SA_EMAIL="your-sa@${PROJECT_ID}.iam.gserviceaccount.com"

# Grant access to all secrets
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SA_EMAIL" \
    --role="roles/secretmanager.secretAccessor"
```

### Rotate Secrets

```bash
# Add new version
echo -n "new-api-key" | gcloud secrets versions add openai-api-key --data-file=-

# Clear cache in application
from backend.secrets import SecretManager
SecretManager().clear_cache()
```

## Troubleshooting

### Secret Not Found

1. Check environment variable is set (local dev)
2. Check secret exists in GCP Secret Manager
3. Check service account has `secretAccessor` role
4. Check secret name matches (with environment prefix)

### Permission Denied

```bash
# Verify service account access
gcloud secrets get-iam-policy YOUR_SECRET_NAME

# Grant access
gcloud secrets add-iam-policy-binding YOUR_SECRET_NAME \
    --member="serviceAccount:YOUR_SA_EMAIL" \
    --role="roles/secretmanager.secretAccessor"
```

### Cache Issues

```python
# Clear cache to force refresh
from backend.secrets import SecretManager
SecretManager().clear_cache()
```

## Security Best Practices

1. **Rotate secrets regularly** - At least every 90 days
2. **Use least privilege** - Only grant access to needed secrets
3. **Audit access** - Review who has access periodically
4. **Monitor usage** - Set up alerts for unusual access patterns
5. **Never log secrets** - The SecretManager never logs values
6. **Use strong secrets** - Generate with `openssl rand -base64 32`

## Adding New Secrets

1. Add definition to `backend/secrets/manager.py`:

```python
"NEW_SECRET": SecretConfig(
    name="NEW_SECRET",
    gcp_secret_id="new-secret",
    required=True,
    default=None  # or a default value
),
```

2. Create in GCP Secret Manager:

```bash
echo -n "secret-value" | gcloud secrets create new-secret --data-file=-
```

3. Update `.env.example` for documentation:

```env
NEW_SECRET=your-new-secret-value
```

4. Use in code:

```python
from backend.secrets import get_secret
value = get_secret("NEW_SECRET")
```
