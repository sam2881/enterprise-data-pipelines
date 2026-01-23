# GitHub CI/CD Setup Guide for Local Airflow

> **Note**: This guide is for deploying DAGs to **local Airflow in Docker**, not GCP Cloud Composer.

## Architecture

```
Data Agent → Creates PR → GitHub (enterprise-data-pipelines) → CI/CD Validates → Local Airflow Docker
```

---

## Step 1: Configure GitHub Repository

Go to your GitHub repository settings and configure the CI/CD workflow.

### For `sam2881/enterprise-data-pipelines`:

1. The workflow file is at: `.github/workflows/deploy-to-gcp.yml`
2. On push to `main`, it validates DAGs and lints code
3. No GCP secrets needed for local Airflow

---

## Step 2: Local Airflow Setup

Your local Airflow runs in Docker via `docker-compose.yml`:

```yaml
airflow-webserver:
  image: apache/airflow:2.9.3-python3.11
  ports:
    - "8080:8080"
  volumes:
    - ./dags:/opt/airflow/dags
```

### Access Airflow:
- **URL**: http://localhost:8080
- **Username**: admin
- **Password**: admin

---

## Step 3: Syncing DAGs to Local Airflow

### Option A: Git Pull on Local Machine

After CI/CD validates DAGs:
```bash
# On the machine running Airflow
cd /path/to/enterprise-data-pipelines
git pull origin main
```

DAGs will automatically sync since the folder is mounted.

### Option B: Webhook Trigger (Advanced)

Configure a webhook to auto-pull on merge:
```bash
# In your local environment
curl -X POST http://localhost:8080/api/v1/dags/sync
```

---

## Step 4: Environment Variables

Update your `.env` file:

```bash
# GitHub Pipelines Repository (for Data Agent)
GITHUB_PIPELINES_TOKEN=ghp_xxx
GITHUB_PIPELINES_OWNER=sam2881
GITHUB_PIPELINES_REPO=enterprise-data-pipelines

# Local Airflow
AIRFLOW_HOST=http://localhost:8080
AIRFLOW_USERNAME=admin
AIRFLOW_PASSWORD=admin
```

---

## Step 5: Test the CI/CD Pipeline

1. Make a change to a file in `dags/` folder
2. Commit and push to a branch
3. Create a Pull Request
4. Check workflow at: https://github.com/sam2881/enterprise-data-pipelines/actions
5. On merge to main, DAGs are validated

---

## Troubleshooting

### Workflow not triggering?
- Check that the workflow file exists at `.github/workflows/deploy-to-gcp.yml`
- Verify the paths filter matches your changes (`dags/**`, `spark/**`)

### DAGs not showing in Airflow?
- Check the volume mount in docker-compose.yml
- Run `docker compose logs airflow-webserver` for errors
- Verify DAG syntax: `python -c "from airflow.models import DagBag; DagBag('dags/')"`

### DAG Import Errors?
- Check dependencies are installed in Airflow container
- Add missing packages to `requirements.txt` and rebuild

---

## Deployment Flow

```
1. Data Agent generates DAG/Spark code
2. Agent creates PR to enterprise-data-pipelines repo
3. GitHub Actions validates DAGs on PR
4. Human reviews and merges PR
5. CI/CD validates on main branch
6. Local Airflow picks up new DAGs from mounted volume
```
