#!/bin/bash
# =============================================================================
# GCP Infrastructure Setup Script
# =============================================================================
# Creates minimal GCP resources for AI Agent Platform (cost-optimized)
#
# RESOURCES CREATED:
# - GCS Buckets: bronze, silver, gold, temp (for Medallion Architecture)
# - BigQuery Dataset: data_warehouse
# - Service Account: ai-agent-sa (with minimal permissions)
# - Dataproc cluster config (on-demand, not created until needed)
#
# COST OPTIMIZATION:
# - Uses Standard storage class (cheapest)
# - BigQuery uses on-demand pricing (pay per query)
# - Dataproc cluster is created on-demand by Airflow (not always running)
# - All resources in single region to avoid egress costs
#
# USAGE:
#   export GCP_PROJECT_ID=your-project-id
#   ./setup-gcp.sh
#
# =============================================================================

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Configuration
GCP_PROJECT_ID=${GCP_PROJECT_ID:-}
GCP_REGION=${GCP_REGION:-us-central1}
GCP_ZONE=${GCP_ZONE:-us-central1-a}

if [ -z "$GCP_PROJECT_ID" ]; then
    echo -e "${RED}Error: GCP_PROJECT_ID environment variable is required${NC}"
    echo "Usage: export GCP_PROJECT_ID=your-project-id && ./setup-gcp.sh"
    exit 1
fi

echo -e "${BLUE}╔═══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║     GCP Infrastructure Setup for AI Agent Platform            ║${NC}"
echo -e "${BLUE}╚═══════════════════════════════════════════════════════════════╝${NC}"

echo -e "\n${YELLOW}Project: ${GCP_PROJECT_ID}${NC}"
echo -e "${YELLOW}Region: ${GCP_REGION}${NC}"

# =============================================================================
# Step 1: Enable Required APIs
# =============================================================================

echo -e "\n${BLUE}[1/6] Enabling GCP APIs...${NC}"

gcloud services enable storage.googleapis.com --project=$GCP_PROJECT_ID
gcloud services enable bigquery.googleapis.com --project=$GCP_PROJECT_ID
gcloud services enable dataproc.googleapis.com --project=$GCP_PROJECT_ID
gcloud services enable iam.googleapis.com --project=$GCP_PROJECT_ID

echo -e "${GREEN}✓ APIs enabled${NC}"

# =============================================================================
# Step 2: Create GCS Buckets (Medallion Architecture)
# =============================================================================

echo -e "\n${BLUE}[2/6] Creating GCS buckets for Medallion Architecture...${NC}"

# Bucket naming: {project}-{layer}
BUCKET_BRONZE="${GCP_PROJECT_ID}-bronze"
BUCKET_SILVER="${GCP_PROJECT_ID}-silver"
BUCKET_GOLD="${GCP_PROJECT_ID}-gold"
BUCKET_TEMP="${GCP_PROJECT_ID}-temp"
BUCKET_RAW="${GCP_PROJECT_ID}-raw-data"

create_bucket() {
    local bucket=$1
    local lifecycle_days=$2

    if gsutil ls -b gs://$bucket 2>/dev/null; then
        echo -e "  ${YELLOW}Bucket gs://$bucket already exists${NC}"
    else
        gsutil mb -p $GCP_PROJECT_ID -l $GCP_REGION -c STANDARD gs://$bucket
        echo -e "  ${GREEN}✓ Created gs://$bucket${NC}"

        # Set lifecycle policy for temp bucket (auto-delete after N days)
        if [ ! -z "$lifecycle_days" ]; then
            cat > /tmp/lifecycle.json << EOF
{
  "rule": [
    {
      "action": {"type": "Delete"},
      "condition": {"age": $lifecycle_days}
    }
  ]
}
EOF
            gsutil lifecycle set /tmp/lifecycle.json gs://$bucket
            echo -e "  ${GREEN}✓ Set ${lifecycle_days}-day lifecycle on gs://$bucket${NC}"
        fi
    fi
}

create_bucket $BUCKET_BRONZE
create_bucket $BUCKET_SILVER
create_bucket $BUCKET_GOLD
create_bucket $BUCKET_TEMP 7  # Auto-delete temp files after 7 days
create_bucket $BUCKET_RAW

# =============================================================================
# Step 3: Create BigQuery Dataset
# =============================================================================

echo -e "\n${BLUE}[3/6] Creating BigQuery dataset...${NC}"

DATASET_NAME="data_warehouse"

if bq show --dataset ${GCP_PROJECT_ID}:${DATASET_NAME} 2>/dev/null; then
    echo -e "  ${YELLOW}Dataset ${DATASET_NAME} already exists${NC}"
else
    bq mk --dataset \
        --location=US \
        --description="AI Agent Platform Data Warehouse" \
        ${GCP_PROJECT_ID}:${DATASET_NAME}
    echo -e "${GREEN}✓ Created BigQuery dataset: ${DATASET_NAME}${NC}"
fi

# Create metadata tables in BigQuery (mirrors PostgreSQL metadata)
echo -e "  Creating BigQuery metadata tables..."

bq query --use_legacy_sql=false --project_id=$GCP_PROJECT_ID << 'EOF' || true
CREATE TABLE IF NOT EXISTS `data_warehouse.pipeline_runs` (
  run_id STRING NOT NULL,
  feed_name STRING NOT NULL,
  posting_date DATE,
  layer STRING,  -- bronze, silver, gold
  status STRING,
  records_processed INT64,
  started_at TIMESTAMP,
  completed_at TIMESTAMP,
  error_message STRING,
  metadata JSON
);

CREATE TABLE IF NOT EXISTS `data_warehouse.data_quality_results` (
  run_id STRING NOT NULL,
  feed_name STRING NOT NULL,
  check_name STRING,
  check_type STRING,
  passed BOOL,
  actual_value STRING,
  expected_value STRING,
  checked_at TIMESTAMP
);
EOF

echo -e "${GREEN}✓ BigQuery metadata tables created${NC}"

# =============================================================================
# Step 4: Create Service Account
# =============================================================================

echo -e "\n${BLUE}[4/6] Creating service account...${NC}"

SA_NAME="ai-agent-sa"
SA_EMAIL="${SA_NAME}@${GCP_PROJECT_ID}.iam.gserviceaccount.com"

if gcloud iam service-accounts describe $SA_EMAIL --project=$GCP_PROJECT_ID 2>/dev/null; then
    echo -e "  ${YELLOW}Service account $SA_EMAIL already exists${NC}"
else
    gcloud iam service-accounts create $SA_NAME \
        --project=$GCP_PROJECT_ID \
        --display-name="AI Agent Platform Service Account" \
        --description="Service account for AI Agent Platform data pipelines"
    echo -e "${GREEN}✓ Created service account: $SA_EMAIL${NC}"
fi

# Grant minimal required permissions
echo -e "  Granting IAM permissions..."

# Storage permissions (for GCS buckets)
gcloud projects add-iam-policy-binding $GCP_PROJECT_ID \
    --member="serviceAccount:$SA_EMAIL" \
    --role="roles/storage.objectAdmin" \
    --condition=None --quiet

# BigQuery permissions
gcloud projects add-iam-policy-binding $GCP_PROJECT_ID \
    --member="serviceAccount:$SA_EMAIL" \
    --role="roles/bigquery.dataEditor" \
    --condition=None --quiet

gcloud projects add-iam-policy-binding $GCP_PROJECT_ID \
    --member="serviceAccount:$SA_EMAIL" \
    --role="roles/bigquery.jobUser" \
    --condition=None --quiet

# Dataproc permissions (for Spark jobs)
gcloud projects add-iam-policy-binding $GCP_PROJECT_ID \
    --member="serviceAccount:$SA_EMAIL" \
    --role="roles/dataproc.editor" \
    --condition=None --quiet

echo -e "${GREEN}✓ IAM permissions granted${NC}"

# =============================================================================
# Step 5: Create Service Account Key
# =============================================================================

echo -e "\n${BLUE}[5/6] Creating service account key...${NC}"

KEY_FILE="./gcp-key.json"

if [ -f "$KEY_FILE" ]; then
    echo -e "  ${YELLOW}Key file $KEY_FILE already exists${NC}"
    read -p "  Overwrite? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo -e "  ${YELLOW}Skipping key creation${NC}"
    else
        gcloud iam service-accounts keys create $KEY_FILE \
            --iam-account=$SA_EMAIL \
            --project=$GCP_PROJECT_ID
        echo -e "${GREEN}✓ Created service account key: $KEY_FILE${NC}"
    fi
else
    gcloud iam service-accounts keys create $KEY_FILE \
        --iam-account=$SA_EMAIL \
        --project=$GCP_PROJECT_ID
    echo -e "${GREEN}✓ Created service account key: $KEY_FILE${NC}"
fi

# =============================================================================
# Step 6: Generate .env file
# =============================================================================

echo -e "\n${BLUE}[6/6] Generating .env configuration...${NC}"

ENV_FILE=".env.gcp"

cat > $ENV_FILE << EOF
# =============================================================================
# GCP Configuration for AI Agent Platform
# Generated by setup-gcp.sh on $(date)
# =============================================================================

# GCP Project
GCP_PROJECT_ID=${GCP_PROJECT_ID}
GCP_REGION=${GCP_REGION}
GCP_ZONE=${GCP_ZONE}

# Service Account Key (relative path from infrastructure/)
GOOGLE_APPLICATION_CREDENTIALS=./gcp-key.json

# GCS Buckets (Medallion Architecture)
GCS_BUCKET_BRONZE=${BUCKET_BRONZE}
GCS_BUCKET_SILVER=${BUCKET_SILVER}
GCS_BUCKET_GOLD=${BUCKET_GOLD}
GCS_BUCKET_TEMP=${BUCKET_TEMP}
GCS_BUCKET_RAW=${BUCKET_RAW}

# BigQuery
BIGQUERY_DATASET=${DATASET_NAME}
BIGQUERY_LOCATION=US

# Dataproc (Spark on GCP)
# Cluster is created on-demand by Airflow - not always running
DATAPROC_CLUSTER=ai-agent-spark
DATAPROC_REGION=${GCP_REGION}

# Dataproc cluster configuration (for on-demand creation)
# Uses preemptible VMs for cost savings
DATAPROC_MASTER_MACHINE_TYPE=n1-standard-4
DATAPROC_WORKER_MACHINE_TYPE=n1-standard-4
DATAPROC_NUM_WORKERS=2
DATAPROC_PREEMPTIBLE_WORKERS=2
EOF

echo -e "${GREEN}✓ Generated $ENV_FILE${NC}"

# =============================================================================
# Summary
# =============================================================================

echo -e "\n${GREEN}╔═══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║     GCP Infrastructure Setup Complete!                        ║${NC}"
echo -e "${GREEN}╚═══════════════════════════════════════════════════════════════╝${NC}"

echo -e "
${YELLOW}Resources Created:${NC}
  • GCS Buckets:
    - gs://${BUCKET_BRONZE} (raw data)
    - gs://${BUCKET_SILVER} (cleaned data)
    - gs://${BUCKET_GOLD} (aggregated data)
    - gs://${BUCKET_TEMP} (temporary files, 7-day lifecycle)
    - gs://${BUCKET_RAW} (source data landing)

  • BigQuery Dataset: ${DATASET_NAME}

  • Service Account: ${SA_EMAIL}

  • Service Account Key: ${KEY_FILE}

${YELLOW}Next Steps:${NC}
  1. Copy GCP settings to your .env file:
     cat ${ENV_FILE} >> .env

  2. Start the platform:
     ./start-all.sh

  3. Upload test data to GCS:
     gsutil cp data.csv gs://${BUCKET_RAW}/

${YELLOW}Cost Optimization Notes:${NC}
  • Dataproc cluster is NOT created automatically
  • Airflow will create on-demand clusters for Spark jobs
  • Clusters auto-delete after job completion
  • Use preemptible VMs for 60-80% cost savings
  • BigQuery uses on-demand pricing (pay per query)

${YELLOW}Estimated Monthly Cost (minimal usage):${NC}
  • GCS: ~\$5-10 (depends on data volume)
  • BigQuery: ~\$5-20 (depends on query volume)
  • Dataproc: ~\$0 when idle, ~\$2-5/hour when running
"
