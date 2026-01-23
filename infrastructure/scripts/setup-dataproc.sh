#!/bin/bash
# =============================================================================
# Dataproc Cluster Setup Script
# =============================================================================
# Creates a minimal Dataproc cluster for Spark job execution.
# Cluster auto-deletes after 30 minutes idle to optimize costs.
#
# USAGE:
#   export GCP_PROJECT_ID=your-project-id
#   ./setup-dataproc.sh [create|delete|status]
#
# COST OPTIMIZATION:
# - Uses n1-standard-2 machines (minimal viable)
# - Auto-deletes after 30 min idle
# - Can use preemptible VMs for 60-80% savings
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
GCP_REGION=${GCP_REGION:-us-central1}
GCP_ZONE=${GCP_ZONE:-us-central1-a}
CLUSTER_NAME=${DATAPROC_CLUSTER:-ai-agent-spark}

# Cluster sizing (minimal for cost optimization)
MASTER_MACHINE_TYPE=${MASTER_MACHINE_TYPE:-n1-standard-2}
WORKER_MACHINE_TYPE=${WORKER_MACHINE_TYPE:-n1-standard-2}
NUM_WORKERS=${NUM_WORKERS:-2}
DISK_SIZE=${DISK_SIZE:-100}

# Auto-delete settings
IDLE_DELETE_TTL=${IDLE_DELETE_TTL:-1800s}  # 30 minutes

# Parse command
COMMAND=${1:-status}

echo -e "${BLUE}╔═══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║     Dataproc Cluster Management - AI Agent Platform           ║${NC}"
echo -e "${BLUE}╚═══════════════════════════════════════════════════════════════╝${NC}"

echo -e "\n${YELLOW}Project: ${GCP_PROJECT_ID}${NC}"
echo -e "${YELLOW}Region: ${GCP_REGION}${NC}"
echo -e "${YELLOW}Cluster: ${CLUSTER_NAME}${NC}"

case $COMMAND in
    create)
        echo -e "\n${BLUE}Creating Dataproc cluster...${NC}"

        # Enable Dataproc API
        gcloud services enable dataproc.googleapis.com --project=$GCP_PROJECT_ID

        # Check if cluster already exists
        if gcloud dataproc clusters describe $CLUSTER_NAME \
            --project=$GCP_PROJECT_ID \
            --region=$GCP_REGION 2>/dev/null; then
            echo -e "${YELLOW}Cluster $CLUSTER_NAME already exists${NC}"
            exit 0
        fi

        # Create cluster
        gcloud dataproc clusters create $CLUSTER_NAME \
            --project=$GCP_PROJECT_ID \
            --region=$GCP_REGION \
            --zone=$GCP_ZONE \
            --master-machine-type=$MASTER_MACHINE_TYPE \
            --master-boot-disk-size=${DISK_SIZE}GB \
            --num-workers=$NUM_WORKERS \
            --worker-machine-type=$WORKER_MACHINE_TYPE \
            --worker-boot-disk-size=${DISK_SIZE}GB \
            --image-version=2.1-debian11 \
            --max-idle=$IDLE_DELETE_TTL \
            --enable-component-gateway \
            --properties=spark:spark.executor.memory=2g,spark:spark.driver.memory=2g,spark:spark.sql.adaptive.enabled=true \
            --labels=environment=dev,managed-by=ai-agent-platform

        echo -e "${GREEN}✓ Cluster $CLUSTER_NAME created${NC}"

        # Display access info
        echo -e "\n${YELLOW}Cluster Access:${NC}"
        echo -e "  Spark History: https://${CLUSTER_NAME}-m-dot-${GCP_PROJECT_ID}.${GCP_REGION}.dataproc.googleusercontent.com/sparkhistory"
        echo -e "  YARN ResourceManager: https://${CLUSTER_NAME}-m-dot-${GCP_PROJECT_ID}.${GCP_REGION}.dataproc.googleusercontent.com/yarn"

        echo -e "\n${YELLOW}Cost Information:${NC}"
        echo -e "  - Cluster will auto-delete after ${IDLE_DELETE_TTL} of idle time"
        echo -e "  - Estimated hourly cost: ~\$0.50/hour (3 x n1-standard-2)"
        ;;

    delete)
        echo -e "\n${BLUE}Deleting Dataproc cluster...${NC}"

        if ! gcloud dataproc clusters describe $CLUSTER_NAME \
            --project=$GCP_PROJECT_ID \
            --region=$GCP_REGION 2>/dev/null; then
            echo -e "${YELLOW}Cluster $CLUSTER_NAME does not exist${NC}"
            exit 0
        fi

        gcloud dataproc clusters delete $CLUSTER_NAME \
            --project=$GCP_PROJECT_ID \
            --region=$GCP_REGION \
            --quiet

        echo -e "${GREEN}✓ Cluster $CLUSTER_NAME deleted${NC}"
        ;;

    status)
        echo -e "\n${BLUE}Checking cluster status...${NC}"

        if gcloud dataproc clusters describe $CLUSTER_NAME \
            --project=$GCP_PROJECT_ID \
            --region=$GCP_REGION 2>/dev/null; then

            STATUS=$(gcloud dataproc clusters describe $CLUSTER_NAME \
                --project=$GCP_PROJECT_ID \
                --region=$GCP_REGION \
                --format="value(status.state)")

            echo -e "\n${GREEN}Cluster Status: $STATUS${NC}"

            # Show cluster details
            gcloud dataproc clusters describe $CLUSTER_NAME \
                --project=$GCP_PROJECT_ID \
                --region=$GCP_REGION \
                --format="table(clusterName,status.state,config.masterConfig.numInstances,config.workerConfig.numInstances)"
        else
            echo -e "${YELLOW}Cluster $CLUSTER_NAME does not exist${NC}"
        fi
        ;;

    submit-test)
        echo -e "\n${BLUE}Submitting test Spark job...${NC}"

        # Create simple test job
        cat > /tmp/test_spark.py << 'EOF'
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TestJob").getOrCreate()
print("=" * 50)
print("Spark test job running successfully!")
print(f"Spark version: {spark.version}")
print("=" * 50)
spark.stop()
EOF

        # Upload to GCS
        gsutil cp /tmp/test_spark.py gs://${GCP_PROJECT_ID}-temp/test/test_spark.py

        # Submit job
        gcloud dataproc jobs submit pyspark \
            gs://${GCP_PROJECT_ID}-temp/test/test_spark.py \
            --project=$GCP_PROJECT_ID \
            --region=$GCP_REGION \
            --cluster=$CLUSTER_NAME

        echo -e "${GREEN}✓ Test job completed${NC}"
        ;;

    *)
        echo -e "${RED}Unknown command: $COMMAND${NC}"
        echo -e "\nUsage: $0 [create|delete|status|submit-test]"
        exit 1
        ;;
esac

echo -e "\n${GREEN}Done!${NC}"
