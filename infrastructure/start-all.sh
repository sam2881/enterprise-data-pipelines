#!/bin/bash
# =============================================================================
# AI Agent Platform - Start All Services
# =============================================================================
# HYBRID DEPLOYMENT: GCP Services + Local Docker
#
# GCP SERVICES (managed):
# - Google Cloud Storage (GCS) - bronze/silver/gold buckets
# - BigQuery - Analytics and data warehouse
# - Dataproc - Managed Spark (on-demand, created by Airflow)
#
# DOCKER SERVICES (local):
# - Kafka, Redis, PostgreSQL - Core infrastructure
# - Weaviate, Neo4j - Vector/Graph databases
# - Airflow - DAG orchestration (submits jobs to GCP)
# - Trino - Query engine (queries GCS + BigQuery)
# - Backend, Data Agent, Frontend - Application services
# - Prometheus, Grafana, LangFuse - Observability
#
# USAGE:
#   ./start-all.sh              # Start all services
#   ./start-all.sh --build      # Rebuild images before starting
#   ./start-all.sh --infra-only # Start only infrastructure
# =============================================================================

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Parse arguments
BUILD_FLAG=""
INFRA_ONLY=false

while [[ "$#" -gt 0 ]]; do
    case $1 in
        --build) BUILD_FLAG="--build"; shift ;;
        --infra-only) INFRA_ONLY=true; shift ;;
        *) echo "Unknown parameter: $1"; exit 1 ;;
    esac
done

echo -e "${PURPLE}╔═══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${PURPLE}║     Starting AI Agent Platform (Hybrid: GCP + Docker)         ║${NC}"
echo -e "${PURPLE}╚═══════════════════════════════════════════════════════════════╝${NC}"

# Check if .env exists
if [ ! -f ".env" ]; then
    echo -e "${YELLOW}Creating .env file from template...${NC}"
    cat > .env << 'EOF'
# =============================================================================
# AI Agent Platform - Environment Variables
# =============================================================================
# HYBRID DEPLOYMENT: GCP Services + Local Docker

# LLM API Keys (required for AI features)
OPENAI_API_KEY=your-openai-key
ANTHROPIC_API_KEY=your-anthropic-key

# =============================================================================
# GCP CONFIGURATION (REQUIRED for data pipelines)
# =============================================================================
# Run ./scripts/setup-gcp.sh to create these resources

GCP_PROJECT_ID=your-gcp-project-id
GCP_REGION=us-central1
GCP_ZONE=us-central1-a

# Service Account Key (path relative to infrastructure/)
GOOGLE_APPLICATION_CREDENTIALS=./gcp-key.json

# GCS Buckets (Medallion Architecture)
GCS_BUCKET_BRONZE=your-project-bronze
GCS_BUCKET_SILVER=your-project-silver
GCS_BUCKET_GOLD=your-project-gold
GCS_BUCKET_TEMP=your-project-temp
GCS_BUCKET_RAW=your-project-raw-data

# BigQuery
BIGQUERY_DATASET=data_warehouse
BIGQUERY_LOCATION=US

# Dataproc (Spark on GCP - created on-demand by Airflow)
DATAPROC_CLUSTER=ai-agent-spark
DATAPROC_REGION=us-central1

# =============================================================================
# DOCKER SERVICES CONFIGURATION
# =============================================================================

# Airflow
AIRFLOW_FERNET_KEY=81HqDtbqAywKSOumSha3BhWNOdQ26slT6K0YaZeZyPs=
AIRFLOW_SECRET_KEY=change-this-airflow-secret-key

# LangFuse (LLM observability)
LANGFUSE_PUBLIC_KEY=
LANGFUSE_SECRET_KEY=
NEXTAUTH_SECRET=change-this-secret-in-production
SALT=change-this-salt-in-production

# =============================================================================
# OPTIONAL INTEGRATIONS
# =============================================================================

# ServiceNow (for IT Service Agent)
SNOW_INSTANCE_URL=
SNOW_USERNAME=
SNOW_PASSWORD=

# GitHub (for remediation execution)
GITHUB_TOKEN=
GITHUB_OWNER=
GITHUB_REPO=
GITHUB_DATA_REPO=data-pipelines

# Slack (for approval notifications)
SLACK_WEBHOOK_URL=
SLACK_CHANNEL=
EOF
    echo ""
    echo -e "${RED}⚠️  IMPORTANT: Please configure .env with your GCP settings${NC}"
    echo -e "${YELLOW}   1. Run: ./scripts/setup-gcp.sh${NC}"
    echo -e "${YELLOW}   2. Copy GCP settings from .env.gcp to .env${NC}"
    echo ""
    exit 1
fi

# Load environment
export $(grep -v '^#' .env | grep -v '^$' | xargs) 2>/dev/null || true

# Validate GCP configuration
if [ -z "$GCP_PROJECT_ID" ] || [ "$GCP_PROJECT_ID" = "your-gcp-project-id" ]; then
    echo -e "${RED}Error: GCP_PROJECT_ID not configured in .env${NC}"
    echo -e "${YELLOW}Run ./scripts/setup-gcp.sh to set up GCP resources${NC}"
    exit 1
fi

if [ ! -f "$GOOGLE_APPLICATION_CREDENTIALS" ] && [ ! -f "./gcp-key.json" ]; then
    echo -e "${RED}Error: GCP service account key not found${NC}"
    echo -e "${YELLOW}Run ./scripts/setup-gcp.sh to create service account${NC}"
    exit 1
fi

echo -e "${GREEN}✓ GCP Configuration validated${NC}"
echo -e "  Project: ${GCP_PROJECT_ID}"
echo -e "  Region: ${GCP_REGION}"

# =============================================================================
# Step 1: Start Core Infrastructure (Docker)
# =============================================================================

echo -e "\n${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}[1/5] Starting Core Infrastructure (Docker)${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

docker-compose up -d zookeeper kafka redis postgres weaviate neo4j $BUILD_FLAG

echo -e "${GREEN}✓ Core infrastructure starting (Kafka, Redis, PostgreSQL, Weaviate, Neo4j)${NC}"
echo -e "${YELLOW}Waiting for services to be ready (30s)...${NC}"
sleep 30

# =============================================================================
# Step 2: Initialize Kafka Topics
# =============================================================================

echo -e "\n${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}[2/5] Initializing Kafka Topics${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

docker-compose up kafka-init $BUILD_FLAG
echo -e "${GREEN}✓ Kafka topics initialized${NC}"

# =============================================================================
# Step 3: Start Airflow (Docker) + Trino
# =============================================================================

echo -e "\n${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}[3/5] Starting Airflow & Trino (Docker)${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

# Initialize Airflow
docker-compose up airflow-init $BUILD_FLAG
echo -e "${GREEN}✓ Airflow database initialized${NC}"

# Start Airflow services
docker-compose up -d airflow-scheduler airflow-webserver $BUILD_FLAG
echo -e "${GREEN}✓ Airflow starting (scheduler + webserver)${NC}"

# Start Trino (queries GCS + BigQuery)
docker-compose up -d trino $BUILD_FLAG
echo -e "${GREEN}✓ Trino starting (SQL engine for GCS + BigQuery)${NC}"

if [ "$INFRA_ONLY" = true ]; then
    echo -e "\n${GREEN}Infrastructure started (--infra-only mode)${NC}"
    echo -e "${YELLOW}Docker: Kafka, Redis, PostgreSQL, Weaviate, Neo4j, Airflow, Trino${NC}"
    echo -e "${YELLOW}GCP: GCS buckets, BigQuery (already available)${NC}"
    exit 0
fi

# =============================================================================
# Step 4: Start Observability Stack
# =============================================================================

echo -e "\n${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}[4/5] Starting Observability Stack${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

docker-compose up -d prometheus grafana tempo langfuse $BUILD_FLAG
echo -e "${GREEN}✓ Observability services starting (Prometheus, Grafana, Tempo, LangFuse)${NC}"

# =============================================================================
# Step 5: Start Application Services
# =============================================================================

echo -e "\n${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}[5/5] Starting Application Services${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

# Backend + Data Agent
docker-compose up -d orchestrator data-agent $BUILD_FLAG
echo -e "${GREEN}✓ Backend Orchestrator starting on port 8000${NC}"
echo -e "${GREEN}✓ Data Agent starting on port 8001${NC}"

# Kafka Consumers
docker-compose up -d jira-consumer incident-consumer pipeline-consumer $BUILD_FLAG
echo -e "${GREEN}✓ Kafka consumers starting (Jira, Incident, Pipeline)${NC}"

# Frontend
docker-compose up -d frontend $BUILD_FLAG
echo -e "${GREEN}✓ Frontend starting on port 3000${NC}"

# =============================================================================
# Health Checks
# =============================================================================

echo -e "\n${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}Checking Service Health...${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

sleep 15

check_service() {
    local name=$1
    local port=$2
    if nc -z localhost $port 2>/dev/null; then
        echo -e "  ${GREEN}✓${NC} $name (port $port)"
        return 0
    else
        echo -e "  ${YELLOW}⏳${NC} $name (port $port - starting...)"
        return 1
    fi
}

echo -e "\n${YELLOW}Docker Services:${NC}"
check_service "Kafka" 9092 || true
check_service "Redis" 6379 || true
check_service "PostgreSQL" 5432 || true
check_service "Weaviate" 8081 || true
check_service "Neo4j" 7474 || true
check_service "Airflow" 8083 || true
check_service "Trino" 8084 || true

echo -e "\n${YELLOW}Application Services:${NC}"
check_service "Orchestrator API" 8000 || true
check_service "Data Agent API" 8001 || true
check_service "Frontend" 3000 || true

echo -e "\n${YELLOW}Observability:${NC}"
check_service "Grafana" 3001 || true
check_service "LangFuse" 3002 || true
check_service "Prometheus" 9090 || true
check_service "Kafka UI" 8085 || true

# =============================================================================
# Summary
# =============================================================================

echo -e "\n${PURPLE}╔═══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${PURPLE}║     AI Agent Platform Started! (Hybrid: GCP + Docker)         ║${NC}"
echo -e "${PURPLE}╚═══════════════════════════════════════════════════════════════╝${NC}"

echo -e "
${GREEN}Application Services (Docker):${NC}
  • Frontend:         http://localhost:3000
  • Backend API:      http://localhost:8000
  • Backend Docs:     http://localhost:8000/docs
  • Data Agent API:   http://localhost:8001
  • Data Agent Docs:  http://localhost:8001/docs

${GREEN}Data Infrastructure:${NC}
  ${YELLOW}GCP Services:${NC}
  • GCS Bronze:       gs://${GCS_BUCKET_BRONZE}
  • GCS Silver:       gs://${GCS_BUCKET_SILVER}
  • GCS Gold:         gs://${GCS_BUCKET_GOLD}
  • BigQuery:         ${GCP_PROJECT_ID}:${BIGQUERY_DATASET}
  • Dataproc:         ${DATAPROC_CLUSTER} (on-demand)

  ${YELLOW}Docker Services:${NC}
  • Airflow:          http://localhost:8083 (admin/admin123)
  • Trino:            http://localhost:8084

${GREEN}Observability:${NC}
  • Grafana:          http://localhost:3001 (admin/admin)
  • LangFuse:         http://localhost:3002
  • Prometheus:       http://localhost:9090
  • Kafka UI:         http://localhost:8085

${GREEN}Databases (Docker):${NC}
  • PostgreSQL:       localhost:5432 (admin/admin123)
  • Redis:            localhost:6379
  • Weaviate:         localhost:8081
  • Neo4j:            localhost:7474 (neo4j/adminadmin)

${YELLOW}GCS Buckets (Medallion Architecture):${NC}
  • gs://${GCS_BUCKET_BRONZE}   Raw data ingestion (ALL STRING)
  • gs://${GCS_BUCKET_SILVER}   Cleaned & validated data (typed)
  • gs://${GCS_BUCKET_GOLD}     Aggregated analytics
  • gs://${GCS_BUCKET_TEMP}     Temporary files (7-day lifecycle)

${YELLOW}Useful Commands:${NC}
  • View logs:        docker-compose logs -f orchestrator
  • Stop all:         ./stop-all.sh
  • Kafka topics:     docker exec ai-agent-kafka kafka-topics --list --bootstrap-server localhost:9092
  • Trino CLI:        docker exec -it ai-agent-trino trino
  • Query BigQuery:   docker exec -it ai-agent-trino trino --catalog bigquery --schema ${BIGQUERY_DATASET}
  • List GCS:         gsutil ls gs://${GCS_BUCKET_BRONZE}/
"
