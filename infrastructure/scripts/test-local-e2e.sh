#!/bin/bash
# =============================================================================
# AI Agent Platform - Local End-to-End Test
# =============================================================================
# This script runs a complete E2E test locally using docker-compose
#
# USAGE: ./test-local-e2e.sh [--full] [--clean]
#   --full   Start full docker-compose stack (all services)
#   --clean  Clean up containers after test
# =============================================================================

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INFRA_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_ROOT="$(dirname "$INFRA_DIR")"

# Parse arguments
FULL_STACK=false
CLEANUP=false

while [[ "$#" -gt 0 ]]; do
    case $1 in
        --full) FULL_STACK=true; shift ;;
        --clean) CLEANUP=true; shift ;;
        *) echo "Unknown parameter: $1"; exit 1 ;;
    esac
done

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}AI Agent Platform - Local E2E Test${NC}"
echo -e "${GREEN}========================================${NC}"

# Step 1: Check prerequisites
echo -e "\n${YELLOW}Step 1: Checking prerequisites...${NC}"
command -v docker >/dev/null 2>&1 || { echo -e "${RED}docker is required but not installed.${NC}" >&2; exit 1; }
command -v docker-compose >/dev/null 2>&1 || command -v "docker compose" >/dev/null 2>&1 || { echo -e "${RED}docker-compose is required but not installed.${NC}" >&2; exit 1; }
command -v python3 >/dev/null 2>&1 || { echo -e "${RED}python3 is required but not installed.${NC}" >&2; exit 1; }
echo -e "${GREEN}All prerequisites met.${NC}"

# Step 2: Start infrastructure services
echo -e "\n${YELLOW}Step 2: Starting infrastructure services...${NC}"
cd "$INFRA_DIR"

# Check if .env exists, if not create one
if [ ! -f ".env" ]; then
    cat > .env << EOF
OPENAI_API_KEY=${OPENAI_API_KEY:-your-openai-key}
ANTHROPIC_API_KEY=${ANTHROPIC_API_KEY:-your-anthropic-key}
LANGFUSE_PUBLIC_KEY=
LANGFUSE_SECRET_KEY=
NEXTAUTH_SECRET=change-this-secret
SALT=change-this-salt
EOF
    echo -e "${YELLOW}Created .env file. Please update with your API keys.${NC}"
fi

if [ "$FULL_STACK" = true ]; then
    echo -e "${BLUE}Starting full stack...${NC}"
    ./start-all.sh
else
    # Start only infrastructure services (not the app services)
    echo -e "${BLUE}Starting Kafka, Redis, Postgres, Weaviate, Neo4j...${NC}"
    docker-compose up -d zookeeper kafka redis postgres weaviate neo4j

    # Wait for services to be healthy
    echo -e "${BLUE}Waiting for services to be ready...${NC}"
    sleep 30

    # Initialize Kafka topics
    docker-compose up kafka-init
fi

# Step 3: Check services
echo -e "\n${YELLOW}Step 3: Checking service health...${NC}"

check_service() {
    local name=$1
    local port=$2
    local max_attempts=30
    local attempt=0

    while [ $attempt -lt $max_attempts ]; do
        if nc -z localhost $port 2>/dev/null; then
            echo -e "${GREEN}✓ $name is running on port $port${NC}"
            return 0
        fi
        attempt=$((attempt + 1))
        sleep 2
    done
    echo -e "${RED}✗ $name failed to start on port $port${NC}"
    return 1
}

check_service "Kafka" 9092
check_service "Redis" 6379
check_service "PostgreSQL" 5432
check_service "Weaviate" 8081
check_service "Neo4j" 7474

# Step 4: Run Data Agent E2E Test
echo -e "\n${YELLOW}Step 4: Running Data Agent Pipeline Test...${NC}"
cd "$PROJECT_ROOT"

if [ -f "data_agent/pipelines/customer_transactions/tests/test_e2e_pipeline.py" ]; then
    echo -e "${BLUE}Running customer_transactions pipeline test...${NC}"
    python3 data_agent/pipelines/customer_transactions/tests/test_e2e_pipeline.py
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ Data Agent pipeline test PASSED${NC}"
    else
        echo -e "${RED}✗ Data Agent pipeline test FAILED${NC}"
    fi
else
    echo -e "${YELLOW}Pipeline test not found, skipping...${NC}"
fi

# Step 5: Test Backend API (if running)
echo -e "\n${YELLOW}Step 5: Testing Backend API...${NC}"

if curl -s -o /dev/null -w "%{http_code}" http://localhost:8000/health 2>/dev/null | grep -q "200"; then
    echo -e "${GREEN}✓ Backend API is running${NC}"

    # Test health endpoint
    echo -e "${BLUE}Testing /health endpoint...${NC}"
    curl -s http://localhost:8000/health | python3 -m json.tool 2>/dev/null || echo "Response received"

    # Test incidents endpoint
    echo -e "${BLUE}Testing /api/incidents endpoint...${NC}"
    INCIDENTS=$(curl -s http://localhost:8000/api/incidents 2>/dev/null)
    if [ ! -z "$INCIDENTS" ]; then
        echo -e "${GREEN}✓ Incidents endpoint working${NC}"
    fi
else
    echo -e "${YELLOW}Backend API not running. Start with: ./start-all.sh${NC}"
fi

# Step 6: Test Frontend (if running)
echo -e "\n${YELLOW}Step 6: Testing Frontend...${NC}"

if curl -s -o /dev/null -w "%{http_code}" http://localhost:3000 2>/dev/null | grep -q "200\|304"; then
    echo -e "${GREEN}✓ Frontend is running on port 3000${NC}"
else
    echo -e "${YELLOW}Frontend not running. Start with: ./start-all.sh${NC}"
fi

# Clean up if requested
if [ "$CLEANUP" = true ]; then
    echo -e "\n${YELLOW}Cleaning up containers...${NC}"
    cd "$INFRA_DIR"
    docker-compose down
    echo -e "${GREEN}Cleanup complete.${NC}"
fi

# Summary
echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}E2E Test Summary${NC}"
echo -e "${GREEN}========================================${NC}"

cd "$INFRA_DIR"
echo -e "\n${YELLOW}Running containers:${NC}"
docker-compose ps

echo -e "\n${YELLOW}To start full platform:${NC}"
echo -e "  cd infrastructure && ./start-all.sh"

echo -e "\n${YELLOW}To stop all services:${NC}"
echo -e "  cd infrastructure && ./stop-all.sh"

echo -e "\n${GREEN}Local E2E test completed!${NC}"
