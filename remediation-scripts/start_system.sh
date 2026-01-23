#!/bin/bash
# Complete System Startup Script - AI Agent Platform

set -e  # Exit on error

echo "================================================================================"
echo "  AI Agent Platform - System Startup"
echo "================================================================================"
echo ""

# Color codes
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

PROJECT_ROOT="/home/samrattidke600/ai_agent_app"
cd "$PROJECT_ROOT"

# Create logs directory
mkdir -p "$PROJECT_ROOT/logs"

# Load environment variables
if [ -f "$PROJECT_ROOT/.env" ]; then
    export $(grep -v '^#' "$PROJECT_ROOT/.env" | xargs)
fi

# Export ServiceNow credentials
export SNOW_INSTANCE_URL="${SNOW_INSTANCE_URL:-https://dev275804.service-now.com}"
export SNOW_USERNAME="${SNOW_USERNAME:-admin}"
export SNOW_PASSWORD="${SNOW_PASSWORD:-YdZ3Y/n1Zlh*}"

# Export Kafka settings
export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:29092}"

# Function to print step
print_step() {
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${GREEN}▶ $1${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
}

# Function to check if service is up
wait_for_service() {
    local name=$1
    local check_cmd=$2
    local max_attempts=${3:-30}
    local attempt=1

    echo -n "  Waiting for $name..."
    while [ $attempt -le $max_attempts ]; do
        if eval $check_cmd > /dev/null 2>&1; then
            echo -e " ${GREEN}Ready${NC}"
            return 0
        fi
        echo -n "."
        sleep 2
        attempt=$((attempt + 1))
    done

    echo -e " ${RED}Timeout${NC}"
    return 1
}

# Step 1: Check Docker
print_step "Step 1: Checking Docker"
if ! command -v docker &> /dev/null; then
    echo -e "${RED}  Docker not found. Please install Docker first.${NC}"
    exit 1
fi

if ! docker ps &> /dev/null; then
    echo -e "${YELLOW}  Docker daemon not accessible. Trying with sudo...${NC}"
    DOCKER_CMD="sudo docker"
    DOCKER_COMPOSE_CMD="sudo docker compose"
else
    DOCKER_CMD="docker"
    DOCKER_COMPOSE_CMD="docker compose"
fi

echo -e "  ${GREEN}Docker is available${NC}"

# Step 2: Start Infrastructure Services
print_step "Step 2: Starting Docker Infrastructure"

cd "$PROJECT_ROOT/deployment"

if [ -f "docker-compose.yml" ]; then
    echo "  Starting all Docker services..."
    $DOCKER_COMPOSE_CMD up -d 2>/dev/null || sudo docker compose up -d 2>/dev/null

    # Wait for services
    wait_for_service "Redis" "curl -s http://localhost:6379" 15 || true
    wait_for_service "Kafka" "$DOCKER_CMD exec deployment-kafka-1 kafka-topics --bootstrap-server localhost:9092 --list" 30 || true
    wait_for_service "Weaviate" "curl -s http://localhost:8081/v1/.well-known/ready" 30 || true
    wait_for_service "Neo4j" "curl -s http://localhost:7474" 30 || true
    wait_for_service "PostgreSQL" "PGPASSWORD=postgres psql -h localhost -U admin -d agentdb -c 'SELECT 1'" 15 || true
else
    echo -e "${YELLOW}  docker-compose.yml not found.${NC}"
fi

cd "$PROJECT_ROOT"

# Step 3: Fix Redis if in replica mode
print_step "Step 3: Ensuring Redis is writable"
$DOCKER_CMD exec deployment-redis-1 redis-cli REPLICAOF NO ONE 2>/dev/null || true
echo -e "  ${GREEN}Redis configured as master${NC}"

# Step 4: Index Scripts in Weaviate (optional)
print_step "Step 4: Indexing Scripts in Weaviate"

if curl -s http://localhost:8081/v1/.well-known/ready > /dev/null 2>&1; then
    if [ -f "$PROJECT_ROOT/backend/rag/script_library_indexer.py" ]; then
        echo "  Running script indexer..."
        python3 "$PROJECT_ROOT/backend/rag/script_library_indexer.py" 2>/dev/null || echo "  Indexer skipped"
    fi
else
    echo -e "${YELLOW}  Weaviate not accessible. Skipping indexing.${NC}"
fi

# Step 5: Start Orchestrator API
print_step "Step 5: Starting Orchestrator API (Port 8000)"

pkill -f "orchestrator/main.py" 2>/dev/null || true
sleep 1

cd "$PROJECT_ROOT/backend"
nohup python3 orchestrator/main.py > "$PROJECT_ROOT/logs/orchestrator.log" 2>&1 &
echo "  Started Orchestrator API (PID: $!)"

wait_for_service "Orchestrator API" "curl -s http://localhost:8000/health" 30

cd "$PROJECT_ROOT"

# Step 6: Start Kafka Streaming Services
print_step "Step 6: Starting Kafka Streaming Services"

# Kill existing streaming processes
pkill -f "servicenow_producer.py" 2>/dev/null || true
pkill -f "incident_consumer.py" 2>/dev/null || true
sleep 2

cd "$PROJECT_ROOT/backend/streaming"

# Start ServiceNow Producer
echo "  Starting ServiceNow Producer..."
nohup python3 servicenow_producer.py > "$PROJECT_ROOT/logs/servicenow_producer.log" 2>&1 &
echo "  ServiceNow Producer started (PID: $!)"

# Start Incident Consumer
echo "  Starting Incident Consumer..."
nohup python3 incident_consumer.py > "$PROJECT_ROOT/logs/incident_consumer.log" 2>&1 &
echo "  Incident Consumer started (PID: $!)"

sleep 3
cd "$PROJECT_ROOT"

# Step 7: Start Frontend
print_step "Step 7: Starting Frontend (Port 3002)"

pkill -f "next-server" 2>/dev/null || true
pkill -f "next start" 2>/dev/null || true
sleep 1

cd "$PROJECT_ROOT/frontend"

if [ -d "node_modules" ] && [ -d ".next" ]; then
    nohup npm start > "$PROJECT_ROOT/logs/frontend.log" 2>&1 &
    echo "  Frontend started (PID: $!)"
    wait_for_service "Frontend" "curl -s http://localhost:3002" 15
else
    echo -e "${YELLOW}  Frontend not built. Run 'npm install && npm run build' first.${NC}"
fi

cd "$PROJECT_ROOT"

# Step 8: Final Status
print_step "Step 8: System Status"

echo ""
echo "  Checking all services..."
echo ""

# Check each service
check_service() {
    local name=$1
    local check=$2
    if eval $check > /dev/null 2>&1; then
        echo -e "  ${GREEN}✓${NC} $name"
    else
        echo -e "  ${RED}✗${NC} $name"
    fi
}

check_service "Docker" "docker ps"
check_service "Redis" "docker exec deployment-redis-1 redis-cli ping"
check_service "Kafka" "docker exec deployment-kafka-1 kafka-topics --bootstrap-server localhost:9092 --list"
check_service "PostgreSQL" "PGPASSWORD=postgres psql -h localhost -U admin -d agentdb -c 'SELECT 1'"
check_service "Weaviate" "curl -s http://localhost:8081/v1/.well-known/ready"
check_service "Neo4j" "curl -s http://localhost:7474"
check_service "Orchestrator API" "curl -s http://localhost:8000/health"
check_service "ServiceNow Producer" "pgrep -f servicenow_producer.py"
check_service "Incident Consumer" "pgrep -f incident_consumer.py"
check_service "Frontend" "curl -s http://localhost:3002"
check_service "Prometheus" "curl -s http://localhost:9090/-/healthy"
check_service "Grafana" "curl -s http://localhost:3001/api/health"

echo ""
echo "================================================================================"
echo "  System Startup Complete!"
echo "================================================================================"
echo ""
echo "  Service URLs:"
echo "    • Frontend UI:      http://localhost:3002"
echo "    • Orchestrator API: http://localhost:8000"
echo "    • API Docs:         http://localhost:8000/docs"
echo "    • Grafana:          http://localhost:3001 (admin/admin)"
echo "    • Prometheus:       http://localhost:9090"
echo "    • Weaviate:         http://localhost:8081"
echo "    • Neo4j Browser:    http://localhost:7474"
echo ""
echo "  Log Files:"
echo "    • Orchestrator:         logs/orchestrator.log"
echo "    • ServiceNow Producer:  logs/servicenow_producer.log"
echo "    • Incident Consumer:    logs/incident_consumer.log"
echo "    • Frontend:             logs/frontend.log"
echo ""
echo "  To stop all services:"
echo "    bash scripts/stop_system.sh"
echo ""
echo "================================================================================"
