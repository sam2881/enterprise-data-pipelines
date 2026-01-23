#!/bin/bash
# Complete System Stop Script - AI Agent Platform

echo "================================================================================"
echo "  AI Agent Platform - Stopping Services"
echo "================================================================================"
echo ""

# Color codes
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

PROJECT_ROOT="/home/samrattidke600/ai_agent_app"

# Function to stop process by pattern
stop_process() {
    local name=$1
    local pattern=$2

    PIDS=$(pgrep -f "$pattern" 2>/dev/null)
    if [ -n "$PIDS" ]; then
        echo "$PIDS" | xargs kill 2>/dev/null
        echo -e "  ${GREEN}Stopped${NC} $name (PIDs: $PIDS)"
        return 0
    else
        echo -e "  ${YELLOW}Not running${NC} $name"
        return 1
    fi
}

# Function to stop process on port
stop_port() {
    local name=$1
    local port=$2

    PID=$(lsof -ti:$port 2>/dev/null)
    if [ -n "$PID" ]; then
        kill $PID 2>/dev/null
        echo -e "  ${GREEN}Stopped${NC} $name on port $port (PID: $PID)"
        return 0
    else
        echo -e "  ${YELLOW}Not running${NC} $name on port $port"
        return 1
    fi
}

echo ""
echo "Stopping Python Services..."
echo ""

# Stop Orchestrator API
stop_process "Orchestrator API" "orchestrator/main.py"

# Stop Kafka Streaming Services
stop_process "ServiceNow Producer" "servicenow_producer.py"
stop_process "Incident Consumer" "incident_consumer.py"

echo ""
echo "Stopping Frontend..."
echo ""

# Stop Frontend (Next.js on port 3000)
stop_process "Next.js Server" "next-server"
stop_port "Frontend" 3002

echo ""
echo "Docker Containers Status..."
echo ""

# Check if user wants to stop Docker containers
if [ "$1" == "--all" ] || [ "$1" == "-a" ]; then
    echo "  Stopping Docker containers..."
    cd "$PROJECT_ROOT/deployment"

    if [ -f "docker-compose.yml" ]; then
        docker compose down 2>/dev/null || sudo docker compose down 2>/dev/null
        echo -e "  ${GREEN}Docker containers stopped${NC}"
    else
        echo -e "  ${YELLOW}docker-compose.yml not found${NC}"
    fi
else
    echo -e "  ${YELLOW}Docker containers left running${NC}"
    echo "  (Use --all or -a flag to also stop Docker containers)"
fi

echo ""
echo "================================================================================"
echo "  Services Stopped"
echo "================================================================================"
echo ""
echo "  To start again:"
echo "    bash scripts/start_system.sh"
echo ""
echo "  To stop Docker containers too:"
echo "    bash scripts/stop_system.sh --all"
echo ""
echo "================================================================================"
