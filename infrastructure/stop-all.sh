#!/bin/bash
# =============================================================================
# Stop All Agent Infrastructure
#
# WHY: Clean shutdown of all platform components.
#
# USAGE: ./stop-all.sh [--clean] [--volumes]
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
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Parse arguments
REMOVE_VOLUMES=false
CLEAN_ALL=false

while [[ "$#" -gt 0 ]]; do
    case $1 in
        --volumes) REMOVE_VOLUMES=true; shift ;;
        --clean) CLEAN_ALL=true; shift ;;
        *) echo "Unknown parameter: $1"; exit 1 ;;
    esac
done

echo -e "${PURPLE}╔═══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${PURPLE}║        Stopping AI Agent Platform Infrastructure             ║${NC}"
echo -e "${PURPLE}╚═══════════════════════════════════════════════════════════════╝${NC}"

cd "$SCRIPT_DIR"

# =============================================================================
# Stop Frontend
# =============================================================================

echo -e "\n${BLUE}[1/4] Stopping Frontend...${NC}"

if [ -f "/tmp/frontend.pid" ]; then
    kill $(cat /tmp/frontend.pid) 2>/dev/null || true
    rm /tmp/frontend.pid
    echo -e "${GREEN}✓ Frontend stopped${NC}"
else
    echo -e "${YELLOW}Frontend PID not found${NC}"
fi

# Also try to kill any remaining npm processes
pkill -f "npm run dev" 2>/dev/null || true
pkill -f "next dev" 2>/dev/null || true

# =============================================================================
# Stop Backend (if running directly)
# =============================================================================

echo -e "\n${BLUE}[2/4] Stopping Backend...${NC}"

if [ -f "/tmp/backend.pid" ]; then
    kill $(cat /tmp/backend.pid) 2>/dev/null || true
    rm /tmp/backend.pid
    echo -e "${GREEN}✓ Backend stopped${NC}"
else
    echo -e "${YELLOW}Backend PID not found${NC}"
fi

# Stop backend container if running
docker stop ai-agent-backend 2>/dev/null || true
docker rm ai-agent-backend 2>/dev/null || true

# =============================================================================
# Stop Data Agent Services
# =============================================================================

echo -e "\n${BLUE}[3/4] Stopping Data Agent services...${NC}"

docker-compose -f docker-compose.data-agent.yml down

if [ "$CLEAN_ALL" = true ]; then
    echo -e "${YELLOW}Removing containers and local images...${NC}"
    docker-compose -f docker-compose.data-agent.yml down --rmi local
fi

echo -e "${GREEN}✓ Data Agent services stopped${NC}"

# =============================================================================
# Stop Main Backend Services (if using deployment compose)
# =============================================================================

echo -e "\n${BLUE}[4/4] Stopping shared services...${NC}"

if [ -f "$PROJECT_ROOT/deployment/docker-compose.yml" ]; then
    docker-compose -f "$PROJECT_ROOT/deployment/docker-compose.yml" down 2>/dev/null || true
fi

echo -e "${GREEN}✓ Shared services stopped${NC}"

# =============================================================================
# Remove Volumes (if requested)
# =============================================================================

if [ "$REMOVE_VOLUMES" = true ]; then
    echo -e "\n${RED}Warning: This will delete all data!${NC}"
    read -p "Remove all volumes? (y/N) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        docker-compose -f docker-compose.data-agent.yml down -v
        echo -e "${GREEN}Volumes removed${NC}"
    else
        echo -e "${YELLOW}Skipped volume removal${NC}"
    fi
fi

# =============================================================================
# Summary
# =============================================================================

echo -e "\n${PURPLE}╔═══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${PURPLE}║        AI Agent Platform Infrastructure Stopped              ║${NC}"
echo -e "${PURPLE}╚═══════════════════════════════════════════════════════════════╝${NC}"

# Check for remaining containers
remaining=$(docker ps --filter "name=data-agent" --filter "name=ai-agent" -q | wc -l)
if [ "$remaining" -gt 0 ]; then
    echo -e "\n${YELLOW}Some containers may still be running:${NC}"
    docker ps --filter "name=data-agent" --filter "name=ai-agent" --format "table {{.Names}}\t{{.Status}}"
else
    echo -e "\n${GREEN}All platform containers stopped.${NC}"
fi

echo -e "
${YELLOW}To restart:${NC}
  ./infrastructure/start-all.sh

${YELLOW}To remove all data:${NC}
  ./infrastructure/stop-all.sh --volumes
"
