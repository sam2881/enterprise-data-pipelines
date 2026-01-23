#!/bin/bash
##
# AI Agent Platform - System Verification Script
# Verifies all components according to README.md architecture
##

set -e

echo "============================================================================="
echo "  AI AGENT PLATFORM - SYSTEM VERIFICATION"
echo "  Based on README.md Architecture"
echo "============================================================================="
echo ""

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Counters
PASSED=0
FAILED=0

check() {
    local name="$1"
    local command="$2"

    echo -n "Checking $name... "
    if eval "$command" > /dev/null 2>&1; then
        echo -e "${GREEN}✓ PASS${NC}"
        ((PASSED++))
        return 0
    else
        echo -e "${RED}✗ FAIL${NC}"
        ((FAILED++))
        return 1
    fi
}

echo "1. Docker Infrastructure Services (README Section: Architecture)"
echo "----------------------------------------------------------------"
check "Kafka" "sudo docker ps | grep -q deployment-kafka-1"
check "Zookeeper" "sudo docker ps | grep -q deployment-zookeeper-1"
check "Postgres" "sudo docker ps | grep -q deployment-postgres-1"
check "Redis" "sudo docker ps | grep -q deployment-redis-1"
check "Neo4j" "sudo docker ps | grep -q deployment-neo4j-1"
check "Weaviate" "sudo docker ps | grep -q deployment-weaviate-1"
echo ""

echo "2. Backend Services (README: Quick Start Section 3 & 4)"
echo "--------------------------------------------------------"
check "Backend API (port 8000)" "curl -sf http://localhost:8000/health"
check "Kafka Consumer" "pgrep -f incident_consumer"
echo ""

echo "3. Frontend Service (README: Quick Start Section 5)"
echo "----------------------------------------------------"
check "Frontend (port 3002)" "curl -sI http://localhost:3002 | head -1 | grep -q 200"
echo ""

echo "4. ServiceNow Integration (README: Integrations - ServiceNow)"
echo "--------------------------------------------------------------"
INCIDENT_COUNT=$(curl -s http://localhost:8000/api/incidents | python3 -c "import sys, json; print(json.load(sys.stdin).get('count', 0))" 2>/dev/null || echo "0")
if [ "$INCIDENT_COUNT" -gt "0" ]; then
    echo -e "ServiceNow Integration... ${GREEN}✓ PASS${NC} ($INCIDENT_COUNT incidents)"
    ((PASSED++))
else
    echo -e "ServiceNow Integration... ${RED}✗ FAIL${NC} (0 incidents)"
    ((FAILED++))
fi
echo ""

echo "5. API Endpoints (README: API Endpoints)"
echo "-----------------------------------------"
check "GET /api/incidents" "curl -sf http://localhost:8000/api/incidents"
check "POST /api/scripts/match" "curl -sf -X POST http://localhost:8000/api/scripts/match -H 'Content-Type: application/json' -d '{\"incident_id\":\"TEST\",\"short_description\":\"test\"}'"
check "GET /api/scripts" "curl -sf http://localhost:8000/api/scripts"
echo ""

echo "6. Kafka Topics (README: Kafka Topics)"
echo "---------------------------------------"
check "servicenow.incidents topic" "sudo docker exec deployment-kafka-1 kafka-topics --list --bootstrap-server localhost:9092 | grep -q servicenow.incidents"
check "gcp.alerts topic" "sudo docker exec deployment-kafka-1 kafka-topics --list --bootstrap-server localhost:9092 | grep -q gcp.alerts"
check "agent.events topic" "sudo docker exec deployment-kafka-1 kafka-topics --list --bootstrap-server localhost:9092 | grep -q agent.events"
echo ""

echo "7. Script Registry (README: Enterprise Runbook Registry)"
echo "---------------------------------------------------------"
SCRIPT_COUNT=$(curl -s http://localhost:8000/api/scripts | python3 -c "import sys, json; print(json.load(sys.stdin).get('count', 0))" 2>/dev/null || echo "0")
if [ "$SCRIPT_COUNT" -ge "10" ]; then
    echo -e "Script Registry... ${GREEN}✓ PASS${NC} ($SCRIPT_COUNT scripts loaded)"
    ((PASSED++))
else
    echo -e "Script Registry... ${RED}✗ FAIL${NC} ($SCRIPT_COUNT scripts, expected >= 10)"
    ((FAILED++))
fi
echo ""

echo "============================================================================="
echo "  VERIFICATION SUMMARY"
echo "============================================================================="
echo -e "  ${GREEN}Passed: $PASSED${NC}"
echo -e "  ${RED}Failed: $FAILED${NC}"
echo ""

if [ $FAILED -eq 0 ]; then
    echo -e "${GREEN}✓ ALL CHECKS PASSED - System is production-ready!${NC}"
    echo ""
    echo "Access URLs:"
    echo "  - Frontend Dashboard: http://localhost:3002"
    echo "  - Backend API: http://localhost:8000"
    echo "  - API Docs: http://localhost:8000/docs"
    echo "  - Kafka UI: http://localhost:8085"
    echo "  - Grafana: http://localhost:3000"
    echo "  - Neo4j Browser: http://localhost:7474"
    exit 0
else
    echo -e "${RED}✗ SOME CHECKS FAILED - Review above output${NC}"
    exit 1
fi
