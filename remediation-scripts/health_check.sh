#!/bin/bash
# System Health Check Script

echo "================================================================================"
echo "🏥 AI Agent Platform - System Health Check"
echo "================================================================================"
echo ""

# Color codes
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to check service
check_service() {
    local name=$1
    local check_cmd=$2

    echo -n "Checking $name... "
    if eval $check_cmd > /dev/null 2>&1; then
        echo -e "${GREEN}✅ UP${NC}"
        return 0
    else
        echo -e "${RED}❌ DOWN${NC}"
        return 1
    fi
}

# Infrastructure Services
echo "📦 Infrastructure Services:"
echo "----------------------------------------"
check_service "Weaviate (8081)    " "curl -s http://localhost:8081/v1/.well-known/ready"
check_service "Neo4j (7474)      " "curl -s http://localhost:7474"
check_service "Kafka (9092)      " "nc -z localhost 9092"
echo ""

# Backend Services
echo "🔧 Backend Services:"
echo "----------------------------------------"
check_service "Orchestrator (8000)" "curl -s http://localhost:8000/health"
check_service "Infra Agent (8013) " "curl -s http://localhost:8013/health"
echo ""

# Frontend
echo "🖥️  Frontend:"
echo "----------------------------------------"
check_service "Next.js UI (3002) " "curl -s http://localhost:3002"
echo ""

# External Services
echo "🌐 External Services:"
echo "----------------------------------------"
echo -n "ServiceNow... "
if [ -n "$SNOW_INSTANCE_URL" ] && [ -n "$SNOW_API_KEY" ]; then
    if curl -s -H "Authorization: Bearer $SNOW_API_KEY" "$SNOW_INSTANCE_URL/api/now/table/incident?sysparm_limit=1" > /dev/null 2>&1; then
        echo -e "${GREEN}✅ Connected${NC}"
    else
        echo -e "${YELLOW}⚠️  Configured but not accessible${NC}"
    fi
else
    echo -e "${YELLOW}⚠️  Not configured${NC}"
fi

echo -n "GitHub API... "
if [ -n "$GITHUB_TOKEN" ]; then
    if curl -s -H "Authorization: token $GITHUB_TOKEN" https://api.github.com/user > /dev/null 2>&1; then
        echo -e "${GREEN}✅ Connected${NC}"
    else
        echo -e "${YELLOW}⚠️  Token invalid${NC}"
    fi
else
    echo -e "${YELLOW}⚠️  Token not configured${NC}"
fi

echo -n "OpenAI API... "
if [ -n "$OPENAI_API_KEY" ]; then
    if curl -s -H "Authorization: Bearer $OPENAI_API_KEY" https://api.openai.com/v1/models > /dev/null 2>&1; then
        echo -e "${GREEN}✅ Connected${NC}"
    else
        echo -e "${YELLOW}⚠️  Key invalid${NC}"
    fi
else
    echo -e "${YELLOW}⚠️  Key not configured${NC}"
fi
echo ""

# Data Status
echo "💾 Data Status:"
echo "----------------------------------------"

# Check Weaviate for indexed scripts
echo -n "Scripts indexed in Weaviate... "
SCRIPT_COUNT=$(python3 -c "
import weaviate
import sys
try:
    c = weaviate.Client('http://localhost:8081', timeout_config=(2, 5))
    result = c.query.aggregate('InfrastructureScript').with_meta_count().do()
    count = result.get('data', {}).get('Aggregate', {}).get('InfrastructureScript', [{}])[0].get('meta', {}).get('count', 0)
    print(count, end='')
    sys.exit(0 if count > 0 else 1)
except Exception as e:
    print('0', end='')
    sys.exit(1)
" 2>/dev/null)

if [ $? -eq 0 ] && [ "$SCRIPT_COUNT" -gt "0" ]; then
    echo -e "${GREEN}✅ $SCRIPT_COUNT scripts${NC}"
else
    echo -e "${RED}❌ 0 scripts${NC}"
    echo -e "${YELLOW}   Run: python3 backend/rag/script_library_indexer.py${NC}"
fi

# Check Neo4j connection
echo -n "Neo4j graph database... "
NEO4J_CHECK=$(python3 -c "
from neo4j import GraphDatabase
import sys
import os
try:
    driver = GraphDatabase.driver(
        os.getenv('NEO4J_URI', 'bolt://localhost:7687'),
        auth=(os.getenv('NEO4J_USERNAME', 'neo4j'),
              os.getenv('NEO4J_PASSWORD', 'password'))
    )
    with driver.session() as session:
        result = session.run('RETURN 1')
        result.single()
    driver.close()
    print('connected', end='')
    sys.exit(0)
except Exception as e:
    print('error', end='')
    sys.exit(1)
" 2>/dev/null)

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ Connected${NC}"
else
    echo -e "${RED}❌ Not connected${NC}"
fi

echo ""

# Docker Containers
echo "🐳 Docker Containers:"
echo "----------------------------------------"
if command -v docker &> /dev/null; then
    RUNNING_CONTAINERS=$(docker ps --format "table {{.Names}}\t{{.Status}}" | grep -v NAMES)
    if [ -n "$RUNNING_CONTAINERS" ]; then
        echo "$RUNNING_CONTAINERS" | while read line; do
            container_name=$(echo $line | awk '{print $1}')
            status=$(echo $line | awk '{print $2}')
            if [[ $status == "Up"* ]]; then
                echo -e "${GREEN}✅${NC} $line"
            else
                echo -e "${RED}❌${NC} $line"
            fi
        done
    else
        echo -e "${YELLOW}⚠️  No containers running${NC}"
    fi
else
    echo -e "${YELLOW}⚠️  Docker not available${NC}"
fi

echo ""
echo "================================================================================"

# Overall Status
echo ""
echo "📊 Overall Status:"
echo "----------------------------------------"

# Count services
TOTAL=0
UP=0

# Infrastructure
curl -s http://localhost:8081/v1/.well-known/ready > /dev/null 2>&1 && UP=$((UP+1))
TOTAL=$((TOTAL+1))

curl -s http://localhost:7474 > /dev/null 2>&1 && UP=$((UP+1))
TOTAL=$((TOTAL+1))

# Backend
curl -s http://localhost:8000/health > /dev/null 2>&1 && UP=$((UP+1))
TOTAL=$((TOTAL+1))

# Check if scripts are indexed
if [ "$SCRIPT_COUNT" -gt "0" ]; then
    UP=$((UP+1))
fi
TOTAL=$((TOTAL+1))

PERCENT=$((UP * 100 / TOTAL))

if [ $PERCENT -eq 100 ]; then
    echo -e "${GREEN}✅ All critical services are running ($UP/$TOTAL)${NC}"
    echo ""
    echo "🚀 System is ready for end-to-end testing!"
    echo ""
    echo "Next steps:"
    echo "  1. Run: python3 scripts/test_intelligent_selection.py"
    echo "  2. Run: python3 scripts/test_complete_workflow.py"
    echo "  3. Open UI: http://localhost:3002/approvals"
elif [ $PERCENT -ge 75 ]; then
    echo -e "${YELLOW}⚠️  Most services are running ($UP/$TOTAL - $PERCENT%)${NC}"
    echo ""
    echo "Some services need attention. Check logs above."
elif [ $PERCENT -ge 50 ]; then
    echo -e "${YELLOW}⚠️  Some services are running ($UP/$TOTAL - $PERCENT%)${NC}"
    echo ""
    echo "Several services need to be started."
else
    echo -e "${RED}❌ Most services are down ($UP/$TOTAL - $PERCENT%)${NC}"
    echo ""
    echo "Please start services:"
    echo "  1. cd deployment && docker-compose up -d"
    echo "  2. python3 backend/rag/script_library_indexer.py"
    echo "  3. cd backend/orchestrator && python3 main.py"
fi

echo ""
echo "================================================================================"
