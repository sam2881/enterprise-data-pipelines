#!/bin/bash

set -e

echo "=========================================="
echo "AI Agent Platform - Setup Script"
echo "=========================================="

# Check if .env exists
if [ ! -f .env ]; then
    echo "Creating .env file from .env.example..."
    cp .env.example .env
    echo "✓ Created .env file"
    echo "⚠️  Please edit .env with your API keys before starting services"
else
    echo "✓ .env file already exists"
fi

# Install Python dependencies (optional, for local development)
if [ -f requirements.txt ]; then
    echo ""
    echo "Do you want to install Python dependencies locally? (y/n)"
    read -r install_deps
    if [ "$install_deps" = "y" ]; then
        echo "Installing Python dependencies..."
        pip install -r requirements.txt
        echo "✓ Dependencies installed"
    fi
fi

# Start Docker Compose services
echo ""
echo "Starting Docker Compose services..."
cd deployment

# Pull latest images
echo "Pulling Docker images..."
docker-compose pull

# Build custom images
echo "Building custom images..."
docker-compose build

# Start services
echo "Starting all services..."
docker-compose up -d

echo ""
echo "Waiting for services to be ready..."
sleep 30

# Check service health
echo ""
echo "Checking service health..."

services=(
    "http://localhost:8000/health|Orchestrator"
    "http://localhost:8010/health|ServiceNow Agent"
    "http://localhost:8011/health|Jira Agent"
    "http://localhost:8012/health|GitHub Agent"
    "http://localhost:8013/health|Infra Agent"
    "http://localhost:8014/health|Data Agent"
    "http://localhost:8090/health|DSPy Optimizer"
)

for service in "${services[@]}"; do
    IFS='|' read -r url name <<< "$service"
    if curl -f -s "$url" > /dev/null 2>&1; then
        echo "✓ $name is healthy"
    else
        echo "✗ $name is not responding"
    fi
done

echo ""
echo "=========================================="
echo "Populating Demo Data..."
echo "=========================================="
echo ""

# Run demo data population
cd ..
bash scripts/run_full_demo.sh

echo ""
echo "=========================================="
echo "Setup Complete!"
echo "=========================================="
echo ""
echo "Access services at:"
echo "  🌐 API Gateway:      http://localhost:8000"
echo "  📊 API Docs:         http://localhost:8000/docs"
echo "  📨 Kafka UI:         http://localhost:8085"
echo "  📈 Grafana:          http://localhost:3000 (admin/admin)"
echo "  🔍 LangFuse:         http://localhost:3001"
echo "  🕸️  Neo4j Browser:    http://localhost:7474 (neo4j/password123)"
echo "  🔮 Weaviate:         http://localhost:8081"
echo "  📉 Prometheus:       http://localhost:9090"
echo "  🎯 DSPy Optimizer:   http://localhost:8090"
echo ""
echo "To stop all services:"
echo "  cd deployment && docker-compose down"
echo ""
echo "To view logs:"
echo "  cd deployment && docker-compose logs -f [service_name]"
echo ""
echo "=========================================="
