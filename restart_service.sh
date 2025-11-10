#!/bin/bash
# Service Restart Script for Production API
# Run this on the server: host74.nird.club

set -e

DEPLOYMENT_DIR="/opt/sportspuff-api/sports-data-service"
API_PORT=34180

echo "=========================================="
echo "Sports Data Service Restart"
echo "=========================================="
echo ""

cd "$DEPLOYMENT_DIR"

# Load environment variables
if [ -f ".env" ]; then
    export $(grep -v '^#' .env | xargs)
    echo "Loaded environment from .env"
else
    echo "⚠ .env file not found, using defaults"
    export API_PORT=${API_PORT:-34180}
fi

# Ensure API_PORT is set
export API_PORT=${API_PORT:-34180}
export COMPOSE_PROJECT_NAME="sports-data-prod"

echo "API_PORT: $API_PORT"
echo ""

# Stop existing containers
echo "Stopping existing containers..."
sudo docker-compose -p "$COMPOSE_PROJECT_NAME" down --remove-orphans || true

# Force stop any remaining containers
sudo docker stop sports-data-prod-service sports-data-prod-sports-service-1 2>/dev/null || true
sudo docker rm -f sports-data-prod-service sports-data-prod-sports-service-1 2>/dev/null || true

# Wait a moment
sleep 2

# Start containers
echo ""
echo "Starting containers..."
sudo -E docker-compose -p "$COMPOSE_PROJECT_NAME" up -d

# Wait for services
echo ""
echo "Waiting for services to start..."
sleep 5

# Check status
echo ""
echo "Container status:"
sudo docker-compose -p "$COMPOSE_PROJECT_NAME" ps

# Test health endpoint
echo ""
echo "Testing health endpoint..."
for i in {1..10}; do
    if curl -s -f "http://localhost:$API_PORT/health" >/dev/null 2>&1; then
        echo "✓ Service is healthy!"
        curl -s "http://localhost:$API_PORT/health"
        exit 0
    fi
    echo "   Attempt $i/10: Waiting for service..."
    sleep 2
done

echo "⚠ Service did not become healthy. Check logs:"
sudo docker-compose -p "$COMPOSE_PROJECT_NAME" logs --tail 50

