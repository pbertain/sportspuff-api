#!/bin/bash
# Service Status Check Script for Production API
# Run this on the server: host74.nird.club

set -e

DEPLOYMENT_DIR="/opt/sportspuff-api/sports-data-service"
API_PORT=34180

echo "=========================================="
echo "Sports Data Service Status Check"
echo "=========================================="
echo ""

# Check if deployment directory exists
echo "1. Checking deployment directory..."
if [ -d "$DEPLOYMENT_DIR" ]; then
    echo "   ✓ Directory exists: $DEPLOYMENT_DIR"
    cd "$DEPLOYMENT_DIR"
else
    echo "   ✗ Directory not found: $DEPLOYMENT_DIR"
    exit 1
fi

# Check Docker Compose status
echo ""
echo "2. Checking Docker Compose containers..."
if [ -f "docker-compose.yml" ]; then
    sudo docker-compose ps || echo "   ⚠ docker-compose ps failed"
else
    echo "   ✗ docker-compose.yml not found"
fi

# Check all sports-data containers
echo ""
echo "3. Checking all sports-data containers..."
sudo docker ps -a --filter "name=sports-data-prod" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" || true

# Check if port is in use
echo ""
echo "4. Checking if port $API_PORT is listening..."
if command -v lsof >/dev/null 2>&1; then
    sudo lsof -i :$API_PORT || echo "   Port $API_PORT is not in use"
elif command -v ss >/dev/null 2>&1; then
    sudo ss -tlnp | grep ":$API_PORT " || echo "   Port $API_PORT is not in use"
else
    echo "   ⚠ Cannot check port (lsof/ss not available)"
fi

# Check container logs
echo ""
echo "5. Checking container logs (last 50 lines)..."
CONTAINER_NAMES=(
    "sports-data-prod-service"
    "sports-data-prod-sports-service-1"
    "sports-data-service"
)

FOUND_CONTAINER=""
for name in "${CONTAINER_NAMES[@]}"; do
    if sudo docker ps -a --format "{{.Names}}" | grep -q "^${name}$"; then
        FOUND_CONTAINER="$name"
        echo "   Found container: $name"
        echo "   Logs:"
        sudo docker logs "$name" --tail 50 2>&1 | sed 's/^/   /'
        break
    fi
done

if [ -z "$FOUND_CONTAINER" ]; then
    echo "   ⚠ No container found with expected names"
    echo "   Available containers:"
    sudo docker ps -a --format "   {{.Names}}" | grep -i sports || echo "   (none)"
fi

# Check container state
echo ""
echo "6. Checking container state..."
if [ -n "$FOUND_CONTAINER" ]; then
    echo "   Container: $FOUND_CONTAINER"
    sudo docker inspect "$FOUND_CONTAINER" --format "   State: {{.State.Status}}" 2>/dev/null || true
    sudo docker inspect "$FOUND_CONTAINER" --format "   Exit Code: {{.State.ExitCode}}" 2>/dev/null || true
    sudo docker inspect "$FOUND_CONTAINER" --format "   Error: {{.State.Error}}" 2>/dev/null || true
fi

# Check health endpoint
echo ""
echo "7. Testing API health endpoint..."
if curl -s -f "http://localhost:$API_PORT/health" >/dev/null 2>&1; then
    echo "   ✓ Health endpoint responding"
    curl -s "http://localhost:$API_PORT/health"
else
    echo "   ✗ Health endpoint not responding"
fi

# Check .env file
echo ""
echo "8. Checking environment configuration..."
if [ -f ".env" ]; then
    echo "   ✓ .env file exists"
    if grep -q "API_PORT" .env; then
        echo "   API_PORT: $(grep API_PORT .env | cut -d'=' -f2)"
    fi
else
    echo "   ⚠ .env file not found"
fi

echo ""
echo "=========================================="
echo "Diagnostic Complete"
echo "=========================================="

