#!/bin/bash
# Script to ensure both dev and prod environments are running
# Run this periodically or via cron to keep both environments up

set -e

echo "=========================================="
echo "Ensuring Dev and Prod are Running"
echo "=========================================="
echo ""

# Function to check and start an environment
check_and_start() {
    local env=$1
    local port=$2
    local dir=$3
    
    echo "Checking $env environment..."
    
    if [ ! -d "$dir/sports-data-service" ]; then
        echo "⚠ $env directory not found: $dir/sports-data-service"
        return 1
    fi
    
    cd "$dir/sports-data-service"
    
    # Check if health endpoint responds
    if curl -s --connect-timeout 3 "http://localhost:$port/health" >/dev/null 2>&1; then
        echo "✓ $env is healthy on port $port"
        return 0
    fi
    
    echo "⚠ $env is not responding, checking containers..."
    
    # Check container status
    export COMPOSE_PROJECT_NAME="sports-data-$env"
    if sudo docker-compose -p "$COMPOSE_PROJECT_NAME" ps | grep -q "Up"; then
        echo "  Containers exist but not healthy, restarting..."
    else
        echo "  Containers not running, starting..."
    fi
    
    # Load environment
    if [ -f "../.env" ]; then
        export $(grep -v '^#' ../.env | xargs)
    fi
    
    export API_PORT=$port
    export COMPOSE_PROJECT_NAME="sports-data-$env"
    
    # Start containers
    sudo -E docker-compose -p "$COMPOSE_PROJECT_NAME" up -d
    
    # Wait and check
    sleep 5
    for i in {1..10}; do
        if curl -s --connect-timeout 3 "http://localhost:$port/health" >/dev/null 2>&1; then
            echo "✓ $env is now healthy!"
            return 0
        fi
        echo "  Waiting for $env to become healthy... ($i/10)"
        sleep 2
    done
    
    echo "⚠ $env did not become healthy after restart"
    return 1
}

# Check and start dev
check_and_start "dev" "34181" "/opt/sportspuff-api-dev"

# Check and start prod
check_and_start "prod" "34180" "/opt/sportspuff-api"

echo ""
echo "=========================================="
echo "Status Summary"
echo "=========================================="
echo "DEV:  $(curl -s --connect-timeout 2 http://localhost:34181/health 2>/dev/null || echo 'DOWN')"
echo "PROD: $(curl -s --connect-timeout 2 http://localhost:34180/health 2>/dev/null || echo 'DOWN')"
echo ""

