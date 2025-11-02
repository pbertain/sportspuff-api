#!/bin/bash
# Don't use set -e, we want to continue even if some commands fail

echo "=== Starting cleanup for port ${API_PORT} ==="

# Function to kill anything using the port
kill_port_listeners() {
  local port=$1
  local attempts=0
  local max_attempts=5
  
  while [ $attempts -lt $max_attempts ]; do
    attempts=$((attempts + 1))
    echo "Attempt $attempts: Checking for processes using port ${port}..."
    
    # Try fuser first (most aggressive)
    if command -v fuser >/dev/null 2>&1; then
      echo "Using fuser to kill processes on port ${port}..."
      sudo fuser -k ${port}/tcp 2>/dev/null || true
      sleep 1
    fi
    
    # Check with ss - look for any binding on the port (127.0.0.1, 0.0.0.0, ::, etc)
    if command -v ss >/dev/null 2>&1; then
      # Check for listeners on the port with any IP binding
      LISTENERS=$(sudo ss -tlnp | grep -E ":[[:space:]]*${port}[[:space:]]" || true)
      if [ -n "$LISTENERS" ]; then
        echo "Found listeners on port ${port}:"
        echo "$LISTENERS"
        # Extract all PIDs from the output
        PIDS=$(echo "$LISTENERS" | sed -n 's/.*pid=\([0-9]*\).*/\1/p' | sort -u || true)
        if [ -z "$PIDS" ]; then
          # Alternative extraction method
          PIDS=$(echo "$LISTENERS" | awk -F'[=,)]' '{for(i=1;i<=NF;i++) if($i ~ /^[0-9]+$/) print $i}' | sort -u || true)
        fi
        if [ -n "$PIDS" ]; then
          echo "Killing processes using port ${port}: $PIDS"
          echo "$PIDS" | xargs -r sudo kill -9 2>/dev/null || true
          sleep 1
        fi
      fi
    fi
    
    # Check with lsof
    if command -v lsof >/dev/null 2>&1; then
      PIDS=$(sudo lsof -ti:${port} 2>/dev/null || true)
      if [ -n "$PIDS" ]; then
        echo "Found processes using port ${port} (via lsof): $PIDS"
        echo "$PIDS" | xargs -r sudo kill -9 2>/dev/null || true
        sleep 1
      fi
    fi
    
    # Check if port is now free (check for any IP binding)
    if command -v ss >/dev/null 2>&1; then
      STILL_LISTENING=$(sudo ss -tlnp | grep -E ":[[:space:]]*${port}[[:space:]]" || true)
      if [ -z "$STILL_LISTENING" ]; then
        echo "Port ${port} is now free!"
        return 0
      else
        echo "Port ${port} still in use: $STILL_LISTENING"
        sleep 2
      fi
    else
      # If ss not available, assume we did our best
      sleep 2
    fi
  done
  
  echo "WARNING: Port ${port} may still be in use after $max_attempts attempts"
  return 1
}

# Stop all Docker containers first
echo "=== Stopping Docker containers ==="
# Stop all sports-data containers from any deployment
sudo docker ps -aq --filter "name=sports-data" | while read container_id; do
  if [ -n "$container_id" ]; then
    echo "Stopping and removing container $container_id"
    sudo docker stop "$container_id" 2>/dev/null || true
    sudo docker rm -f "$container_id" 2>/dev/null || true
  fi
done || true

# Try to stop containers from this specific deployment
if [ -n "${DEPLOYMENT_DIR}" ] && [ -d "${DEPLOYMENT_DIR}/sports-data-service" ]; then
  echo "=== Stopping containers in ${DEPLOYMENT_DIR}/sports-data-service ==="
  cd "${DEPLOYMENT_DIR}/sports-data-service"
  sudo docker-compose down --remove-orphans 2>/dev/null || true
fi

# Force remove containers by name (case where docker-compose didn't clean up)
echo "=== Force removing containers by name ==="
sudo docker rm -f sports-data-postgres sports-data-service 2>/dev/null || true

# Clean up Docker networks that might be holding port mappings
echo "=== Cleaning up Docker networks ==="
# Prune unused networks
sudo docker network prune -f 2>/dev/null || true

# Remove any networks associated with sports-data-service
# Use -q flag to get network IDs to avoid Jinja2 template conflicts
sudo docker network ls -q | while read net_id; do
  if [ -n "$net_id" ]; then
    net_name=$(sudo docker network inspect "$net_id" --format '{{.Name}}' 2>/dev/null || true)
    if echo "$net_name" | grep -qE "sports-data-service|sportspuff"; then
      echo "Removing Docker network: $net_name ($net_id)"
      sudo docker network rm "$net_id" 2>/dev/null || true
    fi
  fi
done || true

# Now aggressively kill anything using the port
echo "=== Killing processes using port ${API_PORT} ==="
kill_port_listeners "${API_PORT}"

# Final check
echo "=== Final port check ==="
sleep 2
if command -v ss >/dev/null 2>&1; then
  FINAL_CHECK=$(sudo ss -tlnp | grep -E ":[[:space:]]*${API_PORT}[[:space:]]" || true)
  if [ -n "$FINAL_CHECK" ]; then
    echo "ERROR: Port ${API_PORT} is still in use:"
    echo "$FINAL_CHECK"
    echo "This will cause the deployment to fail."
    # One last desperate attempt
    echo "Making one final attempt to free the port..."
    sudo fuser -k ${API_PORT}/tcp 2>/dev/null || true
    sleep 2
  else
    echo "SUCCESS: Port ${API_PORT} is free"
  fi
fi

echo "=== Cleanup complete ==="

