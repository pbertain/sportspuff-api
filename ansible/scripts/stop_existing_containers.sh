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

# Additional cleanup: Check for ANY containers using the port
echo "=== Checking for containers using port ${API_PORT} ==="
# Check all running containers for active port mappings
# Use docker ps with --no-trunc to get full output, then parse
sudo docker ps --no-trunc | grep -E ":${API_PORT}->|:${API_PORT}/" | awk '{print $1}' | while read container_id; do
  if [ -n "$container_id" ] && [ "$container_id" != "CONTAINER" ]; then
    echo "Found running container $container_id using port ${API_PORT}, stopping it..."
    sudo docker stop "$container_id" 2>/dev/null || true
    sudo docker rm -f "$container_id" 2>/dev/null || true
  fi
done || true

# Check all containers (including stopped) by inspecting port bindings
# This catches containers that have port bindings configured but are stopped
echo "=== Checking stopped containers for port bindings ==="
sudo docker ps -aq | while read container_id; do
  if [ -n "$container_id" ]; then
    # Get port bindings from inspect JSON
    inspect_output=$(sudo docker inspect "$container_id" 2>/dev/null || true)
    # Check for port bindings that match our API_PORT (format: "0.0.0.0:34180->34180/tcp" or "HostPort":"34180")
    if echo "$inspect_output" | grep -qE "\"${API_PORT}\"|\":${API_PORT}->|:${API_PORT}/"; then
      container_name=$(echo "$inspect_output" | grep -oE '"Name"[[:space:]]*:[[:space:]]*"[^"]*' | head -n1 | sed 's/"Name"[[:space:]]*:[[:space:]]*"//' | sed 's|^/||' || echo "$container_id")
      echo "Found container $container_name ($container_id) with port binding to ${API_PORT}, removing it..."
      sudo docker stop "$container_id" 2>/dev/null || true
      sudo docker rm -f "$container_id" 2>/dev/null || true
    fi
  fi
done || true

# Prune unused networks
sudo docker network prune -f 2>/dev/null || true

# Remove any networks associated with sports-data-service
# List all networks and filter by name pattern
sudo docker network ls --no-trunc | grep -E "(sports-data-service|sportspuff)" | awk '{print $1}' | while read net_id; do
  if [ -n "$net_id" ] && [ "$net_id" != "NETWORK" ]; then
    echo "Removing Docker network: $net_id"
    sudo docker network rm "$net_id" 2>/dev/null || true
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

