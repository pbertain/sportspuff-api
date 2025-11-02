#!/bin/bash
set -e

# First, kill anything using the port directly (most reliable)
# Check with ss first (most common)
if command -v ss >/dev/null 2>&1; then
  # Find what's listening on the port
  LISTENER=$(sudo ss -tlnp | grep ":${API_PORT} " | head -n1 || true)
  if [ -n "$LISTENER" ]; then
    echo "Found listener on port ${API_PORT}: $LISTENER"
    # Extract PID from ss output (format: users:(("process",pid,fd)) or pid=123)
    # Try multiple extraction methods for compatibility
    PID=$(echo "$LISTENER" | sed -n 's/.*pid=\([0-9]*\).*/\1/p' | head -n1 || true)
    if [ -z "$PID" ]; then
      # Alternative: extract from users field
      PID=$(echo "$LISTENER" | awk -F'[=,)]' '{for(i=1;i<=NF;i++) if($i ~ /^[0-9]+$/) {print $i; exit}}' || true)
    fi
    if [ -n "$PID" ]; then
      echo "Killing process $PID using port ${API_PORT}"
      sudo kill -9 "$PID" 2>/dev/null || true
      sleep 1
    else
      # If we can't extract PID, use fuser to kill directly
      echo "Could not extract PID, using fuser to kill process on port ${API_PORT}"
      sudo fuser -k ${API_PORT}/tcp 2>/dev/null || true
      sleep 1
    fi
  fi
fi

# Also check with lsof if available
if command -v lsof >/dev/null 2>&1; then
  PID=$(sudo lsof -ti:${API_PORT} 2>/dev/null || true)
  if [ -n "$PID" ]; then
    echo "Found process $PID using port ${API_PORT} (via lsof), killing it..."
    sudo kill -9 "$PID" 2>/dev/null || true
    sleep 1
  fi
fi

# Now stop all Docker containers that might be using the port
# Stop all sports-data containers from any deployment
# Use -q flag to get IDs only (avoids Jinja2 template conflicts)
sudo docker ps -aq --filter "name=sports-data" | while read container_id; do
  if [ -n "$container_id" ]; then
    echo "Stopping and removing container $container_id"
    sudo docker stop "$container_id" 2>/dev/null || true
    sudo docker rm -f "$container_id" 2>/dev/null || true
  fi
done || true

# Try to stop containers from this specific deployment
cd "${DEPLOYMENT_DIR}/sports-data-service"
sudo docker-compose down --remove-orphans 2>/dev/null || true

# Force remove containers by name (case where docker-compose didn't clean up)
sudo docker rm -f sports-data-postgres sports-data-service 2>/dev/null || true

# Double-check port is free
sleep 2
if command -v ss >/dev/null 2>&1; then
  STILL_LISTENING=$(sudo ss -tlnp | grep ":${API_PORT} " || true)
  if [ -n "$STILL_LISTENING" ]; then
    echo "WARNING: Port ${API_PORT} still in use: $STILL_LISTENING"
    echo "Attempting to kill any remaining processes..."
    sudo fuser -k ${API_PORT}/tcp 2>/dev/null || true
    sleep 2
  fi
fi

