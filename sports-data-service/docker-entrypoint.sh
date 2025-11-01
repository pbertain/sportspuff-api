#!/bin/bash
set -e

# Use API_PORT from environment, default to 34180
# Always bind to 0.0.0.0 inside container so Docker port mapping works
# Access restriction is handled by docker-compose port mapping
PORT=${API_PORT:-34180}

exec uvicorn src.api:app --host 0.0.0.0 --port "$PORT"

