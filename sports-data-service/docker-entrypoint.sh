#!/bin/bash
set -e

# Use API_HOST and API_PORT from environment, defaults
HOST=${API_HOST:-127.0.0.1}
PORT=${API_PORT:-34180}

exec uvicorn src.api:app --host "$HOST" --port "$PORT"

