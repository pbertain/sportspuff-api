#!/bin/bash
set -e

# Use API_PORT from environment, default to 34180
PORT=${API_PORT:-34180}

exec uvicorn src.api:app --host 0.0.0.0 --port "$PORT"

