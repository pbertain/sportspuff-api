#!/bin/bash
set -e

# Get API_PORT from environment, default to 34180
PORT=${API_PORT:-34180}

# Use Python to check health endpoint
python -c "import urllib.request; urllib.request.urlopen(f'http://localhost:{PORT}/health').read()" || exit 1

