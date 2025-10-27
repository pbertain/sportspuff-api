FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy NBA and MLB API dependencies from parent project
COPY ../dependencies/nba_api /app/dependencies/nba_api
COPY ../dependencies/mlb-statsapi /app/dependencies/mlb-statsapi

# Copy application code
COPY src/ ./src/
COPY scripts/ ./scripts/
COPY alembic.ini ./
COPY alembic/ ./alembic/

# Set Python path to include dependencies
ENV PYTHONPATH="/app:/app/dependencies/nba_api/src:/app/dependencies/mlb-statsapi"

# Create non-root user
RUN useradd -m -u 1000 sports && chown -R sports:sports /app
USER sports

# Expose port for health checks
EXPOSE 8000

# Expose API port (default, override via environment)
EXPOSE 34180

# Default command - run API server
# Port can be overridden via API_PORT environment variable
CMD ["uvicorn", "src.api:app", "--host", "0.0.0.0", "--port", "${API_PORT:-34180}"]
