# Quick Migration Guide: Docker → Native Processes

## Current Status
✅ PostgreSQL 16 is already installed and running
✅ Python 3.12.3 is available
✅ Server has adequate resources (85GB free, 3.8GB RAM)

## Migration Steps

### Step 1: Create Native PostgreSQL Databases

```bash
# SSH to server
ssh ansible@host74.nird.club

# Get password from Docker env
cd /opt/sportspuff-api/sports-data-service
POSTGRES_PASSWORD=$(cat .env | grep POSTGRES_PASSWORD | cut -d= -f2)

# Create databases
sudo -u postgres psql << EOF
CREATE DATABASE sports_data_dev;
CREATE DATABASE sports_data_prod;
CREATE USER sports_user WITH PASSWORD '$POSTGRES_PASSWORD';
GRANT ALL PRIVILEGES ON DATABASE sports_data_dev TO sports_user;
GRANT ALL PRIVILEGES ON DATABASE sports_data_prod TO sports_user;
\c sports_data_dev
GRANT ALL ON SCHEMA public TO sports_user;
\c sports_data_prod
GRANT ALL ON SCHEMA public TO sports_user;
EOF
```

### Step 2: Export Data from Docker

```bash
# Export dev data
cd /opt/sportspuff-api-dev/sports-data-service
export COMPOSE_PROJECT_NAME=sports-data-dev
docker-compose exec -T postgres pg_dump -U sports_user sports_data > /tmp/sports_data_dev_backup.sql

# Export prod data
cd /opt/sportspuff-api/sports-data-service
export COMPOSE_PROJECT_NAME=sports-data-prod
docker-compose exec -T postgres pg_dump -U sports_user sports_data > /tmp/sports_data_prod_backup.sql
```

### Step 3: Import Data to Native PostgreSQL

```bash
# Import dev
PGPASSWORD="$POSTGRES_PASSWORD" psql -U sports_user -d sports_data_dev -h localhost < /tmp/sports_data_dev_backup.sql

# Import prod
PGPASSWORD="$POSTGRES_PASSWORD" psql -U sports_user -d sports_data_prod -h localhost < /tmp/sports_data_prod_backup.sql
```

### Step 4: Create Python Virtualenvs

```bash
# Dev
cd /opt/sportspuff-api-dev/sports-data-service
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Prod
cd /opt/sportspuff-api/sports-data-service
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Step 5: Create Systemd Service Files

**`/etc/systemd/system/sports-api-dev.service`:**
```ini
[Unit]
Description=SportsPuff API - Development
After=network.target postgresql.service
Requires=postgresql.service

[Service]
Type=simple
User=ansible
Group=ansible
WorkingDirectory=/opt/sportspuff-api-dev/sports-data-service
Environment="PATH=/opt/sportspuff-api-dev/sports-data-service/venv/bin:/usr/local/bin:/usr/bin:/bin"
Environment="DATABASE_URL=postgresql://sports_user:PASSWORD@localhost:5432/sports_data_dev"
Environment="API_PORT=34181"
Environment="DEPLOYMENT_ENV=dev"
ExecStart=/opt/sportspuff-api-dev/sports-data-service/venv/bin/uvicorn src.api:app --host 0.0.0.0 --port 34181
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

**`/etc/systemd/system/sports-api-prod.service`:**
```ini
[Unit]
Description=SportsPuff API - Production
After=network.target postgresql.service
Requires=postgresql.service

[Service]
Type=simple
User=ansible
Group=ansible
WorkingDirectory=/opt/sportspuff-api/sports-data-service
Environment="PATH=/opt/sportspuff-api/sports-data-service/venv/bin:/usr/local/bin:/usr/bin:/bin"
Environment="DATABASE_URL=postgresql://sports_user:PASSWORD@localhost:5432/sports_data_prod"
Environment="API_PORT=34180"
Environment="DEPLOYMENT_ENV=prod"
ExecStart=/opt/sportspuff-api/sports-data-service/venv/bin/uvicorn src.api:app --host 0.0.0.0 --port 34180
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

**Replace `PASSWORD` with actual password in both files!**

### Step 6: Enable and Start Services

```bash
# Reload systemd
sudo systemctl daemon-reload

# Enable services
sudo systemctl enable sports-api-dev
sudo systemctl enable sports-api-prod

# Start services
sudo systemctl start sports-api-dev
sudo systemctl start sports-api-prod

# Check status
sudo systemctl status sports-api-dev
sudo systemctl status sports-api-prod
```

### Step 7: Test and Verify

```bash
# Check health
curl http://localhost:34181/health  # dev
curl http://localhost:34180/health  # prod

# Test endpoints
curl http://localhost:34181/api/v1/schedule/nfl/today
curl http://localhost:34180/api/v1/schedule/nfl/today
```

### Step 8: Stop Docker (After Verification)

```bash
# Stop Docker services
cd /opt/sportspuff-api-dev/sports-data-service
docker-compose down

cd /opt/sportspuff-api/sports-data-service
docker-compose down
```

## Benefits After Migration

- **Faster deployments**: `systemctl restart sports-api-prod` (seconds vs minutes)
- **Lower overhead**: No Docker daemon (~200MB+ RAM saved)
- **Simpler debugging**: `journalctl -u sports-api-prod -f`
- **Direct code access**: Code on server = running code
- **Better resource usage**: More CPU/memory for actual work

## Rollback (If Needed)

```bash
# Stop native services
sudo systemctl stop sports-api-dev sports-api-prod

# Start Docker
cd /opt/sportspuff-api-dev/sports-data-service
docker-compose up -d

cd /opt/sportspuff-api/sports-data-service
docker-compose up -d
```

