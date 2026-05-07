# Environment Isolation Guide

## Problem
When fixing dev, prod breaks and vice versa. This document explains why and how to prevent it.

## Root Causes

### 1. Database Password Mismatch
**Issue**: The PostgreSQL password in the database doesn't match the password in `.env` files.

**Why it happens**:
- Password contains special characters (`/` and `=`) that need URL encoding
- Database password gets reset during container recreation
- `.env` file password doesn't match what's actually in the database

**Solution**:
- Always ensure database password matches `.env` file
- Use this command to sync passwords:
  ```bash
  cd /opt/sportspuff-api/sports-data-service  # or /opt/sportspuff-api-dev
  export COMPOSE_PROJECT_NAME=sports-data-prod  # or sports-data-dev
  export POSTGRES_PASSWORD=$(cat .env | grep POSTGRES_PASSWORD | cut -d= -f2)
  sudo -E docker-compose exec -T postgres bash -c "psql -U sports_user -d sports_data -c \"ALTER USER sports_user WITH PASSWORD '\$POSTGRES_PASSWORD';\""
  ```

### 2. Container Naming Conflicts
**Issue**: Containers from different environments might conflict if not properly isolated.

**Current Setup** (GOOD):
- Dev: `sports-data-dev-postgres`, `sports-data-dev-service`
- Prod: `sports-data-prod-postgres`, `sports-data-prod-service`
- Different networks: `sports-data-dev_default`, `sports-data-prod_default`
- Different volumes: `sports-data-dev_postgres_data_dev`, `sports-data-prod_postgres_data_prod`

**Verification**:
```bash
# Check containers are properly isolated
sudo docker ps | grep sports-data
sudo docker network ls | grep sports-data
sudo docker volume ls | grep sports-data
```

### 3. Port Conflicts
**Issue**: Both environments trying to use the same port.

**Current Setup** (GOOD):
- Dev: Port 34181
- Prod: Port 34180
- Verified with: `sudo ss -tlnp | grep -E '34180|34181'`

### 4. Shared Resources
**Issue**: Environments sharing Docker volumes or networks.

**Current Status**: ✅ Properly isolated
- Separate volumes per environment
- Separate networks per environment
- Separate container names per environment

## Best Practices

### When Fixing Dev:
1. **Never touch prod files**: `/opt/sportspuff-api/sports-data-service` (prod) vs `/opt/sportspuff-api-dev/sports-data-service` (dev)
2. **Use environment-specific COMPOSE_PROJECT_NAME**: 
   - Dev: `export COMPOSE_PROJECT_NAME=sports-data-dev`
   - Prod: `export COMPOSE_PROJECT_NAME=sports-data-prod`
3. **Verify isolation before making changes**:
   ```bash
   # Check both environments are running
   curl http://localhost:34181/health  # dev
   curl http://localhost:34180/health  # prod
   ```
4. **After fixing dev, verify prod still works**:
   ```bash
   curl http://localhost:34180/health
   curl http://localhost:34180/api/v1/schedule/nfl/today
   ```

### When Fixing Prod:
1. **Never touch dev files**: Work only in `/opt/sportspuff-api/sports-data-service`
2. **Use prod-specific COMPOSE_PROJECT_NAME**: `export COMPOSE_PROJECT_NAME=sports-data-prod`
3. **After fixing prod, verify dev still works**:
   ```bash
   curl http://localhost:34181/health
   curl http://localhost:34181/api/v1/schedule/nfl/today
   ```

### Database Password Sync
If you see password authentication errors, sync the password:

```bash
# For PROD
cd /opt/sportspuff-api/sports-data-service
export COMPOSE_PROJECT_NAME=sports-data-prod
export POSTGRES_PASSWORD=$(cat .env | grep POSTGRES_PASSWORD | cut -d= -f2)
sudo -E docker-compose exec -T postgres bash -c "psql -U sports_user -d sports_data -c \"ALTER USER sports_user WITH PASSWORD '\$POSTGRES_PASSWORD';\""
sudo -E docker-compose restart sports-service

# For DEV
cd /opt/sportspuff-api-dev/sports-data-service
export COMPOSE_PROJECT_NAME=sports-data-dev
export POSTGRES_PASSWORD=$(cat .env | grep POSTGRES_PASSWORD | cut -d= -f2)
sudo -E docker-compose exec -T postgres bash -c "psql -U sports_user -d sports_data -c \"ALTER USER sports_user WITH PASSWORD '\$POSTGRES_PASSWORD';\""
sudo -E docker-compose restart sports-service
```

## Quick Health Check Script

```bash
#!/bin/bash
echo "=== DEV ==="
curl -s http://localhost:34181/health && echo ""

echo "=== PROD ==="
curl -s http://localhost:34180/health && echo ""

echo "=== DEV NFL ==="
curl -s http://localhost:34181/api/v1/schedule/nfl/today | python3 -m json.tool | head -5

echo "=== PROD NFL ==="
curl -s http://localhost:34180/api/v1/schedule/nfl/today | python3 -m json.tool | head -5
```

## Current Status

✅ **Both environments are properly isolated**
- Separate directories
- Separate containers
- Separate networks
- Separate volumes
- Different ports

✅ **Both environments are healthy**
- Dev: http://localhost:34181/health
- Prod: http://localhost:34180/health

## Troubleshooting

If one environment breaks after fixing the other:

1. **Check database password**:
   ```bash
   # Verify password in .env matches database
   cd /opt/sportspuff-api/sports-data-service  # or -dev
   export POSTGRES_PASSWORD=$(cat .env | grep POSTGRES_PASSWORD | cut -d= -f2)
   export COMPOSE_PROJECT_NAME=sports-data-prod  # or -dev
   sudo -E docker-compose exec -T postgres bash -c "psql -U sports_user -d sports_data -c \"SELECT current_user;\""
   ```

2. **Check container status**:
   ```bash
   sudo docker ps -a | grep sports-data
   ```

3. **Check logs**:
   ```bash
   cd /opt/sportspuff-api/sports-data-service  # or -dev
   export COMPOSE_PROJECT_NAME=sports-data-prod  # or -dev
   sudo -E docker-compose logs sports-service --tail 50
   ```

4. **Restart if needed**:
   ```bash
   cd /opt/sportspuff-api/sports-data-service  # or -dev
   export COMPOSE_PROJECT_NAME=sports-data-prod  # or -dev
   export POSTGRES_PASSWORD=$(cat .env | grep POSTGRES_PASSWORD | cut -d= -f2)
   sudo -E docker-compose down
   sudo -E docker-compose up -d
   ```

