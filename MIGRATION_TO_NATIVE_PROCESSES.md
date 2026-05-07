# Migration Plan: Docker → Native Processes

## Overview
Moving from Docker containers to native systemd services for both API and PostgreSQL.

## Benefits
- **Lower overhead**: No Docker daemon, no container overhead
- **Faster deployments**: Just restart service, no image rebuilds
- **Simpler debugging**: Direct process access, standard systemd logs
- **Better resource usage**: More CPU/memory available for actual work
- **Easier maintenance**: Standard Linux service management

## Architecture

### PostgreSQL
- Single PostgreSQL instance (native install)
- Two databases: `sports_data_dev` and `sports_data_prod`
- Same user/password setup, just different databases
- Standard PostgreSQL service: `postgresql.service`

### API Services
- Two systemd services:
  - `sports-api-dev.service` (port 34181)
  - `sports-api-prod.service` (port 34180)
- Two Python virtualenvs:
  - `/opt/sportspuff-api-dev/venv`
  - `/opt/sportspuff-api/venv`
- Code directories remain the same:
  - `/opt/sportspuff-api-dev/sports-data-service`
  - `/opt/sportspuff-api/sports-data-service`

## Migration Steps

### Phase 1: Setup Native PostgreSQL
1. Install PostgreSQL (if not already installed)
2. Create databases and users
3. Export data from Docker Postgres
4. Import to native Postgres
5. Test connections

### Phase 2: Setup Native API Services
1. Create Python virtualenvs
2. Install dependencies
3. Create systemd service files
4. Test services (but don't enable yet)

### Phase 3: Parallel Run
1. Keep Docker running
2. Start native services on different ports (temporarily)
3. Test thoroughly
4. Compare outputs

### Phase 4: Cutover
1. Stop Docker services
2. Start native services on production ports
3. Update NGINX (if needed)
4. Monitor closely

### Phase 5: Cleanup
1. Remove Docker containers (keep images as backup)
2. Remove Docker volumes (after confirming data is safe)
3. Document new deployment process

## Rollback Plan
- Docker containers can be restarted if needed
- Data is backed up before migration
- Can run both in parallel during transition

