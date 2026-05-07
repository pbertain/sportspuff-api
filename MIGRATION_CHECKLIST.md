# Migration Checklist: Docker → Native Processes

## Pre-Migration

- [ ] Backup all Docker data
- [ ] Document current Docker setup
- [ ] Verify server has enough resources
- [ ] Plan maintenance window (if needed)

## Phase 1: PostgreSQL Setup

- [ ] Install PostgreSQL (if needed)
- [ ] Create `sports_data_dev` database
- [ ] Create `sports_data_prod` database
- [ ] Create `sports_user` with proper permissions
- [ ] Export data from Docker Postgres (dev)
- [ ] Export data from Docker Postgres (prod)
- [ ] Import data to native Postgres (dev)
- [ ] Import data to native Postgres (prod)
- [ ] Verify data integrity
- [ ] Test connections from both environments

## Phase 2: API Services Setup

- [ ] Create Python virtualenv for dev
- [ ] Create Python virtualenv for prod
- [ ] Install dependencies in dev venv
- [ ] Install dependencies in prod venv
- [ ] Create systemd service files
- [ ] Test service files (dry-run)
- [ ] Verify environment variables

## Phase 3: Testing (Parallel Run)

- [ ] Start native services on test ports (e.g., 34182, 34183)
- [ ] Keep Docker running on production ports
- [ ] Test dev API endpoints (native)
- [ ] Test prod API endpoints (native)
- [ ] Compare responses (Docker vs Native)
- [ ] Test database operations
- [ ] Test schedule updates
- [ ] Test live polling
- [ ] Monitor resource usage
- [ ] Check logs for errors

## Phase 4: Cutover

- [ ] Stop Docker dev service
- [ ] Start native dev service on port 34181
- [ ] Verify dev is working
- [ ] Stop Docker prod service
- [ ] Start native prod service on port 34180
- [ ] Verify prod is working
- [ ] Test all endpoints
- [ ] Monitor for 30 minutes
- [ ] Check NGINX (if needed)

## Phase 5: Cleanup

- [ ] Stop Docker containers
- [ ] Remove Docker containers (optional - keep as backup)
- [ ] Remove Docker volumes (after confirming data is safe)
- [ ] Update deployment documentation
- [ ] Update monitoring/alerting
- [ ] Document new deployment process

## Rollback Plan

If issues occur:
1. Stop native services
2. Start Docker containers
3. Verify services are working
4. Investigate issues
5. Fix and retry migration

## Post-Migration

- [ ] Update deployment scripts
- [ ] Update CI/CD (GitHub Actions)
- [ ] Update documentation
- [ ] Train team on new process
- [ ] Monitor for 24-48 hours
- [ ] Remove Docker images (optional, after confidence period)

