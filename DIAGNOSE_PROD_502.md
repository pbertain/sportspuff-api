# Diagnosing Prod 502 Error

## Quick Check Commands

SSH to the server and run these commands:

```bash
# Check if containers are running
cd /opt/sportspuff-api/sports-data-service
sudo docker-compose ps

# Check container status
sudo docker ps -a --filter "name=sports-data"

# Check if port 34180 is in use
sudo netstat -tlnp | grep 34180
# or
sudo lsof -i :34180

# Check container logs
sudo docker logs sports-data-service --tail 100

# Check if container exists but isn't running
sudo docker inspect sports-data-service | grep -A 10 "State"

# Try to start the container manually
cd /opt/sportspuff-api/sports-data-service
export POSTGRES_PASSWORD="<your-password>"
export API_PORT="34180"
sudo -E docker-compose up -d

# If that fails, try stopping and removing everything first
sudo docker-compose down --remove-orphans
sudo docker stop sports-data-postgres sports-data-service 2>/dev/null || true
sudo docker rm -f sports-data-postgres sports-data-service 2>/dev/null || true

# Then rebuild and start
sudo -E docker-compose build --no-cache
sudo -E docker-compose up -d

# Verify it's running
sudo docker-compose ps
curl http://localhost:34180/health
```

## Common Issues

1. **Port already in use**: Another process is using port 34180
   - Solution: Find and kill the process, or check if old container is still running

2. **Container crashed on startup**: Check logs for errors
   - Solution: Fix the error and restart

3. **Container not created**: Cleanup didn't work properly
   - Solution: Manually remove containers and redeploy

4. **Database connection issue**: Postgres container not running
   - Solution: Check postgres container status and logs

