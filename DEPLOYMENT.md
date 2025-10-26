# Deployment Guide

## Port Configuration

The service uses customizable ports to coexist with the main Sportspuff application:

- **Production API**: Port `34180`
- **Development API**: Port `34181`
- **Production Site**: Port `34080`
- **Development Site**: Port `34081`

## Environment Configuration

### Production Deployment

1. **Copy environment file**:
   ```bash
   cp env.example .env
   ```

2. **Edit `.env` file** and set:
   ```bash
   API_PORT=34180
   DATABASE_URL=postgresql://sports_user:your_password@postgres:5432/sports_data
   ```

3. **Start services**:
   ```bash
   docker-compose up -d
   ```

4. **Initialize database**:
   ```bash
   docker-compose exec sports-service python scripts/update_schedules.py
   ```

5. **Start live polling**:
   ```bash
   docker-compose exec sports-service python scripts/poll_live_scores.py --once
   ```

### Development Deployment

1. **Copy environment file**:
   ```bash
   cp env.example .env
   ```

2. **Edit `.env` file** and set:
   ```bash
   API_PORT=34181
   DATABASE_URL=postgresql://sports_user:your_password@postgres:5432/sports_data
   LOG_LEVEL=DEBUG
   ```

3. **Start services**:
   ```bash
   API_PORT=34181 docker-compose up
   ```

## Testing the API

### Production API (Port 34180)

```bash
# Health check
curl http://localhost:34180/health

# Get today's NBA schedule (JSON)
curl http://localhost:34180/api/v1/schedule/nba/today

# Get today's NBA schedule (cURL-style)
curl http://localhost:34180/curl/v1/schedule/nba/today

# Get today's MLB scores (JSON)
curl http://localhost:34180/api/v1/scores/mlb/today

# Get today's MLB scores (cURL-style)
curl http://localhost:34180/curl/v1/scores/mlb/today
```

### Development API (Port 34181)

```bash
# Health check
curl http://localhost:34181/health

# Get schedule (development)
curl http://localhost:34181/api/v1/schedule/nba/today
```

## Systemd Service Setup

### Install Services

```bash
# Copy service files
sudo cp systemd/sports-schedule-update.service /etc/systemd/system/
sudo cp systemd/sports-live-poller.service /etc/systemd/system/
sudo cp systemd/sports-schedule-update.timer /etc/systemd/system/

# Reload systemd
sudo systemctl daemon-reload
```

### Enable and Start

```bash
# Enable schedule updates (runs twice daily)
sudo systemctl enable sports-schedule-update.timer
sudo systemctl start sports-schedule-update.timer

# Enable live score polling (runs during game hours)
sudo systemctl enable sports-live-poller.service
sudo systemctl start sports-live-poller.service
```

### Check Status

```bash
# Check timer status
sudo systemctl status sports-schedule-update.timer

# Check service status
sudo systemctl status sports-live-poller.service

# View logs
sudo journalctl -u sports-schedule-update.service -f
sudo journalctl -u sports-live-poller.service -f
```

## Docker Compose Port Override

### Production
```bash
API_PORT=34180 docker-compose up -d
```

### Development
```bash
API_PORT=34181 docker-compose up -d
```

## Nginx Configuration (Optional)

If you want to route traffic through Nginx:

```nginx
# Production API
location /api/ {
    proxy_pass http://localhost:34180;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
}

# Development API
location /api-dev/ {
    proxy_pass http://localhost:34181;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
}
```

## Port Summary

| Service | Port | Usage |
|---------|------|-------|
| Production Site | 34080 | Main Sportspuff web interface |
| Development Site | 34081 | Dev Sportspuff web interface |
| Production API | 34180 | Sports Data Service API |
| Development API | 34181 | Sports Data Service API (dev) |

## Verification

After deployment, verify all services:

```bash
# Check production API
curl http://localhost:34180/health

# Check development API  
curl http://localhost:34181/health

# Test schedule endpoint
curl http://localhost:34180/api/v1/schedule/nba/today

# Test cURL-style endpoint
curl http://localhost:34180/curl/v1/schedule/nba/today

# View API documentation
open http://localhost:34180/docs
```
