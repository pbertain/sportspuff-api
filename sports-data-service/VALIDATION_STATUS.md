# Sports Data Service Validation Status

**Date:** November 4, 2025  
**Status:** ⚠️ Service Not Running

## Current Status

The Sports Data Service API is **not currently running**. The service needs to be started via Docker Compose.

### About Auto-Start

**Important:** The existing systemd services (`sports-live-poller.service` and `sports-schedule-update.service`) are **only for running the data collection scripts** (updating schedules and polling live scores). They do **NOT** start the API service itself.

The API service is configured with `restart: unless-stopped` in docker-compose.yml, which means:
- ✅ Containers will auto-restart if Docker Desktop restarts (if they were running)
- ❌ Containers won't start if Docker Desktop wasn't running
- ❌ On macOS, there's no systemd service to start docker-compose (systemd is Linux-only)

**For macOS auto-start:** I've created a launchd plist file (`com.sportspuff.api.plist`) that you can install to auto-start the service when your Mac boots.

## Quick Start Instructions

### 1. Start Docker Desktop
   - Open Docker Desktop application on your Mac
   - Wait for Docker to fully start (whale icon in menu bar should be steady)

### 2. Start the Service
   ```bash
   cd /Users/paulb/Documents/version-control/git/sportspuff-api/sports-data-service
   docker-compose up -d
   ```

### 3. Initialize Database (if first time)
   ```bash
   docker-compose exec sports-service python scripts/update_schedules.py
   ```

### 4. Validate the Service
   ```bash
   ./validate_service.sh
   ```

## API Endpoints for sportspuff-v6

Once the service is running, you can access these endpoints:

### Base URL
- **Production:** `http://localhost:34180`
- **Development:** `http://localhost:34181`

### Schedule Endpoints (JSON)

**Individual Sport Schedules:**
```bash
# NBA schedule for today
curl http://localhost:34180/api/v1/schedule/nba/today

# NHL schedule for today  
curl http://localhost:34180/api/v1/schedule/nhl/today

# NFL schedule for today
curl http://localhost:34180/api/v1/schedule/nfl/today
```

**All Sports Schedule:**
```bash
# All sports schedules for today
curl http://localhost:34180/api/v1/schedules/today
```

### Response Format

The schedule endpoint returns JSON in this format:
```json
{
  "sport": "nba",
  "date": "2025-11-04",
  "games": [
    {
      "game_id": "12345678",
      "game_date": "2025-11-04",
      "game_time": "2025-11-04T19:00:00-08:00",
      "home_team": "Los Angeles Lakers",
      "home_team_abbrev": "LAL",
      "visitor_team": "Golden State Warriors",
      "visitor_team_abbrev": "GSW",
      "game_status": "scheduled",
      "game_type": "regular_season"
    }
  ]
}
```

### Health Check
```bash
curl http://localhost:34180/health
```

Expected response: `{"status": "healthy"}`

## Testing for sportspuff-v6 Integration

### 1. Test NBA Schedule
```bash
curl -s http://localhost:34180/api/v1/schedule/nba/today | python3 -m json.tool
```

### 2. Test NHL Schedule
```bash
curl -s http://localhost:34180/api/v1/schedule/nhl/today | python3 -m json.tool
```

### 3. Test NFL Schedule
```bash
curl -s http://localhost:34180/api/v1/schedule/nfl/today | python3 -m json.tool
```

### 4. Test Combined Endpoint
```bash
curl -s http://localhost:34180/api/v1/schedules/today | python3 -m json.tool
```

## Integration Notes for sportspuff-v6

Since sportspuff-v6 runs on the same host, you can:

1. **Use localhost URLs** - The API is accessible at `http://localhost:34180`
2. **CORS** - If you need to access from a browser, you may need to add CORS headers (currently not configured)
3. **Date Parameter** - Accepts `today`, `tomorrow`, `yesterday`, or `YYYYMMDD` format
4. **Error Handling** - Check for empty `games` arrays when no games are scheduled

## Auto-Start on macOS (Optional)

To automatically start the service when your Mac boots:

1. Install the launchd service:
   ```bash
   cp com.sportspuff.api.plist ~/Library/LaunchAgents/
   launchctl load ~/Library/LaunchAgents/com.sportspuff.api.plist
   ```

2. To uninstall later:
   ```bash
   launchctl unload ~/Library/LaunchAgents/com.sportspuff.api.plist
   rm ~/Library/LaunchAgents/com.sportspuff.api.plist
   ```

**Note:** This requires Docker Desktop to be set to start automatically (Docker Desktop → Settings → General → "Start Docker Desktop when you log in")

## Next Steps

1. ✅ Start Docker Desktop
2. ✅ Start the service: `docker-compose up -d`
3. ✅ Run validation: `./validate_service.sh`
4. ✅ Test endpoints in sportspuff-v6 app
5. ✅ Verify schedules for NBA, NHL, and NFL are populated
6. (Optional) Install launchd service for auto-start

## Troubleshooting

### Service won't start
- Check Docker Desktop is running: `docker info`
- Check logs: `docker-compose logs sports-service`
- Check database: `docker-compose logs postgres`

### No games in schedule
- Database may need initialization: `docker-compose exec sports-service python scripts/update_schedules.py`
- Check if schedules are being fetched: `docker-compose exec sports-service python scripts/update_schedules.py --stats`

### Port already in use
- Check what's using port 34180: `lsof -i :34180`
- Change port in `.env` file: `API_PORT=34181`

