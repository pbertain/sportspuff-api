# Sports Data Service API Setup

## API Endpoints Summary

The service provides two types of endpoints for each resource:

### JSON API Endpoints (`/api/...`)
- Returns structured JSON data
- Swagger documentation at `/docs`

### cURL-style Endpoints (`/curl/...`)
- Returns formatted text output like the original Sportspuff API
- Designed for terminal/cURL usage

## Available Endpoints

### Schedule Endpoints (v1)

**GET** `/api/v1/schedule/{sport}/{date}`
- Returns JSON schedule data
- Example: `/api/v1/schedule/nba/today`
- Returns: `{"sport": "nba", "date": "2024-12-19", "games": [...])}`

**GET** `/curl/v1/schedule/{sport}/{date}`  
- Returns cURL-style text schedule
- Example: `/curl/v1/schedule/nba/today`
- Returns formatted text like your example

### Score Endpoints (v1)

**GET** `/api/v1/scores/{sport}/{date}`
- Returns JSON score data
- Example: `/api/v1/scores/mlb/today`
- Returns: `{"sport": "mlb", "date": "2024-12-19", "scores": [...]}`

**GET** `/curl/v1/scores/{sport}/{date}`
- Returns cURL-style text scores
- Example: `/curl/v1/scores/mlb/today`
- Returns formatted text with scores

### Standings Endpoints (v1 - TODO)

**GET** `/api/v1/standings/{sport}`
- Returns JSON standings data
- No date parameter needed
- Status: Not yet implemented

**GET** `/curl/v1/standings/{sport}`
- Returns cURL-style text standings
- Status: Not yet implemented

## Date Parameter Formats

The `{date}` parameter accepts:
- `today` - Today's date
- `tomorrow` - Tomorrow's date
- `yesterday` - Yesterday's date
- `YYYYMMDD` - Specific date (e.g., `20241219`)

## Sport Parameters

Accepted sport values:
- `nba` - NBA
- `mlb` - MLB
- `nfl` - NFL
- `nhl` - NHL
- `wnba` - WNBA

## Example Usage

### cURL Examples (Production - Port 34180)

```bash
# Get today's NBA schedule in cURL format
curl http://localhost:34180/curl/v1/schedule/nba/today

# Get yesterday's MLB scores in cURL format
curl http://localhost:34180/curl/v1/scores/mlb/yesterday

# Get tomorrow's NFL schedule
curl http://localhost:34180/curl/v1/schedule/nfl/tomorrow

# Get specific date (December 19, 2024)
curl http://localhost:34180/curl/v1/schedule/mlb/20241219
```

### JSON API Examples (Production - Port 34180)

```bash
# Get today's NBA schedule as JSON
curl http://localhost:34180/api/v1/schedule/nba/today | jq

# Get today's MLB scores as JSON
curl http://localhost:34180/api/v1/scores/mlb/today | jq

# Get tomorrow's NHL schedule
curl http://localhost:34180/api/v1/schedule/nhl/tomorrow | jq
```

### Development Environment (Port 34181)

```bash
# Development schedule
curl http://localhost:34181/curl/v1/schedule/nba/today

# Development scores
curl http://localhost:34181/api/v1/scores/mlb/today
```

## Output Format

### cURL Output Format

The cURL-style output follows your exact specification:

```
Good evening!
Here are today's sports scores
       Thu 18 Sep 2025:
------------------------------
MLB - Reg Season, Week 26:
 SF (h) [ 4- 0] SEA(v) F
 CHC(h) [ 6- 1] STL(v) F/10
 PHI(h) [ 2- 1] ARI(v) F
------------------------------
     All times in Pacific
  Sent from SportsPuff@21:46
------------------------------
```

Features:
- Time-based greeting (morning, afternoon, evening, god it's late)
- Date header
- Grouped by sport
- Period scores for active games
- Final scores marked with "F"
- Pacific time zone
- Timestamp footer

### JSON Output Format

```json
{
  "sport": "nba",
  "date": "2024-12-19",
  "games": [
    {
      "game_id": "0022401201",
      "game_date": "2024-12-19",
      "game_time": "2024-12-19T19:30:00Z",
      "home_team": "New York Knicks",
      "home_team_abbrev": "NYK",
      "visitor_team": "Miami Heat",
      "visitor_team_abbrev": "MIA",
      "game_status": "in_progress",
      "game_type": "regular"
    }
  ]
}
```

## Time-Based Greetings

The API automatically selects greetings based on the current time:

- **0500-1200**: "Good morning"
- **1200-1700**: "Good afternoon"  
- **1700-0000**: "Good evening"
- **0000-0500**: "God it's late"

## Database Requirements

The API queries the PostgreSQL database created by the data collection service. Ensure:

1. Database is running (via `docker-compose up`)
2. Schedules have been populated (via `update_schedules.py`)
3. Live scores are being updated (via `poll_live_scores.py`)

## Swagger Documentation

Access interactive API documentation at:
- `/docs` - Swagger UI
- `/redoc` - ReDoc
- `/openapi.json` - OpenAPI schema

## Next Steps

1. ✅ JSON API endpoints implemented
2. ✅ cURL-style text output implemented
3. ✅ Date parameter parsing (today/tomorrow/yesterday/YYYYMMDD)
4. ⏳ Standings endpoints (TODO - needs standings data collection)
5. ⏳ Week number calculation
6. ⏳ Enhanced game status formatting
