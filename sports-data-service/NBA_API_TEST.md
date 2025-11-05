# NBA API Testing Guide

## Quick Test Commands

### Curl Test (Single Date)

```bash
curl -s --max-time 30 \
  -H "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36" \
  -H "Referer: https://www.nba.com/" \
  -H "Accept: application/json, text/plain, */*" \
  -H "Accept-Language: en-US,en;q=0.9" \
  -H "Origin: https://www.nba.com" \
  "https://stats.nba.com/stats/scoreboardv2?GameDate=2025-11-04&LeagueID=00&DayOffset=0" | \
  python3 -m json.tool
```

### Curl Test (Check Game Count)

```bash
curl -s --max-time 30 \
  -H "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36" \
  -H "Referer: https://www.nba.com/" \
  "https://stats.nba.com/stats/scoreboardv2?GameDate=2025-11-04&LeagueID=00&DayOffset=0" | \
  python3 -c "import sys, json; data=json.load(sys.stdin); games=data.get('resultSets',[{}])[0].get('rowSet',[]); print(f'Games found: {len(games)}')"
```

## Python Test Script

### Required Modules

```python
import requests  # pip install requests
import json
from datetime import date, timedelta
```

### Quick Test

```python
import requests

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Referer': 'https://www.nba.com/',
    'Accept': 'application/json, text/plain, */*'
}

url = "https://stats.nba.com/stats/scoreboardv2?GameDate=2025-11-04&LeagueID=00&DayOffset=0"

response = requests.get(url, headers=headers, timeout=30)
data = response.json()
games = data.get('resultSets', [{}])[0].get('rowSet', [])
print(f"Found {len(games)} games")
```

## Using the Test Scripts

### Shell Script

```bash
# Test from today
./test_nba_api.sh

# Or test specific date
curl -s --max-time 30 \
  -H "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36" \
  -H "Referer: https://www.nba.com/" \
  "https://stats.nba.com/stats/scoreboardv2?GameDate=YYYY-MM-DD&LeagueID=00&DayOffset=0"
```

### Python Script

```bash
# Test today
python3 test_nba_api.py

# Test specific date
python3 test_nba_api.py 2025-11-04

# Test multiple dates (today + 7 days ahead)
python3 test_nba_api.py --multiple 7
```

## Expected Response Format

The NBA API returns JSON with this structure:

```json
{
  "resource": "scoreboard",
  "parameters": {...},
  "resultSets": [
    {
      "name": "GameHeader",
      "headers": ["GAME_ID", "GAME_DATE_EST", ...],
      "rowSet": [
        [GAME_ID, GAME_DATE_EST, GAME_SEQUENCE, GAME_STATUS_ID, GAME_STATUS_TEXT, GAME_STATUS, HOME_TEAM_ID, VISITOR_TEAM_ID, ...],
        ...
      ]
    },
    {
      "name": "LineScore",
      ...
    }
  ]
}
```

## Troubleshooting

### If requests timeout:
- Check network connectivity to `stats.nba.com`
- Verify firewall/proxy settings
- Try from a different network location

### If you get 403/404 errors:
- Ensure User-Agent and Referer headers are set
- Check if the date format is correct (YYYY-MM-DD)
- Verify the endpoint URL is correct

### If you get empty results:
- The date might not have games scheduled
- Try a different date (e.g., today or tomorrow)

## API Endpoint Details

**ScoreboardV2 Endpoint:**
- URL: `https://stats.nba.com/stats/scoreboardv2`
- Parameters:
  - `GameDate`: Date in YYYY-MM-DD format
  - `LeagueID`: "00" for NBA
  - `DayOffset`: Usually "0"

**Required Headers:**
- `User-Agent`: Browser user agent string
- `Referer`: `https://www.nba.com/`
- `Accept`: `application/json, text/plain, */*`

