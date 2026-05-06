"""
FastAPI application for the Sports Data Service API.

Provides REST API endpoints for schedules, scores, and standings
with both JSON and cURL-style text output.
"""

import sys
import os
from datetime import datetime, date, timedelta
from typing import Optional, Dict, Any, List
from fastapi import FastAPI, Path, Query, HTTPException
from fastapi.responses import JSONResponse, PlainTextResponse, HTMLResponse
from fastapi.openapi.utils import get_openapi
import pytz

sys.path.insert(0, '/app/src')

from database import get_db_session
from models import Game
from config import settings
from collectors import NBACollector, MLBCollector, NHLCollector, NFLCollector, WNBACollector, CricketCollector, MLSCollector

import time as _time

# Cache for collector responses: {cache_key: {'data': [...], 'timestamp': float}}
_collector_cache: Dict[str, Any] = {}
_COLLECTOR_CACHE_TTL = 300  # 5 minutes


def _get_cached_games(league: str, target_date, fetcher):
    """Fetch games with 5-minute caching."""
    cache_key = f"{league}:{target_date.isoformat()}"
    cached = _collector_cache.get(cache_key)
    if cached and (_time.time() - cached['timestamp'] < _COLLECTOR_CACHE_TTL):
        return cached['data']
    result = fetcher()
    _collector_cache[cache_key] = {'data': result, 'timestamp': _time.time()}
    return result


def get_collector(league: str):
    """Get collector instance for a league."""
    collectors = {
        'NBA': NBACollector(),
        'MLB': MLBCollector(),
        'NHL': NHLCollector(),
        'NFL': NFLCollector(),
        'WNBA': WNBACollector(),
        'IPL': CricketCollector('IPL'),
        'MLC': CricketCollector('MLC'),
        'MLS': MLSCollector(),
    }
    return collectors.get(league)

app = FastAPI(
    title="Sports Data Service API",
    description="API for accessing sports schedules, scores, and standings",
    version="1.0.0"
)

# Sport mappings
SPORT_MAPPINGS = {
    'nba': 'NBA',
    'mlb': 'MLB',
    'nfl': 'NFL',
    'nhl': 'NHL',
    'wnba': 'WNBA',
    'ipl': 'IPL',
    'mlc': 'MLC',
    'mls': 'MLS',
}

def get_help_json() -> Dict[str, Any]:
    """Generate JSON formatted help content."""
    return {
        "title": "Sports Data Service API Help",
        "version": "1.0.0",
        "endpoints": {
            "schedules": {
                "description": "Get game schedules",
                "json": [
                    "/api/v1/schedules/{date} - All sports schedules",
                    "/api/v1/schedule/{sport}/{date} - Single sport schedule"
                ],
                "curl": [
                    "/curl/v1/schedules/{date} - All sports schedules",
                    "/curl/v1/schedule/{sport}/{date} - Single sport schedule"
                ],
                "sports": ["nba", "mlb", "nfl", "nhl", "wnba", "ipl", "mlc", "all"],
                "date_formats": ["today", "tomorrow", "yesterday", "YYYY-MM-DD", "YYYYMMDD", "M/D/YYYY", "MM/DD/YYYY"],
                "note": "Use 'all' as sport to get schedules for all sports"
            },
            "scores": {
                "description": "Get game scores",
                "json": [
                    "/api/v1/scores/{date} - All sports scores",
                    "/api/v1/scores/{sport}/{date} - Single sport scores"
                ],
                "curl": [
                    "/curl/v1/scores/{date} - All sports scores",
                    "/curl/v1/scores/{sport}/{date} - Single sport scores"
                ],
                "sports": ["nba", "mlb", "nfl", "nhl", "wnba", "ipl", "mlc", "all"],
                "date_formats": ["today", "tomorrow", "yesterday", "YYYY-MM-DD", "YYYYMMDD", "M/D/YYYY", "MM/DD/YYYY"],
                "note": "Use 'all' as sport to get scores for all sports"
            },
            "standings": {
                "description": "Get team standings",
                "json": [
                    "/api/v1/standings/{sport} - Single sport standings"
                ],
                "curl": [
                    "/curl/v1/standings/{sport} - Single sport standings"
                ],
                "sports": ["nba", "mlb", "nfl", "nhl", "wnba"],
                "note": "Standings endpoint is currently under development"
            },
            "season_info": {
                "description": "Get season phase dates (preseason, regular season, playoffs, etc.)",
                "json": [
                    "/api/v1/season-info/{league} - Season dates for a league"
                ],
                "leagues": ["mlb", "nba", "nfl", "nhl", "wnba", "ipl", "mlc"],
                "note": "Returns year, current_phase, and season_types with start/end dates. Cached for 24 hours."
            }
        },
        "timezone": {
            "description": "Change timezone using the 'tz' query parameter",
            "usage": "?tz=<timezone>",
            "examples": [
                "?tz=et - Eastern Time",
                "?tz=pt - Pacific Time",
                "?tz=ct - Central Time",
                "?tz=mt - Mountain Time",
                "?tz=America/New_York - Full timezone name",
                "?tz=Europe/London - International timezone"
            ],
            "supported_aliases": [
                "et, est, edt, eastern - US/Eastern",
                "pt, pst, pdt, pacific - US/Pacific",
                "ct, cst, cdt, central - US/Central",
                "mt, mst, mdt, mountain - US/Mountain",
                "akst, akdt, alaska, ak - US/Alaska",
                "hst, hawaii, hi - US/Hawaii"
            ],
            "default": "US/Pacific (Pacific Time)"
        },
        "help": {
            "json": "/api/help or /api/v1/help",
            "text": "/curl/help or /curl/v1/help",
            "html": "/help"
        }
    }

def get_help_text() -> str:
    """Generate plain text formatted help content."""
    help_text = """Sports Data Service API Help
Version: 1.0.0

ENDPOINTS:

Schedules:
  JSON Format:
    /api/v1/schedules/{date}              - All sports schedules
    /api/v1/schedule/{sport}/{date}        - Single sport schedule

  cURL Format:
    /curl/v1/schedules/{date}              - All sports schedules
    /curl/v1/schedule/{sport}/{date}       - Single sport schedule

Scores:
  JSON Format:
    /api/v1/scores/{date}                  - All sports scores
    /api/v1/scores/{sport}/{date}          - Single sport scores

  cURL Format:
    /curl/v1/scores/{date}                 - All sports scores
    /curl/v1/scores/{sport}/{date}         - Single sport scores

Standings:
  JSON Format:
    /api/v1/standings/{sport}               - Single sport standings

  cURL Format:
    /curl/v1/standings/{sport}              - Single sport standings

  Note: Standings endpoint is currently under development

Season Info:
  JSON Format:
    /api/v1/season-info/{league}           - Season phase dates

  Returns year, current_phase, and season_types with start/end dates.
  Cached for 24 hours.

SPORTS:
  nba, mlb, nfl, nhl, wnba, ipl, mlc, all

  Use 'all' to get data for all sports combined

LEAGUES (for season-info):
  mlb, nba, nfl, nhl, wnba, ipl, mlc

DATE FORMATS:
  today, tomorrow, yesterday
  YYYY-MM-DD (e.g., 2025-01-15)
  YYYYMMDD (e.g., 20250115)
  M/D/YYYY (e.g., 1/15/2025)
  MM/DD/YYYY (e.g., 01/15/2025)

TIMEZONE:
  Change timezone using the 'tz' query parameter: ?tz=<timezone>

  Examples:
    ?tz=et              - Eastern Time
    ?tz=pt              - Pacific Time
    ?tz=ct              - Central Time
    ?tz=mt              - Mountain Time
    ?tz=America/New_York - Full timezone name
    ?tz=Europe/London   - International timezone

  Supported Aliases:
    et, est, edt, eastern     -> US/Eastern
    pt, pst, pdt, pacific     -> US/Pacific
    ct, cst, cdt, central     -> US/Central
    mt, mst, mdt, mountain    -> US/Mountain
    akst, akdt, alaska, ak    -> US/Alaska
    hst, hawaii, hi           -> US/Hawaii

  Default: US/Pacific (Pacific Time)

HELP:
  /api/help or /api/v1/help    - JSON formatted help
  /curl/help or /curl/v1/help  - Plain text help (this format)
  /help                         - HTML formatted help

EXAMPLES:
  curl http://localhost:34180/api/v1/schedule/nba/today
  curl http://localhost:34180/curl/v1/scores/mlb/today?tz=et
  curl http://localhost:34180/api/v1/standings/nba
  curl http://localhost:34180/api/v1/season-info/mlb
  curl http://localhost:34180/curl/v1/schedule/ipl/today
"""
    return help_text

def get_help_html() -> str:
    """Generate HTML formatted help content."""
    html = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Sports Data Service API Help</title>
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            line-height: 1.6;
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
            background-color: #f5f5f5;
        }
        .container {
            background: white;
            padding: 30px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        h1 {
            color: #333;
            border-bottom: 3px solid #4CAF50;
            padding-bottom: 10px;
        }
        h2 {
            color: #555;
            margin-top: 30px;
            border-bottom: 2px solid #e0e0e0;
            padding-bottom: 5px;
        }
        h3 {
            color: #666;
            margin-top: 20px;
        }
        code {
            background-color: #f4f4f4;
            padding: 2px 6px;
            border-radius: 3px;
            font-family: 'Courier New', monospace;
            color: #d63384;
        }
        pre {
            background-color: #f4f4f4;
            padding: 15px;
            border-radius: 5px;
            overflow-x: auto;
            border-left: 4px solid #4CAF50;
        }
        .endpoint {
            background-color: #f9f9f9;
            padding: 10px;
            margin: 10px 0;
            border-radius: 5px;
            border-left: 3px solid #2196F3;
        }
        .sport-list {
            display: inline-block;
            background-color: #e3f2fd;
            padding: 5px 10px;
            border-radius: 3px;
            margin: 2px;
        }
        .note {
            background-color: #fff3cd;
            border-left: 4px solid #ffc107;
            padding: 10px;
            margin: 10px 0;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            margin: 15px 0;
        }
        th, td {
            padding: 10px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }
        th {
            background-color: #4CAF50;
            color: white;
        }
        tr:hover {
            background-color: #f5f5f5;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>Sports Data Service API Help</h1>
        <p><strong>Version:</strong> 1.0.0</p>
        
        <h2>Endpoints</h2>
        
        <h3>Schedules</h3>
        <div class="endpoint">
            <strong>JSON Format:</strong><br>
            <code>/api/v1/schedules/{date}</code> - All sports schedules<br>
            <code>/api/v1/schedule/{sport}/{date}</code> - Single sport schedule
        </div>
        <div class="endpoint">
            <strong>cURL Format:</strong><br>
            <code>/curl/v1/schedules/{date}</code> - All sports schedules<br>
            <code>/curl/v1/schedule/{sport}/{date}</code> - Single sport schedule
        </div>
        
        <h3>Scores</h3>
        <div class="endpoint">
            <strong>JSON Format:</strong><br>
            <code>/api/v1/scores/{date}</code> - All sports scores<br>
            <code>/api/v1/scores/{sport}/{date}</code> - Single sport scores
        </div>
        <div class="endpoint">
            <strong>cURL Format:</strong><br>
            <code>/curl/v1/scores/{date}</code> - All sports scores<br>
            <code>/curl/v1/scores/{sport}/{date}</code> - Single sport scores
        </div>
        
        <h3>Standings</h3>
        <div class="endpoint">
            <strong>JSON Format:</strong><br>
            <code>/api/v1/standings/{sport}</code> - Single sport standings
        </div>
        <div class="endpoint">
            <strong>cURL Format:</strong><br>
            <code>/curl/v1/standings/{sport}</code> - Single sport standings
        </div>
        <div class="note">
            <strong>Note:</strong> Standings endpoint is currently under development
        </div>

        <h3>Season Info</h3>
        <div class="endpoint">
            <strong>JSON Format:</strong><br>
            <code>/api/v1/season-info/{league}</code> - Season phase dates for a league
        </div>
        <div class="note">
            Returns year, current_phase, and season_types with start/end dates. Cached for 24 hours.<br>
            <strong>Leagues:</strong> mlb, nba, nfl, nhl, wnba, ipl, mlc
        </div>

        <h2>Sports</h2>
        <p>
            <span class="sport-list">mlb</span>
            <span class="sport-list">nba</span>
            <span class="sport-list">nfl</span>
            <span class="sport-list">nhl</span>
            <span class="sport-list">wnba</span>
            <span class="sport-list">ipl</span>
            <span class="sport-list">mlc</span>
            <span class="sport-list">all</span>
        </p>
        <p><strong>Note:</strong> Use <code>all</code> as the sport parameter to get data for all sports combined.
           IPL and MLC data is provided by <a href="https://ipl.cloud-puff.net">CricketPuff</a>.</p>
        
        <h2>Date Formats</h2>
        <p>The <code>{date}</code> parameter accepts:</p>
        <ul>
            <li><code>today</code> - Today's date</li>
            <li><code>tomorrow</code> - Tomorrow's date</li>
            <li><code>yesterday</code> - Yesterday's date</li>
            <li><code>YYYY-MM-DD</code> - ISO format (e.g., 2025-01-15)</li>
            <li><code>YYYYMMDD</code> - Compact format (e.g., 20250115)</li>
            <li><code>M/D/YYYY</code> - US format (e.g., 1/15/2025)</li>
            <li><code>MM/DD/YYYY</code> - US format with leading zeros (e.g., 01/15/2025)</li>
        </ul>
        
        <h2>Timezone</h2>
        <p>Change timezone using the <code>tz</code> query parameter: <code>?tz=&lt;timezone&gt;</code></p>
        
        <h3>Examples</h3>
        <table>
            <tr>
                <th>Parameter</th>
                <th>Description</th>
            </tr>
            <tr>
                <td><code>?tz=et</code></td>
                <td>Eastern Time</td>
            </tr>
            <tr>
                <td><code>?tz=pt</code></td>
                <td>Pacific Time</td>
            </tr>
            <tr>
                <td><code>?tz=ct</code></td>
                <td>Central Time</td>
            </tr>
            <tr>
                <td><code>?tz=mt</code></td>
                <td>Mountain Time</td>
            </tr>
            <tr>
                <td><code>?tz=America/New_York</code></td>
                <td>Full timezone name</td>
            </tr>
            <tr>
                <td><code>?tz=Europe/London</code></td>
                <td>International timezone</td>
            </tr>
        </table>
        
        <h3>Supported Aliases</h3>
        <table>
            <tr>
                <th>Aliases</th>
                <th>Timezone</th>
            </tr>
            <tr>
                <td><code>et, est, edt, eastern</code></td>
                <td>US/Eastern</td>
            </tr>
            <tr>
                <td><code>pt, pst, pdt, pacific</code></td>
                <td>US/Pacific</td>
            </tr>
            <tr>
                <td><code>ct, cst, cdt, central</code></td>
                <td>US/Central</td>
            </tr>
            <tr>
                <td><code>mt, mst, mdt, mountain</code></td>
                <td>US/Mountain</td>
            </tr>
            <tr>
                <td><code>akst, akdt, alaska, ak</code></td>
                <td>US/Alaska</td>
            </tr>
            <tr>
                <td><code>hst, hawaii, hi</code></td>
                <td>US/Hawaii</td>
            </tr>
        </table>
        
        <p><strong>Default:</strong> US/Pacific (Pacific Time)</p>
        
        <h2>Help</h2>
        <ul>
            <li><code>/api/help</code> or <code>/api/v1/help</code> - JSON formatted help</li>
            <li><code>/curl/help</code> or <code>/curl/v1/help</code> - Plain text help</li>
            <li><code>/help</code> - HTML formatted help (this page)</li>
        </ul>
        
        <h2>Examples</h2>
        <pre># Get today's NBA schedule (JSON)
curl http://localhost:34180/api/v1/schedule/nba/today

# Get today's MLB scores (cURL format, Eastern Time)
curl http://localhost:34180/curl/v1/scores/mlb/today?tz=et

# Get NBA standings (JSON)
curl http://localhost:34180/api/v1/standings/nba

# Get MLB season info
curl http://localhost:34180/api/v1/season-info/mlb

# Get IPL schedule (cricket)
curl http://localhost:34180/curl/v1/schedule/ipl/today</pre>
    </div>
</body>
</html>"""
    return html

def get_timezone(tz_param: Optional[str] = None):
    """
    Get timezone object from query parameter.
    
    Supports:
    - Common US timezone aliases (et, est, pt, pst, etc.)
    - Any pytz timezone name (e.g., 'America/New_York', 'Europe/London', 'Europe/Berlin', 'Asia/Tokyo')
    - Case-insensitive matching for pytz timezone names
    
    Returns US/Pacific (Pacific time) as default if timezone cannot be determined.
    
    Note: For best results, use full pytz timezone names like 'Europe/Berlin' instead of
    abbreviations like 'CEST'. pytz handles daylight saving time automatically.
    """
    if not tz_param:
        return pytz.timezone('US/Pacific')
    
    tz_param = tz_param.strip()
    tz_param_lower = tz_param.lower()
    
    # Map common US timezone aliases (user-friendly shortcuts)
    us_aliases = {
        'et': 'US/Eastern',
        'est': 'US/Eastern',
        'edt': 'US/Eastern',
        'eastern': 'US/Eastern',
        'pt': 'US/Pacific',
        'pst': 'US/Pacific',
        'pdt': 'US/Pacific',
        'pacific': 'US/Pacific',
        'ct': 'US/Central',
        'cst': 'US/Central',
        'cdt': 'US/Central',
        'central': 'US/Central',
        'mt': 'US/Mountain',
        'mst': 'US/Mountain',
        'mdt': 'US/Mountain',
        'mountain': 'US/Mountain',
        'akst': 'US/Alaska',
        'akdt': 'US/Alaska',
        'alaska': 'US/Alaska',
        'ak': 'US/Alaska',
        'hst': 'US/Hawaii',
        'hawaii': 'US/Hawaii',
        'hi': 'US/Hawaii',
    }
    
    # Check US aliases first
    if tz_param_lower in us_aliases:
        return pytz.timezone(us_aliases[tz_param_lower])
    
    # Try to parse as a pytz timezone name directly (case-sensitive first)
    try:
        return pytz.timezone(tz_param)
    except pytz.exceptions.UnknownTimeZoneError:
        pass
    
    # Try case-insensitive lookup in all pytz timezones
    # This allows users to use 'europe/berlin', 'EUROPE/BERLIN', etc.
    try:
        for tz_name in pytz.all_timezones:
            if tz_name.lower() == tz_param_lower:
                return pytz.timezone(tz_name)
    except Exception:
        pass
    
    # Try common timezone abbreviations that pytz doesn't recognize directly
    # Only a minimal set for very common abbreviations
    common_abbrevs = {
        'utc': 'UTC',
        'z': 'UTC',
        'gmt': 'Europe/London',
        'cest': 'Europe/Berlin',  # Central European Summer Time
        'cet': 'Europe/Berlin',   # Central European Time
        'bst': 'Europe/London',   # British Summer Time
    }
    if tz_param_lower in common_abbrevs:
        try:
            return pytz.timezone(common_abbrevs[tz_param_lower])
        except:
            pass
    
    # Default to Pacific if we can't determine the timezone
    return pytz.timezone('US/Pacific')

def get_greeting(tz: pytz.BaseTzInfo = None) -> str:
    """Get greeting based on time of day in specified timezone."""
    if tz is None:
        tz = pytz.timezone('US/Pacific')  # Default to Pacific
    
    now = datetime.now(tz)
    hour = now.hour
    
    if 0 <= hour < 5:
        return "Good god... it's late ⏾ from SportsPuff!"
    elif 5 <= hour < 12:
        return "Good morning 🌇 from SportsPuff!"
    elif 12 <= hour < 17:
        return "Good afternoon 🌞 from SportsPuff!"
    else:
        return "Good evening ✨ from SportsPuff!"

def parse_date_param(date_param: Optional[str], tz: pytz.BaseTzInfo = None) -> date:
    """
    Parse date parameter with support for multiple formats.
    
    Supports:
    - Relative dates: today, tomorrow, yesterday (uses Pacific time by default)
    - YYYY-MM-DD (ISO format, e.g., 2025-11-05)
    - YYYYMMDD (compact format, e.g., 20251105)
    - M/D/YYYY or MM/DD/YYYY (US format, e.g., 11/5/2025 or 11/05/2025)
    - M-D-YYYY or MM-DD-YYYY (US format with dashes, e.g., 11-5-2025)
    - YYYY/M/D or YYYY/MM/DD (alternative format, e.g., 2025/11/5)
    
    Uses dateutil.parser as fallback for other formats.
    
    Args:
        date_param: Date string to parse
        tz: Timezone for relative dates (defaults to Pacific)
    """
    if tz is None:
        tz = pytz.timezone('US/Pacific')  # Default to Pacific for "today"
    
    # Get today's date in the specified timezone
    now_tz = datetime.now(tz)
    today = now_tz.date()
    
    if date_param is None or date_param.lower() == 'today':
        return today
    elif date_param.lower() == 'tomorrow':
        return today + timedelta(days=1)
    elif date_param.lower() == 'yesterday':
        return today - timedelta(days=1)
    
    # Try multiple date formats
    date_formats = [
        '%Y-%m-%d',      # YYYY-MM-DD (ISO format)
        '%Y%m%d',         # YYYYMMDD (compact format)
        '%m/%d/%Y',       # M/D/YYYY or MM/DD/YYYY
        '%m-%d-%Y',       # M-D-YYYY or MM-DD-YYYY
        '%Y/%m/%d',       # YYYY/M/D or YYYY/MM/DD
        '%m.%d.%Y',       # M.D.YYYY (alternative)
        '%d/%m/%Y',       # D/M/YYYY (European format)
        '%d-%m-%Y',       # D-M-YYYY (European format)
        '%Y-%m-%d',       # YYYY-MM-DD (redundant but explicit)
    ]
    
    # Try each format
    for fmt in date_formats:
        try:
            return datetime.strptime(date_param, fmt).date()
        except ValueError:
            continue
    
    # Fallback to dateutil.parser for flexible parsing (handles many formats)
    try:
        from dateutil import parser
        parsed_date = parser.parse(date_param)
        return parsed_date.date()
    except (ValueError, TypeError) as e:
        # If all parsing fails, provide helpful error message
        raise HTTPException(
            status_code=400,
            detail=f"Invalid date format: '{date_param}'. Supported formats: today/tomorrow/yesterday, YYYY-MM-DD, YYYYMMDD, M/D/YYYY, MM/DD/YYYY, M-D-YYYY, etc."
        )

def format_game_for_curl(game: Game, sport: str, tz: pytz.BaseTzInfo = None) -> str:
    """Format a single game for curl-style schedule output.

    Final:       *PHI [ 14- 20] ( 7@ 2)  MIA [ 16- 18]  F
    In-progress:  PHI [ 14- 20] ( 2@ 1)  MIA [ 16- 18]  TOP 5
    Scheduled:    PHI [ 14- 20]    @      MIA [ 16- 18]  7:00 PM PDT - in 3h
    """
    if tz is None:
        tz = pytz.timezone('US/Pacific')

    if sport.lower() in ('ipl', 'mlc'):
        return _format_cricket_game(game, tz)

    visitor_wins = game.visitor_wins or 0
    visitor_losses = game.visitor_losses or 0
    home_wins = game.home_wins or 0
    home_losses = game.home_losses or 0

    visitor_abbrev = game.visitor_team_abbrev
    if not visitor_abbrev or visitor_abbrev.strip() == '':
        visitor_abbrev = (game.visitor_team or '???')[:4].upper()
    abbrev_width = 4 if sport.lower() == 'mls' else 3
    visitor_abbrev = visitor_abbrev.ljust(abbrev_width)

    home_abbrev = game.home_team_abbrev
    if not home_abbrev or home_abbrev.strip() == '':
        home_abbrev = (game.home_team or '???')[:4].upper()
    home_abbrev = home_abbrev.ljust(abbrev_width)

    game_type = getattr(game, 'game_type', 'regular')

    if sport.lower() == 'nhl':
        visitor_otl = getattr(game, 'visitor_otl', 0) or 0
        home_otl = getattr(game, 'home_otl', 0) or 0
        if game_type == 'playoffs':
            away_rec = f"[{visitor_wins}-{visitor_losses}]"
            home_rec = f"[{home_wins}-{home_losses}]"
        else:
            away_rec = f"[{visitor_wins:3d}-{visitor_losses:3d}-{visitor_otl:2d}]"
            home_rec = f"[{home_wins:3d}-{home_losses:3d}-{home_otl:2d}]"
    elif sport.lower() == 'mls':
        visitor_draws = getattr(game, 'visitor_draws', 0) or 0
        home_draws = getattr(game, 'home_draws', 0) or 0
        v_pts = visitor_wins * 3 + visitor_draws
        h_pts = home_wins * 3 + home_draws
        away_rec = f"[{visitor_wins:2d}-{visitor_draws:2d}-{visitor_losses:2d} {v_pts:2d}pts]"
        home_rec = f"[{home_wins:2d}-{home_draws:2d}-{home_losses:2d} {h_pts:2d}pts]"
    elif game_type == 'playoffs':
        away_rec = f"[{visitor_wins}-{visitor_losses}]"
        home_rec = f"[{home_wins}-{home_losses}]"
    else:
        away_rec = f"[{visitor_wins:3d}-{visitor_losses:3d}]"
        home_rec = f"[{home_wins:3d}-{home_losses:3d}]"

    vs = game.visitor_score_total or 0
    hs = game.home_score_total or 0

    if game.is_final:
        visitor_won = vs > hs
        home_won = hs > vs
        v_mark = '*' if visitor_won else ' '
        h_mark = '*' if home_won else ' '

        if sport.lower() == 'nhl':
            period = str(game.current_period) if game.current_period is not None else '?'
            try:
                period_num = int(period) if str(period).isdigit() else 0
                status = "F/OT" if period_num >= 4 else "F"
            except (ValueError, TypeError):
                status = "F"
        elif sport.lower() == 'mls':
            status = "FT"
        else:
            status = "F"

        return f" {v_mark}{visitor_abbrev} {away_rec} ({vs:2d}@{hs:2d}) {h_mark}{home_abbrev} {home_rec}  {status}"

    elif game.game_status == 'in_progress' or (vs > 0 or hs > 0):
        period = str(game.current_period) if game.current_period is not None else '?'
        time_left = game.time_remaining or ''

        if sport.lower() == 'nhl':
            try:
                period_num = int(period) if str(period).isdigit() else 0
                period_display = 'OT' if period_num >= 4 else f'P{period_num}'
            except (ValueError, TypeError):
                period_display = f'P{period}'
            status = f"{period_display} {time_left}".strip() if time_left and time_left.strip() else period_display
        elif period and str(period).upper() in ('FINAL', 'F', 'END', 'FIN'):
            status = "F"
        elif sport.lower() == 'mlb':
            inning_state = time_left.strip().upper() if time_left else ''
            inning_abbrev = {'TOP': 'TOP', 'BOTTOM': 'BOT', 'MIDDLE': 'MID', 'END': 'END'}.get(inning_state, inning_state)
            status = f"{inning_abbrev} {period}" if inning_abbrev else f"INN {period}"
        else:
            period_prefix = 'Q'
            is_halftime = (period == '2' and time_left in ('0:00', '') and game.game_status == 'in_progress')
            if is_halftime:
                status = "HT"
            elif game.game_status == 'in_progress' and time_left and time_left.strip():
                status = f"{period_prefix}{period} {time_left}"
            elif game.game_status == 'in_progress' and period and period not in ('?', '', '0'):
                status = f"{period_prefix}{period}"
            else:
                status = "F"

        return f"  {visitor_abbrev} {away_rec} ({vs:2d}@{hs:2d})  {home_abbrev} {home_rec}  {status}"

    else:
        if game.game_time:
            try:
                gt = game.game_time
                if hasattr(gt, 'tzinfo') and gt.tzinfo is None:
                    gt = pytz.UTC.localize(gt)
                game_time_local = gt.astimezone(tz)
                tz_abbrev = game_time_local.strftime('%Z')
                time_str = game_time_local.strftime('%-I:%M %p')
                now = datetime.now(tz)
                diff = game_time_local - now
                total_seconds = int(diff.total_seconds())
                if total_seconds > 0:
                    hours, remainder = divmod(total_seconds, 3600)
                    minutes = remainder // 60
                    status = f"{time_str} {tz_abbrev} - in {hours}h{minutes:02d}m"
                else:
                    status = f"{time_str} {tz_abbrev}"
            except Exception:
                status = "TBD"
        else:
            status = "TBD"

        return f"  {visitor_abbrev} {away_rec}    @     {home_abbrev} {home_rec}  {status}"


def _format_cricket_game(game, tz):
    """Format a cricket match for curl output."""
    home_abbrev = (getattr(game, 'home_team_abbrev', '') or '???').ljust(4)
    away_abbrev = (getattr(game, 'visitor_team_abbrev', '') or '???').ljust(4)

    home_score_str = getattr(game, 'cricket_home_score', '') or ''
    away_score_str = getattr(game, 'cricket_away_score', '') or ''
    away_outcome = getattr(game, 'cricket_away_outcome', '') or ''
    start_time = getattr(game, 'cricket_start_time', {}) or {}

    if game.is_final and (home_score_str or away_score_str):
        away_part = f"{away_abbrev} ({away_score_str})" if away_score_str else str(away_abbrev)
        home_part = f"{home_abbrev} ({home_score_str})" if home_score_str else str(home_abbrev)
        outcome = away_outcome.rjust(4) if away_outcome else '    '
        return f" {away_part:18s} {outcome} @ {home_part}"
    elif game.is_final:
        cricket_status = getattr(game, 'cricket_status', '') or ''
        return f" {cricket_status}" if cricket_status else f" {away_abbrev} @ {home_abbrev} F"
    elif getattr(game, 'game_status', '') == 'in_progress':
        cricket_status = getattr(game, 'cricket_status', '') or ''
        if home_score_str or away_score_str:
            away_part = f"{away_abbrev} ({away_score_str})" if away_score_str else str(away_abbrev)
            home_part = f"{home_abbrev} ({home_score_str})" if home_score_str else str(home_abbrev)
            return f" {away_part} @ {home_part} LIVE"
        elif cricket_status:
            return f" {away_abbrev} @ {home_abbrev} {cricket_status}"
        else:
            return f" {away_abbrev} @ {home_abbrev} LIVE"
    else:
        pt_str = start_time.get('pt', '')
        ist_str = start_time.get('ist', '')
        if pt_str and ist_str:
            time_str = f"{pt_str}/{ist_str}"
        elif pt_str:
            time_str = pt_str
        else:
            time_str = "TBD"

        countdown = ''
        gt = getattr(game, 'game_time', None)
        if gt:
            try:
                now = datetime.now(tz)
                diff = gt.astimezone(tz) - now
                total_seconds = int(diff.total_seconds())
                if total_seconds > 0:
                    hours, remainder = divmod(total_seconds, 3600)
                    minutes = remainder // 60
                    countdown = f" - in {hours}h{minutes:02d}m"
            except Exception:
                pass

        return f" {away_abbrev} @ {home_abbrev} {time_str}{countdown}"


def _get_season_type_for_sport(sport: str, target_date: date) -> str:
    """Get season type for a sport from database when there are no games for the date."""
    sport_to_league = {
        'mlb': 'MLB',
        'wnba': 'WNBA',
        'nba': 'NBA',
        'nfl': 'NFL',
        'nhl': 'NHL'
    }
    league = sport_to_league.get(sport)
    if not league:
        return "Off Season"
    
    # Try to find the most recent game for this sport to determine season type
    try:
        with get_db_session() as db:
            # Look for games within a reasonable range (30 days before/after)
            start_date = target_date - timedelta(days=30)
            end_date = target_date + timedelta(days=30)
            
            recent_game = db.query(Game).filter(
                Game.league == league,
                Game.game_date >= start_date,
                Game.game_date <= end_date
            ).order_by(Game.game_date.desc()).first()
            
            if recent_game:
                game_type_map = {
                    'preseason': 'Preseason',
                    'regular': 'Regular Season',
                    'playoffs': 'Playoffs',
                    'allstar': 'All-Star',
                    'nba_cup': 'Emirates NBA Cup',
                    'postseason': 'Playoffs'
                }
                return game_type_map.get(recent_game.game_type.lower(), recent_game.game_type.title().replace('_', ' '))
    except Exception:
        pass
    
    # Default fallback
    return "Off Season"


def _format_curl_header(tz, target_date, label):
    greeting = get_greeting(tz)
    output = f"{greeting}\n"
    output += "\n"
    output += f"{label}\n"
    output += "-" * 45 + "\n"
    return output


def _format_curl_footer(tz):
    now_tz = datetime.now(tz)
    tz_abbrev = now_tz.strftime('%Z')
    date_str = now_tz.strftime('%a %b %d %Y')
    time_str = now_tz.strftime('%H:%M')
    output = f"          All times in {tz_abbrev}\n"
    output += f"    Sent on {date_str} @{time_str}{tz_abbrev}\n"
    output += "-" * 45 + "\n"
    return output


def format_schedule_curl(games: List[Game], target_date: date, tz: pytz.BaseTzInfo = None, show_all_sports: bool = False) -> str:
    if tz is None:
        tz = pytz.timezone('US/Pacific')

    if not games and not show_all_sports:
        return "No games scheduled"

    by_sport: Dict[str, List[Game]] = {}
    for game in games:
        sport = game.league.lower()
        if sport not in by_sport:
            by_sport[sport] = []
        by_sport[sport].append(game)

    output = _format_curl_header(tz, target_date, "Here is the schedule:")

    sport_order = ['ipl', 'mlb', 'mlc', 'mls', 'nba', 'nfl', 'nhl', 'wnba']
    sport_to_league = {
        'ipl': 'IPL', 'mlb': 'MLB', 'mlc': 'MLC', 'mls': 'MLS',
        'nba': 'NBA', 'nfl': 'NFL', 'nhl': 'NHL', 'wnba': 'WNBA'
    }

    game_type_map = {
        'preseason': 'Preseason', 'regular': 'Regular Season',
        'playoffs': 'Post Season (Playoffs)', 'postseason': 'Post Season (Playoffs)',
        'allstar': 'All-Star', 'nba_cup': 'Emirates NBA Cup'
    }

    for sport in sport_order:
        if not show_all_sports and sport not in by_sport:
            continue

        sport_games = by_sport.get(sport, [])

        if sport_games:
            first_game = sport_games[0]
            season_type = game_type_map.get(first_game.game_type.lower(), first_game.game_type.title().replace('_', ' '))
            league_name = first_game.league
        else:
            league_name = sport_to_league.get(sport, sport.upper())
            season_type = _get_season_type_for_sport(sport, target_date)

        output += f"{league_name} [{season_type}]\n"
        output += "-" * 45 + "\n"

        if sport_games:
            for game in sport_games:
                output += format_game_for_curl(game, sport, tz)
                output += "\n"
        else:
            output += " No games scheduled\n"

        output += "-" * 45 + "\n"

    output += _format_curl_footer(tz)
    return output


def format_scores_curl(games: List[Game], target_date: date, tz: pytz.BaseTzInfo = None, show_all_sports: bool = False) -> str:
    if tz is None:
        tz = pytz.timezone('US/Pacific')

    by_sport: Dict[str, List[Game]] = {}
    for game in games:
        sport = game.league.lower()
        if sport not in by_sport:
            by_sport[sport] = []
        by_sport[sport].append(game)

    output = _format_curl_header(tz, target_date, "Here are the scores:")

    sport_order = ['ipl', 'mlb', 'mlc', 'mls', 'nba', 'nfl', 'nhl', 'wnba']
    sport_to_league = {
        'ipl': 'IPL', 'mlb': 'MLB', 'mlc': 'MLC', 'mls': 'MLS',
        'nba': 'NBA', 'nfl': 'NFL', 'nhl': 'NHL', 'wnba': 'WNBA'
    }

    game_type_map = {
        'preseason': 'Preseason', 'regular': 'Regular Season',
        'playoffs': 'Post Season (Playoffs)', 'postseason': 'Post Season (Playoffs)',
        'allstar': 'All-Star', 'nba_cup': 'Emirates NBA Cup'
    }
    
    for sport in sport_order:
        # If show_all_sports is False, skip sports with no games
        if not show_all_sports and sport not in by_sport:
            continue
        
        sport_games = by_sport.get(sport, [])
        
        # Show games with scores (final, in progress, or scheduled with scores > 0)
        # Also deduplicate by game_id to avoid showing the same game twice
        seen_game_ids = set()
        scored_games = []
        for g in sport_games:
            game_id = getattr(g, 'game_id', None) or getattr(g, 'gameId', None)
            if game_id and game_id in seen_game_ids:
                continue  # Skip duplicates
            # Only include games that have scores (final, in progress, or have non-zero scores)
            # Skip games that are just scheduled (score 0-0 and status is scheduled)
            has_score = (g.visitor_score_total and g.visitor_score_total > 0) or (g.home_score_total and g.home_score_total > 0)
            is_final_or_live = g.is_final or g.game_status == 'in_progress' or has_score
            
            if is_final_or_live:
                seen_game_ids.add(game_id or 'no_id')
                scored_games.append(g)
        
        # Get season info - either from games or from database
        if scored_games:
            # Determine season info from first game
            first_game = scored_games[0]
            season_type = game_type_map.get(first_game.game_type.lower(), first_game.game_type.title().replace('_', ' '))
            league_name = first_game.league
        else:
            league_name = sport_to_league.get(sport, sport.upper())
            season_type = _get_season_type_for_sport(sport, target_date)

        output += f"{league_name} [{season_type}]\n"
        output += "-" * 45 + "\n"

        if scored_games:
            if sport in ('ipl', 'mlc'):
                for game in scored_games:
                    output += _format_cricket_game(game, tz)
                    output += "\n"
            else:
                abbr_w = 4 if sport == 'mls' else 3
                for game in scored_games:
                    away_abbr = (game.visitor_team_abbrev or '???').ljust(abbr_w)
                    home_abbr = (game.home_team_abbrev or '???').ljust(abbr_w)

                    away_score = game.visitor_score_total or 0
                    home_score = game.home_score_total or 0

                    if game.is_final:
                        if sport == 'nhl':
                            period = str(game.current_period) if game.current_period is not None else '?'
                            try:
                                period_num = int(period) if str(period).isdigit() else 0
                                status = "F/OT" if period_num >= 4 else "F"
                            except (ValueError, TypeError):
                                status = "F"
                        elif sport == 'mls':
                            status = "FT"
                        else:
                            status = "F"
                        output += f" {away_abbr} {away_score:2d}-{home_score:2d} {home_abbr} {status}\n"
                    elif game.game_status == 'in_progress' or (away_score > 0 or home_score > 0):
                        period = str(game.current_period) if game.current_period is not None else '?'
                        time_left = game.time_remaining or ''

                        if sport == 'nhl':
                            try:
                                period_num = int(period) if str(period).isdigit() else 0
                                period_display = 'OT' if period_num >= 4 else f'P{period_num}'
                            except (ValueError, TypeError):
                                period_display = f'P{period}'
                            status = f"{period_display} {time_left}".strip() if time_left and time_left.strip() else period_display
                        elif period and str(period).upper() in ('FINAL', 'F', 'END', 'FIN'):
                            status = "F"
                        elif sport == 'mlb':
                            inning_state = time_left.strip().upper() if time_left else ''
                            inning_abbrev = {'TOP': 'TOP', 'BOTTOM': 'BOT', 'MIDDLE': 'MID', 'END': 'END'}.get(inning_state, inning_state)
                            status = f"{inning_abbrev} {period}" if inning_abbrev else f"INN {period}"
                        else:
                            period_prefix = 'Q'
                            if game.game_status == 'in_progress' and time_left and time_left.strip():
                                status = f"{period_prefix}{period} {time_left}"
                            elif game.game_status == 'in_progress' and period and period not in ('?', '', '0'):
                                status = f"{period_prefix}{period}"
                            else:
                                status = "F"

                        output += f" {away_abbr} {away_score:2d}-{home_score:2d} {home_abbr} {status}\n"
        else:
            output += " No games scheduled\n"
        
        output += "-" * 45 + "\n"

    output += _format_curl_footer(tz)
    return output
@app.get("/", response_class=HTMLResponse)
def root():
    """Landing page."""
    return """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>SportsPuff API</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{
  font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,'Helvetica Neue',Arial,sans-serif;
  background:linear-gradient(135deg,#1A0B3D 0%,#2D1B69 50%,#3D2A7A 100%);
  color:#F5F5F5;
  min-height:100vh;
  display:flex;
  flex-direction:column;
  align-items:center;
}
header{
  width:100%;
  background:linear-gradient(135deg,#2D1B69 0%,#FF3B30 100%);
  padding:2rem 0;
  text-align:center;
  box-shadow:0 2px 10px rgba(26,42,108,0.3);
}
header img{height:120px;margin-bottom:0.5rem}
header h1{font-size:2.5rem;font-weight:700;text-shadow:2px 2px 4px rgba(0,0,0,0.5)}
header p{font-size:1.1rem;color:rgba(245,245,245,0.8);margin-top:0.25rem}
.container{
  max-width:800px;width:100%;
  padding:2rem;margin:2rem auto;
  background:rgba(26,11,61,0.9);
  border-radius:20px;
  border:1px solid rgba(255,255,255,0.2);
  box-shadow:0 10px 30px rgba(0,0,0,0.4);
}
h2{font-size:1.5rem;font-weight:700;margin-bottom:1rem;color:#FFB400}
.blurb{font-size:1rem;line-height:1.6;margin-bottom:2rem;color:#B8B8B8}
.blurb a{color:#FF3B30;text-decoration:none}
.blurb a:hover{text-decoration:underline}
.section{margin-bottom:2rem}
.endpoint-group h3{
  font-size:1.1rem;font-weight:600;margin:1.25rem 0 0.5rem;
  padding-bottom:0.25rem;border-bottom:2px solid rgba(255,255,255,0.1);
}
table{width:100%;border-collapse:collapse}
td{padding:0.35rem 0.5rem;vertical-align:top;font-size:0.9rem}
td:first-child{white-space:nowrap}
td a{
  color:#F5F5F5;text-decoration:none;
  font-family:'SF Mono',SFMono-Regular,Consolas,'Liberation Mono',Menlo,monospace;
  font-size:0.85rem;
  padding:0.15rem 0.4rem;
  background:rgba(255,255,255,0.08);
  border-radius:4px;
  transition:all 0.2s ease;
}
td a:hover{background:rgba(255,59,48,0.25);color:#fff}
td:last-child{color:#B8B8B8}
.tag{
  display:inline-block;font-size:0.7rem;font-weight:600;
  padding:0.1rem 0.45rem;border-radius:10px;margin-left:0.4rem;
  vertical-align:middle;
}
.tag-json{background:rgba(112,40,228,0.3);color:#c4a0ff}
.tag-text{background:rgba(255,180,0,0.2);color:#FFB400}
footer{
  text-align:center;padding:2rem 1rem;
  font-size:0.8rem;color:rgba(245,245,245,0.4);
}
@media(max-width:600px){
  header img{height:80px}
  header h1{font-size:1.8rem}
  .container{margin:1rem;padding:1.25rem;border-radius:14px}
  td a{font-size:0.78rem}
}
</style>
</head>
<body>
<header>
  <img src="https://www.splitsp.lat/logos/sportspuff/sportspuff-logo.png" alt="SportsPuff"
       onerror="this.style.display='none'">
  <h1>SportsPuff API</h1>
  <p>v1.0.0</p>
</header>

<div class="container">
  <div class="section">
    <h2>About</h2>
    <p class="blurb">
      This is the API backend for
      <a href="https://www.sportspuff.org">www.sportspuff.org</a>.
      It serves live scores, schedules, and standings for MLB, NBA, NFL, NHL, and WNBA.
      Responses are available as JSON (for apps) or plain text (for curl/terminal use).
    </p>
  </div>

  <div class="section endpoint-group">
    <h2>Endpoints</h2>

    <h3>Scores</h3>
    <table>
      <tr>
        <td><a href="/api/v1/scores/today">/api/v1/scores/today</a> <span class="tag tag-json">JSON</span></td>
        <td>All sports</td>
      </tr>
      <tr>
        <td><a href="/api/v1/scores/nba/today">/api/v1/scores/{sport}/today</a> <span class="tag tag-json">JSON</span></td>
        <td>Single sport</td>
      </tr>
      <tr>
        <td><a href="/curl/v1/scores/today">/curl/v1/scores/today</a> <span class="tag tag-text">TEXT</span></td>
        <td>All sports</td>
      </tr>
      <tr>
        <td><a href="/curl/v1/scores/nba/today">/curl/v1/scores/{sport}/today</a> <span class="tag tag-text">TEXT</span></td>
        <td>Single sport</td>
      </tr>
    </table>

    <h3>Schedules</h3>
    <table>
      <tr>
        <td><a href="/api/v1/schedules/today">/api/v1/schedules/today</a> <span class="tag tag-json">JSON</span></td>
        <td>All sports</td>
      </tr>
      <tr>
        <td><a href="/api/v1/schedule/nba/today">/api/v1/schedule/{sport}/today</a> <span class="tag tag-json">JSON</span></td>
        <td>Single sport</td>
      </tr>
      <tr>
        <td><a href="/curl/v1/schedules/today">/curl/v1/schedules/today</a> <span class="tag tag-text">TEXT</span></td>
        <td>All sports</td>
      </tr>
      <tr>
        <td><a href="/curl/v1/schedule/nba/today">/curl/v1/schedule/{sport}/today</a> <span class="tag tag-text">TEXT</span></td>
        <td>Single sport</td>
      </tr>
    </table>

    <h3>Standings</h3>
    <table>
      <tr>
        <td><a href="/api/v1/standings/nba">/api/v1/standings/{sport}</a> <span class="tag tag-json">JSON</span></td>
        <td>JSON</td>
      </tr>
      <tr>
        <td><a href="/curl/v1/standings/nba">/curl/v1/standings/{sport}</a> <span class="tag tag-text">TEXT</span></td>
        <td>Plain text</td>
      </tr>
    </table>

    <h3>Season Info</h3>
    <table>
      <tr>
        <td><a href="/api/v1/season-info/mlb">/api/v1/season-info/{league}</a> <span class="tag tag-json">JSON</span></td>
        <td>Season phase dates (cached 24h)</td>
      </tr>
    </table>

    <h3>Help</h3>
    <table>
      <tr>
        <td><a href="/help">/help</a></td>
        <td>Full endpoint reference (HTML)</td>
      </tr>
      <tr>
        <td><a href="/api/help">/api/help</a> <span class="tag tag-json">JSON</span></td>
        <td>Full endpoint reference</td>
      </tr>
      <tr>
        <td><a href="/curl/help">/curl/help</a> <span class="tag tag-text">TEXT</span></td>
        <td>Full endpoint reference</td>
      </tr>
    </table>
  </div>

  <div class="section">
    <h2>Usage</h2>
    <p class="blurb">
      Replace <code style="background:rgba(255,255,255,0.08);padding:0.1rem 0.4rem;border-radius:4px">today</code>
      with <code style="background:rgba(255,255,255,0.08);padding:0.1rem 0.4rem;border-radius:4px">yesterday</code>,
      <code style="background:rgba(255,255,255,0.08);padding:0.1rem 0.4rem;border-radius:4px">tomorrow</code>,
      or a date like <code style="background:rgba(255,255,255,0.08);padding:0.1rem 0.4rem;border-radius:4px">2026-04-28</code>.
      Sports: <code style="background:rgba(255,255,255,0.08);padding:0.1rem 0.4rem;border-radius:4px">mlb</code>,
      <code style="background:rgba(255,255,255,0.08);padding:0.1rem 0.4rem;border-radius:4px">nba</code>,
      <code style="background:rgba(255,255,255,0.08);padding:0.1rem 0.4rem;border-radius:4px">nfl</code>,
      <code style="background:rgba(255,255,255,0.08);padding:0.1rem 0.4rem;border-radius:4px">nhl</code>,
      <code style="background:rgba(255,255,255,0.08);padding:0.1rem 0.4rem;border-radius:4px">wnba</code>,
      <code style="background:rgba(255,255,255,0.08);padding:0.1rem 0.4rem;border-radius:4px">ipl</code>,
      <code style="background:rgba(255,255,255,0.08);padding:0.1rem 0.4rem;border-radius:4px">mlc</code>,
      or <code style="background:rgba(255,255,255,0.08);padding:0.1rem 0.4rem;border-radius:4px">all</code>.
      Add <code style="background:rgba(255,255,255,0.08);padding:0.1rem 0.4rem;border-radius:4px">?tz=et</code>
      for Eastern time (default is Pacific).
    </p>
  </div>
</div>

<footer>SportsPuff &mdash; sportspuff.org</footer>
</body>
</html>"""


@app.get("/help", response_class=HTMLResponse)
def help_html():
    """HTML formatted help page."""
    return get_help_html()


@app.get("/api/help")
def help_api():
    """JSON formatted help."""
    return get_help_json()


@app.get("/api/v1/help")
def help_api_v1():
    """JSON formatted help."""
    return get_help_json()


@app.get("/curl/help", response_class=PlainTextResponse)
def help_curl():
    """Plain text formatted help."""
    return get_help_text()


@app.get("/curl/v1/help", response_class=PlainTextResponse)
def help_curl_v1():
    """Plain text formatted help."""
    return get_help_text()


@app.get("/api/v1/schedules/{date}")
def get_schedules_all_sports_api_v1(
    date: str = Path(..., description="Date: today/tomorrow/yesterday, YYYY-MM-DD, YYYYMMDD, M/D/YYYY, MM/DD/YYYY, or other formats"),
    tz: Optional[str] = Query(None, description="Timezone for relative dates (default: Pacific)"),
):
    """Get schedules for all sports in JSON format."""
    try:
        timezone = get_timezone(tz)
        target_date = parse_date_param(date, timezone)
        result = {}

        for sport_key, league in SPORT_MAPPINGS.items():
            games = _get_games_for_curl(league, target_date, timezone)
            result[sport_key] = [_game_wrapper_to_dict(g, league) for g in games]

        return {
            "date": target_date.isoformat(),
            "sports": result
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/curl/v1/schedules/{date}", response_class=PlainTextResponse)
def get_schedules_all_sports_curl_v1(
    date: str = Path(..., description="Date: today/tomorrow/yesterday, YYYY-MM-DD, YYYYMMDD, M/D/YYYY, MM/DD/YYYY, or other formats"),
    tz: Optional[str] = Query(None, description="Timezone: et/est/eastern, pt/pst/pdt/pacific (default: Pacific)"),
):
    """Get schedules for all sports in curl-style text format."""
    try:
        timezone = get_timezone(tz)
        target_date = parse_date_param(date, timezone)

        all_games = []
        for sport_key in SPORT_MAPPINGS.keys():
            league = SPORT_MAPPINGS[sport_key]
            games = _get_games_for_curl(league, target_date, timezone)
            all_games.extend(games)

        return format_schedule_curl(all_games, target_date, timezone)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/scores/{date}")
def get_scores_all_sports_api_v1(
    date: str = Path(..., description="Date: today/tomorrow/yesterday, YYYY-MM-DD, YYYYMMDD, M/D/YYYY, MM/DD/YYYY, or other formats"),
    tz: Optional[str] = Query(None, description="Timezone for relative dates (default: Pacific)"),
):
    """Get scores for all sports in JSON format."""
    try:
        timezone = get_timezone(tz)
        target_date = parse_date_param(date, timezone)
        result = {}

        for sport_key, league in SPORT_MAPPINGS.items():
            games = _get_games_for_curl(league, target_date, timezone)
            result[sport_key] = [_game_wrapper_to_dict(g, league) for g in games]

        return {
            "date": target_date.isoformat(),
            "sports": result
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/curl/v1/scores/{date}", response_class=PlainTextResponse)
def get_scores_all_sports_curl_v1(
    date: str = Path(..., description="Date: today/tomorrow/yesterday, YYYY-MM-DD, YYYYMMDD, M/D/YYYY, MM/DD/YYYY, or other formats"),
    tz: Optional[str] = Query(None, description="Timezone: et/est/eastern, pt/pst/pdt/pacific (default: Pacific)"),
):
    """Get scores for all sports in curl-style text format."""
    try:
        timezone = get_timezone(tz)
        target_date = parse_date_param(date, timezone)

        all_games = []
        for sport_key in SPORT_MAPPINGS.keys():
            league = SPORT_MAPPINGS[sport_key]
            games = _get_games_for_curl(league, target_date, timezone)
            all_games.extend(games)

        return format_scores_curl(all_games, target_date, timezone)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/schedule/{sport}/{date}")
def get_schedule_api_v1(
    sport: str = Path(..., description="Sport (nba, mlb, nfl, nhl, wnba, all)"),
    date: str = Path(..., description="Date: today/tomorrow/yesterday, YYYY-MM-DD, YYYYMMDD, M/D/YYYY, MM/DD/YYYY, or other formats"),
    tz: Optional[str] = Query(None, description="Timezone for relative dates (default: Pacific)"),
):
    """Get schedule in JSON format."""
    try:
        timezone = get_timezone(tz)
        target_date = parse_date_param(date, timezone)
        sport_lower = sport.lower()

        if sport_lower == 'all':
            all_games = []
            for sport_key in SPORT_MAPPINGS.keys():
                league = SPORT_MAPPINGS[sport_key]
                games = _get_games_for_curl(league, target_date, timezone)
                for g in games:
                    d = _game_wrapper_to_dict(g, league)
                    d['sport'] = sport_key
                    all_games.append(d)
            all_games.sort(key=lambda x: x.get('game_time') or '')
            return {"sport": "all", "date": target_date.isoformat(), "games": all_games}

        league = SPORT_MAPPINGS.get(sport_lower)
        if not league:
            raise HTTPException(status_code=400, detail=f"Invalid sport: {sport}")

        games = _get_games_for_curl(league, target_date, timezone)
        return {
            "sport": sport,
            "date": target_date.isoformat(),
            "games": [_game_wrapper_to_dict(g, league) for g in games]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def _get_schedule_for_league(league: str, target_date: date, timezone: pytz.BaseTzInfo) -> List[Dict[str, Any]]:
    """Helper function to get schedule for a specific league."""
    now_tz = datetime.now(timezone)
    today = now_tz.date()
    games_list = []
    
    collector = get_collector(league)
    
    # For today's games, try to get live data first
    # But only use live_scores if we actually have games for today
    # (get_live_scores may include yesterday's games that are still in progress)
    if target_date == today:
        if collector:
            # First try get_schedule to get today's scheduled games
            schedule_games = collector.get_schedule(target_date)
            
            # Handle both list and dict formats from NBA collector
            if isinstance(schedule_games, dict) and 'leagueSchedule' in schedule_games:
                # NBA collector sometimes returns dict format
                game_dates = schedule_games['leagueSchedule'].get('gameDates', [])
                schedule_games = []
                for gd in game_dates:
                    games = gd.get('games', [])
                    schedule_games.extend(games)
            
            if schedule_games:
                # We have scheduled games for today, use those
                seen_game_ids = set()
                for game_dict in schedule_games:
                    game_id = game_dict.get('game_id', '')
                    if game_id and game_id in seen_game_ids:
                        continue
                    seen_game_ids.add(game_id)
                    
                    game_time = game_dict.get('game_time')
                    game_date_str = game_dict.get('game_date', '')
                    
                    # For NBA, convert game_date to Pacific timezone if game_time is available
                    # (game_date from collector may be UTC date, but we want Pacific date)
                    if league.upper() == 'NBA' and game_time:
                        try:
                            if isinstance(game_time, str):
                                from dateutil import parser
                                game_time_obj = parser.parse(game_time)
                            elif hasattr(game_time, 'isoformat'):
                                # Already a datetime object
                                game_time_obj = game_time
                            else:
                                game_time_obj = None
                            
                            if game_time_obj:
                                if game_time_obj.tzinfo is None:
                                    game_time_obj = pytz.UTC.localize(game_time_obj)
                                pacific_tz = pytz.timezone('US/Pacific')
                                game_time_pacific = game_time_obj.astimezone(pacific_tz)
                                game_date_str = game_time_pacific.date().isoformat()
                        except Exception as e:
                            logger.debug(f"Error converting NBA game_date to Pacific: {e}")
                            # Keep original game_date_str
                    
                    games_list.append({
                        "game_id": game_id,
                        "game_date": game_date_str if game_date_str else target_date.isoformat(),
                        "game_time": game_time.isoformat() if game_time else None,
                        "home_team": game_dict.get('home_team', ''),
                        "home_team_abbrev": game_dict.get('home_team_abbrev', ''),
                        "visitor_team": game_dict.get('visitor_team', ''),
                        "visitor_team_abbrev": game_dict.get('visitor_team_abbrev', ''),
                        "game_status": game_dict.get('game_status', 'scheduled'),
                        "game_type": game_dict.get('game_type', 'regular'),
                        "home_wins": game_dict.get('home_wins', 0),
                        "home_losses": game_dict.get('home_losses', 0),
                        "home_otl": game_dict.get('home_otl', 0) if league.upper() == 'NHL' else None,
                        "visitor_wins": game_dict.get('visitor_wins', 0),
                        "visitor_losses": game_dict.get('visitor_losses', 0),
                        "visitor_otl": game_dict.get('visitor_otl', 0) if league.upper() == 'NHL' else None,
                        "current_period": game_dict.get('current_period', ''),
                        "time_remaining": game_dict.get('time_remaining', ''),
                        "home_score_total": game_dict.get('home_score_total', 0),
                        "visitor_score_total": game_dict.get('visitor_score_total', 0),
                        "is_final": game_dict.get('is_final', False),
                    })
            
            # If no scheduled games, try live_scores (may include in-progress games from yesterday)
            # If no scheduled games, try live_scores as fallback
            # For NBA, we'll filter by date to ensure games are actually for today
            if not games_list:
                live_games = collector.get_live_scores(target_date)
                if live_games:
                        # Filter live games to ensure they're actually for today (Pacific time)
                        # This is especially important for NBA where get_live_scores may return games from other dates
                        seen_game_ids = set()
                        pacific_tz = pytz.timezone('US/Pacific')
                        target_date_str = target_date.strftime('%Y-%m-%d')
                        
                        for game_dict in live_games:
                            game_id = game_dict.get('game_id', '')
                            if game_id and game_id in seen_game_ids:
                                continue
                            
                            # For NBA, verify the game is actually for today by checking both game_date and game_time
                            # The game_date field should match, and game_time should be for today in Pacific time
                            if league.upper() == 'NBA':
                                game_date_from_dict = game_dict.get('game_date', '')
                                game_time = game_dict.get('game_time')
                            
                                # First check: game_date should match target_date (this is the actual scheduled date)
                                # If game_date is "2025-11-13" but target is "2025-11-12", skip it
                                if game_date_from_dict:
                                    # Normalize game_date (remove time if present)
                                    game_date_normalized = game_date_from_dict.split()[0] if ' ' in game_date_from_dict else game_date_from_dict
                                    if game_date_normalized != target_date_str:
                                        # Skip games that don't match the target date
                                        continue
                            
                                # Second check: game_time should also be for today in Pacific time
                                if game_time:
                                    try:
                                        from dateutil import parser
                                        if isinstance(game_time, str):
                                            game_time_obj = parser.parse(game_time)
                                        elif hasattr(game_time, 'isoformat'):
                                            game_time_obj = game_time
                                        else:
                                            # Can't parse game_time, skip this game
                                            continue
                                        
                                        if game_time_obj.tzinfo is None:
                                            game_time_obj = pytz.UTC.localize(game_time_obj)
                                        game_time_pacific = game_time_obj.astimezone(pacific_tz)
                                        game_date_pacific = game_time_pacific.date().strftime('%Y-%m-%d')
                                        
                                        # Only include if the game is actually for today
                                        if game_date_pacific != target_date_str:
                                            continue
                                    except Exception as e:
                                        continue
                                elif game_date_from_dict and game_date_from_dict != target_date_str:
                                    # If no game_time but game_date doesn't match, skip
                                    logger.debug(f"Skipping NBA game {game_id} - game_date {game_date_from_dict} doesn't match target {target_date_str}")
                                    continue
                            
                            seen_game_ids.add(game_id)
                            
                            # Get game_time from live data if available
                            game_time = game_dict.get('game_time')
                            game_date_str = game_dict.get('game_date', '')
                            
                            # If game_time is not in live data, try to get it from database
                            if not game_time:
                                with get_db_session() as db:
                                    db_game = db.query(Game).filter(
                                        Game.game_id == game_id
                                    ).first()
                                    if db_game and db_game.game_time:
                                        game_time = db_game.game_time
                            
                            games_list.append({
                                "game_id": game_id,
                                "game_date": game_date_str if game_date_str else target_date.isoformat(),
                                "game_time": game_time.isoformat() if hasattr(game_time, 'isoformat') else (str(game_time) if game_time else None),
                                "home_team": game_dict.get('home_team', ''),
                                "home_team_abbrev": game_dict.get('home_team_abbrev', ''),
                                "visitor_team": game_dict.get('visitor_team', ''),
                                "visitor_team_abbrev": game_dict.get('visitor_team_abbrev', ''),
                                "game_status": game_dict.get('game_status', 'scheduled'),
                                "game_type": game_dict.get('game_type', 'regular'),
                                "home_wins": game_dict.get('home_wins', 0),
                                "home_losses": game_dict.get('home_losses', 0),
                                "home_otl": game_dict.get('home_otl', 0) if league.upper() == 'NHL' else None,
                                "visitor_wins": game_dict.get('visitor_wins', 0),
                                "visitor_losses": game_dict.get('visitor_losses', 0),
                                "visitor_otl": game_dict.get('visitor_otl', 0) if league.upper() == 'NHL' else None,
                                "current_period": game_dict.get('current_period', ''),
                                "time_remaining": game_dict.get('time_remaining', ''),
                                "home_score_total": game_dict.get('home_score_total', 0),
                                "visitor_score_total": game_dict.get('visitor_score_total', 0),
                                "is_final": game_dict.get('is_final', False),
                                "cricket_home_score": game_dict.get('cricket_home_score', ''),
                                "cricket_away_score": game_dict.get('cricket_away_score', ''),
                                "cricket_winner": game_dict.get('cricket_winner', ''),
                                "cricket_result": game_dict.get('cricket_result', ''),
                            })
    
    # For any date (today or past), try get_schedule from collector
    # Also check database for stored games if collector returns nothing
    if not games_list:
        if collector:
            schedule_games = collector.get_schedule(target_date)
            if schedule_games:
                seen_game_ids = set()
                for game_dict in schedule_games:
                    game_id = game_dict.get('game_id', '')
                    if game_id and game_id in seen_game_ids:
                        continue
                    seen_game_ids.add(game_id)
                
                game_time = game_dict.get('game_time')
                game_date_str = game_dict.get('game_date', '')
                
                # For NBA, convert game_date to Pacific timezone if game_time is available
                # (game_date from collector may be UTC date, but we want Pacific date)
                if league.upper() == 'NBA' and game_time:
                    try:
                        if isinstance(game_time, str):
                            from dateutil import parser
                            game_time_obj = parser.parse(game_time)
                        elif hasattr(game_time, 'isoformat'):
                            # Already a datetime object
                            game_time_obj = game_time
                        else:
                            game_time_obj = None
                        
                        if game_time_obj:
                            if game_time_obj.tzinfo is None:
                                game_time_obj = pytz.UTC.localize(game_time_obj)
                            pacific_tz = pytz.timezone('US/Pacific')
                            game_time_pacific = game_time_obj.astimezone(pacific_tz)
                            game_date_str = game_time_pacific.date().isoformat()
                    except Exception as e:
                        logger.debug(f"Error converting NBA game_date to Pacific: {e}")
                        # Keep original game_date_str
                
                games_list.append({
                    "game_id": game_id,
                    "game_date": game_date_str if game_date_str else target_date.isoformat(),
                    "game_time": game_time.isoformat() if hasattr(game_time, 'isoformat') else (str(game_time) if game_time else None),
                    "home_team": game_dict.get('home_team', ''),
                    "home_team_abbrev": game_dict.get('home_team_abbrev', ''),
                    "visitor_team": game_dict.get('visitor_team', ''),
                    "visitor_team_abbrev": game_dict.get('visitor_team_abbrev', ''),
                    "game_status": game_dict.get('game_status', 'scheduled'),
                    "game_type": game_dict.get('game_type', 'regular'),
                    "home_wins": game_dict.get('home_wins', 0),
                    "home_losses": game_dict.get('home_losses', 0),
                    "home_otl": game_dict.get('home_otl', 0) if league.upper() == 'NHL' else None,
                    "visitor_wins": game_dict.get('visitor_wins', 0),
                    "visitor_losses": game_dict.get('visitor_losses', 0),
                    "visitor_otl": game_dict.get('visitor_otl', 0) if league.upper() == 'NHL' else None,
                    "current_period": game_dict.get('current_period', ''),
                    "time_remaining": game_dict.get('time_remaining', ''),
                    "home_score_total": game_dict.get('home_score_total', 0),
                    "visitor_score_total": game_dict.get('visitor_score_total', 0),
                    "is_final": game_dict.get('is_final', False),
                })
    
    # Fallback to database if no collector games found
    # For NBA, filter by Pacific timezone date since games may be stored with UTC dates
    if not games_list:
        with get_db_session() as db:
            if league.upper() == 'NBA':
                # For NBA, games may be stored with UTC dates but we want Pacific dates
                # Get games from target_date and target_date-1 (yesterday) to catch timezone edge cases
                from datetime import timedelta
                yesterday = target_date - timedelta(days=1)
                all_games = db.query(Game).filter(
                    Game.league == league,
                    Game.game_date.in_([target_date, yesterday])
                ).order_by(Game.game_time).all()
                
                # Filter by Pacific timezone date
                pacific_tz = pytz.timezone('US/Pacific')
                games = []
                for game in all_games:
                    if game.game_time:
                        # Convert game_time to Pacific and check date
                        if game.game_time.tzinfo is None:
                            # Assume UTC if no timezone
                            game_time_utc = pytz.UTC.localize(game.game_time)
                        else:
                            game_time_utc = game.game_time
                        game_time_pacific = game_time_utc.astimezone(pacific_tz)
                        if game_time_pacific.date() == target_date:
                            games.append(game)
                    elif game.game_date == target_date:
                        # If no game_time, use game_date (should match)
                        games.append(game)
            else:
                # For other leagues, use simple date match
                games = db.query(Game).filter(
                    Game.league == league,
                    Game.game_date == target_date
                ).order_by(Game.game_time).all()
            
            games_list = [
                {
                    "game_id": game.game_id,
                    "game_date": game.game_date.isoformat(),
                    "game_time": game.game_time.isoformat() if game.game_time else None,
                    "home_team": game.home_team,
                    "home_team_abbrev": game.home_team_abbrev,
                    "visitor_team": game.visitor_team,
                    "visitor_team_abbrev": game.visitor_team_abbrev,
                    "game_status": game.game_status,
                    "game_type": game.game_type,
                    "home_wins": game.home_wins or 0,
                    "home_losses": game.home_losses or 0,
                    "home_otl": game.home_otl if league.upper() == 'NHL' and hasattr(game, 'home_otl') else None,
                    "visitor_wins": game.visitor_wins or 0,
                    "visitor_losses": game.visitor_losses or 0,
                    "visitor_otl": game.visitor_otl if league.upper() == 'NHL' and hasattr(game, 'visitor_otl') else None,
                    "current_period": game.current_period or '',
                    "time_remaining": game.time_remaining or '',
                    "home_score_total": game.home_score_total or 0,
                    "visitor_score_total": game.visitor_score_total or 0,
                    "is_final": game.is_final or False,
                }
                for game in games
            ]
    
    return games_list

def _game_wrapper_to_dict(g, league: str = '') -> Dict[str, Any]:
    """Convert a GameWrapper to a JSON-serializable dict."""
    gt = getattr(g, 'game_time', None)
    gt_str = None
    if gt and hasattr(gt, 'isoformat'):
        gt_str = gt.isoformat()
    gd = getattr(g, 'game_date', '')
    gd_str = gd.isoformat() if hasattr(gd, 'isoformat') else str(gd)
    return {
        "game_id": getattr(g, 'game_id', ''),
        "game_date": gd_str,
        "game_time": gt_str,
        "home_team": getattr(g, 'home_team', ''),
        "home_team_abbrev": getattr(g, 'home_team_abbrev', ''),
        "visitor_team": getattr(g, 'visitor_team', ''),
        "visitor_team_abbrev": getattr(g, 'visitor_team_abbrev', ''),
        "game_status": getattr(g, 'game_status', 'scheduled'),
        "game_type": getattr(g, 'game_type', 'regular'),
        "home_score": getattr(g, 'home_score_total', 0),
        "visitor_score": getattr(g, 'visitor_score_total', 0),
        "is_final": getattr(g, 'is_final', False),
        "current_period": getattr(g, 'current_period', ''),
        "time_remaining": getattr(g, 'time_remaining', ''),
        "home_wins": getattr(g, 'home_wins', 0),
        "home_losses": getattr(g, 'home_losses', 0),
        "home_otl": getattr(g, 'home_otl', None) if league == 'NHL' else None,
        "visitor_wins": getattr(g, 'visitor_wins', 0),
        "visitor_losses": getattr(g, 'visitor_losses', 0),
        "visitor_otl": getattr(g, 'visitor_otl', None) if league == 'NHL' else None,
    }


def _get_games_for_curl(league: str, target_date: date, timezone: pytz.BaseTzInfo) -> List[Any]:
    """Helper function to get games for curl formatting (returns GameWrapper objects)."""
    games = []

    class GameWrapper:
        def __init__(self, data):
            for k, v in data.items():
                setattr(self, k, v)

    collector = get_collector(league)
    if not collector:
        return games

    now_tz = datetime.now(timezone)
    today = now_tz.date()

    def _fetch():
        raw = collector.get_live_scores(target_date) or collector.get_schedule(target_date) or []
        # For past dates, if the API returns games with no scores/status (empty shells),
        # discard them so the DB fallback is used instead
        if target_date < today and raw:
            has_real_data = any(
                g.get('is_final') or g.get('game_status') in ('final', 'in_progress')
                or (g.get('home_score_total') or 0) > 0 or (g.get('visitor_score_total') or 0) > 0
                for g in raw
            )
            if not has_real_data:
                return []
        return raw

    raw_games = _get_cached_games(league, target_date, _fetch)

    if raw_games:
        seen_game_ids = set()
        for game_dict in raw_games:
                    game_id = game_dict.get('game_id', '')
                    if game_id and game_id in seen_game_ids:
                        continue  # Skip duplicates
                    seen_game_ids.add(game_id)
                    
                    # Skip games with empty team names (both abbreviation and full name)
                    home_team = game_dict.get('home_team', '')
                    home_abbrev = game_dict.get('home_team_abbrev', '')
                    visitor_team = game_dict.get('visitor_team', '')
                    visitor_abbrev = game_dict.get('visitor_team_abbrev', '')
                    
                    if (not home_team or home_team.strip() == '') and (not home_abbrev or home_abbrev.strip() == ''):
                        continue
                    if (not visitor_team or visitor_team.strip() == '') and (not visitor_abbrev or visitor_abbrev.strip() == ''):
                        continue
                    
                    # Get game_time from database if not in live data
                    game_time = game_dict.get('game_time')
                    if not game_time:
                        with get_db_session() as db:
                            db_game = db.query(Game).filter(
                                Game.game_id == game_id
                            ).first()
                            if db_game and db_game.game_time:
                                game_time = db_game.game_time
                    
                    # Ensure time_remaining is included from live data
                    time_remaining = game_dict.get('time_remaining', '')
                    current_period = game_dict.get('current_period', '')
                    
                    game_data = {
                        'league': league,
                        'game_id': game_id,
                        'game_date': datetime.strptime(game_dict.get('game_date', ''), '%Y-%m-%d').date() if game_dict.get('game_date') else target_date,
                        'game_time': game_time,
                        'game_type': game_dict.get('game_type', 'regular'),
                        'home_team': game_dict.get('home_team', ''),
                        'home_team_abbrev': game_dict.get('home_team_abbrev', ''),
                        'visitor_team': game_dict.get('visitor_team', ''),
                        'visitor_team_abbrev': game_dict.get('visitor_team_abbrev', ''),
                        'home_score_total': int(game_dict.get('home_score_total', 0) or 0),
                        'visitor_score_total': int(game_dict.get('visitor_score_total', 0) or 0),
                        'game_status': game_dict.get('game_status', 'scheduled'),
                        'current_period': current_period,
                        'time_remaining': time_remaining,
                        'is_final': game_dict.get('is_final', False),
                        'home_wins': int(game_dict.get('home_wins', 0) or 0),
                        'home_losses': int(game_dict.get('home_losses', 0) or 0),
                        'home_otl': int(game_dict.get('home_otl', 0) or 0),
                        'visitor_wins': int(game_dict.get('visitor_wins', 0) or 0),
                        'visitor_losses': int(game_dict.get('visitor_losses', 0) or 0),
                        'visitor_otl': int(game_dict.get('visitor_otl', 0) or 0),
                        'cricket_status': game_dict.get('cricket_status', ''),
                        'cricket_venue': game_dict.get('cricket_venue', ''),
                        'cricket_start_time': game_dict.get('cricket_start_time', {}),
                        'cricket_home_nr': int(game_dict.get('cricket_home_nr', 0) or 0),
                        'cricket_away_nr': int(game_dict.get('cricket_away_nr', 0) or 0),
                        'cricket_home_score': game_dict.get('cricket_home_score', ''),
                        'cricket_away_score': game_dict.get('cricket_away_score', ''),
                        'cricket_winner': game_dict.get('cricket_winner', ''),
                        'cricket_result': game_dict.get('cricket_result', ''),
                        'cricket_away_outcome': game_dict.get('cricket_away_outcome', ''),
                        'home_draws': int(game_dict.get('home_draws', 0) or 0),
                        'visitor_draws': int(game_dict.get('visitor_draws', 0) or 0),
                        'mls_detail': game_dict.get('mls_detail', ''),
                    }
                    games.append(GameWrapper(game_data))

    # Fallback to database ONLY if no collector games were found
    if not games:
        with get_db_session() as db:
            db_games = db.query(Game).filter(
                Game.league == league,
                Game.game_date == target_date
            ).order_by(Game.game_time).all()
            
            # Convert while session is still open
            for game in db_games:
                # Skip games with empty team names (both abbreviation and full name)
                if (not game.home_team or game.home_team.strip() == '') and (not game.home_team_abbrev or game.home_team_abbrev.strip() == ''):
                    continue
                if (not game.visitor_team or game.visitor_team.strip() == '') and (not game.visitor_team_abbrev or game.visitor_team_abbrev.strip() == ''):
                    continue
                
                # Ensure time_remaining is properly extracted
                time_remaining = game.time_remaining or ''
                current_period = game.current_period or ''
                
                game_data = {
                    'league': game.league,
                    'game_id': game.game_id,
                    'game_date': game.game_date,
                    'game_time': game.game_time,
                    'game_type': game.game_type,
                    'home_team': game.home_team,
                    'home_team_abbrev': game.home_team_abbrev,
                    'visitor_team': game.visitor_team,
                    'visitor_team_abbrev': game.visitor_team_abbrev,
                    'home_score_total': game.home_score_total or 0,
                    'visitor_score_total': game.visitor_score_total or 0,
                    'game_status': game.game_status,
                    'current_period': current_period,
                    'time_remaining': time_remaining,
                    'is_final': game.is_final or False,
                    'home_wins': game.home_wins or 0,
                    'home_losses': game.home_losses or 0,
                    'home_otl': getattr(game, 'home_otl', 0) or 0,
                    'visitor_wins': game.visitor_wins or 0,
                    'visitor_losses': game.visitor_losses or 0,
                    'visitor_otl': getattr(game, 'visitor_otl', 0) or 0,
                }
                games.append(GameWrapper(game_data))
    
    return games


@app.get("/curl/v1/schedule/{sport}/{date}", response_class=PlainTextResponse)
def get_schedule_curl_v1(
    sport: str = Path(..., description="Sport (nba, mlb, nfl, nhl, wnba, all)"),
    date: str = Path(..., description="Date: today/tomorrow/yesterday, YYYY-MM-DD, YYYYMMDD, M/D/YYYY, MM/DD/YYYY, or other formats"),
    tz: Optional[str] = Query(None, description="Timezone: et/est/eastern, pt/pst/pdt/pacific, akst/alaska, hst/hawaii (default: Pacific)"),
):
    """Get schedule in curl-style text format."""
    try:
        timezone = get_timezone(tz)
        # Parse date using the timezone (so "today" is in the correct timezone)
        target_date = parse_date_param(date, timezone)
        sport_lower = sport.lower()
        
        # Handle 'all' sport - aggregate from all sports
        if sport_lower == 'all':
            all_games = []
            for sport_key in SPORT_MAPPINGS.keys():
                league = SPORT_MAPPINGS[sport_key]
                games = _get_games_for_curl(league, target_date, timezone)
                all_games.extend(games)
            
            return format_schedule_curl(all_games, target_date, timezone, show_all_sports=True)

        
        # Single sport logic
        league = SPORT_MAPPINGS.get(sport_lower)
        if not league:
            raise HTTPException(status_code=400, detail=f"Invalid sport: {sport}")
        
        games = _get_games_for_curl(league, target_date, timezone)
        
        # Deduplicate games by game_id before formatting
        seen_game_ids = set()
        unique_games = []
        for game in games:
            game_id = getattr(game, 'game_id', None) or getattr(game, 'gameId', None)
            if game_id:
                if game_id not in seen_game_ids:
                    seen_game_ids.add(game_id)
                    unique_games.append(game)
                # Skip duplicates
            else:
                # If no game_id, include it (shouldn't happen)
                unique_games.append(game)
        
        return format_schedule_curl(unique_games, target_date, timezone)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/scores/{sport}/{date}")
def get_scores_api_v1(
    sport: str = Path(..., description="Sport (nba, mlb, nfl, nhl, wnba, all)"),
    date: str = Path(..., description="Date: today/tomorrow/yesterday, YYYY-MM-DD, YYYYMMDD, M/D/YYYY, MM/DD/YYYY, or other formats"),
    tz: Optional[str] = Query(None, description="Timezone for relative dates (default: Pacific)"),
):
    """Get scores in JSON format."""
    try:
        timezone = get_timezone(tz)
        target_date = parse_date_param(date, timezone)
        sport_lower = sport.lower()

        if sport_lower == 'all':
            all_scores = []
            for sport_key in SPORT_MAPPINGS.keys():
                league = SPORT_MAPPINGS[sport_key]
                games = _get_games_for_curl(league, target_date, timezone)
                for g in games:
                    d = _game_wrapper_to_dict(g, league)
                    d['sport'] = sport_key
                    all_scores.append(d)
            return {"sport": "all", "date": target_date.isoformat(), "scores": all_scores}

        league = SPORT_MAPPINGS.get(sport_lower)
        if not league:
            raise HTTPException(status_code=400, detail=f"Invalid sport: {sport}")

        games = _get_games_for_curl(league, target_date, timezone)
        return {
            "sport": sport,
            "date": target_date.isoformat(),
            "scores": [_game_wrapper_to_dict(g, league) for g in games]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def _get_scores_for_league(league: str, target_date: date) -> List[Dict[str, Any]]:
    """Helper function to get scores for a specific league."""
    from datetime import datetime
    now_tz = datetime.now(pytz.timezone('US/Pacific'))
    today = now_tz.date()
    
    # Get live scores from collector (includes in-progress and final games)
    collector = get_collector(league)
    if collector:
        # For today, try live scores first
        if target_date == today:
            live_games = collector.get_live_scores(target_date)
            if live_games:
                return [
                    {
                        "game_id": game.get('game_id', ''),
                        "home_team": game.get('home_team', ''),
                        "home_score": game.get('home_score_total', 0),
                        "visitor_team": game.get('visitor_team', ''),
                        "visitor_score": game.get('visitor_score_total', 0),
                        "is_final": game.get('is_final', False),
                        "game_status": game.get('game_status', 'scheduled'),
                        "current_period": game.get('current_period', ''),
                        "time_remaining": game.get('time_remaining', ''),
                        "home_wins": game.get('home_wins', 0),
                        "home_losses": game.get('home_losses', 0),
                        "home_otl": game.get('home_otl', 0) if league.upper() == 'NHL' else None,
                        "visitor_wins": game.get('visitor_wins', 0),
                        "visitor_losses": game.get('visitor_losses', 0),
                        "visitor_otl": game.get('visitor_otl', 0) if league.upper() == 'NHL' else None,
                        "cricket_home_score": game.get('cricket_home_score', ''),
                        "cricket_away_score": game.get('cricket_away_score', ''),
                        "cricket_winner": game.get('cricket_winner', ''),
                        "cricket_result": game.get('cricket_result', ''),
                    }
                    for game in live_games
                ]
        
        # For past or future dates, try get_schedule
        schedule_games = collector.get_schedule(target_date)
        if schedule_games:
            # For past dates, filter for games that have scores (final or in-progress)
            # For future dates, return all scheduled games
            if target_date < today:
                scored_games = [
                    game for game in schedule_games
                    if game.get('is_final') or game.get('home_score_total', 0) > 0 or game.get('visitor_score_total', 0) > 0
                ]
                if scored_games:
                    return [
                        {
                            "game_id": game.get('game_id', ''),
                            "home_team": game.get('home_team', ''),
                            "home_score": game.get('home_score_total', 0),
                            "visitor_team": game.get('visitor_team', ''),
                            "visitor_score": game.get('visitor_score_total', 0),
                            "is_final": game.get('is_final', False),
                            "game_status": game.get('game_status', 'scheduled'),
                            "current_period": game.get('current_period', ''),
                            "time_remaining": game.get('time_remaining', ''),
                            "home_wins": game.get('home_wins', 0),
                            "home_losses": game.get('home_losses', 0),
                            "home_otl": game.get('home_otl', 0) if league.upper() == 'NHL' else None,
                            "visitor_wins": game.get('visitor_wins', 0),
                            "visitor_losses": game.get('visitor_losses', 0),
                            "visitor_otl": game.get('visitor_otl', 0) if league.upper() == 'NHL' else None,
                        }
                        for game in scored_games
                    ]
            else:
                # Future dates - return scheduled games (may not have scores yet)
                return [
                    {
                        "game_id": game.get('game_id', ''),
                        "home_team": game.get('home_team', ''),
                        "home_score": game.get('home_score_total', 0),
                        "visitor_team": game.get('visitor_team', ''),
                        "visitor_score": game.get('visitor_score_total', 0),
                        "is_final": game.get('is_final', False),
                        "game_status": game.get('game_status', 'scheduled'),
                        "current_period": game.get('current_period', ''),
                        "time_remaining": game.get('time_remaining', ''),
                        "home_wins": game.get('home_wins', 0),
                        "home_losses": game.get('home_losses', 0),
                        "home_otl": game.get('home_otl', 0) if league.upper() == 'NHL' else None,
                        "visitor_wins": game.get('visitor_wins', 0),
                        "visitor_losses": game.get('visitor_losses', 0),
                        "visitor_otl": game.get('visitor_otl', 0) if league.upper() == 'NHL' else None,
                    }
                    for game in schedule_games
                ]
    
    # Fallback to database
    with get_db_session() as db:
        games = db.query(Game).filter(
            Game.league == league,
            Game.game_date == target_date
        ).all()
        
        return [
            {
                "game_id": game.game_id,
                "home_team": game.home_team,
                "home_score": game.home_score_total,
                "visitor_team": game.visitor_team,
                "visitor_score": game.visitor_score_total,
                "is_final": game.is_final,
                "game_status": game.game_status,
                "current_period": game.current_period,
                "time_remaining": game.time_remaining,
                "home_wins": game.home_wins or 0,
                "home_losses": game.home_losses or 0,
                "home_otl": game.home_otl if league.upper() == 'NHL' and hasattr(game, 'home_otl') else None,
                "visitor_wins": game.visitor_wins or 0,
                "visitor_losses": game.visitor_losses or 0,
                "visitor_otl": game.visitor_otl if league.upper() == 'NHL' and hasattr(game, 'visitor_otl') else None,
            }
            for game in games
        ]


@app.get("/curl/v1/scores/{sport}/{date}", response_class=PlainTextResponse)
def get_scores_curl_v1(
    sport: str = Path(..., description="Sport (nba, mlb, nfl, nhl, wnba, all)"),
    date: str = Path(..., description="Date: today/tomorrow/yesterday, YYYY-MM-DD, YYYYMMDD, M/D/YYYY, MM/DD/YYYY, or other formats"),
    tz: Optional[str] = Query(None, description="Timezone: et/est/eastern, pt/pst/pdt/pacific (default: Pacific)"),
):
    """Get scores in curl-style text format."""
    try:
        timezone = get_timezone(tz)
        # Parse date using the timezone (so "today" is in the correct timezone)
        target_date = parse_date_param(date, timezone)
        sport_lower = sport.lower()
        
        # Handle 'all' sport - aggregate from all sports
        if sport_lower == 'all':
            all_games = []
            for sport_key in SPORT_MAPPINGS.keys():
                league = SPORT_MAPPINGS[sport_key]
                games = _get_games_for_curl(league, target_date, timezone)
                all_games.extend(games)
            
            return format_scores_curl(all_games, target_date, timezone, show_all_sports=True)

        
        # Single sport logic
        league = SPORT_MAPPINGS.get(sport_lower)
        if not league:
            raise HTTPException(status_code=400, detail=f"Invalid sport: {sport}")
        
        # Always try to get live scores first (for today's games, always use live data)
        collector = get_collector(league)
        game_objects = []
        
        if collector:
            live_games = collector.get_live_scores(target_date)
            if live_games:
                # Convert dicts to Game-like objects for formatting
                class GameWrapper:
                    def __init__(self, data):
                        for k, v in data.items():
                            setattr(self, k, v)
                
                # Use a set to track game_ids and avoid duplicates
                seen_game_ids = set()
                for game_dict in live_games:
                    game_id = game_dict.get('game_id', '')
                    if game_id and game_id in seen_game_ids:
                        continue  # Skip duplicates
                    seen_game_ids.add(game_id)
                    
                    game_data = {
                        'league': league,
                        'game_id': game_id,
                        'game_date': datetime.strptime(game_dict.get('game_date', ''), '%Y-%m-%d').date() if game_dict.get('game_date') else target_date,
                        'home_team': game_dict.get('home_team', ''),
                        'home_team_abbrev': game_dict.get('home_team_abbrev', ''),
                        'visitor_team': game_dict.get('visitor_team', ''),
                        'visitor_team_abbrev': game_dict.get('visitor_team_abbrev', ''),
                        'home_score_total': int(game_dict.get('home_score_total', 0) or 0),
                        'visitor_score_total': int(game_dict.get('visitor_score_total', 0) or 0),
                        'game_status': game_dict.get('game_status', 'scheduled'),
                        'current_period': game_dict.get('current_period', ''),
                        'time_remaining': game_dict.get('time_remaining', ''),
                        'is_final': game_dict.get('is_final', False),
                        'game_type': game_dict.get('game_type', 'regular')
                    }
                    game_objects.append(GameWrapper(game_data))
        
        # If we have live games, use them (they're always more up-to-date)
        if game_objects:
            return format_scores_curl(game_objects, target_date, timezone)

        
        # Fallback to database only if no live games
        with get_db_session() as db:
            games = db.query(Game).filter(
                Game.league == league,
                Game.game_date == target_date
            ).all()
            
            # Deduplicate database games
            seen_game_ids = set()
            unique_games = []
            for game in games:
                if game.game_id and game.game_id not in seen_game_ids:
                    seen_game_ids.add(game.game_id)
                    unique_games.append(game)
                elif not game.game_id:
                    unique_games.append(game)
            
            return format_scores_curl(unique_games, target_date, timezone)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/standings/{sport}")
def get_standings_api_v1(
    sport: str = Path(..., description="Sport (nba, mlb, nfl, nhl, wnba, all)"),
):
    """Get standings in JSON format."""
    sport_lower = sport.lower()
    
    # Handle 'all' sport
    if sport_lower == 'all':
        # TODO: Implement standings endpoint for all sports
        return {
            "sport": "all",
            "message": "Standings endpoint - TODO",
            "note": "When implemented, this will return standings for all sports"
        }
    
    # Validate single sport
    if sport_lower not in SPORT_MAPPINGS:
        raise HTTPException(status_code=400, detail=f"Invalid sport: {sport}")
    
    # TODO: Implement standings endpoint
    return {
        "sport": sport,
        "message": "Standings endpoint - TODO"
    }


@app.get("/curl/v1/standings/{sport}", response_class=PlainTextResponse)
def get_standings_curl_v1(
    sport: str = Path(..., description="Sport (nba, mlb, nfl, nhl, wnba, all)"),
):
    """Get standings in curl-style text format."""
    sport_lower = sport.lower()
    
    # Handle 'all' sport
    if sport_lower == 'all':
        # TODO: Implement standings endpoint for all sports
        return "Standings endpoint - TODO\nWhen implemented, this will return standings for all sports\n"
    
    # Validate single sport
    if sport_lower not in SPORT_MAPPINGS:
        raise HTTPException(status_code=400, detail=f"Invalid sport: {sport}")
    
    # TODO: Implement standings endpoint
    return f"Standings endpoint - TODO\nSport: {sport}\n"


# Season info cache: {league: {'data': ..., 'timestamp': float}}
_season_info_cache: Dict[str, Any] = {}
_SEASON_INFO_TTL = 86400  # 24 hours


def _get_season_info_from_db(league: str) -> Optional[Dict[str, Any]]:
    """Derive season phase dates from game records in the database."""
    from sqlalchemy import func
    try:
        with get_db_session() as db:
            rows = db.query(
                Game.game_type,
                func.min(Game.game_date).label('start_date'),
                func.max(Game.game_date).label('end_date'),
            ).filter(
                Game.league == league,
            ).group_by(Game.game_type).all()

            if not rows:
                return None

            type_display = {
                'preseason': 'Preseason',
                'regular': 'Regular Season',
                'playoffs': 'Post Season (Playoffs)',
                'postseason': 'Post Season (Playoffs)',
                'allstar': 'All-Star',
                'nba_cup': 'Emirates NBA Cup',
            }
            type_order = ['preseason', 'regular', 'allstar', 'nba_cup', 'playoffs', 'postseason']

            season_types = []
            latest_year = None
            for game_type, start_d, end_d in rows:
                if game_type in ('postseason',) and any(r[0] == 'playoffs' for r in rows):
                    continue
                name = type_display.get(game_type, game_type.title().replace('_', ' '))
                season_types.append({
                    'name': name,
                    'start_date': start_d.isoformat(),
                    'end_date': end_d.isoformat(),
                    'game_type': game_type,
                })
                if latest_year is None or end_d.year > latest_year:
                    latest_year = end_d.year

            season_types.sort(key=lambda x: type_order.index(x['game_type']) if x['game_type'] in type_order else 99)
            for t in season_types:
                del t['game_type']

            today = datetime.now().strftime('%Y-%m-%d')
            current_phase = 'Off Season'
            for t in season_types:
                if t['start_date'] <= today <= t['end_date']:
                    current_phase = t['name']

            return {
                'year': latest_year or datetime.now().year,
                'current_phase': current_phase,
                'season_types': season_types,
            }
    except Exception as e:
        logger.error(f"Error deriving season info from DB for {league}: {e}")
        return None


@app.get("/api/v1/season-info/{league}")
def get_season_info(
    league: str = Path(..., description="League (mlb, nba, nfl, nhl, wnba)"),
):
    """Get season phase dates for a league."""
    league_upper = league.upper()

    valid_leagues = set(v for v in SPORT_MAPPINGS.values())
    if league_upper not in valid_leagues:
        raise HTTPException(status_code=400, detail=f"Invalid league: {league}")

    import time as _time
    cached = _season_info_cache.get(league_upper)
    if cached and (_time.time() - cached['timestamp'] < _SEASON_INFO_TTL):
        return cached['data']

    collector = get_collector(league_upper)
    result = None

    if collector:
        result = collector.get_season_info()

    if not result:
        result = _get_season_info_from_db(league_upper)

    if not result:
        result = {"year": datetime.now().year, "current_phase": "Off Season", "season_types": []}

    _season_info_cache[league_upper] = {'data': result, 'timestamp': _time.time()}
    return result


@app.get("/health")
def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}


# Catch-all routes for unknown /api/ and /curl/ paths - return help
@app.get("/api/v1/debug/{sport}/{date}")
def debug_schedule_data(
    sport: str = Path(..., description="Sport (nba, mlb, nfl, nhl, wnba)"),
    date: str = Path(..., description="Date: today/tomorrow/yesterday, YYYY-MM-DD, etc."),
    tz: Optional[str] = Query(None, description="Timezone for relative dates (default: Pacific)"),
):
    """Debug endpoint to see raw schedule data."""
    try:
        timezone = get_timezone(tz)
        target_date = parse_date_param(date, timezone)
        sport_lower = sport.lower()
        league = SPORT_MAPPINGS.get(sport_lower)
        
        if not league:
            raise HTTPException(status_code=400, detail=f"Invalid sport: {sport}")
        
        # Get data from collector
        collector = get_collector(league)
        collector_data = []
        raw_api_team_data = []  # For NHL, show raw team structure
        if collector:
            collector_data = collector.get_schedule(target_date)
            # For NHL, also get raw API response to inspect team structure
            if sport_lower == 'nhl' and collector_data:
                # Get raw API response by making a direct call
                try:
                    from datetime import datetime
                    date_str = target_date.strftime('%Y-%m-%d')
                    url = f"https://api-web.nhle.com/v1/schedule/{date_str}"
                    import requests
                    response = requests.get(url, timeout=10)
                    if response.status_code == 200:
                        data = response.json()
                        # Extract first game's team structure if available
                        if 'gameWeek' in data and len(data['gameWeek']) > 0:
                            for day in data['gameWeek']:
                                if 'games' in day and len(day['games']) > 0:
                                    first_game = day['games'][0]
                                    raw_api_team_data = {
                                        "homeTeam_keys": list(first_game.get('homeTeam', {}).keys()) if 'homeTeam' in first_game else [],
                                        "awayTeam_keys": list(first_game.get('awayTeam', {}).keys()) if 'awayTeam' in first_game else [],
                                        "homeTeam_sample": {k: v for k, v in first_game.get('homeTeam', {}).items() if k in ['wins', 'losses', 'otLosses', 'ot_losses', 'overtimeLosses', 'ot', 'otl']} if 'homeTeam' in first_game else {},
                                        "awayTeam_sample": {k: v for k, v in first_game.get('awayTeam', {}).items() if k in ['wins', 'losses', 'otLosses', 'ot_losses', 'overtimeLosses', 'ot', 'otl']} if 'awayTeam' in first_game else {},
                                    }
                                    break
                except Exception as e:
                    raw_api_team_data = {"error": str(e)}
        
        # Get data from live scores
        live_data = []
        if collector:
            live_data = collector.get_live_scores(target_date)
        
        # Get data from database
        db_data = []
        with get_db_session() as db:
            db_games = db.query(Game).filter(
                Game.league == league,
                Game.game_date == target_date
            ).order_by(Game.game_time).all()
            
            db_data = [
                {
                    "game_id": game.game_id,
                    "home_team": game.home_team,
                    "home_team_abbrev": game.home_team_abbrev,
                    "visitor_team": game.visitor_team,
                    "visitor_team_abbrev": game.visitor_team_abbrev,
                    "home_wins": game.home_wins,
                    "home_losses": game.home_losses,
                    "visitor_wins": game.visitor_wins,
                    "visitor_losses": game.visitor_losses,
                    "game_time": game.game_time.isoformat() if game.game_time else None,
                    "game_status": game.game_status,
                }
                for game in db_games
            ]
        
        # Get what _get_games_for_curl would return
        curl_games = _get_games_for_curl(league, target_date, timezone)
        curl_data = []
        for game in curl_games:
            curl_data.append({
                "game_id": getattr(game, 'game_id', None),
                "home_team": getattr(game, 'home_team', None),
                "home_team_abbrev": getattr(game, 'home_team_abbrev', None),
                "visitor_team": getattr(game, 'visitor_team', None),
                "visitor_team_abbrev": getattr(game, 'visitor_team_abbrev', None),
                "home_wins": getattr(game, 'home_wins', None),
                "home_losses": getattr(game, 'home_losses', None),
                "visitor_wins": getattr(game, 'visitor_wins', None),
                "visitor_losses": getattr(game, 'visitor_losses', None),
                "game_time": getattr(game, 'game_time', None),
                "league": getattr(game, 'league', None),
            })
        
        result = {
            "sport": sport,
            "league": league,
            "date": target_date.isoformat(),
            "collector_schedule": collector_data[:3] if collector_data else [],  # First 3 games
            "collector_live_scores": live_data[:3] if live_data else [],  # First 3 games
            "database_games": db_data[:3] if db_data else [],  # First 3 games
            "curl_format_games": curl_data[:3] if curl_data else [],  # First 3 games
            "counts": {
                "collector_schedule": len(collector_data),
                "collector_live_scores": len(live_data),
                "database_games": len(db_data),
                "curl_format_games": len(curl_data),
            }
        }
        # Add raw API team structure for NHL
        if sport_lower == 'nhl' and raw_api_team_data:
            result["raw_api_team_structure"] = raw_api_team_data
        
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/{path:path}")
def api_catch_all(path: str):
    """Catch-all for unknown /api/ paths - returns JSON help."""
    return get_help_json()


@app.get("/curl/{path:path}", response_class=PlainTextResponse)
def curl_catch_all(path: str):
    """Catch-all for unknown /curl/ paths - returns plain text help."""
    return get_help_text()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=settings.api_host, port=settings.api_port)
