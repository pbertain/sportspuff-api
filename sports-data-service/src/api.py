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
from collectors import NBACollector, MLBCollector, NHLCollector, NFLCollector, WNBACollector

def get_collector(league: str):
    """Get collector instance for a league."""
    collectors = {
        'NBA': NBACollector(),
        'MLB': MLBCollector(),
        'NHL': NHLCollector(),
        'NFL': NFLCollector(),
        'WNBA': WNBACollector(),
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
    'wnba': 'WNBA'
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
                "sports": ["nba", "mlb", "nfl", "nhl", "wnba", "all"],
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
                "sports": ["nba", "mlb", "nfl", "nhl", "wnba", "all"],
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
                "sports": ["nba", "mlb", "nfl", "nhl", "wnba", "all"],
                "note": "Standings endpoint is currently under development"
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

SPORTS:
  nba, mlb, nfl, nhl, wnba, all
  
  Use 'all' to get data for all sports combined

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
        
        <h2>Sports</h2>
        <p>
            <span class="sport-list">nba</span>
            <span class="sport-list">mlb</span>
            <span class="sport-list">nfl</span>
            <span class="sport-list">nhl</span>
            <span class="sport-list">wnba</span>
            <span class="sport-list">all</span>
        </p>
        <p><strong>Note:</strong> Use <code>all</code> as the sport parameter to get data for all sports combined.</p>
        
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
curl http://localhost:34180/api/v1/standings/nba</pre>
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
        return "Good God! It's so early (or late!)!!"
    elif 5 <= hour < 12:
        return "Good morning"
    elif 12 <= hour < 17:
        return "Good afternoon"
    elif 17 <= hour < 24:
        return "Good evening"
    else:
        return "Good God! It's so early (or late!)!!"

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

def format_game_for_curl(game: Game, sport: str) -> str:
    """Format a single game for curl-style output."""
    # Format team names with wins/losses (handle None values)
    visitor_wins = game.visitor_wins or 0
    visitor_losses = game.visitor_losses or 0
    home_wins = game.home_wins or 0
    home_losses = game.home_losses or 0
    
    # Use abbreviation if available, otherwise fall back to full team name (first 3 chars)
    visitor_abbrev = game.visitor_team_abbrev
    if not visitor_abbrev or visitor_abbrev.strip() == '':
        # Fallback to first 3 characters of team name, or full name if shorter
        visitor_abbrev = (game.visitor_team or '???')[:3].upper()
    
    home_abbrev = game.home_team_abbrev
    if not home_abbrev or home_abbrev.strip() == '':
        # Fallback to first 3 characters of team name, or full name if shorter
        home_abbrev = (game.home_team or '???')[:3].upper()
    
    # For NHL, format records as W-L-OTL (overtime losses)
    if sport.lower() == 'nhl':
        visitor_otl = getattr(game, 'visitor_otl', 0) or 0
        home_otl = getattr(game, 'home_otl', 0) or 0
        away_team = f"{visitor_abbrev} [{visitor_wins:3d}-{visitor_losses:2d}-{visitor_otl:2d}]"
        home_team = f"{home_abbrev} [{home_wins:3d}-{home_losses:2d}-{home_otl:2d}]"
    else:
        away_team = f"{visitor_abbrev} [{visitor_wins:3d}-{visitor_losses:2d}]"
        home_team = f"{home_abbrev} [{home_wins:3d}-{home_losses:2d}]"
    
    # Format time/status
    # Priority: Show scores if game is final or in progress, otherwise show scheduled time
    if game.is_final:
        if game.home_score_total is not None and game.visitor_score_total is not None:
            time_status = f"({game.visitor_score_total:2d}-{game.home_score_total:2d}) F"
        else:
            time_status = "F"
    elif game.game_status == 'in_progress' or (game.visitor_score_total and game.visitor_score_total > 0) or (game.home_score_total and game.home_score_total > 0):
        # Game is in progress or has a score - show the score in schedule format
        period = game.current_period or '?'
        time_left = game.time_remaining or ''
        
        if sport.lower() == 'nhl':
            # For NHL: Period 4+ is overtime (OT), format as "P{period} MM:SS" (matching NBA format)
            try:
                period_num = int(period) if str(period).isdigit() else 0
                if period_num >= 4:
                    period_display = 'OT'
                else:
                    period_display = f'P{period_num}'
            except (ValueError, TypeError):
                period_display = f'P{period}'
            
            # Always show time if available, otherwise just show period
            if time_left and time_left.strip():
                # Format: (score-score) P1 MM:SS or OT MM:SS (no dash, matching NBA)
                time_status = f"({game.visitor_score_total or 0:2d}-{game.home_score_total or 0:2d}) {period_display} {time_left}"
            else:
                # If no time available, still show period
                time_status = f"({game.visitor_score_total or 0:2d}-{game.home_score_total or 0:2d}) {period_display}"
        else:
            # For other sports, use 'Q' for Quarter
            period_prefix = 'Q'
            # Check if it's halftime (period 2, time 0:00, and status is in_progress)
            is_halftime = (period == '2' and time_left in ('0:00', '') and 
                          game.game_status == 'in_progress')
            if is_halftime:
                time_status = f"({game.visitor_score_total or 0:2d}-{game.home_score_total or 0:2d}) HT"
            elif time_left and time_left.strip():
                time_status = f"({game.visitor_score_total or 0:2d}-{game.home_score_total or 0:2d}) {period_prefix}{period} {time_left}"
            else:
                time_status = f"({game.visitor_score_total or 0:2d}-{game.home_score_total or 0:2d}) {period_prefix}{period}"
    else:
        # Scheduled game - show time
        if game.game_time:
            # Convert to Pacific time
            pt = pytz.timezone('America/Los_Angeles')
            game_time_pt = game.game_time.astimezone(pt)
            time_status = game_time_pt.strftime('%H:%M')
        else:
            time_status = "TBD"
    
    if game.game_status == 'in_progress':
        return f" {away_team}@{home_team} {time_status}"
    else:
        return f" {away_team}@{home_team} {time_status}"

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

def format_schedule_curl(games: List[Game], target_date: date, tz: pytz.BaseTzInfo = None, show_all_sports: bool = False) -> str:
    """Format games in curl-style schedule format.
    
    Args:
        games: List of games to format
        target_date: Target date for the schedule
        tz: Timezone for display
        show_all_sports: If True, show all sports even if they have no games
    """
    if tz is None:
        tz = pytz.timezone('US/Pacific')  # Default to Pacific
    
    # If no games and not showing all sports, return early
    if not games and not show_all_sports:
        return "No games scheduled"
    
    # Group by sport
    by_sport: Dict[str, List[Game]] = {}
    for game in games:
        sport = game.league.lower()
        if sport not in by_sport:
            by_sport[sport] = []
        by_sport[sport].append(game)
    
    # Format the output
    greeting = get_greeting(tz)
    date_str = target_date.strftime('%a %d %b %Y')
    
    output = f"{greeting}!\nHere is the schedule:\n"
    output += f"       {date_str}:\n"
    output += "-" * 30 + "\n"
    
    # Sort sports by custom order
    sport_order = ['mlb', 'wnba', 'nba', 'nfl', 'nhl']
    sport_to_league = {
        'mlb': 'MLB',
        'wnba': 'WNBA',
        'nba': 'NBA',
        'nfl': 'NFL',
        'nhl': 'NHL'
    }
    
    for sport in sport_order:
        # If show_all_sports is False, skip sports with no games
        if not show_all_sports and sport not in by_sport:
            continue
        
        sport_games = by_sport.get(sport, [])
        
        # Get season info - either from games or from database
        if sport_games:
            # Determine season info from first game
            first_game = sport_games[0]
            # Map game_type to display format
            game_type_map = {
                'preseason': 'Preseason',
                'regular': 'Regular Season',
                'playoffs': 'Playoffs',
                'allstar': 'All-Star',
                'nba_cup': 'Emirates NBA Cup',
                'postseason': 'Playoffs'
            }
            season_type = game_type_map.get(first_game.game_type.lower(), first_game.game_type.title().replace('_', ' '))
            league_name = first_game.league
        else:
            # No games for this sport - try to get season info from database
            league_name = sport_to_league.get(sport, sport.upper())
            season_type = _get_season_type_for_sport(sport, target_date)
        
        output += f"{league_name} - {season_type}:\n"
        
        if sport_games:
            for game in sport_games:
                output += format_game_for_curl(game, sport)
                output += "\n"
        else:
            output += " No games scheduled\n"
        
        output += "-" * 30 + "\n"
    
    # Determine timezone display name
    tz_name = tz.zone if hasattr(tz, 'zone') else str(tz)
    
    # Try to extract a readable timezone name
    # Common patterns: America/New_York -> Eastern, Europe/London -> London, etc.
    if '/' in tz_name:
        # Extract the city/region name (e.g., "New_York" from "America/New_York")
        parts = tz_name.split('/')
        if len(parts) > 1:
            city_name = parts[-1].replace('_', ' ')
            # Map common timezone names to shorter display names
            display_map = {
                'New York': 'Eastern',
                'Los Angeles': 'Pacific',
                'Chicago': 'Central',
                'Denver': 'Mountain',
                'Anchorage': 'Alaska',
                'Honolulu': 'Hawaii',
                'London': 'London',
                'Paris': 'Paris',
                'Tokyo': 'Tokyo',
                'Sydney': 'Sydney',
            }
            tz_display = display_map.get(city_name, city_name)
        else:
            tz_display = tz_name
    elif tz_name in ['GMT', 'UTC']:
        tz_display = tz_name
    else:
        # Fallback: use the timezone name as-is, or try to extract readable part
        if 'Pacific' in tz_name:
            tz_display = 'Pacific'
        elif 'Eastern' in tz_name:
            tz_display = 'Eastern'
        elif 'Central' in tz_name:
            tz_display = 'Central'
        elif 'Mountain' in tz_name:
            tz_display = 'Mountain'
        elif 'Alaska' in tz_name:
            tz_display = 'Alaska'
        elif 'Hawaii' in tz_name:
            tz_display = 'Hawaii'
        else:
            tz_display = tz_name.replace('_', ' ').replace('/', ' ')
    
    output += f"     All times in {tz_display}\n"
    
    # Format timestamp in the specified timezone
    now_tz = datetime.now(tz)
    output += f"  Sent from SportsPuff@{now_tz.strftime('%H:%M')}\n"
    output += "-" * 30 + "\n"
    
    return output

def format_scores_curl(games: List[Game], target_date: date, tz: pytz.BaseTzInfo = None, show_all_sports: bool = False) -> str:
    """Format games in curl-style scores format.
    
    Args:
        games: List of games to format
        target_date: Target date for the scores
        tz: Timezone for display
        show_all_sports: If True, show all sports even if they have no scores
    """
    if tz is None:
        tz = pytz.timezone('US/Pacific')  # Default to Pacific
    
    # Group by sport
    by_sport: Dict[str, List[Game]] = {}
    for game in games:
        sport = game.league.lower()
        if sport not in by_sport:
            by_sport[sport] = []
        by_sport[sport].append(game)
    
    # Format the output
    greeting = get_greeting(tz)
    date_str = target_date.strftime('%a %d %b %Y')
    
    output = f"{greeting}!\nHere are the scores:\n"
    output += f"       {date_str}:\n"
    output += "-" * 30 + "\n"
    
    # Sort sports by custom order
    sport_order = ['mlb', 'wnba', 'nba', 'nfl', 'nhl']
    sport_to_league = {
        'mlb': 'MLB',
        'wnba': 'WNBA',
        'nba': 'NBA',
        'nfl': 'NFL',
        'nhl': 'NHL'
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
            # Map game_type to display format
            game_type_map = {
                'preseason': 'Preseason',
                'regular': 'Regular Season',
                'playoffs': 'Playoffs',
                'allstar': 'All-Star',
                'nba_cup': 'Emirates NBA Cup',
                'postseason': 'Playoffs'
            }
            season_type = game_type_map.get(first_game.game_type.lower(), first_game.game_type.title().replace('_', ' '))
            league_name = first_game.league
        else:
            # No scores for this sport - try to get season info from database
            league_name = sport_to_league.get(sport, sport.upper())
            season_type = _get_season_type_for_sport(sport, target_date)
        
        output += f"{league_name} - {season_type}:\n"
        
        if scored_games:
            for game in scored_games:
                away_abbr = game.visitor_team_abbrev
                home_abbr = game.home_team_abbrev
                
                away_score = game.visitor_score_total or 0
                home_score = game.home_score_total or 0
                
                if game.is_final:
                    status = "F"
                    output += f" {away_abbr} [{away_score:3d}-{home_score:3d}] {home_abbr} {status}\n"
                elif game.game_status == 'in_progress' or (away_score > 0 or home_score > 0):
                    period = game.current_period or '?'
                    time_left = game.time_remaining or ''
                    
                    if sport == 'nhl':
                        # For NHL: Period 4+ is overtime (OT), format as "P{period} MM:SS" (matching NBA format)
                        try:
                            period_num = int(period) if str(period).isdigit() else 0
                            if period_num >= 4:
                                period_display = 'OT'
                            else:
                                period_display = f'P{period_num}'
                        except (ValueError, TypeError):
                            period_display = f'P{period}'
                        
                        # Always show time if available, otherwise just show period
                        if time_left and time_left.strip():
                            status = f"{period_display} {time_left}"
                        else:
                            # If no time available, still show period
                            status = period_display
                    else:
                        # For other sports, use 'Q' for Quarter
                        period_prefix = 'Q'
                        if time_left and time_left.strip():
                            status = f"{period_prefix}{period} {time_left}"
                        else:
                            status = f"{period_prefix}{period}"
                    
                    output += f" {away_abbr} [{away_score:3d}-{home_score:3d}] {home_abbr} {status}\n"
        else:
            output += " No games scheduled\n"
        
        output += "-" * 30 + "\n"
    
    # Determine timezone display name (same logic as format_schedule_curl)
    tz_name = tz.zone if hasattr(tz, 'zone') else str(tz)
    
    # Try to extract a readable timezone name
    if '/' in tz_name:
        parts = tz_name.split('/')
        if len(parts) > 1:
            city_name = parts[-1].replace('_', ' ')
            display_map = {
                'New York': 'Eastern',
                'Los Angeles': 'Pacific',
                'Chicago': 'Central',
                'Denver': 'Mountain',
                'Anchorage': 'Alaska',
                'Honolulu': 'Hawaii',
                'London': 'London',
                'Paris': 'Paris',
                'Tokyo': 'Tokyo',
                'Sydney': 'Sydney',
            }
            tz_display = display_map.get(city_name, city_name)
        else:
            tz_display = tz_name
    elif tz_name in ['GMT', 'UTC']:
        tz_display = tz_name
    else:
        if 'Pacific' in tz_name:
            tz_display = 'Pacific'
        elif 'Eastern' in tz_name:
            tz_display = 'Eastern'
        elif 'Central' in tz_name:
            tz_display = 'Central'
        elif 'Mountain' in tz_name:
            tz_display = 'Mountain'
        elif 'Alaska' in tz_name:
            tz_display = 'Alaska'
        elif 'Hawaii' in tz_name:
            tz_display = 'Hawaii'
        else:
            tz_display = tz_name.replace('_', ' ').replace('/', ' ')
    
    output += f"     All times in {tz_display}\n"
    
    # Format timestamp in the specified timezone
    now_tz = datetime.now(tz)
    output += f"  Sent from SportsPuff@{now_tz.strftime('%H:%M')}\n"
    output += "-" * 30 + "\n"
    
    return output


@app.get("/")
def root():
    """Root endpoint."""
    return {"message": "Sports Data Service API", "version": "1.0.0"}


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
        
        with get_db_session() as db:
            for sport_key, league in SPORT_MAPPINGS.items():
                games = db.query(Game).filter(
                    Game.league == league,
                    Game.game_date == target_date
                ).order_by(Game.game_time).all()
                
                result[sport_key] = [
                    {
                        "game_id": game.game_id,
                        "game_date": game.game_date.isoformat(),
                        "game_time": game.game_time.isoformat() if game.game_time else None,
                        "home_team": game.home_team,
                        "home_team_abbrev": game.home_team_abbrev,
                        "visitor_team": game.visitor_team,
                        "visitor_team_abbrev": game.visitor_team_abbrev,
                        "game_status": game.game_status,
                        "game_type": game.game_type
                    }
                    for game in games
                ]
        
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
        
        # Convert Game objects to dicts while session is open
        games_data = []
        with get_db_session() as db:
            for sport_key, league in SPORT_MAPPINGS.items():
                games = db.query(Game).filter(
                    Game.league == league,
                    Game.game_date == target_date
                ).order_by(Game.game_time).all()
                # Convert to dicts while session is open
                for game in games:
                    games_data.append({
                        'league': game.league,
                        'game_id': game.game_id,
                        'game_date': game.game_date,
                        'game_time': game.game_time,
                        'game_type': game.game_type,
                        'home_team': game.home_team,
                        'home_team_abbrev': game.home_team_abbrev,
                        'home_wins': game.home_wins,
                        'home_losses': game.home_losses,
                        'visitor_team': game.visitor_team,
                        'visitor_team_abbrev': game.visitor_team_abbrev,
                        'visitor_wins': game.visitor_wins,
                        'visitor_losses': game.visitor_losses,
                        'game_status': game.game_status,
                        'current_period': game.current_period,
                        'time_remaining': game.time_remaining,
                        'is_final': game.is_final,
                    })
        
        # Convert back to Game-like objects
        class GameProxy:
            def __init__(self, data):
                for k, v in data.items():
                    setattr(self, k, v)
        
        all_games = [GameProxy(g) for g in games_data]
        
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
        
        with get_db_session() as db:
            for sport_key, league in SPORT_MAPPINGS.items():
                games = db.query(Game).filter(
                    Game.league == league,
                    Game.game_date == target_date,
                    Game.is_final == True
                ).all()
                
                result[sport_key] = [
                    {
                        "game_id": game.game_id,
                        "home_team": game.home_team,
                        "home_score": game.home_score_total,
                        "visitor_team": game.visitor_team,
                        "visitor_score": game.visitor_score_total,
                        "is_final": game.is_final
                    }
                    for game in games
                ]
        
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
        
        # Use joinedload or ensure we access attributes within session context
        # Better: convert Game objects to dicts while session is open
        games_data = []
        with get_db_session() as db:
            for sport_key, league in SPORT_MAPPINGS.items():
                games = db.query(Game).filter(
                    Game.league == league,
                    Game.game_date == target_date
                ).all()
                # Convert to dicts while session is open
                for game in games:
                    games_data.append({
                        'league': game.league,
                        'game_id': game.game_id,
                        'game_date': game.game_date,
                        'game_time': game.game_time,
                        'game_type': game.game_type,
                        'home_team': game.home_team,
                        'home_team_abbrev': game.home_team_abbrev,
                        'home_score_total': game.home_score_total,
                        'visitor_team': game.visitor_team,
                        'visitor_team_abbrev': game.visitor_team_abbrev,
                        'visitor_score_total': game.visitor_score_total,
                        'game_status': game.game_status,
                        'current_period': game.current_period,
                        'time_remaining': game.time_remaining,
                        'is_final': game.is_final,
                    })
        
        # Now convert back to Game-like objects or modify format_scores_curl to accept dicts
        # Actually, let's create a simple wrapper class
        class GameProxy:
            def __init__(self, data):
                for k, v in data.items():
                    setattr(self, k, v)
        
        all_games = [GameProxy(g) for g in games_data]
        
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
        
        # Handle 'all' sport - aggregate from all sports
        if sport_lower == 'all':
            all_games = []
            for sport_key in SPORT_MAPPINGS.keys():
                league = SPORT_MAPPINGS[sport_key]
                games_list = _get_schedule_for_league(league, target_date, timezone)
                # Add sport identifier to each game
                for game in games_list:
                    game['sport'] = sport_key
                all_games.extend(games_list)
            
            # Sort by game_time
            all_games.sort(key=lambda x: x.get('game_time') or '')
            
            return {
                "sport": "all",
                "date": target_date.isoformat(),
                "games": all_games
            }
        
        # Single sport logic
        league = SPORT_MAPPINGS.get(sport_lower)
        if not league:
            raise HTTPException(status_code=400, detail=f"Invalid sport: {sport}")
        
        games_list = _get_schedule_for_league(league, target_date, timezone)
        
        return {
            "sport": sport,
            "date": target_date.isoformat(),
            "games": games_list
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
    if target_date == today:
        if collector:
            live_games = collector.get_live_scores(target_date)
            if live_games:
                # Use live data - convert to format expected by frontend
                seen_game_ids = set()
                for game_dict in live_games:
                    game_id = game_dict.get('game_id', '')
                    if game_id and game_id in seen_game_ids:
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
    
    # For any date (today or past), try get_schedule from collector
    if not games_list and collector:
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
    
    # Fallback to database if no collector games found
    if not games_list:
        with get_db_session() as db:
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

def _get_games_for_curl(league: str, target_date: date, timezone: pytz.BaseTzInfo) -> List[Any]:
    """Helper function to get games for curl formatting (returns GameWrapper objects)."""
    now_tz = datetime.now(timezone)
    today = now_tz.date()
    games = []
    
    class GameWrapper:
        def __init__(self, data):
            for k, v in data.items():
                setattr(self, k, v)
    
    if target_date == today:
        # Try to get live data first for today
        collector = get_collector(league)
        if collector:
            live_games = collector.get_live_scores(target_date)
            if live_games:
                # Use a set to track game_ids and avoid duplicates
                seen_game_ids = set()
                for game_dict in live_games:
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
                        'home_score_total': game_dict.get('home_score_total', 0),
                        'visitor_score_total': game_dict.get('visitor_score_total', 0),
                        'game_status': game_dict.get('game_status', 'scheduled'),
                        'current_period': current_period,
                        'time_remaining': time_remaining,
                        'is_final': game_dict.get('is_final', False),
                        'home_wins': game_dict.get('home_wins', 0),
                        'home_losses': game_dict.get('home_losses', 0),
                        'home_otl': game_dict.get('home_otl', 0),
                        'visitor_wins': game_dict.get('visitor_wins', 0),
                        'visitor_losses': game_dict.get('visitor_losses', 0),
                        'visitor_otl': game_dict.get('visitor_otl', 0)
                    }
                    games.append(GameWrapper(game_data))
    
    # For any date (today or past), try get_schedule from collector if we don't have games yet
    if not games:
        collector = get_collector(league)
        if collector:
            schedule_games = collector.get_schedule(target_date)
            if schedule_games:
                seen_game_ids = set()
                for game_dict in schedule_games:
                    game_id = game_dict.get('game_id', '')
                    if game_id and game_id in seen_game_ids:
                        continue
                    seen_game_ids.add(game_id)
                    
                    # Skip games with empty team names
                    home_team = game_dict.get('home_team', '')
                    home_abbrev = game_dict.get('home_team_abbrev', '')
                    visitor_team = game_dict.get('visitor_team', '')
                    visitor_abbrev = game_dict.get('visitor_team_abbrev', '')
                    
                    if (not home_team or home_team.strip() == '') and (not home_abbrev or home_abbrev.strip() == ''):
                        continue
                    if (not visitor_team or visitor_team.strip() == '') and (not visitor_abbrev or visitor_abbrev.strip() == ''):
                        continue
                    
                    game_time = game_dict.get('game_time')
                    game_data = {
                        'league': league,
                        'game_id': game_id,
                        'game_date': datetime.strptime(game_dict.get('game_date', ''), '%Y-%m-%d').date() if game_dict.get('game_date') else target_date,
                        'game_time': game_time,
                        'game_type': game_dict.get('game_type', 'regular'),
                        'home_team': home_team,
                        'home_team_abbrev': home_abbrev,
                        'visitor_team': visitor_team,
                        'visitor_team_abbrev': visitor_abbrev,
                        'home_score_total': game_dict.get('home_score_total', 0),
                        'visitor_score_total': game_dict.get('visitor_score_total', 0),
                        'game_status': game_dict.get('game_status', 'scheduled'),
                        'current_period': game_dict.get('current_period', ''),
                        'time_remaining': game_dict.get('time_remaining', ''),
                        'is_final': game_dict.get('is_final', False),
                        'home_wins': game_dict.get('home_wins', 0),
                        'home_losses': game_dict.get('home_losses', 0),
                        'home_otl': game_dict.get('home_otl', 0),
                        'visitor_wins': game_dict.get('visitor_wins', 0),
                        'visitor_losses': game_dict.get('visitor_losses', 0),
                        'visitor_otl': game_dict.get('visitor_otl', 0)
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
        
        # Handle 'all' sport - aggregate from all sports
        if sport_lower == 'all':
            all_scores = []
            for sport_key in SPORT_MAPPINGS.keys():
                league = SPORT_MAPPINGS[sport_key]
                scores_list = _get_scores_for_league(league, target_date)
                # Add sport identifier to each score
                for score in scores_list:
                    score['sport'] = sport_key
                all_scores.extend(scores_list)
            
            return {
                "sport": "all",
                "date": target_date.isoformat(),
                "scores": all_scores
            }
        
        # Single sport logic
        league = SPORT_MAPPINGS.get(sport_lower)
        if not league:
            raise HTTPException(status_code=400, detail=f"Invalid sport: {sport}")
        
        scores_list = _get_scores_for_league(league, target_date)
        
        return {
            "sport": sport,
            "date": target_date.isoformat(),
            "scores": scores_list
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
                        'home_score_total': game_dict.get('home_score_total', 0),
                        'visitor_score_total': game_dict.get('visitor_score_total', 0),
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
