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
from fastapi.responses import JSONResponse, PlainTextResponse
from fastapi.openapi.utils import get_openapi
import pytz

sys.path.insert(0, '/app/src')

from database import get_db_session
from models import Game
from config import settings

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

def get_greeting() -> str:
    """Get greeting based on time of day."""
    now = datetime.now()
    hour = now.hour
    
    if 5 <= hour < 12:
        return "Good morning"
    elif 12 <= hour < 17:
        return "Good afternoon"
    elif 17 <= hour < 24:
        return "Good evening"
    else:
        return "God it's late"

def parse_date_param(date_param: Optional[str]) -> date:
    """Parse date parameter (today, tomorrow, yesterday, or YYYYMMDD)."""
    today = date.today()
    
    if date_param is None or date_param.lower() == 'today':
        return today
    elif date_param.lower() == 'tomorrow':
        return today + timedelta(days=1)
    elif date_param.lower() == 'yesterday':
        return today - timedelta(days=1)
    elif len(date_param) == 8 and date_param.isdigit():
        # YYYYMMDD format
        try:
            return datetime.strptime(date_param, '%Y%m%d').date()
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYYMMDD")
    else:
        raise HTTPException(status_code=400, detail="Invalid date parameter")

def format_game_for_curl(game: Game, sport: str) -> str:
    """Format a single game for curl-style output."""
    # Format team names with wins/losses
    away_team = f"{game.visitor_team_abbrev} [{game.visitor_wins:3d}-{game.visitor_losses:2d}]"
    home_team = f"{game.home_team_abbrev} [{game.home_wins:3d}-{game.home_losses:2d}]"
    
    # Format time/status
    if game.is_final:
        if game.home_score_total is not None and game.visitor_score_total is not None:
            time_status = f"({game.visitor_score_total:2d}-{game.home_score_total:2d}) F"
        else:
            time_status = "F"
    elif game.game_status == 'in_progress':
        period = game.current_period or '?'
        time_left = game.time_remaining or '?:??'
        time_status = f"{time_left} {period}"
    else:
        # Scheduled game
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

def format_schedule_curl(games: List[Game], target_date: date) -> str:
    """Format games in curl-style schedule format."""
    if not games:
        return "No games scheduled"
    
    # Group by sport
    by_sport: Dict[str, List[Game]] = {}
    for game in games:
        sport = game.league.lower()
        if sport not in by_sport:
            by_sport[sport] = []
        by_sport[sport].append(game)
    
    # Format the output
    greeting = get_greeting()
    date_str = target_date.strftime('%a %d %b %Y')
    
    output = f"{greeting}!\nHere is {date_str} sports schedule\n"
    output += f"       {date_str}:\n"
    output += "-" * 30 + "\n"
    
    # Sort sports by custom order
    sport_order = ['mlb', 'wnba', 'nba', 'nfl', 'nhl']
    
    for sport in sport_order:
        if sport not in by_sport:
            continue
        
        sport_games = by_sport[sport]
        if not sport_games:
            continue
        
        # Determine season info from first game
        first_game = sport_games[0]
        season_type = first_game.game_type.title().replace('_', ' ')
        
        # Try to determine week (this is simplified - you may need to calculate)
        week = "???"  # Placeholder
        
        output += f"{first_game.league} - {season_type}, Week {week}:\n"
        
        for game in sport_games:
            output += format_game_for_curl(game, sport)
            output += "\n"
        
        output += "-" * 30 + "\n"
    
    output += "     All times in Pacific\n"
    output += f"  Sent from SportsPuff@{datetime.now().strftime('%H:%M')}\n"
    output += "-" * 30 + "\n"
    
    return output

def format_scores_curl(games: List[Game], target_date: date) -> str:
    """Format games in curl-style scores format."""
    if not games:
        return "No scores available"
    
    # Group by sport
    by_sport: Dict[str, List[Game]] = {}
    for game in games:
        sport = game.league.lower()
        if sport not in by_sport:
            by_sport[sport] = []
        by_sport[sport].append(game)
    
    # Format the output
    greeting = get_greeting()
    date_str = target_date.strftime('%a %d %b %Y')
    
    output = f"{greeting}!\nHere are {date_str} sports scores\n"
    output += f"       {date_str}:\n"
    output += "-" * 30 + "\n"
    
    # Sort sports by custom order
    sport_order = ['mlb', 'wnba', 'nba', 'nfl', 'nhl']
    
    for sport in sport_order:
        if sport not in by_sport:
            continue
        
        sport_games = by_sport[sport]
        if not sport_games:
            continue
        
        # Only show games that have scores (final or in progress)
        scored_games = [g for g in sport_games if g.is_final or g.game_status == 'in_progress']
        if not scored_games:
            continue
        
        # Determine season info from first game
        first_game = scored_games[0]
        season_type = first_game.game_type.title().replace('_', ' ')
        week = "???"  # Placeholder
        
        output += f"{first_game.league} - {season_type}, Week {week}:\n"
        
        for game in scored_games:
            away_abbr = game.visitor_team_abbrev
            home_abbr = game.home_team_abbrev
            
            away_score = game.visitor_score_total or 0
            home_score = game.home_score_total or 0
            
            if game.is_final:
                status = f"({away_score:2d}-{home_score:2d}) F"
            elif game.game_status == 'in_progress':
                period = game.current_period or '?'
                time_left = game.time_remaining or '?:??'
                status = f"({away_score:2d}-{home_score:2d}) {time_left}"
            else:
                status = "TBD"
            
            output += f" {away_abbr}(v) [{away_score:3d}-{home_score:2d}] {home_abbr}(h) {status}\n"
        
        output += "-" * 30 + "\n"
    
    output += "     All times in Pacific\n"
    output += f"  Sent from SportsPuff@{datetime.now().strftime('%H:%M')}\n"
    output += "-" * 30 + "\n"
    
    return output


@app.get("/")
def root():
    """Root endpoint."""
    return {"message": "Sports Data Service API", "version": "1.0.0"}


@app.get("/api/v1/schedules/{date}")
def get_schedules_all_sports_api_v1(
    date: str = Path(..., description="Date: today, tomorrow, yesterday, or YYYYMMDD"),
):
    """Get schedules for all sports in JSON format."""
    try:
        target_date = parse_date_param(date)
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
    date: str = Path(..., description="Date: today, tomorrow, yesterday, or YYYYMMDD"),
):
    """Get schedules for all sports in curl-style text format."""
    try:
        target_date = parse_date_param(date)
        all_games = []
        
        with get_db_session() as db:
            for sport_key, league in SPORT_MAPPINGS.items():
                games = db.query(Game).filter(
                    Game.league == league,
                    Game.game_date == target_date
                ).order_by(Game.game_time).all()
                all_games.extend(games)
        
        return format_schedule_curl(all_games, target_date)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/scores/{date}")
def get_scores_all_sports_api_v1(
    date: str = Path(..., description="Date: today, tomorrow, yesterday, or YYYYMMDD"),
):
    """Get scores for all sports in JSON format."""
    try:
        target_date = parse_date_param(date)
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
    date: str = Path(..., description="Date: today, tomorrow, yesterday, or YYYYMMDD"),
):
    """Get scores for all sports in curl-style text format."""
    try:
        target_date = parse_date_param(date)
        all_games = []
        
        with get_db_session() as db:
            for sport_key, league in SPORT_MAPPINGS.items():
                games = db.query(Game).filter(
                    Game.league == league,
                    Game.game_date == target_date
                ).all()
                # Access all needed attributes while session is open
                for game in games:
                    # Trigger lazy loading of all attributes
                    _ = game.league
                    _ = game.game_id
                    _ = game.game_date
                    _ = game.game_time
                    _ = game.game_type
                    _ = game.home_team
                    _ = game.home_team_abbrev
                    _ = game.home_score_total
                    _ = game.visitor_team
                    _ = game.visitor_team_abbrev
                    _ = game.visitor_score_total
                    _ = game.game_status
                    _ = game.current_period
                    _ = game.time_remaining
                    _ = game.is_final
                all_games.extend(games)
        
        return format_scores_curl(all_games, target_date)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/schedule/{sport}/{date}")
def get_schedule_api_v1(
    sport: str = Path(..., description="Sport (nba, mlb, nfl, nhl, wnba)"),
    date: str = Path(..., description="Date: today, tomorrow, yesterday, or YYYYMMDD"),
):
    """Get schedule in JSON format."""
    try:
        target_date = parse_date_param(date)
        league = SPORT_MAPPINGS.get(sport.lower())
        
        if not league:
            raise HTTPException(status_code=400, detail=f"Invalid sport: {sport}")
        
        with get_db_session() as db:
            games = db.query(Game).filter(
                Game.league == league,
                Game.game_date == target_date
            ).order_by(Game.game_time).all()
            
            return {
                "sport": sport,
                "date": target_date.isoformat(),
                "games": [
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
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/curl/v1/schedule/{sport}/{date}", response_class=PlainTextResponse)
def get_schedule_curl_v1(
    sport: str = Path(..., description="Sport (nba, mlb, nfl, nhl, wnba)"),
    date: str = Path(..., description="Date: today, tomorrow, yesterday, or YYYYMMDD"),
):
    """Get schedule in curl-style text format."""
    try:
        target_date = parse_date_param(date)
        league = SPORT_MAPPINGS.get(sport.lower())
        
        if not league:
            raise HTTPException(status_code=400, detail=f"Invalid sport: {sport}")
        
        with get_db_session() as db:
            games = db.query(Game).filter(
                Game.league == league,
                Game.game_date == target_date
            ).order_by(Game.game_time).all()
            
            return format_schedule_curl(games, target_date)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/scores/{sport}/{date}")
def get_scores_api_v1(
    sport: str = Path(..., description="Sport (nba, mlb, nfl, nhl, wnba)"),
    date: str = Path(..., description="Date: today, tomorrow, yesterday, or YYYYMMDD"),
):
    """Get scores in JSON format."""
    try:
        target_date = parse_date_param(date)
        league = SPORT_MAPPINGS.get(sport.lower())
        
        if not league:
            raise HTTPException(status_code=400, detail=f"Invalid sport: {sport}")
        
        with get_db_session() as db:
            games = db.query(Game).filter(
                Game.league == league,
                Game.game_date == target_date,
                Game.is_final == True  # Only final games have scores
            ).all()
            
            return {
                "sport": sport,
                "date": target_date.isoformat(),
                "scores": [
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
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/curl/v1/scores/{sport}/{date}", response_class=PlainTextResponse)
def get_scores_curl_v1(
    sport: str = Path(..., description="Sport (nba, mlb, nfl, nhl, wnba)"),
    date: str = Path(..., description="Date: today, tomorrow, yesterday, or YYYYMMDD"),
):
    """Get scores in curl-style text format."""
    try:
        target_date = parse_date_param(date)
        league = SPORT_MAPPINGS.get(sport.lower())
        
        if not league:
            raise HTTPException(status_code=400, detail=f"Invalid sport: {sport}")
        
        with get_db_session() as db:
            games = db.query(Game).filter(
                Game.league == league,
                Game.game_date == target_date
            ).all()
            
            return format_scores_curl(games, target_date)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/standings/{sport}")
def get_standings_api_v1(
    sport: str = Path(..., description="Sport (nba, mlb, nfl, nhl, wnba)"),
):
    """Get standings in JSON format."""
    # TODO: Implement standings endpoint
    return {"message": "Standings endpoint - TODO"}


@app.get("/curl/v1/standings/{sport}", response_class=PlainTextResponse)
def get_standings_curl_v1(
    sport: str = Path(..., description="Sport (nba, mlb, nfl, nhl, wnba)"),
):
    """Get standings in curl-style text format."""
    # TODO: Implement standings endpoint
    return "Standings endpoint - TODO\n"


@app.get("/health")
def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=settings.api_host, port=settings.api_port)
