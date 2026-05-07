"""
NBA data collector for the sports data service.
"""

import sys
import os
import signal
import time
import json
import re
from datetime import datetime, date
from typing import Dict, List, Optional, Any
import logging

# Add NBA API to path
sys.path.insert(0, '/app/dependencies/nba_api/src')

# Setup proxy before importing nba_api
from utils.proxy import setup_proxy, get_proxy_config
setup_proxy()

from nba_api.live.nba.endpoints import scoreboard
from nba_api.stats.endpoints import scoreboardv2
from nba_api.stats.endpoints import leaguegamefinder
try:
    from nba_api.stats.endpoints import scheduleleaguev2
except ImportError:
    # scheduleleaguev2 may not exist in all versions
    scheduleleaguev2 = None
import requests
from sqlalchemy.orm import Session

from .base import BaseCollector
from models import Game

logger = logging.getLogger(__name__)


class NBACollector(BaseCollector):
    """NBA data collector using the NBA API."""
    
    def __init__(self):
        super().__init__("NBA")
        self.timeout_handler = None
        # Custom headers for NBA API - NBA.com may block requests without proper headers
        self.nba_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': 'https://www.nba.com/',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Origin': 'https://www.nba.com'
        }
        # Get proxy configuration
        self.proxy_config = get_proxy_config()
        if self.proxy_config:
            logger.info("Using proxy for NBA API requests")
    
    def _timeout_handler(self, signum, frame):
        """Handle timeout for NBA API calls."""
        raise TimeoutError("NBA API call timed out")
    
    def _call_with_timeout(self, func, timeout_seconds: int = None):
        """Call a function with a timeout.
        
        Note: signal.SIGALRM only works in the main thread, so we skip timeout
        in worker threads and just call the function directly.
        """
        if timeout_seconds is None:
            timeout_seconds = self.api_timeout
        
        # Check if we're in the main thread
        import threading
        if threading.current_thread() is threading.main_thread():
            try:
                signal.signal(signal.SIGALRM, self._timeout_handler)
                signal.alarm(timeout_seconds)
                try:
                    result = func()
                    return result
                finally:
                    signal.alarm(0)
            except (ValueError, OSError):
                # Signal not available (e.g., Windows or not in main thread)
                # Just call the function without timeout
                return func()
        else:
            # Not in main thread, can't use signals - just call the function
            return func()
    
    def get_schedule(self, date: Optional[date] = None) -> List[Dict[str, Any]]:
        """
        Get NBA schedule for specified date or current games.
        
        Args:
            date: Date to get schedule for (optional, defaults to today)
            
        Returns:
            List of game dictionaries
        """
        self._check_rate_limit()
        
        try:
            # Determine the correct season based on the date
            if date:
                date_obj = datetime.combine(date, datetime.min.time())
                year = date_obj.year
                month = date_obj.month
            else:
                now = datetime.now()
                year = now.year
                month = now.month
            
            # NBA season spans two calendar years (e.g., 2024-25 runs Oct 2024 to June 2025)
            if month >= 10:  # October onwards
                season = f"{year}-{str(year + 1)[-2:]}"
            elif month <= 6:  # January to June (still previous season)
                season = f"{year - 1}-{str(year)[-2:]}"
            else:  # July to September (off season, use previous season)
                season = f"{year - 1}-{str(year)[-2:]}"
            
            # Try scoreboardv2 endpoint first (more reliable for specific dates)
            def get_schedule_data():
                try:
                    # For specific dates, use scoreboardv2 which accepts a date parameter
                    # This is more reliable than live scoreboard which only returns "today" in UTC
                    if date is not None:
                        date_str = date.strftime('%Y-%m-%d')
                        logger.info(f"Fetching NBA schedule for {date_str} using scoreboardv2 endpoint")
                        # Use custom headers with shorter timeout to fail faster
                        # NBA.com may block requests without proper User-Agent headers
                        # Reduced timeout to 30s to fail faster and use fallback
                        scoreboard_data = scoreboardv2.ScoreboardV2(
                            game_date=date_str, 
                            timeout=30,
                            headers=self.nba_headers
                        )
                        scoreboard_dict = scoreboard_data.get_dict()
                        
                        # ScoreboardV2 returns data in resultSets format
                        # resultSets[0] = GameHeader (game info)
                        # resultSets[1] = LineScore (team info and scores)
                        games = []
                        if 'resultSets' in scoreboard_dict and len(scoreboard_dict['resultSets']) > 0:
                            # Get game header data
                            game_header = scoreboard_dict['resultSets'][0]
                            line_score = scoreboard_dict['resultSets'][1] if len(scoreboard_dict['resultSets']) > 1 else None

                            game_headers = game_header.get('headers', [])
                            game_rows = game_header.get('rowSet', [])
                            line_headers = line_score.get('headers', []) if line_score else []
                            line_rows = line_score.get('rowSet', []) if line_score else []

                            # Build column index maps
                            gh = {name: idx for idx, name in enumerate(game_headers)}
                            lh = {name: idx for idx, name in enumerate(line_headers)}

                            # Create a mapping of game_id to line score rows (multiple per game)
                            line_score_map = {}
                            game_id_col = lh.get('GAME_ID', 0)
                            for line_row in line_rows:
                                if len(line_row) > game_id_col:
                                    gid = line_row[game_id_col]
                                    if gid not in line_score_map:
                                        line_score_map[gid] = []
                                    line_score_map[gid].append(line_row)

                            # Parse each game (scoreboardv2 already filtered by requested date)
                            for game_row in game_rows:
                                if len(game_row) >= 8:
                                    game_id = game_row[gh.get('GAME_ID', 2)]
                                    game_date_est = game_row[gh.get('GAME_DATE_EST', 0)]
                                    home_team_id = game_row[gh.get('HOME_TEAM_ID', 6)]
                                    visitor_team_id = game_row[gh.get('VISITOR_TEAM_ID', 7)]
                                    game_status_text = game_row[gh.get('GAME_STATUS_TEXT', 4)] if gh.get('GAME_STATUS_TEXT', 4) < len(game_row) else ''

                                    # Get line score rows for this game
                                    game_line_rows = line_score_map.get(game_id, [])

                                    # Build game object compatible with parse_game_data
                                    game_obj = {
                                        'gameId': str(game_id) if game_id else '',
                                        'gameDate': game_date_est,
                                        'gameTimeUTC': game_date_est,
                                        'homeTeam': {
                                            'teamId': home_team_id,
                                        },
                                        'awayTeam': {
                                            'teamId': visitor_team_id,
                                        },
                                        'gameStatusText': game_status_text,
                                        'gameStatus': game_status_text,
                                        '_lineScore': game_line_rows[0] if game_line_rows else [],
                                    }

                                    # Add team info from line score rows
                                    team_id_col = lh.get('TEAM_ID', 1)
                                    abbrev_col = lh.get('TEAM_ABBREVIATION', 2)
                                    city_col = lh.get('TEAM_CITY_NAME', 3)
                                    name_col = lh.get('TEAM_NAME', 4)
                                    pts_col = lh.get('PTS', 21)

                                    for lr in game_line_rows:
                                        if len(lr) > max(team_id_col, abbrev_col, name_col):
                                            tid = lr[team_id_col]
                                            if tid == home_team_id:
                                                game_obj['homeTeam'].update({
                                                    'teamCity': lr[city_col] if len(lr) > city_col else '',
                                                    'teamName': lr[name_col] if len(lr) > name_col else '',
                                                    'teamTricode': lr[abbrev_col] if len(lr) > abbrev_col else '',
                                                    'score': lr[pts_col] if len(lr) > pts_col else 0,
                                                })
                                            elif tid == visitor_team_id:
                                                game_obj['awayTeam'].update({
                                                    'teamCity': lr[city_col] if len(lr) > city_col else '',
                                                    'teamName': lr[name_col] if len(lr) > name_col else '',
                                                    'teamTricode': lr[abbrev_col] if len(lr) > abbrev_col else '',
                                                    'score': lr[pts_col] if len(lr) > pts_col else 0,
                                                })

                                    games.append(game_obj)
                        
                        # Wrap in leagueSchedule format for compatibility with existing parser
                        return {'leagueSchedule': {'gameDates': [{'gameDate': date_str, 'games': games}]}}
                    else:
                        # For "today" (no date specified), try live scoreboard endpoint (no proxy required, works reliably)
                        logger.info("Trying live scoreboard endpoint (no proxy required)")
                        board = scoreboard.ScoreBoard()
                        games_data = board.games.get_dict()
                        
                        # Convert to our format
                        games = []
                        for game in games_data:
                            # Live scoreboard format: {'gameId': '...', 'gameTimeUTC': '...', 
                            #                          'awayTeam': {...}, 'homeTeam': {...}, ...}
                            game_obj = {
                                'gameId': game.get('gameId', ''),
                                'gameDate': game.get('gameTimeUTC', ''),
                                'gameTimeUTC': game.get('gameTimeUTC', ''),
                                'homeTeam': game.get('homeTeam', {}),
                                'awayTeam': game.get('awayTeam', {}),
                                'gameStatus': game.get('gameStatusText', 'scheduled'),
                                '_live_scoreboard': True
                            }
                            games.append(game_obj)
                        
                        # Get date string for return format
                        date_str = datetime.now().strftime('%Y-%m-%d')
                        
                        # Parse all games before returning
                        parsed_games_list = []
                        for game in games:
                            parsed_game = self._parse_live_scoreboard_game(game)
                            if parsed_game:
                                parsed_games_list.append(parsed_game)
                        
                        # Wrap in leagueSchedule format for compatibility
                        return {'leagueSchedule': {'gameDates': [{'gameDate': date_str, 'games': parsed_games_list}]}}
                    
                except Exception as e:
                    logger.warning(f"Scoreboardv2 failed: {e}, trying live scoreboard fallback")
                    # Fallback to live scoreboard if scoreboardv2 fails
                    # Note: Live scoreboard only returns "today" in UTC, so we'll be lenient with date filtering
                    try:
                        logger.info("Trying live scoreboard endpoint as fallback")
                        board = scoreboard.ScoreBoard()
                        games_data = board.games.get_dict()
                        
                        # Convert to our format
                        games = []
                        for game in games_data:
                            game_obj = {
                                'gameId': game.get('gameId', ''),
                                'gameDate': game.get('gameTimeUTC', ''),
                                'gameTimeUTC': game.get('gameTimeUTC', ''),
                                'homeTeam': game.get('homeTeam', {}),
                                'awayTeam': game.get('awayTeam', {}),
                                'gameStatus': game.get('gameStatusText', 'scheduled'),
                                '_live_scoreboard': True
                            }
                            games.append(game_obj)
                        
                        # Get date string for return format
                        if date is None:
                            date_str = datetime.now().strftime('%Y-%m-%d')
                        else:
                            date_str = date.strftime('%Y-%m-%d')
                        
                        # Parse all games before returning
                        # Since scoreboardv2 failed and live scoreboard only returns "today" games,
                        # we'll include all games but try to match the target date if possible
                        parsed_games_list = []
                        import pytz
                        pacific_tz = pytz.timezone('US/Pacific')
                        
                        for game in games:
                            parsed_game = self._parse_live_scoreboard_game(game)
                            if parsed_game:
                                # If date is specified, try to match it, but be lenient
                                if date is not None:
                                    parsed_date = parsed_game.get('game_date', '')
                                    game_time = parsed_game.get('game_time')
                                    
                                    # Check if game_date matches
                                    if parsed_date == date_str:
                                        parsed_games_list.append(parsed_game)
                                    elif game_time:
                                        # Check if game_time falls on the target date in Pacific time
                                        try:
                                            if isinstance(game_time, str):
                                                from dateutil import parser
                                                game_time_obj = parser.parse(game_time)
                                            else:
                                                game_time_obj = game_time

                                            if game_time_obj.tzinfo is None:
                                                game_time_obj = pytz.UTC.localize(game_time_obj)

                                            game_time_pacific = game_time_obj.astimezone(pacific_tz)
                                            game_date_pacific = game_time_pacific.date()

                                            if game_date_pacific == date:
                                                parsed_games_list.append(parsed_game)
                                        except Exception:
                                            pass
                                else:
                                    # No date specified - include all games
                                    parsed_games_list.append(parsed_game)
                        
                        logger.info(f"Live scoreboard returned {len(parsed_games_list)} games (lenient filtering for {date_str})")
                        
                        # Wrap in leagueSchedule format for compatibility
                        return {'leagueSchedule': {'gameDates': [{'gameDate': date_str, 'games': parsed_games_list}]}}
                    except Exception as e2:
                        logger.error(f"Error getting schedule via live scoreboard: {e2}")
                        return {}
                    # Fallback to LeagueGameFinder if scoreboard fails
                    # This is more reliable for specific dates
                    try:
                        logger.info(f"Falling back to LeagueGameFinder for date {date_str if date else 'today'}")
                        from nba_api.stats.endpoints import leaguegamefinder
                        
                        # LeagueGameFinder requires date range, so use the target date as both start and end
                        if date:
                            date_from = date.strftime('%m/%d/%Y')
                            date_to = date.strftime('%m/%d/%Y')
                        else:
                            today = datetime.now().date()
                            date_from = today.strftime('%m/%d/%Y')
                            date_to = today.strftime('%m/%d/%Y')
                        
                        finder = leaguegamefinder.LeagueGameFinder(
                            date_from_nullable=date_from,
                            date_to_nullable=date_to,
                            timeout=60,
                            headers=self.nba_headers
                        )
                        finder_dict = finder.get_dict()
                        
                        # LeagueGameFinder returns games in resultSets[0]
                        if 'resultSets' in finder_dict and len(finder_dict['resultSets']) > 0:
                            game_results = finder_dict['resultSets'][0]
                            game_rows = game_results.get('rowSet', [])
                            
                            logger.info(f"LeagueGameFinder returned {len(game_rows)} rows (will deduplicate by game)")
                            
                            # Group rows by game_id (each game has 2 rows - one per team)
                            games_by_id = {}
                            
                            for row in game_rows:
                                if len(row) >= 7:
                                    game_id = str(row[4]) if len(row) > 4 else ''
                                    game_date_str = row[5] if len(row) > 5 else ''
                                    matchup = row[6] if len(row) > 6 else ''
                                    
                                    if not game_id or game_id in games_by_id:
                                        continue  # Skip if no game_id or already processed
                                    
                                    # Parse matchup (e.g., "BOS @ TOR" or "BOS vs. TOR")
                                    matchup_parts = matchup.split()
                                    if len(matchup_parts) < 3:
                                        continue
                                    
                                    visitor_abbrev = matchup_parts[0]
                                    home_abbrev = matchup_parts[2]
                                    
                                    # Find team data for this game (we have 2 rows, find both teams)
                                    home_team_row = None
                                    away_team_row = None
                                    
                                    for check_row in game_rows:
                                        if len(check_row) >= 7 and str(check_row[4]) == game_id:
                                            team_abbrev = check_row[2] if len(check_row) > 2 else ''
                                            if team_abbrev == home_abbrev:
                                                home_team_row = check_row
                                            elif team_abbrev == visitor_abbrev:
                                                away_team_row = check_row
                                    
                                    if not home_team_row or not away_team_row:
                                        continue  # Skip if we can't find both teams
                                    
                                    # Extract team info from rows
                                    # Format: [SEASON_ID, TEAM_ID, TEAM_ABBREVIATION, TEAM_NAME, GAME_ID, GAME_DATE, MATCHUP, ...]
                                    home_team_id = str(home_team_row[1]) if len(home_team_row) > 1 else ''
                                    home_team_name = home_team_row[3] if len(home_team_row) > 3 else ''
                                    away_team_id = str(away_team_row[1]) if len(away_team_row) > 1 else ''
                                    away_team_name = away_team_row[3] if len(away_team_row) > 3 else ''
                                    
                                    # Parse team name: TEAM_NAME is usually "City Name" (e.g., "Boston Celtics")
                                    home_parts = home_team_name.split() if home_team_name else []
                                    home_city = ' '.join(home_parts[:-1]) if len(home_parts) > 1 else (home_parts[0] if home_parts else '')
                                    home_name = home_parts[-1] if home_parts else ''
                                    
                                    away_parts = away_team_name.split() if away_team_name else []
                                    away_city = ' '.join(away_parts[:-1]) if len(away_parts) > 1 else (away_parts[0] if away_parts else '')
                                    away_name = away_parts[-1] if away_parts else ''
                                    
                                    # Parse date - LeagueGameFinder returns YYYY-MM-DD format
                                    try:
                                        game_date_obj = datetime.strptime(game_date_str, '%Y-%m-%d')
                                        game_date_formatted = game_date_obj.strftime('%m/%d/%Y')  # parse_game_data expects MM/DD/YYYY
                                    except:
                                        continue
                                    
                                    # Build game object compatible with parse_game_data
                                    game_obj = {
                                        'gameId': game_id,
                                        'gameDate': game_date_formatted,
                                        'homeTeam': {
                                            'teamId': home_team_id,
                                            'teamTricode': home_abbrev,
                                            'teamCity': home_city,
                                            'teamName': home_name
                                        },
                                        'awayTeam': {
                                            'teamId': away_team_id,
                                            'teamTricode': visitor_abbrev,
                                            'teamCity': away_city,
                                            'teamName': away_name
                                        },
                                        'gameStatus': 'scheduled',
                                        '_leagueGameFinder': True
                                    }
                                    
                                    parsed_game = self.parse_game_data(game_obj, game_date_formatted)
                                    if parsed_game:
                                        games_by_id[game_id] = parsed_game
                            
                            all_games = list(games_by_id.values())
                            logger.info(f"LeagueGameFinder returned {len(all_games)} unique games")
                            
                            # Wrap in leagueSchedule format for compatibility
                            date_str = date.strftime('%Y-%m-%d') if date else datetime.now().strftime('%Y-%m-%d')
                            return {'leagueSchedule': {'gameDates': [{'gameDate': date_str, 'games': all_games}]}}
                        else:
                            logger.warning("LeagueGameFinder returned no games")
                            return {}
                    except Exception as e3:
                        logger.error(f"Error getting schedule via LeagueGameFinder: {e3}")
                        # Final fallback to season schedule if available
                        if scheduleleaguev2 is not None:
                            try:
                                logger.info(f"Falling back to season schedule for {season}")
                                schedule_data = scheduleleaguev2.ScheduleLeagueV2(season=season)
                                return schedule_data.get_dict()
                            except Exception as e4:
                                logger.error(f"Error getting season schedule: {e4}")
                                return {}
                        return {}
            
            start_time = time.time()
            # Increase timeout to allow for scoreboardv2 (60s) + fallbacks
            data = self._call_with_timeout(get_schedule_data, timeout_seconds=90)
            response_time = int((time.time() - start_time) * 1000)
            
            if 'leagueSchedule' in data and 'gameDates' in data['leagueSchedule']:
                game_dates = data['leagueSchedule']['gameDates']
                
                # Find games for the specified date
                # Games from live scoreboard fallback are already parsed and filtered
                target_games = []
                for game_date in game_dates:
                    game_date_str = game_date.get('gameDate', '')
                    games_for_date = game_date.get('games', [])
                    
                    # Check if games are already parsed (from live scoreboard fallback)
                    if games_for_date and isinstance(games_for_date[0], dict) and 'game_id' in games_for_date[0]:
                        # Games are already parsed - use them directly
                        for parsed_game in games_for_date:
                            # Additional date check - be lenient for live scoreboard games
                            if date is not None:
                                parsed_date = parsed_game.get('game_date', '')
                                game_time = parsed_game.get('game_time')
                                
                                # Check if date matches
                                if parsed_date == date.strftime('%Y-%m-%d'):
                                    target_games.append(parsed_game)
                                elif game_time:
                                    # Check Pacific timezone date
                                    import pytz
                                    try:
                                        if isinstance(game_time, str):
                                            from dateutil import parser
                                            game_time_obj = parser.parse(game_time)
                                        else:
                                            game_time_obj = game_time
                                        
                                        if game_time_obj.tzinfo is None:
                                            game_time_obj = pytz.UTC.localize(game_time_obj)
                                        
                                        pacific_tz = pytz.timezone('US/Pacific')
                                        game_time_pacific = game_time_obj.astimezone(pacific_tz)
                                        game_date_pacific = game_time_pacific.date()
                                        
                                        if game_date_pacific == date or abs((game_date_pacific - date).days) <= 1:
                                            target_games.append(parsed_game)
                                    except Exception:
                                        # If parsing fails, include it (be lenient)
                                        target_games.append(parsed_game)
                                else:
                                    # No game_time - include it (be lenient for live scoreboard)
                                    target_games.append(parsed_game)
                            else:
                                target_games.append(parsed_game)
                    else:
                        # Games need to be parsed
                        for game in games_for_date:
                            parsed_game = self.parse_game_data(game, game_date_str)
                            if parsed_game:
                                # Additional date check if needed
                                parsed_date = parsed_game.get('game_date', '')
                                if date is None or parsed_date == date.strftime('%Y-%m-%d'):
                                    target_games.append(parsed_game)
                
                return target_games
            else:
                return []
                
        except TimeoutError as e:
            logger.error(f"NBA API timeout: {e}")
            return []
        except Exception as e:
            logger.error(f"Error fetching NBA schedule: {e}")
            return []
    
    def get_season_schedule(self, season: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get full NBA season schedule.
        
        Args:
            season: Season identifier (e.g., "2024-25"). If None, determines current season.
            
        Returns:
            List of game dictionaries for the entire season
        """
        self._check_rate_limit()
        
        try:
            # Determine season if not provided
            if season is None:
                now = datetime.now()
                year = now.year
                month = now.month
                
                if month >= 10:  # October onwards
                    season = f"{year}-{str(year + 1)[-2:]}"
                elif month <= 6:  # January to June
                    season = f"{year - 1}-{str(year)[-2:]}"
                else:  # July to September
                    season = f"{year - 1}-{str(year)[-2:]}"
            
            logger.info(f"Fetching full NBA season schedule for {season}")
            
            # Use LeagueGameFinder for full season schedule (more reliable than ScheduleLeagueV2)
            # LeagueGameFinder can return up to ~30,000 games and works better for full seasons
            def get_season_data():
                try:
                    # Convert season format: "2025-26" -> "2025-26" (LeagueGameFinder expects this format)
                    # For NBA, season_nullable format is "YYYY-YY" (e.g., "2025-26")
                    logger.info(f"Using LeagueGameFinder to fetch NBA season {season}")
                    
                    # LeagueGameFinder with season filter
                    # season_nullable format: "2025-26" for NBA
                    game_finder = leaguegamefinder.LeagueGameFinder(
                        season_nullable=season,
                        league_id_nullable='00',  # NBA league ID
                        headers=self.nba_headers,
                        timeout=60
                    )
                    return game_finder.get_dict()
                except Exception as e:
                    logger.warning(f"LeagueGameFinder failed: {e}, trying ScheduleLeagueV2 fallback")
                    # Fallback to ScheduleLeagueV2 if available
                    if scheduleleaguev2 is not None:
                        schedule_data = scheduleleaguev2.ScheduleLeagueV2(
                            season=season,
                            headers=self.nba_headers,
                            timeout=60
                        )
                        return schedule_data.get_dict()
                    return {}
            
            start_time = time.time()
            data = self._call_with_timeout(get_season_data, timeout_seconds=90)  # Longer timeout for full season
            response_time = int((time.time() - start_time) * 1000)
            
            all_games = []
            
            # LeagueGameFinder returns data in resultSets format
            # Note: Each game has TWO rows (one per team), so we need to deduplicate by game_id
            if 'resultSets' in data and len(data['resultSets']) > 0:
                # Get the game finder results
                game_results = data['resultSets'][0]
                game_rows = game_results.get('rowSet', [])
                headers = game_results.get('headers', [])
                
                logger.info(f"LeagueGameFinder returned {len(game_rows)} rows (will deduplicate by game)")
                
                # Group rows by game_id (each game has 2 rows - one per team)
                games_by_id = {}
                
                for row in game_rows:
                    if len(row) >= 7:
                        game_id = str(row[4]) if len(row) > 4 else ''
                        game_date_str = row[5] if len(row) > 5 else ''
                        matchup = row[6] if len(row) > 6 else ''
                        
                        if not game_id or game_id in games_by_id:
                            continue  # Skip if no game_id or already processed
                        
                        # Parse matchup (e.g., "BOS @ TOR" or "BOS vs. TOR")
                        matchup_parts = matchup.split()
                        if len(matchup_parts) < 3:
                            continue
                        
                        visitor_abbrev = matchup_parts[0]
                        home_abbrev = matchup_parts[2]
                        
                        # Find team data for this game (we have 2 rows, find both teams)
                        home_team_row = None
                        away_team_row = None
                        
                        for check_row in game_rows:
                            if len(check_row) >= 7 and str(check_row[4]) == game_id:
                                team_abbrev = check_row[2] if len(check_row) > 2 else ''
                                if team_abbrev == home_abbrev:
                                    home_team_row = check_row
                                elif team_abbrev == visitor_abbrev:
                                    away_team_row = check_row
                        
                        if not home_team_row or not away_team_row:
                            continue  # Skip if we can't find both teams
                        
                        # Extract team info from rows
                        # Format: [SEASON_ID, TEAM_ID, TEAM_ABBREVIATION, TEAM_NAME, GAME_ID, GAME_DATE, MATCHUP, ...]
                        home_team_id = str(home_team_row[1]) if len(home_team_row) > 1 else ''
                        home_team_name = home_team_row[3] if len(home_team_row) > 3 else ''
                        away_team_id = str(away_team_row[1]) if len(away_team_row) > 1 else ''
                        away_team_name = away_team_row[3] if len(away_team_row) > 3 else ''
                        
                        # Parse team name: TEAM_NAME is usually "City Name" (e.g., "Boston Celtics")
                        # Split into city and name (last word is usually the team name)
                        home_parts = home_team_name.split() if home_team_name else []
                        home_city = ' '.join(home_parts[:-1]) if len(home_parts) > 1 else (home_parts[0] if home_parts else '')
                        home_name = home_parts[-1] if home_parts else ''
                        
                        away_parts = away_team_name.split() if away_team_name else []
                        away_city = ' '.join(away_parts[:-1]) if len(away_parts) > 1 else (away_parts[0] if away_parts else '')
                        away_name = away_parts[-1] if away_parts else ''
                        
                        # Parse date - LeagueGameFinder returns YYYY-MM-DD format
                        try:
                            game_date_obj = datetime.strptime(game_date_str, '%Y-%m-%d')
                            game_date_formatted = game_date_obj.strftime('%m/%d/%Y')  # parse_game_data expects MM/DD/YYYY
                        except:
                            continue
                        
                        # Build game object compatible with parse_game_data
                        game_obj = {
                            'gameId': game_id,
                            'gameDate': game_date_formatted,
                            'homeTeam': {
                                'teamId': home_team_id,
                                'teamTricode': home_abbrev,
                                'teamCity': home_city,
                                'teamName': home_name
                            },
                            'awayTeam': {
                                'teamId': away_team_id,
                                'teamTricode': visitor_abbrev,
                                'teamCity': away_city,
                                'teamName': away_name
                            },
                            'gameStatus': 'scheduled',
                            '_leagueGameFinder': True
                        }
                        
                        parsed_game = self.parse_game_data(game_obj, game_date_formatted)
                        if parsed_game:
                            games_by_id[game_id] = parsed_game
                
                all_games = list(games_by_id.values())
                logger.info(f"Fetched {len(all_games)} unique games for NBA season {season}")
                return all_games
            
            # Fallback: Check if it's in leagueSchedule format (from ScheduleLeagueV2)
            elif 'leagueSchedule' in data and 'gameDates' in data['leagueSchedule']:
                game_dates = data['leagueSchedule']['gameDates']
                
                for game_date in game_dates:
                    game_date_str = game_date.get('gameDate', '')
                    games_for_date = game_date.get('games', [])
                    
                    for game in games_for_date:
                        parsed_game = self.parse_game_data(game, game_date_str)
                        if parsed_game:
                            all_games.append(parsed_game)
                
                logger.info(f"Fetched {len(all_games)} games for NBA season {season}")
                return all_games
            else:
                logger.warning(f"No schedule data found for season {season}")
                logger.debug(f"Response keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")
                return []
                
        except TimeoutError as e:
            logger.error(f"NBA API timeout fetching season schedule: {e}")
            return []
        except Exception as e:
            logger.error(f"Error fetching NBA season schedule: {e}")
            return []
    
    def get_live_scores(self, date: Optional[date] = None) -> List[Dict[str, Any]]:
        """
        Get live NBA scores for specified date.
        
        Uses live scoreboard endpoint which works without proxy and provides
        real-time scores for games in progress.
        
        Args:
            date: Date to get scores for (optional, defaults to today)
            
        Returns:
            List of game dictionaries with live score data
        """
        self._check_rate_limit()
        
        try:
            # Use live scoreboard endpoint (no proxy required, has live scores)
            logger.info("Fetching live scores using live scoreboard endpoint")
            board = scoreboard.ScoreBoard()
            games_data = board.games.get_dict()
            
            # Filter by date if specified
            games = []
            if date is not None:
                from dateutil import parser
                import pytz
                target_date_str = date.strftime('%Y-%m-%d')
                pacific_tz = pytz.timezone('US/Pacific')
                
                for game in games_data:
                    game_time_utc = game.get('gameTimeUTC', '')
                    if game_time_utc:
                        try:
                            game_time_obj = parser.parse(game_time_utc)
                            # Convert UTC to Pacific time for date comparison
                            # (since game times are in UTC but we want games for a Pacific date)
                            if game_time_obj.tzinfo is None:
                                game_time_obj = pytz.UTC.localize(game_time_obj)
                            game_time_pacific = game_time_obj.astimezone(pacific_tz)
                            game_date_pacific = game_time_pacific.date()
                            game_date_str = game_date_pacific.strftime('%Y-%m-%d')
                            
                            if game_date_str == target_date_str:
                                parsed_game = self._parse_live_scoreboard_game(game)
                                if parsed_game:
                                    games.append(parsed_game)
                        except Exception as e:
                            logger.debug(f"Error parsing game time {game_time_utc}: {e}")
                            pass
            
            # If no date specified, get all games (today's games and in-progress games)
            else:
                # Get today's date in UTC (since gameTimeUTC is in UTC)
                from datetime import datetime, timedelta
                import pytz
                utc_now = datetime.now(pytz.UTC)
                today_utc = utc_now.date()
                yesterday_utc = today_utc - timedelta(days=1)
                
                for game in games_data:
                    game_time_utc = game.get('gameTimeUTC', '')
                    game_status_text = game.get('gameStatusText', '').strip().lower()
                    is_in_progress = 'halftime' in game_status_text or 'live' in game_status_text or game.get('gameStatus') == 2
                    
                    if game_time_utc:
                        try:
                            game_time_obj = parser.parse(game_time_utc)
                            game_date_utc = game_time_obj.date()
                            # Include games from today or yesterday (if still in progress)
                            if game_date_utc == today_utc or (game_date_utc == yesterday_utc and is_in_progress):
                                parsed_game = self._parse_live_scoreboard_game(game)
                                if parsed_game:
                                    games.append(parsed_game)
                        except:
                            # If parsing fails, include it anyway if it looks like it's in progress
                            if is_in_progress:
                                parsed_game = self._parse_live_scoreboard_game(game)
                                if parsed_game:
                                    games.append(parsed_game)
                    else:
                        # If no time but game is in progress, include it
                        if is_in_progress:
                            parsed_game = self._parse_live_scoreboard_game(game)
                            if parsed_game:
                                games.append(parsed_game)
            
            # Deduplicate games by game_id
            seen_game_ids = set()
            unique_games = []
            for game in games:
                game_id = game.get('game_id', '')
                if game_id and game_id not in seen_game_ids:
                    seen_game_ids.add(game_id)
                    unique_games.append(game)
                elif not game_id:
                    # If no game_id, include it (shouldn't happen)
                    unique_games.append(game)
            
            logger.info(f"Retrieved {len(unique_games)} unique games with live scores (from {len(games)} total)")
            return unique_games
            
        except Exception as e:
            logger.error(f"Error fetching NBA live scores: {e}")
            return []
    
    def parse_game_data(self, raw_game: Dict[str, Any], game_date_str: str = None) -> Dict[str, Any]:
        """
        Parse raw NBA game data into standardized format.
        
        Args:
            raw_game: Raw game data from NBA API
            game_date_str: Game date string from parent game_date object
            
        Returns:
            Standardized game dictionary
        """
        try:
            # Handle live scoreboard format (from nba_api.live.nba.endpoints.scoreboard)
            if raw_game.get('_live_scoreboard'):
                return self._parse_live_scoreboard_game(raw_game, game_date_str)
            
            # Extract team information
            home_team = raw_game.get('homeTeam', {})
            away_team = raw_game.get('awayTeam', {})
            
            if not home_team or not away_team:
                logger.warning(f"No team data found for game {raw_game.get('gameId', 'unknown')}")
                return None
            
            # Handle date parsing
            if not game_date_str:
                game_date_str = raw_game.get('gameDate', '')

            if ' ' in game_date_str:
                game_date_str = game_date_str.split(' ')[0]
            if 'T' in game_date_str:
                game_date_str = game_date_str.split('T')[0]

            game_date = None
            for fmt in ('%m/%d/%Y', '%Y-%m-%d', '%Y%m%d'):
                try:
                    game_date_obj = datetime.strptime(game_date_str, fmt)
                    game_date = game_date_obj.strftime('%Y-%m-%d')
                    break
                except ValueError:
                    continue
            if not game_date:
                try:
                    from dateutil import parser as dateutil_parser
                    game_date = dateutil_parser.parse(game_date_str).strftime('%Y-%m-%d')
                except Exception:
                    logger.warning(f"Invalid date format: {game_date_str}")
                    return None
            
            # Detect season type using NBA API fields
            game_type = self._detect_nba_season_type(raw_game)
            
            # Parse period scores
            home_period_scores = self._parse_period_scores(raw_game.get('homeTeam', {}))
            visitor_period_scores = self._parse_period_scores(raw_game.get('awayTeam', {}))
            
            # Extract game time
            game_time = None
            game_time_utc = raw_game.get('gameTimeUTC') or raw_game.get('gameDateTimeUTC') or ''
            game_status_str = raw_game.get('gameStatusText', '') or raw_game.get('gameStatus', '') or ''

            if game_time_utc and 'T' in game_time_utc and game_time_utc.split('T')[1] != '00:00:00':
                try:
                    from dateutil import parser as dtparser
                    game_time_obj = dtparser.parse(game_time_utc)
                    if game_time_obj.tzinfo is None:
                        import pytz
                        game_time_obj = pytz.UTC.localize(game_time_obj)
                    game_time = game_time_obj
                except:
                    pass

            if not game_time and game_status_str:
                import re
                time_match = re.match(r'(\d{1,2}:\d{2}\s*(?:PM|AM)\s*(?:ET|EST|EDT))', game_status_str, re.IGNORECASE)
                if time_match:
                    try:
                        from dateutil import parser as dtparser
                        import pytz
                        time_part = time_match.group(1).strip()
                        time_part = re.sub(r'\s*(ET|EST|EDT)$', '', time_part, flags=re.IGNORECASE).strip()
                        date_part = game_date if game_date else datetime.now().strftime('%Y-%m-%d')
                        dt = dtparser.parse(f"{date_part} {time_part}")
                        eastern = pytz.timezone('US/Eastern')
                        game_time = eastern.localize(dt)
                    except:
                        pass
            
            return {
                'league': 'NBA',
                'game_id': raw_game.get('gameId', ''),
                'game_date': game_date,
                'game_time': game_time,
                'game_type': game_type,
                'home_team': f"{home_team.get('teamCity', '')} {home_team.get('teamName', '')}".strip(),
                'home_team_abbrev': home_team.get('teamTricode', ''),
                'home_team_id': str(home_team.get('teamId', '')),
                'home_wins': home_team.get('wins', 0),
                'home_losses': home_team.get('losses', 0),
                'home_score_total': home_team.get('score', 0),
                'visitor_team': f"{away_team.get('teamCity', '')} {away_team.get('teamName', '')}".strip(),
                'visitor_team_abbrev': away_team.get('teamTricode', ''),
                'visitor_team_id': str(away_team.get('teamId', '')),
                'visitor_wins': away_team.get('wins', 0),
                'visitor_losses': away_team.get('losses', 0),
                'visitor_score_total': away_team.get('score', 0),
                'game_status': self.normalize_game_status(raw_game.get('gameStatus', 'scheduled')),
                'current_period': raw_game.get('period', {}).get('current', ''),
                'time_remaining': raw_game.get('clock', ''),
                'is_final': 'final' in str(raw_game.get('gameStatus', '')).lower() or 'final' in str(raw_game.get('gameStatusText', '')).lower(),
                'is_overtime': raw_game.get('isOvertime', False),
                'home_period_scores': home_period_scores,
                'visitor_period_scores': visitor_period_scores,
            }
            
        except Exception as e:
            logger.error(f"Error parsing NBA game data: {e}")
            return None
    
    def _parse_live_scoreboard_game(self, raw_game: Dict[str, Any], game_date_str: str = None) -> Dict[str, Any]:
        """
        Parse game data from live scoreboard endpoint.
        
        Args:
            raw_game: Raw game data from live scoreboard
            game_date_str: Optional date string override
            
        Returns:
            Standardized game dictionary
        """
        try:
            from dateutil import parser
            
            home_team = raw_game.get('homeTeam', {})
            away_team = raw_game.get('awayTeam', {})
            
            # Check if teams are empty dicts or None
            if not home_team or not away_team or (isinstance(home_team, dict) and not home_team) or (isinstance(away_team, dict) and not away_team):
                # Try to get team data from other fields
                home_team_id = raw_game.get('homeTeamId', '')
                away_team_id = raw_game.get('awayTeamId', '')
                if not home_team_id and not away_team_id:
                    return None
                # Create minimal team objects if we have IDs
                if not home_team:
                    home_team = {'teamId': home_team_id}
                if not away_team:
                    away_team = {'teamId': away_team_id}
            
            # Parse game time from UTC
            game_time_utc = raw_game.get('gameTimeUTC', '')
            game_time = None
            game_date = None
            
            if game_time_utc:
                try:
                    # Parse UTC time (format: '2025-11-05T03:00:00Z')
                    import pytz
                    game_time_obj = parser.parse(game_time_utc)
                    # Ensure timezone-aware (assume UTC if not specified)
                    if game_time_obj.tzinfo is None:
                        game_time_obj = pytz.UTC.localize(game_time_obj)
                    # Convert to Pacific time for date (NBA games are scheduled in Pacific/Eastern time)
                    pacific_tz = pytz.timezone('US/Pacific')
                    game_time_pacific = game_time_obj.astimezone(pacific_tz)
                    game_date = game_time_pacific.date()  # Use Pacific date, not UTC date
                    game_time = game_time_obj  # Keep original UTC time for storage
                except:
                    pass
            
            if not game_date:
                if game_date_str:
                    try:
                        game_date = datetime.strptime(game_date_str, '%Y-%m-%d').date()
                    except:
                        game_date = datetime.now().date()
                else:
                    game_date = datetime.now().date()
            
            # Extract team info - handle both dict formats
            if isinstance(home_team, dict):
                home_team_name = f"{home_team.get('teamCity', '')} {home_team.get('teamName', '')}".strip()
                home_team_abbrev = home_team.get('teamTricode', '')
                home_team_id = str(home_team.get('teamId', ''))
                home_score = home_team.get('score', 0)
                home_wins = home_team.get('wins', 0)
                home_losses = home_team.get('losses', 0)
            else:
                home_team_name = str(home_team)
                home_team_abbrev = ''
                home_team_id = ''
                home_score = 0
                home_wins = 0
                home_losses = 0
            
            if isinstance(away_team, dict):
                away_team_name = f"{away_team.get('teamCity', '')} {away_team.get('teamName', '')}".strip()
                away_team_abbrev = away_team.get('teamTricode', '')
                away_team_id = str(away_team.get('teamId', ''))
                away_score = away_team.get('score', 0)
                away_wins = away_team.get('wins', 0)
                away_losses = away_team.get('losses', 0)
            else:
                away_team_name = str(away_team)
                away_team_abbrev = ''
                away_team_id = ''
                away_score = 0
                away_wins = 0
                away_losses = 0
            
            # Get period info - could be a number or dict
            period_info = raw_game.get('period', {})
            if isinstance(period_info, dict):
                current_period = str(period_info.get('current', period_info.get('period', '')))
            else:
                current_period = str(period_info) if period_info else ''
            
            # Get game clock - check both 'clock' and 'gameClock' fields
            game_clock_raw = raw_game.get('gameClock', '') or raw_game.get('clock', '')
            
            # Parse ISO 8601 duration format (PT02M54.00S) to readable format (2:54)
            game_clock = self._parse_game_clock(game_clock_raw)
            
            # Determine if game is final
            game_status_text = raw_game.get('gameStatusText', '').strip().lower()
            is_final = 'final' in game_status_text
            
            return {
                'league': 'NBA',
                'game_id': str(raw_game.get('gameId', '')),
                'game_date': game_date.strftime('%Y-%m-%d'),
                'game_time': game_time,
                'game_type': self._detect_nba_season_type(raw_game),
                'home_team': home_team_name,
                'home_team_abbrev': home_team_abbrev,
                'home_team_id': home_team_id,
                'home_wins': home_wins,
                'home_losses': home_losses,
                'home_score_total': home_score,
                'visitor_team': away_team_name,
                'visitor_team_abbrev': away_team_abbrev,
                'visitor_team_id': away_team_id,
                'visitor_wins': away_wins,
                'visitor_losses': away_losses,
                'visitor_score_total': away_score,
                'game_status': self.normalize_game_status(raw_game.get('gameStatusText', 'scheduled')),
                'current_period': current_period,
                'time_remaining': game_clock,
                'is_final': is_final,
                'is_overtime': False,  # Could be enhanced
                'home_period_scores': {},
                'visitor_period_scores': {},
            }
        except Exception as e:
            logger.error(f"Error parsing live scoreboard game data: {e}")
            return None
    
    def _parse_game_clock(self, clock_str: str) -> str:
        """
        Parse ISO 8601 duration format (PT02M54.00S) to readable format (2:54).
        
        Args:
            clock_str: ISO 8601 duration string (e.g., "PT02M54.00S", "PT08M008.00S")
            
        Returns:
            Formatted time string (e.g., "2:54", "8:08") or original string if parsing fails
        """
        if not clock_str or not clock_str.strip():
            return ''
        
        # Handle ISO 8601 duration format: PT[HH]H[MM]M[SS]S
        # Examples: PT02M54.00S, PT08M008.00S, PT12M34S
        try:
            # Match pattern: PT (optional hours H) (minutes M) (seconds S)
            match = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:([\d.]+)S)?', clock_str)
            if match:
                hours = int(match.group(1) or 0)
                minutes = int(match.group(2) or 0)
                seconds = float(match.group(3) or 0)
                
                # Convert to MM:SS format (NBA games don't typically exceed an hour)
                total_seconds = int(hours * 3600 + minutes * 60 + seconds)
                mins = total_seconds // 60
                secs = total_seconds % 60
                
                return f"{mins}:{secs:02d}"
            else:
                # If it doesn't match ISO format, return as-is (might already be formatted)
                return clock_str.strip()
        except Exception:
            # If parsing fails, return original
            return clock_str.strip()
    
    def parse_live_game_data(self, raw_game: List[Any]) -> Dict[str, Any]:
        """
        Parse live game data from scoreboard API.
        
        Args:
            raw_game: Raw game data from scoreboard API
            
        Returns:
            Standardized game dictionary
        """
        try:
            # Scoreboard API returns data in a different format
            # This is a simplified parser - you'll need to adjust based on actual API response
            return {
                'league': 'NBA',
                'game_id': str(raw_game[0]) if len(raw_game) > 0 else '',
                'game_date': datetime.now().strftime('%Y-%m-%d'),
                'game_type': self._detect_nba_season_type({'gameId': str(raw_game[0]) if len(raw_game) > 0 else ''}),
                'home_team': raw_game[6] if len(raw_game) > 6 else '',
                'home_team_abbrev': raw_game[7] if len(raw_game) > 7 else '',
                'home_team_id': str(raw_game[5]) if len(raw_game) > 5 else '',
                'home_score_total': raw_game[21] if len(raw_game) > 21 else 0,
                'visitor_team': raw_game[4] if len(raw_game) > 4 else '',
                'visitor_team_abbrev': raw_game[3] if len(raw_game) > 3 else '',
                'visitor_team_id': str(raw_game[2]) if len(raw_game) > 2 else '',
                'visitor_score_total': raw_game[20] if len(raw_game) > 20 else 0,
                'game_status': self.normalize_game_status(raw_game[8] if len(raw_game) > 8 else 'scheduled'),
                'current_period': raw_game[9] if len(raw_game) > 9 else '',
                'time_remaining': raw_game[10] if len(raw_game) > 10 else '',
                'is_final': raw_game[8] == 'Final' if len(raw_game) > 8 else False,
                'is_overtime': raw_game[11] if len(raw_game) > 11 else False,
            }
            
        except Exception as e:
            logger.error(f"Error parsing NBA live game data: {e}")
            return None
    
    def _detect_nba_season_type(self, game_data: Dict[str, Any]) -> str:
        game_label = game_data.get('gameLabel', '')
        game_subtype = game_data.get('gameSubtype', '')

        if game_label == 'Preseason':
            return 'preseason'
        elif game_label == 'Emirates NBA Cup':
            return 'nba_cup'
        elif 'playoff' in game_label.lower() or 'playoff' in game_subtype.lower():
            return 'playoffs'

        # Detect from game ID: 002=regular, 003=allstar, 004=playoffs, 005=playin
        game_id = str(game_data.get('gameId', ''))
        if len(game_id) >= 3:
            prefix = game_id[:3]
            if prefix == '004':
                return 'playoffs'
            elif prefix == '005':
                return 'playoffs'
            elif prefix == '003':
                return 'allstar'
            elif prefix == '001':
                return 'preseason'

        return 'regular'
    
    def _parse_period_scores(self, team_data: Dict[str, Any]) -> Dict[str, int]:
        """
        Parse period scores from team data.
        
        Args:
            team_data: Team data from NBA API
            
        Returns:
            Dictionary of period scores
        """
        scores = {}
        
        # NBA has quarters (Q1, Q2, Q3, Q4)
        for i in range(1, 5):
            quarter_key = f'Q{i}'
            if quarter_key in team_data:
                scores[f'q{i}'] = team_data[quarter_key]
        
        return scores
