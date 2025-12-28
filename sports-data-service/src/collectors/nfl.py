"""
NFL data collector for the sports data service.
"""

import requests
import time
from datetime import datetime, date
from typing import Dict, List, Optional, Any
import logging

from .base import BaseCollector

logger = logging.getLogger(__name__)


class NFLCollector(BaseCollector):
    """NFL data collector using the Tank01 NFL API."""
    
    # Standard NFL team abbreviation mapping
    # Maps various API abbreviations to standard 3-letter abbreviations
    TEAM_ABBREV_MAP = {
        # Standard abbreviations (from fetch_nfl_team_data.py)
        'ARI': 'ARI', 'ATL': 'ATL', 'BAL': 'BAL', 'BUF': 'BUF',
        'CAR': 'CAR', 'CHI': 'CHI', 'CIN': 'CIN', 'CLE': 'CLE',
        'DAL': 'DAL', 'DEN': 'DEN', 'DET': 'DET', 'GB': 'GB',
        'HOU': 'HOU', 'IND': 'IND', 'JAX': 'JAX', 'KC': 'KC',
        'LV': 'LV', 'LAC': 'LAC', 'LAR': 'LAR', 'MIA': 'MIA',
        'MIN': 'MIN', 'NE': 'NE', 'NO': 'NO', 'NYG': 'NYG',
        'NYJ': 'NYJ', 'PHI': 'PHI', 'PIT': 'PIT', 'SF': 'SF',
        'SEA': 'SEA', 'TB': 'TB', 'TEN': 'TEN', 'WSH': 'WSH',
        # Common API variations that need mapping
        'GNB': 'GB',   # Green Bay Packers
        'KAN': 'KC',   # Kansas City Chiefs
        'OAK': 'LV',   # Las Vegas Raiders (old)
        'LVR': 'LV',   # Las Vegas Raiders (alternative)
        'NWE': 'NE',   # New England Patriots
        'NOS': 'NO',   # New Orleans Saints
        'SFO': 'SF',   # San Francisco 49ers
        'TAM': 'TB',   # Tampa Bay Buccaneers
        'WAS': 'WSH',  # Washington Commanders
        'JAC': 'JAX',  # Jacksonville Jaguars
    }
    
    def __init__(self, api_key: str = None):
        super().__init__("NFL")
        self.api_key = api_key or "913a411db3msh6a5a6bcd37bb86dp1cb551jsnd7718d4e5a0e"
        self.base_url = "https://tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com"
        self.headers = {
            "x-rapidapi-key": self.api_key,
            "x-rapidapi-host": "tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com"
        }
    
    def _normalize_abbrev(self, abbrev: str) -> str:
        """
        Normalize team abbreviation to standard 3-letter format.
        
        Args:
            abbrev: Team abbreviation from API (may be non-standard)
            
        Returns:
            Standard 3-letter abbreviation
        """
        if not abbrev:
            return abbrev
        
        abbrev_upper = abbrev.upper().strip()
        # Return mapped abbreviation if available, otherwise return uppercase version
        return self.TEAM_ABBREV_MAP.get(abbrev_upper, abbrev_upper)
    
    def _normalize_period(self, period: str) -> str:
        """
        Normalize period string to just the number (e.g., "3rd" -> "3", "4th" -> "4").
        
        Args:
            period: Period string from API (e.g., "1st", "2nd", "3rd", "4th", "OT")
            
        Returns:
            Normalized period string (e.g., "1", "2", "3", "4", "OT")
        """
        if not period:
            return period
        
        period = period.strip()
        # Remove ordinal suffixes (st, nd, rd, th)
        import re
        # Match patterns like "1st", "2nd", "3rd", "4th" and extract just the number
        match = re.match(r'^(\d+)(st|nd|rd|th)?$', period, re.IGNORECASE)
        if match:
            return match.group(1)  # Return just the number
        
        # If it's already just a number or "OT", return as-is
        if period.isdigit() or period.upper() in ('OT', 'OVERTIME'):
            return period
        
        # Try to extract number from strings like "Q3", "Quarter 3", etc.
        number_match = re.search(r'(\d+)', period)
        if number_match:
            return number_match.group(1)
        
        # If no number found, return original (might be "OT" or similar)
        return period
    
    def get_schedule(self, date: Optional[date] = None) -> List[Dict[str, Any]]:
        """
        Get NFL schedule for specified date or current games.
        
        Args:
            date: Date to get schedule for (optional, defaults to today)
            
        Returns:
            List of game dictionaries
        """
        self._check_rate_limit()
        
        try:
            if date:
                # Tank01 API requires YYYYMMDD format (no dashes)
                date_str = date.strftime('%Y%m%d')
            else:
                date_str = datetime.now().strftime('%Y%m%d')
            
            # Tank01 API requires gameDate as a query parameter, not path parameter
            url = f"{self.base_url}/getNFLGamesForDate"
            params = {'gameDate': date_str}
            
            start_time = time.time()
            response = requests.get(url, headers=self.headers, params=params, timeout=self.api_timeout)
            response_time = int((time.time() - start_time) * 1000)
            
            if response.status_code == 200:
                data = response.json()
                games = []
                
                # Tank01 API returns data in a wrapper: {"statusCode": 200, "body": [...]}
                if isinstance(data, dict):
                    if 'body' in data:
                        data = data['body']
                    elif 'games' in data:
                        data = data['games']
                
                if isinstance(data, list):
                    for game in data:
                        parsed_game = self.parse_game_data(game)
                        if parsed_game:
                            games.append(parsed_game)
                
                return games
            else:
                logger.error(f"NFL API error: {response.status_code} - {response.text[:200]}")
                return []
                
        except Exception as e:
            logger.error(f"Error fetching NFL schedule: {e}")
            return []
    
    def get_season_schedule(self, season: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get full NFL season schedule.
        
        Args:
            season: Season year (e.g., "2024"). If None, uses current year.
            
        Returns:
            List of game dictionaries for the entire season
            
        Note: NFL API uses date-specific endpoints. For full season, we would
        need to fetch week-by-week or day-by-day. This is a placeholder.
        """
        logger.warning("NFL full season fetch not implemented - would need to fetch week-by-week")
        # For now, return empty - could implement week-by-week fetching if needed
        return []
    
    def get_live_scores(self, date: Optional[date] = None) -> List[Dict[str, Any]]:
        """
        Get live NFL scores for specified date.
        
        Args:
            date: Date to get scores for (optional, defaults to today)
            
        Returns:
            List of game dictionaries with live score data
        """
        self._check_rate_limit()
        
        try:
            if date:
                # Tank01 API requires YYYYMMDD format (no dashes)
                date_str = date.strftime('%Y%m%d')
            else:
                date_str = datetime.now().strftime('%Y%m%d')
            
            # Use getNFLScoresOnly endpoint for live scores (returns actual scores, game clock, etc.)
            url = f"{self.base_url}/getNFLScoresOnly"
            params = {'gameDate': date_str, 'topPerformers': 'false'}
            
            start_time = time.time()
            response = requests.get(url, headers=self.headers, params=params, timeout=self.api_timeout)
            response_time = int((time.time() - start_time) * 1000)
            
            if response.status_code == 200:
                data = response.json()
                games = []
                
                # Tank01 API returns: {"statusCode": 200, "body": {"gameID": {...}}}
                if isinstance(data, dict):
                    if 'body' in data:
                        body_data = data['body']
                        # body_data is a dict keyed by gameID
                        if isinstance(body_data, dict):
                            for game_id, game_data in body_data.items():
                                # Parse the game data from getNFLScoresOnly format
                                parsed_game = self._parse_scores_only_game(game_data, game_id)
                                if parsed_game:
                                    games.append(parsed_game)
                    elif 'games' in data:
                        # Fallback format
                        for game in data['games']:
                            parsed_game = self.parse_game_data(game)
                            if parsed_game:
                                games.append(parsed_game)
                
                # If no live scores found, fallback to getNFLGamesForDate for scheduled games
                if not games:
                    url_schedule = f"{self.base_url}/getNFLGamesForDate"
                    params_schedule = {'gameDate': date_str}
                    response_schedule = requests.get(url_schedule, headers=self.headers, params=params_schedule, timeout=self.api_timeout)
                    
                    if response_schedule.status_code == 200:
                        schedule_data = response_schedule.json()
                        if isinstance(schedule_data, dict):
                            if 'body' in schedule_data:
                                schedule_data = schedule_data['body']
                            elif 'games' in schedule_data:
                                schedule_data = schedule_data['games']
                        
                        if isinstance(schedule_data, list):
                            for game in schedule_data:
                                parsed_game = self.parse_game_data(game)
                                if parsed_game:
                                    games.append(parsed_game)
                
                return games
            else:
                logger.error(f"NFL API error: {response.status_code} - {response.text[:200]}")
                return []
                
        except Exception as e:
            logger.error(f"Error fetching NFL live scores: {e}")
            return []
    
    def _parse_scores_only_game(self, game_data: Dict[str, Any], game_id: str) -> Dict[str, Any]:
        """
        Parse game data from getNFLScoresOnly endpoint format.
        
        Args:
            game_data: Game data from getNFLScoresOnly response
            game_id: Game ID (e.g., "20251113_NYJ@NE")
            
        Returns:
            Standardized game dictionary
        """
        try:
            line_score = game_data.get('lineScore', {})
            away_line = line_score.get('away', {})
            home_line = line_score.get('home', {})
            
            # Extract scores
            away_score = int(away_line.get('totalPts', 0) or 0)
            home_score = int(home_line.get('totalPts', 0) or 0)
            
            # Extract period and game clock
            # Normalize period: "3rd" -> "3", "4th" -> "4", etc.
            period_raw = line_score.get('period', '')
            period = self._normalize_period(period_raw)
            game_clock = line_score.get('gameClock', '')
            
            # Get team IDs and abbreviations
            away_id = str(away_line.get('teamID', ''))
            home_id = str(home_line.get('teamID', ''))
            away_abbrev = self._normalize_abbrev(away_line.get('teamAbv', game_data.get('away', '')))
            home_abbrev = self._normalize_abbrev(home_line.get('teamAbv', game_data.get('home', '')))
            
            # Get team records
            team_records = self._fetch_team_records()
            home_record = team_records.get(home_id, {'wins': 0, 'losses': 0, 'ties': 0})
            away_record = team_records.get(away_id, {'wins': 0, 'losses': 0, 'ties': 0})
            
            # Parse quarter scores
            away_quarters = []
            home_quarters = []
            for q in ['Q1', 'Q2', 'Q3', 'Q4']:
                away_q_score = away_line.get(q, '')
                home_q_score = home_line.get(q, '')
                if away_q_score:
                    away_quarters.append(int(away_q_score))
                else:
                    away_quarters.append(0)
                if home_q_score:
                    home_quarters.append(int(home_q_score))
                else:
                    home_quarters.append(0)
            
            # Parse game date from gameID (format: YYYYMMDD_TEAM@TEAM)
            game_date_str = game_id.split('_')[0] if '_' in game_id else ''
            try:
                if len(game_date_str) == 8 and game_date_str.isdigit():
                    game_date_obj = datetime.strptime(game_date_str, '%Y%m%d')
                    game_date = game_date_obj.strftime('%Y-%m-%d')
                else:
                    game_date = datetime.now().strftime('%Y-%m-%d')
            except ValueError:
                game_date = datetime.now().strftime('%Y-%m-%d')
            
            # Parse game time
            game_time = None
            if game_data.get('gameTime_epoch'):
                try:
                    game_time = datetime.fromtimestamp(float(game_data['gameTime_epoch']))
                except (ValueError, TypeError):
                    pass
            
            # Determine game status
            game_status_raw = game_data.get('gameStatus', '').upper()
            is_final = 'FINAL' in game_status_raw or game_status_raw == 'FINAL'
            is_in_progress = 'LIVE' in game_status_raw or 'IN PROGRESS' in game_status_raw or game_status_raw == 'LIVE'
            
            if is_final:
                normalized_status = 'final'
            elif is_in_progress:
                normalized_status = 'in_progress'
            else:
                normalized_status = 'scheduled'
            
            return {
                'league': 'NFL',
                'game_id': game_id,
                'game_date': game_date,
                'game_time': game_time,
                'game_type': 'regular',  # Could be enhanced to detect playoffs
                'home_team': home_abbrev,  # Use abbrev as name if full name not available
                'home_team_abbrev': home_abbrev,
                'home_team_id': home_id,
                'home_wins': home_record.get('wins', 0),
                'home_losses': home_record.get('losses', 0),
                'home_score_total': home_score,
                'visitor_team': away_abbrev,
                'visitor_team_abbrev': away_abbrev,
                'visitor_team_id': away_id,
                'visitor_wins': away_record.get('wins', 0),
                'visitor_losses': away_record.get('losses', 0),
                'visitor_score_total': away_score,
                'game_status': normalized_status,
                'current_period': period,
                'time_remaining': game_clock,
                'is_final': is_final,
                'is_overtime': False,  # Could be enhanced
                'home_period_scores': home_quarters,
                'visitor_period_scores': away_quarters,
            }
            
        except Exception as e:
            logger.error(f"Error parsing scores-only game data: {e}")
            return None
    
    def _get_game_details(self, game_id: str) -> Dict[str, Any]:
        """Get detailed game data from NFL API."""
        url = f"{self.base_url}/getNFLGameBoxScore/{game_id}"
        response = requests.get(url, headers=self.headers, timeout=self.api_timeout)
        
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to get game details: {response.status_code}")
    
    def parse_game_data(self, raw_game: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse raw NFL game data into standardized format.
        
        Args:
            raw_game: Raw game data from NFL API
            
        Returns:
            Standardized game dictionary
        """
        try:
            # Tank01 API returns different structures:
            # - Scheduled games: home/away (abbrevs), teamIDHome/teamIDAway
            # - Live games: homeTeam/awayTeam (objects with full details)
            
            # Check if we have full team objects (live games) or just abbreviations (scheduled)
            home_team = raw_game.get('homeTeam', {})
            away_team = raw_game.get('awayTeam', {})
            
            # If no team objects, use abbreviations and IDs from scheduled game format
            if not home_team or not away_team:
                home_abbrev_raw = raw_game.get('home', '')
                away_abbrev_raw = raw_game.get('away', '')
                home_id = raw_game.get('teamIDHome', '')
                away_id = raw_game.get('teamIDAway', '')
                
                if not home_abbrev_raw or not away_abbrev_raw:
                    logger.warning(f"No team data found for game {raw_game.get('gameID', 'unknown')}")
                    return None
                
                # Normalize abbreviations to standard format
                home_abbrev = self._normalize_abbrev(home_abbrev_raw)
                away_abbrev = self._normalize_abbrev(away_abbrev_raw)
                
                # Get team records from cache (if available)
                team_records = self._fetch_team_records()
                home_record = team_records.get(str(home_id), {'wins': 0, 'losses': 0, 'ties': 0})
                away_record = team_records.get(str(away_id), {'wins': 0, 'losses': 0, 'ties': 0})
                
                # Create minimal team objects for scheduled games
                home_team = {
                    'teamName': home_abbrev,  # Will use abbrev as name if full name not available
                    'teamAbbr': home_abbrev,
                    'teamID': home_id,
                    'score': 0,
                    'wins': home_record.get('wins', 0),
                    'losses': home_record.get('losses', 0),
                    'ties': home_record.get('ties', 0)
                }
                away_team = {
                    'teamName': away_abbrev,
                    'teamAbbr': away_abbrev,
                    'teamID': away_id,
                    'score': 0,
                    'wins': away_record.get('wins', 0),
                    'losses': away_record.get('losses', 0),
                    'ties': away_record.get('ties', 0)
                }
            else:
                # For live games, normalize abbreviations from team objects
                if 'teamAbbr' in home_team:
                    home_team['teamAbbr'] = self._normalize_abbrev(home_team.get('teamAbbr', ''))
                if 'teamAbbrev' in home_team:
                    home_team['teamAbbrev'] = self._normalize_abbrev(home_team.get('teamAbbrev', ''))
                if 'teamAbbr' in away_team:
                    away_team['teamAbbr'] = self._normalize_abbrev(away_team.get('teamAbbr', ''))
                if 'teamAbbrev' in away_team:
                    away_team['teamAbbrev'] = self._normalize_abbrev(away_team.get('teamAbbrev', ''))
                
                # For live games, if wins/losses are missing, try to get from cache
                if not home_team.get('wins') and not home_team.get('losses'):
                    team_records = self._fetch_team_records()
                    home_id = str(home_team.get('teamID', ''))
                    if home_id in team_records:
                        home_team['wins'] = team_records[home_id].get('wins', 0)
                        home_team['losses'] = team_records[home_id].get('losses', 0)
                        home_team['ties'] = team_records[home_id].get('ties', 0)
                
                if not away_team.get('wins') and not away_team.get('losses'):
                    team_records = self._fetch_team_records()
                    away_id = str(away_team.get('teamID', ''))
                    if away_id in team_records:
                        away_team['wins'] = team_records[away_id].get('wins', 0)
                        away_team['losses'] = team_records[away_id].get('losses', 0)
                        away_team['ties'] = team_records[away_id].get('ties', 0)
            
            # Parse game date (can be YYYYMMDD or YYYY-MM-DD format)
            game_date_str = raw_game.get('gameDate', '')
            try:
                # Try YYYYMMDD format first (Tank01 API format)
                if len(game_date_str) == 8 and game_date_str.isdigit():
                    game_date_obj = datetime.strptime(game_date_str, '%Y%m%d')
                else:
                    # Try YYYY-MM-DD format
                    game_date_obj = datetime.strptime(game_date_str, '%Y-%m-%d')
                game_date = game_date_obj.strftime('%Y-%m-%d')
            except ValueError:
                logger.warning(f"Invalid date format: {game_date_str}")
                game_date = datetime.now().strftime('%Y-%m-%d')
            
            # Check if game should be in progress based on game time
            # If API still shows "Scheduled" but game time has passed, mark as potentially in progress
            game_status_raw = raw_game.get('gameStatus', '').upper()
            game_time_epoch = raw_game.get('gameTime_epoch')
            if game_status_raw == 'SCHEDULED' and game_time_epoch:
                try:
                    import time as time_module
                    current_epoch = time_module.time()
                    game_start_epoch = float(game_time_epoch)
                    # If game should have started (more than 10 minutes ago), treat as in progress
                    if current_epoch >= game_start_epoch + 600:  # 10 minutes after scheduled time
                        # Override status to in_progress if we have scores or if enough time has passed
                        if home_team.get('score', 0) > 0 or away_team.get('score', 0) > 0:
                            game_status_raw = 'IN PROGRESS'
                        elif current_epoch >= game_start_epoch + 1800:  # 30 minutes after, assume in progress
                            game_status_raw = 'IN PROGRESS'
                except (ValueError, TypeError):
                    pass
            
            # Parse game time
            game_time = None
            if raw_game.get('gameTime'):
                try:
                    game_time_str = raw_game['gameTime']
                    # Handle formats like "8:15p" or "8:15 PM" or "20:15"
                    if 'p' in game_time_str.lower() or 'a' in game_time_str.lower():
                        # 12-hour format with a/p suffix
                        time_clean = game_time_str.replace('p', ' PM').replace('a', ' AM').replace('P', ' PM').replace('A', ' AM')
                        game_time = datetime.strptime(f"{game_date} {time_clean}", '%Y-%m-%d %I:%M %p')
                    else:
                        # Try 24-hour format
                        game_time = datetime.strptime(f"{game_date} {game_time_str}", '%Y-%m-%d %H:%M:%S')
                except:
                    # If parsing fails, try using epoch time if available
                    if raw_game.get('gameTime_epoch'):
                        try:
                            game_time = datetime.fromtimestamp(float(raw_game['gameTime_epoch']))
                        except:
                            pass
            
            # Detect game type
            game_type = self._detect_nfl_game_type(raw_game)
            
            # Parse quarter scores (only available for live/finished games)
            home_period_scores = self._parse_quarter_scores(home_team.get('quarters', [])) if home_team.get('quarters') else []
            visitor_period_scores = self._parse_quarter_scores(away_team.get('quarters', [])) if away_team.get('quarters') else []
            
            return {
                'league': 'NFL',
                'game_id': str(raw_game.get('gameID', '')),
                'game_date': game_date,
                'game_time': game_time,
                'game_type': game_type,
                'home_team': home_team.get('teamName', ''),
                'home_team_abbrev': self._normalize_abbrev(home_team.get('teamAbbr', home_team.get('teamAbbrev', ''))),
                'home_team_id': str(home_team.get('teamID', '')),
                'home_wins': home_team.get('wins', 0),
                'home_losses': home_team.get('losses', 0),
                'home_score_total': home_team.get('score', 0),
                'visitor_team': away_team.get('teamName', ''),
                'visitor_team_abbrev': self._normalize_abbrev(away_team.get('teamAbbr', away_team.get('teamAbbrev', ''))),
                'visitor_team_id': str(away_team.get('teamID', '')),
                'visitor_wins': away_team.get('wins', 0),
                'visitor_losses': away_team.get('losses', 0),
                'visitor_score_total': away_team.get('score', 0),
                'game_status': self.normalize_game_status(game_status_raw if 'game_status_raw' in locals() else raw_game.get('gameStatus', 'scheduled')),
                'current_period': self._normalize_period(raw_game.get('quarter', '')),
                'time_remaining': raw_game.get('timeRemaining', ''),
                'is_final': raw_game.get('gameStatus') == 'Final',
                'is_overtime': raw_game.get('isOvertime', False),
                'home_period_scores': home_period_scores,
                'visitor_period_scores': visitor_period_scores,
            }
            
        except Exception as e:
            logger.error(f"Error parsing NFL game data: {e}")
            return None
    
    def parse_live_game_data(self, raw_game: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse live game data from detailed NFL API.
        
        Args:
            raw_game: Raw detailed game data from NFL API
            
        Returns:
            Standardized game dictionary
        """
        try:
            # This is a simplified parser for detailed game data
            # You'll need to adjust based on the actual API response structure
            home_team = raw_game.get('homeTeam', {})
            away_team = raw_game.get('awayTeam', {})
            
            return {
                'league': 'NFL',
                'game_id': str(raw_game.get('gameID', '')),
                'game_date': datetime.now().strftime('%Y-%m-%d'),
                'game_type': 'regular',
                'home_team': home_team.get('teamName', ''),
                'home_team_abbrev': self._normalize_abbrev(home_team.get('teamAbbr', home_team.get('teamAbbrev', ''))),
                'home_team_id': str(home_team.get('teamID', '')),
                'home_score_total': home_team.get('score', 0),
                'visitor_team': away_team.get('teamName', ''),
                'visitor_team_abbrev': self._normalize_abbrev(away_team.get('teamAbbr', away_team.get('teamAbbrev', ''))),
                'visitor_team_id': str(away_team.get('teamID', '')),
                'visitor_score_total': away_team.get('score', 0),
                'game_status': self.normalize_game_status(raw_game.get('gameStatus', 'scheduled')),
                'current_period': self._normalize_period(raw_game.get('quarter', '')),
                'time_remaining': raw_game.get('timeRemaining', ''),
                'is_final': raw_game.get('gameStatus') == 'Final',
                'is_overtime': raw_game.get('isOvertime', False),
                'home_period_scores': self._parse_quarter_scores(home_team.get('quarters', [])),
                'visitor_period_scores': self._parse_quarter_scores(away_team.get('quarters', [])),
            }
            
        except Exception as e:
            logger.error(f"Error parsing NFL live game data: {e}")
            return None
    
    def _detect_nfl_game_type(self, game_data: Dict[str, Any]) -> str:
        """
        Detect NFL game type.
        
        Args:
            game_data: Raw game data from NFL API
            
        Returns:
            Normalized game type
        """
        # NFL game types are typically regular season, playoffs, or preseason
        # You'll need to adjust this based on the actual API response
        return 'regular'
    
    def _parse_quarter_scores(self, quarters: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Parse quarter scores from NFL quarters data.
        
        Args:
            quarters: List of quarter data from NFL API
            
        Returns:
            Dictionary of quarter scores
        """
        scores = {}
        
        for i, quarter in enumerate(quarters):
            quarter_num = i + 1
            score = quarter.get('score', 0)
            scores[f'q{quarter_num}'] = score
        
        return scores
    
    def _sync_teams_from_api(self):
        """
        Sync NFL teams from API to teams table.
        Uses /getNFLTeams endpoint with proper parameters to get team records.
        """
        try:
            from database import get_db_session
            from models import Team
            
            teams_synced = {}
            
            # Use /getNFLTeams endpoint with proper parameters (as shown in user's example)
            endpoint = f"{self.base_url}/getNFLTeams"
            # Determine current NFL season (typically starts in September)
            from datetime import datetime
            current_year = datetime.now().year
            current_month = datetime.now().month
            # NFL season year is the year in which the season starts (if after August, use current year, else previous year)
            season_year = current_year if current_month >= 9 else current_year - 1
            
            params = {
                "sortBy": "standings",
                "rosters": "false",
                "schedules": "false",
                "topPerformers": "true",
                "teamStats": "true",
                "teamStatsSeason": str(season_year)  # Dynamic based on current season
            }
            
            try:
                logger.debug(f"Syncing NFL teams from: {endpoint}")
                self._check_rate_limit()
                response = requests.get(endpoint, headers=self.headers, params=params, timeout=self.api_timeout)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Parse response - Tank01 API returns: {"statusCode": 200, "body": [...]}
                    teams_data = []
                    if isinstance(data, dict):
                        if 'body' in data:
                            if isinstance(data['body'], list):
                                teams_data = data['body']
                            elif 'teams' in data['body']:
                                teams_data = data['body']['teams']
                        elif 'teams' in data:
                            teams_data = data['teams']
                    elif isinstance(data, list):
                        teams_data = data
                    
                    for team in teams_data:
                        team_id = str(team.get('teamID', team.get('id', '')))
                        team_abbrev_raw = team.get('teamAbv', team.get('teamAbbr', team.get('teamAbbrev', '')))
                        team_abbrev = self._normalize_abbrev(team_abbrev_raw)  # Normalize abbreviation
                        team_name = team.get('teamName', '')
                        team_city = team.get('teamCity', '')
                        
                        if team_id and team_abbrev:
                            # Build full team name
                            if team_city and team_name:
                                full_name = f"{team_city} {team_name}"
                            elif team_name:
                                full_name = team_name
                            else:
                                full_name = team_abbrev
                            
                            # Extract wins, losses, ties (API uses 'wins', 'loss', 'tie')
                            wins = int(team.get('wins', 0) or 0)
                            losses = int(team.get('loss', team.get('losses', 0)) or 0)
                            ties = int(team.get('tie', team.get('ties', 0)) or 0)
                            
                            teams_synced[team_abbrev] = {
                                'team_id': team_id,
                                'team_name': full_name,
                                'team_abbrev': team_abbrev,
                                'wins': wins,
                                'losses': losses,
                                'ties': ties
                            }
                    
                    if teams_synced:
                        logger.info(f"Successfully fetched {len(teams_synced)} NFL teams from API")
                        
            except Exception as e:
                logger.warning(f"Could not sync from {endpoint}: {e}")
                # Fall through to fallback method
            
            # If no teams from standings, try to extract from recent schedule
            if not teams_synced:
                try:
                    from datetime import timedelta
                    today = date.today()
                    # Try last 7 days of schedules
                    for days_ago in range(7):
                        check_date = today - timedelta(days=days_ago)
                        games = self.get_schedule(check_date)
                        for game in games:
                            home_abbrev = game.get('home_team_abbrev', '')
                            home_id = game.get('home_team_id', '')
                            home_name = game.get('home_team', '')
                            visitor_abbrev = game.get('visitor_team_abbrev', '')
                            visitor_id = game.get('visitor_team_id', '')
                            visitor_name = game.get('visitor_team', '')
                            
                            # Normalize abbreviations
                            home_abbrev = self._normalize_abbrev(home_abbrev)
                            visitor_abbrev = self._normalize_abbrev(visitor_abbrev)
                            
                            if home_id and home_abbrev and home_abbrev not in teams_synced:
                                teams_synced[home_abbrev] = {
                                    'team_id': home_id,
                                    'team_name': home_name or home_abbrev,
                                    'team_abbrev': home_abbrev,
                                    'wins': 0,
                                    'losses': 0,
                                    'ties': 0
                                }
                            
                            if visitor_id and visitor_abbrev and visitor_abbrev not in teams_synced:
                                teams_synced[visitor_abbrev] = {
                                    'team_id': visitor_id,
                                    'team_name': visitor_name or visitor_abbrev,
                                    'team_abbrev': visitor_abbrev,
                                    'wins': 0,
                                    'losses': 0,
                                    'ties': 0
                                }
                        
                        if len(teams_synced) >= 32:  # All 32 NFL teams
                            break
                except Exception as e:
                    logger.debug(f"Could not extract teams from schedule: {e}")
            
            # Update teams table
            if teams_synced:
                with get_db_session() as db:
                    for abbrev, team_info in teams_synced.items():
                        # Check if team exists
                        existing_team = db.query(Team).filter(
                            Team.league == 'NFL',
                            Team.team_abbrev == abbrev
                        ).first()
                        
                        if existing_team:
                            # Update API team ID and records if missing or different
                            updated = False
                            if existing_team.api_team_id != team_info['team_id']:
                                existing_team.api_team_id = team_info['team_id']
                                updated = True
                            if existing_team.wins != team_info['wins'] or existing_team.losses != team_info['losses'] or existing_team.ties != team_info['ties']:
                                existing_team.wins = team_info['wins']
                                existing_team.losses = team_info['losses']
                                existing_team.ties = team_info['ties']
                                updated = True
                            if existing_team.team_name != team_info['team_name']:
                                existing_team.team_name = team_info['team_name']
                                updated = True
                            if updated:
                                db.commit()
                        else:
                            # Create new team
                            new_team = Team(
                                league='NFL',
                                team_name=team_info['team_name'],
                                team_abbrev=abbrev,
                                api_team_id=team_info['team_id'],
                                wins=team_info['wins'],
                                losses=team_info['losses'],
                                ties=team_info['ties']
                            )
                            db.add(new_team)
                            db.commit()
                    
                    logger.info(f"Synced {len(teams_synced)} NFL teams to database")
                    
        except Exception as e:
            logger.debug(f"Could not sync NFL teams from API (non-critical): {e}")
    
    def _fetch_team_records(self) -> Dict[str, Dict[str, int]]:
        """
        Fetch team records (W-L-T) from database first, then API if needed.
        Uses teams table for abbreviation-to-ID mapping.
        
        Returns:
            Dictionary mapping team_id (string) to {'wins': int, 'losses': int, 'ties': int}
        """
        # Check cache
        if self._standings_cache_time and time.time() - self._standings_cache_time < self._standings_cache_ttl:
            return self._team_records_cache
        
        try:
            # First, try to get records from database
            from database import get_db_session
            from models import Team
            
            records = {}
            with get_db_session() as db:
                teams = db.query(Team).filter(Team.league == 'NFL').all()
                for team in teams:
                    if team.api_team_id and (team.wins is not None or team.losses is not None or team.ties is not None):
                        records[str(team.api_team_id)] = {
                            'wins': team.wins or 0,
                            'losses': team.losses or 0,
                            'ties': team.ties or 0
                        }
            
            # If we have records from DB, use them (but still check if cache is stale)
            if records:
                # Check if DB records are recent (updated within last 24 hours)
                # For now, we'll use DB records if available
                self._team_records_cache = records
                self._standings_cache_time = time.time()
                logger.debug(f"Loaded {len(records)} NFL team records from database")
                return records
            
            # If no DB records, sync teams from API first
            self._sync_teams_from_api()
            
            # Try DB again after sync
            with get_db_session() as db:
                teams = db.query(Team).filter(Team.league == 'NFL').all()
                for team in teams:
                    if team.api_team_id and (team.wins is not None or team.losses is not None or team.ties is not None):
                        records[str(team.api_team_id)] = {
                            'wins': team.wins or 0,
                            'losses': team.losses or 0,
                            'ties': team.ties or 0
                        }
            
            if records:
                self._team_records_cache = records
                self._standings_cache_time = time.time()
                logger.info(f"Loaded {len(records)} NFL team records from database after sync")
                return records
            
            # Fallback: fetch directly from API (if DB sync failed)
            # Use the same endpoint and parameters as _sync_teams_from_api() for consistency
            from datetime import datetime
            current_year = datetime.now().year
            current_month = datetime.now().month
            season_year = current_year if current_month >= 9 else current_year - 1
            
            endpoint = f"{self.base_url}/getNFLTeams"
            params = {
                "sortBy": "standings",
                "rosters": "false",
                "schedules": "false",
                "topPerformers": "true",
                "teamStats": "true",
                "teamStatsSeason": str(season_year)
            }
            
            records = {}
            
            try:
                logger.debug(f"Trying to fetch NFL records from: {endpoint} with params: {params}")
                self._check_rate_limit()
                response = requests.get(endpoint, headers=self.headers, params=params, timeout=self.api_timeout)
                    
                if response.status_code == 200:
                    data = response.json()
                    
                    # Try to parse different response formats
                    teams_data = []
                    if isinstance(data, list):
                        teams_data = data
                    elif isinstance(data, dict):
                        if 'body' in data:
                            if isinstance(data['body'], list):
                                teams_data = data['body']
                            elif 'teams' in data['body']:
                                teams_data = data['body']['teams']
                        elif 'teams' in data:
                            teams_data = data['teams']
                    
                    for team in teams_data:
                        team_id = str(team.get('teamID', team.get('id', '')))
                        if team_id:
                            # Extract wins, losses, ties (API uses 'wins', 'loss', 'tie')
                            wins = int(team.get('wins', 0) or 0)
                            losses = int(team.get('loss', team.get('losses', 0)) or 0)
                            ties = int(team.get('tie', team.get('ties', 0)) or 0)
                            
                            records[team_id] = {'wins': wins, 'losses': losses, 'ties': ties}
                    
                    if records:
                        # Also update the database with these records for future use
                        try:
                            from database import get_db_session
                            from models import Team
                            with get_db_session() as db:
                                for team_id, record_data in records.items():
                                    team = db.query(Team).filter(
                                        Team.league == 'NFL',
                                        Team.api_team_id == team_id
                                    ).first()
                                    if team:
                                        team.wins = record_data['wins']
                                        team.losses = record_data['losses']
                                        team.ties = record_data['ties']
                                db.commit()
                        except Exception as db_error:
                            logger.debug(f"Could not update database with fetched records: {db_error}")
                        
                        self._team_records_cache = records
                        self._standings_cache_time = time.time()
                        logger.info(f"Fetched records for {len(records)} NFL teams from API")
                        return records
                else:
                    logger.debug(f"API returned status {response.status_code} for {endpoint}")
                            
            except Exception as e:
                logger.debug(f"Could not fetch from {endpoint}: {e}")
            
            # If no records found, return empty dict (will default to 0-0-0)
            logger.warning("Could not fetch NFL team records from API. Records will default to 0-0-0")
            return {}
            
        except Exception as e:
            # Non-critical failure - team records are optional
            logger.debug(f"Could not fetch team records from NFL API (non-critical): {e}")
            return {}
