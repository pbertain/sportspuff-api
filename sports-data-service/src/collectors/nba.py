"""
NBA data collector for the sports data service.
"""

import sys
import os
import signal
import time
import json
from datetime import datetime, date
from typing import Dict, List, Optional, Any
import logging

# Add NBA API to path
sys.path.insert(0, '/app/dependencies/nba_api/src')

from nba_api.live.nba.endpoints import scoreboard
from nba_api.stats.endpoints import scoreboardv2
try:
    from nba_api.stats.endpoints import scheduleleaguev2
except ImportError:
    # scheduleleaguev2 may not exist in all versions
    scheduleleaguev2 = None
from sqlalchemy.orm import Session

from .base import BaseCollector
from models import Game

logger = logging.getLogger(__name__)


class NBACollector(BaseCollector):
    """NBA data collector using the NBA API."""
    
    def __init__(self):
        super().__init__("NBA")
        self.timeout_handler = None
    
    def _timeout_handler(self, signum, frame):
        """Handle timeout for NBA API calls."""
        raise TimeoutError("NBA API call timed out")
    
    def _call_with_timeout(self, func, timeout_seconds: int = None):
        """Call a function with a timeout."""
        if timeout_seconds is None:
            timeout_seconds = self.api_timeout
            
        signal.signal(signal.SIGALRM, self._timeout_handler)
        signal.alarm(timeout_seconds)
        try:
            result = func()
            return result
        finally:
            signal.alarm(0)
    
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
            
            # Get schedule for the determined season with timeout
            def get_schedule_data():
                if scheduleleaguev2 is None:
                    # Fallback: use scoreboard endpoint for today's games
                    try:
                        if date is None:
                            date_str = datetime.now().strftime('%Y-%m-%d')
                        else:
                            date_str = date.strftime('%Y-%m-%d')
                        scoreboard_data = scoreboardv2.ScoreboardV2(game_date=date_str)
                        scoreboard_dict = scoreboard_data.get_dict()
                        # Extract games from scoreboard format
                        games = []
                        if 'resultSets' in scoreboard_dict and len(scoreboard_dict['resultSets']) > 0:
                            # ScoreboardV2 returns games in resultSets[0]
                            game_rows = scoreboard_dict['resultSets'][0].get('rowSet', [])
                            for row in game_rows:
                                # Format: [GAME_ID, GAME_DATE_EST, GAME_SEQUENCE, GAME_STATUS_ID, ...]
                                games.append({'gameId': row[0] if len(row) > 0 else None})
                        # Wrap in leagueSchedule format for compatibility
                        return {'leagueSchedule': {'gameDates': [{'gameDate': date_str, 'games': games}]}}
                    except Exception as e:
                        logger.error(f"Error getting schedule via scoreboard: {e}")
                        return {}
                schedule_data = scheduleleaguev2.ScheduleLeagueV2(season=season)
                return schedule_data.get_dict()
            
            start_time = time.time()
            data = self._call_with_timeout(get_schedule_data, timeout_seconds=15)
            response_time = int((time.time() - start_time) * 1000)
            
            if 'leagueSchedule' in data and 'gameDates' in data['leagueSchedule']:
                game_dates = data['leagueSchedule']['gameDates']
                
                if date is None:
                    # Get today's games
                    target_date = datetime.now().strftime('%m/%d/%Y')
                else:
                    # Convert date to MM/DD/YYYY format
                    target_date = date.strftime('%m/%d/%Y')
                
                # Find games for the specified date
                target_games = []
                for game_date in game_dates:
                    game_date_str = game_date.get('gameDate', '')
                    if game_date_str.startswith(target_date):
                        games_for_date = game_date.get('games', [])
                        for game in games_for_date:
                            parsed_game = self.parse_game_data(game, game_date_str)
                            if parsed_game:
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
    
    def get_live_scores(self, date: Optional[date] = None) -> List[Dict[str, Any]]:
        """
        Get live NBA scores for specified date.
        
        Args:
            date: Date to get scores for (optional, defaults to today)
            
        Returns:
            List of game dictionaries with live score data
        """
        self._check_rate_limit()
        
        try:
            if date is None:
                date_str = datetime.now().strftime('%Y-%m-%d')
            else:
                date_str = date.strftime('%Y-%m-%d')
            
            # Get live scoreboard
            def get_scoreboard_data():
                scoreboard_data = scoreboardv2.ScoreboardV2(game_date=date_str)
                return scoreboard_data.get_dict()
            
            start_time = time.time()
            data = self._call_with_timeout(get_scoreboard_data, timeout_seconds=10)
            response_time = int((time.time() - start_time) * 1000)
            
            games = []
            if 'resultSets' in data and len(data['resultSets']) > 0:
                game_header = data['resultSets'][0]
                if 'rowSet' in game_header:
                    for game_data in game_header['rowSet']:
                        parsed_game = self.parse_live_game_data(game_data)
                        if parsed_game:
                            games.append(parsed_game)
            
            return games
            
        except TimeoutError as e:
            logger.error(f"NBA API timeout: {e}")
            return []
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
                # Format: "10/02/2025 00:00:00"
                game_date_str = game_date_str.split(' ')[0]
            
            try:
                game_date_obj = datetime.strptime(game_date_str, '%m/%d/%Y')
                game_date = game_date_obj.strftime('%Y-%m-%d')
            except ValueError:
                logger.warning(f"Invalid date format: {game_date_str}")
                return None
            
            # Detect season type using NBA API fields
            game_type = self._detect_nba_season_type(raw_game)
            
            # Parse period scores
            home_period_scores = self._parse_period_scores(raw_game.get('homeTeam', {}))
            visitor_period_scores = self._parse_period_scores(raw_game.get('awayTeam', {}))
            
            # Extract game time
            game_time = None
            if raw_game.get('gameTimeEst'):
                try:
                    # Parse EST time and convert to UTC
                    game_time_str = raw_game['gameTimeEst']
                    # This is a simplified parsing - you might need more robust timezone handling
                    game_time = datetime.strptime(f"{game_date} {game_time_str}", '%Y-%m-%d %H:%M:%S')
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
                'is_final': raw_game.get('gameStatus', '') == 'Final',
                'is_overtime': raw_game.get('isOvertime', False),
                'home_period_scores': home_period_scores,
                'visitor_period_scores': visitor_period_scores,
            }
            
        except Exception as e:
            logger.error(f"Error parsing NBA game data: {e}")
            return None
    
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
                'game_type': 'regular',
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
        """
        Detect NBA season type using API fields.
        
        Args:
            game_data: Raw game data from NBA API
            
        Returns:
            Normalized game type
        """
        game_label = game_data.get('gameLabel', '')
        game_subtype = game_data.get('gameSubtype', '')
        
        if game_label == 'Preseason':
            return 'preseason'
        elif game_label == 'Emirates NBA Cup':
            return 'nba_cup'
        else:
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
