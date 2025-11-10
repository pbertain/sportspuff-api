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
    
    def __init__(self, api_key: str = None):
        super().__init__("NFL")
        self.api_key = api_key or "913a411db3msh6a5a6bcd37bb86dp1cb551jsnd7718d4e5a0e"
        self.base_url = "https://tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com"
        self.headers = {
            "x-rapidapi-key": self.api_key,
            "x-rapidapi-host": "tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com"
        }
    
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
                    # The getNFLGamesForDate endpoint returns all games for the date
                    # with sufficient data for live scores - no need for individual game calls
                    # This allows us to get all scores in a single API call per minute
                    for game in data:
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
            # Extract team information
            home_team = raw_game.get('homeTeam', {})
            away_team = raw_game.get('awayTeam', {})
            
            if not home_team or not away_team:
                logger.warning(f"No team data found for game {raw_game.get('gameID', 'unknown')}")
                return None
            
            # Parse game date
            game_date_str = raw_game.get('gameDate', '')
            try:
                game_date_obj = datetime.strptime(game_date_str, '%Y-%m-%d')
                game_date = game_date_obj.strftime('%Y-%m-%d')
            except ValueError:
                logger.warning(f"Invalid date format: {game_date_str}")
                game_date = datetime.now().strftime('%Y-%m-%d')
            
            # Parse game time
            game_time = None
            if raw_game.get('gameTime'):
                try:
                    game_time_str = raw_game['gameTime']
                    game_time = datetime.strptime(f"{game_date} {game_time_str}", '%Y-%m-%d %H:%M:%S')
                except:
                    pass
            
            # Detect game type
            game_type = self._detect_nfl_game_type(raw_game)
            
            # Parse quarter scores
            home_period_scores = self._parse_quarter_scores(home_team.get('quarters', []))
            visitor_period_scores = self._parse_quarter_scores(away_team.get('quarters', []))
            
            return {
                'league': 'NFL',
                'game_id': str(raw_game.get('gameID', '')),
                'game_date': game_date,
                'game_time': game_time,
                'game_type': game_type,
                'home_team': home_team.get('teamName', ''),
                'home_team_abbrev': home_team.get('teamAbbr', ''),
                'home_team_id': str(home_team.get('teamID', '')),
                'home_wins': home_team.get('wins', 0),
                'home_losses': home_team.get('losses', 0),
                'home_score_total': home_team.get('score', 0),
                'visitor_team': away_team.get('teamName', ''),
                'visitor_team_abbrev': away_team.get('teamAbbr', ''),
                'visitor_team_id': str(away_team.get('teamID', '')),
                'visitor_wins': away_team.get('wins', 0),
                'visitor_losses': away_team.get('losses', 0),
                'visitor_score_total': away_team.get('score', 0),
                'game_status': self.normalize_game_status(raw_game.get('gameStatus', 'scheduled')),
                'current_period': raw_game.get('quarter', ''),
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
                'home_team_abbrev': home_team.get('teamAbbr', ''),
                'home_team_id': str(home_team.get('teamID', '')),
                'home_score_total': home_team.get('score', 0),
                'visitor_team': away_team.get('teamName', ''),
                'visitor_team_abbrev': away_team.get('teamAbbr', ''),
                'visitor_team_id': str(away_team.get('teamID', '')),
                'visitor_score_total': away_team.get('score', 0),
                'game_status': self.normalize_game_status(raw_game.get('gameStatus', 'scheduled')),
                'current_period': raw_game.get('quarter', ''),
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
