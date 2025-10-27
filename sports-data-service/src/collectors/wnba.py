"""
WNBA data collector for the sports data service.
"""

import requests
import time
from datetime import datetime, date
from typing import Dict, List, Optional, Any
import logging

from .base import BaseCollector

logger = logging.getLogger(__name__)


class WNBACollector(BaseCollector):
    """WNBA data collector using the Tank01 WNBA API."""
    
    def __init__(self, api_key: str = None):
        super().__init__("WNBA")
        self.api_key = api_key or "4a9b7c50edmsh3b89fc2aa5cd47dp1bd2ccjsn6bd287bec4d9"
        self.base_url = "https://tank01-wnba-live-in-game-real-time-statistics-wnba.p.rapidapi.com"
        self.headers = {
            "x-rapidapi-key": self.api_key,
            "x-rapidapi-host": "tank01-wnba-live-in-game-real-time-statistics-wnba.p.rapidapi.com"
        }
    
    def get_schedule(self, date: Optional[date] = None) -> List[Dict[str, Any]]:
        """
        Get WNBA schedule for specified date or current games.
        
        Args:
            date: Date to get schedule for (optional, defaults to today)
            
        Returns:
            List of game dictionaries
        """
        self._check_rate_limit()
        
        try:
            if date:
                date_str = date.strftime('%Y-%m-%d')
            else:
                date_str = datetime.now().strftime('%Y-%m-%d')
            
            url = f"{self.base_url}/getWNBAGamesForDate/{date_str}"
            
            start_time = time.time()
            response = requests.get(url, headers=self.headers, timeout=self.api_timeout)
            response_time = int((time.time() - start_time) * 1000)
            
            if response.status_code == 200:
                data = response.json()
                games = []
                
                if isinstance(data, list):
                    for game in data:
                        parsed_game = self.parse_game_data(game)
                        if parsed_game:
                            games.append(parsed_game)
                
                return games
            else:
                logger.error(f"WNBA API error: {response.status_code}")
                return []
                
        except Exception as e:
            logger.error(f"Error fetching WNBA schedule: {e}")
            return []
    
    def get_live_scores(self, date: Optional[date] = None) -> List[Dict[str, Any]]:
        """
        Get live WNBA scores for specified date.
        
        Args:
            date: Date to get scores for (optional, defaults to today)
            
        Returns:
            List of game dictionaries with live score data
        """
        self._check_rate_limit()
        
        try:
            if date:
                date_str = date.strftime('%Y-%m-%d')
            else:
                date_str = datetime.now().strftime('%Y-%m-%d')
            
            url = f"{self.base_url}/getWNBAGamesForDate/{date_str}"
            
            start_time = time.time()
            response = requests.get(url, headers=self.headers, timeout=self.api_timeout)
            response_time = int((time.time() - start_time) * 1000)
            
            if response.status_code == 200:
                data = response.json()
                games = []
                
                if isinstance(data, list):
                    for game in data:
                        # Get detailed game data for live scores
                        game_id = game.get('gameID')
                        if game_id:
                            try:
                                detailed_game = self._get_game_details(game_id)
                                parsed_game = self.parse_live_game_data(detailed_game)
                                if parsed_game:
                                    games.append(parsed_game)
                            except Exception as e:
                                logger.warning(f"Could not get detailed data for game {game_id}: {e}")
                                # Fall back to basic game data
                                parsed_game = self.parse_game_data(game)
                                if parsed_game:
                                    games.append(parsed_game)
                        else:
                            parsed_game = self.parse_game_data(game)
                            if parsed_game:
                                games.append(parsed_game)
                
                return games
            else:
                logger.error(f"WNBA API error: {response.status_code}")
                return []
                
        except Exception as e:
            logger.error(f"Error fetching WNBA live scores: {e}")
            return []
    
    def _get_game_details(self, game_id: str) -> Dict[str, Any]:
        """Get detailed game data from WNBA API."""
        url = f"{self.base_url}/getWNBAGameBoxScore/{game_id}"
        response = requests.get(url, headers=self.headers, timeout=self.api_timeout)
        
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to get game details: {response.status_code}")
    
    def parse_game_data(self, raw_game: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse raw WNBA game data into standardized format.
        
        Args:
            raw_game: Raw game data from WNBA API
            
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
            game_type = self._detect_wnba_game_type(raw_game)
            
            # Parse quarter scores
            home_period_scores = self._parse_quarter_scores(home_team.get('quarters', []))
            visitor_period_scores = self._parse_quarter_scores(away_team.get('quarters', []))
            
            return {
                'league': 'WNBA',
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
            logger.error(f"Error parsing WNBA game data: {e}")
            return None
    
    def parse_live_game_data(self, raw_game: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse live game data from detailed WNBA API.
        
        Args:
            raw_game: Raw detailed game data from WNBA API
            
        Returns:
            Standardized game dictionary
        """
        try:
            # This is a simplified parser for detailed game data
            # You'll need to adjust based on the actual API response structure
            home_team = raw_game.get('homeTeam', {})
            away_team = raw_game.get('awayTeam', {})
            
            return {
                'league': 'WNBA',
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
            logger.error(f"Error parsing WNBA live game data: {e}")
            return None
    
    def _detect_wnba_game_type(self, game_data: Dict[str, Any]) -> str:
        """
        Detect WNBA game type.
        
        Args:
            game_data: Raw game data from WNBA API
            
        Returns:
            Normalized game type
        """
        # WNBA game types are typically regular season, playoffs, or preseason
        # You'll need to adjust this based on the actual API response
        return 'regular'
    
    def _parse_quarter_scores(self, quarters: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Parse quarter scores from WNBA quarters data.
        
        Args:
            quarters: List of quarter data from WNBA API
            
        Returns:
            Dictionary of quarter scores
        """
        scores = {}
        
        for i, quarter in enumerate(quarters):
            quarter_num = i + 1
            score = quarter.get('score', 0)
            scores[f'q{quarter_num}'] = score
        
        return scores
