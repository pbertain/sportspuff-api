"""
MLB data collector for the sports data service.
"""

import sys
import os
import time
from datetime import datetime, date
from typing import Dict, List, Optional, Any
import logging

# Add MLB Stats API to path
sys.path.append('/app/dependencies/mlb-statsapi')
import statsapi

from .base import BaseCollector

logger = logging.getLogger(__name__)


class MLBCollector(BaseCollector):
    """MLB data collector using the MLB Stats API."""
    
    def __init__(self):
        super().__init__("MLB")
    
    def get_schedule(self, date: Optional[date] = None) -> List[Dict[str, Any]]:
        """
        Get MLB schedule for specified date or current games.
        
        Args:
            date: Date to get schedule for (optional, defaults to today)
            
        Returns:
            List of game dictionaries
        """
        self._check_rate_limit()
        
        try:
            if date:
                date_str = date.strftime('%Y-%m-%d')
                games = statsapi.schedule(date=date_str)
            else:
                games = statsapi.schedule()
            
            parsed_games = []
            for game in games:
                parsed_game = self.parse_game_data(game)
                if parsed_game:
                    parsed_games.append(parsed_game)
            
            return parsed_games
            
        except Exception as e:
            logger.error(f"Error fetching MLB schedule: {e}")
            return []
    
    def get_season_schedule(self, season: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get full MLB season schedule.
        
        Args:
            season: Season year (e.g., "2024"). If None, uses current year.
            
        Returns:
            List of game dictionaries for the entire season
        """
        self._check_rate_limit()
        
        try:
            # MLB statsapi.schedule() without date returns all games
            logger.info("Fetching full MLB season schedule")
            games = statsapi.schedule()
            
            parsed_games = []
            for game in games:
                parsed_game = self.parse_game_data(game)
                if parsed_game:
                    parsed_games.append(parsed_game)
            
            logger.info(f"Fetched {len(parsed_games)} games for MLB season")
            return parsed_games
            
        except Exception as e:
            logger.error(f"Error fetching MLB season schedule: {e}")
            return []
    
    def get_live_scores(self, date: Optional[date] = None) -> List[Dict[str, Any]]:
        """
        Get live MLB scores for specified date.
        
        Args:
            date: Date to get scores for (optional, defaults to today)
            
        Returns:
            List of game dictionaries with live score data
        """
        self._check_rate_limit()
        
        try:
            if date:
                date_str = date.strftime('%Y-%m-%d')
                games = statsapi.schedule(date=date_str)
            else:
                games = statsapi.schedule()
            
            parsed_games = []
            for game in games:
                # Get detailed game data for live scores
                game_id = game.get('game_id')
                if game_id:
                    try:
                        detailed_game = statsapi.get('game', {'gamePk': game_id})
                        parsed_game = self.parse_live_game_data(detailed_game)
                        if parsed_game:
                            parsed_games.append(parsed_game)
                    except Exception as e:
                        logger.warning(f"Could not get detailed data for game {game_id}: {e}")
                        # Fall back to basic game data
                        parsed_game = self.parse_game_data(game)
                        if parsed_game:
                            parsed_games.append(parsed_game)
                else:
                    parsed_game = self.parse_game_data(game)
                    if parsed_game:
                        parsed_games.append(parsed_game)
            
            return parsed_games
            
        except Exception as e:
            logger.error(f"Error fetching MLB live scores: {e}")
            return []
    
    def parse_game_data(self, raw_game: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse raw MLB game data into standardized format.
        
        Args:
            raw_game: Raw game data from MLB Stats API
            
        Returns:
            Standardized game dictionary
        """
        try:
            # Extract team information
            home_team = raw_game.get('home_name', '')
            away_team = raw_game.get('away_name', '')
            
            if not home_team or not away_team:
                logger.warning(f"No team data found for game {raw_game.get('game_id', 'unknown')}")
                return None
            
            # Parse game date
            game_date_str = raw_game.get('game_date', '')
            try:
                game_date_obj = datetime.strptime(game_date_str, '%Y-%m-%d')
                game_date = game_date_obj.strftime('%Y-%m-%d')
            except ValueError:
                logger.warning(f"Invalid date format: {game_date_str}")
                return None
            
            # Parse game time
            game_time = None
            if raw_game.get('game_time'):
                try:
                    game_time_str = raw_game['game_time']
                    game_time = datetime.strptime(f"{game_date} {game_time_str}", '%Y-%m-%d %H:%M:%S')
                except:
                    pass
            
            # Detect game type
            game_type = self._detect_mlb_game_type(raw_game)
            
            # Parse inning scores
            home_period_scores = self._parse_inning_scores(raw_game.get('home_inning_scores', []))
            visitor_period_scores = self._parse_inning_scores(raw_game.get('away_inning_scores', []))
            
            return {
                'league': 'MLB',
                'game_id': str(raw_game.get('game_id', '')),
                'game_date': game_date,
                'game_time': game_time,
                'game_type': game_type,
                'home_team': home_team,
                'home_team_abbrev': raw_game.get('home_abbrev', ''),
                'home_team_id': str(raw_game.get('home_id', '')),
                'home_wins': raw_game.get('home_wins', 0),
                'home_losses': raw_game.get('home_losses', 0),
                'home_score_total': raw_game.get('home_score', 0),
                'visitor_team': away_team,
                'visitor_team_abbrev': raw_game.get('away_abbrev', ''),
                'visitor_team_id': str(raw_game.get('away_id', '')),
                'visitor_wins': raw_game.get('away_wins', 0),
                'visitor_losses': raw_game.get('away_losses', 0),
                'visitor_score_total': raw_game.get('away_score', 0),
                'game_status': self.normalize_game_status(raw_game.get('status', 'scheduled')),
                'current_period': raw_game.get('inning', ''),
                'time_remaining': raw_game.get('inning_state', ''),
                'is_final': raw_game.get('status') == 'Final',
                'is_overtime': False,  # MLB doesn't have overtime
                'home_period_scores': home_period_scores,
                'visitor_period_scores': visitor_period_scores,
                # MLB specific fields
                'home_hits': raw_game.get('home_hits', 0),
                'home_runs': raw_game.get('home_runs', 0),
                'home_errors': raw_game.get('home_errors', 0),
                'visitor_hits': raw_game.get('away_hits', 0),
                'visitor_runs': raw_game.get('away_runs', 0),
                'visitor_errors': raw_game.get('away_errors', 0),
            }
            
        except Exception as e:
            logger.error(f"Error parsing MLB game data: {e}")
            return None
    
    def parse_live_game_data(self, raw_game: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse live game data from detailed MLB API.
        
        Args:
            raw_game: Raw detailed game data from MLB API
            
        Returns:
            Standardized game dictionary
        """
        try:
            # This is a simplified parser for detailed game data
            # You'll need to adjust based on the actual API response structure
            game_data = raw_game.get('gameData', {})
            live_data = raw_game.get('liveData', {})
            
            teams = game_data.get('teams', {})
            home_team = teams.get('home', {})
            away_team = teams.get('away', {})
            
            # Extract inning scores
            linescore = live_data.get('linescore', {})
            innings = linescore.get('innings', [])
            
            home_period_scores = {}
            visitor_period_scores = {}
            
            for i, inning in enumerate(innings):
                inning_num = i + 1
                home_score = inning.get('home', {}).get('runs', 0)
                away_score = inning.get('away', {}).get('runs', 0)
                
                home_period_scores[f'inning_{inning_num}'] = home_score
                visitor_period_scores[f'inning_{inning_num}'] = away_score
            
            return {
                'league': 'MLB',
                'game_id': str(raw_game.get('gamePk', '')),
                'game_date': datetime.now().strftime('%Y-%m-%d'),
                'game_type': 'regular',
                'home_team': home_team.get('name', ''),
                'home_team_abbrev': home_team.get('abbreviation', ''),
                'home_team_id': str(home_team.get('id', '')),
                'home_score_total': linescore.get('teams', {}).get('home', {}).get('runs', 0),
                'visitor_team': away_team.get('name', ''),
                'visitor_team_abbrev': away_team.get('abbreviation', ''),
                'visitor_team_id': str(away_team.get('id', '')),
                'visitor_score_total': linescore.get('teams', {}).get('away', {}).get('runs', 0),
                'game_status': self.normalize_game_status(raw_game.get('status', {}).get('detailedState', 'scheduled')),
                'current_period': linescore.get('currentInning', ''),
                'time_remaining': linescore.get('inningState', ''),
                'is_final': raw_game.get('status', {}).get('detailedState') == 'Final',
                'is_overtime': False,
                'home_period_scores': home_period_scores,
                'visitor_period_scores': visitor_period_scores,
            }
            
        except Exception as e:
            logger.error(f"Error parsing MLB live game data: {e}")
            return None
    
    def _detect_mlb_game_type(self, game_data: Dict[str, Any]) -> str:
        """
        Detect MLB game type.
        
        Args:
            game_data: Raw game data from MLB API
            
        Returns:
            Normalized game type
        """
        game_type = game_data.get('game_type', 'R')
        
        type_map = {
            'R': 'regular',
            'P': 'playoffs',
            'A': 'allstar',
            'S': 'preseason'
        }
        
        return type_map.get(game_type, 'regular')
    
    def _parse_inning_scores(self, inning_scores: List[int]) -> Dict[str, int]:
        """
        Parse inning scores into standardized format.
        
        Args:
            inning_scores: List of inning scores
            
        Returns:
            Dictionary of inning scores
        """
        scores = {}
        for i, score in enumerate(inning_scores):
            scores[f'inning_{i+1}'] = score
        return scores
