"""
NHL data collector for the sports data service.
"""

import requests
import time
from datetime import datetime, date
from typing import Dict, List, Optional, Any
import logging

from .base import BaseCollector

logger = logging.getLogger(__name__)


class NHLCollector(BaseCollector):
    """NHL data collector using the NHL Web API."""
    
    def __init__(self):
        super().__init__("NHL")
        self.base_url = "https://api-web.nhle.com"
    
    def get_schedule(self, date: Optional[date] = None) -> List[Dict[str, Any]]:
        """
        Get NHL schedule for specified date or current games.
        
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
            
            url = f"{self.base_url}/v1/schedule/{date_str}"
            
            start_time = time.time()
            response = requests.get(url, timeout=self.api_timeout)
            response_time = int((time.time() - start_time) * 1000)
            
            if response.status_code == 200:
                data = response.json()
                games = []
                seen_game_ids = set()  # Track game IDs to prevent duplicates
                
                if 'gameWeek' in data and len(data['gameWeek']) > 0:
                    for day in data['gameWeek']:
                        if 'games' in day:
                            for game in day['games']:
                                game_id = str(game.get('id', ''))
                                # Only process games for the requested date and avoid duplicates
                                game_date_str = game.get('startTimeUTC', '')
                                if game_date_str:
                                    try:
                                        game_date_obj = datetime.fromisoformat(game_date_str.replace('Z', '+00:00'))
                                        game_date_formatted = game_date_obj.strftime('%Y-%m-%d')
                                        # Strictly filter by date - only include games matching the requested date
                                        if game_date_formatted == date_str and game_id not in seen_game_ids:
                                            seen_game_ids.add(game_id)
                                            parsed_game = self.parse_game_data(game)
                                            if parsed_game:
                                                games.append(parsed_game)
                                        else:
                                            # Log if we're skipping a game due to date mismatch
                                            if game_date_formatted != date_str:
                                                logger.debug(f"Skipping game {game_id} - date {game_date_formatted} doesn't match requested {date_str}")
                                    except ValueError as e:
                                        # If date parsing fails, skip the game (don't include games without valid dates)
                                        logger.debug(f"Skipping game {game_id} - date parsing failed: {e}")
                                        continue
                                else:
                                    # No date - skip the game (we need a valid date to match)
                                    logger.debug(f"Skipping game {game_id} - no startTimeUTC")
                                    continue
                
                return games
            else:
                logger.error(f"NHL API error: {response.status_code}")
                return []
                
        except Exception as e:
            logger.error(f"Error fetching NHL schedule: {e}")
            return []
    
    def get_season_schedule(self, season: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get full NHL season schedule.
        
        Args:
            season: Season year (e.g., "2024"). If None, uses current year.
            
        Returns:
            List of game dictionaries for the entire season
            
        Note: NHL API doesn't have a direct season endpoint, so we fetch
        day by day for the season (Oct-Apr). This is a fallback implementation.
        """
        logger.warning("NHL full season fetch not implemented - would need to fetch day-by-day")
        # For now, return empty - could implement day-by-day fetching if needed
        return []
    
    def get_live_scores(self, date: Optional[date] = None) -> List[Dict[str, Any]]:
        """
        Get live NHL scores for specified date.
        
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
            
            url = f"{self.base_url}/v1/schedule/{date_str}"
            
            start_time = time.time()
            response = requests.get(url, timeout=self.api_timeout)
            response_time = int((time.time() - start_time) * 1000)
            
            if response.status_code == 200:
                data = response.json()
                games = []
                
                if 'gameWeek' in data and len(data['gameWeek']) > 0:
                    for day in data['gameWeek']:
                        if 'games' in day:
                            for game in day['games']:
                                # Get detailed game data for live scores
                                game_id = game.get('id')
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
                logger.error(f"NHL API error: {response.status_code}")
                return []
                
        except Exception as e:
            logger.error(f"Error fetching NHL live scores: {e}")
            return []
    
    def _get_game_details(self, game_id: str) -> Dict[str, Any]:
        """Get detailed game data from NHL API."""
        url = f"{self.base_url}/v1/gamecenter/{game_id}/boxscore"
        response = requests.get(url, timeout=self.api_timeout)
        
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to get game details: {response.status_code}")
    
    def parse_game_data(self, raw_game: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse raw NHL game data into standardized format.
        
        Args:
            raw_game: Raw game data from NHL API
            
        Returns:
            Standardized game dictionary
        """
        try:
            # Extract team information
            home_team = raw_game.get('homeTeam', {})
            away_team = raw_game.get('awayTeam', {})
            
            if not home_team or not away_team:
                logger.warning(f"No team data found for game {raw_game.get('id', 'unknown')}")
                return None
            
            # Parse game date
            game_datetime = raw_game.get('startTimeUTC', '')
            try:
                if game_datetime:
                    game_date_obj = datetime.fromisoformat(game_datetime.replace('Z', '+00:00'))
                    game_date = game_date_obj.strftime('%Y-%m-%d')
                    game_time = game_date_obj
                else:
                    game_date = datetime.now().strftime('%Y-%m-%d')
                    game_time = None
            except ValueError:
                logger.warning(f"Invalid datetime format: {game_datetime}")
                game_date = datetime.now().strftime('%Y-%m-%d')
                game_time = None
            
            # Detect game type
            game_type = self._detect_nhl_game_type(raw_game)
            
            # Parse period scores
            home_period_scores = self._parse_period_scores(home_team.get('periods', []))
            visitor_period_scores = self._parse_period_scores(away_team.get('periods', []))
            
            # Extract team names more robustly
            home_place_name = home_team.get('placeName', {})
            if isinstance(home_place_name, dict):
                home_place_name = home_place_name.get('default', '')
            else:
                home_place_name = str(home_place_name) if home_place_name else ''
            
            home_common_name = home_team.get('commonName', {})
            if isinstance(home_common_name, dict):
                home_common_name = home_common_name.get('default', '')
            else:
                home_common_name = str(home_common_name) if home_common_name else ''
            
            home_team_name = f"{home_place_name} {home_common_name}".strip()
            if not home_team_name:
                logger.warning(f"Empty home team name for game {raw_game.get('id', 'unknown')}")
            
            away_place_name = away_team.get('placeName', {})
            if isinstance(away_place_name, dict):
                away_place_name = away_place_name.get('default', '')
            else:
                away_place_name = str(away_place_name) if away_place_name else ''
            
            away_common_name = away_team.get('commonName', {})
            if isinstance(away_common_name, dict):
                away_common_name = away_common_name.get('default', '')
            else:
                away_common_name = str(away_common_name) if away_common_name else ''
            
            away_team_name = f"{away_place_name} {away_common_name}".strip()
            if not away_team_name:
                logger.warning(f"Empty away team name for game {raw_game.get('id', 'unknown')}")
            
            return {
                'league': 'NHL',
                'game_id': str(raw_game.get('id', '')),
                'game_date': game_date,
                'game_time': game_time,
                'game_type': game_type,
                'home_team': home_team_name,
                'home_team_abbrev': home_team.get('abbrev', ''),
                'home_team_id': str(home_team.get('id', '')),
                'home_wins': home_team.get('wins', 0),
                'home_losses': home_team.get('losses', 0),
                'home_score_total': home_team.get('score', 0),
                'visitor_team': away_team_name,
                'visitor_team_abbrev': away_team.get('abbrev', ''),
                'visitor_team_id': str(away_team.get('id', '')),
                'visitor_wins': away_team.get('wins', 0),
                'visitor_losses': away_team.get('losses', 0),
                'visitor_score_total': away_team.get('score', 0),
                'game_status': self.normalize_game_status(raw_game.get('gameState', 'scheduled')),
                'current_period': raw_game.get('periodDescriptor', {}).get('number', ''),
                'time_remaining': raw_game.get('clock', {}).get('timeRemaining', ''),
                'is_final': raw_game.get('gameState') == 'FINAL',
                'is_overtime': raw_game.get('periodDescriptor', {}).get('periodType') == 'OVERTIME',
                'home_period_scores': home_period_scores,
                'visitor_period_scores': visitor_period_scores,
            }
            
        except Exception as e:
            logger.error(f"Error parsing NHL game data: {e}")
            return None
    
    def parse_live_game_data(self, raw_game: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse live game data from detailed NHL API.
        
        Args:
            raw_game: Raw detailed game data from NHL API
            
        Returns:
            Standardized game dictionary
        """
        try:
            # The detailed game API uses homeTeam/awayTeam at root level, similar to schedule API
            home_team = raw_game.get('homeTeam', {})
            away_team = raw_game.get('awayTeam', {})
            
            if not home_team or not away_team:
                logger.warning(f"No team data found in detailed game {raw_game.get('id', 'unknown')}")
                return None
            
            # Extract team names using same logic as parse_game_data
            home_place_name = home_team.get('placeName', {})
            if isinstance(home_place_name, dict):
                home_place_name = home_place_name.get('default', '')
            else:
                home_place_name = str(home_place_name) if home_place_name else ''
            
            home_common_name = home_team.get('commonName', {})
            if isinstance(home_common_name, dict):
                home_common_name = home_common_name.get('default', '')
            else:
                home_common_name = str(home_common_name) if home_common_name else ''
            
            home_team_name = f"{home_place_name} {home_common_name}".strip()
            
            away_place_name = away_team.get('placeName', {})
            if isinstance(away_place_name, dict):
                away_place_name = away_place_name.get('default', '')
            else:
                away_place_name = str(away_place_name) if away_place_name else ''
            
            away_common_name = away_team.get('commonName', {})
            if isinstance(away_common_name, dict):
                away_common_name = away_common_name.get('default', '')
            else:
                away_common_name = str(away_common_name) if away_common_name else ''
            
            away_team_name = f"{away_place_name} {away_common_name}".strip()
            
            # Parse game date
            game_date_str = raw_game.get('gameDate', '')
            if not game_date_str:
                game_date_str = datetime.now().strftime('%Y-%m-%d')
            
            # Parse game time
            game_time = None
            game_datetime = raw_game.get('startTimeUTC', '')
            if game_datetime:
                try:
                    game_time = datetime.fromisoformat(game_datetime.replace('Z', '+00:00'))
                except ValueError:
                    pass
            
            # Detect game type
            game_type = self._detect_nhl_game_type(raw_game)
            
            # Get scores from boxscore if available, otherwise 0
            home_score = raw_game.get('homeTeam', {}).get('score', 0)
            away_score = raw_game.get('awayTeam', {}).get('score', 0)
            
            return {
                'league': 'NHL',
                'game_id': str(raw_game.get('id', '')),
                'game_date': game_date_str,
                'game_time': game_time,
                'game_type': game_type,
                'home_team': home_team_name,
                'home_team_abbrev': home_team.get('abbrev', ''),
                'home_team_id': str(home_team.get('id', '')),
                'home_score_total': home_score,
                'visitor_team': away_team_name,
                'visitor_team_abbrev': away_team.get('abbrev', ''),
                'visitor_team_id': str(away_team.get('id', '')),
                'visitor_score_total': away_score,
                'game_status': self.normalize_game_status(raw_game.get('gameState', 'scheduled')),
                'current_period': raw_game.get('clock', {}).get('timeRemaining', ''),
                'time_remaining': raw_game.get('clock', {}).get('timeRemaining', ''),
                'is_final': raw_game.get('gameState') == 'FINAL',
                'is_overtime': False,  # Would need to check period descriptor if available
                'home_wins': 0,  # Detailed API doesn't include wins/losses
                'home_losses': 0,
                'visitor_wins': 0,
                'visitor_losses': 0,
                'home_period_scores': {},
                'visitor_period_scores': {},
            }
            
        except Exception as e:
            logger.error(f"Error parsing NHL live game data: {e}")
            return None
    
    def _detect_nhl_game_type(self, game_data: Dict[str, Any]) -> str:
        """
        Detect NHL game type.
        
        Args:
            game_data: Raw game data from NHL API
            
        Returns:
            Normalized game type
        """
        game_type = game_data.get('gameType', 2)
        
        type_map = {
            1: 'preseason',
            2: 'regular',
            3: 'playoffs'
        }
        
        return type_map.get(game_type, 'regular')
    
    def _parse_period_scores(self, periods: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Parse period scores from NHL periods data.
        
        Args:
            periods: List of period data from NHL API
            
        Returns:
            Dictionary of period scores
        """
        scores = {}
        
        for i, period in enumerate(periods):
            period_num = i + 1
            score = period.get('score', 0)
            scores[f'period_{period_num}'] = score
        
        return scores
