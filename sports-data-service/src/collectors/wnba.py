"""
WNBA data collector for the sports data service.
"""

import sys
import requests
import time
from datetime import datetime, date
from typing import Dict, List, Optional, Any
import logging

# Add NBA API to path (WNBA is part of nba_api library)
sys.path.insert(0, '/app/dependencies/nba_api/src')

# Setup proxy before importing nba_api
from utils.proxy import setup_proxy, get_proxy_config
setup_proxy()

from nba_api.stats.endpoints import leaguegamefinder

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
        # Get proxy configuration (for LeagueGameFinder calls)
        self.proxy_config = get_proxy_config()
        if self.proxy_config:
            logger.info("Using proxy for WNBA API requests (LeagueGameFinder)")
    
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
    
    def get_season_schedule(self, season: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get full WNBA season schedule using LeagueGameFinder.
        
        Args:
            season: Season year (e.g., "2025"). If None, uses current year.
            
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
                
                # WNBA season typically runs May-September
                # If we're past September, use current year
                # If we're before May, use previous year
                if month >= 10 or month < 5:
                    season = str(year)
                else:
                    season = str(year)
            
            logger.info(f"Fetching full WNBA season schedule for {season}")
            
            # LeagueGameFinder with WNBA league ID (10)
            # For WNBA, season format is just the year (e.g., "2025")
            # But LeagueGameFinder expects "YYYY-YY" format like NBA
            # Actually, let's check - WNBA might use different format
            # Try with just year first, then try YYYY-YY format
            season_formats = [season, f"{season}-{str(int(season) + 1)[-2:]}"]
            
            nba_headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Referer': 'https://www.nba.com/',
                'Accept': 'application/json, text/plain, */*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Origin': 'https://www.nba.com'
            }
            
            all_games = []
            
            for season_format in season_formats:
                try:
                    logger.info(f"Trying WNBA season format: {season_format}")
                    game_finder = leaguegamefinder.LeagueGameFinder(
                        season_nullable=season_format,
                        league_id_nullable='10',  # WNBA league ID
                        headers=nba_headers,
                        timeout=60
                    )
                    data = game_finder.get_dict()
                    
                    # LeagueGameFinder returns data in resultSets format
                    # Note: Each game has TWO rows (one per team), so we need to deduplicate by game_id
                    if 'resultSets' in data and len(data['resultSets']) > 0:
                        game_results = data['resultSets'][0]
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
                                
                                # Parse matchup (e.g., "NY @ CHI" or "NY vs. CHI")
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
                                
                                # Parse date - LeagueGameFinder returns YYYY-MM-DD format
                                try:
                                    game_date_obj = datetime.strptime(game_date_str, '%Y-%m-%d')
                                    game_date_formatted = game_date_obj.strftime('%Y-%m-%d')  # WNBA parse_game_data expects YYYY-MM-DD
                                except:
                                    continue
                                
                                # Build game object compatible with parse_game_data
                                game_obj = {
                                    'gameID': game_id,
                                    'gameDate': game_date_formatted,
                                    'homeTeam': {
                                        'teamID': home_team_id,
                                        'teamAbbr': home_abbrev,
                                        'teamName': home_team_name
                                    },
                                    'awayTeam': {
                                        'teamID': away_team_id,
                                        'teamAbbr': visitor_abbrev,
                                        'teamName': away_team_name
                                    },
                                    'gameStatus': 'scheduled',
                                    '_leagueGameFinder': True
                                }
                                
                                parsed_game = self.parse_game_data(game_obj)
                                if parsed_game:
                                    games_by_id[game_id] = parsed_game
                        
                        all_games = list(games_by_id.values())
                        if len(all_games) > 0:
                            logger.info(f"Successfully fetched {len(all_games)} unique WNBA games for season {season}")
                            return all_games
                    
                except Exception as e:
                    logger.warning(f"Failed to fetch WNBA season {season_format}: {e}")
                    continue
            
            if len(all_games) == 0:
                logger.warning(f"No WNBA games found for season {season}")
            
            return all_games
                
        except Exception as e:
            logger.error(f"Error fetching WNBA season schedule: {e}")
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
