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
        self.base_url = "https://api-web.nhle.com/v1"
        # Stats API endpoint - using main API instead of deprecated statsapi.web.nhl.com
        # Team records are optional - if this fails, we'll just use 0-0-0 records
        self.stats_api_url = "https://api-web.nhle.com/v1/standings/now"
        self._team_records_cache = {}  # Cache team records: {team_id: {'wins': int, 'losses': int, 'ot': int}}
        self._standings_cache_time = None
        self._standings_cache_ttl = 3600  # Cache standings for 1 hour
    
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
            
            url = f"{self.base_url}/schedule/{date_str}"
            
            start_time = time.time()
            response = requests.get(url, timeout=self.api_timeout)
            response_time = int((time.time() - start_time) * 1000)
            
            if response.status_code == 200:
                data = response.json()
                games = []
                seen_game_ids = set()  # Track game IDs to prevent duplicates
                
                if 'gameWeek' in data and len(data['gameWeek']) > 0:
                    for day in data['gameWeek']:
                        # Use the date field from the day object (NHL API groups games by date)
                        day_date = day.get('date', '')
                        # Only process games from days that match the requested date
                        if day_date == date_str and 'games' in day:
                            for game in day['games']:
                                game_id = str(game.get('id', ''))
                                if game_id and game_id not in seen_game_ids:
                                    seen_game_ids.add(game_id)
                                    # For in-progress games, fetch detailed data to get clock info
                                    game_state = game.get('gameState', '').upper()
                                    if game_state in ('LIVE', 'CRITICAL'):
                                        try:
                                            detailed_game = self._get_game_details(game_id)
                                            parsed_game = self.parse_game_data(detailed_game)
                                        except Exception as e:
                                            logger.warning(f"Could not get detailed data for game {game_id}: {e}")
                                            # Fall back to basic game data
                                            parsed_game = self.parse_game_data(game)
                                    else:
                                        parsed_game = self.parse_game_data(game)
                                    if parsed_game:
                                        games.append(parsed_game)
                
                return games
            elif response.status_code == 429:
                logger.warning(f"Rate limited when fetching schedule for {date_str}")
                # Wait and return empty - caller can retry
                time.sleep(2)
                return []
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
            
        Note: According to NHL API reference, we can get team season schedules.
        For full league schedule, we fetch day-by-day for the season (Oct-Apr).
        """
        if season is None:
            season = datetime.now().year
        
        logger.info(f"Fetching full season schedule for {season} - this may take a while")
        all_games = []
        
        # NHL season typically runs from early October to late April
        # Fetch games day by day for the season
        start_date = date(season, 10, 1)  # October 1
        end_date = date(season + 1, 4, 30)  # April 30 of next year
        
        current_date = start_date
        while current_date <= end_date:
            try:
                games = self.get_schedule(current_date)
                all_games.extend(games)
                # Small delay to avoid rate limiting
                time.sleep(0.1)
            except Exception as e:
                logger.warning(f"Error fetching schedule for {current_date}: {e}")
            
            # Move to next day
            from datetime import timedelta
            current_date += timedelta(days=1)
            
            # Stop if we've gone past the current date significantly
            if current_date > datetime.now().date() + timedelta(days=30):
                break
        
        logger.info(f"Fetched {len(all_games)} games for season {season}")
        return all_games
    
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
            
            url = f"{self.base_url}/schedule/{date_str}"
            
            start_time = time.time()
            response = requests.get(url, timeout=self.api_timeout)
            response_time = int((time.time() - start_time) * 1000)
            
            if response.status_code == 200:
                data = response.json()
                games = []
                seen_game_ids = set()  # Track game IDs to prevent duplicates
                
                if 'gameWeek' in data and len(data['gameWeek']) > 0:
                    for day in data['gameWeek']:
                        # Use the date field from the day object (NHL API groups games by date)
                        day_date = day.get('date', '')
                        # Only process games from days that match the requested date
                        if day_date == date_str and 'games' in day:
                            for game in day['games']:
                                game_id = str(game.get('id', ''))
                                if game_id and game_id not in seen_game_ids:
                                    seen_game_ids.add(game_id)
                                    # Only fetch detailed data for games that are in progress or final
                                    # This reduces API calls significantly
                                    game_state = game.get('gameState', '').upper()
                                    if game_state in ('LIVE', 'FINAL', 'CRITICAL', 'OFF'):
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
                                        # For scheduled games, use basic game data (no extra API call needed)
                                        parsed_game = self.parse_game_data(game)
                                        if parsed_game:
                                            games.append(parsed_game)
                
                return games
            elif response.status_code == 429:
                logger.warning(f"Rate limited when fetching live scores for {date_str}")
                # Wait and return empty - caller can retry
                time.sleep(2)
                return []
            else:
                logger.error(f"NHL API error: {response.status_code}")
                return []
                
        except Exception as e:
            logger.error(f"Error fetching NHL live scores: {e}")
            return []
    
    def _get_game_details(self, game_id: str) -> Dict[str, Any]:
        """Get detailed game data from NHL API."""
        # Check rate limit before each detailed game request
        self._check_rate_limit()
        
        url = f"{self.base_url}/gamecenter/{game_id}/boxscore"
        response = requests.get(url, timeout=self.api_timeout)
        
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 429:
            logger.warning(f"Rate limited when fetching game {game_id} details")
            # Wait a bit before retrying
            time.sleep(2)
            raise Exception(f"Rate limited: {response.status_code}")
        else:
            raise Exception(f"Failed to get game details: {response.status_code}")
    
    def _fetch_team_records(self) -> Dict[int, Dict[str, int]]:
        """
        Fetch team records (W-L-OTL) from NHL standings API.
        
        Returns:
            Dictionary mapping team_id to {'wins': int, 'losses': int, 'ot': int}
        """
        # Check cache
        if self._standings_cache_time and time.time() - self._standings_cache_time < self._standings_cache_ttl:
            return self._team_records_cache
        
        try:
            self._check_rate_limit()
            response = requests.get(self.stats_api_url, timeout=self.api_timeout)
            
            if response.status_code == 200:
                data = response.json()
                standings = data.get('standings', [])
                records = {}
                
                # Build a mapping from team abbreviation to team ID first
                # We'll need to fetch team info to match abbreviations to IDs
                abbrev_to_id = {}
                for team_standing in standings:
                    team_abbrev = team_standing.get('teamAbbrev', {})
                    if isinstance(team_abbrev, dict):
                        abbrev = team_abbrev.get('default', '')
                    else:
                        abbrev = str(team_abbrev)
                    
                    if abbrev:
                        # Try to get team ID from a team info endpoint or match by name
                        # For now, we'll create a lookup that matches by abbreviation
                        # and fetch team IDs from the schedule API
                        abbrev_to_id[abbrev] = None  # Will be populated below
                
                # Fetch team IDs by getting recent game schedules (check last 7 days to ensure we get all teams)
                try:
                    from datetime import timedelta
                    for days_ago in range(7):
                        check_date = (datetime.now() - timedelta(days=days_ago)).strftime('%Y-%m-%d')
                        schedule_url = f"{self.base_url}/schedule/{check_date}"
                        schedule_response = requests.get(schedule_url, timeout=self.api_timeout)
                        if schedule_response.status_code == 200:
                            schedule_data = schedule_response.json()
                            game_weeks = schedule_data.get('gameWeek', [])
                            for week in game_weeks:
                                games = week.get('games', [])
                                for game in games:
                                    home_team = game.get('homeTeam', {})
                                    away_team = game.get('awayTeam', {})
                                    
                                    home_abbrev = home_team.get('abbrev', '')
                                    home_id = home_team.get('id')
                                    if home_abbrev and home_id and home_abbrev not in abbrev_to_id:
                                        abbrev_to_id[home_abbrev] = home_id
                                    
                                    away_abbrev = away_team.get('abbrev', '')
                                    away_id = away_team.get('id')
                                    if away_abbrev and away_id and away_abbrev not in abbrev_to_id:
                                        abbrev_to_id[away_abbrev] = away_id
                        
                        # Stop early if we have all 32 teams
                        if len(abbrev_to_id) >= 32:
                            break
                except Exception as e:
                    logger.debug(f"Could not fetch team IDs from schedule: {e}")
                
                # Now build records dictionary using team IDs
                for team_standing in standings:
                    team_abbrev = team_standing.get('teamAbbrev', {})
                    if isinstance(team_abbrev, dict):
                        abbrev = team_abbrev.get('default', '')
                    else:
                        abbrev = str(team_abbrev)
                    
                    team_id = abbrev_to_id.get(abbrev) if abbrev else None
                    
                    if team_id:
                        try:
                            team_id_int = int(team_id)
                            records[team_id_int] = {
                                'wins': team_standing.get('wins', 0),
                                'losses': team_standing.get('losses', 0),
                                'ot': team_standing.get('otLosses', 0)  # Overtime losses
                            }
                        except (ValueError, TypeError):
                            logger.debug(f"Invalid team_id format: {team_id}")
                            continue
                    else:
                        logger.debug(f"Could not find team ID for abbreviation: {abbrev}")
                
                # Cache the results
                if records:
                    self._team_records_cache = records
                    self._standings_cache_time = time.time()
                    logger.info(f"Fetched standings for {len(records)} teams from NHL API")
                    return records
                else:
                    logger.warning("No team records found in standings API response")
                    return {}
            else:
                logger.warning(f"NHL standings API returned status {response.status_code}")
                return {}
                
        except Exception as e:
            # Non-critical failure - team records are optional
            logger.debug(f"Could not fetch team records from NHL standings API (non-critical): {e}")
            return {}
    
    def _get_team_record(self, team_id: str) -> Dict[str, int]:
        """
        Get W-L-OTL record for a team.
        
        Args:
            team_id: Team ID as string
            
        Returns:
            Dictionary with 'wins', 'losses', 'ot' keys
        """
        if not self._team_records_cache:
            self._fetch_team_records()
        
        try:
            team_id_int = int(team_id)
            return self._team_records_cache.get(team_id_int, {'wins': 0, 'losses': 0, 'ot': 0})
        except (ValueError, TypeError):
            return {'wins': 0, 'losses': 0, 'ot': 0}
    
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
            
            # Debug: Log available keys in team objects to check for OTL data
            # Remove this after we identify the OTL field name
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Home team keys: {list(home_team.keys())}")
                logger.debug(f"Away team keys: {list(away_team.keys())}")
                # Check for common OTL field names
                for otl_field in ['otLosses', 'ot_losses', 'overtimeLosses', 'ot', 'otl']:
                    if otl_field in home_team:
                        logger.debug(f"Found OTL field '{otl_field}' in home_team: {home_team.get(otl_field)}")
                    if otl_field in away_team:
                        logger.debug(f"Found OTL field '{otl_field}' in away_team: {away_team.get(otl_field)}")
            
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
            
            # Get team records from standings API
            home_team_id_str = str(home_team.get('id', ''))
            away_team_id_str = str(away_team.get('id', ''))
            home_record = self._get_team_record(home_team_id_str)
            away_record = self._get_team_record(away_team_id_str)
            
            return {
                'league': 'NHL',
                'game_id': str(raw_game.get('id', '')),
                'game_date': game_date,
                'game_time': game_time,
                'game_type': game_type,
                'home_team': home_team_name,
                'home_team_abbrev': home_team.get('abbrev', ''),
                'home_team_id': home_team_id_str,
                'home_wins': home_record.get('wins', 0),
                'home_losses': home_record.get('losses', 0),
                'home_otl': home_record.get('ot', 0),
                'home_score_total': home_team.get('score', 0),
                'visitor_team': away_team_name,
                'visitor_team_abbrev': away_team.get('abbrev', ''),
                'visitor_team_id': away_team_id_str,
                'visitor_wins': away_record.get('wins', 0),
                'visitor_losses': away_record.get('losses', 0),
                'visitor_otl': away_record.get('ot', 0),
                'visitor_score_total': away_team.get('score', 0),
                'game_status': self.normalize_game_status(raw_game.get('gameState', 'scheduled')),
                'current_period': raw_game.get('periodDescriptor', {}).get('number', ''),
                'time_remaining': raw_game.get('clock', {}).get('timeRemaining', ''),
                'is_final': raw_game.get('gameState') in ('FINAL', 'OFF'),
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
            
            # Get team records from standings API
            home_team_id_str = str(home_team.get('id', ''))
            away_team_id_str = str(away_team.get('id', ''))
            home_record = self._get_team_record(home_team_id_str)
            away_record = self._get_team_record(away_team_id_str)
            
            return {
                'league': 'NHL',
                'game_id': str(raw_game.get('id', '')),
                'game_date': game_date_str,
                'game_time': game_time,
                'game_type': game_type,
                'home_team': home_team_name,
                'home_team_abbrev': home_team.get('abbrev', ''),
                'home_team_id': home_team_id_str,
                'home_score_total': home_score,
                'visitor_team': away_team_name,
                'visitor_team_abbrev': away_team.get('abbrev', ''),
                'visitor_team_id': away_team_id_str,
                'visitor_score_total': away_score,
                'game_status': self.normalize_game_status(raw_game.get('gameState', 'scheduled')),
                'current_period': raw_game.get('periodDescriptor', {}).get('number', ''),
                'time_remaining': raw_game.get('clock', {}).get('timeRemaining', ''),
                'is_final': raw_game.get('gameState') in ('FINAL', 'OFF'),
                'is_overtime': raw_game.get('periodDescriptor', {}).get('periodType') == 'OVERTIME',
                'home_wins': home_record.get('wins', 0),
                'home_losses': home_record.get('losses', 0),
                'home_otl': home_record.get('ot', 0),
                'visitor_wins': away_record.get('wins', 0),
                'visitor_losses': away_record.get('losses', 0),
                'visitor_otl': away_record.get('ot', 0),
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
