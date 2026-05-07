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

    TEAM_ABBREV_MAP = {
        108: 'LAA', 109: 'ARI', 110: 'BAL', 111: 'BOS',
        112: 'CHC', 113: 'CIN', 114: 'CLE', 115: 'COL',
        116: 'DET', 117: 'HOU', 118: 'KC',  119: 'LAD',
        120: 'WSH', 121: 'NYM', 133: 'OAK', 134: 'PIT',
        135: 'SD',  136: 'SEA', 137: 'SF',  138: 'STL',
        139: 'TB',  140: 'TEX', 141: 'TOR', 142: 'MIN',
        143: 'PHI', 144: 'ATL', 145: 'CWS', 146: 'MIA',
        147: 'NYY', 158: 'MIL',
    }

    def __init__(self):
        super().__init__("MLB")
        self._standings_cache = {}
        self._standings_cache_time = None
        self._standings_cache_ttl = 300

    def _fetch_standings(self):
        if self._standings_cache_time and time.time() - self._standings_cache_time < self._standings_cache_ttl:
            return self._standings_cache
        try:
            data = statsapi.standings_data()
            records = {}
            for div_id, div_data in data.items():
                for team in div_data.get('teams', []):
                    tid = team.get('team_id')
                    if tid:
                        records[tid] = {'wins': team.get('w', 0), 'losses': team.get('l', 0)}
            if records:
                self._standings_cache = records
                self._standings_cache_time = time.time()
            return records
        except Exception as e:
            logger.debug(f"Could not fetch MLB standings: {e}")
            return self._standings_cache

    def _get_team_abbrev(self, team_id, fallback=''):
        try:
            return self.TEAM_ABBREV_MAP.get(int(team_id), fallback)
        except (ValueError, TypeError):
            return fallback
    
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

    def get_season_info(self, year: int = None) -> Optional[Dict[str, Any]]:
        if year is None:
            now = datetime.now()
            year = now.year if now.month >= 3 else now.year - 1
        try:
            data = statsapi.get("season", {"seasonId": str(year), "sportId": 1})
            seasons = data.get('seasons', [])
            if not seasons:
                return None
            s = seasons[0]
            season_types = []
            if s.get('preSeasonStartDate') and s.get('preSeasonEndDate'):
                season_types.append({
                    'name': 'Spring Training',
                    'start_date': s['preSeasonStartDate'],
                    'end_date': s['preSeasonEndDate'],
                })
            if s.get('regularSeasonStartDate') and s.get('regularSeasonEndDate'):
                season_types.append({
                    'name': 'Regular Season',
                    'start_date': s['regularSeasonStartDate'],
                    'end_date': s['regularSeasonEndDate'],
                })
            if s.get('postSeasonStartDate') and s.get('postSeasonEndDate'):
                season_types.append({
                    'name': 'Postseason',
                    'start_date': s['postSeasonStartDate'],
                    'end_date': s['postSeasonEndDate'],
                })
            today = datetime.now().strftime('%Y-%m-%d')
            current_phase = 'Off Season'
            for t in season_types:
                if t['start_date'] <= today <= t['end_date']:
                    current_phase = t['name']
            return {
                'year': year,
                'current_phase': current_phase,
                'season_types': season_types,
            }
        except Exception as e:
            logger.error(f"Error fetching MLB season info: {e}")
            return None

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
        Uses schedule data for all games (already includes scores, status, innings).
        Only fetches detailed per-game data for games currently in progress.
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
                status = game.get('status', '')
                game_id = game.get('game_id')

                if status in ('In Progress', 'Live') and game_id:
                    try:
                        detailed_game = statsapi.get('game', {'gamePk': game_id})
                        parsed_game = self.parse_live_game_data(detailed_game)
                        if parsed_game:
                            parsed_games.append(parsed_game)
                            continue
                    except Exception as e:
                        logger.warning(f"Could not get detailed data for game {game_id}: {e}")

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
            
            # Parse game time from game_datetime (ISO 8601 UTC)
            game_time = None
            game_datetime_str = raw_game.get('game_datetime', '')
            if game_datetime_str:
                try:
                    game_time = datetime.fromisoformat(game_datetime_str.replace('Z', '+00:00'))
                except (ValueError, TypeError):
                    pass
            
            # Detect game type
            game_type = self._detect_mlb_game_type(raw_game)
            
            # Parse inning scores
            home_period_scores = self._parse_inning_scores(raw_game.get('home_inning_scores', []))
            visitor_period_scores = self._parse_inning_scores(raw_game.get('away_inning_scores', []))
            
            home_id = raw_game.get('home_id', '')
            away_id = raw_game.get('away_id', '')
            standings = self._fetch_standings()
            home_rec = standings.get(home_id, {})
            away_rec = standings.get(away_id, {})

            return {
                'league': 'MLB',
                'game_id': str(raw_game.get('game_id', '')),
                'game_date': game_date,
                'game_time': game_time,
                'game_type': game_type,
                'home_team': home_team,
                'home_team_abbrev': self._get_team_abbrev(home_id, home_team[:3].upper()),
                'home_team_id': str(home_id),
                'home_wins': home_rec.get('wins', 0),
                'home_losses': home_rec.get('losses', 0),
                'home_score_total': raw_game.get('home_score', 0),
                'visitor_team': away_team,
                'visitor_team_abbrev': self._get_team_abbrev(away_id, away_team[:3].upper()),
                'visitor_team_id': str(away_id),
                'visitor_wins': away_rec.get('wins', 0),
                'visitor_losses': away_rec.get('losses', 0),
                'visitor_score_total': raw_game.get('away_score', 0),
                'game_status': self.normalize_game_status(raw_game.get('status', 'scheduled')),
                'current_period': raw_game.get('current_inning', ''),
                'time_remaining': raw_game.get('inning_state', ''),
                'is_final': raw_game.get('status') in ('Final', 'Game Over', 'Completed Early'),
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
            
            # Get status from gameData (not root level)
            status_obj = game_data.get('status', {})
            detailed_state = status_obj.get('detailedState', 'scheduled')

            # Get team records from gameData
            home_record = home_team.get('record', {})
            away_record = away_team.get('record', {})

            return {
                'league': 'MLB',
                'game_id': str(raw_game.get('gamePk', '')),
                'game_date': datetime.now().strftime('%Y-%m-%d'),
                'game_type': 'regular',
                'home_team': home_team.get('name', ''),
                'home_team_abbrev': home_team.get('abbreviation', ''),
                'home_team_id': str(home_team.get('id', '')),
                'home_wins': home_record.get('wins', 0),
                'home_losses': home_record.get('losses', 0),
                'home_score_total': linescore.get('teams', {}).get('home', {}).get('runs', 0),
                'visitor_team': away_team.get('name', ''),
                'visitor_team_abbrev': away_team.get('abbreviation', ''),
                'visitor_team_id': str(away_team.get('id', '')),
                'visitor_wins': away_record.get('wins', 0),
                'visitor_losses': away_record.get('losses', 0),
                'visitor_score_total': linescore.get('teams', {}).get('away', {}).get('runs', 0),
                'game_status': self.normalize_game_status(detailed_state),
                'current_period': linescore.get('currentInning', ''),
                'time_remaining': linescore.get('inningState', ''),
                'is_final': detailed_state in ('Final', 'Game Over', 'Completed Early'),
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
