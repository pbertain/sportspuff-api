"""
Cricket data collector for the sports data service.
Proxies data from the CricketPuff API at ipl.cloud-puff.net.
"""

import requests
import re
from datetime import datetime, date
from typing import Dict, List, Optional, Any
import logging

from .base import BaseCollector

logger = logging.getLogger(__name__)

CRICKETPUFF_BASE = "https://ipl.cloud-puff.net/api/v1"


class CricketCollector(BaseCollector):
    """Cricket data collector using the CricketPuff API."""

    def __init__(self, league: str = "IPL"):
        super().__init__(league)
        self.league_slug = league.lower()

    def get_schedule(self, date: Optional[date] = None) -> List[Dict[str, Any]]:
        self._check_rate_limit()
        try:
            date_param = date.strftime('%Y%m%d') if date else 'today'
            url = f"{CRICKETPUFF_BASE}/schedule/{self.league_slug}/{date_param}"
            response = requests.get(url, timeout=self.api_timeout)
            if response.status_code == 200:
                data = response.json()
                return [self.parse_game_data(m) for m in data.get('matches', []) if m]
            return []
        except Exception as e:
            logger.error(f"Error fetching {self.league} schedule: {e}")
            return []

    def get_live_scores(self, date: Optional[date] = None) -> List[Dict[str, Any]]:
        self._check_rate_limit()
        try:
            date_param = date.strftime('%Y%m%d') if date else 'today'
            url = f"{CRICKETPUFF_BASE}/scores/{self.league_slug}/{date_param}"
            response = requests.get(url, timeout=self.api_timeout)
            if response.status_code == 200:
                data = response.json()
                return [self._parse_score(m) for m in data.get('matches', []) if m]
            return []
        except Exception as e:
            logger.error(f"Error fetching {self.league} scores: {e}")
            return []

    def parse_game_data(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        home = raw.get('home', {})
        away = raw.get('away', {})
        if isinstance(home, str):
            home = {'abbrev': home, 'name': home, 'record': ''}
        if isinstance(away, str):
            away = {'abbrev': away, 'name': away, 'record': ''}

        home_record = self._parse_record(home.get('record', ''))
        away_record = self._parse_record(away.get('record', ''))

        status_text = raw.get('status', 'scheduled')
        is_final = bool(raw.get('winner')) or 'won' in status_text.lower()

        start_time = raw.get('start_time', {})

        return {
            'league': self.league,
            'game_id': f"{self.league_slug}-{raw.get('match_no', 0)}",
            'game_date': datetime.now().strftime('%Y-%m-%d'),
            'game_time': None,
            'game_type': 'regular',
            'home_team': home.get('name', home.get('abbrev', '')),
            'home_team_abbrev': home.get('abbrev', ''),
            'home_wins': home_record[0],
            'home_losses': home_record[1],
            'home_score_total': 0,
            'visitor_team': away.get('name', away.get('abbrev', '')),
            'visitor_team_abbrev': away.get('abbrev', ''),
            'visitor_wins': away_record[0],
            'visitor_losses': away_record[1],
            'visitor_score_total': 0,
            'game_status': 'final' if is_final else 'scheduled',
            'current_period': '',
            'time_remaining': '',
            'is_final': is_final,
            'cricket_status': status_text,
            'cricket_venue': raw.get('venue', ''),
            'cricket_start_time': start_time,
            'cricket_home_nr': home_record[2],
            'cricket_away_nr': away_record[2],
        }

    def _parse_score(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        home_abbrev = raw.get('home', '')
        away_abbrev = raw.get('away', '')
        status_text = raw.get('result', raw.get('status', 'scheduled'))
        is_final = bool(raw.get('winner')) or 'won' in status_text.lower()

        return {
            'league': self.league,
            'game_id': f"{self.league_slug}-{raw.get('match_no', 0)}",
            'game_date': datetime.now().strftime('%Y-%m-%d'),
            'game_time': None,
            'game_type': 'regular',
            'home_team': home_abbrev,
            'home_team_abbrev': home_abbrev,
            'home_wins': 0,
            'home_losses': 0,
            'home_score_total': 0,
            'visitor_team': away_abbrev,
            'visitor_team_abbrev': away_abbrev,
            'visitor_wins': 0,
            'visitor_losses': 0,
            'visitor_score_total': 0,
            'game_status': 'final' if is_final else 'scheduled',
            'current_period': '',
            'time_remaining': '',
            'is_final': is_final,
            'cricket_status': status_text,
            'cricket_venue': raw.get('venue', ''),
        }

    def get_season_info(self, year: int = None) -> Optional[Dict[str, Any]]:
        try:
            url = f"{CRICKETPUFF_BASE}/season-info/{self.league_slug}"
            response = requests.get(url, timeout=self.api_timeout)
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            logger.error(f"Error fetching {self.league} season info: {e}")
            return None

    def _parse_record(self, record_str: str):
        """Parse W-L-NR record string. Returns (wins, losses, no_result)."""
        if not record_str:
            return (0, 0, 0)
        parts = record_str.split('-')
        try:
            w = int(parts[0]) if len(parts) > 0 else 0
            l = int(parts[1]) if len(parts) > 1 else 0
            nr = int(parts[2]) if len(parts) > 2 else 0
            return (w, l, nr)
        except (ValueError, IndexError):
            return (0, 0, 0)
