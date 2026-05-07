"""
Cricket data collector for the sports data service.
Proxies data from the CricketPuff API at ipl.cloud-puff.net.
"""

import requests
import re
from datetime import datetime, date
from typing import Dict, List, Optional, Any
import logging
import pytz

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
                api_date = data.get('date', '')
                return [self._parse_schedule_match(m, api_date) for m in data.get('matches', []) if m]
            return []
        except Exception as e:
            logger.error(f"Error fetching {self.league} schedule: {e}")
            return []

    def get_live_scores(self, date: Optional[date] = None) -> List[Dict[str, Any]]:
        self._check_rate_limit()
        try:
            date_param = date.strftime('%Y%m%d') if date else 'today'

            # Fetch both schedule (has records, start times) and scores (has results)
            sched_url = f"{CRICKETPUFF_BASE}/schedule/{self.league_slug}/{date_param}"
            scores_url = f"{CRICKETPUFF_BASE}/scores/{self.league_slug}/{date_param}"

            sched_resp = requests.get(sched_url, timeout=self.api_timeout)
            scores_resp = requests.get(scores_url, timeout=self.api_timeout)

            sched_by_match = {}
            if sched_resp.status_code == 200:
                for m in sched_resp.json().get('matches', []):
                    sched_by_match[m.get('match_no')] = m

            api_date = ''
            results = []
            if scores_resp.status_code == 200:
                scores_data = scores_resp.json()
                api_date = scores_data.get('date', '')
                for m in scores_data.get('matches', []):
                    sched = sched_by_match.get(m.get('match_no'), {})
                    results.append(self._parse_merged_match(m, sched, api_date))

            # Add any scheduled matches that don't have scores yet
            scored_nos = {m.get('match_no') for m in scores_resp.json().get('matches', [])} if scores_resp.status_code == 200 else set()
            for match_no, sched in sched_by_match.items():
                if match_no not in scored_nos:
                    results.append(self._parse_schedule_match(sched, api_date))

            return results
        except Exception as e:
            logger.error(f"Error fetching {self.league} scores: {e}")
            return []

    def parse_game_data(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return self._parse_schedule_match(raw, '')

    def _parse_schedule_match(self, raw: Dict[str, Any], api_date: str) -> Dict[str, Any]:
        home = raw.get('home', {})
        away = raw.get('away', {})
        if isinstance(home, str):
            home = {'abbrev': home, 'name': home, 'record': ''}
        if isinstance(away, str):
            away = {'abbrev': away, 'name': away, 'record': ''}

        home_record = self._parse_record(home.get('record', ''))
        away_record = self._parse_record(away.get('record', ''))

        status_text = raw.get('status', 'scheduled')
        is_final = 'won' in status_text.lower() or 'lost' in status_text.lower() or 'beat' in status_text.lower() or 'tied' in status_text.lower() or 'no result' in status_text.lower()
        is_in_progress = not is_final and bool(status_text) and status_text.lower() not in ('scheduled', '')

        start_time = raw.get('start_time', {})
        game_time = self._parse_pt_time(start_time.get('pt', ''), api_date)

        return {
            'league': self.league,
            'game_id': f"{self.league_slug}-{raw.get('match_no', 0)}",
            'game_date': api_date or datetime.now().strftime('%Y-%m-%d'),
            'game_time': game_time,
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
            'game_status': 'final' if is_final else ('in_progress' if is_in_progress else 'scheduled'),
            'current_period': '',
            'time_remaining': '',
            'is_final': is_final,
            'cricket_status': status_text,
            'cricket_venue': raw.get('venue', ''),
            'cricket_start_time': start_time,
            'cricket_home_nr': home_record[2],
            'cricket_away_nr': away_record[2],
            'cricket_home_score': '',
            'cricket_away_score': '',
            'cricket_winner': '',
            'cricket_result': status_text if is_final else '',
        }

    def _parse_merged_match(self, score_data: Dict, sched_data: Dict, api_date: str) -> Dict[str, Any]:
        home_abbrev = score_data.get('home', '')
        away_abbrev = score_data.get('away', '')
        result = score_data.get('result', '')
        winner = score_data.get('winner', '')
        home_score_str = score_data.get('home_score', '')
        away_score_str = score_data.get('away_score', '')

        is_final = bool(winner) or 'won' in result.lower() or 'lost' in result.lower() or 'beat' in result.lower() or 'tied' in result.lower() or 'no result' in result.lower()
        is_in_progress = not is_final and bool(result) and result.lower() not in ('scheduled', '')

        # Get records and start time from schedule data
        home_sched = sched_data.get('home', {})
        away_sched = sched_data.get('away', {})
        if isinstance(home_sched, str):
            home_sched = {'abbrev': home_sched, 'record': ''}
        if isinstance(away_sched, str):
            away_sched = {'abbrev': away_sched, 'record': ''}

        home_record = self._parse_record(home_sched.get('record', ''))
        away_record = self._parse_record(away_sched.get('record', ''))

        start_time = sched_data.get('start_time', {})
        game_time = self._parse_pt_time(start_time.get('pt', ''), api_date)

        # Determine visitor outcome
        if winner and winner == away_abbrev:
            away_outcome = 'won'
        elif winner and winner == home_abbrev:
            away_outcome = 'lost'
        elif winner:
            away_outcome = 'lost'
        else:
            away_outcome = ''

        return {
            'league': self.league,
            'game_id': f"{self.league_slug}-{score_data.get('match_no', 0)}",
            'game_date': api_date or datetime.now().strftime('%Y-%m-%d'),
            'game_time': game_time,
            'game_type': 'regular',
            'home_team': home_sched.get('name', home_abbrev),
            'home_team_abbrev': home_abbrev,
            'home_wins': home_record[0],
            'home_losses': home_record[1],
            'home_score_total': 0,
            'visitor_team': away_sched.get('name', away_abbrev),
            'visitor_team_abbrev': away_abbrev,
            'visitor_wins': away_record[0],
            'visitor_losses': away_record[1],
            'visitor_score_total': 0,
            'game_status': 'final' if is_final else ('in_progress' if is_in_progress else 'scheduled'),
            'current_period': '',
            'time_remaining': '',
            'is_final': is_final,
            'cricket_status': result or sched_data.get('status', 'scheduled'),
            'cricket_venue': score_data.get('venue', sched_data.get('venue', '')),
            'cricket_start_time': start_time,
            'cricket_home_nr': home_record[2],
            'cricket_away_nr': away_record[2],
            'cricket_home_score': home_score_str,
            'cricket_away_score': away_score_str,
            'cricket_winner': winner,
            'cricket_result': result,
            'cricket_away_outcome': away_outcome,
        }

    def _parse_pt_time(self, pt_str: str, date_str: str) -> Optional[datetime]:
        """Parse '7:00AM PDT' into a timezone-aware datetime."""
        if not pt_str or not date_str:
            return None
        try:
            clean = pt_str.strip()
            # Remove timezone suffix (PDT, PST, etc.)
            clean = re.sub(r'\s*(PDT|PST|PT)\s*$', '', clean, flags=re.IGNORECASE).strip()
            dt = datetime.strptime(f"{date_str} {clean}", '%Y-%m-%d %I:%M%p')
            pacific = pytz.timezone('US/Pacific')
            return pacific.localize(dt)
        except Exception as e:
            logger.debug(f"Could not parse PT time '{pt_str}' with date '{date_str}': {e}")
            return None

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
