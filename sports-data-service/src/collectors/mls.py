"""
MLS data collector for the sports data service.
Uses the ESPN undocumented API.
"""

import requests
import pytz
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any
import logging

from .base import BaseCollector

logger = logging.getLogger(__name__)

ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports/soccer/usa.1"


class MLSCollector(BaseCollector):
    """MLS data collector using the ESPN API."""

    def __init__(self):
        super().__init__("MLS")
        self._standings_cache = {}
        self._standings_cache_time = None
        self._standings_cache_ttl = 3600

    def _fetch_standings(self):
        if self._standings_cache_time and (datetime.now().timestamp() - self._standings_cache_time < self._standings_cache_ttl):
            return self._standings_cache
        try:
            url = "https://site.api.espn.com/apis/v2/sports/soccer/usa.1/standings"
            response = requests.get(url, timeout=self.api_timeout)
            if response.status_code == 200:
                data = response.json()
                records = {}
                for child in data.get('children', []):
                    for entry in child.get('standings', {}).get('entries', []):
                        team = entry.get('team', {})
                        abbrev = team.get('abbreviation', '')
                        stats = {s['name']: s.get('value', 0) for s in entry.get('stats', [])}
                        if abbrev:
                            records[abbrev] = {
                                'wins': int(stats.get('wins', 0)),
                                'draws': int(stats.get('ties', 0)),
                                'losses': int(stats.get('losses', 0)),
                            }
                if records:
                    self._standings_cache = records
                    self._standings_cache_time = datetime.now().timestamp()
                return records
        except Exception as e:
            logger.debug(f"Could not fetch MLS standings: {e}")
        return self._standings_cache

    def get_schedule(self, date: Optional[date] = None) -> List[Dict[str, Any]]:
        self._check_rate_limit()
        try:
            target = date or datetime.now().date()
            date_str = target.strftime('%Y%m%d')
            url = f"{ESPN_BASE}/scoreboard?dates={date_str}"
            response = requests.get(url, timeout=self.api_timeout)
            if response.status_code == 200:
                data = response.json()
                return [g for g in (self._parse_event(e) for e in data.get('events', [])) if g]
            return []
        except Exception as e:
            logger.error(f"Error fetching MLS schedule: {e}")
            return []

    def get_live_scores(self, date: Optional[date] = None) -> List[Dict[str, Any]]:
        return self.get_schedule(date)

    def parse_game_data(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        return self._parse_event(raw)

    def _parse_event(self, event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            comps = event.get('competitions', [{}])[0]
            competitors = comps.get('competitors', [])
            if len(competitors) < 2:
                return None

            home = next((c for c in competitors if c.get('homeAway') == 'home'), competitors[0])
            away = next((c for c in competitors if c.get('homeAway') == 'away'), competitors[1])

            home_team = home.get('team', {})
            away_team = away.get('team', {})

            status = comps.get('status', {})
            status_type = status.get('type', {})
            state = status_type.get('state', 'pre')
            status_name = status_type.get('name', '')
            detail = status_type.get('detail', '')

            if state == 'post' or status_name == 'STATUS_FULL_TIME':
                game_status = 'final'
                is_final = True
            elif state == 'in':
                game_status = 'in_progress'
                is_final = False
            else:
                game_status = 'scheduled'
                is_final = False

            game_time = None
            event_date = event.get('date', '')
            game_date_str = ''
            if event_date:
                try:
                    dt = datetime.fromisoformat(event_date.replace('Z', '+00:00'))
                    game_time = dt
                    pacific = pytz.timezone('US/Pacific')
                    game_date_str = dt.astimezone(pacific).strftime('%Y-%m-%d')
                except (ValueError, TypeError):
                    pass
            if not game_date_str:
                game_date_str = datetime.now().strftime('%Y-%m-%d')

            home_score = int(home.get('score', 0) or 0)
            away_score = int(away.get('score', 0) or 0)

            home_rec = self._parse_record(home.get('records', [{}])[0].get('summary', '') if home.get('records') else '')
            away_rec = self._parse_record(away.get('records', [{}])[0].get('summary', '') if away.get('records') else '')

            # Fall back to standings cache if records not in game data
            if home_rec == (0, 0, 0) or away_rec == (0, 0, 0):
                standings = self._fetch_standings()
                home_abbr = home_team.get('abbreviation', '')
                away_abbr = away_team.get('abbreviation', '')
                if home_rec == (0, 0, 0) and home_abbr in standings:
                    sr = standings[home_abbr]
                    home_rec = (sr['wins'], sr['losses'], sr['draws'])
                if away_rec == (0, 0, 0) and away_abbr in standings:
                    sr = standings[away_abbr]
                    away_rec = (sr['wins'], sr['losses'], sr['draws'])

            season = event.get('season', {})
            season_type = season.get('slug', 'regular-season')
            game_type_map = {
                'regular-season': 'regular',
                'postseason': 'playoffs',
                'preseason': 'preseason',
            }
            game_type = game_type_map.get(season_type, 'regular')

            clock = status.get('displayClock', '')
            period = status.get('period', 0)

            return {
                'league': 'MLS',
                'game_id': str(event.get('id', '')),
                'game_date': game_date_str,
                'game_time': game_time,
                'game_type': game_type,
                'home_team': home_team.get('displayName', home_team.get('name', '')),
                'home_team_abbrev': home_team.get('abbreviation', ''),
                'home_team_id': str(home_team.get('id', '')),
                'home_wins': home_rec[0],
                'home_losses': home_rec[1],
                'home_draws': home_rec[2],
                'home_score_total': home_score,
                'visitor_team': away_team.get('displayName', away_team.get('name', '')),
                'visitor_team_abbrev': away_team.get('abbreviation', ''),
                'visitor_team_id': str(away_team.get('id', '')),
                'visitor_wins': away_rec[0],
                'visitor_losses': away_rec[1],
                'visitor_draws': away_rec[2],
                'visitor_score_total': away_score,
                'game_status': game_status,
                'current_period': str(period) if period else '',
                'time_remaining': clock if clock and clock != '0:00' else '',
                'is_final': is_final,
                'mls_detail': detail,
            }
        except Exception as e:
            logger.error(f"Error parsing MLS event: {e}")
            return None

    def get_season_info(self, year: int = None) -> Optional[Dict[str, Any]]:
        try:
            if year is None:
                year = datetime.now().year
            self._check_rate_limit()
            url = f"{ESPN_BASE}/scoreboard?dates={year}0101-{year}1231"
            response = requests.get(url, timeout=self.api_timeout)
            if response.status_code == 200:
                data = response.json()
                calendar = data.get('leagues', [{}])[0].get('calendar', [])
                match_days = sorted({c[:10] for c in calendar if c})
                if match_days:
                    today = datetime.now().strftime('%Y-%m-%d')
                    season_types = self._build_season_segments(match_days)
                    current_phase = 'Off Season'
                    for t in season_types:
                        if t['start_date'] <= today <= t['end_date']:
                            current_phase = t['name']
                    return {
                        'year': year,
                        'current_phase': current_phase,
                        'season_types': season_types,
                    }
            return None
        except Exception as e:
            logger.error(f"Error fetching MLS season info: {e}")
            return None

    def _build_season_segments(self, match_days):
        """Split the season around its longest mid-season gap (the FIFA World Cup break)."""
        days = [datetime.strptime(d, '%Y-%m-%d').date() for d in match_days]
        gap_idx = None
        gap_len = timedelta(0)
        for i in range(1, len(days)):
            delta = days[i] - days[i - 1]
            if delta > gap_len:
                gap_len = delta
                gap_idx = i
        if gap_idx is not None and gap_len >= timedelta(days=21):
            return [
                {'name': 'Regular Season', 'start_date': match_days[0], 'end_date': match_days[gap_idx - 1]},
                {
                    'name': 'FIFA World Cup Break',
                    'start_date': (days[gap_idx - 1] + timedelta(days=1)).isoformat(),
                    'end_date': (days[gap_idx] - timedelta(days=1)).isoformat(),
                },
                {'name': 'Regular Season', 'start_date': match_days[gap_idx], 'end_date': match_days[-1]},
            ]
        return [{'name': 'Regular Season', 'start_date': match_days[0], 'end_date': match_days[-1]}]

    def _parse_record(self, record_str: str):
        """Parse W-D-L or W-L-D record string. Returns (wins, losses, draws)."""
        if not record_str:
            return (0, 0, 0)
        parts = record_str.split('-')
        try:
            w = int(parts[0]) if len(parts) > 0 else 0
            d = int(parts[1]) if len(parts) > 1 else 0
            l = int(parts[2]) if len(parts) > 2 else 0
            return (w, l, d)
        except (ValueError, IndexError):
            return (0, 0, 0)
