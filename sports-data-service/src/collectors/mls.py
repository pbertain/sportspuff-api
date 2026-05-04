"""
MLS data collector for the sports data service.
Uses the ESPN undocumented API.
"""

import requests
import pytz
from datetime import datetime, date
from typing import Dict, List, Optional, Any
import logging

from .base import BaseCollector

logger = logging.getLogger(__name__)

ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports/soccer/usa.1"


class MLSCollector(BaseCollector):
    """MLS data collector using the ESPN API."""

    def __init__(self):
        super().__init__("MLS")

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
                if calendar:
                    start_date = min(calendar)[:10] if calendar else None
                    end_date = max(calendar)[:10] if calendar else None
                    if start_date and end_date:
                        today = datetime.now().strftime('%Y-%m-%d')
                        current_phase = 'Off Season'
                        if start_date <= today <= end_date:
                            current_phase = 'Regular Season'
                        return {
                            'year': year,
                            'current_phase': current_phase,
                            'season_types': [
                                {'name': 'Regular Season', 'start_date': start_date, 'end_date': end_date},
                            ],
                        }
            return None
        except Exception as e:
            logger.error(f"Error fetching MLS season info: {e}")
            return None

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
