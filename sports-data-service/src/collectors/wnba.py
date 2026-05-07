"""
WNBA data collector for the sports data service.
Uses the wnba-api.p.rapidapi.com API.
"""

import os
import requests
import time
import pytz
from datetime import datetime, date
from typing import Dict, List, Optional, Any
import logging

from .base import BaseCollector

logger = logging.getLogger(__name__)


class WNBACollector(BaseCollector):
    """WNBA data collector using the wnba-api RapidAPI."""

    def __init__(self):
        super().__init__("WNBA")
        self.api_key = os.environ.get('WNBA_API_KEY', '')
        self.base_url = "https://wnba-api.p.rapidapi.com"
        self.headers = {
            "x-rapidapi-key": self.api_key,
            "x-rapidapi-host": "wnba-api.p.rapidapi.com",
            "Content-Type": "application/json",
        }

    def _fetch_schedule(self, target_date: date) -> List[Dict[str, Any]]:
        url = f"{self.base_url}/wnbaschedule"
        params = {
            "year": target_date.year,
            "month": f"{target_date.month:02d}",
            "day": f"{target_date.day:02d}",
        }
        response = requests.get(url, headers=self.headers, params=params, timeout=self.api_timeout)
        if response.status_code != 200:
            logger.error(f"WNBA API error: {response.status_code}")
            return []

        data = response.json()
        date_key = target_date.strftime('%Y%m%d')

        games_raw = data.get(date_key, [])
        if not games_raw:
            for key, val in data.items():
                if key == date_key and isinstance(val, list):
                    games_raw = val
                    break

        return games_raw

    def get_schedule(self, date: Optional[date] = None) -> List[Dict[str, Any]]:
        self._check_rate_limit()
        try:
            target_date = date or datetime.now().date()
            games_raw = self._fetch_schedule(target_date)
            return [g for g in (self.parse_game_data(r) for r in games_raw) if g]
        except Exception as e:
            logger.error(f"Error fetching WNBA schedule: {e}")
            return []

    def get_live_scores(self, date: Optional[date] = None) -> List[Dict[str, Any]]:
        return self.get_schedule(date)

    def parse_game_data(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            competitors = raw.get('competitors', [])
            if len(competitors) < 2:
                return None

            home = next((c for c in competitors if c.get('isHome')), competitors[0])
            away = next((c for c in competitors if not c.get('isHome')), competitors[1])

            if not home.get('abbrev') or not away.get('abbrev'):
                return None

            status = raw.get('status', {})
            state = status.get('state', 'pre')
            detail = status.get('detail', '')
            alt_detail = status.get('altDetail', '')
            completed = raw.get('completed', False)

            season = raw.get('season', {})
            season_slug = season.get('slug', 'regular-season')
            game_type_map = {
                'preseason': 'preseason',
                'regular-season': 'regular',
                'postseason': 'playoffs',
                'off-season': 'regular',
            }
            game_type = game_type_map.get(season_slug, 'regular')

            if state == 'post' or completed:
                game_status = 'final'
                is_final = True
            elif state == 'in':
                game_status = 'in_progress'
                is_final = False
            else:
                game_status = 'scheduled'
                is_final = False

            game_time = None
            date_str = raw.get('date', '')
            game_date_str = ''
            if date_str:
                try:
                    dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                    game_time = dt
                    pacific = pytz.timezone('US/Pacific')
                    game_date_str = dt.astimezone(pacific).strftime('%Y-%m-%d')
                except (ValueError, TypeError):
                    pass
            if not game_date_str:
                game_date_str = datetime.now().strftime('%Y-%m-%d')

            home_score = home.get('score', 0)
            away_score = away.get('score', 0)
            try:
                home_score = int(home_score) if home_score else 0
                away_score = int(away_score) if away_score else 0
            except (ValueError, TypeError):
                home_score = 0
                away_score = 0

            current_period = ''
            time_remaining = ''
            is_overtime = False
            if state == 'in' and detail:
                if 'OT' in detail.upper():
                    is_overtime = True

            home_rec = self._parse_record(home.get('recordSummary', ''))
            away_rec = self._parse_record(away.get('recordSummary', ''))

            return {
                'league': 'WNBA',
                'game_id': str(raw.get('id', '')),
                'game_date': game_date_str,
                'game_time': game_time,
                'game_type': game_type,
                'home_team': home.get('displayName', home.get('name', '')),
                'home_team_abbrev': home.get('abbrev', ''),
                'home_team_id': str(home.get('id', '')),
                'home_wins': home_rec[0],
                'home_losses': home_rec[1],
                'home_score_total': home_score,
                'visitor_team': away.get('displayName', away.get('name', '')),
                'visitor_team_abbrev': away.get('abbrev', ''),
                'visitor_team_id': str(away.get('id', '')),
                'visitor_wins': away_rec[0],
                'visitor_losses': away_rec[1],
                'visitor_score_total': away_score,
                'game_status': game_status,
                'current_period': current_period,
                'time_remaining': time_remaining,
                'is_final': is_final,
                'is_overtime': is_overtime,
            }
        except Exception as e:
            logger.error(f"Error parsing WNBA game data: {e}")
            return None

    def get_season_info(self, year: int = None) -> Optional[Dict[str, Any]]:
        if year is None:
            year = datetime.now().year
        try:
            self._check_rate_limit()
            url = f"{self.base_url}/wnbastandings"
            params = {"year": year}
            response = requests.get(url, headers=self.headers, params=params, timeout=self.api_timeout)
            if response.status_code != 200:
                return None

            data = response.json()
            season_types = []

            if 'seasons' in data and isinstance(data['seasons'], list):
                for s in data['seasons']:
                    if s.get('year') == year and 'types' in s:
                        for t in s['types']:
                            start = t.get('startDate', '')
                            end = t.get('endDate', '')
                            if start:
                                start = start.split('T')[0]
                            if end:
                                end = end.split('T')[0]
                            season_types.append({
                                'name': t.get('name', ''),
                                'start_date': start,
                                'end_date': end,
                            })
                        break

            today = datetime.now().strftime('%Y-%m-%d')
            current_phase = 'Off Season'
            for t in season_types:
                if t['start_date'] and t['end_date'] and t['start_date'] <= today <= t['end_date']:
                    current_phase = t['name']

            return {
                'year': year,
                'current_phase': current_phase,
                'season_types': season_types,
            }
        except Exception as e:
            logger.error(f"Error fetching WNBA season info: {e}")
            return None

    def _parse_record(self, record_str: str):
        """Parse 'W-L' record string. Returns (wins, losses)."""
        if not record_str:
            return (0, 0)
        parts = record_str.split('-')
        try:
            return (int(parts[0]), int(parts[1]))
        except (ValueError, IndexError):
            return (0, 0)
