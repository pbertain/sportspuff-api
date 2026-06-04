"""
NBA collector backed by TheSportsDB. Used when NBA_PROVIDER=thesportsdb.

NBA season convention: "2025-2026" means Oct 2025 -> Jun 2026.

Schedule + scores come from TheSportsDB's bulk-season feed (works from
prod, where stats.nba.com is unreachable). Standings and season-info
delegate to the legacy NBACollector since:
  - TheSportsDB's lookuptable.php returns 0 bytes for NBA
  - The ESPN-derived season-info (added in PR #15) is the right source
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .thesportsdb import TheSportsDBCollector


class NBATheSportsDBCollector(TheSportsDBCollector):
    LEAGUE_ID = 4387
    SPORTSPUFF_CODE = "NBA"

    def __init__(self):
        super().__init__("NBA")
        self._legacy = None

    def _legacy_collector(self):
        """Lazy NBACollector (legacy nba_api/ESPN) for standings + season-info."""
        if self._legacy is None:
            from .nba import NBACollector
            self._legacy = NBACollector()
        return self._legacy

    def current_season(self) -> str:
        """NBA "2025-2026" labels the season ending in 2026."""
        now = datetime.now(timezone.utc)
        end_year = now.year + 1 if now.month >= 8 else now.year
        return f"{end_year - 1}-{end_year}"

    def _parse_event(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            dt = self._parse_event_datetime(raw)
            game_date = self._local_date(raw)
            if not game_date:
                return None
            home_score = self._parse_int(raw.get("intHomeScore"))
            away_score = self._parse_int(raw.get("intAwayScore"))
            status = self._normalize_status(raw)
            return {
                "league": "NBA",
                "game_id": str(raw.get("idEvent") or ""),
                "game_date": game_date.strftime("%Y-%m-%d"),
                "game_time": dt,
                "game_type": "regular",
                "home_team": raw.get("strHomeTeam") or "",
                "home_team_abbrev": "",
                "home_team_id": str(raw.get("idHomeTeam") or ""),
                "home_wins": 0,
                "home_losses": 0,
                "home_score_total": home_score,
                "visitor_team": raw.get("strAwayTeam") or "",
                "visitor_team_abbrev": "",
                "visitor_team_id": str(raw.get("idAwayTeam") or ""),
                "visitor_wins": 0,
                "visitor_losses": 0,
                "visitor_score_total": away_score,
                "game_status": status,
                "current_period": "",
                "time_remaining": "",
                "is_final": status == "final",
                "is_overtime": False,
                "home_period_scores": {},
                "visitor_period_scores": {},
                "venue": raw.get("strVenue") or "",
                "home_team_badge": raw.get("strHomeTeamBadge") or "",
                "visitor_team_badge": raw.get("strAwayTeamBadge") or "",
            }
        except Exception:
            return None

    # Delegate standings + season-info to the legacy NBACollector.
    def get_standings(self) -> List[Dict[str, Any]]:
        return self._legacy_collector().get_standings()

    def get_season_info(self, year: int = None) -> Optional[Dict[str, Any]]:
        return self._legacy_collector().get_season_info(year=year)

    def set_timezone(self, timezone) -> None:
        super().set_timezone(timezone)
        # Mirror to legacy collector for downstream date logic in standings.
        legacy = self._legacy_collector()
        if hasattr(legacy, "set_timezone"):
            legacy.set_timezone(timezone)

