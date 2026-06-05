"""
FIFA World Cup collector backed by TheSportsDB (league_id 4429).

Soccer / football scoring (3 points for a win, 1 for a draw). Group-stage
standings derived from completed matches; group identifier (A, B, C, ...)
isn't exposed by TheSportsDB so the standings table is flat — frontends
that want grouped tables can derive groups from the FIFA bracket on their
side, or we can add a hand-curated team->group mapping later.

Knockout-round events (round of 32, 16, etc.) are added by TheSportsDB
once group standings are decided; until then only group matches appear
in eventsseason.php.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pytz

from .thesportsdb import TheSportsDBCollector

logger = logging.getLogger(__name__)


class WorldCupTheSportsDBCollector(TheSportsDBCollector):
    LEAGUE_ID = 4429
    SPORTSPUFF_CODE = "WC"

    def __init__(self):
        super().__init__("WC")
        self.timezone = pytz.timezone("US/Pacific")

    def current_season(self) -> str:
        """FIFA World Cup runs every 4 years. Use the most recent year that
        is a WC year and <= current year. Hosted years: 2022, 2026, 2030.
        For 2025 we'd return '2026' (the upcoming WC); for 2027 we'd
        return '2026' (the most recently completed WC)."""
        n = datetime.now(timezone.utc)
        # The set of WC years TheSportsDB has seasons for. Update as the
        # tournament rolls forward.
        wc_years = (2014, 2018, 2022, 2026, 2030)
        # If we're inside a WC year, use it; otherwise use the latest
        # past WC year.
        if n.year in wc_years:
            return str(n.year)
        past = [y for y in wc_years if y <= n.year]
        return str(max(past)) if past else "2026"

    def _parse_event(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            home = raw.get("strHomeTeam") or ""
            away = raw.get("strAwayTeam") or ""
            if not home or not away:
                return None

            dt = self._parse_event_datetime(raw)
            game_date = self._local_date(raw) or (dt.date() if dt else datetime.now().date())

            home_score = self._parse_int(raw.get("intHomeScore"))
            away_score = self._parse_int(raw.get("intAwayScore"))
            status = self._normalize_status(raw)
            is_final = status == "final"

            game_type = self._round_label(raw)

            return {
                "league": "WC",
                "game_id": str(raw.get("idEvent") or ""),
                "game_date": game_date.strftime("%Y-%m-%d"),
                "game_time": dt,
                "game_type": game_type,
                "home_team": home,
                "home_team_abbrev": (raw.get("strHomeTeamShort") or home[:3] or "").upper(),
                "home_team_id": str(raw.get("idHomeTeam") or ""),
                "home_wins": 0,
                "home_losses": 0,
                "home_score_total": home_score,
                "visitor_team": away,
                "visitor_team_abbrev": (raw.get("strAwayTeamShort") or away[:3] or "").upper(),
                "visitor_team_id": str(raw.get("idAwayTeam") or ""),
                "visitor_wins": 0,
                "visitor_losses": 0,
                "visitor_score_total": away_score,
                "game_status": status,
                "current_period": "",
                "time_remaining": "",
                "is_final": is_final,
                "is_overtime": False,
                "home_period_scores": {},
                "visitor_period_scores": {},
                "venue": raw.get("strVenue") or "",
                "home_team_badge": raw.get("strHomeTeamBadge") or "",
                "visitor_team_badge": raw.get("strAwayTeamBadge") or "",
                # World Cup-specific
                "wc_round": raw.get("intRound") or "",
                "wc_round_label": game_type,
            }
        except Exception as e:
            logger.error("WorldCup parse error: %s", e)
            return None

    @staticmethod
    def _round_label(raw: Dict[str, Any]) -> str:
        """Map intRound to a human-readable phase. TheSportsDB uses 1/2/3
        for matchdays 1-3 of the group stage; knockout round numbers vary
        per tournament. Fall back to 'group_stage' or 'knockout' generically."""
        try:
            r = int(raw.get("intRound") or 0)
        except Exception:
            return "group_stage"
        if r in (1, 2, 3):
            return f"group_matchday_{r}"
        if r in (4,):
            return "round_of_16"
        if r in (5,):
            return "quarterfinal"
        if r in (6,):
            return "semifinal"
        if r in (7,):
            return "third_place"
        if r in (8,):
            return "final"
        return "knockout"

    # ---- soccer-style standings (3-1-0 points) -----------------------------
    def get_standings(self) -> List[Dict[str, Any]]:
        season = self.current_season()
        try:
            events = self._season_events(season)
        except Exception as e:
            logger.error("WC standings: cannot fetch season: %s", e)
            return []

        records: Dict[str, Dict[str, Any]] = defaultdict(
            lambda: {"matches": 0, "wins": 0, "draws": 0, "losses": 0,
                     "goals_for": 0, "goals_against": 0}
        )

        for raw in events:
            if (raw.get("strStatus") or "").upper() != "FT":
                continue
            try:
                r = int(raw.get("intRound") or 0)
            except Exception:
                r = 0
            # Standings are only meaningful for the group stage (rounds 1-3).
            # Knockouts don't roll up to a points table.
            if r not in (1, 2, 3):
                continue
            home = raw.get("strHomeTeam") or ""
            away = raw.get("strAwayTeam") or ""
            hs = self._parse_int(raw.get("intHomeScore"), default=-1)
            as_ = self._parse_int(raw.get("intAwayScore"), default=-1)
            if not home or not away or hs < 0 or as_ < 0:
                continue
            r_h = records[home]; r_a = records[away]
            r_h["matches"] += 1; r_a["matches"] += 1
            r_h["goals_for"] += hs; r_h["goals_against"] += as_
            r_a["goals_for"] += as_; r_a["goals_against"] += hs
            if hs == as_:
                r_h["draws"] += 1; r_a["draws"] += 1
            elif hs > as_:
                r_h["wins"] += 1; r_a["losses"] += 1
            else:
                r_a["wins"] += 1; r_h["losses"] += 1

        ordered = []
        for team_name, rec in records.items():
            points = rec["wins"] * 3 + rec["draws"]
            gd = rec["goals_for"] - rec["goals_against"]
            ordered.append({
                "team_name": team_name,
                "abbreviation": (team_name[:3] or "").upper(),
                "matches": rec["matches"],
                "wins": rec["wins"],
                "draws": rec["draws"],
                "losses": rec["losses"],
                "goals_for": rec["goals_for"],
                "goals_against": rec["goals_against"],
                "goal_difference": gd,
                "points": points,
                "record": f"{rec['wins']}-{rec['draws']}-{rec['losses']}",
            })
        ordered.sort(key=lambda r: (-r["points"], -r["goal_difference"], -r["goals_for"]))
        for rank, rec in enumerate(ordered, 1):
            rec["rank"] = rank
        return ordered

    def get_season_info(self, year: int = None) -> Optional[Dict[str, Any]]:
        """Build season_types from the bulk events (start = first match,
        end = latest knockout). Until knockouts are populated, this just
        spans the group stage; it'll auto-extend when TheSportsDB adds
        knockout fixtures."""
        season = year or self.current_season()
        if isinstance(season, int):
            season = str(season)
        try:
            events = self._season_events(season)
        except Exception:
            return None
        if not events:
            return None
        dates = sorted({(e.get("dateEvent") or "")[:10] for e in events if e.get("dateEvent")})
        if not dates:
            return None
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        current_phase = "Tournament" if dates[0] <= today <= dates[-1] else (
            "Upcoming" if today < dates[0] else "Off Season"
        )
        return {
            "year": int(season) if season.isdigit() else season,
            "current_phase": current_phase,
            "season_types": [
                {"name": "FIFA World Cup", "start_date": dates[0], "end_date": dates[-1]},
            ],
        }
