"""
IPL / MLC collectors backed by TheSportsDB.

TheSportsDB returns one bulk-season feed per league, which we cache on disk.
Each cricket event has runs (intHomeScore/intAwayScore — no wickets) plus
team names, venue, status, and time. We synthesize the cricket-specific
response fields the existing api.py response-builder expects:

  cricket_home_score, cricket_away_score, cricket_status, cricket_venue,
  cricket_winner, cricket_start_time, cricket_home_nr, cricket_away_nr

Standings are derived from completed events using the same wins/losses/NRR
shape cricket.py already produces, so /api/v1/standings/{ipl,mlc} keeps
working downstream.

Team abbreviations and aliases are reused from cricket.LEAGUE_CONFIGS so the
output is byte-compatible with the existing CricAPI-backed collector.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

import pytz

from .cricket import LEAGUE_CONFIGS
from .thesportsdb import TheSportsDBCollector

logger = logging.getLogger(__name__)


# Map our internal cricket-league codes to TheSportsDB IDs (from recon).
LEAGUE_IDS: Dict[str, int] = {
    "IPL": 4460,
    "MLC": 5401,
}


class CricketTheSportsDBCollector(TheSportsDBCollector):
    """Cricket variant of the TheSportsDB collector. Subclasses set LEAGUE_ID
    via the constructor argument; team abbreviations come from
    cricket.LEAGUE_CONFIGS."""

    def __init__(self, league_code: str):
        super().__init__(league_code.upper())
        self.LEAGUE_ID = LEAGUE_IDS[self.SPORTSPUFF_CODE]
        self.config = LEAGUE_CONFIGS[self.SPORTSPUFF_CODE]
        # Cricket display defaults to Pacific until set_timezone overrides.
        self.timezone = pytz.timezone("US/Pacific")

    def current_season(self) -> str:
        # IPL / MLC label seasons by the calendar year they're played in.
        return str(datetime.now(timezone.utc).year)

    # ---- abbreviations / aliases (reuse LEAGUE_CONFIGS from cricket.py) ----
    def _canonical(self, team_name: str) -> str:
        return self.config.get("aliases", {}).get(team_name, team_name)

    def _abbr(self, team_name: str) -> str:
        if not team_name:
            return ""
        team_name = self._canonical(team_name)
        return self.config["teams"].get(team_name, team_name[:4].upper())

    # ---- per-event parser --------------------------------------------------
    def _parse_event(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            home = self._canonical(raw.get("strHomeTeam") or "")
            away = self._canonical(raw.get("strAwayTeam") or "")
            if not home or not away:
                return None

            dt = self._parse_event_datetime(raw)
            game_date = self._local_date(raw) or (dt.date() if dt else datetime.now().date())

            home_score_raw = raw.get("intHomeScore")
            away_score_raw = raw.get("intAwayScore")
            home_score_int = self._parse_int(home_score_raw, default=-1)
            away_score_int = self._parse_int(away_score_raw, default=-1)
            has_score = home_score_int >= 0 or away_score_int >= 0

            home_score_str = str(home_score_raw) if home_score_raw not in (None, "") else ""
            away_score_str = str(away_score_raw) if away_score_raw not in (None, "") else ""

            status = self._normalize_status(raw)
            is_final = status == "final"

            winner_abbr = ""
            if is_final and home_score_int != away_score_int and (home_score_int >= 0 and away_score_int >= 0):
                winner_abbr = self._abbr(home if home_score_int > away_score_int else away)

            cricket_status = "Final" if is_final else (raw.get("strStatus") or "scheduled")

            return {
                "league": self.SPORTSPUFF_CODE,
                "game_id": f"{self.SPORTSPUFF_CODE.lower()}-{raw.get('idEvent', '')}",
                "game_date": game_date.strftime("%Y-%m-%d"),
                "game_time": dt,
                "game_type": "playoffs" if self._is_playoff_round(raw) else "regular",
                "home_team": home,
                "home_team_abbrev": self._abbr(home),
                "home_wins": 0,
                "home_losses": 0,
                "home_score_total": 0,
                "visitor_team": away,
                "visitor_team_abbrev": self._abbr(away),
                "visitor_wins": 0,
                "visitor_losses": 0,
                "visitor_score_total": 0,
                "game_status": status,
                "current_period": "",
                "time_remaining": "",
                "is_final": is_final,
                # Cricket-specific fields preserved from the legacy collector
                "cricket_status": cricket_status,
                "cricket_venue": raw.get("strVenue") or "",
                "cricket_start_time": self._format_match_times(dt),
                "cricket_home_nr": 0,
                "cricket_away_nr": 0,
                "cricket_home_score": home_score_str if has_score else "",
                "cricket_away_score": away_score_str if has_score else "",
                "cricket_winner": winner_abbr,
                # Optional enrichment fields (frontend can ignore)
                "venue": raw.get("strVenue") or "",
                "home_team_badge": raw.get("strHomeTeamBadge") or "",
                "visitor_team_badge": raw.get("strAwayTeamBadge") or "",
            }
        except Exception as e:
            logger.error("CricketTheSportsDB parse error: %s", e)
            return None

    @staticmethod
    def _is_playoff_round(raw: Dict[str, Any]) -> bool:
        # IPL/MLC final-stage matches use intRound 150-200 in TheSportsDB
        # (regular league matches use 1-100). Conservative threshold.
        try:
            return int(raw.get("intRound") or 0) >= 150
        except Exception:
            return False

    def _format_match_times(self, dt) -> Dict[str, str]:
        if not dt:
            return {"local": "TBD", "pt": "TBD", "utc": "TBD", "ist": "TBD"}
        try:
            pt = dt.astimezone(pytz.timezone("US/Pacific"))
            local = dt.astimezone(self.timezone)
            utc = dt.astimezone(pytz.utc)
            ist = dt.astimezone(pytz.timezone("Asia/Kolkata"))
            return {
                "local": local.strftime("%-I:%M%p %Z"),
                "pt": pt.strftime("%-I:%M%p %Z"),
                "utc": utc.strftime("%H:%M UTC"),
                "ist": ist.strftime("%H:%M IST"),
            }
        except Exception:
            return {"local": "TBD", "pt": "TBD", "utc": "TBD", "ist": "TBD"}

    # ---- standings derived from completed events ---------------------------
    def get_standings(self) -> List[Dict[str, Any]]:
        season = self.current_season()
        try:
            events = self._season_events(season)
        except Exception as e:
            logger.error("%s: standings: cannot fetch season: %s", self.SPORTSPUFF_CODE, e)
            return []

        records: Dict[str, Dict[str, Any]] = defaultdict(
            lambda: {"matches": 0, "wins": 0, "losses": 0, "no_result": 0,
                     "runs_for": 0, "runs_against": 0}
        )

        for raw in events:
            if (raw.get("strStatus") or "").upper() != "FT":
                continue
            home = self._canonical(raw.get("strHomeTeam") or "")
            away = self._canonical(raw.get("strAwayTeam") or "")
            hs = self._parse_int(raw.get("intHomeScore"), default=-1)
            as_ = self._parse_int(raw.get("intAwayScore"), default=-1)
            if not home or not away or hs < 0 or as_ < 0:
                continue
            r_h = records[home]; r_a = records[away]
            r_h["matches"] += 1; r_a["matches"] += 1
            r_h["runs_for"] += hs; r_h["runs_against"] += as_
            r_a["runs_for"] += as_; r_a["runs_against"] += hs
            if hs == as_:
                r_h["no_result"] += 1; r_a["no_result"] += 1
            elif hs > as_:
                r_h["wins"] += 1; r_a["losses"] += 1
            else:
                r_a["wins"] += 1; r_h["losses"] += 1

        ordered = []
        for team_name, rec in records.items():
            points = rec["wins"] * 2 + rec["no_result"]
            nrr_value = (rec["runs_for"] - rec["runs_against"]) / max(rec["matches"], 1)
            ordered.append({
                "team_name": team_name,
                "abbreviation": self._abbr(team_name),
                "matches": rec["matches"],
                "wins": rec["wins"],
                "losses": rec["losses"],
                "no_result": rec["no_result"],
                "points": points,
                "nrr": f"{nrr_value:+.3f}",
                "nrr_value": nrr_value,
                "record": f"{rec['wins']}-{rec['losses']}-{rec['no_result']}",
            })
        ordered.sort(key=lambda r: (-r["points"], -r["nrr_value"]))
        for rank, rec in enumerate(ordered, 1):
            rec["rank"] = rank
        return ordered

    # ---- public collector overrides: enrich live games with CricAPI -------
    def get_live_scores(self, target_date=None) -> List[Dict[str, Any]]:
        games = super().get_live_scores(target_date)
        return self._enrich_live(games, target_date)

    def get_schedule(self, target_date=None) -> List[Dict[str, Any]]:
        games = super().get_schedule(target_date)
        return self._enrich_live(games, target_date)

    def _enrich_live(self, games, target_date):
        if not games:
            return games
        try:
            from ..services.cricket_live_enricher import enrich_with_cricapi_live
            return enrich_with_cricapi_live(self.SPORTSPUFF_CODE, games, target_date)
        except Exception as e:
            logger.debug("cricket live enrich skipped: %s", e)
            return games

    # ---- season feed for /api/v1/cricket/{league}/season -------------------
    def get_season(self) -> Dict[str, Any]:
        """Bulk feed CricketPuff consumes. Mirrors the cricket.py shape but
        sourced from TheSportsDB events."""
        season = self.current_season()
        try:
            events = self._season_events(season)
            live = True
        except Exception as e:
            logger.error("%s: season feed: %s", self.SPORTSPUFF_CODE, e)
            events = []
            live = False

        matches = [m for m in (self._parse_event(e) for e in events) if m]
        matches.sort(key=lambda m: m.get("game_date", "") or "")

        # Enrich any in-progress matches with CricAPI's per-inning detail
        # (overs, wickets, formatted score). No-op when disabled or when no
        # match is currently in progress.
        matches = self._enrich_live(matches, datetime.now(timezone.utc).date())

        standings = self.get_standings()

        # Find series id (TheSportsDB doesn't have a series concept; use league id).
        return {
            "league": self.SPORTSPUFF_CODE,
            "series_id": str(self.LEAGUE_ID),
            "series_name": self.config.get("name_match", "").title() or self.SPORTSPUFF_CODE,
            "live": live,
            "matches": matches,
            "standings": standings,
            "api_stats": {
                "hits_today": 0,
                "hits_used": 0,
                "hits_limit": 0,
                "date": datetime.now(timezone.utc).date().isoformat(),
                "provider": "thesportsdb",
            },
        }


def IPLTheSportsDBCollector() -> CricketTheSportsDBCollector:
    return CricketTheSportsDBCollector("IPL")


def MLCTheSportsDBCollector() -> CricketTheSportsDBCollector:
    return CricketTheSportsDBCollector("MLC")
