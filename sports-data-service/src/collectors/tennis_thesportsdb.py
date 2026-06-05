"""
Tennis ATP/WTA collector backed by TheSportsDB.

Tennis on TheSportsDB has a different shape from team sports:
- No `strHomeTeam` / `strAwayTeam` fields. Both players live inside
  `strEvent`, e.g. "Wimbledon Sinner vs Alcaraz".
- No `intHomeScore` / `intAwayScore`. Match outcomes (set scores) are
  not exposed via this API. We surface the fixture and status only.
- Tournaments span ~7-14 days. `intRound` is per-event but semantics
  vary (round-of-128, round-of-64, ..., final).

We parse `strEvent` into tournament + two player names (best-effort
using a known-tournament prefix list) so v6 can render
"Wimbledon · Sinner vs Alcaraz" without doing the parsing on its side.

Standings don't apply to tennis (no league table — every tournament is
single-elimination). The standings route returns an empty payload with
a hint.
"""

from __future__ import annotations

import logging
import re
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pytz

from .thesportsdb import TheSportsDBCollector

logger = logging.getLogger(__name__)


LEAGUE_IDS: Dict[str, int] = {
    "ATP": 4464,
    "WTA": 4517,
}


# Tournaments TheSportsDB labels with multi-word names. Order matters: longer
# names first so "Australian Open Cup" doesn't lose to "Australian Open".
KNOWN_TOURNAMENTS: List[str] = [
    "Australian Open",
    "Roland Garros",
    "US Open",
    "Olympics Tennis",
    "Davis Cup",
    "United Cup",
    "Hopman Cup",
    "Laver Cup",
    "BNP Paribas Open",
    "Miami Open",
    "Mutua Madrid Open",
    "Mutua Madrid",
    "Internazionali BNL",
    "Monte Carlo Masters",
    "Monte Carlo",
    "Indian Wells Masters",
    "Indian Wells",
    "Halle Open",
    "Berlin Open",
    "Bad Homburg",
    "Brisbane International",
    "Queen's Club",
    "Adelaide International",
    "ASB Classic",
    "ATX Open",
    "Mubadala World Tennis",
    "ABN AMRO",
    "Dubai Tennis",
    "Qatar Open",
    "Qatar Exxon",
    "Rio Open",
    "Stuttgart Open",
    "Madrid Open",
    "Italian Open",
    "Eastbourne International",
    "Wimbledon",
]


_VS_RE = re.compile(r"\s+vs\s+", re.IGNORECASE)


def parse_tennis_strevent(strevent: str) -> Dict[str, str]:
    """Return {tournament, home_player, visitor_player, raw} parsed from
    a TheSportsDB tennis event name. Best-effort; missing fields are ''.
    """
    if not strevent:
        return {"tournament": "", "home_player": "", "visitor_player": "", "raw": ""}
    parts = _VS_RE.split(strevent, maxsplit=1)
    if len(parts) != 2:
        return {"tournament": "", "home_player": "", "visitor_player": "", "raw": strevent}
    left, right = parts[0].strip(), parts[1].strip()

    # Find the longest matching known-tournament prefix.
    tournament = ""
    home_player = left
    for t in KNOWN_TOURNAMENTS:
        if left.startswith(t):
            rest = left[len(t):].strip()
            if rest:  # Tournament + at least one word for home player
                tournament = t
                home_player = rest
                break
    if not tournament:
        # Fall back: heuristic — last 1 or 2 words are the player surname,
        # everything before is the tournament. Multi-word player names
        # (e.g. "Felix Auger Aliassime") aren't separable without a
        # deeper parse, so we punt and put the full left in home_player.
        tournament = ""
        home_player = left

    return {
        "tournament": tournament,
        "home_player": home_player,
        "visitor_player": right,
        "raw": strevent,
    }


class TennisTheSportsDBCollector(TheSportsDBCollector):
    """Generic ATP/WTA collector. Subclass via league_code constructor arg."""

    def __init__(self, league_code: str):
        super().__init__(league_code.upper())
        self.LEAGUE_ID = LEAGUE_IDS[self.SPORTSPUFF_CODE]
        self.timezone = pytz.timezone("US/Pacific")

    def current_season(self) -> str:
        return str(datetime.now(timezone.utc).year)

    def _parse_event(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            strevent = raw.get("strEvent") or ""
            parsed = parse_tennis_strevent(strevent)
            if not parsed["home_player"] or not parsed["visitor_player"]:
                return None

            dt = self._parse_event_datetime(raw)
            game_date = self._local_date(raw) or (dt.date() if dt else datetime.now().date())
            status = self._normalize_status(raw)
            is_final = status == "final"

            return {
                "league": self.SPORTSPUFF_CODE,
                "game_id": str(raw.get("idEvent") or ""),
                "game_date": game_date.strftime("%Y-%m-%d"),
                "game_time": dt,
                "game_type": "match",
                # Map players into the standard home_team/visitor_team fields
                # so existing api.py response builders pass them through.
                "home_team": parsed["home_player"],
                "home_team_abbrev": "",
                "home_team_id": "",
                "home_wins": 0,
                "home_losses": 0,
                "home_score_total": 0,
                "visitor_team": parsed["visitor_player"],
                "visitor_team_abbrev": "",
                "visitor_team_id": "",
                "visitor_wins": 0,
                "visitor_losses": 0,
                "visitor_score_total": 0,
                "game_status": status,
                "current_period": "",
                "time_remaining": "",
                "is_final": is_final,
                "is_overtime": False,
                "home_period_scores": {},
                "visitor_period_scores": {},
                "venue": raw.get("strVenue") or "",
                # Tennis-specific
                "tennis_tournament": parsed["tournament"],
                "tennis_match_label": parsed["raw"],
                "tennis_round": raw.get("intRound") or "",
                "tennis_country": raw.get("strCountry") or "",
                "tennis_video": raw.get("strVideo") or "",
                "league_badge": raw.get("strLeagueBadge") or "",
            }
        except Exception as e:
            logger.error("Tennis parse error: %s", e)
            return None

    # No standings table for tennis. Returns [] with a hint via the status row.
    def get_standings(self) -> List[Dict[str, Any]]:
        return []

    def get_season_info(self, year: int = None) -> Optional[Dict[str, Any]]:
        """Surface the current tournament (if any) as `current_phase` so
        v6 can render "Currently: Wimbledon" banners during slam weeks."""
        season = str(year) if year else self.current_season()
        try:
            events = self._season_events(season)
        except Exception:
            return None
        if not events:
            return None
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        # Bucket events by tournament + date range.
        by_t: Dict[str, Dict[str, Any]] = {}
        for raw in events:
            parsed = parse_tennis_strevent(raw.get("strEvent") or "")
            t = parsed["tournament"]
            d = (raw.get("dateEvent") or "")[:10]
            if not t or not d:
                continue
            slot = by_t.setdefault(t, {"start": d, "end": d, "n": 0})
            slot["n"] += 1
            if d < slot["start"]:
                slot["start"] = d
            if d > slot["end"]:
                slot["end"] = d

        # Find tournament covering today.
        current = "Off Tour"
        for t, slot in by_t.items():
            if slot["start"] <= today <= slot["end"]:
                current = t
                break

        # season_types: list of tournaments (sorted by start) for the year.
        ordered = sorted(by_t.items(), key=lambda kv: kv[1]["start"])
        season_types = [
            {"name": name, "start_date": s["start"], "end_date": s["end"]}
            for name, s in ordered
        ]

        return {
            "year": int(season) if season.isdigit() else season,
            "current_phase": current,
            "season_types": season_types,
        }


def ATPTheSportsDBCollector() -> TennisTheSportsDBCollector:
    return TennisTheSportsDBCollector("ATP")


def WTATheSportsDBCollector() -> TennisTheSportsDBCollector:
    return TennisTheSportsDBCollector("WTA")
