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


def parse_tennis_strevent(strevent: str, player_set: Optional[set] = None) -> Dict[str, str]:
    """Return {tournament, home_player, visitor_player, raw} parsed from
    a TheSportsDB tennis event name. Best-effort; missing fields are ''.

    `player_set` is a collection of known player names (typically the right
    side of every "X vs Y" event in a season — those are unambiguous). When
    KNOWN_TOURNAMENTS doesn't match, we look for the longest player name in
    the set that's a suffix of the left side; the remainder becomes the
    tournament. This rescues multi-word surnames like "Davidovich Fokina"
    or "Dada Mascoll" that a structural heuristic can't separate.
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

    if not tournament and player_set:
        # Find the longest player from the set that's a suffix of `left`.
        # Match either left == player or left endswith " <player>" so we
        # don't grab a substring inside another word (e.g. "Sinner" should
        # not match "Sinner-Open").
        best = ""
        for p in player_set:
            if not p:
                continue
            if left == p or left.endswith(" " + p):
                if len(p) > len(best):
                    best = p
        if best:
            home_player = best
            tournament = left[:-len(best)].strip()

    if not tournament:
        # Last-resort fallback: peel the final word off `left` as the home
        # player's surname; everything before becomes the tournament name.
        # Matches v6's existing client-side behavior. Loses information on
        # multi-word surnames (e.g. "Davidovich Fokina" → "Fokina") that
        # weren't rescued by KNOWN_TOURNAMENTS or player_set above — those
        # need either a curated list entry or the player to appear as
        # visitor somewhere else in the season.
        words = left.split()
        if len(words) >= 2:
            home_player = words[-1]
            tournament = " ".join(words[:-1]).strip()
        else:
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
        self._player_set_cache: Dict[str, set] = {}

    def current_season(self) -> str:
        return str(datetime.now(timezone.utc).year)

    def _player_set_for_season(self, season: str) -> set:
        """Build a set of known player names from the right side of every
        'X vs Y' strEvent in the season. The right side is unambiguous (no
        tournament prefix), so this gives us a reliable lookup table for
        disambiguating multi-word surnames on the home side.

        Cached per-instance per-season. Underlying _season_events is itself
        memory+disk cached, so worst-case cost is one upstream fetch the
        first time a season is parsed in this process."""
        if season in self._player_set_cache:
            return self._player_set_cache[season]
        try:
            events = self._season_events(season) or []
        except Exception as e:
            logger.warning("Tennis player-set fetch failed for %s %s: %s",
                           self.SPORTSPUFF_CODE, season, e)
            events = []
        players: set = set()
        for raw in events:
            strevent = raw.get("strEvent") or ""
            parts = _VS_RE.split(strevent, maxsplit=1)
            if len(parts) == 2:
                visitor = parts[1].strip()
                if visitor:
                    players.add(visitor)
        self._player_set_cache[season] = players
        return players

    def _parse_event(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            strevent = raw.get("strEvent") or ""
            season = raw.get("strSeason") or self.current_season()
            player_set = self._player_set_for_season(season)
            parsed = parse_tennis_strevent(strevent, player_set)
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
        player_set = self._player_set_for_season(season)

        # Bucket events by tournament + date range.
        by_t: Dict[str, Dict[str, Any]] = {}
        for raw in events:
            parsed = parse_tennis_strevent(raw.get("strEvent") or "", player_set)
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
