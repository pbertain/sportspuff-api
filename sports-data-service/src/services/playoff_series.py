"""
Playoff series record enrichment via ESPN's scoreboard.

For NBA/WNBA (and future basketball/baseball/hockey) playoff games, ESPN's
event payload includes a `competitions[0].series` block with:
  - type: "playoff"
  - summary: "NY leads series 1-0"
  - completed: bool
  - totalCompetitions: int (e.g. 7 for best-of-7)
  - competitors: [{id, wins, ...}]

We don't migrate scores to ESPN — TheSportsDB / RapidAPI / etc. remain the
source of truth for game data. We just hit ESPN once per (sport, date) and
attach the playoff-series block to matching games. Regular-season games
have no `series` field and get nothing added.

Match key is normalized team display name; ESPN team IDs don't line up
with TheSportsDB's idTeam values.
"""

from __future__ import annotations

import logging
import time
from datetime import date as _date
from threading import Lock
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)


# Sport key (matches the URL slug in our routes) -> ESPN scoreboard subpath.
_ESPN_SUBPATH: Dict[str, str] = {
    "nba": "basketball/nba",
    "wnba": "basketball/wnba",
    # Future: "mlb": "baseball/mlb", "nhl": "hockey/nhl"
}


# Per-(sport, date) cache so multiple requests for the same date during a
# burst share one ESPN fetch. Short TTL — ESPN updates series records
# within seconds of game finishes.
_cache: Dict[str, Dict[str, Any]] = {}
_cache_lock = Lock()
_CACHE_TTL_SECONDS = 60


def _normalize(name: Optional[str]) -> str:
    return (name or "").strip().lower()


def _fetch_espn_series_map(sport: str, target_date: _date) -> Optional[Dict[str, Dict[str, Any]]]:
    """Return {normalized_team_name: series_payload} for the requested sport+date,
    or None if ESPN isn't supported / fails."""
    subpath = _ESPN_SUBPATH.get(sport.lower())
    if not subpath:
        return None

    cache_key = f"{sport.lower()}:{target_date.isoformat()}"
    now_ts = time.time()
    with _cache_lock:
        entry = _cache.get(cache_key)
        if entry and now_ts - entry["ts"] < _CACHE_TTL_SECONDS:
            return entry["data"]

    date_str = target_date.strftime("%Y%m%d")
    url = (
        f"https://site.api.espn.com/apis/site/v2/sports/{subpath}/scoreboard"
        f"?dates={date_str}"
    )
    try:
        resp = requests.get(
            url, timeout=8, headers={"User-Agent": "sportspuff-api/1.0"}
        )
        if resp.status_code != 200:
            logger.debug("ESPN series enrich %s: HTTP %s", cache_key, resp.status_code)
            return None
        data = resp.json()
    except Exception as e:
        logger.debug("ESPN series enrich %s failed: %s", cache_key, e)
        return None

    by_team: Dict[str, Dict[str, Any]] = {}
    for ev in data.get("events") or []:
        comps = ev.get("competitions") or []
        if not comps:
            continue
        comp = comps[0]
        series = comp.get("series")
        if not series or series.get("type") != "playoff":
            continue

        notes = comp.get("notes") or []
        round_headline = ""
        if notes and isinstance(notes, list) and isinstance(notes[0], dict):
            round_headline = notes[0].get("headline") or ""

        summary = series.get("summary") or ""
        completed = bool(series.get("completed", False))
        total = series.get("totalCompetitions")

        # Series-side wins by team id.
        wins_by_id: Dict[str, int] = {}
        for sc in series.get("competitors") or []:
            wins_by_id[str(sc.get("id"))] = int(sc.get("wins") or 0)

        for c in comp.get("competitors") or []:
            tid = str(c.get("id"))
            tname = (c.get("team") or {}).get("displayName") or ""
            if not tname:
                continue
            my_wins = wins_by_id.get(tid, 0)
            opponent_wins = sum(v for k, v in wins_by_id.items() if k != tid)
            by_team[_normalize(tname)] = {
                "series_wins": my_wins,
                "series_losses": opponent_wins,
                "series_summary": summary,
                "series_round": round_headline,
                "series_total": total,
                "series_completed": completed,
            }

    with _cache_lock:
        _cache[cache_key] = {"data": by_team, "ts": now_ts}
    return by_team


def enrich_games(
    sport: str,
    target_date: _date,
    games: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Attach playoff series fields to any game whose home/visitor team
    matches a playoff entry in ESPN's scoreboard for that date.

    Mutates each dict in place AND returns the list (for chaining).
    Regular-season games and games with no team-name match are unchanged.
    """
    if not games:
        return games
    by_team = _fetch_espn_series_map(sport, target_date)
    if not by_team:
        return games

    for g in games:
        if not isinstance(g, dict):
            continue
        ht = _normalize(g.get("home_team"))
        vt = _normalize(g.get("visitor_team"))
        h_data = by_team.get(ht)
        v_data = by_team.get(vt)
        if not h_data and not v_data:
            continue

        # Pick whichever side matched first for the shared series-level fields.
        shared = h_data or v_data
        if shared:
            g["is_playoff"] = True
            g["series_summary"] = shared["series_summary"]
            g["series_round"] = shared["series_round"]
            g["series_total"] = shared["series_total"]
            g["series_completed"] = shared["series_completed"]

        if h_data:
            g["home_series_wins"] = h_data["series_wins"]
            g["home_series_losses"] = h_data["series_losses"]
        if v_data:
            g["visitor_series_wins"] = v_data["series_wins"]
            g["visitor_series_losses"] = v_data["series_losses"]

    return games
