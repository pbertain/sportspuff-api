"""
Box-score enrichment via ESPN's per-sport scoreboards.

The MLB stats API returns inning data only at the detailed per-game endpoint,
not at the schedule level — so finalized games come back to our route with
empty home_period_scores / visitor_period_scores. TheSportsDB NBA has the
same gap. ESPN's scoreboards expose `linescores` per competitor across all
team sports we care about; this enricher fills the period dicts in.

Match key is the team's normalized displayName (matches our home_team /
visitor_team for the leagues we use). Same convention as playoff_series.

Per-league key conventions match what _apply_box_score in api.py expects:
- NBA / WNBA / NFL: q1..q4, then ot, ot2, ...
- NHL: period_1..period_3, then ot, so
- MLB: inning_1..inning_N (extras as inning_10+)
- MLS / WC: h1, h2 (and h3/h4 if extra time appears)
"""

from __future__ import annotations

import logging
import time
from datetime import date as _date
from threading import Lock
from typing import Any, Callable, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)


def _nba_fmt(n: int) -> str:
    if n <= 4:
        return f"q{n}"
    if n == 5:
        return "ot"
    return f"ot{n - 4}"


def _nhl_fmt(n: int) -> str:
    if n <= 3:
        return f"period_{n}"
    if n == 4:
        return "ot"
    if n == 5:
        return "so"
    return f"period_{n}"


def _mlb_fmt(n: int) -> str:
    return f"inning_{n}"


def _soccer_fmt(n: int) -> str:
    return f"h{n}"


_ESPN_BOX_CONFIG: Dict[str, Dict[str, Any]] = {
    "nba":  {"subpath": "basketball/nba",   "fmt": _nba_fmt},
    "wnba": {"subpath": "basketball/wnba",  "fmt": _nba_fmt},
    "nfl":  {"subpath": "football/nfl",     "fmt": _nba_fmt},
    "nhl":  {"subpath": "hockey/nhl",       "fmt": _nhl_fmt},
    "mlb":  {"subpath": "baseball/mlb",     "fmt": _mlb_fmt},
    "mls":  {"subpath": "soccer/usa.1",     "fmt": _soccer_fmt},
    "wc":   {"subpath": "soccer/fifa.world","fmt": _soccer_fmt},
}


_cache: Dict[str, Dict[str, Any]] = {}
_cache_lock = Lock()
_CACHE_TTL_SECONDS = 30


def _normalize(name: Optional[str]) -> str:
    return (name or "").strip().lower()


def _fetch_espn_box(sport: str, target_date: _date) -> Optional[Dict[str, Dict[str, int]]]:
    """Returns {normalized_team_name: {period_key: int_score}} for the given
    sport and date, or None if ESPN isn't supported / fails."""
    cfg = _ESPN_BOX_CONFIG.get(sport.lower())
    if not cfg:
        return None

    cache_key = f"{sport.lower()}:{target_date.isoformat()}"
    now_ts = time.time()
    with _cache_lock:
        entry = _cache.get(cache_key)
        if entry and now_ts - entry["ts"] < _CACHE_TTL_SECONDS:
            return entry["data"]

    date_str = target_date.strftime("%Y%m%d")
    url = (
        f"https://site.api.espn.com/apis/site/v2/sports/{cfg['subpath']}/scoreboard"
        f"?dates={date_str}"
    )
    try:
        resp = requests.get(
            url, timeout=8,
            headers={"User-Agent": "sportspuff-api/1.0"},
        )
        if resp.status_code != 200:
            logger.debug("ESPN box-score %s: HTTP %s", cache_key, resp.status_code)
            return None
        data = resp.json()
    except Exception as e:
        logger.debug("ESPN box-score %s failed: %s", cache_key, e)
        return None

    fmt: Callable[[int], str] = cfg["fmt"]
    by_team: Dict[str, Dict[str, int]] = {}
    for ev in data.get("events") or []:
        for comp in ev.get("competitions") or []:
            for c in comp.get("competitors") or []:
                team = c.get("team") or {}
                tname = team.get("displayName") or ""
                if not tname:
                    continue
                ls_raw = c.get("linescores") or []
                periods: Dict[str, int] = {}
                for i, ls in enumerate(ls_raw):
                    try:
                        v = int(ls.get("value") or 0)
                    except (TypeError, ValueError):
                        v = 0
                    periods[fmt(i + 1)] = v
                if periods:
                    by_team[_normalize(tname)] = periods

    with _cache_lock:
        _cache[cache_key] = {"data": by_team, "ts": now_ts}
    return by_team


def enrich_games(
    sport: str,
    target_date: _date,
    games: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Fill home_period_scores / visitor_period_scores from ESPN when our
    primary collector didn't populate them. Doesn't override non-empty dicts.

    Mutates each game dict in place AND returns the list (for chaining).
    Sports not in _ESPN_BOX_CONFIG are no-ops.
    """
    if sport.lower() not in _ESPN_BOX_CONFIG or not games:
        return games
    by_team = _fetch_espn_box(sport, target_date)
    if not by_team:
        return games

    for g in games:
        if not isinstance(g, dict):
            continue
        ht = _normalize(g.get("home_team"))
        vt = _normalize(g.get("visitor_team"))
        h_periods = by_team.get(ht)
        v_periods = by_team.get(vt)
        if h_periods and not g.get("home_period_scores"):
            g["home_period_scores"] = h_periods
        if v_periods and not g.get("visitor_period_scores"):
            g["visitor_period_scores"] = v_periods
    return games
