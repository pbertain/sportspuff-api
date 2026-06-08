"""
Champion lookup — generic across leagues.

Strategy: query TheSportsDB's bulk-season feed for the most recently completed
season, identify the championship game (highest intRound + latest dateEvent),
declare the higher-scoring side the champion. Cache result on disk.

Per-league config in CHAMPION_CONFIG declares:
- league_id (TheSportsDB)
- last_season_for(now): callable returning the season string for the
  most recently completed season as of `now`.
- abbr_lookup(team): optional fn returning the team's abbreviation.

Sports where this naturally works (team vs team, single championship game):
NBA, NFL, NHL, MLS, MLB (close enough), IPL, MLC.

Sports that need different treatment (individual or stage-based):
Tennis Grand Slams, Tour de France, Golf majors. Out of scope here.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from datetime import date, datetime, timezone
from threading import Lock
from typing import Any, Callable, Dict, Optional, Tuple

import requests

from ..config import settings

logger = logging.getLogger(__name__)


_THESPORTSDB_BASE = "https://www.thesportsdb.com/api/v1/json"
_DEFAULT_CACHE_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "cache",
    "thesportsdb",
)


def _cricket_abbr(league_code: str) -> Callable[[str], str]:
    """Lookup function backed by cricket.LEAGUE_CONFIGS."""
    from ..collectors.cricket import LEAGUE_CONFIGS
    cfg = LEAGUE_CONFIGS.get(league_code, {})
    teams = cfg.get("teams") or {}
    aliases = cfg.get("aliases") or {}
    def lookup(team: str) -> str:
        if not team:
            return ""
        canonical = aliases.get(team, team)
        return teams.get(canonical, canonical[:4].upper())
    return lookup


def _three_letter(team: str) -> str:
    """Generic abbreviator for non-cricket leagues. Picks 3 letters from
    significant words. 'New York Knicks' -> 'NYK', 'Los Angeles Lakers' -> 'LAL',
    'Boston Celtics' -> 'BOS'."""
    if not team:
        return ""
    words = [w for w in re.split(r"\s+", team.strip()) if w]
    if not words:
        return ""
    if len(words) == 1:
        return words[0][:3].upper()
    if len(words) == 2:
        # 'Boston Celtics' -> 'BOS'
        return words[0][:3].upper()
    # 3+ words: take first letter of each up to 3
    return "".join(w[0] for w in words[:3]).upper()


def _last_finished_season_year(now: Optional[datetime] = None) -> int:
    """Calendar year of the most recently completed season, single-year sports."""
    n = now or datetime.now(timezone.utc)
    return n.year if n.month >= 7 else n.year - 1


def _last_finished_split_year(now: Optional[datetime] = None) -> str:
    """For NBA/NHL/MLS-ish sports labeled 'YYYY-YYYY', the most recently
    completed season. NBA 2024-2025 ends ~June; if before ~Aug 1, use the
    season ending in `now.year`; else use the one ending in `now.year + 1` if
    we're past Sept (preseason). Conservative default: ending in `now.year`."""
    n = now or datetime.now(timezone.utc)
    if n.month >= 8:
        return f"{n.year - 1}-{n.year}"
    # Jan-Jul: latest finished is the season ending the previous year.
    return f"{n.year - 1}-{n.year}"  # ESPN convention "YYYY-YYYY"


def _last_finished_nfl_year(now: Optional[datetime] = None) -> int:
    """NFL season (e.g. 2024) ends ~Feb of next calendar year. Latest
    finished as of `now`: if month >= 3, n.year - 1 (last year's
    Super Bowl was just played). If Feb, also n.year - 1. If Jan and
    playoffs ongoing: still last year's season. Practical: always
    n.year - 1 unless we're in Aug+ which means current season just
    started and last finished is n.year - 1 still (since last played
    Super Bowl in Feb of n.year for season labeled n.year - 1)."""
    n = now or datetime.now(timezone.utc)
    return n.year - 1


CHAMPION_CONFIG: Dict[str, Dict[str, Any]] = {
    "IPL": {
        "league_id": 4460,
        "season_fn": lambda now=None: str(_last_finished_season_year(now)),
        "abbr_fn":   _cricket_abbr("IPL"),
    },
    "MLC": {
        "league_id": 5401,
        "season_fn": lambda now=None: str(_last_finished_season_year(now)),
        "abbr_fn":   _cricket_abbr("MLC"),
    },
    "NBA": {
        "league_id": 4387,
        "season_fn": _last_finished_split_year,
        "abbr_fn":   _three_letter,
    },
    "NHL": {
        "league_id": 4380,
        "season_fn": _last_finished_split_year,
        "abbr_fn":   _three_letter,
    },
    "MLS": {
        "league_id": 4346,
        "season_fn": lambda now=None: str(_last_finished_season_year(now)),
        "abbr_fn":   _three_letter,
    },
    "NFL": {
        "league_id": 4391,
        "season_fn": lambda now=None: str(_last_finished_nfl_year(now)),
        "abbr_fn":   _three_letter,
    },
    "MLB": {
        "league_id": 4424,
        "season_fn": lambda now=None: str(_last_finished_season_year(now)),
        "abbr_fn":   _three_letter,
    },
    "WNBA": {
        "league_id": 4516,
        "season_fn": lambda now=None: str(_last_finished_season_year(now)),
        "abbr_fn":   _three_letter,
    },
}


_memory_cache: Dict[str, Dict[str, Any]] = {}
_memory_lock = Lock()
_MEMORY_TTL = 3600  # 1h


def _cache_dir() -> str:
    return settings.thesportsdb_cache_dir or _DEFAULT_CACHE_DIR


def _disk_path(slug: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9._-]", "_", slug)
    return os.path.join(_cache_dir(), f"{safe}.json")


def _read_disk(slug: str, ttl_seconds: Optional[float] = None) -> Optional[Any]:
    path = _disk_path(slug)
    try:
        if not os.path.exists(path):
            return None
        if ttl_seconds is not None and time.time() - os.path.getmtime(path) > ttl_seconds:
            return None
        with open(path) as f:
            return json.load(f)
    except Exception:
        return None


def _write_disk(slug: str, data: Any) -> None:
    path = _disk_path(slug)
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            json.dump(data, f)
    except Exception as e:
        logger.debug("Could not persist champion cache %s: %s", slug, e)


def _fetch_season_events(league_id: int, season: str) -> Optional[list]:
    """Hit TheSportsDB eventsseason.php; reuse the on-disk season cache that
    the cricket/NBA collectors already populate, falling back to a fresh
    fetch if not available."""
    slug = f"season_{league_id}_{season}"
    disk = _read_disk(slug, ttl_seconds=86400)
    if disk is not None:
        return disk

    key = (settings.thesportsdb_key or "").strip()
    if not key:
        return None
    url = f"{_THESPORTSDB_BASE}/{key}/eventsseason.php?id={league_id}&s={season}"
    try:
        resp = requests.get(url, timeout=20, headers={"User-Agent": "sportspuff-api/1.0"})
        resp.raise_for_status()
        events = (resp.json() or {}).get("events") or []
        _write_disk(slug, events)
        return events
    except Exception as e:
        logger.warning("champion lookup: fetch %s s=%s failed: %s", league_id, season, e)
        return None


def _next_season(season: str) -> Optional[str]:
    """One season newer than `season`. Handles 'YYYY' and 'YYYY-YYYY'."""
    if season.isdigit():
        return str(int(season) + 1)
    m = re.match(r"^(\d{4})-(\d{4})$", season)
    if m:
        a, b = int(m.group(1)), int(m.group(2))
        return f"{a + 1}-{b + 1}"
    return None


# Statuses TheSportsDB uses for events that have reached a terminal state.
# A season with no events outside this set is considered concluded.
_TERMINAL_STATUSES = {"FT", "POSTPONED", "POSTP", "CANCELLED", "CANCELED", "ABANDONED"}


def _is_season_concluded(events: list) -> bool:
    """True if every event has a terminal status (no scheduled or in-progress
    games left). Empty seasons are not considered concluded."""
    if not events:
        return False
    for e in events:
        status = (e.get("strStatus") or "").upper().strip()
        if status not in _TERMINAL_STATUSES:
            return False
    return True


def _identify_champion(events: list) -> Optional[Dict[str, Any]]:
    """Pick the championship game and the winner.

    Strategy: latest finished game wins. intRound semantics are inconsistent
    across TheSportsDB leagues (e.g. NBA uses 500 for regular season and
    lower numbers for playoffs; IPL uses 200 for the final and lower for
    league matches), so we don't filter on it. The single latest dated FT
    game in a season is the championship for every league we currently
    care about.
    """
    finished = [
        e for e in events
        if (e.get("strStatus") or "").upper() == "FT"
        and (e.get("dateEvent") or "")
    ]
    if not finished:
        return None

    finished.sort(key=lambda e: (e.get("dateEvent") or ""), reverse=True)
    final = finished[0]

    home = final.get("strHomeTeam") or ""
    away = final.get("strAwayTeam") or ""
    try:
        home_score = int(final.get("intHomeScore") or 0)
        away_score = int(final.get("intAwayScore") or 0)
    except Exception:
        return None
    if home_score == away_score:
        return None  # Tie — can't declare a champion
    winner_name = home if home_score > away_score else away
    if not winner_name:
        return None
    return {
        "team": winner_name,
        "home_team": home,
        "away_team": away,
        "home_score": home_score,
        "away_score": away_score,
        "date": final.get("dateEvent"),
        "round": int(final.get("intRound") or 0),
    }


def get_last_champion(league_code: str, now: Optional[datetime] = None) -> Optional[Dict[str, Any]]:
    """Return {team, abbreviation, year} for the most recently completed season's
    championship, or None if it can't be determined for this league.

    Tries the season one newer than the heuristic's pick first (covers leagues
    that wrap before the heuristic's calendar cutoff, e.g. IPL ending in late
    May). A candidate season is only accepted if it has events AND every event
    is in a terminal state — otherwise we'd misread an in-progress regular
    season as a championship and pick the latest FT regular-season game as
    the champion.

    Cached per (league, season) for 1h in memory and 24h on disk.
    """
    cfg = CHAMPION_CONFIG.get(league_code.upper())
    if not cfg:
        return None
    primary = cfg["season_fn"](now)
    nxt = _next_season(primary)
    candidates = [s for s in (nxt, primary) if s]
    abbr_fn = cfg["abbr_fn"]
    now_ts = time.time()

    for season in candidates:
        cache_key = f"{league_code}:{season}"
        with _memory_lock:
            mem = _memory_cache.get(cache_key)
            if mem and now_ts - mem["ts"] < _MEMORY_TTL:
                if mem["data"]:
                    return mem["data"]
                continue

        events = _fetch_season_events(cfg["league_id"], season)
        if not events or not _is_season_concluded(events):
            with _memory_lock:
                _memory_cache[cache_key] = {"data": None, "ts": now_ts}
            continue

        info = _identify_champion(events)
        if not info:
            with _memory_lock:
                _memory_cache[cache_key] = {"data": None, "ts": now_ts}
            continue

        year_field: Any = int(season) if season.isdigit() else season
        out = {
            "team": info["team"],
            "abbreviation": abbr_fn(info["team"]),
            "year": year_field,
        }
        with _memory_lock:
            _memory_cache[cache_key] = {"data": out, "ts": now_ts}
        return out

    return None
