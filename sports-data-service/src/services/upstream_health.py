"""
Upstream health bookkeeping. Collectors and route handlers call
record_success / record_failure at the HTTP boundary; the status service
reads snapshot() to build the upstreams[] view without re-probing.

Process-local. Multi-worker deployments will see per-worker state — that's
acceptable for a status surface; first request to each worker repopulates.
"""

from contextlib import contextmanager
from datetime import datetime, timezone
from threading import Lock
from typing import Any, Dict, Iterator, Optional

# Default freshness budget per upstream (seconds). Anything older than this
# without a fresh success is "stale".
UPSTREAM_TTLS: Dict[str, int] = {
    "ESPN": 900,
    "NHL": 900,
    "NBA stats": 900,
    "MLB stats": 900,
    "Tank01 NFL": 1800,
    "RapidAPI WNBA": 1800,
    "CricAPI": 3600,
    "TheSportsDB": 1800,
}

# Endpoint freshness budgets used to populate results[].meta.ttl_seconds.
# Match the collector-layer cache (5 min) so a row that's actually fresh in
# the cache doesn't get synth-flagged as stale just because we computed
# meta.cached_at from upstream.last_success_at.
ENDPOINT_TTLS: Dict[str, int] = {
    "standings": 1800,
    "season-info": 86400,
    "scores": 300,
    "schedule": 600,
}

# (league, endpoint_kind) -> upstream name. Endpoint kinds are the four
# we surface in the status results table.
SPORT_UPSTREAM_MAP: Dict[tuple, str] = {
    ("mlb", "standings"): "ESPN",
    ("mlb", "season-info"): "MLB stats",
    ("mlb", "scores"): "MLB stats",
    ("mlb", "schedule"): "MLB stats",
    ("nba", "standings"): "ESPN",
    ("nba", "season-info"): "NBA stats",
    ("nba", "scores"): "TheSportsDB",
    ("nba", "schedule"): "TheSportsDB",
    ("nfl", "standings"): "ESPN",
    ("nfl", "season-info"): "Tank01 NFL",
    ("nfl", "scores"): "Tank01 NFL",
    ("nfl", "schedule"): "Tank01 NFL",
    ("nhl", "standings"): "NHL",
    ("nhl", "season-info"): "NHL",
    ("nhl", "scores"): "NHL",
    ("nhl", "schedule"): "NHL",
    ("mls", "standings"): "ESPN",
    ("mls", "season-info"): "ESPN",
    ("mls", "scores"): "ESPN",
    ("mls", "schedule"): "ESPN",
    ("wnba", "standings"): "ESPN",
    ("wnba", "season-info"): "RapidAPI WNBA",
    ("wnba", "scores"): "RapidAPI WNBA",
    ("wnba", "schedule"): "RapidAPI WNBA",
    ("ipl", "standings"): "CricAPI",
    ("ipl", "season-info"): "CricAPI",
    ("ipl", "scores"): "CricAPI",
    ("ipl", "schedule"): "CricAPI",
    ("mlc", "standings"): "CricAPI",
    ("mlc", "season-info"): "CricAPI",
    ("mlc", "scores"): "CricAPI",
    ("mlc", "schedule"): "CricAPI",
    ("wc", "standings"): "TheSportsDB",
    ("wc", "season-info"): "TheSportsDB",
    ("wc", "scores"): "TheSportsDB",
    ("wc", "schedule"): "TheSportsDB",
    ("atp", "standings"): "TheSportsDB",
    ("atp", "season-info"): "TheSportsDB",
    ("atp", "scores"): "TheSportsDB",
    ("atp", "schedule"): "TheSportsDB",
    ("wta", "standings"): "TheSportsDB",
    ("wta", "season-info"): "TheSportsDB",
    ("wta", "scores"): "TheSportsDB",
    ("wta", "schedule"): "TheSportsDB",
}


_state: Dict[str, Dict[str, Any]] = {}
_lock = Lock()


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso_z(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    s = dt.astimezone(timezone.utc).isoformat()
    return s.replace("+00:00", "Z")


def record_success(upstream: str) -> None:
    if not upstream:
        return
    with _lock:
        s = _state.setdefault(upstream, {})
        s["last_success_at"] = _now()


def record_failure(upstream: str, error: str) -> None:
    if not upstream:
        return
    with _lock:
        s = _state.setdefault(upstream, {})
        s["last_error_at"] = _now()
        s["last_error"] = (error or "")[:200]


@contextmanager
def track(upstream: str) -> Iterator[None]:
    """Run a block; record success/failure on exit."""
    try:
        yield
    except Exception as e:
        record_failure(upstream, f"{type(e).__name__}: {e}")
        raise
    else:
        record_success(upstream)


def snapshot() -> Dict[str, Dict[str, Any]]:
    with _lock:
        return {k: dict(v) for k, v in _state.items()}


def upstream_row(name: str, *, ttl: Optional[int] = None,
                 detail_override: Optional[str] = None) -> Dict[str, Any]:
    """Build a contract-shaped upstreams[] row from current bookkeeping."""
    if ttl is None:
        ttl = UPSTREAM_TTLS.get(name, 900)
    s = _state.get(name, {})
    last_ok = s.get("last_success_at")
    last_err = s.get("last_error_at")
    last_err_msg = s.get("last_error")

    age = int((_now() - last_ok).total_seconds()) if last_ok else None
    stale = age is None or age > ttl

    if last_ok is None and last_err is None:
        category = "warning"
        detail = "no requests yet this process"
    elif last_err and (last_ok is None or last_err > last_ok):
        category = "error"
        detail = last_err_msg or "last attempt failed"
    elif stale:
        category = "warning"
        detail = f"no fresh success within {ttl}s (age {age}s)"
    else:
        category = "ok"
        detail = f"fresh ({age}s ago)"

    return {
        "name": name,
        "category": category,
        "last_success_at": _iso_z(last_ok),
        "last_error_at": _iso_z(last_err),
        "last_error": last_err_msg,
        "age_seconds": age,
        "stale": stale,
        "detail": detail_override or detail,
    }


def upstream_for(league: str, endpoint_kind: str) -> Optional[str]:
    return SPORT_UPSTREAM_MAP.get((league.lower(), endpoint_kind))
