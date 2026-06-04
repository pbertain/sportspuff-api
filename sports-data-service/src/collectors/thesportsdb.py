"""
TheSportsDB collector — generic foundation for all leagues we cut over.

Architecture:
- One bulk-season fetch via eventsseason.php returns the entire season's events
  (schedule + finished games with scores) in a single response.
- Disk-cached so we serve last-good if TheSportsDB is unreachable.
- Status hooks via upstream_health record success/failure on every call.
- Hourly circuit breaker via api_tracker (record_request pattern) so a
  runaway loop trips an alarm in minutes, not hours.

Each league subclass declares:
- LEAGUE_ID (TheSportsDB integer ID)
- SEASON_FORMAT (callable returning the current season string for this league)
- SPORTSPUFF_CODE (our internal league code, e.g. 'NBA')
- _parse_event(raw): converts a TheSportsDB event dict to our standard
  game shape (matches existing nba.py / nfl.py / etc. output).

The base class handles the bulk fetch, caching, in-flight coalescing
(via api.py:_get_cached_games called from the route handler), and parsing
loop. Subclasses just supply per-league configuration and parsing.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from datetime import datetime, date, timezone
from threading import Lock
from typing import Any, Dict, List, Optional

import requests

from .base import BaseCollector
from ..config import settings

logger = logging.getLogger(__name__)


THESPORTSDB_BASE = "https://www.thesportsdb.com/api/v1/json"
_DEFAULT_CACHE_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "cache",
    "thesportsdb",
)


class TheSportsDBUnreachable(Exception):
    """Raised when TheSportsDB returns an error and no disk cache is available."""


# Process-shared state across all league subclasses.
_season_memory_cache: Dict[str, Dict[str, Any]] = {}
_season_memory_lock = Lock()
_recent_calls: "list[float]" = []  # sliding-window timestamps for hourly cap
_recent_calls_lock = Lock()


class TheSportsDBCollector(BaseCollector):
    """Generic TheSportsDB-backed collector."""

    LEAGUE_ID: int = 0
    SPORTSPUFF_CODE: str = ""

    def __init__(self, sportspuff_code: str):
        super().__init__(sportspuff_code)
        # Subclasses can override LEAGUE_ID; we keep SPORTSPUFF_CODE in sync
        # with the league string on the BaseCollector.
        self.SPORTSPUFF_CODE = sportspuff_code.upper()
        # Per-request timezone, set via set_timezone() by api.py route handlers.
        import pytz
        self.timezone = pytz.timezone("US/Pacific")

    def set_timezone(self, timezone) -> None:
        if timezone is not None:
            self.timezone = timezone

    # --- season conventions ---------------------------------------------------
    def current_season(self) -> str:
        """Return the season string TheSportsDB expects for THIS LEAGUE.

        Override per-league. Defaults to the calendar year.
        """
        return str(datetime.now(timezone.utc).year)

    # --- HTTP / cache ---------------------------------------------------------
    def _key(self) -> str:
        return (settings.thesportsdb_key or "").strip()

    def _enforce_hourly_cap(self) -> None:
        """Trip the hourly circuit breaker before issuing any HTTP call."""
        now_ts = time.time()
        with _recent_calls_lock:
            # prune older than 1h
            cutoff = now_ts - 3600
            while _recent_calls and _recent_calls[0] < cutoff:
                _recent_calls.pop(0)
            if len(_recent_calls) >= settings.thesportsdb_max_requests_per_hour:
                msg = (
                    f"TheSportsDB hourly cap reached "
                    f"({len(_recent_calls)}/{settings.thesportsdb_max_requests_per_hour})"
                )
                try:
                    from ..services.upstream_health import record_failure
                    record_failure("TheSportsDB", f"budget gate: {msg}")
                except Exception:
                    pass
                raise TheSportsDBUnreachable(msg)
            _recent_calls.append(now_ts)

    def _http_get(self, path: str, timeout: int = 30) -> Dict[str, Any]:
        key = self._key()
        if not key:
            raise TheSportsDBUnreachable("THESPORTSDB_KEY not configured")
        self._enforce_hourly_cap()
        url = f"{THESPORTSDB_BASE}/{key}/{path}"
        started = time.time()
        try:
            resp = requests.get(url, timeout=timeout, headers={"User-Agent": "sportspuff-api/1.0"})
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            try:
                from ..services.upstream_health import record_failure
                record_failure("TheSportsDB", f"{type(e).__name__}: {e}")
            except Exception:
                pass
            raise TheSportsDBUnreachable(str(e)) from e
        try:
            from ..services.upstream_health import record_success
            record_success("TheSportsDB")
        except Exception:
            pass
        return data

    def _cache_dir(self) -> str:
        base = settings.thesportsdb_cache_dir or _DEFAULT_CACHE_DIR
        return base

    def _disk_path(self, slug: str) -> str:
        safe = re.sub(r"[^A-Za-z0-9._-]", "_", slug)
        return os.path.join(self._cache_dir(), f"{safe}.json")

    def _read_disk(self, slug: str, ttl: Optional[float] = None) -> Optional[Any]:
        path = self._disk_path(slug)
        try:
            if not os.path.exists(path):
                return None
            if ttl is not None and time.time() - os.path.getmtime(path) > ttl:
                return None
            with open(path) as f:
                return json.load(f)
        except Exception as e:
            logger.debug("Could not read TheSportsDB disk cache %s: %s", slug, e)
            return None

    def _write_disk(self, slug: str, data: Any) -> None:
        path = self._disk_path(slug)
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w") as f:
                json.dump(data, f)
        except Exception as e:
            logger.debug("Could not write TheSportsDB disk cache %s: %s", slug, e)

    # --- bulk season fetch ----------------------------------------------------
    def _season_events(self, season: str) -> List[Dict[str, Any]]:
        """Fetch (or serve from cache) the full season's events for this league."""
        slug = f"season_{self.LEAGUE_ID}_{season}"
        now_ts = time.time()
        with _season_memory_lock:
            mem = _season_memory_cache.get(slug)
            if mem and now_ts - mem["ts"] < settings.thesportsdb_season_cache_ttl:
                return mem["data"]

        # Try fresh fetch first; fall back to disk on any failure.
        try:
            data = self._http_get(f"eventsseason.php?id={self.LEAGUE_ID}&s={season}")
            events = data.get("events") or []
            with _season_memory_lock:
                _season_memory_cache[slug] = {"data": events, "ts": now_ts}
            self._write_disk(slug, events)
            return events
        except TheSportsDBUnreachable as e:
            stale = self._read_disk(slug)
            if stale is not None:
                logger.warning(
                    "TheSportsDB unreachable for %s season %s; serving stale cache: %s",
                    self.SPORTSPUFF_CODE, season, e,
                )
                return stale
            raise

    # --- parsing helpers ------------------------------------------------------
    @staticmethod
    def _parse_int(v: Any, default: int = 0) -> int:
        if v is None or v == "":
            return default
        try:
            return int(v)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _parse_event_datetime(raw: Dict[str, Any]):
        """Return a timezone-aware UTC datetime from TheSportsDB event fields."""
        ts = raw.get("strTimestamp") or ""
        if ts:
            try:
                # Format: "YYYY-MM-DDTHH:MM:SS"
                dt = datetime.fromisoformat(ts)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except Exception:
                pass
        date_str = raw.get("dateEvent") or ""
        time_str = raw.get("strTime") or "00:00:00"
        if date_str:
            try:
                dt = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M:%S")
                return dt.replace(tzinfo=timezone.utc)
            except Exception:
                return None
        return None

    def _normalize_status(self, raw: Dict[str, Any]) -> str:
        """Map TheSportsDB strStatus to our standard status values."""
        status = (raw.get("strStatus") or "").strip()
        s_lower = status.lower()
        # Treat empty / "NS" (Not Started) / future as scheduled. "FT" = final.
        if status in ("FT", "Match Finished", "AET", "AP") or "final" in s_lower:
            return "final"
        if status in ("HT",) or "live" in s_lower or "in progress" in s_lower:
            return "in_progress"
        return "scheduled"

    # Subclasses MUST override.
    def _parse_event(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        raise NotImplementedError

    # --- public collector API -------------------------------------------------
    def get_schedule(self, target_date: Optional[date] = None) -> List[Dict[str, Any]]:
        return self._games_on(target_date)

    def get_live_scores(self, target_date: Optional[date] = None) -> List[Dict[str, Any]]:
        return self._games_on(target_date)

    def parse_game_data(self, raw_game: Dict[str, Any]) -> Dict[str, Any]:
        return self._parse_event(raw_game)

    def get_season_schedule(self, season: Optional[str] = None) -> List[Dict[str, Any]]:
        season = season or self.current_season()
        events = self._season_events(season)
        parsed = [self._parse_event(e) for e in events]
        return [g for g in parsed if g]

    # --- internals ------------------------------------------------------------
    def _games_on(self, target_date: Optional[date]) -> List[Dict[str, Any]]:
        """Filter the season feed down to games on `target_date` (in the
        league's standard timezone — typically America/Los_Angeles for US
        leagues; subclasses can override `_local_date`)."""
        target = target_date or datetime.now(timezone.utc).date()
        season = self.current_season()
        try:
            events = self._season_events(season)
        except TheSportsDBUnreachable as e:
            logger.error("%s: TheSportsDB unreachable and no cache: %s", self.SPORTSPUFF_CODE, e)
            return []
        out = []
        for raw in events:
            local = self._local_date(raw)
            if local != target:
                continue
            parsed = self._parse_event(raw)
            if parsed:
                out.append(parsed)
        return out

    def _local_date(self, raw: Dict[str, Any]) -> Optional[date]:
        """Date the event happens in this league's display timezone."""
        dt = self._parse_event_datetime(raw)
        if not dt:
            return None
        try:
            import pytz
            tz = getattr(self, "timezone", None) or pytz.timezone("US/Pacific")
            return dt.astimezone(tz).date()
        except Exception:
            return dt.date()
