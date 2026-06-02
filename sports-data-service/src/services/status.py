"""
Status snapshot for /api/v1/status, matching the contract documented in
HANDOFF_api-status-stale.md. Cheap to call: probes only this service's own
routes (localhost) for results[]; reads upstream health from in-memory
bookkeeping rather than re-probing external services. Caches the assembled
payload for ~10s.
"""

import os
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from threading import Lock
from typing import Any, Dict, List, Optional

import requests

from ..config import settings
from ..collectors.cricket import _cricapi_usage
from . import upstream_health

logger = logging.getLogger(__name__)

SELF_RESULT_KEYS = ("teams", "standings", "scores", "games", "season_types", "matches")
SELF_LEAGUES = ("mlb", "nba", "nfl", "nhl", "mls", "wnba", "ipl", "mlc")
SELF_ENDPOINT_KINDS = ("standings", "season-info", "schedule", "scores")

_PAYLOAD_TTL = 10  # seconds
_payload_cache: Dict[str, Any] = {"data": None, "ts": 0.0, "key": None}
_payload_lock = Lock()


def _now_iso_z() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _count_results(data: Any) -> int:
    if isinstance(data, list):
        return len(data)
    if isinstance(data, dict):
        for key in SELF_RESULT_KEYS:
            value = data.get(key)
            if isinstance(value, list):
                return len(value)
            if isinstance(value, dict):
                return len(value)
    return 0


def _probe_self(name: str, league: str, kind: str, url: str,
                timeout: int = 6) -> Dict[str, Any]:
    started = time.time()
    upstream = upstream_health.upstream_for(league, kind)
    ttl = upstream_health.ENDPOINT_TTLS.get(kind)
    row: Dict[str, Any] = {
        "name": name,
        "url": url,
        "category": "error",
        "status_code": None,
        "count": None,
        "detail": "",
        "upstream": upstream,
        "meta": _meta_for(upstream, ttl),
    }
    try:
        resp = requests.get(url, timeout=timeout)
        row["status_code"] = resp.status_code
        if resp.status_code >= 400:
            row["detail"] = f"HTTP {resp.status_code}"
            row["category"] = "error"
            return row
        try:
            data = resp.json()
        except ValueError:
            row["detail"] = "Invalid JSON"
            row["category"] = "error"
            return row
        count = _count_results(data)
        row["count"] = count
        if count == 0:
            row["category"] = "warning"
            row["detail"] = "0 results"
        else:
            row["category"] = "ok"
            row["detail"] = f"{count} result" + ("" if count == 1 else "s")
        if row["meta"] and row["meta"].get("stale"):
            row["category"] = "warning"
            row["detail"] = f"{row['detail']} (served from stale cache)"
    except requests.exceptions.RequestException as e:
        row["detail"] = type(e).__name__
        row["category"] = "error"
    return row


def _meta_for(upstream: Optional[str], ttl: Optional[int]) -> Optional[Dict[str, Any]]:
    """Synthesize a per-row meta block from upstream bookkeeping.

    True per-response cached_at lives in the data endpoints themselves and is
    a follow-up; here we approximate cached_at as the upstream's last success
    so the frontend's stale badge has signal to render today.
    """
    if not upstream:
        return None
    snap = upstream_health.snapshot().get(upstream, {})
    last_ok = snap.get("last_success_at")
    last_err = snap.get("last_error_at")
    if last_ok is None and last_err is None:
        return None
    cached_at = last_ok
    if cached_at is None:
        return {
            "cached_at": None,
            "age_seconds": None,
            "ttl_seconds": ttl,
            "stale": True,
            "source": "live",
        }
    age = int((datetime.now(timezone.utc) - cached_at).total_seconds())
    upstream_failed_after = bool(last_err and last_err > cached_at)
    stale = (ttl is not None and age > ttl) or upstream_failed_after
    return {
        "cached_at": cached_at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
        "age_seconds": age,
        "ttl_seconds": ttl,
        "stale": stale,
        "source": "cache" if upstream_failed_after else "live",
    }


def _self_probes(api_base: str) -> List[Dict[str, str]]:
    probes = []
    for lg in SELF_LEAGUES:
        u = lg.upper()
        probes.append({"league": lg, "kind": "standings",
                       "name": f"{u} standings",
                       "url": f"{api_base}/api/v1/standings/{lg}"})
        probes.append({"league": lg, "kind": "season-info",
                       "name": f"{u} season-info",
                       "url": f"{api_base}/api/v1/season-info/{lg}"})
        probes.append({"league": lg, "kind": "schedule",
                       "name": f"{u} schedule (today)",
                       "url": f"{api_base}/api/v1/schedule/{lg}/today"})
        probes.append({"league": lg, "kind": "scores",
                       "name": f"{u} scores (today)",
                       "url": f"{api_base}/api/v1/scores/{lg}/today"})
    return probes


def _summarize(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    out = {"error": 0, "warning": 0, "ok": 0}
    for r in rows:
        cat = r.get("category", "error")
        if cat in out:
            out[cat] += 1
        else:
            out[cat] = 1
    return out


def _sort_key(r: Dict[str, Any]) -> tuple:
    order = {"error": 0, "warning": 1, "ok": 2}
    return (order.get(r.get("category"), 3), r.get("name", ""))


def _cricapi_upstream_row() -> Dict[str, Any]:
    """Override the generic CricAPI row with quota + cache mtime detail."""
    cache_dir = settings.cricapi_cache_dir or _default_cache_dir()
    newest_mtime: Optional[float] = None
    cached_files = 0
    if cache_dir and os.path.isdir(cache_dir):
        for fname in os.listdir(cache_dir):
            if fname == "usage.json" or not fname.endswith(".json"):
                continue
            path = os.path.join(cache_dir, fname)
            try:
                mtime = os.path.getmtime(path)
            except OSError:
                continue
            cached_files += 1
            if newest_mtime is None or mtime > newest_mtime:
                newest_mtime = mtime

    hits_today = _cricapi_usage.get("hits_today") or 0
    hits_limit = _cricapi_usage.get("hits_limit") or settings.cricapi_max_requests_per_day
    reserve = settings.cricapi_usage_reserve

    if hits_today >= max(0, hits_limit - reserve):
        detail = f"quota exhausted ({hits_today}/{hits_limit}, reserve {reserve}); serving cache"
    else:
        budget_left = max(0, hits_limit - reserve - hits_today)
        detail = f"{hits_today}/{hits_limit} hits today, {budget_left} left in budget"
    if cached_files:
        detail += f"; {cached_files} cache files"
    return upstream_health.upstream_row("CricAPI", detail_override=detail)


def _default_cache_dir() -> str:
    here = os.path.abspath(os.path.dirname(__file__))
    service_root = os.path.abspath(os.path.join(here, os.pardir, os.pardir))
    return os.path.join(service_root, "cache", "cricket")


def get_status(api_base: str) -> Dict[str, Any]:
    """Build the contract-shaped status payload (with a small assembly cache)."""
    with _payload_lock:
        cached = _payload_cache
        if (
            cached["data"] is not None
            and cached["key"] == api_base
            and time.time() - cached["ts"] < _PAYLOAD_TTL
        ):
            return cached["data"]

    self_probes = _self_probes(api_base)
    results: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = [
            ex.submit(_probe_self, p["name"], p["league"], p["kind"], p["url"])
            for p in self_probes
        ]
        for fut in as_completed(futures):
            try:
                results.append(fut.result())
            except Exception as e:
                results.append({
                    "name": "probe",
                    "url": "",
                    "category": "error",
                    "status_code": None,
                    "count": None,
                    "detail": f"probe crashed: {type(e).__name__}",
                    "upstream": None,
                    "meta": None,
                })
    results.sort(key=_sort_key)

    upstream_names = sorted({
        upstream_health.upstream_for(p["league"], p["kind"])
        for p in self_probes
        if upstream_health.upstream_for(p["league"], p["kind"])
    })
    upstreams: List[Dict[str, Any]] = []
    for name in upstream_names:
        if name == "CricAPI":
            upstreams.append(_cricapi_upstream_row())
        else:
            upstreams.append(upstream_health.upstream_row(name))
    upstreams.sort(key=_sort_key)

    payload = {
        "api_base_url": api_base,
        "checked_at": _now_iso_z(),
        "summary": _summarize(results),
        "upstreams": upstreams,
        "results": results,
    }

    with _payload_lock:
        _payload_cache["data"] = payload
        _payload_cache["ts"] = time.time()
        _payload_cache["key"] = api_base
    return payload
