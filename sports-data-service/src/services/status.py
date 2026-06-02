"""
Status service: probe upstream APIs and this service's own routes, classify
results, and return a structured snapshot for the /status page and JSON
endpoint.
"""

import os
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests

from ..config import settings
from ..collectors.cricket import _cricapi_usage

logger = logging.getLogger(__name__)

UPSTREAM_PROBES = [
    {
        "name": "ESPN site API",
        "url": "https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard",
        "expect_keys": ("events", "leagues"),
    },
    {
        "name": "NHL api-web",
        "url": "https://api-web.nhle.com/v1/standings/now",
        "expect_keys": ("standings", "wildCardIndicator"),
    },
]

SELF_RESULT_KEYS = ("teams", "standings", "scores", "games", "season_types", "matches")
SELF_LEAGUES = ("mlb", "nba", "nfl", "nhl", "mls", "wnba", "ipl", "mlc")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _classify(count: Optional[int], status_code: Optional[int], detail: str) -> str:
    if status_code is not None and status_code >= 400:
        return "error"
    if count is None:
        return "error" if "error" in detail.lower() or "invalid" in detail.lower() else "warning"
    if count == 0:
        return "warning"
    return "ok"


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


def _probe(name: str, url: str, expect_keys=None, timeout: int = 6) -> Dict[str, Any]:
    started = time.time()
    result: Dict[str, Any] = {
        "name": name,
        "url": url,
        "status_code": None,
        "latency_ms": None,
        "count": None,
        "category": "error",
        "detail": "",
    }
    try:
        resp = requests.get(url, timeout=timeout)
        result["status_code"] = resp.status_code
        result["latency_ms"] = int((time.time() - started) * 1000)
        if resp.status_code >= 400:
            result["detail"] = f"HTTP {resp.status_code}"
            result["category"] = "error"
            return result
        try:
            data = resp.json()
        except ValueError:
            result["detail"] = "Invalid JSON"
            result["category"] = "error"
            return result
        if expect_keys:
            if not isinstance(data, dict) or not any(k in data for k in expect_keys):
                result["detail"] = f"Missing expected key ({', '.join(expect_keys)})"
                result["category"] = "warning"
                return result
        count = _count_results(data)
        result["count"] = count
        result["detail"] = f"{count} result" + ("" if count == 1 else "s")
        result["category"] = _classify(count, resp.status_code, result["detail"])
    except requests.exceptions.RequestException as e:
        result["latency_ms"] = int((time.time() - started) * 1000)
        result["detail"] = type(e).__name__
        result["category"] = "error"
    return result


def _cricapi_status() -> Dict[str, Any]:
    """Report CricAPI quota and cache state without spending a hit."""
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
    budget_left = max(0, hits_limit - reserve - hits_today)

    age_seconds: Optional[int] = None
    if newest_mtime is not None:
        age_seconds = int(time.time() - newest_mtime)

    if hits_today >= hits_limit - reserve:
        category = "warning"
        detail = f"quota exhausted ({hits_today}/{hits_limit}, reserve {reserve}); serving cache"
    elif cached_files == 0 and hits_today == 0:
        category = "warning"
        detail = "no cached data yet"
    else:
        category = "ok"
        detail = f"{hits_today}/{hits_limit} hits today, {budget_left} left in budget"

    return {
        "name": "CricAPI",
        "url": "https://api.cricapi.com/v1",
        "status_code": None,
        "latency_ms": None,
        "count": cached_files,
        "category": category,
        "detail": detail,
        "extra": {
            "hits_today": hits_today,
            "hits_limit": hits_limit,
            "reserve": reserve,
            "date": _cricapi_usage.get("date"),
            "cache_dir": cache_dir,
            "cached_files": cached_files,
            "newest_cache_age_seconds": age_seconds,
        },
    }


def _default_cache_dir() -> str:
    # Mirror collectors/cricket.py:_DEFAULT_CACHE_DIR computation without
    # importing private state — keeps the status module decoupled.
    here = os.path.abspath(os.path.dirname(__file__))
    service_root = os.path.abspath(os.path.join(here, os.pardir, os.pardir))
    return os.path.join(service_root, "cache", "cricket")


def _self_probes(api_base: str) -> List[Dict[str, str]]:
    """Build the set of own-API probes (one league x endpoint per row)."""
    probes = []
    for lg in SELF_LEAGUES:
        probes.append({"name": f"{lg.upper()} standings", "url": f"{api_base}/api/v1/standings/{lg}"})
        probes.append({"name": f"{lg.upper()} season-info", "url": f"{api_base}/api/v1/season-info/{lg}"})
        probes.append({"name": f"{lg.upper()} schedule (today)", "url": f"{api_base}/api/v1/schedule/{lg}/today"})
        probes.append({"name": f"{lg.upper()} scores (today)", "url": f"{api_base}/api/v1/scores/{lg}/today"})
    return probes


def _summarize(results: List[Dict[str, Any]]) -> Dict[str, int]:
    summary = {"ok": 0, "warning": 0, "error": 0}
    for r in results:
        cat = r.get("category", "error")
        summary[cat] = summary.get(cat, 0) + 1
    return summary


def _sort_key(r: Dict[str, Any]) -> tuple:
    order = {"error": 0, "warning": 1, "ok": 2}
    return (order.get(r.get("category"), 3), r.get("name", ""))


def get_status(api_base: str) -> Dict[str, Any]:
    """Run upstream + self probes concurrently and return a snapshot."""
    upstream_results: List[Dict[str, Any]] = []
    self_results: List[Dict[str, Any]] = []

    self_probes = _self_probes(api_base)

    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = {}
        for u in UPSTREAM_PROBES:
            futures[ex.submit(_probe, u["name"], u["url"], u.get("expect_keys"))] = "upstream"
        for s in self_probes:
            futures[ex.submit(_probe, s["name"], s["url"])] = "self"
        for fut in as_completed(futures):
            kind = futures[fut]
            try:
                result = fut.result()
            except Exception as e:
                result = {
                    "name": "probe",
                    "url": "",
                    "status_code": None,
                    "latency_ms": None,
                    "count": None,
                    "category": "error",
                    "detail": f"probe crashed: {type(e).__name__}",
                }
            (upstream_results if kind == "upstream" else self_results).append(result)

    upstream_results.append(_cricapi_status())

    upstream_results.sort(key=_sort_key)
    self_results.sort(key=_sort_key)

    return {
        "checked_at": _now_iso(),
        "api_base": api_base,
        "summary": {
            "upstreams": _summarize(upstream_results),
            "endpoints": _summarize(self_results),
        },
        "upstreams": upstream_results,
        "endpoints": self_results,
    }
