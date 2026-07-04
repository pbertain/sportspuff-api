"""
FastAPI application for the Sports Data Service API.

Provides REST API endpoints for schedules, scores, and standings
with both JSON and cURL-style text output.
"""

from datetime import datetime, date, timedelta
from pathlib import Path as FSPath
from typing import Optional, Dict, Any, List, Tuple
from fastapi import FastAPI, Path, Query, HTTPException, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, PlainTextResponse, HTMLResponse
from fastapi.openapi.utils import get_openapi
import pytz
import logging
import concurrent.futures

from .database import get_db_session
from .models import Game
from .config import settings
from .collectors import NBACollector, MLBCollector, NHLCollector, NFLCollector, WNBACollector, CricketCollector, MLSCollector
from .utils import api_tracker
from .services import upstream_health
from . import schemas

import time as _time

logger = logging.getLogger(__name__)

# Cache for collector responses: {cache_key: {'data': [...], 'timestamp': float}}
_collector_cache: Dict[str, Any] = {}
_COLLECTOR_CACHE_TTL = 300  # 5 minutes
_standings_cache: Dict[str, Any] = {}
_STANDINGS_CACHE_TTL = 1800  # 30 minutes
_wc_bracket_cache: Dict[str, Any] = {}
_WC_BRACKET_TTL = 1800  # 30 minutes

# In-flight fetch coalescing: when N concurrent requests miss the same cache
# key, only the first runs the fetcher; the rest wait on its result. Prevents
# thundering-herd fan-out at cache-miss boundaries (especially against shared
# upstreams like CricAPI).
import threading as _threading
_inflight_fetches: Dict[str, "_threading.Event"] = {}
_inflight_lock = _threading.Lock()
_INFLIGHT_WAIT_TIMEOUT = 30  # seconds


def _collector_cache_key(league: str, target_date: date, cache_context: str = "") -> str:
    return f"{league}:{target_date.isoformat()}:{cache_context}"


def _cache_snapshot(timestamp: Optional[float] = None, *, cache_hit: bool = False) -> Dict[str, Any]:
    return {
        "timestamp": timestamp if timestamp is not None else _time.time(),
        "cache_hit": cache_hit,
    }


def _iso_utc_from_timestamp(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp, tz=pytz.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _normalize_iso_timestamp(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        dt = value
        if dt.tzinfo is None:
            dt = pytz.UTC.localize(dt)
        return dt.astimezone(pytz.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    if isinstance(value, (int, float)):
        return _iso_utc_from_timestamp(float(value))
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        for candidate in (raw, raw.replace("Z", "+00:00")):
            try:
                dt = datetime.fromisoformat(candidate)
                if dt.tzinfo is None:
                    dt = pytz.UTC.localize(dt)
                return dt.astimezone(pytz.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
            except Exception:
                continue
    return None


def _extract_source_updated_at(payload: Any) -> Optional[str]:
    keys = (
        "source_updated_at",
        "upstream_updated_at",
        "updated_at",
        "updatedAt",
        "last_updated",
        "lastUpdate",
        "lastModified",
        "dateModified",
    )

    def _scan(value: Any, depth: int = 0) -> List[str]:
        if depth > 3 or value is None:
            return []
        found: List[str] = []
        if isinstance(value, dict):
            for key in keys:
                normalized = _normalize_iso_timestamp(value.get(key))
                if normalized:
                    found.append(normalized)
            for child in value.values():
                if isinstance(child, (dict, list)):
                    found.extend(_scan(child, depth + 1))
        elif isinstance(value, list):
            for child in value:
                if isinstance(child, (dict, list)):
                    found.extend(_scan(child, depth + 1))
        return found

    candidates = _scan(payload)
    return max(candidates) if candidates else None


def _collector_source_updated_at(collector: Any, context: Optional[str] = None) -> Optional[str]:
    getter = getattr(collector, "get_source_updated_at", None)
    if callable(getter):
        try:
            return _normalize_iso_timestamp(getter(context))
        except Exception as exc:
            logger.debug("Could not read collector source_updated_at: %s", exc)
    cache_time = getattr(collector, "_standings_cache_time", None)
    normalized = _normalize_iso_timestamp(cache_time)
    if normalized:
        return normalized
    return None


def _build_endpoint_meta(
    snapshot: Dict[str, Any],
    freshness_seconds: int,
    *,
    source_updated_at: Optional[str] = None,
    empty_state: Optional[str] = None,
) -> Dict[str, Any]:
    timestamp = snapshot.get("timestamp", _time.time())
    cache_age_seconds = max(0, int(_time.time() - timestamp))
    fetched_at = _iso_utc_from_timestamp(timestamp)
    normalized_source_updated_at = _normalize_iso_timestamp(source_updated_at or snapshot.get("source_updated_at"))
    return {
        "as_of": fetched_at,
        "fetched_at": fetched_at,
        "cache_age_seconds": cache_age_seconds,
        "stale": cache_age_seconds > freshness_seconds,
        "empty_state": empty_state,
        "source_updated_at": normalized_source_updated_at,
    }


def _is_real_empty(payload: Any) -> bool:
    """Classify intentional empty payloads using explicit upstream hints first."""
    if isinstance(payload, dict):
        if payload.get("empty_state") == "real_empty":
            return True
        if payload.get("status") in ("off_season", "no_games", "empty"):
            return True
        if payload.get("available") is False:
            return True
        if payload.get("season_types") == [] and payload.get("current_phase") == "Off Season":
            return True
        if payload.get("matches") == [] and payload.get("live") is False:
            return True
    return False


def _is_suspect_empty(payload: Any) -> bool:
    """Return True when a payload is empty in a way we should distrust.

    This catches ambiguous dict-shaped payloads that look "empty" even though
    they did not explicitly say they were an intentional off-season/empty state.
    """
    if payload in (None, [], {}):
        return True
    if isinstance(payload, dict):
        if payload.get("season_types") == []:
            return payload.get("current_phase") != "Off Season"
        if payload.get("matches") == []:
            return payload.get("status") != "off_season"
    return False


def _scores_freshness_seconds(target_date: date, timezone: pytz.BaseTzInfo) -> int:
    today = datetime.now(timezone).date()
    if target_date == today:
        return 120
    if target_date < today:
        return 1800
    return 3600


def _schedule_freshness_seconds(target_date: date, timezone: pytz.BaseTzInfo) -> int:
    today = datetime.now(timezone).date()
    if target_date <= today + timedelta(days=1):
        return 900
    return 21600


def _standings_freshness_seconds(sport_lower: str) -> int:
    if sport_lower == "nfl":
        return 3600
    if sport_lower in ("atp", "wta", "cycling"):
        return 21600
    return 900


def _get_cached_payload(
    cache: Dict[str, Any],
    cache_key: str,
    ttl_seconds: int,
    fetcher,
    *,
    prefer_stale_on_suspect_empty: bool = True,
) -> Tuple[Any, Dict[str, Any]]:
    cached = cache.get(cache_key)
    if cached and (_time.time() - cached["timestamp"] < ttl_seconds):
        return cached["data"], {
            **_cache_snapshot(cached["timestamp"], cache_hit=True),
            "source_updated_at": cached.get("source_updated_at"),
            "empty_state": cached.get("empty_state"),
        }

    data = fetcher()
    timestamp = _time.time()
    source_updated_at = _extract_source_updated_at(data)
    empty_state = "real_empty" if _is_real_empty(data) else ("suspect_empty" if _is_suspect_empty(data) else None)
    if prefer_stale_on_suspect_empty and empty_state == "suspect_empty" and cached and cached.get("data") is not None:
        return cached["data"], {
            **_cache_snapshot(cached["timestamp"], cache_hit=True),
            "source_updated_at": cached.get("source_updated_at"),
            "empty_state": "suspect_empty",
        }
    cache[cache_key] = {
        "data": data,
        "timestamp": timestamp,
        "source_updated_at": source_updated_at,
        "empty_state": empty_state,
    }
    return data, {
        **_cache_snapshot(timestamp, cache_hit=False),
        "source_updated_at": source_updated_at,
        "empty_state": empty_state,
    }


def _get_cached_games(
    league: str,
    target_date,
    fetcher,
    cache_context: str = "",
    *,
    include_metadata: bool = False,
    prefer_stale_on_suspect_empty: bool = True,
):
    """Fetch games with 5-minute caching."""
    # Keep the timezone in the cache key. Cricket collectors filter games by
    # the request timezone's local date, so sharing one cache slot across
    # ?tz= variants can hide valid "today" games for another timezone.
    # Upstream fan-out is already bounded lower in the stack by the
    # collector-level season caches.
    cache_key = _collector_cache_key(league, target_date, cache_context)
    cached = _collector_cache.get(cache_key)
    if cached and (_time.time() - cached['timestamp'] < _COLLECTOR_CACHE_TTL):
        if include_metadata:
            return cached['data'], {
                **_cache_snapshot(cached['timestamp'], cache_hit=True),
                "source_updated_at": cached.get("source_updated_at"),
                "empty_state": cached.get("empty_state"),
            }
        return cached['data']

    # In-flight coalescing: if another request is already fetching this key,
    # wait for it instead of starting a parallel fetch.
    with _inflight_lock:
        existing = _inflight_fetches.get(cache_key)
        if existing is not None:
            event = existing
            we_lead = False
        else:
            event = _threading.Event()
            _inflight_fetches[cache_key] = event
            we_lead = True

    if not we_lead:
        event.wait(timeout=_INFLIGHT_WAIT_TIMEOUT)
        cached = _collector_cache.get(cache_key)
        if cached and (_time.time() - cached['timestamp'] < _COLLECTOR_CACHE_TTL):
            if include_metadata:
                return cached['data'], {
                    **_cache_snapshot(cached['timestamp'], cache_hit=True),
                    "source_updated_at": cached.get("source_updated_at"),
                    "empty_state": cached.get("empty_state"),
                }
            return cached['data']
        # In-flight leader failed or timed out; fall through and try ourselves.

    try:
        try:
            with get_db_session() as db:
                can_fetch = api_tracker.can_make_budgeted_request(league, db)
        except Exception as exc:
            logger.warning("Could not check persistent API budget for %s: %s", league, exc)
            can_fetch = api_tracker.can_make_budgeted_request(league)

        if not can_fetch:
            logger.warning("Skipping %s collector fetch due to API budget/rate limit", league)
            upstream = upstream_health.upstream_for(league, cache_context or "scores")
            if upstream:
                upstream_health.record_failure(upstream, "budget gate: rate/budget limit reached")
            if include_metadata:
                return [], {
                    **_cache_snapshot(),
                    "empty_state": "suspect_empty",
                }
            return []

        started_at = _time.time()
        success = True
        error_message = None
        try:
            result = fetcher()
        except Exception as exc:
            success = False
            error_message = str(exc)
            raise
        finally:
            response_time_ms = int((_time.time() - started_at) * 1000)
            # NFL/WNBA collectors record per-HTTP-call internally; logging again
            # at the method boundary would double-count against their paid quotas.
            skip_method_record = league.upper() in ("NFL", "WNBA")
            if not skip_method_record:
                api_tracker.record_request(league, 'api_collector', success=success, response_time_ms=response_time_ms, error_message=error_message)
            upstream = upstream_health.upstream_for(league, cache_context or "scores")
            if upstream:
                if success:
                    upstream_health.record_success(upstream)
                else:
                    upstream_health.record_failure(upstream, error_message or "fetch failed")
            if not skip_method_record:
                try:
                    with get_db_session() as db:
                        api_tracker.log_to_database(
                            db,
                            league,
                            'api_collector',
                            success=success,
                            response_time_ms=response_time_ms,
                            error_message=error_message,
                        )
                except Exception as exc:
                    logger.warning("Could not persist API usage for %s: %s", league, exc)
        timestamp = _time.time()
        source_updated_at = _extract_source_updated_at(result)
        empty_state = "real_empty" if _is_real_empty(result) else ("suspect_empty" if _is_suspect_empty(result) else None)
        if prefer_stale_on_suspect_empty and empty_state == "suspect_empty" and cached and cached.get("data") is not None:
            if include_metadata:
                return cached['data'], {
                    **_cache_snapshot(cached['timestamp'], cache_hit=True),
                    "source_updated_at": cached.get("source_updated_at"),
                    "empty_state": "suspect_empty",
                }
            return cached['data']
        _collector_cache[cache_key] = {
            'data': result,
            'timestamp': timestamp,
            'source_updated_at': source_updated_at,
            'empty_state': empty_state,
        }
        if include_metadata:
            return result, {
                **_cache_snapshot(timestamp, cache_hit=False),
                "source_updated_at": source_updated_at,
                "empty_state": empty_state,
            }
        return result
    finally:
        if we_lead:
            with _inflight_lock:
                _inflight_fetches.pop(cache_key, None)
            event.set()


def get_collector(league: str):
    """Get collector instance for a league.

    Provider switches:
      - settings.nba_provider:     'thesportsdb' (default) | 'nba_api'
      - settings.cricket_provider: 'thesportsdb' (default) | 'cricapi'

    NBA legacy (nba_api) is unreachable from prod (stats.nba.com timeouts).
    Cricket legacy (CricAPI) shares a 2000/day key with another consumer.
    Both default to TheSportsDB.
    """
    if league == 'NBA' and settings.nba_provider == 'thesportsdb':
        from .collectors.nba_thesportsdb import NBATheSportsDBCollector
        nba = NBATheSportsDBCollector()
    else:
        nba = NBACollector()

    if league in ('IPL', 'MLC') and settings.cricket_provider == 'thesportsdb':
        from .collectors.cricket_thesportsdb import CricketTheSportsDBCollector
        ipl = CricketTheSportsDBCollector('IPL') if league == 'IPL' else None
        mlc = CricketTheSportsDBCollector('MLC') if league == 'MLC' else None
    else:
        ipl = CricketCollector('IPL')
        mlc = CricketCollector('MLC')

    wc = None
    if league == 'WC':
        from .collectors.world_cup_thesportsdb import WorldCupTheSportsDBCollector
        wc = WorldCupTheSportsDBCollector()

    atp = None
    wta = None
    if league == 'ATP':
        from .collectors.tennis_thesportsdb import TennisTheSportsDBCollector
        atp = TennisTheSportsDBCollector('ATP')
    if league == 'WTA':
        from .collectors.tennis_thesportsdb import TennisTheSportsDBCollector
        wta = TennisTheSportsDBCollector('WTA')

    cycling = None
    if league == 'CYCLING':
        from .collectors.cycling_thesportsdb import CyclingTheSportsDBCollector
        cycling = CyclingTheSportsDBCollector()
        from .collectors.cycling_file import CyclingDecoratedCollector, CyclingFileCollector
        cycling_data_dir = (settings.cycling_data_dir or "").strip()
        if not cycling_data_dir:
            cycling_data_dir = str(FSPath(__file__).resolve().parents[1] / "templates")
        overlay = CyclingFileCollector(cycling_data_dir)
        cycling = CyclingDecoratedCollector(cycling, overlay)

    collectors = {
        'NBA': nba,
        'MLB': MLBCollector(),
        'NHL': NHLCollector(),
        'NFL': NFLCollector(),
        'WNBA': WNBACollector(),
        'IPL': ipl if league == 'IPL' else CricketCollector('IPL'),
        'MLC': mlc if league == 'MLC' else CricketCollector('MLC'),
        'MLS': MLSCollector(),
        'WC':  wc,
        'ATP': atp,
        'WTA': wta,
        'CYCLING': cycling,
    }
    return collectors.get(league)

def set_collector_timezone(collector, timezone: pytz.BaseTzInfo) -> None:
    if collector and hasattr(collector, 'set_timezone'):
        collector.set_timezone(timezone)

app = FastAPI(
    title="SportsPuff Sports Data API",
    description=(
        "Scores, schedules, standings, and season info across 12 sports/leagues — "
        "MLB, NBA, NFL, NHL, WNBA, MLS, IPL, MLC, FIFA World Cup, ATP, WTA, and the "
        "UCI World Tour (Tour de France, Giro, classics). Responses available as "
        "JSON or plain text. Canonical routes live under /v1 with Accept-based "
        "content negotiation. Health snapshot at /v1/status."
    ),
    version="1.0.0",
    # Disable the default docs routes so we can serve branded ones below.
    docs_url=None,
    redoc_url=None,
)


@app.on_event("startup")
def _log_runtime_config() -> None:
    logger.info(
        "runtime config: cricket_provider=%s cricket_live_enrichment=%s cricapi_key_present=%s cycling_provider=%s cycling_data_dir_present=%s",
        settings.cricket_provider,
        settings.cricket_live_enrichment,
        bool((settings.cricapi_key or "").strip()),
        settings.cycling_provider,
        bool((settings.cycling_data_dir or "").strip()),
    )


@app.exception_handler(HTTPException)
async def api_http_exception_handler(request: Request, exc: HTTPException):
    route = request.url.path
    if _request_prefers_plain_text_errors(request):
        return _plain_text_error_response(exc.status_code, route, exc.detail)
    return JSONResponse(
        status_code=exc.status_code,
        content=_api_error_payload(exc.status_code, route, exc.detail),
    )


@app.exception_handler(RequestValidationError)
async def api_validation_exception_handler(request: Request, exc: RequestValidationError):
    route = request.url.path
    if _request_prefers_plain_text_errors(request):
        return _plain_text_error_response(422, route, "Validation failed")
    return JSONResponse(
        status_code=422,
        content=_api_error_payload(422, route, "Validation failed", details=exc.errors()),
    )


@app.get("/api/v1/debug/runtime")
def debug_runtime_config():
    return {
        "cricket_provider": settings.cricket_provider,
        "cricket_live_enrichment": settings.cricket_live_enrichment,
        "cricapi_key_present": bool((settings.cricapi_key or "").strip()),
        "cricapi_live_refresh": settings.cricapi_live_refresh,
        "cycling_provider": settings.cycling_provider,
        "cycling_data_dir_present": bool((settings.cycling_data_dir or "").strip()),
    }

# SportsPuff logo. Prefer a locally-bundled file (faster, works offline,
# survives splitsp.lat outages) and fall back to the canonical remote URL.
SPORTSPUFF_LOGO_REMOTE = "https://www.splitsp.lat/logos/sportspuff/sportspuff-logo.png"


def _sportspuff_logo_url() -> str:
    for fname in ("sportspuff-logo.png", "sportspuff-logo.svg"):
        if _os.path.exists(_os.path.join(_static_dir, fname)):
            return f"/static/{fname}"
    return SPORTSPUFF_LOGO_REMOTE


SPORTSPUFF_LOGO_URL = SPORTSPUFF_LOGO_REMOTE  # rebound below after _static_dir is defined

# Serve /static for the branded docs CSS.
import os as _os
from fastapi.staticfiles import StaticFiles
from fastapi.openapi.docs import get_swagger_ui_html, get_redoc_html
_static_dir = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "static")
if _os.path.isdir(_static_dir):
    app.mount("/static", StaticFiles(directory=_static_dir), name="static")

# Resolve once now that _static_dir exists.
SPORTSPUFF_LOGO_URL = _sportspuff_logo_url()


@app.get("/docs", include_in_schema=False)
def custom_swagger_ui_html():
    return get_swagger_ui_html(
        openapi_url=app.openapi_url,
        title=f"{app.title} — Swagger UI",
        swagger_favicon_url=SPORTSPUFF_LOGO_URL,
        swagger_css_url="/static/sportspuff-docs.css",
        swagger_ui_parameters={
            "defaultModelsExpandDepth": 0,
            "docExpansion": "list",
            "tryItOutEnabled": True,
            "persistAuthorization": True,
        },
    )


@app.get("/redoc", include_in_schema=False)
def custom_redoc_html():
    return get_redoc_html(
        openapi_url=app.openapi_url,
        title=f"{app.title} — ReDoc",
        redoc_favicon_url=SPORTSPUFF_LOGO_URL,
        with_google_fonts=False,
    )

# Sport mappings
SPORT_MAPPINGS = {
    'nba': 'NBA',
    'mlb': 'MLB',
    'nfl': 'NFL',
    'nhl': 'NHL',
    'wnba': 'WNBA',
    'ipl': 'IPL',
    'mlc': 'MLC',
    'mls': 'MLS',
    'wc':  'WC',
    'atp': 'ATP',
    'wta': 'WTA',
    'cycling': 'CYCLING',
}


def _parse_accept_header(accept_header: Optional[str]) -> List[tuple[str, float]]:
    """Parse Accept into ordered (media_type, q) pairs."""
    if not accept_header:
        return []

    parsed: List[tuple[str, float]] = []
    for raw_part in accept_header.split(','):
        part = raw_part.strip()
        if not part:
            continue
        media_type = part
        quality = 1.0
        if ';' in part:
            media_type, *params = [p.strip() for p in part.split(';')]
            for param in params:
                if param.startswith('q='):
                    try:
                        quality = float(param[2:])
                    except ValueError:
                        quality = 0.0
        parsed.append((media_type.lower(), quality))
    return parsed


def _client_prefers_plain_text(request: Request) -> bool:
    """Return True when Accept explicitly prefers text/plain over JSON."""
    accepts = _parse_accept_header(request.headers.get("accept"))
    if not accepts:
        return False

    def _best_q(*media_types: str) -> float:
        best = 0.0
        for media_type, quality in accepts:
            if media_type in media_types:
                best = max(best, quality)
        return best

    text_q = _best_q("text/plain", "text/*")
    json_q = _best_q("application/json")
    wildcard_q = _best_q("*/*")

    if text_q <= 0:
        return False
    if json_q <= 0 and wildcard_q <= 0:
        return True
    return text_q > max(json_q, wildcard_q)


def _request_base_url(request: Request) -> str:
    """Return the externally visible base URL, preferring proxy headers."""
    forwarded_host = request.headers.get("x-forwarded-host")
    forwarded_proto = request.headers.get("x-forwarded-proto")
    host = (forwarded_host or request.headers.get("host") or "").split(",")[0].strip()
    proto = (forwarded_proto or request.url.scheme or "https").split(",")[0].strip()

    if host:
        return f"{proto}://{host}"
    return str(request.base_url).rstrip("/")


def _add_vary_accept(response: Response) -> Response:
    """Ensure response caches vary by Accept without clobbering existing Vary."""
    existing = response.headers.get("Vary")
    if not existing:
        response.headers["Vary"] = "Accept"
        return response

    vary_values = [value.strip() for value in existing.split(",") if value.strip()]
    if "accept" not in {value.lower() for value in vary_values}:
        vary_values.append("Accept")
        response.headers["Vary"] = ", ".join(vary_values)
    return response


def _internal_error_payload(route: str) -> Dict[str, Any]:
    return {
        "error": {
            "code": "internal_server_error",
            "message": "Internal server error",
            "route": route,
        }
    }


def _internal_error_response(route: str, exc: Exception, plain_text: bool = False):
    logger.exception("Unhandled error in %s: %s", route, exc)
    if plain_text:
        return PlainTextResponse(
            f"internal_server_error: Internal server error ({route})",
            status_code=500,
        )
    return JSONResponse(status_code=500, content=_internal_error_payload(route))


def _api_error_code(status_code: int) -> str:
    return {
        400: "invalid_request",
        401: "unauthorized",
        403: "forbidden",
        404: "not_found",
        405: "method_not_allowed",
        409: "conflict",
        422: "validation_error",
        429: "rate_limited",
        503: "service_unavailable",
    }.get(status_code, "http_error")


def _api_error_message(status_code: int, detail: Any) -> str:
    if isinstance(detail, str) and detail.strip():
        return detail
    return {
        400: "Invalid request",
        401: "Authentication required",
        403: "Forbidden",
        404: "Not found",
        405: "Method not allowed",
        409: "Conflict",
        422: "Validation failed",
        429: "Rate limit exceeded",
        503: "Service unavailable",
    }.get(status_code, "Request failed")


def _api_error_payload(status_code: int, route: str, detail: Any, details: Any = None) -> Dict[str, Any]:
    payload = {
        "error": {
            "code": _api_error_code(status_code),
            "message": _api_error_message(status_code, detail),
            "route": route,
        }
    }
    if details is not None:
        payload["error"]["details"] = details
    return payload


def _plain_text_error_response(status_code: int, route: str, detail: Any) -> PlainTextResponse:
    return PlainTextResponse(
        f"{_api_error_code(status_code)}: {_api_error_message(status_code, detail)} ({route})",
        status_code=status_code,
    )


def _request_prefers_plain_text_errors(request: Request) -> bool:
    path = request.url.path
    if path.startswith("/curl/"):
        return True
    if path.startswith("/v1/"):
        return _client_prefers_plain_text(request)
    return False

def get_help_json(base_url: str = "https://api.sportspuff.net") -> Dict[str, Any]:
    """Generate JSON formatted help content."""
    return {
        "title": "Sports Data Service API Help",
        "version": "1.0.0",
        "endpoints": {
            "schedules": {
                "description": "Get game schedules",
                "canonical": [
                    "GET /v1/schedules/{date} with Accept: application/json or text/plain",
                    "GET /v1/schedule/{sport}/{date} with Accept: application/json or text/plain"
                ],
                "legacy_compatibility": [
                    "/api/v1/schedules/{date} - JSON compatibility route",
                    "/api/v1/schedule/{sport}/{date} - JSON compatibility route",
                    "/curl/v1/schedules/{date} - Plain-text compatibility route",
                    "/curl/v1/schedule/{sport}/{date} - Plain-text compatibility route"
                ],
                "sports": ["nba", "mlb", "mls", "nfl", "nhl", "wnba", "ipl", "mlc", "all"],
                "date_formats": ["today", "tomorrow", "yesterday", "YYYY-MM-DD", "YYYYMMDD", "M/D/YYYY", "MM/DD/YYYY"],
                "note": "Use 'all' as sport to get schedules for all sports"
            },
            "scores": {
                "description": "Get game scores",
                "canonical": [
                    "GET /v1/scores/{date} with Accept: application/json or text/plain",
                    "GET /v1/scores/{sport}/{date} with Accept: application/json or text/plain"
                ],
                "legacy_compatibility": [
                    "/api/v1/scores/{date} - JSON compatibility route",
                    "/api/v1/scores/{sport}/{date} - JSON compatibility route",
                    "/curl/v1/scores/{date} - Plain-text compatibility route",
                    "/curl/v1/scores/{sport}/{date} - Plain-text compatibility route"
                ],
                "sports": ["nba", "mlb", "mls", "nfl", "nhl", "wnba", "ipl", "mlc", "all"],
                "date_formats": ["today", "tomorrow", "yesterday", "YYYY-MM-DD", "YYYYMMDD", "M/D/YYYY", "MM/DD/YYYY"],
                "note": "Use 'all' as sport to get scores for all sports"
            },
            "standings": {
                "description": "Get team standings",
                "canonical": [
                    "GET /v1/standings/{sport} with Accept: application/json or text/plain"
                ],
                "legacy_compatibility": [
                    "/api/v1/standings/{sport} - JSON compatibility route",
                    "/curl/v1/standings/{sport} - Plain-text compatibility route"
                ],
                "sports": ["nba", "mlb", "mls", "nfl", "nhl", "wnba"],
                "note": "Standings endpoint is currently under development"
            },
            "season_info": {
                "description": "Get season phase dates (preseason, regular season, playoffs, etc.)",
                "canonical": [
                    "GET /v1/season-info/{league} - Season dates for a league"
                ],
                "legacy_compatibility": [
                    "/api/v1/season-info/{league} - JSON compatibility route"
                ],
                "leagues": ["mlb", "nba", "nfl", "nhl", "wnba", "ipl", "mlc"],
                "note": "Returns year, current_phase, and season_types with start/end dates. Cached for 24 hours."
            }
        },
        "timezone": {
            "description": "Change timezone using the 'tz' query parameter",
            "usage": "?tz=<timezone>",
            "examples": [
                "?tz=et - Eastern Time",
                "?tz=pt - Pacific Time",
                "?tz=ct - Central Time",
                "?tz=mt - Mountain Time",
                "?tz=America/New_York - Full timezone name",
                "?tz=Europe/London - International timezone"
            ],
            "supported_aliases": [
                "et, est, edt, eastern - US/Eastern",
                "pt, pst, pdt, pacific - US/Pacific",
                "ct, cst, cdt, central - US/Central",
                "mt, mst, mdt, mountain - US/Mountain",
                "akst, akdt, alaska, ak - US/Alaska",
                "hst, hawaii, hi - US/Hawaii"
            ],
            "default": "US/Pacific (Pacific Time)"
        },
        "help": {
            "canonical": {
                "json": "GET /v1/... with Accept: application/json",
                "text": "GET /v1/... with Accept: text/plain"
            },
            "json": "/api/help or /api/v1/help",
            "text": "/curl/help or /curl/v1/help",
            "html": "/help"
        },
        "examples": [
            f'curl -H "Accept: application/json" {base_url}/v1/schedule/nba/today',
            f'curl -H "Accept: text/plain" "{base_url}/v1/scores/mlb/today?tz=et"',
            f'curl -H "Accept: application/json" {base_url}/v1/standings/nba',
            f"curl {base_url}/v1/season-info/mlb",
            f"curl {base_url}/curl/v1/schedule/ipl/today",
        ],
    }

def get_help_text(base_url: str) -> str:
    """Generate plain text formatted help content."""
    help_text = """Sports Data Service API Help
Version: 1.0.0

CANONICAL V1 ROUTES:
  Use /v1/... with Accept: application/json or Accept: text/plain

LEGACY COMPATIBILITY ROUTES:
  /api/v1/... serves JSON compatibility responses
  /curl/v1/... serves plain-text compatibility responses

ENDPOINTS:

Schedules:
  Canonical:
    /v1/schedules/{date}                   - All sports schedules
    /v1/schedule/{sport}/{date}            - Single sport schedule

  Legacy Compatibility:
    /api/v1/schedules/{date}               - JSON compatibility route
    /api/v1/schedule/{sport}/{date}        - JSON compatibility route
    /curl/v1/schedules/{date}              - Plain-text compatibility route
    /curl/v1/schedule/{sport}/{date}       - Plain-text compatibility route

Scores:
  Canonical:
    /v1/scores/{date}                      - All sports scores
    /v1/scores/{sport}/{date}              - Single sport scores

  Legacy Compatibility:
    /api/v1/scores/{date}                  - JSON compatibility route
    /api/v1/scores/{sport}/{date}          - JSON compatibility route
    /curl/v1/scores/{date}                 - Plain-text compatibility route
    /curl/v1/scores/{sport}/{date}         - Plain-text compatibility route

Standings:
  Canonical:
    /v1/standings/{sport}                  - Single sport standings

  Legacy Compatibility:
    /api/v1/standings/{sport}              - JSON compatibility route
    /curl/v1/standings/{sport}             - Plain-text compatibility route

  Note: Standings endpoint is currently under development

Season Info:
  Canonical:
    /v1/season-info/{league}               - Season phase dates

  Legacy Compatibility:
    /api/v1/season-info/{league}           - JSON compatibility route

  Returns year, current_phase, and season_types with start/end dates.
  Cached for 24 hours.

SPORTS:
  nba, mlb, mls, nfl, nhl, wnba, ipl, mlc, all

  Use 'all' to get data for all sports combined

LEAGUES (for season-info):
  mlb, nba, nfl, nhl, wnba, ipl, mlc

DATE FORMATS:
  today, tomorrow, yesterday
  YYYY-MM-DD (e.g., 2025-01-15)
  YYYYMMDD (e.g., 20250115)
  M/D/YYYY (e.g., 1/15/2025)
  MM/DD/YYYY (e.g., 01/15/2025)

TIMEZONE:
  Change timezone using the 'tz' query parameter: ?tz=<timezone>

  Examples:
    ?tz=et              - Eastern Time
    ?tz=pt              - Pacific Time
    ?tz=ct              - Central Time
    ?tz=mt              - Mountain Time
    ?tz=America/New_York - Full timezone name
    ?tz=Europe/London   - International timezone

  Supported Aliases:
    et, est, edt, eastern     -> US/Eastern
    pt, pst, pdt, pacific     -> US/Pacific
    ct, cst, cdt, central     -> US/Central
    mt, mst, mdt, mountain    -> US/Mountain
    akst, akdt, alaska, ak    -> US/Alaska
    hst, hawaii, hi           -> US/Hawaii

  Default: US/Pacific (Pacific Time)

HELP:
  /api/help or /api/v1/help    - JSON formatted help
  /curl/help or /curl/v1/help  - Plain text help (this format)
  /help                         - HTML formatted help

EXAMPLES:
  curl -H "Accept: application/json" __BASE_URL__/v1/schedule/nba/today
  curl -H "Accept: text/plain" "__BASE_URL__/v1/scores/mlb/today?tz=et"
  curl -H "Accept: application/json" __BASE_URL__/v1/standings/nba
  curl __BASE_URL__/v1/season-info/mlb
  curl __BASE_URL__/curl/v1/schedule/ipl/today
"""
    return help_text.replace("__BASE_URL__", base_url)

def get_help_html(base_url: str) -> str:
    """Generate HTML formatted help content."""
    html = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Sports Data Service API Help</title>
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            line-height: 1.6;
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
            background-color: #f5f5f5;
        }
        .container {
            background: white;
            padding: 30px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        h1 {
            color: #333;
            border-bottom: 3px solid #4CAF50;
            padding-bottom: 10px;
        }
        h2 {
            color: #555;
            margin-top: 30px;
            border-bottom: 2px solid #e0e0e0;
            padding-bottom: 5px;
        }
        h3 {
            color: #666;
            margin-top: 20px;
        }
        code {
            background-color: #f4f4f4;
            padding: 2px 6px;
            border-radius: 3px;
            font-family: 'Courier New', monospace;
            color: #d63384;
        }
        pre {
            background-color: #f4f4f4;
            padding: 15px;
            border-radius: 5px;
            overflow-x: auto;
            border-left: 4px solid #4CAF50;
        }
        .endpoint {
            background-color: #f9f9f9;
            padding: 10px;
            margin: 10px 0;
            border-radius: 5px;
            border-left: 3px solid #2196F3;
        }
        .sport-list {
            display: inline-block;
            background-color: #e3f2fd;
            padding: 5px 10px;
            border-radius: 3px;
            margin: 2px;
        }
        .note {
            background-color: #fff3cd;
            border-left: 4px solid #ffc107;
            padding: 10px;
            margin: 10px 0;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            margin: 15px 0;
        }
        th, td {
            padding: 10px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }
        th {
            background-color: #4CAF50;
            color: white;
        }
        tr:hover {
            background-color: #f5f5f5;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>Sports Data Service API Help</h1>
        <p><strong>Version:</strong> 1.0.0</p>
        
        <h2>Endpoints</h2>
        
        <h3>Schedules</h3>
        <div class="endpoint">
            <strong>Canonical:</strong><br>
            <code>/v1/schedules/{date}</code> - All sports schedules<br>
            <code>/v1/schedule/{sport}/{date}</code> - Single sport schedule
        </div>
        <div class="endpoint">
            <strong>Legacy compatibility:</strong><br>
            <code>/api/v1/schedules/{date}</code> - JSON compatibility route<br>
            <code>/api/v1/schedule/{sport}/{date}</code> - JSON compatibility route<br>
            <code>/curl/v1/schedules/{date}</code> - Plain-text compatibility route<br>
            <code>/curl/v1/schedule/{sport}/{date}</code> - Plain-text compatibility route
        </div>
        
        <h3>Scores</h3>
        <div class="endpoint">
            <strong>Canonical:</strong><br>
            <code>/v1/scores/{date}</code> - All sports scores<br>
            <code>/v1/scores/{sport}/{date}</code> - Single sport scores
        </div>
        <div class="endpoint">
            <strong>Legacy compatibility:</strong><br>
            <code>/api/v1/scores/{date}</code> - JSON compatibility route<br>
            <code>/api/v1/scores/{sport}/{date}</code> - JSON compatibility route<br>
            <code>/curl/v1/scores/{date}</code> - Plain-text compatibility route<br>
            <code>/curl/v1/scores/{sport}/{date}</code> - Plain-text compatibility route
        </div>
        
        <h3>Standings</h3>
        <div class="endpoint">
            <strong>Canonical:</strong><br>
            <code>/v1/standings/{sport}</code> - Single sport standings
        </div>
        <div class="endpoint">
            <strong>Legacy compatibility:</strong><br>
            <code>/api/v1/standings/{sport}</code> - JSON compatibility route<br>
            <code>/curl/v1/standings/{sport}</code> - Plain-text compatibility route
        </div>
        <div class="note">
            <strong>Note:</strong> Standings endpoint is currently under development
        </div>

        <h3>Season Info</h3>
        <div class="endpoint">
            <strong>Canonical:</strong><br>
            <code>/v1/season-info/{league}</code> - Season phase dates for a league
        </div>
        <div class="endpoint">
            <strong>Legacy compatibility:</strong><br>
            <code>/api/v1/season-info/{league}</code> - JSON compatibility route
        </div>
        <div class="note">
            Returns year, current_phase, and season_types with start/end dates. Cached for 24 hours.<br>
            <strong>Leagues:</strong> mlb, mls, nba, nfl, nhl, wnba, ipl, mlc
        </div>

        <h2>Sports</h2>
        <p>
            <span class="sport-list">mlb</span>
            <span class="sport-list">mls</span>
            <span class="sport-list">nba</span>
            <span class="sport-list">nfl</span>
            <span class="sport-list">nhl</span>
            <span class="sport-list">wnba</span>
            <span class="sport-list">ipl</span>
            <span class="sport-list">mlc</span>
            <span class="sport-list">all</span>
        </p>
        <p><strong>Note:</strong> Use <code>all</code> as the sport parameter to get data for all sports combined.
           IPL and MLC data is collected from CricAPI, with CricketPuff fallback support.</p>
        
        <h2>Date Formats</h2>
        <p>The <code>{date}</code> parameter accepts:</p>
        <ul>
            <li><code>today</code> - Today's date</li>
            <li><code>tomorrow</code> - Tomorrow's date</li>
            <li><code>yesterday</code> - Yesterday's date</li>
            <li><code>YYYY-MM-DD</code> - ISO format (e.g., 2025-01-15)</li>
            <li><code>YYYYMMDD</code> - Compact format (e.g., 20250115)</li>
            <li><code>M/D/YYYY</code> - US format (e.g., 1/15/2025)</li>
            <li><code>MM/DD/YYYY</code> - US format with leading zeros (e.g., 01/15/2025)</li>
        </ul>
        
        <h2>Timezone</h2>
        <p>Change timezone using the <code>tz</code> query parameter: <code>?tz=&lt;timezone&gt;</code></p>
        
        <h3>Examples</h3>
        <table>
            <tr>
                <th>Parameter</th>
                <th>Description</th>
            </tr>
            <tr>
                <td><code>?tz=et</code></td>
                <td>Eastern Time</td>
            </tr>
            <tr>
                <td><code>?tz=pt</code></td>
                <td>Pacific Time</td>
            </tr>
            <tr>
                <td><code>?tz=ct</code></td>
                <td>Central Time</td>
            </tr>
            <tr>
                <td><code>?tz=mt</code></td>
                <td>Mountain Time</td>
            </tr>
            <tr>
                <td><code>?tz=America/New_York</code></td>
                <td>Full timezone name</td>
            </tr>
            <tr>
                <td><code>?tz=Europe/London</code></td>
                <td>International timezone</td>
            </tr>
        </table>
        
        <h3>Supported Aliases</h3>
        <table>
            <tr>
                <th>Aliases</th>
                <th>Timezone</th>
            </tr>
            <tr>
                <td><code>et, est, edt, eastern</code></td>
                <td>US/Eastern</td>
            </tr>
            <tr>
                <td><code>pt, pst, pdt, pacific</code></td>
                <td>US/Pacific</td>
            </tr>
            <tr>
                <td><code>ct, cst, cdt, central</code></td>
                <td>US/Central</td>
            </tr>
            <tr>
                <td><code>mt, mst, mdt, mountain</code></td>
                <td>US/Mountain</td>
            </tr>
            <tr>
                <td><code>akst, akdt, alaska, ak</code></td>
                <td>US/Alaska</td>
            </tr>
            <tr>
                <td><code>hst, hawaii, hi</code></td>
                <td>US/Hawaii</td>
            </tr>
        </table>
        
        <p><strong>Default:</strong> US/Pacific (Pacific Time)</p>
        
        <h2>Help</h2>
        <ul>
            <li><code>/api/help</code> or <code>/api/v1/help</code> - JSON formatted help</li>
            <li><code>/curl/help</code> or <code>/curl/v1/help</code> - Plain text help</li>
            <li><code>/help</code> - HTML formatted help (this page)</li>
        </ul>
        
        <h2>Examples</h2>
        <pre># Get today's NBA schedule (canonical JSON)
curl -H "Accept: application/json" __BASE_URL__/v1/schedule/nba/today

# Get today's MLB scores (canonical plain text, Eastern Time)
curl -H "Accept: text/plain" "__BASE_URL__/v1/scores/mlb/today?tz=et"

# Get NBA standings (canonical JSON)
curl -H "Accept: application/json" __BASE_URL__/v1/standings/nba

# Get MLB season info
curl __BASE_URL__/v1/season-info/mlb

# Legacy compatibility route example
curl __BASE_URL__/curl/v1/schedule/ipl/today</pre>
    </div>
</body>
</html>"""
    return html

def get_timezone(tz_param: Optional[str] = None):
    """
    Get timezone object from query parameter.
    
    Supports:
    - Common US timezone aliases (et, est, pt, pst, etc.)
    - Any pytz timezone name (e.g., 'America/New_York', 'Europe/London', 'Europe/Berlin', 'Asia/Tokyo')
    - Case-insensitive matching for pytz timezone names
    
    Returns US/Pacific (Pacific time) as default if timezone cannot be determined.
    
    Note: For best results, use full pytz timezone names like 'Europe/Berlin' instead of
    abbreviations like 'CEST'. pytz handles daylight saving time automatically.
    """
    if not tz_param:
        return pytz.timezone('US/Pacific')
    
    tz_param = tz_param.strip()
    tz_param_lower = tz_param.lower()
    
    # Map common US timezone aliases (user-friendly shortcuts)
    us_aliases = {
        'et': 'US/Eastern',
        'est': 'US/Eastern',
        'edt': 'US/Eastern',
        'eastern': 'US/Eastern',
        'pt': 'US/Pacific',
        'pst': 'US/Pacific',
        'pdt': 'US/Pacific',
        'pacific': 'US/Pacific',
        'ct': 'US/Central',
        'cst': 'US/Central',
        'cdt': 'US/Central',
        'central': 'US/Central',
        'mt': 'US/Mountain',
        'mst': 'US/Mountain',
        'mdt': 'US/Mountain',
        'mountain': 'US/Mountain',
        'akst': 'US/Alaska',
        'akdt': 'US/Alaska',
        'alaska': 'US/Alaska',
        'ak': 'US/Alaska',
        'hst': 'US/Hawaii',
        'hawaii': 'US/Hawaii',
        'hi': 'US/Hawaii',
    }
    
    # Check US aliases first
    if tz_param_lower in us_aliases:
        return pytz.timezone(us_aliases[tz_param_lower])
    
    # Try to parse as a pytz timezone name directly (case-sensitive first)
    try:
        return pytz.timezone(tz_param)
    except pytz.exceptions.UnknownTimeZoneError:
        pass
    
    # Try case-insensitive lookup in all pytz timezones
    # This allows users to use 'europe/berlin', 'EUROPE/BERLIN', etc.
    try:
        for tz_name in pytz.all_timezones:
            if tz_name.lower() == tz_param_lower:
                return pytz.timezone(tz_name)
    except Exception:
        pass
    
    # Try common timezone abbreviations that pytz doesn't recognize directly
    # Only a minimal set for very common abbreviations
    common_abbrevs = {
        'utc': 'UTC',
        'z': 'UTC',
        'gmt': 'Europe/London',
        'cest': 'Europe/Berlin',  # Central European Summer Time
        'cet': 'Europe/Berlin',   # Central European Time
        'bst': 'Europe/London',   # British Summer Time
    }
    if tz_param_lower in common_abbrevs:
        try:
            return pytz.timezone(common_abbrevs[tz_param_lower])
        except:
            pass
    
    # Default to Pacific if we can't determine the timezone
    return pytz.timezone('US/Pacific')

def get_greeting(tz: pytz.BaseTzInfo = None) -> str:
    """Get greeting based on time of day in specified timezone."""
    if tz is None:
        tz = pytz.timezone('US/Pacific')  # Default to Pacific
    
    now = datetime.now(tz)
    hour = now.hour
    
    if 0 <= hour < 5:
        return "Good god... it's late ⏾ from SportsPuff!"
    elif 5 <= hour < 12:
        return "Good morning 🌇 from SportsPuff!"
    elif 12 <= hour < 17:
        return "Good afternoon 🌞 from SportsPuff!"
    else:
        return "Good evening ✨ from SportsPuff!"

def parse_date_param(date_param: Optional[str], tz: pytz.BaseTzInfo = None) -> date:
    """
    Parse date parameter with support for multiple formats.
    
    Supports:
    - Relative dates: today, tomorrow, yesterday (uses Pacific time by default)
    - YYYY-MM-DD (ISO format, e.g., 2025-11-05)
    - YYYYMMDD (compact format, e.g., 20251105)
    - M/D/YYYY or MM/DD/YYYY (US format, e.g., 11/5/2025 or 11/05/2025)
    - M-D-YYYY or MM-DD-YYYY (US format with dashes, e.g., 11-5-2025)
    - YYYY/M/D or YYYY/MM/DD (alternative format, e.g., 2025/11/5)
    
    Uses dateutil.parser as fallback for other formats.
    
    Args:
        date_param: Date string to parse
        tz: Timezone for relative dates (defaults to Pacific)
    """
    if tz is None:
        tz = pytz.timezone('US/Pacific')  # Default to Pacific for "today"
    
    # Get today's date in the specified timezone
    now_tz = datetime.now(tz)
    today = now_tz.date()
    
    if date_param is None or date_param.lower() == 'today':
        return today
    elif date_param.lower() == 'tomorrow':
        return today + timedelta(days=1)
    elif date_param.lower() == 'yesterday':
        return today - timedelta(days=1)
    
    # Try multiple date formats
    date_formats = [
        '%Y-%m-%d',      # YYYY-MM-DD (ISO format)
        '%Y%m%d',         # YYYYMMDD (compact format)
        '%m/%d/%Y',       # M/D/YYYY or MM/DD/YYYY
        '%m-%d-%Y',       # M-D-YYYY or MM-DD-YYYY
        '%Y/%m/%d',       # YYYY/M/D or YYYY/MM/DD
        '%m.%d.%Y',       # M.D.YYYY (alternative)
        '%d/%m/%Y',       # D/M/YYYY (European format)
        '%d-%m-%Y',       # D-M-YYYY (European format)
        '%Y-%m-%d',       # YYYY-MM-DD (redundant but explicit)
    ]
    
    # Try each format
    for fmt in date_formats:
        try:
            return datetime.strptime(date_param, fmt).date()
        except ValueError:
            continue
    
    # Fallback to dateutil.parser for flexible parsing (handles many formats)
    try:
        from dateutil import parser
        parsed_date = parser.parse(date_param)
        return parsed_date.date()
    except (ValueError, TypeError) as e:
        # If all parsing fails, provide helpful error message
        raise HTTPException(
            status_code=400,
            detail=f"Invalid date format: '{date_param}'. Supported formats: today/tomorrow/yesterday, YYYY-MM-DD, YYYYMMDD, M/D/YYYY, MM/DD/YYYY, M-D-YYYY, etc."
        )

def format_game_for_curl(game: Game, sport: str, tz: pytz.BaseTzInfo = None) -> str:
    """Format a single game for curl-style schedule output.

    Final:       *PHI [ 14- 20] ( 7@ 2)  MIA [ 16- 18]  F
    In-progress:  PHI [ 14- 20] ( 2@ 1)  MIA [ 16- 18]  TOP 5
    Scheduled:    PHI [ 14- 20]    @      MIA [ 16- 18]  7:00 PM PDT - in 3h
    """
    if tz is None:
        tz = pytz.timezone('US/Pacific')

    if sport.lower() in ('ipl', 'mlc'):
        return _format_cricket_game(game, tz)
    if sport.lower() == 'cycling':
        return _format_cycling_game(game, tz)
    if sport.lower() == 'wc':
        return _format_world_cup_game(game, tz)

    visitor_wins = game.visitor_wins or 0
    visitor_losses = game.visitor_losses or 0
    home_wins = game.home_wins or 0
    home_losses = game.home_losses or 0

    # For playoff games, prefer the per-series record over the regular-season
    # record (which is 0-0 once the post-season starts). Series fields are
    # populated by services.playoff_series.enrich_games when ESPN has the data.
    if getattr(game, 'is_playoff', False) or game.game_type == 'playoffs':
        h_sw = getattr(game, 'home_series_wins', None)
        v_sw = getattr(game, 'visitor_series_wins', None)
        if h_sw is not None or v_sw is not None:
            visitor_wins = v_sw or 0
            visitor_losses = getattr(game, 'visitor_series_losses', 0) or 0
            home_wins = h_sw or 0
            home_losses = getattr(game, 'home_series_losses', 0) or 0

    visitor_abbrev = game.visitor_team_abbrev
    if not visitor_abbrev or visitor_abbrev.strip() == '':
        visitor_abbrev = (game.visitor_team or '???')[:4].upper()
    abbrev_width = 4 if sport.lower() == 'mls' else 3
    visitor_abbrev = visitor_abbrev.ljust(abbrev_width)

    home_abbrev = game.home_team_abbrev
    if not home_abbrev or home_abbrev.strip() == '':
        home_abbrev = (game.home_team or '???')[:4].upper()
    home_abbrev = home_abbrev.ljust(abbrev_width)

    game_type = getattr(game, 'game_type', 'regular')

    if sport.lower() == 'nhl':
        visitor_otl = getattr(game, 'visitor_otl', 0) or 0
        home_otl = getattr(game, 'home_otl', 0) or 0
        if game_type == 'playoffs':
            away_rec = f"[{visitor_wins}-{visitor_losses}]"
            home_rec = f"[{home_wins}-{home_losses}]"
        else:
            away_rec = f"[{visitor_wins:3d}-{visitor_losses:3d}-{visitor_otl:2d}]"
            home_rec = f"[{home_wins:3d}-{home_losses:3d}-{home_otl:2d}]"
    elif sport.lower() == 'mls':
        visitor_draws = getattr(game, 'visitor_draws', 0) or 0
        home_draws = getattr(game, 'home_draws', 0) or 0
        v_pts = visitor_wins * 3 + visitor_draws
        h_pts = home_wins * 3 + home_draws
        away_rec = f"[{visitor_wins:2d}-{visitor_draws:2d}-{visitor_losses:2d} {v_pts:2d}pts]"
        home_rec = f"[{home_wins:2d}-{home_draws:2d}-{home_losses:2d} {h_pts:2d}pts]"
    elif game_type == 'playoffs':
        away_rec = f"[{visitor_wins}-{visitor_losses}]"
        home_rec = f"[{home_wins}-{home_losses}]"
    else:
        away_rec = f"[{visitor_wins:3d}-{visitor_losses:3d}]"
        home_rec = f"[{home_wins:3d}-{home_losses:3d}]"

    vs = game.visitor_score_total or 0
    hs = game.home_score_total or 0

    if game.is_final:
        visitor_won = vs > hs
        home_won = hs > vs
        v_mark = '*' if visitor_won else ' '
        h_mark = '*' if home_won else ' '

        if sport.lower() == 'nhl':
            period = str(game.current_period) if game.current_period is not None else '?'
            try:
                period_num = int(period) if str(period).isdigit() else 0
                status = "F/OT" if period_num >= 4 else "F"
            except (ValueError, TypeError):
                status = "F"
        elif sport.lower() == 'mlb':
            period = str(game.current_period) if game.current_period is not None else ''
            try:
                inn = int(period) if period.isdigit() else 0
                status = f"F/{inn}" if inn > 9 else "F"
            except (ValueError, TypeError):
                status = "F"
        elif sport.lower() == 'mls':
            status = "FT"
        elif sport.lower() == 'wc':
            home_so = getattr(game, 'home_shootout_score', None)
            visitor_so = getattr(game, 'visitor_shootout_score', None)
            if home_so is not None or visitor_so is not None:
                status = f"FT-PK {visitor_so or 0}-{home_so or 0}"
            else:
                status = "FT"
        else:
            status = "F"

        return f" {v_mark}{visitor_abbrev} {away_rec} ({vs:2d}@{hs:2d}) {h_mark}{home_abbrev} {home_rec}  {status}"

    elif game.game_status == 'in_progress' or (vs > 0 or hs > 0):
        period = str(game.current_period) if game.current_period is not None else '?'
        time_left = game.time_remaining or ''

        if sport.lower() == 'nhl':
            try:
                period_num = int(period) if str(period).isdigit() else 0
                period_display = 'OT' if period_num >= 4 else f'P{period_num}'
            except (ValueError, TypeError):
                period_display = f'P{period}'
            status = f"{period_display} {time_left}".strip() if time_left and time_left.strip() else period_display
        elif period and str(period).upper() in ('FINAL', 'F', 'END', 'FIN'):
            status = "F"
        elif sport.lower() == 'mlb':
            inning_state = time_left.strip().upper() if time_left else ''
            inning_abbrev = {'TOP': 'TOP', 'BOTTOM': 'BOT', 'MIDDLE': 'MID', 'END': 'END'}.get(inning_state, inning_state)
            status = f"{inning_abbrev} {period}" if inning_abbrev else f"INN {period}"
        else:
            period_prefix = 'Q'
            is_halftime = (period == '2' and time_left in ('0:00', '') and game.game_status == 'in_progress')
            if is_halftime:
                status = "HT"
            elif game.game_status == 'in_progress' and time_left and time_left.strip():
                status = f"{period_prefix}{period} {time_left}"
            elif game.game_status == 'in_progress' and period and period not in ('?', '', '0'):
                status = f"{period_prefix}{period}"
            else:
                status = "F"

        return f"  {visitor_abbrev} {away_rec} ({vs:2d}@{hs:2d})  {home_abbrev} {home_rec}  {status}"

    else:
        if game.game_time:
            try:
                gt = game.game_time
                if hasattr(gt, 'tzinfo') and gt.tzinfo is None:
                    gt = pytz.UTC.localize(gt)
                game_time_local = gt.astimezone(tz)
                tz_abbrev = game_time_local.strftime('%Z')
                time_str = game_time_local.strftime('%-I:%M %p')
                now = datetime.now(tz)
                diff = game_time_local - now
                total_seconds = int(diff.total_seconds())
                if total_seconds > 0:
                    hours, remainder = divmod(total_seconds, 3600)
                    minutes = remainder // 60
                    status = f"{time_str} {tz_abbrev} - in {hours}h{minutes:02d}m"
                else:
                    status = f"{time_str} {tz_abbrev}"
            except Exception:
                status = "TBD"
        else:
            status = "TBD"

        return f"  {visitor_abbrev} {away_rec}    @     {home_abbrev} {home_rec}  {status}"


def _format_world_cup_record(game: Any, side: str) -> str:
    """Format a World Cup team record as W-D-L, falling back to raw fields."""
    record = getattr(game, f"{side}_record", "") or ""
    if record:
        return f"[{record}]"
    wins = int(getattr(game, f"{side}_wins", 0) or 0)
    draws = int(getattr(game, f"{side}_draws", 0) or 0)
    losses = int(getattr(game, f"{side}_losses", 0) or 0)
    return f"[{wins}-{draws}-{losses}]"


def _format_world_cup_game(game: Any, tz: pytz.BaseTzInfo = None) -> str:
    """Format a World Cup game with group-stage records visible for both sides."""
    if tz is None:
        tz = pytz.timezone('US/Pacific')

    visitor_abbrev = (getattr(game, "visitor_team_abbrev", "") or (getattr(game, "visitor_team", "") or "???")[:4]).ljust(3)
    home_abbrev = (getattr(game, "home_team_abbrev", "") or (getattr(game, "home_team", "") or "???")[:4]).ljust(3)
    away_rec = _format_world_cup_record(game, "visitor")
    home_rec = _format_world_cup_record(game, "home")

    vs = int(getattr(game, "visitor_score_total", 0) or 0)
    hs = int(getattr(game, "home_score_total", 0) or 0)

    if getattr(game, "is_final", False):
        visitor_won = vs > hs
        home_won = hs > vs
        v_mark = '*' if visitor_won else ' '
        h_mark = '*' if home_won else ' '
        home_so = getattr(game, 'home_shootout_score', None)
        visitor_so = getattr(game, 'visitor_shootout_score', None)
        if home_so is not None and visitor_so is not None and home_so != visitor_so:
            status = f"FT-PK {visitor_so or 0}-{home_so or 0}"
        else:
            status = "FT"
        return f" {v_mark}{visitor_abbrev} {away_rec} ({vs:2d}@{hs:2d}) {h_mark}{home_abbrev} {home_rec}  {status}"

    period = str(getattr(game, "current_period", "") or "")
    time_left = getattr(game, "time_remaining", "") or ""
    if getattr(game, "game_status", "") == "in_progress" or vs > 0 or hs > 0:
        if period and time_left and time_left.strip():
            status = f"Q{period} {time_left}"
        elif period and period not in ("?", "", "0"):
            status = f"Q{period}"
        else:
            status = "LIVE"
    else:
        game_time = getattr(game, "game_time", None)
        if game_time:
            try:
                gt = game_time
                if hasattr(gt, "tzinfo") and gt.tzinfo is None:
                    gt = pytz.UTC.localize(gt)
                game_time_local = gt.astimezone(tz)
                tz_abbrev = game_time_local.strftime("%Z")
                time_str = game_time_local.strftime("%-I:%M %p")
                now = datetime.now(tz)
                diff = game_time_local - now
                total_seconds = int(diff.total_seconds())
                if total_seconds > 0:
                    hours, remainder = divmod(total_seconds, 3600)
                    minutes = remainder // 60
                    status = f"{time_str} {tz_abbrev} - in {hours}h{minutes:02d}m"
                else:
                    status = f"{time_str} {tz_abbrev}"
            except Exception:
                status = "TBD"
        else:
            status = "TBD"

    return f"  {visitor_abbrev} {away_rec}    @     {home_abbrev} {home_rec}  {status}"


def _format_cricket_game(game, tz):
    """Format a cricket match for curl output."""
    home_abbrev = (getattr(game, 'home_team_abbrev', '') or '???').ljust(4)
    away_abbrev = (getattr(game, 'visitor_team_abbrev', '') or '???').ljust(4)

    home_score_str = getattr(game, 'cricket_home_score', '') or ''
    away_score_str = getattr(game, 'cricket_away_score', '') or ''
    away_outcome = getattr(game, 'cricket_away_outcome', '') or ''
    start_time = getattr(game, 'cricket_start_time', {}) or {}

    if game.is_final and (home_score_str or away_score_str):
        away_part = f"{away_abbrev} ({away_score_str})" if away_score_str else str(away_abbrev)
        home_part = f"{home_abbrev} ({home_score_str})" if home_score_str else str(home_abbrev)
        outcome = away_outcome.rjust(4) if away_outcome else '    '
        return f" {away_part:18s} {outcome} @ {home_part}"
    elif game.is_final:
        cricket_status = getattr(game, 'cricket_status', '') or ''
        return f" {cricket_status}" if cricket_status else f" {away_abbrev} @ {home_abbrev} F"
    elif getattr(game, 'game_status', '') == 'in_progress':
        cricket_status = getattr(game, 'cricket_status', '') or ''
        if home_score_str or away_score_str:
            away_part = f"{away_abbrev} ({away_score_str})" if away_score_str else str(away_abbrev)
            home_part = f"{home_abbrev} ({home_score_str})" if home_score_str else str(home_abbrev)
            return f" {away_part} @ {home_part} LIVE"
        elif cricket_status:
            return f" {away_abbrev} @ {home_abbrev} {cricket_status}"
        else:
            return f" {away_abbrev} @ {home_abbrev} LIVE"
    else:
        local_str = start_time.get('local', '')
        pt_str = start_time.get('pt', '')
        ist_str = start_time.get('ist', '')
        if local_str and ist_str and local_str != ist_str:
            time_str = f"{local_str}/{ist_str}"
        elif local_str:
            time_str = local_str
        elif pt_str and ist_str:
            time_str = f"{pt_str}/{ist_str}"
        elif pt_str:
            time_str = pt_str
        else:
            time_str = "TBD"

        countdown = ''
        gt = getattr(game, 'game_time', None)
        if gt:
            try:
                now = datetime.now(tz)
                diff = gt.astimezone(tz) - now
                total_seconds = int(diff.total_seconds())
                if total_seconds > 0:
                    hours, remainder = divmod(total_seconds, 3600)
                    minutes = remainder // 60
                    countdown = f" - in {hours}h{minutes:02d}m"
            except Exception:
                pass

        return f" {away_abbrev} @ {home_abbrev} {time_str}{countdown}"


def _format_tennis_match(game, tz) -> str:
    """Format a tennis match for curl output. Two-line layout — visitor on
    line 1, home on line 2, set-score columns aligned, status on the home
    line. ESPN-sourced set scores show when present; otherwise the rows
    just carry the names. Tournament context is rendered as a sub-banner
    one level up."""
    home_name = (getattr(game, 'home_full_name', '') or getattr(game, 'home_team', '') or '').strip() or '?'
    visitor_name = (getattr(game, 'visitor_full_name', '') or getattr(game, 'visitor_team', '') or '').strip() or '?'
    home_seed = getattr(game, 'home_seed', None)
    visitor_seed = getattr(game, 'visitor_seed', None)

    def _seeded(name: str, seed) -> str:
        if seed in (None, "", 0):
            return name
        return f"{name} [{seed}]"

    home = _seeded(home_name, home_seed)
    visitor = _seeded(visitor_name, visitor_seed)
    name_w = 14

    set_scores = getattr(game, 'tennis_set_scores', None) or []
    winner = getattr(game, 'tennis_winner', None)
    v_mark = '*' if winner == 'visitor' else ' '
    h_mark = '*' if winner == 'home' else ' '

    v_sets = ""
    h_sets = ""
    if set_scores:
        v_sets = "  " + " ".join(f"{s['visitor']:>2d}" for s in set_scores)
        h_sets = "  " + " ".join(f"{s['home']:>2d}" for s in set_scores)

    if game.is_final:
        status = "F"
    elif getattr(game, 'game_status', '') == 'in_progress':
        status = "LIVE"
    elif game.game_time:
        try:
            gt = game.game_time
            if hasattr(gt, 'tzinfo') and gt.tzinfo is None:
                gt = pytz.UTC.localize(gt)
            game_time_local = gt.astimezone(tz)
            tz_abbrev = game_time_local.strftime('%Z')
            time_str = game_time_local.strftime('%-I:%M %p')
            status = f"{time_str} {tz_abbrev}"
        except Exception:
            status = "TBD"
    else:
        status = "TBD"

    line1 = f" {v_mark}{visitor.ljust(name_w)}{v_sets}"
    line2 = f" {h_mark}{home.ljust(name_w)}{h_sets}  {status}"
    return f"{line1}\n{line2}"


def _format_cycling_game(game, tz) -> str:
    """Format a cycling stage/rest day for curl-style output."""
    race = (getattr(game, 'cycling_race', '') or getattr(game, 'home_team', '') or 'Cycling').strip()
    stage = (getattr(game, 'cycling_stage_label', '') or getattr(game, 'visitor_team', '') or '').strip()
    event_label = (getattr(game, 'cycling_event_label', '') or f"{race} {stage}".strip()).strip()
    game_date = getattr(game, 'game_date', None)
    if hasattr(game_date, "isoformat"):
        date_text = game_date.isoformat()
    else:
        date_text = str(game_date or '').strip()
    start_city = (getattr(game, 'start_city', '') or '').strip()
    finish_city = (getattr(game, 'finish_city', '') or '').strip()
    race_type = (getattr(game, 'race_type', '') or '').strip()
    distance = getattr(game, 'cycling_distance_km', '') or ''
    url = (getattr(game, 'cycling_url', '') or '').strip()
    url_label = (getattr(game, 'cycling_url_label', '') or '').strip()

    status = (getattr(game, 'game_status', '') or 'scheduled').strip().upper()
    if getattr(game, 'is_final', False):
        status = 'FINAL'
    elif status == 'IN_PROGRESS':
        status = 'LIVE'

    parts = [date_text or "TBD", stage or event_label]
    if start_city or finish_city:
        if start_city and finish_city and start_city != finish_city:
            parts.append(f"{start_city} -> {finish_city}")
        elif start_city:
            parts.append(start_city)
        elif finish_city:
            parts.append(finish_city)
    if race_type:
        parts.append(race_type)
    if distance:
        parts.append(f"{distance} km")
    parts.append(status)
    if url:
        if url_label:
            parts.append(f"link: {url_label} -> {url}")
        else:
            parts.append(f"link: {url}")

    return " | ".join(parts)


def _get_season_type_for_sport(sport: str, target_date: date) -> str:
    """Get season type for a sport from database when there are no games for the date."""
    sport_to_league = {
        'mlb': 'MLB',
        'wnba': 'WNBA',
        'nba': 'NBA',
        'nfl': 'NFL',
        'nhl': 'NHL'
    }
    league = sport_to_league.get(sport)
    if not league:
        return "Off Season"
    
    # Try to find the most recent game for this sport to determine season type
    try:
        with get_db_session() as db:
            # Look for games within a reasonable range (30 days before/after)
            start_date = target_date - timedelta(days=30)
            end_date = target_date + timedelta(days=30)
            
            recent_game = db.query(Game).filter(
                Game.league == league,
                Game.game_date >= start_date,
                Game.game_date <= end_date
            ).order_by(Game.game_date.desc()).first()
            
            if recent_game:
                game_type_map = {
                    'preseason': 'Preseason',
                    'regular': 'Regular Season',
                    'playoffs': 'Playoffs',
                    'allstar': 'All-Star',
                    'nba_cup': 'Emirates NBA Cup',
                    'postseason': 'Playoffs'
                }
                return game_type_map.get(recent_game.game_type.lower(), recent_game.game_type.title().replace('_', ' '))
    except Exception:
        pass
    
    # Default fallback
    return "Off Season"


def _format_curl_header(tz, target_date, label):
    greeting = get_greeting(tz)
    output = f"{greeting}\n"
    output += "\n"
    output += f"{label}\n"
    output += "-" * 45 + "\n"
    return output


def _format_curl_footer(tz):
    now_tz = datetime.now(tz)
    tz_abbrev = now_tz.strftime('%Z')
    date_str = now_tz.strftime('%a %b %d %Y')
    time_str = now_tz.strftime('%H:%M')
    output = f"          All times in {tz_abbrev}\n"
    output += f"    Sent on {date_str} @{time_str}{tz_abbrev}\n"
    output += "-" * 45 + "\n"
    return output


def format_schedule_curl(games: List[Game], target_date: date, tz: pytz.BaseTzInfo = None, show_all_sports: bool = False) -> str:
    if tz is None:
        tz = pytz.timezone('US/Pacific')

    if not games and not show_all_sports:
        return "No games scheduled"

    by_sport: Dict[str, List[Game]] = {}
    for game in games:
        sport = game.league.lower()
        if sport not in by_sport:
            by_sport[sport] = []
        by_sport[sport].append(game)

    output = _format_curl_header(tz, target_date, "Here is the schedule:")

    sport_order = ['ipl', 'mlb', 'mlc', 'mls', 'nba', 'nfl', 'nhl', 'wnba', 'wc', 'atp', 'wta', 'cycling']
    sport_to_league = {
        'ipl': 'IPL', 'mlb': 'MLB', 'mlc': 'MLC', 'mls': 'MLS',
        'nba': 'NBA', 'nfl': 'NFL', 'nhl': 'NHL', 'wnba': 'WNBA',
        'wc': 'WC', 'atp': 'ATP', 'wta': 'WTA', 'cycling': 'CYCLING',
    }

    game_type_map = {
        'preseason': 'Preseason', 'regular': 'Regular Season',
        'playoffs': 'Post Season (Playoffs)', 'postseason': 'Post Season (Playoffs)',
        'allstar': 'All-Star', 'nba_cup': 'Emirates NBA Cup'
    }

    for sport in sport_order:
        if not show_all_sports and sport not in by_sport:
            continue

        sport_games = by_sport.get(sport, [])

        if sport in ('atp', 'wta') and sport_games:
            league_name = sport_games[0].league
            by_t: Dict[str, List[Game]] = {}
            for g in sport_games:
                t = (getattr(g, 'tennis_tournament', '') or '').strip() or 'Other'
                by_t.setdefault(t, []).append(g)
            for t_name in sorted(by_t.keys()):
                output += f"{league_name} — {t_name}\n"
                output += "-" * 45 + "\n"
                matches = by_t[t_name]
                for i, game in enumerate(matches):
                    output += _format_tennis_match(game, tz)
                    output += "\n"
                    if i < len(matches) - 1:
                        output += "  ---\n"
                output += "-" * 45 + "\n"
            continue

        if sport_games:
            first_game = sport_games[0]
            if sport == 'cycling':
                season_type = getattr(first_game, 'cycling_race', '') or getattr(first_game, 'game_type', 'Cycling').title()
            elif getattr(first_game, 'is_playoff', False):
                season_type = 'Post Season (Playoffs)'
            else:
                season_type = game_type_map.get(first_game.game_type.lower(), first_game.game_type.title().replace('_', ' '))
            league_name = first_game.league
        else:
            league_name = sport_to_league.get(sport, sport.upper())
            season_type = _get_season_type_for_sport(sport, target_date)

        output += f"{league_name} [{season_type}]\n"
        output += "-" * 45 + "\n"

        if sport_games:
            for game in sport_games:
                output += format_game_for_curl(game, sport, tz)
                output += "\n"
        else:
            output += " No games scheduled\n"

        output += "-" * 45 + "\n"

    output += _format_curl_footer(tz)
    return output


def format_scores_curl(games: List[Game], target_date: date, tz: pytz.BaseTzInfo = None, show_all_sports: bool = False) -> str:
    if tz is None:
        tz = pytz.timezone('US/Pacific')

    by_sport: Dict[str, List[Game]] = {}
    for game in games:
        sport = game.league.lower()
        if sport not in by_sport:
            by_sport[sport] = []
        by_sport[sport].append(game)

    output = _format_curl_header(tz, target_date, "Here are the scores:")

    sport_order = ['ipl', 'mlb', 'mlc', 'mls', 'nba', 'nfl', 'nhl', 'wnba', 'wc', 'atp', 'wta', 'cycling']
    sport_to_league = {
        'ipl': 'IPL', 'mlb': 'MLB', 'mlc': 'MLC', 'mls': 'MLS',
        'nba': 'NBA', 'nfl': 'NFL', 'nhl': 'NHL', 'wnba': 'WNBA',
        'wc': 'WC', 'atp': 'ATP', 'wta': 'WTA', 'cycling': 'CYCLING',
    }

    game_type_map = {
        'preseason': 'Preseason', 'regular': 'Regular Season',
        'playoffs': 'Post Season (Playoffs)', 'postseason': 'Post Season (Playoffs)',
        'allstar': 'All-Star', 'nba_cup': 'Emirates NBA Cup'
    }
    
    for sport in sport_order:
        # If show_all_sports is False, skip sports with no games
        if not show_all_sports and sport not in by_sport:
            continue
        
        sport_games = by_sport.get(sport, [])
        
        # Show games with scores (final, in progress, or scheduled with scores > 0)
        # Also deduplicate by game_id to avoid showing the same game twice
        seen_game_ids = set()
        scored_games = []
        for g in sport_games:
            game_id = getattr(g, 'game_id', None) or getattr(g, 'gameId', None)
            if game_id and game_id in seen_game_ids:
                continue  # Skip duplicates
            if sport in ('ipl', 'mlc'):
                seen_game_ids.add(game_id or 'no_id')
                scored_games.append(g)
                continue
            # Only include games that have scores (final, in progress, or have non-zero scores)
            # Skip games that are just scheduled (score 0-0 and status is scheduled)
            has_score = (g.visitor_score_total and g.visitor_score_total > 0) or (g.home_score_total and g.home_score_total > 0)
            is_final_or_live = g.is_final or g.game_status == 'in_progress' or has_score
            
            if is_final_or_live:
                seen_game_ids.add(game_id or 'no_id')
                scored_games.append(g)

        if sport == 'cycling' and scored_games:
            race_name = scored_games[0].cycling_race or scored_games[0].league
            output += f"{race_name} [Stage Schedule]\n"
            output += "-" * 45 + "\n"
            for game in scored_games:
                output += _format_cycling_game(game, tz)
                output += "\n"
            output += "-" * 45 + "\n"
            continue

        if sport == 'wc' and scored_games:
            output += "WC [Group/Knockout]\n"
            output += "-" * 45 + "\n"
            for game in scored_games:
                output += _format_world_cup_game(game, tz)
                output += "\n"
            output += "-" * 45 + "\n"
            continue

        if sport in ('atp', 'wta') and scored_games:
            league_name = scored_games[0].league
            by_t: Dict[str, List[Game]] = {}
            for g in scored_games:
                t = (getattr(g, 'tennis_tournament', '') or '').strip() or 'Other'
                by_t.setdefault(t, []).append(g)
            for t_name in sorted(by_t.keys()):
                output += f"{league_name} — {t_name}\n"
                output += "-" * 45 + "\n"
                matches = by_t[t_name]
                for i, game in enumerate(matches):
                    output += _format_tennis_match(game, tz)
                    output += "\n"
                    if i < len(matches) - 1:
                        output += "  ---\n"
                output += "-" * 45 + "\n"
            continue

        # Get season info - either from games or from database
        if scored_games:
            # Determine season info from first game
            first_game = scored_games[0]
            if getattr(first_game, 'is_playoff', False):
                season_type = 'Post Season (Playoffs)'
            else:
                season_type = game_type_map.get(first_game.game_type.lower(), first_game.game_type.title().replace('_', ' '))
            league_name = first_game.league
        else:
            league_name = sport_to_league.get(sport, sport.upper())
            season_type = _get_season_type_for_sport(sport, target_date)

        output += f"{league_name} [{season_type}]\n"
        output += "-" * 45 + "\n"

        if scored_games:
            if sport == 'cycling':
                for game in scored_games:
                    output += _format_cycling_game(game, tz)
                    output += "\n"
            elif sport in ('ipl', 'mlc'):
                for game in scored_games:
                    output += _format_cricket_game(game, tz)
                    output += "\n"
            else:
                abbr_w = 4 if sport == 'mls' else 3
                for game in scored_games:
                    away_abbr = (game.visitor_team_abbrev or '???').ljust(abbr_w)
                    home_abbr = (game.home_team_abbrev or '???').ljust(abbr_w)

                    away_score = game.visitor_score_total or 0
                    home_score = game.home_score_total or 0

                    if game.is_final:
                        if sport == 'nhl':
                            period = str(game.current_period) if game.current_period is not None else '?'
                            try:
                                period_num = int(period) if str(period).isdigit() else 0
                                status = "F/OT" if period_num >= 4 else "F"
                            except (ValueError, TypeError):
                                status = "F"
                        elif sport == 'mlb':
                            period = str(game.current_period) if game.current_period is not None else ''
                            try:
                                inn = int(period) if period.isdigit() else 0
                                status = f"F/{inn}" if inn > 9 else "F"
                            except (ValueError, TypeError):
                                status = "F"
                        elif sport == 'mls':
                            status = "FT"
                        else:
                            status = "F"
                        output += f" {away_abbr} {away_score:2d}-{home_score:2d} {home_abbr} {status}\n"
                    elif game.game_status == 'in_progress' or (away_score > 0 or home_score > 0):
                        period = str(game.current_period) if game.current_period is not None else '?'
                        time_left = game.time_remaining or ''

                        if sport == 'nhl':
                            try:
                                period_num = int(period) if str(period).isdigit() else 0
                                period_display = 'OT' if period_num >= 4 else f'P{period_num}'
                            except (ValueError, TypeError):
                                period_display = f'P{period}'
                            status = f"{period_display} {time_left}".strip() if time_left and time_left.strip() else period_display
                        elif period and str(period).upper() in ('FINAL', 'F', 'END', 'FIN'):
                            status = "F"
                        elif sport == 'mlb':
                            inning_state = time_left.strip().upper() if time_left else ''
                            inning_abbrev = {'TOP': 'TOP', 'BOTTOM': 'BOT', 'MIDDLE': 'MID', 'END': 'END'}.get(inning_state, inning_state)
                            status = f"{inning_abbrev} {period}" if inning_abbrev else f"INN {period}"
                        else:
                            period_prefix = 'Q'
                            if game.game_status == 'in_progress' and time_left and time_left.strip():
                                status = f"{period_prefix}{period} {time_left}"
                            elif game.game_status == 'in_progress' and period and period not in ('?', '', '0'):
                                status = f"{period_prefix}{period}"
                            else:
                                status = "F"

                        output += f" {away_abbr} {away_score:2d}-{home_score:2d} {home_abbr} {status}\n"
        else:
            output += " No games scheduled\n"
        
        output += "-" * 45 + "\n"

    output += _format_curl_footer(tz)
    return output
@app.get("/", response_class=HTMLResponse)
def root():
    """Landing page."""
    return """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>SportsPuff API</title>
<link rel="icon" type="image/png" href="https://www.splitsp.lat/logos/sportspuff/sportspuff-logo.png">
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{
  font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,'Helvetica Neue',Arial,sans-serif;
  background:linear-gradient(135deg,#1A0B3D 0%,#2D1B69 50%,#3D2A7A 100%);
  color:#F5F5F5;
  min-height:100vh;
  display:flex;
  flex-direction:column;
  align-items:center;
}
header{
  width:100%;
  background:linear-gradient(135deg,#2D1B69 0%,#FF3B30 100%);
  padding:2rem 0;
  text-align:center;
  box-shadow:0 2px 10px rgba(26,42,108,0.3);
}
header img{height:120px;margin-bottom:0.5rem}
header h1{font-size:2.5rem;font-weight:700;text-shadow:2px 2px 4px rgba(0,0,0,0.5)}
header p{font-size:1.1rem;color:rgba(245,245,245,0.8);margin-top:0.25rem}
.container{
  max-width:800px;width:100%;
  padding:2rem;margin:2rem auto;
  background:rgba(26,11,61,0.9);
  border-radius:20px;
  border:1px solid rgba(255,255,255,0.2);
  box-shadow:0 10px 30px rgba(0,0,0,0.4);
}
h2{font-size:1.5rem;font-weight:700;margin-bottom:1rem;color:#FFB400}
.blurb{font-size:1rem;line-height:1.6;margin-bottom:2rem;color:#B8B8B8}
.blurb a{color:#FF3B30;text-decoration:none}
.blurb a:hover{text-decoration:underline}
.section{margin-bottom:2rem}
.endpoint-group h3{
  font-size:1.1rem;font-weight:600;margin:1.25rem 0 0.5rem;
  padding-bottom:0.25rem;border-bottom:2px solid rgba(255,255,255,0.1);
}
table{width:100%;border-collapse:collapse}
td{padding:0.35rem 0.5rem;vertical-align:top;font-size:0.9rem}
td:first-child{white-space:nowrap}
td a{
  color:#F5F5F5;text-decoration:none;
  font-family:'SF Mono',SFMono-Regular,Consolas,'Liberation Mono',Menlo,monospace;
  font-size:0.85rem;
  padding:0.15rem 0.4rem;
  background:rgba(255,255,255,0.08);
  border-radius:4px;
  transition:all 0.2s ease;
}
td a:hover{background:rgba(255,59,48,0.25);color:#fff}
td:last-child{color:#B8B8B8}
.tag{
  display:inline-block;font-size:0.7rem;font-weight:600;
  padding:0.1rem 0.45rem;border-radius:10px;margin-left:0.4rem;
  vertical-align:middle;
}
.tag-json{background:rgba(112,40,228,0.3);color:#c4a0ff}
.tag-text{background:rgba(255,180,0,0.2);color:#FFB400}
footer{
  text-align:center;padding:2rem 1rem;
  font-size:0.8rem;color:rgba(245,245,245,0.4);
}
@media(max-width:600px){
  header img{height:80px}
  header h1{font-size:1.8rem}
  .container{margin:1rem;padding:1.25rem;border-radius:14px}
  td a{font-size:0.78rem}
}
</style>
</head>
<body>
<header>
  <img src="https://www.splitsp.lat/logos/sportspuff/sportspuff-logo.png" alt="SportsPuff"
       onerror="this.style.display='none'">
  <h1>SportsPuff API</h1>
  <p>v1.0.0</p>
</header>

<div class="container">
  <div class="section">
    <h2>About</h2>
    <p class="blurb">
      This is the API backend for
      <a href="https://www.sportspuff.org">www.sportspuff.org</a>.
      It serves live scores, schedules, standings, and season info across
      <strong>12 sports/leagues</strong>:
      MLB, NBA, NFL, NHL, WNBA, MLS, IPL, MLC, FIFA World Cup, ATP, WTA, and the UCI World Tour (Tour de France, Giro, Vuelta, classics).
      Canonical API routes live under <strong>/v1</strong> and negotiate JSON vs plain text with the
      <code>Accept</code> header. Legacy <code>/api/v1</code> and <code>/curl/v1</code> routes remain
      available as compatibility shims.
      Interactive API docs at <a href="/docs">/docs</a> and <a href="/redoc">/redoc</a>.
    </p>
  </div>

  <div class="section endpoint-group">
    <h2>Sports</h2>
    <table>
      <thead>
        <tr>
          <th style="text-align:left">League</th>
          <th>Slug</th>
          <th>Scores</th>
          <th>Schedule</th>
          <th>Standings</th>
          <th>Season info</th>
        </tr>
      </thead>
      <tbody>
        <tr><td>MLB</td><td><code>mlb</code></td><td>✓</td><td>✓</td><td>✓</td><td>✓</td></tr>
        <tr><td>NBA</td><td><code>nba</code></td><td>✓</td><td>✓</td><td>✓</td><td>✓</td></tr>
        <tr><td>NFL</td><td><code>nfl</code></td><td>✓</td><td>✓</td><td>✓</td><td>✓</td></tr>
        <tr><td>NHL</td><td><code>nhl</code></td><td>✓</td><td>✓</td><td>✓</td><td>✓</td></tr>
        <tr><td>WNBA</td><td><code>wnba</code></td><td>✓</td><td>✓</td><td>✓</td><td>✓</td></tr>
        <tr><td>MLS</td><td><code>mls</code></td><td>✓</td><td>✓</td><td>✓</td><td>✓</td></tr>
        <tr><td>IPL (cricket)</td><td><code>ipl</code></td><td>✓</td><td>✓</td><td>✓</td><td>✓</td></tr>
        <tr><td>MLC (cricket)</td><td><code>mlc</code></td><td>✓</td><td>✓</td><td>✓</td><td>✓</td></tr>
        <tr><td>FIFA World Cup</td><td><code>wc</code></td><td>✓</td><td>✓</td><td>✓ (group stage)</td><td>✓</td></tr>
        <tr><td>ATP tennis</td><td><code>atp</code></td><td>fixture only</td><td>✓</td><td>n/a</td><td>tour calendar</td></tr>
        <tr><td>WTA tennis</td><td><code>wta</code></td><td>fixture only</td><td>✓</td><td>n/a</td><td>tour calendar</td></tr>
        <tr><td>UCI World Tour (cycling)</td><td><code>cycling</code></td><td>calendar + results fields</td><td>✓</td><td>GC when configured</td><td>race calendar</td></tr>
      </tbody>
    </table>
    <p class="blurb" style="margin-top:1rem;font-size:0.85rem">
      Tennis still has fixture-only coverage upstream. Cycling now surfaces stage winner and GC rank fields when the configured collector provides them; standings are available when the source exposes GC data.
    </p>
  </div>

  <div class="section endpoint-group">
    <h2>Endpoints</h2>

    <h3>Scores</h3>
    <table>
      <tr>
        <td><a href="/v1/scores/today">/v1/scores/today</a> <span class="tag tag-json">CANONICAL</span></td>
        <td>All sports; use <code>Accept</code> to choose JSON or text</td>
      </tr>
      <tr>
        <td><a href="/v1/scores/nba/today">/v1/scores/{sport}/today</a> <span class="tag tag-json">CANONICAL</span></td>
        <td>Single sport; use <code>Accept</code> to choose JSON or text</td>
      </tr>
      <tr>
        <td><a href="/api/v1/scores/today">/api/v1/scores/today</a> <span class="tag tag-json">JSON</span></td>
        <td>Legacy JSON compatibility route</td>
      </tr>
      <tr>
        <td><a href="/api/v1/scores/nba/today">/api/v1/scores/{sport}/today</a> <span class="tag tag-json">JSON</span></td>
        <td>Legacy JSON compatibility route</td>
      </tr>
      <tr>
        <td><a href="/curl/v1/scores/today">/curl/v1/scores/today</a> <span class="tag tag-text">TEXT</span></td>
        <td>Legacy plain-text compatibility route</td>
      </tr>
      <tr>
        <td><a href="/curl/v1/scores/nba/today">/curl/v1/scores/{sport}/today</a> <span class="tag tag-text">TEXT</span></td>
        <td>Legacy plain-text compatibility route</td>
      </tr>
    </table>

    <h3>Schedules</h3>
    <table>
      <tr>
        <td><a href="/v1/schedules/today">/v1/schedules/today</a> <span class="tag tag-json">CANONICAL</span></td>
        <td>All sports; use <code>Accept</code> to choose JSON or text</td>
      </tr>
      <tr>
        <td><a href="/v1/schedule/nba/today">/v1/schedule/{sport}/today</a> <span class="tag tag-json">CANONICAL</span></td>
        <td>Single sport; use <code>Accept</code> to choose JSON or text</td>
      </tr>
      <tr>
        <td><a href="/api/v1/schedules/today">/api/v1/schedules/today</a> <span class="tag tag-json">JSON</span></td>
        <td>Legacy JSON compatibility route</td>
      </tr>
      <tr>
        <td><a href="/api/v1/schedule/nba/today">/api/v1/schedule/{sport}/today</a> <span class="tag tag-json">JSON</span></td>
        <td>Legacy JSON compatibility route</td>
      </tr>
      <tr>
        <td><a href="/curl/v1/schedules/today">/curl/v1/schedules/today</a> <span class="tag tag-text">TEXT</span></td>
        <td>Legacy plain-text compatibility route</td>
      </tr>
      <tr>
        <td><a href="/curl/v1/schedule/nba/today">/curl/v1/schedule/{sport}/today</a> <span class="tag tag-text">TEXT</span></td>
        <td>Legacy plain-text compatibility route</td>
      </tr>
    </table>

    <h3>Standings</h3>
    <table>
      <tr>
        <td><a href="/v1/standings/nba">/v1/standings/{sport}</a> <span class="tag tag-json">CANONICAL</span></td>
        <td>Use <code>Accept</code> to choose JSON or text</td>
      </tr>
      <tr>
        <td><a href="/api/v1/standings/nba">/api/v1/standings/{sport}</a> <span class="tag tag-json">JSON</span></td>
        <td>Legacy JSON compatibility route</td>
      </tr>
      <tr>
        <td><a href="/curl/v1/standings/nba">/curl/v1/standings/{sport}</a> <span class="tag tag-text">TEXT</span></td>
        <td>Legacy plain-text compatibility route</td>
      </tr>
    </table>

    <h3>Season Info</h3>
    <table>
      <tr>
        <td><a href="/v1/season-info/mlb">/v1/season-info/{league}</a> <span class="tag tag-json">CANONICAL</span></td>
        <td>Season phase dates, current phase, last champion (when known)</td>
      </tr>
      <tr>
        <td><a href="/api/v1/season-info/mlb">/api/v1/season-info/{league}</a> <span class="tag tag-json">JSON</span></td>
        <td>Legacy JSON compatibility route</td>
      </tr>
    </table>

    <h3>Status &amp; docs</h3>
    <table>
      <tr>
        <td><a href="/v1/status">/v1/status</a> <span class="tag tag-json">CANONICAL</span></td>
        <td>Upstream + endpoint health snapshot; use <code>Accept</code> to choose JSON or text</td>
      </tr>
      <tr>
        <td><a href="/api/v1/status">/api/v1/status</a> <span class="tag tag-json">JSON</span></td>
        <td>Legacy JSON compatibility route</td>
      </tr>
      <tr>
        <td><a href="/status">/status</a></td>
        <td>HTML status page</td>
      </tr>
      <tr>
        <td><a href="/curl/v1/status">/curl/v1/status</a> <span class="tag tag-text">TEXT</span></td>
        <td>Legacy plain-text compatibility route</td>
      </tr>
      <tr>
        <td><a href="/docs">/docs</a></td>
        <td>Interactive API docs (Swagger UI)</td>
      </tr>
      <tr>
        <td><a href="/redoc">/redoc</a></td>
        <td>Reference API docs (ReDoc)</td>
      </tr>
      <tr>
        <td><a href="/openapi.json">/openapi.json</a> <span class="tag tag-json">JSON</span></td>
        <td>OpenAPI 3 spec (for client codegen)</td>
      </tr>
    </table>

    <h3>Help</h3>
    <table>
      <tr>
        <td><a href="/help">/help</a></td>
        <td>Full endpoint reference (HTML)</td>
      </tr>
      <tr>
        <td><a href="/api/help">/api/help</a> <span class="tag tag-json">JSON</span></td>
        <td>Full endpoint reference</td>
      </tr>
      <tr>
        <td><a href="/curl/help">/curl/help</a> <span class="tag tag-text">TEXT</span></td>
        <td>Full endpoint reference</td>
      </tr>
    </table>
  </div>

  <div class="section">
    <h2>Usage</h2>
    <p class="blurb">
      Canonical endpoints live under <code style="background:rgba(255,255,255,0.08);padding:0.1rem 0.4rem;border-radius:4px">/v1</code>
      and negotiate format via the <code style="background:rgba(255,255,255,0.08);padding:0.1rem 0.4rem;border-radius:4px">Accept</code> header.
      Legacy <code style="background:rgba(255,255,255,0.08);padding:0.1rem 0.4rem;border-radius:4px">/api/v1</code> and
      <code style="background:rgba(255,255,255,0.08);padding:0.1rem 0.4rem;border-radius:4px">/curl/v1</code> routes remain available as compatibility shims.
    </p>
    <p class="blurb">
      Replace <code style="background:rgba(255,255,255,0.08);padding:0.1rem 0.4rem;border-radius:4px">today</code>
      with <code style="background:rgba(255,255,255,0.08);padding:0.1rem 0.4rem;border-radius:4px">yesterday</code>,
      <code style="background:rgba(255,255,255,0.08);padding:0.1rem 0.4rem;border-radius:4px">tomorrow</code>,
      or a date like <code style="background:rgba(255,255,255,0.08);padding:0.1rem 0.4rem;border-radius:4px">2026-04-28</code>.
      Sports: <code style="background:rgba(255,255,255,0.08);padding:0.1rem 0.4rem;border-radius:4px">mlb</code>,
      <code style="background:rgba(255,255,255,0.08);padding:0.1rem 0.4rem;border-radius:4px">nba</code>,
      <code style="background:rgba(255,255,255,0.08);padding:0.1rem 0.4rem;border-radius:4px">nfl</code>,
      <code style="background:rgba(255,255,255,0.08);padding:0.1rem 0.4rem;border-radius:4px">nhl</code>,
      <code style="background:rgba(255,255,255,0.08);padding:0.1rem 0.4rem;border-radius:4px">wnba</code>,
      <code style="background:rgba(255,255,255,0.08);padding:0.1rem 0.4rem;border-radius:4px">ipl</code>,
      <code style="background:rgba(255,255,255,0.08);padding:0.1rem 0.4rem;border-radius:4px">mlc</code>,
      or <code style="background:rgba(255,255,255,0.08);padding:0.1rem 0.4rem;border-radius:4px">all</code>.
      Add <code style="background:rgba(255,255,255,0.08);padding:0.1rem 0.4rem;border-radius:4px">?tz=et</code>
      for Eastern time (default is Pacific).
    </p>
  </div>
</div>

<footer>SportsPuff &mdash; sportspuff.org</footer>
</body>
</html>""".replace(SPORTSPUFF_LOGO_REMOTE, SPORTSPUFF_LOGO_URL)


@app.get("/help", response_class=HTMLResponse)
def help_html(request: Request):
    """HTML formatted help page."""
    base_url = _request_base_url(request)
    return get_help_html(base_url).replace(
        SPORTSPUFF_LOGO_REMOTE,
        SPORTSPUFF_LOGO_URL,
    ).replace("__BASE_URL__", base_url)


@app.get("/api/help")
def help_api(request: Request):
    """JSON formatted help."""
    return get_help_json(_request_base_url(request))


@app.get("/api/v1/help")
def help_api_v1(request: Request):
    """JSON formatted help."""
    return get_help_json(_request_base_url(request))


@app.get("/curl/help", response_class=PlainTextResponse)
def help_curl(request: Request):
    """Plain text formatted help."""
    return get_help_text(_request_base_url(request))


@app.get("/curl/v1/help", response_class=PlainTextResponse)
def help_curl_v1(request: Request):
    """Plain text formatted help."""
    return get_help_text(_request_base_url(request))


@app.get("/v1/help")
def help_v1(request: Request, *, response: Response = None):
    """Canonical help endpoint negotiated via Accept."""
    if _client_prefers_plain_text(request):
        return _add_vary_accept(PlainTextResponse(get_help_text(_request_base_url(request))))
    if response is not None:
        _add_vary_accept(response)
    return get_help_json(_request_base_url(request))


@app.get("/v1/schedules/{date}")
def get_schedules_all_sports_v1(
    request: Request,
    date: str = Path(..., description="Date: today/tomorrow/yesterday, YYYY-MM-DD, YYYYMMDD, M/D/YYYY, MM/DD/YYYY, or other formats"),
    tz: Optional[str] = Query(None, description="Timezone for relative dates (default: Pacific)"),
    *,
    response: Response = None,
):
    """Canonical all-sports schedule endpoint negotiated via Accept."""
    if _client_prefers_plain_text(request):
        return _add_vary_accept(PlainTextResponse(get_schedules_all_sports_curl_v1(date, tz)))
    if response is not None:
        _add_vary_accept(response)
    return get_schedules_all_sports_api_v1(date, tz)


@app.get("/v1/schedules/all/{date}")
def get_schedules_all_sports_v1_compat(
    request: Request,
    date: str = Path(..., description="Date: today/tomorrow/yesterday, YYYY-MM-DD, YYYYMMDD, M/D/YYYY, MM/DD/YYYY, or other formats"),
    tz: Optional[str] = Query(None, description="Timezone for relative dates (default: Pacific)"),
    *,
    response: Response = None,
):
    """Legacy alias for clients that send /v1/schedules/all/{date}."""
    return get_schedules_all_sports_v1(request, date, tz, response=response)


@app.get("/v1/scores/{date}")
def get_scores_all_sports_v1(
    request: Request,
    date: str = Path(..., description="Date: today/tomorrow/yesterday, YYYY-MM-DD, YYYYMMDD, M/D/YYYY, MM/DD/YYYY, or other formats"),
    tz: Optional[str] = Query(None, description="Timezone for relative dates (default: Pacific)"),
    *,
    response: Response = None,
):
    """Canonical all-sports scores endpoint negotiated via Accept."""
    if _client_prefers_plain_text(request):
        return _add_vary_accept(PlainTextResponse(get_scores_all_sports_curl_v1(date, tz)))
    if response is not None:
        _add_vary_accept(response)
    return get_scores_all_sports_api_v1(date, tz)


@app.get("/v1/scores/all/{date}")
def get_scores_all_sports_v1_compat(
    request: Request,
    date: str = Path(..., description="Date: today/tomorrow/yesterday, YYYY-MM-DD, YYYYMMDD, M/D/YYYY, MM/DD/YYYY, or other formats"),
    tz: Optional[str] = Query(None, description="Timezone for relative dates (default: Pacific)"),
    *,
    response: Response = None,
):
    """Legacy alias for clients that send /v1/scores/all/{date}."""
    return get_scores_all_sports_v1(request, date, tz, response=response)


@app.get("/api/v1/schedules/{date}", response_model=schemas.AllSportsScheduleResponse)
def get_schedules_all_sports_api_v1(
    date: str = Path(..., description="Date: today/tomorrow/yesterday, YYYY-MM-DD, YYYYMMDD, M/D/YYYY, MM/DD/YYYY, or other formats"),
    tz: Optional[str] = Query(None, description="Timezone for relative dates (default: Pacific)"),
):
    """Get schedules for all sports in JSON format."""
    try:
        timezone = get_timezone(tz)
        target_date = parse_date_param(date, timezone)
        sport_games = _get_all_sport_games(target_date, timezone)
        result = {
            sport_key: [_game_wrapper_to_dict(g, SPORT_MAPPINGS[sport_key]) for g in games]
            for sport_key, games in sport_games.items()
        }

        return {
            "date": target_date.isoformat(),
            "sports": result
        }
    except Exception as e:
        return _internal_error_response("/api/v1/schedules/{date}", e)


@app.get("/api/v1/schedules/all/{date}", response_model=schemas.AllSportsScheduleResponse)
def get_schedules_all_sports_api_v1_compat(
    date: str = Path(..., description="Date: today/tomorrow/yesterday, YYYY-MM-DD, YYYYMMDD, M/D/YYYY, MM/DD/YYYY, or other formats"),
    tz: Optional[str] = Query(None, description="Timezone for relative dates (default: Pacific)"),
):
    """Legacy alias for clients that send /api/v1/schedules/all/{date}."""
    return get_schedules_all_sports_api_v1(date, tz)


@app.get("/curl/v1/schedules/{date}", response_class=PlainTextResponse)
def get_schedules_all_sports_curl_v1(
    date: str = Path(..., description="Date: today/tomorrow/yesterday, YYYY-MM-DD, YYYYMMDD, M/D/YYYY, MM/DD/YYYY, or other formats"),
    tz: Optional[str] = Query(None, description="Timezone: et/est/eastern, pt/pst/pdt/pacific (default: Pacific)"),
):
    """Get schedules for all sports in curl-style text format."""
    try:
        timezone = get_timezone(tz)
        target_date = parse_date_param(date, timezone)

        sport_games = _get_all_sport_games(target_date, timezone)
        for sport_key, games in sport_games.items():
            _enrich_curl_wrappers(sport_key, target_date, games)
        all_games = []
        for games in sport_games.values():
            all_games.extend(games)

        return format_schedule_curl(all_games, target_date, timezone)

    except Exception as e:
        return _internal_error_response("/curl/v1/schedules/{date}", e, plain_text=True)


@app.get("/curl/v1/schedules/all/{date}", response_class=PlainTextResponse)
def get_schedules_all_sports_curl_v1_compat(
    date: str = Path(..., description="Date: today/tomorrow/yesterday, YYYY-MM-DD, YYYYMMDD, M/D/YYYY, MM/DD/YYYY, or other formats"),
    tz: Optional[str] = Query(None, description="Timezone: et/est/eastern, pt/pst/pdt/pacific (default: Pacific)"),
):
    """Legacy alias for clients that send /curl/v1/schedules/all/{date}."""
    return get_schedules_all_sports_curl_v1(date, tz)


@app.get("/api/v1/scores/{date}", response_model=schemas.AllSportsScoresResponse)
def get_scores_all_sports_api_v1(
    date: str = Path(..., description="Date: today/tomorrow/yesterday, YYYY-MM-DD, YYYYMMDD, M/D/YYYY, MM/DD/YYYY, or other formats"),
    tz: Optional[str] = Query(None, description="Timezone for relative dates (default: Pacific)"),
):
    """Get scores for all sports in JSON format."""
    try:
        timezone = get_timezone(tz)
        target_date = parse_date_param(date, timezone)
        sport_games = _get_all_sport_games(target_date, timezone)
        result = {
            sport_key: [_game_wrapper_to_dict(g, SPORT_MAPPINGS[sport_key]) for g in games]
            for sport_key, games in sport_games.items()
        }

        return {
            "date": target_date.isoformat(),
            "sports": result
        }
    except Exception as e:
        return _internal_error_response("/api/v1/scores/{date}", e)


@app.get("/api/v1/scores/all/{date}", response_model=schemas.AllSportsScoresResponse)
def get_scores_all_sports_api_v1_compat(
    date: str = Path(..., description="Date: today/tomorrow/yesterday, YYYY-MM-DD, YYYYMMDD, M/D/YYYY, MM/DD/YYYY, or other formats"),
    tz: Optional[str] = Query(None, description="Timezone for relative dates (default: Pacific)"),
):
    """Legacy alias for clients that send /api/v1/scores/all/{date}."""
    return get_scores_all_sports_api_v1(date, tz)


@app.get("/curl/v1/scores/{date}", response_class=PlainTextResponse)
def get_scores_all_sports_curl_v1(
    date: str = Path(..., description="Date: today/tomorrow/yesterday, YYYY-MM-DD, YYYYMMDD, M/D/YYYY, MM/DD/YYYY, or other formats"),
    tz: Optional[str] = Query(None, description="Timezone: et/est/eastern, pt/pst/pdt/pacific (default: Pacific)"),
):
    """Get scores for all sports in curl-style text format."""
    try:
        timezone = get_timezone(tz)
        target_date = parse_date_param(date, timezone)

        sport_games = _get_all_sport_games(target_date, timezone)
        for sport_key, games in sport_games.items():
            _enrich_curl_wrappers(sport_key, target_date, games)
        all_games = []
        for games in sport_games.values():
            all_games.extend(games)

        return format_scores_curl(all_games, target_date, timezone)

    except Exception as e:
        return _internal_error_response("/curl/v1/scores/{date}", e, plain_text=True)


@app.get("/curl/v1/scores/all/{date}", response_class=PlainTextResponse)
def get_scores_all_sports_curl_v1_compat(
    date: str = Path(..., description="Date: today/tomorrow/yesterday, YYYY-MM-DD, YYYYMMDD, M/D/YYYY, MM/DD/YYYY, or other formats"),
    tz: Optional[str] = Query(None, description="Timezone: et/est/eastern, pt/pst/pdt/pacific (default: Pacific)"),
):
    """Legacy alias for clients that send /curl/v1/scores/all/{date}."""
    return get_scores_all_sports_curl_v1(date, tz)


@app.get("/api/v1/schedule/{sport}/{date}", response_model=schemas.ScheduleResponse)
def get_schedule_api_v1(
    sport: str = Path(..., description="Sport (nba, mlb, nfl, nhl, wnba, all)"),
    date: str = Path(..., description="Date: today/tomorrow/yesterday, YYYY-MM-DD, YYYYMMDD, M/D/YYYY, MM/DD/YYYY, or other formats"),
    tz: Optional[str] = Query(None, description="Timezone for relative dates (default: Pacific)"),
):
    """Get schedule in JSON format."""
    try:
        timezone = get_timezone(tz)
        target_date = parse_date_param(date, timezone)
        sport_lower = sport.lower()

        if sport_lower == 'all':
            sport_games = _get_all_sport_games(target_date, timezone)
            all_games = []
            by_sport: Dict[str, List[Dict[str, Any]]] = {}
            for sport_key, games in sport_games.items():
                for g in games:
                    d = _game_wrapper_to_dict(g, SPORT_MAPPINGS[sport_key])
                    d['sport'] = sport_key
                    all_games.append(d)
                    by_sport.setdefault(sport_key, []).append(d)
            for sport_key, batch in by_sport.items():
                _apply_dict_enrichers(sport_key, batch, target_date)
            all_games.sort(key=lambda x: x.get('game_time') or '')
            return {"sport": "all", "date": target_date.isoformat(), "games": all_games}

        league = SPORT_MAPPINGS.get(sport_lower)
        if not league:
            raise HTTPException(status_code=400, detail=f"Invalid sport: {sport}")

        games, cache_snapshot = _get_games_for_curl(league, target_date, timezone, include_metadata=True)
        games_out = [_game_wrapper_to_dict(g, league) for g in games]
        _apply_dict_enrichers(sport_lower, games_out, target_date)
        return {
            "sport": sport,
            "date": target_date.isoformat(),
            "games": games_out,
            "meta": _build_endpoint_meta(
                cache_snapshot,
                _schedule_freshness_seconds(target_date, timezone),
                empty_state=cache_snapshot.get("empty_state"),
            ),
        }
    except Exception as e:
        return _internal_error_response("/api/v1/schedule/{sport}/{date}", e)

def _get_schedule_for_league(league: str, target_date: date, timezone: pytz.BaseTzInfo) -> List[Dict[str, Any]]:
    """Helper function to get schedule for a specific league."""
    now_tz = datetime.now(timezone)
    today = now_tz.date()
    games_list = []
    
    collector = get_collector(league)
    set_collector_timezone(collector, timezone)
    
    # For today's games, try to get live data first
    # But only use live_scores if we actually have games for today
    # (get_live_scores may include yesterday's games that are still in progress)
    if target_date == today:
        if collector:
            # First try get_schedule to get today's scheduled games
            schedule_games = collector.get_schedule(target_date)
            
            # Handle both list and dict formats from NBA collector
            if isinstance(schedule_games, dict) and 'leagueSchedule' in schedule_games:
                # NBA collector sometimes returns dict format
                game_dates = schedule_games['leagueSchedule'].get('gameDates', [])
                schedule_games = []
                for gd in game_dates:
                    games = gd.get('games', [])
                    schedule_games.extend(games)
            
            if schedule_games:
                # We have scheduled games for today, use those
                seen_game_ids = set()
                for game_dict in schedule_games:
                    game_id = game_dict.get('game_id', '')
                    if game_id and game_id in seen_game_ids:
                        continue
                    seen_game_ids.add(game_id)
                    
                    game_time = game_dict.get('game_time')
                    game_date_str = game_dict.get('game_date', '')
                    
                    # For NBA, convert game_date to Pacific timezone if game_time is available
                    # (game_date from collector may be UTC date, but we want Pacific date)
                    if league.upper() == 'NBA' and game_time:
                        try:
                            if isinstance(game_time, str):
                                from dateutil import parser
                                game_time_obj = parser.parse(game_time)
                            elif hasattr(game_time, 'isoformat'):
                                # Already a datetime object
                                game_time_obj = game_time
                            else:
                                game_time_obj = None
                            
                            if game_time_obj:
                                if game_time_obj.tzinfo is None:
                                    game_time_obj = pytz.UTC.localize(game_time_obj)
                                pacific_tz = pytz.timezone('US/Pacific')
                                game_time_pacific = game_time_obj.astimezone(pacific_tz)
                                game_date_str = game_time_pacific.date().isoformat()
                        except Exception as e:
                            logger.debug(f"Error converting NBA game_date to Pacific: {e}")
                            # Keep original game_date_str
                    
                    games_list.append({
                        "game_id": game_id,
                        "game_date": game_date_str if game_date_str else target_date.isoformat(),
                        "game_time": game_time.isoformat() if game_time else None,
                        "home_team": game_dict.get('home_team', ''),
                        "home_team_abbrev": game_dict.get('home_team_abbrev', ''),
                        "visitor_team": game_dict.get('visitor_team', ''),
                        "visitor_team_abbrev": game_dict.get('visitor_team_abbrev', ''),
                        "game_status": game_dict.get('game_status', 'scheduled'),
                        "game_type": game_dict.get('game_type', 'regular'),
                        "home_wins": game_dict.get('home_wins', 0),
                        "home_losses": game_dict.get('home_losses', 0),
                        "home_otl": game_dict.get('home_otl', 0) if league.upper() == 'NHL' else None,
                        "visitor_wins": game_dict.get('visitor_wins', 0),
                        "visitor_losses": game_dict.get('visitor_losses', 0),
                        "visitor_otl": game_dict.get('visitor_otl', 0) if league.upper() == 'NHL' else None,
                        "current_period": game_dict.get('current_period', ''),
                        "time_remaining": game_dict.get('time_remaining', ''),
                        "home_score_total": game_dict.get('home_score_total', 0),
                        "visitor_score_total": game_dict.get('visitor_score_total', 0),
                        "is_final": game_dict.get('is_final', False),
                    })
            
            # If no scheduled games, try live_scores (may include in-progress games from yesterday)
            # If no scheduled games, try live_scores as fallback
            # For NBA, we'll filter by date to ensure games are actually for today
            if not games_list:
                live_games = collector.get_live_scores(target_date)
                if live_games:
                        # Filter live games to ensure they're actually for today (Pacific time)
                        # This is especially important for NBA where get_live_scores may return games from other dates
                        seen_game_ids = set()
                        pacific_tz = pytz.timezone('US/Pacific')
                        target_date_str = target_date.strftime('%Y-%m-%d')
                        
                        for game_dict in live_games:
                            game_id = game_dict.get('game_id', '')
                            if game_id and game_id in seen_game_ids:
                                continue
                            
                            # For NBA, verify the game is actually for today by checking both game_date and game_time
                            # The game_date field should match, and game_time should be for today in Pacific time
                            if league.upper() == 'NBA':
                                game_date_from_dict = game_dict.get('game_date', '')
                                game_time = game_dict.get('game_time')
                            
                                # First check: game_date should match target_date (this is the actual scheduled date)
                                # If game_date is "2025-11-13" but target is "2025-11-12", skip it
                                if game_date_from_dict:
                                    # Normalize game_date (remove time if present)
                                    game_date_normalized = game_date_from_dict.split()[0] if ' ' in game_date_from_dict else game_date_from_dict
                                    if game_date_normalized != target_date_str:
                                        # Skip games that don't match the target date
                                        continue
                            
                                # Second check: game_time should also be for today in Pacific time
                                if game_time:
                                    try:
                                        from dateutil import parser
                                        if isinstance(game_time, str):
                                            game_time_obj = parser.parse(game_time)
                                        elif hasattr(game_time, 'isoformat'):
                                            game_time_obj = game_time
                                        else:
                                            # Can't parse game_time, skip this game
                                            continue
                                        
                                        if game_time_obj.tzinfo is None:
                                            game_time_obj = pytz.UTC.localize(game_time_obj)
                                        game_time_pacific = game_time_obj.astimezone(pacific_tz)
                                        game_date_pacific = game_time_pacific.date().strftime('%Y-%m-%d')
                                        
                                        # Only include if the game is actually for today
                                        if game_date_pacific != target_date_str:
                                            continue
                                    except Exception as e:
                                        continue
                                elif game_date_from_dict and game_date_from_dict != target_date_str:
                                    # If no game_time but game_date doesn't match, skip
                                    logger.debug(f"Skipping NBA game {game_id} - game_date {game_date_from_dict} doesn't match target {target_date_str}")
                                    continue
                            
                            seen_game_ids.add(game_id)
                            
                            # Get game_time from live data if available
                            game_time = game_dict.get('game_time')
                            game_date_str = game_dict.get('game_date', '')
                            
                            # If game_time is not in live data, try to get it from database
                            if not game_time:
                                with get_db_session() as db:
                                    db_game = db.query(Game).filter(
                                        Game.game_id == game_id
                                    ).first()
                                    if db_game and db_game.game_time:
                                        game_time = db_game.game_time
                            
                            games_list.append({
                                "game_id": game_id,
                                "game_date": game_date_str if game_date_str else target_date.isoformat(),
                                "game_time": game_time.isoformat() if hasattr(game_time, 'isoformat') else (str(game_time) if game_time else None),
                                "home_team": game_dict.get('home_team', ''),
                                "home_team_abbrev": game_dict.get('home_team_abbrev', ''),
                                "visitor_team": game_dict.get('visitor_team', ''),
                                "visitor_team_abbrev": game_dict.get('visitor_team_abbrev', ''),
                                "game_status": game_dict.get('game_status', 'scheduled'),
                                "game_type": game_dict.get('game_type', 'regular'),
                                "home_wins": game_dict.get('home_wins', 0),
                                "home_losses": game_dict.get('home_losses', 0),
                                "home_otl": game_dict.get('home_otl', 0) if league.upper() == 'NHL' else None,
                                "visitor_wins": game_dict.get('visitor_wins', 0),
                                "visitor_losses": game_dict.get('visitor_losses', 0),
                                "visitor_otl": game_dict.get('visitor_otl', 0) if league.upper() == 'NHL' else None,
                                "current_period": game_dict.get('current_period', ''),
                                "time_remaining": game_dict.get('time_remaining', ''),
                                "home_score_total": game_dict.get('home_score_total', 0),
                                "visitor_score_total": game_dict.get('visitor_score_total', 0),
                                "is_final": game_dict.get('is_final', False),
                                "cricket_home_score": game_dict.get('cricket_home_score', ''),
                                "cricket_away_score": game_dict.get('cricket_away_score', ''),
                                "cricket_winner": game_dict.get('cricket_winner', ''),
                                "cricket_result": game_dict.get('cricket_result', ''),
                            })
    
    # For any date (today or past), try get_schedule from collector
    # Also check database for stored games if collector returns nothing
    if not games_list:
        if collector:
            schedule_games = collector.get_schedule(target_date)
            if schedule_games:
                seen_game_ids = set()
                for game_dict in schedule_games:
                    game_id = game_dict.get('game_id', '')
                    if game_id and game_id in seen_game_ids:
                        continue
                    seen_game_ids.add(game_id)
                
                game_time = game_dict.get('game_time')
                game_date_str = game_dict.get('game_date', '')
                
                # For NBA, convert game_date to Pacific timezone if game_time is available
                # (game_date from collector may be UTC date, but we want Pacific date)
                if league.upper() == 'NBA' and game_time:
                    try:
                        if isinstance(game_time, str):
                            from dateutil import parser
                            game_time_obj = parser.parse(game_time)
                        elif hasattr(game_time, 'isoformat'):
                            # Already a datetime object
                            game_time_obj = game_time
                        else:
                            game_time_obj = None
                        
                        if game_time_obj:
                            if game_time_obj.tzinfo is None:
                                game_time_obj = pytz.UTC.localize(game_time_obj)
                            pacific_tz = pytz.timezone('US/Pacific')
                            game_time_pacific = game_time_obj.astimezone(pacific_tz)
                            game_date_str = game_time_pacific.date().isoformat()
                    except Exception as e:
                        logger.debug(f"Error converting NBA game_date to Pacific: {e}")
                        # Keep original game_date_str
                
                games_list.append({
                    "game_id": game_id,
                    "game_date": game_date_str if game_date_str else target_date.isoformat(),
                    "game_time": game_time.isoformat() if hasattr(game_time, 'isoformat') else (str(game_time) if game_time else None),
                    "home_team": game_dict.get('home_team', ''),
                    "home_team_abbrev": game_dict.get('home_team_abbrev', ''),
                    "visitor_team": game_dict.get('visitor_team', ''),
                    "visitor_team_abbrev": game_dict.get('visitor_team_abbrev', ''),
                    "game_status": game_dict.get('game_status', 'scheduled'),
                    "game_type": game_dict.get('game_type', 'regular'),
                    "home_wins": game_dict.get('home_wins', 0),
                    "home_losses": game_dict.get('home_losses', 0),
                    "home_otl": game_dict.get('home_otl', 0) if league.upper() == 'NHL' else None,
                    "visitor_wins": game_dict.get('visitor_wins', 0),
                    "visitor_losses": game_dict.get('visitor_losses', 0),
                    "visitor_otl": game_dict.get('visitor_otl', 0) if league.upper() == 'NHL' else None,
                    "current_period": game_dict.get('current_period', ''),
                    "time_remaining": game_dict.get('time_remaining', ''),
                    "home_score_total": game_dict.get('home_score_total', 0),
                    "visitor_score_total": game_dict.get('visitor_score_total', 0),
                    "is_final": game_dict.get('is_final', False),
                })
    
    # Fallback to database if no collector games found
    # For NBA, filter by Pacific timezone date since games may be stored with UTC dates
    if not games_list:
        with get_db_session() as db:
            if league.upper() == 'NBA':
                # For NBA, games may be stored with UTC dates but we want Pacific dates
                # Get games from target_date and target_date-1 (yesterday) to catch timezone edge cases
                from datetime import timedelta
                yesterday = target_date - timedelta(days=1)
                all_games = db.query(Game).filter(
                    Game.league == league,
                    Game.game_date.in_([target_date, yesterday])
                ).order_by(Game.game_time).all()
                
                # Filter by Pacific timezone date
                pacific_tz = pytz.timezone('US/Pacific')
                games = []
                for game in all_games:
                    if game.game_time:
                        # Convert game_time to Pacific and check date
                        if game.game_time.tzinfo is None:
                            # Assume UTC if no timezone
                            game_time_utc = pytz.UTC.localize(game.game_time)
                        else:
                            game_time_utc = game.game_time
                        game_time_pacific = game_time_utc.astimezone(pacific_tz)
                        if game_time_pacific.date() == target_date:
                            games.append(game)
                    elif game.game_date == target_date:
                        # If no game_time, use game_date (should match)
                        games.append(game)
            else:
                # For other leagues, use simple date match
                games = db.query(Game).filter(
                    Game.league == league,
                    Game.game_date == target_date
                ).order_by(Game.game_time).all()
            
            games_list = [
                {
                    "game_id": game.game_id,
                    "game_date": game.game_date.isoformat(),
                    "game_time": game.game_time.isoformat() if game.game_time else None,
                    "home_team": game.home_team,
                    "home_team_abbrev": game.home_team_abbrev,
                    "visitor_team": game.visitor_team,
                    "visitor_team_abbrev": game.visitor_team_abbrev,
                    "game_status": game.game_status,
                    "game_type": game.game_type,
                    "home_wins": game.home_wins or 0,
                    "home_losses": game.home_losses or 0,
                    "home_otl": game.home_otl if league.upper() == 'NHL' and hasattr(game, 'home_otl') else None,
                    "visitor_wins": game.visitor_wins or 0,
                    "visitor_losses": game.visitor_losses or 0,
                    "visitor_otl": game.visitor_otl if league.upper() == 'NHL' and hasattr(game, 'visitor_otl') else None,
                    "current_period": game.current_period or '',
                    "time_remaining": game.time_remaining or '',
                    "home_score_total": game.home_score_total or 0,
                    "visitor_score_total": game.visitor_score_total or 0,
                    "is_final": game.is_final or False,
                }
                for game in games
            ]
    
    return games_list

def _game_wrapper_to_dict(g, league: str = '') -> Dict[str, Any]:
    """Convert a GameWrapper to a JSON-serializable dict."""
    gt = getattr(g, 'game_time', None)
    gt_str = None
    if gt and hasattr(gt, 'isoformat'):
        gt_str = gt.isoformat()
    gd = getattr(g, 'game_date', '')
    gd_str = gd.isoformat() if hasattr(gd, 'isoformat') else str(gd)
    d = {
        "game_id": getattr(g, 'game_id', ''),
        "game_date": gd_str,
        "game_time": gt_str,
        "home_team": getattr(g, 'home_team', ''),
        "home_team_abbrev": getattr(g, 'home_team_abbrev', ''),
        "visitor_team": getattr(g, 'visitor_team', ''),
        "visitor_team_abbrev": getattr(g, 'visitor_team_abbrev', ''),
        "game_status": getattr(g, 'game_status', 'scheduled'),
        "game_type": getattr(g, 'game_type', 'regular'),
        "home_score": getattr(g, 'home_score_total', 0),
        "visitor_score": getattr(g, 'visitor_score_total', 0),
        "is_final": getattr(g, 'is_final', False),
        "current_period": getattr(g, 'current_period', ''),
        "time_remaining": getattr(g, 'time_remaining', ''),
        "home_wins": getattr(g, 'home_wins', 0),
        "home_losses": getattr(g, 'home_losses', 0),
        "home_otl": getattr(g, 'home_otl', None) if league == 'NHL' else None,
        "visitor_wins": getattr(g, 'visitor_wins', 0),
        "visitor_losses": getattr(g, 'visitor_losses', 0),
        "visitor_otl": getattr(g, 'visitor_otl', None) if league == 'NHL' else None,
        "home_period_scores": getattr(g, 'home_period_scores', None) or {},
        "visitor_period_scores": getattr(g, 'visitor_period_scores', None) or {},
        "home_shootout_score": getattr(g, 'home_shootout_score', None),
        "visitor_shootout_score": getattr(g, 'visitor_shootout_score', None),
    }
    if league == 'CYCLING':
        d["cycling_race"] = getattr(g, 'cycling_race', '') or ''
        d["cycling_stage_label"] = getattr(g, 'cycling_stage_label', '') or ''
        d["cycling_stage_number"] = getattr(g, 'cycling_stage_number', None)
        d["cycling_event_label"] = getattr(g, 'cycling_event_label', '') or ''
        d["cycling_country"] = getattr(g, 'cycling_country', '') or ''
        d["cycling_url"] = getattr(g, 'cycling_url', '') or ''
        d["cycling_url_label"] = getattr(g, 'cycling_url_label', '') or ''
        d["cycling_video"] = getattr(g, 'cycling_video', '') or ''
        d["cycling_winner"] = getattr(g, 'cycling_winner', '') or ''
        d["cycling_rank"] = getattr(g, 'cycling_rank', None)
        d["race_type"] = getattr(g, 'race_type', '') or ''
        d["start_city"] = getattr(g, 'start_city', '') or ''
        d["finish_city"] = getattr(g, 'finish_city', '') or ''
    if league == 'WC':
        d["wc_round"] = getattr(g, 'wc_round', '')
        d["wc_round_label"] = getattr(g, 'wc_round_label', '')
        d["wc_winner"] = getattr(g, 'wc_winner', '') or ''
    if league in ('IPL', 'MLC'):
        d["home_score"] = getattr(g, 'cricket_home_score', '') or ''
        d["visitor_score"] = getattr(g, 'cricket_away_score', '') or ''
        d["result"] = getattr(g, 'cricket_status', '') or ''
        d["venue"] = getattr(g, 'cricket_venue', '') or ''
        d["winner"] = getattr(g, 'cricket_winner', '') or ''
        d["home_no_result"] = getattr(g, 'cricket_home_nr', 0)
        d["visitor_no_result"] = getattr(g, 'cricket_away_nr', 0)
        start_time = getattr(g, 'cricket_start_time', {}) or {}
        if start_time:
            d["start_time"] = start_time
    if league == 'MLS':
        d["home_draws"] = getattr(g, 'home_draws', 0)
        d["visitor_draws"] = getattr(g, 'visitor_draws', 0)
    if league in ('ATP', 'WTA'):
        d["tennis_tournament"] = getattr(g, 'tennis_tournament', '') or ''
        d["tennis_match_label"] = getattr(g, 'tennis_match_label', '') or ''
        d["tennis_round"] = getattr(g, 'tennis_round', '') or ''
        d["tennis_country"] = getattr(g, 'tennis_country', '') or ''
        d["tennis_video"] = getattr(g, 'tennis_video', '') or ''
        d["home_full_name"] = getattr(g, 'home_full_name', '') or ''
        d["visitor_full_name"] = getattr(g, 'visitor_full_name', '') or ''
        d["home_seed"] = getattr(g, 'home_seed', None)
        d["visitor_seed"] = getattr(g, 'visitor_seed', None)
        d["home_rank"] = getattr(g, 'home_seed', None)
        d["visitor_rank"] = getattr(g, 'visitor_seed', None)
        # ESPN-sourced enrichment. None if match wasn't matched against ESPN.
        d["tennis_set_scores"] = getattr(g, 'tennis_set_scores', None)
        d["home_sets_won"] = getattr(g, 'home_sets_won', None)
        d["visitor_sets_won"] = getattr(g, 'visitor_sets_won', None)
        d["tennis_summary"] = getattr(g, 'tennis_summary', None)
        d["tennis_winner"] = getattr(g, 'tennis_winner', None)
    return d


def _apply_dict_enrichers(sport: str, games_dicts: list, target_date: date) -> list:
    """Run all dict-based enrichers (playoff_series, tennis_scores,
    box_score) on a list of game dicts. Each enricher is a no-op for sports
    it doesn't handle, so this can be called for any sport."""
    from .services.playoff_series import enrich_games as _enrich_playoff
    from .services.cricket_live_enricher import enrich_with_cricapi_live as _enrich_cricket
    from .services.tennis_scores import enrich_games as _enrich_tennis
    from .services.box_score import enrich_games as _enrich_box
    _enrich_playoff(sport, target_date, games_dicts)
    _enrich_cricket(sport, games_dicts, target_date)
    _enrich_tennis(sport, target_date, games_dicts)
    # Fill period dicts from ESPN before _apply_box_score derives the contract.
    _enrich_box(sport, target_date, games_dicts)
    _apply_tennis_contract(sport, games_dicts)
    _apply_box_score(sport, games_dicts)
    _apply_world_cup_team_records(sport, games_dicts)
    _apply_world_cup_winner(sport, games_dicts)
    return games_dicts


def _apply_world_cup_team_records(sport: str, games_dicts: list) -> None:
    """Attach current World Cup group-stage records to any WC game dicts."""
    if sport.lower() != "wc":
        return

    collector = get_collector("WC")
    if not collector or not hasattr(collector, "get_team_records"):
        return

    try:
        team_records = collector.get_team_records() or {}
    except Exception as e:
        logger.debug("World Cup team record lookup failed: %s", e)
        return

    if not team_records:
        return

    def _resolve(team_name: str) -> Optional[Dict[str, Any]]:
        if not team_name:
            return None
        normalized = collector._normalize_team_name(team_name)
        if normalized in team_records:
            return team_records[normalized]
        abbrev = normalized.strip().upper()
        if abbrev in team_records:
            return team_records[abbrev]
        return None

    for g in games_dicts:
        if not isinstance(g, dict):
            continue
        home = _resolve(g.get("home_team") or g.get("home_team_abbrev") or "")
        visitor = _resolve(g.get("visitor_team") or g.get("visitor_team_abbrev") or "")
        if home:
            g["home_wins"] = home.get("wins", 0)
            g["home_draws"] = home.get("draws", 0)
            g["home_losses"] = home.get("losses", 0)
            g["home_record"] = home.get("record", "")
            g["home_group"] = home.get("group", "")
            g["home_group_rank"] = home.get("group_rank")
            g["home_currently_advancing"] = home.get("currently_advancing")
            g["home_advancement_path"] = home.get("advancement_path", "")
            g["home_third_place_rank"] = home.get("third_place_rank")
            g["home_points"] = home.get("points", 0)
            g["home_goals_for"] = home.get("goals_for", 0)
            g["home_goals_against"] = home.get("goals_against", 0)
            g["home_goal_difference"] = home.get("goal_difference", 0)
        if visitor:
            g["visitor_wins"] = visitor.get("wins", 0)
            g["visitor_draws"] = visitor.get("draws", 0)
            g["visitor_losses"] = visitor.get("losses", 0)
            g["visitor_record"] = visitor.get("record", "")
            g["visitor_group"] = visitor.get("group", "")
            g["visitor_group_rank"] = visitor.get("group_rank")
            g["visitor_currently_advancing"] = visitor.get("currently_advancing")
            g["visitor_advancement_path"] = visitor.get("advancement_path", "")
            g["visitor_third_place_rank"] = visitor.get("third_place_rank")
            g["visitor_points"] = visitor.get("points", 0)
            g["visitor_goals_for"] = visitor.get("goals_for", 0)
            g["visitor_goals_against"] = visitor.get("goals_against", 0)
            g["visitor_goal_difference"] = visitor.get("goal_difference", 0)


def _apply_world_cup_winner(sport: str, games_dicts: list) -> None:
    """Derive a winner label for World Cup knockout matches.

    Prefer shootout scores when ESPN exposed them; otherwise fall back to
    the regulation-time scoreline.
    """
    if sport.lower() != "wc":
        return
    for g in games_dicts:
        if not isinstance(g, dict):
            continue
        if g.get("wc_winner"):
            continue
        if not g.get("is_final"):
            continue
        home = g.get("home_team") or ""
        visitor = g.get("visitor_team") or ""
        home_score = g.get("home_score_total")
        visitor_score = g.get("visitor_score_total")
        home_so = g.get("home_shootout_score")
        visitor_so = g.get("visitor_shootout_score")
        if home_so is not None and visitor_so is not None and home_so != visitor_so:
            g["wc_winner"] = home if int(home_so or 0) > int(visitor_so or 0) else visitor
        elif home_score is not None and visitor_score is not None and home_score != visitor_score:
            g["wc_winner"] = home if int(home_score or 0) > int(visitor_score or 0) else visitor


# Per-league period-score conventions for the box_score block.
# `prefix` matches the dict key the collectors write
# (mlb.py writes 'inning_1', nhl.py writes 'period_1', nba/nfl write 'q1').
# `label` is the column header per period number.
# `total_label` is the final-score column at the end.
_BOX_SCORE_CONFIG: Dict[str, Dict[str, Any]] = {
    "nba":  {"prefix": "q",       "label": lambda n: f"Q{n}", "total_label": "F"},
    "wnba": {"prefix": "q",       "label": lambda n: f"Q{n}", "total_label": "F"},
    "nfl":  {"prefix": "q",       "label": lambda n: f"Q{n}", "total_label": "F"},
    "nhl":  {"prefix": "period_", "label": lambda n: str(n),  "total_label": "F"},
    "mlb":  {"prefix": "inning_", "label": lambda n: str(n),  "total_label": "R"},
    "mls":  {"prefix": "h",       "label": lambda n: f"H{n}", "total_label": "F"},
    "wc":   {"prefix": "h",       "label": lambda n: f"H{n}", "total_label": "F"},
}


def _apply_box_score(sport: str, games_dicts: list) -> None:
    """Derive a v6-facing `box_score` block per game from the collector's
    period-score dicts. Per-league key conventions handled in _BOX_SCORE_CONFIG.

      box_score = {
        "columns": ["Q1","Q2","Q3","Q4","F"],
        "visitor": [24,28,25,32,109],
        "home":    [29,31,22,34,116],
      }

    Periods that don't fit the per-league prefix (e.g. 'ot', 'so') are
    appended verbatim with their key uppercased. World Cup / MLS penalty
    shootouts are surfaced as a PK column when available. No box_score is
    added when both sides have empty period dicts."""
    cfg = _BOX_SCORE_CONFIG.get(sport.lower())
    if not cfg:
        return
    prefix = cfg["prefix"]
    label_fn = cfg["label"]
    total_label = cfg["total_label"]

    def _period_num(key: str) -> Optional[int]:
        if not key.startswith(prefix):
            return None
        try:
            return int(key[len(prefix):])
        except ValueError:
            return None

    for g in games_dicts:
        if not isinstance(g, dict):
            continue
        home_p = dict(g.get("home_period_scores") or {})
        visitor_p = dict(g.get("visitor_period_scores") or {})
        home_so = g.get("home_shootout_score")
        visitor_so = g.get("visitor_shootout_score")
        if sport.lower() in ("mls", "wc") and (home_so is not None or visitor_so is not None):
            home_p.setdefault("pk", int(home_so or 0))
            visitor_p.setdefault("pk", int(visitor_so or 0))
        if not home_p and not visitor_p:
            g.setdefault("box_score", None)
            continue
        # Order numbered periods first (by number), then any extras (OT/SO) by key.
        all_keys = set(home_p.keys()) | set(visitor_p.keys())
        numbered = sorted(
            [k for k in all_keys if _period_num(k) is not None],
            key=_period_num,
        )
        extras = sorted(k for k in all_keys if _period_num(k) is None)
        columns: List[str] = []
        home_arr: List[int] = []
        visitor_arr: List[int] = []
        for k in numbered:
            n = _period_num(k)
            columns.append(label_fn(n))
            home_arr.append(int(home_p.get(k, 0) or 0))
            visitor_arr.append(int(visitor_p.get(k, 0) or 0))
        for k in extras:
            columns.append(k.upper())
            home_arr.append(int(home_p.get(k, 0) or 0))
            visitor_arr.append(int(visitor_p.get(k, 0) or 0))
        columns.append(total_label)
        home_arr.append(int(g.get("home_score") or 0))
        visitor_arr.append(int(g.get("visitor_score") or 0))
        g["box_score"] = {
            "columns": columns,
            "home": home_arr,
            "visitor": visitor_arr,
        }


def _apply_tennis_contract(sport: str, games_dicts: list) -> None:
    """Add the v6-facing tennis contract (player1_*/player2_* aliases and a
    tennis_score block) on top of the existing tennis fields. Pure aliasing
    — every new field is derived from values already on the dict.

    Convention: player1 = visitor, player2 = home. Matches the curl layout
    where the visitor row prints first."""
    if sport.lower() not in ("atp", "wta"):
        return
    for g in games_dicts:
        if not isinstance(g, dict):
            continue
        g.setdefault("match_status", g.get("game_status") or "scheduled")
        g.setdefault("tournament_name", g.get("tennis_tournament") or "")

        visitor_last = (g.get("visitor_team") or "").strip()
        home_last = (g.get("home_team") or "").strip()
        g["player1_last_name"] = visitor_last
        g["player2_last_name"] = home_last
        g["player1_name"] = g.get("visitor_full_name") or visitor_last
        g["player2_name"] = g.get("home_full_name") or home_last
        g["player1_seed"] = g.get("visitor_seed")
        g["player2_seed"] = g.get("home_seed")
        g["player1_rank"] = g.get("visitor_seed")
        g["player2_rank"] = g.get("home_seed")

        sets = g.get("tennis_set_scores") or []
        if sets:
            g["player1_score"] = [s.get("visitor", 0) for s in sets]
            g["player2_score"] = [s.get("home", 0) for s in sets]
        else:
            g["player1_score"] = None
            g["player2_score"] = None
        g["player1_sets_won"] = g.get("visitor_sets_won")
        g["player2_sets_won"] = g.get("home_sets_won")

        tw = g.get("tennis_winner")
        if tw == "visitor":
            g["winner"] = "player1"
            g["winner_name"] = g["player1_name"]
        elif tw == "home":
            g["winner"] = "player2"
            g["winner_name"] = g["player2_name"]
        else:
            g["winner"] = None
            g["winner_name"] = None

        if sets:
            g["tennis_score"] = {
                "columns": [f"S{i+1}" for i in range(len(sets))],
                "player1": g["player1_score"],
                "player2": g["player2_score"],
                "winner": g["winner"],
            }
        else:
            g["tennis_score"] = None


def _enrich_curl_wrappers(sport: str, target_date: date, wrappers: list) -> list:
    """Apply dict-based enrichers (playoff series, cricket, tennis scores) to
    GameWrapper instances by round-tripping through dict proxies. Mirrors
    the JSON path so curl/text routes get the same series-record + tennis
    set-score data."""
    if not wrappers:
        return wrappers
    proxies = [
        {
            "home_team": getattr(w, "home_team", "") or "",
            "visitor_team": getattr(w, "visitor_team", "") or "",
            "home_full_name": getattr(w, "home_full_name", "") or "",
            "visitor_full_name": getattr(w, "visitor_full_name", "") or "",
            "home_seed": getattr(w, "home_seed", None),
            "visitor_seed": getattr(w, "visitor_seed", None),
            "player1_rank": getattr(w, "player1_rank", None),
            "player2_rank": getattr(w, "player2_rank", None),
            "home_score_total": getattr(w, "home_score_total", 0) or 0,
            "visitor_score_total": getattr(w, "visitor_score_total", 0) or 0,
            "home_period_scores": getattr(w, "home_period_scores", None) or {},
            "visitor_period_scores": getattr(w, "visitor_period_scores", None) or {},
            "home_shootout_score": getattr(w, "home_shootout_score", None),
            "visitor_shootout_score": getattr(w, "visitor_shootout_score", None),
            "is_final": getattr(w, "is_final", False),
            "wc_winner": getattr(w, "wc_winner", "") or "",
            "cycling_url": getattr(w, "cycling_url", "") or "",
            "cycling_url_label": getattr(w, "cycling_url_label", "") or "",
            "venue_name": getattr(w, "venue_name", "") or "",
            "court_name": getattr(w, "court_name", "") or "",
        }
        for w in wrappers
    ]
    _apply_dict_enrichers(sport, proxies, target_date)
    keys = (
        # playoff_series
        "is_playoff", "series_summary", "series_round",
        "series_total", "series_completed",
        "home_series_wins", "home_series_losses",
        "visitor_series_wins", "visitor_series_losses",
        # cricket live enrichment
        "cricket_status", "cricket_venue", "cricket_start_time",
        "cricket_home_nr", "cricket_away_nr",
        "cricket_home_score", "cricket_away_score",
        "cricket_winner", "cricket_result", "cricket_away_outcome",
        # tennis_scores
        "tennis_set_scores", "home_sets_won", "visitor_sets_won",
        "tennis_summary", "tennis_winner", "home_full_name", "visitor_full_name",
        "home_seed", "visitor_seed", "player1_rank", "player2_rank", "venue_name", "court_name",
        # world cup group-stage metadata
        "home_wins", "home_draws", "home_losses",
        "home_record", "home_group", "home_group_rank", "home_currently_advancing",
        "home_advancement_path", "home_third_place_rank", "home_points",
        "home_goals_for", "home_goals_against", "home_goal_difference",
        "visitor_wins", "visitor_draws", "visitor_losses",
        "visitor_record", "visitor_group", "visitor_group_rank", "visitor_currently_advancing",
        "visitor_advancement_path", "visitor_third_place_rank", "visitor_points",
        "visitor_goals_for", "visitor_goals_against", "visitor_goal_difference",
        # tennis_scores may also override these:
        "is_final", "game_status",
        "home_shootout_score", "visitor_shootout_score",
        # world cup / cycling extras
        "wc_winner",
        "cycling_url",
        "cycling_url_label",
    )
    for w, p in zip(wrappers, proxies):
        for k in keys:
            if k in p:
                setattr(w, k, p[k])
    return wrappers


def _get_games_for_curl(
    league: str,
    target_date: date,
    timezone: pytz.BaseTzInfo,
    prefer_db: bool = False,
    allow_collector_fallback: bool = True,
    include_metadata: bool = False,
):
    """Helper function to get games for curl formatting (returns GameWrapper objects)."""
    games = []
    db_source_updated_at: Optional[str] = None

    class GameWrapper:
        def __init__(self, data):
            for k, v in data.items():
                setattr(self, k, v)

    def _append_db_games() -> None:
        nonlocal db_source_updated_at
        standings_records = {}
        wnba_collector = None
        if league == 'WNBA':
            try:
                wnba_collector = get_collector('WNBA')
                if wnba_collector and hasattr(wnba_collector, 'get_team_records'):
                    standings_records = wnba_collector.get_team_records()
            except Exception as exc:
                logger.debug("Could not fetch WNBA standings records for DB rows: %s", exc)

        with get_db_session() as db:
            db_games = db.query(Game).filter(
                Game.league == league,
                Game.game_date == target_date
            ).order_by(Game.game_time).all()

            for game in db_games:
                if (not game.home_team or game.home_team.strip() == '') and (not game.home_team_abbrev or game.home_team_abbrev.strip() == ''):
                    continue
                if (not game.visitor_team or game.visitor_team.strip() == '') and (not game.visitor_team_abbrev or game.visitor_team_abbrev.strip() == ''):
                    continue

                game_data = {
                    'league': game.league,
                    'game_id': game.game_id,
                    'game_date': game.game_date,
                    'game_time': game.game_time,
                    'game_type': game.game_type,
                    'home_team': game.home_team,
                    'home_team_abbrev': game.home_team_abbrev,
                    'visitor_team': game.visitor_team,
                    'visitor_team_abbrev': game.visitor_team_abbrev,
                    'home_score_total': game.home_score_total or 0,
                    'visitor_score_total': game.visitor_score_total or 0,
                    'game_status': game.game_status,
                    'current_period': game.current_period or '',
                    'time_remaining': game.time_remaining or '',
                    'is_final': game.is_final or False,
                    'home_wins': game.home_wins or 0,
                    'home_losses': game.home_losses or 0,
                    'home_otl': getattr(game, 'home_otl', 0) or 0,
                    'visitor_wins': game.visitor_wins or 0,
                    'visitor_losses': game.visitor_losses or 0,
                    'visitor_otl': getattr(game, 'visitor_otl', 0) or 0,
                    'home_period_scores': getattr(game, 'home_period_scores', None) or {},
                    'visitor_period_scores': getattr(game, 'visitor_period_scores', None) or {},
                    'home_shootout_score': getattr(game, 'home_shootout_score', None),
                    'visitor_shootout_score': getattr(game, 'visitor_shootout_score', None),
                    'cycling_race': getattr(game, 'cycling_race', '') or '',
                    'cycling_stage_label': getattr(game, 'cycling_stage_label', '') or '',
                    'cycling_stage_number': getattr(game, 'cycling_stage_number', None),
                    'cycling_event_label': getattr(game, 'cycling_event_label', '') or '',
                    'cycling_country': getattr(game, 'cycling_country', '') or '',
                    'cycling_video': getattr(game, 'cycling_video', '') or '',
                    'cycling_winner': getattr(game, 'cycling_winner', '') or '',
                    'cycling_rank': getattr(game, 'cycling_rank', None),
                    'wc_round': getattr(game, 'wc_round', ''),
                    'wc_round_label': getattr(game, 'wc_round_label', ''),
                    'wc_winner': getattr(game, 'wc_winner', '') or '',
                }
                if league == 'WNBA' and standings_records and wnba_collector:
                    home_record = standings_records.get(wnba_collector._normalize_abbrev(game.home_team_abbrev))
                    visitor_record = standings_records.get(wnba_collector._normalize_abbrev(game.visitor_team_abbrev))
                    if home_record:
                        game_data['home_wins'] = home_record['wins']
                        game_data['home_losses'] = home_record['losses']
                    if visitor_record:
                        game_data['visitor_wins'] = visitor_record['wins']
                        game_data['visitor_losses'] = visitor_record['losses']
                updated_at = getattr(game, 'updated_at', None)
                normalized_updated_at = _normalize_iso_timestamp(updated_at)
                if normalized_updated_at:
                    db_source_updated_at = max(db_source_updated_at, normalized_updated_at) if db_source_updated_at else normalized_updated_at
                games.append(GameWrapper(game_data))

    if prefer_db:
        _append_db_games()
        if games or not allow_collector_fallback:
            if include_metadata:
                return games, {
                    **_cache_snapshot(),
                    "source_updated_at": db_source_updated_at,
                }
            return games

    collector = get_collector(league)
    if not collector:
        if include_metadata:
            return games, {
                **_cache_snapshot(),
                "source_updated_at": db_source_updated_at,
            }
        return games
    set_collector_timezone(collector, timezone)

    now_tz = datetime.now(timezone)
    today = now_tz.date()

    def _fetch():
        raw = collector.get_live_scores(target_date) or collector.get_schedule(target_date) or []
        # For past dates, if the API returns games with no scores/status (empty shells),
        # discard them so the DB fallback is used instead
        if target_date < today and raw:
            has_real_data = any(
                g.get('is_final') or g.get('game_status') in ('final', 'in_progress')
                or (g.get('home_score_total') or 0) > 0 or (g.get('visitor_score_total') or 0) > 0
                for g in raw
            )
            if not has_real_data:
                return []
        return raw

    if include_metadata:
        raw_games, cache_snapshot = _get_cached_games(
            league,
            target_date,
            _fetch,
            getattr(timezone, 'zone', str(timezone)),
            include_metadata=True,
        )
    else:
        raw_games = _get_cached_games(
            league,
            target_date,
            _fetch,
            getattr(timezone, 'zone', str(timezone)),
        )
        cache_snapshot = None

    if raw_games:
        seen_game_ids = set()
        for game_dict in raw_games:
                    game_id = game_dict.get('game_id', '')
                    if game_id and game_id in seen_game_ids:
                        continue  # Skip duplicates
                    seen_game_ids.add(game_id)
                    
                    # Skip games with empty team names (both abbreviation and full name)
                    home_team = game_dict.get('home_team', '')
                    home_abbrev = game_dict.get('home_team_abbrev', '')
                    visitor_team = game_dict.get('visitor_team', '')
                    visitor_abbrev = game_dict.get('visitor_team_abbrev', '')
                    
                    if (not home_team or home_team.strip() == '') and (not home_abbrev or home_abbrev.strip() == ''):
                        continue
                    if (not visitor_team or visitor_team.strip() == '') and (not visitor_abbrev or visitor_abbrev.strip() == ''):
                        continue
                    
                    # Get game_time from database if not in live data
                    game_time = game_dict.get('game_time')
                    if not game_time:
                        with get_db_session() as db:
                            db_game = db.query(Game).filter(
                                Game.game_id == game_id
                            ).first()
                            if db_game and db_game.game_time:
                                game_time = db_game.game_time
                    
                    # Ensure time_remaining is included from live data
                    time_remaining = game_dict.get('time_remaining', '')
                    current_period = game_dict.get('current_period', '')
                    
                    game_data = {
                        'league': league,
                        'game_id': game_id,
                        'game_date': datetime.strptime(game_dict.get('game_date', ''), '%Y-%m-%d').date() if game_dict.get('game_date') else target_date,
                        'game_time': game_time,
                        'game_type': game_dict.get('game_type', 'regular'),
                        'home_team': game_dict.get('home_team', ''),
                        'home_team_abbrev': game_dict.get('home_team_abbrev', ''),
                        'visitor_team': game_dict.get('visitor_team', ''),
                        'visitor_team_abbrev': game_dict.get('visitor_team_abbrev', ''),
                        'home_score_total': int(game_dict.get('home_score_total', 0) or 0),
                        'visitor_score_total': int(game_dict.get('visitor_score_total', 0) or 0),
                        'game_status': game_dict.get('game_status', 'scheduled'),
                        'current_period': current_period,
                        'time_remaining': time_remaining,
                        'is_final': game_dict.get('is_final', False),
                        'home_wins': int(game_dict.get('home_wins', 0) or 0),
                        'home_losses': int(game_dict.get('home_losses', 0) or 0),
                        'home_otl': int(game_dict.get('home_otl', 0) or 0),
                        'visitor_wins': int(game_dict.get('visitor_wins', 0) or 0),
                        'visitor_losses': int(game_dict.get('visitor_losses', 0) or 0),
                        'visitor_otl': int(game_dict.get('visitor_otl', 0) or 0),
                        'cricket_status': game_dict.get('cricket_status', ''),
                        'cricket_venue': game_dict.get('cricket_venue', ''),
                        'cricket_start_time': game_dict.get('cricket_start_time', {}),
                        'cricket_home_nr': int(game_dict.get('cricket_home_nr', 0) or 0),
                        'cricket_away_nr': int(game_dict.get('cricket_away_nr', 0) or 0),
                        'cricket_home_score': game_dict.get('cricket_home_score', ''),
                        'cricket_away_score': game_dict.get('cricket_away_score', ''),
                        'cricket_winner': game_dict.get('cricket_winner', ''),
                        'cricket_result': game_dict.get('cricket_result', ''),
                        'cricket_away_outcome': game_dict.get('cricket_away_outcome', ''),
                        'home_draws': int(game_dict.get('home_draws', 0) or 0),
                        'visitor_draws': int(game_dict.get('visitor_draws', 0) or 0),
                        'mls_detail': game_dict.get('mls_detail', ''),
                        'tennis_tournament': game_dict.get('tennis_tournament', ''),
                        'tennis_match_label': game_dict.get('tennis_match_label', ''),
                        'tennis_round': game_dict.get('tennis_round', ''),
                        'tennis_country': game_dict.get('tennis_country', ''),
                        'tennis_video': game_dict.get('tennis_video', ''),
                        'tennis_set_scores': game_dict.get('tennis_set_scores'),
                        'home_sets_won': game_dict.get('home_sets_won'),
                        'visitor_sets_won': game_dict.get('visitor_sets_won'),
                        'tennis_summary': game_dict.get('tennis_summary'),
                        'tennis_winner': game_dict.get('tennis_winner'),
                        'home_period_scores': game_dict.get('home_period_scores') or {},
                        'visitor_period_scores': game_dict.get('visitor_period_scores') or {},
                        'home_shootout_score': game_dict.get('home_shootout_score'),
                        'visitor_shootout_score': game_dict.get('visitor_shootout_score'),
                        'cycling_race': game_dict.get('cycling_race', ''),
                        'cycling_stage_label': game_dict.get('cycling_stage_label', ''),
                        'cycling_stage_number': game_dict.get('cycling_stage_number', None),
                        'cycling_event_label': game_dict.get('cycling_event_label', ''),
                        'cycling_country': game_dict.get('cycling_country', ''),
                        'cycling_url': game_dict.get('cycling_url', ''),
                        'cycling_video': game_dict.get('cycling_video', ''),
                        'cycling_winner': game_dict.get('cycling_winner', ''),
                        'cycling_rank': game_dict.get('cycling_rank', None),
                        'race_type': game_dict.get('race_type', ''),
                        'start_city': game_dict.get('start_city', ''),
                        'finish_city': game_dict.get('finish_city', ''),
                        'wc_round': game_dict.get('wc_round', ''),
                        'wc_round_label': game_dict.get('wc_round_label', ''),
                        'wc_winner': game_dict.get('wc_winner', ''),
                    }
                    games.append(GameWrapper(game_data))

    # Fallback to database ONLY if no collector games were found
    if not games:
        _append_db_games()

    if include_metadata:
        if games and raw_games:
            if not cache_snapshot.get("source_updated_at"):
                cache_snapshot["source_updated_at"] = _collector_source_updated_at(collector, "games")
            return games, cache_snapshot
        return games, {
            **_cache_snapshot(),
            "source_updated_at": db_source_updated_at,
        }
    return games


def _get_all_sport_games(target_date: date, timezone: pytz.BaseTzInfo) -> Dict[str, List[Any]]:
    """Get all sports with DB-first reads and bounded parallel collector fallback."""
    results: Dict[str, List[Any]] = {sport_key: [] for sport_key in SPORT_MAPPINGS}
    max_workers = max(1, len(SPORT_MAPPINGS))
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
    futures = {
        executor.submit(_get_games_for_curl, league, target_date, timezone, True, True): sport_key
        for sport_key, league in SPORT_MAPPINGS.items()
    }

    try:
        for future in concurrent.futures.as_completed(futures, timeout=25):
            sport_key = futures[future]
            try:
                results[sport_key] = future.result()
            except Exception as exc:
                logger.warning("Failed to fetch %s games for all-sports response: %s", sport_key, exc)
    except concurrent.futures.TimeoutError:
        pending = [sport for future, sport in futures.items() if not future.done()]
        logger.warning("Timed out fetching all-sports data for: %s", ", ".join(pending))
    finally:
        executor.shutdown(wait=False, cancel_futures=True)

    return results


@app.get("/curl/v1/schedule/{sport}/{date}", response_class=PlainTextResponse)
def get_schedule_curl_v1(
    sport: str = Path(..., description="Sport (nba, mlb, nfl, nhl, wnba, all)"),
    date: str = Path(..., description="Date: today/tomorrow/yesterday, YYYY-MM-DD, YYYYMMDD, M/D/YYYY, MM/DD/YYYY, or other formats"),
    tz: Optional[str] = Query(None, description="Timezone: et/est/eastern, pt/pst/pdt/pacific, akst/alaska, hst/hawaii (default: Pacific)"),
):
    """Get schedule in curl-style text format."""
    try:
        timezone = get_timezone(tz)
        # Parse date using the timezone (so "today" is in the correct timezone)
        target_date = parse_date_param(date, timezone)
        sport_lower = sport.lower()
        
        # Handle 'all' sport - aggregate from all sports
        if sport_lower == 'all':
            sport_games = _get_all_sport_games(target_date, timezone)
            for sport_key, games in sport_games.items():
                _enrich_curl_wrappers(sport_key, target_date, games)
            all_games = []
            for games in sport_games.values():
                all_games.extend(games)
            
            return format_schedule_curl(all_games, target_date, timezone, show_all_sports=True)

        
        # Single sport logic
        league = SPORT_MAPPINGS.get(sport_lower)
        if not league:
            raise HTTPException(status_code=400, detail=f"Invalid sport: {sport}")
        
        games = _get_games_for_curl(league, target_date, timezone)
        
        # Deduplicate games by game_id before formatting
        seen_game_ids = set()
        unique_games = []
        for game in games:
            game_id = getattr(game, 'game_id', None) or getattr(game, 'gameId', None)
            if game_id:
                if game_id not in seen_game_ids:
                    seen_game_ids.add(game_id)
                    unique_games.append(game)
                # Skip duplicates
            else:
                # If no game_id, include it (shouldn't happen)
                unique_games.append(game)

        _enrich_curl_wrappers(sport_lower, target_date, unique_games)
        return format_schedule_curl(unique_games, target_date, timezone)

    except Exception as e:
        return _internal_error_response("/curl/v1/schedule/{sport}/{date}", e, plain_text=True)


@app.get("/v1/schedule/{sport}/{date}")
def get_schedule_v1(
    request: Request,
    sport: str = Path(..., description="Sport (nba, mlb, nfl, nhl, wnba, all)"),
    date: str = Path(..., description="Date: today/tomorrow/yesterday, YYYY-MM-DD, YYYYMMDD, M/D/YYYY, MM/DD/YYYY, or other formats"),
    tz: Optional[str] = Query(None, description="Timezone for relative dates (default: Pacific)"),
    *,
    response: Response = None,
):
    """Canonical single-sport schedule endpoint negotiated via Accept."""
    if _client_prefers_plain_text(request):
        return _add_vary_accept(PlainTextResponse(get_schedule_curl_v1(sport, date, tz)))
    if response is not None:
        _add_vary_accept(response)
    return get_schedule_api_v1(sport, date, tz)


@app.get("/api/v1/scores/{sport}/{date}", response_model=schemas.ScoresResponse)
def get_scores_api_v1(
    sport: str = Path(..., description="Sport (nba, mlb, nfl, nhl, wnba, all)"),
    date: str = Path(..., description="Date: today/tomorrow/yesterday, YYYY-MM-DD, YYYYMMDD, M/D/YYYY, MM/DD/YYYY, or other formats"),
    tz: Optional[str] = Query(None, description="Timezone for relative dates (default: Pacific)"),
):
    """Get scores in JSON format."""
    try:
        timezone = get_timezone(tz)
        target_date = parse_date_param(date, timezone)
        sport_lower = sport.lower()

        if sport_lower == 'all':
            sport_games = _get_all_sport_games(target_date, timezone)
            for sport_key, games in sport_games.items():
                _enrich_curl_wrappers(sport_key, target_date, games)
            all_scores = []
            by_sport: Dict[str, List[Dict[str, Any]]] = {}
            for sport_key, games in sport_games.items():
                for g in games:
                    d = _game_wrapper_to_dict(g, SPORT_MAPPINGS[sport_key])
                    d['sport'] = sport_key
                    all_scores.append(d)
                    by_sport.setdefault(sport_key, []).append(d)
            for sport_key, batch in by_sport.items():
                _apply_dict_enrichers(sport_key, batch, target_date)
            return {"sport": "all", "date": target_date.isoformat(), "scores": all_scores}

        league = SPORT_MAPPINGS.get(sport_lower)
        if not league:
            raise HTTPException(status_code=400, detail=f"Invalid sport: {sport}")

        games, cache_snapshot = _get_games_for_curl(league, target_date, timezone, include_metadata=True)
        scores = [_game_wrapper_to_dict(g, league) for g in games]
        _apply_dict_enrichers(sport_lower, scores, target_date)
        return {
            "sport": sport,
            "date": target_date.isoformat(),
            "scores": scores,
            "meta": _build_endpoint_meta(
                cache_snapshot,
                _scores_freshness_seconds(target_date, timezone),
                empty_state=cache_snapshot.get("empty_state"),
            ),
        }
    except Exception as e:
        return _internal_error_response("/api/v1/scores/{sport}/{date}", e)

def _get_scores_for_league(league: str, target_date: date) -> List[Dict[str, Any]]:
    """Helper function to get scores for a specific league."""
    from datetime import datetime
    now_tz = datetime.now(pytz.timezone('US/Pacific'))
    today = now_tz.date()
    is_wc = league.upper() == 'WC'
    
    # Get live scores from collector (includes in-progress and final games)
    collector = get_collector(league)
    if collector:
        # For today, try live scores first
        if target_date == today:
            live_games = collector.get_live_scores(target_date)
            if live_games:
                return [
                    {
                        "game_id": game.get('game_id', ''),
                        "home_team": game.get('home_team', ''),
                        "home_score": game.get('home_score_total', 0),
                        "visitor_team": game.get('visitor_team', ''),
                        "visitor_score": game.get('visitor_score_total', 0),
                        "home_shootout_score": game.get('home_shootout_score') if is_wc else None,
                        "visitor_shootout_score": game.get('visitor_shootout_score') if is_wc else None,
                        "is_final": game.get('is_final', False),
                        "game_status": game.get('game_status', 'scheduled'),
                        "current_period": game.get('current_period', ''),
                        "time_remaining": game.get('time_remaining', ''),
                        "home_wins": game.get('home_wins', 0),
                        "home_losses": game.get('home_losses', 0),
                        "home_otl": game.get('home_otl', 0) if league.upper() == 'NHL' else None,
                        "visitor_wins": game.get('visitor_wins', 0),
                        "visitor_losses": game.get('visitor_losses', 0),
                        "visitor_otl": game.get('visitor_otl', 0) if league.upper() == 'NHL' else None,
                        "cricket_home_score": game.get('cricket_home_score', ''),
                        "cricket_away_score": game.get('cricket_away_score', ''),
                        "cricket_winner": game.get('cricket_winner', ''),
                        "cricket_result": game.get('cricket_result', ''),
                    }
                    for game in live_games
                ]
        
        # For past or future dates, try get_schedule
        schedule_games = collector.get_schedule(target_date)
        if schedule_games:
            # For past dates, filter for games that have scores (final or in-progress)
            # For future dates, return all scheduled games
            if target_date < today:
                scored_games = [
                    game for game in schedule_games
                    if game.get('is_final') or game.get('home_score_total', 0) > 0 or game.get('visitor_score_total', 0) > 0
                ]
                if scored_games:
                    return [
                        {
                            "game_id": game.get('game_id', ''),
                            "home_team": game.get('home_team', ''),
                            "home_score": game.get('home_score_total', 0),
                            "visitor_team": game.get('visitor_team', ''),
                            "visitor_score": game.get('visitor_score_total', 0),
                            "home_shootout_score": game.get('home_shootout_score') if is_wc else None,
                            "visitor_shootout_score": game.get('visitor_shootout_score') if is_wc else None,
                            "is_final": game.get('is_final', False),
                            "game_status": game.get('game_status', 'scheduled'),
                            "current_period": game.get('current_period', ''),
                            "time_remaining": game.get('time_remaining', ''),
                            "home_wins": game.get('home_wins', 0),
                            "home_losses": game.get('home_losses', 0),
                            "home_otl": game.get('home_otl', 0) if league.upper() == 'NHL' else None,
                            "visitor_wins": game.get('visitor_wins', 0),
                            "visitor_losses": game.get('visitor_losses', 0),
                            "visitor_otl": game.get('visitor_otl', 0) if league.upper() == 'NHL' else None,
                        }
                        for game in scored_games
                    ]
            else:
                # Future dates - return scheduled games (may not have scores yet)
                return [
                    {
                        "game_id": game.get('game_id', ''),
                        "home_team": game.get('home_team', ''),
                        "home_score": game.get('home_score_total', 0),
                        "visitor_team": game.get('visitor_team', ''),
                        "visitor_score": game.get('visitor_score_total', 0),
                        "home_shootout_score": game.get('home_shootout_score') if is_wc else None,
                        "visitor_shootout_score": game.get('visitor_shootout_score') if is_wc else None,
                        "is_final": game.get('is_final', False),
                        "game_status": game.get('game_status', 'scheduled'),
                        "current_period": game.get('current_period', ''),
                        "time_remaining": game.get('time_remaining', ''),
                        "home_wins": game.get('home_wins', 0),
                        "home_losses": game.get('home_losses', 0),
                        "home_otl": game.get('home_otl', 0) if league.upper() == 'NHL' else None,
                        "visitor_wins": game.get('visitor_wins', 0),
                        "visitor_losses": game.get('visitor_losses', 0),
                        "visitor_otl": game.get('visitor_otl', 0) if league.upper() == 'NHL' else None,
                    }
                    for game in schedule_games
                ]
    
    # Fallback to database
    with get_db_session() as db:
        games = db.query(Game).filter(
            Game.league == league,
            Game.game_date == target_date
        ).all()
        
        return [
            {
                "game_id": game.game_id,
                "home_team": game.home_team,
                "home_score": game.home_score_total,
                "visitor_team": game.visitor_team,
                "visitor_score": game.visitor_score_total,
                "home_shootout_score": getattr(game, 'home_shootout_score', None) if is_wc else None,
                "visitor_shootout_score": getattr(game, 'visitor_shootout_score', None) if is_wc else None,
                "is_final": game.is_final,
                "game_status": game.game_status,
                "current_period": game.current_period,
                "time_remaining": game.time_remaining,
                "home_wins": game.home_wins or 0,
                "home_losses": game.home_losses or 0,
                "home_otl": game.home_otl if league.upper() == 'NHL' and hasattr(game, 'home_otl') else None,
                "visitor_wins": game.visitor_wins or 0,
                "visitor_losses": game.visitor_losses or 0,
                "visitor_otl": game.visitor_otl if league.upper() == 'NHL' and hasattr(game, 'visitor_otl') else None,
            }
            for game in games
        ]


@app.get("/curl/v1/scores/{sport}/{date}", response_class=PlainTextResponse)
def get_scores_curl_v1(
    sport: str = Path(..., description="Sport (nba, mlb, nfl, nhl, wnba, all)"),
    date: str = Path(..., description="Date: today/tomorrow/yesterday, YYYY-MM-DD, YYYYMMDD, M/D/YYYY, MM/DD/YYYY, or other formats"),
    tz: Optional[str] = Query(None, description="Timezone: et/est/eastern, pt/pst/pdt/pacific (default: Pacific)"),
):
    """Get scores in curl-style text format."""
    try:
        timezone = get_timezone(tz)
        # Parse date using the timezone (so "today" is in the correct timezone)
        target_date = parse_date_param(date, timezone)
        sport_lower = sport.lower()
        
        # Handle 'all' sport - aggregate from all sports
        if sport_lower == 'all':
            sport_games = _get_all_sport_games(target_date, timezone)
            all_games = []
            for games in sport_games.values():
                all_games.extend(games)
            
            return format_scores_curl(all_games, target_date, timezone, show_all_sports=True)

        
        # Single sport logic
        league = SPORT_MAPPINGS.get(sport_lower)
        if not league:
            raise HTTPException(status_code=400, detail=f"Invalid sport: {sport}")
        
        games = _get_games_for_curl(league, target_date, timezone)

        seen_game_ids = set()
        unique_games = []
        for game in games:
            game_id = getattr(game, 'game_id', None) or getattr(game, 'gameId', None)
            if game_id:
                if game_id in seen_game_ids:
                    continue
                seen_game_ids.add(game_id)
            unique_games.append(game)

        _enrich_curl_wrappers(sport_lower, target_date, unique_games)
        return format_scores_curl(unique_games, target_date, timezone)

    except Exception as e:
        return _internal_error_response("/curl/v1/scores/{sport}/{date}", e, plain_text=True)


@app.get("/v1/scores/{sport}/{date}")
def get_scores_v1(
    request: Request,
    sport: str = Path(..., description="Sport (nba, mlb, nfl, nhl, wnba, all)"),
    date: str = Path(..., description="Date: today/tomorrow/yesterday, YYYY-MM-DD, YYYYMMDD, M/D/YYYY, MM/DD/YYYY, or other formats"),
    tz: Optional[str] = Query(None, description="Timezone for relative dates (default: Pacific)"),
    *,
    response: Response = None,
):
    """Canonical single-sport scores endpoint negotiated via Accept."""
    if _client_prefers_plain_text(request):
        return _add_vary_accept(PlainTextResponse(get_scores_curl_v1(sport, date, tz)))
    if response is not None:
        _add_vary_accept(response)
    return get_scores_api_v1(sport, date, tz)


@app.get("/api/v1/standings/{sport}", response_model=schemas.StandingsResponse)
def get_standings_api_v1(
    sport: str = Path(..., description="Sport (nba, mlb, nfl, nhl, wnba, mls, ipl, mlc, all)"),
):
    """Get standings in JSON format."""
    sport_lower = sport.lower()

    if sport_lower not in SPORT_MAPPINGS and sport_lower != 'all':
        raise HTTPException(status_code=400, detail=f"Invalid sport: {sport}")

    def _build_standings_payload() -> Dict[str, Any]:
        if sport_lower == 'mls':
            collector = get_collector('MLS')
            if collector:
                try:
                    standings = collector._fetch_standings()
                    upstream_health.record_success(upstream_health.upstream_for('mls', 'standings'))
                except Exception as e:
                    upstream_health.record_failure(upstream_health.upstream_for('mls', 'standings'), f"{type(e).__name__}: {e}")
                    raise
                teams = []
                for abbrev, rec in sorted(standings.items(), key=lambda x: -(x[1]['wins'] * 3 + x[1]['draws'])):
                    pts = rec['wins'] * 3 + rec['draws']
                    teams.append({
                        'abbreviation': abbrev,
                        'wins': rec['wins'],
                        'draws': rec['draws'],
                        'losses': rec['losses'],
                        'points': pts,
                        'record': f"{rec['wins']}-{rec['draws']}-{rec['losses']}",
                    })
                return {
                    "sport": "mls",
                    "teams": teams,
                    "source_updated_at": _collector_source_updated_at(collector, "standings"),
                }

        if sport_lower in ('ipl', 'mlc'):
            collector = get_collector(SPORT_MAPPINGS[sport_lower])
            try:
                standings = collector.get_standings() if collector and hasattr(collector, 'get_standings') else []
                upstream_health.record_success(upstream_health.upstream_for(sport_lower, 'standings'))
            except Exception as e:
                upstream_health.record_failure(upstream_health.upstream_for(sport_lower, 'standings'), f"{type(e).__name__}: {e}")
                raise
            teams = []
            for rec in standings:
                teams.append({
                    'rank': rec['rank'],
                    'team_name': rec['team_name'],
                    'abbreviation': rec['abbreviation'],
                    'matches': rec['matches'],
                    'wins': rec['wins'],
                    'losses': rec['losses'],
                    'no_result': rec['no_result'],
                    'points': rec['points'],
                    'nrr': rec['nrr'],
                    'record': rec['record'],
                })
            return {
                "sport": sport_lower,
                "teams": teams,
                "available": bool(teams),
                "source_updated_at": _collector_source_updated_at(collector, "standings"),
            }

        if sport_lower in ('nba', 'mlb', 'nfl', 'nhl', 'wnba'):
            collector = get_collector(SPORT_MAPPINGS[sport_lower])
            try:
                standings = collector.get_standings() if collector and hasattr(collector, 'get_standings') else []
                upstream_health.record_success(upstream_health.upstream_for(sport_lower, 'standings'))
            except Exception as e:
                upstream_health.record_failure(upstream_health.upstream_for(sport_lower, 'standings'), f"{type(e).__name__}: {e}")
                raise
            teams = []
            for rec in standings:
                team = {
                    'rank': rec['rank'],
                    'team_name': rec['team_name'],
                    'abbreviation': rec['abbreviation'],
                    'wins': rec['wins'],
                    'losses': rec['losses'],
                    'win_pct': rec['win_pct'],
                    'games_back': rec['games_back'],
                    'streak': rec['streak'],
                    'record': rec['record'],
                }
                for optional_key in ('conference', 'division', 'ties', 'ot', 'points'):
                    if optional_key in rec:
                        team[optional_key] = rec[optional_key]
                teams.append(team)
            return {
                "sport": sport_lower,
                "teams": teams,
                "source_updated_at": _collector_source_updated_at(collector, "standings"),
            }

        if sport_lower == 'wc':
            collector = get_collector('WC')
            try:
                standings = collector.get_standings() if collector else []
                groups = collector.get_group_standings() if collector and hasattr(collector, 'get_group_standings') else []
                knockout_bracket = collector.get_knockout_bracket() if collector and hasattr(collector, 'get_knockout_bracket') else None
                upstream_health.record_success(upstream_health.upstream_for('wc', 'standings'))
            except Exception as e:
                upstream_health.record_failure(upstream_health.upstream_for('wc', 'standings'), f"{type(e).__name__}: {e}")
                raise
            teams = []
            for rec in standings:
                teams.append({
                    'rank': rec['rank'],
                    'team_name': rec['team_name'],
                    'abbreviation': rec['abbreviation'],
                    'matches': rec['matches'],
                    'wins': rec['wins'],
                    'draws': rec['draws'],
                    'losses': rec['losses'],
                    'goals_for': rec['goals_for'],
                    'goals_against': rec['goals_against'],
                    'goal_difference': rec['goal_difference'],
                    'points': rec['points'],
                    'record': rec['record'],
                    'group': rec.get('group'),
                    'group_rank': rec.get('group_rank'),
                    'currently_advancing': rec.get('currently_advancing'),
                    'advancement_path': rec.get('advancement_path'),
                    'third_place_rank': rec.get('third_place_rank'),
                })
            return {
                "sport": "wc",
                "teams": teams,
                "groups": groups,
                "knockout_bracket": knockout_bracket,
                "available": bool(teams),
                "source_updated_at": _collector_source_updated_at(collector, "standings"),
            }

        if sport_lower in ('atp', 'wta'):
            collector = get_collector(SPORT_MAPPINGS[sport_lower])
            return {
                "sport": sport_lower,
                "teams": [],
                "available": False,
                "message": "Tennis has no league table; see /api/v1/season-info/{atp,wta} for the tour calendar.",
                "source_updated_at": _collector_source_updated_at(collector, "standings"),
            }

        if sport_lower == 'cycling':
            collector = get_collector('CYCLING')
            try:
                standings = collector.get_standings() if collector and hasattr(collector, 'get_standings') else []
                upstream_health.record_success(upstream_health.upstream_for('cycling', 'standings'))
            except Exception as e:
                upstream_health.record_failure(upstream_health.upstream_for('cycling', 'standings'), f"{type(e).__name__}: {e}")
                raise
            teams = []
            for rec in standings:
                team = {
                    'rank': rec.get('rank'),
                    'team_name': rec.get('team_name'),
                    'abbreviation': rec.get('abbreviation'),
                    'record': rec.get('record'),
                    'wins': rec.get('wins', 0),
                    'losses': rec.get('losses', 0),
                    'win_pct': rec.get('win_pct'),
                    'games_back': rec.get('games_back'),
                    'streak': rec.get('streak'),
                    'points': rec.get('points', 0),
                    'matches': rec.get('matches'),
                }
                if rec.get('cycling_rank') is not None:
                    team['cycling_rank'] = rec.get('cycling_rank')
                teams.append(team)
            return {
                "sport": "cycling",
                "teams": teams,
                "available": bool(teams),
                "message": "Cycling standings are available when the configured collector provides GC data.",
                "source_updated_at": _collector_source_updated_at(collector, "standings"),
            }

        return {"sport": sport, "message": "Standings endpoint - TODO for this sport"}

    payload, cache_snapshot = _get_cached_payload(
        _standings_cache,
        sport_lower,
        _STANDINGS_CACHE_TTL,
        _build_standings_payload,
    )
    return {
        **payload,
        "meta": _build_endpoint_meta(
            cache_snapshot,
            _standings_freshness_seconds(sport_lower),
            empty_state=cache_snapshot.get("empty_state"),
        ),
    }


@app.get("/curl/v1/standings/{sport}", response_class=PlainTextResponse)
def get_standings_curl_v1(
    sport: str = Path(..., description="Sport (nba, mlb, nfl, nhl, wnba, mls, ipl, mlc, all)"),
):
    """Get standings in curl-style text format."""
    sport_lower = sport.lower()
    
    if sport_lower == 'all':
        sports = ['ipl', 'mlb', 'mlc', 'mls', 'nba', 'nfl', 'nhl', 'wnba']
    else:
        sports = [sport_lower]
    
    if sport_lower not in SPORT_MAPPINGS and sport_lower != 'all':
        raise HTTPException(status_code=400, detail=f"Invalid sport: {sport}")

    output = _format_curl_header(pytz.timezone('US/Pacific'), datetime.now().date(), "Here are the standings:")

    for sport_name in sports:
        if sport_name in ('ipl', 'mlc'):
            collector = get_collector(SPORT_MAPPINGS[sport_name])
            standings = collector.get_standings() if collector and hasattr(collector, 'get_standings') else []
            output += f"{SPORT_MAPPINGS[sport_name]} [Standings]\n"
            output += "-" * 45 + "\n"
            if standings:
                output += f"  {'#':>2} {'Team':<5} {'M':>2} {'W':>2} {'L':>2} {'NR':>2} {'Pts':>3} {'NRR':>7}\n"
                output += f"  {'-' * 35}\n"
                for rec in standings:
                    output += (
                        f"  {rec['rank']:>2} {rec['abbreviation']:<5} {rec['matches']:>2} "
                        f"{rec['wins']:>2} {rec['losses']:>2} {rec['no_result']:>2} "
                        f"{rec['points']:>3} {rec['nrr']:>7}\n"
                    )
            else:
                output += "  No standings data available\n"
            output += "\n"
        elif sport_name == 'mls':
            collector = get_collector('MLS')
            standings = collector._fetch_standings() if collector else {}
            output += "MLS [Standings]\n"
            output += "-" * 45 + "\n"
            if standings:
                output += f"  {'Team':<5} {'W':>2} {'D':>2} {'L':>2} {'Pts':>3}\n"
                output += f"  {'-' * 20}\n"
                ordered = sorted(standings.items(), key=lambda x: -(x[1]['wins'] * 3 + x[1]['draws']))
                for abbrev, rec in ordered:
                    pts = rec['wins'] * 3 + rec['draws']
                    output += f"  {abbrev:<5} {rec['wins']:>2} {rec['draws']:>2} {rec['losses']:>2} {pts:>3}\n"
            else:
                output += "  No standings data available\n"
            output += "\n"
        elif sport_name == 'cycling':
            collector = get_collector('CYCLING')
            standings = collector.get_standings() if collector and hasattr(collector, 'get_standings') else []
            output += "CYCLING [Standings]\n"
            output += "-" * 45 + "\n"
            if standings:
                output += f"  {'#':>2} {'Team':<20} {'Pts':>4} {'GB':>6}\n"
                output += f"  {'-' * 40}\n"
                for rec in standings:
                    output += (
                        f"  {str(rec.get('rank') or ''):>2} {rec.get('team_name', ''):<20} "
                        f"{str(rec.get('points', 0) or 0):>4} {str(rec.get('games_back', '') or ''):>6}\n"
                    )
            else:
                output += "  No standings data available\n"
            output += "\n"
        elif sport_name in ('nba', 'mlb', 'nfl', 'nhl', 'wnba'):
            league = SPORT_MAPPINGS[sport_name]
            collector = get_collector(league)
            standings = collector.get_standings() if collector and hasattr(collector, 'get_standings') else []
            output += f"{league} [Standings]\n"
            output += "-" * 45 + "\n"
            if standings:
                if sport_name == 'nhl':
                    output += f"  {'#':>2} {'Team':<5} {'W':>2} {'L':>2} {'OT':>2} {'Pts':>3} {'STRK':>4}\n"
                    output += f"  {'-' * 34}\n"
                elif sport_name == 'nfl':
                    output += f"  {'#':>2} {'Team':<5} {'W':>2} {'L':>2} {'T':>2} {'PCT':>5} {'STRK':>4}\n"
                    output += f"  {'-' * 35}\n"
                else:
                    output += f"  {'#':>2} {'Team':<5} {'W':>2} {'L':>2} {'PCT':>5} {'GB':>4} {'STRK':>4}\n"
                    output += f"  {'-' * 34}\n"
                for rec in standings:
                    if sport_name == 'nhl':
                        output += (
                            f"  {rec['rank']:>2} {rec['abbreviation']:<5} {rec['wins']:>2} "
                            f"{rec['losses']:>2} {rec.get('ot', 0):>2} {rec.get('points', 0):>3} "
                            f"{rec['streak']:>4}\n"
                        )
                    elif sport_name == 'nfl':
                        output += (
                            f"  {rec['rank']:>2} {rec['abbreviation']:<5} {rec['wins']:>2} "
                            f"{rec['losses']:>2} {rec.get('ties', 0):>2} {rec['win_pct']:>5} "
                            f"{rec['streak']:>4}\n"
                        )
                    else:
                        output += (
                            f"  {rec['rank']:>2} {rec['abbreviation']:<5} {rec['wins']:>2} "
                            f"{rec['losses']:>2} {rec['win_pct']:>5} {rec['games_back']:>4} "
                            f"{rec['streak']:>4}\n"
                        )
            else:
                output += "  No standings data available\n"
            output += "\n"
        elif sport_name == 'wc':
            collector = get_collector('WC')
            groups = collector.get_group_standings() if collector and hasattr(collector, 'get_group_standings') else []
            output += "WC [Group Standings]\n"
            output += "-" * 45 + "\n"
            if groups:
                for group in groups:
                    group_label = group.get('group') or '?'
                    teams = group.get('teams') or []
                    output += f"  Group {group_label}\n"
                    output += f"    {'#':>1} {'Team':<5} {'GP':>2} {'W':>2} {'D':>2} {'L':>2} {'F':>2} {'A':>2} {'GD':>3} {'P':>3} {'R32':>3}\n"
                    for rec in teams:
                        r32 = "*" if rec.get('currently_advancing') else ""
                        output += (
                            f"    {rec.get('group_rank', rec.get('rank', 0)):>1} "
                            f"{rec.get('abbreviation', ''):<5} "
                            f"{rec.get('matches', 0):>2} {rec.get('wins', 0):>2} "
                            f"{rec.get('draws', 0):>2} {rec.get('losses', 0):>2} "
                            f"{rec.get('goals_for', 0):>2} {rec.get('goals_against', 0):>2} "
                            f"{rec.get('goal_difference', 0):>3} {rec.get('points', 0):>3} {r32:>3}\n"
                        )
                    output += "\n"
            else:
                output += "  No standings data available\n\n"
        else:
            output += f"{sport_name.upper()} standings endpoint - TODO\n\n"

    output += _format_curl_footer(pytz.timezone('US/Pacific'))
    return output


@app.get("/v1/standings/{sport}")
def get_standings_v1(
    request: Request,
    sport: str = Path(..., description="Sport (nba, mlb, nfl, nhl, wnba, mls, ipl, mlc, all)"),
    *,
    response: Response = None,
):
    """Canonical standings endpoint negotiated via Accept."""
    if _client_prefers_plain_text(request):
        return _add_vary_accept(PlainTextResponse(get_standings_curl_v1(sport)))
    if response is not None:
        _add_vary_accept(response)
    return get_standings_api_v1(sport)


@app.get("/v1/season-info/{league}", response_model=schemas.SeasonInfoResponse)
def get_season_info_v1(
    league: str = Path(..., description="League (mlb, nba, nfl, nhl, wnba)"),
):
    """Canonical season-info endpoint."""
    return get_season_info(league)


# Season info cache: {league: {'data': ..., 'timestamp': float}}
_season_info_cache: Dict[str, Any] = {}
_SEASON_INFO_TTL = 86400  # 24 hours


def _get_season_info_from_db(league: str) -> Optional[Dict[str, Any]]:
    """Derive season phase dates from game records in the database."""
    from sqlalchemy import func
    try:
        with get_db_session() as db:
            rows = db.query(
                Game.game_type,
                func.min(Game.game_date).label('start_date'),
                func.max(Game.game_date).label('end_date'),
            ).filter(
                Game.league == league,
            ).group_by(Game.game_type).all()
            source_updated_at = db.query(func.max(Game.updated_at)).filter(
                Game.league == league,
            ).scalar()

            if not rows:
                return None

            type_display = {
                'preseason': 'Preseason',
                'regular': 'Regular Season',
                'playoffs': 'Post Season (Playoffs)',
                'postseason': 'Post Season (Playoffs)',
                'allstar': 'All-Star',
                'nba_cup': 'Emirates NBA Cup',
            }
            type_order = ['preseason', 'regular', 'allstar', 'nba_cup', 'playoffs', 'postseason']

            season_types = []
            latest_year = None
            for game_type, start_d, end_d in rows:
                if game_type in ('postseason',) and any(r[0] == 'playoffs' for r in rows):
                    continue
                name = type_display.get(game_type, game_type.title().replace('_', ' '))
                season_types.append({
                    'name': name,
                    'start_date': start_d.isoformat(),
                    'end_date': end_d.isoformat(),
                    'game_type': game_type,
                })
                if latest_year is None or end_d.year > latest_year:
                    latest_year = end_d.year

            season_types.sort(key=lambda x: type_order.index(x['game_type']) if x['game_type'] in type_order else 99)
            for t in season_types:
                del t['game_type']

            today = datetime.now().strftime('%Y-%m-%d')
            current_phase = 'Off Season'
            for t in season_types:
                if t['start_date'] <= today <= t['end_date']:
                    current_phase = t['name']

            return {
                'year': latest_year or datetime.now().year,
                'current_phase': current_phase,
                'season_types': season_types,
                'source_updated_at': _normalize_iso_timestamp(source_updated_at),
            }
    except Exception as e:
        logger.error(f"Error deriving season info from DB for {league}: {e}")
        return None


def _get_wc_knockout_bracket_payload() -> Dict[str, Any]:
    """Build the World Cup knockout bracket payload once and reuse it.

    The collector already emits a structured lattice (`sides`, `rounds`,
    and match metadata); this helper exposes that shape directly to callers
    that do not want to reconstruct the bracket from the match feed.
    """
    collector = get_collector("WC")
    bracket: Dict[str, Any] = {}

    if collector:
        try:
            if hasattr(collector, "get_knockout_bracket"):
                bracket = collector.get_knockout_bracket() or {}
            upstream_health.record_success(upstream_health.upstream_for("wc", "season-info"))
        except Exception as e:
            upstream_health.record_failure(
                upstream_health.upstream_for("wc", "season-info"),
                f"{type(e).__name__}: {e}",
            )
            raise

    if not bracket:
        bracket = {
            "format": "round_of_32",
            "sides": {"left": [], "right": []},
            "rounds": [],
        }

    return {
        "sport": "wc",
        "knockout_bracket": bracket,
        "available": bool((bracket.get("rounds") or []) or (bracket.get("sides") or {}).get("left") or (bracket.get("sides") or {}).get("right")),
        "source_updated_at": _collector_source_updated_at(collector, "season-info"),
    }


@app.get("/api/v1/season-info/{league}", response_model=schemas.SeasonInfoResponse)
def get_season_info(
    league: str = Path(..., description="League (mlb, nba, nfl, nhl, wnba)"),
):
    """Get season phase dates for a league."""
    league_upper = league.upper()

    valid_leagues = set(v for v in SPORT_MAPPINGS.values())
    if league_upper not in valid_leagues:
        raise HTTPException(status_code=400, detail=f"Invalid league: {league}")

    def _fetch_season_info() -> Dict[str, Any]:
        collector = get_collector(league_upper)
        result = None

        if collector:
            try:
                result = collector.get_season_info()
                if result is not None and not result.get("source_updated_at"):
                    result["source_updated_at"] = _collector_source_updated_at(collector, "season-info")
                upstream_health.record_success(upstream_health.upstream_for(league_upper.lower(), 'season-info'))
            except Exception as e:
                upstream_health.record_failure(
                    upstream_health.upstream_for(league_upper.lower(), 'season-info'),
                    f"{type(e).__name__}: {e}",
                )
                result = None

        if not result:
            result = _get_season_info_from_db(league_upper)

        if not result:
            result = {"year": datetime.now().year, "current_phase": "Off Season", "season_types": []}

        if league_upper == "WC":
            try:
                bracket_payload, bracket_meta = _get_cached_payload(
                    _wc_bracket_cache,
                    "wc",
                    _WC_BRACKET_TTL,
                    _get_wc_knockout_bracket_payload,
                )
                result["knockout_bracket"] = bracket_payload.get("knockout_bracket")
                if not result.get("source_updated_at"):
                    result["source_updated_at"] = bracket_meta.get("source_updated_at")
            except Exception as e:
                logger.debug("WC knockout bracket lookup failed: %s", e)

        try:
            from .services.champions import get_last_champion
            champion = get_last_champion(league_upper)
            if champion:
                result['last_champion'] = champion
        except Exception as e:
            logger.debug("champion lookup failed for %s: %s", league_upper, e)

        return result

    payload, cache_snapshot = _get_cached_payload(
        _season_info_cache,
        league_upper,
        _SEASON_INFO_TTL,
        _fetch_season_info,
    )
    return {
        **payload,
        "meta": _build_endpoint_meta(
            cache_snapshot,
            _SEASON_INFO_TTL,
            source_updated_at=cache_snapshot.get("source_updated_at"),
            empty_state=cache_snapshot.get("empty_state"),
        ),
    }


@app.get("/api/v1/world-cup/bracket", response_model=schemas.WorldCupBracketResponse)
def get_world_cup_bracket_api_v1():
    """Get the World Cup knockout lattice in a frontend-friendly shape."""
    payload, cache_snapshot = _get_cached_payload(
        _wc_bracket_cache,
        "wc",
        _WC_BRACKET_TTL,
        _get_wc_knockout_bracket_payload,
    )
    return {
        **payload,
        "meta": _build_endpoint_meta(
            cache_snapshot,
            _WC_BRACKET_TTL,
            source_updated_at=cache_snapshot.get("source_updated_at"),
            empty_state=cache_snapshot.get("empty_state"),
        ),
    }


@app.get("/api/v1/cricket/{league}/season", response_model=schemas.CricketSeasonResponse)
def get_cricket_season(
    league: str = Path(..., description="Cricket league (ipl, mlc)"),
):
    """Full enriched season feed (every match with raw per-inning scores,
    standings, and CricAPI usage). Lets CricketPuff source its schedule cache
    from here instead of calling CricAPI directly."""
    league_lower = league.lower()
    if league_lower not in ('ipl', 'mlc'):
        raise HTTPException(status_code=400, detail=f"Invalid cricket league: {league}")
    league_upper = league_lower.upper()

    collector = get_collector(SPORT_MAPPINGS[league_lower])
    if not collector:
        raise HTTPException(status_code=503, detail="Cricket collector unavailable")
    payload = collector.get_season()
    if payload.get("status") == "error":
        return JSONResponse(
            status_code=503,
            content=_api_error_payload(
                503,
                "/api/v1/cricket/{league}/season",
                f"{league_upper} season feed unavailable",
                details={
                    "league": league_upper,
                    "reason": payload.get("reason") or "upstream_refresh_failed",
                },
            ),
        )
    if not payload.get("status"):
        payload["status"] = "ok" if payload.get("matches") else "off_season"
        payload["stale"] = False
        payload["reason"] = None if payload["status"] == "ok" else "off_season"
    return payload


@app.get("/health", response_model=schemas.HealthOut)
def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}


def _status_api_base(request_url: str) -> str:
    # Probe ourselves over the same origin the client used so dev/prod hosts
    # and ports don't need to be configured separately.
    from urllib.parse import urlsplit
    parts = urlsplit(request_url)
    return f"{parts.scheme}://{parts.netloc}"


@app.get("/api/v1/status", response_model=schemas.StatusResponse)
def api_status_json(request: Request):
    """JSON snapshot of upstream + own-endpoint health."""
    from .services.status import get_status
    return get_status(_status_api_base(str(request.url)))


@app.get("/curl/v1/status", response_class=PlainTextResponse)
def api_status_curl(
    request: Request,
    only: Optional[str] = Query(None, description="Filter rows: errors, warnings, all (default all)"),
):
    """Plain-text status table for terminal use."""
    from .services.status import get_status
    snap = get_status(_status_api_base(str(request.url)))

    keep = {"errors": {"error"}, "warnings": {"error", "warning"}}.get(
        (only or "").lower(), {"ok", "warning", "error"}
    )

    summary = snap["summary"]
    sum_line = f"summary: ok={summary.get('ok',0)}  warn={summary.get('warning',0)}  err={summary.get('error',0)}"

    up_lines = ["== UPSTREAMS =="]
    up_lines.append(f"{'CAT':<5} {'AGE':>7}  {'NAME':<16} DETAIL")
    shown_up = [u for u in snap["upstreams"] if u.get("category") in keep]
    if not shown_up:
        up_lines.append("(none)")
    for u in shown_up:
        age = u.get("age_seconds")
        age_s = f"{age}s" if age is not None else "-"
        up_lines.append(
            f"{u.get('category','')[:5]:<5} {age_s:>7}  {u.get('name',''):<16} {u.get('detail','')}"
        )

    res_lines = ["== RESULTS =="]
    res_lines.append(f"{'CAT':<5} {'SRC':<5} {'HTTP':>4} {'CNT':>4}  {'NAME':<28} {'UPSTREAM':<14} DETAIL")
    shown_res = [r for r in snap["results"] if r.get("category") in keep]
    if not shown_res:
        res_lines.append("(none)")
    for r in shown_res:
        sc = r.get("status_code")
        cnt = r.get("count")
        src = "synth" if (r.get("detail") or "").startswith("synth:") else "live"
        res_lines.append(
            f"{r.get('category','')[:5]:<5} "
            f"{src:<5} "
            f"{(str(sc) if sc is not None else '-'):>4} "
            f"{(str(cnt) if cnt is not None else '-'):>4}  "
            f"{r.get('name',''):<28} "
            f"{(r.get('upstream') or '-'):<14} "
            f"{r.get('detail','')}"
        )

    return "\n".join([
        f"sportspuff-api status  ({snap['checked_at']})",
        f"base: {snap['api_base_url']}",
        sum_line,
        "",
        *up_lines,
        "",
        *res_lines,
        "",
    ])


@app.get("/v1/status")
def api_status_v1(
    request: Request,
    only: Optional[str] = Query(None, description="Filter rows: errors, warnings, all (default all)"),
    *,
    response: Response = None,
):
    """Canonical status endpoint negotiated via Accept."""
    if _client_prefers_plain_text(request):
        return _add_vary_accept(PlainTextResponse(api_status_curl(request, only)))
    if response is not None:
        _add_vary_accept(response)
    return api_status_json(request)


@app.get("/status", response_class=HTMLResponse)
def api_status_page(request: Request):
    """HTML status page (upstreams + per-endpoint health)."""
    from .services.status import get_status
    snap = get_status(_status_api_base(str(request.url)))

    def chip(cat):
        cls = {"ok": "chip-ok", "warning": "chip-warn", "error": "chip-err"}.get(cat, "chip-err")
        return f"<span class='chip {cls}'>{cat}</span>"

    def upstream_row(u):
        age = u.get("age_seconds")
        age_s = f"{age}s" if age is not None else ""
        last_ok = u.get("last_success_at") or ""
        last_err = u.get("last_error_at") or ""
        stale = "yes" if u.get("stale") else "no"
        return (
            f"<tr>"
            f"<td>{chip(u.get('category','error'))}</td>"
            f"<td>{u.get('name','')}</td>"
            f"<td class='mono'>{u.get('detail','')}</td>"
            f"<td class='mono num'>{age_s}</td>"
            f"<td class='mono num'>{stale}</td>"
            f"<td class='mono'>{last_ok}</td>"
            f"<td class='mono'>{last_err}</td>"
            f"</tr>"
        )

    def result_row(r):
        sc = r.get("status_code")
        sc_s = str(sc) if sc is not None else ""
        cnt = r.get("count")
        cnt_s = str(cnt) if cnt is not None else ""
        is_synth = (r.get("detail") or "").startswith("synth:")
        method_chip = (
            "<span class='chip chip-synth' title='Derived from in-memory bookkeeping (no live probe — protects upstream quota)'>synth</span>"
            if is_synth else
            "<span class='chip chip-live' title='Live HTTP probe of this endpoint'>live</span>"
        )
        meta = r.get("meta") or {}
        meta_s = ""
        if meta:
            ms = "stale" if meta.get("stale") else "fresh"
            age = meta.get("age_seconds")
            ttl = meta.get("ttl_seconds")
            src = meta.get("source") or "?"
            meta_s = f"{ms} · src={src}"
            if age is not None and ttl is not None:
                meta_s += f" · {age}s/{ttl}s"
        return (
            f"<tr>"
            f"<td>{chip(r.get('category','error'))}</td>"
            f"<td>{method_chip}</td>"
            f"<td>{r.get('name','')}</td>"
            f"<td class='mono'>{r.get('detail','')}</td>"
            f"<td class='mono num'>{sc_s}</td>"
            f"<td class='mono num'>{cnt_s}</td>"
            f"<td>{r.get('upstream') or ''}</td>"
            f"<td class='mono'>{meta_s}</td>"
            f"<td class='mono url'>{r.get('url','')}</td>"
            f"</tr>"
        )

    s = snap["summary"]
    summary_html = (
        f"<span class='chip chip-ok'>ok {s.get('ok',0)}</span>"
        f"<span class='chip chip-warn'>warn {s.get('warning',0)}</span>"
        f"<span class='chip chip-err'>err {s.get('error',0)}</span>"
    )

    up_table = (
        "<table><thead><tr>"
        "<th>Status</th><th>Upstream</th><th>Detail</th>"
        "<th>Age</th><th>Stale</th><th>Last OK</th><th>Last err</th>"
        "</tr></thead><tbody>"
        + "".join(upstream_row(u) for u in snap["upstreams"])
        + "</tbody></table>"
    )
    res_table = (
        "<table><thead><tr>"
        "<th>Status</th><th>Source</th><th>Name</th><th>Detail</th>"
        "<th>HTTP</th><th>Count</th><th>Upstream</th><th>Meta</th><th>URL</th>"
        "</tr></thead><tbody>"
        + "".join(result_row(r) for r in snap["results"])
        + "</tbody></table>"
    )

    return f"""<!DOCTYPE html>
<html lang="en"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>SportsPuff API · Status</title>
<link rel="icon" type="image/png" href="https://www.splitsp.lat/logos/sportspuff/sportspuff-logo.png">
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif;
  background:linear-gradient(135deg,#1A0B3D 0%,#2D1B69 50%,#3D2A7A 100%);
  color:#F5F5F5;min-height:100vh}}
header{{background:linear-gradient(135deg,#2D1B69 0%,#FF3B30 100%);
  padding:1.5rem;text-align:center;box-shadow:0 2px 10px rgba(0,0,0,.3)}}
header h1{{font-size:1.8rem}}
header p{{font-size:.9rem;color:rgba(245,245,245,.75);margin-top:.25rem}}
.container{{max-width:1200px;margin:1.5rem auto;padding:1.5rem;
  background:rgba(26,11,61,.9);border-radius:14px;
  border:1px solid rgba(255,255,255,.15)}}
h2{{font-size:1.15rem;color:#FFB400;margin:1.25rem 0 .5rem}}
h2:first-child{{margin-top:0}}
.summary{{margin-bottom:.5rem;font-size:.85rem}}
table{{width:100%;border-collapse:collapse;font-size:.83rem;margin-bottom:1rem}}
th,td{{padding:.4rem .55rem;text-align:left;
  border-bottom:1px solid rgba(255,255,255,.08);vertical-align:top}}
th{{font-weight:600;color:#B8B8B8;font-size:.76rem;text-transform:uppercase;letter-spacing:.04em}}
.mono{{font-family:'SF Mono',Menlo,monospace;font-size:.76rem}}
.num{{text-align:right;white-space:nowrap}}
.url{{color:#9aa;word-break:break-all}}
.chip{{display:inline-block;padding:.1rem .5rem;border-radius:10px;
  font-size:.72rem;font-weight:600;margin-right:.3rem}}
.chip-ok{{background:rgba(46,160,67,.25);color:#7ee08a}}
.chip-warn{{background:rgba(255,180,0,.25);color:#FFB400}}
.chip-err{{background:rgba(255,59,48,.25);color:#ff8a82}}
.chip-live{{background:rgba(112,40,228,.25);color:#c4a0ff}}
.chip-synth{{background:rgba(255,255,255,.08);color:#9aa;border:1px dashed rgba(255,255,255,.2)}}
footer{{text-align:center;padding:1.5rem;font-size:.75rem;color:rgba(245,245,245,.4)}}
</style>
</head><body>
<header><h1>API status</h1><p>checked {snap['checked_at']} · {snap['api_base_url']}</p></header>
<div class="container">
  <h2>Upstreams</h2>
  {up_table}
  <h2>Endpoints <span class="summary">{summary_html}</span></h2>
  {res_table}
</div>
<footer>JSON: <a href="/api/v1/status" style="color:#9aa">/api/v1/status</a> · curl: <a href="/curl/v1/status" style="color:#9aa">/curl/v1/status</a></footer>
</body></html>""".replace(SPORTSPUFF_LOGO_REMOTE, SPORTSPUFF_LOGO_URL)


# Catch-all routes for unknown /api/ and /curl/ paths - return help
@app.get("/api/v1/debug/{sport}/{date}")
def debug_schedule_data(
    sport: str = Path(..., description="Sport (nba, mlb, nfl, nhl, wnba)"),
    date: str = Path(..., description="Date: today/tomorrow/yesterday, YYYY-MM-DD, etc."),
    tz: Optional[str] = Query(None, description="Timezone for relative dates (default: Pacific)"),
):
    """Debug endpoint to see raw schedule data."""
    try:
        timezone = get_timezone(tz)
        target_date = parse_date_param(date, timezone)
        sport_lower = sport.lower()
        league = SPORT_MAPPINGS.get(sport_lower)
        
        if not league:
            raise HTTPException(status_code=400, detail=f"Invalid sport: {sport}")
        
        # Get data from collector
        collector = get_collector(league)
        collector_data = []
        raw_api_team_data = []  # For NHL, show raw team structure
        if collector:
            set_collector_timezone(collector, timezone)
            collector_data = collector.get_schedule(target_date)
            # For NHL, also get raw API response to inspect team structure
            if sport_lower == 'nhl' and collector_data:
                # Get raw API response by making a direct call
                try:
                    from datetime import datetime
                    date_str = target_date.strftime('%Y-%m-%d')
                    url = f"https://api-web.nhle.com/v1/schedule/{date_str}"
                    import requests
                    response = requests.get(url, timeout=10)
                    if response.status_code == 200:
                        data = response.json()
                        # Extract first game's team structure if available
                        if 'gameWeek' in data and len(data['gameWeek']) > 0:
                            for day in data['gameWeek']:
                                if 'games' in day and len(day['games']) > 0:
                                    first_game = day['games'][0]
                                    raw_api_team_data = {
                                        "homeTeam_keys": list(first_game.get('homeTeam', {}).keys()) if 'homeTeam' in first_game else [],
                                        "awayTeam_keys": list(first_game.get('awayTeam', {}).keys()) if 'awayTeam' in first_game else [],
                                        "homeTeam_sample": {k: v for k, v in first_game.get('homeTeam', {}).items() if k in ['wins', 'losses', 'otLosses', 'ot_losses', 'overtimeLosses', 'ot', 'otl']} if 'homeTeam' in first_game else {},
                                        "awayTeam_sample": {k: v for k, v in first_game.get('awayTeam', {}).items() if k in ['wins', 'losses', 'otLosses', 'ot_losses', 'overtimeLosses', 'ot', 'otl']} if 'awayTeam' in first_game else {},
                                    }
                                    break
                except Exception as e:
                    raw_api_team_data = {"error": str(e)}
        
        # Get data from live scores
        live_data = []
        if collector:
            live_data = collector.get_live_scores(target_date)
        
        # Get data from database
        db_data = []
        with get_db_session() as db:
            db_games = db.query(Game).filter(
                Game.league == league,
                Game.game_date == target_date
            ).order_by(Game.game_time).all()
            
            db_data = [
                {
                    "game_id": game.game_id,
                    "home_team": game.home_team,
                    "home_team_abbrev": game.home_team_abbrev,
                    "visitor_team": game.visitor_team,
                    "visitor_team_abbrev": game.visitor_team_abbrev,
                    "home_wins": game.home_wins,
                    "home_losses": game.home_losses,
                    "visitor_wins": game.visitor_wins,
                    "visitor_losses": game.visitor_losses,
                    "game_time": game.game_time.isoformat() if game.game_time else None,
                    "game_status": game.game_status,
                }
                for game in db_games
            ]
        
        # Get what _get_games_for_curl would return
        curl_games = _get_games_for_curl(league, target_date, timezone)
        curl_data = []
        for game in curl_games:
            curl_data.append({
                "game_id": getattr(game, 'game_id', None),
                "home_team": getattr(game, 'home_team', None),
                "home_team_abbrev": getattr(game, 'home_team_abbrev', None),
                "visitor_team": getattr(game, 'visitor_team', None),
                "visitor_team_abbrev": getattr(game, 'visitor_team_abbrev', None),
                "home_wins": getattr(game, 'home_wins', None),
                "home_losses": getattr(game, 'home_losses', None),
                "visitor_wins": getattr(game, 'visitor_wins', None),
                "visitor_losses": getattr(game, 'visitor_losses', None),
                "game_time": getattr(game, 'game_time', None),
                "league": getattr(game, 'league', None),
            })
        
        result = {
            "sport": sport,
            "league": league,
            "date": target_date.isoformat(),
            "collector_schedule": collector_data[:3] if collector_data else [],  # First 3 games
            "collector_live_scores": live_data[:3] if live_data else [],  # First 3 games
            "database_games": db_data[:3] if db_data else [],  # First 3 games
            "curl_format_games": curl_data[:3] if curl_data else [],  # First 3 games
            "counts": {
                "collector_schedule": len(collector_data),
                "collector_live_scores": len(live_data),
                "database_games": len(db_data),
                "curl_format_games": len(curl_data),
            }
        }
        # Add raw API team structure for NHL
        if sport_lower == 'nhl' and raw_api_team_data:
            result["raw_api_team_structure"] = raw_api_team_data
        
        return result
    except Exception as e:
        return _internal_error_response("/api/v1/debug/{sport}/{date}", e)


@app.get("/api/{path:path}")
def api_catch_all(path: str):
    """Catch-all for unknown /api/ paths - returns JSON help."""
    return get_help_json()


@app.get("/curl/{path:path}", response_class=PlainTextResponse)
def curl_catch_all(path: str, request: Request):
    """Catch-all for unknown /curl/ paths - returns plain text help."""
    return get_help_text(_request_base_url(request))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=settings.api_host, port=settings.api_port)
