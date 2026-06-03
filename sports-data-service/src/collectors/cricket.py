"""
Cricket data collector for the sports data service.

Uses CricAPI directly when CRICAPI_KEY is configured. CricketPuff remains as a
fallback so older deployments keep working while credentials are rolled out.
"""

import requests
import re
import os
import json
import time
from datetime import datetime, date, timedelta, timezone
from typing import Dict, List, Optional, Any
import logging
import pytz

from .base import BaseCollector
from ..config import settings

logger = logging.getLogger(__name__)

CRICKETPUFF_BASE = "https://ipl.cloud-puff.net/api/v1"
CRICAPI_BASE = "https://api.cricapi.com/v1"

NO_WINNER_VALUES = {"no winner", "no result", "match abandoned", "no result due to rain"}

LEAGUE_CONFIGS = {
    "IPL": {
        "search": "indian premier league",
        "name_match": "indian premier league",
        "tbc_start": 71,
        "teams": {
            "Chennai Super Kings": "CSK",
            "Delhi Capitals": "DC",
            "Gujarat Titans": "GT",
            "Kolkata Knight Riders": "KKR",
            "Lucknow Super Giants": "LSG",
            "Mumbai Indians": "MI",
            "Punjab Kings": "PBKS",
            "Rajasthan Royals": "RR",
            "Royal Challengers Bengaluru": "RCB",
            "Sunrisers Hyderabad": "SRH",
        },
        "aliases": {
            "Royal Challengers Bangalore": "Royal Challengers Bengaluru",
        },
        "home_grounds": {
            "Narendra Modi Stadium, Ahmedabad": "Gujarat Titans",
            "M. Chinnaswamy Stadium, Bengaluru": "Royal Challengers Bengaluru",
            "MA Chidambaram Stadium, Chennai": "Chennai Super Kings",
            "Arun Jaitley Stadium, Delhi": "Delhi Capitals",
            "HPCA Stadium, Dharamsala": "Punjab Kings",
            "Rajiv Gandhi International Stadium, Hyderabad": "Sunrisers Hyderabad",
            "Eden Gardens, Kolkata": "Kolkata Knight Riders",
            "Wankhede Stadium, Mumbai": "Mumbai Indians",
            "Sawai Mansingh Stadium, Jaipur": "Rajasthan Royals",
            "Bharat Ratna Shri Atal Bihari Vajpayee Ekana Cricket Stadium, Lucknow": "Lucknow Super Giants",
        },
    },
    "MLC": {
        "search": "major league cricket",
        "name_match": "major league cricket",
        "tbc_start": 31,
        "teams": {
            "Los Angeles Knight Riders": "LAKR",
            "Mi New York": "MINY",
            "San Francisco Unicorns": "SFU",
            "Seattle Orcas": "SEA",
            "Texas Super Kings": "TSK",
            "Washington Freedom": "WAS",
        },
        "aliases": {
            "MI New York": "Mi New York",
        },
        "home_grounds": {
            "Grand Prairie Stadium, Grand Prairie": "Texas Super Kings",
            "Church Street Park, Morrisville": "Washington Freedom",
            "Central Broward Regional Park, Lauderhill": "Mi New York",
            "Nassau County International Cricket Stadium, East Meadow": "Mi New York",
        },
    },
}

_cricapi_cache: Dict[str, Dict[str, Any]] = {}
_MATCH_INFO_TTL = 86400  # ended-match details don't change; keep for a day
# Whole-season response cache (TTL from settings.cricapi_season_cache_ttl):
# bounds CricAPI spend if the season feed is hit frequently, since each live
# build force-refreshes in-progress matches.
_season_response_cache: Dict[str, Dict[str, Any]] = {}
# Sliding-window timestamps of CricAPI calls for hourly circuit-breaker. We
# track our own count because CricAPI's info.hitsToday is daily-only.
from collections import deque as _deque
_cricapi_recent_calls: "_deque[float]" = _deque()
_DEFAULT_CACHE_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "cache", "cricket"
)

# Latest CricAPI account usage, read from each response's "info" block. CricAPI
# reports account-wide hits, so this reflects the quota shared with other apps.
_cricapi_usage: Dict[str, Any] = {"hits_today": 0, "hits_limit": None, "date": None}
_usage_loaded = False


class CricAPIBudgetExceeded(Exception):
    """Raised when the shared CricAPI daily quota is (near) exhausted."""


class CricketCollector(BaseCollector):
    """Cricket data collector using CricAPI with CricketPuff fallback."""

    def __init__(self, league: str = "IPL"):
        super().__init__(league)
        self.league_slug = league.lower()
        self.cricapi_key = settings.cricapi_key
        self.config = LEAGUE_CONFIGS.get(self.league, LEAGUE_CONFIGS["IPL"])
        self.timezone = pytz.timezone('US/Pacific')

    def set_timezone(self, timezone: pytz.BaseTzInfo) -> None:
        self.timezone = timezone or pytz.timezone('US/Pacific')

    def get_schedule(self, date: Optional[date] = None) -> List[Dict[str, Any]]:
        self._check_rate_limit()
        if self.cricapi_key:
            try:
                target_date = date or datetime.now(self.timezone).date()
                matches = self._get_cricapi_matches(target_date)
                standings = self._calculate_standings(matches)
                cricapi_games = [
                    self._parse_cricapi_match(m, standings)
                    for m in matches
                    if self._match_date(m) == target_date
                ]
                if not cricapi_games:
                    current_matches = self._get_current_cricapi_matches(target_date)
                    if current_matches:
                        standings = self._calculate_standings(matches + current_matches)
                        cricapi_games = [self._parse_cricapi_match(m, standings) for m in current_matches]
                if cricapi_games:
                    return cricapi_games
                logger.info("No %s matches found from CricAPI for %s; trying CricketPuff fallback", self.league, target_date)
            except Exception as e:
                logger.error(f"Error fetching {self.league} schedule from CricAPI: {e}")

        try:
            date_param = date.strftime('%Y%m%d') if date else 'today'
            url = f"{CRICKETPUFF_BASE}/schedule/{self.league_slug}/{date_param}"
            response = requests.get(url, timeout=self.api_timeout)
            if response.status_code == 200:
                data = response.json()
                api_date = data.get('date', '')
                return [self._parse_schedule_match(m, api_date) for m in data.get('matches', []) if m]
            return []
        except Exception as e:
            logger.error(f"Error fetching {self.league} schedule: {e}")
            return []

    def get_live_scores(self, date: Optional[date] = None) -> List[Dict[str, Any]]:
        self._check_rate_limit()
        if self.cricapi_key:
            try:
                target_date = date or datetime.now(self.timezone).date()
                matches = self._get_cricapi_matches(target_date)
                standings = self._calculate_standings(matches)
                cricapi_games = [
                    self._parse_cricapi_match(m, standings)
                    for m in matches
                    if self._match_date(m) == target_date
                ]
                if not cricapi_games:
                    current_matches = self._get_current_cricapi_matches(target_date)
                    if current_matches:
                        standings = self._calculate_standings(matches + current_matches)
                        cricapi_games = [self._parse_cricapi_match(m, standings) for m in current_matches]
                if cricapi_games:
                    return cricapi_games
                logger.info("No %s scores found from CricAPI for %s; trying CricketPuff fallback", self.league, target_date)
            except Exception as e:
                logger.error(f"Error fetching {self.league} scores from CricAPI: {e}")

        try:
            date_param = date.strftime('%Y%m%d') if date else 'today'

            # Fetch both schedule (has records, start times) and scores (has results)
            sched_url = f"{CRICKETPUFF_BASE}/schedule/{self.league_slug}/{date_param}"
            scores_url = f"{CRICKETPUFF_BASE}/scores/{self.league_slug}/{date_param}"

            sched_resp = requests.get(sched_url, timeout=self.api_timeout)
            scores_resp = requests.get(scores_url, timeout=self.api_timeout)

            sched_by_match = {}
            if sched_resp.status_code == 200:
                for m in sched_resp.json().get('matches', []):
                    sched_by_match[m.get('match_no')] = m

            api_date = ''
            results = []
            if scores_resp.status_code == 200:
                scores_data = scores_resp.json()
                api_date = scores_data.get('date', '')
                for m in scores_data.get('matches', []):
                    sched = sched_by_match.get(m.get('match_no'), {})
                    results.append(self._parse_merged_match(m, sched, api_date))

            # Add any scheduled matches that don't have scores yet
            scored_nos = {m.get('match_no') for m in scores_resp.json().get('matches', [])} if scores_resp.status_code == 200 else set()
            for match_no, sched in sched_by_match.items():
                if match_no not in scored_nos:
                    results.append(self._parse_schedule_match(sched, api_date))

            return results
        except Exception as e:
            logger.error(f"Error fetching {self.league} scores: {e}")
            return []

    def parse_game_data(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return self._parse_schedule_match(raw, '')

    def get_standings(self) -> List[Dict[str, Any]]:
        """Return standings derived from CricAPI match data."""
        if not self.cricapi_key:
            return []

        try:
            matches = self._get_cricapi_matches()
        except Exception as e:
            logger.error(f"Error fetching {self.league} standings from CricAPI: {e}")
            return []
        standings = self._calculate_standings(matches)
        ordered = sorted(
            standings.values(),
            key=lambda rec: (rec["points"], rec["nrr_value"]),
            reverse=True,
        )
        for rank, rec in enumerate(ordered, 1):
            rec["rank"] = rank
        return ordered

    def get_season(self) -> Dict[str, Any]:
        """Return the full enriched season: every match with raw per-inning
        scores, plus derived standings and current CricAPI usage. This is the
        single-source feed CricketPuff consumes so it no longer hits CricAPI.

        Never raises: on quota exhaustion or upstream error it serves whatever
        is cached and flags the payload with live=False so consumers can choose
        to keep their existing snapshot instead of overwriting with stale/empty
        data. Results are briefly cached to bound CricAPI spend under load."""
        cache_key = f"{self.league}:season"
        cached = _season_response_cache.get(cache_key)
        if cached and time.time() - cached["timestamp"] < settings.cricapi_season_cache_ttl:
            return cached["data"]

        series = None
        matches = []
        live = True
        try:
            series = self._find_series()
            if self.cricapi_key:
                matches = self._get_cricapi_matches()
        except CricAPIBudgetExceeded:
            live = False
            logger.warning("CricAPI budget exceeded building %s season; serving cached data", self.league)
        except Exception as e:
            live = False
            logger.error("Error building %s season: %s", self.league, e)

        standings = self._calculate_standings(matches) if matches else {}
        ordered = sorted(
            standings.values(),
            key=lambda rec: (rec["points"], rec["nrr_value"]),
            reverse=True,
        )
        for rank, rec in enumerate(ordered, 1):
            rec["rank"] = rank

        hits_today = _cricapi_usage["hits_today"]
        payload = {
            "league": self.league,
            "series_id": series.get("id", "") if series else "",
            "series_name": series.get("name", "") if series else "",
            "live": live,
            "matches": matches,
            "standings": ordered,
            "api_stats": {
                "hits_today": hits_today,
                "hits_used": hits_today,
                "hits_limit": _cricapi_usage["hits_limit"] or self._usage_limit(),
                "date": _cricapi_usage["date"],
            },
        }
        _season_response_cache[cache_key] = {"data": payload, "timestamp": time.time()}
        return payload

    def _cricapi_get(self, endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
        self._enforce_usage_budget()
        params = dict(params)
        params["apikey"] = self.cricapi_key
        _cricapi_recent_calls.append(time.time())
        try:
            response = requests.get(f"{CRICAPI_BASE}/{endpoint}", params=params, timeout=self.api_timeout)
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            try:
                from ..services.upstream_health import record_failure
                record_failure("CricAPI", f"{type(e).__name__}: {e}")
            except Exception:
                pass
            raise
        self._track_usage(data)
        try:
            from ..services.upstream_health import record_success
            record_success("CricAPI")
        except Exception:
            pass
        return data

    def _track_usage(self, data: Dict[str, Any]) -> None:
        info = data.get("info") or {}
        if "hitsToday" in info:
            _cricapi_usage["hits_today"] = info.get("hitsToday", _cricapi_usage["hits_today"])
            _cricapi_usage["date"] = datetime.now(timezone.utc).date().isoformat()
        if info.get("hitsLimit"):
            _cricapi_usage["hits_limit"] = info["hitsLimit"]
        if "hitsToday" in info:
            self._persist_usage()

    def _usage_file(self) -> str:
        base = settings.cricapi_cache_dir or _DEFAULT_CACHE_DIR
        return os.path.join(base, "usage.json")

    def _persist_usage(self) -> None:
        try:
            os.makedirs(os.path.dirname(self._usage_file()), exist_ok=True)
            with open(self._usage_file(), "w") as f:
                json.dump({
                    "date": _cricapi_usage["date"],
                    "hits_today": _cricapi_usage["hits_today"],
                    "hits_limit": _cricapi_usage["hits_limit"],
                }, f)
        except Exception as e:
            logger.debug("Could not persist CricAPI usage: %s", e)

    def _load_usage(self) -> None:
        # Timer-driven processes (poller/updater) are short-lived, so seed the
        # shared usage counter from disk to honour the daily cap across restarts.
        global _usage_loaded
        if _usage_loaded:
            return
        _usage_loaded = True
        try:
            with open(self._usage_file()) as f:
                saved = json.load(f)
            if saved.get("date") == datetime.now(timezone.utc).date().isoformat():
                _cricapi_usage["hits_today"] = saved.get("hits_today", 0)
                _cricapi_usage["hits_limit"] = saved.get("hits_limit")
                _cricapi_usage["date"] = saved.get("date")
        except Exception:
            pass

    def _usage_limit(self) -> int:
        return _cricapi_usage["hits_limit"] or settings.cricapi_max_requests_per_day

    def _cricapi_can_fetch(self) -> bool:
        self._load_usage()
        # Reset the counter when CricAPI's daily quota rolls over at UTC midnight,
        # otherwise a blocked process would never probe again to learn it's clear.
        today = datetime.now(timezone.utc).date().isoformat()
        if _cricapi_usage["date"] != today:
            _cricapi_usage["hits_today"] = 0
            _cricapi_usage["date"] = today

        # Hourly circuit-breaker: prune timestamps older than 1h, then check
        # against the configured hourly cap. Catches runaway fan-outs in
        # minutes instead of letting them drain the daily quota.
        now_ts = time.time()
        while _cricapi_recent_calls and now_ts - _cricapi_recent_calls[0] > 3600:
            _cricapi_recent_calls.popleft()
        if len(_cricapi_recent_calls) >= settings.cricapi_max_requests_per_hour:
            logger.warning(
                "CricAPI hourly circuit-breaker tripped: %s/%s in last 60 min",
                len(_cricapi_recent_calls),
                settings.cricapi_max_requests_per_hour,
            )
            return False

        hits = _cricapi_usage["hits_today"]
        if not hits:
            return True
        return hits < max(0, self._usage_limit() - settings.cricapi_usage_reserve)

    def _enforce_usage_budget(self) -> None:
        if not self._cricapi_can_fetch():
            now_ts = time.time()
            recent = sum(1 for ts in _cricapi_recent_calls if now_ts - ts <= 3600)
            hourly_cap = settings.cricapi_max_requests_per_hour
            if recent >= hourly_cap:
                reason = f"hourly cap reached ({recent}/{hourly_cap} in last 60 min)"
            else:
                reason = (
                    f"daily usage {_cricapi_usage['hits_today']}/{self._usage_limit()} "
                    f"(reserve {settings.cricapi_usage_reserve}) reached"
                )
            try:
                from ..services.upstream_health import record_failure
                record_failure("CricAPI", f"budget gate: {reason}")
            except Exception:
                pass
            raise CricAPIBudgetExceeded(f"CricAPI {reason}")

    def _disk_cache_path(self, key: str) -> str:
        base = settings.cricapi_cache_dir or _DEFAULT_CACHE_DIR
        safe = re.sub(r'[^A-Za-z0-9._-]', '_', key)
        return os.path.join(base, f"{safe}.json")

    def _read_disk_cache(self, key: str, ttl: int):
        path = self._disk_cache_path(key)
        try:
            if os.path.exists(path) and time.time() - os.path.getmtime(path) < ttl:
                with open(path) as f:
                    return json.load(f)
        except Exception as e:
            logger.debug("Could not read cricket disk cache %s: %s", key, e)
        return None

    def _write_disk_cache(self, key: str, data: Any) -> None:
        path = self._disk_cache_path(key)
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w") as f:
                json.dump(data, f)
        except Exception as e:
            logger.debug("Could not write cricket disk cache %s: %s", key, e)

    def _cached(self, key: str, fetcher, ttl: int = None):
        if ttl is None:
            ttl = settings.cricapi_cache_ttl
        now = time.time()
        mem = _cricapi_cache.get(key)
        if mem and now - mem["timestamp"] < ttl:
            return mem["data"]
        disk = self._read_disk_cache(key, ttl)
        if disk is not None:
            _cricapi_cache[key] = {"data": disk, "timestamp": now}
            return disk
        try:
            data = fetcher()
        except CricAPIBudgetExceeded:
            # Quota is spent: serve the last-known-good snapshot (ignoring TTL)
            # rather than failing, so consumers keep working until it resets.
            stale = mem["data"] if mem else self._read_disk_cache(key, ttl=float("inf"))
            if stale is not None:
                logger.warning("CricAPI budget exceeded; serving stale cache for %s", key)
                _cricapi_cache[key] = {"data": stale, "timestamp": now}
                return stale
            raise
        _cricapi_cache[key] = {"data": data, "timestamp": now}
        self._write_disk_cache(key, data)
        return data

    def _find_series(self, target_date: Optional[date] = None) -> Optional[Dict[str, Any]]:
        target = target_date or datetime.now(self.timezone).date()

        def fetch():
            candidates = []
            seen_ids = set()
            best_in_window = None
            for offset in range(0, 100, 25):
                data = self._cricapi_get("series", {"offset": offset, "search": self.config["search"]})
                page = data.get("data", []) or []
                for series in page:
                    series_id = series.get("id")
                    if series_id in seen_ids:
                        continue
                    if self.config["name_match"] in series.get("name", "").lower():
                        candidates.append(series)
                        seen_ids.add(series_id)
                        start = self._series_date(series.get("startDate"))
                        end = self._series_date(series.get("endDate"))
                        if start and end and start <= target <= end:
                            best_in_window = series
                # Stop paging as soon as we have the series covering the target date.
                if best_in_window or len(page) < 25:
                    break

            if not candidates:
                return None
            if best_in_window:
                return best_in_window

            target_year = str(target.year)
            for series in candidates:
                if target_year in series.get("name", ""):
                    return series

            def sort_key(series):
                end = self._series_date(series.get("endDate")) or date.min
                start = self._series_date(series.get("startDate")) or date.min
                return (end, start)

            return sorted(candidates, key=sort_key, reverse=True)[0]
        cache_date = target_date.isoformat() if target_date else "current"
        return self._cached(f"{self.league}:series:{cache_date}", fetch)

    def _get_cricapi_matches(self, target_date: Optional[date] = None) -> List[Dict[str, Any]]:
        series = self._find_series(target_date)
        if not series:
            return []

        def fetch_series_info():
            data = self._cricapi_get("series_info", {"id": series["id"]})
            return data.get("data", {}).get("matchList", [])

        matches = self._cached(f"{self.league}:matches:{series['id']}", fetch_series_info)
        enriched = []
        focus = target_date or datetime.now(self.timezone).date()
        for raw in matches:
            match = dict(raw)
            if match.get("id"):
                is_live = bool(match.get("matchStarted")) and not match.get("matchEnded")
                match_date = self._match_date(match)
                # Force a fresh fetch only for live games and matches that ended in
                # the last 24 hours (to capture final scores). Everything else
                # is served from the persistent cache, so historical enrichment is a
                # one-time cost rather than a per-refresh fan-out.
                recently_ended = bool(match.get("matchEnded")) and match_date and (focus - match_date) <= timedelta(days=1)
                force = settings.cricapi_live_refresh and (is_live or bool(recently_ended))
                try:
                    info = self._get_match_info(match["id"], force_refresh=force)
                except CricAPIBudgetExceeded:
                    info = None
                if info:
                    match.update({k: v for k, v in info.items() if v not in (None, "", [])})

            self._assign_home_away(match)
            enriched.append(match)

        enriched.sort(key=lambda m: m.get("dateTimeGMT", ""))
        tbc_no = self.config["tbc_start"]
        current_no = 1
        for match in enriched:
            teams = match.get("teams", [])
            is_tbc = teams and all(t.lower().startswith(("tbc", "tba", "to be")) for t in teams)
            match["matchNo"] = tbc_no if is_tbc else current_no
            if is_tbc:
                tbc_no += 1
            else:
                current_no += 1
        return enriched

    def _get_current_cricapi_matches(self, target_date: date) -> List[Dict[str, Any]]:
        try:
            series = self._find_series(target_date)
            series_id = series.get("id") if series else ""
            data = self._cricapi_get("currentMatches", {"offset": 0})
            matches = []
            known_teams = set(self.config["teams"].keys()) | set(self.config.get("aliases", {}).keys())
            for raw in data.get("data", []) or []:
                match = dict(raw)
                teams = [self._canonical(team) for team in match.get("teams", [])]
                same_series = bool(series_id and match.get("series_id") == series_id)
                has_known_team = any(team in known_teams for team in teams)
                if (same_series or has_known_team) and self._match_date(match) == target_date:
                    self._assign_home_away(match)
                    matches.append(match)
            return matches
        except Exception as e:
            logger.debug("Could not fetch current %s matches from CricAPI: %s", self.league, e)
            return []

    def _series_date(self, value: Any) -> Optional[date]:
        if not value:
            return None
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00")).date()
        except Exception:
            try:
                return datetime.strptime(str(value), "%Y-%m-%d").date()
            except Exception:
                return None

    def _get_match_info(self, match_id: str, force_refresh: bool = False) -> Optional[Dict[str, Any]]:
        key = f"{self.league}:match:{match_id}"
        if force_refresh:
            data = self._cricapi_get("match_info", {"id": match_id}).get("data")
            if data:
                _cricapi_cache[key] = {"data": data, "timestamp": time.time()}
                self._write_disk_cache(key, data)
            return data
        return self._cached(
            key,
            lambda: self._cricapi_get("match_info", {"id": match_id}).get("data"),
            ttl=_MATCH_INFO_TTL,
        )

    def _assign_home_away(self, match: Dict[str, Any]) -> None:
        teams = match.get("teams", [])
        venue = match.get("venue", "")
        home = self.config["home_grounds"].get(venue)
        if home in teams:
            match["home_team"] = home
            match["visitor_team"] = next((t for t in teams if t != home), "")
        elif len(teams) >= 2:
            match["home_team"] = teams[0]
            match["visitor_team"] = teams[1]
        else:
            match["home_team"] = teams[0] if teams else ""
            match["visitor_team"] = ""
        match["home_team"] = self._canonical(match["home_team"])
        match["visitor_team"] = self._canonical(match["visitor_team"])

    def _match_date(self, match: Dict[str, Any]) -> Optional[date]:
        try:
            dt = datetime.fromisoformat(match.get("dateTimeGMT", "").replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(self.timezone).date()
        except Exception:
            return None

    def _match_time(self, match: Dict[str, Any]) -> Optional[datetime]:
        try:
            dt = datetime.fromisoformat(match.get("dateTimeGMT", "").replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            return None

    def _format_match_times(self, match: Dict[str, Any]) -> Dict[str, str]:
        dt = self._match_time(match)
        if not dt:
            return {"pt": "TBD", "utc": "TBD", "ist": "TBD"}
        pt = dt.astimezone(pytz.timezone("US/Pacific"))
        local = dt.astimezone(self.timezone)
        utc = dt.astimezone(pytz.utc)
        ist = dt.astimezone(pytz.timezone("Asia/Kolkata"))
        return {
            "local": local.strftime("%-I:%M%p %Z"),
            "pt": pt.strftime("%-I:%M%p %Z"),
            "utc": utc.strftime("%H:%M UTC"),
            "ist": ist.strftime("%H:%M IST"),
        }

    def _canonical(self, team_name: str) -> str:
        return self.config.get("aliases", {}).get(team_name, team_name)

    def _abbr(self, team_name: str) -> str:
        if not team_name:
            return ""
        team_name = self._canonical(team_name)
        return self.config["teams"].get(team_name, team_name[:4].upper())

    def _strip_inning(self, inning: str) -> str:
        return re.sub(r'\s+Inning\s+\d+\s*$', '', inning, flags=re.IGNORECASE).strip()

    def _overs_to_balls(self, overs: Any) -> int:
        overs_float = float(overs)
        full = int(overs_float)
        balls = round((overs_float - full) * 10)
        return full * 6 + balls

    def _score_map(self, match: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        scores = {}
        for score in match.get("score", []) or []:
            inning = score.get("inning", "")
            key = self._strip_inning(inning)
            if key and key not in scores:
                scores[key] = score
        return scores

    def _find_score(self, score_map: Dict[str, Dict[str, Any]], team: str) -> Dict[str, Any]:
        if team in score_map:
            return score_map[team]
        team_lower = team.lower()
        for key, value in score_map.items():
            key_lower = key.lower()
            canonical_key = self._canonical(key).lower()
            if (
                key_lower == team_lower
                or canonical_key == team_lower
                or team_lower in key_lower
                or key_lower in team_lower
                or team_lower in canonical_key
                or canonical_key in team_lower
            ):
                return value
        return {}

    def _fmt_score(self, score: Dict[str, Any]) -> str:
        if not score:
            return ""
        overs = float(score.get('o', 0))
        overs_str = str(int(overs)) if overs == int(overs) else str(overs)
        return f"{score.get('r', 0)}/{score.get('w', 0)}[{overs_str}]"

    def _calculate_standings(self, matches: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        records = {
            team: {
                "team_name": team,
                "abbreviation": abbrev,
                "matches": 0,
                "wins": 0,
                "losses": 0,
                "no_result": 0,
                "points": 0,
                "runs_scored": 0,
                "balls_faced": 0,
                "runs_conceded": 0,
                "balls_bowled": 0,
            }
            for team, abbrev in self.config["teams"].items()
        }

        for match in matches:
            teams = [self._canonical(team) for team in match.get("teams", [])]
            if len(teams) != 2:
                continue
            winner = self._canonical(match.get("matchWinner", "") or "")
            if winner.lower() in NO_WINNER_VALUES:
                winner = ""
            if not winner and not match.get("matchEnded"):
                continue

            score_map = self._score_map(match)
            for team in teams:
                if team not in records:
                    records[team] = {
                        "team_name": team,
                        "abbreviation": self._abbr(team),
                        "matches": 0,
                        "wins": 0,
                        "losses": 0,
                        "no_result": 0,
                        "points": 0,
                        "runs_scored": 0,
                        "balls_faced": 0,
                        "runs_conceded": 0,
                        "balls_bowled": 0,
                    }
                opponent = next((t for t in teams if t != team), "")
                if not opponent:
                    continue
                rec = records[team]
                rec["matches"] += 1
                if winner == team:
                    rec["wins"] += 1
                    rec["points"] += 2
                elif winner == opponent:
                    rec["losses"] += 1
                else:
                    rec["no_result"] += 1
                    rec["points"] += 1

                team_score = self._find_score(score_map, team)
                opponent_score = self._find_score(score_map, opponent)
                if team_score and opponent_score:
                    try:
                        rec["runs_scored"] += int(team_score.get("r", 0))
                        rec["balls_faced"] += self._overs_to_balls(team_score.get("o", 0))
                        rec["runs_conceded"] += int(opponent_score.get("r", 0))
                        rec["balls_bowled"] += self._overs_to_balls(opponent_score.get("o", 0))
                    except Exception:
                        pass

        for rec in records.values():
            try:
                nrr = (
                    rec["runs_scored"] / (rec["balls_faced"] / 6)
                    - rec["runs_conceded"] / (rec["balls_bowled"] / 6)
                )
            except ZeroDivisionError:
                nrr = 0.0
            rec["nrr_value"] = nrr
            rec["nrr"] = f"{nrr:.3f}"
            rec["record"] = f"{rec['wins']}-{rec['losses']}-{rec['no_result']}"
        return records

    def _parse_cricapi_match(self, raw: Dict[str, Any], standings: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        home = self._canonical(raw.get("home_team", ""))
        away = self._canonical(raw.get("visitor_team", ""))
        home_record = standings.get(home, {})
        away_record = standings.get(away, {})
        score_map = self._score_map(raw)
        home_score = self._fmt_score(self._find_score(score_map, home))
        away_score = self._fmt_score(self._find_score(score_map, away))
        winner = self._canonical(raw.get("matchWinner", "") or "")
        if winner.lower() in NO_WINNER_VALUES:
            winner = ""

        result = raw.get("status", "") or ""
        if not result and raw.get("matchEnded") and not winner:
            result = "No Result"

        is_final = bool(raw.get("matchEnded")) or "won" in result.lower() or "no result" in result.lower()
        is_in_progress = bool(raw.get("matchStarted")) and not is_final

        away_outcome = ""
        if winner:
            away_outcome = "won" if winner == away else "lost"

        match_date = self._match_date(raw)
        return {
            'league': self.league,
            'game_id': f"{self.league_slug}-{raw.get('id') or raw.get('matchNo', 0)}",
            'game_date': (match_date or datetime.now().date()).strftime('%Y-%m-%d'),
            'game_time': self._match_time(raw),
            'game_type': 'playoffs' if self._is_playoff_match(raw) else 'regular',
            'home_team': home,
            'home_team_abbrev': self._abbr(home),
            'home_wins': home_record.get("wins", 0),
            'home_losses': home_record.get("losses", 0),
            'home_score_total': 0,
            'visitor_team': away,
            'visitor_team_abbrev': self._abbr(away),
            'visitor_wins': away_record.get("wins", 0),
            'visitor_losses': away_record.get("losses", 0),
            'visitor_score_total': 0,
            'game_status': 'final' if is_final else ('in_progress' if is_in_progress else 'scheduled'),
            'current_period': '',
            'time_remaining': '',
            'is_final': is_final,
            'cricket_status': result or 'scheduled',
            'cricket_venue': raw.get('venue', ''),
            'cricket_start_time': self._format_match_times(raw),
            'cricket_home_nr': home_record.get("no_result", 0),
            'cricket_away_nr': away_record.get("no_result", 0),
            'cricket_home_score': home_score,
            'cricket_away_score': away_score,
            'cricket_winner': self._abbr(winner) if winner else '',
            'cricket_result': result,
            'cricket_away_outcome': away_outcome,
        }

    def _is_playoff_match(self, match: Dict[str, Any]) -> bool:
        name = match.get("name", "").lower()
        teams = match.get("teams", [])
        return (
            any(keyword in name for keyword in ("qualifier", "eliminator", "final", "playoff", "semi"))
            or bool(teams and all(t.lower().startswith(("tbc", "tba", "to be")) for t in teams))
        )

    def _parse_schedule_match(self, raw: Dict[str, Any], api_date: str) -> Dict[str, Any]:
        home = raw.get('home', {})
        away = raw.get('away', {})
        if isinstance(home, str):
            home = {'abbrev': home, 'name': home, 'record': ''}
        if isinstance(away, str):
            away = {'abbrev': away, 'name': away, 'record': ''}

        home_record = self._parse_record(home.get('record', ''))
        away_record = self._parse_record(away.get('record', ''))

        status_text = raw.get('status', 'scheduled')
        is_final = 'won' in status_text.lower() or 'lost' in status_text.lower() or 'beat' in status_text.lower() or 'tied' in status_text.lower() or 'no result' in status_text.lower()
        is_in_progress = not is_final and bool(status_text) and status_text.lower() not in ('scheduled', '')

        start_time = raw.get('start_time', {})
        game_time = self._parse_pt_time(start_time.get('pt', ''), api_date)

        return {
            'league': self.league,
            'game_id': f"{self.league_slug}-{raw.get('match_no', 0)}",
            'game_date': api_date or datetime.now().strftime('%Y-%m-%d'),
            'game_time': game_time,
            'game_type': 'regular',
            'home_team': home.get('name', home.get('abbrev', '')),
            'home_team_abbrev': home.get('abbrev', ''),
            'home_wins': home_record[0],
            'home_losses': home_record[1],
            'home_score_total': 0,
            'visitor_team': away.get('name', away.get('abbrev', '')),
            'visitor_team_abbrev': away.get('abbrev', ''),
            'visitor_wins': away_record[0],
            'visitor_losses': away_record[1],
            'visitor_score_total': 0,
            'game_status': 'final' if is_final else ('in_progress' if is_in_progress else 'scheduled'),
            'current_period': '',
            'time_remaining': '',
            'is_final': is_final,
            'cricket_status': status_text,
            'cricket_venue': raw.get('venue', ''),
            'cricket_start_time': start_time,
            'cricket_home_nr': home_record[2],
            'cricket_away_nr': away_record[2],
            'cricket_home_score': '',
            'cricket_away_score': '',
            'cricket_winner': '',
            'cricket_result': status_text if is_final else '',
        }

    def _parse_merged_match(self, score_data: Dict, sched_data: Dict, api_date: str) -> Dict[str, Any]:
        home_abbrev = score_data.get('home', '')
        away_abbrev = score_data.get('away', '')
        result = score_data.get('result', '')
        winner = score_data.get('winner', '')
        home_score_str = score_data.get('home_score', '')
        away_score_str = score_data.get('away_score', '')

        is_final = bool(winner) or 'won' in result.lower() or 'lost' in result.lower() or 'beat' in result.lower() or 'tied' in result.lower() or 'no result' in result.lower()
        is_in_progress = not is_final and bool(result) and result.lower() not in ('scheduled', '')

        # Get records and start time from schedule data
        home_sched = sched_data.get('home', {})
        away_sched = sched_data.get('away', {})
        if isinstance(home_sched, str):
            home_sched = {'abbrev': home_sched, 'record': ''}
        if isinstance(away_sched, str):
            away_sched = {'abbrev': away_sched, 'record': ''}

        home_record = self._parse_record(home_sched.get('record', ''))
        away_record = self._parse_record(away_sched.get('record', ''))

        start_time = sched_data.get('start_time', {})
        game_time = self._parse_pt_time(start_time.get('pt', ''), api_date)

        # Determine visitor outcome
        if winner and winner == away_abbrev:
            away_outcome = 'won'
        elif winner and winner == home_abbrev:
            away_outcome = 'lost'
        elif winner:
            away_outcome = 'lost'
        else:
            away_outcome = ''

        return {
            'league': self.league,
            'game_id': f"{self.league_slug}-{score_data.get('match_no', 0)}",
            'game_date': api_date or datetime.now().strftime('%Y-%m-%d'),
            'game_time': game_time,
            'game_type': 'regular',
            'home_team': home_sched.get('name', home_abbrev),
            'home_team_abbrev': home_abbrev,
            'home_wins': home_record[0],
            'home_losses': home_record[1],
            'home_score_total': 0,
            'visitor_team': away_sched.get('name', away_abbrev),
            'visitor_team_abbrev': away_abbrev,
            'visitor_wins': away_record[0],
            'visitor_losses': away_record[1],
            'visitor_score_total': 0,
            'game_status': 'final' if is_final else ('in_progress' if is_in_progress else 'scheduled'),
            'current_period': '',
            'time_remaining': '',
            'is_final': is_final,
            'cricket_status': result or sched_data.get('status', 'scheduled'),
            'cricket_venue': score_data.get('venue', sched_data.get('venue', '')),
            'cricket_start_time': start_time,
            'cricket_home_nr': home_record[2],
            'cricket_away_nr': away_record[2],
            'cricket_home_score': home_score_str,
            'cricket_away_score': away_score_str,
            'cricket_winner': winner,
            'cricket_result': result,
            'cricket_away_outcome': away_outcome,
        }

    def _parse_pt_time(self, pt_str: str, date_str: str) -> Optional[datetime]:
        """Parse '7:00AM PDT' into a timezone-aware datetime."""
        if not pt_str or not date_str:
            return None
        try:
            clean = pt_str.strip()
            # Remove timezone suffix (PDT, PST, etc.)
            clean = re.sub(r'\s*(PDT|PST|PT)\s*$', '', clean, flags=re.IGNORECASE).strip()
            dt = datetime.strptime(f"{date_str} {clean}", '%Y-%m-%d %I:%M%p')
            pacific = pytz.timezone('US/Pacific')
            return pacific.localize(dt)
        except Exception as e:
            logger.debug(f"Could not parse PT time '{pt_str}' with date '{date_str}': {e}")
            return None

    def get_season_info(self, year: int = None) -> Optional[Dict[str, Any]]:
        try:
            url = f"{CRICKETPUFF_BASE}/season-info/{self.league_slug}"
            response = requests.get(url, timeout=self.api_timeout)
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            logger.error(f"Error fetching {self.league} season info: {e}")
            return None

    def _parse_record(self, record_str: str):
        """Parse W-L-NR record string. Returns (wins, losses, no_result)."""
        if not record_str:
            return (0, 0, 0)
        parts = record_str.split('-')
        try:
            w = int(parts[0]) if len(parts) > 0 else 0
            l = int(parts[1]) if len(parts) > 1 else 0
            nr = int(parts[2]) if len(parts) > 2 else 0
            return (w, l, nr)
        except (ValueError, IndexError):
            return (0, 0, 0)
