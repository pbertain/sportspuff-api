"""
Cricket live-match enrichment via CricAPI.

When TheSportsDB-sourced cricket games (IPL/MLC) are in progress, augment
them with CricAPI's per-inning detail — overs, wickets, formatted scores
like "161/3 (19.5 ov)", venue, ball-by-ball status — that TheSportsDB
doesn't expose.

Quota math:
- Only fires for games whose game_status == 'in_progress'
- Reuses CricAPI cache + budget gates from collectors/cricket.py
- 1-2 concurrent IPL/MLC matches × 1-min poll cadence × ~4h match window
  = ~240 hits per match. Per league per day during a match-day: well
  under the 2000/day shared cap.

Disabled when settings.cricket_live_enrichment != 'cricapi' or when
CRICAPI_KEY isn't configured.

Match-up between TheSportsDB and CricAPI is via canonical team names
(LEAGUE_CONFIGS aliases collapse "Royal Challengers Bangalore" and
"Royal Challengers Bengaluru" to one key).
"""

from __future__ import annotations

import logging
from datetime import date as _date, datetime, timezone
from typing import Any, Dict, List, Optional

from ..config import settings

logger = logging.getLogger(__name__)


def _is_enabled() -> bool:
    return (
        (settings.cricket_live_enrichment or "").lower() == "cricapi"
        and bool((settings.cricapi_key or "").strip())
    )


def _has_live_games(games: List[Dict[str, Any]]) -> bool:
    return any(g.get("game_status") == "in_progress" for g in games)


def enrich_with_cricapi_live(
    league_code: str,
    games: List[Dict[str, Any]],
    target_date: Optional[_date] = None,
) -> List[Dict[str, Any]]:
    """Mutate games[] in place: for any game with status 'in_progress', look
    up the corresponding CricAPI match and overlay rich cricket detail
    (per-inning scores, overs, wickets, formatted score string) onto our
    cricket_* fields. Returns the same list (for chaining).

    No-op (returns unchanged) when:
      - cricket_live_enrichment != 'cricapi'
      - CRICAPI_KEY is empty
      - games[] has no in-progress matches
      - CricAPI is unreachable / over budget
    """
    if not games:
        return games
    if not _is_enabled():
        return games
    if not _has_live_games(games):
        return games

    league_code = league_code.upper()
    if league_code not in ("IPL", "MLC"):
        return games

    # Build a CricAPI-backed CricketCollector just for its matching/parsing
    # helpers. Heavy lifting (series lookup + match_info caching + budget
    # gates) is reused as-is.
    try:
        from ..collectors.cricket import CricketCollector, CricAPIBudgetExceeded
        legacy = CricketCollector(league_code)
    except Exception as e:
        logger.debug("cricket enrich: cannot construct legacy collector: %s", e)
        return games

    # Pull the CricAPI-side enriched match list for the target date. This
    # internally finds the IPL/MLC series, fetches series_info, and force-
    # refreshes live/recently-ended matches. Cached aggressively.
    try:
        cricapi_matches = legacy._get_cricapi_matches(target_date)
    except CricAPIBudgetExceeded as e:
        logger.warning("cricket enrich: cricapi budget exceeded: %s", e)
        return games
    except Exception as e:
        logger.warning("cricket enrich: cricapi fetch failed: %s", e)
        return games

    if not cricapi_matches:
        return games

    # Index CricAPI matches by canonical (home, away) and reverse so we
    # can match regardless of which side the two sources call "home".
    index: Dict[tuple, Dict[str, Any]] = {}
    for m in cricapi_matches:
        teams = m.get("teams") or []
        if len(teams) < 2:
            continue
        a, b = legacy._canonical(teams[0]), legacy._canonical(teams[1])
        index[(a, b)] = m
        index[(b, a)] = m

    enriched_count = 0
    for g in games:
        if g.get("game_status") != "in_progress":
            continue
        h = legacy._canonical(g.get("home_team") or "")
        a = legacy._canonical(g.get("visitor_team") or "")
        if not h or not a:
            continue
        cricapi_m = index.get((h, a))
        if not cricapi_m:
            continue

        score_map = legacy._score_map(cricapi_m)
        h_score = legacy._find_score(score_map, h)
        a_score = legacy._find_score(score_map, a)

        if h_score:
            g["cricket_home_score"] = legacy._fmt_score(h_score)
        if a_score:
            g["cricket_away_score"] = legacy._fmt_score(a_score)

        # CricAPI's status field is the rich live-status text
        # (e.g. "RCB need 12 runs in 18 balls").
        live_status = cricapi_m.get("status")
        if live_status:
            g["cricket_status"] = live_status

        venue = cricapi_m.get("venue")
        if venue:
            g["cricket_venue"] = venue

        enriched_count += 1

    if enriched_count:
        logger.info("cricket enrich: augmented %d %s game(s) with CricAPI",
                    enriched_count, league_code)

    return games
