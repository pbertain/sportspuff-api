"""
Live-score enrichment for ATP/WTA tennis via ESPN's tennis scoreboard.

TheSportsDB exposes the fixture (player1 vs player2, date, status) but not
set scores. ESPN does. We hit ESPN's tennis scoreboard once per (sport,
date), build a per-match list keyed off the two athlete display names, and
attach set scores + a summary string to matches whose surnames suffix-match.

Match key: ESPN's `displayName` is the player's full name ("Gauthier Onclin").
Our `home_team` / `visitor_team` carry the parsed surname or multi-word
surname ("Onclin", "Davidovich Fokina"). We accept a match when our value
is a word-boundary suffix of ESPN's full name.

Doubles are skipped — our event shape only carries one player per side.
"""

from __future__ import annotations

import logging
import re
import time
from datetime import date as _date, datetime, timezone
from threading import Lock
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)


_ESPN_SUBPATH: Dict[str, str] = {
    "atp": "tennis/atp",
    "wta": "tennis/wta",
}

_ESPN_GROUP_SLUGS: Dict[str, str] = {
    "atp": "mens-singles",
    "wta": "womens-singles",
}


# ESPN puts seed in parens before the player's full name, e.g.
#   "(7) Alejandro Davidovich Fokina (ESP) bt Mattia Bellucci (ITA) 6-1 6-3"
# Captures (seed, full_name).
_SEED_PATTERN = re.compile(r"\((\d+)\)\s+([A-Z][^()]*?)\s+\([A-Z]{2,3}\)")


_cache: Dict[str, Dict[str, Any]] = {}
_cache_lock = Lock()
_CACHE_TTL_SECONDS = 30


def _surname_suffix_match(ours: str, espn_full: str) -> bool:
    """True if `ours` is a word-boundary suffix of `espn_full` (case-insensitive)."""
    if not ours or not espn_full:
        return False
    s = espn_full.strip().lower()
    n = ours.strip().lower()
    return s == n or s.endswith(" " + n)


def _competition_to_match(comp: Dict[str, Any], tournament: str) -> Optional[Dict[str, Any]]:
    competitors = comp.get("competitors") or []
    if len(competitors) != 2:
        return None

    status = comp.get("status") or {}
    status_type = status.get("type") or {}
    is_final = bool(status_type.get("completed"))
    state = status_type.get("state") or ""  # 'pre' / 'in' / 'post'

    sides = []
    for c in competitors:
        ath = c.get("athlete") or {}
        ls_raw = c.get("linescores") or []
        linescores: List[int] = []
        for ls in ls_raw:
            try:
                linescores.append(int(ls.get("value") or 0))
            except (TypeError, ValueError):
                linescores.append(0)
        sets_won = sum(1 for ls in ls_raw if ls.get("winner"))
        sides.append({
            "name": ath.get("displayName") or "",
            "linescores": linescores,
            "sets_won": sets_won,
            "winner": bool(c.get("winner")),
        })

    n_sets = max(len(sides[0]["linescores"]), len(sides[1]["linescores"]))
    set_scores: List[Dict[str, int]] = []
    for i in range(n_sets):
        s1 = sides[0]["linescores"][i] if i < len(sides[0]["linescores"]) else 0
        s2 = sides[1]["linescores"][i] if i < len(sides[1]["linescores"]) else 0
        set_scores.append({"set": i + 1, "side1": s1, "side2": s2})

    notes = comp.get("notes") or []
    summary = ""
    if notes and isinstance(notes, list) and isinstance(notes[0], dict):
        summary = (notes[0].get("text") or "").strip()

    venue = comp.get("venue") or {}
    venue_name = venue.get("fullName") or ""
    court_name = venue.get("court") or ""

    # Parse seeds out of the summary string. ESPN tags the seeded player
    # with "(N)" before their full name. Map per full-name (lowercased).
    seeds: Dict[str, int] = {}
    if summary:
        for m in _SEED_PATTERN.finditer(summary):
            try:
                seeds[m.group(2).strip().lower()] = int(m.group(1))
            except (ValueError, IndexError):
                continue

    return {
        "competition_date": (comp.get("date") or "")[:10],
        "competition_time": comp.get("date") or "",
        "tournament": tournament,
        "side1_name": sides[0]["name"],
        "side2_name": sides[1]["name"],
        "side1_sets_won": sides[0]["sets_won"],
        "side2_sets_won": sides[1]["sets_won"],
        "side1_winner": sides[0]["winner"],
        "side2_winner": sides[1]["winner"],
        "side1_seed": seeds.get(sides[0]["name"].strip().lower()),
        "side2_seed": seeds.get(sides[1]["name"].strip().lower()),
        "set_scores": set_scores,
        "summary": summary,
        "is_final": is_final,
        "state": state,
        "venue_name": venue_name,
        "court_name": court_name,
    }


def _fetch_matches(sport: str, target_date: _date) -> Optional[List[Dict[str, Any]]]:
    subpath = _ESPN_SUBPATH.get(sport.lower())
    if not subpath:
        return None
    group_slug = _ESPN_GROUP_SLUGS.get(sport.lower())

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
            url, timeout=8,
            headers={"User-Agent": "sportspuff-api/1.0"},
        )
        if resp.status_code != 200:
            logger.debug("ESPN tennis enrich %s: HTTP %s", cache_key, resp.status_code)
            return None
        data = resp.json()
    except Exception as e:
        logger.debug("ESPN tennis enrich %s failed: %s", cache_key, e)
        return None

    matches: List[Dict[str, Any]] = []
    target_iso = target_date.isoformat()
    for ev in data.get("events") or []:
        tournament = ev.get("name") or ""
        for grp in ev.get("groupings") or []:
            slug = ((grp.get("grouping") or {}).get("slug") or "").lower()
            # Skip anything outside the tour-specific singles bracket.
            if group_slug and slug != group_slug:
                continue
            for comp in grp.get("competitions") or []:
                m = _competition_to_match(comp, tournament)
                if m and m["competition_date"] == target_iso:
                    matches.append(m)

    with _cache_lock:
        _cache[cache_key] = {"data": matches, "ts": now_ts}
    return matches


def _parse_iso_datetime(value: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def build_schedule_games(sport: str, target_date: _date) -> List[Dict[str, Any]]:
    """Build fallback ATP/WTA schedule rows from ESPN when TheSportsDB is empty."""
    matches = _fetch_matches(sport, target_date)
    if not matches:
        return []

    games: List[Dict[str, Any]] = []
    for idx, m in enumerate(matches, start=1):
        home_name = (m.get("side2_name") or "").strip()
        visitor_name = (m.get("side1_name") or "").strip()
        if not home_name or not visitor_name:
            continue

        dt = _parse_iso_datetime(m.get("competition_time") or "")
        game_date = m.get("competition_date") or target_date.isoformat()
        state = m.get("state") or ""
        game_status = "scheduled"
        if state == "in":
            game_status = "in_progress"
        elif m.get("is_final"):
            game_status = "final"

        game_id = f"{sport.lower()}-espn-{game_date}-{idx}"
        games.append({
            "league": sport.upper(),
            "game_id": game_id,
            "game_date": game_date,
            "game_time": dt,
            "game_type": "match",
            "home_team": home_name,
            "home_team_abbrev": "",
            "home_team_id": "",
            "home_wins": 0,
            "home_losses": 0,
            "home_score_total": 0,
            "visitor_team": visitor_name,
            "visitor_team_abbrev": "",
            "visitor_team_id": "",
            "visitor_wins": 0,
            "visitor_losses": 0,
            "visitor_score_total": 0,
            "game_status": game_status,
            "current_period": "",
            "time_remaining": "",
            "is_final": bool(m.get("is_final")),
            "is_overtime": False,
            "home_period_scores": {},
            "visitor_period_scores": {},
            "venue": "",
            "tennis_tournament": m.get("tournament") or "",
            "tennis_match_label": f"{m.get('tournament') or ''} {home_name} vs {visitor_name}".strip(),
            "tennis_round": "",
            "tennis_country": "",
            "tennis_video": "",
            "league_badge": "",
            "home_full_name": home_name,
            "visitor_full_name": visitor_name,
            "home_seed": m.get("side2_seed"),
            "visitor_seed": m.get("side1_seed"),
            "tennis_set_scores": [
                {"set": s["set"], "home": s["side2"], "visitor": s["side1"]}
                for s in (m.get("set_scores") or [])
            ],
            "home_sets_won": m.get("side2_sets_won"),
            "visitor_sets_won": m.get("side1_sets_won"),
            "tennis_summary": m.get("summary"),
            "tennis_winner": "home" if m.get("side2_winner") else ("visitor" if m.get("side1_winner") else None),
            "venue_name": m.get("venue_name") or "",
            "court_name": m.get("court_name") or "",
        })
    return games


def _find_match(matches: List[Dict[str, Any]], home: str, visitor: str) -> Optional[Dict[str, Any]]:
    """Find an ESPN match whose two players line up with our home/visitor
    surnames. Returns the match dict with `_swap` indicating whether ESPN's
    side1 is our visitor (rather than our home)."""
    for m in matches:
        n1, n2 = m["side1_name"], m["side2_name"]
        if _surname_suffix_match(home, n1) and _surname_suffix_match(visitor, n2):
            return {**m, "_swap": False}
        if _surname_suffix_match(home, n2) and _surname_suffix_match(visitor, n1):
            return {**m, "_swap": True}
    return None


def enrich_games(
    sport: str,
    target_date: _date,
    games: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Attach set scores, set counts, and a result summary to ATP/WTA games.

    Mutates each dict in place AND returns the list (for chaining). Games
    whose players don't line up with any ESPN entry are unchanged.
    """
    if sport.lower() not in _ESPN_SUBPATH or not games:
        return games
    matches = _fetch_matches(sport, target_date)
    if not matches:
        return games

    for g in games:
        if not isinstance(g, dict):
            continue
        home = (g.get("home_team") or "").strip()
        visitor = (g.get("visitor_team") or "").strip()
        if not home or not visitor:
            continue
        m = _find_match(matches, home, visitor)
        if not m:
            continue

        swap = m["_swap"]
        per_set: List[Dict[str, int]] = []
        for s in m["set_scores"]:
            home_v = s["side2"] if swap else s["side1"]
            visitor_v = s["side1"] if swap else s["side2"]
            per_set.append({"set": s["set"], "home": home_v, "visitor": visitor_v})
        g["tennis_set_scores"] = per_set
        g["home_sets_won"] = m["side2_sets_won"] if swap else m["side1_sets_won"]
        g["visitor_sets_won"] = m["side1_sets_won"] if swap else m["side2_sets_won"]
        g["tennis_summary"] = m["summary"]
        home_won = m["side2_winner"] if swap else m["side1_winner"]
        visitor_won = m["side1_winner"] if swap else m["side2_winner"]
        g["tennis_winner"] = "home" if home_won else ("visitor" if visitor_won else None)

        # Full names (ESPN's displayName) — preserve so v6 can render
        # "Alejandro Davidovich Fokina" alongside the surname.
        g["home_full_name"] = m["side2_name"] if swap else m["side1_name"]
        g["visitor_full_name"] = m["side1_name"] if swap else m["side2_name"]
        g["home_seed"] = m["side2_seed"] if swap else m["side1_seed"]
        g["visitor_seed"] = m["side1_seed"] if swap else m["side2_seed"]
        if m.get("venue_name"):
            g["venue_name"] = m["venue_name"]
        if m.get("court_name"):
            g["court_name"] = m["court_name"]

        # ESPN is more authoritative on status — if it says final, trust it.
        if m["is_final"]:
            g["is_final"] = True
            g["game_status"] = "final"
        elif m["state"] == "in":
            g["game_status"] = "in_progress"

    return games
