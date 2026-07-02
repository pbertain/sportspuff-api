"""
Cycling collector backed by TheSportsDB UCI World Tour (id=4465).

Covers all UCI World Tour stage races and one-day classics under a single
league: Tour de France, Giro d'Italia, Vuelta a España, Paris-Nice,
Critérium du Dauphiné, Tour Down Under, etc. Each event is one stage of
one race (or a single-day classic).

TheSportsDB cycling shape:
- strEvent like "Tour de France Stage 1" or "Cadel Evans Great Ocean Road Race"
- No intHomeScore / intAwayScore (cycling isn't team-vs-team)
- No rider/winner data (TheSportsDB only has calendar/schedule)
- strVenue is sparse; strCity often null. Race route detail isn't here.

We surface:
- The race name (e.g. "Tour de France")
- The stage label ("Stage 1", "Prologue", or "" for one-day classics)
- The stage number when parseable
- Date, time, country, video link if available

Standings don't apply (general-classification standings are per-race and
not in TheSportsDB).
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pytz

from .thesportsdb import TheSportsDBCollector

logger = logging.getLogger(__name__)


# Match trailing "Stage N", "Prologue", "ITT" (individual time trial),
# "TTT" (team time trial). Anything else is a one-day classic (the whole
# event name is the race; no stage label).
_STAGE_TAIL = re.compile(
    r"\s+(Stage\s+\d+|Prologue|ITT|TTT)\s*$",
    re.IGNORECASE,
)


def parse_cycling_strevent(strevent: str) -> Dict[str, str]:
    """{'race': 'Tour de France', 'stage_label': 'Stage 1', 'stage_number': 1}.

    For one-day classics, stage_label is '' and stage_number is None.
    """
    if not strevent:
        return {"race": "", "stage_label": "", "stage_number": None, "raw": ""}
    m = _STAGE_TAIL.search(strevent)
    if not m:
        return {"race": strevent.strip(), "stage_label": "", "stage_number": None, "raw": strevent}
    label = m.group(1).strip()
    race = strevent[: m.start()].strip()
    stage_number = None
    sm = re.match(r"Stage\s+(\d+)", label, re.IGNORECASE)
    if sm:
        try:
            stage_number = int(sm.group(1))
        except ValueError:
            stage_number = None
    return {"race": race, "stage_label": label, "stage_number": stage_number, "raw": strevent}


class CyclingTheSportsDBCollector(TheSportsDBCollector):
    LEAGUE_ID = 4465
    SPORTSPUFF_CODE = "CYCLING"

    def __init__(self):
        super().__init__("CYCLING")
        self.timezone = pytz.timezone("US/Pacific")

    def current_season(self) -> str:
        return str(datetime.now(timezone.utc).year)

    def _parse_event(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            strevent = raw.get("strEvent") or ""
            parsed = parse_cycling_strevent(strevent)
            if not parsed["race"]:
                return None

            dt = self._parse_event_datetime(raw)
            game_date = self._local_date(raw) or (dt.date() if dt else datetime.now().date())
            status = self._normalize_status(raw)
            is_final = status == "final"

            # game_type categorizes one-day classics vs stages so the frontend
            # can render differently if it wants.
            if parsed["stage_label"]:
                game_type = "stage"
            else:
                game_type = "one_day"

            return {
                "league": "CYCLING",
                "game_id": str(raw.get("idEvent") or ""),
                "game_date": game_date.strftime("%Y-%m-%d"),
                "game_time": dt,
                "game_type": game_type,
                # Cycling doesn't fit the team-vs-team model. Surface the race
                # name as home_team and the stage label as visitor_team so
                # routes that pass these through to v6 still show meaningful
                # text without v6 needing the cycling-specific fields below.
                "home_team": parsed["race"],
                "home_team_abbrev": "",
                "home_team_id": "",
                "home_wins": 0,
                "home_losses": 0,
                "home_score_total": 0,
                "visitor_team": parsed["stage_label"] or "Race Day",
                "visitor_team_abbrev": "",
                "visitor_team_id": "",
                "visitor_wins": 0,
                "visitor_losses": 0,
                "visitor_score_total": 0,
                "game_status": status,
                "current_period": "",
                "time_remaining": "",
                "is_final": is_final,
                "is_overtime": False,
                "home_period_scores": {},
                "visitor_period_scores": {},
                "venue": raw.get("strVenue") or "",
                # Cycling-specific
                "cycling_race": parsed["race"],
                "cycling_stage_label": parsed["stage_label"],
                "cycling_stage_number": parsed["stage_number"],
                "cycling_event_label": parsed["raw"],
                "cycling_country": raw.get("strCountry") or "",
                "cycling_video": raw.get("strVideo") or "",
                "cycling_winner": None,
                "cycling_rank": None,
                "league_badge": raw.get("strLeagueBadge") or "",
            }
        except Exception as e:
            logger.error("Cycling parse error: %s", e)
            return None

    def get_standings(self) -> List[Dict[str, Any]]:
        return []

    def get_season_info(self, year: int = None) -> Optional[Dict[str, Any]]:
        """Surface the calendar of UCI World Tour races for the year, plus
        a `current_phase` set to the active race name (or 'Off Season')."""
        season = str(year) if year else self.current_season()
        try:
            events = self._season_events(season)
        except Exception:
            return None
        if not events:
            return None

        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        by_race: Dict[str, Dict[str, Any]] = {}
        for raw in events:
            parsed = parse_cycling_strevent(raw.get("strEvent") or "")
            r = parsed["race"]
            d = (raw.get("dateEvent") or "")[:10]
            if not r or not d:
                continue
            slot = by_race.setdefault(r, {"start": d, "end": d, "n": 0})
            slot["n"] += 1
            if d < slot["start"]:
                slot["start"] = d
            if d > slot["end"]:
                slot["end"] = d

        starts = [slot["start"] for slot in by_race.values() if slot.get("start")]
        ends = [slot["end"] for slot in by_race.values() if slot.get("end")]
        current = "Off Season"
        for race, slot in by_race.items():
            if slot["start"] <= today <= slot["end"]:
                current = race
                break
        if current == "Off Season" and starts and today < min(starts):
            current = "Upcoming"
        elif current == "Off Season" and ends and today > max(ends):
            current = "Off Season"

        ordered = sorted(by_race.items(), key=lambda kv: kv[1]["start"])
        season_types = [
            {"name": race, "start_date": s["start"], "end_date": s["end"]}
            for race, s in ordered
        ]

        return {
            "year": int(season) if season.isdigit() else season,
            "current_phase": current,
            "season_types": season_types,
        }
