"""
File-backed cycling collector.

Reads simple CSV inputs from a configured directory and exposes the same
shape as the other cycling collectors. Intended as a human-editable source
of truth for Tour de France stage results, GC, team classification, and
jersey standings.
"""

from __future__ import annotations

import csv
import os
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pytz


class CyclingFileCollector:
    SPORTSPUFF_CODE = "CYCLING"

    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self.timezone = pytz.timezone("US/Pacific")

    def set_timezone(self, timezone) -> None:
        if timezone is not None:
            self.timezone = timezone

    def _path(self, name: str) -> str:
        return os.path.join(self.data_dir, name)

    def _read_csv(self, name: str) -> List[Dict[str, str]]:
        path = self._path(name)
        if not os.path.exists(path):
            return []
        with open(path, newline="", encoding="utf-8") as f:
            return [dict(row) for row in csv.DictReader(f)]

    @staticmethod
    def _int(value: Any, default: int = 0) -> int:
        try:
            return int(float(value))
        except Exception:
            return default

    @staticmethod
    def _clean(value: Any) -> str:
        return "" if value is None else str(value).strip()

    def _stage_rows(self) -> List[Dict[str, Any]]:
        rows = []
        for raw in self._read_csv("cycling_stages.csv"):
            race = self._clean(raw.get("race"))
            date_s = self._clean(raw.get("date"))
            if not race or not date_s:
                continue
            stage_number = self._int(raw.get("stage_number"), 0)
            stage_name = self._clean(raw.get("stage_name")) or (f"Stage {stage_number}" if stage_number else "Race Day")
            status = self._clean(raw.get("status")) or "scheduled"
            home_team = self._clean(raw.get("home_team")) or race
            visitor_team = self._clean(raw.get("visitor_team")) or stage_name
            rows.append({
                "league": "CYCLING",
                "game_id": self._clean(raw.get("game_id")) or f"{race}-{stage_number or date_s}",
                "game_date": date_s,
                "game_time": None,
                "game_type": self._clean(raw.get("game_type")) or ("stage" if stage_number else "one_day"),
                "home_team": home_team,
                "home_team_abbrev": self._clean(raw.get("home_team_abbrev")),
                "home_team_id": self._clean(raw.get("home_team_id")),
                "home_wins": 0,
                "home_losses": 0,
                "home_score_total": 0,
                "visitor_team": visitor_team,
                "visitor_team_abbrev": self._clean(raw.get("visitor_team_abbrev")),
                "visitor_team_id": self._clean(raw.get("visitor_team_id")),
                "visitor_wins": 0,
                "visitor_losses": 0,
                "visitor_score_total": 0,
                "game_status": status,
                "current_period": "",
                "time_remaining": "",
                "is_final": status == "final",
                "is_overtime": False,
                "home_period_scores": {},
                "visitor_period_scores": {},
                "venue": self._clean(raw.get("venue")),
                "home_team_badge": self._clean(raw.get("home_team_badge")),
                "visitor_team_badge": self._clean(raw.get("visitor_team_badge")),
                "league_badge": self._clean(raw.get("league_badge")),
                "cycling_race": race,
                "cycling_stage_label": stage_name if stage_name != race else "",
                "cycling_stage_number": stage_number or None,
                "cycling_event_label": self._clean(raw.get("cycling_event_label")) or f"{race} {stage_name}".strip(),
                "cycling_country": self._clean(raw.get("cycling_country")),
                "cycling_video": self._clean(raw.get("cycling_video")),
                "cycling_distance_km": self._clean(raw.get("distance_km")),
                "cycling_winner": self._clean(raw.get("winner")),
            })
        return rows

    def get_live_scores(self, target_date):
        return self._stage_rows()

    def get_schedule(self, target_date):
        return self._stage_rows()

    def get_standings(self) -> List[Dict[str, Any]]:
        gc_rows = self._read_csv("cycling_gc.csv")
        teams = []
        for raw in gc_rows:
            team = {
                "rank": self._int(raw.get("rank"), 0) or None,
                "team_name": self._clean(raw.get("team")),
                "abbreviation": self._clean(raw.get("team_abbrev")) or self._clean(raw.get("team")),
                "record": self._clean(raw.get("time_back")),
                "wins": 0,
                "losses": 0,
                "win_pct": None,
                "games_back": self._clean(raw.get("time_back")),
                "streak": None,
                "points": self._int(raw.get("points"), 0),
                "matches": None,
            }
            if team["team_name"]:
                teams.append(team)
        return teams

    def get_team_classification(self) -> List[Dict[str, Any]]:
        rows = []
        for raw in self._read_csv("cycling_team_classification.csv"):
            team = self._clean(raw.get("team"))
            if not team:
                continue
            rows.append({
                "rank": self._int(raw.get("rank"), 0) or None,
                "team_name": team,
                "abbreviation": self._clean(raw.get("team_abbrev")) or team[:4].upper(),
                "record": self._clean(raw.get("time_back")),
                "wins": 0,
                "losses": 0,
                "win_pct": None,
                "games_back": self._clean(raw.get("time_back")),
                "streak": None,
                "points": 0,
            })
        return rows

    def get_jersey_standings(self) -> Dict[str, List[Dict[str, Any]]]:
        jerseys = defaultdict(list)
        for raw in self._read_csv("cycling_jerseys.csv"):
            classification = self._clean(raw.get("classification")).lower()
            if not classification:
                continue
            jerseys[classification].append({
                "rank": self._int(raw.get("rank"), 0) or None,
                "rider": self._clean(raw.get("rider")),
                "team": self._clean(raw.get("team")),
            })
        return dict(jerseys)

    def get_season_info(self):
        stages = self._stage_rows()
        if not stages:
            return None
        dates = [r["game_date"] for r in stages if r.get("game_date")]
        if not dates:
            return None
        season_types = []
        by_race: Dict[str, Dict[str, str]] = {}
        for row in stages:
            race = row.get("cycling_race") or "Cycling"
            slot = by_race.setdefault(race, {"start": row["game_date"], "end": row["game_date"]})
            slot["start"] = min(slot["start"], row["game_date"])
            slot["end"] = max(slot["end"], row["game_date"])
        for race, slot in sorted(by_race.items(), key=lambda kv: kv[1]["start"]):
            season_types.append({"name": race, "start_date": slot["start"], "end_date": slot["end"]})
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        current = "Off Season"
        for race, slot in by_race.items():
            if slot["start"] <= today <= slot["end"]:
                current = race
                break
        return {
            "year": int(today[:4]),
            "current_phase": current,
            "season_types": season_types,
        }
