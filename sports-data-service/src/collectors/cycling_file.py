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
from datetime import date as date_cls, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytz


class CyclingFileCollector:
    SPORTSPUFF_CODE = "CYCLING"

    def __init__(self, data_dir: str):
        self.data_dir = data_dir or str(Path(__file__).resolve().parents[2] / "templates")
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
        # UTF-8-SIG strips a BOM from the first header row if the file was
        # edited in spreadsheet software.
        with open(path, newline="", encoding="utf-8-sig") as f:
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

    @staticmethod
    def _safe_int(value: Any, default: Optional[int] = None) -> Optional[int]:
        try:
            if value in (None, ""):
                return default
            return int(float(value))
        except Exception:
            return default

    @staticmethod
    def _parse_date(value: Any) -> Optional[date_cls]:
        text = "" if value is None else str(value).strip()
        if not text:
            return None
        for fmt in ("%Y-%m-%d", "%Y%m%d", "%m/%d/%Y", "%m/%d/%y", "%m-%d-%Y", "%m-%d-%y"):
            try:
                return datetime.strptime(text, fmt).date()
            except ValueError:
                continue
        return None

    def _date_text(self, value: Any) -> str:
        parsed = self._parse_date(value)
        if parsed:
            return parsed.isoformat()
        text = self._clean(value)
        return text

    @staticmethod
    def _rest_day_label(stage_number: str, stage_name: str) -> str:
        if stage_name:
            return stage_name
        if stage_number:
            return f"Rest {stage_number}"
        return "Rest Day"

    def _stage_rows(self, target_date: Optional[date_cls] = None) -> List[Dict[str, Any]]:
        rows = []
        for raw in self._read_csv("cycling_stages.csv"):
            race = self._clean(raw.get("race"))
            stage_date = self._parse_date(raw.get("date"))
            if not race or not stage_date:
                continue
            if target_date and stage_date != target_date:
                continue

            stage_number_raw = self._clean(raw.get("stage_number"))
            stage_number = self._int(stage_number_raw, 0)
            stage_name = self._clean(raw.get("stage_name"))
            race_type = self._clean(raw.get("race_type"))
            if race_type.lower() == "rest day" or stage_number_raw.upper().startswith("R"):
                stage_name = self._rest_day_label(stage_number_raw.lstrip("Rr"), stage_name)
                game_type = "rest_day"
            else:
                stage_name = stage_name or (f"Stage {stage_number}" if stage_number else "Race Day")
                game_type = "stage" if stage_number or stage_name.lower().startswith("stage") else "one_day"
            status = self._clean(raw.get("status")) or "scheduled"
            home_team = self._clean(raw.get("home_team")) or race
            visitor_team = self._clean(raw.get("visitor_team")) or stage_name
            rows.append({
                "league": "CYCLING",
                "game_id": self._clean(raw.get("game_id")) or f"{race}-{stage_number_raw or stage_date.isoformat()}",
                "game_date": stage_date.isoformat(),
                "game_time": None,
                "game_type": self._clean(raw.get("game_type")) or game_type,
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
                "cycling_url": self._clean(raw.get("cycling_url")),
                "cycling_url_label": self._clean(raw.get("cycling_url_label")) or (stage_name or race or "Details"),
                "cycling_video": self._clean(raw.get("cycling_video")),
                "cycling_distance_km": self._clean(raw.get("distance_km")),
                "cycling_winner": self._clean(raw.get("winner")),
                "cycling_rank": self._safe_int(raw.get("rank")),
                "race_type": race_type,
                "start_city": self._clean(raw.get("start_city")),
                "finish_city": self._clean(raw.get("finish_city")),
            })
        return rows

    def get_live_scores(self, target_date):
        return self._stage_rows(target_date)

    def get_schedule(self, target_date):
        return self._stage_rows(target_date)

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
        teams.sort(key=lambda rec: (rec["rank"] or 999999, rec["team_name"]))
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
        starts = [slot["start"] for slot in by_race.values() if slot.get("start")]
        ends = [slot["end"] for slot in by_race.values() if slot.get("end")]
        if current == "Off Season" and starts and today < min(starts):
            current = "Upcoming"
        elif current == "Off Season" and ends and today > max(ends):
            current = "Off Season"
        return {
            "year": int(today[:4]),
            "current_phase": current,
            "season_types": season_types,
        }


class CyclingDecoratedCollector:
    """Overlay file-backed cycling rows onto a base collector.

    The base collector stays authoritative for upstream data; the file
    collector is used to fill in missing stage metadata, URLs, rest days,
    and local race notes.
    """

    SPORTSPUFF_CODE = "CYCLING"

    def __init__(self, base_collector, overlay_collector: CyclingFileCollector):
        self.base_collector = base_collector
        self.overlay_collector = overlay_collector

    def set_timezone(self, timezone) -> None:
        if self.base_collector and hasattr(self.base_collector, "set_timezone"):
            self.base_collector.set_timezone(timezone)
        if self.overlay_collector and hasattr(self.overlay_collector, "set_timezone"):
            self.overlay_collector.set_timezone(timezone)

    @staticmethod
    def _row_key(row: Dict[str, Any]) -> tuple:
        return (
            str(row.get("game_date") or ""),
            str(row.get("cycling_race") or row.get("home_team") or "").strip().lower(),
            str(row.get("cycling_stage_label") or row.get("visitor_team") or "").strip().lower(),
            str(row.get("cycling_event_label") or "").strip().lower(),
        )

    @staticmethod
    def _merge_rows(base_rows: List[Dict[str, Any]], overlay_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        overlay_map: Dict[tuple, List[Dict[str, Any]]] = defaultdict(list)
        for row in overlay_rows:
            overlay_map[CyclingDecoratedCollector._row_key(row)].append(row)

        merged: List[Dict[str, Any]] = []
        for row in base_rows:
            key = CyclingDecoratedCollector._row_key(row)
            if overlay_map.get(key):
                overlay = overlay_map[key].pop(0)
                combined = dict(row)
                for k, v in overlay.items():
                    if v not in (None, "", []):
                        combined[k] = v
                merged.append(combined)
            else:
                merged.append(row)

        for rows in overlay_map.values():
            for row in rows:
                merged.append(row)

        merged.sort(key=lambda r: (r.get("game_date") or "", r.get("cycling_stage_number") or 999999, r.get("cycling_stage_label") or "", r.get("cycling_event_label") or ""))
        return merged

    def get_live_scores(self, target_date):
        base_rows = self.base_collector.get_live_scores(target_date) if self.base_collector else []
        overlay_rows = self.overlay_collector.get_live_scores(target_date) if self.overlay_collector else []
        return self._merge_rows(base_rows or [], overlay_rows or [])

    def get_schedule(self, target_date):
        base_rows = self.base_collector.get_schedule(target_date) if self.base_collector else []
        overlay_rows = self.overlay_collector.get_schedule(target_date) if self.overlay_collector else []
        return self._merge_rows(base_rows or [], overlay_rows or [])

    def get_standings(self) -> List[Dict[str, Any]]:
        overlay_rows = self.overlay_collector.get_standings() if self.overlay_collector else []
        if overlay_rows:
            return overlay_rows
        return self.base_collector.get_standings() if self.base_collector else []

    def get_season_info(self):
        overlay_info = self.overlay_collector.get_season_info() if self.overlay_collector else None
        if overlay_info:
            return overlay_info
        return self.base_collector.get_season_info() if self.base_collector else None
