from __future__ import annotations

import csv
import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo


def _iso_utc_from_epoch(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _safe_int(value: Any) -> Optional[int]:
    try:
        if value in (None, ""):
            return None
        return int(float(value))
    except Exception:
        return None


def _read_csv(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8-sig") as f:
        return [dict(row) for row in csv.DictReader(f)]


def _parse_date(value: Any) -> Optional[str]:
    text = _clean(value)
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%Y%m%d", "%m/%d/%Y", "%m/%d/%y", "%m-%d-%Y", "%m-%d-%y"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    return None


_CLASSIFICATION_TYPE_ORDER = {
    "stage": 0,
    "gc": 1,
    "points": 2,
    "kom": 3,
    "youth": 4,
    "teams": 5,
    "combative": 6,
}

_TOUR_TIMEZONE = ZoneInfo("Europe/Paris")


def _local_time_to_utc_iso(stage_date: Optional[str], local_time: Any) -> Optional[str]:
    if not stage_date or not local_time:
        return None
    try:
        naive = datetime.strptime(f"{stage_date} {str(local_time).strip()}", "%Y-%m-%d %H:%M")
    except ValueError:
        return None
    localized = naive.replace(tzinfo=_TOUR_TIMEZONE)
    return localized.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _timezone_abbrev(stage_date: Optional[str], local_time: Any) -> str:
    if stage_date and local_time:
        try:
            naive = datetime.strptime(f"{stage_date} {str(local_time).strip()}", "%Y-%m-%d %H:%M")
            return naive.replace(tzinfo=_TOUR_TIMEZONE).tzname() or "Europe/Paris"
        except ValueError:
            pass
    return datetime.now(_TOUR_TIMEZONE).tzname() or "Europe/Paris"


class TourDeFranceDataService:
    bundle_basename = "letour_app_bundle"
    default_race = "Tour de France"
    enable_stage_overlay = True

    def __init__(self, data_dir: str):
        self.data_dir = Path(data_dir)
        self.repo_root = Path(__file__).resolve().parents[3]

    def _bundle_path(self, year: int) -> Path:
        preferred = self.data_dir / f"{self.bundle_basename}_{year}.json"
        if preferred.exists():
            return preferred
        return self.data_dir / f"{self.bundle_basename}.json"

    def _csv_path(self, name: str) -> Path:
        return self.data_dir / name

    def _source_updated_at(self, path: Path, payload: Optional[Dict[str, Any]] = None) -> Optional[str]:
        if payload:
            generated_at = payload.get("generated_at")
            if generated_at:
                raw = str(generated_at).strip().replace("Z", "+00:00")
                try:
                    dt = datetime.fromisoformat(raw)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
                except Exception:
                    pass
        if path.exists():
            return _iso_utc_from_epoch(path.stat().st_mtime)
        return None

    def _load_bundle(self, year: int) -> Dict[str, Any]:
        path = self._bundle_path(year)
        if path.exists():
            payload = json.loads(path.read_text(encoding="utf-8"))
            payload["_bundle_path"] = str(path)
            return payload

        stages = _read_csv(self._csv_path("stages.csv"))
        classifications = _read_csv(self._csv_path("classifications.csv"))
        teams = _read_csv(self._csv_path("teams.csv"))
        riders = _read_csv(self._csv_path("riders.csv"))
        stage_map: Dict[int, Dict[str, Any]] = {}
        for raw in stages:
            stage_number = _safe_int(raw.get("stage_number"))
            if stage_number is None:
                continue
            stage_map[stage_number] = {
                "stage": raw,
                "schedule": {
                    key: raw.get(key)
                    for key in (
                        "stage_number",
                        "stage_name",
                        "cycling_url",
                        "rankings_url",
                        "stage_start_local",
                        "stage_finish_expected_local",
                        "stage_first_start_local",
                        "stage_last_arrival_local",
                        "poll_state",
                        "recommended_poll_minutes",
                    )
                },
                "classifications": [],
            }
        for raw in classifications:
            stage_number = _safe_int(raw.get("stage_number"))
            if stage_number is None:
                continue
            stage_map.setdefault(stage_number, {"stage": {}, "schedule": {}, "classifications": []})
            stage_map[stage_number]["classifications"].append(raw)

        bundle = {
            "race": self.default_race,
            "source": f"{self.bundle_basename}-csv",
            "generated_files": [],
            "teams": teams,
            "riders": riders,
            "stages": [stage_map[k] for k in sorted(stage_map)],
            "_bundle_path": str(self.data_dir / "stages.csv"),
        }
        return bundle

    def _overlay_stage_rows(self) -> List[Dict[str, Any]]:
        if not self.enable_stage_overlay:
            return []
        for path in (
            self._csv_path("cycling_stages.csv"),
            self.repo_root / "sports-data-service" / "templates" / "cycling_stages.csv",
        ):
            rows = _read_csv(path)
            if rows:
                return rows
        return []

    def _overlay_stage_dates(self, stages: List[Dict[str, Any]]) -> None:
        rows = self._overlay_stage_rows()
        by_stage = {}
        for raw in rows:
            stage_number_raw = _clean(raw.get("stage_number"))
            if not stage_number_raw.isdigit():
                continue
            stage_number = _safe_int(stage_number_raw)
            if stage_number is not None:
                by_stage[stage_number] = raw

        for entry in stages:
            stage = entry.get("stage") or {}
            stage_number = _safe_int(stage.get("stage_number"))
            if stage_number is None:
                continue
            overlay = by_stage.get(stage_number) or {}
            if not stage.get("date"):
                stage["date"] = _parse_date(overlay.get("date"))
            if not stage.get("distance_km") and overlay.get("distance_km"):
                stage["distance_km"] = overlay.get("distance_km")
            if not stage.get("race_type") and overlay.get("race_type"):
                stage["race_type"] = overlay.get("race_type")
            if not stage.get("cycling_country") and overlay.get("cycling_country"):
                stage["cycling_country"] = overlay.get("cycling_country")

    def _normalize_stage(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        stage = dict(raw)
        stage["stage_number"] = _safe_int(stage.get("stage_number"))
        stage["recommended_poll_minutes"] = _safe_int(stage.get("recommended_poll_minutes"))
        stage["date"] = _parse_date(stage.get("date")) or _clean(stage.get("date")) or None
        status = _clean(stage.get("status")).lower()
        if status in ("completed", "final", "finished"):
            status = "final"
        elif status in ("live", "in_progress", "in progress", "active_window"):
            status = "in_progress"
        elif not status:
            status = "scheduled"
        stage["status"] = status
        return stage

    def _normalize_schedule(self, raw: Any) -> Dict[str, Any]:
        if isinstance(raw, list):
            raw = raw[0] if raw else {}
        elif not isinstance(raw, dict):
            raw = {}
        schedule = dict(raw)
        schedule["stage_number"] = _safe_int(schedule.get("stage_number"))
        schedule["recommended_poll_minutes"] = _safe_int(schedule.get("recommended_poll_minutes"))
        return schedule

    def _annotate_stage_timezones(self, stage: Dict[str, Any], schedule: Dict[str, Any]) -> None:
        stage_date = stage.get("date")
        timezone_abbrev = _timezone_abbrev(
            stage_date,
            stage.get("stage_start_local") or schedule.get("stage_start_local"),
        )
        shared = {
            "stage_timezone": "Europe/Paris",
            "stage_timezone_abbrev": timezone_abbrev,
        }
        stage.update(shared)
        schedule.update(shared)

        for field in (
            "stage_start_local",
            "stage_finish_expected_local",
            "stage_first_start_local",
            "stage_last_arrival_local",
        ):
            utc_field = field.replace("_local", "_utc")
            local_value = stage.get(field) or schedule.get(field)
            utc_value = _local_time_to_utc_iso(stage_date, local_value)
            stage[utc_field] = utc_value
            schedule[utc_field] = utc_value

    def _normalize_classification_row(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        row = dict(raw)
        row["stage_number"] = _safe_int(row.get("stage_number"))
        row["rank"] = _safe_int(row.get("rank"))
        row["bib"] = _safe_int(row.get("bib"))
        row["classification_type"] = _clean(row.get("classification_type")).lower()
        return row

    def _classification_boards(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        grouped: Dict[str, List[Dict[str, Any]]] = {}
        for row in rows:
            ctype = _clean(row.get("classification_type")).lower()
            if not ctype:
                continue
            grouped.setdefault(ctype, []).append(row)

        ordered_types = sorted(
            grouped,
            key=lambda ctype: (_CLASSIFICATION_TYPE_ORDER.get(ctype, 999), ctype),
        )
        return [
            {
                "classification_type": ctype,
                "rows": grouped[ctype],
            }
            for ctype in ordered_types
        ]

    def _latest_classifications(self, stages: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        latest: Dict[str, Dict[str, Any]] = {}
        for entry in stages:
            stage = entry.get("stage") or {}
            stage_number = _safe_int(stage.get("stage_number")) or 0
            for row in entry.get("classification_rows") or []:
                ctype = _clean(row.get("classification_type")).lower()
                if not ctype:
                    continue
                slot = latest.get(ctype)
                if slot is None or stage_number >= slot["stage_number"]:
                    latest[ctype] = {"stage_number": stage_number, "rows": entry.get("classification_rows") or []}

        boards: Dict[str, List[Dict[str, Any]]] = {}
        for ctype, payload in latest.items():
            boards[ctype] = [
                row for row in payload["rows"]
                if _clean(row.get("classification_type")).lower() == ctype
            ]
        return boards

    def _current_stage(self, stages: List[Dict[str, Any]], year: int) -> Optional[Dict[str, Any]]:
        if not stages:
            return None
        today = date.today().isoformat()

        active = [
            entry for entry in stages
            if (entry.get("stage") or {}).get("poll_state") == "active_window"
        ]
        if active:
            return active[0]

        dated = []
        undated = []
        for entry in stages:
            stage = entry.get("stage") or {}
            if stage.get("date"):
                dated.append(entry)
            else:
                undated.append(entry)

        future_or_today = [
            entry for entry in dated
            if (entry.get("stage") or {}).get("date") >= today
        ]
        if future_or_today:
            return sorted(future_or_today, key=lambda item: (item.get("stage") or {}).get("date") or "")[0]

        if dated:
            return sorted(
                dated,
                key=lambda item: (
                    (item.get("stage") or {}).get("date") or "",
                    (item.get("stage") or {}).get("stage_number") or 0,
                ),
            )[-1]

        if undated:
            return sorted(
                undated,
                key=lambda item: (item.get("stage") or {}).get("stage_number") or 0,
            )[-1]

        return None

    def get_bundle(self, year: int) -> Dict[str, Any]:
        payload = self._load_bundle(year)
        source_updated_at = self._source_updated_at(Path(payload.get("_bundle_path", "")), payload)
        stages = payload.get("stages") or []
        self._overlay_stage_dates(stages)

        normalized_stages = []
        for entry in stages:
            stage = self._normalize_stage(entry.get("stage") or {})
            schedule = self._normalize_schedule(entry.get("schedule") or {})
            self._annotate_stage_timezones(stage, schedule)
            classification_rows = [self._normalize_classification_row(row) for row in (entry.get("classifications") or [])]
            normalized_stages.append({
                "stage": stage,
                "schedule": schedule,
                "classifications": self._classification_boards(classification_rows),
                "classification_rows": classification_rows,
            })

        current_stage = self._current_stage(normalized_stages, year)
        latest_classifications = self._latest_classifications(normalized_stages)
        bundle = {
            "race": payload.get("race") or self.default_race,
            "year": year,
            "source": payload.get("source") or self.bundle_basename,
            "generated_at": payload.get("generated_at") or source_updated_at,
            "source_updated_at": source_updated_at,
            "generated_files": payload.get("generated_files") or [],
            "current_stage": current_stage,
            "stages": normalized_stages,
            "latest_classifications": latest_classifications,
            "teams": payload.get("teams") or [],
            "riders": payload.get("riders") or [],
        }
        return bundle

    def get_stage(self, year: int, stage_number: int) -> Optional[Dict[str, Any]]:
        bundle = self.get_bundle(year)
        for entry in bundle.get("stages") or []:
            if _safe_int((entry.get("stage") or {}).get("stage_number")) == stage_number:
                stage_results: List[Dict[str, Any]] = []
                for board in entry.get("classifications") or []:
                    if _clean(board.get("classification_type")).lower() == "stage":
                        stage_results = list(board.get("rows") or [])
                        break
                return {
                    "race": bundle.get("race"),
                    "year": bundle.get("year"),
                    "source": bundle.get("source"),
                    "generated_at": bundle.get("generated_at"),
                    "source_updated_at": bundle.get("source_updated_at"),
                    "stage_results": stage_results,
                    "overall_classifications": bundle.get("latest_classifications") or {},
                    **entry,
                }
        return None


class LaVueltaDataService(TourDeFranceDataService):
    bundle_basename = "lavuelta_app_bundle"
    default_race = "La Vuelta"
    enable_stage_overlay = False
