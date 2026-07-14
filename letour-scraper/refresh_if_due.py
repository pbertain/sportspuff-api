#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo


TOUR_TIMEZONE = ZoneInfo("Europe/Paris")


def _bundle_path(outdir: Path, year: int) -> Path:
    preferred = outdir / f"letour_app_bundle_{year}.json"
    if preferred.exists():
        return preferred
    return outdir / "letour_app_bundle.json"


def _recommended_interval_minutes(bundle_path: Path) -> int:
    if not bundle_path.exists():
        return 15
    try:
        payload = json.loads(bundle_path.read_text(encoding="utf-8"))
    except Exception:
        return 15

    intervals = []
    for entry in payload.get("stages") or []:
        stage = entry.get("stage") or {}
        poll_state = stage.get("poll_state")
        minutes = stage.get("recommended_poll_minutes")
        if not isinstance(minutes, int):
            try:
                minutes = int(minutes)
            except Exception:
                minutes = None
        if minutes:
            intervals.append((poll_state, minutes))

    active = [minutes for poll_state, minutes in intervals if poll_state == "active_window"]
    if active:
        return min(active)
    passive = [minutes for _, minutes in intervals]
    if passive:
        return max(passive)
    return 60


def _bundle_generated_at(bundle_path: Path) -> datetime | None:
    if not bundle_path.exists():
        return None
    try:
        payload = json.loads(bundle_path.read_text(encoding="utf-8"))
    except Exception:
        return None

    generated_at = payload.get("generated_at")
    if not generated_at:
        return None
    try:
        return datetime.fromisoformat(str(generated_at).replace("Z", "+00:00"))
    except ValueError:
        return None


def _bundle_payload(bundle_path: Path) -> dict | None:
    if not bundle_path.exists():
        return None
    try:
        payload = json.loads(bundle_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _parse_stage_date(value: object) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def _parse_stage_end(stage: dict, stage_day: date) -> datetime | None:
    local_time = stage.get("stage_last_arrival_local") or stage.get("stage_finish_expected_local")
    text = str(local_time or "").strip()
    if not text:
        return None
    try:
        finish_time = datetime.strptime(text, "%H:%M").time()
    except ValueError:
        return None
    return datetime.combine(stage_day, finish_time, tzinfo=TOUR_TIMEZONE)


def _bundle_has_recoverable_gap(bundle_path: Path, now: datetime) -> bool:
    payload = _bundle_payload(bundle_path)
    if not payload:
        return True

    today = now.date()
    for entry in payload.get("stages") or []:
        stage = entry.get("stage") or {}
        stage_day = _parse_stage_date(stage.get("date"))
        if stage_day is None:
            continue

        status = str(stage.get("status") or "").strip().lower()
        race_type = str(stage.get("race_type") or "").strip().lower()
        winner = str(stage.get("winner") or "").strip()

        if stage_day < today and status != "final":
            return True

        finish_at = _parse_stage_end(stage, stage_day)
        if finish_at is not None and now >= finish_at + timedelta(minutes=60) and status != "final":
            return True

        if race_type != "team time-trial" and status == "final" and not winner:
            return True

    return False


def _is_due(bundle_path: Path, interval_minutes: int, now: datetime | None = None) -> bool:
    generated_at = _bundle_generated_at(bundle_path)
    if generated_at is None:
        return True
    now = now or datetime.now(TOUR_TIMEZONE)
    if _bundle_has_recoverable_gap(bundle_path, now):
        return True
    if generated_at.tzinfo is None:
        generated_at = generated_at.replace(tzinfo=TOUR_TIMEZONE)
    age_seconds = max(0, (now.astimezone(generated_at.tzinfo) - generated_at).total_seconds())
    return age_seconds >= interval_minutes * 60


def main() -> int:
    parser = argparse.ArgumentParser(description="Refresh Tour bundle only when polling guidance says it is due.")
    parser.add_argument("--outdir", default=".", help="Directory containing letour bundle artifacts.")
    args = parser.parse_args()

    outdir = Path(args.outdir).resolve()
    year = datetime.now(TOUR_TIMEZONE).year
    bundle_path = _bundle_path(outdir, year)
    interval_minutes = _recommended_interval_minutes(bundle_path)
    if not _is_due(bundle_path, interval_minutes):
        generated_at = _bundle_generated_at(bundle_path)
        print(f"Skipping refresh: {bundle_path.name} generated at {generated_at.isoformat() if generated_at else 'unknown'} is newer than {interval_minutes} minutes")
        return 0

    cmd = [
        sys.executable,
        str((Path(__file__).resolve().parent / "letour_multi_stage_builder.py")),
        "--year",
        str(year),
        "--start-stage",
        "1",
        "--end-stage",
        "21",
        "--outdir",
        str(outdir),
    ]
    print(f"Refreshing Tour bundle for {year} into {outdir}")
    return subprocess.run(cmd, check=False).returncode


if __name__ == "__main__":
    raise SystemExit(main())
