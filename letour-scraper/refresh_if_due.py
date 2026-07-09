#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime
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


def _is_due(bundle_path: Path, interval_minutes: int) -> bool:
    if not bundle_path.exists():
        return True
    age_seconds = max(0, time.time() - bundle_path.stat().st_mtime)
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
        print(f"Skipping refresh: {bundle_path.name} is newer than {interval_minutes} minutes")
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
