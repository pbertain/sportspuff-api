#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo


RACE_TIMEZONE = ZoneInfo("Europe/Rome")


def _bundle_path(outdir: Path, year: int) -> Path:
    preferred = outdir / f"giro_app_bundle_{year}.json"
    if preferred.exists():
        return preferred
    return outdir / "giro_app_bundle.json"


def _bundle_generated_at(bundle_path: Path) -> datetime | None:
    try:
        payload = json.loads(bundle_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    raw = str(payload.get("generated_at") or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _is_due(bundle_path: Path, now: datetime, freshness_minutes: int = 60) -> bool:
    generated_at = _bundle_generated_at(bundle_path)
    if generated_at is None:
        return True
    return (now.astimezone(timezone.utc) - generated_at).total_seconds() >= freshness_minutes * 60


def _run_builder(outdir: Path, year: int) -> int:
    cmd = [
        sys.executable,
        str((Path(__file__).resolve().parent / "giro_multi_stage_builder.py")),
        "--year",
        str(year),
        "--start-stage",
        "1",
        "--end-stage",
        "21",
        "--outdir",
        str(outdir),
    ]
    print(f"Refreshing Giro bundle for {year} into {outdir}")
    return subprocess.run(cmd, check=False).returncode


def main() -> int:
    parser = argparse.ArgumentParser(description="Giro d'Italia refresh hook.")
    parser.add_argument("--outdir", default=".", help="Directory containing Giro bundle artifacts.")
    args = parser.parse_args()

    outdir = Path(args.outdir).resolve()
    year = datetime.now(RACE_TIMEZONE).year
    bundle_path = _bundle_path(outdir, year)
    now = datetime.now(timezone.utc)
    generated_at = _bundle_generated_at(bundle_path)
    if generated_at is None and bundle_path.exists():
        print(f"Giro bundle exists but is unreadable: {bundle_path}")
        return 0

    if generated_at is not None and not _is_due(bundle_path, now):
        age_seconds = max(0, int((now - generated_at).total_seconds()))
        print(f"Skipping refresh: {bundle_path.name} generated_at is newer than 60 minutes (age={age_seconds}s)")
        return 0

    return _run_builder(outdir, year)


if __name__ == "__main__":
    raise SystemExit(main())
