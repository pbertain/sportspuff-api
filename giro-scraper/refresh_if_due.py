#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
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


def main() -> int:
    parser = argparse.ArgumentParser(description="Giro d'Italia refresh hook.")
    parser.add_argument("--outdir", default=".", help="Directory containing Giro bundle artifacts.")
    args = parser.parse_args()

    outdir = Path(args.outdir).resolve()
    year = datetime.now(RACE_TIMEZONE).year
    bundle_path = _bundle_path(outdir, year)
    now = datetime.now(timezone.utc)
    if not bundle_path.exists():
        print(f"Giro bundle not present yet for {year}; skipping refresh")
        return 0

    generated_at = _bundle_generated_at(bundle_path)
    if generated_at is None:
        print(f"Giro bundle exists but is unreadable: {bundle_path}")
        return 0

    if not _is_due(bundle_path, now):
        age_seconds = max(0, int((now - generated_at).total_seconds()))
        print(f"Skipping refresh: {bundle_path.name} generated_at is newer than 60 minutes (age={age_seconds}s)")
        return 0

    print(f"Giro bundle present for {year}; generated_at={generated_at.astimezone(RACE_TIMEZONE).isoformat()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
