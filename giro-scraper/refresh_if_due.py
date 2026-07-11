#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo


RACE_TIMEZONE = ZoneInfo("Europe/Rome")


def _bundle_path(outdir: Path, year: int) -> Path:
    preferred = outdir / f"giro_app_bundle_{year}.json"
    if preferred.exists():
        return preferred
    return outdir / "giro_app_bundle.json"


def main() -> int:
    parser = argparse.ArgumentParser(description="Placeholder Giro d'Italia refresh hook.")
    parser.add_argument("--outdir", default=".", help="Directory containing Giro bundle artifacts.")
    args = parser.parse_args()

    outdir = Path(args.outdir).resolve()
    year = datetime.now(RACE_TIMEZONE).year
    bundle_path = _bundle_path(outdir, year)
    if not bundle_path.exists():
        print(f"Giro bundle not present yet for {year}; skipping refresh")
        return 0

    try:
        payload = json.loads(bundle_path.read_text(encoding="utf-8"))
    except Exception:
        print(f"Giro bundle exists but is unreadable: {bundle_path}")
        return 0

    age_seconds = max(0, time.time() - bundle_path.stat().st_mtime)
    print(f"Giro bundle present for {payload.get('year') or year}; age={int(age_seconds)}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
