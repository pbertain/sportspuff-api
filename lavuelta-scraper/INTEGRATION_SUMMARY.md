# La Vuelta integration summary

This bundle mirrors the Tour de France setup, but uses a separate script and a separate output directory.

The Vuelta calendar is now pulled from `https://www.lavuelta.es/en/overall-route`, which gives the full 21-stage list with dates, stage types, start/finish cities, and distances for stage cards.

## CLI compatibility

The La Vuelta script intentionally matches the Tour script's CLI:

```bash
python lavuelta_multi_stage_builder.py   --year 2026   --start-stage 1   --end-stage 21   --outdir output/lavuelta-prod
```

Supported flags are the same shape as the Tour builder:
- `--year`
- `--start-stage`
- `--end-stage`
- `--outdir`

The default output directory is separate from the Tour version, so the two races do not overwrite each other.

## What the script generates

Each run dynamically regenerates:
- `stages.csv`
- `classifications.csv`
- `teams.csv`
- `riders.csv`
- `stage_schedule.csv`
- `polling_plan.json`
- `suggested_cron.txt`
- `lavuelta_app_bundle.json`
- `manifest.csv`

## Integration pattern

1. Keep `lavuelta_multi_stage_builder.py` as the Vuelta-specific entry point.
2. Schedule it the same way as the Tour script.
3. Read outputs from the Vuelta-specific output directory.
4. Ingest `lavuelta_app_bundle.json` if you want a single JSON payload, or consume the CSV files directly.

The operational model is the same as the Tour setup; only the upstream site and output path differ.
