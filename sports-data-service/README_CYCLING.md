# Cycling CSV Ingest

This directory documents the file-backed cycling overlay used when `CYCLING_DATA_DIR` is configured.

## Overview

The API reads a directory of CSV files and converts them into the existing cycling response shape:

- stage results
- GC standings
- team classification
- jersey standings

The goal is to keep the source files easy for a human to edit while still producing the richer JSON needed by the API and frontend.
When `CYCLING_DATA_DIR` is configured, the file rows are used as an overlay on top of the live cycling feed so they can supply dates, URLs, stage labels, and local notes without replacing the upstream feed.

## Configuration

Set:

```bash
CYCLING_PROVIDER=file
CYCLING_DATA_DIR=/path/to/cycling-data
```

The directory should contain CSV files named:

- `cycling_stages.csv`
- `cycling_gc.csv`
- `cycling_team_classification.csv`
- `cycling_jerseys.csv`

## Templates

Sample templates live in `templates/`:

- `templates/cycling_stages.csv`
- `templates/cycling_gc.csv`
- `templates/cycling_team_classification.csv`
- `templates/cycling_jerseys.csv`

## Notes

- Leave rows blank for classifications you do not want to publish yet.
- If `CYCLING_DATA_DIR` is missing or empty, the API uses the TheSportsDB cycling collector alone.
- The CSV import is intentionally narrow: it is designed for Tour de France season data first, then can be extended to other stage races if needed.
