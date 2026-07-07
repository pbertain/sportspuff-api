# Tour de France integration summary

## What this bundle contains

- `letour_multi_stage_builder.py`: scraper/normalizer entry point.
- `letour_app_bundle.json`: app-friendly JSON export with stage metadata, schedules, classifications, riders, and teams.
- CSVs for stages, classifications, teams, riders, and stage schedule.

## Suggested integration flow

1. Run `letour_multi_stage_builder.py` on a schedule.
2. Read `stage_schedule.csv` or the JSON stage objects to determine polling cadence.
3. During active windows, refresh every 15 minutes; otherwise refresh hourly.
4. Load `letour_app_bundle.json` into your app and render from that single object.
5. Use `riders` and `teams` arrays, or the `rider_url` and `team_url` fields embedded in classification rows, to generate links.

## Example command

```bash
python letour_multi_stage_builder.py   --year 2026   --start-stage 1   --end-stage 21   --outdir output/letour-prod
```

## JSON shape

- `race`: race name.
- `source`: upstream site.
- `teams[]`: team lookup rows with `team_name`, `team_slug`, `team_url`.
- `riders[]`: rider lookup rows with `rider_name`, `rider_slug`, `rider_url`.
- `stages[]`: one object per stage:
  - `stage`: stage metadata plus poll hints.
  - `schedule`: schedule helper row(s).
  - `classifications`: normalized ranking rows for that stage.

## Operational notes

- The builder validates stage-specific URLs before exporting.
- A stage is treated as active from 30 minutes before start until 60 minutes after expected finish/last arrival.
- Treat the stage as effectively complete when two consecutive polls return unchanged results after the finish window.
- The extraction is HTML-based and should be monitored for layout changes on letour.fr.
