# Tour de France letour-scraper handoff

This note explains how to incorporate `letour-scraper/` into the existing Sports Data Service so SPv6 can consume detailed Tour de France data.

## Current status

As of July 7, 2026, this handoff is implementation-ready for SPv6.

What is now shipped in-repo:

- detailed API routes:
  - `GET /api/v1/cycling/tour-de-france`
  - `GET /api/v1/cycling/tour-de-france/{year}`
  - `GET /api/v1/cycling/tour-de-france/{year}/stages/{stage_number}`
- a file-backed Tour loader in [sports-data-service/src/services/tour_de_france.py](/Users/paulb/Documents/version-control/git/sportspuff-api/sports-data-service/src/services/tour_de_france.py:1)
- explicit response schemas in [sports-data-service/src/schemas.py](/Users/paulb/Documents/version-control/git/sportspuff-api/sports-data-service/src/schemas.py:312)
- a repaired scraper in [letour-scraper/letour_multi_stage_builder.py](/Users/paulb/Documents/version-control/git/sportspuff-api/letour-scraper/letour_multi_stage_builder.py:1) that now exports:
  - real stage dates
  - normalized `scheduled | in_progress | final` stage status
  - stage-window-based `poll_state` and `recommended_poll_minutes`
  - clean classification types: `stage`, `gc`, `points`, `kom`, `youth`, `teams`
  - `generated_at` and `source_updated_at`
- refreshed checked-in Tour bundle data under [letour-scraper](/Users/paulb/Documents/version-control/git/sportspuff-api/letour-scraper)

Current verification:

- `cd letour-scraper && ../.venv312/bin/python -m pytest tests/test_letour_multi_stage_builder.py -q`
- `cd sports-data-service && ../.venv312/bin/python -m pytest tests/test_route_compat.py -q`

Both are passing in the local Python 3.12 review venv.

## SPv6 guidance

SPv6 should use [TOUR_DE_FRANCE_LETOUR_HANDOFF.md](/Users/paulb/Documents/version-control/git/sportspuff-api/TOUR_DE_FRANCE_LETOUR_HANDOFF.md) as the handoff document of record.

For frontend integration, the safe contract to depend on now is:

- `race`
- `year`
- `current_stage`
- `stages[]`
- `latest_classifications.gc[]`
- `latest_classifications.points[]`
- `latest_classifications.kom[]`
- `latest_classifications.youth[]`
- `latest_classifications.teams[]`
- `teams[]`
- `riders[]`
- `meta.source_updated_at`
- `generated_at`

As of July 7, 2026, the repo already has:

- a generic cycling calendar collector from TheSportsDB
- a file-backed cycling overlay for coarse Tour de France fields
- a standalone `letour-scraper/` bundle that contains richer stage-level data

The main conclusion: `letour-scraper` should not be wired directly into the existing cycling score/standings contract alone. It is richer than that contract. The right path is:

1. keep the existing cycling score cards for high-level race/stage rows
2. add a Tour-specific detailed endpoint backed by `letour-scraper`
3. optionally derive the old overlay CSVs from the scraper so the legacy cycling routes stay consistent

## What the scraper already gives us

`letour-scraper/INTEGRATION_SUMMARY.md` and `letour-scraper/letour_app_bundle.json` show the intended bundle shape:

- `stages[]`
  - `stage`: stage metadata, URLs, schedule windows, poll hints
  - `schedule`: schedule helper rows
  - `classifications`: ranking rows for that stage
- `teams[]`
- `riders[]`

That is materially better than the current cycling overlay, which only exposes:

- stage rows from `cycling_stages.csv`
- GC rows from `cycling_gc.csv`
- team classification rows from `cycling_team_classification.csv`
- jersey rows from `cycling_jerseys.csv`

See [sports-data-service/src/collectors/cycling_file.py](/Users/paulb/Documents/version-control/git/sportspuff-api/sports-data-service/src/collectors/cycling_file.py:92) and [sports-data-service/src/collectors/cycling_file.py](/Users/paulb/Documents/version-control/git/sportspuff-api/sports-data-service/src/collectors/cycling_file.py:167).

## Important current limitations

The current scraper output is useful, but not ready to treat as authoritative without fixes.

### 1. Stage status is hardcoded

`build_for_stage()` always sets:

- `date = None`
- `status = "completed"`

See [letour-scraper/letour_multi_stage_builder.py](/Users/paulb/Documents/version-control/git/sportspuff-api/letour-scraper/letour_multi_stage_builder.py:193).

That means a live or upcoming stage will still be exported as completed.

### 2. Poll window logic ignores the stage date

`infer_stage_state()` combines the parsed stage times with `now_local.date()` instead of the stage's own date.

See [letour-scraper/letour_multi_stage_builder.py](/Users/paulb/Documents/version-control/git/sportspuff-api/letour-scraper/letour_multi_stage_builder.py:147).

That works only if you are evaluating the current stage on the same calendar day. It is wrong for future stages and historical stages.

### 3. Classification extraction is duplicating tables

`extract_tables()` reads all HTML tables once through `pd.read_html(html)` and then again by iterating each `<table>` element.

See [letour-scraper/letour_multi_stage_builder.py](/Users/paulb/Documents/version-control/git/sportspuff-api/letour-scraper/letour_multi_stage_builder.py:48).

The current bundle reflects that problem:

- `classification_type` only shows `stage` and `classification_2`
- `classification_2` appears to be a duplicate of the stage result table

So the scraper is not currently producing clean GC, points, KOM, youth, or team-classification data.

### 4. Winner derivation is fragile

Winner is taken from the first `stage` row:

See [letour-scraper/letour_multi_stage_builder.py](/Users/paulb/Documents/version-control/git/sportspuff-api/letour-scraper/letour_multi_stage_builder.py:248).

But some top rows have a blank `rider_name` in the exported CSV, so `winner` can remain empty even when the stage is populated.

### 5. Existing backend config does not actually switch cycling providers

The config advertises `cycling_provider: thesportsdb | file`:

See [sports-data-service/src/config.py](/Users/paulb/Documents/version-control/git/sportspuff-api/sports-data-service/src/config.py:39).

But `get_collector("CYCLING")` always instantiates TheSportsDB and then always wraps it with the file overlay:

See [sports-data-service/src/api.py](/Users/paulb/Documents/version-control/git/sportspuff-api/sports-data-service/src/api.py:429).

So there is currently no real provider selection for cycling.

## Recommended integration design

### Phase 1: fast path for current Tour coverage

Use `letour-scraper` as a Tour-specific data source, not as a replacement for the entire cycling collector.

Add a new route family, for example:

- `GET /api/v1/cycling/tour-de-france`
- `GET /api/v1/cycling/tour-de-france/{year}`
- optional: `GET /api/v1/cycling/tour-de-france/{year}/stages/{stage_number}`

Suggested payload:

- `race`
- `year`
- `source`
- `generated_at`
- `current_stage`
- `stages`
- `classifications_by_stage`
- `classification_leaders`
- `teams`
- `riders`
- `meta`

This lets SPv6 render:

- stage cards
- full stage results
- GC / points / KOM / youth boards
- rider/team detail links
- freshness / polling state

without overloading the generic `scores` and `standings` contracts.

### Phase 2: keep legacy cycling routes in sync

Generate the existing file-overlay CSVs from the scraper output:

- `cycling_stages.csv` from `stages.csv`
- `cycling_gc.csv` from the latest GC classification
- `cycling_team_classification.csv` from the latest team classification
- `cycling_jerseys.csv` from points / KOM / youth / GC leader rows

That preserves:

- `GET /api/v1/scores/cycling/{date}`
- `GET /api/v1/standings/cycling`
- `GET /api/v1/season-info/cycling`

for simple widgets, while SPv6 uses the detailed Tour endpoint for the full experience.

## Concrete backend shape

### 1. Create a Tour bundle loader

Add a small loader service or collector that reads:

- `letour_app_bundle.json` if present
- otherwise falls back to `stages.csv`, `classifications.csv`, `teams.csv`, `riders.csv`

It should expose methods like:

- `get_bundle(year)`
- `get_stage(year, stage_number)`
- `get_latest_stage(year)`
- `get_classification(year, classification_type, stage_number=None)`
- `get_source_updated_at()`

### 2. Normalize the payload for API use

The API payload should not leak the scraper's temporary quirks.

Backend normalization should:

- parse or inject the real stage date
- map status into `scheduled | in_progress | final`
- compute `current_stage`
- group classifications by stage
- derive latest leaderboards from the most recent valid classification rows
- include `recommended_poll_minutes`

### 3. Add explicit schemas

Add Pydantic models for:

- `CyclingTourBundleResponse`
- `CyclingStageDetail`
- `CyclingClassificationRow`
- `CyclingClassificationBoard`

Do not try to squeeze this into `GameOut`. That model is too flat for detailed cycling data.

### 4. Keep metadata explicit

The detailed endpoint should carry:

- `source_updated_at`
- `generated_at`
- `recommended_poll_minutes`
- `poll_state`

SPv6 should rely on those instead of inferring freshness on the client.

## Scraper fixes required before calling it production-ready

### Required

1. Remove duplicate table extraction in `extract_tables()`.
2. Make classification detection reliable for:
   - `stage`
   - `gc`
   - `points`
   - `kom`
   - `youth`
   - `teams`
3. Export stage dates.
4. Export real stage status instead of hardcoded `completed`.
5. Base poll-window calculation on the stage date, not `now_local.date()`.
6. Make winner extraction resilient when rider name is missing in the first parsed row.

### Strongly recommended

1. Add `generated_at` to the main JSON bundle.
2. Add `source_updated_at` from the scraper run time.
3. Add a checksum or snapshot hash so the poller can detect unchanged results.
4. Add tests using saved HTML fixtures for at least one flat stage and one mountain stage.

## Minimal SPv6 contract

If the backend team needs the narrowest useful deliverable for SPv6, ship this:

1. `GET /api/v1/cycling/tour-de-france/2026`
2. Payload fields:
   - `race`
   - `year`
   - `current_stage`
   - `stages[]`
   - `latest_classifications.gc[]`
   - `latest_classifications.points[]`
   - `latest_classifications.kom[]`
   - `latest_classifications.youth[]`
   - `latest_classifications.teams[]`
   - `teams[]`
   - `riders[]`
   - `meta.source_updated_at`
   - `meta.generated_at`
3. Keep the old `/scores/cycling` and `/standings/cycling` endpoints alive for summary widgets.

That is enough for SPv6 to build:

- race header
- current stage module
- detailed classification tabs
- rider/team links
- refresh cadence

## Operational recommendation

For July 2026 race coverage:

- run the scraper hourly outside the active stage window
- run it every 15 minutes during the active window
- treat the detailed Tour endpoint as a cached file-backed API view, not as a live per-request scraper

That matches the scheduler guidance already generated by `letour-scraper`.

## Bottom line

`letour-scraper` is the right source for detailed Tour de France coverage, but it should back a new detailed Tour endpoint, not only the existing generic cycling routes.

The existing cycling overlay remains useful for summary cards. SPv6 should consume the new Tour payload for the detailed Tour experience.
