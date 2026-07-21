# SPv6 cycling rest-days and stage-points handoff

This note is the short integration guide for two additions to the cycling bundle/stage endpoints:

- Rest days now appear in the Tour de France stage list. **Tour de France only** — see below.
- KOM and Points-jersey classification rows now carry a `points_earned` field (points scored in that specific stage, not the cumulative jersey total). **All three races** — Tour de France, La Vuelta, and Giro d'Italia.

Applies to:

- `GET /api/v1/cycling/tour-de-france/{year}` and its stage route — both additions
- `GET /api/v1/cycling/la-vuelta/{year}` and its stage route — `points_earned` only
- `GET /api/v1/cycling/giro-d-italia/{year}` and its stage route — `points_earned` only

No endpoint paths or existing fields changed. Both additions are new fields on the existing response shape, so old clients keep working unmodified.

## Data-freshness caveat (Vuelta and Giro)

Each race's bundle data is cached and auto-refreshed on the backend: a request triggers a re-scrape only when the existing bundle is older than a freshness window (60 minutes for Giro, a dynamic interval based on stage timing for Vuelta and the Tour). If you test against Vuelta or Giro and don't see `kom`/`points` rows or `points_earned` yet, it means the last scrape predates this backend change and hasn't hit its refresh window — it'll pick up the new fields on the next auto-refresh, no client-side action needed. Tour de France should show the new fields immediately since scraping worked correctly there before this change too.

## Points earned per stage (KOM and Points jerseys) — Tour de France, La Vuelta, Giro d'Italia

Every classification row with `classification_type === "kom"` or `"points"` now has a `points_earned` field, on all three races:

```json
{
  "classification_type": "points",
  "rank": 1,
  "rider_name": "T. POGACAR",
  "team_name": "UAE TEAM EMIRATES XRG",
  "points": "55 PTS",
  "points_earned": 30
}
```

- `points` — unchanged, still the cumulative jersey total as of that stage (a display string, e.g. `"55 PTS"`).
- `points_earned` — new, an integer, the points that rider scored in that specific stage only.

This is on every KOM/points row in `classification_rows` and inside the matching `classifications[].rows` board — same objects, so reading either place gives the same value.

### Where the number comes from (matters for how you should treat missing/zero values)

- **Tour de France:** computed as the difference between this stage's cumulative total and the previous stage's, matched per rider by bib number. A rider absent from a stage's KOM/points table (they scored nothing to date) won't have a row at all that stage — there's no zero-row to render.
- **La Vuelta:** sourced the same way (delta between consecutive stages), same behavior as above.
- **Giro d'Italia:** sourced directly from that stage's own results page, not computed as a delta — Giro's cumulative jersey pages don't support historical "as of stage N" lookups, so there's no reliable cumulative-per-stage number to diff. Treat `points_earned` as authoritative here; don't try to reconcile it against `points` for Giro since `points` on the per-stage KOM/points rows may be smaller than you'd expect from a pure cumulative jersey standing.

**What to render:** a "+N pts today" style badge next to a rider's cumulative jersey points, using `points_earned`. If the field is `0` or absent on a row, that rider scored nothing new that stage — don't show a badge, don't treat it as an error.

## Rest days — Tour de France only

The Tour's `stages[]` list now includes rest-day entries alongside numbered stages, sorted chronologically by date. A rest-day entry looks like a stage entry but has no `stage_number`:

```json
{
  "stage": {
    "race": "Tour de France",
    "stage_number": null,
    "stage_name": "Rest 1",
    "date": "2026-07-13",
    "status": "final",
    "race_type": "Rest Day",
    "is_rest_day": true,
    "start_city": "Cantal",
    "finish_city": null
  },
  "schedule": {},
  "classifications": []
}
```

**What to check:** `stage.is_rest_day === true`. Don't key off `race_type === "Rest Day"` alone — that's a convenience label, `is_rest_day` is the stable flag.

**What to render:** a rest-day card/row has no results, no classification boards, and no start/finish times. Skip attempting to render `classifications`/`classification_rows` for these entries — they're always empty arrays.

**Not affected:** the stage-specific route (`/stages/{stage_number}`) is unaffected — a rest day has no `stage_number`, so it's never reachable there and never returned by that endpoint. Rest days only show up in the bundle's `stages[]` array.

**La Vuelta / Giro d'Italia:** rest days are not implemented for these two races at all — there's no rest-day data source wired up, so their `stages[]` arrays only ever contain numbered stages. This isn't a freshness issue like the points-earned caveat above; it needs backend work before it can appear.

## Minimal SPv6 example

```ts
// Tour de France: rest days + points earned
const tdf = await fetch(`/api/v1/cycling/tour-de-france/2026`).then(r => r.json());

const stageList = tdf.stages.filter((s) => !s.stage.is_rest_day);
const restDays = tdf.stages.filter((s) => s.stage.is_rest_day);

const stage = tdf.stages.find((s) => s.stage.stage_number === 4);
const pointsBoard = (stage?.classification_rows ?? []).filter(
  (row) => row.classification_type === "points"
);
const leaderEarnedToday = pointsBoard[0]?.points_earned ?? 0;

// La Vuelta / Giro d'Italia: same points_earned field, no rest days
const vuelta = await fetch(`/api/v1/cycling/la-vuelta/2026`).then(r => r.json());
const vueltaStage = vuelta.stages.find((s) => s.stage.stage_number === 4);
const vueltaKomToday = (vueltaStage?.classification_rows ?? [])
  .filter((row) => row.classification_type === "kom")[0]?.points_earned ?? 0;
```

## Practical rule

Use `points_earned` for "today's gain" badges on KOM/points boards across all three races — never derive it yourself by diffing `points` client-side, since Giro's number isn't a diffable cumulative value. For the Tour de France only, also render the stage list in date order including rest days (they're already sorted in) and skip results rendering for `is_rest_day` entries.

