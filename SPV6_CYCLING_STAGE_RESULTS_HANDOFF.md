# SPv6 cycling stage-results handoff

This note is the short integration guide for SPv6.

## What to call

Use the stage-specific endpoint when you want the results for one stage:

- `GET /api/v1/cycling/tour-de-france/{year}/stages/{stage_number}`
- `GET /api/v1/cycling/la-vuelta/{year}/stages/{stage_number}`

Use the bundle endpoint when you want the whole race and plan to locate the stage yourself:

- `GET /api/v1/cycling/tour-de-france/{year}`
- `GET /api/v1/cycling/la-vuelta/{year}`

## Where the stage results are

For the stage-specific endpoint, read:

- `stage_results`

That is the explicit list of rider results for the stage.

The same response also includes:

- `stage`
- `schedule`
- `classifications`
- `classification_rows`
- `overall_classifications`
- `meta.source_updated_at`
- `meta.generated_at`

## If you use the bundle endpoint instead

The bundle response does not have a top-level `stage_results` field.

Find the matching item in `stages[]` and read:

- `classification_rows`

Then filter to:

- `classification_type === "stage"`

That filtered list is the stage result table.

## Expected behavior

- Stage 1 of the Tour can legitimately have no individual stage results if it is a team time trial.
- Completed individual stages should have a non-empty stage-result list.
- If `stage_results` is empty on a completed individual stage, that is a backend data problem, not a UI guess.

## Minimal SPv6 example

```ts
const res = await fetch(`/api/v1/cycling/tour-de-france/2026/stages/6`);
const data = await res.json();

const stageResults = data.stage_results ?? [];
const overall = data.overall_classifications ?? {};
```

If you are using the bundle route:

```ts
const bundle = await fetch(`/api/v1/cycling/tour-de-france/2026`).then(r => r.json());
const stage = bundle.stages.find((s) => s.stage.stage_number === 6);
const stageResults = (stage?.classification_rows ?? []).filter(
  (row) => row.classification_type === "stage"
);
```

## Practical rule

For SPv6 stage pages, prefer the stage-specific endpoint first. It is the smallest and clearest contract for showing:

- stage winner
- full stage results
- overall classifications beneath the stage table

