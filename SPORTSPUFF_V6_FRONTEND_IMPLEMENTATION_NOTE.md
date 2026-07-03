# Sportspuff v6 frontend implementation note

This note covers the API contract needed for the next frontend pass:

1. Tour de France / cycling stage winners and live rank updates.
2. World Cup knockout bracket rendering.
3. The minimum fallback behavior when a field is absent.

The API side already exposes the data. SPv6 should treat these fields as the source of truth instead of deriving them locally.

## 1. Cycling: stage winners and live rank updates

### Primary endpoints

- `GET /api/v1/scores/cycling/{date}`
- `GET /api/v1/standings/cycling`
- `GET /api/v1/season-info/cycling`

If SPv6 already uses the canonical `/v1` routes, the same shapes are available there as well.

### What to render from scores

For each cycling entry in the scores payload, use:

- `cycling_race`
- `cycling_stage_label`
- `cycling_stage_number`
- `cycling_event_label`
- `cycling_winner`
- `cycling_rank`
- `game_status`
- `game_date`
- `game_time`
- `source_updated_at` in `meta`

Recommended frontend behavior:

- Show the race name as the header, e.g. `Tour de France`.
- Show the stage label when present, e.g. `Stage 1`.
- Show `cycling_winner` when a stage is final or a winner is known.
- Show `cycling_rank` when present. This is the individual stage ranking or GC placement surfaced by the API.
- Use `meta.source_updated_at` or `meta.fetched_at` to label freshness if you show a timestamp.

### What to render from standings

Use `/api/v1/standings/cycling` for the GC table. The response contains:

- `teams[].rank`
- `teams[].team_name`
- `teams[].points`
- `teams[].games_back`
- `teams[].cycling_rank` when available

This endpoint is the right place for the GC board or leaderboard widget.

### Season info behavior

`/api/v1/season-info/cycling` now returns:

- `year`
- `current_phase`
- `season_types`

The `current_phase` can be:

- the active race name, such as `Tour de France`
- `Upcoming`
- `Off Season`

Frontend guidance:

- When `current_phase` is a race name, treat that as the active headline.
- When it is `Upcoming`, show the next race banner instead of `Off Season`.
- Poll cycling scores during race day. For live coverage, refresh more frequently while the stage is `in progress` or `live`.

### Suggested UI split

- Race header: `current_phase`
- Stage card: `cycling_stage_label`, `game_status`, `cycling_winner`
- Live leaderboard: `/api/v1/standings/cycling`

## 2. World Cup: knockout bracket

### Primary endpoints

- `GET /api/v1/season-info/wc`
- `GET /api/v1/world-cup/bracket`
- `GET /api/v1/standings/wc`
- `GET /api/v1/scores/wc/{date}`

### Bracket payload

The bracket payload is a lattice, not a flat list. The key shape is:

- `knockout_bracket.format`
- `knockout_bracket.sides.left`
- `knockout_bracket.sides.right`
- `knockout_bracket.rounds`
- `rounds[0].matches`

Each match entry may contain:

- `match_number`
- `home_slot`
- `away_slot`
- `home_team`
- `away_team`
- `game_id`
- `game_date`
- `game_time`
- `game_status`
- `winner`

### Recommended rendering approach

- Use `season-info/wc.knockout_bracket` when present.
- Fall back to `/api/v1/world-cup/bracket` if you want the bracket independently of season info.
- Render the bracket in rounds, but keep the side-based structure so the left and right halves line up with the official lattice.
- If a match has a known `winner`, highlight that team and collapse the visual state accordingly.

### World Cup scores

Scores entries now include:

- `wc_round`
- `wc_round_label`
- `wc_winner`

Use `wc_winner` for knockout matches once final, including shootout-decided results.

## 3. Fallback rules

SPv6 should be resilient when fields are missing:

- If `cycling_winner` is empty, do not fabricate a winner from team names.
- If `cycling_rank` is empty, hide the rank badge instead of showing `0`.
- If `knockout_bracket` is missing, render the group-stage or summary view only.
- If `winner` is missing from a World Cup match, render the match as pending.
- If a scores payload is empty but `meta.empty_state` is `real_empty`, treat that as a valid no-results state.

## 4. Suggested polling cadence

- Cycling race day: every 2 to 5 minutes.
- World Cup knockout stage: every 5 minutes, faster only while a match is live.
- GC table: every 10 to 15 minutes unless the user is actively viewing the leaderboard.

## 5. Minimal integration order

If SPv6 wants the fastest path:

1. Add `cycling_winner` and `cycling_rank` to the cycling score cards.
2. Wire `/api/v1/standings/cycling` into the GC board.
3. Render `knockout_bracket` from `season-info/wc`, with `/api/v1/world-cup/bracket` as fallback.
4. Add `wc_winner` display for final World Cup matches.

## 6. Notes for implementation

- The backend already carries these fields through the legacy `/api/v1` routes.
- No frontend-side inference is required for winners or bracket layout.
- Prefer the API-provided `meta` timestamps when showing freshness or auto-refresh state.

## 7. World Cup bracket renderer addendum

The backend does not return a pre-rendered bracket image. SPv6 should render the bracket from the lattice data.

### Core visual structure

- Treat `knockout_bracket.sides.left` and `knockout_bracket.sides.right` as the two halves of the bracket.
- Treat `rounds[].matches` as the canonical match list.
- Render one column per round:
  - Round of 32
  - Round of 16
  - Quarter-finals
  - Semi-finals
  - Final
- Draw connector lines between adjacent round columns so the visual reads like a tournament tree.

### Box content

Each match box should show:

- Match label or round label
- Date and venue if available
- Home team
- Away team
- Winner highlight if `winner` is present

If a side is still unknown, keep the slot visible as an empty placeholder so the bracket geometry stays intact.

### Data mapping

Use the following fields in order of priority:

- Team names: `home_team` and `away_team`
- Match timing: `game_date` and `game_time`
- Live state: `game_status`
- Winner: `winner`
- Slot identifiers: `home_slot` and `away_slot`
- Match identity: `match_number`

### Layout behavior

- Keep the left and right halves mirrored.
- Preserve vertical spacing even when some matches are not yet scheduled.
- For completed matches, visually promote the winner and dim the loser.
- For pending matches, use the slot names as placeholders if actual teams are not known yet.

### Mobile behavior

- On small screens, collapse the bracket into a vertical stack by round.
- Keep each round expandable so users can inspect the tree without horizontal scrolling if possible.
- If SPv6 cannot support a full mobile bracket immediately, horizontal scroll is acceptable as a first pass, but the round grouping should still be explicit.

### Suggested component breakdown

- `WorldCupBracket`
- `BracketRound`
- `BracketMatch`
- `BracketConnector`

That split keeps the geometry logic separate from the match card rendering and makes it easier to swap in updated data without rewriting the layout.
