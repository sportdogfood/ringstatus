# RingStatus Daily Log - 2026-05-28

**Status:** Live transition incident  
**Primary failure:** No smooth `DAY -> NIGHT` transition. The focused show/date was manually set, but the runner still carried conflicting logic and fields.

## What Failed

The intended operating rule was already known:

```text
show table -> heartbeat view -> use that row's show_id, customer_id, and focus_day
```

The code did not fully honor that rule.

### Failure 1 - Heartbeat View Row Was Rejected

The `show` table `heartbeat` view had one row:

```text
show_id=200000063
customer_id=15
focus_day=2026-05-29
start_date=2026-05-29
end_date=2026-05-31
heartbeat=true
```

Instead of accepting that row as the focused scope, both local runner resolution and tagger still applied an additional active-window rule:

```text
today inside start_date/end_date
OR shifted_to_next_day=true + mode_control=NIGHT + focus_day=tomorrow
```

That caused the one manually selected heartbeat row to resolve as `no-active-feeds` when `mode_control` was blank.

Files fixed:

- `tagger.js`
- `runner_pipeline_common.ps1`
- `tests/show_active_scope_contract.test.js`

Current rule after fix:

```text
If a row is present in show.heartbeat and has show_id, customer_id, and focus_day, use it.
Do not require mode_control, shifted_to_next_day, today/tomorrow, or endpoint agreement.
```

### Failure 2 - Heartbeat Primary Fields Still Used Endpoint Clock

After the selector was fixed, the heartbeat row still wrote raw endpoint clock values into primary fields:

```text
show_id=200000062
sql_date=2026-05-28
app_show_id=200000063
app_sql_date=2026-05-29
```

That created a second conflicting identity inside the same heartbeat row.

Files fixed:

- `tagger.js`
- `tests/show_scope_contract.test.js`

Current rule after fix:

```text
heartbeat.show_id = show.heartbeat.show_id
heartbeat.show_date = show.heartbeat.focus_day
heartbeat.sql_date = show.heartbeat.focus_day
heartbeat.app_show_id = show.heartbeat.show_id
heartbeat.app_sql_date = show.heartbeat.focus_day
```

Raw endpoint clock can still be used for cadence/mode timing, but it must not override the focused show/date identity.

## Live Proof After Fix

The 2026-05-28 17:35 heartbeat wrote:

```text
show_id=200000063
show_date=2026-05-29
sql_date=2026-05-29
app_show_id=200000063
app_sql_date=2026-05-29
show_scope_key=15|200000063|2026-05-29
heartbeat_id=200000063-2026-05-29-1780004105
```

## Tests Added Or Updated

```text
node .\tests\show_active_scope_contract.test.js
node .\tests\show_scope_contract.test.js
```

These tests now explicitly guard that:

- `show.heartbeat` view membership is the focused-show selector.
- Tagger must not require `mode_control=NIGHT` to accept a heartbeat-view row.
- Runner must not apply the shifted NIGHT/tomorrow filter before accepting a heartbeat-view row.
- Heartbeat primary `show_id` and `sql_date` must come from focused `show.heartbeat` scope before endpoint clock fallback.

## First Checks Tomorrow

Start with these checks before changing code:

1. Check `show` table, `heartbeat` view.
   Expected: only the intended active focused show rows are visible.

2. Check latest `heartbeat` row.
   Expected primary fields must match `show.heartbeat`:

```text
show_id == focused show.show_id
sql_date == focused show.focus_day
show_date == focused show.focus_day
app_show_id == focused show.show_id
app_sql_date == focused show.focus_day
show_scope_key == customer_id|show_id|focus_day
```

3. If heartbeat is correct but schedule/trips do not move, inspect only the downstream lane that failed.
   Do not add another heartbeat selector rule.

4. If endpoint raw values disagree with focused `show.heartbeat`, do not treat that as a conflict.
   The focused `show.heartbeat` row wins for scope identity.

## Airtable Review Filters

### `watch_schedule`

For active schedule review, use all of these filters together:

```text
is_current_scope = checked
archive = unchecked
inactive = unchecked
dropped_at = empty
```

Do not use only:

```text
is_current_scope = checked
dropped_at = empty
```

Reason: archived historical rows can still have `is_current_scope` checked. On 2026-05-28, that incomplete filter returned 68 rows:

```text
55 rows = current focused show 200000063 / 2026-05-29
13 rows = archived 200000062 / 2026-05-28 rows
```

Adding `archive = unchecked` and `inactive = unchecked` returned the expected 55 active `watch_schedule` rows.

### `watch_trips`

For active trip review, use all of these filters together:

```text
is_current_scope = checked
archive = unchecked
inactive = unchecked
dropped_at = empty
```

Do not use only:

```text
is_current_scope = checked
dropped_at = empty
```

Reason: archived historical rows can still have `is_current_scope` checked. On 2026-05-28, that incomplete filter returned 106 rows:

```text
27 rows = current focused show 200000063 / 2026-05-29
34 rows = archived 200000006 / 2026-05-16 rows
24 rows = archived 200000006 / 2026-05-17 rows
21 rows = archived 200000062 / 2026-05-28 rows
```

Adding `archive = unchecked` and `inactive = unchecked` returned the expected 27 active `watch_trips` rows.

## Explicit Do Not Repeat

Do not solve tomorrow's transition by adding a new gate around:

- `tomorrow`
- `mode_control`
- `shifted_to_next_day`
- endpoint `show_id`
- endpoint `sql_date`
- legacy `shows`

The source of truth is already the `show` table `heartbeat` view.
