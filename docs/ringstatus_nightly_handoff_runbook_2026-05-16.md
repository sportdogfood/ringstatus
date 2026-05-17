# RingStatus Nightly Handoff Runbook - 2026-05-16

**Status:** Active operational handoff
**Purpose:** Prevent repeated DAY/NIGHT transition drift before touching code or live Airtable data.

## Hard Gate

Before answering, diagnosing, or editing RingStatus nightly workflow issues, load these files first:

1. `docs/ringstatus_show_scope_shift_2026-05-16.md`
2. `docs/ringstatus_pipeline_scope_2026-05-08.md`
3. `docs/ringstatus_daily_scope_2026-05-09.md`

No workflow action should proceed from memory, day names, endpoint defaults, or stale heartbeat assumptions.

## Absolute Rule

Do not invent or substitute:

- dates
- show IDs
- customer IDs
- endpoints
- Airtable field names
- payload values
- class counts
- trip counts
- times

If a value is not found in live Airtable, source payloads, local files, or scope docs, keep it unknown and stop the affected write path.

## Required First Checks

Run these checks before code changes or live writes:

1. `git diff --name-only`
2. Confirm the focused `show` record:
   - `show_id`
   - `customer_id`
   - `focus_day`
   - `heartbeat`
   - `shifted_to_next_day`
   - `set_to_default_app_sql_date`
   - `mode_control`
   - `show_scope_key`
3. Confirm latest heartbeat copied scope:
   - `app_show_id`
   - `app_sql_date`
   - `customer_id`
   - `focus_day`
   - `show_scope_key`
   - `mode`
   - `shifted_to_next_day`
4. Confirm the target date using exact date fields, not day names:
   - use `2026-05-17 (app_dow_raw=Sun)`, not just `Sunday`
5. Confirm `watch_schedule` scoped row counts for the exact focused date.
6. Confirm `watch_trips` scoped row counts for the exact focused date.
7. Confirm stale prior-date active rows separately.

## Current Incident Snapshot

Verified on 2026-05-16 after the 2026-05-17 trip refresh:

- latest heartbeat scope: `show_id=200000006`, `customer_id=10002`, `app_sql_date=2026-05-17`, `focus_day=2026-05-17`
- `trips_dailyv2.js` live run created `24` `watch_trips` rows for `2026-05-17`
- live run wrote `created=24`, `updated=0`, `dropped=0`
- independent Airtable check found:
  - `watch_trips` show `200000006`: `58` total rows
  - `2026-05-16`: `34`
  - `2026-05-17`: `24`
  - `2026-05-17` missing `customer_id`, `focus_day`, `show_scope_key`, `heartbeat`, or `watch_schedule`: `0`

After the `watch_trips` heartbeat view filter was adjusted, these stale `2026-05-16` rows became visible in the view but remained active:

```text
200000006|2026-05-16|1|720|1|8778|1330
200000006|2026-05-16|1|762|1|8778|1337
200000006|2026-05-16|3|313|1|8778|1334
200000006|2026-05-16|1|762|1|8778|1332
```

Direct cause: before the view filter change, those rows were not visible to the writer input set. After the view filter change, the current `2026-05-17` writer still did not mark them inactive because `trips_dailyv2.js` correctly guards against cross-date drops during the focused-date refresh.

## Known Failure Pattern

When focus moves from one date to the next:

- current-date refresh must not destructively update prior-date rows
- prior-date cleanup must not rely on the current-date `trips_dailyv2.js` run
- stale rows from the prior date can remain active if they were hidden by the view during the prior-date cleanup window

This is not fixed by rerunning the current focused date. It requires a scoped prior-date cleanup pass.

## Required Cleanup Rule

Until a dedicated cleanup script exists, stale prior-date `watch_trips` rows must be handled as a separate step:

1. Identify prior focused date explicitly.
2. Query `watch_trips` by exact `show_id`, `customer_id`, and prior date.
3. Compare to the last confirmed keep set for that same prior date.
4. Mark only confirmed stale prior-date rows:
   - `inactive = true`
   - `archive = true`
   - `scope_status = dropped` when the option exists
   - `dropped_at = current run date`
5. Do not run the current focused-date refresh as a substitute for prior-date cleanup.

## Writer Boundary

`trips_dailyv2.js` current-date refresh should:

- use `/people/{pid}?pid={pid}&show_id={show_id}&customer_id={customer_id}`
- use focused `customer_id` from heartbeat/show scope
- use scoped `watch_schedule` as schedule context
- update/create rows only for the focused `app_sql_date`
- avoid cross-date drops

`trips_dailyv2.js` should not be expected to clean stale rows from a prior focused date while the active heartbeat is already on the next focused date.

## Next Code Work

Add a dedicated, explicit stale-scope cleanup lane. It should accept exact inputs:

```text
show_id
customer_id
cleanup_date
```

It should refuse to run if any input is missing. It should dry-run by default and report exact candidate keys before patching Airtable.

## Stop Conditions

Stop before live writes if:

- latest heartbeat `app_sql_date` does not match the intended focused date
- `show.customer_id` is missing
- `watch_trips` or `watch_schedule` views hide records needed for the intended cleanup
- the intended action depends on a day name instead of an exact date
- a field name is assumed from memory and not verified in live schema
- a prior-date cleanup is being attempted through the current-date refresh path
