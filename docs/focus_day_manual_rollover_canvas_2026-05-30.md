# Focus Day Manual Rollover Canvas - 2026-05-30

## Locked Rule

`show.focus_day` is the heartbeat date source of truth.

`mode`, `NIGHT`, `OVERNIGHT`, endpoint clock values, and `shifted_to_next_day` do not choose or advance the heartbeat date. They are cadence or metadata only.

## Manual Change

When `show.focus_day` is manually changed to:

```text
2026-05-31
```

the next heartbeat/run should use `2026-05-31` as the selected date.

## NIGHT to OVERNIGHT

When `NIGHT` transitions to `OVERNIGHT`, the heartbeat mode/cadence may change, but the heartbeat date must not move by code.

Expected behavior:

```text
show.focus_day remains the date source of truth
heartbeat.app_sql_date stays equal to show.focus_day
app_sql_date_source stays show_heartbeat_target
shifted_to_next_day remains metadata only
trips_tagger does not apply NIGHT + 1
heartbeat_slot_orchestrator does not change trips cadence from shifted_to_next_day
```

If `show.focus_day` is still:

```text
2026-05-30
```

then after OVERNIGHT starts, heartbeat should still show:

```text
focus_day=2026-05-30
app_sql_date=2026-05-30
app_sql_date_source=show_heartbeat_target
```

The date only moves when `show.focus_day` is manually changed.

## Expected Heartbeat Identity

```text
show_id=200000063
customer_id=15
show_date=2026-05-31
sql_date=2026-05-31
app_show_id=200000063
app_sql_date=2026-05-31
show_scope_key=15|200000063|2026-05-31
```

## Expected Schedule Runner

`schedules_dailyv2.js` should resolve:

```text
app_show_idv2=200000063
app_sql_datev2=2026-05-31
focus_day=2026-05-31
app_sql_date_source=show_heartbeat_target
```

`mode` may be `NIGHT`, but it must not alter the selected date.

It should ping:

```text
https://sglapi.wellingtoninternational.com/schedule?date=2026-05-31&show_id=200000063&customer_id=15
```

It should not ping backward to:

```text
2026-05-30
```

Because `2026-05-31` is the show end date, forward schedule cache should be empty:

```text
schedule_payload_cache.forward=[]
forward_schedule_writes.dates_seen=0
```

## Expected Watch Schedule Scope

```text
2026-05-31 rows -> is_current_scope checked
2026-05-31 rows -> is_target checked
2026-05-30 rows -> is_current_scope unchecked
2026-05-30 rows -> is_target unchecked
```

`dropped_at` should only be set when a `2026-05-31` row that previously existed is missing from the fresh `2026-05-31` payload.

Rows should not be deleted.

Write order must be:

```text
1. create any new rows for show.focus_day
2. update existing rows for show.focus_day
3. clear old same-show rows that are no longer current scope
4. mark true missing focus-day rows as dropped
```

Old rows must not be cleared before the replacement/current rows have been written.

## Expected Trips Runner

`trips_dailyv2.js` should resolve:

```text
app_show_id=200000063
app_sql_date=2026-05-31
selected_target_date=2026-05-31
```

It should fetch each active tenant independently:

```text
https://sglapi.wellingtoninternational.com/people/8778?pid=8778&show_id=200000063&customer_id=15
https://sglapi.wellingtoninternational.com/people/19676?pid=19676&show_id=200000063&customer_id=15
```

## Expected Watch Trips Scope

```text
Trips scheduled 2026-05-31 -> is_current_scope checked
Trips scheduled 2026-05-30 or other show-window dates -> retained, is_current_scope unchecked
```

If a tenant has no trips:

```text
write no_trips audit to automation_errs
do not create watch_trips rows for that tenant
continue processing other tenants
```

## Verification Points After Rollover

Check the next logs for:

```text
schedules_dailyv2.js:
  app_sql_datev2=2026-05-31
  focus_day=2026-05-31
  schedule date=2026-05-31
  no forward date 2026-05-30

trips_dailyv2.js:
  app_sql_date=2026-05-31
  selected_target_date=2026-05-31
  8778 fetched independently
  19676 fetched independently
```

## Next Workflow Steps

1. Verify the next heartbeat row after a manual `show.focus_day` change has:

```text
app_sql_date=show.focus_day
app_sql_date_source=show_heartbeat_target
show_scope_key=customer_id|show_id|show.focus_day
```

2. Verify `schedules_dailyv2.js` writes the selected focus-day rows before clearing old current-scope rows.

3. Verify the Airtable schedules view shows only the selected focus-day rows when filtered by:

```text
dropped_at is empty
is_current_scope is checked
inactive is unchecked
archive is unchecked
is_target is checked
```

4. Verify no deprecated active table can block the workflow:

```text
active_groups skipped with reason=active_tables_deprecated
active_classes not used
active_entries not used
```

## Known Errors To Confirm Fixed

These are blockers for the next rollover workflow. Confirm each item before running another manual rollover.

```text
ERROR: heartbeat date moved by NIGHT, OVERNIGHT, endpoint clock, or shifted_to_next_day
FIXED WHEN: app_sql_date always equals show.focus_day and source is show_heartbeat_target

ERROR: schedules_dailyv2.js selected or pinged a prior date after focus_day changed
FIXED WHEN: schedule endpoint date equals show.focus_day

ERROR: old watch_schedule rows stayed is_current_scope checked after rollover
FIXED WHEN: same-show non-focus rows are is_current_scope unchecked and heartbeat link cleared

ERROR: current watch_schedule rows were created but hidden from TODAY view because is_target was blank
FIXED WHEN: focus-day rows are is_current_scope checked and is_target checked

ERROR: rows were cleared before fresh focus-day rows were created/updated
FIXED WHEN: logs show creates/updates complete before current_scope_cleared

ERROR: active_groups, active_classes, or active_entries blocked or shaped schedule output
FIXED WHEN: they are skipped/not used and never determine watch_schedule scope
```

## Required Preflight Before Next Rollover

Before changing `show.focus_day` again, confirm:

```text
1. latest heartbeat row uses show.focus_day
2. latest heartbeat row has app_sql_date_source=show_heartbeat_target
3. schedules_dailyv2.js writes focus-day rows with is_current_scope=true
4. schedules_dailyv2.js writes focus-day rows with is_target=true
5. schedules_dailyv2.js clears same-show non-focus rows with is_current_scope=false and is_target=false
6. fresh focus-day creates/updates run before current-scope clears
7. Airtable TODAY watch_schedule view returns rows under the standard filter
```

## Known Troubleshooting Check

If the TODAY `watch_schedule` view appears empty after rollover, first remove only the `is_target` filter.

Expected interpretation:

```text
Rows appear after removing is_target:
  records exist, but is_target is blank/false or the filter is excluding them

Rows still do not appear after removing is_target:
  continue tracing heartbeat scope, schedule runner execution, and schedule row writes
```

This check separates missing records from records hidden by view filters.
