# Focus Day Manual Rollover Canvas - 2026-05-30 17:00 to 2026-05-31 DAY

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

## Verified Code Expectations

Reviewed on 2026-05-31 after the morning DAY session started.

```text
heartbeat date source:
  schedules_dailyv2.js showHeartbeatTargetDate() sets targetDate = show.focus_day
  trips_dailyv2.js showHeartbeatTargetDate() sets targetDate = show.focus_day
  shifted_to_next_day is retained as metadata, not used to advance the date

heartbeat slot/cadence:
  heartbeat_patterns.js rotates isA/isB/isC/isD only
  heartbeat_slot_orchestrator.js uses mode for cadence/runners
  heartbeat_slot_orchestrator.js does not compute focus_day from NIGHT or OVERNIGHT

schedule runner:
  schedules_dailyv2.js builds the dated schedule endpoint from scope.app_sql_datev2
  show heartbeat scope sets app_sql_date_source=show_heartbeat_target
  dated schedule manual fallback is allowed when the live schedule payload is stripped
  classes endpoint remains skipped as unreliable for schedule lane
  active_groups remains skipped as active_tables_deprecated

trips runner:
  trips_dailyv2.js resolves show heartbeat rows independently
  trips_dailyv2.js writes is_current_scope based on resolved trip scheduled date matching heartbeat.app_sql_date
  active_groups, active_classes, active_entries, and active links remain skipped as active_tables_deprecated

live runners:
  live_groups_daily.js is DAY-only
  live_class_detail.js is DAY-only
  live_groups_daily.js now clears stale live_groups scope for same show/customer older focus days
  live_groups_daily.js now scopes live_group_changes by focus_day and is_cuurent_scope
  live_class_detail.js now scopes live_classes by focus_day and is_cuurent_scope
```

Code still contains legacy/fallback branches, but the expected path is:

```text
show.focus_day -> heartbeat app_sql_date -> schedule/trips/live scope
```

Legacy/fallback branches must not replace that path.

## Observed Rollover Timeline

### 2026-05-30 17:00 through manual focus_day change

Expected state before manual change:

```text
mode may become NIGHT
shifted_to_next_day may be true/false
show.focus_day remains the only date source
heartbeat date must not move unless show.focus_day changes
```

Manual action:

```text
show.focus_day changed to 2026-05-31
show_id=200000063
customer_id=15
```

Expected first successful result:

```text
heartbeat.app_sql_date=2026-05-31
heartbeat.show_scope_key=15|200000063|2026-05-31
schedules_dailyv2.js pings date=2026-05-31
trips_dailyv2.js processes app_sql_date=2026-05-31
```

### 2026-05-31 06:49 DAY session

Session started with:

```text
it is 6:49A 2026-05-31 DAY
```

Verified runner state:

```text
heartbeat mode=DAY
heartbeat app_sql_date=2026-05-31
heartbeat show_scope_key=15|200000063|2026-05-31
live_groups_daily.js firing
live_class_detail.js firing
publisher firing after upstream lanes
```

Observed live runner behavior:

```text
live_groups_daily:
  rows=40
  watch_schedule_matches=69
  watch_trips_matches=39
  estimated_start_time available on all 40 live_groups rows

live_class_detail:
  has_json view pings on A/C
  is_live view pings on A/B/C/D when rows exist
  current is_live rows=0 during observed checks
```

## Current Airtable State Snapshot

Verified after morning fixes on 2026-05-31:

```text
watch_schedule:
  2026-05-31 current rows=77
  older checked current rows=0
  current rows with estimated_start_time=77

watch_trips:
  2026-05-31 current rows=40
  older checked current rows=0
  current rows with estimated_start_time=40

live_groups:
  2026-05-31 current rows=40
  older checked current rows=0
  current rows with estimated_start_time=40

live_group_changes:
  2026-05-31 checked rows=12
  older checked rows=0

live_classes:
  2026-05-31 checked rows=46
  older checked rows=0
```

Estimated start time propagation verified:

```text
watch_schedule linked to live_groups:
  checked=69
  matched live_groups.estimated_start_time=69
  mismatches=0

watch_trips linked to live_groups:
  checked=39
  matched live_groups.estimated_start_time=39
  mismatches=0
```

## Confirmed Morning Fixes

These were found after the 2026-05-31 DAY session began and are now fixed:

```text
live_groups stale scope:
  prior issue: 2026-05-29 and 2026-05-30 live_groups rows stayed is_cuurent_scope checked
  fix: clear same show/customer live_groups rows where live_focus_day/day != current focus_day
  verified: older checked rows=0

live_group_changes scope:
  prior issue: log rows did not carry current-scope state
  fix: write focus_day and is_cuurent_scope=true on new rows; check current focus_day rows and uncheck older rows
  verified: older checked rows=0

live_classes scope:
  prior issue: log rows did not carry current-scope state
  fix: write focus_day and is_cuurent_scope=true on new rows; check current focus_day rows and uncheck older rows
  verified: older checked rows=0

live estimated_start_time propagation:
  prior concern: confirm live updates both watch tables
  fix/check: live_groups_daily propagation writes estimated_start_time to watch_schedule and watch_trips unless manual_time_override is checked
  verified: watch_schedule mismatches=0 and watch_trips mismatches=0

watch table scope flags:
  observed 2026-05-31: watch_schedule had retained older same-show rows stamped with old focus_day, and some closed rows had stale is_target=true
  observed 2026-05-31: watch_trips had retained closed rows stamped with old focus_day
  fix: schedules_dailyv2.js now runs a post-write scope sync over same-show watch_schedule rows
  fix: trips_dailyv2.js now runs a post-write scope sync over same-show watch_trips rows
  rule: focus_day is stamped to the active show focus day; only open rows whose scheduled date equals focus_day are is_current_scope=true
  rule: watch_schedule is_target follows the same open scheduled-date check as is_current_scope
  verified after live run: watch_schedule bad scope rows=0; watch_trips bad scope rows=0
```

## Full Workflow From 17:00 to DAY

Use this as the beginning-to-end checklist for the next rollover:

```text
1. Before 17:00
   show.focus_day is today's publish date.
   watch_schedule/watch_trips current scope should match show.focus_day.

2. At/after 17:00
   mode may move to NIGHT.
   Do not expect date movement from code.
   show.focus_day remains the date source.

3. Manual rollover
   Change show.focus_day to the next intended publish date.
   This is the only required date action.

4. First heartbeat after manual rollover
   Confirm heartbeat.app_sql_date = show.focus_day.
   Confirm heartbeat.show_scope_key = customer_id|show_id|show.focus_day.
   Confirm source remains show_heartbeat_target where visible.

5. Schedule lane
   schedules_dailyv2.js must write focus-day rows.
   It must then clear old same-show current-scope rows.
   It must stamp all same-show rows with the current focus_day.
   It must set is_current_scope/is_target only on open rows dated focus_day.
   It must not clear/delete rows before current rows exist.

6. Trips lane
   trips_dailyv2.js must process each active tenant independently.
   No-trips tenants write automation_errs and do not block other tenants.
   Non-focus show-window trips are retained but not current.
   It must stamp all same-show rows with the current focus_day.
   It must set is_current_scope only on open rows dated focus_day.

7. Publisher
   Publisher should run only after upstream due lanes succeed.
   Published current data should reflect the current focus day.

8. DAY/live lane
   live_groups_daily.js runs in DAY.
   live_groups updates live_groups, watch_schedule, and watch_trips.
   live_class_detail.js runs in DAY.
   has_json pings on due slots; is_live pings only when the is_live view has rows.

9. Scope log lanes
   live_groups, live_group_changes, and live_classes must show only current focus_day rows checked.
   Older focus days remain retained but unchecked/dropped as appropriate.

10. Final verification
   watch_schedule current rows > 0
   watch_trips current rows > 0
   older checked current rows = 0
   live estimated_start_time matches both watch tables
```

## Remaining Audit Notes

These are not current blockers, but they are known surfaces to keep watching:

```text
schedules_dailyv2.js still contains fallback/current-heartbeat resolver branches.
Expected behavior is safe only when show_heartbeat_target remains the selected source.

schedules_dailyv2.js still contains groups_live fallback.
Current observed result: groups_live rows=0.

schedules_dailyv2.js still contains forward schedule write machinery.
Current observed result for 2026-05-31 end date: dates_seen=0, rows_written=0.

live_class_detail is healthy but only is_live-pings when live_groups/is_live view has rows.
Current observed is_live rows=0.
```
