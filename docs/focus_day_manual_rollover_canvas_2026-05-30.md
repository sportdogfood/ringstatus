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
2026-05-30 rows -> is_current_scope unchecked
```

`dropped_at` should only be set when a `2026-05-31` row that previously existed is missing from the fresh `2026-05-31` payload.

Rows should not be deleted.

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
