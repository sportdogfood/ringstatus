# Daily Schedule App Source

This folder is a local, read-only data procurement workspace for the future daily schedule app. It does not create the final nested dataset and does not write to Airtable.

Current dated scope checkpoint:

- `docs/ringstatus_daily_schedule_app_source_scope_2026-05-13_v0.1.md`

Primary lane order:

```text
heartbeat -> show -> rings -> groups -> class-related-start-time -> classes -> entries -> trip-related-go-time -> trips -> horses -> riders
```

Outside lanes:

```text
results, alerts, logs
```

## Source Rules

- `watch_schedule` owns class/group schedule state and class-related start time.
- `watch_trips` owns current trip state and current app-facing `rs_*` fields.
- `trip_logs` owns latest calculator/log evidence and `rs_*` diffs.
- Overlapping fields stay namespaced. Current row values are never silently replaced by log values.
- Heartbeat lookup fields on row tables are not row truth.

## Key Scheme

```text
full_nesting_key = sid|sql_date|ring_number|time|cgid|class_number|class_sequence|pid|entry_number
schedule_key = sid|sql_date|ring_number|class_number|class_sequence
schedule_instance_key = schedule_key|cgid:{class_group_id}
schedule_short = ring_number|class_number|class_sequence
trips_key = sid|sql_date|ring_number|class_number|class_sequence|pid|entry_number
trip_instance_key = trips_key|entry_sequence:{entry_sequence}
trips_short_key = class_number|class_sequence|pid|entry_number
```

`schedule_key` and `trips_key` are stable grouping/identity keys. They intentionally stay compact and do not absorb every tie-breaker.

`schedule_instance_key` and `trip_instance_key` separate imperfect row instances without changing the parent keys. Fallbacks are deterministic and reported:

- schedule instance fallback: `class_group_id`, then `class_groupxclasses_id`, then `estimated_start_time + record_id`
- trip instance fallback: `h_eid`, then `entry_id`, then `record_id`

Duplicate or imperfect rows are validation warnings unless no deterministic instance key can be produced.

## Run

Live read-only Airtable extraction:

```powershell
node .\daily_schedule_app_source\extract_daily_schedule_source.js
```

Required environment:

```powershell
$env:AIRTABLE_TOKEN = "..."
$env:AIRTABLE_BASE_ID = "..."
```

Outputs:

- `samples/latest_daily_schedule_app_source.json`
- `reports/latest_validation_report.json`

Build local feed files from the latest extracted source:

```powershell
node .\daily_schedule_app_source\build_feed_files.js
```

Feed outputs:

- `feed/feed.raw.json`: source lanes preserved, no nesting.
- `feed/feed.indexed.json`: flat `rows` plus `indexed.rider`, `indexed.horse`, `indexed.status`, `indexed.ring`, `indexed.class_type`, and `indexed.group_name_tags`.
- `feed/feed.status.json`: derived Ring/Rider/Horse status shape using existing calculator outputs where available.

Time policy for feed files:

- Show clock strings such as `estimated_start_time`, `rs_start_time`, and `rs_go_time` are preserved as source display-clock values.
- The feed builder does not convert those clock strings with `SHOW_TIME_ZONE_OFFSET`.
- Relative minute fields such as `starts_in_mins` and `go_starts_in_mins` are sourced from calculator `rs_*` values when present.
- `SHOW_TZ` and `SHOW_TIME_ZONE_OFFSET` are included as metadata so downstream endpoint code can decide whether a true timestamp conversion is needed.

Local fixture mode:

```powershell
node .\daily_schedule_app_source\extract_daily_schedule_source.js --fixture .\daily_schedule_app_source\samples\sample_fixture.json --out .\daily_schedule_app_source\samples\sample_daily_schedule_app_source.json --report .\daily_schedule_app_source\reports\sample_validation_report.json
```

## Verify

```powershell
node --test .\daily_schedule_app_source\extract_daily_schedule_source.test.js
node --test .\daily_schedule_app_source\build_feed_files.test.js
node --check .\daily_schedule_app_source\extract_daily_schedule_source.js
node --check .\daily_schedule_app_source\build_feed_files.js
```
