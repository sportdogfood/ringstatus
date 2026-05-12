# Daily Schedule App Source

This folder is a local, read-only data procurement workspace for the future daily schedule app. It does not create the final nested dataset and does not write to Airtable.

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
schedule_short = ring_number|class_number|class_sequence
trips_key = sid|sql_date|ring_number|class_number|class_sequence|pid|entry_number
trips_short_key = class_number|class_sequence|pid|entry_number
```

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

Local fixture mode:

```powershell
node .\daily_schedule_app_source\extract_daily_schedule_source.js --fixture .\daily_schedule_app_source\samples\sample_fixture.json --out .\daily_schedule_app_source\samples\sample_daily_schedule_app_source.json --report .\daily_schedule_app_source\reports\sample_validation_report.json
```

## Verify

```powershell
node --test .\daily_schedule_app_source\extract_daily_schedule_source.test.js
node --check .\daily_schedule_app_source\extract_daily_schedule_source.js
```
