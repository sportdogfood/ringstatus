# RingStatus Daily Schedule App Source Scope - 2026-05-13 v0.1

**Date:** 2026-05-13  
**Version:** v0.1  
**Status:** paused scope checkpoint  
**Workspace:** `daily_schedule_app_source`  
**Boundary:** local files only; no branch, no Airtable schema changes, no active lane changes

## Purpose

This scope captures the current local daily schedule app source work before adding visual identifiers or wiring them into the rendered schedule.

The current goal is to produce reviewable local source/feed artifacts for a future schedule app. The final nested dataset and flyup behavior are intentionally not built yet.

## Current Deliverables

Local source folder:

```text
daily_schedule_app_source/
```

Current generated outputs:

- `samples/latest_daily_schedule_app_source.json`
- `reports/latest_validation_report.json`
- `render/schedule_preview.html`
- `render/schedule_preview_model.json`
- `feed/feed.raw.json`
- `feed/feed.indexed.json`
- `feed/feed.status.json`

Current feed split:

- `raw`: flat source lanes preserved.
- `indexed`: flat rows plus lookup indexes for `rider`, `horse`, `status`, `ring`, `class_type`, and `group_name_tags`.
- `status`: derived Ring/Rider/Horse status shape using existing calculator outputs where available.

## Source Lane Contract

Primary lane order remains:

```text
heartbeat -> show -> rings -> groups -> class-related-start-time -> classes -> entries -> trip-related-go-time -> trips -> horses -> riders
```

Outside lanes remain:

```text
results, alerts, logs
```

`watch_schedule` owns class/group schedule state and class-related start time.

`watch_trips` owns current trip state and current app-facing `rs_*` values.

`schedule_logs` and `trip_logs` supply calculator/log evidence under separate namespaces. They do not silently replace current row values.

## Key Contract

Use the existing key scheme exactly:

```text
full_nesting_key = sid|sql_date|ring_number|time|cgid|class_number|class_sequence|pid|entry_number
schedule_key = sid|sql_date|ring_number|class_number|class_sequence
schedule_instance_key = schedule_key|cgid:{class_group_id}
trips_key = sid|sql_date|ring_number|class_number|class_sequence|pid|entry_number
trip_instance_key = trips_key|entry_sequence:{entry_sequence}
```

`entry_sequence` may be used as an instance/tie-breaker, but it is not added to `trips_key`.

Duplicate or imperfect rows are warnings unless no deterministic instance key can be produced.

## Render Boundary

The current schedule preview is for review only:

```text
daily_schedule_app_source/render/schedule_preview.html
```

The app-wide `ring-collection` structure is:

```text
ring-collection
  Ring Eyebrow
    opens ring_detail later, not wired yet
  ring
    group
      classes
        this_class + time
          tenant (pid)
            entries
              this_trip + time
              horses
              riders
              results
```

Do not wire `ring_detail` or flyups until flyup content is explicitly selected.

## Time Policy

Do not calculate dates or times inside Airtable for this phase.

Show clock strings from source rows are preserved as display-clock values. Examples:

```text
estimated_start_time
estimated_go_time
rs_start_time
rs_go_time
```

Do not convert these clock strings with `SHOW_TIME_ZONE_OFFSET` unless the source is explicitly a UTC/epoch timestamp.

The current local feed metadata records:

```text
SHOW_TZ = America/New_York
SHOW_TIME_ZONE_OFFSET = -240
```

Relative minute fields such as `starts_in_mins` and `go_starts_in_mins` should come from existing calculator `rs_*` outputs where present.

## Calculator Reuse

Do not duplicate calculator logic in the visual/app-source layer when `schedules_calculatorv2.js`, `trips_calculatorv2.js`, or `trips_calculator.js` already calculates the value.

Known reusable calculator outputs include:

- class start/end: `rs_start_time`, `rs_end_time`
- class progress: `rs_completed_trips`, `rs_total_trips`
- class relative timing: `rs_mins_till_start`, `rs_mins_since_start`
- trip go time: `rs_go_time`
- trip relative timing: `rs_min_till_go`
- order/go position: `rs_order_of_go`, `rs_running_order_of_go`, `rs_running_order_of_go_mins_till`

`watch_trips.rs_*` remains current row state. Latest `trip_logs.rs_*` remains log evidence.

## Endpoint Scope

The agreed endpoint/file split is:

1. `raw`
2. `indexed`
3. `status` / `derived`

Do not collapse raw and derived payloads. The app needs both source truth and app-facing status helpers.

## Visual Identifier Scope

Visual identifiers are not wired into the schedule render yet. They are the next design artifact.

The first identifier group is status language normalization because several endpoints use different terms for the same idea.

Canonical compact status vocabulary:

```text
NOW   NEXT   FOL   UPC   DONE
```

Known incoming terms to normalize:

```text
Now, Next, Following
Not Started, Upcoming, Underway, Completed
Coming, Now, Done
upcoming, livenow, completed
```

The visual system should support short, precise, small-screen identifiers:

- compact all-caps labels
- shade/fill/outline differences
- badge or pill representation
- muted tags for secondary metadata
- no dependency on long labels such as `Completed` in dense row views

Gold-standard visual direction:

- dense dark interface
- compact rows
- bold primary identifier on the left
- muted supporting label below
- compact status/metric tokens on the right
- thin row dividers
- small pills similar to OpenAI/ChatGPT task status pills
- no decorative cards or speculative flyup UI

Entities requiring visual identifiers:

```text
Ring
Group
Class
Class_Time
Pid / Tenant / Trainer
Entry
Trip
Trip_Time
Horse
Rider
Results
Groom
```

Other identifier groups to cover next:

```text
record = Active, Inactive
group_tags
class_type = Equitation, Hunters, Jumpers
is_first_up, is_handy, is_warmup, is_mulligan, is_schooling_pony, is_add_back, is_classic, is_usf
is_target, not_target
class_sequences = 1, 2, 3, 4, ...
entry_sequences = 0, 1, 2, 5, 9, ...
schedule_sequencetype = Over Fences, Under Saddle/Flat
```

## Current Verification

Latest local verification command:

```powershell
node --test .\daily_schedule_app_source\*.test.js
```

Observed result:

```text
25 tests passed
0 failed
```

## Next Step

Stop implementation here.

Next work should create the visual identifier contract and preview in a local artifact before applying it to the schedule render.

The visual identifier work should start with status normalization and compact token treatments, then move through entity, tag, class type, target, sequence, and schedule sequence identifiers.
