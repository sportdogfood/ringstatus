# WEC Lane Codex Session Map - 2026-07-08

## Purpose

This document records the Codex session ownership map for the current WEC lane split.

The lane split is:

```text
core build
time-engine + trigger
live
rider-results
publish
```

Each lane should be treated as a separate work surface. A lane may publish state changes that make another lane eligible to wake, but it should not directly own or repair another lane.

Outside support lanes now exist for Core testing and mirror visibility. They do not replace production cadence proof.

Routing and prompt ownership are defined in:

- `docs/horseshowing/ringstatus-scheduling-routing-agent-contract.md`
- `docs/horseshowing/ringstatus-scheduling-specialist-agent-prompt-pack.md`

## Session Map

| Lane | Codex session ID | Notes |
|---|---|---|
| `core-build` | `019f4240-49ce-78e3-b89a-13d3a19cb02b` | This chat. Current core/downstream coordination thread. |
| `live` | `019f3f43-7819-7dd0-bcf6-a4b143c43bd2` | `get_rings` current-state updater lane. |
| `time-engine + trigger` | `019f4321-ba83-7760-959e-298b47af1970` | Time-engine and trigger-readiness lane. |
| `rider-results` | `019f4315-5aab-72b3-8ba5-5aa96c9770cf` | Watched rider business result lane. |
| `alerts` | Not established | Alert eligibility, queue/send state, and delivery logging lane. |
| `publish` | `019f4319-9b1e-7190-ba93-be475cdea80c` | Publish/output lane. |
| `endpoints` | `019f4433-1405-7cb0-b289-581b2203bafe` | Endpoint contracts, aliases, payload shapes, and Webflow/Catalyst route drift. |
| `hot-patch-manual-correction` | Not established | Operator correction, correction protection, Catalyst/Airtable write-back/overlay, and durable bug handoff. |

## Outside Support Lanes

| Lane | Owner | Purpose | Current state |
|---|---|---|---|
| `core-next-day-preflight` | Core Build | Real next-day source acquisition, bounded probe/parse, and runtime projection without production writes. | Contracted in `ringstatus-data/catalyst-workspaces/horseshowing/docs/core_1_4_next_day_preflight_contract.md`; lab script exists as `core_1_4_lab.js`. |
| `stage-4S-sync` | Core visibility sync | Mirror Catalyst Step 4 runtime rows to Airtable for staging review. | Code lane deployed; manual catch-up completed; temporary Codex monitor `wec-stage-4s-mirror-sync-monitor` active every 30 minutes; Catalyst scheduler still missing. |
| `step-3-mirror-sync` | Core visibility sync | Mirror Step 2/3 source and parse rows to Airtable for review. | Existing action `wec-step3-airtable-mirror`; scheduler missing or not confirmed. |

## Standing Headwinds

- Webflow Cloud HTML/style/embed constraints.
- Catalyst/Airtable master table drift.
- Catalyst/Airtable helper table drift.
- Hot patch work that fixes the moment but is not captured as durable correction state.

## Standing Pain

- focus_day and show changes must transition smoothly.
- Data must calculate effectively and efficiently.
- Source calls should be reduced where possible without losing data integrity.
- Airtable manual corrections are necessary and must not be overwritten blindly by the next workflow cycle.

## Lane Boundaries

## Cleaner Target Workflow

The cleaner target workflow is:

```text
Core Build
  update_schedule -> show_no, focus_day, iso_date, ring_day_no, ring_no, class_no, entry_no where available
  update_schedule -> class_start_times
  update_schedule -> ring_status
  update_schedule -> class_oog_raw probe queue
  class_oog_raw -> class_oog
  class_oog -> entry_go_times
  entry_go_times -> watched rider_results lane

Live
  get_rings -> time_engine current state

Time Engine
  updates ring_status, class_start_times, entry_go_times, rider_results eligibility

Alerts
  sources: ring_status, class_start_times, entry_go_times, rider_results

Logs
  append-only: ring_change_logs, class_change_logs, entry_change_logs, result_change_logs, alert_change_logs

Publish
  endpoints read prepared/current tables plus logs
```

This removes `get_ring_days` as a required hot Core input, removes hot `get_orders`, and defers broad class-results machinery unless a later business requirement proves it is needed.

### `core-build`

Writes:

- `hs_update_schedule`
- `hs_class_oog_raw`
- `hs_class_oog`
- `hs_ring_status`
- `hs_class_start_times`
- `hs_entry_go_times`

Defers:

- Stage 4S Airtable mirror sync/backlog for Core runtime tables
- `get_ring_days` compatibility/fallback unless proven still required

Wakes:

- `live` when `focus_show.live_enrichment=true`
- `time-engine + trigger` with `wake_reason=core_runtime_ready` after runtime prep passes

Does not wake directly:

- `publish`

### `live`

Gate:

- `focus_show.live_enrichment=true`

Calls:

- `get_rings.php`

Does not call in the hot path:

- `get_orders.php`, unless a later live-detail lane proves it is required

Writes:

- live fields on runtime tables

Wakes:

- `time-engine + trigger`
- `publish`

### `time-engine + trigger`

Reads:

- `hs_ring_status`
- `hs_class_start_times`
- `hs_entry_go_times`

Writes:

- `time_engine`
- `time_engine_logs`
- timer/result trigger-ready rows

Wakes:

- `rider-results`
- `alerts`
- `publish`

### `rider-results`

Gate:

- `focus_show.results_enabled=true`
- time-engine rider-result-ready row exists
- watched rider/class/entry eligibility exists

Calls:

- approved rider_results source call

Writes:

- rider_results queue/state rows
- `result_change_logs`

Defers:

- broad class-results machinery until needed later

Wakes:

- `alerts`
- `publish`

### `alerts`

Owner:

- mostly Airtable automation

Trigger:

- new timer/result alert-ready rows
- new rider_results rows

Writes:

- alerts log
- send/delivery state

Wakes:

- `publish`

### `publish`

Trigger:

- live changed
- time-engine changed
- rider_results changed
- alerts changed

Reads:

- runtime tables
- `time_engine`
- rider_results
- append-only logs
- alerts log

Writes:

- output payload/cache/endpoints

Does not:

- mutate source/runtime identity

## Cron Interpretation

Crons are wake-up checks, not ownership chains.

Each cron should only ask whether its lane has eligible work. If yes, it runs that lane and writes a lane summary. If no, it exits cleanly.

The intended shape is not:

```text
core cron -> live -> time-engine -> rider-results -> alerts -> publish
```

The intended shape is:

```text
core cron wakes -> core-build runs or sleeps -> writes state -> stops
live cron wakes -> checks gate/state and get_rings need -> runs or sleeps -> writes state -> stops
time-engine wakes -> checks changed inputs -> runs or sleeps -> writes state -> stops
rider-results cron wakes -> checks watched rider result gate/readiness -> runs or sleeps -> writes state -> stops
alerts automation wakes -> checks alert-ready rows -> acts or sleeps -> writes state -> stops
publish wakes -> checks prepared-data versions -> rebuilds or sleeps -> writes state -> stops
```

## Operating Rule

No lane should patch, repair, or silently compensate for another lane.

If a lane cannot proceed because upstream data, schema, or state is invalid, it should return `FAIL` or `WAITING` with the exact upstream blocker.
