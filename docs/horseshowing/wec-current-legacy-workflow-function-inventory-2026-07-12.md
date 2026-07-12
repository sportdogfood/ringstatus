# WEC Current Legacy Workflow Function Inventory

Date: `2026-07-12`

Mode: `READ-ONLY CODE AUDIT`

## Proof Boundary

This document maps the current legacy workflow found in workspace source. It
does not claim that every function is currently scheduled, deployed at the
inspected version, or passing today.

```text
code changes              none
Airtable changes          none
workflow executions       none
manual endpoint calls     none
production repairs        none
```

## Legacy Workflow Summary

```text
focus_show / heartbeat
  -> Stage 1 ring-day acquisition
  -> Stage 2 update_schedule
  -> Stage 3A FAST probe
  -> Stage 3B parse and stage
  -> Stage 4 runtime preparation

Live get_rings
  -> runtime live enrichment

Time Engine
  -> time_engine
  -> time_engine_logs
  -> time_engine_triggers

Legacy Results
  -> hs_result_queue
  -> hs_result_classes
  -> hs_class_results

Outputs
  -> schedule, mobile, print, rich, and entity views
```

Router logging wraps the Core, Live, Time Engine, Results, alert, and output
boundaries.

## Legacy Function and Action Owners

| Area | Catalyst function | Main action |
|---|---|---|
| Core Stages 1-4 | `wec_stage1_3_clean_proof` | `wec-clean-cadence-stack` |
| Core schedule build | `wec_stage1_3_clean_proof` | `wec-clean-build-update` |
| Stage 3A | `wec_stage1_3_clean_proof` | `wec-clean-probe-3a` |
| Stage 3B | `wec_stage1_3_clean_proof` | `wec-clean-process-3b` |
| Stage 4 | `wec_stage1_3_clean_proof` | `wec-clean-runtime-4` |
| Live | `horseshowing_sync` | `wec-step5-live-enrichment` |
| Time Engine | `wec_stage1_3_clean_proof` | `wec-time-engine` |
| Results | `horseshowing_results_runner` | `wec-step6-results` |
| Outputs | `horseshowing_sync` | `wec-mobile-live`, `wec-print-live`, `wec-schedule-ui`, `wec-rich-live` |

## Preconditions and Control

### Existing functions

| Function | File | Line | Purpose |
|---|---|---:|---|
| `getActiveFocusShow()` | `wec_stage1_3_clean_proof/handler.js` | 568 | Resolve the active show and focus day for Core and Time Engine |
| `getActiveAirtableFocusShowStrict()` | `horseshowing_sync/index.js` | 12532 | Resolve strict Airtable-owned focus state for Live and legacy step actions |
| `getOutputFocusShow()` | `horseshowing_sync/index.js` | 12574 | Resolve active focus state for outputs |
| `writeStageHeartbeat()` | `horseshowing_sync/index.js` | 8070 | Write stage heartbeat status |

### Control inputs

```text
show_no
focus_day
is_pause
is_lock
live_enrichment
results_enabled
```

## Stage 1 - Ring-Day Acquisition

### Current clean Core function

| Function | File | Line | Purpose |
|---|---|---:|---|
| `runStage1HeartbeatAndRingDays()` | `wec_stage1_3_clean_proof/index.js` | 180 | Resolve focus, fetch ring days, write heartbeat, and upsert `hs_get_ring_days` |

### Legacy compatibility functions

| Function | File | Line | Purpose |
|---|---|---:|---|
| `runWecStep1HeartbeatGetRingDays()` | `horseshowing_sync/index.js` | 8169 | Standalone legacy Step 1 action |
| `fetchAndSyncRingDays()` | `horseshowing_sync/index.js` | 14990 | Fetch `get_ring_days.php` and materialize ring-day rows |

### Writes

```text
hs_heartbeat
hs_get_ring_days
```

## Stage 2 - Update Schedule

### Current clean Core functions

| Function | File | Line | Purpose |
|---|---|---:|---|
| `runCleanBuildUpdateOnly()` | `wec_stage1_3_clean_proof/handler.js` | 3296 | Coordinate Stage 1 and Stage 2 |
| `runStage2UpdateSchedule()` | `wec_stage1_3_clean_proof/index.js` | 216 | Fetch and normalize schedules for Stage 1 ring days |

### Legacy compatibility functions

| Function | File | Line | Purpose |
|---|---|---:|---|
| `runWecStep2UpdateScheduleOnly()` | `horseshowing_sync/index.js` | 8374 | Standalone legacy Step 2 action |
| `fetchAndSyncUpdateScheduleOnly()` | `horseshowing_sync/index.js` | 16032 | Fetch one `update_schedule.php` ring-day schedule and write keyed rows |
| `parseRingDayScheduleRows()` | `horseshowing_sync/index.js` | 822 | Parse schedule HTML source rows |

### Writes

```text
hs_update_schedule
```

Stage 2 also supplies schedule evidence used to seed class runtime rows.

## Stage 3A - FAST Probe

| Function | File | Line | Purpose |
|---|---|---:|---|
| `runFast3AOnly()` | `wec_stage1_3_clean_proof/handler.js` | 3092 | Select eligible schedule rows and coordinate bounded probing |
| `runProbe3A()` | `wec_stage1_3_clean_proof/index.js` | 253 | Probe class documents for tracked horse/trainer evidence |
| `runWecStep3ClassOogProbeOnly()` | `horseshowing_sync/index.js` | 9205 | Legacy standalone probe action |
| `fetchAndSyncClassOogForScheduleRow()` | `horseshowing_sync/index.js` | 8906 | Fetch and store one class document |

### Reads

```text
hs_update_schedule
hs_horses
hs_trainers
```

### Writes

```text
hs_class_oog_raw
probe status and retry evidence
```

## Stage 3B - Parse and Stage

| Function | File | Line | Purpose |
|---|---|---:|---|
| `runFast3BOnly()` | `wec_stage1_3_clean_proof/handler.js` | 3189 | Coordinate pending raw-document parsing |
| `runProbe3B()` | `wec_stage1_3_clean_proof/index.js` | 314 | Parse unprocessed Stage 3A documents |
| `parseClassOogRaw()` | `wec_stage1_3_clean_proof/handler.js` | 2480 | Convert one raw class document into normalized entry rows |
| `runWecStep3ClassOogParseOnly()` | `horseshowing_sync/index.js` | 9351 | Legacy standalone parse action |

### Reads

```text
hs_class_oog_raw
hs_horses
hs_trainers
hs_riders
```

### Writes

```text
hs_class_oog
```

## Stage 4 - Runtime Preparation

| Function | File | Line | Purpose |
|---|---|---:|---|
| `runStep4RuntimePrepCleanOnly()` | `wec_stage1_3_clean_proof/handler.js` | 4465 | Coordinate current ring, class, and entry runtime preparation |
| `ringStatusRowsFromRingDays()` | `wec_stage1_3_clean_proof/handler.js` | 1069 | Build ring current-state seed rows |
| `classStartRowsFromUpdateSchedule()` | `wec_stage1_3_clean_proof/handler.js` | 1095 | Build class start-time seed rows |
| `entryGoRowsFromClassOog()` | `wec_stage1_3_clean_proof/handler.js` | 1135 | Build tracked entry timing seed rows |
| `runWecStep4RuntimePrepOnly()` | `horseshowing_sync/index.js` | 15570 | Legacy standalone runtime-prep action |

### Reads

```text
hs_get_ring_days
hs_update_schedule
hs_class_oog
```

### Writes

```text
hs_ring_status
hs_class_start_times
hs_entry_go_times
```

## Composite Core Cadence

| Function | File | Line | Purpose |
|---|---|---:|---|
| `runCleanCadenceStack()` | `wec_stage1_3_clean_proof/handler.js` | 3576 | Continue the current Core workflow through its eligible boundary |

Action routes:

```text
wec-clean-build-update
wec-clean-probe-3a
wec-clean-process-3b
wec-clean-runtime-4
wec-clean-cadence-stack
```

The former all-in-one `wec-clean-stage1-3b-proof` action is disabled and
returns HTTP 410. The current code requires split stages.

## Stage 5 - Legacy Live Enrichment

| Function | File | Line | Purpose |
|---|---|---:|---|
| `runWecStep5LiveEnrichmentOnly()` | `horseshowing_sync/index.js` | 15796 | Coordinate gated Live execution |
| `fetchStep5LiveSource()` | `horseshowing_sync/index.js` | 15133 | Fetch and parse `get_rings.php` |
| `parseRingRows()` | `horseshowing_sync/index.js` | 714 | Parse live ring source rows |
| `getRingsSourceRow()` | `horseshowing_sync/index.js` | 1462 | Normalize one live source row |
| `enrichStep5RuntimeRows()` | `horseshowing_sync/index.js` | 15309 | Apply live fields to runtime rows |

### Reads

```text
focus_show.live_enrichment
get_rings.php
hs_ring_status
hs_class_start_times
hs_entry_go_times
```

### Writes

```text
hs_get_rings
hs_ring_status
hs_class_start_times
hs_entry_go_times
hs_heartbeat
```

`get_orders` is explicitly retired from the hot Live path.

## Legacy Time Engine

| Function | File | Line | Purpose |
|---|---|---:|---|
| `runTimeEngineOnly()` | `wec_stage1_3_clean_proof/handler.js` | 2280 | Coordinate current Time Engine execution |
| `buildTimeEngineRows()` | `wec_stage1_3_clean_proof/handler.js` | 2015 | Calculate ring, class, entry, and result-readiness state |
| `buildTimeEngineTrigger()` | `wec_stage1_3_clean_proof/handler.js` | 1896 | Build canonical trigger event |
| `insertNewTimeEngineTriggers()` | `wec_stage1_3_clean_proof/handler.js` | 1995 | Insert unseen trigger keys |
| `appendAirtableTimeEngineTriggers()` | `wec_stage1_3_clean_proof/handler.js` | 1865 | Append trigger evidence to Airtable |

### Reads

```text
hs_ring_status
hs_class_start_times
hs_entry_go_times
hs_update_schedule
```

### Writes

```text
time_engine
time_engine_logs
time_engine_triggers
```

The current route records a Results handoff when new trigger rows are inserted,
but the router log is not proof that the Results scheduler was invoked.

## Legacy Alert Calculations

| Function | File | Line | Purpose |
|---|---|---:|---|
| `buildRingAlertEvents()` | `wec_stage1_3_clean_proof/handler.js` | 1371 | Build ring transition candidates |
| `buildClassAlertEvents()` | `wec_stage1_3_clean_proof/handler.js` | 1311 | Build class threshold candidates |
| `buildEntryAlertEvents()` | `wec_stage1_3_clean_proof/handler.js` | 1442 | Build entry threshold candidates |

These functions currently feed `time_engine_triggers`. Direct Time Engine alert
record creation is disabled in `runTimeEngineOnly()`.

## Stage 6 - Legacy Results

| Function | File | Line | Purpose |
|---|---|---:|---|
| `runWecStep6Results()` | `horseshowing_results_runner/index.js` | 1649 | Coordinate gated legacy Results execution |
| `timeEngineResultReadyClassKeys()` | `horseshowing_results_runner/index.js` | 1287 | Read result-ready classes from Time Engine |
| `buildStep6ClassRows()` | `horseshowing_results_runner/index.js` | 1392 | Build classes eligible for result polling |
| `fetchResults()` | `horseshowing_results_runner/index.js` | 952 | Fetch class result source |
| `parseResults()` | `horseshowing_results_runner/index.js` | 904 | Parse class result source |
| `upsertCatalystClassResult()` | `horseshowing_results_runner/index.js` | 1009 | Write a legacy class-result record |
| `step6ResultClassRow()` | `horseshowing_results_runner/index.js` | 1450 | Build legacy class status row |
| `step6ClassResultRow()` | `horseshowing_results_runner/index.js` | 1485 | Build legacy entry result row |
| `step6ResultQueueRow()` | `horseshowing_results_runner/index.js` | 1517 | Build legacy queue/retry row |

### Reads

```text
focus_show.results_enabled
time_engine result_ready state
hs_class_start_times
hs_entry_go_times
hs_update_schedule compatibility state
```

### Writes

```text
hs_result_queue
hs_result_classes
hs_class_results
```

### Confirmed gap

The legacy Results route logs:

```text
rider_results_target
future_target_not_implemented
```

No legacy producer writes `hs_rider_results`.

## Legacy Result Alerts

| Function | File | Line | Purpose |
|---|---|---:|---|
| `writeResultAlertsForClassResults()` | `horseshowing_results_runner/index.js` | 792 | Write result alerts from legacy class-result rows |

This is separate from result acquisition and does not implement the future
`hs_rider_results` producer.

## Helpers

| Function | File | Line | Purpose |
|---|---|---:|---|
| `syncCatalystHorseHelpersFromAirtable()` | `horseshowing_sync/index.js` | 13232 | Sync Airtable horse helper state into Catalyst |
| `syncOneHelperTable()` | `horseshowing_sync/index.js` | 13709 | Sync one configured helper table |
| `runWecSyncHelpers()` | `horseshowing_sync/index.js` | 13776 | Coordinate helper synchronization |
| `runWecHelperSearch()` | `horseshowing_sync/index.js` | 14361 | Search and hydrate helper results |

Helpers support Core, Results, and outputs but are intended to remain
non-blocking.

## Router Logging

| Function | File | Line | Purpose |
|---|---|---:|---|
| `buildRouterLogKey()` | `router-logger/index.js` | 33 | Build deterministic router event identity |
| `createRouterRun()` | `router-logger/index.js` | 182 | Create one instrumented run |
| `executeLoggedAction()` | `router-logger/index.js` | 266 | Wrap a business action with start, boundary, error, and finish logs |

Router logs provide execution evidence. They do not execute the next lane.

## Legacy Outputs

| Function | File | Line | Purpose |
|---|---|---:|---|
| `buildScheduleJson()` | `horseshowing_sync/index.js` | 4365 | Build legacy schedule JSON |
| `buildRichApiPayload()` | `horseshowing_sync/index.js` | 4682 | Build rich schedule/runtime payload |
| `getStep4RuntimeRows()` | `horseshowing_sync/index.js` | 7027 | Read runtime ring, class, and entry rows |
| `buildStep4RuntimeMobilePayload()` | `horseshowing_sync/index.js` | 7232 | Build mobile payload |
| `buildScheduleUiEntityPayload()` | `horseshowing_sync/index.js` | 7633 | Build entity list/detail payload |
| `buildScheduleUiOverviewPayload()` | `horseshowing_sync/index.js` | 7775 | Build schedule overview |
| `buildScheduleUiDensePayload()` | `horseshowing_sync/index.js` | 7799 | Build dense schedule/detail payload |
| `getScheduleUiPayloadRows()` | `horseshowing_sync/index.js` | 7825 | Read runtime, trigger, and result rows for UI outputs |

Actions:

```text
schedule-json
wec-schedule-live
wec-mobile-live
wec-print-live
wec-schedule-ui
wec-rich-live
wec-rich-api
wec-print-layout
```

Outputs read prepared data. They do not prove upstream completion and must not
be used as substitute workflow runners.

## Endpoint-Only Legacy Surfaces

The following surfaces read existing rows but do not produce them:

```text
wec-data-statewise-now
wec-data-hs-rider-results
wec-data-time-engine-triggers
wec-data-hs-get-rings
```

`statewise_now` and `hs_rider_results` currently have endpoint formatting and
filter functions but no producer.

## Current Legacy Gaps

1. Core still depends on ring-day acquisition before `update_schedule`.
2. Live current-state enrichment is incomplete relative to the approved future
   fields and calculations.
3. Time Engine trigger scope and vocabulary do not match the future contract.
4. `statewise_now` has no producer.
5. Results still writes broad legacy queue, class, and class-result tables.
6. `hs_rider_results` has no producer.
7. Router handoff logs identify intended next actions but do not themselves
   execute those actions.
8. Outputs can return HTTP 200 while upstream producers remain incomplete.

