# WEC Current and Future Stack Function Inventory

Date: `2026-07-12`

Mode: `READ-ONLY CODE AUDIT`

## Proof Boundary

This document inventories functions present in the current workspace source.
It does not prove that a function is deployed, scheduled, or currently passing.

```text
code changes              none
Airtable changes          none
workflow executions       none
manual endpoint calls     none
production repairs        none
```

Classifications:

- **Producer**: contains business-table write behavior.
- **Endpoint-only**: reads or formats an existing table but does not produce it.
- **Missing**: no producer for the required target behavior was found.

## Current Stack

| Airtable stage | Existing functions currently associated |
|---|---|
| Preconditions | `getActiveFocusShow()`, `getActiveAirtableFocusShowStrict()`, `getOutputFocusShow()` |
| Router start, boundaries, finish | `createRouterRun()`, `executeLoggedAction()`, `buildRouterLogKey()` |
| Schedule preparation | `runCleanBuildUpdateOnly()` -> `runStage1HeartbeatAndRingDays()` -> `runStage2UpdateSchedule()` |
| FAST probe | `runFast3AOnly()` -> `runProbe3A()` |
| Parse and stage | `runFast3BOnly()` -> `runProbe3B()` -> `parseClassOogRaw()` |
| Runtime preparation | `runStep4RuntimePrepCleanOnly()` using `ringStatusRowsFromRingDays()`, `classStartRowsFromUpdateSchedule()`, `entryGoRowsFromClassOog()` |
| Live enrichment | `runWecStep5LiveEnrichmentOnly()` -> `fetchStep5LiveSource()` -> `enrichStep5RuntimeRows()` |
| Time Engine calculate and expedite | `runTimeEngineOnly()` -> `buildTimeEngineRows()` |
| Time triggers | `buildTimeEngineTrigger()` -> `insertNewTimeEngineTriggers()` -> `appendAirtableTimeEngineTriggers()` |
| `statewise_now` | **Endpoint-only:** `statewiseNowEndpointFields()`, `normalizeStatewiseNowEndpointFilters()`, `sliceStatewiseNowSnapshots()` |
| Legacy Results | `runWecStep6Results()` -> `buildStep6ClassRows()` -> `fetchResults()` -> `parseResults()` |
| Legacy result writes | `upsertCatalystClassResult()`, `step6ResultClassRow()`, `step6ClassResultRow()`, `step6ResultQueueRow()` |
| `hs_rider_results` | **Endpoint-only:** `riderResultEndpointFields()` and the existing read endpoint |
| Helpers | `syncOneHelperTable()`, `runWecSyncHelpers()`, `syncCatalystHorseHelpersFromAirtable()` |
| Alert-event calculations | `buildRingAlertEvents()`, `buildClassAlertEvents()`, `buildEntryAlertEvents()` |
| Legacy result alerts | `writeResultAlertsForClassResults()` |
| Outputs | `getStep4RuntimeRows()`, `buildStep4RuntimeMobilePayload()`, `getScheduleUiPayloadRows()`, `buildScheduleUiOverviewPayload()`, `buildScheduleUiDensePayload()`, `buildScheduleUiEntityPayload()` |

## Current Function Locations

### Core Schedule, Probe, Parse, Runtime

File: `ringstatus-data/catalyst-workspaces/horseshowing/functions/wec_stage1_3_clean_proof/index.js`

| Function | Line | Role |
|---|---:|---|
| `runStage1HeartbeatAndRingDays()` | 180 | Current ring-day and heartbeat acquisition |
| `runStage2UpdateSchedule()` | 216 | Current `update_schedule` acquisition |
| `runProbe3A()` | 253 | FAST class-document probe |
| `runProbe3B()` | 314 | Raw-document parsing and staging |

File: `ringstatus-data/catalyst-workspaces/horseshowing/functions/wec_stage1_3_clean_proof/handler.js`

| Function | Line | Role |
|---|---:|---|
| `parseClassOogRaw()` | 2480 | Parse stored raw class document |
| `runFast3AOnly()` | 3092 | Bounded Stage 3A coordinator |
| `runFast3BOnly()` | 3189 | Bounded Stage 3B coordinator |
| `runCleanBuildUpdateOnly()` | 3296 | Schedule-build coordinator |
| `runCleanCadenceStack()` | 3576 | Current composite Core cadence |
| `runStep4RuntimePrepCleanOnly()` | 4465 | Build current ring, class, and entry runtime rows |

The active Core implementation still acquires ring days before
`update_schedule`. The `update_schedule`-first target has not fully replaced
that path.

### Live

File: `ringstatus-data/catalyst-workspaces/horseshowing/functions/horseshowing_sync/index.js`

| Function | Line | Role |
|---|---:|---|
| `fetchStep5LiveSource()` | 15133 | Fetch and parse `get_rings` live source |
| `enrichStep5RuntimeRows()` | 15309 | Update live fields on runtime projections |
| `runWecStep5LiveEnrichmentOnly()` | 15796 | Live action coordinator |

`get_orders` is retired from the hot Live path.

### Time Engine and Triggers

File: `ringstatus-data/catalyst-workspaces/horseshowing/functions/wec_stage1_3_clean_proof/handler.js`

| Function | Line | Role |
|---|---:|---|
| `buildClassAlertEvents()` | 1311 | Build class transition events |
| `buildRingAlertEvents()` | 1371 | Build ring transition events |
| `buildEntryAlertEvents()` | 1442 | Build entry transition events |
| `appendAirtableTimeEngineTriggers()` | 1865 | Append trigger evidence to Airtable |
| `buildTimeEngineTrigger()` | 1896 | Build canonical trigger row |
| `insertNewTimeEngineTriggers()` | 1995 | Deduplicated Catalyst trigger insert |
| `buildTimeEngineRows()` | 2015 | Calculate current time and transition state |
| `runTimeEngineOnly()` | 2280 | Time Engine coordinator |

### Statewise

File: `ringstatus-data/catalyst-workspaces/horseshowing/functions/wec_stage1_3_clean_proof/handler.js`

| Function | Line | Role |
|---|---:|---|
| `statewiseNowEndpointFields()` | 5021 | Format existing rows for the endpoint |
| `normalizeStatewiseNowEndpointFilters()` | 5086 | Validate endpoint filters |
| `sliceStatewiseNowSnapshots()` | 5136 | Slice existing snapshots around a timestamp |

**Missing:** no JavaScript producer or upsert for `statewise_now` was found.

### Results

File: `ringstatus-data/catalyst-workspaces/horseshowing/functions/horseshowing_results_runner/index.js`

| Function | Line | Role |
|---|---:|---|
| `parseResults()` | 904 | Parse source result payload |
| `fetchResults()` | 952 | Fetch class results |
| `upsertCatalystClassResult()` | 1009 | Write legacy class-result row |
| `timeEngineResultReadyClassKeys()` | 1287 | Read result-ready class scope |
| `buildStep6ClassRows()` | 1392 | Build legacy result candidates |
| `runWecStep6Results()` | 1649 | Legacy Results coordinator |

**Missing:** no producer writes `hs_rider_results`. The current runner records
`future_target_not_implemented` for that handoff.

### Helpers

File: `ringstatus-data/catalyst-workspaces/horseshowing/functions/horseshowing_sync/index.js`

| Function | Line | Role |
|---|---:|---|
| `syncCatalystHorseHelpersFromAirtable()` | 13232 | Horse helper listener/sync behavior |
| `syncOneHelperTable()` | 13709 | Sync one configured helper table |
| `runWecSyncHelpers()` | 13776 | Helper sync coordinator |

### Router Logging

File: `ringstatus-data/catalyst-workspaces/horseshowing/router-logger/index.js`

| Function | Line | Role |
|---|---:|---|
| `buildRouterLogKey()` | 33 | Build deterministic router event key |
| `createRouterRun()` | 182 | Create one instrumented router run |
| `executeLoggedAction()` | 266 | Wrap a business action with router events |

### Outputs

File: `ringstatus-data/catalyst-workspaces/horseshowing/functions/horseshowing_sync/index.js`

| Function | Line | Role |
|---|---:|---|
| `buildScheduleJson()` | 4365 | Build schedule JSON output |
| `buildRichApiPayload()` | 4682 | Build rich output payload |
| `getStep4RuntimeRows()` | 7027 | Read prepared runtime lists |
| `buildStep4RuntimeMobilePayload()` | 7232 | Build mobile current-state payload |
| `buildScheduleUiEntityPayload()` | 7633 | Build entity list/detail payload |
| `buildScheduleUiOverviewPayload()` | 7775 | Build overview schedule payload |
| `buildScheduleUiDensePayload()` | 7799 | Build dense/detail payload |
| `getScheduleUiPayloadRows()` | 7825 | Read prepared rows, events, and results |

## Future Stack

| Future stack | Keep existing | Modify existing | Missing or new producer |
|---|---|---|---|
| **Stack 5 Live** | `runWecStep5LiveEnrichmentOnly()`, `fetchStep5LiveSource()` | `enrichStep5RuntimeRows()`, `sourceDerivedPaceSeconds()`, `classStatusForStart()` | Reliable not-live reconciliation, snapshot pace, frozen class start, current entry position |
| **Stack 6 Time Engine** | `runTimeEngineOnly()`, `buildTimeEngineRows()`, field-variable builders | Trigger scope and `build*AlertEvents()` vocabulary | `class_start_60`, `entry_go_40`, `entry_class_10_gone`, `entry_10_away`, `statewise_snapshot_due` calculations |
| **Stack 6 Statewise** | Existing endpoint and filter functions | None currently produce rows | New twelve-minute `statewise_now` producer |
| **Stack 7 Rider Results** | `runWecStep6Results()`, readiness selection, `fetchResults()`, `parseResults()` | Tracked-entry scope, terminal handling, finished-time preservation | New `hs_rider_results` row builder and writer |
| **Outputs** | Existing mobile, print, rich, and schedule UI builders | None during Stacks 5-7 | No replacement planned |

## Stack 5 Existing Function Decision

### Keep

- `runWecStep5LiveEnrichmentOnly()` as coordinator.
- `fetchStep5LiveSource()` for `get_rings` acquisition.
- Existing ring/class identity helpers.
- `get_orders` retirement.

### Modify

- `enrichStep5RuntimeRows()` to complete approved current-state updates.
- `sourceDerivedPaceSeconds()` to enforce the approved pace range and support
  snapshot-delta calculation.
- Existing status and time helpers only where they match the approved contract.

### Missing Capability

- Reliable `is_live=false` reconciliation.
- `entry_count_now`, `n_gone_now`, and `n_to_go_now` production.
- Snapshot-to-snapshot pace.
- Observed and frozen class start.
- Current tracked-entry position and go-time propagation.
- Ring now, next, end, and lateness after schedule slack.

## Stack 6 Existing Function Decision

### Keep

- `runTimeEngineOnly()` as coordinator.
- `buildTimeEngineRows()` as the current calculation owner.
- Existing field-variable builders.
- `buildTimeEngineTrigger()` and `insertNewTimeEngineTriggers()` for event
  identity and deduplication.

### Modify

- Restrict tracked-class calculations to unique classes represented in
  `hs_entry_go_times`.
- Update the trigger vocabulary and transition calculations.
- Keep `ring_class_change` and `result_ready` as internal events.
- Remove customer emission of `ring_live` and inferred `ring_gate` under the
  approved target contract.

### Missing Capability

- `class_start_60`.
- `entry_go_40`.
- `entry_class_10_gone`.
- `entry_10_away`.
- `statewise_snapshot_due`.
- A twelve-minute `statewise_now` producer.

## Stack 7 Existing Function Decision

### Keep

- `runWecStep6Results()` as the bounded Results coordinator.
- Time Engine result-ready class selection.
- Tracked entry/class scope helpers.
- `fetchResults()` and `parseResults()`.
- Existing legacy result writers and outputs while the narrow lane is added.

### Modify

- Restrict candidates to approved `result_ready` plus tracked
  `hs_entry_go_times` scope.
- Match source results by `class_no + entry_no`.
- Preserve place, score, and finished time.
- Add terminal placed/no-place handling.
- Stop polling when all tracked entries for the class are terminal.

### Missing Capability

- `hs_rider_results` row builder.
- `hs_rider_results` writer.
- Terminal placed/no-place classifier.
- Completed-class stop rule based on tracked-entry outcomes.

## Inactive Alternative Core

`wec_v2_core_lane` contains prototype source and pipeline functions, but it is
not an active producer:

- its scheduler is disabled;
- it is absent from `catalyst.json`;
- its handler imports a missing `service.js`.

It must not be treated as the current or future approved Core without a
separate decision.

