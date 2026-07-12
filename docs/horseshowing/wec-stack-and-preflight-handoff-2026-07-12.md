# WEC Stack and Preflight Handoff - 2026-07-12

Show: `14910`

Focus day: `2026-07-12`

## Airtable-Presented Stack

This is the workflow order currently presented by Airtable `hs_endpoints`,
sorted by `group_priority` and `sort_priority`.

| Priority | Presented stage | Main records |
|---:|---|---|
| `-1` | Preconditions | `hs_heartbeat`, `shows`, `focus_show` |
| `0` | Router start | `hs_router`, `hs_router_logs` |
| `1` | Schedule preparation | `hs_update_schedule`, `hs_class_start_times` |
| `2` | Probe, parse, stage, entries | `FAST_probe`, `hs_class_oog_raw`, `hs_class_oog`, `hs_entry_go_times` |
| `3` | Recurring live enrichment | `hs_get_rings`, `hs_ring_status`, `statewise_now` |
| `4` | Calculation and dispatch | Time Engine calculate, expedite, `time_engine_logs`, `time_engine_triggers` |
| `5` | Result readiness | `horseshowing_results_runner`, `hs_rider_results` |
| `6` | Non-blocking helpers | `hs_horses`, `hs_riders`, `hs_trainers` |
| `7` | Transition alerts | `ringwise`, `classwise`, `entrywise`, `riderwise` |
| `8-9` | Reserved | Router placeholders |
| `10` | Read-only outputs | `wec-print`, `wec-mobile`, `wec-mobile-pro`, `wec-mobile-entry`, `barn_entry_review` |
| `11` | Workflow finish | `hs_router_logs` |

Airtable presents a router-log boundary after each major section.

### Known Presentation Corrections

- `statewise_now` is incorrectly described as a Live router boundary.
- The Results records are incorrectly scoped as `enrich-live`.

## Core Preflight Proof

The established outside-lane preflight ran against the live Horseshowing
source for show `14910` and focus day `2026-07-12`.

Result: `PASS`

| Stage | Proven result |
|---|---:|
| Ring days found | `9` |
| Rings returning schedules | `9 of 9` |
| Schedule rows collected | `114` |
| Rows containing `class_no` | `86` |
| Preflight-only rows excluded | `1` |
| Eligible classes | `85` |
| Classes producing useful raw documents | `24` |
| Classes with no tracked evidence after three probes | `61` |
| Raw documents parsed | `24 of 24` |
| Tracked entries produced | `37` |
| Projected `hs_ring_status` rows | `9` |
| Projected `hs_class_start_times` rows | `85` |
| Projected `hs_entry_go_times` rows | `37` |
| Step 4 blockers | `0` |

### Class Accounting

```text
85 eligible classes
= 24 useful raw documents
+ 61 confirmed no-match classes
```

```text
24 parsed documents
-> 37 tracked entries
```

### Runtime Projection

```text
hs_ring_status        9
hs_class_start_times 85
hs_entry_go_times    37
```

Natural gate: `runtime_ready`

The projected Core dataset contained the ring, class, and entry identities
required for Time Engine handoff.

## Proof Boundary

```text
dry_run             true
wrote_records       false
heartbeat_written   false
date_rewrite        false
```

This proves that the July 12 live source dataset can pass through the Core
preparation and runtime-projection path without a Step 4 blocker.

It does not prove that the scheduled production cadence wrote those records or
that Live, Time Engine, Statewise, Time Triggers, Results, alerts, or outputs
completed successfully.

## Preflight Command

```powershell
node .\core_1_4_lab.js --dataset-source live --show-no 14910 --source-focus-day 2026-07-12 --run-probe true --retry-no-match-to-cap true
```

