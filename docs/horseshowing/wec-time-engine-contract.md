# WEC Time Engine Owner Contract

Version: 2026-07-08 v1

## Purpose

`time_engine` is the owned timing and trigger-readiness lane.

It reads the current runtime state, calculates time-sensitive field variables, writes durable trigger rows, and logs each engine run.

It is not a renderer, sender, HorseShowing source fetcher, schedule builder, or live pace owner.

## Ownership Boundary

`time-engine owner + downstream trigger observer`

- Owns review/documentation/changes for `time_engine`.
- May inspect upstream `core 1-4` and `live-enrich` only to verify inputs are current enough to calculate timing.
- May inspect downstream `results`, `alerts`, and `publish` only to verify Time Engine output is being consumed.
- Must not patch `core 1-4`, `live-enrich`, `results`, `alerts`, or `publish` unless explicitly approved.
- If Core/Live input is wrong, report the upstream blocker and route it to that lane.
- If Time Engine output is wrong, fix Time Engine.
- If downstream logic is wrong, report the downstream blocker and route it to that lane.

## Current Lane

| Item | Contract |
|---|---|
| Action | `wec-time-engine` |
| Function | `wec_stage1_3_clean_proof` |
| Owner | Time Engine lane |
| Scheduler | `wec_time_engine_clock_6_min` |
| Scheduler owner | Catalyst Job Scheduling / Webhook pool |
| Scheduler cadence | `*/6 6-21 * * *` America/New_York |
| Scheduler target | `https://horseshowing-700800454.development.catalystserverless.com/server/wec_stage1_3_clean_proof/?action=wec-time-engine&wake_reason=clock_window` |
| Retries | `0` |
| Source tables | `hs_ring_status`, `hs_class_start_times`, `hs_entry_go_times` |
| Output table | `time_engine` |
| Log table | `time_engine_logs` |
| Airtable mirrors | `time_engine`, `time_engine_logs` |

## Wake Contract

| Wake | Rule |
|---|---|
| `clock_window` | Runs only when active `focus_show` is inside `show_start_time` / `show_end_time` for the active `focus_day` |
| `state_wake` | Runs when upstream state change explicitly wakes Time Engine |
| Core change | Valid input wake after runtime prep changes |
| Live change | Valid input wake after live enrichment changes |

Clock wake must log `SKIPPED` with a reason when the gate blocks the run.

Accepted show-window formats include `HH:MM`, `HH:MM:SS`, and numeric minutes/seconds.

## What It Calculates

| Field | Meaning |
|---|---|
| `starts_in_mins` | minutes from now to `class_start_time` |
| `ends_in_mins` | minutes from now to estimated class end |
| `go_in_mins` | minutes from now to `entry_go_time` |
| `tags` | threshold tags such as `starts_in_30`, `starts_in_60`, `go_in_20`, `go_in_40` |
| `status` | current timing/status value for downstream consumers |
| `trigger_ready` | true when a row has a downstream trigger condition |

## What It Does Not Do

| Exclusion | Reason |
|---|---|
| calculate ring pace | pace belongs to live/runtime enrichment |
| fetch `get_rings` | live source lane owns this |
| fetch `get_orders` | live source lane owns this |
| fetch results | results lane owns this |
| send alerts | message publish/send lane owns this |
| render mobile/print | output lanes own rendering |
| mutate source/runtime identity | Core owns identity and runtime base rows |

## Downstream Consumers

| Consumer | Reads |
|---|---|
| `mobile_pro` | `time_engine` plus runtime tables |
| print | `time_engine` plus runtime tables |
| two-way service | `time_engine` rows by ring/class/entry |
| alerts/messages | `time_engine` rows where `trigger_ready=true` |
| audit/debug | `time_engine_logs` |

## Required Live Wiring

`time_engine` must read live-enriched runtime fields when live data is available.

Live wiring should update the source runtime tables first:

| Live source | Updates |
|---|---|
| `get_rings` | ring/class live state, `n_gone`, `n_to_go`, elapsed fields, pace fields |
| `get_orders` | current/next entry order and source-derived entry timing |

Then `time_engine` recalculates from those enriched runtime rows.

The engine should not bypass the runtime tables or calculate its own live pace.

## Expected Run Pattern

| Step | Behavior |
|---|---|
| 1 | Scheduler wakes `wec-time-engine` |
| 2 | Engine resolves active `focus_show` |
| 3 | Gate checks wake reason and show window |
| 4 | Engine reads current focus-day runtime rows |
| 5 | Engine writes `time_engine` rows |
| 6 | Engine writes one `time_engine_logs` row |
| 7 | Airtable mirrors are updated for visibility/review |
| 8 | Downstream lanes read trigger-ready rows |

## Current Proof

| Proof | Result |
|---|---|
| Date verified | 2026-07-08 |
| Show | `14910` |
| Focus day | `2026-07-08` |
| Scheduler | `wec_time_engine_clock_6_min` |
| Scheduler status | enabled |
| Scheduler-owned job | `5614000000756552` |
| HTTP response | `200` |
| Time Engine status | `PASS` |
| Wake reason | `clock_window` |
| Gate reason | `clock_window` |
| Source `hs_ring_status` | `9` |
| Source `hs_class_start_times` | `51` |
| Source `hs_entry_go_times` | `15` |
| Rows written | `75` |
| Current `time_engine` count | `75` |
| live endpoints run | no |
| alerts sent | no |
| output published | no |

## 2026-07-08 Continuity Fix

The scheduler was alive, but Time Engine skipped because the active `focus_show` show window came through as `6:00:00` / `22:00:00`.

The clock parser only accepted `HH:MM`, so the lane logged `focus_show.show_window_missing`.

The fix was to accept `HH:MM:SS` in the Time Engine show-window parser. After deploy, the scheduler-owned clock wake passed and wrote 75 rows.

## Status

Time Engine is active and verified through its scheduler-owned clock wake.

Next required work is downstream consumption verification by `results`, `alerts`, and `publish`; those are observer checks unless explicitly approved for cross-lane edits.
