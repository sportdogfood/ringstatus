# WEC Time Engine Contract

## Purpose

`time_engine` is the cadence-owned timing and trigger lane.

It reads the current runtime state, calculates time-sensitive field variables, writes durable trigger rows, and logs each engine run.

It is not a renderer, sender, live fetcher, or pace calculator.

## Current Lane

| Item | Contract |
|---|---|
| Action | `wec-time-engine` |
| Function | `wec_stage1_3_clean_proof` |
| Owner | Catalyst Development |
| Cadence position | after clean Step 4 runtime prep |
| Source tables | `hs_ring_status`, `hs_class_start_times`, `hs_entry_go_times` |
| Output table | `time_engine` |
| Log table | `time_engine_logs` |
| Airtable mirrors | `time_engine`, `time_engine_logs` |

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

## Downstream Consumers

| Consumer | Reads |
|---|---|
| `mobile_pro` | `time_engine` plus runtime tables |
| print | `time_engine` plus runtime tables |
| two-way service | `time_engine` rows by ring/class/entry |
| alerts/messages | `time_engine` rows where `trigger_ready=true` |
| audit/debug | `time_engine_logs` |

## Required Live Wiring

`time_engine` must be wired to live-enriched runtime fields when live data is available.

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
| 1 | heartbeat/cadence completes runtime prep |
| 2 | `wec-time-engine` reads current focus-day runtime rows |
| 3 | engine writes bounded `time_engine` rows |
| 4 | engine writes one `time_engine_logs` row |
| 5 | Airtable mirrors are updated for visibility/review |
| 6 | downstream lanes read trigger-ready rows |

## Latest Prototype Proof

| Proof | Result |
|---|---|
| Catalyst `time_engine` table | created |
| Catalyst `time_engine_logs` table | created |
| Airtable `time_engine` mirror | created |
| Airtable `time_engine_logs` mirror | created |
| bounded engine run | passed in 3 chunks |
| rows written/mirrored | 116 |
| live endpoints run | no |
| alerts sent | no |
| output published | no |

## Status

Prototype lane is built and verified for current clean runtime rows.

Next required work is live wiring from existing live enrichment fields into the runtime tables before `time_engine` runs.
