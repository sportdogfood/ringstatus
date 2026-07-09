# WEC Results Lane Contract

Version: 2026-07-08 v2

## Purpose

The Results lane owns result ingestion only.

It wakes on its own scheduler, checks the active focus show, probes HorseShowing results only for tracked classes, writes result rows, and stops.

## Owner

`results owner + upstream observer`

Meaning:

- Results owns `horseshowing_results_runner`.
- Results owns `wec-step6-results`.
- Results owns `hs_result_queue`, `hs_result_classes`, and `hs_class_results`.
- Results may observe `focus_show`, `hs_update_schedule`, `hs_class_start_times`, `hs_entry_go_times`, and `time_engine`.
- Results must not mutate core schedule/runtime identity.
- Results must not run or patch `core 1-4`, `live-enrich`, `time-engine`, `alerts`, or `publish`.

## Scheduler

Cron:

- `wec_results_6_min`

Target:

```text
horseshowing_results_runner/?action=wec-step6-results&limit=3
```

Rules:

- The cron must stay enabled during active result windows.
- The cron target must not hardcode old `show_no` or `focus_day`.
- The runner resolves active `focus_show`.
- Scheduler-owned proof must come from Catalyst JobScheduling, not a manual endpoint call.

## Gate

Results runs only when active `focus_show` has:

- `active = true`
- `results_enabled = true`
- valid `show_no`
- valid `focus_day`

All Catalyst reads must filter by exact active:

```text
show_no + focus_day
```

No back-date fallback is allowed.

## Candidate Scope

Results candidates are allowed only from classes present in:

```text
hs_entry_go_times
```

`hs_update_schedule` is reference data only.

It may provide:

- `live_flag`
- `class_start_time`
- `entry_count`
- native `class_no`
- class/ring metadata

It must not broaden the candidate list.

Candidate rule:

```text
class exists in hs_entry_go_times for active show_no + focus_day
AND class exists in hs_class_start_times
AND result eligibility is true
AND class is not already completed/exhausted
```

## Result Eligibility

Results accepts either readiness source:

1. `time_engine` result readiness:

```text
time_engine.level = class
time_engine.trigger_ready = true
tags/status/payload indicate result/check_results readiness
```

2. Narrow watched fallback:

```text
class is present in hs_entry_go_times
AND now >= class_start_time + (entry_count * 3.3 minutes)
```

If any current-day `hs_update_schedule.live_flag = 1` rows exist, watched fallback is limited to those live-flag rows.

If no live-flag rows exist, watched fallback is limited to tracked `hs_entry_go_times` classes only.

`result_ready` means "probe results now." It does not mean results exist and does not mark a class Done.

## Source Request

HorseShowing source:

```text
show_results4.php
```

Request uses native:

```text
class_no
```

Internal keys must not be sent upstream.

## Retry Contract

`hs_result_queue` owns retry state.

Fields:

- `status`
- `attempts`
- `last_checked_at`
- `next_check_at`
- `result_rows`

Statuses:

- `pending`
- `completed`
- `exhausted`

Rules:

- Completed classes are skipped.
- Exhausted classes are skipped.
- Classes with attempts >= 5 are skipped.
- Pending classes retry only when `next_check_at` is due.
- No fake result rows.

## Completion Rule

When real results are returned:

- write `hs_result_queue`
- write `hs_result_classes`
- write `hs_class_results`
- mirror to Airtable `hs_*` result tables
- update related `hs_class_start_times` status to `Done`

Do not mark `Done` from time estimate alone.

## Explicit Exclusions

Results must not run:

- core 1-4
- live-enrich
- time-engine
- alerts
- publish
- mobile
- print
- Webflow publish
- Production deploy

Results must not read:

```text
class_oog_staging.active_entries
```

## Required Response Evidence

Each `wec-step6-results` response should expose:

- active `show_no`
- active `focus_day`
- `results_enabled`
- source tables
- `hs_entry_go_times` tracked class count
- `time_engine_result_ready`
- watched-result-ready count
- guard: `only_hs_entry_go_times_classes = true`
- guard: `outside_tracked_class_keys = 0`
- probed class numbers
- completed classes
- result row counts
- `step_1_5_run = false`
- `result_alerts_run = false`

## Current Proof

Date: 2026-07-08

Active focus:

- `show_no = 14910`
- `focus_day = 2026-07-08`
- `results_enabled = true`

Scheduler:

- `wec_results_6_min`
- scheduler-owned submit succeeded
- HTTP `200`

Current Catalyst result state after scheduler-owned runs:

- `hs_result_queue`: 7 completed tracked classes
- completed class numbers:
  - `33331`
  - `33327`
  - `31361`
  - `33332`
  - `31381`
  - `31488`
  - `31386`

Known Airtable mirror proof:

- class `33332`
- `hs_result_queue`: 1
- `hs_result_classes`: 1
- `hs_class_results`: 20
- includes `Sandenal` / `Tanner Korotkin`

## Current Guardrail

The lane must stay narrow:

```text
Results candidate = active-focus class present in hs_entry_go_times
```

The full `hs_update_schedule` day list is not a candidate list.

## Known Caution

Do not use `no_write=1` as a proof path for Step 6 until it is explicitly fixed and verified.

