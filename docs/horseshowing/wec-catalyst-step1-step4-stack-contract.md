# WEC Catalyst Step 1-4 Stack Contract

## Purpose

This document locks the proven Catalyst-owned WEC Step 1-4 stack. The stack exists to create and refresh the current active show/day foundation through runtime prep without entering live endpoints, alerts, customer-facing output, or downstream messaging lanes.

## Active Focus Source

The stack resolves the active WEC context dynamically from Airtable `focus_show`.

Locked active focus proof:

- `show_no = 14909`
- `focus_day = 2026-07-03`
- `focus_show_record_id = recLp41QwgnS5R2Ut`
- focus source: `airtable.focus_show.active`

No step may hardcode `focus_day` or fall back to an old day.

## Catalyst Scheduler Owner

Catalyst Job Scheduling owns the cadence.

Scheduler target action:

```text
action=wec-cadence-step1-step4
```

The scheduler-owned proof used:

```text
wec_heartbeat_day_every_6_min
```

The submitted scheduler job targeted:

```text
https://horseshowing-700800454.development.catalystserverless.com/server/horseshowing_sync/?action=wec-cadence-step1-step4&cadence_window=day
```

## Step 1 Contract

Step 1 writes the cadence heartbeat and current-day ring-day source mirror.

Tables:

- `hs_heartbeat`
- `hs_get_ring_days`

Required behavior:

- Resolve active `focus_show` dynamically.
- Fetch HorseShowing ring days for the active `show_no`.
- Materialize current active `focus_day` ring-day rows.
- Stop only after Step 1 evidence is written.

Step 1 must not run:

- `get_orders`
- `get_rings`
- `get_results`
- alerts
- mobile
- print
- PDF
- two_way

## Step 2 Contract

Step 2 reads current-day `hs_get_ring_days` and writes the schedule mirror.

Table:

- `hs_update_schedule`

Required behavior:

- Read current active-focus `hs_get_ring_days`.
- Request `update_schedule.php` per eligible current-day `ring_day_no`.
- Materialize parsed schedule rows into `hs_update_schedule`.
- Preserve the locked preflight split.
- Stop after `hs_update_schedule`.

Step 2 must not write or run:

- `update_schedule_staging`
- `class_start_times`
- `class_oog`
- `entry_go_times`
- `get_orders`
- `get_rings`
- `get_results`
- alerts
- output lanes

## Step 3 Contract

Step 3 is checkpointed `class_oog`.

Table:

- `hs_class_oog`

Checkpoint location:

- `hs_heartbeat`

Checkpoint key pattern:

```text
show_no|focus_day|step3-checkpoint
```

Required behavior:

- Read current active-focus `hs_update_schedule`.
- Apply the locked preflight rule.
- Probe only non-preflight classes.
- Probe in bounded chunks.
- Materialize only active-trainer matching `entry_no` rows.
- Skip broad nonmatching `class_oog` rows.
- Store checkpoint progress.
- Stop after `hs_class_oog`.

Step 3 must not run:

- `update_schedule_staging`
- `class_start_times`
- `entry_go_times`
- `get_orders`
- `get_rings`
- `get_results`
- alerts
- output lanes

## Step 4 Contract

Step 4 creates runtime prep tables from the locked Step 1-3 source/mirror lanes.

Source tables:

- `hs_get_ring_days`
- `hs_update_schedule`
- `hs_class_oog`

Destination tables:

- `hs_ring_status`
- `hs_class_start_times`
- `hs_entry_go_times`

Required behavior:

- Resolve active `focus_show` dynamically.
- Read current active-focus rows only.
- Build `hs_ring_status` from `hs_get_ring_days`.
- Build `hs_class_start_times` from `hs_update_schedule`.
- Build `hs_entry_go_times` from `hs_class_oog`.
- Preserve visual-key fields.
- Stop after Step 4.

Step 4 must not run:

- `get_orders`
- `get_rings`
- `get_results`
- alerts
- mobile
- print
- PDF
- two_way

## Visual-Key Contract

The stack carries `ring_name_normalized` forward and uses these visual keys:

```text
ring_visual_key = ring_no|ring_name_normalized
class_visual_key = ring_name_normalized|class_no
entry_visual_key = ring_name_normalized|class_no|entry_no
```

Examples:

```text
ring_visual_key = 640|grand
class_visual_key = grand|26788
entry_visual_key = grand|26788|1038
```

## Runtime-Key Contract

Runtime tables use the visual keys as their stable runtime keys:

```text
hs_ring_status.ring_status_key = ring_visual_key
hs_class_start_times.class_start_key = class_visual_key
hs_entry_go_times.entry_go_key = entry_visual_key
```

## Latest Scheduler-Owned PASS Evidence

Scheduler-owned run:

- scheduler: `wec_heartbeat_day_every_6_min`
- scheduler job id: `5614000000713255`
- job status: `SUCCESS`
- response code: `200`
- target: `action=wec-cadence-step1-step4&cadence_window=day`

Active context:

- `show_no = 14909`
- `focus_day = 2026-07-03`
- `focus_show_record_id = recLp41QwgnS5R2Ut`

Stage evidence:

- Step 1: `status=pass`, `branch=step1_get_ring_days`
- Step 2: `status=pass`, `branch=step2_update_schedule`
- Step 3 checkpoint: `checked_class_count=59`, `complete=true`
- Step 4: `status=pass`, `branch=step4_runtime_prep`

Runtime counts:

- `hs_ring_status = 9`
- `hs_class_start_times = 59`
- `hs_entry_go_times = 29`

Runtime visual-key proof:

- `ring_status_key = ring_visual_key`
- `class_start_key = class_visual_key`
- `entry_go_key = entry_visual_key`

Excluded lane proof:

- `get_orders_run = false`
- `get_rings_run = false`
- `get_results_run = false`
- `alerts_send_run = false`

## Explicitly Excluded Lanes

These lanes are not part of the locked Step 1-4 stack:

- `get_orders`
- `get_rings`
- `get_results`
- alerts
- mobile
- print
- PDF
- two_way

They require separate contracts and separate gates.

## Stop Condition

The stack stops after Step 4 runtime prep.

Successful stack stop reason:

```text
step4_complete
```

## Next Gate

The next gate is a separate live-lane contract for:

- `get_rings`
- `get_orders`
- `get_results`

That live lane must remain separate from the Step 1-4 stack until explicitly approved and proven.
