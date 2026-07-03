# WEC Catalyst Cadence Step 1 / Step 2 Contract

## Purpose

This document locks the current proven WEC Catalyst cadence foundation through Step 2 only.

The contract is intended to prevent one-off workflow wrangling by defining the exact cadence owner, action, source inputs, destination outputs, stop point, and proof required before any Step 3 work begins.

## Active Focus Source

The active WEC show/day context is resolved dynamically from Airtable `focus_show`.

The cadence must use the active `focus_show` record as the source for:

- `show_no`
- `focus_day`
- `focus_show_record_id`
- `is_pause`
- `is_lock`
- `live_enrichment`

The cadence must not hardcode `show_no` or `focus_day`.

## Catalyst Job Scheduling Owner

The WEC cadence owner is Catalyst Job Scheduling.

The scheduled target action is:

```text
wec-cadence-step1-step2
```

This action owns the locked Step 1 -> Step 2 cadence sequence.

## Action Contract

Action:

```text
horseshowing_sync?action=wec-cadence-step1-step2
```

Required behavior:

1. Resolve active `focus_show`.
2. Run Step 1.
3. Run Step 2 only if Step 1 passes.
4. Stop after Step 2.

This action must not continue into downstream lanes.

## Step 1 Contract

Step 1 is:

```text
heartbeat + hs_get_ring_days
```

Step 1 must:

- write cadence proof to `hs_heartbeat`
- refresh Catalyst `hs_get_ring_days`
- refresh Airtable mirror `hs_get_ring_days`
- preserve active `focus_show` context
- stop without running schedule materialization unless called through `wec-cadence-step1-step2`

Step 1 must not run:

- `update_schedule_staging`
- `class_start_times`
- `class_oog`
- `entry_go_times`
- `get_orders`
- `get_rings`
- alerts
- print/PDF/UI work

## Step 2 Contract

Step 2 is:

```text
hs_update_schedule only
```

Step 2 must:

- read eligible current-day rows from Catalyst `hs_get_ring_days`
- request HorseShowing `update_schedule.php` per eligible `ring_day_no`
- parse schedule rows
- upsert parsed rows into Catalyst `hs_update_schedule`
- upsert the same parsed rows into Airtable mirror `hs_update_schedule`
- write Step 2 proof to `hs_heartbeat`
- stop after `hs_update_schedule`

Step 2 must not write:

- Airtable `update_schedule`
- Airtable `update_schedule_staging`
- downstream staging tables

## Catalyst Tables Touched

The locked Step 1 / Step 2 cadence may touch only these Catalyst tables:

- `hs_heartbeat`
- `hs_get_ring_days`
- `hs_update_schedule`

## Airtable Mirror Tables Touched

The locked Step 1 / Step 2 cadence may touch only these Airtable mirror tables:

- `hs_heartbeat`
- `hs_get_ring_days`
- `hs_update_schedule`

## Preflight Rule

Step 2 reports preflight totals using this rule.

`is_preflight = true` when any of these are true:

- `time_text` is blank
- `class_no` is blank
- `class_no` is `0`
- `event_type = 5`
- `class_name` contains `Ticketed Schooling`
- `class_name` contains `Ticket School`

This classification is part of the Step 2 proof payload.

## Latest PASS Evidence

Latest locked Step 2 proof reported:

```text
raw_schedule_rows = 75
preflight_rows = 25
non_preflight_rows = 50
```

The same proof confirmed:

```text
downstream_run = false
get_orders_run = false
get_rings_run = false
alerts_run = false
```

## Explicit Stop Condition

The locked cadence stops after Step 2.

The stop point is:

```text
hs_update_schedule refreshed and Step 2 heartbeat proof written
```

No downstream stage is part of this locked contract.

## Lanes Not Allowed Yet

The following lanes are explicitly outside this locked contract:

- `update_schedule_staging`
- `class_start_times`
- `class_oog`
- `entry_go_times`
- `get_orders`
- `get_rings`
- alerts
- print/PDF/UI

These lanes must not run from `wec-cadence-step1-step2`.

## Next Required Gate Before Step 3

Before Step 3 begins, a separate approval must define and prove the next handoff contract.

The next gate must identify:

- producing stage/action
- consuming stage/action
- source Catalyst table
- destination Catalyst table
- Airtable mirror requirement, if any
- required keys
- required field mappings
- current-day filter
- pass counts
- stop condition
- lanes still forbidden

Step 3 must not be started from an ad hoc endpoint call, manual row creation, or downstream repair run.
