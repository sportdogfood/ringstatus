# WEC class_oog Queue Contract v0.1

Date: 2026-06-16

## Source

`class_oog.php?class_no={class_no}`

Classes are selected from Airtable `update_schedule_staging`, scoped to:

- `show_no`
- `focus_day`
- `lock = checked`
- optional `ring_no`
- optional explicit `class_nos`

The approved class list is the same Airtable `update_schedule_staging` view used by `class_start_times`:

`lock_schedule`

No separate `full_lock`, planner-derived, raw schedule, or stale `class_oog` list is approved for the default class_oog probe/write lane.

## Destinations

- Catalyst: `hs_class_oog`
- Airtable mirror: `class_oog`

## Airtable Links Written

Each Airtable `class_oog` row links to:

- `shows`
- `focus_show`
- `classes`
- `rings`
- `ring_days`
- `entries`
- `horses`
- `riders`
- `trainers`
- `show_days`
- `update_schedule_staging`

The `update_schedule_staging` link is matched by the confirmed key:

`show_no|ring_day_no|class_no`

This allows `class_oog.lock (from update_schedule_staging)` to display from the linked staging record.

## Active-Trainer Local HTML Probe

Runner:

`docs/horseshowing/run-wec-catalyst-workflow.ps1`

Focused execution:

`-RunClassOogLocalProbeOnly`

The cadence runner reads all current `update_schedule_staging.lock_schedule` classes for the focus day, fetches each `class_oog.php?class_no={class_no}` page locally, scans the HTML order table for rows where `trainer` matches Airtable `trainers.active = checked`, then posts only the matching rows to Catalyst with:

`source=local_html_probe`

Catalyst remains the write/mirror endpoint for `hs_class_oog` and Airtable `class_oog`; the local runner is the approved repeatable fetch/probe path for this stage because Catalyst upstream HTML fetches timed out.

Default heartbeat behavior:

- probe every locked focus-day class in `lock_schedule`
- write only active-trainer `class_oog` rows
- clear stale `class_oog` mirror rows for each selected class context through the Catalyst writer

This removes the stale dependency where active-trainer class selection came from existing Airtable `class_oog` mirror rows.

## Unlocked-Class Safety Audit

Before removing `focus_show.is_pause`, run:

`docs/horseshowing/run-wec-catalyst-workflow.ps1 -RunClassOogUnlockedSafetyOnly`

This probes every current focus-day `update_schedule_staging` row that is:

- `class_no > 0`
- not present in `lock_schedule`

It uses the same local `class_oog.php` parser and the same `trainers.active = checked` match.

It does not write customer-facing `class_oog` rows.

If `update_schedule_staging.2nd_trip` is checked, the class is treated as an approved omission:

- it stays out of `lock_schedule`
- it stays out of `class_start_times`, mobile, and print
- active-trainer entries found there are logged as approved `2nd_trip`, not as blockers

It writes:

- `wec-logs.check_name = core_class_oog_safety`
- `wec-alerts.alert_type = class_oog_unlocked_active_entries` when active-trainer entries are found in omitted classes without `2nd_trip`
- `wec-alerts.alert_type = timed_unlocked_schedule_classes` when unlocked classes now have a populated `time_text` and are not marked `2nd_trip`

The pause should not be removed if this audit finds active-trainer entries in unlocked classes unless the operator explicitly accepts the omission by checking `2nd_trip`.

The same safety audit also runs in the normal cadence before `class_start_times`, `class_oog`, `entry_go_times`, live enrichment, alerts, and results. If active-trainer entries are found in unlocked classes, downstream class/entry stages are blocked for that run.

## Queue Planner

Endpoint:

`horseshowing_class_oog_runner?plan=1&show_no={show_no}&focus_day={focus_day}&max_entries=50`

The planner returns chunks instead of running the fetch. It is retained for diagnostics and explicitly approved backfill planning.

Chunk lanes returned by the planner:

- `active_trainers`
- `full`

The `active_trainers` lane is built from Airtable `trainers.active = checked` and current `class_oog` rows for the same `show_no` and `focus_day`, so it can be stale if class_oog has not already been probed/refreshed.

Default heartbeat execution must use `probe=active-trainers`, not planner-derived active chunks. It must not run the `full` lane unless the user explicitly approves a separate backfill/sweep.

Chunk sizing uses the larger of:

- `update_schedule_staging.entry_count`
- existing Airtable `class_oog` row count for that class

Default target:

`max_entries = 50`

## Chunk Execution

Endpoint:

`horseshowing_class_oog_runner?show_no={show_no}&focus_day={focus_day}&class_nos={class_no,class_no}`

This runs only the listed classes.

This remains the write path for heartbeat/cron after the probe returns matched class numbers.

## Verified Test

Show:

`14906`

Focus day:

`2026-06-14`

Verified state:

- planner target classes: `78`
- active-trainer classes: `22`
- planner chunks at `max_entries=50`: `30`
- active-trainer chunks: `8`
- full chunks: `22`
- first active chunk: `29100`
- first active chunk rows written: `30`
- Catalyst `hs_class_oog`: `844`
- Airtable `class_oog`: `844`
- missing `update_schedule_staging` links: `0`
- missing `lock` lookup: `0`

## Rule

Do not run full class_oog focus-day enrichment as one large job.

Use `probe=active-trainers`, run full `class_oog` only for probe-positive classes, and keep the `full` lane out of the customer-facing class workflow unless explicitly approved.
