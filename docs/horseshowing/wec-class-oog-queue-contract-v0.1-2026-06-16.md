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

## Queue Planner

Endpoint:

`horseshowing_class_oog_runner?plan=1&show_no={show_no}&focus_day={focus_day}&max_entries=50`

The planner returns chunks instead of running the fetch.

Chunk lanes:

- `active_trainers`
- `full`

The `active_trainers` lane is first priority. It is built from Airtable `trainers.active = checked` and current `class_oog` rows for the same `show_no` and `focus_day`.

Chunk sizing uses the larger of:

- `update_schedule_staging.entry_count`
- existing Airtable `class_oog` row count for that class

Default target:

`max_entries = 50`

## Chunk Execution

Endpoint:

`horseshowing_class_oog_runner?show_no={show_no}&focus_day={focus_day}&class_nos={class_no,class_no}`

This runs only the listed classes.

This is the preferred execution path for heartbeat/cron because it avoids long ring-wide runs and keeps active-trainer classes refreshed first.

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

Use the planner, run `active_trainers` chunks first, then continue through `full` chunks over time.
