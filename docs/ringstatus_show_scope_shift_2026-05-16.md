# RingStatus Focused Show Scope Shift

**Version:** v2026.05.16.1
**Date:** 2026-05-16
**Status:** Active scope contract
**Owner review required:** Yes, before changing source-of-truth fields, endpoint formulas, runner scope resolution, or active-record filters.

## Purpose

This document defines the major operating shift from endpoint-default guessing to a manually managed focused `show` mandate.

The core change is that the `show` table is now the earliest trusted decision point for the active RingStatus scope. Endpoint defaults, heartbeat rows, `shows` rows, and lookup fields may provide evidence or execution state, but they do not own the focused show/day/customer decision.

## Absolute Rule

Never invent or substitute dates, show IDs, customer IDs, endpoints, field names, or payload values.

Every operational value must come from one of these confirmed sources:

- user-entered `show` table fields
- live Airtable metadata or row data
- source payload data
- an existing local file path
- explicit environment/config input
- deterministic derivation from confirmed inputs

If a required value is missing, keep it missing, log it, and stop the affected lane from making destructive writes.

## Source Of Truth

### `show`

`show` is the focused source-of-truth table. It is manually managed.

Minimum owner-managed fields:

- `show_id`
- `customer_id`
- `focus_day`
- `heartbeat`

Optional owner-managed controls:

- `shifted_to_next_day`
- `set_to_default_app_sql_date`
- `mode_control`
- `is_default_show_manual_override`
- `manual_day_count`

Generated or enriched fields may include:

- `start_date`
- `end_date`
- `show_name`
- `customers`
- `ring_collection`
- `last_run_at`
- `last_run_epoch`
- `last_run_status`
- `last_run_scope_key`
- `last_run_detail`
- `last_error_at`
- `last_error_stage`
- `last_error_message`

The focused key is:

```text
show_scope_key = customer_id|show_id|focus_day
```

Example from the current focused show:

```text
10002|200000006|2026-05-16
```

Each active show is represented by one `show` record with `heartbeat` checked. Multi-show operation is one heartbeat/pipeline pass per active `show` record.

An active `show` row must also be inside its stated date window when both `start_date` and `end_date` are present. The link-carrying rule is:

```text
start_date <= today <= end_date
```

If today is before `start_date` or after `end_date`, that `show` row is staged or expired even if `heartbeat` remains checked. It should not receive a fresh heartbeat link or relink downstream rows. If no checked rows qualify after this date-window filter, tagger writes `scope_status = no-active-feeds`.

If the `show` heartbeat view has zero active rows, tagger still writes a heartbeat record with `scope_status = no-active-feeds` and skips downstream schedule/trips/rings relinks and heavy work for that pass.

`manual_day_count` is manually owned. It is the expected inclusive count of show days from `start_date` through `end_date`; for example Wednesday through Sunday is `5`. Because off-season show windows vary, the code must compare against this field when present and surface `manual_day_count_mismatch` as suspicious evidence. The code must not invent the expected day count when the field is blank.

### `customers`

`customers` owns customer-specific metadata, not the active show/day decision.

Confirmed fields:

- `customer_id`

Endpoint helper fields may be used only after their exact field names and values are verified in live Airtable schema. Do not assume a customer-specific endpoint field name from memory or convention.

The pipeline may use customer metadata to construct endpoints only after the active `customer_id` has been resolved from `show`.

### `shows`

`shows` remains a legacy/show-history workflow table. Continue using it where existing lanes require it, but it must not supersede `show` as the focused starting point.

`shows` may still receive heartbeat links and show metadata, but the source-of-truth scope comes from `show`.

## Heartbeat Contract

Heartbeat is an execution snapshot and cadence pointer. It is not durable identity.

Heartbeat must copy the focused show values into writable fields:

- `customer_id`
- `focus_day`
- `ring_collection`
- `show_scope_key`
- `show`
- `scope_status` only when the run has no active focused feeds

Trusted focus fields on heartbeat:

- `app_show_id`: current focused show id for this heartbeat
- `app_sql_date`: current focused schedule date for this heartbeat
- `customer_id`: copied from `show`
- `focus_day`: copied from `show`
- `show_scope_key`: copied from `show`

Mode is still used for cadence and lane timing. Mode must not guess the focused show/date when `show` has explicit controls.

`mode_control` now lives on `show`. Stale log labels using `shows_*` should be treated as legacy naming only and replaced with `show_*` naming during cleanup.

## Downstream Copied Scope Fields

Because heartbeat records clear and reappear, downstream tables must receive copied row-owned scope fields. Do not rely on heartbeat lookups as durable identity.

Required copied scope fields on active downstream tables:

- `customer_id`
- `focus_day`
- `ring_collection`
- `show_scope_key`
- `show`

Tables in scope:

- `watch_schedule`
- `watch_trips`
- `watch_rings`
- `active_tenants`

The copied fields are operational filters and diagnostics. They do not replace the row-specific schedule/trip keys.

## Active And Archive Semantics

Use `archive` for owner cleanup.

- `archive = true`: record is outside the active focused scope or should be manually removed later.
- `inactive = true`: preserve existing workflow semantics for dropped/inactive rows.

Do not use `inactive` as the new broad archive marker. The owner may manually delete rows marked `archive = true`.

## Table Roles

### `watch_schedule`

`watch_schedule` owns schedule/class/ring/day rows.

Core responsibilities:

- one row per schedule class occurrence
- schedule-side key and short key
- ring/date/class identity
- estimated start time and group/ring enrichment
- links to heartbeat, show, watch rings, ww rings, and trips when available

Operational matching should use writable schedule keys and row-owned scope fields, not formula/audit keys.

### `watch_trips`

`watch_trips` owns person/horse/rider/entry participation rows.

Core responsibilities:

- one row per trip candidate for a focused schedule class
- link to `watch_schedule` where possible
- trip key and short key
- copied show/customer/focus fields for filtering
- live enrichment fields such as order, gone, actual order, and liveclass evidence

`watch_trips` should consume schedule context from `watch_schedule` rather than becoming a second independent schedule owner.

### `watch_rings`

`watch_rings` owns focused show/day ring state.

Desired key:

```text
watch_rings_id = customer_id|show_id|focus_day|ring_number
```

It should bridge focused show/day scope to seasonal ring metadata in `ww_rings`.

### `ww_rings`

`ww_rings` owns seasonal/customer ring definitions.

Accepted ring collection key shape:

```text
ring_collection_key = customer_id|ring_collection|ring_number
```

Examples:

```text
10002|sfhja_2026_spring|1
10002|sfhja_2026_spring|2
10002|sfhja_2026_spring|3
10002|sfhja_2026_spring|4
```

### `active_tenants`

`active_tenants` owns which people/tenants should be pulled for the focused scope.

It must carry copied scope fields:

- `customer_id`
- `focus_day`
- `ring_collection`
- `show_scope_key`
- `show`

Trip lanes must use the focused customer when constructing `/people/{pid}` endpoints.

### `active_alerts`: `class_tills`

`class_tills` is the first calculator-owned triggered alert workflow.

Intent:

```text
Create one thread for one subscribed profile when a watched class reaches that profile's configured pre-start timing milestone and has tenant-related trips.
```

Operational source:

- `active_alerts` view `active_alerts`
- `active_alerts.rec_name = class_tills` or `active_alerts.alert_id` starts with `class_tills_`
- `active_alerts.active = true`
- linked `trigger_tags` identify the alert lane; dynamic profile milestones define the actual timing window
- linked `ww_profiles` define which profiles are subscribed to that alert lane
- `watch_schedule` supplies the class clock and copied scope fields
- linked `watch_trips` supply the tenant trip rollup and profile trip eligibility
- `schedules_calculatorv2.js` owns detection and direct `thread_logs` creation

The workflow must not depend on Airtable Automations for deciding whether an alert fired or for creating the durable thread event. Airtable Automations may be used later only as optional notification/display plumbing after the runner has created the idempotent thread row.

Current required copied scope inputs:

- `customer_id`
- `focus_day`
- `ring_collection`
- `show_scope_key`

The thread idempotency key is:

```text
thread_static_id = show_scope_key|schedule_key|alert_id|milestone_slot|tenant_profile_key
```

Dynamic profile milestone examples:

```text
show_scope_key|schedule_key|class_tills_milestone1|alert_milestone1|tenant_profile_key
show_scope_key|schedule_key|class_tills_milestone2|alert_milestone2|tenant_profile_key
```

Each split `active_alerts` record carries its own linked `trigger_tags`, `alert_templates`, `active_tenants`, `ww_tenants`, and subscribed `ww_profiles`.

For `alert_id = class_tills_milestone1`, `schedules_calculatorv2.js` reads `alert_milestone1` from each linked `ww_profiles` record.

For `alert_id = class_tills_milestone2`, `schedules_calculatorv2.js` reads `alert_milestone2` from each linked `ww_profiles` record.

When active dynamic milestone records exist, they take precedence over fixed `class_tills_45` / `class_tills_60` records. This avoids duplicate thread creation while allowing the old fixed records to remain in Airtable during transition.

The dynamic runner treats a milestone as hit when `rs_mins_till_start` falls into this cadence window:

```text
milestone - 4 <= rs_mins_till_start <= milestone + 1
```

For example, profile milestone `65` listens at `61..66`; profile milestone `38` listens at `34..39`.

Eligibility:

- no linked `watch_trips`, no alert
- no tenant-matching trips, no alert
- no linked subscribed `ww_profiles`, no alert
- no active `active_subscribers` on the subscribed profile, no alert
- no profile milestone value for the lane, no alert
- if the profile has rider/trainer/groom/horse scope links, only trips matching that profile scope qualify
- if the profile has no rider/trainer/groom/horse scope links, the tenant trip rollup qualifies

The runner writes `thread_logs` directly with available linked context:

- `thread_static_id`
- `thread_source`
- `thread_name`
- `run_id`
- `run_time`
- `active_alerts`
- `alert_templates`
- `active_tenants`
- `ww_tenants`
- `ww_profiles`
- `active_subscribers`
- `trigger_tags`
- `heartbeat`
- `watch_schedule`
- `watch_trips`
- `schedule_logs`
- `qualified_trip_count`
- `alert_scope`
- `tenant_profile_key`
- `customer_id`
- `show_id`
- `focus_day`
- `ring_collection`
- `show_scope_key`
- `show`
- `alert_milestone_slot`
- `alert_milestone_value`
- timing/status fields such as `rs_start_time`, `rs_mins_till_start`, `rs_status`
- the fired trigger checkbox such as `run_60_till` or `run_45_till`

If `class_tills` is not active, its linked trigger tags must not create schedule-log trigger hits or thread rows.

## Endpoint Ownership

### Schedule

Use focused show/customer/day:

```text
/schedule?date={focus_day}&show_id={show_id}&customer_id={customer_id}
```

No schedule endpoint may use a stale hardcoded customer id after `show.customer_id` is resolved.

### People

Use focused show/customer:

```text
/people/{pid}?pid={pid}&show_id={show_id}&customer_id={customer_id}
```

`people_endpointv2` may act as a row/audit endpoint if populated, but the writer must still derive operational scope from `show`/heartbeat unless explicitly changed and tested.

### Liveclass

`watch_trips.getLiveClassData` is the operational row-level liveclass endpoint field.

`LiveClassData` is deprecated and reference-only. It must not drive matching, enrichment, endpoint construction, or pipeline decisions.

The liveclass base URI must come from confirmed customer metadata or explicit config after `show.customer_id` has been resolved. Do not infer the customer path from endpoint defaults or previous runs.

`getLiveClassData` must include a usable `cid`:

```text
getLiveClassData?show_id={show_id}&cid={class_id}&cgid={class_group_id}
```

If `cid` is blank, the endpoint is not valid for live trip enrichment. The row should log `err:missing_liveclass_mapping` and remain intact for the next valid live pass.

### Classsignup

`classsignup_url` is evidence/reference unless explicitly promoted in a future scope update.

It is not a fallback for missing `getLiveClassData`.

If `classsignup_url` is retained, the formula must produce valid query strings. For example:

```text
customer_id=10002&class_number=311
```

not:

```text
customer_id=10002class_number=311
```

Even with a valid URL, classsignup payloads must be considered usable only when they contain usable entry identity fields.

## Runner And GitHub Workflow

All runner paths must resolve focused scope before running pipeline scripts.

Required exported environment:

- `CUSTOMER_ID`
- `HEARTBEAT_TARGET_CUSTOMER_ID`
- `HEARTBEAT_TARGET_APP_SHOW_ID`
- `HEARTBEAT_TARGET_SHOW_RECORD_ID`
- `HEARTBEAT_TARGET_SQL_DATES`

The GitHub workflow must not hardcode:

```text
CUSTOMER_ID=15
```

The local PowerShell runner and GitHub workflow should both use the same focused `show` contract.

## Current Validation Snapshot

Confirmed on 2026-05-16:

- focused `show` row: `show_id=200000006`, `customer_id=10002`, `focus_day=2026-05-16`
- `watch_schedule` heartbeat view had 145 scoped rows with copied scope fields populated
- `watch_trips` heartbeat view had 34 scoped rows with copied scope fields populated after view filter correction
- `active_tenants` heartbeat view had 2 rows with copied scope fields populated
- `watch_rings` had 0 rows in the heartbeat view at the time of validation
- `watch_trips.getLiveClassData` had 34 populated rows and 18 unique URLs; 17 unique URLs returned liveclass JSON with rows, 1 had blank `cid`
- `watch_trips.LiveClassData` is deprecated/reference-only
- `watch_trips.classsignup_url` was populated, but current formula output was malformed and not operational

## Nightly Handoff Runbook

Before any future DAY/NIGHT transition diagnosis, cleanup, or live write, use:

```text
docs/ringstatus_nightly_handoff_runbook_2026-05-16.md
```

This runbook is now the preflight checklist for repeated nightly workflow issues. It records the 2026-05-16 -> 2026-05-17 `watch_trips` incident, including the exact stale keys that remained active because they were hidden from the writer view during the prior cleanup window and then protected by the current-date cross-date drop guard.

## Open Work

1. Make all heavy lanes derive customer/show/day scope from `show` or heartbeat copied fields, not hardcoded defaults.
2. Finish `watch_rings` bridge: `show -> watch_rings -> ww_rings`.
3. Link `watch_schedule <-> watch_rings` using focused show/day/ring data.
4. Decide whether `classsignup_url` remains reference-only or gets a tested operational role.
5. Remove or ignore deprecated `LiveClassData` in downstream app/feed surfaces.
6. Add recurrent validation that every active downstream record has `customer_id`, `focus_day`, `ring_collection`, `show_scope_key`, and `show`.
7. Add a dedicated stale prior-date `watch_trips` cleanup lane with explicit `show_id`, `customer_id`, and `cleanup_date` inputs; do not rely on the current focused-date trip refresh to clean prior-date rows.
