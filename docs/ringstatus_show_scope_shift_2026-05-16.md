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
- `shifted_to_next_day`
- `set_to_default_app_sql_date`
- `mode_control`
- `is_default_show_manual_override`

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

Exactly one `show` record should have `heartbeat` checked for a single-scope run. If there are zero or multiple focused records, heartbeat and heavy lanes should fail before writing downstream rows.

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

Trusted focus fields on heartbeat:

- `app_show_id`: current focused show id for this heartbeat
- `app_sql_date`: current focused schedule date for this heartbeat
- `customer_id`: copied from `show`
- `focus_day`: copied from `show`
- `show_scope_key`: copied from `show`

Mode is still used for cadence and lane timing. Mode must not guess the focused show/date when `show` has explicit controls.

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
Create one thread when a watched class reaches each configured pre-start timing milestone.
```

Operational source:

- `active_alerts.alert_id = class_tills`
- `active_alerts.active = true`
- linked `trigger_tags` define the actual timing windows
- `watch_schedule` supplies the class row and copied scope fields
- `schedules_calculatorv2.js` owns detection and direct `thread_logs` creation

The workflow must not depend on Airtable Automations for deciding whether an alert fired or for creating the durable thread event. Airtable Automations may be used later only as optional notification/display plumbing after the runner has created the idempotent thread row.

Current required copied scope inputs:

- `customer_id`
- `focus_day`
- `ring_collection`
- `show_scope_key`

The thread idempotency key is:

```text
thread_static_id = show_scope_key|schedule_key|class_tills|trigger_name
```

Examples of `trigger_name` values are the linked `trigger_tags` records, such as `run_60_till` or `run_45_till`. If the desired business milestone is 40 minutes instead of 45 minutes, update the Airtable `trigger_tags` configuration first; do not make the calculator substitute 40 for an Airtable tag that still says 45.

The runner writes `thread_logs` directly with available linked context:

- `thread_static_id`
- `thread_source`
- `thread_name`
- `run_id`
- `run_time`
- `active_alerts`
- `alert_templates`
- `active_tenants`
- `heartbeat`
- `watch_schedule`
- `schedule_logs`
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

## Open Work

1. Make all heavy lanes derive customer/show/day scope from `show` or heartbeat copied fields, not hardcoded defaults.
2. Finish `watch_rings` bridge: `show -> watch_rings -> ww_rings`.
3. Link `watch_schedule <-> watch_rings` using focused show/day/ring data.
4. Decide whether `classsignup_url` remains reference-only or gets a tested operational role.
5. Remove or ignore deprecated `LiveClassData` in downstream app/feed surfaces.
6. Add recurrent validation that every active downstream record has `customer_id`, `focus_day`, `ring_collection`, `show_scope_key`, and `show`.
