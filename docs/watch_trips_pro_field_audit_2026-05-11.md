# Watch Trips PRO Field Audit - 2026-05-11

This audit covers the `watch_trips` Airtable view `PRO` (`viwJdzwsu4jAlV0Cq`) and defines which duplicate-looking fields should be treated as the value of truth.

The word `inactive` here means field-ownership inactive for PRO decisions. It does not mean the row-level Airtable checkbox named `inactive`.

Complete field inventory: `docs/watch_trips_pro_field_inventory_2026-05-11.csv`.

## Live Audit Snapshot

Snapshot time: `2026-05-11T16:25:07.693Z`

```text
table: watch_trips
view: PRO
rows audited: 28
fields processed: 364
```

## All-Field Processing Result

Every field in the `watch_trips` table was processed against the live `PRO` rows and assigned a status in `docs/watch_trips_pro_field_inventory_2026-05-11.csv`.

| Status | Field count | Meaning |
| --- | ---: | --- |
| `value_of_truth` | 49 | Canonical field for a concept in PRO or row ownership. |
| `inactive_duplicate_or_lookup` | 36 | Duplicate, lookup, or legacy/source copy that should not be used as independent truth. |
| `inactive_blank_legacy_link` | 7 | Blank legacy link field in current PRO rows. |
| `source_evidence` | 29 | Endpoint, lookup, or evidence field used to explain a value, not replace truth. |
| `relationship_link` | 32 | Airtable link field; useful for joins, not scalar truth. |
| `operational_control` | 27 | Runner, heartbeat, scope, alert, or audit control field. |
| `derived_calculator_output` | 16 | Calculator output used for display/alerting only. |
| `derived_display_or_filter` | 109 | Formula or display/filter helper. |
| `blank_in_pro` | 56 | Empty on all 28 current PRO rows. |
| `active_publisher_field` | 3 | Still part of the publisher contract even though it is not a value-of-truth field. |

The inventory includes field type, Airtable field id, populated count, blank count, unique count, publisher-default flag, action, and top observed values.

| Field group | Rows where populated values matched | Mismatches | Blank rows | Finding |
| --- | ---: | ---: | ---: | --- |
| `completed_trips`, `rs_completed_trips`, `rs_completed_trips (from last_log)` | 26 | 0 | 2 | These are duplicate values in the current PRO sample. |
| `total_trips`, `total_trips (from last_log)` | 26 | 0 | 2 | These are duplicate values in the current PRO sample. |
| `gone_in`, `rs_gone_in` | 26 | 0 | 2 | These are duplicate values in the current PRO sample. |
| status family | 26 | 0 | 2 | These are duplicate values in the current PRO sample. |
| go-time family | 25 | 0 | 3 | Populated values match, but some source fields are blank. |
| start-time family | 26 | 2 | 0 | Mostly aligned, but source lookups can disagree. |
| order/go-order family | 16 | 9 | 3 | This is not a harmless duplicate family; it contains real disagreements. |

## Value Of Truth

| Concept | Value of truth for PRO | Inactive or evidence fields | Rule |
| --- | --- | --- | --- |
| completed trips | `completed_trips` | `rs_completed_trips`, `rs_completed_trips (from last_log)` | `completed_trips` is the canonical writable progress value populated by live/group enrichment. `rs_completed_trips` is a display/calculator duplicate when equal. The last-log lookup is history evidence only. |
| total trips | `total_trips` | `total_trips (from last_log)` | `total_trips` is the canonical writable count. The last-log lookup is history evidence only. |
| gone-in | `gone_in` | `rs_gone_in` | `gone_in` is the canonical writable live trip value. `rs_gone_in` is a display/calculator duplicate when equal. |
| status | `status` | `rs_status`, `latestStatus`, `latest_status`, `status (from groups_live)`, `status (from groups_live) 2` | `status` is the canonical writable state. Formula/display fields can remain visible for review but should not be treated as independent truth. |
| estimated start time | `estimated_start_time` | `estimated_start_time (from watch_schedule)`, `estimated_start_time (from watch_schedule) 2`, `estimated_start_time (from groups_live)`, `estimated_start_time (from groups_live) 2` | `estimated_start_time` is the canonical writable trip start time. Lookup fields are source evidence. If sources disagree, do not overwrite manual corrections blindly. |
| estimated go time | `estimated_go_time` | `latest_estimated_go_time`, `rs_go_time`, `rs_go_time (from last_log)` | `estimated_go_time` is the canonical writable trip go-time. `rs_go_time` is output/display when it matches. Last-log is history evidence only. |
| order of go for PRO display/export | `rs_order_of_go` | `order_of_go`, `rs_order_of_go (from last_log)`, `actual_order`, `rs_running_order_of_go`, `runningOOG` | For PRO display/export, use `rs_order_of_go` because `publisher.js` already emits it and current mismatches show it aligns with live `actual_order`. `order_of_go` is ingestion/pre-live evidence and is not truth when it disagrees. |
| live order evidence | `actual_order` | `order_of_go`, `rs_order_of_go (from last_log)` | `actual_order` is the raw live evidence from live trip enrichment. It should support `rs_order_of_go`, not replace the PRO export field unless publisher ownership changes. |

## Current Mismatch Evidence

The order/go-order family had 9 mismatches in 28 PRO rows. Examples:

| Record | `order_of_go` | `rs_order_of_go` | `actual_order` | Interpretation |
| --- | ---: | ---: | ---: | --- |
| `recNnpIlaVjSMmENb` | 6 | 5 | 5 | `rs_order_of_go` matches live `actual_order`; raw `order_of_go` is not the PRO truth. |
| `reclth6nCQmS8yWla` | 20 | 31 | 31 | `rs_order_of_go` matches live `actual_order`; raw `order_of_go` is stale or a different source order. |
| `recjIAy2WnQbJNkg6` | 26 | 22 | 22 | `rs_order_of_go` matches live `actual_order`; raw `order_of_go` is not the PRO truth. |
| `recmPYQp5tjhPix3K` | 40 | 39 | 39 | `rs_order_of_go` matches live `actual_order`; raw `order_of_go` is not the PRO truth. |
| `recrtqUs6deaNlo8p` | 6 | 4 | 4 | `rs_order_of_go` matches live `actual_order`; raw `order_of_go` is not the PRO truth. |

The start-time family had 2 mismatches in 28 PRO rows:

| Record | `estimated_start_time` | watch schedule lookup | groups live lookup | Interpretation |
| --- | --- | --- | --- | --- |
| `recGLPCE78dnOIe3A` | `09:05:00` | `09:05:00` | `09:00:13` | Canonical row value follows schedule/manual value, not the odd live lookup. |
| `recBVWTfS8T1yeaIY` | `10:17:00` | `10:17:00` | `10:40:09` | Canonical row value follows schedule/manual value, not the odd live lookup. |

## Publisher Contract Check

`publisher.js` currently includes these PRO trip fields by default:

```text
status
latestStatus
scope_status
estimated_start_time
estimated_go_time
completed_trips
rs_order_of_go
rs_go_time
rs_min_till_go
rs_gone_in
last_score
show_date
schedule_show_datev2
scheduled_date
app_sql_datev2
```

This confirms that the current PRO output contract already treats `completed_trips`, `estimated_start_time`, `estimated_go_time`, and `rs_order_of_go` as output-facing values. The audit should not flip PRO consumers to raw `order_of_go`; that is the clearest mismatch family.

## Operating Rule

When multiple PRO fields show the same value, use this pattern:

1. Keep one writable canonical field as `value_of_truth`.
2. Treat `rs_*` fields as display/output fields only when they duplicate the canonical value, except `rs_order_of_go`, which is the current PRO display/export truth.
3. Treat `(from last_log)` fields as inactive history lookups.
4. Treat `(from watch_schedule)` and `(from groups_live)` lookup fields as source evidence, not independent PRO truth.
5. Do not hide, rename, or delete fields during live operation; mark inactive status in this audit first, then clean the Airtable view only after downstream consumers are checked.
