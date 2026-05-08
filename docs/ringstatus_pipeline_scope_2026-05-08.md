# RingStatus Pipeline Scope

**Version:** v2026.05.08.1  
**Date:** 2026-05-08  
**Status:** EVOLVING PIPELINE  
**Owner review required:** Yes, before changing cadence, identifiers, writable fields, or live endpoint behavior.

## Alert

This is an evolving pipeline document. It is not a final stable contract.

This scope is intentionally date-bound. Before using it, compare the document date to the current local date in `America/New_York`.

If this document date is older than today, stop. Refresh this scope document before changing code, running new live lanes, changing cadence, or interpreting pipeline behavior.

Every material pipeline change should update this document with:

- a new version
- a date
- a short changelog entry
- the affected source endpoint or Airtable table
- whether the change affects live writes, publisher output, or alerting logic

Do not treat inferred behavior as permanent. If SGL payload shape changes, log the mismatch and preserve existing Airtable data.

## Changelog

| Version | Date | Change |
| --- | --- | --- |
| v2026.05.08.1 | 2026-05-08 | Refreshed daily scope; added stale-document stop rule; clarified same-day live gating through `getLiveClassStatus`, `ListAjax`, matching `groups_live.day`, and `groups_live.has_JSON`; clarified `getLiveClassData` is active for same-day trip enrichment while `ClassStatus` remains a documented but not-yet-wired status overlay. |
| v2026.05.07.1 | 2026-05-07 | Initial documented scope for split heartbeat, pre-live trip population, same-day liveclassv2 enrichment, groups_live gating, and trip identity rules. |

## Purpose

This document defines the current RingStatus pipeline scope for:

- heartbeat cadence
- pre-live schedule and trip population
- same-day live enrichment
- SGL endpoint usage
- Airtable write protection
- critical field ownership

The goal is to populate useful records before live data exists, then enrich those records safely once same-day live data becomes available.

## Non-Negotiable Rules

- `app_sid`, `app_show_id`, `app_sql_date`, and `app_sql_datev2` are literal app/heartbeat values.
- Do not normalize or timezone-shift `app_sql_date`.
- Do not globally clear fields from `watch_trips` or `watch_schedule`.
- Stale data should be handled at the record, lane, or scope level, not by clearing individual fields.
- `{}` is never a successful SGL payload.
- Small or shape-mismatched payloads must be logged and blocked from destructive writes.
- Existing good Airtable data must not be overwritten by empty, null, or soft-throttled payloads.
- Shows link resolution failures should be logged and the lane should continue where safe.
- Do not add Airtable fields unless the owner approves the field and purpose.

## Pipeline States

### 1. Heartbeat

The heartbeat owns app scope and freshness.

Current expected responsibilities:

- create/update heartbeat records
- maintain app scope values
- expose slot flags such as `isA`, `isB`, `isC`, and `isD`
- decide whether heavier lanes are due
- avoid running all SGL fetch lanes on every heartbeat

The heartbeat must not blindly run schedule, trip, class detail, or live detail endpoints every 5 minutes.

### 2. Pre-Live

Pre-live means before same-day live data is published or before `groups_live.has_JSON = true`.

Available and expected sources:

- `/schedule`
- `/people/{pid}`
- Airtable `watch_schedule`
- Airtable app scope from `heartbeat`

Pre-live goals:

- create `watch_schedule` rows
- create `watch_trips` rows for known people/trips
- link trips to schedule rows when possible
- preserve available class, ring, date, horse, rider, and entry data
- preserve estimated start data when available
- do not require order-of-go or gone-in data yet

Pre-live should treat missing live-only fields as expected, not as a hard failure.

Fields that may be available pre-live:

- `class_group_id`
- `class_groupxclasses_id`
- `class_number`
- `class_name`
- `group_name`
- `ring_number`
- `schedule_show_datev2`
- `estimated_start_time`, when present
- `total_trips`, when present
- `pid`
- `entry_number`
- `horse`
- `rider_name`
- real `entryxclasses_uuid`, when SGL provides it

Fields that are usually not reliable pre-live:

- `order_of_go`
- `gone_in`
- `completed_trips`
- live `status`
- `actual_order`
- live class progress

### 3. Live-Ready

Live-ready means the class/group is day-of and live data is published.

Live feed availability gate:

```text
/homepage/getLiveClassStatus?datetimestamp={CACHE_BUSTER}&customer_id=15
```

This endpoint must return `true` before the liveclassv2 same-day lanes are considered available.

Group feed source:

```text
/iphonev2/index.php/esp/liveclassv2/ListAjax?from_wp_api=true
```

This populates `groups_live`.

Row-level gates:

```text
groups_live.show_id = app_sid
groups_live.day = app_sql_date
groups_live.has_JSON = true
```

If a class or class group is in `groups_live` and `has_JSON = true`, live detail endpoints can be pinged.

If a class or class group is not in `groups_live`, or `has_JSON` is not true, the live detail lane should not probe it repeatedly.

Same-day live data must not be used to populate a shifted next-day scope. When `DAY -> NIGHT` and `shifted_to_next_day = true`, the next-day scope should use pre-live sources only.

Same-day live enrichment should prioritize:

1. classes represented in current-scope `watch_trips`
2. classes/groups with `groups_live.has_JSON = true`
3. classes linked through current `watch_schedule`

Do not spend same-day live endpoint calls on classes with no trip interest unless the owner explicitly expands scope.

### 4. Soft-Blocked

Soft-blocked means the endpoint returned HTTP success but the payload is not valid for the lane.

Examples:

- body parses as `{}`
- body is unexpectedly small for that endpoint family
- expected top-level keys are missing
- requested show/date/class/group does not match returned payload
- payload is an error object

Soft-blocked behavior:

- log the endpoint and reason
- do not write empty values
- do not clear existing fields
- block downstream calculator and publisher for that lane when the failed payload is required

## Source Authority

| Source | Timing | Role | Notes |
| --- | --- | --- | --- |
| `heartbeat` Airtable table | every heartbeat | app scope and cadence state | Owns literal app values. |
| `/schedule?date=...` | pre-live and day-of | schedule classes, groups, ring/date context | Invoked one day at a time. May have `class_id = null`; use fallback keys. Cache successful daily payloads through show `end_date` for fallback. |
| `/people/{pid}` | pre-live and day-of | person trip rows | Full-week payload for a person/show. May omit `entryxclasses_uuid`, `entry_id`, and `class_id`. Cache latest successful payload for fallback while continuing to look for fresher data. |
| `getLiveClassStatus` homepage endpoint | day-of only | live feed availability gate | Must return `true` before liveclassv2 lanes are considered available. |
| `groups_live` Airtable table | day-of | live availability and group status | `has_JSON = true` gates live detail pings. |
| `ListAjax` liveclassv2 endpoint | day-of only | source feed for `groups_live` | Must match expected show/day before downstream live enrichment. |
| `ClassStatus` liveclassv2 endpoint | day-of only | group/class live status | Useful for status, gone, total, estimated start, ring/date. |
| `getLiveClassData` liveclassv2 endpoint | day-of only | class live trip rows | Useful for order, gone-in, actual order, rider/horse/entry number. |
| `/classes/{class_id}` | day-of/enrichment | class detail | Not reliable when `class_id` is missing. Should not run every heartbeat. |
| `/classsignup/{class_group_id}` | day-of/enrichment | order fallback when usable | Payload may contain unusable/null entry fields. Validate shape. |

## Identity Model

### watch_schedule

Preferred machine key:

```text
{class_group_id}_{class_number}
```

Fallback keys:

```text
class_groupxclasses_id
class_id
```

Reason:

Current schedules may return `class_id = null`, while `class_group_id` and `class_number` remain usable.

### watch_trips

Preferred trip key:

```text
people:{class_number}:{entry_number}
```

Backup:

```text
entryxclasses_uuid
```

Rules:

- `entryxclasses_uuid` should contain a real SGL value only.
- Synthetic `fallback:*` values should not be created for new rows.
- Existing legacy `fallback:*` values should be tolerated during transition if `class_number + entry_number` is available.
- Enrichment should match by class/entry identity first, and use `entryxclasses_uuid` only as a backup.

## Same-Day Liveclassv2 Endpoints

These endpoints are same-day only. They should not be used for pre-live population.

## Pre-Live Payload Cache

The schedule endpoint is day-specific, while a show has a clear start and end date.

Example:

```text
show_id = 200000061
show start_date = 2026-05-06
show end_date = 2026-05-10
schedule endpoint date = 2026-05-08
```

Schedule fetch pattern:

```text
https://sglapi.wellingtoninternational.com/schedule?date={YYYY-MM-DD}&show_id={SHOW_ID}&customer_id=15
```

When a schedule payload succeeds for a show, the schedule lane should opportunistically try each subsequent show day through `end_date`, subject to cadence and throttling guards.

For show `200000061`, useful daily schedule cache targets are:

```text
2026-05-06
2026-05-07
2026-05-08
2026-05-09
2026-05-10
```

Successful schedule payloads should be stored as temporary JSON fallback files. These fallback files are not the source of truth when fresh SGL data is available, but they are useful when a needed schedule payload later fails, soft-throttles, or returns an unusable body.

The `/people/{pid}` endpoint is show/week scoped rather than day scoped. A single successful people payload can contain enough full-week trip detail to help pre-live trip creation and fallback matching.

People payload rule:

- keep trying for fresh people data because it changes frequently
- store the last successful people payload as fallback
- never let an old fallback overwrite newer successful data
- log when fallback data is used
- treat fallback data as pre-live support, not same-day live authority

Suggested fallback file families:

```text
manual_sgl_payloads/schedule_show-{SHOW_ID}-{YYYY-MM-DD}-{FETCH_EPOCH}-customer-15-fetched-meta.json
manual_sgl_payloads/people_show-pid-{PID}-{SHOW_ID}-{FETCH_DATE}-{FETCH_EPOCH}-customer-15-fetched-meta.json
```

The final managed cache location and retention policy should be approved before making the fallback automatic.

Manual payload folder:

```text
C:\actions-runner\ringstatus\manual_sgl_payloads\
```

Manual files in this folder are last-resort fallback inputs when the normal SGL fetch path cannot obtain a usable payload at the time the pipeline needs it.

Known manual HTML examples:

```text
C:\actions-runner\ringstatus\manual_sgl_payloads\schedule_html_show-schedule_show-200000061-2026-05-07-1778196302.html
C:\actions-runner\ringstatus\manual_sgl_payloads\schedule_html_show-people_show-pid-8778-200000061-2026-05-07-1778196302.html
```

Derived schedule HTML fallback example:

```text
C:\actions-runner\ringstatus\manual_sgl_payloads\schedule_html_show-schedule_html-200000061-2026-05-08-1778245924.html
```

The derived schedule HTML fallback can be used to form the basic schedule when API schedule data is unavailable. It should be parsed conservatively for:

- `class_group_id`, usually from `cgid=` in schedule links
- `class_id`, when `cid=` exists
- `class_number`, usually from class text such as `[701]` or grouped rows
- `class_name`
- `ring_number`, usually from `ring=` in links or the table/ring header context
- `estimated_start_time`, normalized to `HH:MM:SS`
- entries/total trips when visible

Manual HTML fallback rules:

- use fresh API JSON first when available
- use cached successful JSON before manual HTML when JSON is newer and valid
- use manual HTML only as a last resort for missing schedule basics
- log the manual file path and extracted row count when used
- do not overwrite newer API-derived values with older manual HTML
- do not treat manual HTML as a same-day live source

### Live Feed Availability

Pattern:

```text
https://sglapi.wellingtoninternational.com/homepage/getLiveClassStatus?datetimestamp={CACHE_BUSTER}&customer_id=15
```

Expected result:

```text
true
```

If this does not return `true`, same-day liveclassv2 detail pings should not run.

### ListAjax

Pattern:

```text
https://sgl.wellingtoninternational.com/iphonev2/index.php/esp/liveclassv2/ListAjax?from_wp_api=true
```

Current role:

- feeds Airtable `groups_live`
- provides group-level live status fields
- identifies which class groups have JSON detail available through `has_JSON`
- provides class ids used to build `getLiveClassData` requests

Required validation:

- returned show id must match current app show
- returned day must match current `app_sql_date`
- `has_JSON` must be true before detail pings run

### ClassStatus

Pattern:

```text
https://sgl.wellingtoninternational.com/iphonev2/index.php/esp/liveclassv2/ClassStatus?from_wp_api=true&class_group_id={CLASS_GROUP_ID}&show_id={SHOW_ID}&from_live_class=true
```

Alternate observed form:

```text
https://sgl.wellingtoninternational.com/iphonev2/index.php/esp/liveclassv2/ClassStatus?from_wp_api=true&class_group_id={CLASS_GROUP_ID}&class_id={CLASS_ID}&show_id={SHOW_ID}&from_live_class=0
```

Current implementation note:

This endpoint is documented as a desired same-day group/class status overlay, but it is not yet wired into executable Node lanes. Current executable group-level enrichment comes from `groups_live`, populated by `ListAjax`.

Observed useful fields:

| Payload field | Meaning | Target use |
| --- | --- | --- |
| `is_live` | live flag | gate/diagnostic |
| `class_id` | requested class id | validation |
| `class_group_id` | requested class group id | validation |
| `ring_no` | ring number | `ring_number` fallback |
| `live_classGroup.day` | show day | row-level date fallback only |
| `live_classGroup.estimated_start_time` | live group start | `estimated_start_time` fallback |
| `live_classGroup.status` | live status | `status` |
| `live_classGroup.gone` | completed trips | `completed_trips` |
| `live_classGroup.total` | total trips | `total_trips` |
| `live_classGroup.has_JSON` | live detail availability | gate detail ping |

### getLiveClassData

Pattern:

```text
https://sgl.wellingtoninternational.com/iphonev2/index.php/esp/liveclassv2/getLiveClassData?show_id={SHOW_ID}&cid={CLASS_ID}&t={CACHE_BUSTER}
```

Confirmed identifier behavior:

| Payload field | Meaning |
| --- | --- |
| top-level `ID` | requested `cid`, so treat as `class_id` |
| top-level `recs` | total row count for returned rows |
| `rows[].id` | unique live trip row id / entry-class candidate, not class id |
| `rows[].ENo` | entry number |

Current mapped fields:

| Payload field | Target concept |
| --- | --- |
| `ID` | `class_id` / `cid` validation |
| `recs` | `total_records` diagnostic |
| `ring_number` | `ring_number` |
| `rows[].id` | live trip id candidate, diagnostic until field approved |
| `rows[].ENo` | `entry_number` / `h_eid` |
| `rows[].Hor` | `horse` |
| `rows[].Rid` | `rider_name` |
| `rows[].OOG` | `order_of_go` |
| `rows[].Gone` | `gone_in` |
| `rows[].Actual_OOG` | `actual_order` |
| `rows[].Pos` | placing/position candidate |

Ignored for now:

- `R2_OOG`
- `R2_Qualified`
- `R2_Upcoming`
- `Actual_OOG_R2`
- FEI/country/owner/trainer fields
- team fields

Open mapping question:

```text
Scr
```

The user marked `Scr` as `score`, but observed payload also has a separate `Score` field. Do not write `Scr` to `score` until this is confirmed.

## Priority Classes

Priority classes are classes where RingStatus has trip interest.

Practical priority rule:

```text
current watch_trips rows
joined to watch_schedule by class_number + show/date/group
joined to groups_live by class_group_id + date
where groups_live.has_JSON = true
```

Only these should enter live detail enrichment by default.

## Critical Fields

The most critical fields for downstream workflow are:

- `order_of_go`
- `estimated_start_time`
- `total_trips`
- `completed_trips`
- `status`
- `gone_in`
- `actual_order`

These fields may be updated when a valid source provides a value.

They must not be cleared just because a later source does not provide them.

## Cadence Model

The heartbeat can remain frequent, but heavy SGL fetches should not run on every heartbeat.

Current intended cadence:

- heartbeat: frequent app scope update
- schedule daily refresh: slower cadence and scope-change driven
- trips daily refresh: slower cadence and scope-change driven
- liveclassv2 enrichment: same-day only, gated by `groups_live.has_JSON = true`
- publisher: only after upstream fetches are clean and data changed

Heartbeat slot flags such as `isA`, `isB`, `isC`, and `isD` may be used to stagger lanes without adding more timers.

When heartbeat mode shifts to `NIGHT` and `shifted_to_next_day = true`, the pipeline should treat the target scope as pre-live unless the target day is also the actual live day. Tomorrow's schedule/trip rows should not depend on liveclassv2.

## Implementation Status

Implemented or in progress:

- schedule rows can use `class_group_id + class_number` when `class_id` is null
- `groups_live` can fallback schedule date, ring, start, status, total, and gone values
- `watch_trips` identity can prefer `people:{class_number}:{entry_number}`
- `entryxclasses_uuid` is now backup identity rather than the primary synthetic key
- protected fields are guarded against global clearing
- soft payload checks exist and should block destructive downstream work
- same-day `getLiveClassData` enrichment is active in `trips_tagger.js` when `groups_live.has_JSON = true`
- `classsignup` fallback is lazy and should only be used after stronger same-day live sources fail to provide `order_of_go`
- split orchestration should run `schedules_calculatorv2.js` after due schedule refresh slots so `groups_live` overlays are promoted

Not yet fully implemented:

- executable `getLiveClassStatus` gate
- same-day `ClassStatus` ingestion lane
- runtime logging of this document version
- owner-approved storage field for `rows[].id`, if needed
- final mapping decision for `Scr`

## Required Logging

Every lane touching SGL should log:

- pipeline scope version
- lane name
- endpoint family
- URL or URL family
- show id
- app sql date
- class id, if present
- class group id, if present
- status code
- body length
- parsed top-level keys
- validation result
- rows matched
- rows skipped
- soft failure reason
- writes planned
- writes completed
- downstream blocked or allowed

For this document version:

```text
pipeline_scope_version = v2026.05.08.1
```

## Testing Checklist

Before enabling a new live enrichment change:

0. Confirm this document date is today. If stale, stop and refresh the scope first.
1. Run a public ping against the endpoint and confirm it does not return `{}`.
2. Confirm returned show/class/group identifiers match the requested scope.
3. Confirm body length and top-level shape.
4. Confirm `getLiveClassStatus` returns `true` before liveclassv2 same-day lanes run.
5. Confirm `groups_live.day` matches current `app_sql_date`.
6. Confirm `groups_live.has_JSON = true` before `getLiveClassData` or `ClassStatus` pings.
7. Dry-run matching against current `watch_trips`.
8. Confirm matched rows use `class_number + entry_number` or `class_id + entry_number`, not synthetic fallback identity.
9. Confirm no protected fields are cleared.
10. Confirm skipped rows log a reason.
11. Run calculators only after enrichment succeeds.
12. Run publisher only when upstream is clean and publish data changed.

## Open Questions

- Should `rows[].id` be stored, and if yes, what existing field can safely hold it?
- Is `Scr` actually score, scratch, or another flag?
- Should `Pos` map to existing `placing` or remain a diagnostic until confirmed?
- Which current Airtable table should own liveclassv2 row-level diagnostics, if any?
- Should same-day live enrichment run only on selected heartbeat slots, or every heartbeat once `has_JSON = true`?
- Should `ClassStatus` be added as a group-level overlay before or after `ListAjax`/`groups_live` values?
