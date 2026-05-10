# RingStatus Pipeline Scope

**Version:** v2026.05.09.5  
**Date:** 2026-05-09  
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
| v2026.05.09.5 | 2026-05-09 | Strengthened the repeated DAY -> NIGHT estimated start rule: `estimated_start_time` is critical for next-day minimum viable rows; if fresh/cached/manual schedule JSON leaves it blank, manual schedule HTML in `manual_sgl_payloads` is the required last-resort source for all dates present in that folder, including `schedule_html_2026_05_09_show_id_200000061_1778369104.html`, with display times normalized to `HH:MM:SS`. |
| v2026.05.09.4 | 2026-05-09 | Clarified live trip enrichment endpoint policy: `getLiveClassData` should be constructed from known `show_id`, `class_id`, and `class_group_id`, or read from `watch_trips.getLiveClassData` when explicitly populated; if neither is possible, skip the row with `err:missing_liveclass_mapping` and do not use `classsignup` as the fallback for missing liveclass mapping. |
| v2026.05.09.3 | 2026-05-09 | Clarified the non-live schedule/people refresh contract that runs throughout the day and becomes critical on `DAY -> NIGHT`: fresh schedule and people payloads are primary when successful, successful schedule payloads should be cached for current and remaining show dates, successful people payloads should be cached once per person/show because they are full-week, and manual JSON/HTML folders are secondary fallbacks. |
| v2026.05.09.2 | 2026-05-09 | Added the live schedule backfill rule: when `ListAjax`/`groups_live` provides a reliable `classNumbers[] -> classes[]` pair, blank `watch_schedule.class_id` should be populated from that pair without pinging `/classes/{class_id}`. |
| v2026.05.09.1 | 2026-05-09 | Clarified the same-day live trips enrichment sequence: prove live feed availability, validate current `groups_live`, pair `classNumbers[]` to `classes[]`, build class-scoped `getLiveClassData` requests from those live groups, then enrich existing `watch_trips` rows. Also clarified that `class_id` remains critical while `/classes/{class_id}` is only too unreliable to be a schedule/trips population dependency. |
| v2026.05.08.4 | 2026-05-08 | Made manual schedule HTML time handling explicit: `manual_sgl_payloads/schedule-html/schedule_html_YYYY-MM-DD_show_SHOWID_EPOCH.html` is allowed as a conservative time overlay, and all display start times in `h:mm AM/PM` or `hh:mm AM/PM` format must write `estimated_start_time` as normalized `HH:MM:SS`. |
| v2026.05.08.3 | 2026-05-08 | Added the matching trips-lane boundary: `trips_dailyv2.js` must use one `/people/{pid}` endpoint per active tenant through the local PowerShell fetch path, cache successful people payloads, fall back only to approved people payload folders, and must not use `/classes/{class_id}` as a pre-live substitute for trip population. |
| v2026.05.08.2 | 2026-05-08 | Clarified the schedule-lane endpoint boundary: `schedules_dailyv2.js` must use the single day-scoped `/schedule?date=...` endpoint plus approved payload fallbacks, must not fan out to `/classes/{class_id}`, and must treat `DAY -> NIGHT` shifted next-day schedule creation as pre-live minimum-row population. |
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
- create an `automation_errs` record for every soft/empty SGL payload, including the endpoint path, show/date context, run id, and payload size evidence

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
| `/classes/{class_id}` | separate enrichment only | class detail | Not reliable for schedule population. `schedules_dailyv2.js` must not ping this endpoint while building next-day schedule rows. |
| `/classsignup/{class_group_id}` | day-of/enrichment | order fallback when usable | Payload may contain unusable/null entry fields. Validate shape. |

## Schedule Lane Endpoint Boundary

`schedules_dailyv2.js` is the owner for pre-live and shifted next-day `watch_schedule` population.

This lane must continue running throughout the day outside the liveclassv2 enrichment workflow. Fresh schedule payloads are the most relevant source when successful. Fallbacks are only for soft/empty live schedule payloads or manual repair cases.

For each scoped show/date, the schedule lane must make one primary schedule request:

```text
/schedule?date={YYYY-MM-DD}&show_id={SHOW_ID}&customer_id=15
```

That request must run through the local PowerShell fetch path so the local environment, proxy behavior, and headers match the runner context.

If the live schedule request returns `{}`, a small body, a shape-mismatched body, or another soft/empty payload, the lane must not fan out to class detail endpoints to compensate. It must record the soft payload in `automation_errs`, preserve existing Airtable data, and then use only the approved schedule fallbacks in this order:

1. `early_sgl_payloads/schedule`
2. `manual_sgl_payloads/schedule`
3. `manual_sgl_payloads/schedule-html`, only when manual HTML scrape handling is intentionally invoked

The schedule lane must not use these folders:

```text
tmp/schedule
tmp/sgl_schedule_samples
```

The schedule lane must not ping this endpoint while building schedule rows:

```text
/classes/{class_id}/?show_id={SHOW_ID}&customer_id=15
```

Reason:

`/classes/{class_id}` is no longer reliable enough to be part of schedule population. A fanout from one usable schedule payload into dozens or hundreds of class-detail requests can convert a good schedule run into a failed run because the class-detail family returns `{}` or otherwise unusable payloads.

When heartbeat mode shifts from `DAY` to `NIGHT` and `shifted_to_next_day = true`, the next-day schedule lane should create or refresh minimum viable `watch_schedule` rows from the day-scoped schedule payload or approved fallback payloads. Missing live-only or class-detail-only fields are expected at that point. The next actual live day should rely on the liveclassv2 paths, gated by `groups_live`, to populate richer fields.

When a fresh schedule payload succeeds, the schedule lane should proceed with that payload for the scoped date, store it in `early_sgl_payloads/schedule`, then opportunistically try each remaining show day through `end_date` and store each successful forward-day payload there as fallback support. These forward-day cache fetches are support artifacts; they should not drive same-run Airtable writes for a different date.

This boundary does not remove day-of live enrichment. `ListAjax`, `groups_live`, `getLiveClassStatus`, `ClassStatus`, and `getLiveClassData` remain same-day live paths and must stay separate from pre-live next-day schedule population.

## Trips Lane Endpoint Boundary

`trips_dailyv2.js` is the owner for pre-live and shifted next-day `watch_trips` population from person-scoped trip payloads.

This lane must continue running throughout the day outside the liveclassv2 enrichment workflow. Fresh people payloads are the most relevant source when successful. The `/people/{pid}` payload is show/week scoped, not date scoped, so one successful person/show payload can support all show dates in the week and should be cached once as a fallback artifact.

For each active tenant/person, the trips lane must make one primary people request:

```text
/people/{PID}?pid={PID}&show_id={SHOW_ID}&customer_id=15
```

That request must run through the local PowerShell fetch path so the local environment, proxy behavior, and headers match the runner context.

If the live people request returns `{}`, a small body, a shape-mismatched body, or another soft/empty payload, the lane must not fan out to class detail endpoints to compensate. It must record the soft payload in `automation_errs`, preserve existing Airtable data, and then use only the approved people fallbacks in this order:

1. `early_sgl_payloads/people`
2. `manual_sgl_payloads/people`

The trips lane must not use these folders:

```text
tmp/people
tmp/sgl_people_samples
tmp/sgl_people_retest
```

The trips lane must not ping this endpoint while building pre-live trip rows:

```text
/classes/{class_id}/?show_id={SHOW_ID}&customer_id=15
```

Reason:

`/people/{pid}` is the person/show payload needed to create minimum viable trip rows. The class-detail endpoint is not reliable enough to be a pre-live dependency. A people payload plus current `watch_schedule` is sufficient to create rows with available horse, rider, entry, class, date, schedule link, and ring context. Missing order, gone-in, actual order, live status, and richer class progress fields are expected before live data exists.

When heartbeat mode shifts from `DAY` to `NIGHT` and `shifted_to_next_day = true`, the next-day trips lane should create or refresh minimum viable `watch_trips` rows from people payloads and current scoped `watch_schedule`. The next actual live day should rely on the liveclassv2 paths, gated by `groups_live`, to populate critical live data points.

Successful people payloads should be used for the current run and stored in `early_sgl_payloads/people` as fallback support. Fallback people payloads are not live authority and must not overwrite newer successful live people payloads. Manually added people files in `manual_sgl_payloads/people` are a second fallback only.

## Same-Day Trips Live Enrichment Sequence

Same-day trip enrichment has a fixed gate order:

1. Prove live feed availability with `getLiveClassStatus`.
2. Validate current-scope `groups_live` rows from `ListAjax`.
3. Build class-scoped liveclassv2 requests from those validated groups.
4. Enrich existing `watch_trips` rows from the returned live trip rows.

In this sequence, "classes" means liveclassv2 class identifiers exposed through `groups_live` and queried through `getLiveClassData`. `class_id` is not deprecated; it remains critical for enrichment. The less reliable part is depending on `/classes/{class_id}` as a schedule/trips population endpoint.

The class-scoped live trip endpoint is:

```text
https://sgl.wellingtoninternational.com/iphonev2/index.php/esp/liveclassv2/getLiveClassData?show_id={SHOW_ID}&cid={CLASS_ID}&cgid={CLASS_GROUP_ID}
```

The endpoint also accepts the cache-buster form `&t={CACHE_BUSTER}`. When `class_group_id` is known from `ListAjax`, include `cgid` because it makes the class/group pairing explicit and matches the observed working call shape.

The trip enrichment field contract is:

| Live row field | Target field |
| --- | --- |
| `rows[].OOG` | `order_of_go` |
| `rows[].Gone` | `gone_in` |
| `rows[].Actual_OOG` | `actual_order` |
| `rows[].ENo` | entry-number match key |
| `rows[].Hor` | horse confirmation |
| `rows[].Rid` | rider confirmation |

`ListAjax` should be the default broad live ping because one payload gives the current groups, `estimated_start_time`, `status`, `gone`, `total`, `classes[]`, `classNumbers[]`, and `has_JSON`. `ClassStatus` may be used as a targeted group refresh, but it should not replace `ListAjax` as the default group discovery path.

For groups with multiple classes, pair by array index:

```text
classes[index]      -> class_id
classNumbers[index] -> class_number
```

That pair should be used to build the narrowest viable `getLiveClassData` endpoint for each trip row. If `watch_trips.class_id` is present, prefer it. If it is missing but `class_number` is present, resolve `class_number` through the `ListAjax` pair. Only fall back to all group class ids when no exact class can be resolved. When building the endpoint, pass both `cid = class_id` and `cgid = class_group_id` when both are available.

The same pair should also backfill blank `watch_schedule.class_id` when the schedule row has the matching `class_group_id + class_number`. This is an identifier repair from the live group payload, not a class-detail population dependency, and it must not require a `/classes/{class_id}` ping.

The lane should enrich rows already created by `trips_dailyv2.js`; it should not rebuild `watch_trips` from scratch because liveclassv2 became available. If `getLiveClassData` returns `{}`, invalid JSON, the wrong show/class, or no matching trip row, the run must log the reason and keep the existing pre-live trip row intact.

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

Schedule fetch policy:

1. First try the live SGL schedule endpoint through the local PowerShell fetch path.
2. If that returns a soft/empty unusable payload, look for cached JSON in `early_sgl_payloads/schedule`.
3. If no early cache exists, look for manual JSON in `manual_sgl_payloads/schedule`.
4. Manual HTML belongs in `manual_sgl_payloads/schedule-html` and is a separate scrape input, not the first JSON fallback.

When a live schedule payload succeeds for a show, the schedule lane should store that payload in `early_sgl_payloads/schedule`, then opportunistically try each subsequent show day through `end_date` and store each successful payload there as well. These forward-day cache fetches are support artifacts; they should not drive same-run Airtable writes for a different date.

For show `200000061`, useful daily schedule cache targets are:

```text
2026-05-06
2026-05-07
2026-05-08
2026-05-09
2026-05-10
```

Successful schedule payloads should be stored as early JSON fallback files. These fallback files are not the source of truth when fresh SGL data is available, but they are useful when a needed schedule payload later fails, soft-throttles, or returns an unusable body.

The `/people/{pid}` endpoint is show/week scoped rather than day scoped. A single successful people payload can contain enough full-week trip detail to help pre-live trip creation and fallback matching.

People payload rule:

- keep trying for fresh people data because it changes frequently
- store the last successful people payload as fallback
- never let an old fallback overwrite newer successful data
- log when fallback data is used
- treat fallback data as pre-live support, not same-day live authority

Payload folder contract:

```text
early_sgl_payloads/schedule
early_sgl_payloads/people
manual_sgl_payloads
manual_sgl_payloads/schedule
manual_sgl_payloads/people
manual_sgl_payloads/schedule-html
```

The repo-local `manual_sgl_payloads` folder is a manual fallback contract, not a one-off scratch location. It may contain schedule JSON, people JSON, and schedule HTML directly at the folder root as well as in approved subfolders. The pipeline should discover all available dates in that folder family when falling back.

Schedule JSON filename pattern:

```text
schedule_{YYYY-MM-DD}_show_{SHOW_ID}_{FETCH_EPOCH}.json
```

Examples:

```text
manual_sgl_payloads/schedule/schedule_2026-05-08_show_200000061_1778280002.json
manual_sgl_payloads/schedule/schedule_2026-05-09_show_200000061_1778280002.json
manual_sgl_payloads/schedule/schedule_2026-05-10_show_200000061_1778280002.json
manual_sgl_payloads/people/people_8778_show_200000061_1778280002.json
manual_sgl_payloads/schedule-html/schedule_html_2026-05-08_show_200000061_1778245924.html
manual_sgl_payloads/schedule-html/schedule_html_2026-05-09_show_200000061_1778245924.html
manual_sgl_payloads/schedule_html_2026_05_09_show_id_200000061_1778369104.html
```

Manual files in these folders are last-resort fallback inputs when the normal SGL fetch path cannot obtain a usable payload at the time the pipeline needs it. For `DAY -> NIGHT`, these manual files are especially important because they may be the only source that fills tomorrow's minimum viable rows before live enrichment is available.

Known manual HTML examples:

```text
C:\actions-runner\ringstatus\manual_sgl_payloads\schedule-html\schedule_html_2026-05-08_show_200000061_1778245924.html
C:\actions-runner\ringstatus\manual_sgl_payloads\schedule-html\schedule_html_2026-05-09_show_200000061_1778245924.html
```

Derived schedule HTML fallback example:

```text
C:\actions-runner\ringstatus\manual_sgl_payloads\schedule-html\schedule_html_2026-05-08_show_200000061_1778245924.html
```

The derived schedule HTML fallback can be used to form the basic schedule when API schedule data is unavailable. During `DAY -> NIGHT`, it must also be used as the last resort when schedule rows exist but `estimated_start_time` is still blank after fresh/cached/manual JSON sources. It should be parsed conservatively for:

- `class_group_id`, usually from `cgid=` in schedule links
- `class_id`, when `cid=` exists
- `class_number`, usually from class text such as `[701]` or grouped rows
- `class_name`
- `ring_number`, usually from `ring=` in links or the table/ring header context
- `estimated_start_time`, normalized to `HH:MM:SS`
- entries/total trips when visible

Time handling is explicit: every manual HTML start time in `h:mm AM/PM` or `hh:mm AM/PM` format must be normalized to `HH:MM:SS` before writing `estimated_start_time`. Examples: `8:00 AM` writes `08:00:00`, `8:30 AM` writes `08:30:00`, and `1:40 PM` writes `13:40:00`. Manual HTML time extraction fills missing `estimated_start_time` values only; it must not replace a newer nonblank API-derived time. If manual HTML cannot be matched to the schedule row, log the mismatch instead of silently leaving the transition ambiguous.

Manual HTML fallback rules:

- use fresh API JSON first when available
- use cached successful JSON before manual HTML when JSON is newer and valid
- use manual schedule JSON before manual HTML when JSON contains the needed `estimated_start_time`
- use manual HTML as the final source when `estimated_start_time` remains blank
- use manual HTML only as a last resort for missing schedule basics or missing `estimated_start_time`
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

Preferred constructed pattern:

```text
https://sgl.wellingtoninternational.com/iphonev2/index.php/esp/liveclassv2/getLiveClassData?show_id={SHOW_ID}&cid={CLASS_ID}&cgid={CLASS_GROUP_ID}
```

The endpoint should be constructed from known `show_id`, `class_id`, and `class_group_id`. If `watch_trips.getLiveClassData` is populated, that field may be used as the explicit endpoint for the row, provided it is a `getLiveClassData` URL with a usable `cid`.

If neither construction nor `watch_trips.getLiveClassData` is available, skip live trip enrichment for that row and log `err:missing_liveclass_mapping`. Do not use `classsignup` as the fallback for missing liveclass mapping because it does not reliably yield the live fields currently needed.

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
- `watch_trips.getLiveClassData` can act as an explicit row endpoint when the script cannot otherwise construct the same `getLiveClassData` URL
- missing liveclass mapping is logged as `err:missing_liveclass_mapping`; `classsignup` is not a fallback for that miss
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
