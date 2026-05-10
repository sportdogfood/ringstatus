# RingStatus Daily Scope - 2026-05-09

**Date:** 2026-05-09  
**Status:** Active daily operating scope  
**Primary mode focus:** `DAY`  
**Transition scopes:** `DAY -> NIGHT`, `OVERNIGHT -> DAY`

## Purpose

This daily scope separates three workflows that are easy to confuse:

- current-day `DAY` live enrichment
- continuous non-live schedule and people refresh
- transition preparation for tomorrow when the heartbeat shifts scope

The liveclassv2 workflow enriches current-day rows. The schedule/people workflow must keep running outside live enrichment because it prepares and repairs `watch_schedule` and `watch_trips`, especially when the target date shifts to tomorrow.

## DAY Mode

`DAY` mode focuses on the current `app_sql_date`.

Current-day live enrichment:

1. Check live feed availability with `getLiveClassStatus`.
2. Use `ListAjax` as the broad live group payload.
3. Validate current show/date and `has_JSON = true`.
4. Use `classes[] + classNumbers[]` to pair `class_number -> class_id`.
5. Use `getLiveClassData?show_id={SHOW_ID}&cid={CLASS_ID}&cgid={CLASS_GROUP_ID}` for trip row enrichment.

If `watch_trips.getLiveClassData` is populated, it may be used as the explicit same-day enrichment endpoint for that row. Otherwise the endpoint must be constructed from known `show_id`, `class_id`, and `class_group_id`. If those identifiers cannot be resolved, skip live trip enrichment for that row and log `err:missing_liveclass_mapping`. Do not use `classsignup` as the fallback for missing liveclass mapping.

Current-day schedule/people refresh still runs separately:

- `schedules_dailyv2.js` keeps trying the day-scoped `/schedule?date=...` endpoint through the local PowerShell fetch path.
- A successful schedule payload is the primary source for that run.
- The same successful schedule payload is stored in `early_sgl_payloads/schedule`.
- After a successful schedule payload, the lane should try remaining show dates through `end_date` and store each successful payload in `early_sgl_payloads/schedule`.
- `trips_dailyv2.js` keeps trying `/people/{pid}` through the local PowerShell fetch path for each active tenant/person.
- A successful people payload is the primary source for that run.
- A successful people payload is stored in `early_sgl_payloads/people`.

People payloads are show/week scoped. They do not need a date loop.

Manual fallbacks during the day:

- `manual_sgl_payloads` is the manual fallback contract for all manually supplied show dates.
- Schedule JSON may be supplied directly in `manual_sgl_payloads` or an approved `manual_sgl_payloads/schedule` folder.
- People JSON may be supplied directly in `manual_sgl_payloads` or an approved `manual_sgl_payloads/people` folder.
- Schedule HTML may be supplied directly in `manual_sgl_payloads` or an approved `manual_sgl_payloads/schedule-html` folder, and is only for manually added schedule HTML time extraction.

## DAY -> NIGHT Transition

This is the most critical transition for pre-live population.

When heartbeat mode changes from `DAY` to `NIGHT` and `shifted_to_next_day = true`, the target date is tomorrow. The pipeline must prepare tomorrow's minimum viable rows, not spend the run on current-day live enrichment.

Schedule requirements:

- Use the local PowerShell fetch path first.
- Ping one day-scoped `/schedule?date={target_date}&show_id={show_id}&customer_id=15` endpoint.
- If successful, write/refresh tomorrow's minimum viable `watch_schedule` rows from that payload.
- Store the successful payload in `early_sgl_payloads/schedule`.
- Try remaining show dates and store successful forward-day payloads for fallback support.
- If the fresh schedule payload is soft/empty, fall back to `early_sgl_payloads/schedule`, then manual schedule JSON in `manual_sgl_payloads`.
- `estimated_start_time` is critical for DAY -> NIGHT prep. If it remains empty after fresh/cached/manual JSON, the lane must use manual schedule HTML as the last resort.
- Manual schedule HTML can cover all dates present in `manual_sgl_payloads`; it is not a one-file exception.
- Example last-resort file: `manual_sgl_payloads/schedule_html_2026_05_09_show_id_200000061_1778369104.html`.
- HTML display times such as `8:30 AM` must normalize to `08:30:00` before writing `estimated_start_time`.

Trip requirements:

- Use the local PowerShell fetch path first.
- Ping one `/people/{pid}` endpoint per active tenant/person.
- If successful, write/refresh tomorrow's minimum viable `watch_trips` rows from that payload plus current scoped `watch_schedule`.
- Store the successful people payload in `early_sgl_payloads/people`.
- If the fresh people payload is soft/empty, fall back to `early_sgl_payloads/people`, then manual people JSON in `manual_sgl_payloads`.
- Do not loop people by date because the payload is full-week.

Fields expected for minimum viable rows:

- `class_group_id`
- `class_number`
- `class_id`, when available or resolvable from trusted group/class pairs
- `class_name`
- `group_name`
- `ring_number`
- `schedule_show_datev2` / schedule date fields
- `estimated_start_time`, when available
- trip horse/rider/entry fields from people payloads
- schedule/trip links needed for later enrichment

Fields not required at this transition:

- `order_of_go`
- `gone_in`
- `actual_order`
- live-only progress fields

## OVERNIGHT -> DAY Transition

When the target date becomes the actual live day, the rows created overnight should remain in place.

The pipeline should switch from pre-live minimum-row population to current-day enrichment:

1. Keep existing `watch_schedule` and `watch_trips` rows.
2. Prove live availability with `getLiveClassStatus`.
3. Refresh groups through `ListAjax`.
4. Pair `classes[]` to `classNumbers[]`.
5. Backfill blank `watch_schedule.class_id` where `class_group_id + class_number` identifies the pair.
6. Enrich schedule rows with group-level `estimated_start_time`, `status`, `gone`, `total`, ring, and live group link.
7. Enrich trip rows with `getLiveClassData` for `OOG`, `Gone`, `Actual_OOG`, entry, rider, and horse confirmation.

If live endpoints return `{}`, wrong show, wrong date, invalid JSON, or no matching rows, log the reason and keep the pre-live rows intact.

`classsignup` is not the fallback for live trip enrichment when `getLiveClassData` cannot be built or read from `watch_trips.getLiveClassData`; unresolved rows should remain intact for the next valid live pass.

## Estimated Start Time

`estimated_start_time` remains a known weak point before live group feeds are available. This has been a repeated DAY -> NIGHT failure point and must be treated as a required troubleshooting checkpoint, not a nice-to-have field.

Current allowed sources:

- `/schedule?date=...` when it provides usable time data
- `ListAjax` / `groups_live` once the day is live or live-ready
- `ClassStatus` as a targeted group confirmation path
- manually added HTML in `manual_sgl_payloads` or `manual_sgl_payloads/schedule-html`

Manual HTML time extraction must normalize display times to `HH:MM:SS`:

```text
8:00 AM -> 08:00:00
8:30 AM -> 08:30:00
1:45 PM -> 13:45:00
```

Manual HTML is a fallback, not a primary live source. Continue looking for a more efficient upstream source for pre-live `estimated_start_time` so manual HTML is needed less often.

DAY -> NIGHT rule: if the target date has minimum viable schedule rows but `estimated_start_time` is still blank after JSON sources, use the manual HTML files in `manual_sgl_payloads` as the last resort for every available date. Do not leave the transition without either filling the normalized time or logging why the manual HTML could not be matched.

## Operating Boundary

Do not use `/classes/{class_id}` as a schedule or trips population dependency. `class_id` itself is still important and should be preserved or backfilled from reliable group/class mappings when available.

Do not treat live enrichment as a replacement for the schedule/people refresh workflow. They solve different problems:

- schedule/people refresh creates and repairs minimum viable rows
- live enrichment fills current-day live progress and trip order/detail
