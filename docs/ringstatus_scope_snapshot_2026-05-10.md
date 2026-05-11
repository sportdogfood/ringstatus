# RingStatus Scope Snapshot - 2026-05-10

**Snapshot date:** 2026-05-10  
**Purpose:** non-regression baseline for the current working pipeline behavior  
**Status:** working baseline, with known room for improvement

This snapshot captures the current scope while the pipeline is running well enough to protect. Future changes can improve this baseline, but should not regress the behaviors below without an explicit owner decision.

## Current Baseline

The pipeline has three separate workflows that must remain separate:

- current-day `DAY` live enrichment
- continuous non-live schedule and people refresh
- transition preparation for tomorrow during `DAY -> NIGHT` and `OVERNIGHT -> DAY`

Live enrichment is not a substitute for schedule/people refresh. Schedule/people refresh creates and repairs minimum viable rows. Live enrichment fills same-day progress, order, and live details.

## Heartbeat Manual Controls

Current non-regression baseline after the 2026-05-11 show-date control change:

- `shows.mode_control` is the manual lever for `AUTO`/blank, `DAY`, `NIGHT`, `OVERNIGHT`, `IDLE`, and `OFF`.
- `heartbeat.clock_mode` records the clock-derived mode.
- `heartbeat.mode` records the effective mode after `FORCE_MODE`, show manual control, and default show-date guard logic.
- `heartbeat.mode_source` and `heartbeat.mode_reason` explain why the effective mode was selected.
- `heartbeat.default_show_date_status` and `heartbeat.default_show_date_reason` expose the code-owned default show-date guard result.
- If the default show-date guard needs confirmation and `shows.is_default_show_manual_override` is unchecked, effective mode becomes `OFF` and the slot orchestrator must not run heavy lanes.

## Live Trip Enrichment

Current same-day live enrichment should follow this sequence:

1. Confirm live availability through `getLiveClassStatus`.
2. Use `ListAjax` / `groups_live` as the broad group source.
3. Require the correct show/date and `groups_live.has_JSON = true`.
4. Pair `classes[]` to `classNumbers[]` to resolve `class_number -> class_id`.
5. Ping `getLiveClassData?show_id={SHOW_ID}&cid={CLASS_ID}&cgid={CLASS_GROUP_ID}` for trip-level live data.
6. Enrich existing `watch_trips` rows; do not rebuild rows from scratch.

`class_id` is not deprecated. It remains critical for building `getLiveClassData`. The unreliable dependency is `/classes/{class_id}` as a schedule/trips population source.

`watch_trips.getLiveClassData` may be used as an explicit row endpoint when present. Otherwise the endpoint should be constructed from known `show_id`, `class_id`, and `class_group_id`.

If the liveclass endpoint cannot be constructed, skip live enrichment for that row and record the reason. Do not use `classsignup` as the fallback for missing liveclass mapping.

Verified current evidence:

- Example endpoint returned HTTP `200`: `https://sgl.wellingtoninternational.com/iphonev2/index.php/esp/liveclassv2/getLiveClassData?show_id=200000061&cid=200025174&cgid=200023880`
- The payload returned `ID = 200025174`, `recs = 16`, and `rows = 16`.
- Returned row fields included `Gone`, `OOG`, and `Actual_OOG`.
- A dry-run of `trips_tagger.js` attempted `14` unique liveclass endpoints and matched `27` liveclass trip rows.
- A live run of `trips_tagger.js` created `14` `automation_errs` rows for those `getLiveClassData` attempts.

## Liveclass Attempt Logging

Every `getLiveClassData` ping attempt must write an `automation_errs` row. This includes successful payloads.

Successful rows should use:

- `automation_name = trips_tagger_getLiveClassData`
- `error_type = liveclass_payload_ok`
- `resolved = true`

The `message` should include the endpoint path, full endpoint, `show_id`, `cid`, `cgid`, HTTP status, body length, payload class id, and row count.

Failed, empty, wrong-class, or malformed payloads should use the same `automation_name`, set `resolved = false`, and preserve the liveclass failure reason in `error_type` and `message`.

This table is the endpoint-attempt history. It should let us answer whether the pipeline actually pinged liveclass endpoints without guessing from downstream fields.

## DAY -> NIGHT Baseline

When heartbeat mode changes from `DAY` to `NIGHT` and `shifted_to_next_day = true`, the target date is tomorrow and is pre-live.

Schedule behavior:

- Use the local PowerShell fetch path first.
- Ping one day-scoped `/schedule?date={target_date}&show_id={show_id}&customer_id=15` endpoint.
- If successful, create or refresh tomorrow's minimum viable `watch_schedule` rows.
- Store successful schedule payloads under `early_sgl_payloads/schedule`.
- Try remaining show dates through the show end date and store successful forward-day payloads for fallback support.
- If the fresh payload is soft/empty, fall back to `early_sgl_payloads/schedule`, then manual schedule JSON.
- If `estimated_start_time` is still empty after JSON sources, manual schedule HTML is the last resort.

Trips behavior:

- Use the local PowerShell fetch path first.
- Ping one `/people/{pid}` endpoint per active tenant/person.
- If successful, create or refresh tomorrow's minimum viable `watch_trips` rows from people payloads plus scoped `watch_schedule`.
- Store successful people payloads under `early_sgl_payloads/people`.
- Do not loop people by date because the people payload is show/week scoped.

Do not spend the `DAY -> NIGHT` run on liveclass fanout for tomorrow's rows.

## Estimated Start Time

`estimated_start_time` is a critical transition field and a repeated failure point.

Accepted sources:

- fresh day-scoped schedule JSON
- cached schedule JSON in `early_sgl_payloads/schedule`
- manual schedule JSON in `manual_sgl_payloads`
- manual schedule HTML as last resort before live data is available
- `ListAjax` / `groups_live` once the day is live

Manual HTML must be selected by the current shifted target date, not by an example date from a prior discussion. Display times must normalize to `HH:MM:SS`, for example:

```text
8:00 AM -> 08:00:00
8:30 AM -> 08:30:00
1:45 PM -> 13:45:00
```

Pre-live suspicious times outside `07:00:00` through `19:00:00` should not blindly overwrite existing corrected values.

## Manual Time Override

The real Airtable field is `manual_time_override`.

When checked:

- `watch_schedule` time writers must not patch the row's manual time.
- `watch_trips` time writers must not patch trip timing fields such as `estimated_start_time`, `estimated_end_time`, `estimated_go_time`, `estimated_time`, or `actual_time`.
- Companion display/source fields that can surface the same bad time should not reintroduce the erroneous value.

The misspelled `manual_time_overide` may remain as a defensive alias in code only. It is not the documented field name.

Verified current evidence:

- Locked `watch_schedule` rows had base and latest time fields aligned after repair.
- Locked `watch_trips` rows had base and latest time fields aligned after repair.
- A guarded `schedules_calculatorv2.js --promote` run applied `0` patches and reported `manual_time_override_preserved`.

## Known Improvement Areas

These are improvement targets, not reasons to regress the baseline:

- reduce any remaining low-value endpoint fanout
- keep calculator phases fast and explainable
- improve pre-live `estimated_start_time` sourcing so manual HTML is needed less often
- keep `automation_errs` useful without making it too noisy
- continue replacing inference with row-level evidence for pings, payloads, and skipped rows

## Non-Regression Checks

Before accepting future changes, confirm:

- `trips_tagger.js` still pings `getLiveClassData` for same-day live trip enrichment.
- `automation_errs` receives one row per `getLiveClassData` attempt, including successful payloads.
- `classsignup` is not used as the fallback when liveclass mapping is missing.
- `/classes/{class_id}` is not used as a schedule/trips population dependency.
- `manual_time_override` prevents later schedule/trip time overwrites.
- `DAY -> NIGHT` remains pre-live and uses schedule/people refresh plus approved fallback folders.
- Manual schedule HTML lookup uses the shifted target date.
- Existing pre-live rows remain intact when live endpoints are empty, wrong, or unavailable.
- Manual `shows.mode_control` and `shows.is_default_show_manual_override` still control heartbeat/tagger mode without relying on Airtable formula date math.
