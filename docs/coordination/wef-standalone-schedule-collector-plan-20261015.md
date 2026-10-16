> SUPERSEDED FOR ACTIVATION: see [current schedules-only plan](wef-schedules-only-activation-plan-20261015.md). Historical heartbeat/consumer cutover instructions below are not current.

# Standalone SGL schedule collector — revised minimal plan
Revision: 2026-10-15, after source/history audit.
Status: DOCUMENTED PROPOSAL ONLY. No implementation, scheduler or runtime changes authorized.
Engines development: https://airtable.com/appnOAk2pW8JawiuP/tblqH3wt6MOG02Fam/recc24MWGr0PyS7LY
Runtime: apptdhhNzduxm5gjn. Working source: C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus.

## Decision
Reuse the existing collection implementation. Make a narrow extraction plus necessary correctness/efficiency corrections; do not write a new SGL client, replace normalization, add a queue/framework or create a parallel production writer.
The standalone trigger alone does not solve repeat writes, broad reads, false-success reporting or day-scope coupling.
This revision REPLACES the earlier ordered implementation plan and its blanket rule to preserve archive/inactive/dropped_at. Historical audit evidence remains in wef-schedule-collector-audit-20261015.md.

## What remains unchanged
- Existing SGL endpoint construction, configured fetch adapter/PowerShell transport and authentication.
- Existing schedule normalization, key/tie-breaker compatibility, record IDs and linked records.
- Existing payload validation, dated/manual/HTML fallback and provenance.
- Existing field meanings, source ownership and known manual-time override.
- Required lifecycle behavior when a row reappears in a valid feed, plus downstream clock/mode, scope links and failure gates.
- Owner-selected focus_day and existing manual view-date rollover.
- Other heartbeat lanes, Timing, trips, live jobs and publisher implementation except the minimal acquisition dependency boundary.

## Smallest required changes, in implementation order after approval
| Step | Existing code reused | Narrow change | Required proof |
| --- | --- | --- | --- |
| 1. Truthful completion | schedules_dailyv2.js airtableCreateRecords/airtablePatchRecords, result summary/main; orchestrator exit handling | Required failed batches must yield a failed per-day result and unsuccessful applicable collection outcome. Continue independent date work, but never report a partially persisted day complete. Retain existing diagnostics. | Injected offline create/update failure is reflected in summary and process/consumer result; no false-success downstream gate. |
| 2. Shared day-write protection | hasManualTimeOverride, applyPreliveEstimatedStartTimeGuard and both current/forward write loops | Reuse one preparation path for both dates so manual_time_override protects estimated_start_time everywhere. Keep legacy misspelling compatibility without adding a field. Preserve existing prelive guard conditions; do not broaden them by guess. | Today and tomorrow preserve manual start time; ordinary source-owned values still refresh; omitted payload values do not erase saved values. |
| 3. Bounded reads | fetchExistingRowsForShow, existing key/duplicate resolution | Restore equivalent server-side show filtering, verifying actual field types/names first. Retain all show rows needed for stale-scope cleanup; do not blindly restrict every read to two dates. Select only fields actually needed, including matching, override and lifecycle fields. | Same matched record IDs, duplicate handling and cleanup candidates as baseline; fewer returned rows/bytes on multi-show fixtures. |
| 4. Changed-only writes | buildCurrentFields, sameValue, batch helpers, scope-sync diff | PATCH changed collector-owned business values only. Preserve required observation timestamps, but do not use a new timestamp alone to rewrite every row in the final scope pass. Inspect timestamp consumers before changing their cadence; retain any required observation writes. | Unchanged repeat causes no unnecessary business/scope writes; logs still show run outcome; manual/downstream values are excluded from payloads. |
| 5. Separate per-day acquisition from control | runDaily/runScheduleForBaseContext, existing normalization/fetch/cache functions and scope/lifecycle helpers | Introduce a thin scheduled entry using enabled show controls and manual focus_day; collect focus date and next date independently. Keep source success independent of latest heartbeat availability. Move existing heartbeat patching and scope/lifecycle effects to the existing heartbeat/control path, preserving their order and field meaning. No second implementation of those effects. | Tomorrow failure cannot invalidate successful today; source dates remain correct; returning rows reactivate as before; current-day links and eligibility still resolve. |
| 6. Replace only acquisition's dispatch dependency | heartbeat_slot_orchestrator.js schedules_daily invocation/result gates; existing runtime logging conventions | At approved cutover replace its fetch invocation with a read of completed show/day collection evidence. Keep all unrelated gates and consumers. Use an existing suitable result/log location where possible; inspect and specify it before edits, without inventing a new table. Never use row count or timestamp alone as completion. | Required writes finish before success; failed/in-progress day is not admitted as fresh; other heartbeat work remains active. |
| 7. One scheduled writer | Existing Windows Task Scheduler/runner conventions | One bounded collection run per trigger; no overlap; retain current cadence initially and existing bounded retries. Disable only old acquisition invocation when enabling the new one. | Normal scheduled repeat, restart and rollover, no concurrent old/new writes. |

Steps1–6 are one small compatibility change set, reviewed together before cutover. They are not permission to deploy intermediate incompatible states.
Restoring an in-memory post-write map instead of rereading the table is NOT required in this first cleanup: it risks missing manual/concurrent changes. Initially bound the reads and remove unnecessary writes; optimize the second read only if measurements justify it.

## Manual and lifecycle rules — corrected
1. Existing explicit manual protection: manual_time_override protects estimated_start_time. Reuse it on both dates. Do not add field-by-field override flags or infer manual ownership from every editable Airtable field.
2. Other normalized source fields currently remain source-owned. The collector has no general mechanism to identify manual corrections to class names, dates, counts or end times. Do not promise those edits are automatically protected. If any of those are actually owner-maintained, resolve that specific field before it is included in update payloads; unrelated work can continue.
3. Preserve excluded fields and downstream links by omitting them, not copying a stale snapshot back.
4. archive/inactive/dropped_at reset on reappearance is explicitly required by tests/show_active_scope_contract.test.js and forward-scope tests. Preserve that lifecycle behavior. Do not treat a stored archive/inactive value as a permanent manual lock without evidence. ignore is not assumed to be a universally honored manual-suppression mechanism.
5. is_gotcha reset is existing behavior, but its manual/downstream semantics are not established by the reactivation test. Keep it out of acquisition ownership until its current control responsibility is confirmed; do not invent new semantics.
6. Existing live overlays may populate status, totals and latest times. Preserve their demonstrated precedence during extraction. A generic “latest wins” rewrite is not part of this cleanup; verify stale schedule/live interaction before cutover.
7. Machine identifiers, keys and token fields remain source/matching data, not new manual options.
8. Collector audit timestamps are shared today. Retain downstream-dependent semantics and separate attempt/completion evidence from row last_updated_at.

## Two-day behavior
Today means selected focus_day; tomorrow is the next calendar date. Keep manual selection. Reuse the existing date functions and source availability behavior. Collect both independently; do not wait for all remaining show dates before saving today. Do not relabel tomorrow as today via shared focus_day.
Keep destructive-empty safeguards, but evaluate saved-row protection against the actual selected day rather than future rows elsewhere in the show. An empty valid day, unavailable payload and malformed response remain distinct outcomes. No cleanup/deletion follows an unavailable response.
Use America/New_York, not a fixed EST offset. No new elaborate show calendar, automatic focus-day switch or view-filter automation is included.

## Downstream that still needs heartbeat
| Consumer | Preserve |
| --- | --- |
| tagger/control | Enabled-show context, clock/mode, heartbeat records and eligible links. |
| schedules_calculatorv2 | Latest heartbeat time/date/mode, enrich view membership, heartbeat/run links in output. |
| trips_dailyv2 and trips_tagger | Heartbeat show/date/time scope and linked schedule/entry selection. |
| trips_calculatorv2 / Timing | watch_trips heartbeat view, row time/context and eligible matching trips. |
| live_groups_daily / live_rings_daily | Selected show/focus date and heartbeat DAY/NIGHT behavior. |
| live_class_detail | Heartbeat/orchestrator mode and slot/due-view decisions. |
| publisher | Existing heartbeat-named views and all upstream checks beyond acquisition. |
| rendered enrichment | Existing selected-day/view contracts. Manual view rollover remains. |

The final scope pass has useful stale-row cleanup; it is not simply redundant. Preserve those effects in the existing control path. Once independently collected rows exist, that control path must include them before dependent consumers run.
Known future-date eligibility via live_groups focus_day OR actual date remains a compatibility gate: verified wrong candidate eligibility, not observed wrong live writes. Do not remove date checks or silently accept this as successful two-day operation.

## Deferred cleanup — not needed to deliver the extraction
- Remove unused runPool/FETCH_CONCURRENCY, clearResolvedField and disabled active_groups helpers only after reference checks. They are not proven timeout causes.
- Investigate HOTPATCH_APP_SHOW_ID/HOTPATCH_APP_SQL_DATE usage before removing any existing operational bypass.
- Do not replace SDKs, rewrite fallback, add storage/services, migrate bases or redesign downstream lanes.
- Do not strip class/live enrichment merely because it adds code; field dependencies remain controlling.

## Verification and cutover
Offline verification after implementation approval: same source fixtures/output keys; same record identity and links; source changes; unchanged repeats; both-day overrides; reactivation; valid empty day; malformed/unavailable source; write failure; interruption/restart; tomorrow isolation; show-end behavior.
Inspect existing timestamp consumers and completion-result storage before implementing those boundaries. This is narrow compatibility verification, not a new discovery project.
No tests or manual business runs performed by this planning revision. After separate deployment/scheduler approval, observe NORMAL scheduled execution through repeat runs and a date transition. Timing with no eligible trips cannot prove live Timing completion.
Measure Airtable reads/writes, source requests and duration against equivalent scope. No numerical speed claim without measurements.
Rollback: stop new scheduled writer first, restore recorded old acquisition invocation/settings, preserve business records/manual edits. Never enable both writers or repair records automatically.

## Evidence and baseline
Audit: docs/coordination/wef-schedule-collector-audit-20261015.md (A01–A14, Git references and evidence limits).
Field/source review: schedule_normalizer_v2.js:668–705; schedules_dailyv2.js:1855–1866,1892–2005,2137–2200,2723–2741,2844–2855,2936–2958,3084–3096,3205–3221; lib/scope_fields.js.
HEAD reviewed: f5e895b6d3af5b87d1d2f0d8524061a5e7f171f9.
schedules_dailyv2.js SHA256: 088041C4B55D9820DED9ABA9B3BB89AEAE3DEB2686BEB5518A3D6070230FEE08.
heartbeat_slot_orchestrator.js SHA256: 2E9DDBC36498C3F2978457769D0CE86A8559E1B97A51E7F8BF89CB4837550B31.
Traces: recVPlNQ1ZUdcHec7 and recZQW7xQM0WUKg9n in Engines trace-evidence; docs/coordination/wef-engine-runtime-observation-20261015.md.
473 saved rows were future dates, not current-day classes. Parent timeout does not identify the precise inner call. Git messages do not prove author intent or agent drift.

## Research references
- https://learn.microsoft.com/en-us/powershell/module/scheduledtasks/new-scheduledtasksettingsset?view=windowsserver2025-ps
- https://support.airtable.com/articles/7735693959-managing-api-call-limits-in-airtable
- https://github.com/Airtable/airtable.js/blob/master/src/run_action.ts
- https://docs.aws.amazon.com/prescriptive-guidance/latest/cloud-design-patterns/retry-backoff.html

## Existing field inventory
Schema snapshot from the previous read: 245 watch_schedule fields. Classifications are subordinate to the corrected manual/lifecycle rules above. “Conditional” is a compatibility check, not permission for new precedence behavior. All unassigned fields remain excluded from collector payloads.
| Field | Type | Field classification (read with revised rules above) |
| --- | --- | --- |
| `schedule_key` | singleLineText | Identity — retain; initialize/confirm only |
| `ignore` | checkbox | Excluded from collection writes — retain existing owner/downstream value |
| `dropped_at` | date | Existing lifecycle/control behavior — retain; separate owner deliberately |
| `show_id` | number | Source-owned — existing behavior; no general manual-edit detection |
| `scheduled_date` | singleLineText | Source-owned — existing behavior; no general manual-edit detection |
| `conflict` | checkbox | Excluded from collection writes — retain existing owner/downstream value |
| `is_target` | checkbox | Existing lifecycle/control behavior — retain; separate owner deliberately |
| `shifted_to_next_dayv2` | checkbox | Existing lifecycle/control behavior — retain; separate owner deliberately |
| `eval_key` | formula | Preserve — computed |
| `evaluate_key_date` | formula | Preserve — computed |
| `inactive` | checkbox | Existing lifecycle/control behavior — retain; separate owner deliberately |
| `shifted_to_next_day` | formula | Preserve — computed |
| `new_schedule_key` | formula | Preserve — computed |
| `heartbeat` | multipleRecordLinks | Existing lifecycle/control behavior — retain; separate owner deliberately |
| `show (from heartbeat)` | multipleLookupValues | Preserve — computed |
| `customer_id` | number | Identity — retain; initialize/confirm only |
| `customers` | multipleRecordLinks | Preserve — downstream/unassigned |
| `class_number` | number | Source-owned — existing behavior; no general manual-edit detection |
| `class_numbers` | multipleRecordLinks | Preserve — downstream/unassigned |
| `class_group_id` | number | Source-owned — existing behavior; no general manual-edit detection |
| `class_id (from class_id_lookup)` | multipleLookupValues | Preserve — computed |
| `class_id` | number | Source-owned — existing behavior; no general manual-edit detection |
| `YYYYMMDD` | formula | Preserve — computed |
| `app_show_idv2` | number | Existing lifecycle/control behavior — retain; separate owner deliberately |
| `app_sql_datev2` | singleLineText | Existing lifecycle/control behavior — retain; separate owner deliberately |
| `app_dow_rawv2` | singleLineText | Existing lifecycle/control behavior — retain; separate owner deliberately |
| `record_state` | singleLineText | Audit update — shared semantics, not completion proof |
| `ringName` | formula | Preserve — computed |
| `classes_endpoint` | formula | Preserve — computed |
| `show_date` | singleLineText | Source-owned — existing behavior; no general manual-edit detection |
| `class_groupxclasses_id` | number | Source-owned — existing behavior; no general manual-edit detection |
| `groups_live` | multipleRecordLinks | Preserve — downstream/unassigned |
| `class_search` | formula | Preserve — computed |
| `start_display` | formula | Preserve — computed |
| `class_group_sequence` | number | Source-owned — existing behavior; no general manual-edit detection |
| `group_name` | singleLineText | Source-owned — existing behavior; no general manual-edit detection |
| `class_name` | multilineText | Source-owned — existing behavior; no general manual-edit detection |
| `class_names` | multipleRecordLinks | Preserve — downstream/unassigned |
| `estimated_start_time (from watch_trips) 2` | multipleLookupValues | Preserve — computed |
| `estimated_start_time (from class_names)` | multipleLookupValues | Preserve — computed |
| `estimated_start_time` | singleLineText | Conditional — manual/live precedence required |
| `start_display (from watch_trips)` | multipleLookupValues | Preserve — computed |
| `Count (watch_trips)` | count | Preserve — computed |
| `watch_trips` | multipleRecordLinks | Preserve — downstream/unassigned |
| `rider_name (from watch_trips)` | multipleLookupValues | Preserve — computed |
| `record_id_rollups` | rollup | Preserve — computed |
| `total_trips (from watch_trips)` | multipleLookupValues | Preserve — computed |
| `estimated_end_time` | singleLineText | Conditional — manual/live precedence required |
| `ring_number` | number | Source-owned — existing behavior; no general manual-edit detection |
| `ww_rings` | multipleRecordLinks | Preserve — downstream/unassigned |
| `ring_nickname` | formula | Preserve — computed |
| `group_display` | rollup | Preserve — computed |
| `show_days_report_title` | formula | Preserve — computed |
| `show_days_display_date` | formula | Preserve — computed |
| `dt` | formula | Preserve — computed |
| `sid` | formula | Preserve — computed |
| `record_id` | formula | Preserve — computed |
| `status` | singleLineText | Conditional — manual/live precedence required |
| `class_status` | multipleRecordLinks | Preserve — downstream/unassigned |
| `latest_ingested_at` | singleLineText | Conditional — manual/live precedence required |
| `manual_time_override` | checkbox | Excluded from collection writes — retain existing owner/downstream value |
| `time-check` | formula | Preserve — computed |
| `schedule_sequencetype` | singleLineText | Source-owned — existing behavior; no general manual-edit detection |
| `is_usf` | formula | Preserve — computed |
| `group` | rollup | Preserve — computed |
| `prior_class_group_idv2` | number | Preserve — downstream/unassigned |
| `rollup_entries` | rollup | Preserve — computed |
| `rollup_trips` | rollup | Preserve — computed |
| `rollup_horses` | rollup | Preserve — computed |
| `firstup` | checkbox | Excluded from collection writes — retain existing owner/downstream value |
| `expected_first` | formula | Preserve — computed |
| `expected_second` | formula | Preserve — computed |
| `shows` | multipleRecordLinks | Identity — retain; initialize/confirm only |
| `mode (from heartbeat)` | multipleLookupValues | Preserve — computed |
| `expectFirst` | formula | Preserve — computed |
| `expectSecond` | formula | Preserve — computed |
| `full_schedule_endpoint` | formula | Preserve — computed |
| `isFirstUp` | formula | Preserve — computed |
| `time_sort` | formula | Preserve — computed |
| `class_type` | singleLineText | Source-owned — existing behavior; no general manual-edit detection |
| `oog_endpoint` | formula | Preserve — computed |
| `watch_rings_uid` | formula | Preserve — computed |
| `class_alerts` | multipleRecordLinks | Preserve — downstream/unassigned |
| `created_time` | createdTime | Preserve — computed |
| `app_sql_date` | formula | Preserve — computed |
| `app_dow_raw` | formula | Preserve — computed |
| `class_types` | multipleRecordLinks | Preserve — downstream/unassigned |
| `watch_rings` | multipleRecordLinks | Preserve — downstream/unassigned |
| `run_time (from watch_rings)` | multipleLookupValues | Preserve — computed |
| `next_group_id (from watch_rings)` | multipleLookupValues | Preserve — computed |
| `focusTargetGroupId` | number | Preserve — downstream/unassigned |
| `immediatePriorGroupId` | number | Preserve — downstream/unassigned |
| `diif` | checkbox | Preserve — downstream/unassigned |
| `8778_sched_display` | rollup | Preserve — computed |
| `8778_group_display` | rollup | Preserve — computed |
| `8778_group_alert` | rollup | Preserve — computed |
| `40362_group_display` | rollup | Preserve — computed |
| `active_alerts` | multipleRecordLinks | Preserve — downstream/unassigned |
| `trip_logs` | multipleRecordLinks | Preserve — downstream/unassigned |
| `class_sequences` | multipleRecordLinks | Preserve — downstream/unassigned |
| `is_handy` | formula | Preserve — computed |
| `is_warmup` | formula | Preserve — computed |
| `is_mulligan` | formula | Preserve — computed |
| `is_schooling_pony` | formula | Preserve — computed |
| `is_add_back` | formula | Preserve — computed |
| `is_focus_trip` | checkbox | Preserve — downstream/unassigned |
| `entry_sequence` | number | Preserve — downstream/unassigned |
| `targets_last_at` | singleLineText | Preserve — downstream/unassigned |
| `group_name_tags` | multilineText | Preserve — downstream/unassigned |
| `group_tags_links` | multipleRecordLinks | Preserve — downstream/unassigned |
| `is_current_scope` | checkbox | Existing lifecycle/control behavior — retain; separate owner deliberately |
| `day_number` | number | Source-owned — existing behavior; no general manual-edit detection |
| `schedule_endpoint` | formula | Preserve — computed |
| `schedule_empty_endpoint` | formula | Preserve — computed |
| `classes_endpointv2` | formula | Preserve — computed |
| `enrich_priors_endpoint` | formula | Preserve — computed |
| `schedule_show_datev2` | singleLineText | Source-owned — existing behavior; no general manual-edit detection |
| `schedule_show_display_datev2` | singleLineText | Source-owned — existing behavior; no general manual-edit detection |
| `not_target` | formula | Preserve — computed |
| `focus_day` | date | Existing lifecycle/control behavior — retain; separate owner deliberately |
| `show` | multipleRecordLinks | Existing lifecycle/control behavior — retain; separate owner deliberately |
| `shifted_to_next_day (from heartbeat)` | multipleLookupValues | Preserve — computed |
| `run_tag` | singleLineText | Audit update — shared semantics, not completion proof |
| `ringName_esp` | formula | Preserve — computed |
| `estimated_start_time (from watch_trips)` | multipleLookupValues | Preserve — computed |
| `total_trips` | number | Conditional — manual/live precedence required |
| `ring_nickname_esp` | multipleLookupValues | Preserve — computed |
| `ring_nickname2` | multipleLookupValues | Preserve — computed |
| `large_class_tag` | formula | Preserve — computed |
| `now` | formula | Preserve — computed |
| `latestStatus` | formula | Preserve — computed |
| `status2` | formula | Preserve — computed |
| `latest_status` | singleLineText | Conditional — manual/live precedence required |
| `___latest_estimated_start_time` | singleLineText | Conditional — manual/live precedence required |
| `last_modified_time` | lastModifiedTime | Preserve — computed |
| `start_display2` | formula | Preserve — computed |
| `last_ingested_check` | formula | Preserve — computed |
| `last_updated_at` | singleLineText | Audit update — shared semantics, not completion proof |
| `is_gotcha` | checkbox | Existing lifecycle/control behavior — retain; separate owner deliberately |
| `startKey` | formula | Preserve — computed |
| `startKeyDT` | formula | Preserve — computed |
| `startKeyDTT` | formula | Preserve — computed |
| `nowKeyDTT` | formula | Preserve — computed |
| `secondsTill` | formula | Preserve — computed |
| `30Till` | formula | Preserve — computed |
| `classTrigger` | singleLineText | Preserve — downstream/unassigned |
| `classActual` | number | Preserve — downstream/unassigned |
| `latestStart` | formula | Preserve — computed |
| `group_alert` | rollup | Preserve — computed |
| `nowTime` | formula | Preserve — computed |
| `latestStart24` | formula | Preserve — computed |
| `estimatedStart24` | formula | Preserve — computed |
| `minsTill` | formula | Preserve — computed |
| `entry_id (from watch_trips)` | multipleLookupValues | Preserve — computed |
| `entryID` | formula | Preserve — computed |
| `clear` | checkbox | Excluded from collection writes — retain existing owner/downstream value |
| `epoch` | formula | Preserve — computed |
| `next_due_epoch` | number | Preserve — downstream/unassigned |
| `latestStatus_fallback` | multipleLookupValues | Preserve — computed |
| `showsMode` | formula | Preserve — computed |
| `defaultTimeTo (from ww_rings)` | multipleLookupValues | Preserve — computed |
| `ringWalk` | formula | Preserve — computed |
| `latestStart (from watch_trips)` | multipleLookupValues | Preserve — computed |
| `prev_estimated_start_time` | singleLineText | Preserve — downstream/unassigned |
| `___expected_sort` | formula | Preserve — computed |
| `oog_endpoint copy` | formula | Preserve — computed |
| `order_of_go_endpoint` | formula | Preserve — computed |
| `group_alert2` | rollup | Preserve — computed |
| `latest_estimated_start_time` | singleLineText | Conditional — manual/live precedence required |
| `active_classes` | multipleRecordLinks | Preserve — downstream/unassigned |
| `entry_oog` | multipleRecordLinks | Preserve — downstream/unassigned |
| `is_target (from watch_trips)` | multipleLookupValues | Preserve — computed |
| `is_classic` | formula | Preserve — computed |
| `mode` | formula | Preserve — computed |
| `app_show_id` | formula | Preserve — computed |
| `epoch (from heartbeat)` | multipleLookupValues | Preserve — computed |
| `time (from heartbeat)` | multipleLookupValues | Preserve — computed |
| `app_dow_raw (from heartbeat)` | multipleLookupValues | Preserve — computed |
| `dow_raw` | multilineText | Preserve — downstream/unassigned |
| `heldover_from_sunday` | checkbox | Excluded from collection writes — retain existing owner/downstream value |
| `tripsCount` | count | Preserve — computed |
| `status (from groups_live)` | multipleLookupValues | Preserve — computed |
| `estimated_start_time (from groups_live)` | multipleLookupValues | Preserve — computed |
| `since_lastModified` | formula | Preserve — computed |
| `isA` | checkbox | Preserve — downstream/unassigned |
| `publish_diffs` | multipleRecordLinks | Preserve — downstream/unassigned |
| `tripTarget` | formula | Preserve — computed |
| `nextTargetClassId` | number | Preserve — downstream/unassigned |
| `focusTargetClassId` | number | Preserve — downstream/unassigned |
| `tripTargetClassId` | formula | Preserve — computed |
| `mins_per_trip (from watch_rings)` | multipleLookupValues | Preserve — computed |
| `immediatePriorDelayMinutes` | number | Preserve — downstream/unassigned |
| `perTrip` | formula | Preserve — computed |
| `app_sql_date (from heartbeat)` | multipleLookupValues | Preserve — computed |
| `app_show_id (from heartbeat)` | multipleLookupValues | Preserve — computed |
| `40362_group_alert` | rollup | Preserve — computed |
| `is_classic2` | formula | Preserve — computed |
| `class_NOT_both` | formula | Preserve — computed |
| `thread_logs` | multipleRecordLinks | Preserve — downstream/unassigned |
| `app_dows` | multipleRecordLinks | Preserve — downstream/unassigned |
| `is_target (from watch_trips) 2` | multipleLookupValues | Preserve — computed |
| `class_state` | formula | Preserve — computed |
| `start_display_short` | formula | Preserve — computed |
| `table_name` | formula | Preserve — computed |
| `table_view` | formula | Preserve — computed |
| `scope_key` | formula | Preserve — computed |
| `scope_run_id` | singleLineText | Existing lifecycle/control behavior — retain; separate owner deliberately |
| `scope_status` | singleSelect | Existing lifecycle/control behavior — retain; separate owner deliberately |
| `last_seen_at` | date | Audit update — shared semantics, not completion proof |
| `record_ID Rollup (from watch_trips)` | rollup | Preserve — computed |
| `schedule_logs2` | multipleRecordLinks | Preserve — downstream/unassigned |
| `schedule_logs` | multipleRecordLinks | Preserve — downstream/unassigned |
| `schedule_logs_rid` | multipleLookupValues | Preserve — computed |
| `heartbeat_rid` | multipleLookupValues | Preserve — computed |
| `record_id (from watch_trips)` | multipleLookupValues | Preserve — computed |
| `watch_trips_rid_rollups` | rollup | Preserve — computed |
| `schedule_show_display_date_dayv2` | singleLineText | Source-owned — existing behavior; no general manual-edit detection |
| `shows_rid` | multipleLookupValues | Preserve — computed |
| `completed_trips` | number | Conditional — manual/live precedence required |
| `group_tags copy` | multipleRecordLinks | Preserve — downstream/unassigned |
| `is_usef` | formula | Preserve — computed |
| `class_completed10` | formula | Preserve — computed |
| `prior_class_group_status` | singleLineText | Preserve — downstream/unassigned |
| `prior_class_group_trips_text` | singleLineText | Preserve — downstream/unassigned |
| `prior_class_group_updated_at` | number | Preserve — downstream/unassigned |
| `alert-title` | formula | Preserve — computed |
| `ring_name` | formula | Preserve — computed |
| `is_firstUp` | formula | Preserve — computed |
| `test-key` | formula | Preserve — computed |
| `schedule_short` | singleLineText | Identity — retain; initialize/confirm only |
| `archive` | checkbox | Existing lifecycle/control behavior — retain; separate owner deliberately |
| `ring_collection` | singleLineText | Existing lifecycle/control behavior — retain; separate owner deliberately |
| `show_scope_key` | singleLineText | Existing lifecycle/control behavior — retain; separate owner deliberately |
| `is_tomorrow` | formula | Preserve — computed |
| `is_2nd_trip (from watch_trips)` | multipleLookupValues | Preserve — computed |
| `class_est_time` | singleLineText | Preserve — downstream/unassigned |
| `mj` | multipleRecordLinks | Preserve — downstream/unassigned |
| `sgl_token_raw` | multilineText | Source-owned — existing behavior; no general manual-edit detection |
| `sgl_token_prefix` | singleLineText | Source-owned — existing behavior; no general manual-edit detection |
| `sgl_token_length` | number | Source-owned — existing behavior; no general manual-edit detection |
| `sgl_token_hash` | singleLineText | Source-owned — existing behavior; no general manual-edit detection |
| `class_id (from class_numbers)` | multipleLookupValues | Preserve — computed |
| `groups_live copy` | multipleRecordLinks | Preserve — downstream/unassigned |
| `gets` | multipleRecordLinks | Preserve — downstream/unassigned |
| `Table 271` | multipleRecordLinks | Preserve — downstream/unassigned |

Names schedule_date, sql_date, live_groups and full_nesting_key were absent from the reviewed writable schema despite conditional source references. Do not create them or infer alternate mappings. manual_time_overide is a legacy spelling, not a field to add.

Next action: review this revised minimal plan. No code or runtime changes authorized or performed.



