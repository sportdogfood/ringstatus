# WEF stage contracts - source and natural-run trace

15 October 2026. Discovery only. No code, runtime, configuration, scheduler or business-record changes; no manual business requests. This document records the working system, not a replacement design.

## Evidence scope

Source root: C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus. HEAD f5e895b6d3af5b87d1d2f0d8524061a5e7f171f9 (working-file reads, not a claim of a clean checkout). Read-only scheduled-task inspection confirms ringstatus-heartbeat calls this root's run_tagger_heartbeat_lane.ps1. Existing logs read in bounded tails from C:/actions-runner/ringstatus. No task launched or changed.

## Current keys, endpoints and dependencies

Paths below are relative to the source root. Request shapes describe existing source, not executed requests or automatic proxy compatibility.

| Responsibility | Actual identity / keys | Existing endpoints and dependencies | Evidence |
| --- | --- | --- | --- |
| Heartbeat/control | Selected show/customer/focus day; heartbeat record ID; mode and slot identify a dispatch cycle | Wrapper -> tagger/control -> slot orchestrator. Default discovery is not equivalent to this operational control scope. | run_tagger_heartbeat_lane.ps1; heartbeat_slot_orchestrator.js:1909-1962; observed heartbeat recHyJoQW7SgbRxch below |
| Schedules | Persisted schedule_key = show ID / SQL date / ring number / class number, pipe-separated, with optional occurrence tie-breaker. Short key omits show/day. Normalizer's internal class_group_id_class_number key is a different key, falling back to membership/class ID. | Existing /schedule and /classes requests, selected scope, existing transport/fallback, live-group enrichment and manual-time guards -> watch_schedule. Do not confuse an internal normalizer key with the persisted row key. | schedules_dailyv2.js:211-216,1968-1977; schedule_normalizer_v2.js:112-120; endpoint register |
| Classes and entries | trips_key = show/day/ring/class/person/entry number plus optional occurrence tie-breaker. Source class_id, entry_id and entryxclasses_uuid remain distinct. Intermediate people:classNumber:entryNumber key is not the persisted trips_key. | /people/{sourceId}?pid={sourceId}&show_id={heartbeat.app_show_id}&customer_id={customerId}; active_tenants + heartbeat + matching watch_schedule. Class detail /classes/{class_id} and /classsignup/{class_group_id} enrich entries. | trips_dailyv2.js:295-310,1289,2241-2268; trips_normalizer_v2.js:61-68,235-275; lib/watch_trips_enrichment.js:29-48 |
| Groups | live_groups_key = show/day/customer/ring number/class_group_id. classes lists source IDs; classNumbers lists display numbers. | getLiveClassStatus gate then liveclassv2/ListAjax?from_wp_api=true. Filters payload show and day; updates live groups and matching watch data. DAY/slot controlled. | live_groups_daily.js:113-115,474-478,522-563; orchestrator:1997-1999 |
| Rings | ring query key = customer/show/focus day/ring number; snapshot key appends as-of timestamp. Endpoint ring_id is distinct from ring number. Owner-supplied schedule payload explicitly contains both. | LiveScoreWidget for existing local live-rings lane; ring ID read from live/upcoming/ring data. Selected heartbeat scope and live-group links are dependencies. Separate Airtable ring-alert callers use /ring/{ring_id}; do not replace one lane with the other. | live_rings_daily.js:70-83,297-347,406-450; owner ring JSON attachment 6114a40c-145d-47cd-b4df-d09806297dbd |
| Live class state | show_id + class_id/cid, optional class_group_id/cgid; match entries by participation/entry identity and class context | liveclassv2/getLiveClassData; existing watch_trips and selected scope. Updates live order, gone and position facts. | lib/liveclassv2_enrichment.js:156-159; live_class_detail.js:291,311-335,394-418; trips_tagger.js:1739-1751 |
| Timing / brain | Existing schedule/group and matched trip identities; updates their calculated fields, not a new independent key system | Reads watch_schedule/enrich, live_groups, watch_trips and alert configuration. Schedule calculator currently precedes later live refresh in a cycle, so retained prior-cycle state matters. Trip v2 calculator delegates to existing calculator. | schedules_calculatorv2.js:6-16,1112-1168,1827-1871; trips_calculatorv2.js:1-22; orchestrator:1925-1962 |
| Results | Existing matched trip identity; results_verified and placing are output fields | Currently inside trips_tagger class/trip enrichment, not proved as a separate results runner. | trips_tagger.js:117,126,757-766,1739-1751,1826-1863 |
| Publish / API outputs | publish_queue.dataset_key + configured table/view/path; differ matching includes entryxclasses_uuid | Existing publisher consumes configured datasets; it is not proven to consume only Timing snapshots. Prior nonempty outputs are protected against empty replacement. Exact API consumer-by-consumer contracts still need tracing. | publisher.js:22,34,813-858,872-875,960-969; observed errors below |
| Alerts | Existing event identity/deduplication and configured eligibility; full identity expression still to trace | Schedule calculator reads trigger_tags/active_alerts and writes thread_logs; separate ring-alert processing also exists. An event is not SMS-delivery proof. | schedules_calculatorv2.js:10-14,1837-1871; existing recurring-responsibilities document; deployed ring-alert evidence in endpoints |
| Two-way | Incoming MessageSid/sender/body; selected show and requested ring/rider/horse context | Incoming-message Worker outside the local heartbeat sequence. Prior source inventory identifies ringstatus-sms -> endpoint lookup -> reply/logging. Current deployed key/output contract not refreshed in this pass. | docs/coordination/messages-source-inventory-20261010.md; historical evidence clearly retained as historical |
| Audit | Per-run heartbeat/step timestamps; per-row calculation/change evidence | Logs occur throughout the stages. Code review, discovery and handoff documentation are maintenance work, not new runtime stages. | orchestrator logs; schedule_logs/trip_logs; existing recurring-responsibilities document |

## Actual dependency behavior in current code

- A due schedule failure blocks schedule calculation and dependent trip steps. Trip acquisition/tagger failure blocks dependent trip work. Trip calculation requires applicable trip work to have run. Publisher requires upstreamOk. Source: heartbeat_slot_orchestrator.js:1919-1962,2034.
- Live groups/rings/class state have their own DAY/slot checks and are dispatched separately from the schedule-failure condition. Their failure can still affect upstreamOk/publication. This is current behavior to preserve as reference, not an instruction to reproduce coupling in new responsibilities.
- A separate base does not establish independence. Future scoped work must identify required input data and its owner before copying any block. No new consumer or orchestration mechanism is selected here.
- Existing manual_time_override/legacy spelling protections and field-specific limits remain documented in wef-engine-recurring-tasks-manual-preservation-20261013.md. Do not promise all editable fields are protected or alter overrides during tracing.

## Natural runs observed, not manually executed

- 2026-10-15 21:35:49 Eastern (2026-10-16T01:35:49.901Z): slot A, NIGHT, show200000074, focus2026-10-16, heartbeat recHyJoQW7SgbRxch; schedules_dailyv2 started at01:35:50.216Z. The inspected tail does not yet contain completion for this run.
- Prior slot D completed at01:30:37.514Z. Trip acquisition process exited0, but its log says writes_blocked=true: /people/8778 and /people/19676 for show200000074 returned soft_payload_too_small (520/517 bytes). Exit0 is not populated-entry proof.
- Publisher at01:30:36Z saw44 queue rows and3 dirty bulk jobs. All3 manifest jobs (8778,9999,40362) reported missing_table_or_view despite process exit0. No new successful publication inferred.
- Latest inspected live-group log says no_focus_rows for show200000074/focus2026-10-15. Live-ring log says rows0/created0/updated0/dropped0. These are empty outcomes, not populated live proof. NIGHT dispatch does not provide new DAY evidence.

### Same observation, completion evidence added

The started schedule process completed at2026-10-16T01:37:18.233Z (October15 21:37:18 Eastern), exit0, duration88,017ms. Its summary reports show/heartbeat scope rows1/valid1 and active_groups sync skipped with reason active_tables_deprecated. The dispatcher then ran trips acquisition, tagger, calculator and OOG, all process exit0, and started publisher at01:37:26.714Z. This establishes dispatch order and process completion, not every-row persistence or useful trip/result output. No new Airtable business-row readback was performed in this bounded pass.

## Exact continuation

Read only new natural-run evidence. Next establish schedule row persistence/saved scope, then trace Timing/result/event keys and each output consumer against matching source inputs. For every stage record the actual read/write fields, source versus manual ownership, repeat/stop conditions and consumers. Missing entries/live data remain observed limits; do not execute jobs, fabricate records or repair the baseline to manufacture proof. Independent-function implementation remains a later, separately approved task.
