# WEF Timing coordinator

Updated October 30, 2026. Current role and task record. Documentation does not authorize code edits, deployment, scheduling, data changes or resumption of stopped work. Codex is not the runner.

## Ownership and scope

- Coordinator: wef-timing-coordinator, 01a169bc-0bd6-77d3-b7b4-283a07061e90. Keep one accepted task, reconcile evidence, coordinate owners and delivery. Do not author calculator repairs.
- Ring: 01a16f18-1cfe-7663-85a5-71dc2bbe3576. Writes Ring pace and approved shared runner/API integration.
- Class: 01a16f18-25b9-7222-9b53-b64496ec84d9. Owns Class calculation and independently reviews Ring/shared changes. Review actual code and downstream consumption, not the writer's summary.
- Entry: 01a16f18-32b3-7900-8bbc-e8de123ca3b8. Owns Entry calculations; currently paused/disconnected. No reconnection by inference.
- Records coordinates Data/Live source contracts. Messages and Results are independent and coordinate through Timing/Records. Schedule serving belongs to Publish/application owner.
- The four absorbed specialist assignments are historical evidence, not additional active owners. See each owner workspace's specialist handoff.

Keep Ring/Class correction, retention and Entry reconnection separate. Do not bundle unfinished work into a new approval request or revive another backlog item while explaining the current task.

## Accepted Ring to Class contract

1. Evaluate every relevant group/class for TODAY in America/New_York, every three minutes. No group/class caps, SQLite, Schedules calculation lane, legacy mutations or manual business runs.
2. Approved Ring input target is the EXISTING synced wef-live.groups table in wef-ring-pace. Do not substitute old external collection archives or request a new collector to solve this wiring task.
3. Observed pace uses elapsed recorded observation time divided by additional completed trips for the same group/count scope. Actual start and provider clock are optional for a separate start-based method; not universal prerequisites.
4. Underway to Completed ends expected updates. Preserve independently proven pace and its ORIGINAL measurement endpoints/time. Verified final same-day/group/class pace remains usable without age-only expiration. Never renew clocks to manufacture freshness.
5. Keep old proven pace separate from NEW count quality. A counter reset/bad final count must not fabricate a new interval, erase trusted historical pace or block unrelated calculations. Preserve raw values and local reasons. Never infer actual finish, class/all-round completion or cross-group reuse.
6. Class consumes compatible Ring pace, then the approved three-minute fallback. Group pace/status is not class trip counts, actual start or official Results finality.
7. Use truthful source base/table/record/material hash and original source time. Reject wrong identities, future/malformed clocks and ambiguous same-group evidence. Treat malformed data locally while unrelated valid groups continue.

## Verified tables and inputs

| Purpose | Exact base/table |
| --- | --- |
| Ring answers | wef-ring-pace appYOykKIy1kAquhP / timing-answers tblhIjSjeGs5td23c |
| Shared checkpoint | Same base / tblfWyTqeyvIvhAjb; ring-pace-data-runner |
| Existing Ring synced groups | Same base / wef-live.groups tblSmVxVsHlpudxlQ |
| Existing Ring synced rings | Same base / wef-live.rings tbleertfMY7ISGZCh |
| Existing Ring synced classes | Same base / wef-live.classes tblREU7auaqFoeKZ8 |
| Existing Ring synced entries | Same base / wef-live.entries tbllLxVo8kgHgxrMk; not required for Ring pace |
| Class answers | wef-class-calculator appeqxxJyPGS1iqdm / timing-answers tblmoBFySYW2TkDHf |
| Entry base | appyoLTcNmLrgpHcg; currently paused |

Shows supplies unique default show and selected date; selected date must match TODAY. Data retained immutable handoff supplies verified complete dated membership and schedule evidence. Exact group/class/round scope must remain distinct.

Native sync fields have no private archive _classes property. Derive class source identities from validated raw source-json.classes and exact show/date; physical links are destination record IDs. source-updated-at is not automatically a sporting start or observation clock. Synced Live tables are READ ONLY.

Current source correction: flagged group200028193 has older Live/synced flat total9/gone0 at12:53:26 Eastern. Newer Data class200030821 detail explicitly names the same class_group_id, grouped_class=false, and total_trips9/completed_trips9. Correct completion data exists. Do not label the provider data bad or blindly replace group/round counts with class counts; identify the consumed component and its meaning.

Class base was verified to have no synced input tables. Proposed Class syncs (Ring answers filtered ring-pace plus Live groups/classes/rings) are recommendations ONLY; not created or wired.

## Deployed and verified scope — October30 17:30 Eastern

CURRENT deployed package: C:/Users/gombc/Documents/Codex/staging/wef-ring-sync-final-20261030, installed17:24:03 after Class independent full five-file review. All three reviewed defects and the obsolete assertion are corrected;145/145 offline checks and six independent focused shared-path cases passed. Exact current source hashes are in release-hashes.json and independently matched live after both scheduled runs. Old failed sources/tests/results are preserved in failed-review-baseline; they are not current runtime.

Two NORMAL runs passed: c6d06c99-0540-4dcb-9c20-1a35d0c65926 at17:25:02–17:26:33, and93554292-c1a8-4175-8423-289b0f767856 at17:28:01–17:29:08. Both evaluated all32 TODAY groups/38 classes with no execution errors or skipped groups/classes. First32Ring+10Class writes; second17Ring+10Class writes,15Ring/28Class unchanged. Class independently recomputed all69 new material hashes and verified identities/keys/native physical references/original source clocks.

Actual native sync input contained19 groups,17 final. Ten FINAL SCHEDULE-WINDOW pace estimates remained usable and were consumed by all10 matching Class updates on each run. Zero live observed intervals were available; do not call the estimates measured pace. Old proven measured pace through Completed/reset/out-of-range counters is verified by the actual isolated offline shared-path regression, not a manufactured production transition.

The second run was not zero-write: Ring reason fields changed from baseline-required to repeated-observation, and Class references followed the new immutable Ring answers. Rates/counts/original clocks/material evidence otherwise stayed unchanged;69 answer keys/IDs across both runs were unique. Subsequent steady-state no-op behavior is not proved by these two invocations.

Separate limitation: all38 Class end-time answers remain unavailable because their start/count evidence is missing or stale under the unchanged policy. Final pace consumption is proved; full class end-time readiness is not. No actual finish, class/all-round completion or Results readiness is inferred.

The current authorized correction/review/deployment/two-normal-run scope is COMPLETE. Collectors, schema, Entry disconnect, retention stop, cadence and launchers are untouched; no manual business runs or record repairs. Do not re-request this approval or repeat acceptance scans. Retention remains archive_required and requires its own resumed scope. Canonical acceptance: C:/Users/gombc/Documents/Codex/staging/wef-ring-sync-final-20261030/COORDINATOR-ACCEPTANCE.md and its linked artifacts.
## Required execution and review order

- Before editing, retain the exact deployed baseline, accepted behavior, owner corrections, scope and approval. Reuse working implementations; no speculative new sources or architecture.
- Before first verification, reconcile existing expected assertions with the accepted behavior. An obsolete assertion is not evidence of a runtime defect. Preserve its result and diagnose it; never weaken business guards to force a pass.
- Ring freezes the entire in-scope payload and changed consumer contract. Class reviews the actual path: native synced read -> identity/progress/pace -> saved snapshot/readback -> real Class worker/adapter/evaluator. Do not mock away this handoff.
- Review normal and negative interactions together: invalid clocks plus completion; valid/malformed duplicate in both orders; final counter reset with proven prior interval; unchanged repeats; unrelated groups continuing. Return all bounded findings together.
- Reuse valid unchanged checks. More test counts, copied fields or successful task exits are not acceptance. Keep code review, offline proof, deployment, scheduled execution and usable calculations separately reported.
- An approval that already covers reviewed deployment and normal verification does not need another deployment approval after a PASS. A failed approved verification remains governed by the supplied fresh-APPROVED-TO-EDIT rule; do not reinterpret frustration as waiver.
- Use the established wef-timing-data task route, coherent unique backups, idle/no unresolved intent guards, then normal scheduled verification only. No record repair, substitute runner calls, blind lock clearing or repeated broad preservation audits.
- Report completion only for the criterion actually proved. Report unusable or missing components without calling the whole function working.

## Retention is a separate unfinished task

The approved snapshot policy requires past-day AND at least24-hour-old records, full compressed archive/hash/count and same-base catalog verification before deletion, unchanged-record recheck/durable intent/delete absence proof. Current/future records for ALL shows, synced Live tables and active controls remain protected. Midnight does not require empty tables; TODAY selection is independent of cleanup.

Current deployed Ring code has archiveReady=false/archive_required; Class cleanup is not wired; Entry has candidate-manifest planning only and cannot archive/delete. No operational archival claim is supported by the presence of archive-catalog tables.

Reuse existing staged retention helpers in wef-ring-pace-retention-20261030 (approved fixture correction160/160) and reconciled intent guards in wef-timing-retention-reconciled-20261030 (151/159 failed from outdated expectations). Do not replace the current runner/API with those older packages. Remaining integration includes safe archive-only progression during stale inputs without ANY deletion, separate stricter deletion authority, nonstarving base/lane cursors, explicit owned-table writes and full normal archive/catalog/deletion proof. Fresh scoped retention approval is required; native-sync correction approval does not resume failed retention. Entry retention/reconnection remains separately owned and paused.

## Instructions and continuity

Read applicable supplied global rules, workspace/repository AGENTS.md and explicitly invoked instruction-loader/code-review/rebalance skills. Use context-compact at task boundaries/30 active minutes; prepare private checkpoint at65% actual estimated context or explicit request. Preparing a checkpoint is not running compression; claim an event only after telemetry verifies it. Do not create new monitors/hooks/chats or edit root AGENTS.md.

Keep user-facing updates brief: result, evidence, next action. Do not repeat requirements, add new approval gates or use explanations as a substitute for authorized work. Preserve a precise pending permission when no independent authorized work remains.


