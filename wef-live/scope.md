# wef-live scope

Version: 1.3
Date: 29 October 2026 (America/New_York)

Updated 29 October 2026. This document describes this chat's responsibilities and the existing collector. Proposed work is explicitly marked; documenting it does not authorize implementation. Status evidence is dated, not a continuous health guarantee.

## Entries publication consumer � 29 October 2026

Approved simplification: the existing read-only Entries reference now syncs schedule-link-status, schedule-link-basis, group-link-status, link-issues and source-json. Automatic sync and editing restrictions are unchanged. Source reference view exposes only the existing twelve plus these five fields; no records were repaired or collectors manually invoked.

Live consumes the producer's accepted mapping rather than rereading source schedules/show-collection or comparing the entire source/synced dataset. It retains exact scoped identity, uniqueness, active/successful status, local schedule-key availability and prior-link preservation. Both original sourceAsOf and last-seen must agree and be within 30 minutes, retaining the existing one-minute future-clock tolerance. Publication/heartbeat times cannot renew freshness. Missing evidence or any newer applicable unfinished/failed attempt prevents current qualification. Ordinary class-date mapping requires one schedule; observed-group mapping additionally requires linked group status, matching group ID and same-revision evidence. Live does not infer group eligibility from class-date association.

Live ring/group/class schedule links, collection, archive protection and all three logging tables are unchanged. Missing/stale/unresolved references preserve prior links as historical and record issues while observations continue. The checks described in older release sections below are historical where superseded by this consumer contract.

Verification: 54 tests passed. Normal 13:50 and 13:53 Eastern runs completed Success with matching heartbeat/run-logs, no errors, verified archives and all 397 records/links unchanged. Both had empty live activity; fresh populated qualification remains offline-tested rather than naturally demonstrated. See evidence/entries-contract-20261029/release.md.

## Tenant-code removal � 29 October 2026

Approved Live-only implementation: runtime comparison, uniqueness and overlap-lock helpers now live in `runtime-utils.js`; verified archive creation now lives in `archive.js`. Their behavior is preserved from the deployed baseline, with tenant lock-error labels renamed for Live. Runtime and verification imports no longer load tenant modules. The unmatched-participation explanation now says �No available validated entry reference.� Existing identity matching, links, archive protection, cadence and upstream collectors are unchanged.

All 49 existing tests and three focused dependency/utility tests passed. Two normal scheduled runs at 13:11 and 13:14 Eastern completed successfully; heartbeat/run-logs agreed, errors were absent, verified archives were readable, and all 397 business records and links were unchanged. Both were no-live-activity outcomes; populated collection behavior is covered offline, not newly demonstrated by these runs. See `evidence/tenant-decoupling-20261029.md` for exact acceptance. Earlier documentation-only statements below describe the prior correction, not this separately approved implementation.

## Workflow correction — 29 October 2026

CWF is removed from all workflow responsibilities, collection parameters, dependencies, linking requirements and acceptance checks. Core collection is tenant independent; tenant filtering is downstream and does not control collection. Entries is a linking module only, consuming accepted Data/Season and schedules. The local Season coordinator owns Data collection/retention and Season stewardship. This is a documentation correction, not proof that deployed code has been changed or audited for remaining references. Code, Airtable and scheduled tasks remain unchanged.

## Purpose

Collect today's live ring, group, class and trip observations independently, connect them to schedules and eligible entries, and provide traceable observations for downstream timing and tenant use.

## Ownership

| Owner | Responsibility |
|---|---|
| This wef-live chat | Inspect and maintain approved wef-live code/schema, test changes, verify normal scheduled behavior, document evidence/gaps and coordinate authorized handoffs. |
| Windows task `wef-live-collector` | Execute the production collection every three minutes. Codex does not act as the runner. |
| wef-shows | Sole new/default show discovery and canonical show/date authority; proposed canonical daily readiness publication. |
| wef-schedules | Operational dated schedules, rings, source keys and dated collection health. |
| wef-entries | Linking module only: connects accepted Data/Season participation and entity identities to schedules and local records. No independent source acquisition or tenant selection. |
| Downstream tenant consumers | Slice independently collected data by their parameters; no tenant controls core collection. |
| Data | Complete per-show acquisition, retention, completeness/count evidence and accepted revisions. |
| Season | Cross-show stewardship, history and indexes from verified Data; a distinct writer from Data. |
| LOCAL wef-season-coordinator | Coordinates both Data and Season responsibilities. Former cloud wef-data-coordinator is historical evidence/reference only. |
| wef-records-coordinator | Cross-owner contracts and rollover acceptance. |
| wef-timing | Continuous calculations from qualified observations. Live does not calculate predictions. |
| Results / messaging / publish / waze | Official results management, alerts/two-way messages, Webflow/Astro/API publication, and user interaction respectively; outside Live ownership. |

## Bases and tables

Owned destination: **wef-live — `appgzosUcVqA4vZdf`**.

| Table | Purpose |
|---|---|
| live-rings | Ring identities and observed current/next/completed group references. |
| live-groups | Dated group state, source counts and class/ring/schedule links. |
| live-classes | Dated class observations, counts, current trip and related group/ring/schedule links. |
| live-entries | Live trips with recovered entry/horse/rider identities and validated participation links. |
| collections | Source collection outcome, fingerprint and archive reference. |
| heartbeat | One row per collector invocation, keyed by run ID; start and final outcome. |
| run-logs | Run result, scope, counts, errors and archive details. |
| errs-log | One error per record; err-class, err-trip and err-group only when known. |
| wef-schedules.schedules | Read-only synced schedule references. |
| wef-entries.entries | Read-only synced participation references. |

Read-only dependencies: wef-shows `appCaSmL819KK7UFJ` (`shows`); wef-schedules `appUq5Qz9kARqFija` (`schedules`, `show-collection`); synced records originating from wef-entries `appSxF4pYMBaxpTZd`. No writes to upstream or tenant bases under Live scope.

## Recurring workflow — implemented

1. Hidden launcher runs `run-hidden.vbs` → `run.ps1` → `entry.js`. Acquire the existing overlap lock, then stamp the invocation heartbeat Running.
2. Read wef-shows. Require exactly one default for customer 15, valid show key/date/range and authority checked within 30 minutes. Reject missing, stale or ambiguous authority.
3. Archive and label retained observations outside the canonical show/date historical. Preserve IDs, raw evidence and native links. Previously current participation labels become historical. Repeated labeling makes no further changes.
4. Require selected date to match today in America/New_York before live acquisition. An upcoming selected day is a successful no-collection outcome, not a replacement date chosen by Live.
5. Read the explicit live-status boolean. False skips detailed collection; later scheduled checks continue. True is permission to inspect availability, not proof of live rings or entries.
6. Read the live group list for the canonical show/day. If every nonempty group is completed, skip detailed work. Empty groups are not treated as all-completed.
7. Read local synced schedule/participation references; read ring widget state. A validated empty group/widget response completes successfully without deleting observations. Malformed responses or missing expected rings remain errors.
8. For available classes, collect live details and decoded identities. Match trip/entry/horse/rider identities; retain source evidence. The existing runner allows a bounded second snapshot read for a live identity mismatch. Persistent class errors are logged while other classes continue.
9. Archive prior state, normalize and upsert Live's observations and links. Fresh observations receive scope-status current. This scope label alone does not prove fresh, resolved participation or timing readiness.
10. Only after a clean qualifying populated collection, archive and retire eligible earlier-date records or same-date records for another valid show. Verify exact archive content/hash before deletion. Protect current-show/current-date and future-date records. Empty/skipped runs do not authorize cleanup.
11. Finish run-logs, then heartbeat; a brief Success/Running difference can occur between those writes. Persist receipts and separate error rows. Logging failures are isolated from collection. A failed run remains failed but does not permanently halt the next scheduled invocation.

## Source endpoints

These are scheduled runner inputs, not instructions to call them manually.

| Input | Endpoint |
|---|---|
| Live true/false | `https://sglapi.wellingtoninternational.com/homepage/getLiveClassStatus?customer_id={customer}&datetimestamp={timestamp}` |
| Group list/status | `https://sgl.wellingtoninternational.com/iphonev2/index.php/esp/liveclassv2/ListAjax?from_wp_api=true` |
| Ring widget | `https://sgl.wellingtoninternational.com/iphone.php/esp/webservice/LiveScoreWidget` |
| Live class details | `https://sgl.wellingtoninternational.com/iphonev2/index.php/esp/liveclassv2/getLiveClassData?show_id={show}&cid={class}&t={timestamp}` |
| Decoded class identities | `https://wef-schedules-decoded.gombcg.workers.dev/classes/{class}?show_id={show}&customer_id={customer}&cgid=-1` |

LiveScoreWidget returns ring data, not the boolean status. ClassStatus and `/ring/...` were discussed as reference endpoints; they are not additional current collection steps. Do not alter shared source/decoder modules or legacy routes under this scope.

## Link and identity rules

- Keep source show, group, class, entry, trip, horse and rider IDs distinct. Do not infer missing IDs from names.
- Schedule links require exact active show/date/class/group matches; dropped rows are excluded. Classes may link to multiple groups without assigning every group to every participation.
- Participation key is `show|date|entry|class`. Match the full identity, active status and a successful observation within 30 minutes.
- Consume the producer's accepted schedule association and original-source evidence using the contract above; validate referenced local keys without replaying upstream completeness/parity checks. Do not invent group eligibility.
- Valid participation links are current; failed validation preserves prior links as historical. Unlinked failures are unresolved. Live trips without an available validated entry reference are unmatched and may still be collected.
- Retained old observations are not evidence of current activity. Consumers must use canonical show/date, observation freshness and participation qualification together.

## Errors, records and archives

Preserve proven keys, helpers, native links and source evidence. Log each class/trip/group issue separately, without guessed identifiers. A class error should not stop unrelated classes. Invalid authority, mandatory archive failure or other collection-level integrity failures may fail that run; never claim empty success from those failures.

Existing local receipts and hashed archives live in `runtime/`; `runtime/collector.env` is private and must never be copied into documents. Local archives are the existing implementation and evidence, not authorization for a new durable local storage design. Any migration to the broader Airtable business-storage contract is separate scope. Archive-before-delete remains mandatory.

## Daily readiness gate — proposed, not implemented in Live

Live should consume verified daily totals/readiness published by Shows alongside its existing live-status check. It must not recreate Data counting or Show discovery.

Latest owner-chat reports on 29 October: Data has begun producing daily summaries with normal-run verification; Shows reports the show-days schema created and publication work in progress. These reports do not prove completed publication or Live integration. Exact consumer fields and accepted publication evidence must be confirmed before implementation.

Prerequisites:

- Exact customer/show/date, count policy, coverage, accepted complete evidence, source timestamp and valid revision.
- Only an explicit verified zero may suppress the acquisition covered by that evidence. Missing/stale/partial/error counts remain unknown.
- Filtered downstream counts, zero trips or no competition classes cannot suppress all Live activity. Schooling/classless coverage remains unproved; uncovered ring/group activity must not be suppressed.
- Planning policy reported by the local coordinator: scratches count; verified drops are excluded but retained historically; undated evidence is show-only; a zero lease is five minutes from accepted source time. Confirm the published contract rather than hard-coding this planning statement prematurely.
- Continue lightweight authority/readiness rechecks. Revised counts, added entries, corrected evidence, expiration or default/day changes must release suppression under the approved policy. No permanent day shutdown.
- Preserve heartbeat/run-log evidence of each skip, including scope/revision/reason. Skips do not authorize deletion or establish fresh observations.
- Separate approval, offline coverage and normal Data → Shows → Live skip/resumption verification are required before claiming this gate operational. Three-minute cadence remains unchanged.

## Downstream timing boundary

Supply raw observations and qualification, not predicted start times, inferred results readiness or certified event clocks. Distinguish last-seen observation time, source-provided time labels and downstream calculation time. Saved `estimated_go_time` is time-only, can change and may be zero. Saved `jo_start` is a source-reported candidate, not a proven actual phase anchor. Phase flags/counts are not interchangeable. Results publication and notifications belong to their respective owners. See the saved findings in HANDOFF.md.

## Verification and handoff responsibilities

For approved changes: inspect the existing implementation and deployment route, make the smallest in-scope repeatable change, run relevant offline checks, and verify accepted outcomes through normal scheduled runs. Separate offline proof, observed production behavior and unproven cases. Keep a concise evidence record and handoff with exact files, run IDs/times, protected behavior, failures and next dependency.

No manual collector runs, forced syncs, seeded production proof, record repairs or substitute source calls. A failed approved runner path requires separate authorization before another patch/retry of that path. Coordinate with other owner chats only when the user authorizes messaging. Do not modify other collectors, cadence, shared modules or publication routes without applicable approval.

Current open acceptance items:

| Item | State |
|---|---|
| Empty-response handling | Implemented; two normal October 29 runs verified. |
| Historical rollover labeling | Implemented; two normal runs plus later read-only repeat checks verified record/link preservation. |
| Same-date old-show deletion | Implemented and tested offline; **actual natural deletion proof still pending**. No qualifying production case was manufactured. |
| Populated new-show links and sync-lag behavior | Natural rollover acceptance remains open. |
| Full cross-owner rollover | Not established by Live's empty runs or labeling proof. |
| Participation-specific multi-group eligibility | Reader support exists; natural producer proof remains open. |
| Class 200030429 / trip 200398240 | Historical unresolved identity evidence; separate from rollover/readiness, not asserted to be a current-show problem. |
| Shows readiness consumer | Not implemented in Live; publication and coverage dependencies above. |

## Files and evidence

- `collector.js`, `model.js`, `rollover.js`, `participation.js`: orchestration, normalization, rollover and link validation.
- `activity.js`, `source.js`: source routes and activity gates.
- `airtable.js`, `schema.js`: owned writes, read-only dependencies and archive-protected retirement.
- `entry.js`, `run.ps1`, `run-hidden.vbs`: scheduled execution and receipts.
- `test/`: offline verification; no production collector invocation.
- [HANDOFF.md](HANDOFF.md): detailed dated contract, history and timing findings. Older sections may be superseded by its top updates.
- [Empty-response evidence](evidence/empty-response-20261029.md).
- [Rollover labeling evidence](evidence/rollover-labeling-20261029.md).
- [Participation-link release](evidence/participation-link-release.md).
- [Per-invocation heartbeat evidence](evidence/heartbeat-per-invocation-20261025.md).

This scope document does not change code, Airtable, task configuration or workflow authorization.
