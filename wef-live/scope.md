## Approved 24-hour log retention — 30 October 2026

Owner approved recurring archival/removal of completed heartbeat, run-logs and errs-log records older than24hours. This narrow release does not fix publication defects or change cadence/source collection. Existing normal entry workflow calls retention only after collection ok=true (Partial class/publication outcomes can still be ok=true); failed collection skips it. No manual runs/record repairs.

Retention accepts terminal Success/Skipped/Partial/Failed completion older than24hours; active/incomplete/current invocation and non-run control rows are preserved. Error rows additionally require older completed parent evidence from existing run-logs or saved local receipts. At most250 rows per table per invocation, with four-minute request deadline and a60-second reserve before starting another table; initial backlog drains across normal runs.

Exact immutable gzip batches are verified before deletion. Three fixed collections rows (`log-archive|heartbeat`, `log-archive|run-logs`, `log-archive|errs-log`, source-kind log-archive-catalog) point to immutable chained manifests, retaining previous references/hashes and full original row IDs/fields. Existing seven show archive catalogs and business collections are untouched. Manifest row-count counts archived records. No local archive TTL/purge is introduced.

Every deletion batch rereads unchanged eligible rows, validates exact DELETE acknowledgements and verifies absence. Durable runtime/log-retention-stop.json is created before work, removed only on successful completion or safe deferral before a new table, and retained on failure/interruption. That marker blocks retention without stopping independent collection; no automatic retry/repair of uncertain deletion. Failure requires owner review/approval. Current run logs/heartbeat summarize retention and retain archive descriptions. Local receipts/logs and publication journals/control state are never deleted.

Evidence: evidence/log-retention-20261030/{baseline.json,review.md,tests.txt,deployed.json}.91offline tests passed; independent11retention tests passed. Normal-run acceptance BLOCKED: first postdeployment10:41run failed with source ListAjax HTTP504; retention correctly skipped, no log deletion. Two successful normal retention runs remain unverified (evidence/log-retention-20261030/release.md). Existing3P2publication findings and populatedPartial acceptance remain separate.

## Current operating checkpoint — 30 October 2026

Read [module AGENTS](AGENTS.md), [current README](README.md) and [canonical owner block](../docs/coordination/wef-owner-operating-instructions.md) first. README's current operating reference supersedes conflicting historical paragraphs below. Live owns independent TODAY observations and approved one-field overlays only; Shows is canonical, Data acquisition/Season retention remain with LOCAL Season coordinator, Entries linking-only, Schedules still running. Three-minute collection and per-run recurring archive safeguards are unchanged.

Documentation-only task: canonical block and four installed Operating Skills0.2.0 files actually read; no runtime changes/fixes. Review remains FAIL3P2 / tests80PASS / populatedPARTIAL; record rec5fLj3xEqkYBpxA in tbl1jEtJusopJGC6O/appgzosUcVqA4vZdf readback confirmed by Records. Records confirmed automation `wef-live-monday-code-review` ACTIVE/readback verified, targeting this chat, Mondays 10:00 America/New_York, first November 2, 2026, bounded to 20 minutes. Routine record `recYUERW7D8RF2OXS` in the same code-reviews table remains Pending first review; prior release review stays distinct. Next action: the configured periodic review, not an early/manual collector run. No independent automation creation. Preserve all failure/approval boundaries. No compaction executed or claimed.

## Code-review procedure — 30 October 2026

Review material Live changes before deployment and review observed behavior/verification failures against the exact deployed baseline. Use the explicitly requested code-review skill at C:/Users/gombc/.agents/skills/code-review/SKILL.md. Record release/hash manifest, reviewer/date/scope, actionable severity/file/symbol/trigger/consequence/smallest correction, tests actually inspected/run, scheduled evidence separately, gate-specific PASS/FAIL/PARTIAL/BLOCKED and exact next authorized action. Offline PASS never establishes scheduled acceptance.

Do not rerun an unchanged release review on every invocation or create review timers/monitors. Reviews authorize no fixes, deployment, schema/business-data/task/cadence/source requests, manual collection or repairs. Applicable explicit approval is required for corrective work. Preserve module ownership and existing failure boundaries. For the October 30 review request, Records alone owns Airtable review schema/record writes and readback; Live supplies its report for base appgzosUcVqA4vZdf. This procedure does not reopen migration, retirement or ownership.

## Approved Live publication v1 — 29 October 2026 (scope v1.7)

Live independently collects TODAY observations. Entries is linking-only and is neither read nor written by Live; CWF has no role. Shows remains canonical show/day authority. Source collection, schedule links, logs, three-minute cadence and recurring verified archive-before-delete retirement are preserved.

The narrow approved exception lets Live GET existing destinations and PATCH only `live-observation-json` on Schedules rings/schedules and Data rings/groups/classes/schedules/trips. Seven multiline fields were created and read back. Live owns this overlay, exact matching and its durable audit. Schedules and Data retain their independent collection, fields, keys, records, native links and lifecycle. No sibling runtime, sync, task or record repairs are included.

Only validated observations within six minutes of their original receipt (one minute future tolerance) may publish. Empty/missing/stale detail does not clear or renew overlays. Class counters require successful class detail; ambiguous multi-group occurrences are withheld. Trips require exact dated identity and supported source provenance. Lanes run independently with bounded requests; uncertain writes retain durable intent and require readback on a later normal invocation. Source/retirement safety stops remain unchanged.

Publication audit files retain full source evidence, destination before/proposed-after, intent, outcome and readback. Existing logs also report lane results and separate publication errors. Original acquisition fields and links are never publication writes. Entries has no new enrichment assignment.

Deployment and review evidence: `evidence/publication-v1-20261029/deployed.json`, `review.md`, `tests.txt` (80 passing offline tests). Two NEW normal runs at 20:29 and 20:32 Eastern passed with empty activity, both lanes Skipped, matching logs and no errors. All baseline destination IDs/keys/native links and seven archive catalogs/hashes are preserved; only Data-owned last-success timestamps advanced. Acceptance remains PARTIAL: two naturally populated runs with PATCH/readback and repeat behavior are still required. Old v1.6 runs do not count. No manual runs or seeded proof.

Earlier locks and descriptions below remain historical except for this explicitly approved publication addition.

## LOCKED - wef-live approved scope v1.6 - 29 October 2026

Owner directs this module to remain locked to its latest approved implementation. This scope lock does not stop the three-minute collector. Preserve independent TODAY source collection and identity validation, Shows/Schedules reference reads, logging, existing links/timestamps, verified recurring archive/catalog retirement and current/future protections.

CURRENT dependency correction: Live does not read, qualify or wait for Entries. Its v1.6 reverse-read removal was separately approved, deployed19:33:37Eastern and verified by normal19:35/19:38runs (evidence/no-entries-read-20261029/release.md). Entries' separately approved linking-only restoration also removed its added Live reader and enrichment. Therefore neither current collector reads the other. Keep existing sync tables/native links/historical evidence intact; their presence does not create an active dependency. No Live preparation, enrichment reader, comparison work or new cross-module contract is pending.

Both v1.6 normal Live runs were empty activity; populated link preservation is offline-tested, not naturally proved. Preserve this limit; do not reopen work or manufacture activity to fill it. Separately approved v1.4 recurring retirement remains: verified archives/catalogs before eligible removal on successful valid empty/skipped runs; failed/partial/unresolved inputs do not authorize cleanup; current/future protected.

Reopen only for a verified defect or explicitly approved requirement. Defect investigation does not itself authorize fixes/deployment/task/schema/sync changes or record repairs. Consumer owners own their future reads; no future Entries input is implicitly assigned here. Older v1.5/Entries-read wording below is historical, superseded and not an active assignment. No runtime changes are made by this documentation lock.

## Reverse dependency removed - 29 October 2026 (v1.6)

Live independently collects observations; the current restored Entries collector does not read Live. Live no longer reads the Entries reference, inspects its publication, or qualifies its schedule/participation results. The Airtable adapter rejects Entries reads and metadata discovery; the unused Entries base setting is removed. The existing sync/table/link fields remain intact to preserve native links and history, not as a collection dependency.

For collected observations, existing native entry links and original verification/check timestamps are omitted from writes and preserved. Previously current participation qualification is marked historical because Live no longer validates it. New observations do not create Entries links. Source identity validation, class/ring/group schedule links, class-error logging and recurring verified archive retirement are unchanged. Entries owns current downstream qualification. No manual runs, record repairs or source sync changes.

58 offline tests passed, including populated repeat collection with Entries reads forbidden, exact link/timestamp preservation, unresolved classes, logging and archive/retirement safeguards. Deployment: 19:33:37 Eastern through the existing scheduled task path. Two normal runs at 19:35 and 19:38 Eastern passed: matching Success logs, no errors, unchanged archive catalog and verified file hashes. Evidence: evidence/no-entries-read-20261029/release.md. Both were empty activity; populated behavior remains offline-tested.

## Prior v1.5 inspection - superseded by v1.6 above

**Entries reads wef-live for downstream linking. Live owns independent observation collection.** Entries is linking-only, not a collector, lane-comparison or tenant-selection engine. CWF is not a core input, dependency or acceptance requirement. Season is retention-only and never a current collection gate.

Implementation distinction: Entries collector.js now invokes live-input.js and allows GETs of Live live-entries, live-classes and collections. Live collector.js still reads the older wef-entries.entries reference and calls participation.resolve. This remaining reverse dependency is existing compatibility behavior, not the intended ownership direction. It was not removed by this documentation update; removing it requires separately approved code work preserving links. No populated downstream acceptance is claimed here.

Health checked 29 October at 19:30 Eastern: normal 19:26 and 19:29 runs completed Success, with matching heartbeat/run-logs and no error rows or logging errors. Three-minute cadence remains. All seven old-show archive catalogs are Retired; hashes and exact fields/links verified for 550 archived records. No show 200000075 observations remain active. Current activity is empty; no duplicate retirement occurred.

## 29 October 2026 - v1.4: old-data retirement on successful empty/skipped runs

Owner explicitly approved removing the populated-activity prerequisite. After a valid canonical scope and successful collection outcome (including empty or skipped activity), Live archives and retires eligible older owned observations and operational collections. Partial/unresolved/failed runs still do not authorize retirement. Current scope and future dates remain protected; an upcoming selected date cannot retire today. All snapshots and permanent archive-catalog rows are verified before deletion; entries precede classes/groups/rings and operational collections are last. Catalog rows remain in collections with old show/date, counts, file path, hash and cleanup result. Exact delete acknowledgment IDs are required. Failures stop the retirement attempt without alternate/manual retries.

This supersedes earlier statements that empty or skipped runs always retain old rows. Existing native link values and record IDs are retained in exact archives; removing active records necessarily removes their active reciprocal links. Synced source references are not edited or deleted. Cadence and source collection remain unchanged. 62 offline tests passed; deployed through the existing scheduled-task file path. Two normal runs at 19:08 and 19:11 Eastern passed: 550 older records archived/retired, seven catalogs retained, exact archive fields/links verified, then no repeat deletion. See evidence/retirement-20261029/release.md.

# wef-live scope

Version: 1.6
Date: 29 October 2026 (America/New_York)

Updated 29 October 2026. This document describes this chat's responsibilities and the existing collector. Proposed work is explicitly marked; documenting it does not authorize implementation. Status evidence is dated, not a continuous health guarantee.

## Existing reverse reference - prior implementation evidence — 29 October 2026

Approved simplification: the existing read-only Entries reference now syncs schedule-link-status, schedule-link-basis, group-link-status, link-issues and source-json. Automatic sync and editing restrictions are unchanged. Source reference view exposes only the existing twelve plus these five fields; no records were repaired or collectors manually invoked.

Live consumes the producer's accepted mapping rather than rereading source schedules/show-collection or comparing the entire source/synced dataset. It retains exact scoped identity, uniqueness, active/successful status, local schedule-key availability and prior-link preservation. Both original sourceAsOf and last-seen must agree and be within 30 minutes, retaining the existing one-minute future-clock tolerance. Publication/heartbeat times cannot renew freshness. Missing evidence or any newer applicable unfinished/failed attempt prevents current qualification. Ordinary class-date mapping requires one schedule; observed-group mapping additionally requires linked group status, matching group ID and same-revision evidence. Live does not infer group eligibility from class-date association.

Live ring/group/class schedule links, collection, archive protection and all three logging tables are unchanged. Missing/stale/unresolved references preserve prior links as historical and record issues while observations continue. The checks described in older release sections below are historical where superseded by this consumer contract.

Verification: 54 tests passed. Normal 13:50 and 13:53 Eastern runs completed Success with matching heartbeat/run-logs, no errors, verified archives and all 397 records/links unchanged. Both had empty live activity; fresh populated qualification remains offline-tested rather than naturally demonstrated. See evidence/entries-contract-20261029/release.md.

## Tenant-code removal — 29 October 2026

Approved Live-only implementation: runtime comparison, uniqueness and overlap-lock helpers now live in `runtime-utils.js`; verified archive creation now lives in `archive.js`. Their behavior is preserved from the deployed baseline, with tenant lock-error labels renamed for Live. Runtime and verification imports no longer load tenant modules. The unmatched-participation explanation now says “No available validated entry reference.” Existing identity matching, links, archive protection, cadence and upstream collectors are unchanged.

All 49 existing tests and three focused dependency/utility tests passed. Two normal scheduled runs at 13:11 and 13:14 Eastern completed successfully; heartbeat/run-logs agreed, errors were absent, verified archives were readable, and all 397 business records and links were unchanged. Both were no-live-activity outcomes; populated collection behavior is covered offline, not newly demonstrated by these runs. See `evidence/tenant-decoupling-20261029.md` for exact acceptance. Earlier documentation-only statements below describe the prior correction, not this separately approved implementation.

## Workflow correction â€” 29 October 2026

CWF is removed from all workflow responsibilities, collection parameters, dependencies, linking requirements and acceptance checks. Core collection is tenant independent; tenant filtering is downstream and does not control collection. Entries is linking-only, reading accepted participation/schedule evidence and Live observations. Season is optional retained context, never a current collection gate. The local Season coordinator owns Data collection/retention and Season stewardship. This is a documentation correction, not proof that deployed code has been changed or audited for remaining references. Code, Airtable and scheduled tasks remain unchanged.

## Purpose

Collect today's live ring, group, class and trip observations independently, connect them to schedules and eligible entries, and provide traceable observations for downstream Entries linking and Timing calculations.

## Ownership

| Owner | Responsibility |
|---|---|
| This wef-live chat | Inspect and maintain approved wef-live code/schema, test changes, verify normal scheduled behavior, document evidence/gaps and coordinate authorized handoffs. |
| Windows task `wef-live-collector` | Execute the production collection every three minutes. Codex does not act as the runner. |
| wef-shows | Sole new/default show discovery and canonical show/date authority; proposed canonical daily readiness publication. |
| wef-schedules | Operational dated schedules, rings, source keys and dated collection health. |
| wef-entries | Linking only: reads accepted participation/schedule evidence and wef-live observations; validates supported associations and preserves identities, provenance and source times. No acquisition, comparison or tenant selection. |
| Downstream tenant consumers | Slice independently collected data by their parameters; no tenant controls core collection. |
| Data | Complete per-show acquisition, retention, completeness/count evidence and accepted revisions. |
| Season | Cross-show stewardship, history and indexes from verified Data; a distinct writer from Data. |
| LOCAL wef-season-coordinator | Coordinates both Data and Season responsibilities. Former cloud wef-data-coordinator is historical evidence/reference only. |
| wef-records-coordinator | Cross-owner contracts and rollover acceptance. |
| wef-timing | Continuous calculations from qualified observations. Live does not calculate predictions. |
| Results / messaging / publish / waze | Official results management, alerts/two-way messages, Webflow/Astro/API publication, and user interaction respectively; outside Live ownership. |

## Bases and tables

Owned destination: **wef-live â€” `appgzosUcVqA4vZdf`**.

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
| wef-entries.entries | Retained existing sync/link target only; not read by Live. |

Runtime reads canonical wef-shows shows and local synced schedules. No Entries read or external schedule completeness replay. Existing reference syncs remain untouched; no upstream writes.

## Recurring workflow â€” implemented

1. Hidden launcher runs `run-hidden.vbs` â†’ `run.ps1` â†’ `entry.js`. Acquire the existing overlap lock, then stamp the invocation heartbeat Running.
2. Read wef-shows. Require exactly one default for customer 15, valid show key/date/range and authority checked within 30 minutes. Reject missing, stale or ambiguous authority.
3. Archive and label retained observations outside the canonical show/date historical. Preserve IDs, raw evidence and native links. Previously current participation labels become historical. Repeated labeling makes no further changes.
4. Require selected date to match today in America/New_York before live acquisition. An upcoming selected day is a successful no-collection outcome, not a replacement date chosen by Live.
5. Read the explicit live-status boolean. False skips detailed collection; later scheduled checks continue. True is permission to inspect availability, not proof of live rings or entries.
6. Read the live group list for the canonical show/day. If every nonempty group is completed, skip detailed work. Empty groups are not treated as all-completed.
7. Read local synced schedules only; read ring widget state. A validated empty group/widget response succeeds, preserves current observations and allows eligible older-data retirement. Malformed responses or missing expected rings remain errors.
8. For available classes, collect live details and decoded identities. Match trip/entry/horse/rider identities; retain source evidence. The existing runner allows a bounded second snapshot read for a live identity mismatch. Persistent class errors are logged while other classes continue.
9. Archive prior state, normalize and upsert Live's observations and links. Fresh observations receive scope-status current. This scope label alone does not prove fresh, resolved participation or timing readiness.
10. After a successful outcome, including empty/skipped activity, archive and retire eligible older observations and operational collections. Partial/unresolved/failed runs do not authorize retirement. Verify all archives/catalogs before deletion; protect current scope and future dates. Keep archive catalogs in collections.
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

## Prior participation qualification rules - superseded by v1.6; source identity rules remain

- Keep source show, group, class, entry, trip, horse and rider IDs distinct. Do not infer missing IDs from names.
- Schedule links require exact active show/date/class/group matches; dropped rows are excluded. Classes may link to multiple groups without assigning every group to every participation.
- Participation key is `show|date|entry|class`. Match the full identity, active status and a successful observation within 30 minutes.
- Consume the producer's accepted schedule association and original-source evidence using the contract above; validate referenced local keys without replaying upstream completeness/parity checks. Do not invent group eligibility.
- Valid participation links are current; failed validation preserves prior links as historical. Unlinked failures are unresolved. Live trips without an available validated entry reference are unmatched and may still be collected.
- Retained old observations are not evidence of current activity. Consumers must use canonical show/date, observation freshness and participation qualification together.

## Errors, records and archives

Preserve proven keys, helpers, native links and source evidence. Log each class/trip/group issue separately, without guessed identifiers. A class error should not stop unrelated classes. Invalid authority, mandatory archive failure or other collection-level integrity failures may fail that run; never claim empty success from those failures.

Existing local receipts and hashed archives live in `runtime/`; `runtime/collector.env` is private and must never be copied into documents. Local archives are the existing implementation and evidence, not authorization for a new durable local storage design. Any migration to the broader Airtable business-storage contract is separate scope. Archive-before-delete remains mandatory.

## Daily readiness gate â€” proposed, not implemented in Live

Live should consume verified daily totals/readiness published by Shows alongside its existing live-status check. It must not recreate Data counting or Show discovery.

Latest owner-chat reports on 29 October: Data has begun producing daily summaries with normal-run verification; Shows reports the show-days schema created and publication work in progress. These reports do not prove completed publication or Live integration. Exact consumer fields and accepted publication evidence must be confirmed before implementation.

Prerequisites:

- Exact customer/show/date, count policy, coverage, accepted complete evidence, source timestamp and valid revision.
- Only an explicit verified zero may suppress the acquisition covered by that evidence. Missing/stale/partial/error counts remain unknown.
- Filtered downstream counts, zero trips or no competition classes cannot suppress all Live activity. Schooling/classless coverage remains unproved; uncovered ring/group activity must not be suppressed.
- Planning policy reported by the local coordinator: scratches count; verified drops are excluded but retained historically; undated evidence is show-only; a zero lease is five minutes from accepted source time. Confirm the published contract rather than hard-coding this planning statement prematurely.
- Continue lightweight authority/readiness rechecks. Revised counts, added entries, corrected evidence, expiration or default/day changes must release suppression under the approved policy. No permanent day shutdown.
- Preserve heartbeat/run-log evidence of each skip, including scope/revision/reason. Skips do not authorize deletion or establish fresh observations.
- Separate approval, offline coverage and normal Data â†’ Shows â†’ Live skip/resumption verification are required before claiming this gate operational. Three-minute cadence remains unchanged.

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
