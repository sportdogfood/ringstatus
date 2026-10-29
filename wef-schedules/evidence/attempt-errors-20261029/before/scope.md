# wef-schedules scope

Version: 1.0  
Updated: 29 October 2026, America/New_York  
Owner: wef-schedules chat; cross-module coordination: wef-records-coordinator.

## Purpose

Keep the current show's daily schedules updated: rings, groups, competition classes, schooling sessions and breaks. Supply stable schedule references and dated collection health to Entries, CWF, Live and later approved consumers.

This is a responsibility contract, not authorization to change code, Airtable, tasks or cadence. Dated acceptance below is not a live health guarantee.

## Responsibilities of this chat

- Inspect and maintain the existing independent schedules collector. Make only explicitly approved repeatable code changes, preserving its proven implementation.
- Collect every date in the canonical show's range, including upcoming dates. Keep source-selected date distinct from the computer's date and Live's TODAY scope.
- Normalize source rings, groups, classes and explicit schooling/break blocks, including source identities, labels, sequence, times and reported trip counts.
- Preserve stable schedule keys, record identities, show/ring links, dropped history, unrelated owner fields, static helpers and supported manual time overrides.
- Maintain dated collection status, run/archive logs, heartbeat, visible incomplete exits and archive-before-delete protection.
- Test approved changes offline, obtain required review, deploy through the existing local route and verify the exact release gate through normal scheduled runs.
- Give the coordinator and consumers exact read contracts, evidence, failure history, unresolved limits and ownership handoffs. Message other chats only when the owner authorizes it.
- Perform the agreed routine maintenance/code review. Monday review is the standing agreement; inspect its actual automation/review record before claiming it ran or is scheduled. Review-record updates need the specific authorization; review does not authorize repairs.

**Codex maintains the collector; the scheduled task runs the business process.** No manual collector runs, direct-source substitutes, record repairs or retry-until-state-changes as workflow proof.

## Ownership and dependencies

| Component | Owns / supplies | Schedules responsibility |
| --- | --- | --- |
| wef-shows | Sole canonical show discovery, customer/show/date/default authority and canonical show-days publication. Base appCaSmL819KK7UFJ. | Read validated authority. Never rediscover the default, trigger Shows or treat the local schedules show mirror as another authority. |
| Local wef-season-coordinator: Data function | Acquisition/retention, daily count computation, completeness and accepted revision evidence in wef-data. | Existing schedules acquisition remains independent. No migration to Data inputs or duplication of canonical daily unique-entry counts is implemented here. |
| Local wef-season-coordinator: Season function | Cross-show stewardship, identity/history reconciliation and conflict handling. | Use only later accepted interfaces. No shared-entity migration or new runtime prerequisite is implied. Data and Season are distinct functions under one local owner. |
| Wellington / existing decoded Worker | Dated schedule source and existing supported enrichment. Default Worker: https://wef-schedules-decoded.gombcg.workers.dev. | Preserve established transport, authentication and callers. Proxy changes need separate scope; no bypassing the runner or inventing missing IDs. |
| wef-entries | Tenant/follow-selected participation and entry/horse/rider/trainer/trip relationships. | Supply schedules and dated health. Entries owns participation eligibility, multiple associations and destination linking. |
| wef-tenant-cwf | Tenant helpers, team/private selections and local synced references. | Preserve exported identities/links; do not manage its roster, trainer slots, private follows, roles or collector. |
| wef-live | Independent TODAY live observations and trip identities. | Supply schedule references; do not own live facts, TODAY guards or a Live polling gate. |
| wef-timing | Continuous calculations and answer freshness. | Supply schedule facts/health under an accepted contract; do not calculate predictions. |
| Results / Messages / Publish / Waze | Their own results, alerts/two-way communication, publication and interaction responsibilities. | No output, notification, publishing, check-in or walking-route implementation under schedules scope. |
| wef-records-coordinator | Ownership, interfaces, cross-owner release and rollover acceptance. | Supply exact evidence; do not infer other owners' acceptance from schedules' tests. |
| PC / Windows / Airtable | Awake host, network, credentials, local archive storage, registered task and base access. | Preserve the established deployment. A hidden window does not establish desktop isolation or operation while the PC is asleep. |

The former cloud Data chat and legacy implementation are references, not operational authorities or instructions to recreate older workflows.

## Airtable boundary

Schedules base: **appUq5Qz9kARqFija**.

| Table | Purpose / write boundary |
| --- | --- |
| schedules | Current dated projection, explicit non-class blocks and retained dropped rows. Update source-owned columns and existing local show/ring links; preserve unrelated owner fields. |
| show-collection | Latest result per show/date: completion, dataset, counts, changes, warnings and source context. Operational date health, not canonical entry-count readiness. |
| shows | Local linked show mirror, selection and schedule archive index. Preserve local record IDs. Canonical authority remains in wef-shows. |
| rings | Dated ring identities, source-selected flags, local show/helper links. |
| rings-helper | Facility-scoped lookup by ring ID, number or name. Collector fills supported missing identity fields only. Nicknames, walking estimates, coordinates and notes remain owner-maintained. Placeholder values are not measurements. |
| run-logs | Acquired invocation's result, per-date counts/errors and every archive/cleanup outcome. Finished logs are archived before bounded slots are reused. |
| heartbeat | Latest verified terminal run status/time/ID and context/counts. Does not schedule or trigger work. |
| code-reviews | Findings, evidence, resolved/unresolved history and next-review tracking under specific maintenance authorization. |
| tenants / wef-tenant-index | Retained tenant context/directory. Their presence does not transfer tenant membership, privacy or permissions to schedules. |

Retired meta notes remain preserved; do not recreate or remove unrelated metadata.

Canonical **wef-shows.show-days**, table tbl14MaCCDw6dAywH, is written by Shows using Data-owned retained evidence. Schedules does not write it and has no implemented suppression reader for it.

## Identity and consumer contract

- Local show-key: `customer_id|show_id`. Canonical customer/show identity comes from Shows.
- Preserve schedules.stage-key: `show_id|YYYY-MM-DD|existing source schedule key`. Existing class key builders remain unchanged.
- Supported explicit break groups with empty class lists use `block:<class_group_id>`. Class IDs/numbers stay blank; never fabricate competition classes.
- Dated ring-key: `customer_id|show_id|YYYY-MM-DD|ring_number`. Ring number, ring ID and display name are different identifiers.
- Match participation using show ID, date and source class ID, retaining exact schedule keys and valid group context. Repeated class/group appearances alone do not prove entry eligibility in every group.
- Native links resolve to each consumer's own local synced records. Foreign Airtable record IDs are not portable links.
- schedules.status active/dropped is retention state, not live class status or participation eligibility.
- Freshness comes from relevant dated collection and terminal health evidence. An unchanged changed-at is not proof that a successfully reused dataset is stale. Source observation time and completion time differ.
- total-trips/completed-trips are source-reported values, not daily unique entries, horses or attendance. Missing/null is not zero.

The existing native show link is still named show-details although its table is now shows. Preserve working field/table IDs rather than renaming by assumption.

## Normal recurring workflow

1. Windows starts **wef-schedules-collector** on its existing five-minute repetition. Configuration read on 29 October: hidden PowerShell calls run.ps1. No separate day/night cadence is implemented by this contract.
2. run.ps1 loads the private environment and invokes collect.js. The completion guard defaults to failure until terminal reporting finishes; the lock protects overlap. Contention is not permission to run another lane.
3. Begin the invocation's run-log. Read exactly one customer-15 canonical default from Shows; validate identity, dates, metadata and authority freshness. The existing 30-minute show-authority check is not the five-minute entry-zero policy.
4. Read/decompress the producer's referenced local default payload, verify SHA256 and scope. Missing/stale/conflicting authority or an invalid archive fails explicitly, without independent discovery fallback.
5. Collect all show dates, prioritizing the source-selected day and next day. Reuse the verified canonical default response where the existing path does so; acquire dated schedules through configured adapters.
6. Normalize classes and supported non-class blocks. Apply supported ID/time enrichment and manual guards. Block-only days do not launch class HTML time enrichment. Preserve source times; do not infer end times from labels.
7. Reconcile each date with its last verified dataset. Archive/read back datasets, preserve dropped history, reject duplicate/ambiguous keys and destructive failed/zero replacements, update current records in place, and write/read back show-collection.
8. After all new-show dates succeed, existing finalize archives/verifies and retires old-show schedules/show-collection. This precedes final reference sync and terminal reporting; downstream acknowledgment is not currently a retirement gate.
9. Validate complete dated ring coverage; update the local show mirror, rings and helper identities; verify schedule show/ring links and preserve static owner fields.
10. Save local state, finish/read back run-logs, update the archive catalog and heartbeat through established finalization, and release the lock. Compare process exit, terminal logs, heartbeat and archive proof; one timestamp alone is insufficient.

These are data dependencies, not execution triggers. All collectors remain independently recurring.

## Empty, failed and zero-entry observations

A successful response with no classes, schooling or breaks is an empty schedule observation, subject to destructive-empty protection for established rows. A schooling-only day is not empty. Logs distinguish class and block counts.

Invalid/partial source, wrong scope, archive/hash failure, write/readback errors and missing final reporting remain explicit failure/incomplete outcomes. Do not convert them to empty success or remove established data based on failure. An interrupted Running row remains evidence; do not repair it to pass acceptance.

After an approved scheduled path fails, stop that execution/verification path and report evidence. Continue unrelated authorized work. No manual substitute, retry/repair or code patch after failed verification without the required new owner authorization.

### Canonical verified-zero readiness

Data computes accepted dated counts; Shows publishes scope, coverage, policy, source time, revision and validity. Only an explicit accepted complete fresh zero covering the exact acquisition can support a later approved skip. Tenant emptiness, zero trips, no competition classes, stale/partial/error data or unknown schooling/classless coverage never establish whole-day zero.

Approved policy includes scratches, excludes verified drops with history, keeps undated entries at show scope and expires zero validity five minutes from source observation. New revisions, corrections, uncovered activity or expiry must resume eligible acquisition under an approved reader policy. No permanent day shutdown.

No Live gate, Entries input switch or schedules polling reduction was implemented by the Data/Shows publication release. **Schedules continues checking for schedule changes on zero-entry days.**

October 29's 24 schooling sessions and two breaks explicitly reported total_trips=0 in all 26 objects. That establishes reported block trips, not complete entry absence or schooling attendance. The source header total_trips_count=26 is excluded from attendance/entry totals.

## Archive and rollover duties

Existing runtime history contains verified datasets, attempts, removed/retired records, archived logs and completed-day state. Verify content/hashes before cleanup. A code rollback is not a data rollback.

Do not delete local archives or introduce another store because Data/Season retention exists. Portable/Airtable-only handoff and retention migration need separate approval/proof.

Natural combined rollover remains open. At a genuine default change, correlate old/new show keys, dated health, archived prior state, consumer runs/syncs, local native links, retained helper/private relationships and retirement effects. Same-show repeats, fixtures or discovery alone do not prove cross-consumer rollover. Never fabricate a transition.

## Maintenance and release workflow

1. Establish authorization, applicable AGENTS.md, exact source/deployed baseline, existing contracts and protected changes before planning the smallest delta.
2. Diagnose using saved normal-run evidence and read-only state. Use isolated offline fixtures; never execute production entrypoints as tests.
3. Implement approved repeatable changes; test meaningful failures, repeat behavior, stable keys/links, owner/manual guards and archive order. Obtain independent review where required.
4. Deploy only reviewed files through the existing local route, preserving private runtime, dependencies and tasks. Keep the maintenance source aligned; no wholesale replacement, new scheduler or inferred host migration.
5. Verify the required two normal scheduled runs and relevant downstream effects. Match exact receipts/datasets/revisions/readbacks. Offline tests do not replace operational acceptance.
6. Record versions, artifacts, actual outcomes, failure history, limits and handoffs. Delivered messages are not proof of another owner's implementation or acceptance.

## Implementation locations

- Maintenance source: C:/Users/gombc/.codex/worktrees/wef-schedule-collector/ringstatus/wef-schedules.
- Deployed package: C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-schedules.
- run.ps1 / collect.js / completion-guard.js: launch, entry and completion protection.
- show-source.js: canonical Shows reader and producer payload verification.
- collector-core.js / normalizer.js / schedule-times.js: dated acquisition, normalization and time enrichment.
- store.js / current-store.js: reconciliation, verified datasets, current projection, cleanup and rollover order.
- show-rings.js / show-index.js: local references, helper identities and archive catalog.
- run-logs.js / heartbeat.js: invocation/archive reporting and latest terminal health.
- settings.js / runtime/collector.env: fixed destinations and namespaced private settings. Never expose secrets in documents or logs.
- source-manifest.json / tests/ / evidence/: package versions, offline tests and dated proof.

collect.js still contains an old discoverFocus helper. The deployed staged entry uses show-source.readShowFocus; helper presence is not authorization to use independent discovery or legacy entrypoints.

## Dated acceptance and remaining limits

| Item | Status documented 29 October 2026 |
| --- | --- |
| Classes, schooling and breaks | 139 offline tests and normal 08:30/08:35 runs passed; 471 prior class IDs/links and 504 total schedule IDs preserved on repeat. These are dated counts, not fixed inventory targets. |
| Data-to-Shows publication | Separate owners deployed four canonical day rows; normal 10:04/10:09 Shows runs consumed distinct accepted Data revisions. Stale/Unknown and competition-only coverage remain explicit. Schedules inputs/code were not switched. |
| Natural rollover | Combined consumer/native-link/retirement proof remains open. |
| Portable payload handoff | Same-machine hash-verified archive dependency remains. |
| Multiple schedule associations | Natural participation-specific eligibility remains an Entries/Live/coordinator evidence gap; schedules supplies exact keys. |
| Desktop / host | Hidden launch and five-minute cadence confirmed; full desktop isolation is not established. Operation while the PC is asleep/unavailable is not provided. |
| CWF lifecycle | Separate unresolved Windows termination with later same-run business success. Data/helpers/links preserved; two clean task/business pairs unaccepted. Temporary task-history diagnostic logging awaits separate authorization. No CWF fix/task change belongs to this document. |
| Shared entities and outputs | Separate scope/owner acceptance; no global helper/entity authority or output integration is activated here. |

## Evidence and related contracts

Paths below are relative to the deployed repository root:

- wef-schedules/README.md and source-manifest.json: implementation and release history.
- wef-schedules/evidence/schedule-blocks-20261029/release.md: class/block release and scheduled proof.
- wef-schedules/evidence/schedule-blocks-20261029/today-trip-count-evidence.json: exact 26 source values and limits.
- docs/coordination/wef-records-workflow-ownership-20261026.md: current local owners and amendments.
- docs/coordination/wef-data-boundaries-working.md: current/historical working contracts.
- docs/coordination/wef-rollover-acceptance-assignments-20261024.md: natural rollover acceptance.
- docs/coordination/wef-show-days-plan-20261029.md: canonical counts/readiness contract.
- docs/coordination/evidence/show-days-20261029/acceptance.md: Data/Shows proof and open CWF lifecycle gate.

This scope does not certify unbuilt downstream connections or current operational health.
