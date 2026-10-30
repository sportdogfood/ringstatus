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

# wef-live workflow handoff

Version: 1.6
Date: 29 October 2026 (America/New_York)

## Existing reverse reference - prior implementation evidence — 29 October 2026

Approved simplification: the existing read-only Entries reference now syncs schedule-link-status, schedule-link-basis, group-link-status, link-issues and source-json. Automatic sync and editing restrictions are unchanged. Source reference view exposes only the existing twelve plus these five fields; no records were repaired or collectors manually invoked.

Live consumes the producer's accepted mapping rather than rereading source schedules/show-collection or comparing the entire source/synced dataset. It retains exact scoped identity, uniqueness, active/successful status, local schedule-key availability and prior-link preservation. Both original sourceAsOf and last-seen must agree and be within 30 minutes, retaining the existing one-minute future-clock tolerance. Publication/heartbeat times cannot renew freshness. Missing evidence or any newer applicable unfinished/failed attempt prevents current qualification. Ordinary class-date mapping requires one schedule; observed-group mapping additionally requires linked group status, matching group ID and same-revision evidence. Live does not infer group eligibility from class-date association.

Live ring/group/class schedule links, collection, archive protection and all three logging tables are unchanged. Missing/stale/unresolved references preserve prior links as historical and record issues while observations continue. The checks described in older release sections below are historical where superseded by this consumer contract.

Verification: 54 tests passed. Normal 13:50 and 13:53 Eastern runs completed Success with matching heartbeat/run-logs, no errors, verified archives and all 397 records/links unchanged. Both had empty live activity; fresh populated qualification remains offline-tested rather than naturally demonstrated. See evidence/entries-contract-20261029/release.md.

## Tenant-code removal — 29 October 2026

Approved Live-only implementation: runtime comparison, uniqueness and overlap-lock helpers now live in `runtime-utils.js`; verified archive creation now lives in `archive.js`. Their behavior is preserved from the deployed baseline, with tenant lock-error labels renamed for Live. Runtime and verification imports no longer load tenant modules. The unmatched-participation explanation now says “No available validated entry reference.” Existing identity matching, links, archive protection, cadence and upstream collectors are unchanged.

All 49 existing tests and three focused dependency/utility tests passed. Two normal scheduled runs at 13:11 and 13:14 Eastern completed successfully; heartbeat/run-logs agreed, errors were absent, verified archives were readable, and all 397 business records and links were unchanged. Both were no-live-activity outcomes; populated collection behavior is covered offline, not newly demonstrated by these runs. See `evidence/tenant-decoupling-20261029.md` for exact acceptance. Earlier documentation-only statements below describe the prior correction, not this separately approved implementation.

## Workflow correction â€” CWF removed

CWF is removed from all workflow responsibilities, collection parameters, dependencies, linking requirements and acceptance checks. Shows supplies authority; schedules and Data collect their owned sources; the local Season coordinator owns Data collection/retention and Season stewardship. Entries is linking-only, reading accepted participation/schedule evidence and Live observations without source acquisition. Season is not a current collection gate. Live independently collects current observations. Tenant filtering is downstream and does not control collection. No tenant-specific feed or consuming run is a prerequisite for core acceptance. Historical evidence remains intact in its original files but is not a current workflow requirement.

This update changes documentation only. It does not claim that deployed code has been changed or audited for remaining references. Code, Airtable, existing records and scheduled tasks are unchanged.

The minimal proposed Entries schedule association uses exact show/class/date only when exactly one active schedule matches. Ambiguous matches remain unresolved; do not fan out to groups. Verified group eligibility is separate and retains its own source-evidence requirements. This proposal does not change the existing Live reader or authorize its migration.

## Proposed canonical readiness gate - 29 October 2026

**Planning only; not implemented or operationally verified.** wef-shows remains the sole canonical show authority and owns new/default show discovery. The proposed contract has Shows publish dated counts/readiness derived from Season-owned Data verified evidence. Live consumes that publication; it must not independently rediscover shows. Publication schema, count policy, coverage, freshness/revision rules and consumer gating still need agreement and proof. This section authorizes no code, schema, record, task or source changes.

Current Live behavior remains unchanged: three-minute canonical authority checks, TODAY guard, live-status check, completed-group gate and successful empty-response handling. There is no implemented Shows dated readiness gate and no producer-revision-driven suppression/resumption policy.

### Required before Live may skip covered source polling

1. Shows must expose explicit accepted **complete dated evidence with count zero**, matching customer/show/date, with an approved count policy, declared coverage and a fresh valid evidence revision. These are semantic requirements, not invented Airtable field names. Absence of a record or value is never zero.
2. Coverage must support the exact Live acquisition being suppressed. Live covers all TODAY activity, independently of downstream tenant slices. Zero filtered entries, zero trips or no competition classes cannot suppress the whole live workflow. Schooling/classless attendance or live activity of unknown coverage must remain unknown. If the evidence covers competition entries only, it cannot authorize skipping uncovered rings/groups or schooling activity.
3. Missing, stale, partial, error, ambiguous or mismatched evidence must never become a zero-day skip. Fall back to existing approved collection behavior and its existing guards; do not declare empty success from invalid readiness.
4. Separate lightweight canonical context/readiness rechecks from costly source polling. Agree the producer refresh and consumer recheck policies, revision invalidation and correction handling. Later entries, corrected counts, changed default/date, expired evidence or withdrawn readiness must release the readiness suppression at the approved next check; existing collection guards still apply. No permanent day shutdown. No cadence change is authorized.
5. A readiness skip must retain heartbeat/run-log visibility and record the reason, evidence scope/revision and covered acquisition skipped, while preserving observations and links. A skip is not permission for cleanup or proof of fresh live observations. Historical labeling and verified archive-before-delete rules remain independent.
6. After separate implementation approval, test accepted zero, positive/revised counts, missing/stale/partial/error evidence, wrong scope, ambiguous authority and incomplete schooling/classless coverage. Then verify normal scheduled skip and resumption behavior, record/link preservation and repeat behavior. No seeded production records, forced revisions, manual runs or record repairs to manufacture proof.

Ownership/dependencies: Shows publishes canonical authority/readiness; Season/Data supplies verified complete evidence and coverage; the coordinator settles the cross-owner count/revision/recheck contract; Live implements only its consumer gate after approval. No existing reader switches or collector replacements are authorized. Until these requirements are satisfied, readiness-based suppression remains unavailable.

Keep this proposed gate separate from same-date old-show deletion (implemented, natural deletion proof pending), populated new-show link/sync-lag acceptance, historical class 200030429 identity issue, natural multi-group proof and the unchanged timing-input boundaries. Documentation updated from the agreed planning exchange, not a fresh operational check.

## Rollover update - 29 October 2026

Old-show/date observations now receive scope-status historical after canonical authority validation, including empty/upcoming runs. Prior current participation labels become historical; native links and observation evidence are retained. Same-date other-show retirement selection and deletion guard are implemented with existing verified archive protection and populated clean-run gates. This supersedes the implementation-gap statement below, but natural same-date deletion acceptance remains open. Normal 08:26 and 08:29 Eastern runs verified historical labeling of 397 rows, preserved IDs/data/links and repeat no-op; 49 offline tests passed. See [rollover evidence](evidence/rollover-labeling-20261029.md). Populated new-show links, sync-lag behavior, natural same-date deletion and all-owner rollover acceptance are not claimed.

Confirmed empty live responses now complete successfully without business cleanup; see [empty-response evidence](evidence/empty-response-20261029.md). Subsequent rollover authorization adds historical labeling to those runs. Three-minute cadence remains unchanged.

### Coordinator acceptance handoff

- Normal run `d0db13e4-9987-4935-b080-6825a909c778`, 08:26:14â€“08:27:16 Eastern: archived and labeled 5 rings, 31 groups, 56 classes and 305 entries historical. Two formerly current participation labels became historical. Heartbeat and run-logs Success; no collection/logging errors or errs-log rows.
- Repeat `bb35e0f3-92f0-4151-8c70-f8773814ffe7`, 08:29:13â€“08:29:33 Eastern: Success, no repeat labeling archives, unchanged record IDs/source data/native links. No business deletions in either run.
- **Same-date old-show deletion still needs natural proof.** The code and offline deletion/archive guards are tested, but no qualifying same-date old-show records and populated new-show run were available. At a natural qualifying run, verify the exact old-show rows, archive hash/content, deletion outcome and retained current-show records/links. Do not seed records, trigger a run or repair records to obtain proof.
- Remaining gates are separate: populated new-show links; sync-lag handling; cross-owner rollover acceptance; historical class 200030429 identity issue; natural participation-specific multi-group proof. This handoff does not authorize fixes or change timing-input boundaries.

This coordinator update uses the saved verification above; it is documentation and communication only, with no new code/Airtable changes or collector runs.

## Operational update - 25 October 2026

Owner changed wef-live-collector to every three minutes (PT3M); hidden launcher retained. Owner also approved one heartbeat row per invocation: new row keyed by run ID, stamped started/checked-at with Running before source work, then the same row receives completed/checked-at and terminal result. Existing legacy heartbeat row is preserved. This supersedes the older five-minute cadence and latest-row-only heartbeat description below. Startup/final heartbeat failures remain isolated from collection. 43 offline tests passed; normal-run evidence is in evidence/heartbeat-per-invocation-20261025.md.

errs-log also now has err-class, err-trip and err-group. Each error gets a separate record/JSONL line; known IDs only, ambiguous groups blank. Prior errors were not repaired. See evidence/error-identifiers-20261025.md.

Prepared 24 October 2026. Operational evidence verified through the 13:12 ET scheduled run, completed 13:13:05 ET. This is a handoff document, not authorization for further changes.

Updated 24 October 2026 with historical upstream acceptance evidence and the agreed live-to-timing planning boundaries. The tenant-consumer acceptance requirement is superseded by the 29 October correction above. This documentation update did not refresh live operational status or run any collectors.

Further updated 24 October 2026 with the saved timing-payload investigation and assigned natural-rollover acceptance gaps. Findings below are retained-evidence findings, not a fresh operational check or implementation approval.

## Status and responsibility

The independent TODAY-only live collector is deployed. The participation-link contract passed 40 offline tests, independent review, and two normal scheduled runs. Existing native links were preserved. No manual collector runs or record repairs were used.

Own only wef-live: live observations, local reference links, archives, collections and operational logs. Shows supplies canonical show/date authority; schedules supplies schedule identity and dated collection health; entries links accepted Data/Season participation identities to schedules and local records; it does not independently acquire source entries. Tenant consumers are downstream and do not control collection. Do not edit those collectors or the shared decoding/source modules as part of live work.

Timing is a downstream consumer, not implemented here. It must distinguish current validated observations from retained historical data. Live does not infer timing predictions, official result readiness or participation group eligibility.

## Where it runs

- Checkout and deployed task directory: `C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus\wef-live`.
- Windows task: `wef-live-collector`, every five minutes, hidden WScript launcher -> `run-hidden.vbs` -> `run.ps1` -> `entry.js`. It is not a Codex heartbeat.
- Task inspection at handoff: Ready, last trigger 13:12:12 ET, last result 0, next trigger 13:17:11 ET. This is a dated observation, not a permanent status claim.
- Source checkout HEAD: `28ddf2815d487dbb28144dde34452fdcb38a72d3`. Deployed working files include changes beyond HEAD; use [deployment file hashes](evidence/participation-deployment.json), not HEAD alone, to identify this release. No commit was made for this work.
- Private environment file: `runtime/collector.env`. Never print or copy its credentials into a handoff.

**Documentation warning:** `README.md` contains superseded task-disabled, halt-file and original test-count statements. Current `entry.js` retains older halt receipts as history but does not use them to stop future scheduled execution. Use this handoff, current code and dated evidence below for present behavior.

## Airtable and ownership

| Base | ID | Use by live |
|---|---|---|
| wef-live | appgzosUcVqA4vZdf | Own destination |
| wef-shows | appCaSmL819KK7UFJ | Read canonical shows |
| wef-schedules | appUq5Qz9kARqFija | Read schedules and show-collection health |
| wef-entries | appSxF4pYMBaxpTZd | Receive restricted native entry sync |

[Open wef-live](https://airtable.com/appgzosUcVqA4vZdf).

Owned tables: `live-rings`, `live-groups`, `live-classes`, `live-entries`, `collections`, `run-logs`, `heartbeat`, `errs-log`. Exact field definitions/native links are in `schema.js`; table and field IDs are in `evidence/schema-configured.json`.

Read-only native reference tables:

- `wef-schedules.schedules` (`tblGPXIUnD0sfTEIX`). Groups/classes link to local synced schedule records.
- `wef-entries.entries` (`tblfj77GmXxuiAoB4`). Live entries link through the native `entry` field to these local synced records.
- Entry source view: `wef-live-reference` (`viwRwdJi7CR0zBMEL`) in source entries table `tblqkWmrsYMs2hyQ6`.
- Explicit safe entry fields: entry-key, show-id, date, entry-id, class-id, horse-id, rider-id, status, schedule-key, schedule-keys, last-seen, observation-status. Source editing remains Off; sync is automatic. No private follows or collection provenance are exported.

```mermaid
flowchart LR
  Shows[wef-shows: canonical show/date] --> Live[Independent wef-live collector]
  Source[Wellington live observations + decoded identities] --> Live
  Schedules[wef-schedules: schedules + dated health] --> Live
  Entries[wef-entries: safe participation reference] --> Live
  Live --> Rings[live-rings]
  Rings --> Groups[live-groups]
  Groups --> Classes[live-classes]
  Classes --> Trips[live-entries]
  Trips --> Reference[Local synced participation via entry link]
  Live --> Logs[collections / run-logs / heartbeat / errs-log]
  Live -. downstream observations .-> Timing[wef-timing]
```

## Collection and continuation

Read the canonical default show and selected date from shows; require fresh authority and a match to the current New York date before TODAY collection. An upcoming selected date is not live activity today. Do not replace this with independent show discovery.

`source.js` defines public ListAjax, LiveScoreWidget and getLiveClassData sources. Exact class/trip identities use `wef-schedules-decoded.gombcg.workers.dev/classes/...` through the existing shared `lib/wef-source-read` module. These are runner inputs, not permission for manual source calls.

Five-minute checks continue. If live status is false or all nonempty TODAY groups are completed, detailed collection is skipped and the skip is logged. Empty collections are valid outcomes, not failures by themselves.

Class/source identity errors retain affected observations and links, log the error and allow other classes to continue. An unresolved class is not current validated data merely because the overall run completed. Logging failures are isolated from collection, with local evidence where writable. Mandatory archive/data-integrity failures may still fail a run; subsequent scheduled invocation is not permanently halted by that receipt.

Prior-state/source archives are compressed, hashed and verified before relevant updates/retirement. Unresolved class/participation errors prevent retirement. Do not bypass archive-before-delete protection.

## Participation-link contract

- Stable participation identity: `show|date|entry|class`; preserve existing schedule stage-keys.
- Exact show/date/entry/class/horse/rider IDs must agree. No name-based matching or historical identity substitution.
- Require active participation, `observation-status=successful`, and source observation `last-seen` within 30 minutes.
- `schedule-keys` is sorted, unique JSON. Singleton scalar `schedule-key` must agree; multiple keys require blank scalar. Scalar fallback applies only when the plural field is absent, not when it is present but empty/malformed.
- Require complete fresh dated `show-collection` health, using completion time rather than schedule changed-at. Compare full active source/local stage-key sets for the date; listed keys must each resolve uniquely to active matching schedules. Dropped rows are historical, not eligible.
- Consume participation schedules through the existing synced entry reference. No redundant live-entry schedules link. Do not infer that every class group is eligible for every trip.
- `participation-link-status`: current, historical, unresolved or unmatched. A failed check preserves a previous native link as historical; a rejected candidate without a prior link is unresolved; observations without an available validated entry reference are unmatched.
- Audit fields: `participation-link-reason`, `participation-checked-at`, `participation-verified-at`. A failed check does not erase the prior verified timestamp. Consumers must require current status and appropriate freshness, not timestamp presence alone.

Implementation: `participation.js`, integrated by `collector.js`; error delivery is in `entry.js`. Destination write restrictions/read targets are in `airtable.js`. Metadata setup is `configure.js`; do not rerun setup merely to inspect status.

## Verified scheduled evidence

| Run (24 October, ET) | Run ID | Participation result |
|---|---|---|
| 13:07:13-13:08:03 | d66f6c2e-6dd4-4ea8-b93d-ffb5699e840e | Both TODAY links current; six existing links preserved |
| 13:12:13-13:13:05 | 5257eecc-cbfe-4cb6-93bc-e37e3856b472 | Same result; verification timestamps refreshed |

Each run collected 5 rings, 26 groups, 59 classes and 287 live entries. Both had `ok=true`, no participation errors and no logging errors. Airtable run-logs/heartbeat recorded **Partial**, because the separate class identity error below remained. Do not describe overall collection as error-free.

- [Run 1 evidence](evidence/participation-run-1.json)
- [Run 2 evidence](evidence/participation-run-2.json)
- [Release explanation](evidence/participation-link-release.md)
- [Transition evidence](evidence/participation-transition.json): before the upstream fields populated, links became historical, errors reached errs-log and collection continued.
- [Saved automatic/read-only sync settings](evidence/participation-sync-settings.png)
- Prior continuation baseline: `evidence/logging-isolation-release.md`, `logging-isolation-run-1.json`, `logging-isolation-run-2.json`.

## Historical upstream evidence â€” not a tenant dependency

The earlier upstream release evidence is retained at [release](../wef-entries/evidence/multiple-schedules-20261024/release.md), [pair 1](../wef-entries/evidence/multiple-schedules-20261024/run-pair-1.json), [pair 2](../wef-entries/evidence/multiple-schedules-20261024/run-pair-2.json), and [repeat comparison](../wef-entries/evidence/multiple-schedules-20261024/repeat-comparison.json). These are historical records only. Their tenant-consumer checks do not define current collection scope, dependencies or acceptance requirements.

That evidence demonstrated plural-field transport with naturally available singleton records. It does not prove the new independent Entries matcher or participation-specific multi-group eligibility. Current acceptance must use independent producer evidence and normal scheduled behavior, without a tenant-specific consuming run.

## Agreed timing-input boundaries - accepted for plan

These boundaries describe existing supply and proposed normalization, not authorization to add fields or implement timing. [model.js](model.js) and [schema.js](schema.js) define current mappings.

| Classification | Supply and limits |
|---|---|
| Current ring fields | `current-group`, `next-group`, `completed-groups`: source-reported group links, not inferred actual start/end times. |
| Current group fields | `status`, `is-live`, `has-json`, `estimated-start-time`, `total`, `gone`, `jo-total`, `jo-gone`, `r2-total`, `r2-gone`. These preserve source status/counts and estimates. |
| Current class fields | `total` is live row count; `gone`, `jo-gone`, `r2-gone` count IDs in the respective source lists; `current-trip-id` is source-reported. Separate class JO/R2 totals are not currently exposed. Do not treat counts from different phases as interchangeable. |
| Current trip fields | Exact source identities and native links; `order-of-go`, `actual-order`, `scratch`, `gone`, `position`. Position maps `ActualPos`, falling back to `Pos`; this is not verified official placing. |
| Raw-only values | `source-json.identity.estimated_go_time`; raw live phase start/done values such as `r1_start`, `r1_done`, `jo_start`, `jo_done`; source score/time/place values. Retention does not validate their business semantics. |
| Proposed normalization | Expose `estimated_go_time` explicitly as a source estimate with raw value and provenance preserved. It is not a collector prediction or actual start. No normalized field name or schema change is approved here. |
| Unknown / unverified | Authoritative current source clock; actual group/class start and end anchors; complete phase timestamp/count semantics, including blanks/zeros; verified official-results readiness. |

Retained [class 200030399 payload](runtime/history/class-200030399/551d561fb987eee4c4f88673638121b9d59e8a056aa7423a6a9e474d2e7f2cb7.json.gz), referenced by the 13:07 live run, actually contains `estimated_go_time` values such as `09:03:00`, GMT-labelled `jo_start` values, blank phase start/done fields, and `curr_updated_at = "24oct2026 13:27:24"` without an explicit timezone. GMT-labelled values can be parsed, but their phase meaning, applicability and blank/zero semantics remain unvalidated. This archive does not establish a reliable current source clock, actual group/class anchors or official-results readiness. Do not convert time-only values into absolute timestamps by guessing a date or timezone.

Keep the three clocks distinct:

- Live `last-seen`: collector observation time. A recent observation is not proof the upstream content changed recently.
- `source-updated-at`: retained upstream update label, potentially old or lacking timezone semantics; not a current clock.
- Timing evaluation clock: owned by timing. Show `selected_date` selects scope; it does not replace that clock or independently mean TODAY.

For a Partial run, timing may consume only validated, fresh observations within the relevant show/date/class scope. Use per-class collection/error evidence; do not promote an unresolved class's retained data to current merely because another class succeeded. Participation-dependent calculations must require `participation-link-status=current`, appropriate freshness and the reason/checked-at/verified-at audit fields. The two live runs above establish actual current-link proof for the two TODAY records, not universal readiness for all classes.

Live supplies observations, raw evidence and validated identity links. Timing owns phase-consistent remaining counts, effective order, entries ahead, waits, pace, projections, countdowns, drift/conflicts, freshness/unavailable reasons, provenance and condition-transition events. Results owns official score/fault/time/placing, readiness, corrections and history. `Completed`, `gone`, raw scores or source `place` alone do not establish results readiness. Messaging owns recipients, consent, templates, delivery deduplication and outcomes. These downstream responsibilities are planning boundaries, not implementation acceptance or approval on those owners' behalf.

## Saved timing findings - read-only investigation

The saved-only review inspected 913 class snapshots across 91 classes. Counts here are occurrences across archived snapshots, not unique entries, and describe that review inventory only. No source calls, collector runs or data changes were used.

| Field | Observed coverage / format | Safe use and limit |
|---|---|---|
| `identity.estimated_go_time` | 4,832 `HH:mm:ss` values; 1,967 were `00:00:00`. Estimates changed across snapshots for 270 exact scoped trip keys. No malformed time formats found in this inventory. | Conditionally usable as a nonzero source wall-clock estimate. Class payload supplies show/date context, not a timezone for this field. `00:00:00` is unavailable/ambiguous, not established midnight. No absolute countdown from this field yet. |
| `live.jo_start` | 527 nonblank GMT-labelled occurrences covering 197 class/trip identities. Parsed dates matched payload calendar dates; nonblank values were stable across retained snapshots for the same scoped trip. | Parseable source-reported timestamp candidate. Date/offset syntax does not certify an actual phase-start meaning. No inferred actual group/class anchors. |
| `live.r1_start`, `r1_done`, `jo_done` | All 4,857 live-row occurrences blank. | Unavailable actual start/end evidence. |
| `live.r1_cd`, `jo_cd` | All 4,857 live-row occurrences blank. No live-row `r2_start`/`r2_done` fields observed. | Unavailable source countdown. |
| `curr_updated_at` / `source-updated-at` | Unzoned date-time labels; some unchanged while decoded estimates changed. | Raw provenance only, not current source clock or update time for every decoded identity field. |
| `time_one/two/three`, faults, scores, placing | Numeric result-like values with many zeros; live `Score` also includes `DNS` and `EL`. | Not clock anchors. Preserve zero/status distinctions; results owns units, semantics and readiness. |

Exact-trip example: show 200000075, class 200030399, trip 200398698. In the [earlier snapshot](runtime/history/class-200030399/17857b63e36a9192d33d8a6b2181974a2a052d79884a3d337d41db935af2ed40.json.gz), Gone became 1 while jo_start remained blank. A [subsequent snapshot](runtime/history/class-200030399/7d71c1ab251f632e9a6c32d747c710bbd8366fff6f53b0771cdc82d4469394ef.json.gz) supplied `Sat, 24 Oct 2026 13:02:40 GMT` and JO-list membership. The estimate later changed from 08:48:00 to 08:45:00 and result-like durations populated in the [later retained snapshot](runtime/history/class-200030399/551d561fb987eee4c4f88673638121b9d59e8a056aa7423a6a9e474d2e7f2cb7.json.gz). Match by exact trip ID and validated enclosing class/show/date, never names or capture date alone. These are separate observations, not one synchronized event.

Phase ambiguity: 10 archived occurrences had jo_start without JO-list membership; 29 had JO-list membership with blank jo_start. JO timestamp, JO counts, Gone, and R2 qualification/upcoming flags cannot substitute for each other. Re-entry/attempt semantics remain unestablished. Twenty-nine live-row occurrences lacked matching decoded trip identity and must be excluded from identity-dependent calculations. These counts do not identify a new production incident or override existing unresolved-class handling.

Additional source evidence: decoded `class.results_verified` is actually present, with both 0 and 1 observed (800 and 113 snapshot occurrences). The [class 200030013 archive](runtime/history/class-200030013/84ba6b18e2166b31bff321fefb04903fb08c4ac2b3319692fd752ab5bd9ee52a.json.gz) contains 1. This is an explicit raw source verification flag available to the results workflow, not a normalized live readiness field or proof of current publication/correction readiness. This supplements the earlier unknown-readiness statement; Completed/Gone still do not imply results-ready.

**Smallest optional normalization recommendation:** one read-time parser owned by timing, using existing `source-json.live`, `source-json.identity`, validated identity links and existing observation/source timestamps. No new Airtable fields, duplicate clock or live collector changes are required by this recommendation. Return a parsed value or null, a quality/unavailable reason and the exact raw-field provenance; preserve raw values. Keep estimates wall-clock-only until date/timezone anchoring is agreed. Explicit-zone phase timestamps remain source-reported candidates with unverified phase meaning. Reject missing, malformed or conflicting inputs without guessing. Actual phase durations, phase-based pace/countdowns and inferred actual class/group starts/ends remain unavailable where this evidence is insufficient.

If separately approved, acceptance must cover blank/zero/malformed values, timezone/date boundaries, changing estimates, phase-list disagreements and unresolved identities, followed by two normal scheduled runs preserving raw values and links. Fixture success is not natural source-semantic proof. No manual runs, seeded records or forced source changes.

## Natural default-show rollover - acceptance still open

Authority: [rollover acceptance assignments](../docs/coordination/wef-rollover-acceptance-assignments-20261024.md). This records responsibilities and known gaps only; it authorizes no implementation. wef-records-coordinator owns cross-consumer acceptance tracking.

Live's accountable scope:

- **TODAY guard:** source-selected date must match the configured America/New_York day. That configuration is not evidence of source timezone or UTC offset; both remain unknown. Missing/stale/ambiguous authority must not be treated as valid empty activity.
- **New-show sync lag:** verify new-show schedule/participation readiness and preserve existing links while references lag. Prove current versus historical labeling on the actual transition, not merely repeated same-show runs.
- **Archive/preservation and retirement:** verify archived prior state and native-link preservation. Current date-only retirement covers earlier dates, not rows for another show on the same date; this is an identified acceptance/implementation gap, not a completed fix. Retirement also requires clean source/identity/participation checks.

Cross-owner dependencies remain separate: shows tracks non-atomic default publication and returned identity checks; schedules owns its same-machine verified archive dependency and upstream retirement effects without a downstream acknowledgment gate; entries owns current-show filtering of retained prior-show participation, dated health/parity and sync-lag handling. Live must not implement changes in those owners' areas under this handoff.

Required natural proof: exact old/new customer/show keys; producer publication run IDs/times and canonical default cardinality; independent schedules/entries/live run IDs/times; relevant-date source/local sync readiness; scoped complete/Partial/failed outcomes; archived old state and preserved record/helper/private links; actual retirement effects, including same-date other-show rows. Observe two normal repeats where repeat acceptance is required. Do not trigger collectors, seed a default, force sync/convergence or repair records to obtain proof.

**No all-four natural new-default rollover has been established.** Repeated same-show runs, historical ESP Fall II archives and producer rollover fixtures are not that proof. This gap is distinct from class 200030429 and from missing participation-specific multi-group eligibility.

## Remaining issues and next-owner instructions

1. **Existing unresolved source identity:** class `200030429`, trip `200398240`, error `No decoded identity for trip 200398240`. Last verified in both runs above. Existing observations/links preserved; errs-log populated; other classes continue. No fix is authorized by this documentation request. Investigate retained evidence first if requested; never substitute historical decoded identity as current proof.
2. **Natural multi-group proof unavailable:** reader handles valid plural arrays and fixtures pass, but entries has no proven participation-specific multi-group source mapping. Producer currently uses unique singleton eligibility and rejects ambiguity. Do not claim natural multi-group acceptance or guess group membership.
3. **Natural rollover acceptance open:** actual all-four new-default transition, sync-lag preservation and retirement behavior are unproven; same-date other-show retirement is a distinct known gap. Follow the ownership and evidence requirements above, without manufacturing a rollover.
4. **Timing semantics still bounded:** raw timestamp/estimate findings do not establish actual phase anchors, a source clock or results publication readiness. The read-time parser is a recommendation only, not implemented or authorized here.
5. **Next status check:** read task state, normal receipts, heartbeat/run-logs/errs-log and participation audit fields. Compare timestamps and local link IDs against the evidence above. This handoff update is documentation only and does not claim a fresh status check.

Normal receipts: `runtime/last-run.json` and `runtime/<run-id>.json`; source/prior-state archives: `runtime/history/`; local errors: `runtime/errs-log.jsonl`; launcher output: `runtime/collector-YYYY-MM-DD.log`.

Offline test command (does not run the collector): `node --test wef-live/test/collector.test.js wef-live/test/entry.test.js wef-live/test/participation.test.js` from repository root. Forty tests passed for this release; do not treat tests as a replacement for required scheduled proof.

**Operating boundaries:** Codex is not the runner. Never invoke `entry.js`, `run.ps1`, a task trigger or alternative production endpoints to obtain proof. No record repairs. A failed verification does not authorize another patch; obtain fresh edit authorization for corrections. Preserve unrelated working-tree changes and other collectors. A handoff file or thread link does not itself transfer credentials or authorize messaging another chat.
