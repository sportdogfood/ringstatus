## Current Live status — 30 October 2026, 13:03 Eastern

Latest read-only check: October30 at13:35Eastern. All19 deployed/protected runtime hashes still match. Eleven postdeployment receipts contain no populated activity. Latest completed run4bfd1122-90b5-47a0-90bf-cdf822a84379 (13:32:13–13:32:29Eastern) is ok=true, all-today-groups-completed, with zero collection/logging errors. Two naturally populated runs remain the only acceptance blocker; no code/retention/workflow edits or new approval needed for the already-deployed corrections.

The approved journal/count, cursor and exact-group corrections are implemented, reviewed and deployed. [Release and task evidence](evidence/publication-continuation-20261030/release.md), [independent review](evidence/publication-continuation-20261030/review.md), [deployed hash](evidence/publication-continuation-20261030/deployed.json), [first normal receipt](evidence/publication-continuation-20261030/first-normal-skip.json).112 offline tests pass; independent review includes real-journal restart/failure and deadline cases. Only publication.js changed;18 other runtime files are preserved.

- **Completed:** journal-save failures are logged with accurate remaining counts; unsafe writes without durable intent are deferred, not dispatched. Other safe records/lanes, collection and future invocations continue. Pending intents survive failures and reconcile before retry. Outcome/log failures do not create a global halt. No immediate retry loops.
- **Completed:** first budget-deferred item remains the next cursor; attempted writes including uncertain responses respect the unchanged cap. Exact known show/date/ring/group/native links are filtered before uniqueness, resolving the six false ambiguities. Actual multi-group or duplicate exact matches remain logged and withheld. No new matching rules, fallbacks, fields or budget/freshness relaxation.
- **Normal execution verified:** run6a729be2-fbb4-481b-803b-b6a70f197e6b at13:02Eastern completed safely,0 publication writes,0 errors, retention Success. All18 source groups were completed; both publication lanes correctly Skipped.
- **Only remaining acceptance blocker:** no current populated activity. Two populated postdeployment runs plus ring6 and the six exact class-occurrence writes cannot be proved from skipped runs. Existing collector checks continue every3minutes; inspect saved evidence within authorized work/on request. No manual source runs, repairs, forced joins or observers. Offline injected failures are not claimed as production events.
- **Preserved:** prior24-hour log-retention acceptance, archive catalogs and links, source authority, independent lanes, Entries linking-only, existing Monday review. All business observers remain removed. Scope/code may be locked to this reviewed release; populated operational acceptance remains pending without adding new work.

Earlier checkpoints below are historical where superseded. The prior failed-gate approval request was superseded by the owner's explicit direction to correct these paths.

## Current publication blocker — 30 October 2026, 12:45 Eastern

Publication is not complete. The has_JSON and concurrent-acquisition corrections now have normal-run proof. Deferred counts still underreport when journal-save EPERM escapes the item loop. One eligible Data ring remains70m16s behind its fresh source because an unprocessed budget-boundary item advances the cursor. Six occurrence errors are false ambiguities: each has one exact source-group match; class200030671 remains genuinely multi-group. Prior missing trip200399568 is resolved. See [exact evidence and smallest corrections](evidence/publication-fixes-20261030/current-blocker-20261030.md). This failed verification gate permits no further code patch/retry without separate approval. No observers or workflow changes.

## Current release and scope-lock status — 30 October 2026

Owner approved exactly three publication corrections in Records. They are implemented, independently reviewed, offline-tested (99 PASS) and deployed at 11:42:28 Eastern. [Release/task evidence](evidence/publication-fixes-20261030/release.md), [review](evidence/publication-fixes-20261030/review.md), [exact runtime hashes](evidence/publication-fixes-20261030/deployed.json), [two-run acceptance summary](evidence/publication-fixes-20261030/acceptance-summary.json). Only publication.js and the collector summary changed;17 other Live runtime JS files remain at the protected baseline, including retention and class-error/audit behavior.

- **Completed corrections:** actual has_JSON spelling with true/false/missing preserved; accurate deferred and separate uncertain counts with readback-before-retry; stable identity/matching/link/overlay validation that permits unrelated acquisition metadata updates. Full row audits, one-field external PATCH, original freshness and budgets remain intact.
- **Verified normal runs:** 3d7a24cd-ed6d-4c5f-b0ba-e1ec503c0dbe at11:44 and c8d1fe08-1c80-4097-a1f5-79120c1d258b at11:47 Eastern each collected196 entries. Schedules35+35 writes; Data37+38;145 archived/current readbacks preserve identity/native links and original freshness. Data deferred230 in each; uncertain1 then0 with NotApplied reconciliation before continuation. Both business outcomes remain Partial, not full coverage.
- **Pending acceptance:** natural has_JSON publication into a Data group and positive concurrent-metadata exercise were not encountered in this two-run pair; both behaviors have independent offline regression proof. Full timely publication remains unproved due existing budgets/withheld matches. Same-date rollover and multi-group natural proof remain separate. Inspect saved evidence during authorized work/on request; no forced runs or guessed joins.
- **Complete and protected:** completed-log24-hour retention accepted separately on b7a9e822-461a-42e2-bf25-dd601a268b1e and853114e5-fe5b-4c97-8999-ba0133a0a38c (1,424 archived/deleted, seven show catalogs and protected baseline retained). This is not publication proof.
- **Continuing scope:** independent TODAY Live observations using canonical Shows and current Schedules references; Live tables plus approved single-field Schedules/Data overlays. No reverse Entries dependency or acquisition ownership transfer. Existing three-minute collector, recurring archives, logs and Monday review continue.
- **Removed:** all business observers; do not recreate/replace hourly verify-live-natural-same-date-rollover, daily watch-wef-default-show-changes or wef-running-lanes-october-15-observation. Records context bookkeeping is separate. No task/cadence changes or record repairs.
- **Lock readiness / next action:** reviewed scope/code baseline can be locked with pending acceptance explicitly recorded; full operational lock/acceptance cannot yet be claimed. A scope/code lock never disables collectors or independently approved work. No new corrections are authorized by verification; actual new failed gates retain the owner approval boundary. Existing Airtable review record is dated pre-fix evidence owned administratively by Records; this release receipt is local and no tracker was created.

Earlier status below is historical wherever superseded by this checkpoint.

## Approved 24-hour log retention — 30 October 2026

Owner approved recurring archival/removal of completed heartbeat, run-logs and errs-log records older than24hours. This narrow release does not fix publication defects or change cadence/source collection. Existing normal entry workflow calls retention only after collection ok=true (Partial class/publication outcomes can still be ok=true); failed collection skips it. No manual runs/record repairs.

Retention accepts terminal Success/Skipped/Partial/Failed completion older than24hours; active/incomplete/current invocation and non-run control rows are preserved. Error rows additionally require older completed parent evidence from existing run-logs or saved local receipts. At most250 rows per table per invocation, with four-minute request deadline and a60-second reserve before starting another table; initial backlog drains across normal runs.

Exact immutable gzip batches are verified before deletion. Three fixed collections rows (`log-archive|heartbeat`, `log-archive|run-logs`, `log-archive|errs-log`, source-kind log-archive-catalog) point to immutable chained manifests, retaining previous references/hashes and full original row IDs/fields. Existing seven show archive catalogs and business collections are untouched. Manifest row-count counts archived records. No local archive TTL/purge is introduced.

Every deletion batch rereads unchanged eligible rows, validates exact DELETE acknowledgements and verifies absence. Durable runtime/log-retention-stop.json is created before work, removed only on successful completion or safe deferral before a new table, and retained on failure/interruption. That marker blocks retention without stopping independent collection; no automatic retry/repair of uncertain deletion. Failure requires owner review/approval. Current run logs/heartbeat summarize retention and retain archive descriptions. Local receipts/logs and publication journals/control state are never deleted.

Evidence: evidence/log-retention-20261030/{baseline.json,review.md,tests.txt,deployed.json}.91offline tests passed; independent11retention tests passed. Normal-run acceptance BLOCKED: first postdeployment10:41run failed with source ListAjax HTTP504; retention correctly skipped, no log deletion. Two successful normal retention runs remain unverified (evidence/log-retention-20261030/release.md). Existing3P2publication findings and populatedPartial acceptance remain separate.

# Live operating reference — 30 October 2026

This is the current module orientation; dated sections below are historical evidence, not competing authorization. Actual workspace: `C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-live`. No separate private coordinator workspace was identified or configured for this task. [Local instructions](AGENTS.md) load the physically available [canonical owner instruction block](../docs/coordination/wef-owner-operating-instructions.md), actually read in this checkout.

## Responsibility and source authority

Owner approves; Records coordinates. Canonical Shows → independent Data acquisition → separate Season retention. LOCAL Season coordinator retains both distinct Data/Season responsibilities; the temporary Data fork was reversed. Live independently collects TODAY observations, validates source identity and publishes only `live-observation-json` to approved existing destinations. Entries is linking-only, not a Live input, enrichment assignment or readiness gate. Schedules still runs independently; Data-only switching/retirement is preparation only. Retained rows, successful invocations and group completion are not universal proof of current membership, completeness or whole-class completion.

## Exact owned base and tables

Base **wef-live / appgzosUcVqA4vZdf**. IDs below are saved configured schema plus Records' verified review-table handoff; no new schema inspection or changes are implied.

| Table | ID | Responsibility |
|---|---|---|
| live-rings | tblw8bodkRbCItxIH | Dated observed ring state |
| live-groups | tblMhQtjpvyODEUSn | Dated group state/counts |
| live-classes | tblNaM9ehOJFsrveV | Validated class progress |
| live-entries | tblf7550e7UGT8ZSF | Observed trips and decoded identities |
| collections | tble6fwMDBbeeGxly | Source datasets and permanent archive catalog |
| heartbeat | tbl1K0KT8A2kZEJgS | Invocation/completion stamps |
| run-logs | tblwne646pQi5KlXA | Run outcome/counts/archive evidence |
| errs-log | tblMzOaTvWus2pM4I | Separate error rows |
| code-reviews | tbl1jEtJusopJGC6O | Administrative review evidence; Records writes for this request |

Existing reference tables are not owned acquisition: `wef-schedules.schedules` / tblGPXIUnD0sfTEIX is actively read for group/class schedule links; `wef-entries.entries` / tblfj77GmXxuiAoB4 is retained for historical links, with runtime reads forbidden. Saved schema also includes meta / tblwBvigcmqRlSYUb; it is not a collection authority or active runtime table.

## Interfaces and boundaries

- Upstream authority: Shows appCaSmL819KK7UFJ / shows supplies canonical show/date with its existing freshness guard. Calendar equality prevents collecting an upcoming date as TODAY; it does not select the show.
- Sources: public ListAjax, LiveScoreWidget, getLiveClassData and live-status; decoded class identities through existing wef-schedules-decoded Worker. Keep that service despite its name. Shared bounded source reader is `../lib/wef-source-read.js`.
- Current Schedules base appUq5Qz9kARqFija: active local schedule sync, plus publication to rings tbliUwvXYdMhvJ3Fm and schedules tblqiTFqqO3zk2KJC.
- Data base appdl16HJ5ETm8N69: publication to rings tblhKkfYXppW3TbTq, groups tblQObOvktufkyq54, classes tbl0IFf067w4wUBj5, schedules tbl6KKlYuMIPHEJWe, trips tblt0GvQ39bHIu930.
- Only the additive JSON overlay may be patched externally; no acquisition fields, native links, create/delete, names-based joins or ownership transfer. Downstream timing/outputs must consume original observation timestamps/provenance; Live does not promise whole-class completion or forecasts.

## Actual cadence, archives and semantics

Registered Windows task `wef-live-collector`, inspected 30 October: repetition PT3M, no duration/end boundary configured. Action: wscript.exe //B //Nologo → this module's run-hidden.vbs → run.ps1 → entry.js. This task runs the business process; Codex messages/reviews are separate and may wake this chat. There is no overnight task stop. Do not manually invoke it.

Every normal invocation stamps/logs and reads canonical scope. For an active TODAY date, live-status false or a nonempty all-completed group list skips detailed collection; the three-minute checks continue. Explicit empty activity succeeds without clearing current observations or renewing publication. Missing/invalid/failed responses are not verified empty. Class identity errors preserve prior observations/links and allow other classes; publication errors remain separate per lane and do not invalidate successful collection. Task exit 0 can accompany Partial business results.

Source and class snapshots are content-hashed gzip archives under runtime/history, created/reused during normal collection. Publication source/intent/before-after is durable before PATCH; outcome/readback follows; unknown writes retain journal state for later scheduled reconciliation. There is no separate timed archival job or inferred retention TTL.

Recurring old-data retirement is evaluated after successful valid collection including empty/skipped results, provided no unresolved classes. Failed collection or unresolved class input does not authorize retirement. Publication-only errors do not cancel otherwise eligible retirement. Exact old owned rows are archived and catalog-verified before child-first deletion; current/future protection remains. Permanent catalog rows remain in collections. Seven catalogs/550 historical records were verified in prior retirement proof; see evidence/retirement-20261029/release.md. This dated evidence is not a fresh audit claim.

Publication freshness: original validated observation ≤6 minutes, ≤1 minute future tolerance, same TODAY scope. New actual observations may advance time; replay cannot. Successful class detail is required for class counters; discovery refresh is insufficient. Missing phase counts remain distinct from zero. Ambiguous groups/occurrences and contradictory identities are withheld. No synthetic end time or weaker freshness is permitted.

## Deployment, review and next action

Current deployment includes the October 30 log-retention release, which supersedes entry.js and airtable.js from publication-v1-20261029 and adds log-retention.js. Exact hashes and independent review are in evidence/log-retention-20261030; unchanged publication behavior retains its October 29 evidence. Existing guarded deployment uses this checkout between registered invocations with no collector lock and adequate time before the next trigger, only with applicable approval. No task changes or manual proof.

Recurring log retention archives completed heartbeat, run-logs and errs-log records strictly older than 24 hours after successful collection. It preserves active/incomplete/control rows, existing archive catalogs and links; errors need completed parent evidence. Verified immutable batches and chained catalog manifests precede deletion, with fresh eligibility checks and absence readback. Work is bounded to 250 records per table per invocation and a four-minute request deadline. A durable failure marker stops retention, not independent collection. Local archive files have no purge policy.

Retention acceptance remains BLOCKED: the first postdeployment scheduled run b09174d2-e4a0-49db-9798-c089cf45a1c6 at 10:41 Eastern on October 30 failed with ListAjax HTTP 504 and correctly skipped retention. No two successful scheduled retention runs are claimed. The 91 passing offline tests do not replace that proof. Preserve this stopped verification path; do not retry collectors, repair state or fix the source under documentation authorization. See evidence/log-retention-20261030/release.md.

Code review: [report](evidence/code-review-20261030/review.md), [hash manifest](evidence/code-review-20261030/manifest.json). Three confirmed P2 findings remain: has_JSON casing; early abort deferred-count underreport; unrelated acquisition changes interrupting publication. 80 offline tests PASS; populated operational acceptance PARTIAL, with budget/matching/source-identity limits. No fix authorization. Exact populated audit report: evidence/publication-v1-20261029/populated-verification-20261030.json.

Airtable review: appgzosUcVqA4vZdf / tbl1jEtJusopJGC6O / rec5fLj3xEqkYBpxA; Records reported exact GET readback PASS. Review FAIL3P2 is separate from tests PASS and populated PARTIAL.

Periodic review: Records confirmed ACTIVE configuration and readback on October 30 for automation `wef-live-monday-code-review`, targeting this chat `01a16902-240a-7280-8661-90b0d8d7325a`, Mondays at 10:00 America/New_York, first November 2, 2026, with a 20-minute bound. Routine record `recYUERW7D8RF2OXS` in `appgzosUcVqA4vZdf/tbl1jEtJusopJGC6O` remains Pending first review; configuration verification is not execution proof. Prior release review `rec5fLj3xEqkYBpxA` remains separate. Records owns scheduling; do not create another automation. Reviews are best effort host/app dependent and do not trigger collectors. Review material changes before deployment and failures when observed; reuse unchanged review evidence appropriately, not once per collector invocation.

## Instructions, skills and continuity

Actually READ in full October 30: canonical owner block; common review standard; installed Operating Skills **0.2.0** instruction-loader, context-compact, code-review, rebalancer at `C:/Users/gombc/.codex/plugins/cache/created-by-me-remote/ringstatus-operating-skills/0.2.0/skills/<name>/SKILL.md`. Also read legacy aliases under C:/Users/gombc/.agents/skills (loader/rebalance/code-review) and C:/Users/gombc/.codex/skills/context-compact. Packaged rebalancer is the canonical $rebalance name adaptation. rs-skills verification of all30files/remote agreement was relayed by Records; no install/update performed here. Handoff skill is discoverable but not claimed READ in this documentation step.

APPLIED: instruction loading, current-scope rebalance, review procedure and continuity preservation. VERIFIED: documentation readback, actual registered three-minute task route, prior deployment hashes/tests/review evidence. Reading context-compact is not a claim of actual compaction, transcript archival or measured context utilization.

Resume checkpoint: read AGENTS.md, this current reference, scope/HANDOFF and exact review/manifest evidence; retain approvals/stops and pending next action. Follow ../docs/coordination/context-check-procedure.md at task boundaries/30 minutes. Current task is documentation/review coordination only. Next review is the configured November 2 routine; do not run it early or claim completion before actual review evidence. Corrective runtime work requires separately scoped owner approval. Do not reopen Data-only migration or retirement.

Continuity preparation threshold is the owner's 65%, as relayed by Timing, overriding the skill's generic 70% default. Use actual session telemetry; unavailable measurements remain unknown. This does not authorize extra monitors or resume any stopped workflow. On October 30 the existing review TOML and registered PT3M task were read back directly; no duplicate automation was created.

---

## Historical release and workflow notes (superseded where conflicting)

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

Entries reads wef-live for downstream linking; Live independently collects observations. Live no longer reads the Entries reference, inspects its publication, or qualifies its schedule/participation results. The Airtable adapter rejects Entries reads and metadata discovery; the unused Entries base setting is removed. The existing sync/table/link fields remain intact to preserve native links and history, not as a collection dependency.

For collected observations, existing native entry links and original verification/check timestamps are omitted from writes and preserved. Previously current participation qualification is marked historical because Live no longer validates it. New observations do not create Entries links. Source identity validation, class/ring/group schedule links, class-error logging and recurring verified archive retirement are unchanged. Entries owns current downstream qualification. No manual runs, record repairs or source sync changes.

58 offline tests passed, including populated repeat collection with Entries reads forbidden, exact link/timestamp preservation, unresolved classes, logging and archive/retirement safeguards. Deployment: 19:33:37 Eastern through the existing scheduled task path. Two normal runs at 19:35 and 19:38 Eastern passed: matching Success logs, no errors, unchanged archive catalog and verified file hashes. Evidence: evidence/no-entries-read-20261029/release.md. Both were empty activity; populated behavior remains offline-tested.

## Prior v1.5 inspection - superseded by v1.6 above

**Entries reads wef-live for downstream linking. Live owns independent observation collection.** Entries is linking-only, not a collector, lane-comparison or tenant-selection engine. CWF is not a core input, dependency or acceptance requirement. Season is retention-only and never a current collection gate.

Implementation distinction: Entries collector.js now invokes live-input.js and allows GETs of Live live-entries, live-classes and collections. Live collector.js still reads the older wef-entries.entries reference and calls participation.resolve. This remaining reverse dependency is existing compatibility behavior, not the intended ownership direction. It was not removed by this documentation update; removing it requires separately approved code work preserving links. No populated downstream acceptance is claimed here.

Health checked 29 October at 19:30 Eastern: normal 19:26 and 19:29 runs completed Success, with matching heartbeat/run-logs and no error rows or logging errors. Three-minute cadence remains. All seven old-show archive catalogs are Retired; hashes and exact fields/links verified for 550 archived records. No show 200000075 observations remain active. Current activity is empty; no duplicate retirement occurred.

# Independent WEF live collector

Destination: `wef-live` (`appgzosUcVqA4vZdf`). Built 23 October 2026.
Current task is enabled and runs every three minutes. Earlier failure/recovery notes are historical evidence, not current task status.

## Existing reverse reference - prior implementation evidence — 29 October 2026

Live now consumes the accepted Entries publication through the existing read-only sync, including its five status/evidence fields. It checks exact identity, original-source freshness (30 minutes), unresolved newer attempts and local schedule availability. It no longer replays upstream schedule completeness/parity. A missing/stale mapping does not stop observations; previous links stay historical. Existing class/group schedule links and logs remain unchanged. See evidence/entries-contract-20261029/release.md for acceptance and scope.md v1.5 for the current contract.

## Current boundary — 29 October 2026

Live has no tenant-code imports or tenant-specific collection prerequisite. Entries is linking-only, reading accepted participation/schedule evidence and wef-live observations. Season is not a current collection gate. Historical release notes below are dated evidence, not current dependencies. See scope.md and HANDOFF.md for current ownership and cadence.

## Scope and authority

Read the current default record from `wef-shows.shows`: show ID, customer ID,
selected date, available dates and show start/end. Require live source identities
and dates to match. The selected date must also be the current New York day;
this is an upcoming-day guard, not an alternate show selector. A future selected
date produces an explicit no-activity success, with no live source request.
Canonical authority is base `appCaSmL819KK7UFJ`. The live collector requires one
customer-15 default with a valid show key and a checked timestamp within thirty
minutes. There is no schedules-show fallback and no discovery request.

Collect rings/groups and each available class's live trips independently of
tenant-specific feeds. Tenant slices happen downstream. Public ListAjax and
LiveScoreWidget supply the show/day graph. Public getLiveClassData supplies
trip observations; the unchanged decoded classes Worker supplies exact trip,
entry, horse, rider and trainer identities. No proxy routes were changed.

## Tables and references

`live-rings`, `live-groups`, `live-classes`, `live-entries`, `collections`,
`run-logs`, `heartbeat` and `errs-log` are destination-owned. `schema.js` lists fields and
native links; `evidence/schema-configured.json` records their Airtable IDs.

`wef-schedules.schedules` and `wef-entries.entries` are automatic read-only
native syncs. Their local record IDs are native link targets. The owner approved
one exception to keeping entries untouched: the `wef-live-reference` view,
containing only entry-key, show-id, date, entry-id, class-id, horse-id, rider-id
and status. External-workspace sync and edits from other bases are off. No
private collection/follow associations are shared. Existing entries records,
collector, field schema and task remain unchanged.

Missing reference rows do not prevent live observations. Missing or ambiguous
schedule matches are reported; entries without an available reference may have no binder
link. A failed reference read preserves existing links. No names-only joins.

## Execution and verification

Task `wef-live-collector` repeats every three minutes. Its hidden WScript launcher
runs PowerShell/Node without a console window; Windows sign-in and an awake
computer are still required. This is a separate task, not a Codex heartbeat.
Overlapping task instances are ignored; the execution limit is ten minutes.
`entry.js` is scheduled only. Never invoke it or `run.ps1` manually.

`configure.js` configures destination metadata only. `verify.js` reads normal-run
receipts, Airtable rows and archives, and writes local verification evidence.
The retirement release passed 62 offline tests and two normal scheduled runs. Heartbeat records each invocation; run-logs records outcomes and archives; errs-log records individual errors. Logging failures are isolated. A failed receipt does not permanently stop later scheduled invocations.

Verified archives precede eligible retirement, including on successful empty/skipped runs. Permanent catalogs remain in collections. Current/future records are protected. No manual runs or repairs are part of this workflow.

Schemas and runtime code for schedules and entries are protected. During
preflight, two schedules test files changed outside this work; their paths are
recorded in evidence/preflight.json. Collector runtime hashes stayed unchanged.

The local archives/credentials/operational receipts are under ignored runtime/.
Source inspection fixtures are diagnostic inputs, never scheduled-run proof.

## Normal-run evidence and required correction

Normal start: 23 October 2026, 10:32:12 Eastern; completed 10:34:30.
Run ID: 14a119c2-f1c6-4f77-a31e-6af424a84793. Task exit code 0.
Collected 6 rings, 17 groups, 30 classes and 195 trips; 68 archives verified.
Heartbeat/run-log readback passed. Four exact links to today's entry references
passed. Twenty-one classes linked to schedules; nine were left unlinked.

The source schedules are not established as broken: the new matcher counted
both active and dropped rows. Eight affected classes have one active schedule;
class 200030243 has two active schedules, one for each of its two live groups.
The existing stage keys identify these group contexts. Evidence is in
schedule-link-diagnosis.json and the normal-run JSON under evidence/.

Approved correction deployed on 23 October 2026: match only active schedule
rows using the existing show/date/class and group-specific stage keys; allow
multiple valid schedule links for multi-group classes; give each live group only
its own schedules. Add offline cases for dropped rows and multi-group classes,
then verify two normal runs. No source schedule changes or record repairs.

The task was re-enabled after all 35 entries/live offline tests passed, including
dropped schedules, multiple group-specific schedules, repeat identity stability,
canonical source reads, stale authority rejection and protected writes. Normal
acceptance is recorded under `../wef-shows/evidence/consumer-connection/`.


Final readback for this change: both consumers used canonical show record
`rec4En9UWd6lkT1Oh` directly during their first normal post-deployment run.
Live's 30 classes have 31 correct active schedule links across 17 groups;
all 248 pre-existing live identities and seven entries identities were retained.
This is partial verification, not two successful runs. Live failed on six HTTP
502 class responses and missing decoded identity for trip 200398019 in class
200030319. Entries failed on trainer 50121 HTTP 502. Both wrote Failed to
heartbeat/run-logs. Live remains disabled with its original five-minute trigger;
entries and the other existing task configurations remain unchanged. Source
investigation/correction and renewed normal-run acceptance remain outstanding.


## Source handling update — 23 October 2026, 13:02 Eastern

Approved source correction adds one bounded transient HTTP/timeout retry and
one paired snapshot refresh for a missing trip identity, with timestamped
public detail requests. It never substitutes guessed or historical identities.
Forty offline/deployed tests pass. Worker and schedules are unchanged.
The first normal live run still encountered a persistent 502 for class
200030347; further live starts are disabled. Full live acceptance remains blocked.
Evidence: `evidence/source-fix/`. No additional production changes were made
after this failed gate.


Final acceptance at 13:11 Eastern: entries completed normal runs at 13:04 and
13:09, with seven and nine entries respectively. Both had no source errors and
passed record/link/archive checks. The first full verification passed. The
second full verification was blocked by concurrent changes to
wef-tenants/airtable.js, collector.js and settings.js at 13:07, outside this
task's deployment file list. Those changes were left untouched; expected hashes
were not changed to hide the drift.
Live's 13:02 normal run ended Failed with persistent class 200030347 HTTP 502;
no missing-trip identity error remained and 68 archives verified. Live is disabled.
No production patch or manual retry followed that failed gate. Overall work is
incomplete: persistent 502, concurrent protected drift, and two live runs remain.
Full evidence: wef-live/evidence/source-fix/release-status.json.
