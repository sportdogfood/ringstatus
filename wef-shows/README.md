## Day-priority automation - approved and deployed October 30, 2026

Independent priority-only phase runs on discovery and the first existing wake at/after19:00 Eastern. It uses the canonical planning selected_date as0; midnight does not shift priorities again. Only existing current-show day-priority values within-3..3 are patched/readback; outside-window values stay untouched. Keys, links, counts, source clocks, tasks and protected count stop remain. Priority failure has its own local stop and does not resume counts. Targeted tests and independent0.2.2review passed; normal22:49phase verified4rows (0changes needed) with Airtable readback. FullShowsrun remainsPartial for count stop. [Exact evidence](evidence/day-priority-20261030/automation/README.md).

## Current v1.6 policy - deployed October 30, 2026 at 19:03 Eastern

- At the first normal wake at/after 19:00 America/New_York, select tomorrow only when it falls within the current show date range. Otherwise retain the selected day. Repeat wakes never add another day.
- Start new-show discovery Sunday at 22:00 Eastern; repeat every three elapsed hours from each attempt until the show ID changes, then wait until the following Sunday. Existing five-minute wakes execute due work; no new task.
- Five-minute heartbeat/run/error logging remains. Original source date and last_checked stay separate from calendar selection and authority_checked_at.
- Live and Timing independently operate on TODAY within the show range. Data still receives the canonical selected-date fence; no fabricated handoff or evidence-age reset. Existing archive/retirement safeguards are preserved.
- Normal 19:04 calendar rollover and Airtable heartbeat/run-log readback PASS. Repeat verification stopped: the 19:09 wake exited with Schedule collector already running; its active lock was left untouched. No retry or further patch. The pre-existing count-publication stop remains unchanged; no reset, repair or manual run.

Owner boundary: downstream verification and further downstream work now belong to wef-timing-coordinator and wef-records-coordinator. Both received the deployed handoff. Shows owns only its canonical publication, discovery and own logs/heartbeat.

[Exact release, review and verification](evidence/calendar-discovery-20261030/README.md). This approved v1.6 policy supersedes the earlier v1.5 discovery-window policy below; unrelated locked ownership and protections remain.

# Current discovery policy - October 30, 2026

Scope amendment v1.5 deployed at 18:15 Eastern through the existing task route. Two normal daily-window discovery runs verified at 18:19 and 18:24 Eastern, including Airtable heartbeat/run-log readback. Full runs remain Partial only because of the preserved prior count-publication stop. Sunday/overnight branches have offline proof only; [release, review and evidence](evidence/discovery-windows-20261030/README.md).

- New default show: Sunday 16:00 inclusive to 20:00 exclusive America/New_York.
- Source-selected show-day for the retained show: daily 16:00 inclusive to 20:00 exclusive, following Eastern DST.
- Five-minute heartbeat, run logs and error logs continue. Discovery is logged as skipped outside its window. Count publication continues only where not already stopped.
- Sunday uses the existing ring/default-schedule pair. Other days query default-schedule only for the retained canonical show. Never fabricate tomorrow's date or switch show identity outside Sunday.
- Skipped source checks retain last_checked, archived payloads, IDs and links. Approved companion readers accept a successful closing-window check until the next daily 16:30 deadline; other source observations, count freshness, future-clock and identity safeguards remain unchanged. A missed window expires authority.

Separate existing failure: show-days publication stopped October 30 at 14:07:49 Eastern after Count replay timed out. Its stop was preserved by this deployment. Discovery verification is not count-publication recovery or universal collector success.

The record below describes the prior locked release and dated evidence. The policy above supersedes its unrestricted five-minute discovery wording. All unrelated scope, archive and count-replay protections remain locked.

# Current Shows operating record - 30 October 2026

This section is current; older dated sections below are historical evidence,
not active assignments. Scope **v1.4 remains LOCKED**. Runtime release:
`count-replay-fix-20261030`; [exact hashes and acceptance](evidence/count-replay-fix-20261030/README.md).
This update is documentation only. Reconciled October 30 at 11:27 Eastern:
source/runtime hashes unchanged; existing schedules reused, not recreated.

## Instructions, hierarchy and authority

Load [module AGENTS.md](AGENTS.md), repository instructions and the physical
[owner instruction block](../docs/coordination/wef-owner-operating-instructions.md).
All were read here on October 30. Owner approves; Records coordinates contracts
and evidence; Shows owns canonical show identity/default discovery/show-days.
Canonical Shows supplies Data acquisition; Season separately retains accepted
Data history. The local Season coordinator owns both Data and Season; the temporary
Data fork transfer was reversed. Live independently observes TODAY; Entries links
accepted records only. Schedules still runs; retirement/migration is planning only.

Shows is authoritative for its validated canonical show context, not every entry,
current participation, universal source completeness, timing or live truth.

## Exact base and tables

Owned base: **appCaSmL819KK7UFJ**.

| Table | ID | Responsibility |
|---|---|---|
| shows | tblJA5CUon5s1sWib | Canonical show/default/date context |
| show-days | tbl14MaCCDw6dAywH | Validated day-count publication and native show links |
| heartbeat | tbl7RhSu6a83ev76G | Latest completed collector result |
| run-logs | tblchw5nYX0BhE9DH | Attempts, outcomes, archive and source links |
| sources | tbld8rqxkXyRzCeVK | Input access/validation health, not count freshness |
| err-logs | tblhQWyblTduQHV7c | Actual errors and run/source references |
| code-reviews | tblYDJfIh17xCCtIP | Dated administrative reviews; Records writes for this task |

`meta` (tblwBvigcmqRlSYUb) also exists; the scheduled writer does not manage it.
Do not infer permission to retire it. Verified review record:
[recjXg1esmui5DGOs](https://airtable.com/appCaSmL819KK7UFJ/tblYDJfIh17xCCtIP/recjXg1esmui5DGOs),
exact GET readback PASS reported by Records; [review handoff](evidence/count-replay-fix-20261030/review-handoff.md).

## Inputs, outputs and preserved contracts

Discovery uses the existing decoded Worker `/ring?customer_id=15`, then
`/schedule?date=00/00/00&show_id={show_id}&customer_id=15`; Worker origin is
`https://wef-schedules-decoded.gombcg.workers.dev`. Preserve decoded-response proof,
matching show identity and source-selected date validation; no new endpoint.

Counts come directly from Data **appdl16HJ5ETm8N69**: runs
`tblSxcmUcmB0W9Vhu` and collections `tbl8fQXTGJmv5esNj`. Preserve completed revision,
canonical authority fence, exact retained keys/native references, hashes,
predecessors, count replay and Data archive evidence. Canonical `show-key` remains
`customer_id|show_id`; day key adds date, with native local link to Shows.
Schedules/Entries/Live and Data/Season consume canonical context independently;
Shows never triggers their jobs. Existing default-payload file/hash handoff stays.

Canonical show fields are `customer_id`, `show_id`, `facility_id`, `show_name`,
`start_date`, `end_date`, `selected_date`, `available_dates`, `is_default`,
`last_checked` and `show-key`; archive `default-payload-file`/SHA256 are a technical
handoff, not a portable data-sync replacement. Show-days publish `show-day-key`,
`show`, `date`, count values/coverage/policy/source time, `publication-status`,
`accepted-summary`, evidence reference and attempt status/time/reason. Native
links are local: consumers resolve portable keys to destination-local references,
never copy cross-base native IDs or join by name. Sharing both tables everywhere
is proposed design, not implemented or required: canonical context needs shows;
show-days is conditional on a confirmed daily-count/readiness use. Preserve old
links and historical parents; current sync configuration does not decide design.

Shared files physically under `wef-schedules` support discovery, transport,
locking/reporting and Python chain validation. Their location is not Schedules
collector ownership or a reason to delete them. Shows archives are under
`wef-shows/runtime/history`; Data retained evidence under
`wef-data/runtime/retained-evidence`. Preserve paths, hashes and all consumers.

## Real cadence, archival and deployment

Inspected Windows Task Scheduler task **wef-shows-collector** repeats every five
minutes, anchored at :04/:09/:14 etc., with a four-minute execution limit. Its
established action is `wscript.exe //B //Nologo .../wef-shows/run-hidden.vbs` ->
`run.ps1` -> Node `entry.js`, in this checkout. Runtime configuration is private
`runtime/collector.env`; do not publish credentials. Deploy only authorized changes
through this existing checkout/route and verify normal scheduled runs.

Each successful discovery archives default-ring, default-schedule and shows-before
within the collector; identical content can reuse its compressed SHA256 file after
readback. There is no separate Shows archive timer. At 1,000 run-log rows, the
collector archives an eligible old non-Running row before reusing its slot;
error-linked rows are protected. If all slots are protected it appends, making
1,000 a soft limit. No automatic local-file expiry/deletion is implemented here.
Heartbeat is a single latest-state upsert (`heartbeat-key=WEF-default`), not an
append-only history or a separately archived stream. Sources similarly retain
latest per-source state. Error records are keyed by `error-key`; no automatic
error-log expiry is implemented in the inspected Shows code. Historical show/day
records are not purged by these log-slot rules. No claim of bounded total base
size or full-table archival follows from the 1,000-row log threshold.
This is Shows evidence/log retention, not Season's broader Data archival cadence.

Sunday 16:00-20:00 America/New_York is an expected rollover window, not the only
polling window; the five-minute task continues. Latest local receipt inspected:
`050b7f67-c11a-4bfc-8514-54df0b69d3aa`, October 30 11:24:02-11:24:57 Eastern,
success, four day publications; Task Scheduler exit 0, no day-stop file, and all
four local archive hashes verified at inspection. Show 200000076 remains selected
with source date October 30. Dated acceptance remains the
08:44 and 08:49 actual replay runs in the locked release; this local receipt is
not a new independent Airtable acceptance test.

## Source age, empty outcomes and failures

Source-selected date is not automatically TODAY. Count age uses original accepted
source observation time, never replay, publication or review time. Complete counts
at least five minutes old are Stale; incomplete/missing evidence is Unknown.
Missing current canonical Data context is a successful Unknown publication, with
prior accepted evidence retained. Missing/incomplete data is never verified zero.
Competition-class coverage does not prove schooling/classless attendance or whole-day
emptiness. Review PASS and collection Success do not override these limits.

Publication failure preserves accepted summaries, records Partial and writes
`runtime/show-days-stopped.json`; later normal wakes continue discovery but skip
stopped publication. Do not clear it or retry the failed path without approval.
Source/telemetry failures retain their actual logs; startup/forced termination may
prevent Airtable logging, so local receipt/execution log is fallback evidence.
Count replay timeout recovery is accepted, but its original timeout was not
reproduced; current acceptance does not promise unlimited replay capacity.

## Review schedule, skills and continuity

Review actual authorized material changes before deployment and after observed
behavior/verification failures, plus the separately authorized periodic owner
review. Reuse unchanged independent reviews after checking hashes/contracts;
never turn every collector invocation into a review. Records owns review automation
and review-record writes for this task; preserve dated review history.

Records created **wef-shows-monday-code-review**, independently inspected ACTIVE
on October 30 at 09:19 Eastern and rechecked ACTIVE at 11:07. It targets Shows chat
`01a16976-f0a6-72a1-a75f-75de7f19a815`, **Mondays 09:30 America/New_York**, bounded
at 20 minutes. First scheduled review: **November 2, 2026**. Its existing routine
record **recZTORMGcgsAaezQ** in `appCaSmL819KK7UFJ/tblYDJfIh17xCCtIP` is Pending
first review, not an executed result. Release record `recjXg1esmui5DGOs` remains
separate dated evidence. Future scheduled wakes permit only the named review-record
updates and GET readback; preserve dated history and all runtime boundaries.
The owner removed business observers; do not recreate or replace them. The
existing periodic code review is unchanged. No duplicate review job is needed.
Reviews are best effort when host/app are available.

Actually READ: installed RingStatus Operating Skills **0.2.0** full
`instruction-loader`, `context-compact`, `code-review`, `rebalancer` skill files at
`C:/Users/gombc/.codex/plugins/cache/created-by-me-remote/ringstatus-operating-skills/0.2.0/skills/`.
Owner `$rebalance` maps to packaged `rebalancer`. Local
`C:/Users/gombc/.agents/skills/code-review/SKILL.md` was also read. Records relayed
rs-skills' package/version verification; no install/update was performed here.
APPLIED: scope/authority loading, reconciliation to the locked baseline, reuse of
unchanged reviewed evidence, and documentation-only boundaries. VERIFIED: physical
owner block is readable here, task/cadence inspected, review evidence linked and
runtime hashes checked. Reading a skill is not proof every optional procedure ran.

At boundaries and first tool boundary after 30 minutes of active work, use
[context-check procedure](../docs/coordination/context-check-procedure.md) with this
chat's actual rollout/telemetry; unknown measurements stay unknown. The owner's
**65% context preparation threshold** overrides the skill's 70% default; it does
not authorize forced compaction or resuming stopped work. Before compaction,
prepare a concise authorized private resume with objective, role, approvals/stops,
paths, evidence, blockers and next action; do not copy raw history into Git/Airtable.
No compaction or private history transfer is claimed by this documentation update.
On resume read AGENTS, owner block, this current section, scope and exact release;
reconcile later owner instructions before dependent work.

## Current task reconciliation and lock readiness

| State | Item | Next action / authority |
|---|---|---|
| Complete | Count-replay recovery, two actual replay runs at 08:44/08:49, independent review and Records review readback | Keep locked v1.4/count-replay-fix-20261030. No additional repair is currently authorized here. |
| Active operation | Existing five-minute Shows collection and archive/log handling | Continue unchanged under existing approval. This scope/code lock does not disable the collector or revoke independently approved work. |
| Blocked readiness | Published counts are Stale, despite successful replay/publication | Do not assert fresh zero readiness. Data owns producing newer evidence; Shows evaluates the original source age and coverage normally. No producer repair approval is inferred. |
| Awaiting natural proof | Next genuine default-show rollover after this release and matching canonical/day evidence | Current proof is for show 200000076. A future rollover is not proven by same-show repeats or fixtures. Inspect naturally produced evidence when available under existing authority; do not force a show change, run a collector or add an observer. |
| Awaiting scheduled review | First periodic Shows code review, November 2 at 09:30 Eastern | Existing ACTIVE wef-shows-monday-code-review, 20-minute bound; setup is complete, execution is not yet proven. |
| Proposed / owner decision | Sharing shows-only versus shows plus show-days | Keep [decision map](../docs/coordination/wef-sync-sharing-review-20261030.md) as a proposal. Count consumers, history scope, optional parent-key lookup and final sync wiring remain undecided; no implementation approved. |
| Deferred, not a current blocker | Archive portability and count-replay simplification | Existing local archive handoff and full replay remain protected. Neither removal nor migration is approved. |
| Superseded | Stop discovery after Sunday rollover; old failed release acceptance treated as current | Five-minute discovery continues all week. Preserve dated failures as history; the approved recoveries have their own dated acceptance. Do not restart old work. |

**Lock readiness:** canonical identity/default/date authority, qualified show-days,
current runtime and completed repair scope stay locked now. Future natural rollover
and first periodic-review execution remain explicitly unverified; they are not
retroactively claimed by the existing release acceptance. No open Shows code fix
or failed Shows publication stop was found at this inspection. Freshness limits
and future proof remain real, rather than an instruction to reopen code.

Owner correction, October 30: **no WEF business observers**. Records removed
`watch-wef-default-show-changes` and `wef-running-lanes-october-15-observation`;
`verify-live-natural-same-date-rollover` was already absent. All three local
automation definitions were confirmed absent during this documentation correction.
Do not recreate observers or replacement polling reminders. Natural rollover
acceptance remains unverified; inspect scheduled evidence during authorized work
or on request. Existing collectors, business tasks and periodic code reviews are
unchanged. Context bookkeeping is separate. Observer removal does not stop a
collector or revoke independently approved fixes.

**Next authorized action:** allow the existing scheduled collection/review paths
to operate, retain their real evidence, and review natural rollover evidence when
it exists. Owner decisions are required before sharing implementation or any new
fix, deployment, task/cadence change, record repair or stopped-path resumption.

---

## Prior dated implementation and verification notes

## LOCKED - wef-shows verified scope v1.4 - 29 October 2026

30 October 2026: count-replay recovery verified and relocked. Normal 08:44 and
08:49 Eastern runs each replayed counts and published four show-days, with
independent Airtable/evidence readback and zero errors. Independent code review
passed. See [release, exact deployed hashes and review](evidence/count-replay-fix-20261030/README.md).
Scope v1.4, cadence, archives, count replay and existing handoffs remain protected.
Stale count status remains a source-freshness limit, not a run failure.

Owner directs this module to remain locked. This is a scope lock, not a disabled collector or filesystem restriction. Preserve the five-minute task, canonical shows/show-days, discovery and source checks, Data count reader and protected replay, accepted local archive handoff, keys/links and verified logs/heartbeat. No build or preparation for Entries is pending.

Dependency direction: Entries reads canonical Shows; scheduled Shows does not read, write, qualify or trigger Entries. Count evidence comes directly from wef-data; the Season coordinator owns that producer separately. Season retention is not an intermediary or readiness gate. Older descriptions of Season-supplied summaries below are historical coordinator wording, superseded by this inspected direct-reader clarification. No count replay or archive check is removed.

Reopen only for a verified defect or explicitly approved requirement. A verified defect authorizes investigation, not an unapproved fix, deployment, schema/task change or record repair. Earlier proposals do not reopen this scope. Existing independently approved operation continues.

# WEF default-show authority

## Locked operating plan and responsibilities — 24 October 2026

This is the agreed scope for the wef-shows chat. Changes to this boundary require
explicit owner approval. This is a responsibility baseline, not a filesystem lock.
Clarified 29 October 2026 against [scope.md v1.4](scope.md). Shows remains locked;
reopen only for a verified defect or an explicitly approved requirement. A defect
does not itself authorize a code change.

1. Independently discover Wellington's default show every five minutes.
2. Validate and publish show identity, show dates and the source-selected date.
3. Archive source responses and previous show state; record every run and archive.
4. Supply read-only authority to consumers. Never start their collectors.
5. On discovery failure, retain the last verified show and report the failure.
   A missing default is a failure, not a valid empty collection.

| Owner | Responsibility / Airtable base |
| --- | --- |
| wef-shows | Sole writer of canonical show context in `appCaSmL819KK7UFJ`; owns discovery, its archives, run-logs and heartbeat. Holds cross-consumer source-repair history. |
| wef-schedules | Reads show authority; owns schedule/class/group/ring/date collection and local links in `appUq5Qz9kARqFija`. |
| wef-entries | Linking-only module in `appSxF4pYMBaxpTZd`. Reads canonical Shows authority and links accepted evidence, preserving identities, native links, provenance and source times. No source acquisition, comparison/selection engine or tenant workflow ownership. |
| wef-live | Reads show authority; owns today's live observations, trip identities and local reference links in `appgzosUcVqA4vZdf`. |
| wef-records-coordinator | Records handoffs, dependencies and owner acceptance; coordinates any shared-source changes before implementation. |

The handoff key is `show-key = customer_id|show_id`; consumers match source IDs,
not Airtable record IDs across bases. Show metadata has no private tenant
selections, user follows or permissions. `selected_date` is source-selected,
not automatically TODAY. Missing, ambiguous, inconsistent or older-than-30-minute
authority must not become an invented show or trigger another collector.

Entries reads Shows; Shows does not read, write or trigger wef-entries during
normal scheduled operation. Shows count evidence comes directly from Data,
not Entries. Entries or CWF readiness is not a Shows collection gate.

Downstream collectors, tenant helpers, schemas, tasks and acceptance remain with
their respective owners. This chat hands off exact source IDs, affected runs,
retained evidence and unresolved questions. It does not independently edit
downstream or overlapping shared-source code. CWF is a separate downstream
consumer, outside Shows inputs, outputs, dependencies and acceptance gates;
tenant integration is not assigned to the Entries linking module.

Current plan: preserve the proven discovery implementation and five-minute
cadence. No additional build is needed for this scope. Any future change must
preserve the published keys and links, pass relevant offline checks, and be
verified through two normal scheduled runs. No manual collector runs or record
repairs. Host migration must explicitly resolve the local archive dependency.

## Current operational evidence — checked 24 October, 11:21 a.m. Eastern

- Default: `2026 ESP Fall III (#261901)`, show `200000075`, October 22–25.
- Source-selected date: October 24; one default record, `rec4En9UWd6lkT1Oh`.
- Latest twelve scheduled runs, 10:24 through 11:19, all `Success` in Airtable.
- Latest run: 11:19:03–11:19:09, `8290957b-6acf-43c5-9527-cb18f50ff5a1`.
- Previous run: 11:14:01–11:14:08, `456df403-84b7-4172-8ec9-f0183767c99e`.
- Heartbeat matches the latest completion. All six compressed archives from
  those two runs were read and their SHA256 values verified against run-logs.
- Task enabled/Ready, exit result 0; next scheduled start observed: 11:24.

Evidence was read from Airtable `shows`, `run-logs`, `heartbeat`, the task state,
`runtime/last-run.json`, and the archive paths recorded in the two run logs.
This check made no source requests, collector runs, repairs or runtime changes.
It establishes wef-shows health at the stated time, not downstream readiness.

`wef-shows` owns default-show discovery. It reuses the proven schedules
`discoverFocus` helper, with its own state, transport runtime and task. The helper
remains exported for compatibility and offline fixtures; schedules' normal entry
point reads the verified central show instead of invoking that helper.

Discovery makes the same two requests through the unchanged decoded Worker:

- `/ring?customer_id=15` discovers the source default show ID.
- `/schedule?date=00/00/00&show_id={discovered-id}&customer_id=15` supplies the
  default schedule date, show dates, facility and full default schedule payload.

The independent hidden Windows task `wef-shows-collector` runs every five
minutes, one minute before schedules. It does not start downstream jobs. The
computer must be awake and the current Windows user signed in.

## Ownership

`shows` in base `appCaSmL819KK7UFJ` owns show-key, show_id, customer_id,
facility_id, show_name, start_date, end_date, selected_date, available_dates,
is_default and last_checked. The stable key is `customer|show-id`. Prior shows
remain in the directory; rollover changes the default flag after archiving.
`run-logs` records every attempt, error and archive. `heartbeat` holds the latest
completed discovery result. Existing meta remains untouched.

The complete decoded default response is retained in a compressed, hash-verified
local archive, using the existing archive pattern. The show
record publishes `default-payload-file` and `default-payload-sha256`. Schedules
reads that exact response for its established normalizer/fallback comparison;
it does not reconstruct a partial payload or request default discovery again.
This file handoff is local to this machine, not a hosted download or cross-host
sharing mechanism. A host migration must preserve the archive or explicitly
replace this handoff.

Schedules rejects missing, ambiguous, more-than-30-minute-old or inconsistent
canonical data before collecting. Archive paths are restricted to this runtime's
history folder and compressed contents must match the published SHA256. Failure
does not trigger a manual discovery fallback or erase prior schedules.

The existing schedules `shows` records and their IDs remain in place as linked
local mirrors. Source fields come from wef-shows; archive fields remain owned by
schedules. The mirror's last_checked preserves the canonical discovery time.
Entries and Live read canonical wef-shows directly; the dependency is from each
consumer to Shows, not the reverse. Consumers own their reads and acceptance.
This README makes no current CWF migration or compatibility claim. The initial
release evidence below retains its original, narrower scope and historical checks.

## Scope and evidence

Source-selected date is not automatically TODAY or proof of a live show.
Schedules continues collecting every retained show day, with existing protections
and show-specific manual settings. Discovery has its own cadence and does not
depend on a completed schedules run.

`configure.js` writes destination schema only. `entry.js` is the scheduled
business entry point; do not invoke it manually for proof. `verify.js` reads
existing receipts, records and archives only. Release evidence is in `evidence`.
`verify-consumers.js` remains an existing verification helper with Entries/Live
references; it is not a dependency of scheduled Shows collection and is unchanged
by this documentation clarification.
Private credentials, source responses and operational state are ignored under
`runtime/`. Deployment acceptance requires two normal scheduled consumer runs.

## Release acceptance — PASS, 23 October 2026

Normal producer runs at 10:34 and 10:39 Eastern populated and refreshed the same
canonical show record, rec4En9UWd6lkT1Oh. Normal schedules starts at 10:35 and
10:40 consumed those verified records and completed all four dates, with zero
failed days. The existing empty October 22 warning was preserved. An initial
read-only verification predicate expected only Success; the established schedules
Warning label was checked against zero failed days and only empty_show_day
warnings. No production correction or rerun was needed.

All 449 existing schedule IDs and their show/ring links, all 24 ring IDs and
the existing local show record recpjdzMqBApZ8KPs were preserved. Both schemas,
all three existing collector task definitions and 12 protected code files were
unchanged. Offline checks: 124 schedules, 65 CWF and 10 handoff tests passed.
Normal run logs/heartbeats and compressed archive SHA256 values were read back.
No manual collector runs or record repairs. Exact receipts and checks:
`evidence/release-verification.json`.


## Canonical show-days publication � 29 October 2026

The approved addition publishes canonical `show-days` after existing default-show
discovery, using local native links to `shows`. It reads only Data runs/collections;
source acquisition, Data schemas, downstream inputs and all task cadences stay as-is.
`day-schema.js` is the focused schema configuration function; `configure.js` retains
its original discovery schema role. No manual collector execution is part of deployment.

`day-source.js` reads a completed Data head and exact predecessor/collection references,
uses a hash-and-record-identity checked local cache, and invokes the pure Season
`day_counts.accepted_summaries` validator through `day-replay.py`. The subprocess
has no Airtable credentials or network operations. It uses the existing Data Python
runtime; that deployed module and its replay helper are dependencies of this release.
The terminal head is read again after replay. A normal canonical-context change
before Data catches up produces Unknown days, not invented counts.

`accepted-summary` is authoritative; display columns are projections. It retains
scope, counts, original observation time, upstream revision/digest and exact summary
part locators. Detailed membership stays in the original Data evidence. A reader
must verify the local digest, native show/date scope, read the summary before and
after projections, and require stable revision/digest/status plus matching projections.
Never read just a displayed zero or publication status as a readiness decision.
Unknown observations may have null source time and null counts. The five-minute
age is measured from source as-of, never verification or publication time. Verified
competition-class coverage is not whole-day attendance or a Live suppression gate.

Day publication failures preserve the previous accepted summary, log Partial with
separate discovery success, and create `runtime/show-days-stopped.json`. Later
normal wakes continue original discovery but do not retry the failed day publisher.
Removing this stop or fixing a failed scheduled path requires separate owner approval.
Incomplete/unavailable coverage is a valid Unknown outcome, not an execution failure.
A failed staged projection is nonconsumable until approved recovery; the original
accepted body remains the preserved historical tuple.

`verify-days.js` is GET-only normal-run verification: it compares each accepted body
with an independent replay of its exact producer revision, checks display/native links
and saved run evidence, and writes a local verification receipt. It never invokes a
collector. Two normal producer/consumer pairs are required for deployment acceptance.

## Code-review requirement - 30 October 2026

Review authorized material changes before deployment and after observed behavior
or verification failures. Reuse unchanged reviews only after checking hashes and
scope; no per-run reviews. Records manages separately authorized periodic reviews. Keep offline review, deployment and
normal scheduled acceptance separate. Review failure does not authorize a fix.
For this request Records alone writes and verifies Airtable review records in
Shows base appCaSmL819KK7UFJ; Shows supplies evidence. See the
[standard review report](evidence/count-replay-fix-20261030/review-handoff.md) and
[scope procedure](scope.md). Runtime remains locked v1.4.
