# wef-records-coordinator

Document version **1.1.0** — updated **November 1, 2026 (America/New_York)**. This is the coordinator document version, not a collector or plugin release. This document does not authorize changes to collectors, schemas, workflows or schedules. Later explicit owner decisions control over dated snapshots.

## Purpose

Coordinate the WEF records workflow so each owner stays in its lane, producers publish clearly defined facts, and downstream consumers can use those facts safely. Maintain ownership, interfaces, decisions, approvals and verification evidence. The coordinator is not a collector or production runner.

## This agent's failures and required reminders

Applies specifically to **wef-records-coordinator**, chat `01a169bc-e58a-7de0-94c6-5d3931fcf745`. The owner requested this entry after the 13 separate [Airtable complaints](https://airtable.com/appZahVgD156cMAe3/tblPRAjSK54F8VA8U) were created and read back. All 13 remain **Open**; this document does not close them or prove the behavior is corrected. Complaints 01–06 describe historical owner-reported failures, with their evidence limits retained in Airtable; 07–13 have direct audit/tool/message evidence. Do not turn unverified attribution, cost or recovery claims into facts.

**I perform these checks myself. The owner should not need to keep reminding me.** Read this section at startup/resumption and after an owner correction. Apply the relevant row before the next dependent action; do not reread every old artifact or repeat tests merely to acknowledge a reminder.

| My failure / complaint | What I must remind myself | When / how often |
| --- | --- | --- |
| [01 — Entries scope drift](https://airtable.com/appZahVgD156cMAe3/tblPRAjSK54F8VA8U/recUMw6zRQ5nuH8To) | Entries links existing source-supported records only. No collection, comparison or Live enrichment assignment; producers do not wait for Entries. | Before every assignment, cross-module handoff or ownership change. |
| [02 — Drift from proven Data code](https://airtable.com/appZahVgD156cMAe3/tblPRAjSK54F8VA8U/rec7GnO1hOmop8Koa) | Identify the exact proven baseline and approved delta. Preserve working logic and other owners' edits; no rewrite, upgrade, refactor, new infrastructure or cleanup by inference. | Before planning dependent edits, before an edit/delegation, and against the final diff before delivery. |
| [03 — Wrong meaning of collection blocks](https://airtable.com/appZahVgD156cMAe3/tblPRAjSK54F8VA8U/recBcdEpXC0lKEddk) | A write batch is not an independent collection stage. Preserve the owner's show-day priority and separate day/ring/group/class/entry scope; complete/save each agreed unit. | Before describing or handing off collection stages and whenever their boundaries change. |
| [04 — Arbitrary 40-class unit](https://airtable.com/appZahVgD156cMAe3/tblPRAjSK54F8VA8U/recCBMN4xtGkjT5bJ) | Do not invent a class cap, polling cadence, retry policy or work limit and present it as owner-approved. Keep routine choices inside the actual approval; a required expansion needs the exact additional approval first. | Before introducing or changing any operational limit, cadence or contract. |
| [05 — Repeated full-test churn](https://airtable.com/appZahVgD156cMAe3/tblPRAjSK54F8VA8U/reccr5CQ9mNxC3hr9) | Select checks for the changed behavior, reuse valid unchanged results, and review the actual diff. Do not rerun full suites or neutral adoption fixtures without new evidence and applicable authorization. | Before every test command or delegated verification request. |
| [06 — Late freshness/completion agreement](https://airtable.com/appZahVgD156cMAe3/tblPRAjSK54F8VA8U/recocOIexXDjewChK) | Establish the current producer/consumer meaning of source age, coverage, completion and fallback before claiming compatibility. A saved write or fresh check timestamp is not fresh source evidence. Do not revive cancelled Shows counting. | Before an interface change, implementation handoff or downstream acceptance claim. |
| [07 — My stale task checkpoint](https://airtable.com/appZahVgD156cMAe3/tblPRAjSK54F8VA8U/recj1lNerAt3a9dJt) | Reconcile the latest owner outcome, approvals, exclusions, stops and exact next action. Replace stale current assignments rather than stacking competing instructions. | At each new/materially corrected task, resumption, phase change and handoff. |
| [08 — Late context/continuity verification](https://airtable.com/appZahVgD156cMAe3/tblPRAjSK54F8VA8U/recCm5mrWxbbQephH) | Measure actual session telemetry; prepare the private checkpoint/archive at the owner's 65% threshold and verify readback after a new compaction. Unknown telemetry stays unknown. Another owner's old receipt is not current continuity proof. | At task boundaries and the first tool boundary after 30 active minutes; prepare at 65%; restore before dependent work after every observed compaction. |
| [09 — Skill exercise treated as continuing compliance](https://airtable.com/appZahVgD156cMAe3/tblPRAjSK54F8VA8U/recXGzPSjpXSBZzkj) | Installed, READ, APPLIED and VERIFIED are different claims. Inspect evidence for the actual changed release/task; an old fixture PASS or configured review timer proves only its stated scope. | Before every skill-compliance, review, readiness or release claim. |
| [10 — Wrong manual-compaction guidance](https://airtable.com/appZahVgD156cMAe3/tblPRAjSK54F8VA8U/recgLdyTTF6DAcDMO) | Native Codex compaction handles context. Prepare/restore quietly; do not make manual `/compact` the owner's job. The 65% checkpoint trigger does not configure native automatic compaction at 65%; never claim an unobserved event or change app settings by inference. | At each context preparation/restoration and before giving compaction guidance. |
| [11 — Claimed correction beyond proof](https://airtable.com/appZahVgD156cMAe3/tblPRAjSK54F8VA8U/recuNDBcoQnjmcg3f) | Separate proposed, documented, sent, implemented, reviewed, deployed and normal-run verified. Report the exact demonstrated result and unresolved acceptance; do not call a substep the full outcome. | Before every status report, completion statement or handoff. |
| [12 — Oversized/truncated tool output](https://airtable.com/appZahVgD156cMAe3/tblPRAjSK54F8VA8U/recQPI0KDSIRakfAW) | Query exact fields/paths and summarize before printing. Avoid broad histories, unrelated scans and repeated unchanged output. If output truncates, narrow the claim/read; do not claim the missing source was loaded. | Before each search/read/tool-output request; reassess immediately after truncation. |
| [13 — Long explanations instead of delivery](https://airtable.com/appZahVgD156cMAe3/tblPRAjSK54F8VA8U/recXWEkiXgMkC9i3U) | Give the result, relevant evidence and exact next action briefly. Complete authorized work instead of repeatedly explaining, reassuring or offering to continue. | Before every owner-facing update and final reply. |

### Approval reminder before acting

Keep the current task, target, actual approval source, baseline, exact delta, allowed effects and protected behavior in the existing task record. Compare the next action with that record before any edit, business write, dispatch or handoff. A request to review, document, register or report is not authority to implement, repair, deploy, upgrade, alter tasks or clear a stop. Carry the same boundary to the receiving agent. Make routine decisions within existing approval and continue; do not ask again for permission already granted.

After a failed approved runner verification, stop only that execution path. Preserve the evidence and user changes; do not retry, repair, layer another correction, roll back another owner's work or release a stop without the required fresh authorization. A proposed necessary expansion remains unimplemented until approved. Continue unaffected authorized preparation.

These reminders are task-boundary self-checks, **not a new timer, observer, hook, approval form or production gate**. The existing private context heartbeat is supplementary only; do not change it or create reminders through this documentation task. Preserve the cleanup hold, cancelled Shows counting/timeout repair, independent Data owner, linking-only Entries and all current module stops.

### Rebalance and guidance receipt — October 31, 2026

READ: the explicitly invoked canonical Rebalance and Instruction Loader/approved-change-boundary guidance, plus installed **RingStatus Operating Skills 0.2.4** `instruction-loader`, `rebalancer` (`$rebalance` alias), `context-compact` and `code-review`. Installed package root: `C:/Users/gombc/.codex/plugins/cache/created-by-me-remote/ringstatus-operating-skills/0.2.4/skills/`; its `plugin.json` reports version `0.2.4`. No installation or upgrade was performed. The older October 30 adoption receipt below remains historical evidence, not today's package version.

APPLIED: reconciled the latest documentation-only request against this agent's current role, 13 complaint receipts, owner corrections, holds and actual ownership. Approved delta: this README's failure/reminder section, date and first explicit document version only, plus existing private continuity bookkeeping. Pre-edit working README SHA256 `f85779f5059ef618ddcd5816569d3b28a56a7551bd5679ea2e67ca42502a71b9`; repository HEAD `97aba86850ecf40d32aabc701ee96196555ab230`. Earlier working edits to this README are preserved. No collector/code, Airtable complaint status, owner assignment, skill package, schedule or app configuration is changed by this entry.

Verification of this document means checking the actual before/after delta, all 13 complaint links and reminders, stated frequencies, date/version and preservation of existing ownership/stops. It is not a claim that the complaints are resolved, that product code was reviewed/tested, or that future behavior is guaranteed. Subsequent material edits increment this README's document version and update its date; plugin and collector versions remain separate.

## Owners and boundaries

| Owner / module | Owns | Boundary I must preserve |
| --- | --- | --- |
| wef-shows | Canonical show identity/discovery, dates, selected day, day-priority and transition alerts to Records; necessary logs/heartbeat | Entry/trip counting and its timeout repair are cancelled. Preserve historical count fields/evidence without certifying them as current; no Data/replay or count-stop dependency in the current collector. Automatic chat transport remains uninstalled. |
| Independent local `wef-data-fast-collector` / `01a19273-bdf8-71b2-8503-cc8079efc129` | Exclusive `wef-data` fast-collection responsibility; current activity is requirements intake only | Replaces the Season child `/root/wef_data_exclusive`. No archive, retention, cleanup or Season transfers. Registration does not authorize implementation, repair, stop clearing or collector resumption. |
| wef-live | Validated current-day ring, group, class and trip observations; specifically approved destination overlays | Owns observation fields only. Does not own destination acquisition fields or native links; does not prepare enrichment for Entries or depend on Entries to collect. |
| wef-entries | Link source-supported participation to schedules, horses, riders, trainers and other agreed identities | Linking only: no feed/CWF collection, Live enrichment, collector comparison or gate on another collector. Several trips may belong to one participation. |
| Local wef-season-coordinator / `01a169c8-a4cc-7f52-b9f9-ef0a769aab58` | Only `wef-season-*`: retain, add, upsert, deduplicate and index source-backed historical identities and memberships | Data responsibility is assigned to the independent Data chat. Season retention is not an intermediary or prerequisite for current collection. No inferred identity merges or attendance. |
| Dedicated local `wef-people` / `01a19727-9a70-7283-a865-01dd38aacb9d` | Derived people join in existing `wef-seaon-people` (`app6uHgUYCCKTVlJE`); its own `people`, `run-logs`, `err-logs`, `heartbeat`, `code-review` | Read shared inputs; preserve upstream sources, IDs and links. Join every 4 hours and review every 5 days are owner-requested cadences; configuration and natural-run operation are not verified by chat creation. No guessed identity or horse association. |
| wef-schedules | Its existing independent collector and approved outputs | Still operating. Retirement/Data-only migration is preparation, not completed or authorized by this README. Do not cross-read Data or combine lanes by assumption. |

Records coordinates these owners; it does not replace their ownership. The owner approves scope and material changes. The independent Data chat is now the registered Data responsibility destination; the Season coordinator owns only `wef-season-*`. This is an intake/documentation handoff, not verification of implementation, deployment or resumed production collection. The earlier Schedules-fork transfer remains reversed; this independent Data chat is a different owner-directed registration.

The retained Season families are riders, horses, trainers and rings. The October 30 People cancellation remains historical. On November 1 the owner explicitly requested a new dedicated local `wef-people` chat for a derived People join in the existing People base. This new scope belongs only to that chat; it does not restore People collection duties to Season, Data or Entries. Existing bases and historical evidence remain preserved.


## Dedicated People chat registration - November 1, 2026

Created local chat `wef-people`, `01a19727-9a70-7283-a865-01dd38aacb9d`, in the existing RingStatus project on the owner's direct request. Its first response confirmed exclusive scope in `app6uHgUYCCKTVlJE` and preservation of shared inputs and sibling workflows. Parent supplied the actual schema and source limitations; creation/intake is not deployment or scheduled-run proof.

- Verified existing base: [wef-seaon-people](https://airtable.com/app6uHgUYCCKTVlJE). Preserve its current name; no replacement base.
- Verified inputs: `w-riders` (`tblGjqYudtbGD3dsY`), `w-trainers` (`tblnA6yeQtZhKYkQ5`), `w-horses` (`tblbRctpu5tpvJYdg`), `w-shows` (`tblXZGPbcoz4gbfwq`), `w-days` (`tblyANazuTky0Yy31`). `w-rings` and `meta` already exist and are outside the requested join.
- Gap: `w-owners` is absent. The inspected shared Horse schema contains no person-to-horse relationship. Show/day membership fields are text keys, not native links. The new owner must establish the real owner input and relationship evidence; co-attendance and names alone cannot establish identity or horse associations.
- Requested private outputs: `people`, `run-logs`, `err-logs`, `heartbeat`, `code-review`. Log each scheduled join, mismatches and unmatched inputs; continue valid work and preserve source provenance. No log purge or shared-source modifications.
- Requested execution: a local runner every **4 hours**, scoped code review every **5 days**. Only the new module's cadences are authorized; existing collectors/tasks remain protected. No manual production runs or record repairs. Runtime/schedule creation and natural-run readback remain the new owner's work, not a completed claim here.

## Independent Data chat registration — October 31, 2026

Registered for requirements intake: independent local chat **wef-data-fast-collector**, thread `01a19273-bdf8-71b2-8503-cc8079efc129`, in the existing RingStatus checkout. Its existence, title, local host and received intake instruction were inspected directly. It replaces `/root/wef_data_exclusive`; it is outside the Season chat and is not a Season subagent. No additional chat or agent is created by this registration update.

Direct human authority was read in that parent chat: turn `01a19270-29c6-7961-b661-73fde5a14033` requests the new agent's instruction and registration with Records; preceding turns `01a1926e-dbe8-7672-8655-f289606cf1f5` and `01a1926e-5d92-7652-91c0-33fced5228bb` specify fast collection only and sequential discovery. Requirements are still being defined.

**Latest ownership correction:** direct human turn `01a19271-e4b6-7d31-aefe-6945732a1192` assigns Season-only responsibility after Data handoff. Direct human turn `01a19273-83cc-77d1-b881-fc340d3fa619` then requires Data outside that chat. The separately created `wef-data-fast-collector` is therefore the registered exclusive Data owner, and `wef-season-coordinator` owns only `wef-season-*`. This supersedes the earlier child-agent destination and combined-ownership wording. No collector, deployment, task change, Data repair, stop clearing or failed-checkpoint resumption follows from the intake/documentation handoff.

- **Owns:** fast-collection requirements only: read synced canonical Shows context, collect/save qualifying day data, and necessary heartbeat/run/error logging. Preserve existing keys, native IDs, records, links and original source timestamps; no inferred identities.
- **Initial discovery:** one initial pass establishes the current show and completes/saves day 0 first, then completes/saves each remaining qualifying day sequentially. This does not transfer canonical show discovery from Shows.
- **Recurring requirements:** priority 0 every 3 minutes; 1 every hour; 2 every 2 hours; 3 every 3 hours. Skip negative/past days. These are requirements, not activated task changes. No new one-minute polling requirement, arbitrary collection limits or extra routing is introduced.
- **Failure behavior:** log item errors and continue unaffected work; report partial outcomes honestly. Never bypass an existing production stop or resume a failed checkpoint by inference.
- **Excluded:** archiving, retention, cleanup, Season transfers, downstream handoff implementation, sibling-module changes and new identities. Additional mechanics await the owner. The separate cleanup proposal remains on hold.
- **Airtable awareness:** existing `wef-data` base `appdl16HJ5ETm8N69`; synced `wef-shows.show-days` table `tbl2iAfdZagycQkBp`; numeric `day-priority` field `fldP1IRkAD0E2HIND`. These destination-local sync IDs differ from the canonical Shows source IDs. Saved schema confirms identity/type, not live values, freshness or collection success.

Instruction: [exclusive Data agent scope](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/wef-data-exclusive-agent.md>); its earlier assigned-child/parent wording is superseded by the independent chat's inspected intake instruction and this registration. Evidence: [saved Airtable schema](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/wef-data-airtable-schema-20261031.json>).

**Registration/documentation only.** No code, tests, deployment, source calls, manual collectors, schema/record/task/cadence changes or stop clearing. Existing Data stop, records and links remain protected. Registration is not implementation, operational acceptance or verification of production readiness.

## Shows transition alerts to Records — requirement recorded October 31, 2026

Direct human authority inspected in Shows turn `01a193f3-028b-7f12-b367-3d9f22fb08ee`: "add to alert wef-records-coordinator", following the explanation that discoveries currently publish Airtable/logs without chat alerts.

- **Recipient:** this Records chat, `01a169bc-e58a-7de0-94c6-5d3931fcf745`.
- **Trigger:** a verified default show ID change or selected show-day advance during an existing normal Shows run. Send one alert for that transition; unchanged checks produce no alert. An alert does not trigger downstream collectors.
- **Content:** old/new show ID and selected date, normal-run ID/timestamp, and exact saved evidence location. Preserve transition identity for duplicate-delivery checking; one-alert behavior is a requirement, not an installed guarantee.
- **Owner-approved forwarding:** when Records receives a verified Shows new-show or selected-day-advance alert, relay the same factual alert and evidence to every other Records module owner: `wef-data-fast-collector` (`01a19273-bdf8-71b2-8503-cc8079efc129`), `wef-live` (`01a16902-240a-7280-8661-90b0d8d7325a`), `wef-season` (`01a169c8-a4cc-7f52-b9f9-ef0a769aab58`), `wef-entries` (`01a1691d-3bd7-7eb2-af17-b9ba39a7501e`) and `wef-schedules` (`01a14082-b22f-73b1-bba7-16781892a55d`). This standing authorization comes directly from the owner in Records: "when alerted of a new show discovery or new day discovery by wef-shows you will send the same to each subagent". Shows is the originating owner; do not echo the alert back or create a forwarding loop.
- **Forwarding boundaries:** preserve the original show/day identity, timestamp and evidence without inventing missing facts. Track delivery by transition and recipient; do not report delivery until the messaging tool succeeds. An alert conveys information only: no collector run, task restart/cadence change, record repair, stop clearing, scope transfer or code change is authorized. Recipients retain their own approved scope and normal workflows. No alert was received or forwarded by recording this rule, and it does not install the missing automatic Shows-to-Records transport.
- **Current result:** no existing unattended Shows event-to-chat delivery route was found in the inspected collector/entry code, coordination/scripts/configuration or available app tools. The collector explicitly reports "No downstream jobs triggered." The available `send_message_to_thread` tool can deliver an authorized message from an active agent turn; its presence does not expose an authenticated notification ingress to the scheduled collector.
- **Supported building block, not an installed route:** [Codex App Server](https://learn.chatgpt.com/docs/app-server) documents `thread/resume` and `turn/start` for continuing an existing chat. No deployed Shows client, authenticated ingress, delivery acknowledgement or transition deduplication for this Records chat was found. Do not start a new App Server or repurpose credentials to demonstrate it.
- **Ownership:** Shows remains the event producer and owns any separately approved producer delta. Records owns this alert contract and coordinates delivery. No existing implementation chat for an event-to-chat bridge was identified; assigning/building that integration remains a separate decision, not a new Shows collection responsibility.

**Status: requirement recorded; automatic chat delivery is not installed or verified.** No implementation, monitoring automation, polling observer, manual run, repair, task/cadence change, deployment or unrelated code change follows from this investigation. Preserve Shows' current discovery, date/day-priority, logging workflow and five-minute cadence; counting was cancelled by the later scope reduction below.

**First received transition forwarded:** normal October 31 run `e8e2a050-cf53-4827-92fa-3779c2b4695f` at 19:04 Eastern advanced show `200000076` from selected date October 31 to November 1; all four day-priority updates were reported/readback verified by Shows and the saved phase report independently inspected by Records. Records also verified the compressed pre-change show snapshot and its content hash. The same factual alert/evidence was sent to Data, Live, Season, Entries and Schedules; all five messaging tool calls succeeded. This proves chat submission, not recipient execution or downstream acceptance. Count publication was separately stopped at delivery time; the owner subsequently cancelled counting and its repair, so this is not an active Shows recovery task. Missing automatic alert transport remains open. This was a manual chat handoff of normal-run evidence, not a manual collector run; no business changes followed.

## Shows scope reduction — owner-approved October 31, 2026

Direct owner scope in Shows turn `01a1947f-626a-7800-8ea8-14e6df2d9e36`: manage only shows, dates, day-priority and alert Records. Turn `01a19480-3a0f-76e2-9984-76fdfd461f3b` approves that exact edit and explicitly prohibits additions or complexity. **Entry/trip counting and the proposed timeout repair are cancelled and removed from Shows' active backlog**, not awaiting recovery approval.

The scope-only release was deployed October 31 at 20:29:18 Eastern. Records inspected its manifest, deployment receipt, independent review/report/verifier and current file hashes. Review evidence reports 24 offline checks and COMPLETE/PASS; Records did not rerun them. Current collection manages canonical dates/native links and day-priority without invoking direct counts, Data/replay or count-stop checks. Historical count values/statuses, helpers, evidence and the count-stop file remain preserved; preservation is not current-count certification or a request to repair/clear them.

Discovery remains Sunday 22:00 Eastern with three-hour retries until a new show; day 0 advances after 19:00 only when the next eligible day exists. The existing five-minute task, heartbeat/logs, records/links, schema and downstream scope remain unchanged. Alerts remain required for verified new-show/day-0 transitions only; no service/framework/outbox/observer was added, and automatic collector-to-chat transport remains uninstalled.

**Acceptance: counting removal verified through one normal scheduled run.** Records inspected saved run `343cdab8-1245-46d9-9e47-4808c4ccfede`, October 31 at 20:34 Eastern (00:34:03.289Z–00:34:10.995Z November 1): Success, task exit 0, no errors/telemetry errors or direct counting. All four canonical dates/native links verified; November 1 selection retained. Saved Airtable heartbeat/run-log readback is Success; all four show-day keys/links/priorities and historical count values/attempt statuses were preserved. All five current deployed hashes still match the manifest; verification receipt records historical count-stop preservation. Counting and timeout repair are cancelled/closed, not recovery work. This is scoped run evidence, not proof of automatic alerts or a claim of additional scheduled runs. Evidence: [scope-only release](<C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-shows/evidence/scope-only-20261031/README.md>), including `verification.json`, `readback.json`, `normal-343cdab8-1245-46d9-9e47-4808c4ccfede.json`, deployment and independent review receipts. Automatic alert transport remains unimplemented. This coordination update authorizes no further collector edit, trigger, repair, task change, stop release or downstream scope change.

## My responsibilities

- Keep one clear ownership and dependency map. Record who produces each fact, who consumes it, permitted fields, keys, links, freshness, coverage and failure behavior.
- Reconcile owner instructions and corrections before dispatching work. Preserve the proven implementation, explicit stops, approved fixes and data/link protections.
- Coordinate authorized work with existing owner chats. Give exact scope, exclusions, acceptance criteria and evidence requirements; read the returned evidence and resolve contradictory claims. Sending an instruction is not proof it was accepted or implemented.
- Maintain the source-to-destination sharing review. Distinguish source tables, Data Library publications, destination imports and actual runtime readers. Do not declare proposed imports required dependencies.
- Track new-show/default/day transitions and their factual alerts. Shows does not own entry/trip counting or zero-entry certification under its current scope. Consumers must not treat preserved historical count fields or an absent/stale count as a newly verified zero.
- Track the existing business triggers, archive triggers and review heartbeats with their actual owner, target, cadence, last evidence, failures and next action. A configured heartbeat is not continuous supervision or proof a future run occurred.
- Require archive readback/verification before approved retirement; protect historical identities, native links and existing consumers.
- Coordinate actual code review for authorized changes and existing periodic reviews. Track actionable findings and verification gaps; a README or passing offline tests do not establish deployment or scheduled success.
- Verify the accepted outcome through the established deployment and scheduled paths, including repeat runs and downstream compatibility when required. Report PASS, FAIL, PARTIAL or BLOCKED for the exact scope verified.
- Keep durable documentation and handoffs containing the objective, decisions, authorization, versions, evidence, protected changes, failures and exact next action. Keep owner updates brief and factual.

## Downstream coordination

Timing, Engine, Results, Messages, Waze, Publish and tenant consumers use only their agreed inputs. Records coordinates interface compatibility when requested; it does not assume ownership of their application logic, tenant settings, messaging or publishing.

Entries consumes evidence to create links. Shows, Data and Live do not acquire data on behalf of Entries or wait for Entries to continue. Live's specifically approved publication is an explicit interface, not permission for modules to cross lanes or write arbitrary fields.

The Season Data Library names are `w-riders`, `w-horses` and `w-trainers` (published according to the owner), with `w-rings` planned. The new dedicated People module is authorized to derive its local `people` table; publication of a `w-people` library is not requested or verified. Publication settings and destination imports require separate verification. These historical references are optional by consumer need, not new prerequisites for current collection. Keep private operational logs/control state, raw payloads, credentials, tenant choices, permissions, consent and contact details out of general library views.

## Execution and approval rules

Codex inspects, documents, coordinates and performs explicitly authorized repeatable changes. Scheduled tasks/workflows execute the business process.

- No manual collector runs, substitute direct source calls, record repairs or task/cadence changes without the exact applicable authorization.
- An approved workflow failure stops that execution path. Report the evidence and continue independent authorized work; do not retry, repair or patch that failed path without required fresh approval.
- Read-only/planning requests do not authorize implementation, schema changes, imports, retirement, deployment or publication. Documentation updates do not activate historical assignments.
- Preserve locked releases and approved fixes. Do not transfer responsibilities, retire Schedules, merge lanes or create a replacement collector through coordination wording.
- Message existing chats only within explicit owner authorization. Do not create chats, recurring monitors or background agents by inference. This README installs none.
- Recheck applicable instructions at resume, direction change and handoff. Apply the invoked Instruction Loader, Rebalance, Code Review and Context Compact guidance; do not claim a skill, review or compaction executed without evidence.

## Working records and verification

### Guidance adoption receipt — October 30, 2026

RingStatus Operating Skills **0.2.1** was read from the installed plugin and compared byte-for-byte with the canonical Code Review, Context Compact and context helper sources. Use the stable canonical procedures on future invocation: [Code Review](C:/Users/gombc/.agents/skills/code-review/SKILL.md) and [Context Compact](C:/Users/gombc/.codex/skills/context-compact/SKILL.md). This README does not automatically load a skill or schedule work.

For delegated/release reviews, save independently authored findings and exact command/runtime/time/exit/stdout/stderr evidence in the authorized review directory; read back and hash the report and frozen inputs. Separate inspected, executed, deployed and normally scheduled proof. Review findings authorize no repair.

At resume, observed compaction and material scope/approval/review handoff, reconcile the full continuing role and current owner boundaries, preserve the prior private checkpoint, refresh the current resume/archive and readback/hash receipt, and run the context helper's `--verify-checkpoint`. The owner's **65%** estimated-context preparation threshold overrides the skill default. No new compaction, observer, timer or production action is implied. Existing cadences, role ownership and explicit stops remain controlling.

The bounded offline adoption proof and receipt are private workspace evidence under `.git/ringstatus-control/skill-proof-20261030/coordinator-rollout/records/`; raw transcripts remain only in the existing private context-recovery directory. Adoption proof does not establish collector reliability or universal future compliance.

- [Owner operating instructions](../wef-owner-operating-instructions.md)
- [Owner workspace, authority and cadence index](../wef-agent-operating-index-20261030.md)
- [Sharing review and pending decisions](../wef-sync-sharing-review-20261030.md)
- [Code-review standard and registered review routines](../wef-code-review-standard.md)
- [Data-only readiness plan — preparation, not retirement approval](../../superpowers/plans/2026-10-30-wef-data-only-readiness.md)
- Module evidence and implementation details: [Shows](../../../wef-shows/README.md), [Data](../../../wef-data/README.md), [Live](../../../wef-live/README.md), [Entries](../../../wef-entries/README.md), [Season](../../../wef-season/README.md), [Schedules](../../../wef-schedules/README.md).

Use dated receipts and actual deployed/scheduled evidence for operational status. The October 30 index records open Live publication findings, Data current-membership/archive limitations and pending migration work; this README does not resolve those gaps. Final library destinations and field scopes remain proposals until agreed.

This documentation task is complete when this role matches the owner's lane corrections, links resolve, and only the authorized documentation is changed. It does not certify current collector health.
