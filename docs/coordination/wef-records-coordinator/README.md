# wef-records-coordinator

README version **1.19.0** | Updated **November 9, 2026** | America/New_York

## Current status

Records coordinates five active WEF specialists. The standing operational outbound route is limited to the approved Data cleanup alert. The November 7 owner stop applies to unrelated sending. The former Shows observer and timed all-specialist review were retired.

- **Latest verified log check.** November 9 at 10 a.m. Eastern, show `200000078`, Data run `1794236869626`. All four logging tables were below 1,000 rows. Airtable and local deletion counts were zero. [Run evidence](../../../wef-data/evidence/cleanup-keep500-20261108/verification-1000-20261109/actual-result.json)
- **Latest verified show change.** Data run `1794215050051` completed cleanup for the transition from `200000077` to `200000078` with 9,128 Airtable deletions and 423 local log deletions. [Run evidence](../../../wef-data/evidence/cleanup-keep500-20261108/verification-new-show-20261109/actual-result.json)

### Start here

1. Check current status, [ownership](#purpose-and-ownership), [Project global instructions](#project-global-instructions) and the latest direct owner instructions.
2. Use [Inputs and outputs](#inputs-and-outputs) to locate the authoritative source and [Handoffs](#handoffs) to identify who acts next.
3. When continuing work, use [Resume here](#resume-here) to recover the last verified action and exact next step.

### Known troublespots

- **New-show collection overlapped cleanup.** Rings, entries and trips were empty at the November 9 cleanup readback. Collection coverage was unverified at that point. [Overlap evidence](../../../wef-data/evidence/cleanup-keep500-20261108/verification-new-show-20261109/actual-result.json)
- **Keep-500 proof.** The policy and under-threshold behavior have verification. A genuine above-threshold trim to 500 remains unverified. [Data cleanup policy](../../../wef-data/README.md#current-cleanup-policy---november-8-1007-am-eastern)
- **Proof boundaries.** Record delivery, acknowledgment and task completion separately. A missing or stale count does not establish zero entries.

<details>
<summary>Records complaint reminders and when to apply them</summary>

Use the applicable row at startup, resumption or after an owner correction. The individual complaints carry their evidence and disposition. [Complaint register](https://airtable.com/appZahVgD156cMAe3/tblPRAjSK54F8VA8U)

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

</details>

## Purpose and ownership

Records maintains ownership, approved boundaries, interfaces, authorized handoffs and scheduled acceptance evidence. Each specialist owns its implementation and existing jobs. Timing, Engine, Results, Messages, Waze, Publish and tenant consumers own their application logic.

### Related subagents and ChatGPT chats

Records coordinator chat ID is `01a169bc-e58a-7de0-94c6-5d3931fcf745`. These are existing specialist chats.

| Specialist chat | Local chat ID | Responsibility and boundary |
| --- | --- | --- |
| **wef-shows** | `01a16976-f0a6-72a1-a75f-75de7f19a815` | Canonical show discovery/identity, dates, selected day, day-priority and verified transition notices. Counting and its repair are cancelled; historical count fields do not certify current counts or zero entries. |
| **wef-live** | `01a16902-240a-7280-8661-90b0d8d7325a` | Live ring/group/class/trip observations, specifically approved publication and its own approved cleanup. Preserve its current publication and retention holds. Data owns its separate collection. |
| **wef-data-fast-collector** | `01a19273-bdf8-71b2-8503-cc8079efc129` | Exclusive Data acquisition, independent Data linking and the separately approved event-only cleanup task. |
| **wef-season** | `01a169c8-a4cc-7f52-b9f9-ef0a769aab58` | Retain, add, upsert, deduplicate and index `wef-season-*` horses, riders, trainers and rings, preserving supported history/memberships. Data owns its separate acquisition. |
| **wef-decoder** | `01a1a753-dce7-7951-a147-1b096e1538ba` | Owns the shared `wef-schedules-decoded` proxy, health monitoring and credential updates. Proxy consumers retain their existing collector responsibilities. |

#### Deprecated chats

The owner deprecated Entries and People on November 9. Schedules was already deprecated. These chats are excluded from current assignments and recipients. Their existing workflow history remains available through their operating records.

| Chat | Historical chat ID |
| --- | --- |
| [Entries](../../../wef-entries/README.md) | `01a1691d-3bd7-7eb2-af17-b9ba39a7501e` |
| [People](../../../wef-people/README.md) | `01a19727-9a70-7283-a865-01dd38aacb9d` |
| Schedules | `01a14082-b22f-73b1-bba7-16781892a55d` |

Specialist operating references: [Shows](../../../wef-shows/README.md), [Live](../../../wef-live/README.md), [Data](../../../wef-data/README.md), [Season](../../../wef-season/README.md), [Decoder](../../../workers/wef-schedules-decoded/README.md).

## Inputs and outputs

Records reads direct owner instructions, existing operational evidence and genuine scheduled wakes. The cleanup handoff reads Data's saved cleanup state and the current canonical default show. Records publishes the existing cleanup pointer, keeps delivery and result receipts, and reports verified outcomes.

### Airtable

| Interface | Authority and use |
| --- | --- |
| `w-shows` | Resolve the existing binding in [Data's schema](../../../wef-data/schema.json). Read exactly one canonical default and preserve its original source timestamp. |
| `w-days` | Data's show-day input. A planning day change alone creates no cleanup event. Planning day 0 and the Eastern calendar date are distinct. |
| `cleanupV2` | Data owns cleanup selection and outcome rows. Reference tables and historical cleanup records stay protected. |
| Agent complaints | The [Airtable complaint register](https://airtable.com/appZahVgD156cMAe3/tblPRAjSK54F8VA8U) records evidence and disposition. |

In a sharing review, distinguish the source, Data Library publication, destination import and actual runtime reader. [Sharing review](../wef-sync-sharing-review-20261030.md)

### Integrations and add-ons

- **Airtable and Data Library.** Specialists own their published tables and destination imports. Use the source and runtime-reader checks above when assessing a shared interface.
- **Codex chat handoff.** Records delivers an eligible cleanup alert to the existing Data chat. Windows Task Scheduler executes Data's registered task.
- **Shared Decoder.** Decoder owns runner-first proxy routing, the independent fallback and credential renewal. Consumers retain their own operating rules. [Decoder operating reference](../../../workers/wef-schedules-decoded/README.md)
- **RingStatus Operating Skills.** See [Skills used](#skills-used) for the procedures Records uses in this workspace.

## How it works

### Resume here

- Reconcile the latest owner objective, approvals, stops and exact next action against the private `resume.md`, `state.json` and relevant saved evidence for this chat.
- Check actual context telemetry at task boundaries and the first tool boundary after 30 active minutes. At 65% estimated context, prepare and verify a private checkpoint and archive. After observed compaction, reconcile and verify continuity before dependent work. [Context procedure](../context-check-procedure.md)
- Keep approved, prepared, reviewed, deployed, delivered and verified outcomes separate. Preserve the proven baseline and the exact evidence for any failed path.

### Collection cadence and fallback contract

Records checks cadence and fallback evidence for the specialist that owns the workflow. Its own scheduled wake evaluates cleanup alerts.

| Owner | Collection cadence and fallback reference |
| --- | --- |
| Shows | The existing five-minute wake supplies validation, heartbeat and due discovery. Browser-first discovery uses the approved proxy fallback when the browser path is unusable. [Shows operating reference](../../../wef-shows/README.md) |
| Data | The documented day-0 policy is three minutes when the selected day is today and hourly otherwise. Priorities 1–3 run in order on the documented 30-minute cycle. Decoder owns the approved schedule/class fallback, which stops at the first usable response. [Data operating reference](../../../wef-data/README.md) · [Decoder fallback](../../../workers/wef-schedules-decoded/README.md) |
| Live | Use Live's own active lane, cadence and source boundaries. Its documented day-0 name fallback applies to eligible empty trip links with a blank source ID and a unique name match. [Live operating reference](../../../wef-live/README.md) |
| Season | The existing hourly job owns Season retention and supported associations. Use Season's own workflow and conflict handling for acceptance. [Season operating reference](../../../wef-season/README.md) |

### Cleanup contract

The existing Records reminder checks at 1, 4, 7 and 10 a.m. and p.m. Eastern, with a two-minute check limit. On a genuine wake, read `wef-data/runtime/cleanup/cleanup-state.json` and the existing Records notification ledger. Use the deployed pure `alertFor` selector with the single canonical default and actual time. The selector prioritizes a changed show, then the daily clear at or after 7 p.m. if due, then an eligible log-check slot. [Deployed selector](../../../wef-data/cleanup-lane.js)

Data owns eligible tables, record protections and retention. At 1,000 or more rows in a checked logging table, delete oldest eligible records until 500 total remain, subject to protection of active, current and pending records. The former readiness manifest, schema projection and cleanup stop-release checks are retired. [Data cleanup policy](../../../wef-data/README.md#current-cleanup-policy---november-8-1007-am-eastern)

### Linker contract

Data and Live own their independent linkers. Season owns its supported trip-based associations. Each owner defines the eligible input, target links, match evidence, preservation rules and conflict handling in its own operating reference. Records verifies the authorized handoff and the existing job's evidence. Entries and People remain historical references under [Deprecated chats](#deprecated-chats).

## Handoffs

1. **Records to Data.** Save the selector's exact `event_id`, `trigger`, `show_id`, `calendar_day` and `issued_at` in `.git/ringstatus-control/wef-records-shows-transition-alert-state.json`. Save the Requested receipt before sending one factual alert to the existing Data chat. Deduplicate by event ID and preserve history and original clocks.
2. **Data to Records.** Data alone starts its existing triggerless cleanup task. Record delivery, acknowledgment and completion separately. Verify the saved result, native readback, `cleanupV2` and local counts. Retain the exact failure reason and evaluate later genuine events on their own evidence.
3. **Other specialist coordination.** Use the owner-approved recipient and scope for the specific request. Retain source clocks and facts from Shows notices. Verify adoption through the specialist's existing workflow. The standing stop on unrelated sending remains in effect.

## Future

- Verify keep-500 deletion behavior when an existing log check naturally reaches the threshold.
- Verify new-show collection coverage against its own evidence before reporting completeness.
- The shared README format is a proposal for the specialist documents. Each role retains its own rules and evidence.

- **After the owner locks the Records README**, send this documentation instruction to the existing Shows chat:

  > Simplify README around these main categories. Keep browser discovery and show-day priorities easy to find. Move superseded material into history. Documentation only.

## Development

### Project global instructions

Read the repository [AGENTS.md](../../../AGENTS.md), the [local Codex global instructions](<C:/Users/gombc/.codex/AGENTS.md>) and any applicable instructions closer to the files in scope. Reconcile them with the latest direct owner decision before acting. The project instructions require preserving proven implementations, keeping approved changes narrow and checking context at the stated boundaries. The global instructions govern owner handoffs, approval and delivery, and session resumption.

The brief sits alongside the global `AGENTS.md`. It summarizes the standing rules and organizes 32 checked source links by task, covering approvals, runner boundaries, verification, handoffs, Webflow and skill maintenance. [Global Instructions Brief](</C:/Users/gombc/.codex/GLOBAL-INSTRUCTIONS-BRIEF.md>)

### Skills used

- [Context Compact](<C:/Users/gombc/.codex/skills/context-compact/SKILL.md>) prepares and verifies private continuity checkpoints around observed compaction.
- [Rebalance](<C:/Users/gombc/.agents/skills/rebalance/SKILL.md>) reconciles scope, owner decisions, stops and the next executable action.
- [Code Review](../wef-code-review-standard.md) governs applicable implementation reviews and their evidence.

### Script files and locations

| Workspace location | Role |
| --- | --- |
| `C:/Users/gombc/.codex/skills/context-compact/scripts/inspect_context.py` | Records continuity telemetry, private archive and checkpoint verification. |
| [wef-data/cleanup-hidden.vbs](../../../wef-data/cleanup-hidden.vbs) → [wef-data/cleanup-run.js](../../../wef-data/cleanup-run.js) → [wef-data/cleanup-lane.js](../../../wef-data/cleanup-lane.js) | Data's existing cleanup task chain. Records uses the pure selector exported by the lane. |
| [wef-data/fast_collector.py](../../../wef-data/fast_collector.py) and [wef-data/linking-scheduled.py](../../../wef-data/linking-scheduled.py) | Data collection and scheduled linker references. |
| [wef-shows/collector.js](../../../wef-shows/collector.js), [wef-live/collector.js](../../../wef-live/collector.js) and [wef-live/linker.js](../../../wef-live/linker.js) | Shows and Live collection, and Live linking. |
| [wef-season/season.py](../../../wef-season/season.py) and [workers/wef-schedules-decoded/worker.js](../../../workers/wef-schedules-decoded/worker.js) | Season workflow and shared Decoder implementation. |

### Review and verification

Establish the exact approved change and protected behavior before work. For applicable implementation reviews, retain the reviewer, reviewed version or hashes, findings and verification evidence. Keep independent reviews distinct from author reviews and verify the established scheduled path where required. [Owner instructions](../wef-owner-operating-instructions.md) · [Operating index](../wef-agent-operating-index-20261030.md) · [Code review standard](../wef-code-review-standard.md)

The [Daily Management Plan](DAILY-MANAGEMENT-PLAN.md) holds the planning record. Latest direct owner decisions and deployed evidence control over dated snapshots. The previous README, including its historical approvals, run receipts and superseded procedures, remains in Git at `73699ab4:docs/coordination/wef-records-coordinator/README.md`.
