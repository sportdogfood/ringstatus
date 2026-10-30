# Class ownership consolidation — 30 October 2026

Documentation-only handoff. The human's coordinator-chat instruction was inspected directly: “instruct the agents to pull in any evidence and i will archive the chats” (turn `01a18e46-5d34-78e0-ace3-6a58e48de187`). No chat was archived by this owner. No code, Airtable, task, automation, deployment or production action is authorized by this document.

## Ownership and source record

Class owner `01a16f18-25b9-7222-9b53-b64496ec84d9` now owns class condition detection and class input validation alongside class timing. Timing coordinator `01a169bc-0bd6-77d3-b7b4-283a07061e90` coordinates shared policy and Messages/Results contracts. Ring and entry owners retain their own calculations, conditions and validation. Messages owns recipients, consent, templates and delivery; Results owns official results, readiness and rechecks.

Read specialist messages and their cited evidence references from:

| Source chat | Relevant source turns |
| --- | --- |
| state-and-condition-trigger-evaluator-agent `01a16f18-3e13-7551-b6b7-363f79c72cf9` | `01a1891c-6b99-7612-a89d-ea482d4e0052` proposal; `01a1891d-837f-7bd1-8544-392debcd43d5` consolidation corrections; `01a1707e-bf91-7680-9380-e826e2fdb3f2` Results interface; `01a1707f-220d-7193-92d7-f28ee766989a` serialization; `01a188e6-e41a-7051-9c2d-74d5e9c2ea05` retained evidence review; `01a18d6b-9288-78c0-88c6-35eb34252072` historical operational review |
| input-validity-and-answer-freshness-agent `01a16f18-5b86-7133-99a5-b4692ec1313d` | `01a1891c-73ee-7413-8520-1671e112b2b8` proposal; `01a1891d-8714-7db2-9f5c-ec5a21dbf707` consolidated freshness corrections; `01a1896e-0b88-7442-97f0-f9494a36d326` optional Entries correction; `01a188e6-ef51-7212-84aa-a015fa42bf01` retained evidence review; `01a18d1e-fc33-76d1-b94e-acdbef1d969b` current Data+Live reconciliation |

These are source-message findings and planning decisions, not new operational verification. Original chats remain the source record. Recent relevant messages and one older State page were reviewed; this is not a claim to have audited every historical turn.

## Current implementation versus inherited planning

Current class code is `wef-timing/class-timing.js`, `class-adapter.js`, `class-live-source.js` and `class-worker.js`. Current class output is base `appeqxxJyPGS1iqdm`, answers `tblmoBFySYW2TkDHf`; shared runner integration is in `wef-timing/data-runner.js`. Data+Live and today's calculation scope control. Class occurrence keys and source observations must be preserved. The original README's shell-only, shared-answer-table, task-state and retention-review paragraphs are dated historical snapshots, not current deployment authority.

Historical Schedules lanes, Engine destination `appam4opFT4NeL4mn`, SQLite/storage proposals, one-minute/50-second cadence, six-table revision architecture and alternate field envelopes are superseded proposals. Do not install them through this handoff. Entries is optional enrichment, not a prerequisite for current Data+Live class calculation. Old Entries link statistics are not current class coverage.

No rejected five-minute operational monitor is revived. The State specialist's monitor proposal and historical task/receipt observations are retained only as history. Existing bounded class code review is separate; this consolidation does not change its scope or schedule.

## Class condition rules retained for reconciliation

- Compare consecutive valid observations for the exact customer/show/date/ring/group/class/occurrence/phase and policy. First observation and recovery establish a baseline; do not manufacture missed transitions or catch-up events.
- Detection time is not actual start/end. Repeated processing of one source observation is not independent confirmation. Invalid or stale evidence cannot clear an existing condition.
- Keep actual, estimated and unknown distinct. `completion_candidate` is advisory; phase counts, gone status and equal counts do not establish all-phase completion or official Results readiness.
- Source-group membership alone does not prove operational occurrence. Never derive a group from a stage-key or promote class/date links to phase-compatible participation.
- Preserve stable condition identity, evidence references and repeat/restart deduplication. Corrections/retractions reference earlier events; no automatic corrective message is authorized.
- Historical proposed thresholds remain undecided for implementation: start/go crossings at 15/5 minutes; order crossings at 5/1; estimate movement at least 5 minutes confirmed by two distinct observations with 10-minute spacing; event expiry at the earliest of 120 seconds, supporting expiry and condition end. Class owner does not acquire entry-order/conflict ownership through these proposals.
- No standalone condition-evaluation answer kind was retained by the specialist's final consolidation. Its checkpoint/event architecture was planning only; current schema and durable event persistence must be reconciled before implementation.

## Freshness and validity retained

Current class implementation checks original Data evidence under 30 minutes and Live/observed-interval evidence under 10 minutes, rejects future observations, and rejects equality at expiry. These code checks do not prove freshness of any current saved answer. Reprocessing, heartbeat time, changed-at and collection processing time cannot renew source age.

The specialist's later planning correction supersedes its earlier 60-second future-skew tolerance and inclusive expiry: reject future source times and require `now < valid-until`, including at consumption. Current answers carry evidence expiry in `input_refs`; this handoff does not invent a top-level valid-until field or claim downstream serving-time checks exist.

Preserve exact producer/base/table/key, original observation, revision/hash and consumed values. Missing is not empty; accepted zero coverage is scoped and as-of. Partial eligibility is per required dependency; unknown is never zero. Historical retention does not establish current membership, withdrawal or completion. Failed/unfinished applicable attempts affect only supported matching scopes; do not introduce a global source-failure gate from old proposals.

Remaining shared-policy work: current source-clock verification, consistent dependency expiry at consumers, treatment of exact applicable attempts in the current Data contract, phase identity/coverage, and acceptance of currentness semantics with Timing/Records. Historical missing schedule-clock concerns apply only to consumers that actually require that evidence; they are not a new Schedules dependency.

## Results and Messages handoffs — planning only

State's proposed Results interface used `condition-events`, `event-type=result-recheck-requested`, and `hint-id=event-id`. It proposed one unresolved hint per scope/reason. Accepted/deferred remained open; rejected/resolved were terminal, but a new hint still required new qualifying evidence. Missing/ambiguous acknowledgment did not permit replacement. Expiry bounded initial acceptance; Results owned subsequent rechecks and next-due-at. Completion/gone was never official readiness.

The historical rename from producer-revision-id to producer-revision-key was explicitly unresolved. Committed-manifest/hash/expiry checks, exact serialization and acknowledgment storage must be reconciled to current contracts; no event table or Results integration is claimed deployed. The proposed Results currentness threshold is historical planning, not a newly approved service policy.

Messages acceptance remained pending in the inspected specialist record. No verified SMS/email delivery route or condition delivery proof was supplied. Class detects supported conditions; Timing coordinates handoff; Messages owns all sending. No new permission to message users or other services is implied.

## Evidence paths retained

All paths below are relative to `C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus`; existence was checked during consolidation, contents were not re-audited:

- `wef-entries/evidence/data-input-switch-20261029/verification-1.json`
- `wef-entries/evidence/data-input-switch-20261029/verification-2.json`
- `wef-entries/evidence/data-input-switch-20261029/business-after-2.json`
- `wef-entries/evidence/shared-schedules-20261029/status.md`
- `wef-entries/evidence/shared-schedules-20261029/verification-2.json`
- `docs/coordination/wef-data-boundaries-working.md`
- `docs/coordination/wef-records-owner-acknowledgments-20261023.md`
- `docs/coordination/wef-entry-relationship-map-20261024.md`

Specialists reported two Entries runs consuming retained revision `adaptive-2026-10-29T14:11:18.894144+00:00`, with 117 accepted rows across 160 scopes and 127 stored rows. The later shared-schedules evidence superseded the earlier unresolved-group snapshot: 113 ordinary links, one conflict, 103 source-group memberships, 11 unresolved groups and 13 historical records. These are dated specialist-reviewed reports, not new live verification, fresh Timing eligibility or operational group proof.

Operational references cited by State: `wef-data/attempt-log.js`, `wef-data/entry.js`, `wef-data/runtime/last-run.json`, `wef-data/runtime/archive-deferred.json`, `wef-timing/runtime/data/last-run.json`, and `wef-data/runtime/handoff/head-15-200000076.json`. Runtime files are mutable; source-chat run/timestamp observations must not be treated as present state.

## Stops, gaps and next action

Condition/event implementation, downstream acceptance and restart/expiry proof remain unverified. Absorption changes ownership only. No authorization to implement policies, create schemas, activate monitors, retry stopped work or deploy is introduced.

Preserve this chat's separate review stops: entry stage has the unreachable whole-class gate, filtered occurrence ambiguity and insufficient saved-class verification findings; retention-reconciled candidate has eight failed tests and no stale-input-independent archive path. Exact stages are `C:/Users/gombc/Documents/Codex/staging/wef-timing-error-continuation-20261030` and `C:/Users/gombc/Documents/Codex/staging/wef-timing-retention-reconciled-20261030`; entry source is `wef-entry-calculator`. Later owner-approved corrections must be reviewed against their exact versions; this document is not a live status recheck of concurrent work.

Next class-owned work is coordinator-scoped reconciliation of condition policy and evidence requirements with current class outputs, then separately authorized implementation/verification if requested. Timing coordinates Messages/Results and shared runtime. The human retains chat archival responsibility.
