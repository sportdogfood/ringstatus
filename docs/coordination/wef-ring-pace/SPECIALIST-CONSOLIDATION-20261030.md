# Ring conditions and input validation — specialist consolidation

October 30, 2026. Documentation consolidation only. Ring-pace now owns ring conditions and input validation; Timing coordinates shared policy and Records/Messages handoffs. This does not activate an evaluator, notification service, schema, monitor or runtime change. The owner will archive chats personally.

## Provenance and precedence

Source chats, read through their older-history cursors to the end:

- `state-and-condition-trigger-evaluator-agent`: `01a16f18-3e13-7551-b6b7-363f79c72cf9`.
- `input-validity-and-answer-freshness-agent`: `01a16f18-5b86-7133-99a5-b4692ec1313d`.
- Receiving owner: `wef-ring-pace`, `01a16f18-1cfe-7663-85a5-71dc2bbe3576`.
- Coordinator: `wef-timing-coordinator`, `01a169bc-0bd6-77d3-b7b4-283a07061e90`.

History was used for decisions, corrections, approval limits and evidence pointers; it is not fresh operational verification. Tool previews can truncate long earlier messages. This is a curated handoff, not a raw chat export or a claim that historical schema proposals exist.

Current controlling context is Data + Live, today's New York scope, existing separate ring/class destinations, and the approved Live-first overlapping-field policy in [the common contract](../wef-freshness-completion-contract-20261030.md). Old Schedules/Engine lanes, one-minute/50-second runner proposals, future-day calculation proposals and SQLite plans are historical only. Data acquisition's separately planned future-day work is not permission to expand Timing calculations.

## Retained input-validation rules

- Validate customer/show/date/entity, exact occurrence/phase, source identity, coverage and hashes before using a value. Preserve `stage-key`, participation `entry-key=show|date|entry|class` and `live-entry-key=show|date|class|trip`; native links are destination-local. Names and inferred singleton membership cannot replace required operational occurrence evidence.
- Original observations establish age. Changed-at, processing completion, publication, replay, new evaluation or successful heartbeat do not renew it. A newly observed unchanged value may renew evidence age but does not prove new sporting progress.
- Distinguish absent, stale, failed, unresolved, conflicting, explicit valid-empty, partial and historical. Only an identified successful complete zero-row observation supports empty. Missing evidence is never zero, withdrawal or completion.
- Partial producer outcomes permit independently validated scopes; an unrelated global failure is not a blanket veto. A newer unfinished exact applicable attempt remains material to that scope. Unknown/global attempts are audit evidence, not invented applicable failures.
- Preserve lifecycle separately from quality: active/completed is not fresh/stale. Verified completed-group status and counts remain historical as-of evidence. They do not establish final class/trip detail, actual finish time, complete scheduled coverage or fresh active pace. Retain contradictory counts and status without inventing reconciliation; allow later verified corrections/reopening.
- For approved overlapping components, applicable verified active Live wins; Data is a qualified fallback when usable Live is unavailable. Identity conflicts cannot silently become trusted fallback. Non-overlapping Data authority, native links and acquisition ownership remain unchanged. Data age must not alone invalidate independently usable Live values, but current exact membership/identity still needs proof. This desired correction is not claimed deployed by this document.
- Keep main/JO/R2 counts and group/class/trip meanings distinct. No actual start/end inferred from first trip, group completion or estimated time. Sentinel/unknown values remain unknown. Class fallback pace remains labeled as estimated, not observed.
- Entries is enrichment, not a prerequisite for otherwise supported ring/class answers. Source-group membership with class-date schedule association is weaker than an observed operational group occurrence. `source-trainer-ids` is not access authorization.

Freshness conflict resolution: validity turn `01a1891d-8714-7db2-9f5c-ec5a21dbf707` explicitly superseded the earlier future-skew allowance and expiry equality. Current inspected Timing code rejects future observations and expires at equality: Shows/Data under 30 minutes, Live under 10 minutes. These are current implementation limits, not proven delivery SLAs or authority to change numeric policies. Serving must independently reject expired evidence; immutable saved snapshots are not serving leases. The separately approved Shows five-minute zero safeguard is not replaced by the Timing limits.

## Ring condition semantics retained for design

These are proposed condition contracts, not proof of an implemented event writer:

- Compare consecutive valid evidence within identical dated subject, phase and policy. First valid observation, stale recovery or a policy change establishes a baseline; do not fabricate a transition or catch up missed alerts.
- Repeated evaluation of the same source observation does not confirm an estimate movement. Compare target timestamps, not merely countdown passage. Unknown/stale evidence cannot clear a conflict or incident.
- Preserve stable condition/occurrence/policy identity and predecessor state. Replays preserve event identity; delivery failure never creates a new condition occurrence. Append corrections/retractions rather than rewriting committed events.
- Conditions consume owner-validated class/entry/conflict answers; they do not independently recompute those owners' formulas. No same-pass circular feedback into ring pace. A blocked Results branch does not block unrelated supported ring work.
- Detection time is not actual sporting start/end. Completion/gone can support a recheck hint only under the agreed interface; official Results readiness remains Results-owned.

Unapproved historical numeric proposals retained for an explicit future decision: 15/5-minute downward milestones; 5/1 positions ahead; estimate movement at least 5 minutes sustained over two distinct observations; 10-minute estimate-event cooldown; event expiry at the earliest of detection +120 seconds, supporting expiry and condition end. No current activation or acceptance of these numbers is inferred.

## Publication, persistence and handoff decisions

State turn `01a1891d-837f-7bd1-8544-392debcd43d5` withdrew the separate `condition-evaluation` answer kind. It proposed condition state in `engine-checkpoints` and events in `condition-events`; complete new state must be recoverable from verified immutable publication before a mutable checkpoint/cache advances. Runner-health state must not participate in condition comparison.

Validity turn `01a1891d-8714-7db2-9f5c-ec5a21dbf707` proposed validity evidence in shared inputs/revision coverage, not a separate validity answer/table. Historical `evaluation-revisions`, `evaluation-inputs`, `evaluation-heads` and `condition-events` proposals are not newly authorized schemas or assumed current ring-base tables.

Common design safeguards: exact unique manifest members and logical keys; versioned normalized fields/hashes rather than arbitrary Airtable response hashes; timestamps normalized UTC, IDs preserved, absent/null distinctions explicit; exact readback before commit and head advancement. Consumers verify accepted head, committed revision, manifest membership/provenance/expiry and reread the head. Committed does not automatically mean accepted or currently usable. A head/commit request timeout is unknown, not safely abandoned. Preserve exact uncertain identity for reconciliation; conflicting duplicates block. Airtable transactions, uniqueness or distributed exactly-once claims were never established.

These proposed revision/head protocols do not replace the deployed snapshot and pending-write safety path under documentation authority. Do not automatically reconcile or resume a currently owner-blocked pending intent based on an old proposal.

Messages owns recipients, access/consent, templates, delivery dedupe, retries and provider results. Durable acceptance is not delivery. No phone numbers, tokens, OTPs or private recipient data belong in condition evidence. Early Messages acceptance concerned a boundary/SQLite proposal; SQLite was later withdrawn. The later Airtable receipt schema remained **Messages acceptance pending**, with failed coordination attempts recorded. No successful send or current receipt implementation is inferred. Coordinator owns renewed reconciliation, not automatic repeated messaging.

Results hint proposal: `condition-events`, type `result-recheck-requested`, hint identity equal to event identity; no standalone `result_hints` or mirrored acknowledgment store. Results alone writes acknowledgments. One unresolved hint per scope/reason; accepted/deferred remain open, rejected/resolved terminal; new evidence is still needed before a new occurrence. Expiry/replay must not reset Results' accepted recheck lifecycle. Older `producer-revision-id` versus later `producer-revision-key` needs explicit cross-owner resolution before implementation. Results acceptance/finality must be verified separately.

SQLite is withdrawn. No new local durable evaluator store or index is introduced. Optional derived `ringstatus-data` indexing was only a historical proposal, never consent/freshness/checkpoint authority. Existing approved immutable archive files are not revoked by this documentation consolidation.

## Evidence map and its limits

Paths below are repository-relative to `C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus` unless absolute. The nine principal history/evidence paths were confirmed present during consolidation; saved proof is dated, not current production proof.

| Evidence | What it preserves |
| --- | --- |
| `wef-entries/evidence/data-input-switch-20261029/verification-2.json`, `normal-run-1.json`, `normal-run-2.json`, `business-after-2.json` | Historical ingestion/repeat identity proof. Specialist reports: 117 accepted rows/160 scopes, 127 stored rows; same retained revision, no source-time renewal. Does not establish current timing eligibility. |
| `wef-entries/evidence/shared-schedules-20261029/status.md`, `verification-2.json` | Later saved release supersedes the earlier all-unresolved link assessment: 113 ordinary links, 1 Conflict, 103 source memberships, 11 unresolved groups, 13 historical records; runs `5ca0b0c7-fca9-44f3-9f74-143a472a7da1` and `0b494dd2-560b-499b-9360-ef36f3006369`. Source membership is not operational occurrence. Status document read in consolidation; no fresh collector execution. |
| `docs/coordination/wef-data-boundaries-working.md`, `wef-records-owner-acknowledgments-20261023.md`, `wef-entry-relationship-map-20261024.md` | Historical source boundaries/owner agreements; subordinate to current Data+Live/common contract. |
| `wef-entries/freshness.js`, `collector.js`, `participation-linker.js`, `data-input.js`; `wef-live/participation.js`, `evidence/participation-link-release.md` | Source-chat provenance and relationship validation pointers. Their current production behavior is not recertified here. |
| `wef-timing/pace.js`, `data-runner.js`, `class-adapter.js`, `class-live-source.js`; `runtime/data/last-run.json`, `runner.log` | Current implementation/receipt inspection points; mutable runtime paths require fresh reads before status claims. |
| `wef-data/attempt-log.js`, `entry.js`; `runtime/last-run.json`, `stopped.json`, `archive-deferred.json`, `handoff/head-15-200000076.json` | Source-chat operational diagnosis pointers, not proof these optional files/stops still exist. Data errors table was identified as `err-logs`, not guessed `errs-log`. |
| `C:/Users/gombc/Documents/Codex/staging/wef-timing-error-continuation-20261030/SCHEDULED-VERIFICATION.md` | This ring chat's later two-run snapshot/readback evidence; supersedes older all-days/same-base runner descriptions, not source-freshness proof. |
| `C:/Users/gombc/Documents/Codex/staging/wef-timing-retention-reconciled-20261030/REVIEW-HANDOFF.md`, `offline-tests.txt`, `candidate-hashes.json` | Current retained reconciliation stop: 151/159 offline passes, eight failures; no deployment. Separate fresh approval required before edits/rerun. |

Legacy reviews remain historical regression lessons only: validity turn `01a16f1d-3c46-7682-88fd-f0a064221192` referenced commit `41849ac266deba8abcd119048487791e9b17bfec`; state turn `01a16f1d-3728-78e2-b75b-1e54ceb3b3f5` reviewed `schedules_calculatorv2.js`, `trips_calculator.js` and `lib/airtable/rings.js`. Preserve the lessons (exact dated joins; freshness rather than recency; valid counts/phase/time; no capped unsorted history as authoritative predecessor; unavailable history is not first observation; durable occurrences; countdown passage is not estimate movement). They are static findings, not confirmed current incidents or authorization to repair legacy code.

## Explicit stops, pending decisions and acceptance

1. Both specialists' original authorization was planning/read-only. Their contracts and planning acceptances were not implementation, schema, deployment, collection or sending approval. This consolidation authorizes documentation only.
2. Do not revive the rejected five-minute operational monitor from state turn `01a18d6b-9288-78c0-88c6-35eb34252072`. Its useful incident semantics remain design evidence: notify only on meaningful transition/recovery, task activity is not business health, missing inputs do not clear a failure. No monitor was created or changed here. Separately authorized existing review schedules are not changed.
3. Retention's failed-verification stop persists; do not edit/rerun/deploy that candidate or bypass archive/catalog/pending-intent protection. Shared stale-source remediation and documentation may proceed only within their own authorization. No task changes, record repairs or manual runs.
4. Confirm exact condition policies, event persistence/destination and Messages/Results consumption contracts before activation. Reconcile key naming, correction/expiry handling, access source and durable dedupe. No new schema is presumed.
5. Numeric action-specific freshness remains an owner decision tied to measured delivery and risk. Current code limits, proposed service targets and the five-minute Shows zero safeguard are distinct. Preserve source clocks and measured delays; no optimistic freshness renewal.
6. Prove usable exact inputs/operational membership and completed-group versus final class/trip semantics. Neither a successful runner nor historical link evidence closes these gaps.
7. When implemented under separate approval: independently test first/recovered baselines, same observation replay, source/policy changes, threshold crossings, stale conflict retention, explicit empty, exact applicable-attempt isolation, phase/occurrence mismatch, duplicate/uncertain writes, expiry at serving, correction/retraction and repeat recovery. Normal scheduled evidence and downstream acceptance remain separate from offline tests.

No source chats were archived. Source IDs, turn IDs, dated evidence and unresolved decisions remain available to the coordinator and owner.
