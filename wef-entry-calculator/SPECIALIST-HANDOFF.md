# Entry specialist consolidation — 30 October 2026

Documentation/evidence consolidation only, requested by Timing coordinator. Entry now owns horse/rider timing conflicts, entry conditions and entry validation in addition to go-time/order. Timing coordinates shared policy and downstream handoffs. This transfer does not authorize implementation, change collector ownership or prove any specialist feature works. The owner will archive chats personally; none was archived here.

## Sources retained

Selected final decisions and exact turn IDs are preserved in [specialist evidence](evidence/specialist-handoffs-20261030.json). Recent history plus the relevant earlier State contract was read; complete lifetime histories were not exhaustively reviewed.

| Source chat | Exact chat ID | Relevant decision turns |
| --- | --- | --- |
| timing conflict-calculator agent | 01a16f18-4d50-7861-92ef-a70628886d3b | 01a1891c-71e7-7843-96bf-1987841ebc80; 01a18d1e-f86d-7530-a3d8-5f5970cdadb8 |
| state-and-condition-trigger-evaluator-agent | 01a16f18-3e13-7551-b6b7-363f79c72cf9 | 01a1891c-6b99-7612-a89d-ea482d4e0052; corrections 01a1891d-837f-7bd1-8544-392debcd43d5; monitor proposal 01a18d6b-9288-78c0-88c6-35eb34252072 |
| input-validity-and-answer-freshness-agent, | 01a16f18-5b86-7133-99a5-b4692ec1313d | consolidated validity 01a1891d-8714-7db2-9f5c-ec5a21dbf707; optional Entries 01a1896e-0b88-7442-97f0-f9494a36d326; current-source reconciliation 01a18d1e-fc33-76d1-b94e-acdbef1d969b |

## Current boundaries take precedence

Current entry destination is `appyoLTcNmLrgpHcg`, with exact created tables/fields in [schema.json](schema.json). Relevant TODAY in America/New_York, established three-minute shared Data + Live pass, existing 160-second worker budget, dedicated ring/class/entry destinations. No Schedules lane, SQLite, legacy changes, caps, manual runs or parallel runner. Historical Engine destination `appam4opFT4NeL4mn`, dual-lane proposals, one-minute/50-second schedules, 40/45-second cutoffs, revision/head/event table designs and old retention recommendations are NOT activated or adopted here. Current coordinator README contains older ownership/destination/status text; the later verified owner authorization and this explicit transfer control entry scope.

Entry code remains stopped after offline verification: **11 PASS / 1 FAIL** (protected-meta rejection message expectation). Fresh **APPROVED TO EDIT** remains required before code/test correction or rerun. Whole-class gate correction and all other unfinished work remain listed in [README](README.md). Specialist absorption does not clear that stop.

## Conflict rules to preserve

- Compare every eligible pair of distinct dated participations sharing an exact horse ID or rider ID; evaluate horse and rider separately. Preserve source/customer/show/date and audience scope. Never use names, trainer association or a hash as authorization.
- Require validated start AND finish bounds for each required occupancy window, with phase/trip/occurrence provenance, uncertainty and dependency expiry. A predicted go time, average pace or class finish is not an established entry finish bound. Current entry output supplies no validated occupancy-window contract; conflict findings therefore remain unavailable/unresolved.
- Determinate half-open windows overlap by `max(0, min(endA,endB)-max(startA,startB))/60000`. Touching endpoints have zero temporal overlap; that does not establish feasible travel or preparation.
- For uncertain windows preserve earliest/latest start and finish bounds. Possible occupancy envelopes support possible-overlap; guaranteed occupancy intersections support overlap throughout the supplied bounds. Do not invent confidence percentages, finish bounds, duration, buffers, severity thresholds, alternatives or the legacy 25-minute rule.
- Required phase windows may be compared separately only when phase eligibility is proven. Alternative-window semantics remain unresolved. No-overlap covers only assessed valid pairs; incomplete membership/coverage, stale evidence or absence cannot clear a conflict. Keep coverage/exclusion counts and reasons without leaking inaccessible identities.
- Entries `schedule-link-status=Conflict` means a relationship ambiguity, not a riding conflict. Source-group membership (`linked` with `class-date`) does not prove operational occurrence; the older Entries path required `observed-group` plus supporting evidence. Do not convert this historical producer-specific rule into a mandatory Entries dependency for the current verified Data occurrence path.

## Entry condition rules and proposals

Preserve comparison of consecutive valid values under the same scope/phase/policy. First observation and recovery establish baselines; no inferred catch-up. Repeated processing of one source revision is not a second observation. Detection timestamps are not actual starts/ends. Invalid/stale inputs cannot clear conflicts or prove withdrawal/completion. Unknown/zero/sentinel fields require validated semantics.

Historical proposed thresholds, retained for a future explicit policy decision only: start/go crossings at 15/5 minutes; order crossings at 5/1 ahead (only the nearer milestone if both crossed); estimate movement at least 5 minutes confirmed by two distinct successful observations, at least 10 minutes between events; expiry at earliest dependency/condition end or detection +120 seconds. No threshold is implemented or newly approved by this consolidation. Conflict severity changes cannot operate before a supported severity policy exists.

Stable event identity, replay suppression and recoverable checkpoints are prerequisites. The specialist withdrew its separate condition-evaluation answer kind; it proposed condition state/checkpoints plus events. Preserve complete recoverable state before advancing mutable pointers, unique logical manifest members, canonical typed content, null/absent distinctions and explicit schema/policy versions. Those historical table designs must be mapped to the actual current shared pass before implementation; no condition-events table was created here.

On uncertain publication, read back the exact attempted identity before further publication. Never label a potentially accepted write abandoned or blindly resend. Current entry uncertain-write/failed-verification stops remain stricter than historical auto-reconcile-next-tick proposals. Append corrections/retractions referring to original events rather than overwriting committed events. No automatic corrective SMS.

Results owns official readiness, acknowledgments and fetching. Messages owns recipient/delivery contracts; acceptance remains unproved in the inspected handoffs. Entry conditions do not send messages, select recipients or declare Results ready. The proposed producer-revision-id to producer-revision-key rename remains a downstream contract reconciliation, not an implemented interface.

## Entry validation rules

Exact producer/base/table/key, source scope/revision, consumed evidence/hash and original observation time must survive calculation. Preserve missing, accepted empty, partial, failed, stale, ambiguous and historical states independently. A successful Entries processing timestamp, heartbeat, changed-at or new handoff file cannot renew source age. Prior links remain history. Retirement/absence cannot be inferred.

Entries is optional enrichment, wired last; its local sync absence does not block an independently supported current Data/Live answer. Do not restore external schedule parity checks or global Data-attempt failures as blanket gates. A newer exact applicable class failure/incomplete attempt affects only that dependent scope; unknown/global attempts are audit evidence. Required occurrence/phase mismatches block attribution.

The later accepted planning corrections reject every future source observation and require serving-time `now < valid-until`; equality is expired. Earlier 60-second future tolerance and inclusive expiry are superseded. Host clock verification is required. Earliest required dependency expiry controls an answer; serving-time checks and an explicit expiry contract still need integration into the actual entry output/consumer path. Historical evaluation-inputs/revision-coverage storage is not an existing entry table contract.

Current numeric and Live-first component policies remain Timing-coordinated. Preserve separately proven membership even when Data values are stale; do not allow stale Data values to override fresh applicable Live or use Live to invent missing identity. The current staged worker still couples Live selection to Data age; reconciliation belongs to the stopped implementation review. See the [cross-owner freshness proposal](../docs/coordination/wef-freshness-completion-contract-20261030.md), which is not blanket implementation permission.

## Evidence paths and limits

- `wef-entries/evidence/shared-schedules-20261029/status.md` was read during consolidation. It records two normal producer runs: `5ca0b0c7-fca9-44f3-9f74-143a472a7da1` and `0b494dd2-560b-499b-9360-ef36f3006369`. Specialist reviews report 113 ordinary links, 1 Conflict, 103 source-group memberships, 11 unresolved groups and 13 historical records. This is dated producer evidence, not current freshness or conflict execution proof.
- Exact saved receipts: `wef-entries/evidence/shared-schedules-20261029/verification-1.json`, `verification-2.json`; earlier `wef-entries/evidence/data-input-switch-20261029/verification-1.json`, `verification-2.json`, `normal-run-1.json`, `normal-run-2.json`, `business-after-2.json`. These individual files were cited/read by specialists; they were not all independently reread in this consolidation. Earlier unresolved-group counts were superseded by shared-schedules proof.
- Historical source identities: Shows `appCaSmL819KK7UFJ/tblJA5CUon5s1sWib`; Entries `appSxF4pYMBaxpTZd/tblqkWmrsYMs2hyQ6`, local schedule sync `tblVcbGJHhzLYZRxn`, collections `tbltCxLRlhpdHdHSw`; Live `appgzosUcVqA4vZdf`, rings `tblw8bodkRbCItxIH`, groups `tblMhQtjpvyODEUSn`, classes `tblNaM9ehOJFsrveV`, trips `tblf7550e7UGT8ZSF`, collections `tble6fwMDBbeeGxly`. Current Data base `appdl16HJ5ETm8N69` uses immutable handoff references, not fabricated record IDs.
- Specialist State operational observations (Data collision/head age, unavailable Timing counts, archive_required) are dated saved-receipt findings with no fresh Airtable readback. Do not present them as current system health. Its five-minute monitor/notification proposal was not created or transferred as an active automation.

## Remaining decisions and gates

1. Fresh entry edit approval remains pending. No conflict implementation is authorized by the consolidation request.
2. Establish validated entry occupancy start/finish bounds and phase/alternative semantics before any conflict verdict.
3. Coordinator must reconcile current entry freshness/serving expiry, condition thresholds, event/checkpoint storage and uncertain-write recovery with the actual shared pass. Preserve the stronger existing stops.
4. Establish authorized audience and complete comparison coverage; no clearance from incomplete data.
5. Results/Messages must accept exact downstream identities, expiry and correction semantics before handoffs are called working.
6. After separately authorized implementation: all-pair, touching/uncertain-window, identity/privacy, stale/expiry, first/recovered baseline, repeat/restart/timeout and downstream regressions; then normal scheduled evidence. No specialist planning or producer test counts substitute for that proof.
