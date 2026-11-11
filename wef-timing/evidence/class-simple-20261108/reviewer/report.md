# Independent simple Class workflow review

Reviewer: `/root/class_status_review`, delegated from owner chat `01a16f18-25b9-7222-9b53-b64496ec84d9`.

**Review COMPLETE. Final candidate PASS for the inspected code and offline scope. No remaining actionable code finding.** Deployment, schema application, normal scheduled execution and Trip consumer acceptance are not verified by this result.

## Final checks and useful output

Independently executed the actual final writer suite: **35 checks passed**. Independently authored regression, receipt/audit and destructive-cleanup counterexamples: **16 checks passed**. Five commands exited zero; exact command, runtime, cwd, UTC start/end, exit code, stdout and stderr are in `commands.json`. The writer suite was required unchanged; `final-suite-wrapper.js` redirects only its two generated output artifacts into this reviewer directory.

The frozen November 8 input/history test uses 438 original saved observations and produces four current/next Class snapshots, one with measured Class pace. The useful current Grand Prix example has 36 total, 31 gone, five left, a measured 2.1 minutes/trip and an estimated finish about 15:01:38 Eastern, from its original 14:51:08 observation. Actual start stays unknown. The three Ring 6 Upcoming observations retain morning source times and explicit past-schedule notices; they are not proof of fresh live readiness. The owner example independently verifies 13:59 actual start / 14:28 observation / 30 total / 10 gone => 2.9 minutes/trip, 58 minutes remaining, 15:26 finish, and a 26-minute overrun against next scheduled 15:00.

No November 6 result from the rejected prior package is attributed to this fresh candidate. This review's real saved corpus is November 8, as explicitly recorded in the current task. Synthetic offline cases are distinguished from saved source observations.

## Scope, baseline and interfaces

- Read the actual current task/approval, final source, proposed schema, contract, saved inputs/history, tests, staged integration and relevant baseline/dependencies. The supplied latest owner approval expressly permits necessary in-scope fixes after failed offline/review checks; it excludes deployment, schedule changes, manual production runs and scope expansion.
- Fresh files are `class-snapshot.js` and `class-cleanup.js`. Existing production runner/API/entry and the rejected `class-status.js` package remain unchanged. All **11 protected baseline files** match their frozen hashes. No dependency, runtime or framework upgrade was introduced.
- The staged default preserves the exact proven Ring producer, `require('./ring-snapshot').run`. The default dependency, not merely an injected substitute, was independently checked in a VM. Ring pace production is not reassigned or changed.
- Prepared `data-runner.js` retains the established receipt fields and counters, preserves actual Ring failure messages, distinguishes nonfatal Class warnings, and retains Ring results/unknown Class counts when Class reporting throws.
- Prepared `entry-data.js` preserves the existing lock/deadline/receipt route and adds separately guarded Class cleanup after calculation and Ring cleanup. Its exact unchanged hash reuses the independently executed fake-filesystem entry check in `final-commands.json`, command `final-staged-entry`; this reuse is inspected, not presented as another execution. That check observed calculate -> Ring cleanup -> Class cleanup -> receipt -> unlock with zero production writes.
- `class-status` keeps its 35 existing fields. Six proposed snapshot fields and one local `class-status-as-of` extension are specified in `schema-changes.json`; they have not been applied. Writes are pinned to the Class base. The transport permits only the local summary field on synced Data classes and blocks source fact edits/snapshot deletion.
- Full fake read -> calculate -> compare -> snapshot save/readback -> local Class print/readback -> run/error/heartbeat evidence was exercised. Pagination and Date-field formulas were checked through the real HTTP adapter with a fake request function. Same-cycle deduplication and new-cycle append behavior passed.
- Current and next groups use native Ring pointers. Empty current pointers do not revive stale backlinks. Active-noentries rows remain usable in scope. Missing Class observations affect dependent values without suppressing the other classes. Actual start only comes from original first-trip `r1_start`; scheduled/first-seen time is not relabeled actual. Compatible Class pace outranks shared Ring/default fallback. Shared group workload is not treated as parallel work.

## Findings corrected during this review

All corrections below were made by the designated writer under the standing owner approval, then reviewed and checked on a newly frozen candidate. The reviewer did not edit the implementation. Earlier failed execution logs and frozen source versions remain preserved; they are not passing evidence for the final candidate.

| Finding | Trigger and consequence | Smallest correction now verified |
| --- | --- | --- |
| P1 — stale current selection | Empty Ring current pointer plus old group backlink selected an old Underway group. | Native Ring pointer is authoritative; empty means no current selection. |
| P2 — source activity overwritten | Explicit source is-live false became true solely because the group was selected current. | Preserve source true/false/unknown. |
| P2 — owned logs lacked show identity | New-show cleanup could not identify newly written Class run logs/heartbeat. | Stamp the verified default show on run/error/heartbeat evidence. |
| P2 — all checkpoints unconditionally protected | Explicitly retired checkpoints could never clear. | Use explicit retirement proof with actual checkpoint schema, protecting unproven/active records. The table has no show/date columns; the proof is `{id,show,date}` in authorized control state. |
| P1 — cleanup identity not validated | Missing default show ID became NaN and triggered new-show deletion. | Reject invalid/nonpositive show/customer before deriving an event. |
| P1 — shared workload projected in parallel | Two current classes with ten remaining trips each advanced next-group start after only ten default-paced trips. | Use existing shared group remaining workload once for the next-group boundary, or leave unsupported estimates unknown. |
| P1 — record-ID dependency not protected | A retained answer referenced by Airtable ID could still be selected. | Protect answer keys, IDs and record URLs and inspect retained answer references. |
| P1 — default Ring route drift | Staged wrapper selected a different existing Ring implementation. | Restore the exact baseline `ring-snapshot.run` producer and test the default dependency. |
| P2 — receipt/audit evidence lost | Class final heartbeat failure discarded known delivery counts; root failure fields could omit Ring errors. | Return failed audit disposition with actual saved counts, preserve Ring results and established receipt/failure fields. |
| P1 — pre-delete check preceded archive work | A selected answer changed to a future date during catalog calls and was deleted. | Repeat exact selected-record readback after archive/catalog verification, immediately before intent/delete. Final counterexample preserves the future record and stops deletion. |
| P1 — reporting failure replayed a 500-record allowance | After 500 verified deletions, summary failure left the next three-minute cycle able to delete another 500. | Persist the running event and verified per-table deleted IDs; unfinished events require reconciliation and cannot select a new allowance. Final counterexample remains at exactly 500 deleted. |

Initial round evidence: `round1-commands.json`/`round1/`, `round2-commands.json`/`round2/`. The later archive mutation and 500-record replay failures are preserved in `predelete-order-commands.json` and `cleanup-progress-commands.json`; final successful corrections are separately captured in `commands.json`. The writer's Windows EPERM failure and correction are writer-owned evidence, not independently executed claims. Final state persistence uses write/fsync/close/readback without retry loops; malformed/unfinished state remains unable to authorize deletion.

## Cleanup coverage

Read and traced the separate cleanup contract and executor. Offline checks verify 19:00 Eastern daily cleanup, three-hour 1,000/500 behavior, default-show-ID transitions, pending/unknown/current/future/unresolved protections, full archive/hash/catalog readback, exact postarchive readback, durable intent, absence proof, interrupted-event stop and catalog reuse. Operational run logs contain the runs; no new runs table, schedule or Records alert is added.

Synced inputs, cleanup/code-review/reference/catalog tables and new Class snapshot history are excluded. Snapshot retention is still a business decision, so no history purge is inferred. Old timing answers retain the 24-hour minimum; retired checkpoints need explicit proof and full archive/readback. Active or pending controls, unresolved errors and dependent parent runs remain protected. The durable unfinished-event stop affects cleanup only; the independent Class calculation lane continues.

## Separate acceptance gates

| Gate | Result |
| --- | --- |
| Approval boundary / exact protected baseline | PASS for reviewed candidate; 11 files unchanged |
| Code and offline integration | PASS — 35 suite + 16 independent reviewer checks |
| Saved useful Class calculation | PASS for the recorded November 8 examples, not current provider freshness |
| Schema additions / source deployment | Not applied or deployed; excluded from this authorization |
| Normal scheduled Class writer/cleanup | Unverified; no manual substitute execution |
| Current source freshness / live production readback | Unverified by frozen saved input |
| Trip calculator shared-output consumption | Unverified; no sibling consumer write/integration executed |

Report/receipt/manifests preserve exact final hashes. No production endpoint, collector, task, data repair, live Class write, deletion, schema application or deployment was invoked by this review. Next action is the owner's separately authorized deployment phase, if requested; Code Review PASS does not authorize it.
