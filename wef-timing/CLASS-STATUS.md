# Class snapshots and cleanup

Version 1.0 — 7 November 2026. Candidate, not deployed.

## Scope and output

`class-status.js` evaluates saved synced inputs and returns one new record for every Active Data class in the explicit show/day. No provider reads, network, timer or production writes. The existing shared runner and timing-answers are untouched. Existing production Class code is not replaced.

Output: `appeqxxJyPGS1iqdm/tblxhS1xWpfHKTd6l` (`class-status`), 35 approved fields. Schema receipt: `evidence/class-status-20261107/schema.json`. All source links are URLs, preserving synced input schemas. `previous-snapshot` is the prior snapshot key; `changes` is a JSON array of field/previous/current values. New cycle timestamps produce new keys, including unchanged state. Same-cycle keys are stable for eventual writer deduplication.

Class identity is the existing Data class-key; phase is separate. Multiple schedule appearances are flagged rather than guessed. Input tables: w-shows/w-days establish caller scope; wef-data.classes supplies the class list, identity and native status; schedules supplies order, planned times and per-class workload; Live classes supplies observed class progress; Live groups/rings supplies group phase/activity and links. Synced links and exact keys are used, never names. Explicit caller scope is required; production authority selection is not wired.

Prediction pace stays **2.5746 minutes per trip**. `average-pace` measures elapsed minutes / additional class trips between compatible saved snapshots. Ring pace is informational in observations, linked to the exact original ring-status record. It never replaces the fixed pace. Counts represent trips, not entry-count. Unknown phase totals remain unknown. Group totals are never copied into class total/gone/left.

`is-live` describes the linked group's source value (true/false/unknown). It does not assert this class is on course. Native class Completed is reported as native class status; positive base counts do not prove every round complete. Actual start is unknown with the currently inspected inputs; `first-seen-underway` is separate and never relabeled actual. Original update strings and observed clocks are preserved. Consumers must assess the saved observation age; a new calculation time does not renew it.

Single-class underway finish = observation time + remaining class trips × fixed pace. Upcoming projections sequence ring groups and breaks once, avoiding duplicated shared workload. Shared-group individual finish remains unavailable until its interleaved order supports a class-specific finish. Unsupported fields do not suppress the other Class snapshot fields. Next classes may be plural. No rider-specific go-times, rider alerts or trip-calculator writes are included.

`wef-trip-calculator` can consume exact class-key, date/phase, original observed-at, class/schedule/ring links, progress, estimates and reasons from this log. Consumer integration is not implemented or verified by table creation.

## Cleanup contract

`class-status-cleanup.js` is a separate, injected-transport lane. It has no schedule, Records alerts or default production credentials.

- At/after **19:00 America/New_York**, once per calendar day, clear eligible completed run-logs/heartbeat/errs-log, including completed runs represented in run-logs.
- Every **three elapsed hours**, each logging table with **1,000+ total records** may delete its oldest **500 eligible** records; below 1,000 it deletes none.
- A verified change of the default show ID in w-shows clears eligible records for the previous show from the five approved owned tables: timing-answers, engine-checkpoints, run-logs, heartbeat, errs-log. A new default row with the same show ID is not a transition. The show end date tells the owner when to check for a new default; it does not itself trigger deletion.
- w-days supplies calendar context. No separate day-0 change cleanup. No new schedules.

Protect all synced inputs including ring-status, cleanup controls, code-reviews, meta and archive catalog. Preserve active/current checkpoints, incomplete runs, unresolved errors/dependencies and pending intents. Current/future answer dates for every show stay protected. Unknown identity/eligibility is protected. Only explicitly retired/unreferenced answer/checkpoint IDs may be removed after full dependency inspection. Only resolved error IDs may be removed.

**class-status history stays protected** until its lookback/retention window is agreed. It is not a completed operational log. No nightly snapshot purge is inferred. No separate runs table is created.

Cleanup requires checked controls and a complete protection inventory. Transport must enforce Class base isolation, durable intent, unchanged-record readback before deletion and absence readback afterward. Failure stops the affected path; no automatic repair/retry. Event checkpoints advance only after success; the eventual transport must preserve pending state across interruption. No production transport/trigger is installed by this candidate. Historical archive-before-delete obligations on existing timing-answers are not removed by this script; an executor must satisfy them before providing eligible IDs.

## Verification and remaining integration

Offline test uses the saved November 6 corpus in `evidence/class-status-20261107/yesterday-inputs.json`. Its capture happened later: this proves retrospective calculation against saved rows, not the exact inventory available at every historic minute. Explicit synthetic mutations of those rows check recurring diffs, pace and protection. No production calculation or deletion is run.

Before deployment: independent review, then adapt the existing shared scheduled route to these verified native inputs and output fields under separate deployment authorization. Bind cleanup to its established authorized lane with protection, archive and intent evidence. Two scheduled results and usable trip-calculator readback remain separate acceptance gates. No cadence/task change, migration of timing-answers or resumption of the rejected November 2 reader patch is included.
