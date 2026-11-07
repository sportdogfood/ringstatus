# Independent delivered-local-log cleanup review

Reviewer /root/schedule_stage_review. Reviewed UTC 2026-11-07T03:32:42.426058+00:00.

## Findings and verdict

No actionable defect found in the current candidate within the approved Data-owned local-log cleanup scope. Code review COMPLETE, candidate PASS, independent targeted tests 7/7 PASS. Records alert handoff, actual deployment, normal scheduled local removal and natural-alert cleanup remain UNVERIFIED by this offline review.

## Approval and exact change

The owner approved fixing Records cleanup alert handoff and cleaning delivered local logs, preserving pending logs, independent review and verification through existing alerts, with no new schedules, manual runs or extras. The parent task assigns Records its existing alert route and Data its local log cleanup. The owner separately prohibited fixing/reworking the logger. This candidate leaves attempt-log.js and cleanup.js unchanged and adds only cleanup-local-logs.js plus its two-line invocation/error handling in entry.js after existing log delivery and delivery-error recording. It changes no collection, source, status, cadence, timeout, logging budget or remote cleanup policy. The existing schedule/priority release remains outside this diff.

Synchronous local removal at the existing delivery caller is needed to avoid racing its read-directory/read-file sequence; independent alert cleanup is not given local-file deletion. The helper is scoped to the private root/config already selected for the collector or linker. This is a cleanup of delivered local files, not an additional collection eligibility rule, timer, task, queue or lock.

## Actual integration inspected

Inspected candidate entry.js against its frozen baseline, candidate cleanup-local-logs.js, the actual attempt-log.js payload projections/ack writer/flush loop, run.ps1 and registered task settings. The only production log-delivery call is entry.js. Collector and linker use separate local logging roots. Fresh read-only task inspection confirmed both existing tasks have MultipleInstances=2 (IgnoreNew). The candidate calls prune synchronously only after awaiting delivery, after recording any delivery error. No asynchronous work inside prune yields control to another callback. Latest/active family protection covers the current invocation. Given those current task/caller boundaries, the normal same-workflow delivery path cannot be simultaneously reading files deleted by prune. This conclusion does not authorize manual overlapping invocations.

The helper projects exactly the same current run and error fields, including property order, as the unchanged logger. It requires exact run acknowledgment hash and every associated error acknowledgment hash, with the actual parent Airtable acknowledgment ID in each error payload. Missing, malformed or changed acknowledgments preserve the whole family. Started or nonterminal, missing-completed, other-config and live owner/child-PID families are retained.

The newest attempt, newest Succeeded attempt and parent of the newest error are retained to preserve heartbeat status, last-success and last-error. Deletions remove acknowledged error files before the parent run, then the acknowledgment files. An error deletion failure leaves the parent and remaining error/ack files available; the caller logs the cleanup error. An interruption after successful error deletions leaves only previously delivered data removed and parent still available. Acknowledgment deletion failure may leave harmless orphan acknowledgment files, but does not remove pending log payloads. No sweep of those orphans is added.

## Independently executed checks

Executed node --test test-local-cleanup.js using the current Node runtime. All seven tests pass: fully acknowledged family removal and zero-write heartbeat parity; repeat cleanup is empty; missing error ack preserves parent bytes; changed run/error and malformed/missing acknowledgments retain files; newest success/error families remain; active/Started/other-workflow files remain; error deletion failure leaves readable parent and subsequent delivery succeeds; the actual current entry module executed in VM calls cleanup after delivery settles for both collector and linker, success and failed delivery, and catches prune failure without changing the Succeeded collection result.

The tests invoke actual attempt-log.flush only against a fake request adapter and temporary directories. The entry VM uses fake source process/logging dependencies. No production collector, Airtable business write, source endpoint, task start, synthetic alert or runtime-file removal was performed by this reviewer. Old entry/stage tests built around retired paths were not used as a substitute for the current caller check.

## Protected source and operational acceptance

attempt-log.js SHA-256 remains e913eb7d3a3031636dbbc5adbf0f5e197655ad71816b5fe376a86c1e01238f8a. cleanup.js remains 570b515390ff4f28d782678d768419a5d7c1f17e1633cbb539eef852e4b06e74. No logger or remote cleanup implementation changes were reviewed or made.

The writer must deploy the exact reviewed helper and entry bytes, observe removal through the normal scheduled invocation, verify pending families remain, and reconcile Records' own natural-alert handoff/cleanup evidence. Those operational steps are intentionally unverified in this offline receipt. A successful independent review is not proof of completed alert delivery or live deletions.
