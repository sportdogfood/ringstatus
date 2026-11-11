# Independent Trip cleanup review

## P2. A concurrent log append leaves the trim unfinished

`wef-trip-calculator/cleanup.js:35` computes `remaining` from the original row list. It checks that selected IDs are absent and persists that computed count in `finishIntent` on line 47. The reviewed transport contract permits log rows to be appended between the initial list and deletion.

The independent counterexample starts with 1,000 eligible rows, adds one completed row during the fake delete, and confirms absence of all 500 selected IDs. The candidate persists `remaining:500, trimPending:false` even though 501 rows remain. On the next full read it skips the trim because the count is below 1,000 and the pending flag is false. This violates the requirement to finish trimming to 500. The fake-transport check reproduced the failure in `command-log.json` under `concurrent-log-check`.

Smallest proposed correction: keep a just-completed limit trim pending until a fresh complete normal read observes 500 or fewer rows. The existing caller can supply that read on the next normal check, so this does not require a new schedule or transport method. Preserve the failing case and obtain the required fresh edit approval before any repair or retry.

## Verified scope

Independently executed `node --test wef-trip-calculator/cleanup.test.js`. All eight authored cases passed. The suite covers inclusive thresholds, oldest eligible selection, protected continuation below 1,000, summer/winter 19:00 Eastern gating, eligible log clearing on a default-show change, checked ownership, pending/active/error protections, changed targets and uncertain delete. The ninth, reviewer-owned concurrent append case failed. Exact commands, Node runtime, UTC times and separate streams are in `command-log.json`. Tool output `630f64` records both executions.

Inspected owner clarification and FIX IT handoff against the frozen prior cleanup implementation, README contract, scoped candidate diff, writer log, schema and tests. The reviewed delta stays inside cleanup selection/continuation and documentation. Current deletion allowlist is exactly checked `run-logs`, `heartbeat`, and `errs-log`. The source task record reports no supported checked Trip-owned live tables. This review did not repeat Airtable readback. Synced or unsupported live tables are not cleanup targets. No scheduler, Records alert, runtime dependency, or calculator change was found in the scoped diff. Baseline, candidate and the three protected files matched manifest hashes before and after execution. The earlier combined calculator test remains stopped and is outside this review.

The caller still owns supplying full paginated rows and complete protections, persisting per-table continuation even when every row is protected, and stamping the daily/default-show markers only after an eligible completed operation. `finishIntent(intent,result)` makes continuation available to an injected transport after a successful deletion. The all-protected branch returns continuation to the caller directly. The fake transport proves the interface only. No normal caller, persistence or production reset was exercised. Production verification is outside this separately approved offline correction.

Review COMPLETE. Cleanup candidate FAIL on the independently reproduced continuation defect. No candidate files changed during review.
