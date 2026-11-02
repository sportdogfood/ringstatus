# Delete-only correction review

Reviewer: wef-live author in chat 01a16902-240a-7280-8661-90b0d8d7325a. Author review, not independent review.

Approved change: deploy the owner's settled cadence and cleanup triggers, with deletion only. Current task and 23 runtime files are the baseline; this replaces only airtable.js/retireCleanup, cleanup.js, lanes.js, adds live-cadence.js, and changes the existing task repetition duration. No other collector, schema, dependency, API or observer changes.

Inspected the actual adapter call chain, shared canonical show authority, exact delete scope, original-value comparison before batches, active/current-log protection, exact DELETE response and absence readback. Removed archive-file creation and archive verification from cleanup only; dormant legacy retirement paths remain disabled. Cleanup receipts now contain action/count metadata rather than deleted-record field snapshots. Existing historical files and catalogs are preserved.

Traced new-show and Eastern new-day checkpoints, same-day early return without log inventory, threshold >=1000 with exactly500 eligible completed records, repeat idempotence, log failure and deletion failure stops. Cadence checks occur hourly for false status/date mismatch/outside window, with mismatch warning, and every3minutes for live/today in05:00–19:00Eastern. Not-due dispatches perform only local reads. All three detail lanes still run independently and cleanup follows collection. Stage8 and old24h retention remain disabled.

Offline final suite:81PASS. Fixtures prohibit network and verify no archive creation/calls, metadata-only cleanup receipts, expected retained link preservation, exact deletion checks, same-day no inventory, Eastern boundaries and cadence/logging transitions. Relevant task settings/action/account are unchanged except24h repetition/StopAtDurationEnd=false. No manual business run performed.

No new actionable defect found within this delta. Two existing conditional cleanup-reporting P2 findings remain: failure may leave unexecuted action rows Running; interrupted cleanup's stop branch does not refresh latest summary/Airtable error. These are baseline findings, not claimed fixed or automatically expanded into this release.

Code/offline gates PASS. Deployment and normal-run acceptance must be recorded separately after actual readback. Natural new-show/new-day500 deletion and populated activity remain unverified until those events occur. Existing989-record archive-era proof does not prove delete-only behavior. First inactive check plus a quiet task tick cannot prove two hourly checks; report them distinctly.
