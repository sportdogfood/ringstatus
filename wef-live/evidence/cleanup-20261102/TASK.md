# New-show and quick cleanup — November 2, 2026

Owner approved: cleanup checked tables when default show ID changes, archiving before deleting records not belonging to the new show; quick cleanup only heartbeat/run-logs/errs-log at 1,000 records, oldest 500; separate cleanup-logs; use existing scheduled workflow. Prior stage-9 hold is lifted only for these actions. Stage 8 stays on hold. No manual collector, provider calls, record repairs, new task, cadence change, other collectors or schema upgrades.

Baseline: existing hidden three-minute task, entry.js -> lanes.scheduled; November 1 membership release. Frozen baseline JS and task definition are beside this file. Shows edits visible in git belong to another owner and are protected.

Implementation: cleanup runs after all three lanes finish, so it cannot block collection. Read canonical default directly from Shows; same show/new day is not an event. First adoption reconciles existing old-show records. Checked eight owned tables only; permanent catalogs, references and cleanup logs are excluded. Archive all selected snapshots including native links before any new-show deletion. Active log records are preserved. Quick cleanup selects the oldest 500 by Airtable creation time; no 24-hour policy is re-enabled.

Each action writes cleanup-logs and latest per-table cleanup status, including archive path/hash before deletion. Exact-ID deletion and readback are required. Uncertain/error cleanup leaves a local stop for owner decision; later normal invocations still collect but do not retry cleanup. Already deleted reciprocal native links cannot remain attached; their original values are in the archives. Retained unrelated/current records are not edited by cleanup.

Acceptance: offline boundary/repeat/failure tests, reviewed actual diff, additive schema readback, deployment hashes/task unchanged, two normal scheduled invocations. No forced show change or manual execution. Natural new-show/threshold actions count only if observed; otherwise label those branches unverified.
