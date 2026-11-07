# Independent standalone cleanup review

Reviewer /root/schedule_stage_review. COMPLETE; candidate FAIL on pending preservation and error continuation. Offline review only; no deployment or natural-alert proof.

## Findings

P1 CONFIRMED — cleanup-lane.js localLogs()/pruneLocal(): eligibility and child list are captured before all remote operations, then stale families are unlinked at the end. A new undelivered child added after that snapshot survives while its parent is deleted. Reviewer fixture reproduces the orphaned pending error. More generally, the independent task can delete old local files while logger.flush scans the same directories; attempt-log.js files() has a separate directory read and per-file reads, without shared exclusion. Individual old-family PIDs do not identify a newer process scanning all historical families. Removing entry/launcher service hooks removes their former same-task serialization. Smallest required correction is preserve pending/current families at actual deletion and address the actual reader overlap within owner's authorized independent-lane scope. A snapshot-only live-workflow check cannot eliminate a process starting afterward. This finding does not authorize adding locks, waits, collector gates or other mechanisms.

P2 CONFIRMED — cleanup-lane.js cleanup() prefetch: schema/control/local/parent/heartbeat/error reads happen outside per-table try/catch and before persistent run outcome. A single run-logs read error aborts without history/last-run and prevents independent checked show-table work. Reviewer fake-HTTP fixture selects schedules for new-show, rejects run-logs prefetch, and observes no independent delete and no error receipt. Smallest correction: record initialization/read failures in existing outcome and keep their affected logging/local scope separate from unaffected selected show work. Preserve required parent protection; do not delete log rows whose protection evidence is unavailable.

## Verified scope

Explicit owner-authorized replacement is reflected in task.md. New file imports fs/path/crypto only, not retired cleanup code. Existing cleanup-run launcher points to new main; entry/run.ps1 only remove old cleanup helper hooks, preserving acquisition/linking logic and cadence. No new schedule/stop/archive/package/schema changes found. SHOW/LOGS allowlists exclude reference/history tables. Existing completed-event state cutover/dedup remains; duplicate successful event does not delete another batch. Parent native links are cached before run-log removal, allowing eligible error cleanup after Airtable would unlink parents.

Seven supplied offline checks independently passed: calendar/new-show ownership, new-show-day no-op,1000/oldest500 threshold, batch-error continuation, success dedup and initial pending/current local protection. Three reviewer edge assertions passed, two demonstrating the confirmed defects. A third observes removing latest local success when newer run failed; this is NOT a defect finding: logger omits absent heartbeat fields and native values may remain. No extra success/error retention is requested by this review.

Source checkout new cleanup-lane.js equals frozen candidate; supplied test imports that exact checkout file. Reviewer edges import frozen candidate. Actual command/timing/runtime/output saved in commands.json. No source endpoints, live business data or task executions. No code fixes by reviewer.

## Limits and acceptance

The meaningful failures are scoped, not a request to recreate old architecture. Tests do not establish concurrency safety or normal alert operation. Historical failure explanation remains unchanged/unknown. Code review cannot replace deployment, producer matching and actual Records-alert proof. No approval condition beyond owner's existing boundaries introduced.
