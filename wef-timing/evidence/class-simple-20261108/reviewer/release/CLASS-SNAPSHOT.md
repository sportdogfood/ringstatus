# Simple Class snapshots

Prepared 8 November 2026. **Not deployed.** Existing schedules and production code are unchanged.

1. Read the default show, today's Ring current/next pointers, linked Data classes/schedules, Live counts/trips and saved observations.
2. Measure Class pace and calculate start/end countdowns. Use 2.5746 only when neither Class nor usable shared Ring evidence supplies pace.
3. Compare with the previous Class snapshot.
4. Save a new class-status snapshot, including unchanged cycles, and read it back.
5. Update/read back the Class-owned `class-status-as-of` print on the linked local `wef-data.classes` row. Write run/error/heartbeat evidence.

Example: observed at2:28p.m., actual start1:59,30total/10gone/20left =>2.9minutes/trip,58minutes remaining,3:26finish. Next scheduled3:00 =>estimated3:26 and26minutes late. Without preceding work, scheduled3:00 at2:28 gives32minutes until start. Times are Eastern; countdown units are minutes.

The snapshot contains current/next trip IDs, horse/rider/order and source links. `reportedCurrent` means Live's current-trip pointer; it is not a separately witnessed on-course assertion. Source status/is-live and original observation time remain visible. No per-entry go-times are calculated. Only current and next-group classes are evaluated; this is not a full-day Ring forecast engine.

Actual start comes only from a saved original first-trip `r1_start`, not scheduled start or first-seen. If absent, use compatible Class count observations and keep actual start unknown. Invalid/missing Class fields leave their dependent estimates unknown without stopping independent classes. Shared groups do not run their remaining trips in parallel; fallback group-boundary estimates use the source group workload and are labeled. Explicitly empty Ring pointers never revive stale group backlinks. No provider/collector endpoint is called.

## Prepared deployment artifacts

- Fresh source: `class-snapshot.js`, `class-cleanup.js`.
- Existing-route integration: `evidence/class-simple-20261108/candidate/data-runner.js` and `candidate/entry-data.js`. These are staged copies, not the running files.
- Seven additive fields: `schema-changes.json` adds four requested estimated timing/countdown fields, two trip-context fields and one local Class print. Existing35fields and source acquisition fields are preserved. **Schema changes have not been applied.**
- Existing launcher/task stays `wef-timing-data → run-hidden-data.vbs → run-data.ps1 → entry-data.js`. No new task, cadence or deployment route.
- Apply the prepared schema and reviewed files only after deployment authorization. Normal scheduled runs and actual Trip consumer readback remain unverified until then. No manual substitute runs are accepted.

Output remains `appeqxxJyPGS1iqdm/tblxhS1xWpfHKTd6l`. Class print is a local extension on `tblUgcgOMvfjbEthx`; it does not modify the Data producer's fields or another base. URL links preserve synced input schemas. New-cycle keys append a snapshot; a repeated identical cycle key does not duplicate it.

## Separate cleanup contract

`class-cleanup.js` runs after calculation through a separate guarded invocation in the staged existing entry. It never decides whether calculations may run. Insufficient remaining runner budget defers cleanup; no extra schedule or Records alert is introduced.

- At19:00Eastern: clear eligible completed operational logs, including runs stored in run-logs.
- Every3hours: per logging table at1,000+total rows remove its oldest500eligible rows; below1,000 remove none.
- Different default show ID in w-shows: clear eligible previous-show records from timing-answers,engine-checkpoints,run-logs,heartbeat,errs-log. Same show ID/new day-0 is not a new-show event. w-days provides calendar context; no separate day-0 cleanup. The show end date does not itself delete anything.

Protect all synced/source/reference tables, cleanup, code-reviews, catalog, current/future answer dates, active runs/checkpoints, unresolved errors/dependencies and pending intents. **class-status history remains protected until its retention window is agreed.** Unknown identity remains protected. Retired legacy checkpoints require explicit stored retirement proof `{id,show,date}` because their actual schema has no show/date columns; this script creates no such retirement decisions. Errors remain protected until explicitly resolved; parent logs stay protected while unresolved children remain.

Old timing answers and eligible retired checkpoints are archived in full before deletion, with compressed file/hash readback and same-base archive catalog readback. Old timing answers retain the24-hour minimum. Selected records must still match before deletion; durable local intent precedes deletion; absence is checked afterward. Pending/uncertain deletion stops cleanup for reconciliation. This does not stop the independent Class calculation lane.

## Verification boundary

Saved Nov8 source corpus and438frozen original observations are in `evidence/class-simple-20261108/`. Offline checks cover source selection, actual example math, full read/save/print/readback, repeats, missing values, shared workload, identity and cleanup protections. Fake HTTP/filesystem execution is offline proof only. No live Class snapshots, Class print updates, deletions, schema changes or deployment have been performed for this new workflow.

The rejected Nov7 `class-status.js`/`class-status-cleanup.js` package remains preserved, unmodified and unused by this fresh implementation. The owner now grants routine in-scope fixes/retests without per-edit approval; scope expansion, deployment and manual production runs remain excluded.
