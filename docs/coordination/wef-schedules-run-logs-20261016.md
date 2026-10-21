# Airtable run logs: release record

16 October 2026. Owner approved readable Airtable run logs, local tests, private deployment and verification through normal scheduled runs; no manual runs or existing workflow changes.

## Completed preparation

Loaded current repository/source AGENTS.md, current maintenance handoff and STATUS.md. Adopted completed default-day-priority release without repeating it. Used previously loaded Airtable, debugging, verification and code-review guidance; additionally read test-driven-development, writing-good-tests and brainstorming guidance. Existing baseline at source checkout commit f5e895b6d; product files collect.js/store.js/current-store.js/settings.js matched deployment before edits. Existing standalone-test source/deploy difference predates this work and is protected from overwrite.

Created only new Airtable table run-logs (tbluqwuXxOvgDRUBq) in wef-schedules appUq5Qz9kARqFija via connector. Fields: run-id, started, completed, result, show-id, rows, days-complete, days-failed, summary, errors. Date/time display uses America/New_York. No records inserted manually; normal collector has not been connected to this table yet. Existing table schemas and records untouched.

Prepared source only under C:/Users/gombc/.codex/worktrees/wef-schedule-collector/ringstatus/wef-schedules:
- New run-logs.js and tests/run_logs.test.js.
- collect.js passes the explicit logger into the scheduled collector entry.
- store.js starts/finishes a run log inside the existing lock, catches top-level failure for logging, releases the lock, and includes observed row counts in per-day reports. Existing collection operations and scheduling remain unchanged.

Design: one row per acquired collector run, with Running then Success/Warning/Failed result. Readable per-day summary, observed active row counts, changes and explicit errors. Keep at most 1000 Airtable log rows (about 3.5 days at five-minute cadence); archive oldest finished record exactly as verified gzip before reusing its slot. Running records are never overwritten. No record deletion. Local runner log remains the fallback if Airtable is unavailable; process startup failures before the logger starts cannot be logged to Airtable by this code.

Baseline copies/hashes/task XML: deployed wef-schedules/runtime/run-logs-release-20261016/. Contains baseline.json, protected-before.json, collector-task-before.xml, protected-tasks-before.json and source-file backups. Private environment contents were never read or printed.

## Local failure and exact correction proposed

New focused suite: five tests fail at `Run log readback mismatch: errors`. Initial red suite confirmed missing logger before implementation. Implemented suite then revealed an actual empty-value normalization mismatch: expected empty strings normalize to null, but observed explicit empty strings remain empty strings. The offline Airtable fixture retains `errors: ""`; Airtable normally omits empty cells. No live collector failure occurred and no correction has been made after this failed verification.

Concrete next correction: in run-logs.js readback, normalize both observed and expected blank/null values consistently before comparison. Preserve mismatches for populated values. Add/adjust focused coverage for omitted and explicit blank cells without weakening populated-value readback checks. This changes only new log verification, not schedule behavior.

Global runner instruction controls: "Do not patch code after a failed verification unless the user separately says APPROVED TO EDIT." Work paused pending that separate approval. Do not deploy current source or mark tests passing.

## Remaining authorized path after renewed edit approval

Apply the precise blank-value correction; complete focused tests and full private package suite; review exact source delta and failure/retention behavior. Update manifest only after verification. Deploy only approved changed private files (never runtime secrets/data or preexisting mismatched tests), between normal runs without changing task definition. Observe two normal scheduled runs and connector readback for logs and unchanged downstream schedule behavior. Preserve any new failure and stop that verification path; no manual trigger, retry or repair.

No deployment, task/settings change, operational restart, manual collector run, direct SGL request, record repair, existing workflow change, new agent or AI monitor occurred in this turn. The existing enabled collector remains on its existing five-minute task. No new log records are expected until verified deployment.

## Renewed approval: blank fix passed; release review blocked deployment

Owner separately approved the precise blank-field correction and completion of tests/deployment/normal-run proof. At approximately 11:45 Eastern on 16 October, normalized both actual and expected null/undefined/empty-string cells in source run-logs.js. Added coverage for Airtable-omitted blank fields and rejection of corrupted populated row counts. Six focused tests PASS; full private suite 70 PASS, 0 FAIL. Exact output: isolated source wef-schedules/run-logs-test-results.txt. No tests or business runs were executed against live SGL.

The applicable requesting-code-review skill explicitly required a read-only reviewer; dispatched one ephemeral review subagent, which made no edits/live calls and completed. Its material P1 finding is confirmed by reading the existing collector-core.js transport: airtableCreateRecords lines 916-982 calls fetchWithRetry at line 932; lines 774-824 retry network timeouts and HTTP 429/5xx. If a create committed before a timeout, retrying POST can create two run-log records with the same run-id. Logger readback then rejects ambiguity and leaves duplicate Running records. Near capacity, this can also exceed 1000 rows. Current tests replace the Airtable API above its transport and do not prove this case.

Release review therefore blocks deployment despite all 70 local tests passing. No code correction was made after that finding. Proposed smallest correction for separate owner APPROVED TO EDIT: make only the new logger's record-creation operation a single POST attempt, followed by read-only lookup of its stable run-id if the outcome is uncertain; never retry that POST. A committed timed-out request can be recovered by its run-id, while an unresolved outcome fails explicitly without duplicate creation. Add transport-boundary fixtures for commit-then-timeout, definite failure and ambiguous readback. Do not alter shared/private collector-core.js retry behavior or any existing collection transport. Existing idempotent patches/readback and archive-before-slot-reuse remain intact.

No deployment or task/settings changes occurred. run-logs table remains empty as read through connector during this turn. The existing scheduled collector continues unchanged; no manual run, restart, repair or repeat verification was attempted. Resume only after the separate approval required by the global post-failure rule; then finish release review/tests, scoped deployment and two normal scheduled-run checks.

## Owner scope correction: every run and every archive — 20 October

Owner requires run-logs to document every collector run and every archive. Connector read confirms run-logs still has zero records; the proposed logger is not deployed. Existing current-store.js writes initial-airtable, datasets, attempts, removed and retired archives; the proposed logger also archives completed log records before slot reuse. A run-only summary does not satisfy the corrected scope.

Concrete release requirement: each acquired collector run gets a readable Airtable record with actual start/completion, result, show/date scope, observed row counts and errors. Its archive details must identify every archive actually created during that run: kind (initial snapshot, dataset, attempt, removed records, retired show or retained-log archive), exact local path, affected show/date scope, record count and verified outcome. Preserve archive-before-cleanup ordering and distinguish archiving from subsequent Airtable removal. Include archive failures without claiming successful cleanup. Existing verified archives reused without a new write must be identified as reused if mentioned, not reported as newly created. Log retention archives must be included without recursively generating more archive-log records; attach their details to the owning run. A process terminated mid-run remains visibly Running/incomplete; no fabricated completion. Document inability to report into Airtable during Airtable outages or before process startup.

Use the same run-logs table and existing private collector/archive code; no changes to heartbeat, epoch, pipeline or schedule rules. Add readable archive detail/count fields only if needed for this approved outcome. Preserve previous-show files and do not restore or backfill historical business records manually. Two normal scheduled runs are required after local tests and release review; archive-event behavior not naturally observed may be fixture-proven only and must be labeled accordingly. Existing creation-timeout duplication finding remains unresolved, and the global separate APPROVED TO EDIT requirement still blocks implementation/deployment after that finding. This scope record is documentation only; no code, schema, business-record, task or runtime-checkpoint change was made on 20 October.

## Renewed owner approval and deployment — 20 October, 09:18 Eastern

Owner explicitly approved finishing every-run/every-archive logging, duplicate creation correction, local tests, deployment and two normal scheduled runs. The historical blocked statements above are superseded by this approval and release; their failure evidence is retained.

Implemented single-attempt POST for new run-log creation, with read-only stable-ID lookup after uncertain response; never retry POST. Existing collector-core and its shared/request behavior remain unchanged. Normal task entry explicitly installs logger after lock acquisition. Added observed row count to run reports, plus archive callback in the existing private current store. Verified archive creation/reuse emits kind, timestamp, show/date scope, record count, path and SHA256 into the owning run's readable archives field. Cleanup success/failure updates that same file entry. Archive failure prevents cleanup; existing archive ordering and schedule/manual/drop protections remain intact. Added archive-count and archives fields to run-logs only.

New regression checks first failed for absent behavior, then passed. Full private suite: 75 PASS, 0 FAIL. Covers explicit/omitted blank cells, populated corruption, committed-create timeout recovery, uncommitted/ambiguous outcomes without POST retries, readable run summaries, failed discovery, bounded archive-before-slot-reuse, archive scope/count/hash, reuse, retirement cleanup and blocked deletion on archive failure. Read-only release reviewer reported no material findings. No new dependencies.

Deployed only collect.js, store.js, current-store.js, run-logs.js and their two changed test files, with separate source/deployed manifests. Preexisting source/deployed standalone-test difference preserved. All changed-file hashes and protected-source hashes verified. No runtime environment/data overwrite, task definition/setting change or manual run. Exact deployment time and hashes: wef-schedules/runtime/run-logs-release-20261020/deployment.json. Existing normal triggers expected 09:20 and 09:25; acceptance remains pending until observed. Prior 09:00 interruption is unrelated, retained and not retried manually.

Retention: latest 1000 acquired runs remain browsable in Airtable (roughly 3.5 days at five-minute cadence); exact older completed log records are verified in private history/run-logs before slot reuse, with that retention archive documented in its owning new run. Interrupted Running records are never overwritten. Only actual locked collector starts are logged, not skipped scheduler triggers. Airtable outages or failure before the logger begins can only be reflected in the existing local runner log; no fabricated completion or retrospective backfill is performed.


## Run/archive logging verified - 20 October 2026, 09:26 Eastern

Airtable run-logs is live: one record per actual collector run, readable per-day rows/changes/warnings/errors and incremental archive details (kind, show/date scope, count, file, SHA256 and cleanup outcome). Normal triggers 09:20:01/09:25:01 exited 0, completed 09:20:48/09:25:44. Exactly two distinct run records; four verified attempt archives each. Both report 435 active rows, all four October 22-25 dates complete, no time errors/unmatched rows and only the empty-October-22 warning. All 435 schedule IDs and values remained unchanged; no duplicate log rows or schedule growth. Local suite 75 PASS; release review no material findings. Single-attempt log POST avoids duplicate-create retries; uncertain outcomes use read-only stable-ID lookup. Existing heartbeat/epoch/pipeline/collector task definitions and protected sources unchanged. No manual runs or repairs.

Latest 1000 runs stay in Airtable; older completed logs are verified locally before slot reuse, with that archive noted on the new run. Existing previous-show archives are preserved locally, not retroactively imported. Retirement, failure and retention branches are fixture-proven, not live-observed during these two unchanged runs. Logging cannot report to Airtable when Airtable itself is unavailable or process startup fails; existing local log retains available errors. Do not repeat release implementation/acceptance. Evidence: runtime/run-logs-release-20261020/deployment.json and two-runs-airtable.json.

