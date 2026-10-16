> SUPERSEDED FOR ACTIVATION: see [current schedules-only plan](wef-schedules-only-activation-plan-20261015.md). The historical heartbeat/consumer cutover below is not authorized or current.

# Schedule collection: task ownership, nonblocking staging and cutover plan
Date: 2026-10-15. PLAN ONLY. No code, scheduler, configuration, business-data or schema changes.
Supersedes shared collector/consumer lock as the proposed deployment arrangement. Existing isolated implementation remains locally tested but does NOT yet implement this new staging boundary.

## Verified task wiring and execution evidence
Both actions point to C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus, not the isolated implementation worktree.

### epoch-tagger-local
Enabled; Ready; 5-minute repeating trigger; IgnoreNew applies to this task only; 72-hour execution limit.
cmd.exe -> run_tagger_task.cmd -> run_tagger_task.ps1.
The script resolves the selected show. Multiple focused shows can dispatch run_tagger_task_focused_shows.ps1, which invokes the same pipeline per selected show.
Sequential pipeline:
1. tagger.js
2. heartbeat_patterns.js
3. schedules_dailyv2.js
4. schedules_calculatorv2.js (deferred failure)
5. trips_dailyv2.js
6. trips_tagger.js
7. trips_calculatorv2.js (deferred failure)
8. live_groups_daily.js (deferred failure)
9. publisher.js, only if preceding required/deferred checks permit.
It does not use the slot orchestrator's lock or slot selection. No common pipeline lock was found in this wrapper.
Observed last run: August 1, 2026 01:13:54 America/New_York; result 1. Tail of runner-pipeline.log agrees: schedule collector failed and pipeline stopped. Scheduler still supplies a future NextRunTime; that is not evidence a new run occurred. Why this enabled task has no recent execution is NOT established and does not require starting it to resolve this plan.

### ringstatus-heartbeat
Enabled; Ready; currently 3-minute trigger; IgnoreNew; 72-hour limit.
PowerShell -> run_tagger_heartbeat_lane.ps1 -> runner_pipeline_common.ps1 Invoke-RunnerPipeline.
Sequential outer steps: tagger.js -> heartbeat_patterns.js -> heartbeat_task_cadence.js -> heartbeat_slot_orchestrator.js.
heartbeat_task_cadence.js can update this task's interval from heartbeat controls; 3 minutes is the observed setting, not a permanent promise.
The orchestrator may launch a detached worker, guards its own orchestration with heartbeat-slot-orchestrator.lock, and applies mode, date, slot and environment gates. A successful outer wrapper does NOT prove all inner lanes succeeded.
Default eligible lanes (effective environment can override slots):
- schedules_dailyv2: DAY B/D, NIGHT A/C.
- schedules_calculatorv2: DAY A/B/C/D.
- trips_dailyv2: A/B/C/D; trips_tagger and trips_calculatorv2: A/C with upstream gates.
- sgl_browser_enrichment: explicit schedule scrape request; OOG on A/C or explicit trips scrape request.
- live_groups_daily, live_rings_daily, live_class_detail: DAY A/B/C/D (class-detail can be disabled).
- publisher: A/B/C/D only when upstreamOk.
- WEC branches are opt-in, off by default; keep their existing configuration untouched.
Observed last run: October 15, 2026 10:36:01 America/New_York; task result 0. Fresh runner-pipeline-heartbeat.log confirms outer completion at 10:36:27. This is invocation/wrapper evidence, not full downstream acceptance.

### Actual overlap risk
Both configured pipelines own tagger, heartbeat patterns, schedule acquisition/calculation, trips acquisition/tagging/calculation, live groups and publishing. IgnoreNew does not serialize different tasks. The orchestrator lock cannot protect against the legacy direct pipeline. Fresh simultaneous execution was NOT proved.
At future approved cutover retain ringstatus-heartbeat as the sole pipeline owner. Disable, do not delete, epoch-tagger-local after confirming no active legacy process; retain its export for rollback. Leave the already disabled ringstatus-pipeline-local disabled. Check other task actions for these exact entry points before enabling a new writer. Do not blindly kill an active process.

## Resolve collector/heartbeat overlap
Simply removing the shared lock is unsafe: the current isolated collector changes watch_schedule in batches and invalidates its receipt at start. A consumer could see half of a new collection. Offsetting start times cannot guarantee separation.

Proposed smallest durable boundary using existing collection and write logic:
1. One collector task fetches/normalizes the manual focus day and next day using existing source, fallback and key code. It writes STAGING ONLY.
2. Preserve the last complete dataset for each show/day while the next collection is running. New/incomplete/error rows never replace that completed selection.
3. After all stage rows are confirmed saved and read back, mark that per-day collection complete. Today may complete even if tomorrow fails.
4. The single heartbeat owner selects a completed dataset once at the start of its schedule/control step. It does not acquire the collector lock or wait for acquisition. A newer dataset completing mid-step waits until the next normal heartbeat.
5. Reuse existing key matching, manual override/start-time guards, source diff and lifecycle helpers to apply that selected dataset to runtime watch_schedule. Apply only changed source values. Read current runtime rows before applying, so existing manual overrides/record IDs/links survive.
6. Continue the existing heartbeat scope/link lifecycle before dependent lanes run. This control step remains synchronous, as today; source fetching is removed from its critical path. Existing live overlays must retain precedence using the runtime base and existing helpers, not copied staging record IDs.
7. While collection runs or fails, continue with the last successfully applied same-show/day runtime data and record collection health separately. No new schedule_downstream_blocked solely because the collector is busy. On unchanged datasets, skip source promotion but keep required heartbeat relinking/control.
8. No previous usable data for the selected day, malformed completed data, or failed runtime application is still a real dependency failure. Preserve those specific failure gates; do not fabricate success or stop unrelated lanes. A stale collection is reported with its age; this proposal does not silently remove existing freshness requirements or choose an unlimited acceptable age. Final freshness threshold must be recorded at cutover.

A collector-only lock plus Task Scheduler IgnoreNew prevents duplicate acquisition. Existing orchestrator lock remains. No new queue, service, Airtable automation, continuously resident agent or second downstream writer.
The new task should initially retain the agreed acquisition cadence, specified separately from heartbeat cadence; 5 minutes is a candidate from the legacy trigger, not an effective cadence proven here. Offset is optional load smoothing, not correctness protection.

## New Airtable base proposal
Working name: wef-schedules. Base ID not created or assumed.
Only TWO operational staging tables initially; no duplicate users, trips, Timing, live tables, or profile/control system.

| Table | Purpose | Minimum fields |
| --- | --- | --- |
| collections | One show/day acquisition attempt and its completion evidence | collection-id, show-id, date, status (running/complete/failed), started, completed, dataset-id, row-count, fingerprint, message |
| schedules | Normalized source rows for a completed or in-progress dataset | stage-key (dataset-id + existing schedule_key), dataset-id, schedule-key, show-id, date, existing required source fields only |

Keep existing machine schedule keys unchanged. A stage-key distinguishes staging versions; it must not replace the operational schedule_key.
Reuse the audited source-field inventory rather than copying 245 watch_schedule fields. Do not copy runtime heartbeat/current-scope/calculated fields or cross-base Airtable record IDs into staging as usable links.
A completed dataset is immutable. A changed collection creates a new dataset; an unchanged normalized source fingerprint references the previous dataset rather than rewriting all staged rows. Fingerprint covers actual source values, not collection timestamps. Never mark a collection complete before its referenced rows are fully confirmed.
Keep the currently selected completed dataset and its predecessor plus any in-progress dataset until consumption is confirmed. Record a simple retention/cleanup rule before unattended use; cleanup is not authorized now and must never delete a dataset in use.
Existing raw payload archive remains available locally using the proven code; no new attachment/upload pipeline.

### Ownership and integration
- Existing show/heartbeat controls in apptdhhNzduxm5gjn remain authoritative: heartbeat enabled, focus_day, customer, show end, existing manual fallback inputs. Do not introduce a second editable focus_day in the new base.
- New base is collected source evidence, not a new place to edit operational overrides. Existing manual_time_override and owner/downstream fields stay in runtime watch_schedule and are checked at application time.
- Normalized source-owned fields remain source-owned; no claim that arbitrary manual source-field edits are protected without an existing override rule.
- Use explicit staging-base/table configuration on staging adapters only. Existing runtime adapters keep apptdhhNzduxm5gjn. Do not globally replace AIRTABLE_BASE_ID: runner_pipeline_common.ps1 currently sets it to legacy, and other reads/links depend on it.
- Resolve shows/live_groups links against the existing runtime base using existing lookup helpers. Airtable record IDs are not portable across bases.
- Keep all downstream table names, record IDs, views and consumers unchanged. No Airtable sync/automation bridge is required. The existing Node/Airtable path performs the controlled application.
- Two operational tables do not replace the separate base-guide registration/resume documentation expected for a new base; that documentation is not a runtime dependency.

## Exact proposed changes after approval
- schedules_standalone.js: change its destination from runtime writes to explicit stage adapters; preserve complete datasets while recording new attempts; collector-only lock. Reuse fetch/normalization; no second SGL implementation.
- schedules_dailyv2.js shared per-day path: expose the existing source-preparation/application boundary so acquisition can stage and heartbeat can apply without refetching. Reuse field guards/diff/lifecycle; avoid a second copy of those rules.
- Receipt consumer: read selected completed staging dataset, not a running-attempt receipt/shared lock; retain per-show/day applied evidence and source-result validation. Keep generation choice stable for a whole application.
- Existing heartbeat orchestrator: retain sole lane ownership and due checks; its schedule step consumes/applies rather than acquires. Collection health must not be conflated with runtime application failure.
- Thin collector wrapper: reuse safe environment/logging conventions, but do not invoke all of Initialize-RunnerDefaults or repoint global AIRTABLE_BASE_ID; explicitly pass the two base roles. No scheduler registration until separately authorized.
- Scheduler at approved cutover: retire duplicate legacy trigger, verify old processes finished, enable exactly one collector task; preserve heartbeat/control and downstream scheduling.
No code changes made for this revision. Previously passing local tests cover the earlier same-base implementation, NOT these proposed staging changes.

## Verification before and after cutover
Offline fixtures before activation: heartbeat during a held-open collection uses prior complete data; unfinished/failing staging never changes runtime; current-day completion independent of tomorrow; changed and unchanged generations; manual edit made after staging but before application survives; exact runtime record IDs/links; promotion failure reports failure; restart does not select partial data; date rollover; explicit base routing; downstream source/control ownership.
Normal scheduled proof after separate approval: record task execution timestamps and script hashes; first staged dataset saved/read back; next heartbeat applies that dataset and emits actual consumer outcomes; at least two subsequent unchanged runs preserve IDs/manual fields and avoid unnecessary writes. Observe a naturally overlapping collection/heartbeat and a manual focus-day rollover. No manual collector run, endpoint call or record repair counts as proof.
Inspect inner orchestrator lane events, not Task Scheduler result 0 alone. No entries means Timing remains unproved, not failed or complete by inference.
Bootstrap safely: stage first while legacy behavior remains active (stage writer does not touch runtime), then switch one heartbeat consumer only after a matching completed dataset exists. Disable duplicate legacy trigger before runtime cutover; never leave two runtime schedule writers. Rollback stops collector/consumer cutover and restores exactly one prior pipeline, preserving records. No automatic rollback or task changes authorized.

## Sources inspected
run_tagger_task.cmd; run_tagger_task.ps1:1-30,240-287; run_tagger_task_focused_shows.ps1; run_tagger_heartbeat_lane.ps1; runner_pipeline_common.ps1:Initialize-RunnerDefaults,Invoke-RunnerPipeline; heartbeat_task_cadence.js:17,75,140; heartbeat_slot_orchestrator.js:31-40,682-738,1761,1888-2040.
ScheduledTask settings and ScheduledTaskInfo read-only snapshots, October15 2026; bounded tails of runner-pipeline.log and runner-pipeline-heartbeat.log.
Isolated implementation: C:/Users/gombc/.codex/worktrees/wef-schedule-collector/ringstatus/schedules_standalone.js and schedules_dailyv2.js. Existing plan: wef-standalone-schedule-collector-plan-20261015.md.
