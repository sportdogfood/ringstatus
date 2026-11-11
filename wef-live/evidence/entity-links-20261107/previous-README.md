# Current release — Records alerts removed

**Version 2026.11.07.11 · Deployed 9:14 p.m.; verified and locked November 7, 2026, 9:15 p.m. Eastern.** Owner instruction: remove Records alerts. Live's notification wrapper and CLI sender are removed: restore the exact proven pre-alert entry.js and remove records-alerts.js. Records alert integration and verification are cancelled, not repaired or verified. No replacement notification channel, schedule or observer. Keep historical failed delivery receipts/evidence untouched.

**Cleanup remains:** completed heartbeat/run-logs/errs-log cleared at the daily 7 p.m. Eastern event, including completed run/diff rows; 1,000/oldest500 checked every three hours; checked owned records cleared on a new canonical show. Protect active/current records. Actual actions stay in native cleanup/cleanup-logs. No separate runs table exists in this base.

**Verified cleanup:** the normal 9:03–9:08 p.m. invocation caught up the missed November7 daily event and deleted655completedheartbeat,500run-log and889error-log records. Native cleanup/cleanup-logs readback shows all three actions Success. Two incomplete Running heartbeat records were retained; run-logs/errs-log both0 at9:10. The normal9:09invocation then reported cleanupSkipped, proving no repeated daily clear. The independent three-hour clock stayed9:00p.m.; midnight is not another daily event. Newshow/threehourcases independentlytested; no fake operationalevent was generated.

Independent removal review and four mocked entry checks passed. Prior independent24cleanupchecks reused by exact source hashes; not rerun. Only entry.js restored and the sender helper deleted;25otherJS and task unchanged. No manual collector/sender, record repair, global Records setting or other workflow change. Post-removal normal **9:15 p.m.** run completed: status true, existing date warning logged, zero stage/logging errors, task exit0. Cleanup correctly reported not due, with no repeated daily deletion. No alert module or sender was invoked. [Acceptance](evidence/remove-records-alerts-20261107/acceptance.json). [Independent removal review](evidence/remove-records-alerts-20261107/review.md), [earlier cleanup review](evidence/cleanup-7pm-3hour-20261107/review.md), [installed26JS](evidence/remove-records-alerts-20261107/deployed.json).

Earlier Records requirements/failures below are historical and superseded by this removal. No Records alert or acknowledgement remains a completion requirement.

# Current cleanup rules — 7 p.m. / three hours / new show

**Version 2026.11.07.10 · Deployed November 7, 2026, 9:01 p.m. Eastern.** Owner correction supersedes the midnight clear and every-invocation drain below.

- **Daily at 7 p.m. America/New_York:** clear eligible completed `heartbeat`, `run-logs` (completed runs and diff rows included), and `errs-log`. Active/current records stay protected. The event executes on the next available existing normal invocation; a missed event catches up once. Midnight is not a daily-clear trigger.
- **Every three hours:** check the same three logging tables. At 1,000 or more records, delete the oldest 500 eligible records once. This threshold is not an instantaneous maximum between checks. No per-invocation drain or continuation below 1,000.
- **New canonical show discovery:** clear the checked owned show records and eligible completed logs. References, catalogs, cleanup settings and cleanup history remain. Delete only, no archives or record-field backups.

Every planned action updates `cleanup` with the latest table summary and `cleanup-logs` with permanent table/trigger/run-ID/start/completion/result/deleted-count/error evidence. These tables are not cleanup targets. No separate schedule, observer, task/cadence/source/collection/link change or manual run/repair. Existing error stops and exact deletion checks remain.

Only `cleanup.js` and `airtable.js` changed; 25 other JS and the registered task remain unchanged. **Independent source review found no actionable defect and independently executed 24 offline checks successfully.** Scheduled acceptance and verification via Records alerts remain unverified. Existing Live-to-Records CLI delivery remains blocked by the desktop-owned writer; success cleanup receipts do not by themselves prove an alert was delivered. Records has been asked for the existing approved cleanup-alert contract; none has been returned. No new alert transport or schedule is invented.

[Independent review](evidence/cleanup-7pm-3hour-20261107/review.md), [independent executions](evidence/cleanup-7pm-3hour-20261107/reviewer-execution.json), [exact approved instruction](evidence/cleanup-7pm-3hour-20261107/owner-instruction.json), [installed hashes](evidence/cleanup-7pm-3hour-20261107/deployed.json). Earlier sections below are dated history where superseded.

# Current logging cleanup — verified

**Version 2026.11.07.9 · Verified and locked November 7, 2026, 8:51 p.m. Eastern.** The approved correction is deployed. Every existing normal invocation checks `heartbeat`, `run-logs` and `errs-log`. At **1,000 records**, delete the oldest eligible records in chunks of up to 500 per invocation, continuing subsequent normal invocations until **500 remain**. The final chunk may be smaller. Active/current records remain protected. Untriggered tables below 1,000 stay unchanged; 500 is the drain target rather than an instantaneous hard ceiling between checks.

**Normal-run verification:** Eleven scheduled cleanups from **8:18–8:49 p.m.** removed **5,408 run-log records**, reducing **5,908 → 500**. The last run removed 408 after the count was already below 1,000, proving continuation. Airtable cleanup-logs confirms all eleven successful actions. The **8:51 p.m.** normal invocation deleted nothing; native Airtable count remained **500**, and the draining state cleared. Current heartbeat657 and errs-log889 remained below the 1,000 trigger. No manual runs, deletions, record repairs, observers or separate schedule.

Only `cleanup.js` and `airtable.js` changed; 25 other JS files and the task definition are unchanged. Existing new-day/new-show delete-only rules, source collection, links, decoded pause and active/current protection remain. No per-table skipped-action spam was added on every below-threshold check. Sixteen offline checks and author review passed (not independent). Existing Records active-writer delivery failure remains unfixed and outside this correction.

[Acceptance and exact normal runs](evidence/log-cap-20261107/acceptance.json), [Airtable deletion readback](evidence/log-cap-20261107/airtable-readback.json), [repeat counts](evidence/log-cap-20261107/airtable-repeat-counts.json), [review](evidence/log-cap-20261107/review.md), [installed hashes](evidence/log-cap-20261107/deployed.json). General lock applies outside explicitly approved work. Older sections below are historical where superseded.

# Current Records route — active-writer conflict

**Version 2026.11.07.8 · November7,2026 8:06p.m. Eastern.** Narrow owner-approved investigation/correction deployed: only records-alerts.js changed to use the actual existing desktop Codex executable and retain redacted failure diagnostics. No install/upgrade/new transport. All26otherJS, collection/direct feeds/decoded pause/linking/cleanup/task unchanged. Six offline checks and author review passed; normal delivery did not.

**Normal proof FAILED**: connection-check alert `24ac8784-bfcc-46b6-992f-f700af949f51`, started01:06:03.380Z, completed01:06:03.958Z. CLI exited1: `thread-store conflict: thread 01a169bc-e58a-7de0-94c6-5d3931fcf745 already has an active writer`; `thread/resume` code-32600. The desktop owns the existing Records writer. Switching to the app's current executable did not resolve this ownership conflict. Original failed8:00receipt remains preserved.

**Automatic coordinator delivery is NOT working.** Collection/task still complete independently; task exit0 is not alert success. No manual sender/collector, lock removal, forced session change, retry or record repair performed. Stop this delivery verification path pending a supported submission route to the existing app-owned writer and applicable owner authorization; no new service/schedule/observer was added. Future genuine failure attempts remain wired to the same unverified CLI route. [Acceptance and exact failure](evidence/records-alert-route-fix-20261107/acceptance.json); [review](evidence/records-alert-route-fix-20261107/review.md).

# Current alert delivery — not verified

**Version 2026.11.07.7 · November7,2026 8:00p.m. Eastern.** Owner-approved notification wiring deployed: `entry.js` plus new `records-alerts.js`; all25otherJS, four direct request headers/URLs, decoded pause, collection/link/cleanup logic and task unchanged. Five offline checks and author review passed (not independent).

**Normal delivery FAILED:** the existing local Codex resume route exited1 without acknowledgement during the8:00p.m. normal invocation. Connection-check alert `65a538cc-d259-4571-aa21-2079db3205af`; target Records chat `01a169bc-e58a-7de0-94c6-5d3931fcf745`. Destination readback showed no new test message/acknowledgement. Task exit0 is not delivery success. Collection completed independently; quiet policy unchanged. No manual sender/collector or fabricated failure.

Failure notifications run after the normal invocation finishes, using its existing reports; they do not interrupt collection. Status/parser failures are included despite old ok:true/Skipped report labels; stage/logging, non-warning collection issues and cleanup errors included. Routine date warnings and linker no-match checks stay log-only. Individual delivery receipts in runtime/records-alert-*.json distinguish Started/Delivered/Failed. Legacy in-stage alert flags are not rewritten; these new end-of-invocation delivery receipts are separate. The first normal invocation makes one explicit connection check; it is never labelled a real collection failure and is not repeated automatically.

**Stop:** verification halted after the failed normal delivery. No transport retry/repair/new route or manual chat message performed. Future genuine failures remain wired to this unverified route; do not describe immediate or successful coordinator delivery as working. Owner decision required before correcting the failed route. Existing known trouble unchanged. [Evidence and acceptance](evidence/records-alerts-20261107/acceptance.json); [review](evidence/records-alerts-20261107/review.md); [installed hashes](evidence/records-alerts-20261107/deployed.json).

# Current release — four direct Wellington feeds

**Version 2026.11.07.6 · Deployed November 7, 2026 at 7:13 p.m. Eastern.** Owner-approved narrow request correction. Status verified by one normal scheduled run; groups/rings/class-details scheduled verification remains pending.

The four active public feeds now use direct Wellington URLs and exactly these explicit headers: `Accept: application/json`, `Cache-Control: no-cache`, the approved iPhone Safari User-Agent, and `Sec-Fetch-Site: cross-site`. Status uses `sglapi.wellingtoninternational.com/homepage/getLiveClassStatus`; groups, rings and live-class-details use `sgl.wellingtoninternational.com` at their existing paths/query parameters. This supersedes older all-four-proxy descriptions below. No cookie/token or extra browser headers are supplied by Live. Node may supply normal transport defaults.

Decoded `/classes/{id}` remains proxy-defined and **paused**. Only `activity.js` and `source.js` changed; the other24JS, shared source reader, registered task, cadence, status decisions/parsing, stage4–6 collection/save logic, empty-only linker, relationships and delete-only cleanup remain unchanged. No manual runs, record repairs or new observers.

Nine focused offline checks passed; author review passed (not independent). Exact baseline/candidate/diff, review receipt and installed26JS hashes are in [direct-four-headers evidence](evidence/direct-four-headers-20261107/). Normal scheduled acceptance remains separate from Decoder diagnostic proof. Existing known trouble/alert gaps remain unchanged.

**Normal-run proof:** November7 **7:15 p.m. Eastern**, status run `d602e778-0b69-45fb-b867-e41990d67d2d` returned **true**, with zero stage/logging errors. Existing date-mismatch warning recorded: Shows priority0 isNovember8, today isNovember7. Existing rule kept detailed collection idle and next status due8:15p.m. Three direct detail feeds remain **unverified through scheduled collection** until a naturally eligible day0 window. No forced runs/date changes. [Acceptance](evidence/direct-four-headers-20261107/acceptance.json).

# Current release — corrected w-shows / w-days references

**Version 2026.11.07.5 · Locked 2026-11-07T10:26:13-05:00 · America/New_York.** Narrow owner correction deployed and verified; no extra workflow changes.

- Linker now uses the existing synced **w-shows**, **w-days** and unchanged **wef-data.schedules**. The new `w-shows`/`w-days` fields on five Live tables point to those exact native targets. Former filled `show`/`show-day` fields remain intact as historical references; no destructive retargeting or record migration.
- **Class↔group is confirmed:** active native relationship is **live-classes.groups ↔ live-groups.live-classes**. Both directions are populated (sampleclass200031204/group200028523). No new relationship, duplicated field or class/group code change was needed.
- Empty links remain empty when there is no exact eligible match. Their individual logs now recommend **“No retry recommended until source data changes.”** This is an informational recommendation, not a new retry cache or eligibility gate. Misses use already loaded data; no individual source/network retries are attempted for them. Existing empty-only and Dropped-schedule rules remain unchanged.
- **Stages4–6, Data schedules, source requests, cadence and cleanup are unchanged. Decoded requests remain paused.** Existing null-as-empty handling remains unchanged. Only three existing configuration/advisory files changed;23otherJS/task preserved.10necessary correctly targeted fields were added without manual record writes.

**Verification:**17offlinechecks and author code review passed (not independent review). Normal **10:15 and10:21 Eastern** collection/link passes completed without stage/logging failures and with0decodedrequests. The first filled **1,936 w-reference fields** across968existing records;the second changed0records. All968records and5,197priorpopulatedforwardfields were retained;423trip business rows stayed frozen. Data links stayed961. Seven existing Dropped-only Data misses remained empty and individually logged. Class/group native backlinks verified before and after. No extra checks, tasks, source probes, manual runs or record repairs.

Evidence: [acceptance](evidence/w-shares-20261107/acceptance.json), [review](evidence/w-shares-20261107/review.md), [17checks](evidence/w-shares-20261107/offline.json), [deployed26JS](evidence/w-shares-20261107/deployed.json), [w-field targets](evidence/w-shares-20261107/schema-readback.json), [class/group pair](evidence/w-shares-20261107/class-group-schema.json), [locked baseline](evidence/w-shares-20261107/locked-baseline.json). Existing known-trouble/holds below remain. Older sections describe dated history where superseded. No revert was needed;the reviewed [code-only revert](evidence/w-shares-20261107/revert.ps1) retains all schema/data/validlinks.

# Current release — empty-only linker

**Version 2026.11.07.4 · Locked 2026-11-07T10:06:03-05:00 · America/New_York.** Owner-approved linker deployed and verified; unrelated work stays locked.

- **Stages 4–6 stay unchanged.** Groups, rings and live class details collect/save as before. Status uses Shows' published Eastern time and priority-0 date. Source URLs, headers, three-minute/hourly rules, task and cleanup remain unchanged.
- **Stage 7: `linker.js` runs after collection finishes.** It fills empty native fields only: `show` → synced **wef-shows.show**, `show-day` → synced **wef-shows.show-days**, and `data-schedules` → synced **wef-data.schedules** on all five Live tables. Fifteen missing fields were added; no records were manually written. Existing internal forward links are reused. Airtable maintains reciprocal links automatically. Populated forward fields are not audited, corrected or rebuilt. Old WEF Schedules links stay retained; the linker never reads/writes that target.
- **Exact saved keys only.** Use show/day/ring/group/class/trip IDs and Data's explicit ring/group/class keys, with source ring-ID mapped to ring-number. No names, invented IDs or forced joins. Existing Dropped-schedule exclusion stays. Missing/ambiguous matches remain empty and are individually logged; other links/collection continue. No new readiness gates, tasks or schedules.
- **Decoded-trip requests remain paused.** Empty native links on existing trips can be filled by the approved linker, but decoded identities, results, membership states and other business values remain frozen. No new decoded runner or automatic resume.
- **`completed:null` is handled as an empty list.** Original source evidence, ring record and other links remain. No null-format error, payload correction or waiting condition. Natural null occurrence is not claimed.

**Normal verification:** November 7 **09:51 and 09:57 Eastern** runs passed without stage/logging failures. Both collected35groups/71appearances,7rings,66classes and408live-detail rows; decoded requests0. First linker filled **2,897 empty fields on968 existing records**:968show,968day and961Data-schedule fields. All968priorrecords and2,300populatedforwardfields were retained. All423pausedtrip business records remained unchanged. The second linker wrote **zero fields/records**. Native heartbeat/run-log and one-per-skip errs-log readback confirmed both runs.17offlinechecks and author code review passed (not independent review).

**Seven known skips:** only Dropped Data schedule occurrences match retained groups **200028530 /200028533**, class **200031158**, and its two trips/twoentries. Seven `data-schedules` fields remain empty; show/day and existing native links remain. Each skip is logged, not guessed or repaired. These are not collection stops.

**Revert:** no rollout failure occurred. Reviewed [revert.ps1](evidence/linker-20261107/revert.ps1) restores only the three prior changed JS files and removes the new linker after the normal task finishes; intervening owner edits are protected. Retain added schema/data/valid links, never repair records or manually rerun. No new cleanup archives, observers, dependencies or runtime upgrades.

Evidence: [acceptance](evidence/linker-20261107/acceptance.json), [review](evidence/linker-20261107/review.md), [17offlinechecks](evidence/linker-20261107/offline.json), [deployed26JS](evidence/linker-20261107/deployed.json), [native field targets](evidence/linker-20261107/schema-readback.json), [locked baseline](evidence/linker-20261107/locked-baseline.json). Existing known-trouble/hold notes below remain; older releases are historical where superseded here.

# Current release — independent live details; decoded trips paused

**Version 2026.11.07.3 · Locked 2026-11-07T08:31:03-05:00 (America/New_York).** Owner-approved pause and Shows-time reuse are deployed. General lock remains outside these completed changes.

- **Groups, rings and class-live-details continue independently.** Class details no longer call or wait for decoded `/classes/{id}`. Existing four-class concurrency, source snapshots, changed-only saves, logs, canonical day-0 and cadence remain.
- **Decoded-trip requests and live-trips collection/state updates are paused.** All 423 existing day-0 trip records, read-back fields and native links remained unchanged through two normal runs. Existing legacy live-entries continue receiving live observations on their existing trip-based keys; known entry/entity identity columns and native links are retained. New observations do not invent missing entity IDs. A future separate decoded lane is discussion only; no new worker/task or automatic resume was introduced. Existing approved cleanup remains active.
- **Status uses existing Shows data:** canonical show-days priority-0 date and `eastern-time (from show-times)` supply the date/window decision. No new table/field/request/freshness gate. Missing lookup retains the existing Eastern-clock fallback. Actual invocation timestamps/next-check scheduling still use the host clock. The live-status endpoint still supplies true/false; true/today inside05:00-19:00 collects3minutes, failedstatus retries3minutes, confirmedfalse/mismatcheddate/outsidewindow checks hourly.
- **`completed:null` is handled:** treat it as an empty completed-group list, save that empty value, preserve the ring record and its other links, and continue without an error. Keep the original null in source evidence. The deployed implementation matches the passing source-shaped offline test. No payload correction or wait for a natural null is required; this is not an open fix. A natural null occurrence is not claimed as observed.
- All four active feeds use the existing shared `wef-schedules-decoded.gombcg.workers.dev` proxy: homepage/getLiveClassStatus, ListAjax, LiveScoreWidget and getLiveClassData. Routes/headers/proxy and all other collectors were untouched by this release. `/classes/{id}` remains defined for reference, with no active caller in Live's class lane.

**Verification:** Six focused pause/live-only/null/link checks and nine Shows-time/retry checks passed; author review, not independent review. Normal **07:36 and07:39 Eastern** runs completed without collection/logging errors:36groups,74class appearances,7rings,69class detail requests per run,405then407live-entry rows,**zero decoded requests**.423trip IDs/selected fields/native links remained unchanged by Airtable GET comparison. Normal **07:42 Status** used published `2026-11-07 07:39:13`, canonicalNovember7 and livetrue to collect at3minutes. Current25JS/task readback matches. No manual collector/provider calls, record repairs or observers.

Evidence: [pause acceptance](evidence/pause-decoded-20261107/acceptance.json), [pause review](evidence/pause-decoded-20261107/review.md), [Shows-time acceptance](evidence/shows-time-20261107/acceptance.json), [current deployed hashes](evidence/shows-time-20261107/deployed.json). Older dated sections below are historical where superseded. Stage8publication and old24h retention holds, delete-only cleanup rules and known reporting limitations remain unchanged.

## Known trouble and operating limits

These are recorded limitations, not new work authorized by this lock. Decoded trips remains the only open workflow design discussion.

| Item | Actual behavior / consequence | State |
| --- | --- | --- |
| Immediate coordinator-chat alerts | No alert transport is supplied to the scheduled runner. Stage failures are saved locally/in errs-log; the runner cannot claim immediate delivery to Records. Records' separate review is not a runner chat push. | Existing limitation; no transport added |
| Cleanup action rows left Running | Cleanup publishes all planned actions before executing them. A failure can leave later, unexecuted action rows marked Running. | Existing conditional P2; not fixed |
| Cleanup stop can leave stale summary | After an interrupted/failed cleanup, the stop-file branch saves its own receipt but does not refresh cleanup-last-run/Airtable summary. An earlier success may remain visible. Collection continues, but cleanup does not resume automatically. | Existing conditional P2; not fixed; no stop file present at this check |
| Empty-object ring response | A widget response of `{}` can return no ring rows without a warning because the current missing-show check only reports when data.length is truthy. Existing rings remain. Saved November6 evidence records this case; current feeds are populated. | Existing warning gap; no fix authorized here |
| Heartbeat show/date columns | The original three collection-lane heartbeat writes do not populate native show-id/date cells; the linker heartbeat does. Detailed run-logs and saved receipts carry scope. Skipped Status heartbeat rows do include scope. | Existing metadata limitation |
| Decoded identities and trip freshness | Decoded requests, live-trips updates and trip membership cycling are paused. Existing trips/results/links remain, but they are not fresh observations while paused. New live-detail rows cannot acquire entity/entry IDs absent from that source; existing known IDs are retained. | Deliberately paused, not a collection failure |
| Linker unmatched schedules | Seven retained records have only Dropped Data matches; Data links stay empty and skips are logged. Shows/day links and collection continue. | Known skips; no guessing |
| Three-minute interval | This is the dispatcher/check interval, not a promise that a whole collection finishes within three minutes. Existing Windows policy is IgnoreNew with a ten-minute execution limit; a running task can cause a tick to be missed. | Existing operating behavior; task unchanged |
| Shows clock | Status uses Shows' last published Eastern-time value, which can lag its update cadence. Missing/invalid lookup uses the previous Eastern host-clock fallback. Host clock still schedules actual invocations. | Existing documented fallback; no new gate |

**Resolved:** the direct-feed empty responses were overcome by the existing shared-proxy routing; current four feeds collect. `completed:null` is locally normalized to an empty completed list without a format error; original evidence, ring record and other links are preserved. This is implemented, reviewed and tested, not an open correction or a requirement for Wellington to change its payload. A natural null occurrence is not claimed.

**Verification boundaries:** the two populated 07:36/07:39 normal runs prove independent collection, zero decoded calls and retention of423trip records/selected fields/links. They do not exercise every future cleanup failure/threshold/new-show branch. No forced show change, manual run or repair is required by this documentation update. Historical same-date rollover/multi-group/publication evidence gaps remain historical unless separately verified; they are not reintroduced as collection gates.

**Holds:** cross-base publication (former stage8) and the old24hour archive/retention workflow remain disabled. Current cleanup is active and delete-only: new calendar day clears eligible completed logs; every3hours applies1000/500 to logging tables; new show clears owned show records and eligible logs; no separate show-day cleanup. Protect active/current logs; retain permanent cleanup-logs/catalogs/references and existing saved source evidence. No new cleanup archives or separate cleanup schedule.

# Current release — live-trips only

**Version 2026.11.03.4 · Updated 2026-11-03T15:20:58-05:00.** Owner correction removes unnecessary separate trip-result/message/comment tables. The simplified collector is deployed. General lock remains; scheduled operation continues.

- **live-trips** (`tblhUd5DucnpkEall`) remains the sole new trip table. Day-0 decoded class-source collection, keys, identities and existing native class/ring/group/schedule/legacy links are preserved.
- Results now stay **directly on each trip**: placing, position, results-verified, class-status and results-json. Original source-json retains all source facts. Changed-only saves continue.
- Separate **live-trip-results, live-trip-messages and live-trip-comments were empty and deleted**, with their three obsolete cleanup controls removed. Collector has no reads/writes/dependency on those tables. Airtable converted their former reciprocal columns to empty plain text; no broken table links remain.
- Downstream results/messages/comments can reference the source trip-key. No new local child tables or messaging/comment workflow. Existing live-entries remains untouched and continues its existing collection.
- **Cleanup:** live-trips stays in new-show deletion. New calendar day clears eligible completed heartbeat/run-logs/errs-log; every3hours checks1000/500; no separate show-day action. Active/current/diff logs, action history and references stay protected. Delete only, no new schedule/observer. Records supplies alerts.

**85 offline checks passed; author review completed, not independent review.**25installed runtime hashes match candidate;21other JS and Windows task unchanged. Direct result fields and empty table/config removal were GET-read back. No manual collector/provider runs, business-record repairs or backfill. Source currentlyfalse, canonical day0November5; populated verification remains natural, not forced.

[Review](evidence/live-trips-simple-20261103/review.md) · [Deployment](evidence/live-trips-simple-20261103/deployed.json) · [Schema readback](evidence/live-trips-simple-20261103/schema-readback.json). Normal scheduled proof recorded separately. Stage8/old24hourholds and known conditional cleanup-reporting P2s remain unchanged.

**Normal scheduled verification:** November3 **3:21 and3:24p.m.Eastern** wakes completed exit0. Quiet collection/cleanup correctly skipped; exactly one live-trips cleanup control remains.25runtime hashes and task match. New class-source population remains naturally pending while live=false;85offline checks passed. [Acceptance](evidence/live-trips-simple-20261103/acceptance.json).

## Earlier dated documentation — historical where superseded above

# Current release — day-0 live-trips

**Version 2026.11.03.3 · Updated 2026-11-03T14:57:59-05:00.** Owner-approved addition deployed within wef-live. General lock remains; this approval changes only the documented trips/result/message/comment structure and its existing new-show cleanup integration.

## Collection

The existing classes lane projects **live-trips** from the decoded `/classes/{class-id}?show_id=...&customer_id=15&cgid=-1` response already fetched through `wef-schedules-decoded.gombcg.workers.dev`. No new endpoints, tasks, helpers, count/status/age gates or reverse Entries reads. UI live details supplement matching trips; unavailable/empty UI details do not suppress decoded source trips. Individual errors are logged and valid trips continue. Empty collections succeed. Existing Shows day-priority **0**, live-status/date cadence, three independent lanes and logging remain.

Each trip records show/day/class/ring and source trip ID/UUID, show-level entry-key, horse/rider/trainer/owner IDs and names, order/actual order, scratch/gone and source evidence. Missing source identities stay missing. Composite `trip-key` follows `customer|show|date|ring|class|entry|UUID` with numeric trip-ID fallback. `entry-key` is `show|entry`, allowing many class trips/riders across one horse's show entry. Existing **live-entries** records, keys, links and collection writes remain; retirement is not part of this release.

## Tables and links

| Table | Airtable table ID | Purpose |
| --- | --- | --- |
| live-trips | tblhUd5DucnpkEall | Individual day-0 source trips; native class/ring/group/available schedule and exact legacy-record links |
| live-trip-results | tblPIZYbHp8850uoA | Each trip's source result facts, scores/times/faults/placing and verification state |
| live-trip-messages | tblUbF9tygLGChbM4 | Messages directly related to an individual trip |
| live-trip-comments | tblg29QWHi5GB8p4X | Comments directly related to an individual trip |

Child tables link directly to their trip; parent reciprocal fields are **results, messages, comments**. Message/comment authoring and delivery belong to downstream owners; this collector does not invent or send them. Entity/show-entry source IDs/keys allow downstream linking without a new Live dependency on helper/Entries bases. Source show/date are direct fields; existing class/schedule links provide related context.

## Cleanup and protection

The four new tables are checked in existing **cleanup** controls and included in new-show deletion, child tables before trips. **New show clears all checked owned show records.** New calendar day clears eligible completed heartbeat/run-logs/errs-log; every 3 hours checks those log tables and deletes oldest500eligible at1000+. No separate new-show-day cleanup. Active/current/diff logs, permanent action history and reference/catalog tables remain protected. Delete only; no cleanup archives, new schedule or observer. Records supplies alerts. Existing source-evidence storage is unchanged.

## Verification

**85 offline checks PASS; author review PASS, not independent review.** Four table schemas,8trip links/reciprocals and4cleanup control rows were GET-read back. Installed25JS hashes match candidate;21existing JS and the entire scheduled task are unchanged. [Review](evidence/live-trips-20261103/review.md) · [Schema readback](evidence/live-trips-20261103/schema-readback.json) · [Deployment](evidence/live-trips-20261103/deployed.json).

Normal scheduled proof is recorded separately as available. Source currently reports live=false and Shows day0isNovember5; idle checks do **not** prove populated trip/result/link coverage. No manual runs, forced source/date, historical backfill or record repairs. New-show cleanup with populated new tables also awaits a natural event. Stage8publication and old24hretention remain held; existing two conditional cleanup-reporting P2s remain separate.

**Normal scheduled verification:** November3 **3:00 and3:03p.m.Eastern** completed exit0. Actual status check returnedfalse, no errors; heartbeat/run-log Skipped/completed read back. Following quiet wake and cleanup correctly skipped; four cleanup controls confirmed. New tables are empty pending natural day0activity; populated trip/result/native-link and new-show deletion coverage remain unverified. [Exact acceptance](evidence/live-trips-20261103/acceptance.json).

## Earlier dated documentation — historical where superseded above

# wef-live — locked

**Version 2026.11.03.2 · Updated 2026-11-03T10:04:48-05:00.** Revised cleanup is deployed and locked. Further code, schema, cadence, cleanup-policy or deployment changes require explicit owner approval. Normal scheduled operation continues.

## Cleanup rules

- **New calendar day (America/New_York):** clear completed eligible records from **heartbeat, run-logs and errs-log**, regardless of count.
- **Every 3 hours:** check those three logging tables. At **1,000+ total records**, delete the **oldest 500 eligible completed records**. A full calendar-day or new-show clear restarts this interval.
- **New show:** clear all records in the checked owned **live-rings, live-groups, live-classes, live-class-appearances and live-entries** tables, plus completed eligible logging records. Show records are cleared regardless of their show ID.
- **New show-day:** no separate cleanup action.
- Protect **active/Running logs, current invocation logs and their diff children**. Retain cleanup-logs as the permanent action history, along with catalogs and reference tables.
- **Delete only:** no cleanup archives or deleted-field backups. Existing historical files remain untouched.
- Cleanup uses the **existing wef-live-collector task**. Quiet wakes evaluate local due state; no separate schedule or observer. **wef-records-coordinator supplies alerts.** Existing errs-log/local error and stop behavior remain.

## Verified release

**75 offline checks passed.** Author review completed; this was not an independent review. All **24 installed runtime JS hashes** match the reviewed release; **21 JS files** and the Windows task definition remain unchanged.

Normal scheduled runs on **November 3 at 9:06 and 9:09 a.m. Eastern** completed with exit 0. The 9:06 three-hour check found **231 heartbeat / 498 run-logs / 1 errs-log** records. All were below 1,000, so it recorded three Skipped actions in Airtable with **zero deletions, archives or errors**. The 9:09 run correctly skipped cleanup and preserved the checkpoint and action IDs.

Natural new-calendar-day full clears, new-show full clears and actual 500-record deletion under these revised rules remain unverified operational branches; they have offline coverage. Two preexisting conditional cleanup-reporting P2 findings remain in the review. Stage 8 publication and old 24-hour retention remain disabled. Collection cadence, other collectors and existing links are unchanged by this documentation lock.

[Acceptance](evidence/cleanup-revision-20261103/acceptance.json) · [Review](evidence/cleanup-revision-20261103/review.md) · [Deployment](evidence/cleanup-revision-20261103/deployed.json) · [Lock baseline](evidence/lock-20261103/baseline.json) · [LOCK.md](LOCK.md).

## Earlier dated documentation — historical where superseded above

**LOCKED — November 2, 2026 · documentation version 2026.11.02.13.** Further code/workflow changes require explicit owner approval. Normal scheduled collection, logs and delete-only cleanup continue. This is a documented freeze; no file permissions or Airtable permissions changed. See [LOCK.md](LOCK.md) for the exact baseline and remaining verification gaps.

# Current Live release — delete only

Version **2026.11.02.12** · Updated **2026-11-02T12:58:44-05:00**.

The owner explicitly approved corrections and redeployment. Installed source hashes and existing task definition were read back successfully. This supersedes prior archive-era cleanup and pending-deployment statements below; old dated evidence remains history.

- **New show:** delete nonmatching-show records in checked owned tables. No new cleanup archives or deleted-field backups.
- **New Eastern calendar day only:** check heartbeat/run-logs/errs-log. At1,000+ records delete the oldest500 completed eligible rows; protect active/current invocation rows. Same show/day performs no cleanup table inventory or deletion.
- **Logs:** cleanup-logs records table/action, start, completion, result, show IDs, deleted count and error. Old archive fields remain unused/zero for new actions. Local cleanup receipts contain action/count metadata only. Existing historical archives/catalogs are left untouched.
- **Cadence:** actual live-status checks and registered runs are hourly around the clock when live=false, priority-0 date differs from today, or outside05:00–19:00Eastern. True/date mismatch adds an errs-log warning. True/today inside that window collects every3minutes. The existing hidden task wakes every3minutes24h as a local dispatcher; not-due wakes do not call providers or Airtable and do not create collection logs.
- **Protected behavior:** independent lanes, IDs/links, changed-only saves, exact pre-delete target/value checks, canonical-show recheck, DELETE response/absence readback, active records and permanent catalogs. Stage8 publication and old24h retention remain disabled. Other collectors untouched.

**Verification:**81 offline checks PASS; author review and review-packet verification PASS, not independent review. All24 installed JS hashes match the reviewed candidate;20 existing JS files unchanged. Windows normalized task duration PT24H toP1D and omitted default StopAtDurationEnd=false; semantic readback matches. No manual run, record repair or observer. Scheduled proof is recorded separately in evidence/delete-only-20261102/acceptance.json when available. No claim that future new-show/new-day deletion or populated activity has already occurred. Two known conditional cleanup-reporting P2 findings remain documented in the review.

Evidence: [release task](evidence/delete-only-20261102/TASK.md), [review](evidence/delete-only-20261102/review.md), [installed readback](evidence/delete-only-20261102/deployed.json), [tests](evidence/delete-only-20261102/offline-final.json).

**Scheduled readback:** November2,1:00p.m.Eastern check `0de37502-dff3-4d90-8d06-66c4e487c709` completed with task exit0. Source live=false; heartbeat/run-log both Skipped/completed, no errors. Cleanup correctly Skipped with zero actions on unchanged show/day. Next actual hourly check is2:00p.m.; second hourly check and future event deletion remain unverified. See [acceptance](evidence/delete-only-20261102/acceptance.json).

## Prior dated documentation — historical only

# wef-live — current correction

**Version: 2026.11.02.11**
**Updated: November 2, 2026, 12:48 p.m. Eastern.**

## Current status

**All agreed rules are coded and passed81 combined offline checks. They are NOT deployed.** The old cleanup handoff was withdrawn directly from Data/Ring because it checks log counts on every invocation, contrary to the agreed day-change rule. Use only [the corrected combined preparation](evidence/agreed-rules-20261102/PREPARED.md) for the next release; earlier preparations/proofs are dated history.

- Live false: hourly registered checks around the clock.
- Live true, today differs from Shows priority-0 date: hourly registered check and warning in errs-log.
- Live true, today matches: three-minute collection05:00–19:00 America/New_York; hourly checks outside that window.
- New show: archive/remove nonmatching-show records in checked owned tables.
- New Eastern calendar day: check only heartbeat/run-logs/errs-log; at1000+ archive/delete oldest500 completed eligible rows.
- Same show/day: no log-table inventory, archive, deletion or Airtable cleanup action.

Corrected standalone cleanup source: evidence/agreed-rules-20261102/candidate/cleanup.js, SHA2564c3e0415599ebb71ad7dd11b6b5fba58db75be9308228ef2e59ae8f025b50af6. New-day repeat and same-day1000+ no-read behavior are covered by offline tests. Old989-record scheduled acceptance does not prove these revised triggers.

Prepared change is three JS files and the existing task's24h dispatcher; no new task, schema, dependency or observer. Author review PASS, not independent review. Installed code/task remain the old implementation pending deployment approval. Existing two conditional cleanup-reporting P2 findings are unchanged. No manual run or record repair. [Combined tests](evidence/agreed-rules-20261102/offline.json) · [Review](evidence/agreed-rules-20261102/review.md).

## Previous dated documentation

The material below is preserved history; use the current state above.
# wef-live — cleanup release

**Version: 2026.11.02.10**  
**Updated: November 2, 2026, 11:36 a.m. Eastern.**

## Live-status/date cadence — edited and tested, not deployed

Owner-approved rules: live false -> hourly registered check; live true with today different from Shows priority-0 date -> hourly registered check plus errs-log warning; live true with today matching -> three-minute collection only05:00–19:00 America/New_York. Owner confirmed hourly checks continue overnight. True/matching outside the collection window also checks hourly without collecting. Unknown/unreadable status or canonical context is logged distinctly and skips detail collection until the next hourly check; it is never mislabeled false.

Prepared candidate passed **71 offline checks**, including transitions, Eastern date/DST/window boundaries, startup jitter, logging and collection/cleanup regressions. Status is requested once per actual due check and the validated scope/status is shared by the existing three lanes. The existing task is prepared for a24h quiet three-minute dispatcher; before due time it makes no network call or Airtable stamp. Actual inactive checks/runs are hourly. No new task, table, schema or dependency is introduced.

**Not live yet:** production JavaScript and installed task remain unchanged. Owner approved edit/test only. Prepared changes, exact baseline, task XML, author review and deployment/rollback notes: [live-cadence preparation](evidence/live-cadence-20261102/PREPARED.md). Deployment and normal scheduled proof are pending explicit approval. This cadence change does not implement the separately agreed cleanup trigger revision below or repair the two cleanup-reporting findings. Current cleanup behavior is preserved after due registered checks, including hourly skips.

## Agreed cleanup triggers — not yet implemented

- **New-show discovery:** archive and remove records not matching the new show ID in the checked owned tables.
- **New-day change:** check only heartbeat, run-logs and errs-log. For each table with 1,000+ records, archive and delete its oldest 500 completed eligible records, protecting active/current invocations.
- **Same show and day:** no cleanup. The three-minute interval remains the collection cadence, not a recurring cleanup action.

**Implementation status: NOT IMPLEMENTED for these revised triggers.** The current deployed code still invokes cleanup after every three-minute collection cycle and checks the 1,000-record threshold each time. It checkpoints show identity only; the new-day trigger is not implemented. Earlier tests and normal-run acceptance below prove the earlier implementation, not this revised agreement. No code, task or Airtable change accompanies this README update. Next implementation requires owner approval for the trigger change and normal scheduled-run verification; no manual runs or record repairs.

## Current deployed implementation and dated evidence

New-show cleanup and log-only quick cleanup are deployed in the existing hidden three-minute task, after the three collection lanes finish. Stage 8 publication stays on hold. The previous blanket stage-9 hold is superseded only for the two cleanup actions below; the old 24-hour retention path is not re-enabled.

- **New show:** read canonical default show ID directly from wef-shows. When it differs from cleanup's last handled show, archive and delete records not matching that new show in checked owned tables. First adoption reconciles existing old-show rows. A different day in the same show does not trigger this action.
- **Quick cleanup:** only checked heartbeat, run-logs and errs-log. At 1,000+ records, archive and delete the oldest 500 completed eligible records by Airtable creation time. Active/current invocation logs are protected. This is separate from the new-show rule, which may remove more than 500 old-show logs.
- **Control/latest state:** cleanup / tblZOqOQmpy0VgbUM. Records is the table count at action start; timestamps, result, old/new show IDs, archive/delete counts, archive file, error and run ID show the latest action.
- **Permanent action history:** cleanup-logs / tblnMYLtIBvObY1xQ, one record per table action. Archive path and SHA-256 are recorded and verified before deletion. All new-show snapshots preserve original fields/native links before any deletion. Catalogs, references, code-reviews, meta and cleanup-logs are never deletion targets.
- **Failure:** errs-log plus local cleanup-stop.json; collection continues, cleanup does not retry until owner decision. No observer or new scheduled task.

**Normal-run verification:** November 2 at 9:27 and 9:33 a.m. Eastern. All three lanes completed with zero stage/logging errors. First cleanup run 10dccc0a-592d-48e1-8556-814cb651ab0d archived/deleted **989 records** in eight successful table actions for new show **200000077**. Repeat cleanup 414fb3e2-10bb-44d8-99d7-e8debdd61dae made **zero actions/deletions**, preserving the eight catalogs, archive hashes and retained current-show logs. Collection's existing priority-0 date remains November 5; this release does not change the date rule. Source returned no current live rows in these runs.

Initial bulk cleanup took **207.402 seconds**, spanning the 9:30 tick; there was no 9:30 collection invocation. Repeat cleanup took **2.504 seconds**. Task definition is unchanged. **49 offline checks PASS**; review is author review, not independent. Natural 1,000/500 threshold behavior and a future newly occurring canonical show transition remain unverified operational branches. No manual run, record repair, source/provider call or forced show change was used.

**Latest read-only confirmation — checked November 2 at 9:45 a.m. Eastern:** cleanup checks at **9:39, 9:42 and 9:45** all completed **Success**, with the same show and **zero repeat actions/deletions**. Exact cleanup run IDs respectively: `ea17b3db-3303-4ffe-9ccf-14308c2d30aa`, `e154aea1-e5f8-491a-a5fc-a3fc315fff89`, `7f03c723-3108-42ee-86f2-65f4f99eef84`. No cleanup error was reported. Airtable [cleanup-logs](https://airtable.com/appgzosUcVqA4vZdf/tblnMYLtIBvObY1xQ) still contains the original eight successful actions, totaling **989 archived and 989 deleted records**. All eight compressed archive files were reopened and their SHA-256 hashes reverified. The latest completed collection checked at 9:42 had all three lanes successful with no stage/logging errors. The **1,000/500 quick-cleanup rule is still not naturally exercised**; its proof remains offline only. No manual runs or changes were made during that confirmation.

[Acceptance and exact six lane run IDs](evidence/cleanup-20261102/acceptance.json) · [Release](evidence/cleanup-20261102/release.md) · [Author review](evidence/cleanup-20261102/review.md).

## Previous dated documentation

The material below is preserved history; conflicting hold/retention/status claims are superseded by this release for its stated scope.

# wef-live

**README version: 2026.11.01.6**  
**Updated: November 1, 2026, 12:28 p.m. Eastern.**

## Current status

**Functioning as expected for the approved day-0 collection workflow.** The normal **12:24 and 12:27 p.m. Eastern** invocations completed all three lanes, with no stage or logging failures. The latest task result is **0**, the task is Ready, and the next scheduled wake is 12:30 p.m. The current code hashes match the reviewed deployment.

| Latest source / saved state | Count |
| --- | ---: |
| Live rings | 5 |
| Active groups | 25 |
| Active class appearances | 43 |
| Unique class-detail records refreshed | 41 |
| Resolved Live entry records refreshed | 134 |
| Retained groups now Dropped | 2 |

The two retained groups became **Absent** at 12:24 and **Dropped** at 12:27. All **267 existing records and 987 checked durable links** were preserved. All **43 appearance IDs** stayed stable, all **86 reciprocal appearance links** were verified, and the repeat run made no appearance business writes. Refreshed entry count does not imply deletion of other retained entries.

**Source completeness remains Partial:** five trips have no decoded identity. Each is recorded separately in `errs-log`; other classes and entries continue. No source identities were repaired. These are source-record issues, not failed collection stages.

[Current status check](evidence/day0-membership-20261101/readme-status-1228.json) · [Two-run acceptance and exact IDs](evidence/day0-membership-20261101/acceptance.json) · [Release](evidence/day0-membership-20261101/release.md).

## Owned workflow

- Existing hidden Windows task: `wef-live-collector` → `run-hidden.vbs` → `run.ps1` → `entry.js`.
- Every **three minutes, 05:00–19:00 America/New_York**, unchanged. One scheduled process starts the three lanes together; each collects, saves and logs independently, and the task waits for all three.
- Each lane stamps heartbeat and reads the default show and its **wef-shows show-days day-priority = 0**. Live owns **day 0 only**.
- **Groups:** `ListAjax?from_wp_api=true` saves groups, source class references and group/class-appearance membership.
- **Rings:** `LiveScoreWidget` saves rings and current/next/completed group observations.
- **Class details:** latest saved same-show/day class references drive `getLiveClassData` and the existing decoded Worker `/classes/:id`; save unique class details and resolved entries. Repeated group appearances remain separate in the appearance table.
- The live-status check is recorded, not a collection gate. **Active/Absent/Dropped, Completed, has_JSON, entry totals and source age do not gate collection.** Invalid or missing identities are logged rather than guessed.
- Save changed business values only. Each lane retains separate collection/save timings, successful-collection coverage, last-changed evidence and field diffs. Unchanged polls do not rewrite native `last-seen` solely to stamp time.

## Airtable and membership

Owned base: [wef-live](https://airtable.com/appgzosUcVqA4vZdf).

| Table | Role |
| --- | --- |
| `live-rings` | Ring observations |
| `live-groups` | Groups; competition `status` is separate from `record-status` |
| `live-class-appearances` | One appearance per **show + date + group + class**, linked to existing group and class details |
| `live-classes` | Existing unique class details and preserved links |
| `live-entries` | Resolved source trips/entry observations |
| `heartbeat` | One cadence stamp per lane invocation, completed at lane finish |
| `run-logs` | Run outcome, counts, timings and changed-field reports |
| `errs-log` | One error per record, with class/trip/group identifiers when available |

`record-status` is **Active** when returned; **Absent** after one complete successful missing observation; **Dropped** after the next; **Active** on return. Failed or incomplete inventories do not advance absence; valid returned records still continue. Absence and dropping preserve data, original source clocks and links. These states do not delete records or prevent collection.

Counts are dynamic: **25 groups / 43 appearances** describes the verified payload, not a cap. The two repeated class IDs are retained as separate group appearances while using their existing unique detail records.

## Boundaries and remaining limits

**Stages 8 (cross-base publication) and 9 (archiving/cleanup, including log retention) are ON HOLD and are not invoked.** Source evidence archives remain part of collection; no retirement or deletion is enabled. This release does not claim updates to wef-data or 24-hour log cleanup.

Schedules, Data, Entries and other collectors are untouched. Entries reads Live; Live does not read or enrich Entries. Existing keys, records and links remain; no manual collector/provider calls, record repairs, observers, new tasks or cadence changes.

Immediate coordinator-chat failure delivery remains deferred under the owner's errs-log-only approval. Native heartbeat show/date cells remain blank, and decoded class-level fields are not all included in business diffs; those known limitations were not changed here. Missing/returning class-appearance and false-live-status cases passed offline checks but were not forced in production. Natural future rollover is not claimed proven by these two same-day runs.

Deployment changed `lanes.js`, `schema.js`, `lane-store.js` and new `membership.js`; 18 other original JS files are unchanged. **31 offline checks passed.** Release review was author review, not independent review. [Review evidence](evidence/day0-membership-20261101/review.md), [deployed hashes](evidence/day0-membership-20261101/deployed.json).

## Historical release and design notes

The preserved sections below are dated history. The current workflow and status above supersede conflicting earlier plans, gates, publication, cleanup or acceptance statements.

## Day-0 membership release — November 1, 2026, 12:22 Eastern

Documentation version: **2026.11.01.5**. Deployed through the unchanged hidden three-minute task, 05:00–19:00 America/New_York. Normal **12:24 and 12:27 Eastern** invocations verified: 25 Active groups / 43 Active class appearances, with both retained groups progressing Absent to Dropped. All 267 existing records and 987 checked durable links were preserved; 43 appearance IDs stayed stable and 86 reciprocal links passed. No stage/logging failures; five unresolved source identities remain individually logged. Offline author checks: 31 PASS; review is author review, not independent review. [Exact acceptance](evidence/day0-membership-20261101/acceptance.json).

Live reads the default show and its `wef-shows.show-days.day-priority = 0`. `live-groups.record-status` and the new `live-class-appearances` table label source presence: Active; Absent after one complete missing observation; Dropped after the next; Active on return. Failed/incomplete inventories do not advance absence. Competition status, including Completed, remains separate. False/failed live status, Completed groups, detail-availability, membership state, entry totals and source age do not gate collection. Missing/ambiguous show/day identity is logged rather than invented.

The inspected payload contains 25 groups and 43 group-class appearances (41 unique class details). Counts are dynamic, not limits. Appearance key is show|date|group|class; links point to existing groups/details. Existing class/entry keys and records are not rekeyed, duplicated or removed. Historical links are retained. States apply only to day 0; other days are untouched.

Stages 8 publication and 9 archiving/cleanup remain **ON HOLD**. Logs, per-lane timings, changed-only saves and errs-log-only reporting remain. No manual collector/provider run, record repair, task/cadence/dependency change or observer. Earlier conflicting design/status sections below are historical. [Release evidence](evidence/day0-membership-20261101/TASK.md), [author review](evidence/day0-membership-20261101/review.md), [deployment](evidence/day0-membership-20261101/deployed.json).

# wef-live

Documentation version: **2026.11.01.4**  
Updated: **November 1, 2026**. Documentation only; not a runtime release.

## Current release - November 1, 2026, 09:54 Eastern

Three-lane collection is deployed through the existing hidden task, every three minutes from 05:00 to 19:00 America/New_York. Owner explicitly permits errs-log-only failures for now; immediate Records-chat delivery remains deferred. Stages 8 (cross-base publication) and 9 (retirement/cleanup, including log cleanup) are ON HOLD and are not invoked by the new entry path. No prior stop/evidence file was cleared; no records were repaired or removed. Older publication/retirement sections below are historical and do not authorize resuming those paths.

Two populated normal invocations at **09:51 and 09:54 Eastern** completed all three lanes without stage or logging failures. Latest coverage: 25 groups, 5 rings, 41 class records and 145 entries. Six unresolved trip identities are recorded individually in errs-log; valid records continue. Source identity completeness is therefore Partial, not a claim those six identities were resolved. Normal source collection/save times at 09:54: Groups 0.65s/2.36s, Rings 0.12s/0.80s, Class details 3.68s/3.70s; core lane totals 3.27-11.14s. These totals end before final log writes; Windows task completion/result separately confirms the full invocation finished before the next trigger.

Changed-only repeat proof: 22 group records, 3 ring records and 165 class/entry records were successfully recollected without business writes; last-success coverage advanced and last-changed was preserved. All changed-field reports remain in run-logs and the lane receipt. Heartbeat/run-logs and six individual errs-log rows were read back for both populated invocations. Exact IDs, scopes, receipt hashes and limits: [normal-run acceptance](evidence/three-lanes-20261101/normal-run-acceptance.json); [release](evidence/three-lanes-20261101/release.md). No manual runs, source calls, forced failure, observers or record repairs. Rollback was not needed; saved baseline and rollback script remain available. Natural whole-stage failure and future day rollover under this release were not forced or claimed verified.

## Deployed three-lane collection - November 1, 2026, 09:45 Eastern

Owner approved errs-log-only stage failures temporarily while immediate Records-chat delivery remains deferred. The prepared four-file delta is installed in the existing hidden task route; 21 installed file hashes match the candidate/protected baseline. The task now has a daily local-time 05:00 start, three-minute repetition and 14-hour window; the action/account remain unchanged. Stages 8 and 9 (publication and cleanup) are excluded from this new execution path. Prior stop/evidence files and all existing records/links remain untouched. 18 offline checks pass. Normal scheduled-run verification is in progress; no manual invocation or record repair. [Release and deployment evidence](evidence/three-lanes-20261101/release.md).

## November 1 implementation preparation

Historical preparation checkpoint before the 09:45 deployment: the candidate was prepared separately and 18 targeted offline checks passed. The production baseline was preserved until the owner approved temporary errs-log-only alerts. [Prepared release, exact blocker and rollback](evidence/three-lanes-20261101/release.md), [author review](evidence/three-lanes-20261101/review.md), [actual offline output](evidence/three-lanes-20261101/offline-tests.json).

Immediate failure alerts to the Records chat have no installed runner delivery route. This requirement is not satisfied by local logs or an injected test callback. This hold was superseded by the explicit owner decision above. Current production still follows its prior path, including publication/retention behavior; stages 8 and 9 are held in the candidate, not retroactively disabled in production. No manual runs, record repairs, source calls or stop clears were performed.

## Current Live design: three independent lanes

Documentation version: **2026.11.01.4**. Updated **November 1, 2026**.

Owner-directed design only; not deployed. No code, Airtable, task, cadence or deployment changes are authorized by this documentation update. This section supersedes conflicting older Live design statements below; their implementation evidence remains historical.

**Mission:** collect and save Live source data as quickly as possible. Three separate lanes run every three minutes between **5 AM and 7 PM Eastern (America/New_York)**. Each stamps its own heartbeat, checks live status and reads canonical show/day context from wef-shows. False live status is logged as a skip. Use wef-shows show-day priority for past/current/upcoming context, rather than adding a historical marker. Planning-day advance alone does not establish tomorrow's live activity.

| Lane | Source and shared reads | Own writes in wef-live |
| --- | --- | --- |
| Groups | ListAjax; latest shared ring/class context | Groups, statuses, counts and class references |
| Rings | LiveScoreWidget; latest shared group identities | Rings and current/next/completed group links |
| Class details | Latest saved class references; getLiveClassData and existing decoded class Worker; shared ring context | Classes and entries, including decoded identities |

**Sharing:** use the latest successfully collected saved data for the matching show/day and supported identity, based on its original **last-updated** timestamp. Reading or sharing does not renew that timestamp. Each lane writes its own fields, preserves existing links and finishes independently. Missing shared data is logged; collection continues without waiting for another lane. Class details uses the latest saved class list. No new age cutoff, guessed link, timestamp renewal or cross-lane completion gate is agreed here. Exact physical timestamp/field mapping remains implementation work, not schema approval.

**Completed groups:** save final source Completed statuses before skipping further detail; the existing early exit does not yet do this.

**Collection success, changes and field diffs - all three lanes:**

| Lane | Compare the lane-owned collected values |
| --- | --- |
| Groups | Group status, counts, sequence, class references and other collected group values |
| Rings | Ring values and current/next/completed group identities |
| Class details | Class observations and entry identities, order, scratch, gone, position and other collected detail values |

- **Last successful collection:** the original time a valid collection succeeded, even when no business values changed. This is collection evidence, not a business-change timestamp. A failed attempt does not advance it. Preserve which records were successfully covered; a successful response does not prove every record was valid.
- **Last changed:** advances only when the lane's collected business values differ from the previously saved values. An unchanged successful collection leaves it unchanged.
- **Field diffs - complete business-field coverage:** report diffs for ALL collected business fields owned by each lane, not a selected subset. Exclude timestamps and logging metadata from diff reporting and change detection. Identify the lane, run, stable record identity, field, previous value and new value. Preserve types and changes to/from zero or false. Missing/failed/unresolved input is not an instruction to clear an existing value. A newly discovered record is an initial creation, not a fabricated previous value.
- **Only save changed values:** compare with the saved record and write only changed lane-owned business fields. Create new valid records as needed. If unchanged, make no business-record update solely to refresh a timestamp; keep successful collection/coverage evidence in the lane's existing run logging. Last-changed metadata travels with an actual change. Collection-success evidence remains available without rewriting unchanged rows.
- **Compare business values, not bookkeeping:** collection timestamps, run IDs, publication metadata and changing response tokens do not themselves constitute a business diff. Shared reads do not become fresh source collections or create changes. Preserve source evidence and original clocks; no new archive/cleanup policy is agreed here.
- **Shared freshness:** use the original last successful collection evidence for the matching lane/show/day/identity when deciding which shared observation is latest. Last changed describes when values changed, not whether unchanged data was recently recollected. A read does not advance either clock.

These are logical meanings, not approval to add/rename physical fields or a new diff table. Existing Data `last-success` and Live `last-seen` must not be assumed interchangeable; physical mapping and coverage storage remain implementation details to settle within a separately approved change. Requirements only; no changed-only writer or new instrumentation is claimed deployed.

**Separate measurements per lane:** collection start/end and elapsed time (first source request to last response); save start/end and elapsed time (first Airtable write to last confirmed write); total invocation-to-completion time; source-response-to-corresponding-save delay. Logs identify lane/run and counts. These are required measurements for evaluating the three-minute cadence, not currently verified instrumentation.

**Checks and errors:** individual check/record issues go to the error log and do not fail the stage or stop other records/classes/lanes. Preserve unresolved evidence and existing links. Actual stage failures are logged and immediately alerted to **wef-records-coordinator**; independent work continues. Automatic coordinator alert delivery is not currently installed. The existing physical error table is errs-log; no rename or additional log tables are authorized. Each lane has separate heartbeat, run-log and error attribution.

**Stage 8 - cross-base updates: ON HOLD.** Destination, fields and method remain unfinalized. Must not delay or block revised collection.

**Stage 9 - archiving/removal: ON HOLD.** Cutoff, timing and removal rules remain unfinalized. Must not delay or block revised collection. Log cleanup rules also remain pending within log handling, not a duplicate stage.

These design holds do not disable the existing publisher/archiver or clear the retained log-retention stop. Existing failed-path boundaries remain. No implementation changes have been made.

```mermaid
flowchart LR
    subgraph G["Groups lane"]
        direction TB
        G1["Every 3 min: 5 AM-7 PM Eastern<br/>Heartbeat, Shows, live-status check"]
        G2["Fetch ListAjax<br/>Collection start/end + duration"]
        G3["Save groups + class references<br/>Original last-updated<br/>Save start/end + duration"]
        G4["Own run-log + errors<br/>Record issues: log and continue"]
        G1 --> G2 --> G3 --> G4
    end
    subgraph R["Rings lane"]
        direction TB
        R1["Every 3 min: 5 AM-7 PM Eastern<br/>Heartbeat, Shows, live-status check"]
        R2["Fetch LiveScoreWidget<br/>Collection start/end + duration"]
        R3["Save rings + activity links<br/>Original last-updated<br/>Save start/end + duration"]
        R4["Own run-log + errors<br/>Record issues: log and continue"]
        R1 --> R2 --> R3 --> R4
    end
    subgraph C["Class-details lane"]
        direction TB
        C1["Every 3 min: 5 AM-7 PM Eastern<br/>Heartbeat, Shows, live-status check"]
        C2["Read saved class references<br/>Fetch details + decoded identities<br/>Collection start/end + duration"]
        C3["Save classes + entries<br/>Original last-updated<br/>Save start/end + duration"]
        C4["Own run-log + errors<br/>Record issues: log and continue"]
        C1 --> C2 --> C3 --> C4
    end
    G3 -. "Latest shared group identities" .-> R3
    G3 -. "Latest shared class references" .-> C2
    R3 -. "Latest shared ring context" .-> G3
    R3 -. "Latest shared ring context" .-> C3
    C3 -. "Latest shared class observations" .-> G3
```

Dotted arrows are saved-data reads, not run dependencies. No common completion node. Stages 8 and 9 are intentionally outside the collection lanes.

## Earlier implementation and historical decisions

### October 31 implementation checkpoint

Live independently collects calendar TODAY's rings, groups, classes and entries using canonical Shows identity and existing Schedules references. Entries reads Live; Live has no reverse Entries or CWF dependency. The established scheduled task checks every three minutes.

Source calls, in order:

| Purpose | Endpoint | When called |
|---|---|---|
| Activity gate | `https://sglapi.wellingtoninternational.com/homepage/getLiveClassStatus?customer_id={customer-id}&datetimestamp={timestamp}` | Once per eligible run |
| Groups | `https://sgl.wellingtoninternational.com/iphonev2/index.php/esp/liveclassv2/ListAjax?from_wp_api=true` | If activity is true |
| Ring activity | `https://sgl.wellingtoninternational.com/iphone.php/esp/webservice/LiveScoreWidget` | If TODAY's returned groups are not all completed |
| Class detail | `https://sgl.wellingtoninternational.com/iphonev2/index.php/esp/liveclassv2/getLiveClassData?show_id={show-id}&cid={class-id}&t={timestamp}` | Per selected class during detailed collection |
| Decoded identities | `https://wef-schedules-decoded.gombcg.workers.dev/classes/{class-id}?show_id={show-id}&customer_id={customer-id}&cgid=-1` | Per selected class during detailed collection |

The first four use direct source URLs; decoded identities use the existing Worker. The source reader permits one bounded retry for specified transient failures within the scheduled run.

Live publishes approved observations to existing Schedules/Data records through `live-observation-json`, preserving acquisition fields and native links. Verified zero/false values are retained. The overlap includes ring group context, group status/counts/start estimates, class counts/current trip and trip identity/order/scratch/gone/position. Class and group counts have different scopes; observed position is not verified official results. Actual start/finish and `results_verified` have no published Live equivalent. Complete destination coverage and downstream effective-value selection are not established merely by this mapping.

Latest inspected normal receipt: `runtime/d47ae184-519e-4f23-b386-6fca2021c583.json`, completed **October 31 at 20:45:53 Eastern**, `ok=true`: **5 rings, 30 groups, 59 classes, 245 entries**. Collection date remained October 31 while Shows selected planning date was November 1. Data publication correctly reported **Skipped**, zero writes, because October 31 was no longer canonical `day-priority=0`. Shows' saved priority checkpoint confirms the November 1 anchor at October 31 19:04:07 Eastern. Planning-day advance does not authorize tomorrow's Live collection before calendar rollover.

Outstanding limits: populated publication previously remained Partial because of lane budgets and identity/matching issues; no full-overlap acceptance is claimed. The latest receipt still reports **24-hour log retention Blocked** by the prior timeout stop. Unresolved classes can also withhold old business-record retirement. Same-date old-show cleanup and multi-group natural proof remain separate acceptance gaps. No stop was cleared and no manual collection or repair was performed for this documentation update.

Logs retain separate purposes: `heartbeat` records invocation cadence/outcome; `run-logs` records collection/publication/archive results; `errs-log` records individual errors; `collections` catalogs collected datasets and immutable archive evidence. Archive catalogs and archive-before-delete protections remain required. Existing business observers remain removed.

The dated sections below retain historical evidence. This checkpoint supersedes older statements that populated activity has not occurred or that log retention is currently healthy.

## Day-priority and overlap validation — October 31, 2026

Data publication requires the exact canonical `show-days.day-priority = 0` before inventory and dispatch; Live collection remains TODAY. Existing approved `live-observation-json` mapping preserves verified zero/false and leaves Data acquisition fields/native links intact as fallback. No runtime replacement was needed: installed guard reviewed, 43 targeted publication checks passed. Two existing normal runs at 07:56 and 08:02 Eastern verified 57 Data overlay writes with links/logs preserved. Operational result remains Partial due publication budget/identity issues and the separate blocked log-retention stop; no repairs or stops cleared. External consumer effective selection is not proved by overlay writes. [Exact evidence and limits](evidence/day-priority-precedence-20261031/release.md).

## Shows calendar policy companion - deployed October 30, 2026

Shows now selects tomorrow at 19:00 Eastern within show dates and checks new shows Sunday 22:00, then every three hours until changed. Readers validate authority_checked_at with their original 30-minute/future-clock tolerances; last_checked remains the original source timestamp and must be valid/non-future. Legacy rows retain the prior fallback until normal publication. All unrelated source/count freshness checks remain. Live and Timing use TODAY independently while preserving the canonical Data fence and original retirement guards. [Release and normal-run evidence](../wef-shows/evidence/calendar-discovery-20261030/README.md).

## Canonical Shows authority - approved October 30 discovery windows

The approved companion check now follows Shows' daily 16:00-20:00 America/New_York discovery window. A successful check from the closing 30 minutes remains valid until the next daily 16:30 deadline; other checks retain the existing 30-minute allowance. Original source timestamps, exact show identity/links and all non-authority freshness limits remain unchanged. This does not restart a stopped workflow, retire a source, or change any task cadence. [Shows release and normal-run acceptance](../wef-shows/evidence/discovery-windows-20261030/README.md).

## Current Live status — 30 October 2026, 13:03 Eastern

Latest read-only check: October30 at13:35Eastern. All19 deployed/protected runtime hashes still match. Eleven postdeployment receipts contain no populated activity. Latest completed run4bfd1122-90b5-47a0-90bf-cdf822a84379 (13:32:13–13:32:29Eastern) is ok=true, all-today-groups-completed, with zero collection/logging errors. Two naturally populated runs remain the only acceptance blocker; no code/retention/workflow edits or new approval needed for the already-deployed corrections.

The approved journal/count, cursor and exact-group corrections are implemented, reviewed and deployed. [Release and task evidence](evidence/publication-continuation-20261030/release.md), [independent review](evidence/publication-continuation-20261030/review.md), [deployed hash](evidence/publication-continuation-20261030/deployed.json), [first normal receipt](evidence/publication-continuation-20261030/first-normal-skip.json).112 offline tests pass; independent review includes real-journal restart/failure and deadline cases. Only publication.js changed;18 other runtime files are preserved.

- **Completed:** journal-save failures are logged with accurate remaining counts; unsafe writes without durable intent are deferred, not dispatched. Other safe records/lanes, collection and future invocations continue. Pending intents survive failures and reconcile before retry. Outcome/log failures do not create a global halt. No immediate retry loops.
- **Completed:** first budget-deferred item remains the next cursor; attempted writes including uncertain responses respect the unchanged cap. Exact known show/date/ring/group/native links are filtered before uniqueness, resolving the six false ambiguities. Actual multi-group or duplicate exact matches remain logged and withheld. No new matching rules, fallbacks, fields or budget/freshness relaxation.
- **Normal execution verified:** run6a729be2-fbb4-481b-803b-b6a70f197e6b at13:02Eastern completed safely,0 publication writes,0 errors, retention Success. All18 source groups were completed; both publication lanes correctly Skipped.
- **Only remaining acceptance blocker:** no current populated activity. Two populated postdeployment runs plus ring6 and the six exact class-occurrence writes cannot be proved from skipped runs. Existing collector checks continue every3minutes; inspect saved evidence within authorized work/on request. No manual source runs, repairs, forced joins or observers. Offline injected failures are not claimed as production events.
- **Preserved:** prior24-hour log-retention acceptance, archive catalogs and links, source authority, independent lanes, Entries linking-only, existing Monday review. All business observers remain removed. Scope/code may be locked to this reviewed release; populated operational acceptance remains pending without adding new work.

Earlier checkpoints below are historical where superseded. The prior failed-gate approval request was superseded by the owner's explicit direction to correct these paths.

## Current publication blocker — 30 October 2026, 12:45 Eastern

Publication is not complete. The has_JSON and concurrent-acquisition corrections now have normal-run proof. Deferred counts still underreport when journal-save EPERM escapes the item loop. One eligible Data ring remains70m16s behind its fresh source because an unprocessed budget-boundary item advances the cursor. Six occurrence errors are false ambiguities: each has one exact source-group match; class200030671 remains genuinely multi-group. Prior missing trip200399568 is resolved. See [exact evidence and smallest corrections](evidence/publication-fixes-20261030/current-blocker-20261030.md). This failed verification gate permits no further code patch/retry without separate approval. No observers or workflow changes.

## Current release and scope-lock status — 30 October 2026

Owner approved exactly three publication corrections in Records. They are implemented, independently reviewed, offline-tested (99 PASS) and deployed at 11:42:28 Eastern. [Release/task evidence](evidence/publication-fixes-20261030/release.md), [review](evidence/publication-fixes-20261030/review.md), [exact runtime hashes](evidence/publication-fixes-20261030/deployed.json), [two-run acceptance summary](evidence/publication-fixes-20261030/acceptance-summary.json). Only publication.js and the collector summary changed;17 other Live runtime JS files remain at the protected baseline, including retention and class-error/audit behavior.

- **Completed corrections:** actual has_JSON spelling with true/false/missing preserved; accurate deferred and separate uncertain counts with readback-before-retry; stable identity/matching/link/overlay validation that permits unrelated acquisition metadata updates. Full row audits, one-field external PATCH, original freshness and budgets remain intact.
- **Verified normal runs:** 3d7a24cd-ed6d-4c5f-b0ba-e1ec503c0dbe at11:44 and c8d1fe08-1c80-4097-a1f5-79120c1d258b at11:47 Eastern each collected196 entries. Schedules35+35 writes; Data37+38;145 archived/current readbacks preserve identity/native links and original freshness. Data deferred230 in each; uncertain1 then0 with NotApplied reconciliation before continuation. Both business outcomes remain Partial, not full coverage.
- **Pending acceptance:** natural has_JSON publication into a Data group and positive concurrent-metadata exercise were not encountered in this two-run pair; both behaviors have independent offline regression proof. Full timely publication remains unproved due existing budgets/withheld matches. Same-date rollover and multi-group natural proof remain separate. Inspect saved evidence during authorized work/on request; no forced runs or guessed joins.
- **Complete and protected:** completed-log24-hour retention accepted separately on b7a9e822-461a-42e2-bf25-dd601a268b1e and853114e5-fe5b-4c97-8999-ba0133a0a38c (1,424 archived/deleted, seven show catalogs and protected baseline retained). This is not publication proof.
- **Continuing scope:** independent TODAY Live observations using canonical Shows and current Schedules references; Live tables plus approved single-field Schedules/Data overlays. No reverse Entries dependency or acquisition ownership transfer. Existing three-minute collector, recurring archives, logs and Monday review continue.
- **Removed:** all business observers; do not recreate/replace hourly verify-live-natural-same-date-rollover, daily watch-wef-default-show-changes or wef-running-lanes-october-15-observation. Records context bookkeeping is separate. No task/cadence changes or record repairs.
- **Lock readiness / next action:** reviewed scope/code baseline can be locked with pending acceptance explicitly recorded; full operational lock/acceptance cannot yet be claimed. A scope/code lock never disables collectors or independently approved work. No new corrections are authorized by verification; actual new failed gates retain the owner approval boundary. Existing Airtable review record is dated pre-fix evidence owned administratively by Records; this release receipt is local and no tracker was created.

Earlier status below is historical wherever superseded by this checkpoint.

## Approved 24-hour log retention — 30 October 2026

Owner approved recurring archival/removal of completed heartbeat, run-logs and errs-log records older than24hours. This narrow release does not fix publication defects or change cadence/source collection. Existing normal entry workflow calls retention only after collection ok=true (Partial class/publication outcomes can still be ok=true); failed collection skips it. No manual runs/record repairs.

Retention accepts terminal Success/Skipped/Partial/Failed completion older than24hours; active/incomplete/current invocation and non-run control rows are preserved. Error rows additionally require older completed parent evidence from existing run-logs or saved local receipts. At most250 rows per table per invocation, with four-minute request deadline and a60-second reserve before starting another table; initial backlog drains across normal runs.

Exact immutable gzip batches are verified before deletion. Three fixed collections rows (`log-archive|heartbeat`, `log-archive|run-logs`, `log-archive|errs-log`, source-kind log-archive-catalog) point to immutable chained manifests, retaining previous references/hashes and full original row IDs/fields. Existing seven show archive catalogs and business collections are untouched. Manifest row-count counts archived records. No local archive TTL/purge is introduced.

Every deletion batch rereads unchanged eligible rows, validates exact DELETE acknowledgements and verifies absence. Durable runtime/log-retention-stop.json is created before work, removed only on successful completion or safe deferral before a new table, and retained on failure/interruption. That marker blocks retention without stopping independent collection; no automatic retry/repair of uncertain deletion. Failure requires owner review/approval. Current run logs/heartbeat summarize retention and retain archive descriptions. Local receipts/logs and publication journals/control state are never deleted.

Evidence: evidence/log-retention-20261030/{baseline.json,review.md,tests.txt,deployed.json}.91offline tests passed; independent11retention tests passed. Normal-run acceptance BLOCKED: first postdeployment10:41run failed with source ListAjax HTTP504; retention correctly skipped, no log deletion. Two successful normal retention runs remain unverified (evidence/log-retention-20261030/release.md). Existing3P2publication findings and populatedPartial acceptance remain separate.

# Live operating reference — 30 October 2026

This is the current module orientation; dated sections below are historical evidence, not competing authorization. Actual workspace: `C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-live`. No separate private coordinator workspace was identified or configured for this task. [Local instructions](AGENTS.md) load the physically available [canonical owner instruction block](../docs/coordination/wef-owner-operating-instructions.md), actually read in this checkout.

## Responsibility and source authority

Owner approves; Records coordinates. Canonical Shows → independent Data acquisition → separate Season retention. LOCAL Season coordinator retains both distinct Data/Season responsibilities; the temporary Data fork was reversed. Live independently collects TODAY observations, validates source identity and publishes only `live-observation-json` to approved existing destinations. Entries is linking-only, not a Live input, enrichment assignment or readiness gate. Schedules still runs independently; Data-only switching/retirement is preparation only. Retained rows, successful invocations and group completion are not universal proof of current membership, completeness or whole-class completion.

## Exact owned base and tables

Base **wef-live / appgzosUcVqA4vZdf**. IDs below are saved configured schema plus Records' verified review-table handoff; no new schema inspection or changes are implied.

| Table | ID | Responsibility |
|---|---|---|
| live-rings | tblw8bodkRbCItxIH | Dated observed ring state |
| live-groups | tblMhQtjpvyODEUSn | Dated group state/counts |
| live-classes | tblNaM9ehOJFsrveV | Validated class progress |
| live-entries | tblf7550e7UGT8ZSF | Observed trips and decoded identities |
| collections | tble6fwMDBbeeGxly | Source datasets and permanent archive catalog |
| heartbeat | tbl1K0KT8A2kZEJgS | Invocation/completion stamps |
| run-logs | tblwne646pQi5KlXA | Run outcome/counts/archive evidence |
| errs-log | tblMzOaTvWus2pM4I | Separate error rows |
| code-reviews | tbl1jEtJusopJGC6O | Administrative review evidence; Records writes for this request |

Existing reference tables are not owned acquisition: `wef-schedules.schedules` / tblGPXIUnD0sfTEIX is actively read for group/class schedule links; `wef-entries.entries` / tblfj77GmXxuiAoB4 is retained for historical links, with runtime reads forbidden. Saved schema also includes meta / tblwBvigcmqRlSYUb; it is not a collection authority or active runtime table.

## Interfaces and boundaries

- Upstream authority: Shows appCaSmL819KK7UFJ / shows supplies canonical show/date with its existing freshness guard. Calendar equality prevents collecting an upcoming date as TODAY; it does not select the show.
- Sources: public ListAjax, LiveScoreWidget, getLiveClassData and live-status; decoded class identities through existing wef-schedules-decoded Worker. Keep that service despite its name. Shared bounded source reader is `../lib/wef-source-read.js`.
- Current Schedules base appUq5Qz9kARqFija: active local schedule sync, plus publication to rings tbliUwvXYdMhvJ3Fm and schedules tblqiTFqqO3zk2KJC.
- Data base appdl16HJ5ETm8N69: publication to rings tblhKkfYXppW3TbTq, groups tblQObOvktufkyq54, classes tbl0IFf067w4wUBj5, schedules tbl6KKlYuMIPHEJWe, trips tblt0GvQ39bHIu930.
- Only the additive JSON overlay may be patched externally; no acquisition fields, native links, create/delete, names-based joins or ownership transfer. Downstream timing/outputs must consume original observation timestamps/provenance; Live does not promise whole-class completion or forecasts.

## Actual cadence, archives and semantics

Registered Windows task `wef-live-collector`, inspected 30 October: repetition PT3M, no duration/end boundary configured. Action: wscript.exe //B //Nologo → this module's run-hidden.vbs → run.ps1 → entry.js. This task runs the business process; Codex messages/reviews are separate and may wake this chat. There is no overnight task stop. Do not manually invoke it.

Every normal invocation stamps/logs and reads canonical scope. For an active TODAY date, live-status false or a nonempty all-completed group list skips detailed collection; the three-minute checks continue. Explicit empty activity succeeds without clearing current observations or renewing publication. Missing/invalid/failed responses are not verified empty. Class identity errors preserve prior observations/links and allow other classes; publication errors remain separate per lane and do not invalidate successful collection. Task exit 0 can accompany Partial business results.

Source and class snapshots are content-hashed gzip archives under runtime/history, created/reused during normal collection. Publication source/intent/before-after is durable before PATCH; outcome/readback follows; unknown writes retain journal state for later scheduled reconciliation. There is no separate timed archival job or inferred retention TTL.

Recurring old-data retirement is evaluated after successful valid collection including empty/skipped results, provided no unresolved classes. Failed collection or unresolved class input does not authorize retirement. Publication-only errors do not cancel otherwise eligible retirement. Exact old owned rows are archived and catalog-verified before child-first deletion; current/future protection remains. Permanent catalog rows remain in collections. Seven catalogs/550 historical records were verified in prior retirement proof; see evidence/retirement-20261029/release.md. This dated evidence is not a fresh audit claim.

Publication freshness: original validated observation ≤6 minutes, ≤1 minute future tolerance, same TODAY scope. New actual observations may advance time; replay cannot. Successful class detail is required for class counters; discovery refresh is insufficient. Missing phase counts remain distinct from zero. Ambiguous groups/occurrences and contradictory identities are withheld. No synthetic end time or weaker freshness is permitted.

## Deployment, review and next action

Current deployment includes the October 30 log-retention release, which supersedes entry.js and airtable.js from publication-v1-20261029 and adds log-retention.js. Exact hashes and independent review are in evidence/log-retention-20261030; unchanged publication behavior retains its October 29 evidence. Existing guarded deployment uses this checkout between registered invocations with no collector lock and adequate time before the next trigger, only with applicable approval. No task changes or manual proof.

Recurring log retention archives completed heartbeat, run-logs and errs-log records strictly older than 24 hours after successful collection. It preserves active/incomplete/control rows, existing archive catalogs and links; errors need completed parent evidence. Verified immutable batches and chained catalog manifests precede deletion, with fresh eligibility checks and absence readback. Work is bounded to 250 records per table per invocation and a four-minute request deadline. A durable failure marker stops retention, not independent collection. Local archive files have no purge policy.

Retention acceptance remains BLOCKED: the first postdeployment scheduled run b09174d2-e4a0-49db-9798-c089cf45a1c6 at 10:41 Eastern on October 30 failed with ListAjax HTTP 504 and correctly skipped retention. No two successful scheduled retention runs are claimed. The 91 passing offline tests do not replace that proof. Preserve this stopped verification path; do not retry collectors, repair state or fix the source under documentation authorization. See evidence/log-retention-20261030/release.md.

Code review: [report](evidence/code-review-20261030/review.md), [hash manifest](evidence/code-review-20261030/manifest.json). Three confirmed P2 findings remain: has_JSON casing; early abort deferred-count underreport; unrelated acquisition changes interrupting publication. 80 offline tests PASS; populated operational acceptance PARTIAL, with budget/matching/source-identity limits. No fix authorization. Exact populated audit report: evidence/publication-v1-20261029/populated-verification-20261030.json.

Airtable review: appgzosUcVqA4vZdf / tbl1jEtJusopJGC6O / rec5fLj3xEqkYBpxA; Records reported exact GET readback PASS. Review FAIL3P2 is separate from tests PASS and populated PARTIAL.

Periodic review: Records confirmed ACTIVE configuration and readback on October 30 for automation `wef-live-monday-code-review`, targeting this chat `01a16902-240a-7280-8661-90b0d8d7325a`, Mondays at 10:00 America/New_York, first November 2, 2026, with a 20-minute bound. Routine record `recYUERW7D8RF2OXS` in `appgzosUcVqA4vZdf/tbl1jEtJusopJGC6O` remains Pending first review; configuration verification is not execution proof. Prior release review `rec5fLj3xEqkYBpxA` remains separate. Records owns scheduling; do not create another automation. Reviews are best effort host/app dependent and do not trigger collectors. Review material changes before deployment and failures when observed; reuse unchanged review evidence appropriately, not once per collector invocation.

## Instructions, skills and continuity

Actually READ in full October 30: canonical owner block; common review standard; installed Operating Skills **0.2.0** instruction-loader, context-compact, code-review, rebalancer at `C:/Users/gombc/.codex/plugins/cache/created-by-me-remote/ringstatus-operating-skills/0.2.0/skills/<name>/SKILL.md`. Also read legacy aliases under C:/Users/gombc/.agents/skills (loader/rebalance/code-review) and C:/Users/gombc/.codex/skills/context-compact. Packaged rebalancer is the canonical $rebalance name adaptation. rs-skills verification of all30files/remote agreement was relayed by Records; no install/update performed here. Handoff skill is discoverable but not claimed READ in this documentation step.

APPLIED: instruction loading, current-scope rebalance, review procedure and continuity preservation. VERIFIED: documentation readback, actual registered three-minute task route, prior deployment hashes/tests/review evidence. Reading context-compact is not a claim of actual compaction, transcript archival or measured context utilization.

Resume checkpoint: read AGENTS.md, this current reference, scope/HANDOFF and exact review/manifest evidence; retain approvals/stops and pending next action. Follow ../docs/coordination/context-check-procedure.md at task boundaries/30 minutes. Current task is documentation/review coordination only. Next review is the configured November 2 routine; do not run it early or claim completion before actual review evidence. Corrective runtime work requires separately scoped owner approval. Do not reopen Data-only migration or retirement.

Continuity preparation threshold is the owner's 65%, as relayed by Timing, overriding the skill's generic 70% default. Use actual session telemetry; unavailable measurements remain unknown. This does not authorize extra monitors or resume any stopped workflow. On October 30 the existing review TOML and registered PT3M task were read back directly; no duplicate automation was created.

---

## Historical release and workflow notes (superseded where conflicting)

## Code-review procedure — 30 October 2026

Review material Live changes before deployment and review observed behavior/verification failures against the exact deployed baseline. Use the explicitly requested code-review skill at C:/Users/gombc/.agents/skills/code-review/SKILL.md. Record release/hash manifest, reviewer/date/scope, actionable severity/file/symbol/trigger/consequence/smallest correction, tests actually inspected/run, scheduled evidence separately, gate-specific PASS/FAIL/PARTIAL/BLOCKED and exact next authorized action. Offline PASS never establishes scheduled acceptance.

Do not rerun an unchanged release review on every invocation or create review timers/monitors. Reviews authorize no fixes, deployment, schema/business-data/task/cadence/source requests, manual collection or repairs. Applicable explicit approval is required for corrective work. Preserve module ownership and existing failure boundaries. For the October 30 review request, Records alone owns Airtable review schema/record writes and readback; Live supplies its report for base appgzosUcVqA4vZdf. This procedure does not reopen migration, retirement or ownership.

## Approved Live publication v1 — 29 October 2026 (scope v1.7)

Live independently collects TODAY observations. Entries is linking-only and is neither read nor written by Live; CWF has no role. Shows remains canonical show/day authority. Source collection, schedule links, logs, three-minute cadence and recurring verified archive-before-delete retirement are preserved.

The narrow approved exception lets Live GET existing destinations and PATCH only `live-observation-json` on Schedules rings/schedules and Data rings/groups/classes/schedules/trips. Seven multiline fields were created and read back. Live owns this overlay, exact matching and its durable audit. Schedules and Data retain their independent collection, fields, keys, records, native links and lifecycle. No sibling runtime, sync, task or record repairs are included.

Only validated observations within six minutes of their original receipt (one minute future tolerance) may publish. Empty/missing/stale detail does not clear or renew overlays. Class counters require successful class detail; ambiguous multi-group occurrences are withheld. Trips require exact dated identity and supported source provenance. Lanes run independently with bounded requests; uncertain writes retain durable intent and require readback on a later normal invocation. Source/retirement safety stops remain unchanged.

Publication audit files retain full source evidence, destination before/proposed-after, intent, outcome and readback. Existing logs also report lane results and separate publication errors. Original acquisition fields and links are never publication writes. Entries has no new enrichment assignment.

Deployment and review evidence: `evidence/publication-v1-20261029/deployed.json`, `review.md`, `tests.txt` (80 passing offline tests). Two NEW normal runs at 20:29 and 20:32 Eastern passed with empty activity, both lanes Skipped, matching logs and no errors. All baseline destination IDs/keys/native links and seven archive catalogs/hashes are preserved; only Data-owned last-success timestamps advanced. Acceptance remains PARTIAL: two naturally populated runs with PATCH/readback and repeat behavior are still required. Old v1.6 runs do not count. No manual runs or seeded proof.

Earlier locks and descriptions below remain historical except for this explicitly approved publication addition.

## LOCKED - wef-live approved scope v1.6 - 29 October 2026

Owner directs this module to remain locked to its latest approved implementation. This scope lock does not stop the three-minute collector. Preserve independent TODAY source collection and identity validation, Shows/Schedules reference reads, logging, existing links/timestamps, verified recurring archive/catalog retirement and current/future protections.

CURRENT dependency correction: Live does not read, qualify or wait for Entries. Its v1.6 reverse-read removal was separately approved, deployed19:33:37Eastern and verified by normal19:35/19:38runs (evidence/no-entries-read-20261029/release.md). Entries' separately approved linking-only restoration also removed its added Live reader and enrichment. Therefore neither current collector reads the other. Keep existing sync tables/native links/historical evidence intact; their presence does not create an active dependency. No Live preparation, enrichment reader, comparison work or new cross-module contract is pending.

Both v1.6 normal Live runs were empty activity; populated link preservation is offline-tested, not naturally proved. Preserve this limit; do not reopen work or manufacture activity to fill it. Separately approved v1.4 recurring retirement remains: verified archives/catalogs before eligible removal on successful valid empty/skipped runs; failed/partial/unresolved inputs do not authorize cleanup; current/future protected.

Reopen only for a verified defect or explicitly approved requirement. Defect investigation does not itself authorize fixes/deployment/task/schema/sync changes or record repairs. Consumer owners own their future reads; no future Entries input is implicitly assigned here. Older v1.5/Entries-read wording below is historical, superseded and not an active assignment. No runtime changes are made by this documentation lock.

## Reverse dependency removed - 29 October 2026 (v1.6)

Entries reads wef-live for downstream linking; Live independently collects observations. Live no longer reads the Entries reference, inspects its publication, or qualifies its schedule/participation results. The Airtable adapter rejects Entries reads and metadata discovery; the unused Entries base setting is removed. The existing sync/table/link fields remain intact to preserve native links and history, not as a collection dependency.

For collected observations, existing native entry links and original verification/check timestamps are omitted from writes and preserved. Previously current participation qualification is marked historical because Live no longer validates it. New observations do not create Entries links. Source identity validation, class/ring/group schedule links, class-error logging and recurring verified archive retirement are unchanged. Entries owns current downstream qualification. No manual runs, record repairs or source sync changes.

58 offline tests passed, including populated repeat collection with Entries reads forbidden, exact link/timestamp preservation, unresolved classes, logging and archive/retirement safeguards. Deployment: 19:33:37 Eastern through the existing scheduled task path. Two normal runs at 19:35 and 19:38 Eastern passed: matching Success logs, no errors, unchanged archive catalog and verified file hashes. Evidence: evidence/no-entries-read-20261029/release.md. Both were empty activity; populated behavior remains offline-tested.

## Prior v1.5 inspection - superseded by v1.6 above

**Entries reads wef-live for downstream linking. Live owns independent observation collection.** Entries is linking-only, not a collector, lane-comparison or tenant-selection engine. CWF is not a core input, dependency or acceptance requirement. Season is retention-only and never a current collection gate.

Implementation distinction: Entries collector.js now invokes live-input.js and allows GETs of Live live-entries, live-classes and collections. Live collector.js still reads the older wef-entries.entries reference and calls participation.resolve. This remaining reverse dependency is existing compatibility behavior, not the intended ownership direction. It was not removed by this documentation update; removing it requires separately approved code work preserving links. No populated downstream acceptance is claimed here.

Health checked 29 October at 19:30 Eastern: normal 19:26 and 19:29 runs completed Success, with matching heartbeat/run-logs and no error rows or logging errors. Three-minute cadence remains. All seven old-show archive catalogs are Retired; hashes and exact fields/links verified for 550 archived records. No show 200000075 observations remain active. Current activity is empty; no duplicate retirement occurred.

# Independent WEF live collector

Destination: `wef-live` (`appgzosUcVqA4vZdf`). Built 23 October 2026.
Current task is enabled and runs every three minutes. Earlier failure/recovery notes are historical evidence, not current task status.

## Existing reverse reference - prior implementation evidence — 29 October 2026

Live now consumes the accepted Entries publication through the existing read-only sync, including its five status/evidence fields. It checks exact identity, original-source freshness (30 minutes), unresolved newer attempts and local schedule availability. It no longer replays upstream schedule completeness/parity. A missing/stale mapping does not stop observations; previous links stay historical. Existing class/group schedule links and logs remain unchanged. See evidence/entries-contract-20261029/release.md for acceptance and scope.md v1.5 for the current contract.

## Current boundary — 29 October 2026

Live has no tenant-code imports or tenant-specific collection prerequisite. Entries is linking-only, reading accepted participation/schedule evidence and wef-live observations. Season is not a current collection gate. Historical release notes below are dated evidence, not current dependencies. See scope.md and HANDOFF.md for current ownership and cadence.

## Scope and authority

Read the current default record from `wef-shows.shows`: show ID, customer ID,
selected date, available dates and show start/end. Require live source identities
and dates to match. The selected date must also be the current New York day;
this is an upcoming-day guard, not an alternate show selector. A future selected
date produces an explicit no-activity success, with no live source request.
Canonical authority is base `appCaSmL819KK7UFJ`. The live collector requires one
customer-15 default with a valid show key and a checked timestamp within thirty
minutes. There is no schedules-show fallback and no discovery request.

Collect rings/groups and each available class's live trips independently of
tenant-specific feeds. Tenant slices happen downstream. Public ListAjax and
LiveScoreWidget supply the show/day graph. Public getLiveClassData supplies
trip observations; the unchanged decoded classes Worker supplies exact trip,
entry, horse, rider and trainer identities. No proxy routes were changed.

## Tables and references

`live-rings`, `live-groups`, `live-classes`, `live-entries`, `collections`,
`run-logs`, `heartbeat` and `errs-log` are destination-owned. `schema.js` lists fields and
native links; `evidence/schema-configured.json` records their Airtable IDs.

`wef-schedules.schedules` and `wef-entries.entries` are automatic read-only
native syncs. Their local record IDs are native link targets. The owner approved
one exception to keeping entries untouched: the `wef-live-reference` view,
containing only entry-key, show-id, date, entry-id, class-id, horse-id, rider-id
and status. External-workspace sync and edits from other bases are off. No
private collection/follow associations are shared. Existing entries records,
collector, field schema and task remain unchanged.

Missing reference rows do not prevent live observations. Missing or ambiguous
schedule matches are reported; entries without an available reference may have no binder
link. A failed reference read preserves existing links. No names-only joins.

## Execution and verification

Task `wef-live-collector` repeats every three minutes. Its hidden WScript launcher
runs PowerShell/Node without a console window; Windows sign-in and an awake
computer are still required. This is a separate task, not a Codex heartbeat.
Overlapping task instances are ignored; the execution limit is ten minutes.
`entry.js` is scheduled only. Never invoke it or `run.ps1` manually.

`configure.js` configures destination metadata only. `verify.js` reads normal-run
receipts, Airtable rows and archives, and writes local verification evidence.
The retirement release passed 62 offline tests and two normal scheduled runs. Heartbeat records each invocation; run-logs records outcomes and archives; errs-log records individual errors. Logging failures are isolated. A failed receipt does not permanently stop later scheduled invocations.

Verified archives precede eligible retirement, including on successful empty/skipped runs. Permanent catalogs remain in collections. Current/future records are protected. No manual runs or repairs are part of this workflow.

Schemas and runtime code for schedules and entries are protected. During
preflight, two schedules test files changed outside this work; their paths are
recorded in evidence/preflight.json. Collector runtime hashes stayed unchanged.

The local archives/credentials/operational receipts are under ignored runtime/.
Source inspection fixtures are diagnostic inputs, never scheduled-run proof.

## Normal-run evidence and required correction

Normal start: 23 October 2026, 10:32:12 Eastern; completed 10:34:30.
Run ID: 14a119c2-f1c6-4f77-a31e-6af424a84793. Task exit code 0.
Collected 6 rings, 17 groups, 30 classes and 195 trips; 68 archives verified.
Heartbeat/run-log readback passed. Four exact links to today's entry references
passed. Twenty-one classes linked to schedules; nine were left unlinked.

The source schedules are not established as broken: the new matcher counted
both active and dropped rows. Eight affected classes have one active schedule;
class 200030243 has two active schedules, one for each of its two live groups.
The existing stage keys identify these group contexts. Evidence is in
schedule-link-diagnosis.json and the normal-run JSON under evidence/.

Approved correction deployed on 23 October 2026: match only active schedule
rows using the existing show/date/class and group-specific stage keys; allow
multiple valid schedule links for multi-group classes; give each live group only
its own schedules. Add offline cases for dropped rows and multi-group classes,
then verify two normal runs. No source schedule changes or record repairs.

The task was re-enabled after all 35 entries/live offline tests passed, including
dropped schedules, multiple group-specific schedules, repeat identity stability,
canonical source reads, stale authority rejection and protected writes. Normal
acceptance is recorded under `../wef-shows/evidence/consumer-connection/`.


Final readback for this change: both consumers used canonical show record
`rec4En9UWd6lkT1Oh` directly during their first normal post-deployment run.
Live's 30 classes have 31 correct active schedule links across 17 groups;
all 248 pre-existing live identities and seven entries identities were retained.
This is partial verification, not two successful runs. Live failed on six HTTP
502 class responses and missing decoded identity for trip 200398019 in class
200030319. Entries failed on trainer 50121 HTTP 502. Both wrote Failed to
heartbeat/run-logs. Live remains disabled with its original five-minute trigger;
entries and the other existing task configurations remain unchanged. Source
investigation/correction and renewed normal-run acceptance remain outstanding.


## Source handling update — 23 October 2026, 13:02 Eastern

Approved source correction adds one bounded transient HTTP/timeout retry and
one paired snapshot refresh for a missing trip identity, with timestamped
public detail requests. It never substitutes guessed or historical identities.
Forty offline/deployed tests pass. Worker and schedules are unchanged.
The first normal live run still encountered a persistent 502 for class
200030347; further live starts are disabled. Full live acceptance remains blocked.
Evidence: `evidence/source-fix/`. No additional production changes were made
after this failed gate.


Final acceptance at 13:11 Eastern: entries completed normal runs at 13:04 and
13:09, with seven and nine entries respectively. Both had no source errors and
passed record/link/archive checks. The first full verification passed. The
second full verification was blocked by concurrent changes to
wef-tenants/airtable.js, collector.js and settings.js at 13:07, outside this
task's deployment file list. Those changes were left untouched; expected hashes
were not changed to hide the drift.
Live's 13:02 normal run ended Failed with persistent class 200030347 HTTP 502;
no missing-trip identity error remained and 68 archives verified. Live is disabled.
No production patch or manual retry followed that failed gate. Overall work is
incomplete: persistent 502, concurrent protected drift, and two live runs remain.
Full evidence: wef-live/evidence/source-fix/release-status.json.
