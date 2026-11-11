# Current release — unique name fallback for day-0 trips

**Version 2026.11.08.16 · Deployed November 8, 2026, 1:41 p.m. Eastern.** Empty w-horses/w-riders/w-trainers links on day-0 live-trips now use one unique helper-name match only when the source ID is blank. Supplied IDs retain priority. Names ignore case and surrounding/repeated spaces; punctuation and diacritics remain significant. Ambiguous/missing matches stay empty. Existing populated links, helper records and source ID fields are preserved. No fallback on live-entries.

Only linker.js changed;26otherJS, task, collection, endpoints, decoded pause and cleanup/logging remain unchanged.12offline checks and author review passed (not independent). Normal13:42 and13:48Eastern runs verified306horse,297rider and298trainer links on306day-0 trips. Ambiguous slots stayed empty; existing links/source IDs retained. Repeat linker wrote zero links. [Acceptance](evidence/trip-name-links-20261108/ACCEPTANCE.md). [Task/evidence](evidence/trip-name-links-20261108/TASK.md), [tests](evidence/trip-name-links-20261108/offline.json), [review](evidence/trip-name-links-20261108/review.md), [deployment](evidence/trip-name-links-20261108/deployed.json). This supersedes older ID-only matching wording only for blank-ID day-0 trips.

# Current release — day-0 trips from the existing live response

**Version 2026.11.08.15 · Deployed November 8, 2026, 11:05 a.m. Eastern.** Each already-fetched class-details response now populates live-classes, live-entries and day-0 live-trips. No second request and no decoded request. Existing source identity/results absent from the live payload and populated native links are preserved; missing IDs are not invented. Current day/class complete observations retain Active/Absent/Dropped rules. The existing empty-only linker connects trips.

Only lanes.js and live-trips.js changed;25otherJS, task, endpoints, cleanup, logs and linker remain unchanged.10offline checks and author review passed (not independent). Normal11:06 and11:12Eastern runs verified304day-0 trips,52unique class requests perrun,0decoded requests,0repeat trip creates and all304class/group/ring/show/day/schedule/live-entry links retained. Both scheduled tasks finished. [Acceptance](evidence/live-response-trips-20261108/ACCEPTANCE.md). [Release evidence](evidence/live-response-trips-20261108/TASK.md), [tests](evidence/live-response-trips-20261108/offline.json), [review](evidence/live-response-trips-20261108/review.md), [deployment](evidence/live-response-trips-20261108/deployed.json). Earlier claims that all trip updates are paused are superseded: decoded acquisition is paused; live-response trip updates are active.

# Current release — error repeat suppression and 500-record cleanup target

**Version 2026.11.08.14 · Deployed November 8, 2026, 10:08 a.m. Eastern.** `errs-log` remains enabled. New/changed conditions are logged once; unchanged conditions do not create repeated rows. Full affected-record details remain in saved receipts. A clear run permits a later recurrence. Uncertain error-write outcomes stay visible locally and are not blindly retried.

At 1,000 or more logging records, one cleanup deletes the entire eligible excess until 500 remain, through existing guarded batches. It does not stop after deleting 500. Active/current records remain protected. The 7 p.m. Eastern and new-show delete-only cleanup remain. No collection, source, business-link, task or decoded-pause changes. No Records alerts, manual runs or record repairs.

Tests and independent review passed. Normal10:09 and10:15 runs verified three new condition rows followed by zero unchanged repeats. One cleanup deleted1,893 excess rows and native readback confirmed500remain; repeat cleanup Skipped. Missing source entity IDs remain unresolved. Exact code hashes, review and live evidence: [release](evidence/error-repeat-correction-20261108/RELEASE.md), [deployment](evidence/error-repeat-correction-20261108/deployed.json). This supersedes the incorrect brief errs-log-disable release and the earlier 500-per-invocation limit. Earlier release sections retain historical evidence only.

# Current release — horse, rider and trainer links

**Version 2026.11.07.12 · Deployed November 7, 2026, 10:37 p.m. Eastern.** Owner-approved extension of the existing stage7 linker.

| Destination | New empty-only native fields | Exact source IDs |
| --- | --- | --- |
| live-trips | w-horses, w-riders, w-trainers | horse-id, rider-id, trainer-id |
| live-entries | w-horses, w-riders, w-trainers | horse-id, rider-id, trainer-id |

Each field points to the corresponding existing shared table. One unique ID match fills the empty link. Names/nicknames are not used to guess. Populated links, helpers and business fields are preserved; missing IDs/references or duplicate matches remain empty and individually logged. Helpers are read-only to this code. Rings/groups/classes keep their existing child relationships; no direct entity rollups were added.

Six native field targets and their automatic inverses are verified.25offlinechecks and author code review passed (not independent). Only linker.js,airtable.js,schema.js changed;23otherJS, collection stages, direct feeds, decoded pause, cleanup and task unchanged. No Records notifications, source calls/manual linker run, record repairs, historical backfill or new schedule/observer.

The normal **10:39 p.m.** dispatcher completed with task exit0 after installation. Its existing policy kept collection/linking idle; the last actual linker receipt predates this deployment. This is dispatcher proof only.

**Population remains unverified:** the linker still runs after normal eligible day0 collection. Current date/window rules keep it idle tonight. Existing source IDs are required; paused decoded acquisition is not resumed and missing IDs are not synthesized. Do not call schema/code installation proof of populated scheduled linking. Next acceptance is a naturally eligible linker pass and native entity-link readback, with repeat writes checked only when that pass occurs.

**Field rule:** any field beginning `___` is likely retired or the wrong target. Verify/use its replacement and record the mapping when needed; preserve old values. Owner mappings: `___wef-shows.show-days` → `w-days`; `___wef-shows.show` → `w-shows`. Active linker refs already use those replacements. The detailed references and module rule follow below and in AGENTS.md.

[Tests](evidence/entity-links-20261107/offline.json), [review](evidence/entity-links-20261107/review.md), [six fields](evidence/entity-links-20261107/created-fields.json), [native target readback](evidence/entity-links-20261107/schema-readback.json), [installed26JS](evidence/entity-links-20261107/deployed.json). Older sections are historical where superseded.

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

**Normal-run proof:** November7 **7:15 p.m. Eastern**, status run `d602e778-0b69-45fb-b867-e41990d67d2d` returned **true**, with zero stage/logging errors. Existing date-mismatch warning: Shows priority0 November8, today November7. Existing rule kept detailed collection idle; next status due8:15p.m. Three detail feeds remain **unverified through scheduled collection** until a naturally eligible day0 window. [Acceptance](evidence/direct-four-headers-20261107/acceptance.json).

# WEF Live — locked protected baseline

**Version 2026.11.07.5 · Locked 2026-11-07T10:26:13-05:00 · America/New_York.**

Current linker refs/fields are w-shows,w-days and unchangedData schedules. Old filledshow/show-day fields retained. Existingclass.groups↔group.live-classes reciprocal pair verified and unchanged. All stages4–6/source/cadence/task/cleanup remain unchanged;decoded requests paused. Empty-only linking remains;missingmatches stayempty/logrecommendno retryuntilsourcechanges. No retrycache/newgates added.

Owner approval is required for further business code/schema/source/cadence/cleanup/deployment/resume changes. Normal scheduled operations and authorizedread-onlychecks continue. No ACL/task/process permission change. No manual collector/provider runs or record repairs.

[Protected26JS/task/docs baseline](evidence/w-shares-20261107/locked-baseline.json) · [deployed manifest](evidence/w-shares-20261107/deployed.json) · [README](README.md).

17offlinechecks/authorreviewPASS(notindependent). Normal10:15/10:21passes:1,936neww-fields on968records,then0repeatwrites;5,197populatedforwardfieldsretained;423tripbusinessrowsfrozen;decodedcalls0. NativeSourcekey/readback,class-groupbothdirections andparentlog/heartbeat checks are in [acceptance](evidence/w-shares-20261107/acceptance.json). SevenexistingDropped-onlyDataschedulelinks stayempty/logged. No fatalstage/logfailure orrevert.

## Known trouble and limits retained

- Seven unmatched Data fields: retainedgroups200028530/200028533,class200031158 andtwo trips/twoentries matchonlyDroppedData schedules. No guessing;individual logrecommendation,notneweligibilitycontrol.
- Immediate Records chat transport is not configured;local/Airtable logging is notchatdelivery.
- Cleanup conditionalP2s: unexecutedplans maystayRunning;interruptedstop mayleave stale latest/Airtable summary.
- Widget{} can silentlyyieldnoringrows;original3collectionheartbeatshow/date cells remainblank(linker includes them).
- Pauseddecoded business data/results/IDs notrefreshed;newUI-onlyentries maylackentityIDs,neverinvented.
- Checkinterval3minutes doesnotpromise3minutecompletion;IgnoreNew/PT10Munchanged. Shows clockcanlag;existingfallbackremains. Emptyread/PATCHisnotatomicCASagainstconcurrentedits;native reciprocalappends expected.

Decodedtrips remains the workflowdesign discussion. Cross-basepublication/formerstage8 andold24hretentionheld;approveddelete-onlycleanup active. Existingrecords/links/evidence/catalogs preserved;nonewcleanuparchives or schedules. [Reviewed3filecode-onlyrevert](evidence/w-shares-20261107/revert.ps1) isowner-authorizedonfailedrollout,retainsfields/links/data,andrefusesinterveningedits;notneeded. [Priorlock](evidence/w-shares-20261107/previous-LOCK.md).
