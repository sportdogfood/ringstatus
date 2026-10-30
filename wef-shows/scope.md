## LOCKED - wef-shows verified scope v1.4 - 29 October 2026

Runtime reverified and relocked **30 October 2026** after the separately approved
count-replay timeout repair. Two normal runs at 08:44 and 08:49 Eastern exercised
replay and published all four show-days; independent code review passed. Scope
v1.4 is unchanged. Exact runtime hashes, review and acceptance are in the
[locked release](evidence/count-replay-fix-20261030/README.md). Counts remain
subject to their original freshness/coverage rules; Stale is not fresh readiness.

Owner directs this module to remain locked. This is a scope lock, not a disabled collector or filesystem restriction. Preserve the five-minute task, canonical shows/show-days, discovery and source checks, Data count reader and protected replay, accepted local archive handoff, keys/links and verified logs/heartbeat. No build or preparation for Entries is pending.

Dependency direction: Entries reads canonical Shows; scheduled Shows does not read, write, qualify or trigger Entries. Count evidence comes directly from wef-data; the Season coordinator owns that producer separately. Season retention is not an intermediary or readiness gate. Older descriptions of Season-supplied summaries below are historical coordinator wording, superseded by this inspected direct-reader clarification. No count replay or archive check is removed.

Reopen only for a verified defect or explicitly approved requirement. A verified defect authorizes investigation, not an unapproved fix, deployment, schema/task change or record repair. Earlier proposals do not reopen this scope. Existing independently approved operation continues.

# wef-shows scope

Version: 1.4
Date: 2026-10-29 (America/New_York)
Owner: wef-shows
Coordination: wef-records-coordinator

## Purpose

wef-shows owns the trusted show identity, dates and show-day information, tracks changes and evidence, and supplies schedules, entries, live and Season. It shares ownership, changes, verification and unresolved handoffs with Records coordinator.

Season supplies count evidence; Shows validates and publishes canonical show-days. These are independent responsibilities. Sharing information does not trigger another collector.

## Simple ownership split — October 29 clarification

| Owner | Simplest scope |
|---|---|
| Shows | Discover and publish trusted show identity, name, dates and source-selected date; preserve discovery/change evidence; publish canonical shows and show-day summaries. |
| Data | Collect and normalize detailed records; determine unique-entry and trip counts, completeness and original collection timestamps. |
| Season | Manage collection across shows and dates, preserve historical collections, and supply verified daily summaries to Shows. Data acquisition and Season history remain distinct responsibilities coordinated by wef-season-coordinator. |

Shows supplies show identity and dates to Schedules, Data and Season. Data supplies collected detail to Season; Season supplies verified daily summaries to Shows. Each workflow operates independently; these arrows describe information handoffs, not job triggers.

Preserve the working discovery, stable IDs and downstream links, the archived default-schedule payload that Schedules currently reads, published show-days and their links, and the separate sources, heartbeat, run-logs and err-logs tables. This clarification changes documentation only, not current execution or verification status.

Shows does not need to collect individual horses, riders, entries or trips; manage tenant/CWF selection; start downstream collectors; or calculate timing/live status. Those are outside its scope, not instructions to remove existing shared utilities or dependencies.

**Simplification candidate, not approved for removal:** Shows currently replays retained count evidence and recomputes counts as part of validation. The intended simpler boundary is for Data/Season to verify the counts and for Shows to check the summary's show, date, freshness and completeness before publication. Preserve the existing full count replay until a verified Season summary handoff can safely replace that check and the owner explicitly approves the implementation change. Do not remove or weaken validation under this documentation approval.

The owner resolved the discovery cadence on October 29 as recorded below. The separately approved fetch/heartbeat correction passed two normal runs; its evidence is linked below. Neither decision removes heartbeat or count replay.

## Correction to the earlier description

The earlier description was too narrow. Five minutes describes the current implementation; it does not explain the intended discovery policy. RingStatus Operating Skills had not been loaded before that answer. Its Instruction Loader and Rebalancer were subsequently read and applied during the correction.

The later Shows README described five-minute discovery, conflicting with the October 15 stop-after-discovery decision. That conflict should have been surfaced rather than presenting the running cadence as the intended policy. The October 29 owner decision below now resolves the cadence conflict explicitly.

## Accepted discovery policy — October 29, 2026

Owner decision: "Keep the five-minute schedule. Update scope.md and tell the coordinator Sunday 4–8 p.m. Eastern is the expected rollover window, superseding the stop-after-discovery rule. Keep the archive handoff separate. No code or Airtable changes."

- Retain the existing five-minute schedule throughout the week, including after a default-show change is recorded.
- Sunday, 4-8 p.m. America/New_York (Eastern, following daylight saving), is the expected rollover window, not the exclusive checking window or a guaranteed source-change time.
- This decision supersedes the October 15 stop-after-discovery rule and its restriction against five-minute discovery. No separate within-window cadence is needed.
- No additional six-hour safeguard is adopted or enabled; it remains only a historical proposal.
- Schedule availability: separately expected 4-8 p.m. Eastern the day before the show starts. This is not a discovery-triggered collection instruction.
- Preserve already-approved canonical publication and independent downstream readers. Discovery does not trigger downstream collectors or authorize additional business actions.

Historical source: [October 15 owner clarification](../docs/coordination/wef-schedules-only-activation-plan-20261015.md), beginning at line 5. Preserve it as history; the accepted cadence above controls. The existing task remains unchanged. This is a documentation decision, not a code, Airtable or scheduler change. The separate archive decision below accepts the local handoff and defers portability.

## Accepted local archive handoff — October 29, 2026

Owner decision: keep the local archive handoff for now; portability is deferred, not a blocker. Preserve existing archives and verification checks. No code or Airtable changes.

- Shows continues to produce and preserve the local source payload. Airtable retains its file location and verification fingerprint; Schedules continues to read and validate that file on the same machine.
- Preserve existing archive files, archive-before-delete protection, allowed-path checks, SHA256 verification and show/date consistency checks. This decision authorizes no deletion, migration, weakened validation or replacement handoff.
- The local-file dependency remains real: an unavailable or invalid required file still prevents its dependent consumption. Deferring portability does not bypass those checks.
- Shows owns the produced evidence; Schedules owns consumption and validation. Data/Season retains its distinct broader history responsibility; Records coordinates any future handoff.
- Portability is deferred future work, not an unresolved blocker to the accepted current workflow. Revisit it if collectors move off this machine or shared access is needed, with separate scope, approval and replacement verification before switching.

## Ownership and handoffs

| Owner | Responsibility |
|---|---|
| wef-shows | Canonical show/customer identity, show dates, source-selected date, show-days, validated publication, discovery health, retained change evidence and its own run logs. |
| Data (coordinated by wef-season-coordinator) | Detailed acquisition and normalization; unique-entry/trip counts, scope, completeness, original source observation times and retained identities. |
| Season (coordinated by wef-season-coordinator) | Collection across shows/dates, historical collections and the verified daily-summary handoff to Shows. |
| wef-schedules | Collect operational schedules using canonical show context. |
| wef-entries | Link collected Data/Season and Schedules records; no independent source-entry acquisition. |
| wef-live | Independent current observations using Shows authority. |
| wef-records-coordinator | Record accepted scope, changes, verification, dependencies and handoffs; coordinate unresolved ownership and policy conflicts. |

The closed WEF collection flows through Shows, Schedules, Data and Season (both under wef-season-coordinator); Entries is a linking module. CWF is outside this workflow and is not an input, output, dependency or acceptance gate. Shows does not own tenant selections, downstream execution, timing calculations or whole-show enumeration by assumption. The source-selected date is not automatically TODAY.

## Current implementation and verification

Canonical base: `appCaSmL819KK7UFJ`.

- `shows`: `tblJA5CUon5s1sWib`.
- `show-days`: `tbl14MaCCDw6dAywH`, linked locally to `shows` with a reciprocal `show-days` field.
- Count publication includes counts, coverage, original source time, accepted evidence and publication status. Missing or incomplete information must not become a verified zero. Competition-only coverage does not establish schooling attendance or whole-day emptiness.
- Five-minute zero validity is measured from source observation time. It is separate from discovery cadence and does not authorize a Live suppression gate.

Historical correction: the supplied review text said, "That addition remains unbuilt. No changes made during this review." That accurately described the earlier read-only review, not the subsequent release. Canonical show-days publication was deployed on October 29 at 10:01:45 a.m. Eastern and verified through two normal scheduled Shows runs, starting at 10:04 and 10:09 a.m., consuming two distinct Data revisions. Four day IDs and their native show links remained stable. No manual collector runs or record repairs were used.

Evidence: [release record](evidence/show-days-build-20261029/release.md) and [two normal verification pairs](evidence/show-days-build-20261029/two-normal-pairs.json).

## Operational records

The October 29 approved telemetry addition separates four responsibilities:

- **sources** (`tbld8rqxkXyRzCeVK`): the three existing inputs (default-ring, default-schedule, retained Season/Data counts), their location, purpose, owner, latest attempt/result and last successful read. Source success is successful input access/validation, not a fresh or complete day-count claim.
- **heartbeat**: the existing latest completed collector result, unchanged.
- **run-logs**: every collector attempt, including successful, partial and failed runs, its timestamps, row count, summary, archives, error summary and links to attempted sources.
- **err-logs** (`tblhQWyblTduQHV7c`): separate actual errors with timestamp, phase, message and originating run/source links where available. The scalar run ID and source key retain identity even when a linked record could not be created.

Normal recurring code creates the source records and error records; this release does not backfill or repair records. Successful runs do not create placeholder errors. Error-linked run rows are excluded from the existing archived log-slot reuse so their links cannot be reassigned to later attempts. Reused successful slots clear their old source associations after archival. If all existing slots are protected, append a new run instead of blocking collection; the existing log limit becomes a soft retention target. Source/error logging health is recorded separately in the local report and run-log summary/errors, without changing a successful business heartbeat to Partial.

Startup failures before the collector begins, unavailable Airtable access, or forced process termination may prevent Airtable logging. Existing local execution logs/report remain the fallback evidence; no independent error-delivery guarantee is claimed. No failure is manufactured in production to test error logging.

Release verification: **PARTIAL**, October 29. All 39 offline/deployed tests passed. The normal 1:09 p.m. Eastern run succeeded and its three source/run links, timestamps and heartbeat were verified. The normal 1:14 p.m. run failed during default-ring discovery (`fetch failed`), followed by a heartbeat PATCH HTTP 422. Both real errors were saved in `err-logs` and linked to the failed run; the source failure also links to default-ring. Its previous successful source timestamp was preserved. Heartbeat still displays the previous successful run because its failure update was rejected; it must not be interpreted as current health. Two successful normal-run acceptance remains incomplete. Verification stopped without a patch, manual run, retry or record repair. A separate APPROVED TO EDIT is required for fixes after the failed scheduled verification. See [telemetry release evidence](evidence/sources-errors-build-20261029/release.md).

Subsequent correction: the owner separately approved fixing the source fetch and heartbeat failures. The deployed correction passed 45 tests and two normal scheduled runs at 1:49 and 1:54 p.m. Eastern on October 29, with Airtable readback and count replay verification. Records acknowledged that bounded correction as COMPLETE. The preceding PARTIAL paragraph describes the original failed release, not current acceptance. The original network cause was not retained; failure/recovery paths were tested offline. See [verified correction](evidence/fetch-heartbeat-fix-20261029/release.md).

## Open items and boundaries

### Code-review procedure — owner direction, 30 October 2026

Review material authorized changes before deployment and review after observed
behavior or verification failures. Use the Code Review skill, actual baseline and
deployed hashes, independent implementation review, actionable findings and
separate offline/deployment/normal-run gates. Reuse an unchanged reviewed release
after verifying its hashes and boundaries; do not repeat review per collector run
or create review timers/monitors. A failed review does not authorize fixes.

Record module/base/release/baseline/hash manifest, reviewer/date/scope, findings
with severity/trigger/consequence/smallest correction, tests actually run or
inspected, scheduled evidence, gate results, risks and the exact next authorized
action. Runtime scope remains locked v1.4. For this request, Records alone creates
or updates the Airtable review schema/records and verifies readback in Shows base
`appCaSmL819KK7UFJ`; Shows supplies the report, not Airtable writes. Current report:
[review handoff](evidence/count-replay-fix-20261030/review-handoff.md).

- Discovery cadence is resolved. The local archive arrangement is separately accepted; portability is deferred and is not a current workflow blocker.
- Live count consumption or suppression requires its own approved implementation and verification; Shows publication alone does not activate it.
- The earlier release recorded a separate CWF termination as historical context. The owner subsequently removed CWF from WEF workflows; it is not a gate for this release.
- The existing local archive handoff remains a protected implementation dependency. No archive migration or portability work is authorized by this document.

Version 1.1 recorded the owner-approved sources/err-logs build. Version 1.2 documented the simpler Shows/Data/Season ownership split and protected count replay pending an explicitly approved replacement. Version 1.3 recorded the owner's accepted five-minute cadence, Sunday expected rollover window and superseded stop rule, and referenced the separately completed repair. Version 1.4 accepts the existing local archive handoff and defers portability as non-blocking future work. Ownership, count replay, existing archives and verification checks remain protected. This update authorizes no code, Airtable, task, archive-handoff implementation or downstream changes.
