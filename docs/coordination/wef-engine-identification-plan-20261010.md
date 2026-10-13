# WEF show-engine — current-system identification plan

Date: 10 October 2026  
Status: plan prepared; identification not complete. No implementation authorized.  
Requirements: [Owner discussion overview](wef-engine-discussion-overview-20261010.md).

## Outcome

Produce one evidence-backed map of the existing working WEF engine: where it starts, every stage and smaller lane it uses, the code and data each lane owns, how Timing supplies downstream outputs, and how the whole workflow repeats and resets. The owner must be able to correct this map without rediscovering code for the agent.

The deliverable is an identification report appended to these documents or linked from them when authorized—not a new architecture, code rewrite, implementation handoff, or claim of fresh runtime proof.

## Evidence rules

- Owner statements define intended behavior; inspect working source before interpreting its implementation.
- Identify every relevant source by exact local path and version/hash where available. Follow imports, wrappers, subprocess calls and external automation references to their actual implementations.
- Label findings as owner-confirmed, source-observed, historically demonstrated, currently demonstrated, or unresolved. Never turn a historical PASS, local test count or HTTP success into current full-workflow proof.
- Preserve the two Engines tables as the stage authority. Record any disagreement among registered stage, actual invocation order and data dependency. Do not silently reorder any of them.
- Record apparent redundancy with callers, behavior, side effects and evidence. Do not remove it or recommend removal merely because names look duplicated.
- Use existing logs and artifacts read-only. No production workflow execution, dry-run script invocation, direct endpoint substitutes, record repairs or credentials disclosure.

## Investigation sequence

### Locked two-day scope for this trace

Use the [TODAY/live and tomorrow/staging owner lock](wef-engine-discussion-overview-20261010.md#locked-todaylive-and-tomorrowstaging). Each show date has the same -1 preparation / 0 live lifecycle, with only TODAY and tomorrow concurrently. Manual `focus_day` is accepted; when live is active it identifies TODAY. Display/calculate TODAY and build tomorrow independently. No complex day-by-day calendar logic or automatic rollover is required.

The owner states that tomorrow preparation may be missing from current code. Classify every relevant path as: current-day working behavior; future-date acquisition/cache only; actual staged data; staged-data consumer; or missing/unverified. Do not assume a future-date fetch proves staging, or that NIGHT means changing TODAY to tomorrow. Trace any broader future-day loops as legacy behavior, not as the approved two-day target.

Specific questions: which date is fetched; which date is written into each row/key; whether future data is only cached or also written; whether current-day links/fields are preserved; which records calculations and publication select; and whether a failure preparing tomorrow can interrupt TODAY. Record observations without executing the path or correcting code.

| Pass | Read-only work | Required evidence before proceeding |
| --- | --- | --- |
| 1. Locate the working baseline | Compare the Engines register with local source locations, relevant Git history, runner entrypoints and existing schedule definitions. Resolve the incomplete old runner folder by locating the actual source; do not initialize or repair it. | Exact authoritative source set or a narrowly stated unresolved location; entrypoint-to-dispatch chain; active/configured versus historical distinction. |
| 2. Trace Heartbeat and scope | Follow show/customer/week/day selection, modes, A/B/C/D slots, cadence changes, overlap controls, next-day shift and multi-show behavior. Read referenced configuration without exposing secrets. | One cycle/control diagram and a cadence table, including skip and failure conditions. Explicitly distinguish source defaults from verified deployed settings. |
| 3. Trace upstream data, stages 2–5 | Follow acquisition, fallback, normalization, classes/entries and live state. Map matching keys and field writers to legacy tables, views and relevant Airtable automations. | Inputs/outputs and precedence for each lane, including show-week/day boundaries, manual overrides, deletion/clearing protection and fresh-versus-fallback handling. |
| 4. Deeply trace Timing, stage 6 | Follow both calculators, wrappers, tagger calculations and all referenced helpers. Identify actual equations, units, time zones, assumptions, ordering, pace/duration sources and invalidation rules. Trace each calculated field to every reader. | Calculation-to-consumer map with existing input/output examples. Explain current-cycle versus previous-cycle state and changed-input recalculation. Record possible additional outputs separately as ideas for owner review, never new requirements. |
| 5. Trace Results, Outputs, Alerts and Two-way | Locate all existing implementations for stages 7–11, including smaller logic embedded in other scripts and external automations. Follow publication queue, bulk/differ output, proxy/storage paths, endpoints and consumers. | Existing behavior boundaries and dependencies for Results, Publish and Messages; exact missing references. No assumption that a stage requires its own new service, base or script. |
| 6. Trace repetition, rollover and audit | Use existing run evidence to follow unchanged cycles, updates, delayed/empty data, partial failure, overlap/restart and day/week rollover. Trace audit writers alongside each stage. | Scenario matrix describing observed behavior, evidence and remaining uncertainty; distinguish designed behavior from demonstrated recurring execution. |
| 7. Compare proven references | Inspect only relevant WEC stages and tested RingWaze concepts once the WEF baseline is clear. Identify what can inform the work and what cannot because WEF data is richer/different. | Reference path/version, demonstrated behavior, compatibility limits and proposed relevance. Catalyst remains optional; no exclusive-platform decision. |
| 8. Reconcile with owner | Present the stage map, Timing dependencies, source inconsistencies and concise unresolved business questions. Recommend only the minimum next phase supported by evidence. | Owner agreement that the identified system and boundaries are accurate before implementation planning, agents or handoffs. |

## First-pass depth adjustment — owner clarification

Continue the first pass at stage/function/connection level. Its deliverable is the end-to-end map in the discussion overview, including external automation and consumer boundaries. Do not exhaust the detailed Timing pass before identifying the full system. Start-time and go-time are distinct; their business definitions, equations and edge cases can be agreed in later passes. Preserve the detailed source notes already collected without treating them as accepted definitions. The eight passes above remain the overall identification roadmap; they need not each be exhausted in this first traversal.

## Lane evidence format

Use one compact matrix rather than a new set of management tables. For each of the 26 registered lanes and any discovered supporting lane, capture:

1. Registered stage/lane and its purpose.
2. Exact script, helper, wrapper or automation, plus source version.
3. Caller/trigger, mode/slot, frequency and scope.
4. Source endpoint/file/table/view and required matching keys.
5. Transformation or calculation and fields it owns.
6. Output table/file/endpoint, readers and consumer expectations.
7. State retained between cycles, change detection, fallback and manual override behavior.
8. Failure propagation, retry ownership, duplicate protection and audit destination.
9. Existing evidence and unresolved questions.

One script can appear in several stage rows with separate responsibilities. Multiple wrappers can point to the same implementation without being counted as separate independent algorithms.

## Recurring behavior to establish

These are later verification scenarios to identify from existing evidence now; this plan does not authorize executing them.

| Scenario | Question the map must answer |
| --- | --- |
| Same inputs across many cycles | What is intentionally repeated, what is skipped, and what prevents duplicate side effects? |
| Changed schedule, entry or live state | Which calculations and outputs become eligible, and when do consumers see the change? |
| Missing, empty, delayed or wrong-scope data | What is preserved, blocked, logged or served as fallback? |
| Upstream lane failure | Which dependent lanes stop and which independent work continues? |
| Long cycle, overlapping trigger or restart | What locks/state apply, and who owns recovery? |
| TODAY live alongside tomorrow preparation | Can tomorrow be acquired/staged without changing TODAY's focus, live calculations or output? Distinguish observed behavior from the desired two-day model. |
| Day end and tomorrow preparation | What resets and carries forward? How is the manually selected focus day applied? Is a staged-to-live transition present, absent or unverified? |
| New show week or multiple focused shows | What is isolated by show/customer/day, and what is shared? |
| Output publication or endpoint failure | How are pending output, retry state and freshness represented? |
| Owner/manual correction | Which values are protected and when does automated processing resume? |

Do not promise capacity for thousands of cycles from source inspection alone. First identify configured frequency, per-cycle work, known run durations, API limits and existing operational evidence. Any fresh capacity or live repeatability test requires its own agreed execution scope.

## Skills and connections

Load only the skills needed for the current pass. This is a proposed applicability list, not a claim that every skill is loaded or every connection tested.

| Capability | Use |
| --- | --- |
| Instruction Loader / Planning | Applicable scope, source authority, protected baseline and bounded investigation. Planning was read for this document task. |
| Code Review | Read-only dependency and side-effect review; no correction permission. |
| Airtable schema/filter guidance and connector | Live stage register, schemas, relationships and narrowly selected nonsecret control/automation evidence. Prefer connector over browser. |
| Verification Before Completion | Ensure the identification report supports each claim; separate historical operation from fresh execution. |
| Systematic Debugging | Only for a specific evidenced discrepancy; not an open-ended repair exercise. |
| Cloudflare, Catalyst, Twilio, Webflow/Astro skills | Only after a traced boundary demonstrates their relevance. Inspect specific existing interfaces; do not import unrelated platform patterns. |

Connection inventory must cover Airtable; local files/Git and Windows runner configuration; Wellington/SGL API and rendered-source fetch paths; GitHub publication destinations; Cloudflare proxies/endpoints; existing Airtable automations; and traced Messages/Publish interfaces. WEC/Catalyst and RingWaze are comparison sources. For each connection record purpose, caller, target, nonsecret configuration location and evidence of access. Available tools do not by themselves prove a working runtime connection.

## Time and context controls

Proposed execution budget for the first identification session: 45 minutes of read-only tracing plus 15 minutes to reconcile findings. This is a proposed investigation timebox, not authorization to run production or a promise that full identification fits within one hour.

- Check elapsed time and concrete progress at each pass boundary and by 30 minutes; use the existing context-check procedure without creating monitors or hooks.
- Reuse the first inspection and narrow reads; do not repeatedly dump full schemas, code files or conversation history.
- At the session limit, preserve the exact source checkpoint, completed lane coverage, unresolved items and next read-only action in the identification record. Do not silently extend, launch another agent or manufacture completeness.
- If a source is missing, search established local references/history once in a bounded way, then state the exact missing item. Continue independent trace work.
- A discrepancy changes the evidence map, not the code. A failed production path is never rerun as part of this identification plan.

## Completion criteria for identification

Identification is ready for owner review when every registered lane has a trace or explicit gap; the correct local baseline is located; helpers and external dependencies are accounted for; Heartbeat and daily resets are explained; Timing outputs have named downstream readers; Results/Alerts/Two-way boundaries are understandable; and all remaining uncertainties are visible without guesses.

Implementation remains a separate decision. No new code, agents, subagents or handoffs until the owner agrees that the WEF understanding is comprehensive. A finished report alone does not mark the engine newly tested, migrated or complete.

## Resume here

### Schedule and two-day trace — source observations, 10 October 2026

No engine scripts or endpoints were executed for these findings. They describe inspected code, not successful recent staging. Local references below are relative to the approved RingStatus checkout.

| Finding | Source | Implication for the locked target |
| --- | --- | --- |
| Schedule target is exactly the selected `focus_day`; `candidateDateFromMode` does not add a day. | `schedules_dailyv2.js:297`, `schedules_dailyv2.js:1655`; `tagger.js:844` | Supports the owner's manual focus control. NIGHT/shift labels alone do not establish tomorrow preparation. |
| Forward fetching defaults on and walks subsequent dates through show end, capped at 14; it saves successful payloads locally. | `schedules_dailyv2.js:99`, `schedules_dailyv2.js:1259`, `schedules_dailyv2.js:1272` | Existing reusable acquisition behavior is broader than the locked two-day target. This is not permission to remove or alter it. No dedicated 5 PM gate was observed in this forward loop. |
| The forward path is called after successful current-date acquisition. It is skipped when the current dated payload uses fallback. Individual forward-fetch failures are caught and recorded; they do not immediately throw from the loop. | `schedules_dailyv2.js:2623`, `schedules_dailyv2.js:1308` | Tomorrow preparation is coupled to the current acquisition path. A failing current fetch can prevent forward work. Forward fetching is awaited before the write phase, not an independently scheduled lane. |
| Successful forward payloads are normalized and added to the same create/update batches for `watch_schedule`. Date-bearing keys are used. Rows are marked `prefetch_new`/`prefetch_existing`, with no Heartbeat link and `is_current_scope=false`, `is_target=false`. | `schedules_dailyv2.js:2876`, `schedules_dailyv2.js:2940`, `schedules_dailyv2.js:1984`, `schedules_dailyv2.js:3084` | There is actual future-record write code, not only caching. It is still not proof that tomorrow's complete staged output works. Current and future writes share a batch path. |
| A later show-wide scope-sync pass selects current rows by their schedule date versus the active scope, and applies the active `focus_day`/show-scope metadata across the show's rows. | `schedules_dailyv2.js:2172`, `lib/scope_fields.js:76` | Row schedule date and active focus metadata have different meanings. Do not treat every `focus_day` field as the row's scheduled date or assume all metadata represents independent staging. |
| Clock-derived DAY ends at 4:59 PM; NIGHT starts at 5 PM. Under NIGHT the slot orchestrator excludes schedule calculation and live groups/rings/class-detail; the schedule calculator also returns early outside DAY. Manual mode overrides exist. | `tagger.js:939`, `lib/default_show_date_guard.js:147`, `heartbeat_slot_orchestrator.js:1895`, `schedules_calculatorv2.js:1634` | With automatic clock mode, today's live refresh/calculation does not simply continue after tomorrow's release time. This is a source-level mismatch to reconcile with the locked target, not a claim that all live paths stop or that a current show is failing. |
| Schedule calculation loads its configured Airtable view (default `enrich`), derives target days from returned rows, and joins live groups. Publication loads the table/view specified by each queue record (or configured defaults). | `schedules_calculatorv2.js:16`, `schedules_calculatorv2.js:970`, `schedules_calculatorv2.js:1717`; `publisher.js:816`, `publisher.js:838` | TODAY-only calculation/display cannot be proven from noncurrent flags alone. Verify actual view membership/filtering and queue configuration before claiming isolation. A misleading calculator no-row message says “heartbeat view” although the default selector is `enrich`. |

Remaining trace: actual schedule calculation view selection/membership and publisher queue destinations; trip date selection feeding Timing; existing staged-data consumers and whether first-show-day preparation has operational evidence. Preserve the current working paths. Do not invent automation to fill these gaps.

Completed: live schemas and 11-stage/26-lane register inspected; initial core source trace; owner discussion consolidated; this plan prepared. Application files and Airtable remain unchanged by this documentation task.

Source/Heartbeat checkpoint: the Windows `ringstatus-heartbeat` task points to the approved repository. Its inspected October 10, 9:05 PM Eastern log records 15 tracked files matching current local hashes. It runs `tagger.js` → `heartbeat_patterns.js` → `heartbeat_task_cadence.js` → detached `heartbeat_slot_orchestrator.js`. The inspected cycles have no active WEF show scope, so they prove Heartbeat operation, not a fresh full-engine run. `C:/actions-runner/ringstatus` is the log/runtime-state location for this path. The older full-pipeline task's last inspected execution is August 1; its enabled setting is not evidence of duplicate execution today.

### Publication, trip selection and Timing continuation — 10 October 2026

Source checkpoint remains `a5235466eed1b094a26db14ab34b514726abf91d`. These are source/configuration observations; no engine, endpoint, publication or production-data operation was executed. Queue records were read through the Airtable connector. Their active flags do not prove current selection by the publisher's `all_active` view, dirty filter or lane gate.

#### Publication selections read from the existing queue

| Queue record | Source table/view | Output |
| --- | --- | --- |
| `recD4It9Bl6MWkYFX` | `watch_schedule` / `commit-all` | `docs/schedule/data/print/schedule.json`, `docs/schedule/data/latest/schedule.json` |
| `reclj4YR0j2pq65mj` | `watch_schedule` / `commit-m` | `docs/schedule/data/latest/team.json` |
| `recMMQUmbsACaLT1A` | `watch_schedule` / `master` | `docs/schedules/master.json` |
| `recGcwf3tMKVRTDIY` | `watch_schedule` / `9999-schedule` | `docs/9999/schedules/schedule.json` |
| `reckqMGdhTa37KtMl` | `watch_schedule` / `8778-schedule` | `docs/8778/schedules/schedule.json` |
| `reccbGkJVo98BCihe`, `recIwJW5spY6Pn1u3` | `watch_schedule` / `9999-schedule` | `docs/pro/ws_pro.json`, `docs/pro/ws_proplus.json` |
| `recWMMTZF3xuCk3L2`, `recL8F3qjXv5ipmmM` | `watch_trips` / `9999-trips` | `docs/pro/wt_pro.json`, `docs/pro/wt_proplus.json` |

`publisher.js:811–859` loads the named view and allowed fields. `buildRowsFromRecords` (283) maps every returned record; `shapePublishedContent` (528) preserves those rows and optionally adds indexes. This inspected path adds no TODAY or tenant row filter after fetching. Therefore a tenant-specific output filename alone does not prove tenant selection. Some other tenant paths also use `9999-schedule`; record that as a shared selection requiring consumer/view trace, not an assumed defect.

The bulk path compares the published content before writing and preserves existing nonempty published rows when the newly selected source is empty (`publisher.js:544`, `862–899`). This is an existing preservation rule, not proof that a retained file is fresh.

The connector returned view names/IDs but not their filter definitions, and its record-list operation used here has no view selector. Actual `enrich`, `heartbeat`, `TODAY`, `TOMORROW` and publishing-view membership remains unverified. Do not infer isolation from names or replace the working views during identification.

#### Trip date selection

- `trips_dailyv2.js:2413–2418` explicitly restricts schedule rows to the Heartbeat show/date and excludes archived, inactive or dropped rows; the `is_target` subset is preferred when available.
- Those schedule rows feed the class map used by `normalizeTripsForScope` (`trips_dailyv2.js:2718`). The normalizer matches class ID, then class number (`trips_normalizer_v2.js:136`). A missing schedule match is reported as `outside_schedule` but does not discard the trip (`214–276`). Its scheduled date comes from the matched schedule first, otherwise the people payload (`259`).
- The write selector accepts every normalized unique row with a resolvable scheduled date, not only TODAY (`trips_dailyv2.js:2409`, `2843`). Thus a dated unmatched row can continue; this is not evidence that every future entry is present or usable. The helper `rowScheduledDateMatchesScope` exists, but is not the filter used by this selector.
- Actual scheduled date determines trip keys and current-scope classification; some `app_sql_date`/focus metadata is still assigned from the active Heartbeat (`trips_dailyv2.js:2236`, `2375`, `2861`). Keep row date distinct from active control metadata.
- Trip calculation's v2 wrapper defaults to `watch_trips` / `heartbeat`. Calculator eligibility checks trip identity and `h_eid`, without a separate date test (`trips_calculator.js:1204`). Its input view therefore remains part of the date-selection contract.

#### Stage 6: existing calculation rules, first trace

| Calculation | Existing rule and source |
| --- | --- |
| Schedule start/counts | Live-group estimated start wins over row estimated start; live total/gone win over row totals. Status precedence is row status, then live-group status, then prior latest status. `schedules_calculatorv2.js:1112–1126`. |
| Schedule pace/projection | Positive row `perTrip`, otherwise configured default (source default 3 minutes). Class length = total trips × pace; end = start + length, otherwise source estimated end. Minutes until/since start are clamped separately to zero. `schedules_calculatorv2.js:30`, `1127–1142`. |
| Schedule manual override | Suppresses writes to source start-time fields, but the inspected calculation still chooses live-group start first. Preserve and trace this distinction before interpreting override behavior. `schedules_calculatorv2.js:1115`, `1144–1151`. |
| Schedule output path | Computed projections are assembled for calculation logs. Promote writes `watchScheduleFields` and the `schedule_logs` link; it does not copy all `computedOutputs` directly into `watch_schedule`. Follow linked fields/formulas and consumers next. `schedules_calculatorv2.js:1183`, `1587`, `1874–1906`. |
| Trip start/order | Actual start wins over estimated start. Effective order = first available actual order, actual go, order of go. `trips_calculator.js:1126`, `1220`. |
| Trip pace | Prefer elapsed minutes/completed trips, then remaining minutes/remaining trips, then full class window/total trips, then estimated-go window/(order−1). Completed classes prefer the full class window first. Accept candidates within configured 1.8–3.8 minutes by default; otherwise use 3 minutes and record the fallback. `trips_calculator.js:219–222`, `517–585`. |
| Trip outputs | Running order = effective order − completed trips; blank if already gone or negative. Projected class duration = total × pace. Go time = start + max(order−1,0) × pace, with estimated-go fallback when order is absent. Running-order wait uses max(running order−1,0) × pace. `trips_calculator.js:1230–1329`. These are distinct go-time and remaining-order calculations. |
| Repeated-write boundary | Both calculators default to shadow in source; promotion is explicit and dry-run guards writes. Trip promotion uses changed outputs. Actual orchestrator environment and all log/alert side effects must be included before describing an operational mode as read-only. No calculator was run for this trace. |

The two calculators do not share one identical pace algorithm. This records existing behavior, not a recommendation to unify or replace it. The owner's working baseline remains authoritative.

#### Exact resume checkpoint

Completed in this continuation: live publication queue selection matrix, publisher post-fetch shaping/preservation, trip normalizer/date selector, and first Timing precedence/equation trace. Remaining: actual view filters/membership; schedule-log links/formulas and downstream readers; live state acquisition/tagger precedence; existing alert/results effects; recurring show-day evidence. Future-row acquisition is found, but a complete isolated tomorrow stage is not yet established.

Updated next read-only action: finish the high-level connections from existing alert events through delivery automation, and from Results/published outputs through their consumers. Then reconcile the day/reset boundary. Defer field-by-field `schedule_logs`/`rs_*` equations and definitions to a later pass. The overview now maps all 11 stage responsibilities and their 26 registered lanes, separates actual dispatch order, and identifies the remaining end-to-end connections. Do not execute scripts, fill missing stages or create agents.

Context checkpoint: the existing measurement helper flagged large context (latest measured input 123,444 tokens). This section is the compact resume packet; no chat, hook or monitor was created. Account usage snapshot showed 93% remaining, which is account-wide and cannot be attributed to this tracing task. The interval since the older checkpoint includes idle/other work and is not treated as active tracing time.

### Active-show continuation — 11 October 2026

Owner-confirmed runtime base is `apptdhhNzduxm5gjn`, show record `rec6ERbzfAZvPVMkp`, show `200000073`, heartbeat checked, focus day `2026-10-11`. Fresh read-only runner evidence and exact next trace are in [WEF live trace](wef-engine-live-trace-20261011.md). This supersedes the previous no-active-scope limitation, not the preserved working source or stages stack. Agents task: https://airtable.com/appZahVgD156cMAe3/tbluT1yhP1FJMb8Cm/recTUEXGLAQqnBTVi . Start with schedule calculator input-view selection, then blocked small people payloads and mixed publication jobs; continue high-level alert/results consumer links. No production execution or repair authorized.
