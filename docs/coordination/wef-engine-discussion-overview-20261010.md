# WEF show-engine — owner discussion overview

## Proposed slower reference collection lane - 15 October 2026

Proposal only; not current engine behavior or implementation approval. Collect people, horses and rings across show weeks on a slower recurring cadence. Reuse their known identities and retain each show's associations and participation history. This supports selecting existing records rather than manually entering the same details, narrowing lists and suggesting familiar choices from prior show/tenant history. A suggestion is not an automatic follow, assignment or access grant.

Keep this separate from faster tenant-specific operational updates: those refresh the current details for people, horses and entries a tenant is associated with or watching, rather than repeatedly fetching the entire reference population. Exact intervals and endpoint coverage remain to be established from working source evidence; none are selected here.

Preserve reusable person/horse identity separately from show-specific entry and participation. Repeated appearances must update/link the known identity and add show history, not create another person or horse or overwrite the previous show. Retain source IDs and their verified scope; do not assume an entry ID is a permanent horse ID or merge by name alone. Unresolved identity matches remain unresolved for review. Exact identity keys and existing matching rules must be traced before implementation. Preserve manual corrections and existing records; absence from a later show does not erase history.

Owner's corrected drill-down context: `shows > show > show_day > schedule > rings > start_time > groups > classes > entries > go_time > trips`. Start time and go time remain distinct. Results, alerts, two-way, print and mobile consume relevant outputs; this context is not a new table schema or a claim of strict execution order.

Next discovery: identify the existing identity keys, show-association keys, source endpoints, tenant/follow selection and manual preservation rules that can support this lane. Reuse proven working code; no new collectors, runtime changes, schedules or manual business runs are authorized by this proposal.

### Pagination and proposed lazy weekly collection - 15 October 2026

Subsequent supplied entries capture verifies ten records per page, total 666 (67 pages). This resolves the entries page-size uncertainty below. Verified identity evidence and the smallest proposed saved-progress plan: [resumable slower collection](wef-slower-collection-resume-plan-20261015.md). No endpoint was called; cross-week identity stability is not proven by one captured page.

Owner-supplied /people response for show 200000074 reports total_people=758, records_per_page=10, page=1: 76 requests for one full pass at that size. Peak estimate is 2,500 people plus about 2,500 entries. People would require 250 pages; entries would require another 250 ONLY if its response also reports 10 per page. The entries page size and any supported larger page size are not verified.

Owner proposal: after the default show changes, use the quieter Monday/Tuesday/Wednesday period for a slow first sweep, approximately 500 page requests over 60 hours; repeat approximately 500 over the next 90 hours, then repeat for the next show week. That averages one combined page request every 7 minutes 12 seconds, then every 10 minutes 48 seconds. These are planning examples, not enabled schedules, fixed request budgets or guarantees of completeness. Actual totals can change. Faster tenant-associated/followed detail updates continue separately.

Preserve weekly associations and reusable identities across passes. A multi-day name-sorted list can change between page reads; completing page numbers alone is not proof that every unique record was seen. Retain source IDs, distinguish an incomplete pass, and do not infer deletion from a failed/incomplete collection. Exact identity and reconciliation behavior still needs source-backed definition before implementation.

#### Existing source evidence (read only; deployment not established here)

- `utils/workers/broad-tooth-b8ed/worker.js:57-110,176-230`: forwards allowed page parameters and defaults page to 1; makes one upstream request per incoming request. No all-pages loop or saved cursor in this file.
- `utils/workers/broad-tooth-b8ed/worker2.js:65-199,403-410`: existing optional all/fetch_all/paginate helper for /people, /entries and /classes. Reads response total and records_per_page, calculates ceiling(total/perPage), fetches pages sequentially and accumulates in memory. HTTP/JSON failure returns failed_page; accumulated records and next-page progress are not persisted. It runs inside one request, not across hours. Its loop always starts at page 2 even when a later first page is supplied, so it is not a saved-progress resume mechanism. No identity deduplication or final unique-count reconciliation appears in this helper. Repository presence does not prove it is the deployed Worker.
- `trips_dailyv2.js:514-545`: Airtable offset pagination in a single invocation. Offset is local memory, not a saved SGL page cursor.
- `trips_dailyv2.js:1287-1455`: tenant/source-specific /people/{id} request, with timestamped person/show JSON payload cache and same-person/same-show fallback. Saved payloads are useful existing fallback behavior, not a checkpoint for collecting all people or entries.
- `wef-schedules/current-store.js:30-54`: separate schedules history/current.json persistence, not people/entries paging progress.

Finding: paging code and payload persistence exist, but no durable people/entries directory paging checkpoint was found in these inspected paths. A proposed lazy lane would need to remember show, collection, pass and next uncompleted page after its records are safely saved; exact storage remains undecided. Do not claim this already exists or reuse the all-pages helper unchanged as an hours-long collector. No code, deployment, scheduled-task changes or manual business runs performed.

## Independent recurring responsibilities - owner concept, 15 October 2026

Concept and tracing only. Each future responsibility must be an independent, end-to-end recurring function with defined keys, rules, designated source endpoints, storage and API/output contract. Schedules, entries, groups, rings, live state and results are responsibilities to trace; separate bases are proposed, not provisioned or locked here. Use the independent schedules work as a learning reference, not permission to copy assumptions into another function.

Timing is the brain: it takes the relevant collected data and produces evaluations and snapshots. Publish, APIs, alerts and two-way may pull, push or notify from those outputs. This is the proposed relationship, not a claim that all current consumers already use Timing. Heartbeat/control remains a stage to understand in the existing local flow; the proposed independent responsibilities do not authorize altering it.

The existing working code is both the guide and fallback. Leave all existing scripts, shared dependencies, settings, scheduled tasks, automations and operational records untouched. Any later implementation requires its own approval and isolation. Only reuse, clean up and improve efficiency of the proven behavior; no reinvention, complicated options, replacement architecture or changes to the baseline. Preserve existing manual controls and the owner's practical manual steps.

First identify each responsibility's actual key, request parameters, inputs, outputs, recurring/stop rules, manual ownership and downstream dependencies. Current source and natural-run evidence must be distinguished from proposed boundaries and unobserved behavior. No new functions, bases, agents, schedules or endpoint executions are authorized in this pass.

Default-show discovery is a separate initial-default responsibility only: expected Sunday 4-8 PM Eastern, stop on change until discovery logic calls again; optional six-hour check remains a proposal. It triggers no collection or downstream work. See [discovery clarification](wef-schedules-only-activation-plan-20261015.md).

Read-only continuation: [stage keys, endpoints and dependencies](wef-engine-stage-contract-trace-20261015.md).


Current entry (14 October 2026): use this domain's resume row in [the authoritative-source directory](domain-coordinator-structure-20261013.md). This dated document preserves workflow/source evidence; original setup commands and old dispatch/monitor instructions are historical, not a new assignment. Follow the current owner request and verified checkpoint. Guidance-only updates leave stopped product work stopped.

Current shared structure (13 October): read the relevant sections of [Company, domains and coordinator capabilities](domain-coordinator-structure-20261013.md). It defines responsibility, sibling interfaces, required skill/connection mapping, proven frontend-agent reference and completion/handoff duties. Capabilities are not automatic agent assignments. Current tasks and authorization control; stopped work stays stopped.

Date: 10 October 2026  
Status: owner requirements recorded; current-system identification remains open.  
Scope: WEF only. Documentation and read-only planning; no implementation or execution authorization.  
Next document: [Current-system identification plan](wef-engine-identification-plan-20261010.md).

## Purpose and authority

### Current owner clarification — 13 October 2026

RingStatus is a very small company. Optimize for understandable, reliable recurring work and practical owner correction in Airtable. Commercial-scale infrastructure, additional services and complete automation are not objectives. Keep the existing local cadence and proven implementation; propose separation only where it demonstrably simplifies maintenance or removes repeated work. A manual focus-day selection, data correction or exception handoff is a valid part of delivery, not a defect requiring new infrastructure.

Engine remains in discovery. Stage names, module grouping, Rider order of go, Results, and proposed ownership moves to Publish/Messages are proposals, not locked implementation decisions. Earlier uses of “strict”, “locked” or “owner lock” in these dated engine documents do not authorize stage moves or establish a finalized new architecture. Preserve the owner's stated behavior as discovery context and distinguish it from the current implementation. No invention or replacement without explicit approval; the working WEF engine remains untouched as the reference. Future tests of rearranged blocks must use an authorized separate working copy and compare behavior with that reference; this discovery pass does not execute those tests.

Identify actual recurring responsibilities before deciding the new stack: routine acquisition/normalization/matching; established calculations and evaluations; delivery of outputs/events; and supporting engineering work. Code review, handoffs, discovery and development notes support maintenance and do not automatically run inside every heartbeat. “More intelligence” does not imply an AI dependency in the production loop.

Manual edits require a field-by-field writer map: where the owner enters a correction, which existing control protects it, which jobs consume it, whether calculations honor it, and what resumes automatic ownership. Do not assume a writable Airtable cell is protected, invent a global override, or interpret protecting a stored value as proof it controls downstream calculations. Preserve existing manual controls; unresolved field policies remain questions in discovery rather than new gates.

Continuation evidence: [Recurring responsibilities and manual preservation](wef-engine-recurring-tasks-manual-preservation-20261013.md). This is source inspection, not a new live execution or a claim that every writer/automation has been checked.

The owner built and completed the existing WEF engine through extensive real operation. It is a working end-to-end baseline, not a speculative prototype to replace. The owner is the authority on intended business behavior. Code, configuration and historical run evidence establish how the implementation operates; uncertainties must be identified rather than filled with assumptions.

The goal is to understand the entire working system before agreeing how to preserve and simplify it. The code is old and evolved through many corrections. It may contain duplicate work, unnecessary wrappers, drift, and patches over patches. Working behavior must be preserved; incidental implementation complexity does not automatically become a requirement.

Never reinvent a supplied working solution. Never keep modifying a guessed replacement because it has already consumed effort. Missing fresh proof or incomplete documentation is a tracing gap, not permission to rebuild. All working WEF scripts, imports, wrappers, automations, configuration and downstream consumers belong in the baseline inventory—not just the filenames currently listed in Engines.

## Sources and boundaries

| Source | Role |
| --- | --- |
| [Engines base](https://airtable.com/appnOAk2pW8JawiuP) | Its `stages` and `lanes` tables are the current discovery register. Preserve existing records while evaluating proposed grouping separately; they do not lock the future architecture. |
| [Legacy RingStatus base](https://airtable.com/apptdhhNzduxm5gjn) | Existing working engine data, controls, relationships and automation references. |
| Local WEF code | Working implementation to trace, including dependencies outside the register. Owner confirms the code is parked locally. Exact authoritative source copies still need identification. |
| WEC show-engine | A second working end-to-end example of the same stages. Built later with lessons learned, less robust source data and a Catalyst experiment. Context and comparison, not replacement authority. |
| RingWaze | Tested concepts to inspect for relevant reuse. Specific concepts and their integration points remain to be identified. |

Zoho Catalyst may support selected parts, but will not be the exclusive platform. No hosting migration or new platform is selected by this overview.

## Stage stack

The first inspection found 11 stages and 26 lanes. The stage table has linked lanes; ordering is represented by lane priority. Preserve the registered stage ownership while separately documenting actual runtime order and cross-stage dependencies. A shared script must have its responsibilities mapped to each relevant stage; do not silently move or duplicate it.

| Stage | Responsibility and owner emphasis | Initial implementation references |
| --- | --- | --- |
| 1. Heartbeat/control | Controls recurring scope and cadence. Establish show/week/day, mode, slots and dispatch. | `run_tagger_heartbeat_lane.ps1`, `tagger.js`, `heartbeat_patterns.js`, `heartbeat_task_cadence.js`, `heartbeat_slot_orchestrator.js` |
| 2. Schedule acquisition | Obtain schedule source data and trace existing fallback behavior. | `schedules_dailyv2.js`, `sgl_browser_enrichment.js` |
| 3. Normalization | Preserve source identity, scope, matching and normalized data. | `schedules_dailyv2.js`, `trips_dailyv2.js` |
| 4. Classes and entries | Classes, people, horses, entries, trip matching and order of go. | `trips_dailyv2.js`, `trips_tagger.js`, `sgl_browser_enrichment.js` |
| 5. Live state | Current groups, rings, class and trip state. | `live_groups_daily.js`, `live_rings_daily.js`, `live_class_detail.js` |
| 6. Timing | **The brains of the engine.** Its calculations supply many downstream outputs; additional useful outputs may become apparent after understanding the working logic. | `schedules_calculatorv2.js`, `trips_tagger.js`, `trips_calculatorv2.js` and its delegated `trips_calculator.js` |
| 7. Results | Intended as a clearer separate responsibility to reduce complexity. | Current register points into `trips_tagger.js`; complete boundary still to trace. |
| 8. Outputs | Produce data for consumers, including Publish. Publish may consume endpoints rather than receive direct publication. | `publisher.js`; existing bulk/differ lanes and queue. |
| 9. Audit | Trace changes, errors, calculations and publication outcomes throughout the cycle. | Existing schedule/trip/live/error/publication logs; not assumed to execute only at the end. |
| 10. Alerts | Intended as a clearer separate responsibility, preserving existing working alert logic and downstream dependencies. | Register has a placeholder; existing implementation must be located. |
| 11. Two-way | Intended as a clearer separate responsibility, preserving existing incoming-message, lookup and response behavior. | Register has a placeholder; existing implementation must be located. |

Breaking out Results, Alerts and Two-way is an intended simplification, not approval to create replacements now. Timing's upstream and downstream requirements must be traced with it.

## First-pass depth — owner clarification

The first pass identifies stages, functions and the connections that make the existing system work end to end. Detailed equations, field definitions and improvement decisions belong in later passes as needed. **Start-time and go-time are different concepts and must remain distinct.** Do not infer their complete business definitions from similarly named code fields; agree definitions with the owner later. Previously recorded calculation details remain source notes, not locked definitions or new requirements.

## End-to-end function map — first pass

This maps the 26 registered lanes into their 11 existing stage responsibilities. It is a source-backed system outline, not a new implementation or a claim of fresh live verification.

| Stage / registered lanes | Function and existing path | Connection to the rest of the system |
| --- | --- | --- |
| 1. Heartbeat/control — 1 lane | The Windows wrapper runs scope selection, Heartbeat creation, slot advancement, cadence selection and dispatch. | Supplies the show/day/mode context used throughout the cycle. Manual focus selection remains supported. |
| 2. Schedule acquisition — 2 lanes | `schedules_dailyv2.js` obtains schedule payloads and uses existing fallback/cache paths. Requested rendered acquisition uses `sgl_browser_enrichment.js`. | Supplies source schedule information to normalization. Found forward preparation remains distinct from the locked two-day target. |
| 3. Normalization — 2 lanes | Schedule code establishes date/scope/identity and writes `watch_schedule`; trip code and `trips_normalizer_v2.js` join and deduplicate entries into `watch_trips`. | Establishes the matching keys and links used by live state, Timing and outputs. |
| 4. Classes and entries — 3 lanes | `trips_dailyv2.js` acquires people/horse/entry information; `trips_tagger.js` matches class/entry data and uses signup fallback; browser enrichment supplies rendered order of go. | Provides the participating entries and order information on which Timing and Results depend. Some scripts also serve other stages. |
| 5. Live state — 4 lanes | `live_groups_daily.js` acquires current groups and links watch rows; `live_rings_daily.js` reads ring state; `live_class_detail.js` processes `has_json` and `is_live` views. | Updates group/ring/entry state. The two class-detail paths handle OOG/Actual_OOG/Gone and Scr/Pos/Gone respectively. Supplies changing inputs to later cycles and consumers. |
| 6. Timing — 3 lanes | Schedule calculator handles class-level projections/triggers; trip tagger supplies live timing observations; trip calculator handles trip-level timing/order projections. | Supplies distinct timing outputs to display and alerts. Start-time and go-time remain separate; definitions are deferred. Calculation logs/links are part of the handoff, not incidental documentation. |
| 7. Results — 1 lane | `trips_tagger.js` copies matched live-trip scores, faults, times, placing, gone and verification into the existing trip data. | Results work currently overlaps the tagger's matching/live duties; no separate WEF results runner was established by this inspection. Further result consumers remain to trace. |
| 8. Outputs — 2 lanes | `publisher.js` reads `publish_queue`, named table/views and allowed fields; bulk publishes complete selected datasets, differ also records field differences. | Existing path sends files through the commit proxy and updates queue state. Downstream display reads those datasets. Endpoint-based Publish is an intended boundary, not assumed to replace this proven path. |
| 9. Audit — 6 lanes | `live_group_changes`, `live_classes`, `schedule_logs`, `trip_logs`, `automation_errs`, and publication queue state. | Records changes, calculations, failures and output outcomes across stages. Audit is continuous, not a final sequential job. |
| 10. Alerts — 1 register placeholder | Existing WEF schedule calculator reads active alert/trigger/profile configuration and can create deduplicated `thread_logs` events directly. | An existing producer was found despite the placeholder. The subsequent automation/queue-to-SMS path still needs identification; event creation is not delivery proof. Preserve this path rather than invent a replacement alert engine. |
| 11. Two-way — 1 register placeholder | Existing `ringstatus-sms` Worker source includes incoming SMS parsing, ring or rider/horse lookup, response formatting and an Airtable logging handoff. | Event-driven boundary with Messages, separate from Heartbeat dispatch. Prior source inventory identifies legacy `two-way` automation writing `twoway_logs`; exact current deployed correspondence and full downstream acceptance were not reverified in this pass. |

Source anchors: `heartbeat_slot_orchestrator.js:1890–2037`; `trips_tagger.js:736–766,1826–1892`; `live_groups_daily.js:601–844`; `live_class_detail.js:258–264,488–533`; `publisher.js:811–859,1108–1281`; `schedules_calculatorv2.js:1837–1871`; `lib/cloudflare/ringstatus-sms/worker-err.js:65,591,736,902,1622`. The existing [Messages source inventory](messages-source-inventory-20261010.md) records the earlier control-plane/automation evidence, including automation `wfleDEPvPZQjrHJ8E` and legacy `twoway_logs` table `tblfQDcePLpX2qkHW`. Its earlier observations are references, not new verification. Several Worker variants exist; do not select an alternative by filename or overwrite the modified working source.

### How one recurring cycle connects

The business dependency is **scope → acquire → normalize/match → current state and Timing → results/output/alert consumers**, with audit throughout. This describes responsibilities, not a claim that each stage is a separate sequential program.

The inspected WEF dispatcher actually calls due work in this order: schedule acquisition → schedule calculation → trip acquisition → trip tagger → trip calculation → rendered OOG enrichment → live groups → live rings → live class detail → publisher. Mode/slot gates mean not every call happens every cycle. Requested rendered schedule acquisition is another existing branch. Two-way starts from inbound SMS rather than this list.

A failed due schedule acquisition blocks dependent trip work; trip failures block their dependent steps; accumulated upstream failure blocks publication. Rendered OOG enrichment has separate failure reporting and does not set the same `upstreamOk` flag in the inspected branch. Optional WEC branches also share that flag before publication; retain that as an existing coupling to review, not permission to alter WEF or WEC. Timing preceding live refresh is an observed ordering dependency to reconcile later, not an assumed defect.

### Remaining connections needed to finish the first-pass map

1. Identify the existing WEF alert-event → automation/queue → SMS/output connection and its owner. Do not substitute an event log for the final delivery step.
2. Identify the consumers of existing Results and published files/endpoints, including which reads feed schedule/pro-schedule, print and Messages. Reuse recorded working references.
3. Reconcile TODAY selection, tomorrow preparation and reset/rollover at stage boundaries; retain manual focus control and explicitly mark absent or unverified staging.
4. Consolidate which Airtable views/automations and runtime settings each boundary depends on. Record missing references without redesigning them.

Later passes can define start-time/go-time, examine calculations, review redundancy and verify repeated operation. Nothing in this map authorizes changes or new agents.

## Recurring operation is the product

This engine is built to run end to end, repeatedly, thousands of times a day, then reset and run again each day. A successful isolated command or one good payload does not establish success.

Identification must explain:

- What each heartbeat does, what each slot runs, and what is intentionally skipped.
- Which state is current-cycle, carried from a prior cycle, show-week state, or day-specific state.
- How repeated identical inputs, changed inputs, delayed inputs and missing inputs affect outputs.
- How existing failure, retry, overlap and restart handling work without accidental duplicate effects.
- How tomorrow is prepared, the day rolls over, and stale data is kept from becoming current authority.
- How downstream consumers receive consistent output and distinguish fresh, unchanged, fallback and failed data.

These are inspection questions for the existing system. They do not authorize new infrastructure or a new retry mechanism.

## Locked: TODAY/live and tomorrow/staging

Owner lock, 10 October 2026: each show date is independent. Its lifecycle starts one day before that date (-1, preparation) and ends on the date itself (0, live). Only two concurrent day scopes are intended: TODAY and tomorrow. Do not replace this with a calendar of special-case instructions or automatically processing the whole remaining show week.

- The owner currently sets `focus_day` manually. When live is active, this identifies TODAY. Keeping that manual step is an acceptable delivery path; automatic rollover is not required.
- Display and calculate TODAY. Independently build tomorrow's staged schedule without changing TODAY's scope, replacing its data or stopping its live work.
- The owner's source example publishes tomorrow's schedule at 5 PM the preceding day. Availability of tomorrow's source is an acquisition detail within the same per-date lifecycle, not a switch that ends TODAY's live processing.
- For show 123, October 7–11: October 6 prepares October 7; October 7 live continues while October 8 is prepared; the same pattern repeats. There is no October 12 preparation outside that show. October 11 live work is still allowed to finish.
- Staged information is intended to support advance planning; the exact planning display remains distinct from TODAY's live display/calculations. Do not infer that tomorrow runs live calculations or becomes the current published day.
- How staging is distinguished from live in the data and how tomorrow becomes TODAY must be traced and agreed using existing working behavior. No new fields, services or automation are selected here.

**Current versus target:** the owner explicitly warns that current code may not implement tomorrow preparation because it was too difficult to complete. This lock defines the desired workflow; it does not claim that concurrent TODAY/live and tomorrow/staging already works. A cache, a forward fetch or a `shifted_to_next_day` flag alone does not prove that behavior. Preserve the working current-day baseline and identify the exact missing boundary without inventing a replacement.

## Data and delivery boundaries

The owner reports local memory pressure when the large legacy Airtable base and code tools are open together. Splitting bases is an intentional response to that experience and to clearer responsibilities. Tenants, Publish, Messages and Results are examples of the emerging separation. This document does not assert that all those bases or runtime connections are already established, or that splitting alone has been measured to solve memory use.

Shared trainers, horses, riders and rings must be accounted for without inventing duplicate master data. WEF show-week/day scope must remain distinct from tenant membership and personal follows. Earlier decisions are recorded in [Data and engines working map](data-and-engines-working-map-20261009.md) and its linked consolidated reference; older unresolved notes must be reconciled with later owner decisions.

Publish is a downstream responsibility. Astro is a delivery method, not the engine's business logic. Existing `publisher.js` publication and the intended endpoint-facing Publish boundary must both be traced before deciding their future relationship. Alerts and Two-way also meet Messages; documenting those boundaries does not authorize changing Messages.

## Initial evidence and open questions

The first read-only inspection used approved checkout commit `a5235466eed1b094a26db14ab34b514726abf91d`. Subsequent scheduler/log inspection established that the active local Heartbeat task executes from that approved checkout and that the 15 files recorded in its October 10, 9:05 PM Eastern run match current local hashes. That run had no active WEF show scope; this identifies the operating Heartbeat source, not fresh end-to-end engine proof.

- `trips_calculatorv2.js` is a wrapper that sets defaults and loads `trips_calculator.js`. The owner identifies such layering as a likely consequence of historical evolution; its necessity is not assumed.
- The inspected orchestrator invokes schedule/trip timing before refreshing live groups/rings/class detail. Establish whether this deliberately consumes previous-cycle state, conflicts with the intended stages, or reflects source drift. Do not label it defective without that trace.
- The inspected `C:/actions-runner/ringstatus` location lacks several registered root scripts and contains a different orchestrator. The active Heartbeat wrapper uses it for logs/runtime state and executes source from the approved repository. Do not substitute its loose scripts for the verified scheduled source.
- The historical [Pipeline scope](../ringstatus_pipeline_scope_2026-05-08.md) includes stale implementation statements. Treat it as historical evidence and compare each applicable statement with current source and owner intent.
- Alerts and Two-way have incomplete register references. An incomplete register is not evidence that working code is absent.
- Schedule tracing found future-date fetching and noncurrent `watch_schedule` writes, but not proof of the locked concurrent two-day workflow. The source defaults to fetching through show end, up to 14 days, and makes forward preparation conditional on obtaining the current dated schedule without fallback. The plan records this distinction and the remaining consumer checks.

## Non-negotiable limits for this pass

Only this overview and its identification plan are being created. Original code, configuration, bases, records, schedules and production workflows remain untouched. Do not run the engine, substitute direct endpoints, repair data, deploy, publish, send messages or activate automations. Do not create agents, subagents or handoffs until the owner agrees the specific WEF system is comprehensively understood.

Future clean code alongside the original is a possible subsequent phase, not current authorization. Its smallest necessary changes and acceptance would be agreed only after the baseline is understood.
