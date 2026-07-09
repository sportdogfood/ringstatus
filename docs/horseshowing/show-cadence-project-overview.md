# Show Cadence Project Overview

## Document Control

| Field | Value |
|---|---|
| Version | `v0.2` |
| Date | `2026-07-07` |
| Status | Working architecture overview |
| Current model | WEC / HorseShowing |
| Future model | WEF and other timing/source systems through adapters |
| Implementation authority | Supporting docs and approved code changes only |

## Purpose

This document organizes the full show-cadence model for human and machine education.

WEC is the current working model. The same cadence should support WEF and other timing/source systems later through source adapters, not by changing the core workflow shape.

This document does not approve renaming, code changes, table changes, deploys, or workflow runs.

This document is the broad orientation layer. More specific contracts still own their detailed behavior, especially:

| Contract | Owns |
|---|---|
| `wec-clean-key-date-contract.md` | key/date/control rules |
| `wec-clean-stage1-4-workflow-contract.md` | clean Stage 1-4 handoff rules |
| `ringstatus-data/catalyst-workspaces/horseshowing/docs/core_1_4_next_day_preflight_contract.md` | Core next-day outside-lane preflight and blocker classification |
| `wec-catalyst-step6-results-contract.md` | Step 6 results lane |
| `wec-time-engine-contract.md` | time engine design |
| `wec-alert-message-contract-draft.md` | message/alert draft |

## Core Idea

The system turns show source data into reliable current-day operating views:

```text
show control
-> day/ring/class/entry foundation
-> relevant entry discovery
-> runtime schedule
-> live enrichment
-> timing engine
-> triggers/messages/results
-> publishable outputs
```

The source system may change. The cadence does not.

Core 1-4 now also owns proactive next-day readiness testing outside the working lane. Before a focus-day change, Core must run actual next-day source acquisition in a no-write lab lane, classify the first blocker, and turn repeatable blockers into durable code or policy fixes. Date-key rewrites are only portability checks; they are not next-day readiness proof.

## Operating Principle

The workflow is a stacked system, not a single endpoint.

| Layer | Meaning |
|---|---|
| Build/update | create or update current focus-day source tables |
| Probe | inspect source documents quickly and mark progress/certainty |
| Process | parse stored source documents locally |
| Runtime prep | build stable ring/class/entry runtime rows |
| Next-day preflight | test tomorrow's source acquisition/probe/parse/runtime projection outside the working lane |
| Live enrichment | update runtime rows from live source signals |
| Time engine | calculate reusable time fields and trigger readiness |
| Trigger/message | create durable message/event rows |
| Publish | serve mobile, print, two-way, alerts, and reporting outputs |

Each layer must be able to prove its handoff without relying on old-day fallback or manual repair.

## Primary Objects

| Object | Meaning | Current WEC Examples |
|---|---|---|
| `shows` | Show-level identity | `show_no` |
| `focus_show` | Active show/day control | active day, pause, live/results gates |
| `focus_day` | Current operating date | `YYYY-MM-DD` |
| `ring_days` | Source ring/day combinations | HorseShowing ring day rows |
| `rings` | Ring helpers/reference | ring names, normalized names |
| `classes` | Class helpers/reference | class number/name/type metadata |
| `entries` | Entry helpers/reference | entry number, horse/rider/trainer |
| `results` | Result rows from source result endpoint | class/entry results |
| `comments` | User/operator comments | comments system |
| `outputs` | Rendered/read models | mobile, mobile-pro, print, two-way |

## Entity Model

| Entity | Purpose |
|---|---|
| `tenant` | Barn/customer/business account |
| `trainers` | Allowed/followed trainer scope |
| `horses` | Followed horse scope, barn names, aliases |
| `riders` | Followed rider scope, team/display names |
| `profiles` | Person/subscriber/output profile |
| `users` | Auth/session/operator identity |

## Canonical Identity And Date Rules

The clean workflow uses const keys for dedupe and handoff. Display or visual fields are secondary.

| Field | Rule |
|---|---|
| `focus_day` | ISO date, for example `2026-07-05` |
| `iso_date` | same ISO date as `focus_day` |
| `focus_day_key` | compact date, for example `20260705` |
| `show_const_key` | `show_no` |
| `focus_day_const_key` | `show_no|focus_day_key` |
| `ring_day_const_key` | `show_no|focus_day_key|ring_day_no` |
| `ring_const_key` | `show_no|focus_day_key|ring_day_no|ring_no` |
| `class_const_key` | `show_no|focus_day_key|ring_day_no|ring_no|class_no` |
| `entry_const_key` | `show_no|focus_day_key|ring_day_no|ring_no|class_no|entry_no` |

Deprecated for workflow identity:

| Field | Allowed Use |
|---|---|
| `ring_visual_key` | human/display/reference only |
| `class_visual_key` | human/display/reference only |
| `entry_visual_key` | human/display/reference only |
| `ring_name_slugified` | legacy/reference only |

Required ring naming context:

| Field | Purpose |
|---|---|
| `ring_name_normalized` | readable normalized ring identity |
| `ring_name_prioritized` | preferred helper/operator ring name |
| `ring_name_token` | key-safe token derived from normalized/prioritized name when needed for display/reference |

## Cadence Stages

| Stage | Purpose | Current WEC Shape | Status |
|---|---|---|---|
| Heartbeat | Record cadence context and active focus | `hs_heartbeat` linked to `focus_show` | Active |
| Focus | Select current show/day and gates | `focus_show` | Active |
| Normalize / Tag | Normalize source names, dates, preflight, ring/class/entry identity | ring/class/entry key fields, `is_preflight` | Active / still tightening |
| Prepare / Create | Build source foundation | ring days and update schedule | Active |
| Probe | Quickly inspect class entry documents for relevant evidence | clean 3A class_oog probe | Active / evolving |
| Probe 3A2 | Retry checked/no-match classes up to cap as second-pass refinement | non-blocking retry lane | Active / does not block initial runtime |
| Process | Parse stored relevant source documents locally | clean 3B class_oog process | Active / evolving |
| Process 3B2 | Parse new raw docs found by 3A2 | non-blocking refinement parse | Active / does not block initial runtime |
| Runtime Prep | Build runtime schedule tables | ring status, class start times, entry go times | Active |
| Next-Day Preflight | Proactively test next focus-day source acquisition and Core 1-4 projection in an outside lane | `core_1_4_lab.js` no-write live-source run | Active / Core responsibility |
| Listen | Pull live source state when enabled | get rings/orders/results as separate lanes | Active / gated |
| Time Engine | Calculate timing variables and trigger-ready rows | `time_engine`, `time_engine_logs` | Prototype / needs live wiring |
| Trigger | Create durable message/event rows | future message queue / transitional alerts | WIP |
| Enrich | Add live pace, status, result, helper display context | runtime tables plus results | Active / partial |
| Publish | Serve outputs and downstream read models | mobile, mobile-pro, print, alerts, two-way | WIP |

## Stage Handoff Contract

| Stage | Reads | Writes | Handoff / Gate |
|---|---|---|---|
| Heartbeat | `focus_show` | `hs_heartbeat` | active focus resolved; pause/live/results gates recorded |
| Focus | `focus_show` | `hs_focus_show` mirror where used | Airtable `focus_show` remains control source |
| Ring days | active focus | `hs_get_ring_days` | current focus-day ring rows exist and carry canonical date/key fields |
| Update schedule | `hs_get_ring_days` | `hs_update_schedule` | non-preflight class rows have native `class_no`; preflight stays visible but not downstream eligible |
| Probe 3A | eligible `hs_update_schedule` rows | `hs_class_oog_raw`, probe progress fields | one native class request at a time; full raw doc stored only when evidence exists |
| Probe 3A2 | checked/no-match `hs_update_schedule` rows below retry cap | probe retry progress; possible new `hs_class_oog_raw` | non-blocking second pass; must not hold initial production runtime |
| Process 3B | `hs_class_oog_raw` | `hs_class_oog` | parse stored docs locally; no source request; materialize parsed rows |
| Process 3B2 | raw docs discovered by 3A2 | `hs_class_oog` refinements | non-blocking second-pass parse; downstream refresh only |
| Runtime prep 4 | `hs_get_ring_days`, `hs_update_schedule`, `hs_class_oog` | `hs_ring_status`, `hs_class_start_times`, `hs_entry_go_times` | ringwise/classwise/entrywise runtime rows written with canonical keys |
| Next-day preflight | live HorseShowing source + helper mirrors | no production writes; in-memory/lab report only | PASS/FAIL blocker classification before focus-day change |
| Live 5 | runtime tables plus live source endpoints | `hs_get_rings`, `hs_get_orders`, runtime live fields | gated; enrich only; does not rebuild baseline |
| Results 6 | runtime class/entry rows | `hs_result_queue`, `hs_result_classes`, `hs_class_results` | gated; native `class_no` source requests only |
| Time engine | runtime rows plus live/results fields | `time_engine`, `time_engine_logs` | timing fields and trigger readiness calculated |
| Message/alert | time engine plus runtime/results | message queue / transitional alert tables | create records only; sending is separate |
| Publish | runtime, time engine, results, messages | mobile/print/two-way/read endpoints | read model only; no workflow repair |

## Source Adapters

| Adapter Type | Current WEC Source | Future Shape |
|---|---|---|
| Ring days | HorseShowing ring days | WEF/other system ring-day source |
| Schedule | HorseShowing schedule/update schedule | Any source schedule with rings/classes/times |
| Entry/OOG | HorseShowing `class_oog` | Any class-entry source document/API |
| Live ring status | `get_rings` | Any live ring status endpoint |
| Live order/status | `get_orders` | Any live order/entry endpoint |
| Results | `show_results4.php` | Any source results endpoint |

Adapters feed the same canonical stages. They should not create new workflow shapes.

## Integration Surfaces And Codex Access

| Surface | Role In The System | Codex Access Path |
|---|---|---|
| Zoho Catalyst | canonical `hs_*` runtime tables, functions, job scheduling, API Gateway, deployed endpoints | Zoho/Catalyst MCP or app when exposed, Catalyst CLI/local wrapper, repo code in `ringstatus-data` |
| Airtable | human edit surface, `focus_show`, helper edits, `hs_*` mirrors, forms/webhooks | Airtable MCP/app when authenticated, Airtable CLI skill, Airtable REST/webhook paths, repo docs |
| Webflow | public pages, embeds, customer-facing mobile/print surfaces | Webflow MCP, Webflow Cloud, Webflow CLI/skills, exported HTML/CSS, browser verification |
| Zoho Slate | possible hosted app/form surface if adopted | confirm available access per turn; do not assume dedicated MCP unless exposed |
| Git repos | source, docs, prototypes, workflow contracts | local filesystem and git for `ringstatus` and `ringstatus-data` |
| Codex skills | repeatable project/tool instructions | installed skills, repo-local skills, plugin skills |
| Cloudflare / PDF workers | legacy or supporting edge/PDF routes where still used | repo code, Cloudflare tools if exposed, public endpoint verification |
| Twilio / Airtable-Twilio | two-way/SMS send or reply lanes | Twilio tools if exposed, Airtable connector/webhook records, repo docs |
| Zoho Analytics | reporting/analytics target for Catalyst/Airtable exports | CSV/API/export paths unless a live connector is separately proven |

Codex access is for inspection, approved code changes, documentation, and verification. It is not the cadence runner. Scheduled Catalyst jobs, approved webhooks, or deployed lanes must own the business process.

## System Ownership

| System | Owns | Does Not Own |
|---|---|---|
| Catalyst | canonical `hs_*` runtime/source rows, deployed actions, scheduled jobs | human helper edits unless synced |
| Airtable | `focus_show`, human helper edits, mirror review, forms/webhooks | canonical runtime truth unless explicitly approved |
| Webflow | customer-facing pages and embeds | canonical business logic |
| Zoho Slate | possible hosted/form surface if adopted | current workflow truth until explicitly wired |
| Zoho Analytics | reporting and analysis | operational cadence execution |
| Codex | inspect, patch approved code/docs, verify approved gates | scheduled business process execution |
| Runner / Scheduler | cadence execution | design authority or manual repair |

## Current Core Tables

| Table | Role |
|---|---|
| `hs_heartbeat` | cadence context |
| `focus_show` | active show/day control source |
| `hs_get_ring_days` | source ring-day mirror |
| `hs_update_schedule` | source schedule mirror |
| `hs_class_oog_raw` | stored relevant class-entry source documents |
| `hs_class_oog` | parsed relevant entries |
| `hs_ring_status` | runtime ring state |
| `hs_class_start_times` | runtime class schedule |
| `hs_entry_go_times` | runtime entry schedule |
| `hs_get_rings` | live ring source mirror |
| `hs_get_orders` | live order source mirror |
| `hs_result_queue` | results retry/probe state |
| `hs_result_classes` | class-level result state |
| `hs_class_results` | entry-level result rows |
| `time_engine` | timing/readiness rows |
| `time_engine_logs` | engine run proof |

## Helper Tables

Helpers are show-specific or global reference surfaces used for matching, display, and operator correction.

| Helper | Purpose |
|---|---|
| `hs_rings` | ring identity, aliases, normalized/prioritized names |
| `hs_classes` | class identity and class metadata |
| `hs_entries` | entry identity |
| `hs_trainers` | allowed/followed trainer matching |
| `hs_horses` | followed horse matching, barn names, aliases |
| `hs_riders` | followed rider matching, team/display names |

Airtable remains the human edit surface where approved. Catalyst mirrors are runtime reference tables.

## Helper Sync Expectations

| Helper | Human Surface | Runtime Surface | Key Fields |
|---|---|---|---|
| rings | Airtable `rings` / `hs_rings` mirror | Catalyst `hs_rings` | `ring_no`, ring names, aliases, active/follow |
| classes | Airtable mirror if adopted | Catalyst `hs_classes` | `class_no`, class metadata |
| entries | Airtable mirror if adopted | Catalyst `hs_entries` | `entry_no`, class linkage |
| trainers | Airtable `trainers` / `hs_trainers` mirror | Catalyst `hs_trainers` | trainer name, aliases, allowed/active/follow |
| horses | Airtable `horses` / `hs_horses` mirror | Catalyst `hs_horses` | horse name, barn name, aliases, active/follow |
| riders | Airtable `riders` / `hs_riders` mirror | Catalyst `hs_riders` | rider name, team name, aliases, active/follow |

Helper edits should preserve both Airtable `rec_id` and Catalyst `ROWID` where available. Helper sync must not directly rewrite Stage 1-4 runtime/source rows.

## Live Lanes

| Lane | Purpose | Must Not Do |
|---|---|---|
| `get_rings` | update ring/class live state, gone/to-go, elapsed context | rebuild baseline schedule |
| `get_orders` | update current order/entry timing signals | invent fallback go-times |
| `get_ring_status` | derive ringwise status from runtime/live state | replace source runtime tables |
| `get_results` / results lane | fetch real results | create fake result rows |

Live enriches runtime. It does not own the baseline schedule.

## Status And Class Timing Concepts

| Concept | Meaning |
|---|---|
| `class_status=Today` | default schedule state |
| `class_status=Soon` | trigger/message lane has identified an upcoming class window |
| `class_status=Now` | live source indicates current class or active ring/class state |
| `class_status=Done` | real results found or separately approved done rule is satisfied |
| `ring_status=ontime/late/check_gate` | ringwise timing status derived from runtime/live state |
| `starts_in` / `ends_in` | classwise timing fields for outputs and triggers |
| `go_in` | entrywise timing field for outputs and triggers |

Estimated timing may support display and trigger readiness, but fake results or fake source rows are not allowed.

## Time Engine

The time engine reads runtime tables and calculates reusable timing fields.

| Field | Meaning |
|---|---|
| `starts_in_mins` / `starts_in` | time until class start |
| `ends_in_mins` / `ends_in` | time until estimated class end |
| `go_in_mins` / `go_in` | time until estimated entry go time |
| `tags` | thresholds such as `starts_in_30`, `starts_in_60`, `go_in_20`, `go_in_40` |
| `trigger_ready` | row is ready for downstream message/alert handling |

The time engine does not calculate live pace, fetch live endpoints, fetch results, send alerts, or render output.

## Trigger / Message Model

Alerts are message/event records first. Sending/publishing is separate.

| Message Family | Source |
|---|---|
| `ring_now` | ring status + class start times |
| `ring_status` | ring status |
| `class_start_time` | class start times + time engine |
| `class_status` | class status |
| `entry_go_time` | entry go times + time engine |
| `entry_go_time_change` | entry order/go-time changes |
| `entry_status` | entry runtime state |
| `entry_result` | class results |
| `entry_now` | entry go times |
| `rider_now` | entry go times + rider helpers |
| `rider_results` | class results + rider helpers |

Creation and sending must stay separate.

## Support Surfaces

| Surface | Purpose | Status |
|---|---|---|
| Barn forms | operator/barn expected-entry input and review | Prototype |
| Mobile | current schedule/entry surface | Active / evolving |
| Mobile Pro | richer ring/class/entry/results/timing surface | WIP |
| Print | printable schedule/review surface | Active / evolving |
| Alerts | transitional event/alert records | WIP |
| Two-way | queryable response surface for rings/classes/entries/results | WIP |
| Comments system | user/operator comments and context | WIP |

## Lists / Reports / Endpoints To Supply

These are the current read models implied by the workflow.

| List / Endpoint | Primary Sources | Status |
|---|---|---|
| Rings for focus day | `hs_ring_status`, `hs_get_ring_days` | Active |
| Schedule by ring by time | `hs_class_start_times` | Active |
| Schedule by time | `hs_class_start_times` | Active |
| Class detail | `hs_class_start_times`, `hs_entry_go_times`, `hs_class_oog` | Active / evolving |
| Entry next-ups | `hs_entry_go_times`, `time_engine` | WIP until time engine/live wiring is complete |
| Ring now/next | `hs_ring_status`, `hs_class_start_times`, live enrichment, `time_engine` | WIP |
| Results by trainer/rider | `hs_class_results`, helpers | Active / evolving |
| Alerts/message feed | future queue or transitional alerts, `time_engine` | WIP |
| Two-way index | runtime tables, results, time engine | WIP |
| Barn entry review | barn form selections + `hs_class_oog`/runtime rows | Prototype |
| Workflow log/audit | `workflow_log`, `time_engine_logs`, heartbeat | WIP |

## Endpoint / Action Map

| Endpoint / Action | Output Satisfied | Primary Tables | Status |
|---|---|---|---|
| `wec-mobile-live` | current mobile schedule | `hs_ring_status`, `hs_class_start_times`, `hs_entry_go_times` | Active |
| `wec-print-live` | printable class schedule | `hs_class_start_times`, `hs_entry_go_times` | Active |
| `wec-print-layout` | print layout/ring grouping | `hs_ring_status`, `hs_class_start_times` | Active |
| `mobile-pro` endpoint | ring/class/entry/results/timing views | runtime tables, results, time engine | WIP |
| `barn-entry lookup` endpoint | ring/horse/class picker | `hs_ring_status`, `hs_class_oog`, `hs_class_start_times` | Prototype |
| `wec-step6-results` | results by class/entry/rider/trainer | `hs_result_queue`, `hs_result_classes`, `hs_class_results` | Partial |
| `time-engine` endpoint/lane | starts_in, ends_in, go_in, trigger-ready rows | `hs_class_start_times`, `hs_entry_go_times`, `hs_ring_status` | WIP |
| `message/alert probe` endpoint/lane | alert/message records | time engine plus runtime/results | WIP |
| `twoway-index` endpoint | SMS/query response index | ring/class/entry/result read models | WIP |
| `export-mirror-table` | admin/debug table export | Catalyst `hs_*` tables | Diagnostic |
| `workflow-log` endpoint/lane | workflow proof/audit | cadence stage outputs | WIP |

## Output Satisfaction Map

| Output | Endpoint Needed |
|---|---|
| Schedule by ring/time | `wec-mobile-live`, `wec-print-live`, future `mobile-pro` |
| Schedule by time only | `wec-mobile-live`, future `mobile-pro` |
| Class detail | future `mobile-pro` class detail endpoint |
| Entry next-ups | future `mobile-pro` plus `time-engine` |
| Ring now/next | future `mobile-pro` plus `time-engine` |
| Results by trainer/rider | `wec-step6-results` output endpoint |
| Alerts feed | future message/alert endpoint |
| Two-way SMS answers | future `twoway-index` |
| Barn entry review/print | barn-entry prototype endpoint/form |
| Workflow proof | future `workflow-log` |

## Output Families

| Output | Reads |
|---|---|
| `mobile` | current runtime schedule and entry rollups |
| `mobile-pro` | runtime tables, time engine, results, messages |
| `print` | runtime schedule, entry rollups, optionally time engine |
| `alert_logs` | message/alert records |
| `twoway_index` | ring/class/entry/result read models |

## Proof Vocabulary

| Term | Meaning |
|---|---|
| Proven | verified against the approved gate and current expected source |
| Active | implemented or in use, but may still require current-day proof |
| Partial | works for a bounded piece but not yet full cadence/output proof |
| WIP | design or prototype exists, not locked |
| Diagnostic | useful for inspection, not workflow proof |
| Scheduler proof | proof from the scheduled/cadence-owned path, not a manual endpoint substitute |

Counts alone do not prove a clean handoff. Required keys, date fields, source scope, and link/mirror behavior matter.

## Current Maturity Summary

| Area | Current State |
|---|---|
| Stage 1-3B clean base | locked/proven as the clean foundation |
| Stage 4 runtime prep | active and being locked against the clean source tables |
| Step 5 live enrichment | partial; depends on live source availability and gate |
| Step 6 results | partial; real result rows proven, cadence proof still separate |
| Time engine | documented/prototype; needs live wiring |
| Message/alerts | WIP; create records first, send later |
| Mobile | active/current output |
| Mobile Pro | WIP target for richer ring/class/entry/results/timing views |
| Print | active/current output |
| Two-way | WIP target consuming the same read models |
| Barn forms | prototype and webhook/write path under refinement |
| Helper sync | expected but not fully proven as live listening |
| Zoho Analytics | reporting target; export/sync path still needs final proof |

## Drift Rules

- WEC is the current model, not the permanent naming boundary.
- WEF and other timing software should become adapters feeding the same cadence.
- Baseline schedule stages must not depend on live endpoints.
- Live lanes enrich runtime; they do not rebuild the baseline.
- Results lane is separate and gated.
- Time engine calculates timing fields and trigger-ready rows only.
- Message creation and message publishing/sending are separate.
- Airtable `hs_*` tables are mirrors/review surfaces unless explicitly defined as source.
- Human helper edits belong in approved helper surfaces and sync back through explicit helper lanes.
- No output should rely on old-day fallback.
- Counts alone do not prove a handoff; required identity/link fields matter.

## Current Mental Model

```text
CADENCE
shows -> focus_show -> focus_day
-> ring_days -> rings
-> classes -> entries
-> results/comments
-> outputs

ENTITIES
tenant -> trainers -> horses/riders
profiles -> users

STAGES
heartbeat -> focus
-> normalize/tag
-> prepare/create
-> probe
-> process
-> runtime prep
-> listen/live
-> time engine
-> trigger
-> enrich
-> publish
```

## What Must Stay True

The workflow is a complete system, not a single-day fix, single-event runner, or WEC-only patch.

Every show source should eventually map into the same objects:

```text
show
focus_day
ring
class
entry
result
message
output
```

That is the model to preserve.
