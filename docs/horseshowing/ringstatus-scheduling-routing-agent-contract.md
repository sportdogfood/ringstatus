# RingStatus Scheduling Routing Agent Contract

Date locked: 2026-07-09

## Purpose

This document defines the Routing Agent for the RingStatus Scheduling Project.

The Routing Agent coordinates specialist agents and lane state. It does not become the runner and does not take over specialist lane work.

## Role Boundary

The Routing Agent is responsible for:

- reading current workflow state
- deciding which lane owns the next action
- drafting and maintaining specialist-agent prompts
- keeping lane responsibilities separate
- tracking blockers and drift
- monitoring downstream progress
- reporting exact lane status

The Routing Agent must not:

- run production cadence as a substitute for the runner
- mutate business/source/runtime records directly
- repair records to force a one-time pass
- let one lane compensate silently for another lane
- count manual/direct endpoint success as workflow proof unless explicitly approved

## Active Specialist Lanes

| Lane | Agent | Current session ID | Primary job |
|---|---|---|---|
| `core-build` | Core Agent | `019f4240-49ce-78e3-b89a-13d3a19cb02b` | Build canonical Catalyst scheduling/runtime data from `update_schedule` through runtime rows. |
| `core-next-day-preflight` | Next-Day Preflight Agent | Core-owned outside lane | Test the next focus day in an outside lane without production writes. |
| `stage-4S-sync` | Stage 4S Sync Agent | Core-owned outside lane | Mirror Catalyst Step 4 runtime rows to Airtable for staging/operator visibility. |
| `live` | Live Agent | `019f3f43-7819-7dd0-bcf6-a4b143c43bd2` | Use `get_rings` to update current live state and wake Time Engine. |
| `time-engine + trigger` | Time Engine Agent | `019f4321-ba83-7760-959e-298b47af1970` | Compute time state, alerts eligibility, and `rider_results` eligibility. |
| `rider-results` | Rider Results Agent | `019f4315-5aab-72b3-8ba5-5aa96c9770cf` | Ingest watched rider result records when eligible. |
| `alerts` | Alerts Agent | Not established | Queue/send alert work from eligible trigger/result rows. |
| `publish` | Publish Agent | `019f4319-9b1e-7190-ba93-be475cdea80c` | Build output payloads, caches, and endpoints from prepared upstream data. |
| `endpoints` | Endpoints Agent | `019f4433-1405-7cb0-b289-581b2203bafe` | Own endpoint contracts, request/response surfaces, and endpoint drift. |
| `hot-patch-manual-correction` | Hot Patch Agent | Not established | Capture operator corrections, protect them from overwrite, and document durable fixes. |

## Cleaner Target Workflow

This is the target lane contract for the scheduling project. It supersedes the older assumption that `get_ring_days` is required as a hot Core prerequisite, that `get_orders` belongs in the hot live path, or that broad class-results machinery is required before the business result lane is needed.

```text
Core Build
  update_schedule
    -> yields show_no, focus_day, iso_date, ring_day_no, ring_no, class_no, entry_no where available

  From update_schedule:
    seed class_start_times
    derive/seed ring_status
    seed class_oog_raw probe queue

  From class_oog_raw:
    parse to class_oog

  From class_oog:
    seed entry_go_times

  From entry_go_times:
    seed watched rider_results lane

Live
  get_rings
    -> updates time_engine

  time_engine
    -> updates:
       ring_status
       class_start_times
       entry_go_times
       rider_results eligibility

Alerts
  Alert sources:
    ring_status
    class_start_times
    entry_go_times
    rider_results

Logs
  Append-only:
    ring_change_logs
    class_change_logs
    entry_change_logs
    result_change_logs
    alert_change_logs

Publish
  Endpoints read prepared/current tables plus logs.
```

Contract changes:

- `update_schedule` is the Stage 1 source of truth for day/ring/class structure.
- `get_rings` is the Live current-state updater.
- `rider_results` replaces broad results as the business result lane.
- `get_ring_days` is not required in the hot Core Build contract unless a compatibility shim or fallback proves it is still needed.
- `get_orders` is not part of the hot Live contract unless a later lane explicitly needs order-detail enrichment.
- Broad class-results machinery is deferred unless later business requirements prove it is needed.

## Headwinds

The Routing Agent must keep these active project headwinds visible:

- Webflow Cloud HTML/style/embed constraints.
- Catalyst/Airtable master table drift.
- Catalyst/Airtable helper table drift.
- Hot patch work that solves the moment but is not captured as a durable lane.

## Biggest Operational Pain

The biggest project pain is the focus-day/show transition.

The Routing Agent must watch for:

- focus_day and show changes not propagating smoothly
- stages restarting from stale focus context
- redundant source calls during date changes
- data calculating correctly but inefficiently
- unnecessary probing/parsing after the initial production path is already good enough
- Airtable manual corrections being overwritten by the next 6-minute workflow run

## Manual Correction Reality

This is not a large commercial system with a fully automated data-governance staff. Airtable is intentionally in the middle because operators need a place to manually correct and inspect data.

The Routing Agent must treat Airtable corrections as real operational input, not as noise.

Required design principle:

```text
Manual correction must become explicit state.
Explicit correction state must be visible to Catalyst.
Catalyst must not overwrite protected manual corrections without a recorded superseding source event or approved policy.
```

The current gap:

```text
An operator can fix Airtable.
The workflow can recreate the same mistake six minutes later.
There is no clean quickfix lane that writes the correction back to Catalyst and records the source error.
```

Therefore, `hot-patch-manual-correction` is a required lane, even if the agent/session is not yet established.

The Hot Patch Agent should eventually own:

- operator correction intake
- correction reason and source-error notes
- protected fields/rows that workflows must not overwrite blindly
- Catalyst write-back or overlay state
- expiration/review policy for temporary fixes
- repeatable bug report back to the owning lane

## Canonical Ownership

Catalyst is canonical for workflow/runtime state unless a dedicated contract says otherwise.

Airtable is visibility, review, operator input, or automation surface depending on the table. Airtable mirror lag must be reported as sync drift, not as canonical runtime failure.

## Routing Decisions

The Routing Agent should route by observed state, not by hope or stale memory.

| Observed state | Route |
|---|---|
| Active `focus_show` changed or date-change event is approaching | `core-next-day-preflight` |
| `hs_update_schedule`, `hs_class_oog_raw`, `hs_class_oog`, or runtime rows are missing in Catalyst | `core-build` |
| Catalyst Step 4 runtime exists but Airtable runtime mirror is missing/stale | `stage-4S-sync` |
| Core runtime prep passes but Time Engine has no `core_runtime_ready` seed result | `core-build` for handoff failure, then `time-engine + trigger` for lane diagnosis |
| Time Engine source rows exist and clock/state wake is eligible | `time-engine + trigger` |
| Time Engine rider-result-ready rows exist and `focus_show.results_enabled=true` | `rider-results` |
| Timer/rider-result alert-ready rows exist or new rider_results rows need alert handling | `alerts` |
| Live fields, time-engine rows, rider_results, or alerts changed and output payload/cache is stale | `publish` |
| Live enrichment enabled but current time is outside show/live window | `live` returns WAITING/SKIPPED, no repair |
| Endpoint contract, payload shape, or Webflow/Catalyst route behavior drifts | `endpoints` |
| Operator manually corrected Airtable and workflow may overwrite it | `hot-patch-manual-correction`, then route durable bug to owning lane |

## Wake Rules

Specialist lanes may make another lane eligible, but they do not own that downstream lane.

| Lane | May wake |
|---|---|
| `core-build` | `live` if enabled; `time-engine + trigger` with `wake_reason=core_runtime_ready` after runtime prep |
| `stage-4S-sync` | none as workflow proof; may mark mirror drift resolved |
| `live` | `time-engine + trigger`; `publish` |
| `time-engine + trigger` | `rider-results`; `alerts`; `publish` |
| `rider-results` | `alerts`; `publish` |
| `alerts` | `publish` |
| `publish` | none |
| `endpoints` | none directly; reports contract drift to owner lane |
| `hot-patch-manual-correction` | owning lane for durable fix; `publish` only if output state intentionally changed |

## Drift Controls

The Routing Agent must prevent these drift patterns:

- Core starts doing Airtable mirror work inside the hot lane.
- Stage 4S is treated as Core proof.
- Core requires `get_ring_days` after `update_schedule` can prove the day/ring/class structure directly.
- Live Agent repairs missing Core runtime rows.
- Live Agent keeps `get_orders` in the hot path without a proven current-state need.
- Time Engine runs result ingestion directly.
- Rider Results Agent expands back to broad class-results machinery before watched `rider_results` proves it is needed.
- Rider Results Agent probes schedule or repairs runtime identity.
- Alerts Agent mutates source/runtime identity.
- Publish Agent mutates source/runtime identity.
- Endpoints Agent changes business logic while fixing route shape.
- Hot Patch Agent hides a correction without a source-error note and owner lane handoff.
- Workflows overwrite Airtable manual corrections without checking correction/lock/overlay state.
- Manual endpoint calls are described as runner proof without explicit approval.
- Date-key rewrites are described as next-day proof.

## Status Vocabulary

Use these words consistently:

| Status | Meaning |
|---|---|
| `PASS` | Verified clean against the correct lane contract. |
| `FAIL` | Lane attempted the approved path and hit a blocker. Stop and report blocker. |
| `WAITING` | Lane is gated by time/window/eligibility and should not run yet. |
| `OPEN` | Work remains; not necessarily failed. |
| `BLOCKED` | Cannot proceed without external action or approved contract change. |

## Proof Standards

For workflow proof:

- use approved runner/cadence path
- report run id
- report lane status
- report source counts
- report rows written/read back
- report exact blocker or skip reason

For diagnostic/manual proof:

- mark it as diagnostic or staging catch-up only
- do not call it workflow proof
- do not use it to mask runner failure

## Current Known Routing State

As of the 2026-07-09 handoff:

| Lane | State |
|---|---|
| `core-build` | Catalyst runtime rows present for active focus day under the prior Core 1-4 implementation; cleaner `update_schedule`-first contract is now the target. |
| `stage-4S-sync` | Deployed and working through temporary Codex monitor; real Catalyst cron still missing. |
| `time-engine + trigger` | `core_runtime_ready` seed wake passed and wrote rows. |
| `live` | Expected to wait/skip outside live window. |
| `rider-results` | Depends on Time Engine rider-result-ready eligibility. |
| `alerts` | Depends on timer/rider-result alert-ready rows and new rider_results rows. |
| `publish` | Depends on upstream prepared-data changes. |

## Required Router Output

When asked for status, the Routing Agent should answer in this shape:

```text
focus_show:
  show_no:
  focus_day:

lane_status:
  core-build:
  stage-4S-sync:
  live:
  time-engine + trigger:
  rider-results:
  alerts:
  publish:
  endpoints:
  hot-patch-manual-correction:

current_blockers:
  - lane:
    blocker:
    owner:
    next_action:

runner_proof:
  approved_runner_path:
  latest_run_id:
  manual_diagnostics_used:
```

## Related Documents

- `docs/horseshowing/ringstatus-scheduling-specialist-agent-prompt-pack.md`
- `docs/horseshowing/wec-lane-codex-session-map-2026-07-08.md`
- `docs/horseshowing/wec-clean-stage1-4-workflow-contract.md`
- `docs/horseshowing/wec-time-engine-contract.md`
- `docs/horseshowing/wec-catalyst-step6-results-contract.md`
- `ringstatus-data/catalyst-workspaces/horseshowing/docs/core_1_4_next_day_preflight_contract.md`
