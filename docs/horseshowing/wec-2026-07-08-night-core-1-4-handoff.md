# WEC Core 1-4 Night Handoff - 2026-07-08

## Purpose

This note records tonight's Core 1-4 progress, blockers, outside-lane test surfaces, and the checklist for the next focus-day change event.

This is an operator handoff, not workflow proof. Manual/direct endpoint calls remain diagnostic only unless explicitly accepted as proof.

## Current Focus Context

Observed active focus during this work:

| Field | Value |
|---|---|
| `show_no` | `14910` |
| `focus_day` | `2026-07-09` |
| environment | Catalyst Development |
| Core cadence target | `wec_stage1_3_clean_proof/?action=wec-clean-cadence-stack` |

## Tonight's Progress

### Core Hot Lane

Core 1-4 was clarified as the canonical Catalyst runtime lane.

Core writes:

- `hs_get_ring_days`
- `hs_update_schedule`
- `hs_class_oog_raw`
- `hs_class_oog`
- `hs_ring_status`
- `hs_class_start_times`
- `hs_entry_go_times`

Core does not directly wake `publish`.

After Step 4 runtime prep passes, Core must seed Time Engine with:

```text
wake_reason=core_runtime_ready
```

That seed wake is a Core handoff requirement. A skipped or failed seed is reported as a Core handoff blocker for that run.

### 3A / 3B Policy Correction

The production path now treats checked/no-match probe rows as complete enough for initial runtime when they have no allowed trainer evidence.

Second-pass work is separated:

- `3A2`: retry checked/no-match probe candidates up to the approved cap
- `3B2`: parse any new raw docs found by `3A2`

`3A2` and `3B2` must not hold up initial production runtime prep.

The current retry cap is:

```text
NO_MATCH_PROBE_RETRY_MAX_ATTEMPTS = 3
```

### Airtable Mirror Removed From Core Hot Lane

Airtable mirroring was removed from the Core hot cadence path.

Reason: Airtable row-by-row mirror work caused timeout pressure and made Core appear failed even after Catalyst canonical rows existed.

New rule:

```text
Catalyst runtime rows are Core truth.
Airtable rows are visibility/review mirrors.
Airtable mirror lag is sync drift, not Core runtime failure.
```

Latest known split from the session:

| Table | Catalyst canonical | Airtable mirror |
|---|---:|---:|
| `hs_ring_status` | 9 | 0 |
| `hs_class_start_times` | 59 | 0 |
| `hs_entry_go_times` | 32 | 0 |

This means Core runtime materialization was present in Catalyst, while Airtable staging visibility was still open.

### Stage 4S Named

The Airtable runtime mirror lane is now named:

```text
Stage 4S sync
```

Stage 4 writes canonical Catalyst runtime rows.

Stage 4S reads Catalyst runtime rows and mirrors them to Airtable for staging review/operator visibility.

Current function endpoint alias:

```text
wec-step4-airtable-mirror
```

Current lane response mode in code:

```text
stage-4S-sync
```

The Stage 4S default sync limit was changed locally from `25` to `100`.

Verification completed:

```text
node --check handler.js passed
```

Not yet completed:

```text
Stage 4S scheduler has not been created.
```

Update after approval:

```text
Stage 4S code change deployed to Catalyst Development.
Manual Stage 4S catch-up ran for staging review only.
Recurring Codex-side outside monitor created: wec-stage-4s-mirror-sync-monitor.
```

Manual catch-up result:

| Table | Processed | Total | Complete |
|---|---:|---:|---|
| `hs_ring_status` | 9 | 9 | true |
| `hs_class_start_times` | 59 | 59 | true |
| `hs_entry_go_times` | 32 | 32 | true |

Post-deploy idempotent verification:

```text
mode=stage-4S-sync
endpoint_alias=wec-step4-airtable-mirror
table=hs_class_start_times
processed=59
total=59
limit=100
complete=true
```

## Current Blockers

### Blocker 1: Stage 4S Scheduler Missing

Live Catalyst cron inventory did not show an Airtable mirror/sync cron.

Current active crons are:

- Core day/night cadence
- live-enrich
- time-engine clock
- results

Missing:

```text
Stage 4S Airtable sync cron
```

Impact:

```text
Waiting will not automatically fill Airtable staging mirror rows unless a scheduler exists.
```

### Blocker 2: Catalyst Cron Creation Through MCP Is Not Yet Proven

Attempted to create a Catalyst cron through the scheduler MCP.

Result:

```text
INVALID_INPUT
The input value is not readable. Please check your input.
```

Likely cause:

The Create Cron API schema requires a webhook `target_id`, but the List/Get Cron responses expose only the resolved target URL/name, not the hidden target id needed to create a new cron safely.

Decision:

Do not keep guessing the scheduler payload shape.

Needed next:

- create the Stage 4S cron in Catalyst console, or
- expose the scheduler webhook target id, or
- add a supported target creation/lookup path before using MCP creation.

Temporary mitigation:

```text
Codex heartbeat automation: wec-stage-4s-mirror-sync-monitor
Frequency: every 30 minutes
Scope: Stage 4S mirror only, staging visibility only
```

This is not a substitute for a real Catalyst cron. It is an outside monitor/catch-up guard until the Catalyst scheduler target issue is resolved.

### Blocker 3: Staging Review Depends On Airtable Mirror

Core has canonical Catalyst runtime rows, but staging review needs Airtable mirror rows.

Until Stage 4S runs successfully:

```text
Core runtime: PASS in Catalyst
Airtable staging visibility: OPEN
```

## Outside Lanes

### Outside Lane: Core 1-4 Next-Day Preflight

Contract:

```text
ringstatus-data/catalyst-workspaces/horseshowing/docs/core_1_4_next_day_preflight_contract.md
```

Lab script:

```text
ringstatus-data/catalyst-workspaces/horseshowing/functions/wec_stage1_3_clean_proof/core_1_4_lab.js
```

Command shape:

```powershell
node .\core_1_4_lab.js --dataset-source live --show-no 14910 --source-focus-day YYYY-MM-DD --run-probe true --retry-no-match-to-cap true
```

Purpose:

- use real next-day HorseShowing source data
- run ring-day acquisition
- run schedule acquisition
- run bounded 3A probe
- parse raw docs in memory
- project Step 4 runtime rows in memory
- identify first blocker before production date change

This lane must not write production rows or count as cadence proof.

### Outside Lane: Stage 4S Airtable Sync

Function action:

```text
wec-step4-airtable-mirror
```

Lane name:

```text
stage-4S-sync
```

Tables:

- `hs_ring_status`
- `hs_class_start_times`
- `hs_entry_go_times`

Purpose:

- mirror Catalyst runtime rows to Airtable for staging review
- run outside Core hot cadence
- report sync drift separately from Core runtime state

Current blocker:

```text
No Catalyst scheduler exists yet.
Temporary Codex-side outside monitor exists.
```

### Outside Lane: Step 3 Airtable Mirror

Function action:

```text
wec-step3-airtable-mirror
```

Tables:

- `hs_update_schedule`
- `hs_class_oog_raw`
- `hs_class_oog`

Purpose:

- mirror Step 2/3 source and parse state to Airtable visibility tables
- stay outside Core hot cadence

This lane has not yet been renamed, but it is the Step 3 mirror counterpart to Stage 4S.

## Tomorrow Focus-Day Change Notes

At the next focus-day change, do not start by looking only at Airtable.

First verify Catalyst canonical rows:

1. active `focus_show.show_no`
2. active `focus_show.focus_day`
3. `hs_get_ring_days`
4. `hs_update_schedule`
5. `hs_class_oog_raw`
6. `hs_class_oog`
7. `hs_ring_status`
8. `hs_class_start_times`
9. `hs_entry_go_times`
10. Time Engine seed status from Core heartbeat

Then verify mirror drift separately:

1. Step 3 Airtable mirror counts
2. Stage 4S Airtable mirror counts
3. Stage 4S scheduler state

Expected interpretation:

```text
Catalyst missing runtime rows = Core blocker.
Catalyst has runtime rows but Airtable has zero = Stage 4S sync blocker.
Time Engine not seeded after runtime rows = Core handoff blocker.
Live-enrich outside live window = downstream/live gate, not Core blocker.
```

## Tomorrow Action List

1. Verify the deployed Stage 4S code is still active.
2. Create or configure the real Stage 4S Catalyst scheduler outside the Core hot lane.
3. Run/read the next-day preflight before relying on the production date-change cadence.
4. On date change, verify Catalyst canonical state before Airtable mirror state.
5. If production fails, stop at the first failed stage and report `FAIL`; do not repair records to force a one-time pass.

## Status

```text
Core policy split: documented
3A2 / 3B2 second-pass policy: documented and implemented
Core Time Engine seed wake: implemented
Airtable mirror removed from hot lane: implemented
Stage 4S name/default limit: deployed to Catalyst Development
Stage 4S manual catch-up: completed for staging review
Stage 4S Codex-side monitor: active every 30 minutes
Stage 4S Catalyst scheduler: missing
Next-day preflight responsibility: documented
```
