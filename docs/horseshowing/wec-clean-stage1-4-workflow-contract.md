# WEC Clean Stage 1-4 Workflow Contract

## Purpose

This is the current working contract for the newest WEC Catalyst-first workflow.

Older WEC workflow docs are reference only and should be treated as deprecated unless this document explicitly points back to them.

This document is for the clean rebuild path, not the patched legacy cadence path.

Core 1-4 also owns proactive next-day readiness testing in an outside lane. That responsibility is locked in `ringstatus-data/catalyst-workspaces/horseshowing/docs/core_1_4_next_day_preflight_contract.md`.

## Current Clean Scope

| Stage | Purpose | Current Status |
|---|---|---|
| Stage 0 | Resolve active `focus_show` | Active |
| Stage 1 | Heartbeat and ring days | Active |
| Stage 2 | Update schedule | Active |
| Stage 3A | Fast class_oog probe | Active |
| Stage 3B | Parse stored class_oog docs | Active |
| Stage 4 | Runtime prep | Active, still being locked |
| Outside-lane next-day preflight | Real next focus-day source acquisition, bounded probe/parse, and Step 4 projection without writes | Active Core responsibility |

## Canonical Tables

| Table | Role |
|---|---|
| `hs_heartbeat` | cadence/audit heartbeat |
| `focus_show` | active show/day control |
| `hs_get_ring_days` | current focus-day ring-day source |
| `hs_update_schedule` | current focus-day schedule source |
| `hs_class_oog_raw` | stored class_oog raw docs from 3A |
| `hs_class_oog` | parsed class_oog rows from 3B |
| `hs_ring_status` | Step 4 runtime ring rows |
| `hs_class_start_times` | Step 4 runtime class rows |
| `hs_entry_go_times` | Step 4 runtime entry rows |

## Mirror Rule

Catalyst `hs_*` tables are canonical.

Airtable `hs_*` tables are mirrors for visibility/review.

Core hot-lane proof requires:

- Catalyst row written
- Catalyst readback by key
- Airtable mirror work marked as deferred backlog

Airtable mirror lag, skip, or mismatch must not block Core 1-4 runtime completion. Mirror catch-up must run as Stage 4S, a separate paged sync lane/action, and report its own drift state.

Counts alone are not proof.

## Key Contract

### Const Keys

| Field | Shape |
|---|---|
| `show_const_key` | `show_no` |
| `focus_day_const_key` | `show_no|focus_day_key` |
| `ring_day_const_key` | `show_no|focus_day_key|ring_day_no` |
| `ring_const_key` | `show_no|focus_day_key|ring_day_no|ring_no` |
| `class_const_key` | `show_no|focus_day_key|ring_day_no|ring_no|class_no` |
| `entry_const_key` | `show_no|focus_day_key|ring_day_no|ring_no|class_no|entry_no` |

`focus_day_key` is compact date format: `YYYYMMDD`.

### Visual Keys

Visual keys are for Airtable/human grouping and output grouping, not HorseShowing source requests.

| Field | Shape |
|---|---|
| `ring_visual_key` | `ring_no|ring_name_token` |
| `class_visual_key` | `ring_name_token|class_no` |
| `entry_visual_key` | `ring_name_token|class_no|entry_no` |

`ring_name_token` is derived from `ring_name_normalized`.

## Stage 1 Contract

Stage 1 resolves active `focus_show`, writes heartbeat evidence, and writes current focus-day `hs_get_ring_days`.

Required behavior:

- Use active `focus_show`.
- No hardcoded focus day.
- No old-day fallback.
- Update/create current focus-day ring-day rows.
- Defer Airtable mirror work outside the hot lane.

Output:

- `hs_heartbeat`
- `hs_get_ring_days`

## Stage 2 Contract

Stage 2 reads current focus-day `hs_get_ring_days` and writes current focus-day `hs_update_schedule`.

Required behavior:

- Use current active focus day only.
- Write class schedule rows.
- Keep preflight rows visible.
- Mark preflight rows with `is_preflight` / `preflight_reason` where fields exist.
- Valid downstream rows must have real `class_no`.
- No `event_id` fallback for class identity.

Output:

- `hs_update_schedule`

Downstream eligible rows:

- active focus day
- `is_preflight = false`
- valid `class_no`

## Stage 3A Probe Contract

Stage 3A probes `class_oog.php` one native class at a time from current eligible `hs_update_schedule`.

Required behavior:

- Source: current focus-day non-preflight `hs_update_schedule`.
- Request one native HorseShowing `class_no` at a time.
- Source request uses HorseShowing-native params only.
- Do not send visual keys upstream.
- Scan the full payload for allowed trainer/helper evidence.
- Do not parse/materialize `hs_class_oog` in 3A.
- Store the full raw doc only when evidence exists.
- Mark progress/certainty on the source class/probe state.
- Continue without blocking the entire day on one class.

Output:

- `hs_class_oog_raw`
- probe status/progress fields

Probe evidence is triage, not final truth.

### Stage 3A2 Non-Blocking Retry Contract

`3A2` is the second-pass retry lane for classes already marked `checked` with `probe_reason=no_allowed_trainer_evidence`.

Required behavior:

- `3A2` is follow-up refinement only.
- `3A2` must not block initial Stage 3B or Stage 4 runtime prep.
- Retry no-match probes only up to the approved cap.
- Mark retry attempts and final terminal no-match state.
- Store raw docs only if new evidence appears during retry.
- Route any new raw docs to `3B2`.

The production primary gate must treat checked/no-match rows as complete enough for initial runtime prep. They remain visible as second-pass candidates, not primary blockers.

## Stage 3B Parse Contract

Stage 3B parses stored raw docs locally.

Required behavior:

- Source: `hs_class_oog_raw`
- No HorseShowing requests in 3B.
- Parse raw docs already stored by 3A.
- Materialize parsed class_oog rows into `hs_class_oog`.
- Keep enough broad parsed context for review if useful.
- Step 4 must use only matched/scoped rows for `hs_entry_go_times`.

Output:

- `hs_class_oog`

### Stage 3B2 Non-Blocking Parse Contract

`3B2` parses raw docs discovered by `3A2`.

Required behavior:

- `3B2` runs after `3A2` produces new raw docs.
- `3B2` does not block initial Stage 4 runtime prep.
- Any new parsed/matched entries become refinement rows for downstream refresh.
- Missing `3B2` work must be reported as second-pass pending work, not as failure of the initial Core production cadence.

## Stage 4 Runtime Prep Contract

Stage 4 builds runtime tables from the clean source tables.

Sources:

- `hs_get_ring_days`
- `hs_update_schedule`
- `hs_class_oog`

Outputs:

- `hs_ring_status`
- `hs_class_start_times`
- `hs_entry_go_times`

Required behavior:

- `hs_ring_status` comes from `hs_get_ring_days`.
- `hs_class_start_times` comes from non-preflight `hs_update_schedule`.
- `hs_entry_go_times` comes from matched/scoped `hs_class_oog` rows only.
- Broad nonmatching `hs_class_oog` rows must not become `hs_entry_go_times`.
- Every write must be read back by key.
- Rows not matching current key shape may be marked dropped/stale for the active focus day only.
- After Step 4 passes, Core must seed Time Engine with `wake_reason=core_runtime_ready`.
- A skipped or failed Time Engine seed wake is a Core handoff blocker for that run.
- Core must not wake `publish` directly.
- Core must not run Airtable mirror catch-up inside the hot cadence path.

## Stage 4S Sync Contract

Stage 4S is the Airtable visibility sync for Step 4 runtime rows. It is not Core Step 4.

Stage 4 writes canonical Catalyst runtime rows:

- `hs_ring_status`
- `hs_class_start_times`
- `hs_entry_go_times`

Stage 4S reads those Catalyst rows and mirrors them to Airtable for staging review and operator visibility. Stage 4S may run after Core, may lag Core, and may require multiple bounded passes. Stage 4S failure or backlog must be reported as sync drift, not as Core runtime failure.

The current Stage 4S function endpoint alias is `wec-step4-airtable-mirror`; the lane name is `stage-4S-sync`.

Current latest diagnostic evidence:

- `hs_ring_status = 9`
- `hs_class_start_times = 69`
- `hs_entry_go_times = 76`
- write/readback verified inside deployed clean proof function

This diagnostic evidence is not scheduler proof.

## Next-Day Preflight Responsibility

Core 1-4 must proactively test the next `focus_day` before the live date change.

This is not a one-time rescue path. It is a standing outside-lane responsibility to find the first blocker early, classify it, and feed a repeatable fix back into Core.

Required outside-lane behavior:

- Use actual next-day HorseShowing source data.
- Run `get_ring_days.php` for the show.
- Select ring-day rows for the target next `focus_day`.
- Run `update_schedule.php` for each target ring day.
- Run bounded `class_oog.php` probe according to Core policy.
- Parse stored raw docs in memory.
- Project Step 4 runtime rows in memory.
- Write no heartbeat rows.
- Write no source/runtime rows.
- Repair no production records.

Date-key rewriting is not next-day proof. It only proves that canonical key construction can carry a copied dataset across a different date key.

PASS means:

- focus-day ring rows exist
- update schedule rows exist for the focus-day rings
- probe completes within the approved cap
- raw docs are parsed
- no pending parse docs remain
- Step 4 projection has no blocker
- projected runtime rows are nonzero for `hs_ring_status`, `hs_class_start_times`, and `hs_entry_go_times`

FAIL means:

- stop at the first blocker
- report the failed stage
- report source counts and examples
- classify the blocker as source availability, Core parsing, matching policy, runtime projection, schema/identity drift, or cadence continuation drift
- do not patch live records to produce a one-time pass

Current outside-lane command shape:

```powershell
node .\core_1_4_lab.js --dataset-source live --show-no 14910 --source-focus-day YYYY-MM-DD --run-probe true --retry-no-match-to-cap true
```

This command is diagnostic proof only. It is not production cadence proof.

## Helper Contract

Helper tables are Catalyst-owned runtime helpers with Airtable as the human edit surface.

Expected helper groups:

- `hs_rings`
- `hs_horses`
- `hs_riders`
- `hs_trainers`

Required helper sync concepts:

- Preserve Airtable `rec_id`.
- Preserve Catalyst `ROWID`.
- Support active/follow/allowed fields.
- Support add/edit/ignore/inactive/remove policy without broad deletes.
- Helper edits must not directly mutate Stage 1-4 source/runtime rows.

## Exclusions

This clean Stage 1-4 contract does not run:

- `get_rings`
- `get_orders`
- `get_results`
- alerts
- mobile
- mobile-pro
- print
- PDF
- two_way
- Webflow publish
- Production deploy

## Proof Rules

PASS requires:

- active `focus_show` resolved dynamically
- no old-day fallback
- required keys populated
- required rows written
- write/readback by key
- Stage 4S Airtable mirror backlog is marked/deferred, not blocking
- no downstream/live/output lanes ran

FAIL requires stopping at the exact failed stage.

Manual/direct endpoint runs are diagnostic only unless explicitly approved as proof.

## Deprecated Reference Docs

The following docs remain useful for history but are not the current clean contract:

- `docs/horseshowing/wec-catalyst-cadence-step1-step2-contract.md`
- `docs/horseshowing/wec-catalyst-cadence-step3-class-oog-contract.md`
- `docs/horseshowing/wec-catalyst-step1-step4-stack-contract.md`
- `docs/horseshowing/wec-full-workflow-lock.md`
