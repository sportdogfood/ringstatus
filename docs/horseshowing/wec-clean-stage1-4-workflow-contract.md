# WEC Clean Stage 1-4 Workflow Contract

## Purpose

This is the current working contract for the newest WEC Catalyst-first workflow.

Older WEC workflow docs are reference only and should be treated as deprecated unless this document explicitly points back to them.

This document is for the clean rebuild path, not the patched legacy cadence path.

## Current Clean Scope

| Stage | Purpose | Current Status |
|---|---|---|
| Stage 0 | Resolve active `focus_show` | Active |
| Stage 1 | Heartbeat and ring days | Active |
| Stage 2 | Update schedule | Active |
| Stage 3A | Fast class_oog probe | Active |
| Stage 3B | Parse stored class_oog docs | Active |
| Stage 4 | Runtime prep | Active, still being locked |

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

Mirror proof requires:

- Catalyst row written
- Catalyst readback by key
- Airtable mirror row written
- Airtable mirror row count/key match where required

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
- Preserve mirror behavior.

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

Current latest diagnostic evidence:

- `hs_ring_status = 9`
- `hs_class_start_times = 69`
- `hs_entry_go_times = 76`
- write/readback verified inside deployed clean proof function

This diagnostic evidence is not scheduler proof.

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
- Airtable mirror matches where required
- no downstream/live/output lanes ran

FAIL requires stopping at the exact failed stage.

Manual/direct endpoint runs are diagnostic only unless explicitly approved as proof.

## Deprecated Reference Docs

The following docs remain useful for history but are not the current clean contract:

- `docs/horseshowing/wec-catalyst-cadence-step1-step2-contract.md`
- `docs/horseshowing/wec-catalyst-cadence-step3-class-oog-contract.md`
- `docs/horseshowing/wec-catalyst-step1-step4-stack-contract.md`
- `docs/horseshowing/wec-full-workflow-lock.md`

