# WEC Packing Execution Checklist

This checklist is the execution control for the WEC packing app. The project contract explains the intended app. This file controls what may happen next.

Rule: do not build a layer until the layer underneath it is verified.

## Current Baseline

Trusted baseline:

- `docs/wec_packing_live_app_project_contract.md`

Premature implementation to park or review before reuse:

- `webflow-cloud-test/src/lib/wec-packing.js`
- `webflow-cloud-test/src/pages/wec-packing/`
- `webflow/packing-worksheet/wec-packing.js`
- `webflow/packing-worksheet/wec-packing-preview.html`
- `webflow/packing-worksheet/wec-packing-webflow-embed.html`

Do not continue from these implementation files until the gates below are complete.

## Gate 1: Airtable Registry

Purpose: prove the WEC Airtable registry is usable.

Inputs:

- `wec_meta`
- table API `tbllJywsOstkqT5yZ`

Required output:

- active registry rows
- planned registry rows
- ignored/supporting rows
- `AIRTABLE__TABLE` keys
- `AIRTABLE__VIEW` keys
- `fields_allowed` values

Stop condition:

- Any required live table is missing from `wec_meta`.
- Any planned table lacks `fields_allowed`.
- Any env key row is unclear.

## Gate 2: Physical Tables

Purpose: prove the tables needed for reads and writes physically exist.

Required physical tables:

- `wec_shows`
- `wec_weeks`
- `wec_horses`
- `wec_pack_lists`
- `wec_pack_items`
- `wec_packing_items`
- `wec_packing_item_horses`
- `wec_pack_waves`
- `wec_packing_events`

Required output:

- table name
- table API id
- Airtable metadata confirmation
- views needed by app
- missing fields against `fields_allowed`

Stop condition:

- `wec_pack_waves` does not physically exist.
- `wec_packing_events` does not physically exist.
- Any required `fields_allowed` field is missing.

## Gate 3: Calculation Scope

Purpose: prove worksheet quantities can be calculated without guessing.

Required decisions:

- pack wave records exist for Week 1 Truck, Week 3 Truck, and Week 4 Pack-Up or their approved equivalents
- horse attendance uses `wec_weeks`
- groom count is wave-level manual or ratio-based
- `wec_ranges` is not used by the active packing app; `wec_pack_waves` is sufficient

Stop condition:

- No active/current pack wave exists.
- Groom count source is ambiguous.
- Horse attendance scope is ambiguous.

## Gate 4: Worksheet Snapshot Contract

Purpose: prove the app can save current progress without mutating source templates.

Required decisions:

- `wec_packing_items` stores frozen item snapshots
- `wec_packing_item_horses` stores frozen horse-item snapshots
- `wec_packing_events` stores action history
- writes update current state and append event history

Stop condition:

- Any write action has no matching history event.
- Any current-state table lacks a source item or pack wave link.

## Gate 5: Webflow Cloud Environment

Purpose: prove the server can read/write Airtable without exposing credentials.

Required output:

- env key checklist from `wec_meta`
- Webflow Cloud env values confirmed
- health endpoint returns expected table IDs
- no Airtable token in browser

Stop condition:

- Any required env key is missing.
- API route cannot read `wec_meta`.
- route names are not final.

## Gate 6: Read API

Purpose: prove the app can load live data before any UI wiring.

Required output:

- one read-only API endpoint
- returns show/wave context
- returns sections/lists
- returns worksheet rows
- returns horse members for horse-specific rows
- returns gates/missing setup clearly

Stop condition:

- API response includes guessed quantities.
- API response includes blank `list_plan` rows as normal worksheet rows.

## Gate 7: Write API

Purpose: prove save behavior safely updates progress and history.

Allowed actions:

- quantity add
- quantity clear
- mark packed
- mark not packed
- horse item packed/not packed
- decision max
- decision kill
- decision note
- decision purchase onsite
- decision unresolved
- decision clear

Required behavior:

- validate fields against `fields_allowed`
- update current-state row
- append `wec_packing_events` row
- return updated normalized row

Stop condition:

- Any action writes current state without event history.
- Any action writes fields not present in `fields_allowed`.

## Gate 8: UI Wiring

Purpose: connect the approved prototype cadence to the live API.

Allowed UI work:

- preserve existing cadence
- replace fake/local data with API state
- keep existing list/detail/worksheet behavior
- add only approved labels/minor global styling adjustments

Stop condition:

- new interaction pattern is introduced
- embed is changed before API/env proof
- localStorage becomes source of truth

## Gate 9: Webflow Embed

Purpose: create the deployable embed only after backend and assets are real.

Required output:

- pinned Git commit URLs
- final Webflow Cloud API URL
- root div
- config block
- CSS link
- JS script
- rollback note

Stop condition:

- `REPLACE_WITH_COMMIT` remains.
- URL points to `main` for production.
- Webflow Cloud route has not been verified.
