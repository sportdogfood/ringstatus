# WEC Packing Project Overview And Handoff

Last updated: 2026-05-27

This document is the working project overview for the WEC Ocala packing app. It is intended for the owner, Codex runners, and any future implementation runner who needs to understand the purpose, data model, Webflow Cloud relationship, Webflow embed, approved UI cadence, known conflicts, open tasks, and how to build a similar project without reinventing the brand or interaction model.

## Executive Summary

The WEC packing app is a mobile-first RingStatus/Webflow app for tracking horse-show packing progress over multiple days.

The app answers:

- what needs to be packed
- what has already been packed
- what is still left
- which horse-specific items are packed per horse
- which items have been resolved by a decision instead of packed
- which items need to be purchased onsite
- what should be printed for barn, show, tack, grooming, health, feed, horse, and purchase workflows
- what changed over time through event history

This project is not a fresh design exploration. The visual cadence, button behavior, modal rhythm, row language, and brand styling are inherited from the approved WEC prototype and the HPS/RingStatus app pattern. Future work must preserve the shared `lp-*`, `th-*`, and `packing-*` class language unless the owner explicitly approves a global CSS contract change.

## Non-Negotiable Brand And UI Contract

The app must remain visually consistent with the existing RingStatus/HPS brand.

Do not reinvent:

- modal structure
- row cadence
- pill/token geometry
- font family
- button rhythm
- tab/list pattern
- spacing system
- `lp-*`, `th-*`, or `packing-*` hooks
- Webflow embed delivery pattern

The root font is:

```css
Outfit, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif
```

The shared CSS is global to multiple app roots:

```css
:is(#packing-app, #tack-horses-app, #hps-app)
```

This means changes to shared selectors can affect WEC packing, HPS, and tack-horses surfaces. A runner must assume shared CSS edits are risky unless they are intentionally global. If a change is view-specific, scope it narrowly under the relevant app root and local class.

Approved visual and interaction principles:

- mobile-first
- list/table cadence, not carousel
- overview first
- rows open detail modals
- modals use the HPS-style detail shell
- action buttons are pills/hot text, not custom controls
- tokens with the same purpose must be visually identical
- all repeated row patterns must use the same dimensions and padding across media sizes
- no new framework or card-heavy redesign

## Current Repo Locations

Static Webflow/browser assets:

```text
webflow/packing-worksheet/styles.css
webflow/packing-worksheet/wec-packing.js
webflow/packing-worksheet/wec-packing-webflow-preview.html
webflow/packing-worksheet/wec-packing-webflow-embed.html
```

Webflow Cloud/Astro server routes and shared library:

```text
webflow-cloud-test/src/lib/wec-packing.js
webflow-cloud-test/src/pages/wec-packing/index.js
webflow-cloud-test/src/pages/wec-packing/state.js
webflow-cloud-test/src/pages/wec-packing/action.js
webflow-cloud-test/src/pages/wec-packing/health.js
webflow-cloud-test/src/pages/wec-packing/print.js
webflow-cloud-test/src/pages/wec-packing/reconcile.js
```

Project docs:

```text
docs/wec_packing_live_app_project_contract.md
docs/wec_packing_current_state_tables_audit.md
docs/wec_packing_execution_checklist.md
docs/wec_packing_gate1_registry_report.md
docs/wec_packing_project_overview_handoff.md
```

Related reference docs:

```text
docs/hps_horses_webflow_airtable_connector_readme.md
docs/hps_duplicate_connector_new_chat_prompt.md
docs/webflow_cloud_dataset_template_handoff.md
docs/webflow_interaction_readme.md
```

## Architecture

The app follows the RingStatus two-way Webflow Cloud pattern:

```text
Webflow page embed
  -> public CSS/JS assets from GitHub/jsDelivr
  -> browser config object
  -> Webflow Cloud/Astro API route
  -> Airtable API
  -> Airtable current-state rows
  -> Airtable event/history rows
  -> normalized API response
  -> optimistic UI update plus refresh
```

Important boundaries:

- Browser assets are public and must not contain Airtable credentials.
- Webflow Cloud is the server boundary for Airtable reads/writes.
- Airtable is the source of truth for saved progress.
- Local browser state can be used for fast optimistic feedback, but not as the durable source of truth.
- GitHub/jsDelivr serves static frontend files only; it is not the write path.
- Webflow Designer owns the page/container. The data app fills the container.

## Webflow Cloud Relationship

The server app lives in `webflow-cloud-test/` and exposes WEC routes under:

```text
/test/wec-packing
/test/wec-packing/state
/test/wec-packing/action
/test/wec-packing/health
/test/wec-packing/print
/test/wec-packing/reconcile
```

Route responsibilities:

| route | method | purpose |
| --- | --- | --- |
| `/test/wec-packing/state` | GET | Load normalized app state for a show/wave |
| `/test/wec-packing/action` | POST | Apply a user action, update Airtable current state, append event history, return refreshed state |
| `/test/wec-packing/health` | GET | Check Airtable table registry, envs, physical tables, and missing fields |
| `/test/wec-packing/print` | GET | Render printable HTML for lists, horses, overview, and home modules |
| `/test/wec-packing/reconcile` | GET | Identify dynamic count and quantity mismatch issues |
| `/test/wec-packing` | GET | Basic route entry/health style endpoint |

Local preview currently points to a local Webflow Cloud dev server:

```text
http://127.0.0.1:4331/wec-packing
```

The live embed points to:

```text
https://ringstatus.webflow.io/test/wec-packing
```

Known deployment caveat:

If the local preview shows a feature and the live embed does not, first confirm whether the Webflow Cloud endpoint has been deployed. During development, the static frontend may already have the UI code while the live Cloud endpoint still returns an older state shape.

## Webflow Embed Contract

Current embed file:

```text
webflow/packing-worksheet/wec-packing-webflow-embed.html
```

Current embed shape:

```html
<div id="packing-app">Loading WEC packing...</div>

<script>
  window.WEC_PACKING_CONFIG = {
    mode: "edit",
    apiUrl: "https://ringstatus.webflow.io/test/wec-packing",
    stateUrl: "https://ringstatus.webflow.io/test/wec-packing/state",
    actionUrl: "https://ringstatus.webflow.io/test/wec-packing/action",
    healthUrl: "https://ringstatus.webflow.io/test/wec-packing/health",
    printUrl: "https://ringstatus.com/test/wec-packing/print",
    pdfWorkerUrl: "https://ringstatus-pdf.gombcg.workers.dev/",
    showId: "",
    packWaveId: "",
    packWaveKey: "wave_one"
  };
</script>

<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Outfit:wght@400;500;600;700&display=swap" rel="stylesheet">

<link rel="stylesheet" href="https://cdn.jsdelivr.net/gh/sportdogfood/ringstatus@main/webflow/packing-worksheet/styles.css?v=wec-preview-91">
<script src="https://cdn.jsdelivr.net/gh/sportdogfood/ringstatus@main/webflow/packing-worksheet/wec-packing.js?v=wec-preview-91" defer></script>
```

Production warning:

The current embed uses `@main`. Before final production use, pin CSS/JS to a verified Git commit hash. Do not leave production Webflow pages on `@main` unless the owner explicitly accepts that risk.

Example production pin pattern:

```text
https://cdn.jsdelivr.net/gh/sportdogfood/ringstatus@COMMIT_HASH/webflow/packing-worksheet/styles.css
https://cdn.jsdelivr.net/gh/sportdogfood/ringstatus@COMMIT_HASH/webflow/packing-worksheet/wec-packing.js
```

## Airtable Registry And Env Pattern

`wec_meta` is the registry for WEC tables and environment intent.

Known registry table:

```text
table name: wec_meta
table api:  tbllJywsOstkqT5yZ
```

Important registry fields:

| field | purpose |
| --- | --- |
| `const_env` | indicates the row should be represented as a Webflow Cloud env constant |
| `ignore` | excludes a registry row from active implementation |
| `AIRTABLE__TABLE` | env key for table id |
| `AIRTABLE__VIEW` | env key for view name |
| `fields_allowed` | minimum API field contract |
| `table_name` | logical table name |
| `table_api` | physical Airtable table id |

Server behavior:

- `webflow-cloud-test/src/lib/wec-packing.js` reads `wec_meta`.
- It builds table config from registry rows, physical Airtable schema, and env overrides.
- Required tables must exist and pass health checks.
- Optional tables can be skipped without hard failure.

Required logical tables:

```text
wec_meta
wec_shows
wec_weeks
wec_horses
wec_pack_lists
wec_pack_items
wec_pack_waves
wec_packing_items
wec_packing_item_horses
wec_packing_events
```

Optional/support tables currently used:

```text
wec_list_plans
wec_places
wec_places_tags
```

## Data Model Roles

The important distinction is source records versus worksheet records.

### Source/template layer

```text
wec_pack_lists
wec_pack_items
wec_list_plans
```

Purpose:

- define the reusable packing template
- organize source items into lists and tabs
- define quantity-generation plans
- store item-level metadata such as places/vendors/local guidance

`wec_pack_items` is the source catalog. It is not where live packing progress is stored.

### Wave/show scope layer

```text
wec_shows
wec_weeks
wec_pack_waves
```

Purpose:

- define the active show
- define wave attendance and truck/packing windows
- expose deadline and count data

Current wave fields in use or under discussion:

```text
deadline_date
days_till
groom_count_manual
groom_ratio
horse_count
groom_count_final
horse_sanity
groom_sanity
counts_locked
```

Current frontend header pattern:

```text
{wave} | departs: {deadline_date} | {days_till} days remaining
```

### Current worksheet layer

```text
wec_packing_items
wec_packing_item_horses
```

Purpose:

- store current/frozen worksheet state for a specific wave
- track saved packing progress
- support fast mobile reads
- preserve source traceability

`wec_packing_items` is a derivative/snapshot of `wec_pack_items`.

`wec_packing_item_horses` is a derivative/snapshot of horse-specific item membership. It should only be created when horses are locked for the wave or when explicitly generated by the approved workflow.

### History/event layer

```text
wec_packing_events
```

Purpose:

- append action history
- memorialize quantity changes
- memorialize decisions
- memorialize horse item packed/unpacked events
- memorialize onsite task done/reopened events
- log session starts
- log wave count changes when horse/groom counts drift

The event table cannot replace current-state rows. The app needs fast current answers from current-state tables.

## Core Quantity Logic

Never use `___packing_quantity` as the live quantity source.

Primary plans:

| plan | meaning |
| --- | --- |
| `quantity` | fixed source quantity |
| `per_horse` | count math using effective horse count |
| `horse_specific` | count and named horse-member rows for assigned horses |
| `per_groom` | count math using effective groom count |

### `quantity`

```text
needed = wec_pack_items.quantity
```

### `per_horse`

```text
needed = wec_pack_items.per_horse * effective_horse_count
```

This is count math, not a named horse member list.

### `horse_specific`

```text
eligible_horses =
  source item horses
  filtered to current wave

needed = eligible_horses.count * per_horse
```

This is where named horse-member rows appear.

### `per_groom`

```text
needed = wec_pack_items.per_groom * effective_groom_count_final
```

Grooms are currently capacity counts, not named people.

### Frozen versus dynamic needed values

`quantity_needed_dynamic` can reflect live calculations.

`quantity_needed` is the frozen/current worksheet value.

When counts are unlocked, the app can compare dynamic calculation to frozen value. When counts are locked, frozen values should govern.

## Wave Logic

Approved direction:

- use `wec_pack_waves`
- do not use `wec_ranges` for active app logic
- allow `wave_one`, `wave_two`, and future `return_wave`
- horse rows may be locked separately from wave counts
- return wave should reuse/freeze quantities for pack-up/go-home tracking

Open decision:

The exact `return_wave` behavior still needs to be finalized. The likely pattern is:

```text
outbound wave snapshot
  -> saved packing quantities and item states
  -> return_wave snapshot for pack-up
  -> separate event history
```

Do not silently reuse outbound event history as return-wave truth without owner approval.

## Horse Logic

The Horses tab is not an active/inactive horse editor in the current packing app.

Current purpose:

- show active wave horses
- show progress per horse
- print individual horse packing lists
- click a horse to open a modal of that horse's assigned packing items

Horse detail modal:

- top row is progress, not need/packed/left totals
- shows assigned packing items
- item rows toggle `PACKED` / `NOT PACKED`
- row tokens must use the same `lp-achievement packing-token` cadence

Horse-specific item detail:

- item detail shows horse members
- user cannot add horses from the item detail modal
- user can only mark an existing horse-member row packed or not packed

## Purchase Onsite Logic

`purchase_onsite` is a home module, not a quantity list.

It comes from:

```text
wec_pack_lists view: wec_home
wec_pack_items view: wec_purchase_onsite
```

Purpose:

- act as a reminder list of things to buy onsite
- stay in the Wave One/home context
- use the same list cadence
- print like other lists
- toggle `TASK` / `DONE`
- append a `wec_packing_events` record

It should not affect `quantity_needed`, `quantity_packed`, or `quantity_left`.

Current backend event types:

```text
onsite_task_done
onsite_task_reopen
```

Current detail modal:

- status row with `TASK` / `DONE`
- optional details
- places
- tags
- plan line in footer

Current place/tag relationship:

- Preferred future direct fields may be `wec_pack_items.wec_places` and `wec_local_tags_rollups`.
- Current working data shape uses `wec_places.tack_grocery_items` linked back to `wec_pack_items`.
- `wec_places_tags` supplies tag labels for linked place tags.

Known current data detail:

The tested `Farmvet` place record returns as a place for purchase tasks. Its local tags were empty at the time of verification, so `Tags` will not render until tags are populated.

## Decision Logic

Current decision buttons on packing item detail:

```text
MAX
KILL
ONSITE
```

`UNRESOLVED` currently exists in data/API logic but has been hidden in the UI per owner direction.

Approved concept:

- `MAX` is isolated.
- If `MAX` is active, a secondary row can be shown:

```text
REMOVE | ONSITE | UNRESOLVED | SMS
```

SMS concept:

```text
WEC Packing Conflict: {item}
NEED: {needed}, PACKED: {packed}, LEFT {left}
reply
REMOVE | ONSITE | UNRESOLVED
```

Open task:

This max/secondary conflict workflow is not fully final. Do not build a new decision UI model without owner review.

## Write Actions

Current server action names:

```text
session_start
add_quantity
set_pack_state
set_resolution
update_item_fields
set_horse_pack_state
set_horse_record_state
set_source_flag
set_onsite_task_state
```

Important write rules:

- update current state
- append `wec_packing_events`
- return refreshed normalized state
- apply optimistic browser feedback where safe
- do not make the user wait on a long frozen UI
- add/add+1 should feel instant
- longer actions can show spinner/processing feedback

Current optimistic areas:

- quantity add/add+1 applies local quantity feedback before action response
- onsite task state toggles locally before action response

Open testing need:

Each write action still needs a deliberate Airtable round-trip test with before/after record evidence and event evidence.

## Print And PDF

Current print route:

```text
/test/wec-packing/print
```

Current PDF worker:

```text
https://ringstatus-pdf.gombcg.workers.dev/
```

Printing intent:

- list-specific print from each section
- overview should print a report across tabs/modules
- horse print should print one horse's assigned packing list
- purchase onsite should print as a task list
- output should include print date
- print through the Cloudflare PDF worker for iPhone/mobile reliability
- print one section over however many pages it needs, then print the next section
- do not pack unrelated sections into two columns
- each printed page must include the global header
- if a section continues onto another page, repeat the section header and mark it continued
- row output must be table-based with aligned columns
- do not print inline metric strings like `Need: 1 Packed: 0 Left: 1`
- locked capacity is 11 records per printed page for WEC list PDFs
- each printed page includes a small footer: `printed: page {n} + {date}`

Known conflict:

Browser print from iPhone is not the preferred final path. Use the Cloudflare PDF worker for reliable mobile PDF output.

Known local limitation:

The Cloudflare PDF worker cannot fetch `127.0.0.1`. Local print worker testing requires deployed/externally reachable URLs or a separate local PDF path.

Approved packing print row format:

```text
Header
Section header
NAME | NEEDED | [ ] | PACKED | [ ] | LEFT | [ ] | INITIAL
record row
notes row
```

Print row rules:

- each record prints two rows
- row one is 40% name and 60% meta columns
- row two is 100% notes
- all name columns align
- all meta columns align
- all inner meta columns align
- no Date column
- no inner pseudo boxes inside writable cells
- all meta columns align to the locked worksheet grid
- row heights are locked to support handwriting
- page spill is calculated before rendering at 11 records per page
- quantity values must be whole numbers, never decimals

## Known Potential Conflicts

### Source table versus worksheet table

`wec_pack_items` and `wec_packing_items` are both needed but serve different roles.

```text
wec_pack_items = source/template
wec_packing_items = derivative/current worksheet state
```

Do not rename or merge them.

### `wec_pack_lists` naming

`wec_pack_lists` now also supports home modules such as `purchase_onsite`, not only packing lists. The table name stays as-is. Record this in docs rather than creating a new table without owner approval.

### Static count fields versus dynamic counts

`horse_count` and `groom_count_final` can drift from actual wave membership. The app now has effective/sanity concepts and a reconcile/session-start path. Runners must not assume stored count fields are always current.

### Timezone date display

An Airtable date shown as `6/8/2026` displayed as `6/7/2026` previously when timezone handling was wrong. Dates must render in the intended local show/user timezone, not UTC-shifted.

### Live endpoint versus local endpoint

Local preview may point to:

```text
http://127.0.0.1:4331/wec-packing
```

Live embed points to:

```text
https://ringstatus.webflow.io/test/wec-packing
```

If a feature appears local but not live, check deployment before changing UI.

### CSS shared-root risk

The shared CSS root includes WEC and HPS. A small CSS tweak can alter another app. Avoid one-off overrides and preserve shared token geometry.

### `wec_places` and tags

Current source records may not have direct `wec_places` fields. The current working relationship comes through `wec_places.tack_grocery_items` back to `wec_pack_items`.

`wec_local_tags` was referenced earlier, but the current accessible table for tags is `wec_places_tags`. Confirm current Airtable schema before adding fields.

### Production asset pinning

The current embed references `@main`. Production should pin to a commit. Do not claim production is locked while assets still point to `main`.

### Direct Airtable writes from browser

Never write directly from browser to Airtable. Browser writes go through Webflow Cloud.

## Current Feature State

Implemented or partially implemented:

- WEC app shell
- Webflow embed file
- local preview file
- Webflow Cloud state route
- Webflow Cloud action route
- Webflow Cloud health route
- Webflow Cloud print route
- Webflow Cloud reconcile route
- wave header with deadline/days remaining
- overview/home module list
- tab groups from `wec_pack_lists.tabs`
- section list switcher
- section search
- item detail modal
- horse detail modal
- purchase onsite home module
- purchase onsite task/detail flow
- task/done event logging
- places/tags detail rows
- quantity add and add+1 controls
- inline edit title/packed/needed controls
- horse-specific item packed/not packed toggles
- print URL generation through PDF worker
- optimistic feedback for quantity and onsite task toggles
- session start and count-change event hooks

Not fully verified:

- all write actions against Airtable
- all event payload shapes
- deployed live Cloud endpoint after latest changes
- production embed with pinned commit
- iPhone PDF flow through worker
- return wave generation
- final decision conflict/SMS workflow
- full cross-media visual QA after recent changes

## Open Tasks

### Deployment and embed

- Deploy the latest `webflow-cloud-test` WEC Cloud route.
- Confirm live `/test/wec-packing/state` returns `homeModules`.
- Update Webflow embed after deploy.
- Pin CSS/JS assets to a Git commit for production.
- Keep local preview pointed to local endpoint only.

### Airtable write testing

Test each action:

- `session_start`
- `add_quantity`
- `set_pack_state`
- `set_resolution`
- `update_item_fields`
- `set_horse_pack_state`
- `set_source_flag`
- `set_onsite_task_state`

For each action, verify:

- current-state row changed
- event row was created
- UI updates immediately enough for user trust
- refresh loads the saved value
- failure rolls back or reports clearly

### Count and wave reconciliation

- Decide whether `horse_count` should remain static, formula, rollup, or derived server-side.
- Decide whether `groom_count_final` should be formula, manual override, or server-derived.
- Confirm `horse_sanity` and `groom_sanity` semantics.
- Confirm manual lock behavior for waves and horses.
- Confirm when `quantity_needed_dynamic` should override frozen `quantity_needed`.
- Use `/reconcile` to surface count and quantity drift.
- Append `wave_count_change` events when meaningful drift is detected.

### Return wave

- Define `return_wave`.
- Decide whether return wave starts from outbound needed quantities, packed quantities, or a new source-derived snapshot.
- Decide how to represent pack-up/go-home progress separately from outbound packing.
- Confirm print requirements for return wave.

### Purchase onsite

- Confirm all `wec_purchase_onsite` records have intended places.
- Populate local tags where useful.
- Confirm whether purchase onsite rows need place-type or maps/phone/website detail in modal.
- Confirm `TASK`/`DONE` language remains final.
- Confirm purchase onsite print layout.

### Places/local guide

- Decide how `wec_places`, `wec_places_tags`, and future place-type browsing will appear outside purchase onsite.
- Confirm whether places become a standalone home module later.
- Avoid mixing packing progress with local guide browsing unless owner approves.

### Print/PDF

- Finish Cloudflare PDF worker integration for every print button.
- Remove browser-window print behavior where it conflicts with mobile PDF expectations.
- Refine print row layout with scratch boxes.
- Add date printed.
- Ensure two-column 8.5 x 11 output.
- Ensure long horse-specific sections split cleanly.

### Visual QA

- Verify desktop and mobile widths.
- Verify list fills viewport when content is short.
- Verify tab grid/horizontal behavior.
- Verify detail modal padding matches HPS.
- Verify token geometry is consistent.
- Verify no overflow in inline edit rows.
- Verify all caps/zebra rows remain consistent.

### WEC app isolation

- Treat this app as WEC/RSWS only: production page `rsws`, optional short staging page `rsws2`, and print page `rsws_print`.
- Keep WEC runtime work out of `webflow/hps/hps.js`; HPS is a separate app and can change independently.
- Do not use HPS, LPS, or LP frontend files as the WEC implementation source. WEC frontend changes belong in `webflow/packing-worksheet/wec-packing.js`, WEC embed files, WEC print files, and WEC Webflow Cloud routes.
- Audit legacy shared selector scope before changing it. New WEC behavior should be scoped under `#packing-app` unless the owner explicitly approves a shared base CSS change.
- Treat any existing HPS, LPS, or LP diff as unrelated during WEC work unless the owner explicitly requests those app changes.

## Runner Handoff: Build A Similar Project With This Pattern

Use this sequence for a similar Webflow/Airtable live app.

### 1. Establish source and current-state layers

Create or identify:

```text
source lists table
source items table
plan/rules table
wave/scope table
current item snapshot table
current member snapshot table, if needed
event/history table
```

Do not store live progress directly on the source item table unless the project is explicitly single-use.

### 2. Add registry rows

Add each table to a registry equivalent to `wec_meta`.

Include:

```text
table_name
table_api
AIRTABLE__TABLE
AIRTABLE__VIEW
fields_allowed
ignore
const_env
notes
```

The runner should use registry metadata and live Airtable metadata, not guessed field names.

### 3. Build server boundary first

Create Webflow Cloud routes:

```text
/state
/action
/health
/print
/reconcile
```

Do not build the UI until `/health` and `/state` prove the table/view contract.

### 4. Normalize server response

Return a frontend-friendly shape:

```text
ok
source
wave/scope
lists
tabGroups
homeModules
items
horses
counts
```

Do not force the frontend to understand raw Airtable field quirks.

### 5. Preserve brand shell

Start the frontend from the existing Webflow embed pattern:

```text
root div
window.CONFIG
font link
shared CSS
deferred JS
```

Use the shared class cadence. Do not create a new design system.

### 6. Add writes as actions

Each action should:

```text
validate payload
load current record
patch current-state record
append event row
return refreshed state
```

The browser may optimistically update, but Airtable remains source of truth.

### 7. Add print through a server/PDF path

Do not rely on mobile browser print for production. Generate printable HTML server-side and pass it to the PDF worker when the user taps print.

### 8. Verify in this order

```text
node --check frontend JS
node --check cloud lib
npm run build in webflow-cloud-test
GET /health
GET /state
local preview in browser
one low-risk write action
event row proof
refresh proof
print proof
deployed live endpoint proof
Webflow embed proof
```

## Working Commands

Syntax checks:

```powershell
node --check "webflow/packing-worksheet/wec-packing.js"
node --check "webflow-cloud-test/src/lib/wec-packing.js"
```

Build:

```powershell
Set-Location "C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus\webflow-cloud-test"
npm run build
```

Local preview URL:

```text
http://127.0.0.1:8792/wec-packing-webflow-preview.html
```

Local Cloud endpoint when running:

```text
http://127.0.0.1:4331/wec-packing/state?packWaveKey=wave_one
```

Live state endpoint:

```text
https://ringstatus.webflow.io/test/wec-packing/state?packWaveKey=wave_one
```

## Stop Conditions For Future Runners

Stop and report rather than guessing if:

- a required table is missing from `wec_meta`
- a required field is missing from live Airtable metadata
- the live endpoint differs from local endpoint and deployment status is unknown
- a requested UI change would create a second visual system
- the change requires altering shared `lp-*`, `th-*`, or `packing-*` behavior globally
- a write action would update current state without event history
- the requested behavior depends on `return_wave` before that contract is approved
- the user says stop or asks to discuss before editing

## Current Verification Notes

Recently verified:

- local state endpoint returns purchase onsite home module
- purchase onsite renders in Wave One local preview
- purchase onsite task detail shows `Places`
- `Farmvet` appears for tested purchase tasks
- local tags are wired but did not render for the tested record because the place had no tag values
- `node --check` passed for frontend and Cloud library
- `npm run build` passed for `webflow-cloud-test`

Still needs live verification:

- deployed Webflow Cloud endpoint with latest code
- live Webflow embed after deployment
- production CDN commit pin
- full Airtable write and event proof
- mobile PDF path through Cloudflare worker
