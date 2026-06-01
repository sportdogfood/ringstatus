# WEC Packing Current Project Overview

Last updated: 2026-05-31

This is the current operating overview for the WEC packing dashboard. It documents the exact embed, source files, integration boundaries, open tasks, and known troubleshooting issues after the RSA/Webflow styling lock and the Wave One/Horses filter changes.

## Current Status

The WEC packing dashboard is a mobile-first Webflow embed that renders live Airtable-backed packing lists, horse lists, approved lists, comments, detail modals, inline quantity changes, and print links.

The app is not local-only state. Airtable is both the data source and the write destination. The browser can optimistically update and can hold failed saves locally, but saved truth must round-trip through Webflow Cloud to Airtable.

Current live asset pin:

```text
asset commit: 15d27bc
embed commit: local embed file points to 15d27bc assets
```

Current Webflow pages:

```text
rswp        production page
rswp2       short staging/safety page when needed
rswp-print  dedicated print page embed
```

Current important behavior:

- Wave One shows an always-open list mode menu above the section block: `PACKING`, `APPROVED`, `SEARCH`.
- Horses shows an always-open horse filter menu above the section block: `ALL`, `WAVE 1`, `WAVE 2`, `NOT GOING`.
- Those menus are not controlled by `FILTER` and are not closeable.
- Regular packing sections still use the section-level `SEARCH`, `FILTER`, and `PRINT` controls.
- Print endpoint is live and returned `200` for `https://ringstatus.com/test/wec-packing/print?packWaveKey=wave_one&target=overview`.
- Failed browser writes are queued in device storage and can retry/export/email instead of silently disappearing.

## Paste-Ready Webflow Embed

Use this full embed. It includes the locked RSA/Webflow CSS file copied from the approved template, followed by the WEC packing override CSS and JS.

```html
<div id="packing-app">Loading WEC packing...</div>

<script>
  window.WEC_PACKING_CONFIG = {
    mode: "edit",
    apiUrl: "https://ringstatus.com/test/wec-packing",
    stateUrl: "https://ringstatus.com/test/wec-packing/state",
    actionUrl: "https://ringstatus.com/test/wec-packing/action",
    healthUrl: "https://ringstatus.com/test/wec-packing/health",
    printUrl: "https://ringstatus.com/test/wec-packing/print",
    printPageUrl: "https://ringstatus.com/rswp-print",
    pdfWorkerUrl: "https://ringstatus-pdf.gombcg.workers.dev/",
    showId: "",
    packWaveId: "",
    packWaveKey: "wave_one",
    enableHorseNotNeeded: false
  };
</script>

<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Outfit:wght@400;500;600;700&display=swap" rel="stylesheet">

<link rel="stylesheet" href="https://cdn.jsdelivr.net/gh/sportdogfood/ringstatus@15d27bc/webflow/packing-worksheet/rsa-stylesheets.locked.css?v=wec-20260601-15d27bc">
<link rel="stylesheet" href="https://cdn.jsdelivr.net/gh/sportdogfood/ringstatus@15d27bc/webflow/packing-worksheet/styles.css?v=wec-20260601-15d27bc">
<script src="https://cdn.jsdelivr.net/gh/sportdogfood/ringstatus@15d27bc/webflow/packing-worksheet/wec-packing.js?v=wec-20260601-15d27bc" defer></script>
```

Repo file:

```text
webflow/packing-worksheet/wec-packing-webflow-embed.html
```

Do not open the embed fragment as the only visual proof unless it includes the locked base CSS. The embed is intended for Webflow, but it now carries the locked base CSS so direct file review is less misleading.

## Source Files

Frontend:

```text
webflow/packing-worksheet/rsa-stylesheets.locked.css
webflow/packing-worksheet/styles.css
webflow/packing-worksheet/wec-packing.js
webflow/packing-worksheet/wec-packing-webflow-preview.html
webflow/packing-worksheet/wec-packing-webflow-embed.html
```

Locked RSA/Webflow CSS source:

```text
C:\Users\gombc\Documents\Codex\2026-05-19\review-these-and-we-will-start\wec-layout-prototype\css\rsa-stylesheets.webflow.css
```

Locked CSS SHA-256:

```text
FA5A6FA5747257D287D793BC4271856E3907DCAEF9E0F19AE10FADA86079FA07
```

Webflow Cloud routes:

```text
webflow-cloud-test/src/lib/wec-packing.js
webflow-cloud-test/src/pages/wec-packing/index.js
webflow-cloud-test/src/pages/wec-packing/state.js
webflow-cloud-test/src/pages/wec-packing/action.js
webflow-cloud-test/src/pages/wec-packing/health.js
webflow-cloud-test/src/pages/wec-packing/print.js
webflow-cloud-test/src/pages/wec-packing/reconcile.js
```

## Architecture

Read path:

```text
Webflow embed
  -> WEC_PACKING_CONFIG.stateUrl
  -> /test/wec-packing/state
  -> webflow-cloud-test/src/lib/wec-packing.js stateReport()
  -> Airtable registry/table reads
  -> normalized wave/lists/items/horses/comments/homeModules
  -> browser render
```

Write path:

```text
User action in browser
  -> postAction(payload)
  -> WEC_PACKING_CONFIG.actionUrl
  -> /test/wec-packing/action
  -> actionReport()
  -> validate action
  -> patch current Airtable record
  -> append event/comment record when applicable
  -> return refreshed state
  -> browser render
```

Print path:

```text
Print button
  -> printUrl with packWaveKey/target/horseId
  -> /test/wec-packing/print
  -> server-rendered printable HTML
  -> PDF worker for non-local URLs
```

Browser assets must not contain Airtable credentials. All Airtable reads and writes go through Webflow Cloud.

## Airtable Tables

Required logical tables from the current server library:

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

Optional/support tables:

```text
wec_list_plans
wec_places
wec_places_tags
wec_commenting
```

Important current table roles:

- `wec_pack_lists`: source list definitions, packing tabs, home/approved modules.
- `wec_pack_items`: source item catalog and approved-list source records. It may show reference rollups in Airtable, but it is not the app's stored current-need authority.
- `wec_pack_waves`: active pack wave, show/wave deadline, count context, and groom ratio. Stale `horse_count` must not drive live behavior unless the wave is explicitly manually locked.
- `wec_weeks`: owner-approved count calculations stay here where already established.
- `wec_horses`: roster source for Wave 1, Wave 2, Not Going filters, and linked generic horse-specific kit items.
- `wec_packing_items`: current packed/progress state for normal packing items and decisions. It must not be trusted as the live `NEED` authority.
- `wec_packing_item_horses`: touched packed/not-packed state for horse-specific kit rows. A missing row means not packed.
- `wec_packing_events`: debit/credit event history for quantity, decision, horse-kit, task, edit, and session events.
- `wec_commenting`: current comment records for item/section/tab/wave scopes. Replies are not wired unless a later explicit parent-comment field is approved.

## Locked Live Logic As Of 2026-06-01

The app is already in live use. Do not clear/reset live quantities, delete rows, delete comments, or delete events without an explicit scope approval in the same session.

Live state is calculated from current sources:

```text
wec_pack_waves.count_horses_wave_one for Wave One horse count
+ wec_pack_waves.groom_sanity for groom count
+ wec_horses wave flags for roster/list membership
+ wec_pack_items source item plan/base values
+ current packed/progress rows
= rendered app state
```

`horse_count` and `horse_sanity` on `wec_pack_waves` are stale reference data and must not drive live Wave One behavior unless a future manual-lock rule is explicitly approved. For unlocked Wave One math, the dynamic horse count is `wec_pack_waves.count_horses_wave_one`.

Wave One horse membership is still defined as:

```text
wave_one = wec_horses.wec_wave_1 = true and wec_not_going != true
```

Wave Two can later use the equivalent Wave Two count/embed, but it is not the current focus.

Groom count uses `wec_pack_waves.groom_sanity` directly. Do not recalculate this from `groom_ratio` in the app.

```text
groom_count = wec_pack_waves.groom_sanity
```

### Plan Semantics

`quantity`

- Dynamic `NEED` comes from the source item quantity/base.
- `PACKED` is current packed progress on `wec_packing_items`.

`per_horse`

- Count math only.
- `NEED = source per_horse * current going horse count`.
- No named horse-kit rows.

`per_groom`

- Count math only.
- `NEED = source per_groom * current groom count`.
- Current groom count derives from `wec_pack_waves` ratio/manual fields.

`horse_specific`

- Dynamic checklist rollup only, not quantity math.
- Each going horse has generic linked `wec_pack_items` kit items.
- Each expected horse + source item pair is one kit row.
- `NEED = expected horse-kit row count`.
- `PACKED = packed horse-kit row count`.
- `LEFT = NEED - PACKED`.
- The source item name can carry quantity detail, such as `Bridle (2)`. The app does not treat that as a partial quantity input.
- Missing `wec_packing_item_horses` row means `not_packed`.
- On first packed tap, the app may create a `wec_packing_item_horses` row for that horse/item/wave.
- Removing a horse from a wave recalibrates current rollups down by one for that horse's linked kit items. Existing rows/events/comments remain preserved and are excluded from current rollups if no longer current.

### Decisions

`CLEAR`

- Clears packed progress.
- Sets packed count/state to zero/not packed.
- Logs a negative quantity delta when packed progress existed.

`MAX`

- Sets packed progress to current dynamic `NEED`.
- Logs the positive delta from current packed to current need.

`BUY`

- Moves the item to Purchase Onsite.
- Clears packed progress if any.
- Logs a negative quantity delta only when packed progress was cleared.

`ATTN`

- Moves the item to Needs Attention.
- Does not clear packed progress by default.
- Logs a decision change with zero quantity delta unless a separate clear is used.

### Edit And Comment Logging

- Inline quantity input writes packed progress and should log the debit/credit delta.
- Inline item label edit currently patches `wec_packing_items`; it must add an `item_edit` event before this is considered fully audited.
- Comment add writes to `wec_commenting` when configured, with fallback to `wec_packing_events` only if `wec_commenting` is unavailable.
- Comment edit currently updates the same `wec_commenting` row and marks `comment_status = edited`; it does not yet create a separate audit event.
- Comment replies are not a current feature. If added, replies should be new `wec_commenting` rows with `event_type = comment_reply` and an explicit parent field.

## Current Write Actions

The action endpoint currently recognizes:

```text
session_start
add_quantity
set_pack_state
set_resolution
update_item_fields
set_horse_pack_state
set_horse_kit_state
set_horse_record_state
set_source_flag
set_onsite_task_state
add_comment
update_comment
```

Expected write behavior:

- Quantity adds patch `wec_packing_items.quantity_packed` and update `pack_state`.
- Pack state changes patch `wec_packing_items` and create event history.
- Decision changes patch `wec_packing_items.resolution_state` and create event history.
- Inline item edits patch only allowed fields on `wec_packing_items`.
- Horse-specific packed state uses dynamic expected horse-kit rows. Missing `wec_packing_item_horses` rows mean not packed; packed taps create/update the touched state row and create event history.
- Horse record state patches `wec_horses.record_state`.
- Source flags patch allowed fields on `wec_pack_items`.
- Onsite task state creates an event against the source item from `wec_purchase_onsite`.
- Comment add/edit writes to `wec_commenting` when configured, otherwise comment add falls back to `wec_packing_events`.

Failed writes:

- Stored locally under `wecPackingFailedActions:v1`.
- Retried automatically when state loads and when browser returns online.
- Footer exposes retry/export/email only when failures exist.
- Export creates a JSON file with action payloads so details are not lost.

## Current UI Contract

Do not replace this with a new layout system.

Locked shell pattern:

```text
rsa-dashboard
  rsa-dashboard-block
    rsa-dashboard-container
      rsa-main-grid
        rsa-top
        rsa-actions
        rsa-body
        rsa-bottom
```

Row pattern:

```text
rsa-padding
rsa-item-row-2 is-grid2
rsa-item-block-left
rsa-item-block-right
```

Typography defaults:

```text
Outfit
rsa-H1
rsa-H2
rsa-H5
rsa-p
rsa-text
rsa-text is-xs
rsa-text is-xxs
rsa-text is-link
rsa-text is-line-item
rsa-text is-inline-edit
rsa-text is-number
rsa-text is-inline-input is-link
is-caps
```

Current special view rules:

- Main top section tabs keep their category colors.
- Body/list filter active state is scoped separately.
- Wave One and Horses list menus are always open, above the section body, and not closeable.
- Regular list filters still open through `FILTER`.
- Empty body-level `rsa-actions` renders as `rsa-actions is-hidden` to preserve the skeleton.
- Search rows now carry `is-grid2 is-search-grid` so they inherit the locked left/right grid scaling.

## Open Tasks

High priority:

- Verify the pasted Webflow embed after replacing the Webflow custom code.
- Verify direct phone behavior for quantity add and failed-save queue.
- Run one real Airtable write proof for `add_quantity`: before record, action response, Airtable after record, event evidence, refresh evidence.
- Run one real Airtable write proof for `set_horse_kit_state`: virtual/missing row before, action response, created/updated `wec_packing_item_horses`, event evidence, refresh evidence.
- Run one real Airtable comment proof for `add_comment` and `update_comment` in `wec_commenting`.
- Confirm print buttons from the pasted Webflow page open PDF/print output on phone.
- Confirm the PDF worker result is usable on iPhone.
- Add audit event for inline item label edit (`item_edit`) if label edits need owner-visible audit.
- Decide whether comment edits need a separate audit event beyond `comment_status = edited`.

Data/model:

- Confirm whether list groups should derive fully from `wec_pack_lists` active/list views.
- Confirm final `wec_pack_lists` view for editable/active list management, including `wec_wave_lists` if that remains the intended view.
- Confirm `Purchase Onsite` and `Needs Attention` remain approved lists and not packing lists.
- Confirm Search remains a synthetic all-items actionable view, not an Airtable list record.
- Keep `Needs Attention` driven by decision state unless a dedicated module is explicitly approved.

Horse/Wave:

- Keep existing `wec_weeks` count calculations as reference/accounting only where already established.
- Use `wec_horses` Wave 1 / Wave 2 / Not Going fields for live wave horse counts and roster filters.
- Replace horse-specific quantity math with dynamic horse-kit rollups.
- Do not bulk-generate horse-specific state rows. Create `wec_packing_item_horses` only when a user touches a horse kit state.
- Preserve existing horse-kit rows, comments, and events. Exclude stale rows from current rollups rather than deleting them.
- Define return-wave behavior separately before implementing.

Styling:

- Review the pasted Webflow page at mobile width and max width after the locked CSS embed.
- Confirm search row, table head, line item rows, and comments are still on the locked `is-grid2` scale.
- Make `rsa-table-head` sticky at the shell top, not inside an internal app-scroll container.
- Redesign locale/place modal cards using the approved modal pattern and existing RSA class contract.
- Remove any remaining style rules that are acting as shims instead of combos on locked classes.

Class system guardrail:

- Reuse the approved stacked block model: `rsa-padding`, `rsa-item-row-2`, `rsa-item-block-left`, `rsa-item-block-right`, `rs-quantity-block`, `rsa-table-label`, `rs-tab-link`, `rs-text-link`, and `rsa-comment-panel`.
- Typography stays on approved text classes such as `rsa-H1`, `rsa-H5`, `rsa-p`, `rsa-text`, `is-xs`, `is-xxs`, `is-line-item`, `is-number`, `is-inline-edit`, and `is-inline-input`.
- Combo classes such as `is-active`, `is-open`, `is-hidden`, `is-caps`, `is-grid2`, `is-flex-h`, `is-modal`, and `is-block` may only make scoped adjustments to the base classes.
- Do not add empty grid cells to satisfy an old layout. If a row only needs an action such as `OPEN`, use `rs-quantity-block is-flex-h`.
- Modal content uses the same stacked row/block primitives as the main page; use `is-modal` or `is-block` rather than creating a new table/grid model.

WEC app isolation:

- Treat this app as WEC/packing only: Webflow pages are `rswp`, optional short staging is `rswp2`, and print is `rswp-print`.
- Keep WEC runtime work out of `webflow/hps/hps.js`; HPS is a separate app and may evolve independently.
- Do not use HPS, LPS, or LP frontend code as the WEC implementation source. WEC changes belong in `webflow/packing-worksheet/wec-packing.js`, WEC embed files, WEC print files, and WEC Webflow Cloud routes only.
- Audit legacy shared selectors such as `:is(#packing-app, #tack-horses-app, #hps-app)` before changing them. New WEC-specific behavior should be scoped to `#packing-app` unless an explicitly approved shared base rule is being changed.
- Treat any HPS, LPS, or LP file diff during WEC work as unrelated unless the owner explicitly asks for that app in the same turn.

Print:

- Validate overview, tab, section, approved-list, and horse print targets.
- Confirm whether print should go direct to printable HTML or through PDF worker for every non-local click.
- Add date printed if still desired.

## Known Troubleshooting

### Direct file looks wrong

Cause: opening an embed fragment as `file://` can miss the Webflow base CSS if the embed does not carry it.

Current mitigation: `wec-packing-webflow-embed.html` now includes `rsa-stylesheets.locked.css` before `styles.css`.

### jsDelivr returns 404 for a new pinned commit

Cause: new commit has not reached jsDelivr or stale 404 is cached.

Embed build rule:

- Webflow owns the root element custom attribute.
- Main page root: `#packing-app` with `data-rs-build="COMMIT_SHA"`.
- Print page root: `#wec-packing-print` with `data-rs-build="COMMIT_SHA"`.
- The embed script must read `root.dataset.rsBuild`; do not hardcode a fallback commit in JS.
- If `data-rs-build` is missing, the embed should fail visibly instead of loading stale assets.

Check:

```powershell
Invoke-WebRequest -Method Head "https://cdn.jsdelivr.net/gh/sportdogfood/ringstatus@COMMIT/webflow/packing-worksheet/wec-packing.js"
```

Purge:

```text
https://purge.jsdelivr.net/gh/sportdogfood/ringstatus@COMMIT/webflow/packing-worksheet/wec-packing.js
```

Do not paste a new embed until all pinned CDN asset URLs return `200`.

### Print button appears dead

Likely causes:

- JS asset URL is 404 or stale.
- Browser popup is blocked.
- The PDF worker cannot fetch the print URL.
- The page is opened from a stale fragment after repinning.

Checks:

```powershell
Invoke-WebRequest "https://ringstatus.com/test/wec-packing/print?packWaveKey=wave_one&target=overview"
```

Expected: HTTP `200` and printable HTML.

### Save failed / load failed

Likely causes:

- Webflow Cloud route not deployed.
- Airtable env vars missing.
- Airtable table registry/field mismatch.
- Network drop on mobile.

Current client behavior:

- Failed writes are queued on device.
- The UI must not look saved if Airtable did not accept the write.
- Retry/export/email appears when pending failures exist.

### Live page differs from local preview

Likely causes:

- Webflow embed still points to an old asset commit.
- Webflow Cloud has not deployed the backend route changes.
- jsDelivr cached a stale file.
- User is viewing a cached Webflow page.

Verification order:

1. Check embed file commit and pasted Webflow code.
2. Check CDN asset URLs return `200`.
3. Check `/test/wec-packing/state?packWaveKey=wave_one`.
4. Check `/test/wec-packing/print?packWaveKey=wave_one&target=overview`.
5. Check browser console errors.

## Verification Commands

Frontend syntax:

```powershell
node --check webflow\packing-worksheet\wec-packing.js
```

Cloud library syntax:

```powershell
node --check webflow-cloud-test\src\lib\wec-packing.js
```

Local preview:

```text
http://127.0.0.1:8792/wec-packing-webflow-preview.html
```

Local state:

```text
http://127.0.0.1:4331/wec-packing/state?packWaveKey=wave_one
```

Live state:

```text
https://ringstatus.com/test/wec-packing/state?packWaveKey=wave_one
```

Live print:

```text
https://ringstatus.com/test/wec-packing/print?packWaveKey=wave_one&target=overview
```

## Stop Conditions

Stop and confirm before:

- changing Airtable field/table names
- adding direct browser-to-Airtable writes
- replacing the RSA/Webflow skeleton
- adding a new design system
- adding a new modal pattern
- making return-wave assumptions
- changing `wec_weeks` count formulas
- treating `Purchase Onsite` or `Needs Attention` as normal packing lists without approval
- hiding failed writes from the user
