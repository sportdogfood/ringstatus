# WEC Packing Current Codex Handoff

Date: 2026-05-31

This is a current-state handoff for the next Codex agent. It is not a new design contract. The purpose is to preserve what is known, show the real connections, identify where the current implementation is failing, and prevent another agent from inventing UI, CSS, markup, or data shapes.

## Immediate Operating Rule

The user has supplied CSS and markup for the RSA/Webflow template. That source is the authority.

Do not infer styling from screenshots. Browser screenshots and comments are only evidence of what is currently wrong or where an element is located. If a screenshot conflicts with supplied CSS/markup, use the supplied CSS/markup.

Do not create new visual patterns, utility shims, grids, button styles, modal shells, or responsive behavior unless the user explicitly asks for that exact change.

When touching UI:

- change the shared class once, not one screen at a time
- verify desktop, tablet, and mobile widths
- compare computed styles against the supplied Webflow CSS
- treat same class as same styling everywhere
- do not make a special case for a class unless the user explicitly gives a combo class for that variation

## Repo And Current Paths

Main repo:

```text
C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus
```

WEC packing frontend:

```text
webflow/packing-worksheet/styles.css
webflow/packing-worksheet/wec-packing.js
webflow/packing-worksheet/wec-packing-webflow-preview.html
webflow/packing-worksheet/wec-packing-webflow-embed.html
```

Webflow Cloud/Astro server:

```text
webflow-cloud-test/src/lib/wec-packing.js
webflow-cloud-test/src/pages/wec-packing/index.js
webflow-cloud-test/src/pages/wec-packing/state.js
webflow-cloud-test/src/pages/wec-packing/action.js
webflow-cloud-test/src/pages/wec-packing/health.js
webflow-cloud-test/src/pages/wec-packing/print.js
webflow-cloud-test/src/pages/wec-packing/reconcile.js
```

Print tests:

```text
webflow-cloud-test/test/wec-packing-print-contract.test.js
```

Current user-built RSA/Webflow template source:

```text
C:\Users\gombc\Documents\Codex\2026-05-19\review-these-and-we-will-start\wec-layout-prototype\webflow-template-editable.html
C:\Users\gombc\Documents\Codex\2026-05-19\review-these-and-we-will-start\wec-layout-prototype\css\rsa-stylesheets.webflow.css
```

Older downloaded Webflow export source:

```text
C:\Users\gombc\Downloads\rsa-stylesheets.webflow\index.html
C:\Users\gombc\Downloads\rsa-stylesheets.webflow\css\rsa-stylesheets.webflow.css
C:\Users\gombc\Downloads\rsa-stylesheets.webflow\css\webflow.css
C:\Users\gombc\Downloads\rsa-stylesheets.webflow\css\normalize.css
```

Existing project docs:

```text
docs/wec_packing_project_overview_handoff.md
docs/wec_packing_live_app_project_contract.md
docs/wec_packing_current_state_tables_audit.md
docs/wec_packing_execution_checklist.md
docs/wec_packing_gate1_registry_report.md
```

## Current Browser And Local Connections

Current local preview URL:

```text
http://127.0.0.1:8792/wec-packing-webflow-preview.html
```

The local preview config in `webflow/packing-worksheet/wec-packing-webflow-preview.html` points to local Webflow Cloud routes:

```text
apiUrl:    http://127.0.0.1:4331/wec-packing
stateUrl:  http://127.0.0.1:4331/wec-packing/state
actionUrl: http://127.0.0.1:4331/wec-packing/action
healthUrl: http://127.0.0.1:4331/wec-packing/health
printUrl:  http://127.0.0.1:4331/wec-packing/print
```

The Webflow embed file currently points to live Webflow Cloud routes:

```text
apiUrl:    https://ringstatus.webflow.io/test/wec-packing
stateUrl:  https://ringstatus.webflow.io/test/wec-packing/state
actionUrl: https://ringstatus.webflow.io/test/wec-packing/action
healthUrl: https://ringstatus.webflow.io/test/wec-packing/health
printUrl:  https://ringstatus.com/test/wec-packing/print
pdfWorkerUrl: https://ringstatus-pdf.gombcg.workers.dev/
packWaveKey: wave_one
```

Current embed asset pins:

```text
https://cdn.jsdelivr.net/gh/sportdogfood/ringstatus@e23cfd58f4d136cca0539cfc448a98ed0b13fdd8/webflow/packing-worksheet/styles.css?v=wec-20260530-0230
https://cdn.jsdelivr.net/gh/sportdogfood/ringstatus@e23cfd58f4d136cca0539cfc448a98ed0b13fdd8/webflow/packing-worksheet/wec-packing.js?v=wec-20260530-0230
```

If local preview changes but Webflow does not, check both:

- whether the embed is pinned to an older commit
- whether Webflow Cloud was deployed after server route changes

## High-Level Architecture

The app follows this path:

```text
Webflow page container
  -> WEC_PACKING_CONFIG
  -> jsDelivr CSS/JS assets
  -> browser frontend wec-packing.js
  -> Webflow Cloud/Astro routes
  -> Airtable
  -> normalized state response
  -> frontend render
  -> action route mutations
  -> Airtable event history
  -> refreshed state / optimistic UI feedback
```

The browser must never contain Airtable credentials. Airtable access is only through Webflow Cloud.

## Airtable / Data Connections

`webflow-cloud-test/src/lib/wec-packing.js` defines the required tables:

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

Optional tables:

```text
wec_list_plans
wec_places
wec_places_tags
```

Env-backed tables:

```text
wec_list_plans:
  AIRTABLE_WEC_LIST_PLANS_TABLE
  AIRTABLE_WEC_LIST_PLANS_VIEW

wec_pack_waves:
  AIRTABLE_WEC_PACK_WAVES_TABLE
  AIRTABLE_WEC_PACK_WAVES_VIEW

wec_packing_items:
  AIRTABLE_WEC_PACKING_ITEMS_TABLE
  AIRTABLE_WEC_PACKING_ITEMS_VIEW

wec_packing_item_horses:
  AIRTABLE_WEC_PACKING_ITEM_HORSES_TABLE
  AIRTABLE_WEC_PACKING_ITEM_HORSES_VIEW

wec_packing_events:
  AIRTABLE_WEC_PACKING_EVENTS_TABLE
  AIRTABLE_WEC_PACKING_EVENTS_VIEW
```

Registry source:

```text
wec_meta table id: tbllJywsOstkqT5yZ
```

The user added registry/meta fields such as:

```text
const_env
ignore
AIRTABLE__TABLE
AIRTABLE__VIEW
fields_allowed
```

These exist to support Webflow Cloud env setup and minimum field checks. Do not ignore `wec_meta`.

## Nested Data Model

The app state returned by `stateReport` includes these nested levels:

```text
state
  source
    showId
    packWaveId
    packWaveKey
    tables
  wave
    id
    key / wave
    deadlineDate
    daysTill
    horseCount
    groomRatio
    groomCountFinal
    groomCountManual
  availableWaves[]
  horses[]
  lists[]
  tabGroups[]
  homeModules[]
  sections[]
  counts
```

A normal packing tab nests like this:

```text
wave
  tabGroup, e.g. Show / Barn / Tack
    list switcher, e.g. Blankets / Show Legs
      list
        rows/items
          source pack item
          worksheet packing item
          list plan
          places
          place tags
          horse-specific members, if applicable
          events/history
```

Overview nests like this:

```text
wave_one overview
  home modules from wec_pack_lists view wec_home
    purchase_onsite module
      source items from wec_pack_items view wec_purchase_onsite
        task state derived from wec_packing_events
  tab group summaries
    Barn
    Show
    Tack
    Grooming
    Health
    Feed
    Horses
```

Horse workflow nests like this:

```text
wave
  horses in this wave
    horse row
      horse-specific assigned item rows
        wec_packing_item_horses row
        parent wec_packing_items row
        source wec_pack_items row
        per-horse packed status
        events/history
```

Purchase onsite nests like this:

```text
wec_pack_lists view wec_home
  home module/list
    wec_pack_items view wec_purchase_onsite
      item
        source pack list links
        wec_places
        wec_local_tags_rollups / wec_places_tags
        task/done state from events
```

Print targets nest like this:

```text
target=overview
  print each tab/list report

target=tab:barn or tab:show etc.
  print all lists in that tab group

target=<listId>
  print one list

target=home:<moduleId>
  print home module, e.g. purchase onsite task list

target=horses
  print horse summary

horseId=<id>
  print one horse packing list
```

## Source vs Derivative Tables

The user explicitly clarified:

```text
wec_pack_lists and wec_pack_items are source.
wec_packing_items and wec_packing_item_horses are derivatives.
```

Do not invent worksheet fields that duplicate source detail under different names.

`wec_packing_items` can be created dynamically from source data and wave data.

`wec_packing_item_horses` should only be created on a horse lock. It is less dynamic by design because horse-specific packing can be manually adjusted if a horse cancels.

## Quantity Logic And Sanity

The user is concerned that static `horse_count` is not safe. Horse count should be derived from wave membership when possible. The user added:

```text
horse_sanity
groom_sanity
last_run_notes
```

The wave fields discussed:

```text
deadline_date
days_till
groom_count_manual
groom_ratio
horse_count
groom_count_final
```

Expected meaning:

- `groom_count_manual` is an override.
- `groom_ratio` should support calculating groom needs from horse count.
- `groom_count_final` should be the final formula/result used by quantity logic.
- If horse count or groom count changes, create an event in `wec_packing_events` with details.
- It is useful to log a session created event when a new interaction/session starts.

Quantities should never render decimals in the UI. Use rounded/integer display consistently.

## RSA Template / UI Structure

The current direction is to use the user-built RSA/Webflow template, not the earlier large tab/card layout.

Top-level visual structure:

```text
rsa-page
  rsa-banner / head area
  rsa-list-action-menu packing-section-tabs
  rsa-list-action-menu is-switcher
  rsa-lists-module
    table-module
      rsa-list-header
      rsa-messages is-search
      rsa-list-action-menu filters
      rsa-tables
        rsa-table
          rsa-item-row is-grid2 is-label
          rsa-list-padding
            rsa-item-row is-grid2
```

Header/action markup currently generated by `wec-packing.js`:

```html
<div class="rsa-list-header">
  <div class="rsa-item-row is-grid2">
    <div class="rsa-item-block-left">
      <div class="rsa-item-text">
        <h4 class="rsa-head">...</h4>
        <div class="rs-text-linline">edit</div>
      </div>
    </div>
    <div class="rsa-item-block-right">
      <div class="rsa-action-block is-grid3">
        <div class="rs-text-link">search</div>
        <div class="rs-text-link">filter</div>
        <div class="rs-text-link is-print">print</div>
      </div>
    </div>
  </div>
</div>
```

Item row target markup:

```html
<div class="rsa-item-row is-grid2">
  <div class="rsa-item-block-left">
    <div class="rsa-item-text">
      <div class="indication-color bg-primary-green"></div>
      <div class="rs-table-title">BARN BANNER</div>
      <div class="rs-text-linline">edit</div>
    </div>
  </div>
  <div class="rsa-item-block-right">
    <div class="rs-quantity-block is-grid4">
      <div class="rs-text">10</div>
      <div class="rs-text">10</div>
      <div class="rs-text">10</div>
      <div class="rs-input-inline">input</div>
    </div>
  </div>
</div>
```

Label row target markup:

```html
<div class="rsa-item-row is-grid2 is-label">
  <div class="rsa-item-block-left">
    <div class="rsa-item-text">
      <div class="rs-table-title is-label">ITEM</div>
    </div>
  </div>
  <div class="rsa-item-block-right">
    <div class="rs-quantity-block is-grid4 is-label">
      <div class="rs-text is-label">NEED</div>
      <div class="rs-text is-label">PACKED</div>
      <div class="rs-text is-label">LEFT</div>
      <div class="rs-input-spacer is-label">INPUT</div>
    </div>
  </div>
</div>
```

Important: `w-layout-grid` and old Webflow component classes should not be reintroduced into generated app markup unless the user explicitly asks for them. The current direction was to strip legacy component CSS bleed and rely on the RSA classes.

## Known UX / Design Failures

These are the failures that caused the current breakdown and must be treated as active risk:

1. The generated app does not consistently match the user-supplied RSA/Webflow CSS and markup.

2. The same class has been allowed to render differently in different places. This violates the user's rule. Same class must mean same style.

3. The header/action row has drifted repeatedly:
   - `rsa-list-header`
   - `rsa-action-block is-grid3`
   - `rs-text-link`
   - `rs-text-link is-print`
   - `rs-text-linline`

4. The print button styling is still a sensitive failure point. The source CSS says `rs-text-link is-print` should inherit the base `rs-text-link` behavior and only add the approved print shade/background. Do not guess dimensions, underline behavior, or width from a screenshot.

5. The switcher buttons are a sensitive failure point. The user specifically objected when `rs-tab-link is-switcher` looked different from other `rs-tab-link` buttons. Only use a switcher combo when the user explicitly supplies it. Do not add hidden style differences.

6. The grids are not shrinking correctly. The row layout must respect:
   - `rsa-item-row is-grid2`
   - left item block
   - right quantity/action block
   - `rs-quantity-block is-grid4`
   - media behavior across desktop, tablet, and mobile

7. Sticky behavior has been partially wrong. The user wants `rsa-list-header` to stick to the viewport while list items scroll under it. This likely requires checking containing overflow rules on:
   - `rsa-lists-module`
   - `table-module`
   - `rsa-tables`
   - parent wrappers

8. The modal shell was reinvented at least once. The approved modal look is the HPS-style/WEC detail modal:
   - large H1 title
   - subtitle breadcrumb
   - large circular close button
   - same detail field grid
   - status row
   - need/packed/left row
   - quantity row with add/add+1
   - decision row
   - plan line near footer
   - save metadata footer

9. The browser comments/screenshots were treated as specs. They are not specs. They are evidence. The CSS and markup are the specs.

10. Some UI work was changed without verifying all media widths. This cannot continue. Every CSS change must be checked at least at:
    - desktop
    - approximately 832px
    - approximately 546px
    - mobile under 479px

11. The earlier `lp-*` layout and newer `rsa-*` layout are both present in the codebase. A new runner must avoid creating a third hybrid layer. Either keep the approved HPS/LP modal where explicitly approved, or use the approved RSA list/table template where currently directed.

## Current Sensitive CSS Area

The current WEC CSS has RSA-related rules near the lower section of `webflow/packing-worksheet/styles.css`, including:

```text
rs-tab-link is-section-tab
rs-tab-link is-switcher
rsa-list-action-menu
rsa-item-row
rsa-item-row is-grid2
rsa-item-text
rs-quantity-block is-grid4
rs-text-linline
rs-input-inline
rsa-action-block is-grid3
rsa-list-header
rsa-head
table-module
rsa-tables
rs-text-link is-print
```

Before changing these, compare against:

```text
C:\Users\gombc\Documents\Codex\2026-05-19\review-these-and-we-will-start\wec-layout-prototype\css\rsa-stylesheets.webflow.css
```

## Print / PDF Contract

Printing must be PDF-friendly from iPhone. The user expects the Cloudflare PDF worker path, not browser print behavior as the primary workflow.

PDF worker:

```text
https://ringstatus-pdf.gombcg.workers.dev/
```

Print route:

```text
/test/wec-packing/print
```

The print contract test currently asserts:

- title like `Barn List`
- right-side global header like `wave-one | printed ...`
- no `DATE` column
- no inline `Need:`, `Packed:`, or `Left:` strings
- aligned table rows
- `NAME`
- `NEEDED` spanning two columns
- `PACKED` spanning two columns
- `LEFT` spanning two columns
- notes row exists
- footer has `printed: page N + date`
- pagination repeats global header and section header
- 11 records per page in the current contract test

The user clarified the print rules:

- It is a worksheet, not a screenshot of the UI.
- No outer decorative UI lines.
- Print one section over however many pages it needs, then print the next section.
- Do not split into two columns for the current locked worksheet format.
- Fit to width only.
- If a section spills to another page, the continuation page must include the global header and the section header.
- Each record prints two rows:
  - row 1: name plus aligned meta cells
  - row 2: notes/scratch row
- All columns except name and notes should be consistent width.
- Use table labels, not inline strings like `Need: 1 Packed: 0 Left: 1`.

## Functionality That Must Be Tested

Do not claim functionality is done without testing the actual surface.

Test these:

- state load from `/wec-packing/state`
- `packWaveKey=wave_one`
- `packWaveKey=wave_2`
- eventual return wave setup, not yet final
- tab switching
- list switcher switching
- search open/close
- filter open/close and one-filter-at-a-time behavior
- print button URL construction through PDF worker
- row click opens approved modal
- modal close does not cause `aria-hidden` focused-descendant warnings
- add quantity
- add +1 quantity
- optimistic frontend update before Airtable round trip
- save title override
- save packed override
- save needed override
- decision `MAX`
- expanded decision row after MAX: remove / onsite / unresolved / SMS
- purchase onsite task -> done/task event
- horse row opens horse-specific item modal/list
- per-horse packed/not packed event
- refresh reloads saved Airtable values

## Known Functional Risks

1. Optimistic updates have lagged or failed to show after Airtable write. The user expects immediate device-local feedback and then a clean refresh from Airtable.

2. Add/add+1 should not hang. It should visually click, update the local UI, and show success/failure metadata later.

3. Print buttons have failed or opened an unsuitable browser print surface. Use the PDF worker route and test generated URLs.

4. Purchase onsite has repeatedly not appeared in preview. Confirm:
   - `wec_pack_lists` view `wec_home`
   - `wec_pack_items` view `wec_purchase_onsite`
   - state response `homeModules`
   - frontend overview rows

5. Horse count has shown mismatch. Do not trust a static count blindly. Use wave membership where possible and log sanity mismatch events.

6. `wec_packing_item_horses` can contain stale rows if a horse is deleted or canceled. The current approach tolerates manual cleanup, but the app should not hide that mismatch if it affects counts.

## UX Direction For New Runner

The current user direction is:

- finish wiring the user-built RSA template to live data
- do not keep the older large tab/card UI as the target
- keep the UI tight and operational
- avoid decorative cards, marketing layout, or new patterns
- all repeated rows must share the same grid, height, spacing, and class behavior
- actions are hot text or approved pill buttons, not random buttons
- section tabs and list switchers must use the approved class contract
- print is an action, not a tab
- search/filter toggle panels under the header
- filters need close controls
- only one filter active at a time
- list switchers toggle the displayed list below them
- item row name click opens modal
- edit/input modes show inline controls and save feedback

## What To Do Next

The next agent should not start by coding.

Start with a focused source parity audit:

1. Compare these selectors in `webflow/packing-worksheet/styles.css` to the user CSS in `wec-layout-prototype/css/rsa-stylesheets.webflow.css`:
   - `.rsa-list-header`
   - `.rsa-action-block`
   - `.rsa-action-block.is-grid3`
   - `.rs-text-link`
   - `.rs-text-link.is-print`
   - `.rs-tab-link`
   - `.rs-tab-link.is-switcher`
   - `.rsa-item-row`
   - `.rsa-item-row.is-grid2`
   - `.rsa-item-text`
   - `.rs-quantity-block.is-grid4`
   - `.rs-text-linline`
   - `.rs-input-inline`

2. Remove unauthorized deviations. Do not add compensating shims.

3. Verify in browser at multiple widths.

4. Only then continue data/functionality work.

## Current Git State Note

At the time this handoff was created, `git status --short` showed unrelated modified files:

```text
tmp/rsa-template-8795.log
tmp/wec-static-8792.log
webflow/rsa-dashboard-contract/CONTRACT.md
webflow/rsa-dashboard-contract/rsa-dashboard.css
webflow/rsa-dashboard-contract/rsa-dashboard.html
webflow/rsa-dashboard-contract/rsa-dashboard.js
webflow/rsa-dashboard-contract/rsa-webflow-embed.html
```

Do not revert or overwrite user/other-agent changes. Inspect before editing any dirty file.

## Final Warning For Next Agent

This project is failing primarily from drift, not from lack of code. The user has repeatedly provided exact CSS, exact markup, and exact class expectations. The next agent should treat every unrequested "improvement" as a defect.

Use the real template. Use the real CSS. Use the real Airtable/Webflow Cloud path. Verify the real browser surface. Do not invent.
