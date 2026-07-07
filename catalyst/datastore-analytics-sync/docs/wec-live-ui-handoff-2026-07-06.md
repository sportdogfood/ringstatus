# WEC Live UI Handoff - 2026-07-06

## Purpose

This handoff documents the WEC live schedule UI work in `catalyst/datastore-analytics-sync`, including what functionality was built, which variants exist, what was deployed, and what failed.

The user-provided Webflow export is the desired visual shell reference:

`C:\Users\gombc\Downloads\philips-fantabulous-site-708573.webflow (1)`

The intended target is:

- Use the user's shell/design language.
- Preserve all working UI behavior from the current live schedule app.
- Avoid further patching around AG Grid if AG fights the required row structure.
- Do not reinterpret the mock as only loose inspiration.

## Current Local Variants

### 1. `wec-ag-live`

Path:

`C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus\catalyst\datastore-analytics-sync\wec-ag-live\index.html`

Status:

- Existing AG Grid based Slate app.
- Deployed successfully before this handoff.
- Still registered in `catalyst.json`.
- Contains the broadest working functionality.

Behavior currently built here:

- Fetches WEC live schedule data from the Catalyst function endpoint:
  - primary: `/server/wec_live_grid/execute?action=wec-mobile-live&show_no=14909`
  - fallback: `/server/horseshowing_sync/?action=wec-mobile-live&show_no=14909`
- Preserves `focus_day` query parameter when present.
- Normalizes endpoint payload with `rings[].classes[]`.
- Uses `ring_visual_key`, `ring_name_normalized`, `ring_display`, and `ring_name_prioritized`.
- Groups rows by ring using full-width AG ring rows.
- Renders class rows as full-width AG rows.
- Separates rollup line and class line inside each rendered row.
- Exposes individual rollup items as clickable buttons.
- Clicking a rollup opens a rollup flyup.
- Clicking the class line opens a class flyup.
- Ring filter buttons:
  - click once selects a ring
  - click again unselects
  - multiple rings are OR
  - no active ring means all rings
- Horse filter panel:
  - filter button toggles horse filters
  - horse filters are OR
  - scoped clear button clears horse filters
- Focus mode:
  - scans the entire schedule for duplicate candidates by ring/time/class name
  - keeps the lowest class number
  - hides later duplicate candidates
  - example behavior: `928` hidden when paired with `927`; `911` hidden when paired with `910`
- Hide mode:
  - toggles a hide action row
  - changes visual state while active
  - checkboxes appear before rows
  - clicking rows in hide mode toggles pending hidden state
  - `Clear all` clears pending hidden state
  - `Save` persists hidden rows and exits hide mode
- Show hidden mode:
  - toggles saved hidden rows back into the visible schedule
  - hidden rows are visually distinguished
- Print:
  - prints current viewing/filter/hide/focus state
  - groups by visible ring
  - includes rollup text above related class row
  - preserves compact landscape print intent
- Status line:
  - source label
  - show number
  - focus date
  - last updated
  - visible row count
  - hidden/focus/filter counts

Known issue:

- AG Grid remains the wrong structural fit for the requested Webflow-like row contract. The app uses full-width rows and custom rendered content to force a two-line class wrapper into AG. This created repeated problems with row height, hover, click targets, focus styling, sticky/group behavior, and full-row rollup handling.

### 2. `wec-ag-flat`

Path:

`C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus\catalyst\datastore-analytics-sync\wec-ag-flat\index.html`

Status:

- Copied from `wec-ag-live`.
- Deployed successfully as separate Slate app.
- Registered in `catalyst.json`.

Difference from `wec-ag-live`:

- No emitted AG ring group/container rows.
- Flat display only.
- Default row sort:
  - `ring_name_prioritized`
  - `time`
  - `class_number`
- Ring buttons changed from anchor jumps to ring filters.
- Existing UI behavior otherwise intended to match `wec-ag-live`.

Known issue:

- Still AG-based, so it inherits most AG structural mismatch risk even without ring group rows.

### 3. `wec-webflow-live`

Path:

`C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus\catalyst\datastore-analytics-sync\wec-webflow-live\index.html`

Status:

- Local only.
- Registered in `catalyst.json`.
- Deploy failed because Catalyst returned:
  - `HTTP Error: 403, You have reached the maximum number of deployed apps for this project.`

Intent:

- First non-AG attempt.
- Used Webflow mock CSS/assets but wrote a new HTML renderer.

Built behavior:

- Live/fallback data fetch and normalizer.
- Ring filters.
- Horse filters.
- Focus mode.
- Hide mode.
- Show hidden.
- Print.
- Class flyup.
- Rollup flyup.
- Mobile/desktop render checks were run locally in Chrome.

Failure:

- Version 1 FAIL.
- It treated the Webflow mock as a design reference instead of using the exported shell literally.
- It overrode too much CSS.
- It changed the header/action density.
- It changed the table head from the mock's `.rs-app-collection-head-grid.is-dark`.
- It changed button styling.
- It changed ring header/status treatment.
- It did not preserve the template closely enough.

### 4. `wec-webflow-live-v2`

Path:

`C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus\catalyst\datastore-analytics-sync\wec-webflow-live-v2`

Status:

- Local only.
- Not registered in `catalyst.json` at handoff time.
- Not deployed.
- User marked this V2 as FAIL before verification was completed.

Intent:

- Copy the user's Webflow export nearly literally.
- Add `live.js` to hydrate the existing static shell.
- Replace static repeated content at runtime instead of rewriting the shell.

Files:

- `index.html`: copied from the Webflow export.
- `live.js`: added behavior/hydration script.
- `.catalyst/slate-config.toml`: created with `deployment_name = "wec-webflow-live-v2"`.

Failure:

- Version 2 FAIL.
- Work was paused after the user questioned whether removing AG was viable.
- The user then stated V2 was still just the static template and failed.
- No verified successful V2 behavior should be assumed.

## User's Required Structural Contract

The user's Webflow mock demonstrates the target structure. This should be treated as the contract, not merely inspiration.

Target shell:

```html
rs-app-shell
  rs-app.is-base
    rs-app-head
    rs-app-tools
    rs-app-anchors
    rsa-list-block.rs-app-list
      rs-app-collections-stack
        rs-app-collection-head
          rs-app-collection-head-grid.is-dark
        rs-app-collection-lists
          rs-app-collection-list
            rs-app-collection-list-head
            rs-app-collection-list-items
              rs-app-collection-list-item
                rs-app-litem-stack.is-rollup-line
                  rs-app-item-rollups
                    rs-rollup
                rs-app-litem-stack.is-class-line
                  rs-app-item-grid
```

Required row behavior:

- `rs-app-collection-list-item` is the visual wrapper for the related data.
- `rs-app-litem-stack.is-rollup-line` is its own line.
- Each `rs-rollup` is its own clickable trigger.
- `rs-app-litem-stack.is-class-line` is its own line.
- The full class line opens the class flyup.
- Rollup triggers must not open the class flyup.
- The wrapper exists for shading only, not as a click target.

Required feature set:

- Live data fetch and fallback.
- Ring filtering.
- Horse filtering.
- Focus duplicate suppression.
- Hide mode with pending state, checkboxes, clear-all, save.
- Show hidden.
- Print current view.
- Class flyup.
- Rollup/entry flyup.
- Mobile behavior.
- Current status line/meta.

## AG Grid Decision Notes

AG Grid is useful for:

- Built-in column menus.
- Built-in column sorting/filtering.
- Virtualized large grids.
- AG-owned row model.

AG Grid is a poor fit for the user's required visual contract:

- The target is nested normal HTML:
  - ring container
  - rollup line
  - class line
  - individual rollup triggers
  - wrapper-based shading
- Prior AG attempts required forcing this into full-width rows.
- That produced repeated problems with:
  - row height
  - auto-height
  - hover state ownership
  - row focus styles
  - click trigger boundaries
  - sticky ring behavior
  - ring group sorting
  - rollups spanning full row width

Recommendation:

- If the priority is exact shell/row structure, do not use AG.
- If the priority is AG-native column UI, accept that the shell will likely diverge and the row structure will remain fragile.
- Do not start another AG patch unless the user explicitly accepts AG's structural constraints.

## Deployment State

Registered Slate apps in `catalyst.json` at handoff:

```json
[
  "wec-ag-live",
  "wec-ag-flat",
  "wec-webflow-live"
]
```

Deployment results:

- `wec-ag-live`: deploy succeeded earlier.
- `wec-ag-flat`: deploy succeeded earlier.
- `wec-webflow-live`: deploy failed with Catalyst app limit:
  - `HTTP Error: 403, You have reached the maximum number of deployed apps for this project.`
- `wec-webflow-live-v2`: not deployed.

## Do Not Repeat

- Do not rebuild the design from scratch.
- Do not use the Webflow export as loose inspiration.
- Do not override the Webflow CSS broadly.
- Do not "improve" the visual system without explicit approval.
- Do not patch AG repeatedly to approximate the Webflow row structure.
- Do not assume a new Slate can deploy until the Catalyst app limit is resolved.
- Do not claim a version works without rendered verification.

## Suggested Next Step

Start fresh from the Webflow export, but do it as a literal hydration pass:

1. Copy the export.
2. Keep `index.html` shell and CSS intact.
3. Remove or replace only the repeated static schedule lists.
4. Add one script that hydrates:
   - top action buttons
   - anchors/filters
   - schedule rows
   - flyups
   - print
5. Verify in browser before showing.
6. Resolve Catalyst Slate app limit before attempting deploy.

If a deploy target is needed immediately, decide which existing Slate app to replace:

- replace `wec-ag-flat`, or
- replace failed `wec-webflow-live` registration after freeing an app slot in Catalyst.

