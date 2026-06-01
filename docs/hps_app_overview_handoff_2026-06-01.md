# HPS Horses App Overview And Handoff - 2026-06-01

This document describes the current HPS Horses app, its role, integrations, known trouble spots, and open tasks. It is written as a working contract for future changes. Do not treat this as a casual note. Use it to keep future work scoped and to prevent CSS, JS, data, and Webflow drift.

## App Role

HPS is the Horse Profile System module.

Its primary role is to let the user view and update horse profile data from Airtable in a compact Webflow-embedded interface. It is also the horse context module that other apps can pair with later, including schedules, trips, packing, tack, turnout, feed, and other horse-related workflows.

The module is mobile-first, Webflow-hosted, vanilla HTML/CSS/JS, and Airtable-backed through Webflow Cloud. Do not convert this app to React or another framework unless that is planned and approved before implementation.

## Current Display Locations

Current HPS profile page:

```text
https://ringstatus.com/hps-8778
```

Current local preview:

```text
webflow/hps/hps-preview.html
http://127.0.0.1:8822/webflow/hps/hps-preview.html
```

Current print page:

```text
https://ringstatus.com/hps-stall-card
```

Planned public print page rename:

```text
https://ringstatus.com/hps-print
```

Important: the internal print files and DOM root currently still use `hps-stall-card`. The public page can be renamed to `hps-print`, but the internal root/config names should not be changed unless the JS and CSS are updated together and tested.

## Primary Files

Frontend app:

```text
webflow/hps/hps.js
webflow/hps/hps.css
webflow/hps/hps-preview.html
webflow/hps/hps-8778-webflow-embed.html
```

Print app:

```text
webflow/hps/hps-stall-card.js
webflow/hps/hps-stall-card.css
webflow/hps/hps-stall-card-preview.html
webflow/hps/hps-stall-card-webflow-embed.html
```

Shared brand stylesheet:

```text
webflow/packing-worksheet/styles.css
```

API route:

```text
webflow-cloud-test/src/pages/hps/horses.js
```

Related docs:

```text
docs/hps_module_project_contract.md
docs/hps_horses_webflow_airtable_connector_readme.md
docs/hps_feed_shell_mode_rollback.md
docs/hps_duplicate_connector_new_chat_prompt.md
```

## Styling Contract

The HPS app depends on both:

```text
webflow/packing-worksheet/styles.css
webflow/hps/hps.css
```

`styles.css` is the shared brand skeleton. It owns the approved base cadence for rows, pills, modals, inputs, tabs, typography, spacing, and the visual rhythm used across related Webflow apps.

`hps.css` is app-specific. It owns HPS-only drawer behavior, HPS list controls, HPS modal layout adjustments, feed display refinements, and HPS-specific overrides.

Do not break either file.

Do not remove `#hps-app` from shared selector scope in `styles.css`. HPS markup intentionally reuses shared classes such as:

```text
lp-row
packing-row
packing-horse-row
lp-achievement
packing-token
lp-modal
lp-profile-shell
lp-profile-panel
lp-field-row
lp-edit-input
lp-edit-pill
```

If `styles.css` is changed to only target `#packing-app`, HPS loses the approved styling and falls back to broken native-looking rows, buttons, inputs, and modal layout. This happened once and must not be repeated.

Rule: HPS styling changes must be additive and scoped. Do not reinvent the app. Do not copy a parallel design system into `hps.css`.

## Current Embed: HPS

Current clean HPS embed, pinned to the fixed stylesheet scope commit:

```html
<div id="hps-app">Loading horses...</div>
<script>
  window.HPS_CONFIG = {
    tenantId: "8778",
    apiUrl: "https://ringstatus.webflow.io/test/hps/horses",
    refreshIntervalMinutes: 5,
    stallCardUrl: "https://ringstatus.com/hps-stall-card",
    pdfWorkerUrl: "https://ringstatus-pdf.gombcg.workers.dev/"
  };
</script>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/gh/sportdogfood/ringstatus@dd78cb7dfb2a78d639066e722f9b13e3f3872e36/webflow/packing-worksheet/styles.css">
<link rel="stylesheet" href="https://cdn.jsdelivr.net/gh/sportdogfood/ringstatus@dd78cb7dfb2a78d639066e722f9b13e3f3872e36/webflow/hps/hps.css">
<script src="https://cdn.jsdelivr.net/gh/sportdogfood/ringstatus@dd78cb7dfb2a78d639066e722f9b13e3f3872e36/webflow/hps/hps.js"></script>
```

When the public print page is renamed, update only:

```js
stallCardUrl: "https://ringstatus.com/hps-print"
```

Do not change the Airtable endpoint or tenant id unless creating a new tenant page.

## Current Embed: Print

Current print embed:

```html
<div id="hps-stall-card">Loading stall card...</div>

<script>
  window.HPS_STALL_CARD_CONFIG = {
    apiUrl: "https://ringstatus.webflow.io/test/hps/horses",
    logoUrl: "https://cdn.prod.website-files.com/6982268b7543ac3c80151266/6a0f3a68a0919117ccce7188_CWF%20CASTLEWOOD%20FARM.svg"
  };
</script>

<link rel="stylesheet" href="https://cdn.jsdelivr.net/gh/sportdogfood/ringstatus@1eb59c074c75e38a71ceca671b97d5bd174810bd/webflow/hps/hps-stall-card.css">
<script src="https://cdn.jsdelivr.net/gh/sportdogfood/ringstatus@1eb59c074c75e38a71ceca671b97d5bd174810bd/webflow/hps/hps-stall-card.js"></script>
```

This can live on the renamed Webflow page `/hps-print` without changing the internal root id. The page URL and the internal script naming are separate concerns.

## Webflow Cloud API

Endpoint:

```text
https://ringstatus.webflow.io/test/hps/horses
```

Route file:

```text
webflow-cloud-test/src/pages/hps/horses.js
```

Methods:

```text
GET  loads tenant horse data
POST saves one allowed field and logs the change
OPTIONS supports CORS
```

Tenant id is required. Current tenant:

```text
8778
```

The route validates that the tenant exists in the active tenants table/view before loading or saving.

## Airtable Integration

Default table/view contract:

```text
horse source table: ww_horses
horse source view: hps_<tenant_id>
current view: hps_8778
change log table: hp_cls
feed plan table: hp_feed_plan
linked WEC horse table: wec_horses
active tenants table: active_tenants
active tenants view: active_tenants
```

Environment variables supported by the API:

```text
AIRTABLE_TOKEN
AIRTABLE_BASE_ID or AIRTABLE_BASE
AIRTABLE_HPS_HORSES_TABLE
AIRTABLE_WW_HORSES_TABLE
AIRTABLE_HORSES_TABLE
AIRTABLE_HPS_VIEW_PREFIX
AIRTABLE_HPS_CHANGE_LOG_TABLE
AIRTABLE_HPS_FEED_PLAN_TABLE
AIRTABLE_HPS_FEED_PLAN_VIEW
AIRTABLE_HPS_WEC_HORSES_TABLE
AIRTABLE_WEC_HORSES_TABLE
AIRTABLE_HPS_ACTIVE_TENANTS_TABLE
AIRTABLE_HPS_ACTIVE_TENANTS_VIEW
```

## Data Contract

Read fields currently exposed by the API contract:

```text
horse
horse_id
show_name
pid
last_modified_time
tenant_id
airtable_id
```

Editable fields currently allowed by the API:

```text
barn_name
horse_colors
horse_genders
emergency_phone
emergency_contacts
horse_disciplines
horse_age
hands
app_active
app_inactive
wec_wave_1
wec_wave_2
wec_not_going
print_batch
horse_note
```

Membership fields:

```text
wec_horses_link
lists
```

Deferred or not fully rendered fields:

```text
active_subscribers
ww_riders
horse_profile_tabs_link
```

Action field:

```text
stall_card_input_print
```

The API rejects POST writes for fields outside the editable list.

## Frontend Functionality

The HPS app currently supports:

- Drawer-style horse list anchored to the right.
- Default closed module state.
- Always-visible HPS opener button.
- Active, Inactive, Feed, and Refresh list controls.
- Search across visible horse fields and feed plan text.
- Active and inactive grouping from `app_active` / `app_inactive`.
- Manual refresh.
- Auto refresh on interval from `refreshIntervalMinutes`.
- Horse detail modal.
- Overview tab.
- Profile tab.
- Feed tab.
- Contacts tab.
- Print tab.
- Editable profile fields.
- Optimistic controls for profile updates.
- Session-only include/ignore behavior through browser session storage.
- Read-only feed plan display from `hp_feed_plan`.
- Print-now flow for stall cards.
- Print request checkbox behavior through `print_batch`.
- WEC summer wave controls.

## App Status Behavior

App status is backed by Airtable fields:

```text
app_active
app_inactive
```

Clicking Active:

```text
app_active = true
app_inactive = false
```

Clicking Inactive:

```text
app_active = false
app_inactive = true
```

These values are saved to `ww_horses` and logged in `hp_cls`.

## Session View Behavior

Session view is device/browser-session only.

Values:

```text
include
ignore
```

Storage:

```text
window.sessionStorage
key: hps_session_prefs_<tenantId>
```

Session view does not write to Airtable and does not log to `hp_cls`.

## WEC Summer Behavior

WEC summer is backed by Airtable fields:

```text
wec_wave_1
wec_wave_2
wec_not_going
```

Clicking Wave-1:

```text
toggles wec_wave_1
forces wec_not_going = false
does not force wec_wave_2 off
```

Clicking Wave-2:

```text
toggles wec_wave_2
forces wec_not_going = false
does not force wec_wave_1 off
```

Clicking None:

```text
toggles wec_not_going
if wec_not_going becomes true, forces wec_wave_1 = false and wec_wave_2 = false
```

Scope warning:

Every WEC summer click always writes to `ww_horses`. If the horse has linked record ids in `ww_horses.wec_horses_link`, the API also PATCHes the linked records in `wec_horses` with the same field value. This is intentional current behavior, but it must be understood before changing the click behavior.

## Feed Behavior

Feed data is read-only in HPS.

Source:

```text
hp_feed_plan
```

The API loads feed rows and groups them by horse. A feed row is displayed only when it has at least one visible quantity. Current visible columns are:

```text
Feed
AM
MID
PM
Unit or measure when shown in detail table
```

Hidden feed fields:

```text
feed.type
feed.total
```

The list-level Feed view shows all horses with visible feed rows expanded by default. Horse names in the Feed list are not intended to open the horse detail modal.

Open feed design issue: the feed list has been improved, but it should still be treated as a sensitive layout area. Do not redesign it without previewing the nested table/card structure.

## Print Behavior

Print Now in HPS opens the print page in a new tab/window with query params:

```text
tenantId=<tenant>
horseRecordId=<record id>
autoprint=1
```

The print page loads the same HPS API, finds the single horse in the tenant view, renders the stall card, waits for assets, then calls `window.print()`.

The print page uses the Castlewood Farm logo:

```text
https://cdn.prod.website-files.com/6982268b7543ac3c80151266/6a0f3a68a0919117ccce7188_CWF%20CASTLEWOOD%20FARM.svg
```

Known print concern:

- Browser print behavior varies by device and browser.
- iPhone/Safari may not behave like desktop Chrome.
- Popup blocking is possible.
- The current flow opens a print page, not a server-side PDF download.
- The older Cloudflare PDF worker exists, but Cloudflare Browser Rendering rate limits caused failures.

PDF worker:

```text
https://ringstatus-pdf.gombcg.workers.dev/
```

The worker should not be treated as the default reliable path until rate limiting and user feedback are resolved.

## Change Logging

All saved Airtable field changes should:

1. PATCH `ww_horses`.
2. Create one audit record in `hp_cls`.
3. Include `tenant_id`.
4. Include the horse record id and field name.
5. Include old and new values where available.

For WEC summer fields, linked `wec_horses` updates are included in the backend update result, but the main audit row is still created through the HPS change log flow.

## Known Trouble Spots

### Shared CSS Scope

The biggest confirmed styling failure was caused by removing `#hps-app` from shared `styles.css` selectors. This made HPS rows, pills, inputs, modal, and detail layout fall back to broken native-looking controls.

Do not narrow shared base selectors to `#packing-app` only unless HPS has an approved replacement. The current approved pattern is:

```css
:is(#packing-app, #tack-horses-app, #hps-app)
```

### App-Specific CSS Drift

HPS must not become a copied or reinvented version of packing CSS. Use `hps.css` only for HPS-specific behavior and refinements.

### Embed Pinning

Webflow pages load pinned jsDelivr URLs. Local changes do not appear in Webflow until:

1. Changes are committed.
2. Changes are pushed.
3. The Webflow embed is updated to the new pinned commit.
4. Webflow is published.
5. Browser/CDN cache is refreshed.

If the browser shows old behavior, check the embedded commit SHA before assuming the code is wrong.

### Print Naming

The public page is moving from:

```text
hps-stall-card
```

to:

```text
hps-print
```

Do not rename internal root ids or JS config names casually. The public URL can change first; internal file/root naming can be cleaned up later as a deliberate pass.

### WEC Horses Side Effect

WEC summer clicks can update linked `wec_horses` records if `ww_horses.wec_horses_link` contains record ids. This is not just a display toggle.

### Field Allowlist

`field_not_allowed` means the frontend attempted to save a field that is not listed in `PROFILE_EDITABLE_FIELDS` in the API route. Do not fix this by weakening validation broadly. Add only the exact approved field.

### Airtable Checkbox Omission

Airtable may omit unchecked checkbox fields from API responses. Missing checkbox fields must be treated as false.

### Feed Matching

Feed rows are matched to horses through candidate horse id/link fields. If feed rows do not appear, inspect `hp_feed_plan` field names and the matching candidates before changing UI.

### Auto Refresh

Auto refresh only runs while the page is open. If Safari or another browser closes/suspends the tab, refresh stops. Reopening the app performs a fresh GET.

## Open Tasks

1. Create or confirm the public Webflow page `/hps-print`.
2. Update HPS embed `stallCardUrl` from `/hps-stall-card` to `/hps-print` after the page exists.
3. Decide whether to rename internal print files/root/config from `hps-stall-card` to `hps-print`; do this only as a planned rename with preview verification.
4. Reconfirm print on desktop and iPhone/Safari after `/hps-print` is live.
5. Re-evaluate batch print concept:
   - mark records with `print_batch`
   - collect marked horses
   - render 4-up landscape pages
   - clear marks after successful print action
6. Confirm whether WEC summer should continue updating linked `wec_horses`, or whether that should become opt-in or isolated.
7. Keep Feed read-only unless an edit workflow is separately designed.
8. If adding more paired apps, preserve the HPS drawer/component contract and do not fork the styling.

## Verification Checklist Before Push Or Webflow Publish

Before pushing HPS changes:

```text
git status --short
```

Verify local preview:

```text
webflow/hps/hps-preview.html
```

Check:

- Drawer starts closed.
- Opener remains visible.
- Active list rows use approved row styling.
- Status pills use approved token styling.
- Active/Inactive/Feed/Refresh controls work.
- Search input is styled.
- Detail modal opens at the approved width.
- Detail modal tabs are styled.
- Overview fields are styled.
- App Status reflects `app_active` / `app_inactive`.
- WEC Summer buttons reflect `wec_wave_1`, `wec_wave_2`, `wec_not_going`.
- Feed list uses nested read-only table/card display.
- Print tab opens print page once, not twice.
- No native fallback buttons or inputs appear.

Verify live endpoint:

```text
https://ringstatus.webflow.io/test/hps/horses?tenantId=8778
```

Expected:

```text
ok=true
source.table=ww_horses
source.view=hps_8778
records load
profileContract is present
```

Do not perform live PATCH tests unless explicitly approved.

## Future Connector Duplication Rule

For a similar connector:

1. Create the Airtable source table/view contract first.
2. Add the tenant to `active_tenants`.
3. Confirm the API can load the tenant view.
4. Reuse the HPS component structure and shared stylesheet.
5. Create only the minimal app-specific JS/CSS needed.
6. Pin the Webflow embed to a tested commit.
7. Verify local preview before publish.
8. Verify live GET before any live write.
9. Do not invent fields, tables, or display behavior from memory.

