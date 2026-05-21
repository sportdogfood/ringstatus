# HPS Horses Webflow/Airtable Connector README

This README defines the next direction for the HPS horses connector. It replaces the one-off `8778-tack-horses` naming with a tenant-scoped HPS pattern while keeping `ww_horses` as the Airtable source table.

## Purpose

The HPS connector displays and edits horses for one tenant on one static Webflow page.

Example:

- Webflow static page: `hps_8778`
- Visible tenant: `8778`
- Airtable source table: `ww_horses`
- Airtable source view: `hps_8778`
- Airtable log table: `hp_cls`

The same frontend/API should be reusable for future tenant pages such as `hps_19676`. The static Webflow page controls which tenant is visible by passing a tenant id in the embed config.

## Airtable Contract

### Tenant Discovery

The API should read active tenants from:

```text
table: active_tenants
view: active_tenants
```

For each active tenant id, the connector expects a matching horses view:

```text
hps_<tenant_id>
```

Examples:

```text
tenant_id=8778  -> ww_horses view hps_8778
tenant_id=19676 -> ww_horses view hps_19676
```

Only active tenants listed in `active_tenants` should be allowed. If a requested tenant id is not active, the API should return an error and should not load or save horse data.

### HPS Profile Field Contract

Read fields:

```text
horse
horse_id
show_name
pid
last_modified_time
tenant_id
airtable_id
```

Editable fields:

```text
barn_name
emergency_no
emergency_contact
rider_list
horse_note
trainer_id
horse_types
horse_disciplines
horse_age
horse_colors
horse_genders
horse_profile_tabs
emergency_phone
emergency_contacts
tenant_img
active
priority
ww_grooms
ww_exercisers
```

Action fields:

```text
stall_card_input_print
```

Membership fields:

```text
wec_horses_link
lists
```

Deferred fields:

```text
active_subscribers
ww_riders
horse_profile_tabs_link
```

Linked field references:

```text
tenant_id -> ww_tenants
trainer_id -> ww_trainers
horse_disciplines -> horse_disciplines_link
horse_colors -> horse_colors_link
horse_genders -> horse_genders_link
```

The API must reject POST writes for fields outside the editable field list.

### Horses Source

The source table remains:

```text
ww_horses
```

The API should dynamically choose the source view from the requested tenant id:

```text
view = hps_<tenant_id>
```

For the `hps_8778` Webflow page, the API should load only:

```text
ww_horses / hps_8778
```

Confirmed current view:

```text
ww_horses view hps_8778
```

### Change Log

All edits should create audit rows in:

```text
hp_cls
```

Audit rows must include `tenant_id` so edits are isolated and traceable by tenant.

The primary human-readable log label field is:

```text
change_label
```

Required log behavior:

1. PATCH the source horse record in `ww_horses`.
2. Create one audit row in `hp_cls`.
3. Include `tenant_id` in the browser payload, server validation, and log row.
4. Preserve the current visible save feedback in the horse detail modal.

## Webflow Embed Contract

The Webflow page is still created manually. The embed must be tenant-specific and should be copied into the static page for that tenant.

Confirmed working drop for the `hps_8778` page:

```html
<div id="hps-app">Loading horses...</div>

<script>
  window.HPS_CONFIG = {
    tenantId: "8778",
    apiUrl: "https://ringstatus.webflow.io/test/hps/horses"
  };
</script>

<link rel="stylesheet" href="https://cdn.jsdelivr.net/gh/sportdogfood/ringstatus@a65bbf3/webflow/packing-worksheet/styles.css">
<link rel="stylesheet" href="https://cdn.jsdelivr.net/gh/sportdogfood/ringstatus@a65bbf3/webflow/hps/hps.css">
<script src="https://cdn.jsdelivr.net/gh/sportdogfood/ringstatus@a65bbf3/webflow/hps/hps.js"></script>
```

For another tenant, only the static page name and `tenantId` should change:

```js
window.HPS_CONFIG = {
  tenantId: "19676",
  apiUrl: "https://ringstatus.webflow.io/test/hps/horses"
};
```

Use pinned commit URLs while testing. Move back to `@main` only after confirming jsDelivr has refreshed.

Do not leave the placeholder string `<commit-sha>` in Webflow. If the embed contains `ringstatus@<commit-sha>`, the JS will not load and the page will remain stuck at `Loading horses...`.

## Connection Setup Steps

Follow these steps to create or repair the `hps_8778` connection.

### 1. Confirm Airtable Setup

Confirm these Airtable objects exist:

```text
ww_horses
ww_horses view hps_8778
active_tenants
active_tenants view active_tenants
hp_cls
```

Confirm tenant `8778` appears in `active_tenants / active_tenants`.

### 2. Confirm Webflow Cloud Environment Variables

Set these in Webflow Cloud:

```text
AIRTABLE_BASE_ID=<base id>
AIRTABLE_TOKEN=<token>
AIRTABLE_HPS_HORSES_TABLE=ww_horses
AIRTABLE_HPS_VIEW_PREFIX=hps_
AIRTABLE_HPS_CHANGE_LOG_TABLE=hp_cls
AIRTABLE_HPS_ACTIVE_TENANTS_TABLE=active_tenants
AIRTABLE_HPS_ACTIVE_TENANTS_VIEW=active_tenants
```

Redeploy Webflow Cloud after changing env vars.

### 3. Confirm Webflow Cloud Health

Open:

```text
https://ringstatus.webflow.io/test/health
```

Confirm:

```text
hpsEndpoint=/test/hps/horses
hpsHorsesTable=ww_horses
hpsViewPrefix=hps_
hpsChangeLog=hp_cls
hpsActiveTenantsTable=active_tenants
hpsActiveTenantsView=active_tenants
```

### 4. Confirm HPS API Data

Open:

```text
https://ringstatus.webflow.io/test/hps/horses?tenantId=8778
```

Confirm:

```text
ok=true
tenantId=8778
source.table=ww_horses
source.view=hps_8778
count > 0
```

For the first successful `hps_8778` test, this endpoint returned 64 records.

### 5. Create The Static Webflow Page

Create the static Webflow page manually.

Recommended page slug:

```text
hps-8778
```

Recommended page title:

```text
hps_8778
```

Add one Webflow Embed element containing the full confirmed drop from the Webflow Embed Contract section above.

### 6. Publish And Test The Webflow Page

Publish Webflow and open:

```text
https://ringstatus.com/hps-8778
```

Expected result:

- page does not stay on `Loading horses...`
- header shows `HPS Horses`
- subtitle shows `ww_horses - hps_8778`
- list loads horses from `ww_horses / hps_8778`
- Active and Inactive groups render

### 7. Verify Save Behavior

Only after the page loads correctly:

1. Open one horse detail modal.
2. Make a controlled detail edit.
3. Confirm the browser shows save feedback.
4. Confirm `ww_horses` changed.
5. Confirm `hp_cls` has one log row with `tenant_id=8778`.

Active/inactive toggles must write the real `ww_horses.inactive` checkbox.

## Frontend Naming

The public HPS app structure should use HPS names:

```text
root id: hps-app
config: window.HPS_CONFIG
static assets: webflow/hps/hps.js, webflow/hps/hps.css
```

The shared brand stylesheet remains:

```text
webflow/packing-worksheet/styles.css
```

Do not redesign the visual system. Existing internal CSS classes from the shared stylesheet may remain as implementation hooks until a deliberate stylesheet aliasing pass is approved.

## API Naming

Recommended Webflow Cloud endpoint:

```text
GET/POST https://ringstatus.webflow.io/test/hps/horses
```

Recommended source file:

```text
webflow-cloud-test/src/pages/hps/horses.js
```

The API should:

- read `tenantId` from the query string or POST payload
- validate it against `active_tenants`
- build the Airtable horses view as `hps_<tenant_id>`
- load records from `ww_horses`
- reject saves when the submitted horse record is not present in that tenant's `hps_<tenant_id>` view
- PATCH edits back to `ww_horses`
- write audit rows to `hp_cls`
- include `tenant_id` in responses and logs

## Verification Checklist

Before declaring the HPS connector ready:

1. Confirm health/config reports the HPS endpoint and log table.
2. Confirm `GET /test/hps/horses?tenantId=8778` returns only `ww_horses / hps_8778` records.
3. Confirm `GET /test/hps/horses?tenantId=19676` returns only `ww_horses / hps_19676` records when that tenant is active.
4. Confirm an inactive tenant id is rejected.
5. Confirm the `hps_8778` Webflow page embed passes `tenantId: "8778"`.
6. Confirm active/inactive writes the real `ww_horses.inactive` checkbox.
7. Confirm detail edits PATCH `ww_horses`.
8. Confirm every edit writes one row to `hp_cls` with `tenant_id`.
9. Confirm local preview and live Webflow Cloud endpoint still preserve the existing visual style.

## Current Workflow Review

The current workflow has four separate responsibilities:

1. Airtable owns tenant and horse data.
2. Webflow Cloud owns tenant-scoped API access.
3. The static Webflow page owns the tenant-specific embed config.
4. The browser app owns display, inline edits, active/inactive toggles, and visible save feedback.

The stable flow should be:

```text
Webflow page hps_8778
  -> embed passes tenantId=8778
  -> GET /test/hps/horses?tenantId=8778
  -> API confirms 8778 exists in active_tenants / active_tenants
  -> API loads ww_horses / hps_8778
  -> browser shows only 8778 horses
  -> browser POST includes tenantId=8778
  -> API confirms the horse is still in ww_horses / hps_8778
  -> API PATCHes ww_horses
  -> API writes hp_cls with tenant_id=8778
```

Minor workflow rules:

- Keep Webflow page creation manual until the first tenant page is stable.
- Keep one reusable HPS JS/API pair; do not fork per tenant.
- Keep `ww_horses` as the source table.
- Keep `hps_<tenant_id>` as the tenant view naming rule.
- Keep `hp_cls` as the HPS change-log table.
- Keep the shared brand stylesheet as the visual contract.
- Do not add feature expansion until tenant isolation, save behavior, and logging are verified live.

## Stability Gate

The project is stable only after all of these are true:

1. `https://ringstatus.webflow.io/test/health` reports:

```text
hpsEndpoint=/test/hps/horses
hpsHorsesTable=ww_horses
hpsViewPrefix=hps_
hpsChangeLog=hp_cls
hpsActiveTenantsTable=active_tenants
hpsActiveTenantsView=active_tenants
```

2. `GET /test/hps/horses?tenantId=8778` returns records from `ww_horses / hps_8778`.
3. A bad or inactive tenant id is rejected.
4. The `hps_8778` Webflow page loads with `#hps-app` and `window.HPS_CONFIG.tenantId = "8778"`.
5. Active/inactive toggle writes `ww_horses.inactive`.
6. A detail field edit PATCHes `ww_horses`.
7. Each save creates one `hp_cls` row with `tenant_id`.
8. The browser shows success or server error feedback in the detail modal.

Until this gate passes, do not add new display fields, filters, sorting, tenant dashboards, bulk edits, or new page variants.

## Future Tasks After Stability

Add these only after the stability gate passes:

1. Add a tenant setup checklist for creating a new Webflow page such as `hps_19676`.
2. Add a tenant smoke-test script for `tenantId`, expected view, record count, and active tenant validation.
3. Add a controlled live-edit verification script that can update a harmless field and confirm the matching `hp_cls` row.
4. Add clearer health output for active tenant field detection once the actual field name is confirmed.
5. Consider adding horse-specific CSS aliases only if we need to remove internal `packing-*` class names without changing visuals.
6. Add future UI tasks only after the first tenant page is stable.

## Pattern For Similar Connectors

Use this pattern when creating another tenant-scoped Webflow/Airtable connector.

### Connector Naming

Pick one short connector prefix and use it consistently:

```text
prefix: hps
root id: <prefix>-app
config: window.<PREFIX>_CONFIG
api path: /test/<prefix>/horses
static assets: webflow/<prefix>/<prefix>.js and webflow/<prefix>/<prefix>.css
log table: <prefix>_cls or another explicit connector log table
```

For HPS:

```text
root id: hps-app
config: window.HPS_CONFIG
api path: /test/hps/horses
static assets: webflow/hps/hps.js and webflow/hps/hps.css
log table: hp_cls
```

### Tenant Isolation Rule

Every similar connector should use the same isolation shape:

```text
Webflow static page passes tenantId
API validates tenantId against active_tenants
API builds source view from tenantId
API loads only that view
API rejects saves for records outside that view
API logs tenant_id on every edit
```

### Airtable View Naming

Use deterministic view names:

```text
<connector_prefix>_<tenant_id>
```

Examples:

```text
hps_8778
hps_19676
```

Do not create one-off view names that cannot be derived from the tenant id.

### Webflow Embed Template

For another HPS tenant, copy the working embed and change only `tenantId` and the Webflow page slug/title.

Example for tenant `19676`:

```html
<div id="hps-app">Loading horses...</div>

<script>
  window.HPS_CONFIG = {
    tenantId: "19676",
    apiUrl: "https://ringstatus.webflow.io/test/hps/horses"
  };
</script>

<link rel="stylesheet" href="https://cdn.jsdelivr.net/gh/sportdogfood/ringstatus@a65bbf3/webflow/packing-worksheet/styles.css">
<link rel="stylesheet" href="https://cdn.jsdelivr.net/gh/sportdogfood/ringstatus@a65bbf3/webflow/hps/hps.css">
<script src="https://cdn.jsdelivr.net/gh/sportdogfood/ringstatus@a65bbf3/webflow/hps/hps.js"></script>
```

The matching Airtable view must be:

```text
ww_horses / hps_19676
```

### Minimum Checklist For A New Tenant

For each new tenant:

1. Add tenant to `active_tenants / active_tenants`.
2. Create the Airtable source view, such as `ww_horses / hps_19676`.
3. Create the Webflow static page, such as `hps-19676`.
4. Paste the embed with `tenantId: "19676"`.
5. Publish Webflow.
6. Test `GET /test/hps/horses?tenantId=19676`.
7. Test the published page loads data.
8. Test one controlled edit and confirm `hp_cls.tenant_id`.

Do not fork the JS/API unless the connector has a different data model or behavior. Prefer one reusable connector with tenant-specific embed config.

## Open Schema Confirmation

Before final live validation, confirm the actual active tenant id field name in `active_tenants`.

Candidate field names:

```text
tenant_id
pid
```

The implementation should use the confirmed field name rather than guessing.

## HPS Mobile Drawer/Profile Layout Lock

Status: local preview lock, created in `webflow/hps/hps.js` and `webflow/hps/hps.css`.

This section documents the current HPS preview structure so future work does not drift back into improvised spacing or fake shell offsets.

### Current HPS Shell Structure

The horse list opener and the list panel must live inside one wrapper:

```html
<div class="th-hps-module">
  <button class="th-hps-toggle">...</button>
  <div class="lp-shell th-hps-shell" data-hps-module-shell>
    <main class="lp-content">...</main>
  </div>
</div>
```

The wrapper is the moving unit. Do not position the toggle and the list independently.

### Drawer Behavior

- `.th-hps-module` is fixed at the top right.
- `.th-hps-module` uses horizontal flex and aligns children to the top.
- Closed state: wrapper sits offscreen to the right.
- Open state: `#hps-app.is-hps-open .th-hps-module` moves the wrapper onscreen.
- The toggle is a flex child, not an absolute child.
- The shell is a flex child, not a fake spacer.
- `.lp-content` is static inside the shell. Do not use absolute/relative positioning on `.lp-content` for this drawer.

### Shell Spacing Rules

The shared `.lp-shell` class brings inherited padding and border that do not work for this drawer. HPS overrides this intentionally:

```css
#hps-app .th-hps-shell {
  padding: 0;
  border: 0;
}
```

Do not reintroduce shell padding/border as a layout spacer. If the drawer needs spacing, add it to the correct child.

### Toggle Contract

The opener must not use the shared tab class stack.

Use:

```html
<button class="th-hps-toggle">
  <span class="th-hps-toggle-count" data-th-count>0</span>
  <span class="th-hps-toggle-label">Horses</span>
</button>
```

Do not use:

```text
lp-tab packing-tab packing-theme-horses th-hps-toggle
```

The opener count should render as `64` over `Horses`, not `64 shown Horses`.

### Hidden Public Shell Pieces

Inside the HPS list drawer, these shared shell pieces are hidden:

```text
.lp-header
.lp-tabs
.lp-section-title.packing-section-title
```

The list drawer should start with the sticky search toolbar and then the horse list.

### Sticky List Body

- `.packing-tools.th-toolbar` is sticky at the top of the list panel.
- `.lp-list` scrolls under the sticky toolbar.
- The list panel is capped to the HPS drawer width.

### Modal/Profile Structure

The detail modal inner content is a four-row grid:

```text
top
tabs
body
footer
```

The HPS detail markup uses:

```html
<div class="lp-profile-shell">
  <div class="lp-profile-head th-profile-top">...</div>
  <div class="lp-profile-tabs th-profile-tabs">...</div>
  <section class="lp-profile-panel">...</section>
  <div class="lp-profile-modal-footer th-profile-footer">...</div>
</div>
```

The modal card is capped at `400px`, offset from the right by `16px`, and has `min-height: 95vh`.

The close button is absolute and must stay above modal content:

```css
#hps-app .lp-modal-close {
  position: absolute;
  z-index: 201;
}
```

### Modal Body Grid

The modal body and active tab panel are one-column grids aligned to the top:

```text
display: grid
grid-auto-rows: auto
align-content: start
place-content: start stretch
```

Editable field rows in the profile body are fixed to `63px`.

Profile body row padding is controlled in the HPS CSS only. Do not re-add nested left/right padding through shared shell or record wrappers.

### Footer/State Row

The state row and save status row are grouped in:

```html
<div class="lp-profile-modal-footer th-profile-footer">...</div>
```

The active/inactive state row is currently hidden in the modal footer.

The save status footer remains visible.

### Verification For This Layout

Before publishing a change to this layout:

1. Open `http://127.0.0.1:8822/webflow/hps/hps-preview.html`.
2. Confirm the drawer opens/closes as one flex unit.
3. Confirm the opener moves with the drawer.
4. Confirm the list loads 64 horses for tenant `8778`.
5. Confirm clicking a horse opens the 400px right modal.
6. Confirm modal tabs do not break the four-row structure.
7. Run:

```powershell
node --check webflow\hps\hps.js
```

Do not push or publish this layout without rechecking the preview.

## HPS State And Refresh Contract

Status: locked definitions, implementation staged in HPS frontend/API where fields currently exist.

### Distinct State Definitions

These names must remain separate. Do not overload one Airtable field or one UI label to mean multiple states.

```text
app_active / app_inactive
```

Purpose: HPS app visibility/status.

Behavior:

```text
app_active   -> the horse is active for the HPS app workflow
app_inactive -> the horse is inactive for the HPS app workflow
```

This is the app-level status that should be sent to Airtable after the distinct Airtable fields are created and confirmed.

```text
app_include / app_ignore
```

Purpose: quick include/ignore workflow.

Behavior:

```text
app_include -> include this horse in the current app workflow
app_ignore  -> ignore this horse for now
```

This is not the same as app active/inactive. It is a workflow include/ignore concept.

```text
record_active / record_inactive
```

Purpose: source/record lifecycle status.

Behavior:

```text
record_active   -> source record is active
record_inactive -> source record is inactive
```

This is not related to HPS app active/inactive unless a future source contract explicitly maps it.

### Current Implementation Guard

Until the distinct Airtable fields exist and are confirmed, the HPS list state pill is display-only. It must not POST the older overloaded `inactive` field.

The allowed editable field list is currently:

```text
barn_name
horse_colors
horse_genders
emergency_phone
emergency_contacts
horse_disciplines
horse_age
horse_note
```

`show_name` is display-only and must not be modified by HPS.

### Change Logging

HPS changes write directly to:

```text
ww_horses
```

Successful edits are logged to:

```text
hp_cls
```

The change log captures the changed field through:

```text
field_name
old_value
new_value
tenant_id
raw_payload
```

No separate `hp_cls` fields are required for `app_active`, `app_ignore`, or `record_active` unless reporting needs them later.

### Refresh Behavior

The app gets fresh data by rerunning GET against:

```text
/test/hps/horses?tenantId=<tenant_id>
```

Refresh behavior:

```text
On app load: GET
Manual Refresh button: GET immediately
Every refreshIntervalMinutes while page is open and visible: GET
When browser tab becomes visible again: GET
```

Default interval:

```text
5 minutes
```

Webflow embed setting:

```js
window.HPS_CONFIG = {
  tenantId: "8778",
  apiUrl: "https://ringstatus.webflow.io/test/hps/horses",
  refreshIntervalMinutes: 5
};
```

Safari/iOS rule:

```text
If the tab is closed, the app is not running.
No auto refresh happens.
No background Airtable polling happens.
When the user opens the page again, the app runs a fresh GET.
```

If Safari keeps the tab open but backgrounds it, timers may be paused or throttled. The app compensates by refreshing when the tab becomes visible again.

### Stall Card Print/PDF Workflow

The HPS Print tab must not capture or print the current HPS modal/page.

Correct workflow:

```text
User opens a horse profile
User clicks Print tab
User clicks Print
HPS builds a print-only stall-card URL with tenantId and horseRecordId
HPS sends that URL to the PDF worker
PDF worker renders the print-only Webflow page
PDF opens in a new tab/window
```

The print-only Webflow page is:

```text
https://ringstatus.com/hps-stall-card
```

The PDF worker is:

```text
https://ringstatus-pdf.gombcg.workers.dev/
```

The HPS embed config should include:

```js
window.HPS_CONFIG = {
  tenantId: "8778",
  apiUrl: "https://ringstatus.webflow.io/test/hps/horses",
  refreshIntervalMinutes: 5,
  stallCardUrl: "https://ringstatus.com/hps-stall-card",
  pdfWorkerUrl: "https://ringstatus-pdf.gombcg.workers.dev/"
};
```

The Print tab is read-only. It does not show print-specific inputs. If the stall-card data is wrong, the user should update the normal HPS profile fields first, then print.

The PDF URL shape is:

```text
https://ringstatus-pdf.gombcg.workers.dev/?url=<encoded hps-stall-card URL>&filename=<horse>-stall-card.pdf
```

The print-only page receives:

```text
tenantId
horseRecordId
```

The print-only page loads fresh data from:

```text
/test/hps/horses?tenantId=<tenant_id>
```

Then it renders only the 5.5in x 3.75in stall card.

## Step By Step: Duplicate HPS For A Similar Connector

Use this when creating a similar Webflow/Airtable connector for another dataset, such as riders, trainers, trips, packing, turnout, or tack.

The goal is to reuse the HPS pattern without copying HPS-specific data assumptions into the new module.

### 1. Define The New Connector Name

Choose one short connector prefix.

Examples:

```text
hps
rps
tps
pack
tack
turnout
```

Decide these names before coding:

```text
connector prefix
root id
window config name
api path
source Airtable table
source Airtable view rule
change log table
Webflow page slug
static asset folder
```

Example HPS values:

```text
prefix: hps
root id: hps-app
config: window.HPS_CONFIG
api path: /test/hps/horses
source table: ww_horses
view rule: hps_<tenant_id>
change log table: hp_cls
page slug: hps-8778
asset folder: webflow/hps
```

### 2. Confirm Airtable Tables And Views

For a tenant-scoped connector, create or confirm:

```text
source table
tenant view
change log table
active_tenants table/view if tenant gating is required
```

For HPS:

```text
source table: ww_horses
tenant view: hps_8778
change log table: hp_cls
tenant list: active_tenants / active_tenants
```

For a new connector, do not guess the field names. Confirm:

```text
primary record name field
record id/key field
tenant id field
editable fields
checkbox fields
linked-record fields
lookup fields
fields allowed for PATCH
fields allowed for logs
```

### 3. Define The Allowed Fields

Create the allowed edit field list before enabling saves.

For each field, define:

```text
field name in Airtable
input type
display label
allowed choices if pill/select
whether it is writable
whether it should be logged
```

Do not PATCH lookup fields, formulas, rollups, or display-only fields.

If a field has both a string version and linked-record version, decide which one the app writes. Document it before implementing.

### 4. Copy The Frontend Folder

Copy the HPS frontend folder only after the data contract is clear.

Example:

```text
webflow/hps
-> webflow/<new-prefix>
```

Rename files:

```text
hps.js
hps.css
hps-preview.html
hps-8778-webflow-embed.html
```

to:

```text
<new-prefix>.js
<new-prefix>.css
<new-prefix>-preview.html
<new-prefix>-<tenant_id>-webflow-embed.html
```

### 5. Rename Public App Names

Update only public-facing names first:

```text
#hps-app
window.HPS_CONFIG
HPS Horses
Horse profiles and status
/test/hps/horses
webflow/hps/hps.css
webflow/hps/hps.js
```

Do not rename shared CSS classes such as:

```text
lp-*
packing-*
```

Those are shared brand/skeleton hooks unless a planned global CSS alias pass is approved.

### 6. Update The Webflow Embed

Every Webflow page needs:

```html
<div id="<root-id>">Loading...</div>

<script>
  window.<CONFIG_NAME> = {
    tenantId: "<tenant_id>",
    apiUrl: "https://ringstatus.webflow.io/test/<connector>/<route>"
  };
</script>

<link rel="stylesheet" href="https://cdn.jsdelivr.net/gh/sportdogfood/ringstatus@<commit-sha>/webflow/packing-worksheet/styles.css">
<link rel="stylesheet" href="https://cdn.jsdelivr.net/gh/sportdogfood/ringstatus@<commit-sha>/webflow/<connector>/<connector>.css">
<script src="https://cdn.jsdelivr.net/gh/sportdogfood/ringstatus@<commit-sha>/webflow/<connector>/<connector>.js"></script>
```

For tenant pages, change only:

```text
tenantId
page slug/title
optional visible title/subtitle
```

Do not create a separate JS file per tenant unless the behavior is actually different.

### 7. Copy Or Create The API Route

Create the matching Webflow Cloud route.

Example HPS source:

```text
webflow-cloud-test/src/pages/hps/horses.js
```

New connector example:

```text
webflow-cloud-test/src/pages/<connector>/<route>.js
```

Update:

```text
table name
view name rule
tenant validation
allowed fields
change log table
change log field names
response source labels
health output
```

### 8. Add Environment Variables

Use connector-specific env names.

Example HPS:

```text
AIRTABLE_HPS_HORSES_TABLE=ww_horses
AIRTABLE_HPS_VIEW_PREFIX=hps_
AIRTABLE_HPS_CHANGE_LOG_TABLE=hp_cls
AIRTABLE_HPS_ACTIVE_TENANTS_TABLE=active_tenants
AIRTABLE_HPS_ACTIVE_TENANTS_VIEW=active_tenants
```

For a new connector, create the same pattern with the new prefix.

Do not reuse HPS env names for another module.

### 9. Update Health Output

Add the new connector to:

```text
webflow-cloud-test/src/pages/health.js
```

Health should confirm:

```text
endpoint
source table
view prefix or fixed view
change log table
active tenant table/view if used
```

### 10. Verify GET Before PATCH

Test GET first:

```powershell
Invoke-RestMethod "https://ringstatus.webflow.io/test/<connector>/<route>?tenantId=<tenant_id>"
```

Confirm:

```text
ok=true
tenantId matches
source table matches
source view matches
records load
record count is expected
```

Do not test PATCH until GET and allowed fields are confirmed.

### 11. Verify Local Preview

Open the local preview:

```text
http://127.0.0.1:8822/webflow/<connector>/<connector>-preview.html
```

Confirm:

```text
records load
list opens/closes
search works
detail modal opens
tabs do not break layout
save footer is visible
styling still uses shared CSS cadence
```

### 12. Verify One Controlled Edit

Only after approval, test one small edit.

Confirm:

```text
source table record updates
change log table gets one row
tenant_id is logged
field name is logged
old/new values are logged if available
no disallowed fields are accepted
```

### 13. Publish

After local preview and live endpoint pass:

```text
commit
push
copy commit SHA
update Webflow embed asset URLs
publish Webflow
test published page
```

### 14. Connector Duplication Checklist

Before calling a duplicate connector stable:

1. Data contract is documented.
2. Allowed fields are documented.
3. Webflow embed uses the new root/config/API.
4. JS/CSS public names use the new connector name.
5. Shared CSS classes remain intact.
6. API route uses the correct Airtable table/view/log table.
7. Health reports the connector.
8. GET returns the expected records.
9. Local preview loads.
10. Published Webflow page loads.
11. One approved PATCH writes the table and log.
12. README is updated with exact page, endpoint, and table names.
