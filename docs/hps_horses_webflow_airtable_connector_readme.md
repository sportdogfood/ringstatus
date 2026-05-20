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
