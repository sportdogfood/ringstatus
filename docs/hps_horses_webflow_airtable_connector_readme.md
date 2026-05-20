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

For the `hps_8778` page:

```html
<div id="hps-app">Loading horses...</div>

<script>
  window.HPS_CONFIG = {
    tenantId: "8778",
    apiUrl: "https://ringstatus.webflow.io/test/hps/horses"
  };
</script>

<link rel="stylesheet" href="https://cdn.jsdelivr.net/gh/sportdogfood/ringstatus@<commit-sha>/webflow/packing-worksheet/styles.css">
<link rel="stylesheet" href="https://cdn.jsdelivr.net/gh/sportdogfood/ringstatus@<commit-sha>/webflow/hps/hps.css">
<script src="https://cdn.jsdelivr.net/gh/sportdogfood/ringstatus@<commit-sha>/webflow/hps/hps.js"></script>
```

For another tenant, only the static page name and `tenantId` should change:

```js
window.HPS_CONFIG = {
  tenantId: "19676",
  apiUrl: "https://ringstatus.webflow.io/test/hps/horses"
};
```

Use pinned commit URLs while testing. Move back to `@main` only after confirming jsDelivr has refreshed.

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

## Open Schema Confirmation

Before final live validation, confirm the actual active tenant id field name in `active_tenants`.

Candidate field names:

```text
tenant_id
pid
```

The implementation should use the confirmed field name rather than guessing.
