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

## Open Schema Confirmation

Before final live validation, confirm the actual active tenant id field name in `active_tenants`.

Candidate field names:

```text
tenant_id
pid
```

The implementation should use the confirmed field name rather than guessing.
