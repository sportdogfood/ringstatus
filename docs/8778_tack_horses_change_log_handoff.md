# 8778 Tack Horses Change Log Handoff

This handoff documents the full `8778-tack-horses` flow from the Webflow display to Webflow Cloud and Airtable. Use it as the source of truth for this connector.

## Scope

The template displays horses from Airtable table `ww_horses`, view `8778-tack-horses`.

The browser supports inline edits and active/inactive toggles. Every change must:

1. Patch the source record in `ww_horses`.
2. Create an audit row in `horses_change_log`.
3. Show visible save feedback in the horse detail modal.

This is not the LP History template and should not use `lp-history` app state, tables, or payload names.

## Frontend Assets

Static Webflow embed assets live in:

```text
webflow/8778-tack-horses/
```

Files:

```text
8778-tack-horses.js
8778-tack-horses.css
8778-tack-horses-preview.html
8778-tack-horses-webflow-embed.html
```

The visual contract uses the shared worksheet stylesheet:

```text
webflow/packing-worksheet/styles.css
```

That stylesheet intentionally supports both roots:

```css
:is(#packing-app, #tack-horses-app)
```

The tack horses embed root must be:

```html
<div id="tack-horses-app">Loading horses...</div>
```

Do not use `#packing-app` for this template. It can collide with the packing worksheet.

## Webflow Embed

Use pinned commit URLs while testing to avoid jsDelivr `@main` cache drift.

```html
<div id="tack-horses-app">Loading horses...</div>

<script>
  window.TACK_HORSES_CONFIG = {
    apiUrl: "https://ringstatus.webflow.io/test/8778-tack-horses/horses"
  };
</script>

<link rel="stylesheet" href="https://cdn.jsdelivr.net/gh/sportdogfood/ringstatus@<commit-sha>/webflow/packing-worksheet/styles.css">
<link rel="stylesheet" href="https://cdn.jsdelivr.net/gh/sportdogfood/ringstatus@<commit-sha>/webflow/8778-tack-horses/8778-tack-horses.css">
<script src="https://cdn.jsdelivr.net/gh/sportdogfood/ringstatus@<commit-sha>/webflow/8778-tack-horses/8778-tack-horses.js"></script>
```

Only move back to `@main` after confirming jsDelivr has refreshed. If the page loads stale behavior, pin the current commit again.

## Display Behavior

The front end loads from:

```text
GET https://ringstatus.webflow.io/test/8778-tack-horses/horses
```

Expected response:

```json
{
  "ok": true,
  "source": {
    "table": "ww_horses",
    "view": "8778-tack-horses"
  },
  "count": 64,
  "records": []
}
```

List rules:

- Group horses by Active and Inactive.
- Display horse list name from `barn_name` first.
- Clicking the name opens the horse detail.
- The right-side state pill toggles active/inactive.
- Active/inactive writes the Airtable checkbox field `inactive`.

Important Airtable behavior:

Unchecked checkbox fields are omitted from Airtable API responses. If `inactive` is absent, treat the record as active and still write back to the `inactive` field.

## Editable Source Fields

The detail modal writes to `ww_horses`.

Required editable fields:

| Field | Airtable type | UI behavior |
|---|---|---|
| `inactive` | checkbox | Active/Inactive toggle. `active` means `inactive=false`; `inactive` means `inactive=true`. |
| `show_name` | single line text | Text input. |
| `barn_name` | single line text | Text input and list display name. |
| `color` | single select | Single-choice pills. |
| `gender` | single select | Single-choice pills. |
| `horse_type` | single select | Single-choice pills, Pony/Horse. |
| `disciplines` | multiple select | Multi-choice pills. |
| `horse_age` | number | Small number input. |

Known current options:

```text
color: Black, Bay, Chestnut, Grey, Paint, Palomino, Liverchestnut
gender: Gelding, Mare
horse_type: Pony, Horse
disciplines: Hunters, Jumpers, Equitation
```

## Change Log Table

Audit rows must be written to:

```text
horses_change_log
```

Required fields:

| Field | Airtable type | Purpose |
|---|---|---|
| `horse` | single line text | Primary readable log label. |
| `change_key` | single line text | Unique change key. |
| `horse_record_id` | single line text | Source Airtable record id from `ww_horses`. |
| `horse_key` | single line text | Stable horse key when available. |
| `horse_name` | single line text | Display name at time of edit. |
| `field_name` | single line text | Source field edited. |
| `old_value` | long text / multiline text | Previous value. |
| `new_value` | long text / multiline text | New value. |
| `changed_at` | single line text | ISO timestamp. |
| `source` | single line text | Connector/source string. |
| `raw_payload` | long text / multiline text | Full browser payload plus update metadata. |

Do not use `horses_change_log_writes` unless `horses_change_log` becomes externally synced or non-writable again.

## Webflow Cloud App

Server/API app folder:

```text
webflow-cloud-test/
```

The Webflow Cloud project must deploy this folder, not the repo root.

Webflow Cloud project settings:

```text
Branch: main
Path: /test
Directory path: webflow-cloud-test
Framework: Astro
```

Runtime health endpoint:

```text
https://ringstatus.webflow.io/test/health
```

Horses endpoint:

```text
https://ringstatus.webflow.io/test/8778-tack-horses/horses
```

Endpoint source:

```text
webflow-cloud-test/src/pages/8778-tack-horses/horses.js
```

Astro 6 / Webflow Cloud env vars must be read with:

```js
import { env } from "cloudflare:workers";
```

Do not use `locals.runtime.env`.

## Webflow Cloud Environment Variables

Required:

```text
AIRTABLE_BASE_ID
AIRTABLE_TOKEN
AIRTABLE_HORSES_TABLE=ww_horses
AIRTABLE_HORSES_VIEW=8778-tack-horses
AIRTABLE_HORSES_CHANGE_LOG_TABLE=horses_change_log
```

The endpoint also has defaults for table/view names, but the environment variables should still be set so the runtime health response is explicit.

After changing environment variables, redeploy the Webflow Cloud environment.

## API Save Contract

Browser POST payload:

```json
{
  "horseRecordId": "rec...",
  "horseKey": "arrow-m-z",
  "horseName": "Arrow",
  "fieldName": "barn_name",
  "oldValue": "Arrow",
  "newValue": "Arrow M Z",
  "source": "8778-tack-horses"
}
```

Successful response:

```json
{
  "ok": true,
  "action": "updated_logged",
  "updated": {
    "id": "rec...",
    "fieldName": "barn_name",
    "value": "Arrow M Z",
    "action": "updated"
  },
  "log": {
    "id": "rec...",
    "changeKey": "horse:rec...:barn_name:...",
    "fieldCount": 11,
    "changedAt": "2026-05-20T04:00:00.000Z"
  }
}
```

The browser detail modal must display:

```text
Saved to Airtable at [time] (updated, logged).
```

If a save fails, the visible modal status should show the server error detail, not only `Check console`.

## Verification Checklist

Run before declaring the connector ready:

1. Health:

```powershell
Invoke-RestMethod "https://ringstatus.webflow.io/test/health" | ConvertTo-Json -Depth 6
```

Confirm:

```text
horsesEndpoint=/test/8778-tack-horses/horses
horsesTable=ww_horses
horsesView=8778-tack-horses
horsesChangeLog=horses_change_log
```

2. GET horses:

```powershell
Invoke-RestMethod "https://ringstatus.webflow.io/test/8778-tack-horses/horses"
```

Confirm `ok=true` and records load.

3. Active toggle:

- Toggle a horse from the list.
- Confirm `ww_horses.inactive` changes.
- Confirm a row appears in `horses_change_log`.

4. Detail edit:

- Open a horse detail.
- Change a text or pill field.
- Confirm visible modal save status.
- Confirm `ww_horses` updates.
- Confirm `horses_change_log` logs the change.

5. CDN:

- Prefer a pinned commit URL during testing.
- Confirm pinned JS includes:

```text
tack-horses-app
data-th-detail-status
```

## Known Failure Modes

### Horses load but edits fail

Check health first. If `horsesChangeLog` is wrong, fix Webflow Cloud env and redeploy.

### Active toggle fails only for active horses

This means the browser is not writing `inactive`. Airtable omits unchecked checkbox fields, so absence of `inactive` must still map to `active` and write `inactive=true` when toggled.

### Page uses old root or old styles

jsDelivr `@main` may be stale. Pin a commit SHA.

### `horses_change_log` exists but POST fails with externally synced error

The table is not writable. Either make `horses_change_log` writable or temporarily point:

```text
AIRTABLE_HORSES_CHANGE_LOG_TABLE=horses_change_log_writes
```

The preferred table is still `horses_change_log`.

