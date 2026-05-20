# New Chat Prompt: Duplicate The HPS Connector For A Similar Dataset

Use this prompt to start a new project-specific chat when creating a connector that should behave like HPS but use different Airtable tables, views, env vars, fields, and Webflow page names.

```text
We are starting a new project-specific chat in the ringstatus repo.

Repo:
C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus

Goal:
Recreate the HPS Webflow/Airtable connector pattern for a new similar dataset.

This must be an identical connector pattern, but with different:
- connector name/prefix
- Webflow root id
- window config name
- Webflow page slug
- Webflow embed file
- frontend asset folder/files
- Webflow Cloud API route
- Airtable source table
- Airtable source view or dynamic view rule
- Airtable change-log table
- env var names
- allowed editable fields
- labels/tabs relevant to the new dataset

Do not assume data, fields, env names, or table names from memory. Ask for or inspect the current source dataset/schema before defining allowed fields.

Read these files first:
- docs/hps_horses_webflow_airtable_connector_readme.md
- docs/hps_module_project_contract.md
- webflow/hps/hps.js
- webflow/hps/hps.css
- webflow/hps/hps-preview.html
- webflow/hps/hps-8778-webflow-embed.html
- webflow/packing-worksheet/styles.css
- webflow-cloud-test/src/pages/hps/horses.js
- webflow-cloud-test/src/pages/health.js

Current HPS reference behavior to preserve:
- Mobile-first HTML/CSS/JS, not React.
- Hosted through Webflow static pages.
- API through Webflow Cloud under /test.
- Airtable is the source and write target.
- Shared brand/skeleton CSS is webflow/packing-worksheet/styles.css.
- App-specific CSS may extend the shared CSS but must not invent a new visual system.
- The frontend loads tenant-scoped records.
- The detail modal supports editable fields.
- PATCH updates the source Airtable table.
- Each edit creates a change-log row.
- Tenant id must isolate data and logs.
- Do not add major authentication or security layers unless explicitly planned.

Current HPS layout contract to preserve:
- The list opener and list panel live inside one wrapper:
  <div class="th-hps-module"> opener + shell </div>
- The wrapper moves as one unit.
- The opener must not use shared tab classes.
- The list panel uses the shared CSS cadence.
- The modal inner content is a four-row grid:
  top / tabs / body / footer
- The detail modal is capped at 400px and is right-aligned.
- The close button is absolute with z-index above 200.
- The profile/body rows are compact and grid-aligned.
- Do not use fake shell padding or hidden wrapper spacing to create layout.

Important process rules:
1. Start by summarizing the exact files that will be touched.
2. Do not change behavior until naming, tables, env vars, and allowed fields are confirmed.
3. Do not rename shared lp-* or packing-* CSS hooks unless a planned global CSS alias pass is explicitly approved.
4. Keep changes scoped to the new connector files unless a shared contract change is explicitly requested.
5. Verify local preview before suggesting publish.
6. Verify live GET before live PATCH.
7. Do not perform a live PATCH unless separately approved.
8. Keep responses short and action-oriented.

First tasks in the new chat:
1. Read the reference files listed above.
2. Ask for or inspect the new dataset/table/export.
3. Draft the new connector mapping:
   - prefix
   - root id
   - window config name
   - API route
   - source table
   - source view rule
   - change-log table
   - env var names
   - allowed fields
   - Webflow page slug
4. Create a short implementation checklist.
5. Only then copy/adapt the HPS connector.

Acceptance for the duplicated connector:
- New Webflow embed loads the new root/config/API.
- New API route reads only the intended Airtable table/view.
- Allowed fields are explicit and enforced.
- PATCH writes only allowed fields.
- Change log writes to the new log table with tenant id.
- Local preview loads records.
- Published Webflow page loads records.
- Shared CSS cadence is preserved.
```

## Reference Map

Use these as the authoritative source files for duplication.

### HPS Documentation

```text
docs/hps_horses_webflow_airtable_connector_readme.md
docs/hps_module_project_contract.md
```

### HPS Frontend

```text
webflow/hps/hps.js
webflow/hps/hps.css
webflow/hps/hps-preview.html
webflow/hps/hps-8778-webflow-embed.html
```

### Shared Brand CSS

```text
webflow/packing-worksheet/styles.css
```

### HPS Webflow Cloud API

```text
webflow-cloud-test/src/pages/hps/horses.js
webflow-cloud-test/src/pages/health.js
```

## Values To Replace In A New Connector

| HPS Reference | Replace With |
|---|---|
| `hps` | new connector prefix |
| `#hps-app` | new root id |
| `window.HPS_CONFIG` | new config name |
| `/test/hps/horses` | new API path |
| `webflow/hps` | new asset folder |
| `hps.js` | new JS file |
| `hps.css` | new CSS file |
| `hps-preview.html` | new preview file |
| `hps-8778-webflow-embed.html` | new embed file |
| `ww_horses` | new source table |
| `hps_<tenant_id>` | new view rule |
| `hp_cls` | new change-log table |
| `AIRTABLE_HPS_*` | new env var namespace |
| HPS horse fields | new allowed fields |

## Do Not Replace Without Approval

These are shared skeleton/brand hooks:

```text
lp-*
packing-*
webflow/packing-worksheet/styles.css
```

They may remain in the duplicated connector as internal CSS hooks.

## New Connector Fill-In Template

Use this table at the start of the new chat.

| Item | Value |
|---|---|
| Connector prefix |  |
| Root id |  |
| Config object |  |
| Webflow page slug |  |
| Local preview path |  |
| Embed file path |  |
| JS path |  |
| CSS path |  |
| API path |  |
| API file path |  |
| Source Airtable table |  |
| Source Airtable view/rule |  |
| Change-log table |  |
| Tenant field |  |
| Active tenants table/view |  |
| Env var namespace |  |
| Allowed fields |  |

## Required Verification Commands

```powershell
git status --short
node --check webflow\<new-connector>\<new-connector>.js
Invoke-RestMethod "https://ringstatus.webflow.io/test/<new-connector>/<route>?tenantId=<tenant_id>"
```

Then open:

```text
http://127.0.0.1:8822/webflow/<new-connector>/<new-connector>-preview.html
```

Do not publish until the local preview and live GET both pass.
