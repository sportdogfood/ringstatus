# RingStatus Runner Options Overview

This is the first-read map for a runner entering the RingStatus repo. It explains the available implementation lanes, what each lane is for, where the source lives, and which checks must happen before edits or live writes.

Use this as a routing document. For details, open the lane-specific handoff before touching code.

## Core Principle

RingStatus has several integration surfaces that look similar from the outside but have different edit paths:

```text
Webflow visual page
  -> static GitHub/jsDelivr CSS or JS
  -> optional Webflow Cloud/Astro API route
  -> optional Airtable read/write
  -> optional Cloudflare Worker or local runner output
```

Do not treat all Webflow work as the same thing. A visual page edit, a static embed asset update, a Webflow Cloud API change, an Airtable write path, and a Cloudflare Worker change have different source files and different verification gates.

## Option Matrix

| Option | Use When | Primary Source | Data Direction | Main Verification |
| --- | --- | --- | --- | --- |
| Native Webflow Designer/MCP edit | The page needs native Webflow elements, classes, components, or page structure changes | Webflow Designer via MCP/Bridge; source references in `webflow/rs-template-system/master_ks/` | Webflow site edit | Confirm Webflow MCP tools are exposed in the current chat and Designer Bridge is connected |
| Static Webflow embed | A manual Webflow page needs RingStatus CSS/JS injected | `webflow/<module>/` plus pinned jsDelivr URLs | Browser loads static assets | Verify the Webflow embed uses the intended commit SHA and browser loads the expected asset URLs |
| Two-way Airtable connector | A Webflow embed must read Airtable and save edits back | Frontend in `webflow/<module>/`; API in `webflow-cloud-test/src/pages/...` | Browser -> Webflow Cloud API -> Airtable -> browser | Verify health endpoint, live GET, then require separate approval before live PATCH/POST |
| Static dataset plus optional enrichment | A Webflow page renders public JSON and optionally saves curation/enrichment | `webflow/lp-history/` and `webflow-cloud-test/src/pages/lp-history/enrichment.js` | Static JSON read; optional Airtable write | Verify `/test/health`, enrichment GET, and visible browser save message |
| RS template/source package | A new RingStatus landing/page shell or section set is needed | `webflow/rs-template-system/master_ks/` and future `master_app/` | Source package output, not live data | Preview first; do not edit gold sources unless explicitly unlocked |
| Schedule/source runner work | The task affects heartbeat, watch schedule, trips, scope dates, source feeds, or runner scripts | Root runner scripts, `daily_schedule_app_source/`, docs scope files | Local/runner -> Airtable and generated feeds | Confirm exact show/date/customer scope and live row counts before writes |
| Daily schedule app UI | The task affects the future compact schedule UI only | `daily_schedule_app_ui/` | UI preview only | Run the UI tests and preserve the locked display contract |
| Cloudflare SMS/live lookup | The task affects SMS-style `As of / Now / Next / Following` output or lookup endpoints | `lib/cloudflare/ringstatus-sms/` and `lib/cloudflare/ringstatus-proxy/` | Cloudflare Worker -> schedule/live data -> response | Use worker harness or endpoint proof and compare exact output text |
| Equestrian caption app | The task affects branded caption generation UI | `equestrian-caption-app/` | Local app state/search, not RingStatus runner data | Preserve the shell contract and shared row/tag primitives |

## Current Integration Model

### Webflow Visual Layer

Webflow owns the public visual pages. There are two different ways to interact with it:

- Native edit path: Webflow Designer plus MCP Bridge, for real Webflow elements/classes/components.
- Manual embed path: paste an embed block into Webflow that loads RingStatus assets or calls RingStatus APIs.

Use `docs/webflow_interaction_readme.md` before Webflow work.

Confirmed RingStatus Webflow identifiers in that doc:

```text
siteId: 6982268b7543ac3c80151266
production domain: ringstatus.com
staging domain: ringstatus.webflow.io
test pageId: 6a0fa27816024003404ffeed
```

### GitHub/jsDelivr Static Assets

Many Webflow modules load CSS and JS from this GitHub repo through jsDelivr:

```text
https://cdn.jsdelivr.net/gh/sportdogfood/ringstatus@<commit-sha>/...
```

Use pinned commit SHAs while testing. `@main` can be stale because of CDN caching. If a browser still shows old behavior, first confirm the loaded asset URLs before debugging code.

This layer only serves static browser assets. It does not by itself provide data persistence.

### Webflow Cloud/Astro API Layer

`webflow-cloud-test/` is the server/API app deployed through Webflow Cloud. It uses Astro with Cloudflare runtime behavior.

Typical route shape:

```text
Webflow embed
  -> static frontend JS
  -> https://ringstatus.webflow.io/test/<module>/<endpoint>
  -> server-side Airtable API call
  -> JSON response
```

Important rule: Airtable tokens stay server-side in Webflow Cloud env vars. Do not put Airtable tokens into Webflow embeds or public frontend JS.

Use `docs/webflow_cloud_dataset_template_handoff.md` for this pattern.

### Airtable Read/Write Layer

Several RingStatus modules are two-way Airtable-backed connectors. The browser calls a Webflow Cloud API route, and the server route reads or writes Airtable.

The HPS connector is the current clearest example:

```text
Webflow page hps_8778
  -> #hps-app
  -> webflow/hps/hps.js
  -> /test/hps/horses
  -> Airtable ww_horses view hps_8778
  -> Airtable hp_cls change log
```

For HPS:

```text
source table: ww_horses
tenant validation table: active_tenants
tenant-specific view: hps_<tenant_id>
change log table: hp_cls
API route: webflow-cloud-test/src/pages/hps/horses.js
frontend assets: webflow/hps/
```

Before creating another connector, use `docs/hps_duplicate_connector_new_chat_prompt.md` and confirm the exact table, view, env var, allowed field, root id, and page slug mapping. Do not copy HPS values into a new dataset without confirmation.

## Lane Details

### 1. Native Webflow Designer/MCP Lane

Use this when the requested output must exist as native Webflow page structure, not just an embed.

Read first:

```text
docs/webflow_interaction_readme.md
webflow/rs-template-system/README.md
webflow/rs-template-system/RUNNER_GUIDE.md
webflow/rs-template-system/SECTION_CATALOG.md
```

Runner checks:

1. Confirm the current Codex chat exposes Webflow tools.
2. Confirm the Webflow MCP Bridge App is connected in Designer.
3. Confirm the target site/page.
4. Use `master_ks` as source/spec for approved sections.
5. Do not edit `master_ks` unless the user explicitly unlocks gold-source edits.

### 2. Manual Webflow Embed Lane

Use this when the Webflow page is manually managed and the runner should provide copy/paste embed code.

Read first:

```text
docs/8778_tack_horses_change_log_handoff.md
docs/hps_horses_webflow_airtable_connector_readme.md
docs/webflow_cloud_dataset_template_handoff.md
```

Runner checks:

1. Confirm the root div id.
2. Confirm the global config object name.
3. Confirm the API URL.
4. Confirm pinned CSS/JS asset URLs.
5. Open the browser and verify the loaded CSS/JS URLs match the intended commit SHA.

### 3. Two-Way Airtable Connector Lane

Use this when a Webflow page must display Airtable data and save edits back.

Current proven architecture:

```text
Webflow page
  -> embed root and config
  -> GitHub/jsDelivr frontend CSS/JS
  -> Webflow Cloud/Astro API route
  -> Airtable API
  -> source table
  -> change-log table
```

Runner gates:

1. Inspect `git status --short`.
2. Read the existing connector and handoff docs.
3. Confirm Airtable source table, view rule, log table, env vars, and editable fields.
4. Verify local preview if frontend changed.
5. Verify deployed health endpoint.
6. Verify live GET.
7. Stop for explicit approval before live PATCH/POST unless the user already approved the write.

Do not invent Airtable fields. If a field is not confirmed from source schema, export, live API, or handoff, keep it unknown.

### 4. RS Template Source Lane

Use this for reusable RingStatus shell/page/section work.

Primary source:

```text
webflow/rs-template-system/master_ks/
```

Supporting docs:

```text
webflow/rs-template-system/RUNNER_GUIDE.md
webflow/rs-template-system/SECTION_CATALOG.md
webflow/rs-template-system/master_ks/CONTRACT.json
```

Rules:

1. Copy from gold source first.
2. Output new artifacts outside `master_ks` and `master_app`.
3. Preserve wrapper order and stable section aliases.
4. Preview before editing approved packages.
5. Do not rebuild from memory or older screenshots.

### 5. Schedule, Heartbeat, Trips, and Runner Lane

Use this when the work touches source extraction, live show scope, heartbeat, `watch_schedule`, `watch_trips`, daily source files, or runner scripts.

Read first:

```text
docs/ringstatus_nightly_handoff_runbook_2026-05-16.md
docs/ringstatus_show_scope_shift_2026-05-16.md
docs/ringstatus_pipeline_scope_2026-05-08.md
docs/ringstatus_daily_scope_2026-05-09.md
```

Hard rules:

1. Confirm exact dates, show ids, customer ids, and scope keys.
2. Do not use day names as authoritative date inputs.
3. Confirm live Airtable row counts before write-path changes.
4. Do not use a current-date refresh as a prior-date cleanup.
5. Stop if a required field, endpoint, payload, or row count is unknown.

### 6. Daily Schedule App UI Lane

Use this for UI/display previews only.

Source:

```text
daily_schedule_app_ui/
```

Read first:

```text
daily_schedule_app_ui/README.md
daily_schedule_app_ui/SCHEDULE_DISPLAY_SCOPE.md
```

Boundary:

```text
Allowed: visual identifier contracts, preview rows, compact display language, modal shapes.
Not allowed: Airtable source truth, extraction workflow, active runner behavior, final app nesting.
```

Verification:

```powershell
node --test .\daily_schedule_app_ui\build_visual_identifier_preview.test.js
node --check .\daily_schedule_app_ui\build_visual_identifier_preview.js
```

### 7. Cloudflare SMS and Lookup Lane

Use this when the requested change affects SMS replies, app-native lookup response text, ring mappings, or Worker-hosted schedule/live endpoints.

Source:

```text
lib/cloudflare/ringstatus-sms/
lib/cloudflare/ringstatus-proxy/
```

Expected response concept:

```text
As of ...
Now
Next
Following
```

Runner checks:

1. Identify the exact Worker file used by the current task.
2. Preserve exact output formatting when the user provides sample text.
3. Run the local harness or endpoint check available for that Worker.
4. Compare the rendered response text, not just whether the request returns 200.

### 8. Equestrian Caption App Lane

Use this only for the branded caption app, not the operational RingStatus runner or Webflow connector lanes.

Source:

```text
equestrian-caption-app/
```

Read first:

```text
equestrian-caption-app/SHELL_CONTRACT.md
```

Rules:

1. Preserve the branded shell contract.
2. Reuse shared row/tag primitives.
3. Keep voice/profile/post-type behavior local unless a separate integration is explicitly requested.

## Quick Start for a Runner

1. Identify the lane from the option matrix.
2. Read the lane-specific docs listed above.
3. Check which Codex skills and plugin tools are installed or exposed in the current session.
4. Run `git status --short`.
5. State the exact files likely to be touched.
6. Confirm live schemas, env vars, or page ids before relying on memory.
7. Verify locally or through the live health/GET endpoint before recommending publish.
8. Require separate approval for live Airtable writes unless the user explicitly approved the write in the current task.

## Skills and Tool Preflight

Before planning implementation, confirm the current runner has the skills and tools needed for the chosen lane. Do not assume every Codex session has the same plugin exposure.

Check for these when relevant:

| Lane | Required or Useful Skills/Tools |
| --- | --- |
| Native Webflow Designer/MCP | Webflow MCP tools, MCP Bridge App, `webflow-mcp:*` skills if installed |
| Webflow embeds and frontend previews | Browser/in-app browser skill or equivalent preview tool |
| Webflow Cloud/Astro API | Vercel/Webflow/Cloudflare guidance tools when deployment or runtime behavior is involved |
| Airtable connector work | Airtable schema/source access or confirmed export; do not proceed from memory alone |
| GitHub/jsDelivr assets | Git/GitHub access, ability to confirm commit SHA and loaded browser asset URLs |
| Schedule/runner work | Local shell access, PowerShell, live Airtable/source payload access when writes or row counts are involved |
| Documents/spreadsheets/presentations | Matching document/spreadsheet/presentation skills if the output is an Office-style artifact |

If a needed skill or tool is missing, state the gap and switch to the safest fallback. For example, if Webflow MCP is not exposed, prepare a manual Webflow handoff instead of claiming a native Designer edit was made.

## Stop Conditions

Stop and ask for confirmation when:

- A required skill, plugin, connector, or MCP tool is not installed or not exposed in the current session.
- The task requires a live Airtable write and approval is not explicit.
- Airtable table/view/field names are not confirmed.
- A Webflow page target is ambiguous.
- A connector would require shared CSS hook renames.
- `master_ks` or `master_app` would need direct edits without an unlock.
- The current Codex chat does not expose the Webflow tools needed for a native Designer task.
- A schedule/trips/heartbeat action depends on a relative day name instead of an exact date.
- Browser proof depends on stale screenshots or unverified CDN asset URLs.

## Existing Handoff Documents

Use these as the deeper source for each lane:

```text
docs/webflow_interaction_readme.md
docs/hps_horses_webflow_airtable_connector_readme.md
docs/hps_duplicate_connector_new_chat_prompt.md
docs/8778_tack_horses_change_log_handoff.md
docs/webflow_cloud_dataset_template_handoff.md
webflow/rs-template-system/RUNNER_GUIDE.md
webflow/rs-template-system/SECTION_CATALOG.md
docs/ringstatus_nightly_handoff_runbook_2026-05-16.md
daily_schedule_app_ui/README.md
```
