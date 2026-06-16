# Webflow Custom Pages System Final Overview

## Purpose

This document describes the current RingStatus custom Webflow page-rendering system.

The system is designed to let Webflow host simple permanent embeds while the actual page structure, navigation, sections, copy, and page relationships are managed in Airtable and rendered through Webflow Cloud.

The current working model is:

```text
Webflow page
  -> permanent embed
  -> hosted loader script
  -> compiled page payload endpoint
  -> rendered HTML injected into #rs-page-root
```

The Airtable model remains the source used to build the render output. For runtime speed, the live Webflow page now reads a compiled payload first instead of rebuilding the page from Airtable on every click.

## Current Live Pages

The current front-facing RingStatus page set uses these page keys and URLs:

| Webflow URL | Page key |
| --- | --- |
| `/rs/home` | `rs_home` |
| `/rs/about-me` | `rs_about_me` |
| `/rs/company` | `rs_about_company` |
| `/rs/apps` | `rs_apps` |
| `/rs/contact` | `rs_contact` |
| `/rs/members` | `rs_members` |

The loader also maps `/rs` and `/rs/` to `rs_home`.

## Runtime Files

The current runtime is in the Webflow Cloud app:

```text
webflow-cloud-test/src/pages/rs-page-loader.js
webflow-cloud-test/src/pages/rs-page-render.js
webflow-cloud-test/src/pages/rs-page-payload.js
webflow-cloud-test/src/lib/rs-page-static-payload.js
```

### `rs-page-loader.js`

This is the permanent hosted script loaded by Webflow pages.

Endpoint:

```text
https://ringstatus.com/test/rs-page-loader
```

Responsibilities:

- finds `#rs-page-root` or `[data-rs-page-root]`
- reads `window.RS_PAGE_RENDER_CONFIG`
- determines the initial page key
- fetches compiled HTML from `/test/rs-page-payload`
- injects HTML into the root
- intercepts clicks on `.rs-main .rs-nav-link[data-rs-page-key]`
- uses `history.pushState` for navigation
- stores rendered pages in memory and `sessionStorage`
- preloads the rest of the navigation pages after the active page renders
- falls back to `/test/rs-page-render` if the compiled payload endpoint fails

The loader response now uses:

```text
Cache-Control: no-cache
```

That is deliberate. The script itself should update without changing Webflow embeds. The page HTML payload can be cached; the loader should not get stuck on an older behavior.

### `rs-page-payload.js`

This endpoint serves the compiled static page payload.

Endpoint:

```text
https://ringstatus.com/test/rs-page-payload
```

Supported calls:

```text
/test/rs-page-payload?pageKey=rs_home
/test/rs-page-payload?pageKey=rs_members
/test/rs-page-payload?all=1
```

Responsibilities:

- imports `RS_PAGE_STATIC_PAYLOAD`
- returns one page by `pageKey`
- returns all compiled pages when `all=1`
- returns `404` for unknown page keys

This is the fast path for the public Webflow pages.

### `rs-page-static-payload.js`

This file contains the compiled HTML payload generated from the Airtable-backed renderer.

It currently contains:

- `generatedAt`
- `pages.rs_home`
- `pages.rs_about_me`
- `pages.rs_about_company`
- `pages.rs_apps`
- `pages.rs_contact`
- `pages.rs_members`

Each page contains:

- `html`
- `source`

This file is not hand-authored content. It is the compiled output of the Airtable render model. When Airtable page data changes, this file must be regenerated and redeployed before the compiled endpoint reflects the change.

### `rs-page-render.js`

This is the Airtable-backed dynamic renderer.

Endpoint:

```text
https://ringstatus.com/test/rs-page-render
```

Supported normal call:

```text
/test/rs-page-render?pageKey=rs_home
```

Supported bypass:

```text
/test/rs-page-render?pageKey=rs_home&refresh=1
```

Supported older test mode:

```text
/test/rs-page-render?mode=site_toggle&pageKey=rs_home
```

Responsibilities:

- reads Airtable records from the rscom base
- builds a page tree
- renders HTML for navigation, sections, divs, typography, content, and footer
- escapes text content
- sanitizes trusted HTML content by stripping scripts and inline event attributes
- returns JSON with rendered `html`, `tree`, `source`, and prefetch data

This route also has in-memory page and dataset caches. Those help when the same Cloud isolate handles repeated requests, but they are not dependable as the only runtime performance strategy because Webflow Cloud/Cloudflare can serve different isolates and can override HTTP caching behavior.

## Current Webflow Embed

Every Webflow page should use the same stable loader shape. Only `pageKey` changes per page.

Example for `/rs/home`:

```html
<div id="rs-page-root" data-rs-page-key="rs_home">Loading...</div>

<script>
  window.RS_PAGE_RENDER_CONFIG = {
    pageKey: "rs_home",
    endpointUrl: "https://ringstatus.com/test/rs-page-render"
  };
</script>
<script src="https://ringstatus.com/test/rs-page-loader"></script>
```

For `/rs/apps`, only this value changes:

```js
pageKey: "rs_apps"
```

The embed should not contain the full fetch/render logic anymore. The purpose of the loader endpoint is to stop repeated manual embed changes.

Optional advanced config:

```js
window.RS_PAGE_RENDER_CONFIG = {
  pageKey: "rs_home",
  endpointUrl: "https://ringstatus.com/test/rs-page-render",
  payloadUrl: "https://ringstatus.com/test/rs-page-payload"
};
```

If `payloadUrl` is omitted, the loader defaults to:

```text
https://ringstatus.com/test/rs-page-payload
```

## Webflow Cloud Integration

Current Webflow Cloud details:

| Item | Value |
| --- | --- |
| Webflow site | `ringstatus` |
| Site id | `6982268b7543ac3c80151266` |
| Webflow Cloud project id | `d7d97751-20e1-4148-a5cf-ee58671c128a` |
| Mount path | `/test` |
| Framework | Astro |
| Adapter/runtime | Cloudflare edge via `@astrojs/cloudflare` |
| Local app folder | `webflow-cloud-test` |

Deploy command used:

```powershell
webflow cloud deploy --project-id d7d97751-20e1-4148-a5cf-ee58671c128a --site-id 6982268b7543ac3c80151266 --mount /test --framework astro --no-input --skip-update-check
```

Build command:

```powershell
cd "C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus\webflow-cloud-test"
npm run build
```

The project uses:

```text
astro
@astrojs/cloudflare
@astrojs/react
@cloudflare/vite-plugin
```

## Airtable Integration

Current Airtable base:

```text
base name: rscom
base id: appDN3R51ZPmwgMib
```

Runtime environment variables:

```text
AIRTABLE_TOKEN
AIRTABLE_RSCOM_BASE_ID
RSCOM_AIRTABLE_BASE_ID
```

`AIRTABLE_TOKEN` is required. The renderer falls back to `appDN3R51ZPmwgMib` if no rscom base override is provided.

No Airtable token belongs in Webflow embeds.

Correct security path:

```text
Webflow page
  -> Webflow Cloud endpoint
  -> Airtable API using server-side token
```

Incorrect path:

```text
Webflow page
  -> browser-side Airtable token
```

## Airtable Tables Used By The Renderer

The current renderer reads these tables:

| Runtime key | Airtable table |
| --- | --- |
| `pages` | `rs_pages_index` |
| `blocks` | `rs_page_blocks` |
| `divs` | `rs_page_divs` |
| `typography` | `rs_typography` |
| `content` | `rs_content` |
| `navigation` | `rs_navigation_items` |
| `globals` | `rs_global_params` |

The current renderer reads all records from each table with `pageSize=100` and paginates through offsets.

Important current limitation:

```text
The renderer does not yet use Airtable views or field allow-lists for these tables.
```

That means Airtable structure can be edited, but runtime payload control is still in code until field/view filtering is added.

## Airtable Content Model

The practical hierarchy is:

```text
rs_projects_index
  -> rs_pages_index
    -> rs_page_blocks
      -> rs_page_divs
        -> rs_typography
          -> rs_content
```

Additional support tables:

```text
rs_navigation_items
rs_global_params
rs_section_inventory
rs_section_slots
rs_repeatable_item_types
rs_tone_levers
rs_tone_tags
table_index
```

Not all support tables are used directly by the current runtime renderer yet. Some are part of the design/blueprint model and should be treated as reference or next-stage structure until explicitly wired.

## How A Page Renders

For a page such as `rs_home`:

1. Loader reads `pageKey = rs_home`.
2. Loader requests:

   ```text
   /test/rs-page-payload?pageKey=rs_home
   ```

3. Payload route returns compiled HTML.
4. Loader inserts returned HTML into:

   ```html
   <div id="rs-page-root"></div>
   ```

5. The inserted HTML includes:

   ```html
   <main class="rs-page" data-rs-page="rs_home">
   ```

6. Navigation links include:

   ```html
   <a class="rs-nav-link" href="/rs/apps" data-rs-page-key="rs_apps">Apps</a>
   ```

7. Clicks on those links are intercepted by `rs-page-loader.js`.
8. Loader fetches the compiled payload for the clicked page.
9. Loader updates the root HTML and pushes the browser URL with `history.pushState`.

## How Navigation Works

Navigation is rendered from `rs_navigation_items`.

The current main nav values are expected to include:

```text
Home
About Me
Company
Apps
Contact
Members
```

Navigation links must include:

```text
href
page_key
label
nav_group
sort_order
active
```

The loader only intercepts links matching:

```css
.rs-main .rs-nav-link[data-rs-page-key]
```

If navigation visually renders but clicks do not route inside the app, check:

- whether the link is inside `.rs-main`
- whether it has `class="rs-nav-link"`
- whether it has `data-rs-page-key`
- whether the page key exists in the payload
- whether `href` uses the expected `/rs/...` route

## Styling And HTML Rendering

Current generated structure:

```html
<main class="rs-page" data-rs-page="rs_home">
  <style>...</style>
  <nav class="rs-main">...</nav>
  <section class="rs-section">...</section>
  <footer class="rs-footer">...</footer>
</main>
```

Current major class groups:

```text
rs-page
rs-main
rs-nav-inner
rs-nav-logo
rs-nav-links
rs-nav-link
rs-section
rs-section-container
rs-section-padding
rs-content
rs-content-flex
rs-visual
rs-type-primary
rs-type-secondary
rs-footer
```

Current `rs-content-flex` intent:

```text
left content column + right visual/media area
```

Example HTML stored through content:

```html
<div class="rs-content-flex">
  <div>
    <h5>Hero Section</h5>
    <h1>Built to Scale Cleanly</h1>
    <p>Full-width section skeleton using Webflow base typography defaults. Text elements have no font classes.</p>
  </div>
  <div class="rs-visual">visual / media area</div>
</div>
```

Important rendering rule:

If `rs_content.content_type` is `html`, content is inserted as HTML after basic sanitizing. Otherwise it is escaped as plain text.

## Static Payload Update Flow

When Airtable content changes and the public pages should reflect it:

1. Verify `/test/rs-page-render?pageKey=...&refresh=1` returns correct HTML.
2. Regenerate `webflow-cloud-test/src/lib/rs-page-static-payload.js` from the render endpoint for all live pages.
3. Run:

   ```powershell
   cd "C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus\webflow-cloud-test"
   npm run build
   ```

4. Deploy:

   ```powershell
   webflow cloud deploy --project-id d7d97751-20e1-4148-a5cf-ee58671c128a --site-id 6982268b7543ac3c80151266 --mount /test --framework astro --no-input --skip-update-check
   ```

5. Verify:

   ```text
   /test/rs-page-payload?pageKey=rs_home
   /test/rs-page-loader
   /rs/home
   ```

The payload route is the runtime speed layer. The render route is the source/compiler layer.

## Why The Static Payload Layer Exists

The direct Airtable renderer originally rebuilt every page from several Airtable tables on each request.

That created slow navigation because each click could require:

```text
rs_pages_index
rs_page_blocks
rs_page_divs
rs_typography
rs_content
rs_navigation_items
rs_global_params
```

For six navigation pages, preloading could multiply those reads.

The corrected runtime strategy is:

```text
Compile once
Serve fast payload
Use Airtable render endpoint only as compiler/fallback
```

This keeps the permanent Webflow embed stable while avoiding repeated Airtable reads during ordinary page navigation.

## Known Trouble And Watch Points

### 1. Webflow Cloud can override response caching

The render endpoint can return headers that effectively behave like:

```text
Cache-Control: no-cache, private
CF-Cache-Status: BYPASS
```

Do not rely only on HTTP cache for `/test/rs-page-render`.

Use `/test/rs-page-payload` for public runtime reads.

### 2. Edge isolate memory cache is not reliable as the only fix

`rs-page-render.js` has in-memory page/dataset caches. Those can help when requests hit the same isolate, but repeated calls can still miss because the platform may route requests to different isolates.

Do not treat isolate cache as the public performance contract.

### 3. Compiled payload can become stale

`rs-page-static-payload.js` is a snapshot.

If Airtable changes but the static payload is not regenerated and deployed, the public Webflow page will still show the previous compiled output.

### 4. Footer links may differ from `/rs/...`

Some footer payload output still contains links such as:

```text
/
/apps
/contact
```

The main nav uses `/rs/...`. Footer links should be reviewed in `rs_navigation_items` and normalized if they need to behave the same way.

### 5. Inline page style exists in generated HTML

The current renderer emits a `<style>` block inside each rendered page.

This works for proof and keeps the page self-contained, but the long-term cleaner model should move shared styling into a stable stylesheet or a managed HTML/CSS library table once the class contract is stable.

### 6. The older `site_toggle` mode still exists

`rs-page-render.js` still supports:

```text
mode=site_toggle
```

That mode uses a different toggle/panel model and inline click handlers. It is not the current preferred page model. The current model is the stable loader plus page payload.

### 7. Current renderer does not yet use field allow-lists

The renderer reads full records from each table. The next data hardening step should add allowed fields per table, especially before the model grows.

### 8. Current renderer does not yet use Airtable views

The renderer does not currently pass `view=` when reading records. It uses table-wide reads and code-side filtering.

Views can still help humans manage Airtable, but the runtime code is not yet using them as a read contract.

### 9. Sanitizing trusted HTML is basic

HTML content strips script tags and inline event attributes. It is not a full HTML sanitizer.

Only approved internal HTML patterns should be stored in HTML content rows.

### 10. Webflow page shell can conflict visually

The Webflow page must not include duplicate old static content, duplicate nav, or a styled skeleton that remains visible behind or around `#rs-page-root`.

The Webflow page should be reduced to the permanent embed area unless a surrounding shell is intentionally part of the page.

### 11. Loader updates need no embed changes, but browser cache can still hold a previous script briefly

The loader now returns `no-cache`, but browsers and Webflow/CDN layers can still briefly show older output during deployment propagation.

Verify with:

```text
https://ringstatus.com/test/rs-page-loader
```

and confirm it includes:

```text
rs-page-payload
```

### 12. Do not put business logic into Webflow embeds

Embeds should load the hosted script and provide the page key. Logic belongs in the repo or Airtable model.

## Codex Connections And Tools Used

### Local repo

Workspace:

```text
C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus
```

Main app folder:

```text
webflow-cloud-test
```

### Webflow CLI

Used to deploy Webflow Cloud app.

Known environment:

```text
CLI: @webflow/webflow-cli
Authenticated user: Philip Posa
Available command group: webflow cloud
```

The working deployment command is listed above.

### Webflow Cloud

Used as the secure server layer between Webflow pages and Airtable.

Current live mount:

```text
https://ringstatus.com/test
```

### Airtable API

The renderer calls Airtable directly from Webflow Cloud using:

```text
https://api.airtable.com/v0/{baseId}/{tableName}
Authorization: Bearer AIRTABLE_TOKEN
```

No browser-side Airtable token is used in this system.

### Airtable MCP / CLI

The repo has Airtable MCP CLI installed:

```text
@airtable/mcp-cli@0.2.5
```

This can be used for schema inspection and table/record operations when available, but the runtime renderer itself uses direct Airtable REST calls from Webflow Cloud.

### Browser verification

The in-app browser was used to verify the published page:

```text
https://ringstatus.com/rs/home
```

Verified behavior:

- `#rs-page-root` exists
- loader script is present
- `rs_home` renders
- nav has six links
- clicking Members routes to `/rs/members`
- `rs_members` renders
- no console errors were observed in that check

### Local build verification

Used:

```powershell
node --check webflow-cloud-test/src/pages/rs-page-loader.js
node --check webflow-cloud-test/src/pages/rs-page-payload.js
node --check webflow-cloud-test/src/lib/rs-page-static-payload.js
npm run build
```

## Verification Commands

Check loader:

```powershell
Invoke-WebRequest -Uri "https://ringstatus.com/test/rs-page-loader" -UseBasicParsing
```

Check compiled payload:

```powershell
Invoke-WebRequest -Uri "https://ringstatus.com/test/rs-page-payload?pageKey=rs_members" -UseBasicParsing
```

Check dynamic render source:

```powershell
Invoke-WebRequest -Uri "https://ringstatus.com/test/rs-page-render?pageKey=rs_members&refresh=1" -UseBasicParsing
```

Check all compiled pages:

```powershell
Invoke-WebRequest -Uri "https://ringstatus.com/test/rs-page-payload?all=1" -UseBasicParsing
```

## Current Verified State

The last live verification showed:

```text
/test/rs-page-loader -> 200, payload-aware, no-cache
/test/rs-page-payload?pageKey=rs_home -> 200
/test/rs-page-payload?pageKey=rs_about_me -> 200
/test/rs-page-payload?pageKey=rs_about_company -> 200
/test/rs-page-payload?pageKey=rs_apps -> 200
/test/rs-page-payload?pageKey=rs_contact -> 200
/test/rs-page-payload?pageKey=rs_members -> 200
/rs/home -> rendered rs_home
/rs/home click Members -> rendered rs_members
```

## Management Rules Going Forward

1. Do not change Webflow embeds unless the stable loader contract changes.
2. Use Airtable to manage page rows, blocks, divs, typography, content, navigation, and globals.
3. Use `/test/rs-page-render?refresh=1` to inspect the current Airtable-rendered source.
4. Regenerate `rs-page-static-payload.js` when Airtable content is ready to publish.
5. Deploy Webflow Cloud after regenerating the payload.
6. Verify `/test/rs-page-payload` and the exact live Webflow page.
7. Keep Webflow pages free of duplicate static skeletons that compete with the injected render.
8. Keep Airtable tokens server-side only.
9. Add field allow-lists and view-based reads before expanding this into larger page systems.
10. Treat `rs_home` as the canonical end-to-end reference until all pages have equivalent complete chains.

## Immediate Next Improvements

Priority items:

1. Add a generator script for `rs-page-static-payload.js` so the compile step is repeatable and not manual.
2. Normalize footer navigation hrefs to `/rs/...`.
3. Move shared CSS out of inline page output once the class contract is accepted.
4. Add Airtable field allow-lists per table.
5. Add Airtable view support to `listRecords`.
6. Add a published-page smoke test that clicks all six nav items and asserts rendered page keys.
7. Document which Airtable tables are runtime-active versus blueprint/reference only.

