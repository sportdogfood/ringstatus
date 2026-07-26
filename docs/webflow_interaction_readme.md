# Webflow Interaction README

This document is the operating guide for RingStatus Webflow work from Codex, MCP, the Webflow CLI, and local component projects.

## Confirmed Webflow Site

```text
site name: ringstatus
siteId: 6982268b7543ac3c80151266
workspaceId: 630d3c2e9ffb8f662c77306d
production domain: ringstatus.com
staging domain: ringstatus.webflow.io
timezone: America/New_York
```

Verified MCP/Bridge test page:

```text
pageId: 6a0fa27816024003404ffeed
slug: /thispage
title: thispage
```

## Interaction Surfaces

### Data API

Use the Webflow Data API for site and content operations that do not need an open Designer canvas.

Typical RingStatus uses:

- list sites and confirm `siteId`
- inspect page metadata
- list CMS collections and items
- manage CMS drafts and published items
- register hosted scripts
- apply registered scripts to a site or page
- inspect and manage assets
- inspect webhooks
- publish a site after explicit approval

Data API actions can run without the Webflow MCP Bridge App.

Official docs:

- https://developers.webflow.com/data/reference
- https://developers.webflow.com/reference

### Current RingStatus Data Flow

Most current RingStatus Webflow modules do not use the Webflow Data API as their primary application database.

The current pattern is usually:

```text
Webflow page
  -> embed or hosted frontend JS
  -> Webflow Cloud API route
  -> Airtable API
  -> Airtable records and change-log tables
```

Examples in this repo:

```text
webflow-cloud-test/src/pages/hps/horses.js
webflow-cloud-test/src/pages/lp-profile/content.js
webflow-cloud-test/src/pages/lp-history/enrichment.js
```

Those routes read secrets from Webflow Cloud / Cloudflare runtime env vars, call Airtable server-side, and return JSON to the browser. This keeps Airtable tokens out of Webflow embeds.

For example, `webflow-cloud-test/src/pages/hps/horses.js` uses:

```text
AIRTABLE_TOKEN
AIRTABLE_BASE_ID or AIRTABLE_BASE
AIRTABLE_HPS_HORSES_TABLE
AIRTABLE_HPS_VIEW_PREFIX
AIRTABLE_HPS_CHANGE_LOG_TABLE
AIRTABLE_HPS_ACTIVE_TENANTS_TABLE
AIRTABLE_HPS_ACTIVE_TENANTS_VIEW
```

So, in RingStatus terms:

```text
Webflow Data API:
  Used to manage Webflow itself: pages, CMS, assets, scripts, publish state.

RingStatus application data API:
  Custom Webflow Cloud routes that read/write Airtable for live app data.
```

Do not confuse these. The Webflow Data API can manage Webflow CMS items, but the current HPS, LP profile, and LP history app-data flows are Airtable-backed custom APIs.

### Astro and Webflow Cloud

Astro is the framework currently used by the `webflow-cloud-test/` Webflow Cloud app.

Relevant files:

```text
webflow-cloud-test/webflow.json
webflow-cloud-test/astro.config.mjs
webflow-cloud-test/package.json
webflow-cloud-test/src/pages/health.js
webflow-cloud-test/src/pages/hps/horses.js
webflow-cloud-test/src/pages/lp-profile/content.js
webflow-cloud-test/src/pages/lp-history/enrichment.js
```

`webflow-cloud-test/package.json` uses Astro:

```json
{
  "scripts": {
    "dev": "astro dev",
    "build": "astro build",
    "preview": "astro preview"
  },
  "dependencies": {
    "@astrojs/cloudflare": "^13.5.2",
    "astro": "^6.2.2"
  }
}
```

`astro.config.mjs` configures Astro for server output on Cloudflare:

```js
export default defineConfig({
  output: 'server',
  adapter: cloudflare({
    imageService: 'passthrough'
  })
});
```

Astro's role is not visual Webflow editing. It is the server/API layer deployed by Webflow Cloud.

In practice:

```text
Webflow Designer:
  Owns the visual page and embed placement.

Static frontend JS/CSS:
  Renders the RingStatus module in the browser.

Astro on Webflow Cloud:
  Serves API routes for secure Airtable reads/writes.

Airtable:
  Stores current RingStatus app data.

Webflow Data API:
  Manages Webflow site objects, CMS, assets, scripts, and publish state.
```

### Designer API

Use the Designer API for visual canvas work inside Webflow Designer.

Typical RingStatus uses:

- switch the active Designer page
- inspect the current page element tree
- query elements by id, type, text, tag, attribute, or style
- create Webflow-native elements
- create or update styles
- apply classes to elements
- inspect and manage variables
- inspect and manage components, props, slots, and variants

Under Webflow MCP 2.0, most Designer-data operations do not require an open Designer session. Element-tree, component, style, variable, and page-building operations run directly through MCP.

The MCP Bridge App is required only for:

- element snapshots
- selection and canvas navigation
- reading the current page, mode, branch, or breakpoints
- creating page folders
- uploading an image directly from a public URL

Official docs:

- https://developers.webflow.com/designer/reference/overview
- https://developers.webflow.com/designer/reference/elements-overview
- https://developers.webflow.com/designer/reference/styles-overview
- https://developers.webflow.com/designer/reference/variables-detail-overview

### MCP Server

Use the Webflow MCP server when Codex or another AI client should call Webflow tools directly.

Production MCP endpoint:

```text
https://mcp.webflow.com/mcp
```

Beta MCP endpoint:

```text
https://mcp.webflow.com/beta/mcp
```

Docs-only MCP endpoint:

```text
https://developers.webflow.com/api/fern-docs/mcp
```

The production MCP server exposes Data API and Designer-data tools. Under MCP 2.0, elements, components, styles, variables, and page-building actions run without the MCP Bridge App.

Official docs:

- https://developers.webflow.com/mcp/reference/overview
- https://developers.webflow.com/mcp/reference/how-it-works

### MCP Bridge App

Use the MCP Bridge App only for the MCP 2.0 capabilities that still depend on a live Designer session.

Required for:

- element snapshots
- selecting or reading the selected element
- canvas navigation between pages and component views
- reading the current page, mode, branch, or breakpoints
- creating page folders
- uploading an image from a public URL

Not required for:

- listing sites or reading page metadata
- reading or editing page element trees
- creating native elements
- creating or managing components and instances
- creating or managing styles and variables
- CMS, scripts, assets, forms, Analyze, or sitemap operations

Connection pattern:

```text
1. Open the target site in Webflow Designer.
2. Open Apps in Designer.
3. Launch the Webflow MCP Bridge App.
4. Wait for the bridge to connect.
5. Run MCP Designer tools from Codex.
```

### Webflow CLI

The Webflow CLI is installed globally on this workstation.

Verified:

```text
webflow --version
1.22.0
```

Useful CLI command groups:

```text
webflow auth       authentication and configuration
webflow sites      manage or inspect Webflow sites
webflow cms        manage CMS content
webflow assets     manage assets
webflow devlink    manage Webflow components and DevLink
webflow library    manage Code Components libraries
webflow extension  develop Designer Extensions
webflow cloud      manage Webflow Cloud projects
```

Run CLI commands from a project folder, not `C:\WINDOWS\System32`.

Example:

```powershell
cd "C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus"
webflow auth --help
webflow sites --help
webflow devlink --help
webflow library --help
```

### Code Components

Use Code Components when RingStatus needs React components that can be used directly inside Webflow.

Good fit for:

- reusable interactive UI
- React state, hooks, and effects
- configurable props exposed to Webflow designers
- shared component libraries across pages or sites
- a structured alternative to one-off HTML embeds

Code Components are not the same as HTML embeds. They are packaged React components that Webflow can expose in the Designer as reusable components.

Local clean project created for this lane:

```text
C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus-webflow-components
```

Installed packages:

```text
@webflow/data-types@1.3.0
@webflow/react@1.3.0
```

Official docs:

- https://developers.webflow.com/code-components/introduction
- https://developers.webflow.com/code-components/importing/quick-start

### DevLink

Use DevLink when syncing components between a React codebase and Webflow.

Good fit for:

- importing React components into Webflow
- exporting Webflow components for React use
- maintaining a component library outside Webflow
- connecting Webflow design work with application code

For a pure RingStatus component-library experiment, start with `webflow library` and Code Components. Use `webflow devlink` when the goal is specifically syncing Webflow components with a React app.

Official docs:

- https://developers.webflow.com/devlink/introduction/getting-started

## Proven MCP Edit Sequence

The first successful MCP Bridge edit used this sequence:

```text
1. list_sites
2. get_page_metadata for pageId 6a0fa27816024003404ffeed
3. page_tool.switch_page
4. page_tool.get_current_page
5. element_tool.get_all_elements
6. element_builder created a Section
7. style_tool.query_styles for padding and section styles
8. style_tool.create_style created thispage-section-padding
9. element_tool.set_style applied the class
10. element_tool.query_elements verified the target section
```

Created section:

```json
{
  "component": "6a0fa27816024003404ffeed",
  "element": "8eade920-06f6-2186-a670-903d37baaa97"
}
```

Created style:

```text
thispage-section-padding
```

Properties:

```css
padding-top: 32px;
padding-right: 32px;
padding-bottom: 32px;
padding-left: 32px;
```

## Standard Safe Workflow

Use this order for Webflow page, style, or component changes:

```text
1. Confirm site.
2. Confirm page or component.
3. Open Designer and connect MCP Bridge App if canvas changes are needed.
4. Inspect current state before writing.
5. Query existing styles/components before creating new ones.
6. Prefer existing style names and component contracts.
7. Make one scoped change.
8. Verify by exact element, style, page, or component ID.
9. Keep publish as a separate explicit approval step.
```

## Approval Gates

These actions require explicit approval before running:

- `publish_site`
- deleting pages, branches, elements, styles, CMS items, assets, scripts, or variables
- `set_site_scripts` or `set_page_scripts`, because they replace the whole script list
- broad variable refactors across many styles
- edits to production Webflow pages that are not a test page
- updates to shared RingStatus CSS hooks such as `lp-*`, `packing-*`, or shared root aliases

Prefer these safer single-target operations:

- `add_site_script`
- `add_page_script`
- create one class
- apply one class
- create one element
- update one page setting
- query before mutate

## RingStatus Usage Rules

- Do not guess site IDs, page IDs, component IDs, collection IDs, or field names.
- Verify the active Webflow site and target page before changing anything.
- Preserve existing manual embed contracts unless the task explicitly moves that page to MCP-managed structure.
- Keep connector duplication scoped to new files or new Webflow pages unless a shared contract update is approved.
- Do not publish automatically after Designer edits.
- For visual proof, inspect the current Designer/page state or live page state directly; do not rely on stale screenshots.

## Choosing The Right Path

```text
Need CMS, scripts, assets, webhooks, page metadata:
  Use Data API via MCP.

Need sections, styles, variables, components, or page-tree changes:
  Use Webflow MCP 2.0 directly.

Need snapshots, selection, current canvas state, breakpoints, or page folders:
  Use Webflow MCP 2.0 with the Bridge App.

Need AI-assisted Webflow actions from Codex:
  Use Webflow MCP.

Need a reusable React component available inside Webflow:
  Use Code Components and webflow library.

Need syncing between Webflow components and a React app:
  Use DevLink.

Need the fastest hosted app injection:
  Use a manual embed or registered hosted script.
```

## Local Commands

Check CLI:

```powershell
webflow --version
webflow --help
```

Inspect Webflow command groups:

```powershell
webflow auth --help
webflow sites --help
webflow devlink --help
webflow library --help
webflow extension --help
```

Clean component project:

```powershell
cd "C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus-webflow-components"
npm ls @webflow/data-types @webflow/react
webflow library --help
webflow devlink --help
```

## Notes

- Webflow Designer style writes should use longhand CSS properties, not shorthand.
- Variables refactors should start in audit-only mode.
- Use existing variables when semantics match.
- Do not repurpose a similarly named variable if its meaning differs.
- Use narrowly named test classes on test pages.
- For production pages, prefer established RingStatus classes and contracts.
