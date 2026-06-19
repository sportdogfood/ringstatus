# Webflow Custom Pages New Runner Handoff

Date: 2026-06-19

Purpose: give a new Codex runner enough context to work on a similar Webflow + Airtable custom-pages project without re-educating it from scratch.

## First Rules

Do not guess the current contract. Inspect the live source, Airtable schema, Webflow page, and endpoint output before changing anything.

Do not rebuild the visual system from memory. The approved kitchen-sink source exists in the repo and is the reference for navigation, section shells, typography, drawers, footer, CSS, and shared JS.

Do not put Airtable tokens in browser embeds. Browser pages call Webflow Cloud. Webflow Cloud calls Airtable.

Do not keep changing Webflow embeds. The intended model is one stable embed per page that loads a hosted script. Future changes should happen in repo, Airtable, or Webflow Cloud unless the embed contract itself is explicitly being replaced.

Do not split templates by arbitrary div depth. Split only by approved semantic boundaries and reference those pieces by `template_id`.

## Current Repo Anchors

Primary repo:

```text
C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus
```

Primary custom-page docs:

```text
docs/webflow_custom_pages_system_final_overview.md
docs/webflow-dynamic-render-landing-page-handoff.md
docs/webflow_custom_pages_new_runner_handoff_2026-06-19.md
```

Primary Webflow Cloud app:

```text
webflow-cloud-test/
```

Current page-render runtime:

```text
webflow-cloud-test/src/pages/rs-page-loader.js
webflow-cloud-test/src/pages/rs-page-render.js
webflow-cloud-test/src/pages/rs-page-payload.js
webflow-cloud-test/src/lib/rs-page-static-payload.js
```

Approved template source:

```text
webflow/rs-template-system/master_ks/kitchen_sink_template.html
webflow/rs-template-system/master_ks/kitchen_sink_template.manifest.json
webflow/rs-template-system/master_ks/00_APPROVED_BASE_DO_NOT_REBUILD.html
webflow/rs-template-system/master_ks/CONTRACT.json
webflow/rs-template-system/master_ks/rs-global.css
webflow/rs-template-system/master_ks/rs-scripts.js
```

## Current Rendering Model

The working model is:

```text
Webflow page
  -> stable embed
  -> hosted loader script
  -> compiled page payload endpoint
  -> rendered HTML injected into #rs-page-root
```

The dynamic source model is:

```text
Airtable rscom
  -> page/project/block/section/content/media/navigation rows
  -> Webflow Cloud renderer
  -> compiled HTML payload
  -> Webflow page loader
```

The fast public path should read a compiled payload first. Airtable-backed rebuilds are for compile/refresh/debug, not for rebuilding every public page on every click.

## Stable Webflow Embed Contract

Use one stable embed per Webflow page. Only the `pageKey` changes.

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

Examples:

```text
/rs/home       -> rs_home
/rs/about-me   -> rs_about_me
/rs/company    -> rs_about_company
/rs/apps       -> rs_apps
/rs/contact    -> rs_contact
/rs/members    -> rs_members
```

Do not paste full fetch logic into each Webflow page once the loader is in place.

## Webflow Integration

Current known Webflow Cloud integration:

```text
Webflow site: ringstatus
Site id: 6982268b7543ac3c80151266
Webflow Cloud project id: d7d97751-20e1-4148-a5cf-ee58671c128a
Mount path: /test
Framework: Astro
Runtime: Cloudflare edge via @astrojs/cloudflare
Local app folder: webflow-cloud-test
```

Webflow CLI is used for Webflow Cloud build/deploy.

Webflow MCP/skills should be used when the work requires Webflow page, Designer, custom-code, component, or publish operations. Available Webflow-related skills in this environment include:

```text
webflow-cli:cloud
webflow-cli:code-component
webflow-mcp:custom-code-management
webflow-mcp:designer-tools
webflow-mcp:flowkit-naming
webflow-mcp:safe-publish
```

Use them by purpose:

```text
webflow-cli:cloud             -> build/deploy Webflow Cloud app
webflow-cli:code-component    -> reusable React code components for Webflow Designer
webflow-mcp:custom-code-management -> inspect/add/remove site/page custom code
webflow-mcp:designer-tools    -> inspect/create/manage Webflow pages/elements/styles
webflow-mcp:flowkit-naming    -> class naming audits and consistency
webflow-mcp:safe-publish      -> publish workflow with checks
```

Do not claim MCP has updated Webflow unless the tool actually ran and returned evidence.

## Airtable Integration

The front-facing custom pages use the `rscom` Airtable base.

Known base id from existing docs:

```text
appDN3R51ZPmwgMib
```

Airtable should be inspected live before writes. Use Airtable connector/MCP/CLI when available. Available Airtable skills include:

```text
airtable:airtable-cli
airtable:airtable-filters
airtable:airtable-overview
```

Expected use:

```text
airtable:airtable-cli      -> list bases, inspect tables/fields, read/write records
airtable:airtable-filters  -> filter records by field values/views
airtable:airtable-overview -> explain schema/table/view concepts if needed
```

Server-side API routes should read only required fields where possible. The purpose of allowed-field registries is to avoid pulling full table payloads when a page or component only needs a few fields.

## Airtable Page Stack Model

The current conceptual stack is:

```text
project
  -> pages
    -> page blocks
      -> section inventory
        -> section slots
          -> page divs
            -> typography
              -> content
```

Current table names used in the working model:

```text
rs_projects_index
rs_pages_index
rs_page_blocks
rs_section_inventory
rs_section_slots
rs_page_divs
rs_typography
rs_content
```

Working reference chain:

```text
rs_home
  -> rs_home_section_1
  -> hero section inventory
  -> hero_content slot
  -> rs_home_section_1_content div
  -> typography rows
  -> rs_content rows
```

Treat `rs_home` as the first reference page before changing other pages.

## How Sections Stack

A page is not one HTML blob. It is a stack of blocks.

Recommended page block roles:

```text
navigation
hero
intro
feature
proof
details
list
form
cta
footer
```

A section should resolve to a known template pattern. The approved section shell order from the template contract is:

```text
rs-section
rs-section-container
rs-section-padding
rs-content-container
rs-content-flex
rs-content
```

The renderer should fill approved slots such as:

```text
eyebrow
headline
body
image
video
cards
cta
```

Do not use raw `h1` and `p` as the model. The system needs typed content slots that can render into a template.

## Template ID Model

The approved kitchen-sink template is stored as:

```text
webflow/rs-template-system/master_ks/kitchen_sink_template.html
```

Its manifest is:

```text
webflow/rs-template-system/master_ks/kitchen_sink_template.manifest.json
```

Approved `template_id` strategy:

```text
kitchen_sink_template -> full approved source
rs_nav_main           -> main navigation candidate
rs_nav_mega           -> mega navigation candidate
rs_tools_drawer       -> drawer navigation candidate
rs_page_main          -> main page stack candidate
rs_section            -> section candidate
rs_footer             -> footer candidate
```

These are candidates from the approved source. Before using one as a live component, copy it from the approved source, verify the selector boundary, and mark it approved in the relevant Airtable/repo registry.

## Isolating Content, Markup, Images, Video, And Navigation

Do not overload one table with everything.

Use separate concepts:

```text
markup/template source -> repo template files or rs_html_lib/pak_html_lib style table
page structure         -> rs_pages_index and rs_page_blocks
section patterns       -> rs_section_inventory and rs_section_slots
div/layout placement   -> rs_page_divs
typography roles       -> rs_typography
copy/content           -> rs_content
images/media           -> dedicated media fields or media table
videos                 -> structured video/media table
navigation             -> nav/page rows, not hardcoded links in every page
```

### Content

`rs_content` should support broken-out editable fields such as:

```text
eyebrow
headline
body
content_value
content_type
active
sort
page_key
```

Prefer broken-out fields for normal page copy. Use `content_value` for approved rich HTML only when that is truly the content type.

### Images

Do not bury images inside raw HTML when they need to be managed.

Recommended structured fields:

```text
image_url
image_alt
image_title
image_credit
image_role
image_fit
image_position
active
sort
```

If an image belongs to a content block, link it to the content/block/slot row instead of duplicating it in multiple places.

### Video

Do not paste uncontrolled iframe blobs as the default model.

Recommended structured fields:

```text
provider
video_id
video_url
embed_url
thumbnail_url
title
caption
active
sort
```

Renderer builds safe embed HTML from provider fields. Raw embed code can exist for exceptional trusted cases, but it should be clearly typed as trusted embed content.

### Navigation

Navigation should come from the page model, not separate hardcoded HTML on every Webflow page.

The approved navigation direction is the modern `rs-nav` structure with:

```text
rs-nav
rs-nav-inner
rs-logo
rs-nav-links
rs-nav-button
rs-nav-link
rs-mega
rs-drawer
rs-nav-scrim
body.is-nav-locked
```

Known app pages requested under Apps:

```text
rs_schedules
rs_waze
rs_onsite
rs_twoway
rs-pak
rs-reminders
rs-boards
rs-alerts
```

The nav active class must reflect the current page. Do not ship a nav where every page renders but no active state is applied.

## Repeatable Cards, Tables, And Lists

For repeatable card sections:

```text
section inventory -> card grid pattern
section slot      -> cards
repeatable table  -> card rows
sort field        -> controls order
active field      -> controls inclusion
```

Use a repeatable item/type table when the same pattern can power blogs, apps, member content, testimonials, product cards, or list cards.

Do not hardcode card count in JS.

For data tables:

```text
table source
view/source filter
allowed fields
columns
sort
actions
drawer/detail behavior
```

Tables should not invent counts or fallback rows. If the source table/view has no data, render empty state from the model.

## Runtime And Caching

The public path should avoid rebuilding from Airtable on every page load.

Preferred runtime:

```text
Webflow page
  -> rs-page-loader
  -> rs-page-payload?pageKey=x
  -> sessionStorage/in-memory cache
  -> preloads nav pages
```

Dynamic refresh path:

```text
rs-page-render?pageKey=x&refresh=1
```

Compile/update path:

```text
Airtable change
  -> automation or manual compile trigger
  -> regenerate static payload
  -> deploy or update payload endpoint
```

Known issue: Webflow Cloud can return `no-cache, private`. Do not rely only on HTTP cache. Use browser-side loader cache and/or a compiled/static payload endpoint.

## Airtable Automations

If Airtable content edits must appear without changing Webflow embeds, create an automation that calls a compile/refresh endpoint.

Automation should:

```text
trigger on rs_content relevant fields
send recordId/pageKey if available
POST to Webflow Cloud compile/refresh endpoint
log response
fail loudly if endpoint fails
```

Do not hardcode a single page forever if the content row has a page key. Use the row’s linked page/page key where possible.

## Known Trouble

1. Slow page loads happen if every request rebuilds all pages from Airtable.
2. Webflow embeds become unmanageable if each page has custom fetch/render code.
3. Navigation can drift if it is manually duplicated across pages.
4. Active nav state must be derived from `pageKey` or current path.
5. The approved nav should not be mixed with older `rs-site-toggle` UI.
6. Do not keep `rs-site-toggle-panel-head` if the current approved layout does not use it.
7. Do not use `content_value` rich HTML when broken-out `eyebrow`, `headline`, and `body` fields are the intended source.
8. Do not mix old placeholder templates with approved `rs-content-flex`/section shells.
9. Do not introduce `lp-` classes into this RS template system.
10. Do not create one-off CSS for a single page if it belongs in the global template system.
11. Do not silently add `!important` to solve layout problems.
12. Do not add arbitrary absolute positioning unless the stacking/overlay contract requires it and is verified.
13. Do not claim Airtable changes are live if the compiled payload has not been refreshed.
14. Do not claim Webflow pages are updated unless the live page was opened and verified.
15. Do not expose Airtable tokens in client-side custom code.

## Expected Verification

Before saying PASS:

```text
1. Inspect exact source file(s).
2. Inspect Airtable schema/records if data changed.
3. Run syntax/build checks for edited JS.
4. Verify endpoint output.
5. Verify the exact live Webflow page.
6. Verify nav click/active state if nav changed.
7. Verify mobile width when CSS/layout changed.
8. Confirm no unrelated legacy/old template path is being used.
```

Useful local checks:

```text
node --check webflow-cloud-test/src/pages/rs-page-loader.js
node --check webflow-cloud-test/src/pages/rs-page-render.js
npm run build
```

Browser checks should inspect actual DOM, not just screenshot impressions.

## Handoff For A Similar Project

For a similar project, start with this order:

```text
1. Create/confirm Airtable project/page/block/content/media/navigation tables.
2. Store approved full template in repo.
3. Create template manifest with template_id boundaries.
4. Build Webflow Cloud endpoint that reads Airtable server-side.
5. Build compiled payload endpoint for public pages.
6. Build one stable Webflow embed that loads a hosted script.
7. Add navigation once, driven by page records.
8. Add content sections through table stack.
9. Add images and videos as separate structured sources.
10. Add Airtable automation or manual compile endpoint.
11. Verify one full page live.
12. Duplicate the model to the next page only after the first page is clean.
```

The goal is not “dynamic everything at runtime.” The goal is editable Airtable-managed structure with fast, stable Webflow output.

