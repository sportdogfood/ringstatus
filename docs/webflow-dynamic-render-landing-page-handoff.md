# Webflow Landing Page Template/CSS Handoff

## Objective

Add a landing-page template and CSS path to the existing RingStatus Webflow dynamic render app without creating a separate app, separate endpoint family, or unrelated schema.

The work should extend the current model:

`Webflow page embed -> Webflow Cloud route -> Airtable rscom -> rendered HTML/CSS-aware classes`

## Current Runtime Connection

- Webflow site: RingStatus
- Webflow site id: `6982268b7543ac3c80151266`
- Webflow Cloud project id: `d7d97751-20e1-4148-a5cf-ee58671c128a`
- Cloud mount: `/test`
- Active render endpoint: `https://ringstatus.com/test/rs-page-render`
- Local route file: `webflow-cloud-test/src/pages/rs-page-render.js`
- Airtable base: `rscom`
- Airtable base id: `appDN3R51ZPmwgMib`

## Existing Webflow Embed Pattern

Use this shape on a Webflow page:

```html
<div id="rs-page-root">Loading RingStatus page...</div>

<script>
  (function () {
    var root = document.getElementById("rs-page-root");
    if (!root) return;

    var endpoint = new URL("https://ringstatus.com/test/rs-page-render");
    endpoint.searchParams.set("mode", "site_toggle");
    endpoint.searchParams.set("pageKey", "rs_home");
    endpoint.searchParams.set("_", Date.now());

    root.setAttribute("data-rs-model", "site_toggle_renderer");
    root.setAttribute("data-rs-page-key", "rs_home");
    root.setAttribute("data-rs-status", "loading");

    fetch(endpoint.toString(), { cache: "no-store" })
      .then(function (response) { return response.json(); })
      .then(function (data) {
        if (!data || !data.ok) {
          throw new Error((data && (data.detail || data.error)) || "Render failed");
        }
        root.innerHTML = data.html || "";
        root.setAttribute("data-rs-status", "ready");
      })
      .catch(function (error) {
        root.setAttribute("data-rs-status", "failed");
        root.textContent = "Render failed: " + (error && error.message ? error.message : error);
      });
  })();
</script>
```

For a single landing page stack, omit `mode=site_toggle` or use a future `mode=landing_page` only if the route implements it.

## Airtable Table Chain

The current proof model is:

1. `rs_projects_index`
   - project-level entry.
   - active project opens the model.
2. `rs_pages_index`
   - page rows.
   - current project: `ringstatus_front_facing`.
3. `rs_page_blocks`
   - page stack rows.
   - broad `block_type`: `navigation`, `section`, `footer`.
   - clearer `block_role`: `navigation`, `hero`, `intro`, `feature`, `proof`, `details`, `list`, `form`, `cta`, `footer`.
4. `rs_section_inventory`
   - approved reusable section patterns.
   - use this for landing-page templates instead of one-off HTML.
5. `rs_section_slots`
   - named slots inside section patterns.
   - examples: `hero_content`, `intro_content`, `navigation_content`, `footer_content`.
6. `rs_page_divs`
   - concrete div/layout rows linked to blocks and section slots.
7. `rs_typography`
   - text roles/classes/data keys.
8. `rs_content`
   - actual copy rendered into typography rows.

`rs_home` is the canonical working reference. Follow its links end to end before changing other pages.

## Existing Working Reference

`rs_home` has a complete visible chain:

`rs_home -> rs_home_section_1 -> hero section inventory -> hero_content slot -> rs_home_section_1_content div -> rs_home_section_1_headline typography -> rs_content`

Other pages have been linked through section inventory and slots where rendered blocks exist:

- `rs_home`
- `rs_about_me`
- `rs_about_company`
- `rs_app`
- `rs_contact`
- `rs_members`

Known gap:

- `rs_apps` currently has no rendered blocks in the endpoint output.

## Landing Page Template Work

Add landing-page templates by extending, not replacing, these tables:

- Add approved landing section patterns to `rs_section_inventory`.
- Add slot definitions to `rs_section_slots`.
- Add page-specific block rows to `rs_page_blocks`.
- Add div rows to `rs_page_divs`.
- Add typography rows to `rs_typography`.
- Add text/content rows to `rs_content`.

Recommended initial landing section roles:

- `hero`
- `intro`
- `feature`
- `proof`
- `cta`
- `footer`

If a section contains repeating cards:

- Use `rs_repeatable_item_types`.
- Add a section pattern such as `card_grid`.
- Add a slot such as `cards`.
- Store the repeatable content as rows, ordered by `sort_order`.
- Do not hardcode card count in JS.

If a section contains video:

- Prefer a media source table before raw embeds.
- Store structured values: `provider`, `video_id`, `video_url`, `embed_url`, `thumbnail_url`, `title`, `sort_order`, `active`.
- Renderer should build safe iframe/embed HTML from structured fields.

## CSS Expectations

Do not create one-off styles per page.

Use a shared landing-page CSS layer/class contract:

- section shell class
- section container class
- section padding class
- content class
- typography classes
- card/grid classes if repeatable

CSS should be global enough for all landing pages, but scoped enough to avoid breaking the packing/WEC surfaces.

Avoid changing the approved render root:

- `#rs-page-root`
- `.rs-page`
- `data-rs-page`
- `data-rs-block`
- `data-rs-div`
- `data-rs-value`

If new CSS is needed, add a clearly named shared landing class family, for example:

- `.rs-landing`
- `.rs-landing-section`
- `.rs-landing-container`
- `.rs-landing-grid`
- `.rs-landing-card`

Do not use inline visual hacks as the final model.

## Webflow Integration Tables

Relevant registry tables already exist:

- `webflow_integrations`
- `webflow_project_meta`
- `rs_webflow_cloud`
- `rs_webflow_embeds`
- `rs_embed_versions`
- `rs_airtable_sources`
- `rs_render_bindings`

Use these to document:

- page URL
- page id
- embed key
- endpoint URL
- cloud mount
- asset version
- script/CSS source
- active status

Runtime does not require Webflow MCP.

MCP/CLI/API roles:

- Webflow CLI: deploy Webflow Cloud app to `/test`.
- Webflow MCP: useful for page metadata, embed placement, and safe publishing.
- Webflow API: optional fallback for page/site metadata.
- Webflow Cloud/Astro: current runtime route.

## Important Security Boundary

Do not put Airtable tokens in Webflow embeds.

Correct path:

`Webflow page -> Webflow Cloud endpoint -> Airtable`

Incorrect path:

`Webflow page -> Airtable token in browser`

The LP history edit example currently documented a browser token pattern as a use case to replace, not repeat.

## Current LP History Use Cases Documented In Airtable

Added to `webflow_integrations`:

- `lp_history_public_page`
- `lp_history_edit_page`
- `lp_history_enrichment_endpoint`
- `lp_profile_content_endpoint`

Added to `webflow_project_meta`:

- `/lph`
- `/lph-edit`
- public asset version
- edit data/CSS asset version
- edit script asset version
- public history JSON URL
- public layer JSON URL
- enrichment URL
- profile content URL
- Airtable source reference without token

## Verification Requirements

Before finalizing any template/CSS change:

1. Browser/render check on the exact Webflow page.
2. API/data check on `/test/rs-page-render`.
3. Embed parity check if an embed snippet changed.
4. State result as pass/fail.

Do not claim done if the page was not rendered and checked.

## Suggested Next Step

Build one landing-page section on `rs_home` using only:

`rs_section_inventory -> rs_section_slots -> rs_page_blocks -> rs_page_divs -> rs_typography -> rs_content`

Then update the renderer only if it needs to read the section inventory/slot layer directly.
