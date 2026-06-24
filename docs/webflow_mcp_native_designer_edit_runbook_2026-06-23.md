# Webflow MCP Native Designer Edit Runbook

Date: 2026-06-23

## Purpose

This documents the verified path used to make a native Webflow Designer edit from Codex through Webflow MCP. It is specifically for cases where the goal is to create or inspect actual Webflow page elements, not inject custom-code embeds.

## Confirmed Target

- Site: `ringstatus`
- Site ID: `6982268b7543ac3c80151266`
- Target page used in the successful run: `/rs/home`
- Target page ID: `6a308af40c4cb10adb7432aa`
- Designer mode confirmed: `design`

## Important Reauth Note

If Codex cannot see Webflow tools, reauthenticate or reconnect the Webflow MCP connection and check again.

Failure symptom:

- `tool_search` for `webflow` returns `Found 0 tools`, or returns only unrelated tools.
- Required tools such as `webflow_guide_tool`, `data_sites_tool`, `de_page_tool`, `element_tool`, `whtml_builder`, `element_builder`, or `style_tool` are missing.

Successful symptom:

- `tool_search` for `webflow` exposes namespace `mcp__webflow`.
- Required Designer tools are callable.

## Required Tool Order

The Webflow MCP Designer skill requires this order:

1. Call `webflow_guide_tool` first.
2. Confirm the site with `data_sites_tool`.
3. Confirm the active Designer page with `de_page_tool`.
4. Inspect the element tree with `element_tool`.
5. Capture a before snapshot with `element_snapshot_tool` when changing visible page structure.
6. Create native elements with Webflow Designer tools.
7. Verify exact element IDs or DOM IDs.
8. Capture an after snapshot.
9. Publish only after separate approval.

## Verified Sequence From This Run

1. `webflow_guide_tool` was called first.
2. `data_sites_tool` confirmed:
   - `ringstatus`
   - `6982268b7543ac3c80151266`
3. `de_page_tool` confirmed active page:
   - page ID `6a308af40c4cb10adb7432aa`
   - page name `home`
   - slug `home`
   - parent folder `rs`
   - mode `design`
4. `element_tool` inspected the current page tree.
5. The body element was identified as:
   - `{ component: "6a308af40c4cb10adb7432aa", element: "6a25847710db1a130a33dda9" }`
6. `element_snapshot_tool` captured the body before changes.
7. `whtml_builder` inserted the approved native Webflow element structure at the top of `body`.
8. `element_tool` verified the required inserted DOM IDs:
   - `data-nav-root`
   - `data-mega-menu`
   - `data-nav-scrim`
   - `data-drawer-menu`
9. `element_snapshot_tool` captured the body after changes.
10. No publish was attempted.

## Inserted Elements

The approved structure inserted three top-level body children:

1. `nav.rs-nav#data-nav-root`
2. `div.rs-nav-scrim#data-nav-scrim`
3. `aside.rs-drawer#data-drawer-menu`

The navigation includes:

- `a.rs-logo`
- `div.rs-nav-links`
- `button`-intended controls converted by Webflow as link-like elements with IDs:
  - `data-mega-toggle`
  - `data-drawer-toggle`
- `div.rs-mega#data-mega-menu`

The drawer includes:

- `div.rs-drawer-inner`
- `div.rs-drawer-head`
- `button`-intended close control converted by Webflow as a link-like element with ID:
  - `data-drawer-close`
- `div.rs-drawer-list`

## Verification Evidence

The inserted required IDs were verified with `element_tool > query_elements`:

- `data-nav-root`: one match
- `data-mega-menu`: one match
- `data-nav-scrim`: one match
- `data-drawer-menu`: one match

## Known Tool Behavior

`whtml_builder` can convert raw HTML into Webflow-native elements, but Webflow may normalize some HTML tags. In this run, button markup was represented as link-like Webflow elements while preserving the requested IDs and attributes.

This means future verification should check:

- DOM ID
- attributes
- class names
- final rendered behavior

Do not assume HTML tag type alone survived unchanged.

## Boundaries

This run did not:

- edit repo code
- edit Airtable
- publish Webflow
- change CSS
- add custom-code embed logic

## Repeatable Native Designer Pattern

Use this pattern when the user asks to build actual Webflow Designer elements:

1. Confirm Webflow MCP tools are callable.
2. Run `webflow_guide_tool`.
3. Confirm site ID.
4. Confirm the active page.
5. Inspect the page tree.
6. Identify the exact parent element.
7. Snapshot before.
8. Build only the approved native elements.
9. Verify required IDs/classes/attributes.
10. Snapshot after.
11. Stop before publish unless publish is separately approved.

## Do Not Use This Pattern For

- Airtable data rendering.
- Hosted JavaScript loader updates.
- Webflow Cloud endpoint work.
- Static embed installation.
- Repo CSS changes.

Those are separate workflows and should not be mixed into a native Designer element task without explicit approval.

## Storing Approved Sections For Repeatable Builds

Approved Webflow sections should not live only in chat history. Store each approved section in two places:

1. The repo stores the exact approved markup.
2. Airtable stores the build contract and page assignment.

### Repo Template

Use the repo for the source-controlled template body:

`webflow/templates/approved-sections/{section_key}.html`

Example:

`webflow/templates/approved-sections/rs_nav_kitchen_sink_v1.html`

The repo file should contain the exact approved HTML for that section. This keeps the template diffable, reviewable, and reusable.

### Airtable Contract

Use Airtable to define where and how the approved template is used.

Suggested table:

`rs_approved_sections`

Suggested fields:

- `section_key`: stable template key, for example `rs_nav_kitchen_sink_v1`
- `section_type`: nav, drawer, hero, card_grid, footer, content_section
- `approved_status`: draft, approved, retired
- `repo_path`: exact repo path to the approved HTML
- `build_method`: `whtml_builder`, `element_builder`, or another approved Designer method
- `target_parent_strategy`: body, selected element, specific element id, `rs-main`, etc.
- `default_insert_position`: prepend, append, before, after
- `required_ids`: IDs that must exist after build
- `required_classes`: classes expected after build
- `css_dependency`: required CSS file or class family
- `version`: v1, v2, etc.
- `notes`: warnings and special behavior

For the approved navigation from this run, the recommended key is:

`rs_nav_kitchen_sink_v1`

### Page / Block Assignment

Page-level Airtable tables should link to the approved section row instead of duplicating markup.

Useful linked fields:

- `rs_page_blocks.approved_section_template`
- `rs_section_inventory.approved_section_template`
- `rs_page_stack.approved_section_template`

Example assignment:

- page: `rs_home`
- block: `navigation`
- approved section template: `rs_nav_kitchen_sink_v1`
- target parent strategy: `body`
- insert position: `prepend`

### Repeatable Build Request

A future build request should be phrased like:

`Build rs_nav_kitchen_sink_v1 on page 6a308af40c4cb10adb7432aa into body using its Airtable contract.`

The expected workflow is:

1. Read the Airtable approved-section row.
2. Read the repo template from `repo_path`.
3. Confirm the active Webflow page.
4. Inspect the target parent element.
5. Snapshot before.
6. Insert using the approved `build_method`.
7. Verify `required_ids` and `required_classes`.
8. Snapshot after.
9. Stop before publish unless publish is separately approved.

### Known Caveat

When using `whtml_builder`, Webflow may normalize certain tags into native Webflow element types. Verification must check final Designer IDs, attributes, classes, and rendered behavior instead of assuming raw HTML tag types survived exactly.

## Open Build Note: `rs_overlay_center_v1`

Date: 2026-06-24

Target:

- Webflow site: `6982268b7543ac3c80151266`
- Webflow page: `kitchen-sink`
- Webflow page ID: `6a3c0f785a7ed7e425d31d51`
- Source template: `webflow/rs-template-system/master_ks/00_APPROVED_BASE_DO_NOT_REBUILD.html`
- Section key: `rs_overlay_center_v1`

Verified current Designer state:

- A partial native section exists on `kitchen-sink`.
- Section element ID: `9e46e813-71fc-0425-a266-a618e5990487`
- The section has `data-rs-section-key="rs_overlay_center_v1"`.
- The section currently has only the first wrapper structure:
  - `section`
  - `rs-section-container`
  - `rs-section-padding`

Blocked class state:

- `rs-section` applied successfully to the section.
- `rs-section-container` applied successfully to the first child div.
- `rs-section-padding` applied successfully to the second child div.
- Webflow rejected the section combo classes:
  - `is-overlay`
  - `is-overlay-center`

Important:

- Airtable `rs_section_inventory` draft row: `recN3c5QuO0wl2dox`.
- Do not mark this section complete until the missing overlay combo classes are added in Webflow and the section is finished and snapshotted.
- After the user adds the missing classes, continue the existing section instead of creating a duplicate.

## Open Build Note: `rs_overlay_bottom_left_v1`

Date: 2026-06-24

Target:

- Webflow site: `6982268b7543ac3c80151266`
- Webflow page: `kitchen-sink`
- Webflow page ID: `6a3c0f785a7ed7e425d31d51`
- Source template: `webflow/rs-template-system/master_ks/00_APPROVED_BASE_DO_NOT_REBUILD.html`
- Section key: `rs_overlay_bottom_left_v1`

Verified current Designer state:

- A partial native section exists on `kitchen-sink`.
- Section element ID: `e0e3d872-9be9-7b87-5cfd-3ee84caac1f2`
- The section has `data-rs-section-key="rs_overlay_bottom_left_v1"`.
- The section currently has the first wrapper structure:
  - `section`
  - `rs-section-container`
  - `rs-section-padding`

Blocked class state:

- `rs-section` applied successfully to the section.
- `is-overlay` applied successfully to the section.
- `rs-section-container` applied successfully to the first child div.
- `rs-section-padding` applied successfully to the second child div.
- Webflow rejected the required section combo class:
  - `is-overlay-bottom-left`

Important:

- Airtable `rs_section_inventory` draft row: `recPWB1VrnDwAuL8u`.
- Do not mark this section complete until `is-overlay-bottom-left` is added in Webflow and the section is finished and snapshotted.
- After the missing class is added, continue the existing section instead of creating a duplicate.

## Open Build Note: `rs_form_stack_v1`

Date: 2026-06-24

Target:

- Webflow site: `6982268b7543ac3c80151266`
- Webflow page: `kitchen-sink`
- Webflow page ID: `6a3c0f785a7ed7e425d31d51`
- Source template: `webflow/rs-template-system/master_ks/00_APPROVED_BASE_DO_NOT_REBUILD.html`
- Section key: `rs_form_stack_v1`

Verified current Designer state:

- A partial native section exists on `kitchen-sink`.
- Section element ID: `b5265ee2-ffee-b892-7976-9a29928fa832`
- The section has `data-rs-section-key="rs_form_stack_v1"`.
- The section currently has the first wrapper structure:
  - `section`
  - `rs-section-container`
  - `rs-section-padding`

Blocked class state:

- `rs-section` applied successfully to the section.
- `rs-section-container` applied successfully to the first child div.
- `rs-section-padding` applied successfully to the second child div.
- Webflow rejected the required section combo class:
  - `is-stack`

Important:

- Airtable `rs_section_inventory` draft row: `rec4myKQ6w68QPBaG`.
- Do not mark this section complete until `is-stack` is added in Webflow and the section is finished and snapshotted.
- After the missing class is added, continue the existing section instead of creating a duplicate.

## Deferred Build Notes: Stack Sections

Date: 2026-06-24

These sections were not built because the approved source template requires `is-stack`, and `rs_form_stack_v1` proved Webflow currently rejects that combo class:

- `rs_card_grid_stack_v1`
  - Airtable `rs_section_inventory` draft row: `recwSTQK7kdokOjob`
  - Required modifier class: `is-stack`
- `rs_carousel_stack_v1`
  - Airtable `rs_section_inventory` draft row: `recMkIOLwObTL5fHR`
  - Required modifier class: `is-stack`

Do not create partial native sections for these until the missing `is-stack` combo class is available in Webflow.
