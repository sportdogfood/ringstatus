# WEC Packing Comprehensive Overview

Status: working reference.
Date: 2026-06-06.

This document consolidates the WEC packing architecture, Airtable model, integrations, template/CSS contract, known trouble, open tasks, and future modules. It is intended as the first-read reference before changing the packing app, blueprint tables, templates, or module code.

## Operating Principle

The system is Airtable-first and module-based.

- Airtable owns source records, views, counts, membership, allowed fields, logs, and blueprint metadata wherever possible.
- The browser renders state and handles user interaction, but it should not invent source data, fallback counts, plan memberships, or assignment rows.
- Webflow owns the shell/embed surface and loads pinned static assets.
- Webflow Cloud/Astro owns server-side API routes and Airtable tokens.
- Modules can operate independently, but they may share canonical source tables such as horses, waves, users, comments, fields, and list membership.

Hard rules:

- Do not backfill packing assignments unless explicitly approved.
- Do not clear or rewrite Airtable data to make the UI work.
- Do not use hardcoded fallback data when Airtable data is missing.
- Do not fetch whole Airtable rows once an allowed-field manifest exists.
- Do not create a new table, drawer, grid, label, or nav style for one page.
- Do not hide a missing source behind frontend math. Fix the source, view, field mapping, or blueprint.

## Integration Stack

The app-data path is:

```text
Webflow page/embed
  -> GitHub/jsDelivr CSS and JS assets
  -> Webflow Cloud/Astro API route
  -> Airtable REST API
  -> Airtable tables/views/records/logs
  -> JSON response
  -> frontend render and optimistic state
```

Current key surfaces:

| Layer | Role | Current files / routes |
| --- | --- | --- |
| Webflow shell/embed | Loads root container, config, CSS, JS | `webflow/packing-worksheet/wec-packing-hybrid-webflow-embed.html` |
| Static frontend | Renders nav, stack, tables, drawers, optimistic UI | `webflow/packing-worksheet/wec-packing-hybrid.js` |
| Global CSS | Shared component contract | `webflow/packing-worksheet/rsa-stylesheets.locked.css`, `webflow/packing-worksheet/styles.css`, `webflow/packing-worksheet/horse-kits-static-proof.css` |
| Horse kits API | Horse-specific plan state/actions/print | `webflow-cloud-test/src/pages/wec-packing/horse-kits.js`, `webflow-cloud-test/src/pages/wec-packing/horse-kits/print.js`, `webflow-cloud-test/src/lib/wec-horse-kits.js` |
| Other plan APIs | Quantity, per-horse, per-groom | `webflow-cloud-test/src/pages/wec-packing/quantity.js`, `per-horse.js`, `per-groom.js`, `webflow-cloud-test/src/lib/wec-plan-modules.js` |
| Home/session/nav APIs | Home overview, session ping, module navigation | `webflow-cloud-test/src/pages/wec-packing/home.js`, `session.js`, `index.js` |
| Horse entity API | Horse roster/profile/attribute read and controlled writes | `webflow-cloud-test/src/pages/wec-packing/horses.js`, `webflow-cloud-test/src/lib/horse-entity-ui.js` |
| Blueprint review | Read-only schema/model review | `webflow-cloud-test/src/pages/wec-packing/blueprint.js`, `webflow-cloud-test/src/lib/wec-blueprint.js` |
| Print | HTML print/PDF surfaces | plan-specific print routes under `webflow-cloud-test/src/pages/wec-packing/**/print.js` |

Embed rule:

- Airtable token must never be exposed in Webflow or frontend JS.
- The Webflow embed should only define config, asset version, asset base, API URLs, and root attributes.
- Pinned jsDelivr commit URLs are preferred for publish verification.

## Current Module Families

These names are working concepts. They are not permanent schema guarantees unless the relevant Airtable tables and code explicitly use them.

| Module | Purpose | Operates independently by | Shared dependencies |
| --- | --- | --- | --- |
| Pack | Packing plans, item state, quantity state, logs, lane controls, print | Each plan has its own source/items/links/logs/lanes/slots where needed | `pak_horses_roster`, `wec_pack_waves`, `wec_list_plans`, `pak_groups`, sessions |
| Boards / Pivots | Feed, turnout, braiding, vet reminders, other relationship boards | Pivot-specific entities, links, lanes, slots, logs | Horses, users, contacts, locations, comments |
| Horses | Horse roster/entity UI, attributes, profile/edit surfaces | `pak_horses_roster` remains canonical for rendered modules | Comments, pack waves, plans, fields allowed |
| Users | User entity, roles/types, assignments, tasks | `pak_users` plus role/type/task/assignment tables | Comments, sessions, packing assignments |
| Comments | Comments, comment threads, short comments, logs | Comment tables and parent scoping | Horses, users, plans, boards, lists |
| Filtered Lists | Predefined useful lists and memberships | List family + members/views | Horses, items, users, contacts, locations |
| Supporting Lists | Lanes, slots, tags, roles, helper choices | Support tables and lookup views | Used by modules, not standalone app logic |
| Fields Allowed | Payload control and field-level permissions | `pak_fields` and related source indexes | All modules that read/write Airtable |
| Reminders | Future reminder module | Not built | Users, contacts, horses, comments |
| Schedules | Future schedule module | Not built | Shows, locations, horses, users |
| Places | Future richer place/location module | Not built | Current `pak_locations` is only a simple location list |

## Blueprint And Index Tables

The current blueprint review path recognizes these core tables:

- `pak_system_index`
- `pak_page_index`
- `pak_page_stack_index`
- `pak_page_stack`
- `pak_pages`
- `pak_page_types`
- `pak_wire_index`
- `pak_wire_assignments`
- `pak_pivots`
- `pak_system_styling`
- `pak_system_logic`
- `pak_html_lib`
- `pak_entities_index`
- `pak_items_index`
- `pak_list_family_index`
- `pak_list_members`
- `pak_fields`
- `wec_list_plans`
- `wec_pack_waves`
- `pak_horses_roster`
- `pak_components`
- `pak_groups`
- `table_index`

How they should be used:

| Table family | Owns |
| --- | --- |
| `pak_system_index` | Top-level registry for system concerns such as styling, logic, sources, tenants, sessions, shows, components, and table registries |
| `pak_page_index` | Page-level registry such as home, horses, counts, lists, items, comments, print, dashboard |
| `pak_page_stack_index` | Stack block keys such as header, primary tabs, child nav, summary, search, main table, drawer, comments, footer |
| `pak_wire_index` | Required wire roles such as entity_1, entity_2, links, logs, lanes, slots, comments, support tables |
| `pak_wire_assignments` | Concrete table assignments for a specific wire/module/plan |
| `pak_groups` | Core packing plan stack blueprint. This remains the plan-rendering source until an approved replacement exists |
| `pak_pivots` | Non-pack pivot/board definitions if/when separated from pack groups |
| `pak_entities_index` | Entity UI sources that need add/edit/input/detail behavior, such as horses, users, comments, contacts, locations |
| `pak_items_index` | Item-like entities that may need add/edit/input/detail behavior, such as kit items, quantity items, feed items |
| `pak_list_family_index` | List families and membership-style lists |
| `pak_list_members` | Membership rows for list/list-item relationships |
| `pak_fields` | Field registry, allowed fields, payload reduction, field source table references |
| `pak_html_lib` | Approved HTML fragments/templates for dynamic markup where needed |
| `pak_system_styling` | Global styling tokens/classes/rules, not one-off page CSS |
| `pak_system_logic` | Global logic rules, not per-page hidden frontend logic |

## Page And Stack Contract

All app pages should use the same stack model:

```text
app
  -> module
  -> page
  -> stack section
  -> component
  -> record/list/drawer state
```

Approved high-level navigation model:

- `HOME`
- `HORSES`
- `COUNTS`
- `LISTS`
- `ITEMS`
- `COMMENTS`

Known child navigation:

| Top nav | Children / landing |
| --- | --- |
| Home | Module overview rows |
| Horses | Roster, Profiles, Attributes |
| Counts | Horse Kits, Quantity Counts, Per-Horse Items, Groom Supplies |
| Lists | All, then configured views/lists |
| Items | Kit Items, Quantity Items, Per-Horse Items, Groom Items, future item families |
| Comments | All, Today, dynamic parent, Add |

Blank panels are not allowed. A click must show either a connected surface or an explicit not-connected placeholder.

## Pack Plan Models

### Horse Kits / Horse-Specific

Source model:

- Main entity: `pak_horses_roster`
- Kit source: `pak_kits`
- Item source: `pak_kit_items`
- State/link table: `horse_packing_kits`
- Log table: `horse_kit_changes`
- Blueprint source: `pak_groups` view/records for horse-specific plan

Rules:

- This plan is not quantity-based.
- A kit is packed or not packed at the kit level.
- A kit item is `not_packed`, `packed`, or `not_needed`.
- Counts are counts of assigned items, not quantities.
- If a horse is not going and still shows assigned kits/items, that is a data/source flag to review, not a reason to backfill or override in JS.
- Never create fallback "40" items in code.

### Quantity Counts

Source model from code:

- Source: `pak_byqtys`
- Items: `pak_byqty_items`
- Links: `pak_byqty_links`
- Logs: `pak_byqty_logs`
- Lanes: `pak_byqty_lanes`
- Slots: `pak_byqty_slots`

Rules:

- Starting quantity can be manually set.
- Writes act as debit/credit against the current quantity.
- Adjusting the starting value is allowed only as an explicit action with a reason.
- Exceptions such as unresolved, purchase onsite, and packed max must be modeled explicitly.

### Per-Horse Items

Source model from code:

- Source: `pak_byhorses`
- Items: `pak_byhorse_items`
- Links: `pak_byhorse_links`
- Logs: `pak_byhorse_logs`
- Lanes: `pak_byhorse_lanes`
- Slots: `pak_byhorse_slots`

Rules:

- Needed count is dynamic from horse count and item multiplier.
- Horse count can change.
- Packed count does not automatically change just because needed count changes.
- Writes still use debit/credit style state/log behavior.
- Special exceptions apply.

### Groom Supplies

Source model from code:

- Source: `pak_bygrooms`
- Items: `pak_bygroom_items`
- Links: `pak_bygroom_links`
- Logs: `pak_bygroom_logs`
- Lanes: `pak_bygroom_lanes`
- Slots: `pak_bygroom_slots`

Rules:

- Needed count is dynamic from horse count and groom ratio/count.
- Horse count and groom ratio can change.
- Packed count does not automatically change just because needed count changes.
- Writes still use debit/credit style state/log behavior.
- Special exceptions apply.

## Entity And Item Entity Model

Entities are tables that require UI for review, add, edit, input, details, and often comments. Item entities behave similarly, but represent item families rather than people/horses/comments.

Known entity-style sources:

- `pak_horses_roster`
- `pak_users`
- `pak_contacts`
- `pak_locations`
- comments table(s)

Known item-entity sources:

- `pak_kit_items`
- `pak_byqty_items`
- `pak_byhorse_items`
- `pak_bygroom_items`
- future `feed_items` or other board/list item sources

Entity/item rules:

- `include_on_drawer` decides whether a field/component belongs in the drawer.
- `drill_down` or equivalent should mark records that support a detail drawer/page.
- Add/edit/input actions must use allowed create/write fields.
- The UI should not receive fields that are not active/allowed for that surface.
- Inline edit and add/delete are allowed only where the entity/item configuration says so.

## Users, Roles, Assignments, Tasks

Current created user tables:

| Table | Current Airtable id |
| --- | --- |
| `pak_users` | `tbl0aKxRNYspyHSWX` |
| `user_roles` | `tblwZaCRZRTFvXjC6` |
| `user_types` | `tbltkfBXk682j5Vnq` |
| `user_assignments` | `tbl11eJbjwb7TIJh7` |
| `user_assignment` | `tbltvZk7mvAnDnafl` |
| `user_tasks` | `tbl3VTsC3eu1iVY85` |
| `user_task` | `tblTflPhfQ9h477bM` |

Seeded role/type concepts:

- owner
- admin
- staff
- trainer
- rider
- groom
- user

Rules:

- Users are entities like horses.
- Users need add/edit/input/detail behavior.
- Phone number is expected to be a practical UID.
- Users link to comments and assignments.
- User roles need permission levels.
- Assignments/tasks are list/list-item style surfaces tied to users.
- The comments link needs a real relationship when connector/schema support allows it; `comment_system_key` is a temporary reference field.

## Contacts And Locations

Current created tables:

| Table | Current Airtable id |
| --- | --- |
| `pak_contacts` | `tblrPF8p0J90NCYuj` |
| `pak_locations` | `tbl4TIVML7EJope1c` |

Contacts:

- braiders
- vets
- tack contacts
- feed contacts
- shippers
- other useful contacts

Locations:

- home
- Ocala / WEC
- Kentucky / KHS
- Wellington / WEF

Rules:

- Contacts and locations are simple list/entity supports.
- They are for sorting, filtering, grouping, and route/list membership.
- They are not stall locations, store locations, turnout locations, tracking, audit, or state systems.
- Route examples such as `home -> ocala`, `ocala -> ky`, and `ocala -> home` should be represented as membership/filter/list support, not hardcoded app logic.

## Lists, Membership, And Filtered Lists

Filtered lists should be Airtable-backed and membership-driven.

Examples:

- purchase onsite
- unresolved
- needs attention
- horses going to a location
- horses returning home
- items needed for a route
- contacts by type
- comments by parent or date

Rules:

- A horse or item is a member of a list or it is not.
- The frontend should not compute complex membership if Airtable views/membership tables can provide it.
- Views, rollups, counts, and summaries should be used before frontend filtering.
- Simple on-page search is acceptable.

## Comments

Comments need to become a full system, not only a display block.

Known needs:

- Comment list/feed.
- Add comment.
- Comment threads under parent records.
- Sort by latest.
- Parent scoping: horse, item, plan, board, list, user, or other entity.
- Comment count by entity/list item.
- Comments by user.
- Canned comment shorts where appropriate.
- Comment logs/audit where appropriate.

Current risk:

- Comments currently appear in multiple surfaces, but the complete add/thread/feed model is not fully locked.

## Sessions, Polling, And Optimistic UI

Session requirements:

- Create or confirm `pak_sessions`.
- Create one session record when the page is engaged.
- Store the session key in browser storage.
- Reuse that session key for clicks, polling, heartbeats, and logs.
- Include `sessionKey` or `sessionId` in write requests.
- Coalesce session pings while one request is already in flight.
- Continue polling while active.
- Pause/slow polling when hidden or idle.
- Refresh from Airtable truth after writes.

Current frontend behavior:

- The hybrid app stores a session key in `sessionStorage`.
- It stores a device id in `localStorage`.
- It pings the session endpoint and coalesces pings while in flight.

Optimistic UI rule:

- A click should update the affected UI immediately.
- A save response must not repaint stale state over the optimistic value.
- If Airtable propagation is delayed, refresh should happen after a short delay, not instantly repaint old data.
- Failed saves should revert only the affected optimistic key.

## Templates And CSS Contract

The visual contract is global. The goal is not to redesign per page.

Locked component classes:

- Table: `.rs-airtable-grid`
- Drawer: `.rs-record-drawer`, `.rs-drawer-head`, `.rs-drawer-body`
- Grid/list: `.rs-kit-items`, `.rs-kit-item-row`, `.rs-kit-actions`
- Label: `.rs-stack-label`
- Stack section: `.rs-stack-section`
- Tabs: `.rs-stack-tabs`

Global rules:

- One table style.
- One drawer style.
- One grid/list style.
- One search style.
- One button/pill style family.
- One label style everywhere.
- No page-specific table/drawer/grid CSS.
- No broad `!important` override layer.
- No bare text nodes outside approved wrappers.
- All components must render consistently at 840, 600, 490, and minimum 379 px.
- If a font clamp exists, it belongs in the global style/token model; otherwise the component should shrink/grow through layout width, min/max, and stable columns.

Known CSS requirements:

- Count columns use fixed 60 px minimum columns and centered headers/cells.
- Drawer body cannot render under the close column.
- Drawer should slide from the container on desktop and from bottom on mobile if that is the approved behavior.
- Search clear, tabs, pills, row hover, zebra rows, buttons, and drawers must be consistent.
- Lane controls use `.rs-stack-section.is-lane-controls` and belong at the bottom/sticky bottom where approved.
- Child nav should overlay where specified and not push content down.
- The repo-local CSS audit workflow is `skills/rs-css-audit/SKILL.md`.

## HTML Template Library

`pak_html_lib` is intended to hold approved HTML fragments or template shapes where dynamic HTML precision is needed.

Use cases:

- Header markup fragments.
- Dynamic class names.
- Data-binding placeholders such as `data-rs-value`.
- Button markup variants.
- Record/detail fragments.

Rules:

- HTML library entries should not become a second CSS system.
- They should bind to global component styles.
- They should expose field/value placeholders rather than hardcoded output.
- If a Webflow-designed fragment is approved, the renderer should populate it rather than rebuild it from memory.

## Printing

Known print needs:

- Packing list print.
- Log print.
- Per-horse kit item print.
- Plan-specific print routes for horse kits, quantity, per-horse, and per-groom.
- Full list/search print.
- Comments/log print if approved.

Open risks:

- Print templates have drifted from approved screen templates.
- Each print route needs verification from the actual endpoint and browser print preview.
- Print should use the same data truth as the page, not a separate fallback shape.

## Known Trouble And Risks

| Issue | Impact | Required response |
| --- | --- | --- |
| Hardcoded fallback counts or assignments | Shows believable but wrong data | Remove fallback; render empty/not connected and fix Airtable mapping |
| Repainting stale server state after optimistic click | User sees value bounce back, then forward | Delay refresh or ignore stale snapshots for affected optimistic key |
| Multiple table/drawer/grid renderers | Styling drift and repeated QA loops | Collapse to one approved table, drawer, and grid system |
| Page-specific CSS overrides | Fixes one page and breaks another | Move to global class/token model |
| Unapproved `!important` overrides | Future styling becomes unpredictable | Remove unless there is a documented locked reason |
| Missing Airtable views/fields | Frontend guesses or filters too much | Create/fix views/fields in Airtable or document not connected |
| Airtable connector field-creation limits | Some linked fields may not be creatable through connector | Document blocker, use temporary reference only when approved |
| jsDelivr pinned asset mismatch | Browser tests old code | Verify loaded commit URL before debugging behavior |
| Iframe preview vs direct preview differences | False CSS/layout conclusions | Test the actual intended surface |
| Comments not fully modeled | Add/thread/feed behavior incomplete | Finish comments as its own system |
| Navigation placeholders | Clicks appear broken | Every click must show connected content or explicit not-connected state |
| Print drift | Published output does not match app data/design | Verify every print route and template |

## Open Tasks By Priority

### P0 - Stabilize Existing Product Surface

- Confirm one global table, drawer, grid, search, label, and button style is used everywhere.
- Confirm nav click cadence for Home, Horses, Counts, Lists, Items, Comments.
- Confirm child nav does not push content where overlay behavior is required.
- Confirm `HORSES > ROSTER` renders all/wave one/wave two/not going.
- Confirm `COUNTS > HORSE KITS` renders from horse-specific blueprint/data only.
- Confirm drawer open/close, item search, item filter, and item state click.
- Confirm no stale-state visual bounce after pack item saves.
- Confirm both aggregate tiers can be hidden/shown by blueprint rules.
- Confirm print buttons/routes do not call mismatched templates.

### P1 - Complete Core Entity And Session Systems

- Finish `pak_sessions` record lifecycle and heartbeat/polling behavior.
- Finish users as an entity: roster, profile/detail, add/edit, roles/types, assignment/task linkage.
- Finish comments system: add, feed, threads, parent scoping, latest sort, comment counts.
- Finish fields-allowed enforcement through `pak_fields` for entities, items, lists, and plans.
- Finish contacts and locations as simple list/entity supports.

### P2 - Complete Lists, Items, And Membership

- Build list overview and list detail surfaces.
- Build item family overview and item detail surfaces.
- Wire `pak_list_family_index` and `pak_list_members`.
- Add membership views such as purchase onsite, unresolved, needs attention, routes, and useful filtered lists.
- Keep membership Airtable-driven.

### P3 - Complete Boards / Pivots

- Build pivot/board model for feed, turnout, braiding, vet reminders, and similar surfaces.
- Decide whether pivots use `pak_pivots` separately from `pak_groups`.
- Define wire assignments for each pivot: entity_1, entity_2, links, logs, lanes, slots, comments, support tables.
- Add board-specific print/log outputs if needed.

### Future Modules

- Reminders.
- Schedules.
- Places as a richer system beyond simple `pak_locations`.
- Notifications.
- User permissions enforcement.
- Offline queue or IndexedDB state cache if needed.
- Cross-module dashboard summaries.
- Tenant/admin configuration UI.

## Independent Operation With Shared Sources

Each module should have a page-specific cycle:

```text
load blueprint
  -> resolve allowed source tables/views/fields
  -> read Airtable data
  -> render approved components
  -> accept allowed user action
  -> write source/link/log/session
  -> refresh from Airtable truth
```

Modules are independent when:

- They have their own endpoint or documented endpoint mode.
- They have their own source/items/links/logs when state rules differ.
- They have their own allowed field manifest.
- They do not mutate another module's state tables except through a documented shared entity/action.

Modules integrate when:

- They reference shared canonical entities such as `pak_horses_roster`, `pak_users`, `wec_pack_waves`, comments, contacts, locations, or `pak_fields`.
- They use shared global CSS/components.
- They use shared sessions and device ids.
- They write logs with enough context to identify module, plan, page, user/session, action, and record.

## Immediate Next Build Path

1. Treat this document as the working overview.
2. Verify the blueprint preview endpoint and the app preview render from the same current commit.
3. Lock the global table/drawer/grid/search/nav components before adding more module surfaces.
4. Finish users/comments/sessions before expanding board complexity.
5. Use Airtable views, counts, rollups, and membership tables before adding frontend logic.
6. When adding a module, document its source tables, allowed fields, actions, logs, print route, and not-connected states before wiring UI.

