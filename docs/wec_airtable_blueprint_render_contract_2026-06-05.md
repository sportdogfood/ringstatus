# WEC Airtable Blueprint Render Contract

Date: 2026-06-05

Purpose: document the Airtable-driven blueprint for the WEC packing app so every rendered page, module, table, drawer, entity UI, item UI, and list/feed view follows the same data and styling rules.

This document is a working contract. It is meant to prevent drift, reduce payload size, and make every future runner start from the same source of truth.

## Core Rule

Airtable defines:

- what exists
- which source table/view to read
- which fields are allowed
- which components/sections are active
- which entities/items/lists/pivots are available
- which records are linked or counted

The app defines:

- how approved data renders
- how clicks are handled
- how optimistic UI is applied
- how API writes are made
- how errors and pending states are handled

The app must not invent source tables, fallback counts, fallback labels, fallback records, or extra UI behavior.

## Render Rule

Build at the smallest target width first.

Approved responsive rule:

- design and style from 390px first
- minimum target width is 379px
- the same template grows to 490px, 600px, 840px, and larger
- no separate mobile and desktop templates
- no breakpoint-specific redesign unless explicitly approved
- width may shrink/grow
- min/max values may control layout
- clamp may be used only where explicitly defined

Every component must remain structurally consistent across:

- 840
- 600
- 490
- 379 minimum

Do not create one-off CSS per page. Shared classes should drive shared behavior.

## Styling Storage

Styling should be centralized, likely in:

- `pak_system_styling`
- `pak_components`
- `pak_page_stack`

The exact final storage is still being refined, but the rule is fixed:

- classes and layout rules belong in shared styling/component records
- pages should reference style/component keys
- pages should not duplicate style values
- components should not carry unique CSS unless explicitly approved

## Page-Level Vs List-Level Settings

Airtable interface settings can exist at more than one level.

Page-level settings describe the overall page behavior.

List-level settings describe the actual list/table component behavior inside the page.

Record-level settings describe the record/detail/drawer page behavior for one selected record.

Do not collapse these into one generic setting without confirming the scope.

Page-level examples:

- page type
- page stack
- page source
- page route/embed target
- page-level active state
- page-level default view/context

List-level examples:

- levels
- sort by
- group by
- prefix field
- visible fields
- color by
- field text color
- row height
- wrap headers
- show field descriptions
- collapse all by default
- edit records inline
- add/delete records inline
- click into record details

Suggested storage:

- `pak_pages` / `pak_page_index`: page identity and route context
- `pak_page_stack`: which list/component appears on the page
- `pak_components`: component type and shared behavior defaults
- `pak_fields`: visible/allowed fields
- `pak_entities_index` / `pak_items_index`: entity/item capability defaults
- list-specific table/config record: list-level overrides when a component needs its own Airtable-like settings

The list-level settings are the ones that most closely match the Airtable list UI screenshot.

Record-level examples:

- page name
- title field
- visible fields
- permissions
- title size
- page style
- tab navigation
- collapsible groups
- comments enabled
- revision history enabled
- buttons/actions

Suggested storage:

- `pak_page_stack`: record/detail component placement
- `pak_components`: record/detail component type and shared defaults
- `pak_entities_index` / `pak_items_index`: entity/item capability defaults
- `pak_fields`: record-detail field allowlist and drawer/detail inclusion
- record-specific config table/record: record-level overrides when the detail view needs Airtable-like behavior

Record-level settings are the closest match for drawer/detail configuration.

Record-level fields should not be confused with list fields:

- list fields decide which columns/rows render in a list/table
- record fields decide which fields/groups/actions render inside the selected record detail

Expected style fields include:

- `style_key`
- `scope`
- `target_class`
- `font_family`
- `font_size`
- `font_weight`
- `line_height`
- `letter_spacing`
- `height`
- `padding_x`
- `padding_y`
- `gap`
- `is_clamp`
- `clamp_min`
- `clamp_mid`
- `clamp_max`
- `clamp_value`
- `active`

If `is_clamp` is true, the app can use the clamp values. If not, the value should remain fixed and only the container should shrink/grow.

## Drawer And Drilldown Rules

Every entity and item-entity can optionally open a detail/drilldown/drawer view.

Use a boolean field:

- `include_drilldown`
- or `allow_drilldown`

Meaning:

- true: row/item can open a drawer/detail/drilldown
- false: row/item is list-only and does not open detail

Drawer field inclusion is separate:

- `include_on_drawer = true`: field/component is allowed in drawer
- `include_on_drawer = false`: field/component stays out of drawer

Important distinction:

- `allow_drilldown` controls whether a detail view opens
- `include_on_drawer` controls what appears inside the detail view

## Payload Allowlist Rule

Payloads should be light.

The app should not pull every field from a source table when Airtable already defines the allowed fields.

The pattern is:

```text
source index table -> linked pak_fields -> active allowed fields -> API field list
```

Example:

```text
pak_entities_index.horses
source_table = pak_horses_roster
pak_fields = linked allowed fields
count_pak_fields_active = expected active field count
```

If a field is removed from the linked `pak_fields` list, it should drop from that API payload.

This applies to both:

- entities
- item-entities

## Field Registry

`pak_fields` is the field registry.

Required/current purpose:

- map live Airtable field IDs to field names
- identify the source table
- define render keys
- define human labels
- mark allowed/active fields

Expected fields:

- `field_id`
- `field_source_table`
- `field_key`
- `field_label`
- `data_rs_value`
- `active`
- `inactive`
- `suggest_remove`
- `suggest_allowed`
- `is_alloed`

Current note: `is_alloed` is misspelled in Airtable and should not be silently renamed without approval.

The normalized shape should be:

```text
field_id = Airtable field id
field_source_table = Airtable source table name
field_key = Airtable field name
field_label = human label
data_rs_value = render/binding key
active = usable
```

## Entities

Use:

```text
pak_entities_index
```

Purpose:

```text
Registry of real business entities that need UI.
```

Entity examples:

- horses
- comments
- tenants
- sessions
- profiles

Current confirmed entity:

```text
entity_key = horses
source_table = pak_horses_roster
source_view = roster
ui_type = entity
allow_add = true
allow_inline_edit = true
allow_input = true
active = true
```

Comments are also an entity/feed concept, not a pivot.

Expected fields:

- `entity_key`
- `source_table`
- `source_view`
- `ui_type`
- `allow_add`
- `allow_inline_edit`
- `allow_input`
- `allow_drilldown` or `include_drilldown`
- `pak_fields`
- `count_pak_fields_active`
- `active`
- `notes`

The source table is not the entity name.

Example:

- entity: `horses`
- source table: `pak_horses_roster`
- view/context: `roster`

## Item Entities

Items are not ordinary page rows. They should behave like entities because they need their own UI, input, edit, and drilldown behavior.

Use:

```text
pak_items_index
```

Purpose:

```text
Registry of every table whose records are item-like records.
```

Current item families:

```text
kit -> pak_kit_items
byqty -> pak_byqty_items
byhorse -> pak_byhorse_items
bygroom -> pak_bygroom_items
feed -> feed_items
```

Expected fields:

- `item_family`
- `source_table`
- `source_view`
- `plan_key`
- `entity_key`
- `allow_add`
- `allow_inline_edit`
- `allow_input`
- `allow_drilldown` or `include_drilldown`
- `pak_fields`
- `count_pak_fields_active`
- `active`
- `notes`

Item tables should use the same field allowlist pattern as entities:

```text
pak_items_index -> pak_fields -> API only pulls allowed fields
```

## Lists

Lists are not pivots.

Use:

```text
pak_list_family_index
pak_list_members
```

`pak_list_family_index` defines the list itself.

Examples:

- `purchase_onsite`
- `needs_attention`
- `unresolved`
- `packed_max`

Expected list family fields:

- `list_key`
- `label`
- `scope`
- `applies_to_entity`
- `active`
- `notes`

`pak_list_members` defines membership.

Example:

```text
list_key = purchase_onsite
member_entity = item
member_table = pak_kit_items
member_record_id = rec...
active = true
```

Expected membership fields:

- `member_key`
- `list_key`
- `member_entity`
- `member_table`
- `member_record_id`
- `active`
- `notes`

Meaning:

- list family says the list exists
- list members say a horse/item/etc belongs to that list

## Pivots

Pivots are for relationship workflows that require a built process between entities or states.

Use:

```text
pak_pivots
```

Examples that are pivots:

- feed
- turnout
- stall checkout / turnout assignment
- slot/time assignment
- lane assignment when it is a real operational relationship

Examples that are not pivots:

- horses
- comments
- item list membership

Comments are linked/feed records, not a pivot.

List membership is its own list/member system, not a pivot.

Expected pivot fields:

- `pivot_key`
- `scope`
- `lookup_table`
- `lookup_view`
- `value`
- `active`

## Packing Plan Groups

Use:

```text
pak_groups
```

Purpose:

```text
Core packing plan/page stack blueprint.
```

Plans:

- `horse_specific`
- `quantity`
- `per_horse`
- `per_groom`

Each plan should use the same page/component/render system unless the plan logic requires different math.

Packing plan behavior is not the same as entity/profile behavior.

## Plan Logic

### horse_specific

Rules:

- no quantity math
- one horse can have one assigned kit for the plan
- kit items are tracked as item states
- item state can be not packed, packed, or not needed
- counts are counts, not quantities
- do not backfill
- do not assign kits/items inside the packing UI
- kits/items are setup before the packing stage

Core tables:

- `wec_list_plans`
- `pak_horses_roster`
- `pak_kit_items`
- `horse_packing_kits`
- `horse_kit_changes`

### quantity

Rules:

- manually set starting value
- packed actions debit/credit against that value
- edits to starting value require reason/log
- support exceptions such as unresolved, purchase onsite, packed max

### per_horse

Rules:

- each item has a multiplier
- total needed depends on horse count
- horse count can change
- packed count does not reset just because total target changes
- exceptions still apply

### per_groom

Rules:

- target is based on horse count and groom ratio
- values come from wave/source data
- horse count can change
- groom ratio can change
- packed count does not reset just because target changes
- exceptions still apply

## Comments

Comments need:

- comment container
- comments list/feed
- add comment
- edit comment
- save comment
- comment short/select option
- free input option
- comment log/audit trail

Comments may appear:

- on a module/section
- on a horse/entity
- on an item/detail
- in a full comments feed page

Comments are not a pivot.

Comments should be represented as an entity/feed and can be linked from relevant records.

## Sessions

Sessions need to be explicit.

Expected:

- `pak_sessions`
- session starts when user opens the app/page
- session record is created or reused
- device can keep a local session id
- heartbeat/polling continues while active
- idle timeout eventually stops polling

Possible local storage:

- cookie for device/session identity
- sessionStorage for current page/session state
- IndexedDB if cached shape becomes large enough to justify it

Click behavior:

- listen for clicks
- update UI optimistically where approved
- call API on each state-changing click
- do not fire duplicate writes if the same action is already in progress
- poll or refresh state after successful writes where needed

## Button Rendering

Airtable button fields expose:

- `label`
- `url`

They do not expose color through the Airtable API.

Button use cases tested:

- fixed URL
- dynamic URL
- internal/action placeholder

Rules:

- external URL can render as a link if approved
- dynamic URL can render as a link if approved
- Airtable record URL should not be used as a direct app user link without approval
- internal actions should be handled by the app

Potential internal actions:

- print
- open drawer if row allows drilldown
- edit if editable
- comments
- add

## Webflow Render Binding

The app can either:

- render full HTML into a container
- or inject values into Webflow-owned markup

Example value binding:

```html
<span data-rs-value="horse_name"></span>
```

Rule:

- if Webflow owns the markup, the app should only fill approved data slots
- if the app renders HTML, it must render approved shared classes only
- no generated markup should introduce a new visual system

Embed config should be generated from approved system/page records where possible:

```html
<main
  id="packing-app"
  class="rsa-dashboard"
  data-api-url="..."
  data-print-url="..."
  data-pack-wave-key="wave_one">
  Loading WEC packing...
</main>
```

## Print

Print is required.

Needed print outputs:

- packing list
- individual horse/item kit list
- logs
- full list/search output

Print routes/templates must follow the same source data rules:

- no fallback counts
- no unapproved source tables
- only allowed fields

## Current Blueprint Preview

Local preview:

```text
http://127.0.0.1:8792/wec-blueprint-preview.html
```

Read-only API:

```text
http://127.0.0.1:4331/wec-packing/blueprint
```

Current preview sections:

- warnings
- roster button render test
- indexes
- list layer
- blueprint tables

## Current Blueprint Tables

Known blueprint/index tables:

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
- `pak_entities_index`
- `pak_items_index`
- `pak_list_family_index`
- `pak_list_members`
- `pak_fields`
- `pak_components`
- `pak_groups`
- `table_index`

## Known Cleanup Items

The following are known and should not be ignored:

- `pak_wire_assignments` exists but currently has no records
- `pak_wire_index` contains non-role rows and may need cleanup
- `pak_fields.is_alloed` is misspelled and should only be renamed with approval
- final storage location for layout/classes still needs confirmation
- comments entity needs its allowed fields wired
- item entities need their allowed fields wired
- drilldown boolean field needs to be added consistently
- `include_on_drawer` needs to be standardized across field/component records
- print templates still need complete treatment
- sessions/polling still need a complete implementation

## Non-Negotiable Implementation Rules

- do not invent tables
- do not invent fields
- do not invent counts
- do not backfill records
- do not clear totals
- do not use fallback data
- do not use hardcoded business values
- do not create one-off CSS
- do not create alternate mobile templates
- do not pull all fields when an allowed field list exists
- do not cross-wire unrelated app logic into a plan/entity route
- do not make direct Airtable record links user-facing unless explicitly approved

## Working Model

The intended model is:

```text
system index
  -> page index
    -> page stack
      -> components
      -> groups/plans
      -> entities
      -> item entities
      -> lists
      -> pivots
      -> fields allowlists
      -> render
      -> action API
      -> logs/audit
```

This should let each page/module be built from the same contracts without redesigning the UI or guessing the data source.
