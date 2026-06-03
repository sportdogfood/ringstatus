# WEC Pak Groups Workflow Note

Status: document only. Use this to finish the `pak_groups` `horse_specific` workflow before building the next stacks.

## Current Pattern

`pak_groups` is the page stack contract.

For the Horse Kits page, the app reads `pak_groups` view `horse_specific`, sorts by `sort_order`, and uses:

- `render_key`: stable page stack key.
- `display_label`: UI label.
- `physical_table`: Airtable table to read/write.
- `component_key`: renderer/component family.
- `sort_order`: stack order.

The app should not invent page structure outside this stack. If a needed block is missing, add a clear `pak_groups` row instead of hardcoding a new page section.

## Horse Specific Stack

- `header`: report title/subtitle source.
- `primary_tabs`: top tab source.
- `lane_controls`: lane/date/wave controls.
- `secondary_controls`: view buttons and utility controls.
- `search_aggs`: search and aggregate summary area.
- `main_table`: primary roster/list table.
- `drawer_items`: detail drawer item source.
- `state_links`: active state/linking table.
- `change_log`: audit trail for state changes.
- `kit_source`: kit/source definitions.
- `comments`: comments source.

## Rule

Each workflow should have:

- one primary source table,
- one or more support/source tables,
- one active linking/state table,
- one audit/log table,
- a `pak_groups` view row for each rendered stack block.

## Next Workflows To Build After Horse Specific

- `per_horse`
- `quantity`
- `per_groom`
- `home_overview`
- `reports`
- `lists`

## Boundary

Do not build the next workflows until `horse_specific` is stable and documented enough to reuse without drift.

Temporary hardcodes are allowed only to unblock delivery, and must be documented with the intended `pak_groups` replacement.
