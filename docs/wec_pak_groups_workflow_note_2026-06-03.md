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

## Pak Group Views

These views are part of the same `pak_groups` pattern and should operate similarly:

- `all`
- `horse_specific`
- `per_horse`
- `quantity`
- `per_groom`

Each view should be stacked with the same method, but may point to different physical tables, source views, labels, and components.

## Must Complete

- `home`: overview page.
- `pak_groups`: currently working stack contract.
- `report_lists`: report/list rendering.
- `print`: print button plus a better direct-print solution.

## More Complex

- `horses_rosters_ui`: add/edit/apply horse attributes and roster trail.
- `horse_feeding_ui`: feeding stack, currently work in progress.
- `horse_turnout_ui`: turnout stack, currently work in progress.
- `comments_ui`: section comments plus a full comment feed view.
- `customize_ui`: edit site labels, edit pack items, and assign items to different lists.

## Boundary

Do not build the next workflows until `horse_specific` and the shared `pak_groups` stack method are stable enough to reuse without drift.

Temporary hardcodes are allowed only to unblock delivery, and must be documented with the intended `pak_groups` replacement.
