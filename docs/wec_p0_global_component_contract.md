# WEC P0 Global Component Contract

This is the locked P0 contract for the packing UI surfaces. Use this before building or changing any plan, entity, list, item, pivot, drawer, or print surface.

## Global Rules

- All pages use the same stack order: header, primary tabs, optional child nav, optional search, main table or grid/list, comments, lane controls.
- Use one table style: `.rs-airtable-grid`.
- Use one drawer style: `.rs-record-drawer`, `.rs-drawer-head`, `.rs-drawer-body`.
- Use one grid/list style: `.rs-kit-items`, `.rs-kit-item-row`, `.rs-kit-actions`.
- Use one label style everywhere: `.rs-stack-label`.
- Do not create page-specific table, drawer, grid, or label CSS.
- Do not use fallback counts or fallback assignments. Missing source data should render empty or not connected.

## Record Types And Drawer Slots

| record_type | drawer slots |
| --- | --- |
| kit | head, progress, aggregates, search, filters, list, comments, logs |
| value | head, aggregates, form, actions, comments, logs |
| entity | head, summary, form, actions, comments, logs |
| comment | head, form, thread, logs |
| list | head, summary, aggregates, list, actions |
| list_item | head, summary, actions, comments, logs |
| pivot | head, summary, navigation, list, actions, logs |

## Navigation Landing Contract

Top nav:

- Home: overview of modules.
- Horses: child nav shows Roster, Profiles, Attributes.
- Counts: child nav shows Horse Kits, Quantity Counts, Per-Horse Items, Groom Supplies.
- Lists: child nav shows list family views.
- Items: child nav shows item family views.
- Comments: comments feed.

Each click must either render a connected surface or a visible not-connected placeholder. Blank panels are not allowed.

## Front-Facing Labels

- `quantity` renders as `QUANTITY COUNTS`.
- `per_horse` renders as `PER-HORSE ITEMS`.
- `per_groom` renders as `GROOM SUPPLIES`.

## Optimistic State Contract

- User clicks update the UI immediately.
- Pack-state action responses must not repaint a stale full state snapshot.
- The backend may omit `state` for pack-state actions.
- The frontend keeps the optimistic state and refreshes after propagation.
- Failed saves revert only the affected optimistic key.

## CSS Contract

- No new broad `!important` override layer.
- Nav child tray is part of `.rs-stack-section.is-primary-tabs`.
- Lane controls use `.rs-stack-section.is-lane-controls` and stay at the bottom with sticky positioning.
- Count columns use fixed 60px columns and centered headers/cells.
- Drawer close owns its own header column; drawer body cannot render underneath it.
- Drawer list rows use zebra and hover through `.rs-kit-item-row`.

## Open P0 Verification

- Verify desktop width around 840px.
- Verify mobile width 379px.
- Verify nav click cadence for every top tab and child tab.
- Verify drawer open/close, search clear, item filter, and item state click.
- Verify no stale-state visual bounce after item state save.
