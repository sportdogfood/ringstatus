# WEC Horse Feeding Stack Note

Status: document only. Do not implement during the current Horse Kits delivery pass.

## Proposed Stack

- `horse_feeding`: page/plan level source for feeding.
- `horse_feed`: per-horse feed plan or active feed assignment.
- `horse_feed_items`: reusable feed item definitions.
- `horse_feed_rations`: per-horse/per-feed quantity or ration detail.
- `horse_feed_links`: linking/state table for horse + feed + feed item/ration.
- `horse_feed_logs`: audit trail for add, edit, apply, and remove actions.

## Boundary

- This should follow the same pattern as Horse Kits: source tables, one linking/state table, one log table.
- Do not mix feeding state into `horse_packing_kits` or `horse_kit_changes`.
- If roster attributes are needed, they should remain profile/roster support and not become feeding state.
