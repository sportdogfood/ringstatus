# WEC Horse Turnout Stack Note

Status: document only. Do not implement during the current Horse Kits delivery pass.

## Proposed Stack

- `horse_rosters`: horse roster/source table.
  - `this_horse`: selected horse record.
- `horse_stalls`: stall/location source table.
  - `this_stall`: assigned stall record.
- `horse_turnouts`: turnout destination/source table.
  - `this_turnout`: assigned turnout/paddock record.
- `horse_slots`: time slot/calendar source table.
  - `this_time`: selected turnout time or duration window.
- `horse_turnout_link`: active linking/state table for horse + stall + turnout + time.
- `turnout_logs`: audit trail for checkout, assign, return, complete, edit, and cancel actions.

## Workflow

1. Start with `this_horse` from `horse_rosters`.
2. Confirm the horse's current `this_stall` from `horse_stalls`.
3. Check the horse out of the assigned stall.
4. Assign `this_turnout` from `horse_turnouts`.
5. Assign `this_time` from `horse_slots`.
6. Create or update `horse_turnout_link` as the active state row.
7. Return the horse to the assigned stall.
8. Mark the turnout as complete.
9. Write every state change to `turnout_logs`.

## Boundary

- `horse_turnout_link` is the current state table.
- `turnout_logs` is the history table.
- Do not mix turnout state into Horse Kits, feeding, packing item state, or roster attributes.
- Stall assignment should remain separate from turnout history; turnout only checks out from and returns to the assigned stall.
