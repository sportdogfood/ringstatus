# WEC Packing Remaining Work Contract

Status: draft. This is the working checklist for finishing the rendered WEC packing app without drifting from the `pak_groups` blueprint model.

## Non-Negotiable Rules

- Airtable is the source of truth.
- Browser state is only optimistic, cached, or session-scoped.
- Every rendered page is driven by `pak_groups`.
- Every module gets its own isolated frontend and backend path.
- Shared code is limited to safe Airtable helpers, global CSS classes, session helpers, and generic render utilities.
- Do not use fallback data when an Airtable field/table/view is missing.
- Do not backfill records unless explicitly approved.
- Do not read or write whole Airtable rows when an allowed-field manifest exists.
- Every write updates the current state row and appends a log/audit row.
- Every visual block uses the same global CSS classes for labels, controls, aggs, search, tables, drawers, and comments.

## Shared Session Contract

Still needed:

- Create or confirm the session table.
- Add one session record on first page engagement.
- Reuse that same session record for all clicks, polls, heartbeats, and logs.
- Store the session id on device.
- Add idle detection.
- Continue polling while active.
- Stop or slow polling after idle for the approved idle window.
- Resume polling on user activity.
- Include `sessionId` in every API request.
- Include `actionKey` / `optimisticKey` for every click write.
- Prevent duplicate writes when the same action/key is already in flight.
- After every write, refresh from Airtable truth.

Storage options to decide:

- Cookie: persistent device recognition.
- `sessionStorage`: current-tab/session id and UI state.
- IndexedDB: only if we need larger cached state, offline queue, or retry queue.

Minimum session JSON:

```json
{
  "sessionId": "rs_session_id",
  "pageKey": "horse_specific",
  "moduleKey": "horse_kits",
  "packWaveKey": "wave_one",
  "startedAt": "iso_datetime",
  "lastSeenAt": "iso_datetime",
  "idleState": "active",
  "deviceId": "optional_device_id",
  "clientUrl": "current_url"
}
```

## Shared API JSON Shapes

Every module needs its exact shapes documented before implementation.

Each rendered page/module has its own page-specific cycle:

- page-specific endpoint
- page-specific state shape
- page-specific allowed fields
- page-specific write actions
- page-specific logs
- page-specific polling cadence if needed

Shared entities must not be duplicated per page when the source is the same. For example, horses should come from the same canonical roster/source across all modules that need horses. Do not create a separate `horses.json` or separate horse payload for every page unless the page has a documented reason to use a different horse scope.

Shared source rule:

- `pak_horses_roster` is the canonical roster for the new rendered modules unless a module explicitly documents another source.
- A page can filter or scope the shared horse source, but it should not redefine the horse record shape.
- Cross-module entities such as horses, waves, tabs, plans, and comments should have one canonical normalized shape and be referenced by page modules.
- Page-specific state should reference shared entity ids instead of copying full duplicate records where possible.

## Table Naming Families

Use similar naming patterns, but do not overlap table names or responsibilities across plan families. The names below are examples/placeholders and may need to be renamed to prevent conflicts with existing Airtable tables, legacy app tables, or future module boundaries.

Existing shared roster:

- `pak_horses_roster`

`pak_horses_roster` already exists and does not match the example singular naming pattern. Keep this table as-is unless a separate migration is explicitly approved. New module families should adapt to this existing roster instead of renaming it during page/module work.

Example horse kits:

- `pak_kits`
- `pak_kit_items`
- kit-specific linking table
- kit-specific logs table
- kit-specific lanes if needed
- kit-specific slots if needed

Example quantity:

- `pak_byqtys`
- `pak_byqty_items`
- quantity-specific linking table
- quantity-specific logs table
- quantity-specific lanes if needed
- quantity-specific slots if needed

Example per horse:

- `pak_byhorses`
- `pak_byhorse_items`
- per-horse-specific linking table
- per-horse-specific logs table
- per-horse-specific lanes if needed
- per-horse-specific slots if needed

Example per groom:

- `pak_bygrooms`
- `pak_bygroom_items`
- per-groom-specific linking table
- per-groom-specific logs table
- per-groom-specific lanes if needed
- per-groom-specific slots if needed

Naming rules:

- No table name should serve two plan families.
- Links and logs must be named for their plan family.
- Lanes and slots can follow the same pattern when plan-specific lanes/slots are needed.
- Shared lookup/source entities can be reused only when they are truly cross-module and documented as shared.
- Before creating or renaming a table, check existing Airtable names and legacy references.
- If a proposed name conflicts, rename the new family rather than overloading an existing table.

State request:

```json
{
  "pageKey": "quantity",
  "packWaveKey": "wave_one",
  "sessionId": "rs_session_id"
}
```

State response:

```json
{
  "ok": true,
  "moduleKey": "quantity",
  "pageKey": "quantity",
  "session": {},
  "source": {},
  "blueprint": [],
  "controls": {},
  "aggregates": {},
  "rows": [],
  "drawer": {},
  "comments": [],
  "allowedFields": {},
  "pending": {}
}
```

Click/write request:

```json
{
  "action": "set_state",
  "sessionId": "rs_session_id",
  "actionKey": "stable_action_key",
  "optimisticKey": "stable_ui_key",
  "pageKey": "quantity",
  "packWaveKey": "wave_one",
  "recordId": "airtable_record_id",
  "payload": {}
}
```

Click/write response:

```json
{
  "ok": true,
  "sessionId": "rs_session_id",
  "actionKey": "stable_action_key",
  "write": {},
  "log": {},
  "statePatch": {},
  "refreshAfterMs": 250
}
```

## Allowed Field Contract

Still needed for every module:

- `allowedReadFields`
- `allowedWriteFields`
- `allowedCreateFields`
- `allowedSortFields`
- `allowedSearchFields`
- `allowedFilterFields`
- `linkedRecordFields`
- `rollup/count fields used for display`

The API should only request the fields in the manifest. The frontend should never receive unused Airtable fields.

## Two-Way Add/Edit Contract

Some modules are not just state toggles. They are two-way add/edit modules and must be designed as editors from the start.

Known two-way modules:

- `customize_ui`
- `horse_profiles` / `horses_rosters_ui`
- `comments_ui`
- any future admin/config page approved for direct editing

Rules:

- Add and edit are first-class actions.
- Every add/edit action needs allowed create/write fields.
- Every add/edit action needs validation before Airtable write.
- Every add/edit action needs an audit/log row.
- The UI must distinguish display state from edit state.
- The current/display table remains the display source.
- The log table remains the audit trail.
- Do not use logs as the display source.

## Pak Groups Blueprint Contract

Each page/view needs these records in `pak_groups`:

- `header`
- `primary_tabs`
- `summary_aggs`
- `secondary_controls`
- `secondary_count_aggs`
- `lane_controls`
- `search`
- `main_table`
- `comments`
- drawer-specific blocks where needed

Each `pak_groups` row must define:

- `render_key`
- `display_label`
- `physical_table`
- `component_key`
- `sort_order`
- `active`
- `is_hidden`
- linked `pak_views` when controls are driven by views
- linked `pak_aggs` when aggregates are rendered
- linked entities/link/log rows where state is changed

State blueprint rule:

- `pak_groups` is the only page blueprint that may declare running quantity blocks, item-state blocks, drawer state blocks, or action/control blocks.
- Do not create separate per-page blueprint JSON files for quantity state or item state.
- The actual running values must stay in the correct state/link tables.
- `pak_groups` declares how those state tables are rendered and which component/action pattern is used.
- Module code may read the `pak_groups` blueprint and then load the approved state/link/log tables, but it must not invent a separate state blueprint in code.

## Visual Target And Color Contract

Still needed:

- Define target background colors by plan.
- Define target background colors by `pak_tab`.
- Define aggregate background colors by plan.
- Define aggregate background colors by `pak_tab`.
- Define active class colors by plan.
- Define active class colors by `pak_tab`.
- Decide where these values live: `pak_groups`, `pak_tabs`, `pak_aggs`, `wec_list_plans`, or a linked style/config table.

Rules:

- No one-off hardcoded colors in a page module.
- Active button color, target background, and aggregate background must come from the approved Airtable/config source.
- Plan color and `pak_tab` color must be able to differ.
- If both plan and `pak_tab` define a color, document the priority order before rendering.
- CSS should apply colors through shared classes or CSS variables, not new one-off selectors.

Target values to document per rendered block:

```json
{
  "planKey": "quantity",
  "pakTabKey": "barn",
  "targetBg": "#ffffff",
  "aggBg": "#ffffff",
  "activeClass": "is-active",
  "activeBg": "#0f2742",
  "activeText": "#ffffff"
}
```

## Module Checklist

### 1. Horse Specific / Horse Kits

Status: active module, close to reusable reference but still needs final proof.

Still needed:

- Confirm no fallback counts.
- Confirm optimistic UI with repeated toggles.
- Confirm session record creation and polling.
- Confirm allowed fields are documented.
- Confirm comments add/edit/log.
- Confirm per-horse print and full list print.
- Confirm drawer/table CSS stays locked at mobile width.
- Confirm `pak_groups` is the only page stack source.

Primary tables:

- `pak_groups`
- `pak_horses_roster`
- `pak_kits`
- `pak_kit_items`
- `horse_packing_kits`
- `horse_kit_changes`
- `wec_commenting`
- `comment_shorts`
- `comment_logs`

### 2. Quantity

Status: next blueprint to create.

Rules:

- Manually set starting quantity, for example `10` or `20`.
- No horse member logic.
- Packed count is a quantity counter.
- Add input uses entered quantity.
- Add +1 increments by 1.
- Clear resets packed quantity.
- Mark packed sets packed to needed quantity.
- Left equals needed minus packed.
- Link/state rows use the set quantity.
- Logs use debit/credit entries against that quantity.
- Reversals write the opposite debit/credit.
- Starting needed quantity can be adjusted later.
- Quantity adjustments must log old value, new value, and reason.
- Example adjustment: `20` to `10`, reason: `we do not need as much as expected`.
- Special exception/list states must be supported, including `unresolved`, `purchase_onsite`, and `packed_max`.

Still needed:

- Create `pak_groups` view/records for `quantity`.
- Confirm physical source/state/log tables.
- Define allowed fields.
- Define JSON shape.
- Define aggregates.
- Define drawer controls.
- Define comments behavior.
- Define audit events.

### 3. Per Horse

Status: blueprint still needed.

Rules:

- Count math only.
- Each item has a multiplier.
- Wildcard is total scoped horse count.
- Per-horse quantity/count source comes from the waves table.
- Needed equals item multiplier times current scoped horse count.
- Horse count can change and therefore needed can change.
- Packed total does not change only because needed changes.
- Does not track named horse packed states.
- Packed is normal quantity progress against needed.
- Link/state rows and logs use the current calculated needed quantity.
- Packing actions use the same debit/credit log pattern as `quantity`.
- Reversals write the opposite debit/credit.
- Special exception/list states must be supported, including `unresolved`, `purchase_onsite`, and `packed_max`.

Still needed:

- Create `pak_groups` view/records for `per_horse`.
- Confirm scoped horse count source.
- Confirm whether counts are frozen per wave.
- Define state/log table.
- Define JSON shape and allowed fields.

### 4. Per Groom

Status: blueprint still needed.

Rules:

- Dynamic ratio/count math.
- Per-groom target values come from the waves table.
- Needed is based on total horses and number/ratio of grooms.
- Horse count can change.
- Groom ratio/count can change.
- Each item includes its own wildcard/multiplier.
- Total needed is dynamic.
- Packed total does not change only because needed changes.
- Packing actions use the same debit/credit log pattern as `quantity`.
- Reversals write the opposite debit/credit.
- Special exception/list states must be supported, including `unresolved`, `purchase_onsite`, and `packed_max`.
- Grooms are operational counts, not named groom assignments unless later approved.

Still needed:

- Create `pak_groups` view/records for `per_groom`.
- Confirm groom count source.
- Define state/log table.
- Define JSON shape and allowed fields.

### 5. Home Overview

Status: required page.

Still needed:

- Define `pak_groups` stack.
- Define top-level summary aggs.
- Define links into plan pages.
- Define open/touched/problem rows.
- Define session behavior.
- Define print/report entry points.

### 6. Report Lists

Status: required page.

Still needed:

- Define report/list source tables and views.
- Define list rendering shape.
- Define sorting/filtering.
- Define print behavior.
- Define whether reports are read-only or have write actions.

### 7. Print

Status: required shared service.

Rules:

- Prefer route-driven print HTML.
- Do not create a separate Webflow print page per module unless there is no other path.

Still needed:

- Full list print.
- Per-horse print.
- Per-module print routes.
- Autoprint behavior.
- PDF worker decision.
- Print query param contract.
- Print-safe CSS.

### 8. Comments UI

Status: section comments exist conceptually; full feed still needed.

Tables:

- `wec_commenting`
- `comment_shorts`
- `comment_logs`

Still needed:

- Add comment.
- Edit comment.
- Save comment.
- Two-way add/edit write path.
- Optional comment short select.
- Log every write.
- Full comments page/feed.
- Accordion rows.
- Open accordion min height and internal scroll.
- Latest-first sorting.
- Same global label CSS as every other stack label.

### 9. Horse Profiles / Rosters

Status: not fully implemented.

Tables discussed:

- `pak_horses_roster`
- `ww_horses`
- `horse_genders`
- `horse_disciplines`
- `horse_colors`
- possible `horse_attributes`
- `horses_change_log`

Still needed:

- Decide whether to create unified `horse_attributes`.
- Add horse.
- Edit horse.
- Apply/edit attributes.
- Two-way add/edit write path.
- Log every profile/attribute change.
- Keep profile state separate from kits, feeding, and turnout.

### 10. Boards Feed / Horse Feeding

Status: future module.

Tables discussed:

- `horse_feeding`
- `horse_feed`
- `horse_feed_items`
- `horse_feed_rations`
- `horse_feed_links`
- `horse_feed_logs`

Still needed:

- Define source tables.
- Define active link/state table.
- Define log table.
- Decide whether quantities are plan-level, horse-level, or ration-level.
- Define `pak_groups` stack.
- Define JSON shape and allowed fields.

### 11. Boards Stalls + Turnout

Status: future module.

Tables discussed:

- `pak_horses_roster`
- `horse_stalls`
- `horse_turnouts`
- `horse_slots`
- `horse_turnout_link`
- `turnout_logs`

Workflow:

1. Select horse.
2. Resolve assigned stall.
3. Check horse out of stall.
4. Assign turnout.
5. Assign time slot or duration.
6. Create/update active turnout link.
7. Return horse to stall.
8. Complete turnout.
9. Log every step.

Still needed:

- Confirm exact table names.
- Define selected entities: `this_horse`, `this_stall`, `this_turnout`, `this_time`.
- Define active state model.
- Define audit events.
- Define UI stack.

### 12. Customize UI

Status: future module.

Still needed:

- Edit site/page labels.
- Edit pack item labels.
- Assign items to lists.
- Manage `pak_groups` stack rows if approved.
- Manage views/aggs/control definitions if approved.
- Two-way add/edit write path.
- Strong write gating and audit logs.

## Verification Required Before Any Module Is Called Complete

- Local endpoint returns only allowed fields.
- Local UI loads the exact module path.
- No legacy JS executes for isolated modules.
- Optimistic click updates immediately.
- Duplicate click/action is blocked while in flight.
- API write updates current state row.
- API write appends audit log.
- Refresh returns Airtable truth.
- Session record exists and receives heartbeat/poll activity.
- Mobile width works at 393px without unintended horizontal touch friction.
- Table headers, labels, search, comments, and item titles use the same global CSS.
- Print route works for the module.
- No fallback data appears in UI.
