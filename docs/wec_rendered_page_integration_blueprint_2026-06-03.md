# WEC Rendered Page Integration Blueprint

Status: current working blueprint for Horse Kits and the next stacked WEC pages.

This document exists to prevent drift. The page renderer should paint Airtable-driven blocks from `pak_groups`; it should not invent new layout, typography, table structure, or labels when Airtable already defines the stack.

## Current Surface

Local preview:

- `http://127.0.0.1:8792/horse-kits-static-proof-preview.html`

Webflow page target:

- `/rswp-horse-kits`

Current frontend assets:

- `webflow/packing-worksheet/horse-kits.js`
- `webflow/packing-worksheet/horse-kits.css`
- `webflow/packing-worksheet/rsa-stylesheets.locked.css`
- `webflow/packing-worksheet/styles.css`

Static proof assets:

- `webflow/packing-worksheet/horse-kits-static-proof.js`
- `webflow/packing-worksheet/horse-kits-static-proof.css`
- `webflow/packing-worksheet/horse-kits-static-proof-preview.html`

Webflow Cloud endpoints:

- `GET /test/wec-packing/horse-kits`
- `POST /test/wec-packing/horse-kits`
- `GET /test/wec-packing/horse-kits/print`

Endpoint files:

- `webflow-cloud-test/src/pages/wec-packing/horse-kits.js`
- `webflow-cloud-test/src/pages/wec-packing/horse-kits/print.js`
- shared loader/actions: `webflow-cloud-test/src/lib/wec-packing.js`

## Integration Rule

The browser does not call Airtable directly.

The browser calls Webflow Cloud:

```text
Webflow page embed
  -> CDN frontend JS/CSS
  -> Webflow Cloud route
  -> Airtable REST API
  -> JSON state back to frontend
```

The Webflow Cloud route calls Airtable using:

- `AIRTABLE_TOKEN`
- `AIRTABLE_BASE_ID` or `AIRTABLE_BASE`
- `AIRTABLE_WEC_META_TABLE` optional; default is `tbllJywsOstkqT5yZ`

Do not put the Airtable token in Webflow page code or frontend JS.

## How To Call The Page Data

Local:

```bash
curl "http://127.0.0.1:4331/wec-packing/horse-kits?packWaveKey=wave_one"
```

Production:

```bash
curl "https://ringstatus.com/test/wec-packing/horse-kits?packWaveKey=wave_one"
```

Important query params:

- `packWaveKey=wave_one`
- `packWaveId=<airtable record id>` if a direct wave record is needed later

The response includes:

- `source`: resolved table IDs, table aliases, selected wave, source mode
- `wave`: selected `wec_pack_waves` record normalized for UI
- `counts`: Airtable aggregate counts
- `horses`: visible horses for selected wave
- `allHorses`: all horses available to local secondary filters
- `kits`: kits with nested kit items
- `kitItems`: kit item source records
- `packingRows`: active state/linking rows
- `changes`: recent audit changes
- `comments`
- `commentShorts`
- `commentLogs`
- `primaryTabs`
- `laneControls`
- `secondaryControls`
- `pakAggs`
- `horseAttributes`
- `horseRosterLogs`
- `groupStack`: the `pak_groups` rendered stack

## How To Call Actions

All writes go to:

```text
POST /test/wec-packing/horse-kits?packWaveKey=wave_one
```

Content type:

```http
Content-Type: application/json
```

Set a kit item state:

```json
{
  "action": "set_static_kit_item_state",
  "horseId": "rec...",
  "kitId": "rec...",
  "kitItemId": "rec...",
  "packWaveId": "rec...",
  "packState": "packed"
}
```

Allowed `packState`:

- `not_packed`
- `packed`
- `not_needed`

Backend behavior:

- Creates or patches `horse_packing_kits`.
- Uses `pak_kits` / `pak_kit_items` when the current kit source is `pak`.
- Writes `horse_kit_changes`.
- Returns fresh page state.

Save a comment:

```json
{
  "action": "save_comment",
  "horseId": "rec...",
  "scopeLabel": "Arrow",
  "packWaveId": "rec...",
  "commentShortId": "rec...",
  "comment": "Wash needed"
}
```

Backend behavior:

- Creates or patches `wec_commenting`.
- Writes `comment_logs` when configured.

Apply a horse roster attribute:

```json
{
  "action": "apply_horse_attribute",
  "horseId": "rec...",
  "rosterId": "rec...",
  "attributeId": "rec...",
  "attributeGroup": "horse_colors"
}
```

Backend behavior:

- Resolves `pak_horses_roster`.
- Applies selected attribute to the linked `ww_horses` profile field.
- Writes `horses_change_log`.

## Airtable Tables For Horse Kits

Current data source table groups:

- `wec_pack_waves`: report title, subtitle, current wave, linked tabs.
- `pak_groups`: rendered page blueprint. Current view: `horse_specific`.
- `pak_tabs`: primary tabs.
- `pak_views`: secondary controls.
- `wec_lanes`: lane controls. Current view: `horse_specific`.
- `pak_aggs`: aggregate definitions linked to `pak_groups`.
- `pak_horses_roster`: entity 1 / main table source.
- `pak_kits`: kit/list source.
- `pak_kit_items`: entity 2 / drawer item source.
- `horse_packing_kits`: active linking/state table.
- `horse_kit_changes`: audit trail for kit item state changes.
- `wec_commenting`: comment records.
- `comment_shorts`: canned comment choices.
- `comment_logs`: comment audit trail.
- `horse_genders`, `horse_disciplines`, `horse_colors`: roster attribute option sources.
- `horses_change_log`: roster/profile change trail.

Legacy tables are still readable but should not drive new Horse Kits design when the `pak_*` tables exist:

- `horse_kits`
- `horse_kit_items`
- `wec_horses`

Table aliases used by `pak_groups`:

- `pak_horses` -> `pak_horses_roster`
- `pak_horse_kits_list` -> `pak_kits`
- `pak_horse_kit_items` -> `pak_kit_items`
- `pak_horse_kit_links` -> `horse_packing_kits`
- `pak_horse_kit_logs` -> `horse_kit_changes`
- `pak_comments` -> `wec_commenting`

## Pak Groups As Page Blueprint

`pak_groups` is the page stack contract.

Required fields:

- `render_key`: stable block key.
- `display_label`: label shown by the UI.
- `physical_table`: actual table to read/write.
- `component_key`: renderer family/class.
- `sort_order`: ordering inside the page.

Additional fields used by the loader:

- `group_key`
- `gp_pre`
- `stack`
- `role`
- `table_name`
- `active`
- `is_hidden`
- `include_on_drawer`
- `is_drill_down`
- `add_filter`
- `filter_by`
- `add_search`
- `search_by`
- `add_aggregates`
- `pak_aggs`
- `pak_views`
- `all_aggregates`
- `needs_ui`
- `allow_add_new`
- `allow_inline_edit`

Current Horse Kits view:

- `pak_groups` view `horse_specific`

Approved rendered stack order:

1. `header`
2. `primary_tabs`
3. `summary_aggs`
4. `secondary_controls`
5. `count_aggs`
6. `lane_controls`
7. `search`
8. `main_table`
9. `comments`

Rendered class mapping:

- `header` -> `.rs-stack-section.is-header`
- `primary_tabs` -> `.rs-stack-section.is-primary-tabs`
- `summary_aggs` -> `.rs-stack-section.is-summary-aggs` with `.rs-stack-aggs`
- `secondary_controls` -> `.rs-stack-section.is-secondary-controls`
- `count_aggs` -> `.rs-stack-section.is-count-aggs` with `.rs-secondary-count-aggs`
- `lane_controls` -> `.rs-stack-section.is-lane-controls`
- `search` -> `.rs-stack-section.is-search`
- `main_table` -> `.rs-stack-section.is-main-table`
- `comments` -> `.rs-stack-section.is-comments`

If a future page needs a block, add or update a `pak_groups` row. Do not add a one-off frontend section unless it is documented as a temporary blocker.

## Current Horse Kits Page Behavior

Main table:

- Entity 1 is horse.
- Entity 2 is kit/list.
- Counts are `need | packed | left`.
- Open action is attached to the row/name area, not a separate table column.
- Search includes `name`, `barnName`, `showName`, `barn_name`, `show_name`, and `display_horse_barn_name`.
- Sort supports horse, kit, need, packed, left.

Drawer:

- Header shows selected horse.
- Profile link displays only when `profileUrl` is present.
- Kit label block is hidden.
- Progress and top metrics appear before item controls.
- Search label derives from the `drawer_items` row display label.
- Filter label derives from the `drawer_items` row display label.
- Item table label derives from the `drawer_items` row display label.
- Item filters are local filters, not Airtable views:
  - `All`
  - `Not Packed`
  - `Packed`
  - `Not Needed`
- Kit item state buttons are optimistic: UI updates immediately while POST is pending.
- The backend then writes `horse_packing_kits` and `horse_kit_changes`.

Counts:

- Page-level aggs come from Airtable state/counts and linked `pak_aggs`.
- Row and drawer counts use effective per-horse kit summary when present.
- Do not show contradictory values, such as a horse appearing in `PACKED` while its row says `0 packed`.

## Template CSS Consistency Rules

Do not create one-off typography for labels.

Use `.rs-stack-label` for:

- section labels
- table headers
- drawer search label
- drawer filter label
- drawer item table label
- kit item row title
- `Plan:`
- `System:`
- comments label

Current verified label/table head style:

```css
font-family: Outfit, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
font-size: 12px;
font-weight: 600;
line-height: 15px;
color: #68707a;
text-transform: uppercase;
padding-bottom: 3px;
```

Table headers:

- `#` centered.
- Horse and Kit left aligned.
- Need, Packed, Left centered.
- Need/Packed/Left min width 60px.
- Need/Packed/Left should not shrink or wrap.

Search controls:

- Every search uses the same search component pattern.
- Every search input has a clear `x`.
- Clear `x` is not a bordered circular button inside the input.
- Typing in search must not jump to top.
- Use `focus({ preventScroll: true })` when restoring focus.
- Preserve table scroll and drawer scroll through render.

Buttons:

- Use the same pill/button class for all peer controls.
- Active state uses `.is-active`.
- Do not make one-off print button styling.
- If print is a lane/control, it must either be wired or removed from that control set.

Aggregates:

- Top aggs and secondary count aggs are not links unless explicitly approved.
- Use Airtable `display_label`/linked `pak_aggs` labels.
- Top tier can use stronger backgrounds.
- Secondary tier should be visually related but not identical if requested.
- Keep same width discipline across agg blocks.

Mobile:

- Design target is 390-393px minimum.
- Avoid horizontal overflow as the primary mobile interaction.
- Number columns stay fixed/minimum.
- Entity/name columns take remaining width.

## Webflow Embed Rule

The manual embed should be small and stable.

Current hardcoded embed shape:

```html
<main id="packing-app" class="rsa-dashboard">
  <div id="horse-kits" data-pack-wave-key="wave_one">Loading horse kits...</div>
</main>

<script>
  (function () {
    var root = document.getElementById("horse-kits");
    var assetVersion = "COMMIT_HASH";
    var cacheKey = "horse-kits-20260603-" + assetVersion;
    var assetBase = "https://cdn.jsdelivr.net/gh/sportdogfood/ringstatus@" + assetVersion + "/webflow/packing-worksheet";

    if (!root) return;

    root.dataset.apiUrl = "https://ringstatus.com/test/wec-packing/horse-kits";
    root.dataset.packWaveKey = root.dataset.packWaveKey || "wave_one";

    function addStyle(href) {
      var link = document.createElement("link");
      link.rel = "stylesheet";
      link.href = href;
      document.head.appendChild(link);
    }

    function addScript(src) {
      var script = document.createElement("script");
      script.src = src;
      script.defer = true;
      document.head.appendChild(script);
    }

    addStyle("https://fonts.googleapis.com/css2?family=Outfit:wght@400;500;600;700&display=swap");
    addStyle(assetBase + "/rsa-stylesheets.locked.css?v=" + cacheKey);
    addStyle(assetBase + "/styles.css?v=" + cacheKey);
    addStyle(assetBase + "/horse-kits.css?v=" + cacheKey);
    addScript(assetBase + "/horse-kits.js?v=" + cacheKey);
  })();
</script>
```

Known friction:

- Updating every page embed by hand does not scale.
- Use a build list like `rs-builds.js` only if the page embed can load the build list once and then resolve the page hash automatically.
- If the embed is hardcoded, verify the exact commit hash in the browser network panel or rendered script URL.

## Print

Existing print route:

- `GET /test/wec-packing/horse-kits/print?packWaveKey=wave_one&autoprint=1`

Current print behavior:

- Returns print HTML from `horseKitPrintHtml(report, request.url)`.
- Intended for the full list.

Still needed:

- Per-horse print path for selected horse and related kit items.
- The main app should open print route with enough query params to identify:
  - `packWaveKey`
  - selected horse id or roster id
  - selected kit id if needed
- Do not make separate Webflow print pages per module unless there is no other path. Prefer route-driven print HTML.

## Comments Module

Current drawer/page comment tables:

- `wec_commenting`
- `comment_shorts`
- `comment_logs`

Current behavior:

- Add comment.
- Edit existing comment.
- Save comment.
- Optional comment short select can populate the comment body.
- Log writes to `comment_logs`.

Required next comments UI:

- Keep section comments in the module drawer/page.
- Add full `comments_ui` page/stack for the full feed.
- Full feed should show comment threads as rows inside an accordion.
- When accordion opens, give it a minimum height and internal scroll.
- Sort comments latest first.
- Do not style comments label differently from `Plan`, `System`, search labels, or item labels.

Comment write rule:

- The comment record is the current/display object.
- The comment log is the audit trail.
- Do not use comment logs as the display source.

## Horse Roster / Attributes Module

Not fully implemented as a page yet.

Discussed tables:

- `pak_horses_roster`: roster/source row.
- `ww_horses`: linked horse profile row.
- `horse_genders`: attribute options.
- `horse_disciplines`: attribute options.
- `horse_colors`: attribute options.
- `horse_attributes`: desired unified option table, does not currently exist.
- `horses_change_log`: existing log table, naming is inconsistent but currently used by the backend as `horse_roster_logs`.

Current backend action:

- `apply_horse_attribute`

Required page behavior:

- Add horse.
- Edit horse.
- Apply or edit horse attributes.
- Log every profile/attribute change.
- Keep roster/profile support separate from kit state, feeding state, and turnout state.

Decision still needed:

- Keep separate option tables (`horse_genders`, `horse_disciplines`, `horse_colors`) or create `horse_attributes` as the unified option source.
- If `horse_attributes` is created, document field mapping before replacing existing option tables.

## Feeding Module

Not implemented now. Documented future stack.

Required table chain:

- `horse_feeding`: page/plan level source.
- `horse_feed`: per-horse feed plan or active feed assignment.
- `horse_feed_items`: reusable feed item definitions.
- `horse_feed_rations`: per-horse/per-feed quantity or ration details.
- `horse_feed_links`: active linking/state table.
- `horse_feed_logs`: audit trail.

Rules:

- Follow the same `pak_groups` stack method.
- Use one or more source tables, one active link/state table, and one log table.
- Do not mix feeding state into `horse_packing_kits`, `horse_kit_changes`, or roster attributes.
- If feed quantities are needed, define whether quantity is plan-level, horse-level, or ration-level before rendering controls.

## Turnout And Stall Checkout Module

Not implemented now. Documented future stack.

Required source/state/log chain:

- `horse_rosters` or `pak_horses_roster`: horse source.
- `horse_stalls`: assigned stall/location source.
- `horse_turnouts`: turnout/paddock destination source.
- `horse_slots`: time slot/calendar source.
- `horse_turnout_link`: active state table.
- `turnout_logs`: audit trail.

Conceptual selected entities:

- `this_horse`
- `this_stall`
- `this_turnout`
- `this_time`

Workflow:

1. Select horse from roster.
2. Resolve assigned stall.
3. Check horse out of assigned stall.
4. Assign turnout/paddock.
5. Assign time slot or duration.
6. Create/update `horse_turnout_link`.
7. Return horse to assigned stall.
8. Complete turnout.
9. Write every step to `turnout_logs`.

Rules:

- Stall assignment is source/current location.
- Turnout link is active temporary state.
- Turnout logs are historical.
- Do not mix turnout state into Horse Kits, feeding, packing rows, or profile attributes.

## Next Page Families

These should all reuse the same rendered stack method:

- `all`
- `horse_specific`
- `per_horse`
- `quantity`
- `per_groom`
- `home_overview`
- `report_lists`
- `print`
- `horses_rosters_ui`
- `horse_feeding_ui`
- `horse_turnout_ui`
- `comments_ui`
- `customize_ui`

Each new page needs:

- `pak_groups` rows for every rendered stack block.
- source tables/views.
- optional control tables (`pak_tabs`, `pak_views`, `wec_lanes`).
- aggregate definitions in `pak_aggs` where counts are shown.
- one state/link table if the page changes state.
- one log table if the page writes anything.

## Known Trouble And Fix Rules

Known trouble:

- Agents invented layout instead of following `pak_groups`.
- CSS drift happened because labels used separate classes instead of `.rs-stack-label`.
- Table headers, item titles, search labels, comments labels, Plan/System labels diverged.
- Controls rendered without actions wired.
- Counts could contradict filters.
- Search re-render could jump the user back to top.
- Embed/hash updates caused time loss.
- Raw field/table keys appeared in UI when display labels existed.
- Mobile overflow created extra touch friction.
- Print controls appeared before print behavior was fully wired.
- Legacy tables and new `pak_*` tables were mixed without clear source priority.

Fix rules:

- Verify the live/local rendered DOM, not just code.
- Use `pak_groups` first; add a `pak_groups` row instead of hardcoding a section.
- Use `display_label` for UI labels.
- Use `.rs-stack-label` for label typography everywhere.
- Wire every rendered control or hide/remove it.
- Keep action writes behind Webflow Cloud.
- Every write gets an audit log where the table exists.
- Search must use clear `x` and preserve scroll.
- Do not add page-specific CSS if a shared stack class can carry it.
- Document every temporary hardcode with the Airtable field/row that should replace it.

## Verification Checklist

Before calling a module complete:

- Local preview loads.
- Network uses expected JS/CSS cache or commit hash.
- `GET` endpoint returns `ok: true`.
- `pak_groups` source view is correct.
- Stack order matches approved order.
- Primary controls have active state.
- Secondary controls have active state and filter/switch behavior.
- Lane controls have active state and behavior.
- Search includes expected fields and clear `x`.
- Search does not jump page/table/drawer scroll.
- Table headers match `.rs-stack-label`.
- Count columns are centered and do not wrap.
- Row values match aggregate/filter behavior.
- Drawer opens and reaches usable height.
- Drawer labels derive from `pak_groups` display labels.
- Item filters work.
- Optimistic item state changes before POST completes.
- POST creates/patches state row and writes log.
- Comments save/edit works and writes log.
- Print route opens for the correct scope.
- 393px viewport is checked.

## Current Verified Horse Kits Snapshot

Last local proof state:

- Cache: `20260603-controls-labels-4`
- Stack order: `header`, `primary_tabs`, `summary_aggs`, `secondary_controls`, `count_aggs`, `lane_controls`, `search`, `main_table`, `comments`
- Secondary agg: `37 NEED / 2 PACKED / 35 LEFT`
- `PACKED` lane: 2 rows
- `LEFT` with `ALL`: 51 rows
- Arrow row: `40 NEED / 1 PACKED / 39 LEFT`
- Drawer metrics for Arrow: `40 NEED / 1 PACKED / 39 LEFT`
- Drawer labels: `Search Kit Items`, `Filter Kit Items`, `Kit Items`
- Search scroll preserved in table and drawer
- Optimistic UI verified with intercepted POST payload for `set_static_kit_item_state`

