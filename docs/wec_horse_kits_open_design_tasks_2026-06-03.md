# WEC Horse Kits Open Design Tasks

Status: open checklist. Complete these before adding the next stacks, or the same drift will repeat.

## Table / Main List

- [ ] Container max width must be locked to the approved app width, not full-page render.
- [ ] Mobile must shrink cleanly to 390-393px before any overflow behavior.
- [ ] Remove horizontal overflow slider as the primary mobile experience.
- [ ] Hard min widths:
  - number columns around 60px.
  - number labels centered.
  - name/display column left aligned and allowed to take the remaining width.
- [ ] Horse and Kit/display label columns must be consistent and not random `4 | 3 | 3` style widths.
- [ ] Table labels must never truncate to `Nee..` or use ellipsis for approved visible labels.
- [ ] Open button should not be its own column.
- [ ] On mobile, hide Open button and use a compact hot tag on the name row.
- [ ] Search must include both `barn_name` and `show_name`.
- [ ] Search input must include a clear `x`.

## Typography

- [ ] Outfit must be applied consistently.
- [ ] Label typography must be consistent across:
  - `search_items`
  - kit item labels such as `banamine`
  - `Plan`
  - `System`
  - comments labels
- [ ] `banamine` / kit item row title is too large; reduce to the approved dense label scale.
- [ ] `search_items` label and input font are too large.
- [ ] `search_items` needs left padding.
- [ ] Remove quantity text under kit item names.
- [ ] Kit item row line-height should be tighter, around auto/22px.
- [ ] `0% packed` and progress text must use approved font/weight/scale.

## Aggregates / Progress

- [ ] Aggregates are Airtable-style aggs, not links/cards.
- [ ] Use display labels for both Kit and Kit Item sources.
- [ ] Agg block should show number over label.
- [ ] Agg block needs max width.
- [ ] No color on aggregate blocks unless explicitly approved.
- [ ] Add simple packed progress below the aggregate grid.
- [ ] Progress bar color must use approved packed color when progress exists.

## Drawer / Flyup

- [ ] Drawer must fly up to 100vh when content requires it.
- [ ] Drawer radius removed.
- [ ] Drawer close button must be a real styled closer, not bold text `x`.
- [ ] Drawer header must remain sticky.
- [ ] Kit item list must scroll under sticky drawer header when over viewport height.
- [ ] Drawer item rows must all use the same structure and styling.
- [ ] Modal should show all kit items for the selected horse/kit, not only active test subset unless filtered.

## Kit Item Controls

- [ ] Default item state is `Not Packed`.
- [ ] Active state background:
  - `not_packed`: current approved brown.
  - `packed`: green.
  - `not_needed`: gray.
- [ ] Add filters above kit items:
  - `All`
  - `Not Packed`
  - `Packed`
  - `Not Needed`
- [ ] Filters should not introduce unapproved color.
- [ ] Optimistic UI must be visibly immediate on state buttons.
- [ ] Moving item `not_packed -> packed -> not_packed` must work repeatedly.

## Add Item / Hidden Future Controls

- [ ] `rs-add-row` redesign:
  - label should be `add_item`.
  - input must be much larger for mobile entry.
  - `ADD` uses input value.
  - `ADD +1` increments one click up to max 6.
- [ ] `rs-add-label` styling must match the same label system.
- [ ] Decision/open block can exist as future drawer control but should be `is-hidden` until approved.

## Bottom Blocks / System Text

- [ ] Add bottom blocks like approved example.
- [ ] Treat `Plan:` and `System:` as consistent label/value pairs.
- [ ] `Plan: Horse Specific`.
- [ ] `System: Changes save to Airtable through Webflow Cloud.`
- [ ] Do not render these as oversized standalone heading/card text.

## Comments UI

- [ ] Comments container must follow the same stacked drawer block method.
- [ ] Existing comments list plus add/edit/save.
- [ ] Comment short select or actual text input.
- [ ] Comment logs must preserve audit trail.
- [ ] Full comments feed belongs to future `comments_ui`, not only section drawer comments.

## Data / Source Display

- [ ] Source of Kit label must be `pak_kits.display_label`.
- [ ] Source of Kit Item label must be `pak_kit_items.display_label`.
- [ ] Do not surface raw keys like `fake_tail` when display label exists.
- [ ] Do not source random labels from fallback fields unless documented as temporary.

## Verification Gate

- [ ] Verify at desktop width.
- [ ] Verify at 393px mobile width.
- [ ] Verify no overflow slider appears for normal table use.
- [ ] Verify drawer flyup reaches 100vh when content requires.
- [ ] Verify repeated optimistic state toggles.
- [ ] Verify comment save/edit writes and logs.
