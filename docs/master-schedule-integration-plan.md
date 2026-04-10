# Master Schedule Integration Plan (Concept Template + JS)

## Objective
Implement a schedule component that consumes the **master schedule payload** from:

- `https://ringstatus-proxy.gombcg.workers.dev/docs/schedules/master.json`

and renders it using the provided concept template + JavaScript behavior (top bar, mode toggles, filters, ring sections, and polling).

---

## 1) Data Contract and Normalization

### 1.1 Source payload assumptions
Each row in `master.json` includes keys such as:
- `sid`, `dt`, `ring_number`, `ring_nickname`
- `class_group_sequence`, `class_group_id`, `group_name`
- `class_groupxclasses_id`, `class_id`, `class_name`, `class_type`, `class_number`
- `isFirstUp`, `is_usf`
- `start_display`, `estimated_start_time`, `secondsTill`

### 1.2 Normalize into a stable internal shape
Create a normalization step that guarantees:
- string-safe IDs (`rowKey`, `groupKey`, `ringKey`)
- sanitized text fields
- parsed time sort key from `estimated_start_time` (fallback to `start_display`)
- derived status from timing:
  - `underway` if currently first-up / very near current clock
  - `completed` if `secondsTill` significantly negative and not first-up
  - `upcoming` otherwise

### 1.3 Derived display fields
Compute per row:
- `ringTitle`: use `ring_nickname`, fallback `Ring {ring_number}`
- `classLabel`: `${class_number} - ${class_name}`
- `displayTime`: `start_display` (fallback from parsed estimate)
- `horse/group display`: optional placeholder support (empty for current payload unless later enriched)

---

## 2) Fetching and Refresh Strategy

### 2.1 Endpoints
Use the same architecture as the concept JS but with master payload as primary:
- `MASTER_URL = /docs/schedules/master.json`

If Lite mode depends on a secondary endpoint, keep it optional. If unavailable, derive lite data from master rows.

### 2.2 Polling
- Poll every 6 minutes (`POLL_MS`)
- Track `last-modified` and/or response text hash
- Re-render only on change
- Preserve scroll position on refresh

### 2.3 Empty/error states
- If payload is `[]`, show "No schedule rows"
- On parse/fetch failure, render visible error block in component and keep last good state when possible

---

## 3) UI Wiring to Provided Markup

### 3.1 Required DOM refs
Bind all `.js-*` hooks from provided template:
- `.js-title`, `.js-subtitle`
- `.js-status-filters`, `.js-horse-filters`
- `.js-rings`, `.js-ring-nav`
- `.js-topbar`, `.js-filterbar`, `.js-bottom-nav`

### 3.2 Header behavior
- Title: configurable (e.g., "Schedule")
- Subtitle: date (`dt`) + publish timestamp + next expected publish

### 3.3 Modes
Support:
- **Full**: all rings + grouped classes
- **Lite**: target/related subset (initially can map to "all rows" until target metadata exists)
- **Time**: bucketed sections (`Nextup`, `Morning`, `Afternoon`)

---

## 4) Grouping, Sorting, and Rendering

### 4.1 Ring grouping
Group rows by `ring_number`, ordered ascending numeric.

### 4.2 Group block grouping
Within each ring, group by `class_group_id`, sorted by `class_group_sequence`.

### 4.3 Class row sorting
Within each group sort by:
1) parsed start time
2) `class_number`
3) stable row key

### 4.4 Row content
Render each class row with:
- time column (`start_display`)
- class label (`class_number - class_name`)
- status icon/pill (`upcoming`, `underway`, `completed`)
- optional ring label in Time mode

---

## 5) Filters and Visibility Rules

### 5.1 Status filters
Use icon pills for:
- Upcoming
- Underway
- Completed

### 5.2 Horse filters
Payload currently appears class-centric; horse filter support should remain but be no-op/hidden unless horse tokens are present.

### 5.3 Visibility pipeline
For each render:
1) evaluate group-level horse match
2) evaluate row-level status match
3) hide empty groups
4) hide empty rings
5) rebuild bottom ring nav

---

## 6) Time Mode Rules

### 6.1 Bucket assignment
- `Nextup`: first N upcoming rows by start time (recommend N=5)
- `Morning`: rows before 12:00 PM
- `Afternoon`: rows at/after 12:00 PM

### 6.2 Nextup logic
Given this payload has `secondsTill`, use it as strongest signal:
- upcoming rows are `secondsTill >= 0` OR not completed
- sort by `secondsTill` ascending for immediate queue

---

## 7) Tenant/Config Simplification (for master feed)

The concept script includes tenant switching. For this master feed rollout:
- Keep tenant config scaffolding optional
- Default to a single schedule title/theme
- Re-introduce tenant URL mapping only if master endpoint later becomes tenant-aware

---

## 8) QA and Validation Checklist

### 8.1 Data QA
- Confirm JSON parse succeeds for live endpoint
- Verify duplicate `class_groupxclasses_id` handling (stable key fallback)
- Confirm null-safe handling for `isFirstUp`, `is_usf`, `is_classic`

### 8.2 UI QA
- Full/Lite/Time toggles switch without page reload
- Status filters combine correctly with mode
- Bottom nav scrolls to visible ring sections
- Sticky top/filter/bottom bars compute offsets correctly on resize

### 8.3 Runtime QA
- Polling updates only when changed
- No console errors during normal operation
- Large payload remains responsive

---

## 9) Implementation Sequence (Execution Plan)

1. **Create adapter layer** for master payload normalization.
2. **Swap data source wiring** to `master.json` in the component script.
3. **Map status derivation** from `secondsTill` + `isFirstUp`.
4. **Validate grouping/sorting** for ring → class group → class rows.
5. **Enable Time mode buckets** using parsed start times.
6. **Retain status filters**, conditionally suppress horse filters when empty.
7. **Add robust empty/error handling** and publish subtitle.
8. **Run manual QA** against live endpoint and edge rows (nulls/duplicates).

---

## 10) Risks / Edge Cases

- `secondsTill` may be stale relative to client clock; status rules should be tolerant.
- Multiple rows sharing identical start times and class numbers require stable fallback sorting.
- Some rows may have incomplete fields; all display paths must be null-safe.
- If endpoint latency spikes, keep prior render until new payload is valid.

---

## Deliverable
A production-ready schedule renderer using the provided HTML/JS template behavior, powered by the master schedule payload, with full/lite/time modes, status filtering, and resilient polling.
