# Barn Entry Base-Template Retrofit Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Publish Barn Entry on the locked AG base shell with the approved controls, four-column sortable grid, row-tap edit states, canonical horse mapping, fast Add pickers, working submit review, and separate three-column Ring print sheet.

**Architecture:** Keep `ag-base-shell/source.html` as immutable visual authority and retrofit only `barn-entry/source.html`. A contract test compares the embedded base CSS byte-for-byte, verifies the required DOM order and button mapping, and rejects responsive reinterpretation. Browser behavior remains client-side; the existing `barn-entry.js` POST route remains the Airtable write boundary.

**Tech Stack:** Astro 6, Cloudflare Workers, browser JavaScript, AG Grid Community 36, Node.js test runner.

## Global Constraints

- Do not edit `webflow-cloud-test/src/assets/ag-base-shell/source.html`.
- The base stylesheet, `@media (max-width: 479px)` block, and base print block must remain byte-for-byte unchanged inside Barn Entry.
- Do not add breakpoint-dependent JavaScript or alternate mobile column definitions.
- Interactive columns are exactly `TIME`, `RING`, `CLASS`, `HORSE`, all sortable.
- `action-bar-mini` exposes only `EDIT`/`DONE` and `PRINT`; `action-bar` and `action-anchors` are hidden with `is-hidden`.
- `action-bar-bottom` exposes `EDIT`/`DONE`, `ADD`, and `SEND` according to edit state.
- Whole-row status changes occur only in edit mode: `pending -> confirmed -> declined -> pending`.
- Horse display is canonical `barn_name`; rider values are never a fallback.
- Search begins at two characters and filters already-loaded rows client-side.
- Print uses a separate three-column sheet grouped by Ring.
- Preserve unrelated working-tree changes.

---

### Task 1: Lock the Base Contract in Tests

**Files:**
- Create: `tests/barn_entry_base_template_retrofit.test.js`
- Test: `webflow-cloud-test/src/assets/ag-base-shell/source.html`
- Test: `webflow-cloud-test/src/assets/barn-entry/source.html`

**Interfaces:**
- Consumes: the complete `<style>` block and structural class order from the base shell.
- Produces: a regression gate that rejects styling, responsive, DOM-order, control, grid-column, picker, and print drift.

- [ ] **Step 1: Write failing contract tests**

  Extract the base and Barn Entry style blocks; require Barn Entry to contain the base block verbatim. Assert ordered shell elements, approved IDs/labels, hidden sections, exact column headers, no `innerWidth`/resize-driven column code, helper `top_matches`, two-character search gate, edit-mode row gate, and `.print-columns` with three columns.

- [ ] **Step 2: Verify the tests fail against the old custom shell**

  Run: `node --test tests/barn_entry_base_template_retrofit.test.js`

  Expected: FAIL because the old Barn Entry source does not embed the locked base style or structure.

### Task 2: Retrofit the Locked Shell and Controls

**Files:**
- Modify: `webflow-cloud-test/src/assets/barn-entry/source.html`
- Test: `tests/barn_entry_base_template_retrofit.test.js`

**Interfaces:**
- Consumes: exact base CSS and DOM primitives.
- Produces: `miniEditBtn`, `miniPrintBtn`, `bottomEditBtn`, `bottomAddBtn`, `bottomSendBtn`, `rowCount`, `statusText`, and `agBaseGrid` elements.

- [ ] **Step 1: Replace the old custom shell with the exact base stylesheet and ordered base DOM**

  Preserve the base class names and hierarchy. Add only the approved `is-hidden` and row-outline option styles plus non-responsive functional overlay/print-sheet selectors.

- [ ] **Step 2: Wire both Edit buttons to one edit-mode state**

  Both labels switch between `EDIT` and `DONE`; Add and Send are hidden outside edit mode; Send disables while pending.

- [ ] **Step 3: Run the contract test**

  Run: `node --test tests/barn_entry_base_template_retrofit.test.js`

  Expected: shell/control assertions PASS while unimplemented behavior assertions may still FAIL.

### Task 3: Implement Four-Column Grid and Row Tap State

**Files:**
- Modify: `webflow-cloud-test/src/assets/barn-entry/source.html`
- Test: `tests/barn_entry_base_template_retrofit.test.js`

**Interfaces:**
- Consumes: normalized review rows with `display_time`, `ring_name_normalized`, `class_name`, `class_number`, `barn_name`, `review_key`, and `status`.
- Produces: one invariant `columnDefs` array and `toggleRowStatus(reviewKey)`.

- [ ] **Step 1: Define exactly four sortable columns**

  Use TIME, RING, CLASS, and HORSE. CLASS combines class number and name. HORSE reads only `barn_name` and surfaces an unresolved label when absent.

- [ ] **Step 2: Bind whole-row click and keyboard activation through the edit gate**

  Ignore row activation when `editMode === false`. In edit mode cycle pending, confirmed, declined, pending and refresh row classes.

- [ ] **Step 3: Apply only approved row state classes**

  Pending/default and tap3 have no outline; tap1 is green 2px with -2px offset; tap2 is black 2px with -2px offset.

- [ ] **Step 4: Run the contract test**

  Run: `node --test tests/barn_entry_base_template_retrofit.test.js`

  Expected: PASS for grid and row-state contracts.

### Task 4: Repair Canonical Horse Mapping and Fast Add Pickers

**Files:**
- Modify: `webflow-cloud-test/src/assets/barn-entry/source.html`
- Test: `tests/barn_entry_base_template_retrofit.test.js`

**Interfaces:**
- Consumes: schedule classes, class OOG rows, and helper response arrays including `top_matches`.
- Produces: `classCandidates`, `followedHorseCandidates`, `horseCandidates`, cached normalized `search_text`, and selected class/horse rows.

- [ ] **Step 1: Normalize helper responses from `top_matches`, `results`, or `matches`**

  Build horse candidates with `barn_name` as display and `horse`/`horse_name` as search-only aliases. Never use trainer or rider fields as horse display.

- [ ] **Step 2: Enrich mapped rows before grid insertion**

  Match source horse identifiers/names to helper candidates; set canonical `barn_name` or an explicit unresolved value rather than a rider fallback.

- [ ] **Step 3: Gate searches at two normalized characters**

  Filter preloaded normalized arrays without a request per keystroke. Horse starts with followed rows, can reveal the broader roster, then allows manual entry. Class starts with focus-day schedule rows, can reveal preflight rows, then allows manual entry.

- [ ] **Step 4: Save selected or manual candidates into a complete user-added review row**

  Preserve class/ring/time identifiers, canonical horse display, helper keys, `source: "user_added"`, and `status: "pending"`.

- [ ] **Step 5: Run the contract test**

  Run: `node --test tests/barn_entry_base_template_retrofit.test.js`

  Expected: PASS for helper shape, search gate, and horse display contracts.

### Task 5: Submit, Review, Share, and Three-Column Print

**Files:**
- Modify: `webflow-cloud-test/src/assets/barn-entry/source.html`
- Verify unchanged unless a failing test proves otherwise: `webflow-cloud-test/src/pages/barn-entry.js`
- Test: `tests/barn_entry_base_template_retrofit.test.js`
- Test: `tests/barn_entry_cloudflare_env_contract.test.js`

**Interfaces:**
- Consumes: current review rows and the existing POST response `{ ok, row_count, created }`.
- Produces: status-line progress/errors, submitted read-only state, share text, and dedicated Ring-group print DOM.

- [ ] **Step 1: Keep failed submit state intact**

  Disable SEND while pending; on non-OK response re-enable it, keep edit/grid state, and render the server error in `statusText`.

- [ ] **Step 2: Preserve successful submitted review and share behavior**

  Move to read-only submitted state only after `{ ok: true }`; retain Print and Share.

- [ ] **Step 3: Build a dedicated three-column Ring-group print sheet**

  Group rows by Ring, render Ring headings and compact TIME/CLASS/HORSE rows, omit controls and tap outlines, then call `window.print()`.

- [ ] **Step 4: Run route and retrofit tests**

  Run: `node --test tests/barn_entry_base_template_retrofit.test.js tests/barn_entry_cloudflare_env_contract.test.js`

  Expected: all tests PASS.

### Task 6: Build, Render, and Publish

**Files:**
- Verify: `webflow-cloud-test/dist/`
- Verify: deployed `https://ringstatus.com/test/barn-entry`

**Interfaces:**
- Consumes: verified Barn Entry source and existing Cloudflare/Webflow Cloud deployment configuration.
- Produces: deployed route evidence and an embed decision.

- [ ] **Step 1: Build the Astro project**

  Run: `npm run build` in `webflow-cloud-test`.

  Expected: exit code 0 with no build errors.

- [ ] **Step 2: Render desktop and mobile locally**

  Verify unchanged locked shell geometry, identical four columns, control visibility, edit background, row cycles, two-character class/horse suggestions, Add, submit failure preservation, and print preview Ring grouping.

- [ ] **Step 3: Publish only through the configured deployment command**

  Use the repository's approved Webflow Cloud/Cloudflare publish path; do not substitute a manual endpoint call as deployment proof.

- [ ] **Step 4: Verify the deployed route and embed contract**

  Load the public route with a cache-busting query. If the route URL remains `/test/barn-entry`, report that the current embed remains valid; send a replacement embed only if the route or loader contract changed.

