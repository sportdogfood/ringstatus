# WEC Mobile-Pro Local Prototype Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a locally served WEC mobile-pro AG Grid prototype from the handed special-row template with live read-only schedule/list/detail data and the approved controls.

**Architecture:** Keep the handed CSS as an immutable byte-for-byte asset. Use one HTML shell and one focused browser runtime: the runtime fetches `wec-schedule-ui`, normalizes only confirmed fields, renders flat overview rows through the handed full-width RSS renderer, and owns screen state, print state, switchers, and drawers.

**Tech Stack:** HTML5, handed CSS plus pinned RingStatus CSS, vanilla JavaScript, AG Grid Community 36.0.0, Node.js built-in test runner, browser `fetch`, `localStorage`, and print CSS already present in the handed sources.

## Global Constraints

- Copy `rswp-special-rows-grid-template.css` with SHA-256 `768A8516FA141030F9E4DE8EA494FF90900E5DE80ED65901BF453D19E3903521` byte-for-byte.
- Do not reformat, reorder, rename, minify, merge, or append the handed CSS.
- Preserve AG Grid Community `36.0.0`, Quartz, Outfit, and the pinned shared CSS commit `df8d9d4eb0652f76c952ed718b36e774100148f4`.
- Create only the new prototype folder, its contract test, and this plan; do not modify existing prototype files.
- Use read-only `wec-schedule-ui` views; do not run workflows, mutate endpoints, repair data, publish, or deploy.
- PRINT ignores FOCUS and FILTER but excludes saved HIDE rows.
- Missing data renders null/blank or explicit empty/error state; never manufacture source truth.

---

### Task 1: Add the drift-guard contract and immutable shell

**Files:**
- Create: `tests/wec_mobile_pro_contract.test.js`
- Create: `prototypes/horseshowing/wec-mobile-pro/index.html`
- Create: `prototypes/horseshowing/wec-mobile-pro/rswp-special-rows-grid-template.css`

**Interfaces:**
- Consumes: the handed ZIP CSS and dependency URLs.
- Produces: `#packing-app`, immutable stylesheet, and the script hook consumed by Task 2.

- [ ] **Step 1: Write the failing structure and CSS test**

Create a Node test that reads the three expected files, hashes CSS with `crypto.createHash("sha256")`, and asserts the exact hash, pinned dependency URLs, `#packing-app`, `wec-mobile-pro.js`, and absence of inline `<style>`.

```js
test("mobile-pro preserves the handed CSS and three-file shell", () => {
  assert.equal(sha256(read("rswp-special-rows-grid-template.css")), LOCKED_CSS_SHA);
  const html = read("index.html");
  assert.match(html, /ag-grid-community@36\.0\.0/);
  assert.match(html, /df8d9d4eb0652f76c952ed718b36e774100148f4/);
  assert.match(html, /<main id="packing-app" class="rsa-dashboard"><\/main>/);
  assert.match(html, /src="wec-mobile-pro\.js"/);
  assert.doesNotMatch(html, /<style\b/i);
});
```

- [ ] **Step 2: Run the test and verify RED**

Run: `node --test tests/wec_mobile_pro_contract.test.js`

Expected: FAIL because the prototype files do not exist.

- [ ] **Step 3: Create the minimal shell and exact CSS copy**

Use `apply_patch` for `index.html`. Copy the ZIP entry without transformation and verify its hash before continuing.

```html
<main id="packing-app" class="rsa-dashboard"></main>
<script src="https://cdn.jsdelivr.net/npm/ag-grid-community@36.0.0/dist/ag-grid-community.min.js"></script>
<script src="wec-mobile-pro.js"></script>
```

- [ ] **Step 4: Run the test and verify the shell assertions pass**

Run: `node --test tests/wec_mobile_pro_contract.test.js`

Expected: CSS/shell test PASS; runtime tests added in later tasks may still fail.

- [ ] **Step 5: Commit Task 1**

```powershell
git add -- tests/wec_mobile_pro_contract.test.js prototypes/horseshowing/wec-mobile-pro/index.html prototypes/horseshowing/wec-mobile-pro/rswp-special-rows-grid-template.css
git commit -m "test: lock wec mobile-pro template shell"
```

### Task 2: Implement live overview and the special-row AG contract

**Files:**
- Modify: `tests/wec_mobile_pro_contract.test.js`
- Create: `prototypes/horseshowing/wec-mobile-pro/wec-mobile-pro.js`

**Interfaces:**
- Consumes: `{ ok, show, counts, resources, rows }` from `view=overview`.
- Produces: `normalizeOverviewRow(row)`, `classLabelGetter(params)`, `minuteFormatter(params)`, `rssRowShapeRenderer(params)`, `refreshGridRows()`, and one AG Grid instance.

- [ ] **Step 1: Add failing runtime contract assertions**

Assert the runtime contains the live base/action/view, no sample fallback, stable `getRowId`, getters/formatters, full-width configuration, conditional height, class rules, and Grid API updates.

```js
assert.match(js, /action=wec-schedule-ui/);
assert.match(js, /view=overview/);
assert.match(js, /getRowId:\s*params\s*=>\s*params\.data\.rowKey/);
assert.match(js, /valueGetter:\s*classLabelGetter/);
assert.match(js, /valueFormatter:\s*minuteFormatter/);
assert.match(js, /isFullWidthRow/);
assert.match(js, /fullWidthCellRenderer:\s*rssRowShapeRenderer/);
assert.match(js, /getRowHeight/);
assert.match(js, /rowClassRules/);
assert.match(js, /setGridOption\("rowData"/);
assert.doesNotMatch(js, /SAMPLE_ROWS|sample\|ring/i);
```

- [ ] **Step 2: Run the test and verify RED**

Run: `node --test tests/wec_mobile_pro_contract.test.js`

Expected: FAIL because `wec-mobile-pro.js` is missing.

- [ ] **Step 3: Implement the minimal overview runtime**

Start from the handed JavaScript renderer grammar. Build the shell, fetch overview once, normalize confirmed camelCase fields without fallback joins, and create the grid.

```js
const API_BASE = "https://horseshowing-700800454.development.catalystserverless.com/server/horseshowing_sync/";
const endpoint = view => `${API_BASE}?action=wec-schedule-ui&view=${encodeURIComponent(view)}`;
const classLabelGetter = params => [params.data?.classNumber, params.data?.className].filter(Boolean).join(" - ");
const minuteFormatter = params => params.value == null || params.value === "" ? "--" : `${params.value}m`;
```

Use `entryRollups[].entries[]` to build independent rollup buttons with persisted `entryKey` and `entryDayKey`. Preserve `rss-class-related-data`, `rss-entry-line`, `rss-entry-rollups`, `rss-entry-rollup`, `rss-class-line`, and all existing class-cell selectors.

- [ ] **Step 4: Run the test and verify GREEN**

Run: `node --test tests/wec_mobile_pro_contract.test.js`

Expected: all current tests PASS.

- [ ] **Step 5: Commit Task 2**

```powershell
git add -- tests/wec_mobile_pro_contract.test.js prototypes/horseshowing/wec-mobile-pro/wec-mobile-pro.js
git commit -m "feat: render mobile-pro live special rows"
```

### Task 3: Add approved controls, lists, details, and print boundary

**Files:**
- Modify: `tests/wec_mobile_pro_contract.test.js`
- Modify: `prototypes/horseshowing/wec-mobile-pro/wec-mobile-pro.js`

**Interfaces:**
- Consumes: overview rows plus class, entry, ring, results, alerts list/detail responses.
- Produces: `setView(view)`, `loadResourceList(view)`, `openDetail(type, key)`, `applyScreenFilters()`, `savedPrintRows()`, `buildPrintSheet()`, and `scrollToRing(ringKey)`.

- [ ] **Step 1: Add failing behavior contract assertions**

Assert all five list/detail names and identity parameters, loading/empty/error states, AbortController/request token protection, local-storage HIDE, FOCUS/FILTER screen-state separation, three-column print, and anchor API calls.

```js
for (const marker of ["class_list", "entry_list", "ring_list", "results_list", "alerts_list", "class_detail", "entry_detail", "ring_detail", "result_detail", "alert_detail"]) {
  assert.match(js, new RegExp(marker));
}
for (const key of ["rowKey", "entryDayKey", "ringKey", "resultKey", "triggerKey"]) {
  assert.match(js, new RegExp(key));
}
assert.match(js, /localStorage/);
assert.match(js, /columns:\s*3\s+250px/);
assert.match(js, /getRowNode/);
assert.match(js, /ensureNodeVisible/);
assert.match(js, /AbortController|detailRequestToken/);
```

- [ ] **Step 2: Run the test and verify RED**

Run: `node --test tests/wec_mobile_pro_contract.test.js`

Expected: FAIL on the first missing control/list/detail marker.

- [ ] **Step 3: Implement the approved behavior layer**

Keep state boundaries explicit:

```js
const screenRows = () => applyFocus(applySelectedFilters(nonHiddenOverviewRows()));
const savedPrintRows = () => state.overviewRows.filter(row => !state.hiddenRowKeys.has(row.rowKey));
```

Render switchers using the handed button/class grammar. Load list views on selection; load detail by the documented key into the handed drawer. HIDE saves only persisted `rowKey` values. Ring anchors call `getRowNode` and `ensureNodeVisible(node, "top")`.

- [ ] **Step 4: Run the test and verify GREEN**

Run: `node --test tests/wec_mobile_pro_contract.test.js`

Expected: all contract tests PASS.

- [ ] **Step 5: Commit Task 3**

```powershell
git add -- tests/wec_mobile_pro_contract.test.js prototypes/horseshowing/wec-mobile-pro/wec-mobile-pro.js
git commit -m "feat: add mobile-pro controls and drawers"
```

### Task 4: Verify the local prototype without publishing

**Files:**
- Modify only if a failing test identifies a defect: the new prototype/test files.

**Interfaces:**
- Consumes: completed local files and read-only live endpoint.
- Produces: fresh static-test, local-server, browser, and CSS-hash evidence.

- [ ] **Step 1: Run the complete contract suite**

Run: `node --test tests/wec_mobile_pro_contract.test.js tests/ag_shared_scalability_contract.test.js`

Expected: zero failures.

- [ ] **Step 2: Verify CSS hash independently**

```powershell
Get-FileHash -Algorithm SHA256 prototypes/horseshowing/wec-mobile-pro/rswp-special-rows-grid-template.css
```

Expected: `768A8516FA141030F9E4DE8EA494FF90900E5DE80ED65901BF453D19E3903521`.

- [ ] **Step 3: Serve the repository locally**

Run a non-publishing local static server and open:

```text
http://127.0.0.1:<port>/prototypes/horseshowing/wec-mobile-pro/
```

- [ ] **Step 4: Verify browser behavior**

Confirm live overview special rows, hydrated/empty heights, independent entry/class drawers, five switchers, available detail views, FOCUS/FILTER screen-only behavior, HIDE persistence and PRINT exclusion, dynamic anchors, explicit empty results, and no console errors.

- [ ] **Step 5: Verify worktree scope**

Run: `git status --short` and `git diff --stat HEAD~3..HEAD`.

Expected: only the approved design/plan, new prototype folder, and new test are part of this implementation; pre-existing dirty files remain unmodified and unstaged.
