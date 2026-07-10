const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");

const root = path.resolve(__dirname, "..");
const prototypeDir = path.join(root, "prototypes", "horseshowing");
const reports = [
  "barn-entry-ag-review-v2.html",
  "ring-classes.html"
];
const sharedCss = "ag-report-shared.css";
const sharedRuntime = "ag-report-runtime.js";
const approvedCss = "https://cdn.jsdelivr.net/gh/sportdogfood/ringstatus@df8d9d4eb0652f76c952ed718b36e774100148f4/webflow/packing-worksheet/styles.css";

function read(file) {
  return fs.readFileSync(path.join(prototypeDir, file), "utf8");
}

test("both reports reference one pinned AG Grid and one shared implementation", () => {
  for (const report of reports) {
    const html = read(report);
    assert.match(html, /ag-grid-community@36\.0\.0/);
    assert.match(html, new RegExp(`href=["']${sharedCss}["']`));
    assert.match(html, new RegExp(`src=["']${sharedRuntime}["']`));
    assert.match(html, new RegExp(`href=["']${approvedCss.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}["']`));
    assert.match(html, /family=Outfit/);
    assert.doesNotMatch(html, /<style\b/i);
    assert.doesNotMatch(html, /function\s+(?:init|updateRows|buildPrintSheet|renderRingAnchors)\s*\(/);
    assert.match(html, /window\.RS_AG_REPORT_CONFIG\s*=/);
    assert.match(html, /<main id="packing-app" class="rsa-dashboard"><\/main>/);
    assert.match(html, /location\.protocol\s*===\s*["']file:["']/);
  }
});

test("shared CSS and runtime own the required lifecycle and states", () => {
  const css = read(sharedCss);
  const runtime = read(sharedRuntime);

  assert.match(css, /\.ag-report-shell/);
  assert.match(runtime, /lp-filter-toggle/);
  assert.match(css, /@media print/);
  assert.match(css, /@media \(max-width: 720px\)/);
  assert.match(css, /@media \(max-width: 479px\)/);
  assert.match(runtime, /lp-section-block packing-theme-show lp-theme-classes ag-report-shell/);
  assert.match(runtime, /lp-section-title packing-section-title app-head/);
  assert.match(runtime, /packing-list-switcher action-bar/);
  assert.match(runtime, /packing-section-search filter-bar/);
  assert.match(runtime, /rss-class-related-data/);
  assert.match(runtime, /rss-class-line/);
  assert.match(runtime, /rss-entry-rollup/);

  assert.match(runtime, /function renderShell/);
  assert.match(runtime, /function applyFilters/);
  assert.match(runtime, /function updateRows/);
  assert.match(runtime, /function buildPrintSheet/);
  assert.match(runtime, /function openDrawer/);
  assert.match(runtime, /function openEntryDrawer/);
  assert.match(runtime, /function closeDrawer/);
  assert.match(runtime, /function rssRowShapeRenderer/);
  assert.match(runtime, /class="lp-modal"/);
  assert.match(runtime, /class="lp-modal-card"/);
  assert.match(runtime, /class="lp-profile-shell/);
  assert.match(runtime, /agGrid\.createGrid/);
  assert.match(runtime, /setGridOption\("rowData"/);
  assert.match(runtime, /data-state="loading"/);
  assert.match(runtime, /setStatus\([^\n]+"empty"\)/);
  assert.match(runtime, /setStatus\([^\n]+"error"\)/);
});

test("each RSS entry rollup owns an independent drawer trigger and inherits the approved font", () => {
  const css = read(sharedCss);
  const runtime = read(sharedRuntime);

  assert.match(css, /#packing-app \.ag-theme-quartz \.ag-root-wrapper\s*\{[^}]*--ag-font-family:\s*"Outfit", sans-serif/);
  assert.match(css, /--ag-cell-font-family:\s*var\(--ag-font-family\)/);
  assert.match(css, /--ag-header-font-family:\s*var\(--ag-font-family\)/);
  assert.match(css, /button\.rss-entry-rollup/);
  assert.match(css, /button\.rss-entry-rollup:hover/);
  assert.match(runtime, /<button type="button" class="rss-entry-rollup"/);
  assert.match(runtime, /data-rollup-index="\$\{index \+ 1\}"/);
  assert.match(runtime, /data-row-key=/);
  assert.match(runtime, /data-entry-key=/);
  assert.match(runtime, /function openEntryDrawer/);
  assert.match(runtime, /entryButton\.addEventListener\("click"/);
  assert.match(runtime, /classLine\.addEventListener\("click"/);
});

test("shared runtime implements the locked RSS stacked form and special row contract", () => {
  const css = read(sharedCss);
  const runtime = read(sharedRuntime);
  const barn = read("barn-entry-ag-review-v2.html");
  const ring = read("ring-classes.html");
  const source = runtime + barn + ring;

  for (const component of [
    "app-head",
    "header-left",
    "app-title",
    "app-meta",
    "header-right",
    "action-bar-mini",
    "action-anchors-top",
    "action-bar",
    "grid-frame"
  ]) {
    assert.match(runtime, new RegExp(`class=["'][^"']*${component}`));
  }

  for (const selector of [
    "rss-shape-cell",
    "rss-class-related-data",
    "rss-entry-line",
    "rss-entry-rollups",
    "rss-entry-rollup",
    "rss-class-line",
    "rss-time-cell",
    "rss-class-time",
    "rss-class-ring",
    "rss-class-name",
    "rss-class-entry",
    "rss-class-status",
    "rss-class-token",
    "rss-is-empty",
    "rss-is-hydrated",
    "rss-row-is-hydrated"
  ]) {
    assert.match(runtime + css, new RegExp(selector));
  }

  assert.match(runtime, /function rssRowShapeRenderer/);
  assert.match(runtime, /data-entry-key=/);
  assert.match(runtime, /event\.stopPropagation\(\)/);
  assert.match(runtime, /event\.key !== "Enter" && event\.key !== " "/);
  assert.match(runtime, /domLayout:\s*"autoHeight"/);
  assert.match(source, /autoHeight:\s*true/);
  assert.match(source, /flex:\s*1/);
  assert.match(runtime, /rowClassRules/);
  assert.match(css, /\.rss-class-line\s*\{[^}]*min-height:\s*41px/s);
  assert.match(css, /grid-template-columns:\s*0\.8fr\s+1fr\s+minmax\(0, 160px\)\s+0\.4fr\s+0\.6fr/s);
  assert.match(css, /\.rss-entry-line\.rss-is-empty\s*\{[^}]*display:\s*none/s);
  assert.match(css, /\.ag-row\.rss-row-is-hydrated\s*\{[^}]*background-color:\s*#f4f4f4/s);
  assert.match(barn, /shape:\s*"rss"/);

  assert.doesNotMatch(runtime, /class="(?:class-related-data|entry-line|class-line|rollup-item)"/);
  assert.doesNotMatch(css, /(^|[,{\s])\.(?:class-related-data|entry-line|class-line|rollup-item)(?:[\s:{.#]|$)/m);
  assert.doesNotMatch(runtime, /isFullWidthRow|fullWidthCellRenderer|getRowHeight/);
});

test("report configuration stays inside the permitted differences", () => {
  for (const report of reports) {
    const html = read(report);
    assert.match(html, /data:/);
    assert.match(html, /columns:/);
    assert.match(html, /actions:/);
    assert.match(html, /filters:/);
    assert.match(html, /row:/);
    assert.match(html, /print:/);
  }
});

test("Barn Entry and Ring Classes retain their required report-specific behavior", () => {
  const barn = read("barn-entry-ag-review-v2.html");
  const ring = read("ring-classes.html");

  assert.match(barn, /SUBMIT_URL/);
  assert.match(barn, /title:\s*"Add Entry"/);
  assert.match(barn, /id:\s*"toggle-status"/);
  assert.match(barn, /id:\s*"focus"/);
  assert.match(barn, /id:\s*"horse"/);
  assert.match(barn, /id:\s*"print"/);
  assert.match(barn, /horse_items|rollup_items/);

  assert.match(ring, /id:\s*"focus"/);
  assert.match(ring, /id:\s*"horse"/);
  assert.match(ring, /id:\s*"clear"/);
  assert.match(ring, /id:\s*"print"/);
  assert.match(ring, /flattenRollups/);
  assert.match(ring, /shape:\s*"rss"/);
  assert.match(ring, /entryDrawer:entryDrawerDetail/);
  assert.doesNotMatch(ring, /SUBMIT_URL|title:\s*"Add Entry"/);
});

test("locked lightweight WEC template uses the RSS special-grid contract", () => {
  const template = fs.readFileSync(path.join(root, "docs", "horseshowing", "ag-output-references", "wec_ag_styled_template_live.html"), "utf8");

  assert.match(template, /ag-grid-community@36\.0\.0/);
  assert.match(template, /headerName:\s*"TIME"[\s\S]*field:\s*"time"/);
  assert.match(template, /headerName:\s*"RING"[\s\S]*field:\s*"ring"/);
  assert.match(template, /headerName:\s*"CLASS"/);
  assert.match(template, /headerName:\s*"IN"[\s\S]*field:\s*"starts_in"/);
  assert.match(template, /headerName:\s*"STATUS"[\s\S]*field:\s*"status"/);
  assert.match(template, /headerName:\s*"TIME"[\s\S]*width:\s*88[\s\S]*maxWidth:\s*88[\s\S]*sortable:\s*false[\s\S]*filter:\s*false/);
  assert.match(template, /headerName:\s*"RING"[\s\S]*width:\s*88[\s\S]*maxWidth:\s*88[\s\S]*sortable:\s*false[\s\S]*filter:\s*false/);
  assert.match(template, /headerName:\s*"CLASS"[\s\S]*flex:\s*1[\s\S]*sortable:\s*true[\s\S]*filter:\s*"agTextColumnFilter"/);
  assert.match(template, /\.rss-class-name\s*\{[^}]*max-width:\s*160px/s);
  assert.match(template, /headerName:\s*"IN"[\s\S]*width:\s*88[\s\S]*maxWidth:\s*88[\s\S]*sortable:\s*false[\s\S]*filter:\s*false/);
  assert.match(template, /headerName:\s*"STATUS"[\s\S]*width:\s*88[\s\S]*maxWidth:\s*88[\s\S]*sortable:\s*false[\s\S]*filter:\s*false/);
  assert.match(template, /function rssRowShapeRenderer/);
  assert.match(template, /root\.className\s*=\s*"rss-class-related-data/);
  assert.match(template, /class="rss-entry-line/);
  assert.match(template, /class="rss-entry-rollups/);
  assert.match(template, /class="rss-entry-rollup/);
  assert.match(template, /class="rss-class-line/);
  assert.match(template, /data-entry-key=/);
  assert.match(template, /data-class-key=/);
  assert.match(template, /domLayout:\s*"autoHeight"/);
  assert.match(template, /isFullWidthRow/);
  assert.match(template, /fullWidthCellRenderer:\s*rssRowShapeRenderer/);
  assert.match(template, /getRowHeight/);
  assert.match(template, /rowClassRules/);
  assert.match(template, /event\.key !== "Enter" && event\.key !== " "/);
  assert.match(template, /grid-template-columns:\s*88px\s+88px\s+minmax\(0,\s*1fr\)\s+88px\s+88px/);
  assert.match(template, /function openFlyout/);
  assert.match(template, /function applyFilters/);
  assert.match(template, /function buildPrintSheet/);
  assert.match(template, /function togglePendingHidden/);

  assert.doesNotMatch(template, /rssEntryFullWidthRenderer|rssHeaderRenderer|RssHeaderRenderer|rss-column-labels|scheduleRollupRelocation|relocateRollups/);
  assert.doesNotMatch(template, /class="(?:class-related-data|class-related-rollup|class-line|rollup-item|ag-row-rollup-band)/);
});
