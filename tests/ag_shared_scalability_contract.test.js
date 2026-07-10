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

function read(file) {
  return fs.readFileSync(path.join(prototypeDir, file), "utf8");
}

test("both reports reference one pinned AG Grid and one shared implementation", () => {
  for (const report of reports) {
    const html = read(report);
    assert.match(html, /ag-grid-community@36\.0\.0/);
    assert.match(html, new RegExp(`href=["']${sharedCss}["']`));
    assert.match(html, new RegExp(`src=["']${sharedRuntime}["']`));
    assert.doesNotMatch(html, /<style\b/i);
    assert.doesNotMatch(html, /function\s+(?:init|updateRows|buildPrintSheet|renderRingAnchors)\s*\(/);
    assert.match(html, /window\.RS_AG_REPORT_CONFIG\s*=/);
    assert.match(html, /<div id="agReportRoot"><\/div>/);
  }
});

test("shared CSS and runtime own the required lifecycle and states", () => {
  const css = read(sharedCss);
  const runtime = read(sharedRuntime);

  assert.match(css, /\.app-shell/);
  assert.match(css, /\.rs-button/);
  assert.match(css, /@media print/);
  assert.match(css, /@media \(max-width:/);

  assert.match(runtime, /function renderShell/);
  assert.match(runtime, /function applyFilters/);
  assert.match(runtime, /function updateRows/);
  assert.match(runtime, /function buildPrintSheet/);
  assert.match(runtime, /agGrid\.createGrid/);
  assert.match(runtime, /setGridOption\("rowData"/);
  assert.match(runtime, /data-state="loading"/);
  assert.match(runtime, /setStatus\([^\n]+"empty"\)/);
  assert.match(runtime, /setStatus\([^\n]+"error"\)/);
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
  assert.match(barn, /data-status-key/);
  assert.match(barn, /id:\s*"focus"/);
  assert.match(barn, /id:\s*"horse"/);
  assert.match(barn, /id:\s*"print"/);

  assert.match(ring, /id:\s*"focus"/);
  assert.match(ring, /id:\s*"horse"/);
  assert.match(ring, /id:\s*"clear"/);
  assert.match(ring, /id:\s*"print"/);
  assert.doesNotMatch(ring, /SUBMIT_URL|title:\s*"Add Entry"/);
});
