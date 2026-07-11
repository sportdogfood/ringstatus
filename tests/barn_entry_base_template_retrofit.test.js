const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const vm = require("node:vm");

const root = path.resolve(__dirname, "..", "webflow-cloud-test", "src", "assets");
const base = fs.readFileSync(path.join(root, "ag-base-shell", "source.html"), "utf8");
const barn = fs.readFileSync(path.join(root, "barn-entry", "source.html"), "utf8");

function styleBlock(html) {
  const match = html.match(/<style>([\s\S]*?)<\/style>/);
  assert.ok(match, "expected a style block");
  return match[0];
}

function inOrder(source, tokens) {
  let cursor = -1;
  for (const token of tokens) {
    const next = source.indexOf(token, cursor + 1);
    assert.ok(next > cursor, `expected ${token} after previous shell element`);
    cursor = next;
  }
}

test("barn-entry embeds the locked base stylesheet byte-for-byte", () => {
  assert.ok(barn.includes(styleBlock(base)));
  assert.equal((barn.match(/@media \(max-width: 479px\)/g) || []).length, 1);
  assert.doesNotMatch(barn, /max-width:\s*478px|innerWidth|matchMedia|addEventListener\(["']resize/);
});

test("barn-entry preserves base shell order and approved visibility mapping", () => {
  inOrder(barn, [
    'class="app-head"',
    'class="action-bar is-hidden"',
    'class="action-anchors is-hidden"',
    'class="grid-frame"',
    'class="action-bar-bottom"',
    'class="status-line"'
  ]);
  assert.match(barn, /id="miniEditBtn"[^>]*>EDIT<\/button>/);
  assert.match(barn, /id="miniPrintBtn"[^>]*>PRINT<\/button>/);
  assert.match(barn, /<button[^>]*(?:id="miniUnusedBtn"[^>]*class="[^"]*is-hidden|class="[^"]*is-hidden[^"]*"[^>]*id="miniUnusedBtn")/);
  assert.match(barn, /id="bottomEditBtn"[^>]*>EDIT<\/button>/);
  assert.match(barn, /id="bottomAddBtn"[^>]*>ADD<\/button>/);
  assert.match(barn, /id="bottomSendBtn"[^>]*>SEND<\/button>/);
  assert.match(barn, /id="rowCount"/);
  assert.match(barn, /id="statusText"/);
});

test("barn-entry uses one four-column sortable grid at every viewport", () => {
  const headers = [...barn.matchAll(/headerName:\s*["']([^"']+)["']/g)].map((match) => match[1]);
  assert.deepEqual(headers.slice(0, 4), ["TIME", "RING", "CLASS", "HORSE"]);
  assert.equal(headers.includes("Tap"), false);
  assert.match(barn, /defaultColDef:\s*\{[\s\S]*?sortable:\s*true/);
  assert.doesNotMatch(barn, /isCompactLayout|compactEntryRenderer|setGridOption\(["']columnDefs["']/);
});

test("row tap is edit-gated and uses only approved outline states", () => {
  assert.match(barn, /if\s*\(!state\.editMode\)\s*return/);
  assert.match(barn, /pending["']\s*,\s*["']confirmed["']\s*,\s*["']declined/);
  assert.match(barn, /\.row-confirmed[^}]*outline:\s*2px solid[^;]*(?:#0f7a3c|var\(--rs-ok\))/s);
  assert.match(barn, /\.row-confirmed[^}]*outline-offset:\s*-2px/s);
  assert.match(barn, /\.row-declined[^}]*outline:\s*2px solid #000/s);
  assert.match(barn, /\.row-declined[^}]*outline-offset:\s*-2px/s);
  assert.match(barn, /rowClassRules:\s*\{/);
  assert.doesNotMatch(barn, /getRowClass:/);
  assert.match(barn, /getRowNode\(key\)/);
  assert.match(barn, /redrawRows\(\{\s*rowNodes:/);
  assert.match(barn, /closest\(["']\.ag-row\[row-id\]["']\)/);
  assert.match(barn, /getAttribute\(["']row-id["']\)/);
});

test("horse helper supports top_matches and canonical barn_name display", () => {
  assert.match(barn, /top_matches/);
  assert.match(barn, /barn_name/);
  assert.doesNotMatch(barn, /barn_name:\s*row\.(?:rider|trainer)|horseName\s*=.*(?:rider|trainer)/);
  assert.match(barn, /if\s*\([^)]*\.length\s*<\s*2\)/);
  assert.match(barn, /dedupeHorseCandidates\(\[\.\.\.state\.rows,\s*\.\.\.state\.helperHorseRows\]\)/);
});

test("mapped horse recovery uses source payload horse and rejects rider substitution", () => {
  assert.match(barn, /function sourceHorseName\(row\)/);
  assert.match(barn, /source_payload/);
  assert.match(barn, /parts\[2\]/);
  assert.match(barn, /normalize\(candidate\)\s*===\s*normalize\(row\.rider\)\)\s*return\s*["']["']/);
  assert.match(barn, /canonicalHorse\?\.barn_name\s*\|\|\s*sourceHorseName\(row\)/);
});

test("print builds a separate three-column sheet grouped by Ring", () => {
  assert.match(barn, /class="print-sheet"/);
  assert.match(barn, /class="print-columns"/);
  assert.match(barn, /columns:\s*3 250px/);
  assert.match(barn, /print-ring-group/);
  assert.match(barn, /groupRowsByRing|groupedByRing/);
  assert.match(barn, /window\.print\(\)/);
});

test("barn-entry inline application script parses", () => {
  const scripts = [...barn.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/g)]
    .map((match) => match[1])
    .filter((source) => source.trim());
  assert.equal(scripts.length, 1);
  assert.doesNotThrow(() => new vm.Script(scripts[0]));
});
