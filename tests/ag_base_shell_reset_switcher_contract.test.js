const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const assert = require("node:assert/strict");

const sourcePath = path.join(
  __dirname,
  "..",
  "prototypes",
  "horseshowing",
  "ag-base-shell-reset.html"
);
const source = fs.readFileSync(sourcePath, "utf8");

test("reset shell shows the switcher and preserves hidden action structures", () => {
  assert.match(source, /\.is-hidden\s*\{\s*display:\s*none\s*!important;?\s*\}/);
  assert.match(source, /<section class="action-bar is-hidden"[^>]*>/);
  assert.match(source, /<nav class="action-switcher" aria-label="View switcher">/);
  assert.match(source, /<nav class="action-anchors is-hidden"[^>]*>/);

  const actionBar = source.indexOf('class="action-bar is-hidden"');
  const switcher = source.indexOf('class="action-switcher"');
  const anchors = source.indexOf('class="action-anchors is-hidden"');
  const grid = source.indexOf('class="grid-frame"');

  assert.ok(actionBar < switcher);
  assert.ok(switcher < anchors);
  assert.ok(anchors < grid);
});

test("switcher copies the anchor controls without wiring behavior", () => {
  const match = source.match(
    /<nav class="action-switcher"[^>]*>([\s\S]*?)<\/nav>/
  );

  assert.ok(match);
  assert.match(match[1], />Ring<\/button>/);
  assert.match(match[1], />Class<\/button>/);
  assert.match(match[1], />Entry<\/button>/);
  assert.match(match[1], />Results<\/button>/);
  assert.doesNotMatch(match[1], /\sid=/);
});

test("visible shell rows reserve the flexible row for AG Grid", () => {
  assert.match(
    source,
    /\.app-shell\s*\{[\s\S]*?grid-template-rows:\s*auto auto 1fr auto auto;/
  );
});
