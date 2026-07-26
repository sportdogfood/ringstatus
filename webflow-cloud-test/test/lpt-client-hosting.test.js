import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import test from "node:test";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const clientPath = resolve(__dirname, "../src/assets/lpt/client.js");
const cssPath = resolve(__dirname, "../src/assets/lpt/client.css");
const jsRoutePath = resolve(__dirname, "../src/pages/lpt/client.js.ts");
const cssRoutePath = resolve(__dirname, "../src/pages/lpt/client.css.ts");
const loaderPath = resolve(__dirname, "../../webflow/lp-history/lpt-native-loader.html");

test("LPT client assets are served by stable Webflow Cloud routes", () => {
  for (const path of [clientPath, cssPath, jsRoutePath, cssRoutePath, loaderPath]) {
    assert.equal(existsSync(path), true, `${path} must exist`);
  }

  assert.match(readFileSync(jsRoutePath, "utf8"), /text\/javascript/);
  assert.match(readFileSync(cssRoutePath, "utf8"), /text\/css/);
  assert.match(readFileSync(loaderPath, "utf8"), /\/test\/lpt\/client\.css/);
  assert.match(readFileSync(loaderPath, "utf8"), /\/test\/lpt\/client\.js/);
});

test("LPT controller is scoped and implements the approved interaction contract", () => {
  const source = readFileSync(clientPath, "utf8");
  const css = readFileSync(cssPath, "utf8");

  assert.match(source, /querySelector\("\.lpt-shell"\)/);
  assert.match(source, /data-lpt-lane-prev/);
  assert.match(source, /data-lpt-lane-next/);
  assert.match(source, /lptCardType/);
  assert.match(source, /lptRecordId/);
  assert.match(source, /is-detail/);
  assert.match(source, /is-quick/);
  assert.match(source, /youtube-nocookie\.com/);
  assert.match(source, /lpt:filters-changed/);
  assert.match(css, /\.lpt-drawer\.is-open/);
  assert.match(css, /translate3d\(0, 104%, 0\)/);
  assert.match(css, /position: fixed/);
  assert.match(css, /overflow-x: auto/);
});

test("LPT loader remains short and does not embed application markup", () => {
  const loader = readFileSync(loaderPath, "utf8").trim();
  assert.ok(Buffer.byteLength(loader) < 260);
  assert.doesNotMatch(loader, /<div/i);
});
