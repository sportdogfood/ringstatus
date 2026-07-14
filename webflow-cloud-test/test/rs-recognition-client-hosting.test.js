import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import test from "node:test";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const clientPath = resolve(__dirname, "../src/assets/rs-recognition/client.js");
const routePath = resolve(__dirname, "../src/pages/rs-recognition/client.js.ts");
const loaderPath = resolve(__dirname, "../../webflow/rs-recognition/rs-recognition-loader.html");
const previewPath = resolve(__dirname, "../../webflow/rs-recognition/rs-recognition-test.html");

test("Webflow Cloud serves the single recognition client asset", () => {
  assert.equal(existsSync(clientPath), true, "recognition client asset must exist");
  assert.equal(existsSync(routePath), true, "recognition client route must exist");

  const source = readFileSync(clientPath, "utf8");
  const routeSource = readFileSync(routePath, "utf8");

  assert.match(routeSource, /client\.js\?raw/);
  assert.match(routeSource, /text\/javascript; charset=utf-8/);
  assert.match(routeSource, /cache-control/);
  assert.match(source, /document\.body\.appendChild\(root\)/);
  assert.match(source, /rs-recognition\/device/);
  assert.match(source, /rs-recognition\/session/);
  assert.match(source, /rs-recognition\/action/);
});

test("Webflow pages use one short versioned loader", () => {
  assert.equal(existsSync(loaderPath), true, "page loader must exist");
  const loader = readFileSync(loaderPath, "utf8").trim();

  assert.equal(loader, '<script src="https://ringstatus.webflow.io/test/rs-recognition/client.js?v=1" defer></script>');
  assert.ok(Buffer.byteLength(loader) < 200);
});

test("local preview loads the same client instead of duplicating the component", () => {
  const preview = readFileSync(previewPath, "utf8");

  assert.match(preview, /src="\.\.\/\.\.\/webflow-cloud-test\/src\/assets\/rs-recognition\/client\.js"/);
  assert.doesNotMatch(preview, /id="rs-recognition-test"/);
  assert.ok(Buffer.byteLength(preview) < 1000);
});
