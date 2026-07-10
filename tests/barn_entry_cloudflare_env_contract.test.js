const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");

const routePath = path.resolve(
  __dirname,
  "..",
  "webflow-cloud-test",
  "src",
  "pages",
  "barn-entry.js"
);

test("barn-entry submit reads Astro 6 Cloudflare bindings from cloudflare:workers", () => {
  const route = fs.readFileSync(routePath, "utf8");

  assert.match(route, /import\s*\{\s*env\s*\}\s*from\s*["']cloudflare:workers["']/);
  assert.match(route, /export const POST = async \(\{ request \}\) => \{/);
  assert.match(route, /const airtable = airtableConfig\(env\);/);
  assert.match(route, /error:\s*"barn_entry_submit_failed"/);
  assert.match(route, /const DEFAULT_BASE_ID = "app6XS1RvsPNRT6os";/);
  assert.match(route, /env\.AIRTABLE_WEC_BASE_ID/);
  assert.doesNotMatch(route, /locals\?*\.runtime|locals\?*\.runtime\?*\.env/);
});
