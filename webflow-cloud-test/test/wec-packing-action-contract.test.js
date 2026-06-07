import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";

const __dirname = dirname(fileURLToPath(import.meta.url));
const source = readFileSync(resolve(__dirname, "../src/lib/wec-packing.js"), "utf8");
const helper = source.match(/function shouldSkipReturnedState\(action\) \{[\s\S]*?\n\}/)?.[0] || "";

test("quantity add action does not wait for a full state rebuild", () => {
  assert.match(helper, /"add_quantity"/);
});

test("comment actions still return refreshed state for merged comment data", () => {
  assert.doesNotMatch(helper, /"add_comment"/);
  assert.doesNotMatch(helper, /"update_comment"/);
});
