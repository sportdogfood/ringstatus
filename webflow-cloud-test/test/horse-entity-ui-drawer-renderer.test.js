import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import test from "node:test";

const repoRoot = resolve(import.meta.dirname, "..", "..");
const js = readFileSync(resolve(repoRoot, "webflow", "horse-entity-ui", "horse-entity-ui.js"), "utf8");
const css = readFileSync(resolve(repoRoot, "webflow", "horse-entity-ui", "horse-entity-ui.css"), "utf8");

test("horse profile drawer renders screenshot-style tabs and overview sections", () => {
  for (const label of ["OVERVIEW", "PROFILE", "FEED", "CONTACTS", "PRINT"]) {
    assert.match(js, new RegExp(`"${label}"`));
  }

  for (const label of ["SHOW NAME", "BARN NAME", "NOTE", "APP STATUS", "WEC-SUMMER"]) {
    assert.match(js, new RegExp(label));
  }

  for (const className of ["rs-horse-profile-shell", "rs-profile-tabs", "rs-profile-card", "rs-segment-row", "rs-save-note"]) {
    assert.match(js + css, new RegExp(className));
  }

  assert.doesNotMatch(js, /set-item-state|packState|kitItemId|quantity_packed/);
});
