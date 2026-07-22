import assert from "node:assert/strict";
import test from "node:test";
import { homeBackgroundFields, normalizeHex, normalizePageKey, parseBase64Image, validateChanges } from "./rs-edit.js";
import { HOME_BINDINGS } from "./rs-edit-home-bindings.js";

test("normalizes page keys and colors", () => {
  assert.equal(normalizePageKey("Home"), "home");
  assert.equal(normalizePageKey("../bad"), "");
  assert.equal(normalizeHex("#aabbcc"), "#AABBCC");
  assert.equal(normalizeHex("red"), "");
});

test("accepts only allowlisted text changes", () => {
  const allowlist = new Map([["home:01:a", { fieldKey: "home:01:a", pageKey: "home", fieldType: "text", editable: true }]]);
  const result = validateChanges({ pageKey: "home", changes: [{ fieldKey: "home:01:a", fieldType: "text", textContent: "Approved text" }] }, allowlist);
  assert.equal(result.ok, true);
  assert.equal(result.changes[0].textContent, "Approved text");
  assert.equal(validateChanges({ pageKey: "home", changes: [{ fieldKey: "missing", fieldType: "text", textContent: "No" }] }, allowlist).error, "field_not_allowed");
});

test("validates bounded image payloads", () => {
  const image = parseBase64Image({ filename: "test.png", contentType: "image/png", base64: "aGVsbG8=" });
  assert.equal(image.ok, true);
  assert.equal(parseBase64Image({ filename: "test.svg", contentType: "image/svg+xml", base64: "aGVsbG8=" }).error, "unsupported_image_type");
});

test("keeps the resolved Home map at 90 strings on 87 unique parents", () => {
  assert.equal(HOME_BINDINGS.length, 90);
  assert.equal(new Set(HOME_BINDINGS.map((binding) => binding.fieldKey)).size, 90);
  const backgrounds = homeBackgroundFields(HOME_BINDINGS);
  assert.equal(backgrounds.length, 87);
  assert.equal(new Set(backgrounds.map((field) => field.fieldKey)).size, 87);
  assert.ok(backgrounds.every((field) => field.pageKey === "home" && field.fieldType === "color" && field.editable));
});
