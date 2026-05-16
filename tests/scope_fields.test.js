const assert = require("assert");
const fs = require("fs");
const path = require("path");

const {
  buildAirtableFieldMeta,
  buildScopeFieldPatch,
} = require("../lib/scope_fields");

const formulaMeta = buildAirtableFieldMeta([
  { name: "customer_id", type: "formula", isComputed: true },
  { name: "focus_day", type: "date" },
  { name: "ring_collection", type: "singleLineText" },
  { name: "show_scope_key", type: "singleLineText" },
  { name: "show", type: "multipleRecordLinks" },
]);

assert.deepStrictEqual(
  buildScopeFieldPatch(formulaMeta, {
    customerId: 10002,
    focusDay: "2026-05-16",
    ringCollection: "10002|sfhja_2026_spring",
    showScopeKey: "10002|200000006|2026-05-16",
    showRecordId: "recShow",
  }),
  {
    focus_day: "2026-05-16",
    ring_collection: "10002|sfhja_2026_spring",
    show_scope_key: "10002|200000006|2026-05-16",
    show: ["recShow"],
  },
  "scope patch must not attempt to write a formula customer_id field"
);

const writableMeta = buildAirtableFieldMeta([
  { name: "customer_id", type: "number" },
  { name: "focus_day", type: "date" },
  { name: "ring_collection", type: "singleLineText" },
  { name: "show_scope_key", type: "singleLineText" },
  { name: "show", type: "multipleRecordLinks" },
]);

assert.deepStrictEqual(
  buildScopeFieldPatch(writableMeta, {
    customer_id: 10002,
    focus_day: "2026-05-16",
    ring_collection: "10002|sfhja_2026_spring",
    show_scope_key: "10002|200000006|2026-05-16",
    show_record_id: "recShow",
  }),
  {
    customer_id: 10002,
    focus_day: "2026-05-16",
    ring_collection: "10002|sfhja_2026_spring",
    show_scope_key: "10002|200000006|2026-05-16",
    show: ["recShow"],
  },
  "scope patch must write customer_id once it is a writable field"
);

for (const fileName of ["tagger.js", "schedules_dailyv2.js", "trips_dailyv2.js"]) {
  const source = fs.readFileSync(path.resolve(__dirname, "..", fileName), "utf8");
  assert.ok(
    source.includes("buildScopeFieldPatch"),
    `${fileName} must write copied show scope fields through the shared writable-field helper`
  );
}

console.log("scope_fields tests passed");
