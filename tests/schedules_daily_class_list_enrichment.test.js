const assert = require("assert");

const {
  applyClassListIdEnrichment,
  buildClassIdByNumberFromClassesPayload,
  buildClassListEndpoint,
} = require("../schedules_dailyv2");

const catalog = buildClassIdByNumberFromClassesPayload({
  classes: [
    {
      class_id: 200025346,
      number: 282,
      name: "Adult Amateur Hunter",
      entry_count: 2,
    },
    {
      class_id: 200025699,
      number: 283,
      name: "Adult Amateur Hunter",
      entry_count: 2,
    },
  ],
});

assert.strictEqual(catalog.catalog_rows, 2);
assert.strictEqual(catalog.usable_rows, 2);
assert.strictEqual(catalog.byNumber.get("282").class_id, 200025346);

const overlay = applyClassListIdEnrichment([
  {
    key: "schedule-282",
    fields: {
      show_id: 200000062,
      schedule_show_datev2: "2026-05-28",
      ring_number: 3,
      class_number: 282,
      class_name: "Adult Amateur Hunter",
    },
  },
  {
    key: "schedule-283-existing",
    fields: {
      class_number: 283,
      class_id: 123,
    },
  },
], catalog.byNumber);

assert.strictEqual(overlay.enriched, 1);
assert.strictEqual(overlay.rows[0].fields.class_id, 200025346);
assert.strictEqual(overlay.rows[0].class_id_enrichment.source, "classes_list");
assert.strictEqual(
  overlay.rows[1].fields.class_id,
  123,
  "class list enrichment must not overwrite an existing class_id"
);

const conflictCatalog = buildClassIdByNumberFromClassesPayload([
  { class_id: 1, number: 999 },
  { class_id: 2, number: 999 },
]);
assert.strictEqual(conflictCatalog.byNumber.has("999"), false);
assert.strictEqual(conflictCatalog.conflicts.length, 1);

assert.strictEqual(
  buildClassListEndpoint(200000062, 15),
  "https://sglapi.wellingtoninternational.com/classes?show_id=200000062&customer_id=15"
);

console.log("schedules_daily_class_list_enrichment tests passed");
