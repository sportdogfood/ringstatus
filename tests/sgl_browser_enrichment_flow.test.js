const assert = require("assert");
const { countOrderSetStatuses, groupRecordsByUrl, isUnderSaddleRecord, selectScheduleScrapeRequest, selectTripsScrapeRequest } = require("../lib/sgl_browser_enrichment");

const rows = [
  { id: "old", fields: { show_id: 200000069, focus_day: "2026-09-11" } },
  { id: "ready", fields: { show_id: 200000069, focus_day: "2026-09-12", schedule_scrape_now: true } },
];

assert.strictEqual(selectScheduleScrapeRequest(rows, "2026-09-12")?.id, "ready");
assert.strictEqual(selectScheduleScrapeRequest(rows, "2026-09-11")?.id, "ready");

const tripsRows = [
  { id: "old-trips", fields: { focus_day: "2026-09-11", trips_scrape_oog_now: true } },
  { id: "ready-trips", fields: { focus_day: "2026-09-12", trips_scrape_oog_now: true } },
];
assert.strictEqual(selectTripsScrapeRequest(tripsRows, "2026-09-12")?.id, "ready-trips");
assert.strictEqual(isUnderSaddleRecord({ fields: { schedule_sequencetype: "Under Saddle/Flat" } }), true);
assert.strictEqual(isUnderSaddleRecord({ fields: { order_set: "Class is Under Saddle" } }), true);
assert.deepStrictEqual(countOrderSetStatuses([
  { fields: { order_set: "Order Not Yet Set" } },
  { fields: { order_set: "Order is Set" } },
  { fields: { order_set: "Class is Under Saddle" } },
  { fields: { order_set: "Err" } },
]), { order_not_yet_set: 1, order_is_set: 1, class_is_under_saddle: 1, err: 1, blank_or_other: 0 });

const records = [
  { id: "a", fields: { url: "https://example.test/class/1" } },
  { id: "b", fields: { url: "https://example.test/class/1" } },
  { id: "c", fields: { url: "https://example.test/class/2" } },
  { id: "missing", fields: {} },
];
const grouped = groupRecordsByUrl(records, (record) => record.fields.url);

assert.strictEqual(grouped.groups.size, 2);
assert.strictEqual(grouped.groups.get("https://example.test/class/1").length, 2);
assert.deepStrictEqual(grouped.missing.map((record) => record.id), ["missing"]);

console.log("sgl browser enrichment flow tests passed");
