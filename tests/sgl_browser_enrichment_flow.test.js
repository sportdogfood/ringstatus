const assert = require("assert");
const { groupRecordsByUrl, selectScheduleScrapeRequest } = require("../lib/sgl_browser_enrichment");

const rows = [
  { id: "old", fields: { show_id: 200000069, focus_day: "2026-09-11" } },
  { id: "ready", fields: { show_id: 200000069, focus_day: "2026-09-12", schedule_scrape_now: true } },
];

assert.strictEqual(selectScheduleScrapeRequest(rows, "2026-09-12")?.id, "ready");
assert.strictEqual(selectScheduleScrapeRequest(rows, "2026-09-11")?.id, "ready");

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
