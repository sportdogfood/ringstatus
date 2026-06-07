const assert = require("assert");
const fs = require("fs");
const path = require("path");

const root = path.resolve(__dirname, "..");
const legacyTableName = ["groups", "live"].join("_");
const forbiddenPatterns = [
  legacyTableName,
  "TABLE_" + ["GROUPS", "LIVE"].join("_"),
];

const activeFiles = [
  "schedules_dailyv2.js",
  "schedules_calculatorv2.js",
  "trips_tagger.js",
  "monitor_watch_trips_health.js",
  "lib/liveclassv2_enrichment.js",
];

for (const fileName of activeFiles) {
  const source = fs.readFileSync(path.join(root, fileName), "utf8");
  for (const pattern of forbiddenPatterns) {
    assert.ok(
      !source.includes(pattern),
      `${fileName} must not reference deprecated live group table token ${pattern}`
    );
  }
}

console.log("live_groups_no_legacy_table_contract tests passed");
