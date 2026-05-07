const assert = require("assert");
const fs = require("fs");
const path = require("path");

const scripts = ["trips_tagger.js", "trips_dailyv2.js"];
const protectedFields = [
  "status",
  "estimated_start_time",
  "estimated_end_time",
  "estimated_go_time",
  "order_of_go",
  "total_trips",
  "completed_trips",
  "actual_time",
  "estimated_time",
  "gone_in",
  "h_eid",
  "entry_id",
  "entry_number",
  "time_one",
  "time_two",
  "time_three",
  "score",
  "score1",
  "score2",
  "score3",
  "placing",
];

for (const script of scripts) {
  const source = fs.readFileSync(path.resolve(__dirname, "..", script), "utf8");

  assert.ok(
    source.includes("PROTECTED_WATCH_TRIPS_FIELDS"),
    `${script} must define protected watch_trips fields`
  );

  assert.ok(
    /function\s+sanitizeWatchTripsPatchUpdates\s*\(/.test(source),
    `${script} must sanitize watch_trips patch updates`
  );

  assert.ok(
    /function\s+isBlankPatchValue\s*\(/.test(source),
    `${script} must detect blank patch values`
  );

  for (const field of protectedFields) {
    assert.ok(
      source.includes(field) || source.includes(field.toUpperCase()),
      `${script} must protect ${field}`
    );
  }

  assert.ok(
    /delete\s+fields\s*\[\s*fieldName\s*\]/.test(source),
    `${script} must remove blank protected fields before patching`
  );
}

console.log("watch_trips_protected_patch_guard tests passed");
