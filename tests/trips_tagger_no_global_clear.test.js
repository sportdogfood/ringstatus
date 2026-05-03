const assert = require("assert");
const fs = require("fs");
const path = require("path");

const source = fs.readFileSync(path.resolve(__dirname, "..", "trips_tagger.js"), "utf8");

assert.ok(
  !/function\s+clearTripLevelFields\s*\(/.test(source),
  "trips_tagger.js must not have a global field clearing helper"
);

assert.ok(
  !/clearTripLevelFields\s*\(/.test(source),
  "trips_tagger.js must not call global field clearing"
);

assert.ok(
  !/updateFields\s*\[\s*FIELD_[^\]]+\]\s*=\s*null\s*;/.test(source),
  "trips_tagger.js must not write null into watch_trips fields"
);

assert.ok(
  !/updateFields\s*\[\s*FIELD_[^\]]+\]\s*=\s*\[\]\s*;/.test(source),
  "trips_tagger.js must not clear linked fields with empty arrays"
);

console.log("trips_tagger_no_global_clear tests passed");
