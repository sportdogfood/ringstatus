const assert = require("assert");

const {
  tripRowKeyFromFields,
} = require("../trips_dailyv2");

assert.strictEqual(
  tripRowKeyFromFields({
    trips_key: "200000061|2026-05-10|3|723|1|8778|2807",
    class_number: 723,
    entry_number: 2807,
  }),
  "200000061|2026-05-10|3|723|1|8778|2807",
  "watch_trips matching should prefer writable trips_key"
);

assert.strictEqual(
  tripRowKeyFromFields({
    entryxclasses_uuid: "REAL-UUID",
    class_number: 715,
    entry_number: 3160,
  }),
  "people:715:3160",
  "watch_trips matching should prefer class_number + entry_number"
);

assert.strictEqual(
  tripRowKeyFromFields({
    entryxclasses_uuid: "REAL-UUID",
  }),
  "REAL-UUID",
  "entryxclasses_uuid should remain the backup key when people pair is missing"
);

assert.strictEqual(
  tripRowKeyFromFields({
    entryxclasses_uuid: "fallback:8778:3160:715:markanto_a",
    class_number: 715,
    entry_number: 3160,
  }),
  "people:715:3160",
  "legacy fallback rows should match the new people trip key when the pair exists"
);

console.log("trips_daily_trip_key tests passed");
