const assert = require("assert");

const {
  buildTripKeyParts,
  tripRowKeyFromFields,
} = require("../trips_dailyv2");

assert.strictEqual(
  tripRowKeyFromFields({
    trips_key: "200000061|2026-05-10|3|723|8778|2807",
    class_number: 723,
    entry_number: 2807,
  }),
  "200000061|2026-05-10|3|723|8778|2807",
  "watch_trips matching should prefer writable trips_key"
);

assert.deepStrictEqual(
  buildTripKeyParts({
    sid: 200000061,
    sqlDate: "2026-05-10",
    ringNumber: 3,
    classNumber: 723,
    pid: 8778,
    entryNumber: 2807,
    time: "08:30:00",
    cgid: 200023690,
  }),
  {
    scheduleKey: "200000061|2026-05-10|3|723",
    scheduleShort: "3|723",
    tripsKey: "200000061|2026-05-10|3|723|8778|2807",
    tripsShortKey: "723|8778|2807",
    fullNestingKey: "200000061|2026-05-10|3|08:30:00|200023690|723|8778|2807",
  },
  "base trip keys must not include an invented class_group_sequence"
);

assert.deepStrictEqual(
  buildTripKeyParts({
    sid: 200000061,
    sqlDate: "2026-05-10",
    ringNumber: 3,
    classNumber: 723,
    tieBreaker: 4,
    pid: 8778,
    entryNumber: 2807,
    time: "08:30:00",
    cgid: 200023690,
  }),
  {
    scheduleKey: "200000061|2026-05-10|3|723|4",
    scheduleShort: "3|723|4",
    tripsKey: "200000061|2026-05-10|3|723|8778|2807|4",
    tripsShortKey: "723|8778|2807|4",
    fullNestingKey: "200000061|2026-05-10|3|08:30:00|200023690|723|8778|2807|4",
  },
  "real class_group_sequence tie-breaker belongs at the end of trip keys"
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
