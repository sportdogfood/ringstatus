const assert = require("assert");

const {
  buildPeopleTripKey,
  buildScheduleMap,
  normalizeTripsForScope,
} = require("../trips_normalizer_v2");

assert.strictEqual(
  buildPeopleTripKey({ classNumber: 715, entryNumber: 3160 }),
  "people:715:3160",
  "people trip key should be class_number + entry_number"
);

const scheduleByClassId = buildScheduleMap([
  {
    id: "recSchedule",
    fields: {
      class_number: 715,
      class_group_id: 200023694,
      class_groupxclasses_id: 200035397,
      class_name: "1.10m Open Jumper II2d",
      is_target: true,
      schedule_show_datev2: "2026-05-07",
    },
  },
]);

const peoplePayloads = new Map([
  [8778, {
    trips: [
      {
        entryxclasses_uuid: null,
        entry_id: null,
        entry_number: 3160,
        class_id: null,
        class_number: 715,
        horse: "MARKANTO A",
        rider_name: "TANNER KOROTKIN",
      },
    ],
  }],
]);

const result = normalizeTripsForScope({
  sourceIds: [8778],
  peoplePayloads,
  scheduleByClassId,
});

assert.strictEqual(result.row_count, 1);
assert.strictEqual(result.unique_row_count, 1);
const row = result.normalized_rows[0];
assert.strictEqual(row.trip_key, "people:715:3160");
assert.strictEqual(row.entryxclasses_uuid, null);
assert.ok(
  result.unique_rows_by_key.has("people:715:3160"),
  "dedupe should use people trip key when class_number + entry_number exist"
);
assert.ok(
  !result.unique_rows_by_key.has("fallback:8778:3160:715:markanto_a"),
  "dedupe should not create fallback entryxclasses_uuid keys"
);

console.log("trips_normalizer_people_trip_key tests passed");
