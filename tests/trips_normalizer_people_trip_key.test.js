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
        schedule_starttime: "09:09:00",
        _: "Mjk2NjUzMjV2ariIyyCQZPDv7zbOun9FgTDI3QkcOx/cVS9pqXo5GQ0lNngLH21XXxq0SFQyQf31lxLEK+tt9or4yVi6+MyVXZ61a1n9ISsBo434LDaiy8AAV/Cz5Rqb4wkI8t1908kwnimmbY3v3oKpcv+ys2k1du22eE+SsjVU1HRBrkS+mzmmmEnl/CwvegmFf4fh31p15umFfpD5JuTEFKwr/cQ6tn51TQ==",
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
assert.strictEqual(
  row.schedule_starttime,
  "09:09:00",
  "people/PIP schedule_starttime must carry through to watch_trips rows"
);
assert.strictEqual(row.sgl_token_prefix, "29665325v");
assert.strictEqual(row.sgl_token_length, row.sgl_token_raw.length);
assert.ok(
  result.unique_rows_by_key.has("8778|people:715:3160"),
  "dedupe should use tenant + people trip key when class_number + entry_number exist"
);
assert.ok(
  !result.unique_rows_by_key.has("fallback:8778:3160:715:markanto_a"),
  "dedupe should not create fallback entryxclasses_uuid keys"
);

const choppyResult = normalizeTripsForScope({
  sourceIds: [8778],
  peoplePayloads: new Map([[
    8778,
    {
      data: {
        rows: [
          {
            entry_number: 2298,
            class_number: 770,
            horse: "INSIDER BH",
            rider_name: "VICTORIA ROTSAERT",
          },
        ],
      },
    },
  ]]),
  scheduleByClassId: buildScheduleMap([
    {
      id: "recScheduleChoppy",
      fields: {
        class_number: 770,
        class_group_id: 200023690,
        class_groupxclasses_id: 200035393,
        class_name: "1.30m Open Jumper II2.1",
        ring_number: 1,
        schedule_show_datev2: "2026-05-08",
      },
    },
  ]),
});

assert.strictEqual(choppyResult.row_count, 1);
assert.strictEqual(choppyResult.normalized_rows[0].trip_key, "people:770:2298");
assert.strictEqual(choppyResult.normalized_rows[0].entry_id, null);
assert.strictEqual(choppyResult.normalized_rows[0].entry_number, 2298);
assert.strictEqual(choppyResult.normalized_rows[0].class_number, 770);
assert.strictEqual(choppyResult.normalized_rows[0].horse, "INSIDER BH");
assert.strictEqual(choppyResult.normalized_rows[0].rider_name, "VICTORIA ROTSAERT");
assert.strictEqual(choppyResult.normalized_rows[0].class_group_id, 200023690);
assert.strictEqual(choppyResult.normalized_rows[0].ring_number, 1);

const unmatchedScheduleResult = normalizeTripsForScope({
  sourceIds: [19676],
  peoplePayloads: new Map([[
    19676,
    {
      trips: [
        {
          class_id: null,
          class_number: 550,
          class_name: "Dover Saddlery/USEF Hunter Seat Medal",
          class_type: "Equitation",
          entry_number: 2,
          horse: "JASON",
          rider_name: "ZOEY BURTON",
          ring: 5,
          ring_id: 48,
          ring_name: "GDF Hunter Ring 1",
          scheduled_date: "2026-05-31T00:00:00.000Z",
          schedule_starttime: "08:36:00",
        },
      ],
    },
  ]]),
  scheduleByClassId: buildScheduleMap([]),
});

assert.strictEqual(
  unmatchedScheduleResult.row_count,
  1,
  "people trips must still create watch_trips rows when no matching watch_schedule row exists"
);
assert.strictEqual(unmatchedScheduleResult.outside_schedule.length, 1);
assert.strictEqual(unmatchedScheduleResult.unique_row_count, 1);
assert.ok(unmatchedScheduleResult.unique_rows_by_key.has("19676|people:550:2"));
assert.strictEqual(unmatchedScheduleResult.normalized_rows[0].trip_key, "people:550:2");
assert.strictEqual(unmatchedScheduleResult.normalized_rows[0].schedule_show_datev2, "2026-05-31");
assert.strictEqual(unmatchedScheduleResult.normalized_rows[0].ring_number, 5);
assert.strictEqual(unmatchedScheduleResult.normalized_rows[0].class_number, 550);
assert.strictEqual(unmatchedScheduleResult.normalized_rows[0].class_name, "Dover Saddlery/USEF Hunter Seat Medal");
assert.strictEqual(unmatchedScheduleResult.normalized_rows[0].estimated_start_time, "08:36:00");
assert.strictEqual(unmatchedScheduleResult.normalized_rows[0].watch_schedule_record_id, undefined);

const invalidDateResult = normalizeTripsForScope({
  sourceIds: [19676],
  peoplePayloads: new Map([[
    19676,
    {
      trips: [
        {
          class_number: 999,
          entry_number: 1,
          horse: "BAD DATE",
          rider_name: "DATE GUARD",
          scheduled_date: "0000-00-00",
        },
      ],
    },
  ]]),
  scheduleByClassId: buildScheduleMap([]),
});

assert.strictEqual(
  invalidDateResult.normalized_rows[0].scheduled_date,
  undefined,
  "0000-00-00 must not be treated as a writable Airtable date"
);

console.log("trips_normalizer_people_trip_key tests passed");
