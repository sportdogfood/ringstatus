const assert = require("assert");

const {
  buildScheduleMachineKey,
  normalizeSchedulePayload,
} = require("../schedule_normalizer_v2");

const scope = {
  app_show_idv2: 200000061,
  app_sql_datev2: "2026-05-07",
  app_dow_rawv2: "Thu",
  shifted_to_next_dayv2: false,
  scope_run_id: "test",
};

assert.strictEqual(
  buildScheduleMachineKey({
    class_group_id: 200023690,
    class_number: 770,
    class_groupxclasses_id: 200035393,
  }),
  "200023690_770",
  "schedule machine key should prefer class_group_id + class_number"
);

const result = normalizeSchedulePayload({
  show: { show_id: 200000061 },
  show_date: "2026-05-07",
  classes: [
    {
      group_name: "1.30m Open Jumper II2.1 & 1.30m Young Jumper II2.1",
      class_number: 770,
      class_name: "1.30m Open Jumper II2.1",
      class_groupxclasses_id: 200035393,
      class_group_id: 200023690,
    },
  ],
}, {
  scope,
  source: "test",
  generatedAt: "2026-05-07T12:00:00.000Z",
  generatedDate: "2026-05-07",
});

assert.strictEqual(result.rows.length, 1, "sample schedule class should normalize to one row");
assert.strictEqual(
  result.rows[0].key,
  "200023690_770",
  "normalized rows should use the composite machine key even when class_id is null"
);

const resultWithoutClassGroupXClassesId = normalizeSchedulePayload({
  show: { show_id: 200000061 },
  show_date: "2026-05-07",
  classes: [
    {
      group_name: "1.30m Open Jumper II2.1",
      class_group_id: 200023690,
      class_number: 770,
      class_name: "1.30m Open Jumper II2.1",
    },
  ],
}, {
  scope,
  source: "test",
  generatedAt: "2026-05-07T12:00:00.000Z",
  generatedDate: "2026-05-07",
});

assert.strictEqual(
  resultWithoutClassGroupXClassesId.rows.length,
  1,
  "schedule classes with class_group_id + class_number must not require class_groupxclasses_id"
);
assert.strictEqual(
  resultWithoutClassGroupXClassesId.rows[0].key,
  "200023690_770",
  "missing class_groupxclasses_id must still use the locked class_group_id + class_number machine key"
);

console.log("schedule_normalizer_machine_key tests passed");
