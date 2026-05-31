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
      day_number: 1,
      class_number: 770,
      class_name: "1.30m Open Jumper II2.1",
      _: "Mjk2NjUzMjV2ariIyyCQZPDv7zbOun9FgTDI3QkcOx/cVS9pqXo5GQ0lNngLH21XXxq0SFQyQf31lxLEK+tt9or4yVi6+MyVXZ61a1n9ISsBo434LDaiy8AAV/Cz5Rqb4wkI8t1908kwnimmbY3v3oKpcv+ys2k1du22eE+SsjVU1HRBrkS+mzmmmEnl/CwvegmFf4fh31p15umFfpD5JuTEFKwr/cQ6tn51TQ==",
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
assert.strictEqual(
  resultWithoutClassGroupXClassesId.rows[0].fields.day_number,
  1,
  "schedule rows should capture source day_number when SGL provides it"
);
assert.strictEqual(
  resultWithoutClassGroupXClassesId.rows[0].fields.sgl_token_prefix,
  "29665325v",
  "schedule rows should monitor the source SGL token prefix without using it for keys"
);
assert.strictEqual(
  resultWithoutClassGroupXClassesId.rows[0].fields.sgl_token_length,
  resultWithoutClassGroupXClassesId.rows[0].fields.sgl_token_raw.length,
  "schedule token length should be the raw token character length"
);

const groupedTimeResult = normalizeSchedulePayload({
  show: { show_id: 200000063 },
  show_date: "2026-05-31",
  rings: [
    {
      ring_number: 1,
      classes: [
        {
          class_group_id: 200024660,
          group_name: "1.20m Open Jumper II2d",
          estimated_start_time: "08:00:00",
          start_time_default: "08:00:00",
          class_list: "712",
          classes: [
            {
              class_groupxclasses_id: 200036462,
              class_group_id: 200024660,
              class_number: 712,
              class_name: "1.20m Open Jumper II2d",
            },
          ],
        },
      ],
    },
  ],
}, {
  scope: {
    ...scope,
    app_show_idv2: 200000063,
    app_sql_datev2: "2026-05-31",
    app_dow_rawv2: "Sun",
  },
  source: "test",
  generatedAt: "2026-05-30T22:00:00.000Z",
  generatedDate: "2026-05-30",
});

assert.strictEqual(groupedTimeResult.rows.length, 1);
assert.strictEqual(
  groupedTimeResult.rows[0].fields.estimated_start_time,
  "08:00:00",
  "class rows must inherit group-level estimated_start_time from SGL schedule payloads"
);

console.log("schedule_normalizer_machine_key tests passed");
