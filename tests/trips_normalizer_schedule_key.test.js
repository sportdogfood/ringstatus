const assert = require("assert");

const {
  buildScheduleMap,
} = require("../trips_normalizer_v2");

const rows = [
  {
    id: "rec_non_target",
    fields: {
      class_number: 650,
      class_group_id: 1,
      ring_number: 6,
      class_name: "Warm Up Hunter",
      is_target: false,
    },
  },
  {
    id: "rec_target",
    fields: {
      class_number: 650,
      class_group_id: 2,
      ring_number: 5,
      class_name: "Target Warm Up Hunter",
      is_target: true,
    },
  },
];

const scheduleByClassId = buildScheduleMap(rows);
const byClassNumber = scheduleByClassId.byClassNumber;

assert.ok(byClassNumber instanceof Map, "buildScheduleMap should expose class_number fallback map");
assert.strictEqual(
  byClassNumber.get("650").recordId,
  "rec_target",
  "class_number fallback should prefer is_target rows when duplicates exist"
);

console.log("trips_normalizer_schedule_key tests passed");
