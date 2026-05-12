const assert = require("node:assert/strict");
const test = require("node:test");

const {
  buildDisplayIntent,
  classifyField,
} = require("./build_display_intent");

test("classifyField separates keys and evidence from display candidates", () => {
  assert.equal(classifyField("schedule_key"), "key");
  assert.equal(classifyField("latest_trip_log_record_id"), "evidence");
  assert.equal(classifyField("class_name"), "display_candidate");
  assert.equal(classifyField("estimated_start_time"), "timing_candidate");
  assert.equal(classifyField("rs_current"), "calculator_state");
});

test("buildDisplayIntent reports display and non-display fields by lane", () => {
  const payload = {
    lane_order: ["classes"],
    lanes: {
      classes: [
        {
          id: "class:1",
          schedule_key: "k",
          schedule_instance_key: "ki",
          class_name: "Hunter",
          class_number: 535,
          latest_schedule_log: {},
        },
      ],
    },
    side_lanes: {},
  };

  const report = buildDisplayIntent(payload);
  const lane = report.lanes.classes;

  assert.deepEqual(lane.display_candidates.sort(), ["class_name", "class_number"].sort());
  assert.deepEqual(lane.not_for_primary_display.key.sort(), ["id", "schedule_instance_key", "schedule_key"].sort());
  assert.deepEqual(lane.not_for_primary_display.evidence, ["latest_schedule_log"]);
});
