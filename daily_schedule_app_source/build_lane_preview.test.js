const assert = require("node:assert/strict");
const test = require("node:test");

const {
  buildLanePreview,
} = require("./build_lane_preview");

test("buildLanePreview summarizes fields and samples per lane", () => {
  const payload = {
    lane_order: ["heartbeat", "trips"],
    lanes: {
      heartbeat: [{ id: "hb1", mode: "DAY", app_sql_date: "2026-05-10" }],
      trips: [
        { id: "t1", trips_key: "k1", status: "active", score: null },
        { id: "t2", trips_key: "k2", status: "", score: 88 },
      ],
    },
    side_lanes: {
      logs: {
        trip_logs: [{ id: "l1", calc_status: "changed" }],
      },
    },
  };

  const preview = buildLanePreview(payload, { sampleSize: 1 });

  assert.equal(preview.lanes.heartbeat.record_count, 1);
  assert.equal(preview.lanes.trips.record_count, 2);
  assert.deepEqual(preview.lanes.trips.fields.status, { populated: 1, blank: 1 });
  assert.deepEqual(preview.lanes.trips.fields.score, { populated: 1, blank: 1 });
  assert.equal(preview.lanes.trips.samples.length, 1);
  assert.equal(preview.side_lanes.logs.trip_logs.record_count, 1);
});

test("buildLanePreview does not invent render decisions", () => {
  const preview = buildLanePreview({
    lane_order: ["classes"],
    lanes: {
      classes: [{ id: "c1", class_name: "Hunter" }],
    },
    side_lanes: {},
  });

  assert.equal(preview.render_decisions_made, false);
  assert.deepEqual(preview.render_decisions, []);
});
