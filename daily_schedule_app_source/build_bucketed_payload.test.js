const assert = require("node:assert/strict");
const test = require("node:test");

const {
  bucketRecord,
  buildBucketedPayload,
} = require("./build_bucketed_payload");

test("bucketRecord separates identity, display, timing, state, evidence, and operational fields", () => {
  const bucketed = bucketRecord({
    id: "trip_go:k1",
    trips_key: "k1",
    trip_instance_key: "ki1",
    rider_name: "Example Rider",
    estimated_go_time: "09:20:00",
    rs_current: { rs_go_time: "09:21:00" },
    latest_trip_log_record_id: "log1",
    manual_time_override: true,
    unknown_field: "kept",
  });

  assert.deepEqual(bucketed.identity, {
    id: "trip_go:k1",
    trips_key: "k1",
    trip_instance_key: "ki1",
  });
  assert.deepEqual(bucketed.display, { rider_name: "Example Rider" });
  assert.deepEqual(bucketed.timing, { estimated_go_time: "09:20:00" });
  assert.deepEqual(bucketed.state, { rs_current: { rs_go_time: "09:21:00" } });
  assert.deepEqual(bucketed.evidence, { latest_trip_log_record_id: "log1" });
  assert.deepEqual(bucketed.operational, { manual_time_override: true });
  assert.deepEqual(bucketed.data, { unknown_field: "kept" });
});

test("buildBucketedPayload preserves lane order and record counts", () => {
  const payload = {
    meta: { generated_at: "2026-05-12T12:00:00.000Z" },
    lane_order: ["classes", "trips"],
    lanes: {
      classes: [{ id: "class:1", class_name: "Hunter", schedule_key: "s1" }],
      trips: [{ id: "trip:1", rider_name: "Rider", trips_key: "t1" }],
    },
    side_lanes: {
      logs: {
        trip_logs: [{ id: "log:1", calc_status: "changed" }],
      },
    },
  };

  const bucketed = buildBucketedPayload(payload);

  assert.deepEqual(bucketed.lane_order, ["classes", "trips"]);
  assert.equal(bucketed.lanes.classes.length, 1);
  assert.equal(bucketed.lanes.trips.length, 1);
  assert.equal(bucketed.side_lanes.logs.trip_logs.length, 1);
  assert.equal(bucketed.shape, "bucketed_flat_lanes");
});
