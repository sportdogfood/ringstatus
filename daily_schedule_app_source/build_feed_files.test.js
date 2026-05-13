const assert = require("node:assert/strict");
const test = require("node:test");

const {
  buildIndexedFeed,
  buildStatusFeed,
  buildRawFeed,
} = require("./build_feed_files");

const sourcePayload = {
  meta: {
    generated_at: "2026-05-12T20:25:11.456Z",
    row_counts: { watch_schedule: 2, watch_trips: 1 },
  },
  lane_order: ["heartbeat", "show", "rings", "groups", "class_start", "classes", "entries", "trip_go", "trips", "horses", "riders"],
  lanes: {
    heartbeat: [{ time: "08:45 AM", app_sql_date: "2026-05-10" }],
    show: [{ sid: 200000061, sql_date: "2026-05-10" }],
    rings: [{ ring_number: 6, ringName: "Hunter 2" }],
    groups: [
      {
        schedule_record_id: "sched1",
        schedule_key: "200000061|2026-05-10|6|411|2",
        schedule_instance_key: "200000061|2026-05-10|6|411|2|cgid:200023861",
        ring_number: 6,
        group_name: "Small Pony Hunter",
        group_name_tags: "pony, hunter, Over Fences",
        completed_trips: 1,
        total_trips: 15,
        status: "Underway",
      },
      {
        schedule_record_id: "sched2",
        schedule_key: "200000061|2026-05-10|6|570|1",
        schedule_instance_key: "200000061|2026-05-10|6|570|1|cgid:200023862",
        ring_number: 6,
        group_name: "ASPCA Maclay",
        group_name_tags: "equitation, medal",
        completed_trips: 0,
        total_trips: 8,
        status: "Upcoming",
      },
    ],
    class_start: [
      {
        schedule_record_id: "sched1",
        schedule_key: "200000061|2026-05-10|6|411|2",
        schedule_instance_key: "200000061|2026-05-10|6|411|2|cgid:200023861",
        estimated_start_time: "08:40:00",
        latest_schedule_log_record_id: "slog1",
        latest_schedule_log: {
          rs_start_time: "08:40:00",
          rs_end_time: "09:25:00",
          rs_mins_till_start: -5,
          rs_status: "Underway",
          rs_completed_trips: 1,
          rs_total_trips: 15,
        },
      },
      {
        schedule_record_id: "sched2",
        schedule_key: "200000061|2026-05-10|6|570|1",
        schedule_instance_key: "200000061|2026-05-10|6|570|1|cgid:200023862",
        estimated_start_time: "10:00:00",
      },
    ],
    classes: [
      {
        schedule_record_id: "sched1",
        schedule_key: "200000061|2026-05-10|6|411|2",
        schedule_instance_key: "200000061|2026-05-10|6|411|2|cgid:200023861",
        class_number: 411,
        class_sequence: 2,
        class_name: "Small Pony Hunter",
        class_type: "Hunters",
      },
      {
        schedule_record_id: "sched2",
        schedule_key: "200000061|2026-05-10|6|570|1",
        schedule_instance_key: "200000061|2026-05-10|6|570|1|cgid:200023862",
        class_number: 570,
        class_sequence: 1,
        class_name: "ASPCA Maclay",
        class_type: "Equitation",
      },
    ],
    entries: [{
      trips_key: "200000061|2026-05-10|6|411|2|8778|2815",
      trip_instance_key: "200000061|2026-05-10|6|411|2|8778|2815|entry_sequence:5",
      schedule_key: "200000061|2026-05-10|6|411|2",
      pid: 8778,
      entry_number: 2815,
      entry_sequence: 5,
    }],
    trip_go: [{
      trip_record_id: "trip1",
      schedule_record_id: "sched1",
      trips_key: "200000061|2026-05-10|6|411|2|8778|2815",
      trip_instance_key: "200000061|2026-05-10|6|411|2|8778|2815|entry_sequence:5",
      schedule_key: "200000061|2026-05-10|6|411|2",
      pid: 8778,
      entry_number: 2815,
      estimated_go_time: "08:55:00",
      rs_current: {
        rs_go_time: "08:55:00",
        rs_min_till_go: 10,
        rs_order_of_go: 4,
      },
      rs_latest_log: {
        rs_go_time: "08:55:00",
        rs_min_till_go: 10,
        rs_order_of_go: 4,
      },
    }],
    trips: [{
      trip_record_id: "trip1",
      schedule_record_id: "sched1",
      trips_key: "200000061|2026-05-10|6|411|2|8778|2815",
      trip_instance_key: "200000061|2026-05-10|6|411|2|8778|2815|entry_sequence:5",
      schedule_key: "200000061|2026-05-10|6|411|2",
      pid: 8778,
      entry_number: 2815,
    }],
    horses: [{ trips_key: "200000061|2026-05-10|6|411|2|8778|2815", horse: "FORT KNOX", horse_name: "Knox" }],
    riders: [{ trips_key: "200000061|2026-05-10|6|411|2|8778|2815", pid: 8778, rider_name: "JESSICA HEAP" }],
  },
  side_lanes: { results: [], alerts: [], logs: { schedule_logs: [], trip_logs: [] } },
};

test("buildRawFeed keeps the source payload lanes unchanged", () => {
  const raw = buildRawFeed(sourcePayload, { generatedAt: "2026-05-13T12:00:00.000Z" });

  assert.equal(raw.lanes, sourcePayload.lanes);
  assert.equal(raw.side_lanes, sourcePayload.side_lanes);
  assert.equal(raw.meta.feed_generated_at, "2026-05-13T12:00:00.000Z");
});

test("buildIndexedFeed emits rows with requested indexes", () => {
  const indexed = buildIndexedFeed(sourcePayload, { generatedAt: "2026-05-13T12:00:00.000Z" });

  assert.equal(indexed.rows.length, 2);
  assert.deepEqual(Object.keys(indexed.indexed), ["rider", "horse", "status", "ring", "class_type", "group_name_tags"]);
  assert.deepEqual(indexed.indexed.rider["JESSICA HEAP"], ["200000061|2026-05-10|6|411|2|cgid:200023861"]);
  assert.deepEqual(indexed.indexed.horse.Knox, ["200000061|2026-05-10|6|411|2|cgid:200023861"]);
  assert.deepEqual(indexed.indexed.status.Underway, ["200000061|2026-05-10|6|411|2|cgid:200023861"]);
});

test("buildStatusFeed uses calculator outputs as sourced derived values without time conversion", () => {
  const status = buildStatusFeed(sourcePayload, { generatedAt: "2026-05-13T12:00:00.000Z" });
  const ring = status.by_ring["6"];
  const rider = status.by_rider["JESSICA HEAP"];

  assert.equal(status.time_policy.convert_show_clock_strings, false);
  assert.equal(ring.now.class_name, "Small Pony Hunter");
  assert.equal(ring.now.start.time, "08:40:00");
  assert.equal(ring.now.start.source, "schedule_logs.latest_schedule_log.rs_start_time");
  assert.equal(ring.now.starts_in_mins.value, -5);
  assert.equal(ring.now.starts_in_mins.source, "schedule_logs.latest_schedule_log.rs_mins_till_start");
  assert.equal(ring.next.class_name, "ASPCA Maclay");
  assert.equal(rider.now.trip.go.time, "08:55:00");
  assert.equal(rider.now.trip.go.source, "watch_trips.rs_current.rs_go_time");
  assert.equal(rider.now.trip.go_starts_in_mins.value, 10);
  assert.equal(rider.now.trip.go_starts_in_mins.source, "watch_trips.rs_current.rs_min_till_go");
  assert.equal(rider.now.previous_class, null);
});

test("buildStatusFeed does not treat duplicate trips in one class as previous classes", () => {
  const payload = JSON.parse(JSON.stringify(sourcePayload));
  payload.lanes.trip_go.push({
    ...payload.lanes.trip_go[0],
    trip_record_id: "trip2",
    trips_key: "200000061|2026-05-10|6|411|2|8778|2818",
    trip_instance_key: "200000061|2026-05-10|6|411|2|8778|2818|entry_sequence:6",
    entry_number: 2818,
  });
  payload.lanes.entries.push({
    ...payload.lanes.entries[0],
    trips_key: "200000061|2026-05-10|6|411|2|8778|2818",
    trip_instance_key: "200000061|2026-05-10|6|411|2|8778|2818|entry_sequence:6",
    entry_number: 2818,
    entry_sequence: 6,
  });
  payload.lanes.horses.push({
    trips_key: "200000061|2026-05-10|6|411|2|8778|2818",
    horse: "POPTART",
    horse_name: "Poptart",
  });

  const status = buildStatusFeed(payload, { generatedAt: "2026-05-13T12:00:00.000Z" });

  assert.equal(status.by_rider["JESSICA HEAP"].now.previous_class, null);
  assert.equal(status.by_horse.Poptart.now.previous_class, null);
});

test("buildStatusFeed does not choose completed rows as active just because counts conflict", () => {
  const payload = JSON.parse(JSON.stringify(sourcePayload));
  payload.lanes.groups[0].status = "Completed";
  payload.lanes.groups[0].completed_trips = 1;
  payload.lanes.groups[0].total_trips = 15;
  payload.lanes.class_start[0].latest_schedule_log.rs_status = "Completed";

  const status = buildStatusFeed(payload, { generatedAt: "2026-05-13T12:00:00.000Z" });

  assert.equal(status.by_ring["6"].now.class_name, "ASPCA Maclay");
  assert.equal(status.by_ring["6"].now.previous_class.class_name, "Small Pony Hunter");
});
