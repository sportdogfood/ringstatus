const assert = require("node:assert/strict");
const test = require("node:test");

const {
  buildSourcePayload,
  makeScheduleKey,
  makeTripsKey,
  makeFullNestingKey,
} = require("./extract_daily_schedule_source");

function rec(id, fields) {
  return { id, fields };
}

test("builds the required key strings from row-owned fields", () => {
  const parts = {
    sid: "200000061",
    sql_date: "2026-05-10",
    ring_number: "1",
    time: "09:05:00",
    cgid: "987",
    class_number: "101",
    class_sequence: "2",
    pid: "555",
    entry_number: "44",
  };

  assert.equal(makeScheduleKey(parts), "200000061|2026-05-10|1|101|2");
  assert.equal(makeTripsKey(parts), "200000061|2026-05-10|1|101|2|555|44");
  assert.equal(makeFullNestingKey(parts), "200000061|2026-05-10|1|09:05:00|987|101|2|555|44");
});

test("keeps watch_trips rs current values separate from latest trip_logs evidence", () => {
  const payload = buildSourcePayload({
    generatedAt: "2026-05-12T12:00:00.000Z",
    heartbeatRows: [
      rec("rechb", {
        show_id: 200000061,
        app_show_id: 200000061,
        app_sql_date: "2026-05-10",
        app_dow_raw: "Sun",
        mode: "DAY",
        time: "09:30:00",
      }),
    ],
    scheduleRows: [
      rec("recsched", {
        sid: 200000061,
        show_id: 200000061,
        app_show_idv2: 200000061,
        schedule_show_datev2: "2026-05-10",
        app_sql_datev2: "2026-05-10",
        ring_number: 1,
        class_group_id: 987,
        class_group_sequence: 2,
        class_number: 101,
        estimated_start_time: "09:05:00",
        class_name: "Low Adult Jumper",
        group_name: "Low Adult Jumper",
      }),
    ],
    tripRows: [
      rec("rectrip", {
        show_id: 200000061,
        app_show_idv2: 200000061,
        schedule_show_datev2: "2026-05-10",
        scheduled_date: "2026-05-10",
        ring_number: 1,
        class_group_id: 987,
        class_group_sequence: 2,
        class_number: 101,
        pid: 555,
        entry_number: 44,
        watch_schedule: ["recsched"],
        schedule_rid: "recsched",
        estimated_start_time: "09:05:00",
        estimated_go_time: "09:20:00",
        rs_go_time: "09:21:00",
        rs_min_till_go: 11,
        rs_order_of_go: 5,
        actual_order: 5,
        rider_name: "Example Rider",
        horse: "Example Horse",
      }),
    ],
    scheduleLogRows: [],
    tripLogRows: [
      rec("reclog", {
        watch_trips: ["rectrip"],
        watch_trip_record_id: "rectrip",
        created_at: "2026-05-12T11:59:00.000Z",
        rs_go_time: "09:21:00",
        rs_min_till_go: 11,
        rs_order_of_go: 5,
        rs_go_time_diff: "same",
        computed_outputs_json: "{\"rs_go_time\":\"09:21:00\"}",
      }),
    ],
  });

  const tripGo = payload.lanes.trip_go[0];
  const log = payload.side_lanes.logs.trip_logs[0];

  assert.equal(tripGo.rs_current.rs_go_time, "09:21:00");
  assert.equal(tripGo.rs_current.rs_order_of_go, 5);
  assert.equal(tripGo.rs_latest_log.rs_go_time, "09:21:00");
  assert.equal(tripGo.rs_diff.rs_go_time_diff, "same");
  assert.equal(log.source_table, "trip_logs");
});

test("reports trips that cannot resolve a schedule parent", () => {
  const payload = buildSourcePayload({
    generatedAt: "2026-05-12T12:00:00.000Z",
    heartbeatRows: [],
    scheduleRows: [],
    tripRows: [
      rec("orphan", {
        show_id: 200000061,
        schedule_show_datev2: "2026-05-10",
        ring_number: 1,
        class_number: 101,
        class_group_sequence: 2,
        pid: 555,
        entry_number: 44,
      }),
    ],
    scheduleLogRows: [],
    tripLogRows: [],
  });

  assert.equal(payload.reports.validation.unresolved_trip_parents.length, 1);
  assert.equal(payload.reports.validation.unresolved_trip_parents[0].reason, "missing_schedule_parent");
});

test("exposes pid and entry_sequence next to trip keys without removing pid from trips_key", () => {
  const payload = buildSourcePayload({
    generatedAt: "2026-05-12T12:00:00.000Z",
    heartbeatRows: [],
    scheduleRows: [
      rec("sched", {
        sid: 200000061,
        schedule_show_datev2: "2026-05-10",
        ring_number: 6,
        class_number: 411,
        class_group_sequence: 2,
      }),
    ],
    tripRows: [
      rec("trip", {
        show_id: 200000061,
        schedule_show_datev2: "2026-05-10",
        ring_number: 6,
        class_number: 411,
        class_group_sequence: 2,
        pid: 8778,
        entry_number: 2815,
        entry_sequence: 7,
        watch_schedule: ["sched"],
      }),
    ],
    scheduleLogRows: [],
    tripLogRows: [],
  });

  const tripGo = payload.lanes.trip_go[0];
  const trip = payload.lanes.trips[0];
  const entry = payload.lanes.entries[0];

  assert.equal(tripGo.trips_key, "200000061|2026-05-10|6|411|2|8778|2815");
  assert.equal(tripGo.pid, 8778);
  assert.equal(tripGo.entry_sequence, 7);
  assert.equal(tripGo.trip_tie_breaker, 7);
  assert.equal(trip.pid, 8778);
  assert.equal(entry.pid, 8778);
  assert.equal(entry.entry_sequence, 7);
});

test("uses entry_sequence as a duplicate schedule tie breaker without changing schedule_key", () => {
  const payload = buildSourcePayload({
    generatedAt: "2026-05-12T12:00:00.000Z",
    heartbeatRows: [],
    scheduleRows: [
      rec("schedA", {
        sid: 200000061,
        schedule_show_datev2: "2026-05-10",
        ring_number: 4,
        class_number: 535,
        class_group_sequence: 4,
        entry_sequence: 1,
        estimated_start_time: "09:00:00",
      }),
      rec("schedB", {
        sid: 200000061,
        schedule_show_datev2: "2026-05-10",
        ring_number: 4,
        class_number: 535,
        class_group_sequence: 4,
        entry_sequence: 2,
        estimated_start_time: "09:00:00",
      }),
    ],
    tripRows: [],
    scheduleLogRows: [],
    tripLogRows: [],
  });

  const key = "200000061|2026-05-10|4|535|4";
  const classStarts = payload.lanes.class_start.filter((row) => row.schedule_key === key);
  const duplicate = payload.reports.validation.duplicate_schedule_keys[0];

  assert.equal(classStarts.length, 2);
  assert.deepEqual(classStarts.map((row) => row.schedule_tie_breaker), [1, 2]);
  assert.equal(duplicate.key, key);
  assert.equal(duplicate.tie_breaker_field, "entry_sequence");
  assert.deepEqual(duplicate.tie_breakers, [1, 2]);
});
