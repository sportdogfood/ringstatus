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
