const assert = require("assert");
const {
  buildCurrentFields,
  resolveHeartbeatScopeFromCurrentHeartbeat,
  showHeartbeatTargetDate,
  scopeForScheduleDate,
} = require("../schedules_dailyv2");

const scope = resolveHeartbeatScopeFromCurrentHeartbeat({
  heartbeat_record_id: "recHeartbeat",
  heartbeat_rid: "recHeartbeat",
  hb_at: "2026-05-29T21:00:00.000Z",
  app_show_idv2: 200000063,
  scope_run_id: "hb-1",
  heartbeat_time: "05:00 PM",
  heartbeat_show_date: "2026-05-29",
  raw_sql_date: "2026-05-29",
  mode: "NIGHT",
  current_app_sql_date: "2026-05-29",
  current_app_dow_raw: "Fri",
  current_shifted_to_next_day: true,
  current_set_to_default_app_sql_date: false,
  current_default_app_sql_date_is: "2026-05-29",
  current_show_app_sql_start_date: "2026-05-29",
  current_show_app_sql_end_date: "2026-05-31",
  current_show_app_name: "2026 ESP June I",
  current_app_sql_date_source: "show_focus_day",
  customer_id: 15,
  focus_day: "2026-05-29",
  ring_collection: null,
  show_scope_key: null,
  show_record_id: "recrFptx3113Vv0gd",
}, "test");

assert.strictEqual(scope.app_show_idv2, 200000063);
assert.strictEqual(scope.mode, "NIGHT");
assert.strictEqual(scope.shifted_to_next_dayv2, true);
assert.strictEqual(scope.app_sql_datev2, "2026-05-29");
assert.strictEqual(scope.app_sql_date_source, "show_focus_day");

const oneDayAfterFive = showHeartbeatTargetDate({
  focus_day: "2026-05-28",
  start_date: "2026-05-28",
  end_date: "2026-05-28",
  shifted_to_next_day: true,
}, new Date("2026-05-28T21:30:00.000Z"));
assert.strictEqual(oneDayAfterFive.target_date, "2026-05-28");
assert.strictEqual(oneDayAfterFive.proposed_target_date, "2026-05-28");
assert.strictEqual(oneDayAfterFive.reason, null);

const threeDayAfterFive = showHeartbeatTargetDate({
  focus_day: "2026-05-29",
  start_date: "2026-05-29",
  end_date: "2026-05-31",
  shifted_to_next_day: true,
}, new Date("2026-05-29T21:30:00.000Z"));
assert.strictEqual(threeDayAfterFive.target_date, "2026-05-29");

const threeDayDayWindow = showHeartbeatTargetDate({
  focus_day: "2026-05-29",
  start_date: "2026-05-29",
  end_date: "2026-05-31",
  shifted_to_next_day: true,
}, new Date("2026-05-30T14:30:00.000Z"));
assert.strictEqual(threeDayDayWindow.target_date, "2026-05-29");

const forwardScope = scopeForScheduleDate(scope, "2026-05-31");
const forwardFields = buildCurrentFields(
  {
    fields: {
      show_id: 200000063,
      schedule_show_datev2: "2026-05-31",
      scheduled_date: "2026-05-31",
      show_date: "2026-05-31",
      ring_number: 1,
      class_number: 540,
      class_group_id: 200024552,
      group_name: "M&S USEF Pony Medal",
    },
  },
  forwardScope,
  null,
  "recShow",
  "2026-05-29T21:30:00.000Z",
  "2026-05-29",
  "prefetch_new",
  null,
  new Set(["is_current_scope", "heartbeat", "dropped_at", "inactive", "archive", "scheduled_date", "schedule_date"]),
  { isCurrentScope: false }
);
assert.strictEqual(forwardFields.is_current_scope, false);
assert.deepStrictEqual(forwardFields.heartbeat, []);
assert.strictEqual(forwardFields.dropped_at, null);
assert.strictEqual(forwardFields.inactive, false);
assert.strictEqual(forwardFields.archive, false);
assert.strictEqual(forwardFields.scheduled_date, "2026-05-31");

console.log("schedules_daily_night_shift_scope tests passed");
