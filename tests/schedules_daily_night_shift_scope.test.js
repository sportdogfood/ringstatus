const assert = require("assert");
const {
  resolveHeartbeatScopeFromCurrentHeartbeat,
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
assert.strictEqual(scope.app_sql_datev2, "2026-05-30");
assert.strictEqual(scope.app_sql_date_source, "night_shifted_next_day");

console.log("schedules_daily_night_shift_scope tests passed");
