const assert = require("assert");

const { WATCH_TRIPS_HEARTBEAT_FIELDS } = require("../trips_dailyv2");

for (const fieldName of [
  "trips_key",
  "show_id",
  "show_date",
  "app_show_idv2",
  "app_sql_datev2",
  "schedule_show_datev2",
  "scheduled_date",
  "ring_number",
  "class_number",
  "pid",
  "entry_number",
  "watch_schedule",
  "heartbeat",
  "is_current_scope",
]) {
  assert.ok(
    WATCH_TRIPS_HEARTBEAT_FIELDS.includes(fieldName),
    `watch_trips heartbeat fetch must include ${fieldName} so stale trips can be dropped`
  );
}

console.log("watch_trips_heartbeat_fields tests passed");
