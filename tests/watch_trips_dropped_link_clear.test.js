const assert = require("assert");

const { buildDroppedFields, preserveExistingLinkFields } = require("../trips_dailyv2");

const allWritableFields = new Set([
  "heartbeat",
  "watch_schedule",
  "is_current_scope",
  "scope_status",
  "inactive",
  "archive",
  "dropped_at",
  "run_id",
  "run_time",
  "last_seen_at",
]);

const droppedFields = buildDroppedFields(
  { scope_run_id: "run-123" },
  "2026-05-15T12:00:00.000Z",
  "2026-05-15",
  "dropped",
  allWritableFields
);

assert.ok(
  !Object.prototype.hasOwnProperty.call(droppedFields, "watch_schedule"),
  "dropped trips must not clear watch_schedule links"
);

assert.ok(
  !Object.prototype.hasOwnProperty.call(droppedFields, "heartbeat"),
  "dropped trips must not clear heartbeat links"
);

assert.strictEqual(droppedFields.scope_status, "dropped");
assert.strictEqual(droppedFields.inactive, true);
assert.strictEqual(droppedFields.is_current_scope, false);

const noWatchScheduleField = buildDroppedFields(
  { scope_run_id: "run-123" },
  "2026-05-15T12:00:00.000Z",
  "2026-05-15",
  "dropped",
  new Set(["heartbeat"])
);

assert.ok(
  !Object.prototype.hasOwnProperty.call(noWatchScheduleField, "watch_schedule"),
  "drop patch must only write watch_schedule when the live table exposes that field"
);

assert.deepStrictEqual(
  preserveExistingLinkFields(droppedFields, {
    heartbeat: ["recHeartbeat"],
    watch_schedule: ["recSchedule"],
  }),
  {
    is_current_scope: false,
    scope_status: "dropped",
    inactive: true,
    archive: true,
    dropped_at: "2026-05-15",
    run_id: "run-123",
    run_time: "2026-05-15T12:00:00.000Z",
    last_seen_at: "2026-05-15",
    heartbeat: ["recHeartbeat"],
    watch_schedule: ["recSchedule"],
  },
  "drop patches must preserve existing relationship links when applying an update"
);

console.log("watch_trips_dropped_link_clear tests passed");
