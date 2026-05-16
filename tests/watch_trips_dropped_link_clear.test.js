const assert = require("assert");

const { buildDroppedFields } = require("../trips_dailyv2");

const allWritableFields = new Set([
  "heartbeat",
  "watch_schedule",
  "is_current_scope",
  "scope_status",
  "inactive",
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

assert.deepStrictEqual(
  droppedFields.watch_schedule,
  [],
  "dropped trips must clear watch_schedule links so schedule rollups do not count stale trips"
);

assert.deepStrictEqual(
  droppedFields.heartbeat,
  [],
  "dropped trips must clear heartbeat links"
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

console.log("watch_trips_dropped_link_clear tests passed");
