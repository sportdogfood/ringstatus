const assert = require("assert");

const {
  applyGroupsLiveFallback,
  buildCurrentFields,
  scheduleRowKeyFromFields,
} = require("../schedules_dailyv2");

assert.strictEqual(
  scheduleRowKeyFromFields({
    schedule_key: "200000061|2026-05-07|5|770|1",
    class_group_id: 200023690,
    class_number: 770,
  }),
  "200000061|2026-05-07|5|770|1",
  "watch_schedule matching should prefer writable schedule_key"
);

assert.strictEqual(
  scheduleRowKeyFromFields({
    class_group_id: 200023690,
    class_number: 770,
    class_groupxclasses_id: 200035393,
  }),
  "200023690_770",
  "watch_schedule matching should prefer class_group_id + class_number"
);

const rows = [
  {
    key: "200023690_770",
    fields: {
      class_group_id: 200023690,
      class_number: 770,
      class_groupxclasses_id: 200035393,
    },
  },
];

const groupsById = new Map([
  ["200023690", {
    recordId: "recGroupLive",
    class_group_id: 200023690,
    day: "2026-05-07",
    ring_number: 5,
    estimated_start_time: "08:30:00",
    status: "In Progress",
    total: 42,
    gone: 7,
    class_ids: ["200025008", "200025009"],
    class_numbers: ["770", "771"],
    ingested_at: "2026-05-07T12:00:00.000Z",
  }],
]);

const overlay = applyGroupsLiveFallback(rows, groupsById);
assert.strictEqual(overlay.matched, 1, "groups_live should match by class_group_id");

const fieldMeta = {
  writableNames: new Set([
    "groups_live",
    "ring_number",
    "show_date",
    "sql_date",
    "schedule_show_datev2",
    "estimated_start_time",
    "latest_estimated_start_time",
    "___latest_estimated_start_time",
    "status",
    "latest_status",
    "total_trips",
    "completed_trips",
    "latest_ingested_at",
    "schedule_date",
    "scheduled_date",
    "class_id",
    "inactive",
    "schedule_key",
    "schedule_short",
  ]),
  writableByTrim: new Map(),
};

const fields = buildCurrentFields(
  overlay.rows[0],
  { app_show_idv2: 200000061, app_sql_datev2: "2026-05-07", scope_run_id: "test" },
  "recHeartbeat",
  null,
  "2026-05-07T12:00:00.000Z",
  "2026-05-07",
  "existing",
  "current",
  fieldMeta
);

assert.deepStrictEqual(fields.groups_live, ["recGroupLive"]);
assert.strictEqual(fields.ring_number, 5);
assert.strictEqual(fields.show_date, "2026-05-07");
assert.strictEqual(fields.sql_date, "2026-05-07");
assert.strictEqual(fields.schedule_show_datev2, "2026-05-07");
assert.strictEqual(fields.schedule_date, "2026-05-07");
assert.strictEqual(fields.scheduled_date, "2026-05-07");
assert.strictEqual(fields.estimated_start_time, "08:30:00");
assert.strictEqual(fields.latest_estimated_start_time, "08:30:00");
assert.strictEqual(fields.___latest_estimated_start_time, "08:30:00");
assert.strictEqual(fields.status, "In Progress");
assert.strictEqual(fields.latest_status, "In Progress");
assert.strictEqual(fields.total_trips, 42);
assert.strictEqual(fields.completed_trips, 7);
assert.strictEqual(fields.class_id, 200025008);
assert.strictEqual(fields.schedule_key, "200000061|2026-05-07|5|770");
assert.strictEqual(fields.schedule_short, "5|770");
assert.ok(
  !Object.prototype.hasOwnProperty.call(fields, "class_sequence"),
  "schedule writer must not invent class_sequence"
);

const noLiveFields = buildCurrentFields(
  { fields: { class_group_id: 1, class_number: 2 } },
  { app_show_idv2: 200000061, app_sql_datev2: "2026-05-07", scope_run_id: "test" },
  "recHeartbeat",
  null,
  "2026-05-07T12:00:00.000Z",
  "2026-05-07",
  "existing",
  "current",
  fieldMeta
);
assert.ok(
  !Object.prototype.hasOwnProperty.call(noLiveFields, "completed_trips"),
  "missing live/class values must not clear completed_trips"
);

console.log("schedules_daily_groups_live_fallback tests passed");
