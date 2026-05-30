const assert = require("assert");
const fs = require("fs");
const path = require("path");

const {
  buildCurrentFields,
  buildDuplicateTripArchiveUpdates,
  buildTripKeyParts,
  preserveExistingLinkFields,
  selectTripRowsForWriteScope,
  showHeartbeatTargetDate,
  tripRowKeyFromFields,
} = require("../trips_dailyv2");

const tripsDailySource = fs.readFileSync(path.resolve(__dirname, "..", "trips_dailyv2.js"), "utf8");
assert.ok(
  !tripsDailySource.includes("No current watch_schedule rows matched heartbeat scope"),
  "watch_trips must not no-op just because watch_schedule has no current rows"
);
assert.ok(
  tripsDailySource.includes('reason: "active_tables_deprecated"'),
  "active_groups/classes/entries must remain deprecated and skipped in trips_dailyv2"
);
assert.ok(
  tripsDailySource.includes("resolveHeartbeatScopesFromShowTarget"),
  "watch_trips must resolve every valid show.heartbeat row instead of only one"
);
assert.ok(
  tripsDailySource.includes('source: "show_heartbeat_multi"'),
  "watch_trips must report multi-show heartbeat processing when more than one show is active"
);
assert.ok(
  !tripsDailySource.includes("currently processes the first valid show target per run"),
  "watch_trips must not leave the multi-show heartbeat case as a known caveat"
);

assert.strictEqual(
  tripRowKeyFromFields({
    trips_key: "200000061|2026-05-10|3|723|8778|2807",
    class_number: 723,
    entry_number: 2807,
  }),
  "200000061|2026-05-10|3|723|8778|2807",
  "watch_trips matching should prefer writable trips_key"
);

assert.deepStrictEqual(
  buildTripKeyParts({
    sid: 200000061,
    sqlDate: "2026-05-10",
    ringNumber: 3,
    classNumber: 723,
    pid: 8778,
    entryNumber: 2807,
    time: "08:30:00",
    cgid: 200023690,
  }),
  {
    scheduleKey: "200000061|2026-05-10|3|723",
    scheduleShort: "3|723",
    tripsKey: "200000061|2026-05-10|3|723|8778|2807",
    tripsShortKey: "723|8778|2807",
    fullNestingKey: "200000061|2026-05-10|3|08:30:00|200023690|723|8778|2807",
  },
  "base trip keys must not include an invented class_group_sequence"
);

assert.deepStrictEqual(
  buildTripKeyParts({
    sid: 200000061,
    sqlDate: "2026-05-10",
    ringNumber: 3,
    classNumber: 723,
    tieBreaker: 4,
    pid: 8778,
    entryNumber: 2807,
    time: "08:30:00",
    cgid: 200023690,
  }),
  {
    scheduleKey: "200000061|2026-05-10|3|723|4",
    scheduleShort: "3|723|4",
    tripsKey: "200000061|2026-05-10|3|723|8778|2807|4",
    tripsShortKey: "723|8778|2807|4",
    fullNestingKey: "200000061|2026-05-10|3|08:30:00|200023690|723|8778|2807|4",
  },
  "real class_group_sequence tie-breaker belongs at the end of trip keys"
);

assert.strictEqual(
  tripRowKeyFromFields({
    entryxclasses_uuid: "REAL-UUID",
    class_number: 715,
    entry_number: 3160,
  }),
  "people:715:3160",
  "watch_trips matching should prefer class_number + entry_number"
);

assert.strictEqual(
  tripRowKeyFromFields({
    entryxclasses_uuid: "REAL-UUID",
  }),
  "REAL-UUID",
  "entryxclasses_uuid should remain the backup key when people pair is missing"
);

assert.strictEqual(
  tripRowKeyFromFields({
    entryxclasses_uuid: "fallback:8778:3160:715:markanto_a",
    class_number: 715,
    entry_number: 3160,
  }),
  "people:715:3160",
  "legacy fallback rows should match the new people trip key when the pair exists"
);

const writeRows = selectTripRowsForWriteScope(new Map([
  ["focus", { scheduled_date: "2026-05-29", class_number: 715 }],
  ["forward", { scheduled_date: "2026-05-31", class_number: 550 }],
  ["invalid", { scheduled_date: "0000-00-00", class_number: 999 }],
]), { app_sql_date: "2026-05-29" });

assert.deepStrictEqual(
  writeRows.map((row) => row.class_number),
  [715, 550],
  "watch_trips writes must keep non-focus show trip rows instead of filtering them out"
);

assert.strictEqual(
  showHeartbeatTargetDate({
    focus_day: "2026-05-29",
    start_date: "2026-05-29",
    end_date: "2026-05-31",
    shifted_to_next_day: true,
  }, new Date("2026-05-30T10:30:00.000Z")).target_date,
  "2026-05-30",
  "watch_trips show target must follow shifted_to_next_day, not local 6am clock"
);

const nonFocusFields = buildCurrentFields(
  {
    scheduled_date: "2026-05-31",
    ring_number: 5,
    class_number: 550,
    entry_number: 2,
    pid: 19676,
  },
  {
    recordId: "recHeartbeat",
    app_show_id: 200000063,
    app_sql_date: "2026-05-29",
    app_time: "17:00:00",
    app_dow_raw: "Friday",
    shifted_to_next_day: true,
    scope_run_id: "run-1",
    mode: "NIGHT",
  },
  "recShow",
  "2026-05-29T21:00:00.000Z",
  "2026-05-29",
  "current",
  new Set([
    "heartbeat",
    "shows",
    "show_id",
    "show_date",
    "app_show_id",
    "app_sql_date",
    "app_show_idv2",
    "app_sql_datev2",
    "schedule_show_datev2",
    "scheduled_date",
    "trips_key",
    "is_current_scope",
    "inactive",
    "archive",
    "dropped_at",
    "pid",
    "entry_number",
    "ring_number",
    "class_number",
  ])
);

assert.strictEqual(
  nonFocusFields.is_current_scope,
  false,
  "non-focus show trip rows should be retained but not marked current"
);
assert.strictEqual(
  nonFocusFields.inactive,
  false,
  "non-focus show trip rows should not be archived/inactivated just because they are not focus_day"
);
assert.strictEqual(
  nonFocusFields.dropped_at,
  null,
  "current trip updates must explicitly clear stale dropped_at values"
);
assert.strictEqual(
  nonFocusFields.show_date,
  "2026-05-31",
  "trip row show_date should preserve its scheduled date"
);
assert.strictEqual(
  nonFocusFields.schedule_show_datev2,
  "2026-05-31",
  "trip row schedule_show_datev2 should preserve its scheduled date"
);
assert.deepStrictEqual(
  nonFocusFields.heartbeat,
  ["recHeartbeat"],
  "active non-current show-window trip rows must still keep the heartbeat link"
);

const droppedFields = require("../trips_dailyv2").buildDroppedFields(
  {
    app_show_id: 200000063,
    app_sql_date: "2026-05-30",
    app_dow_raw: "Saturday",
    shifted_to_next_day: true,
    scope_run_id: "run-1",
    mode: "NIGHT",
  },
  "2026-05-29T21:00:00.000Z",
  "2026-05-29",
  "dropped",
  new Set(["heartbeat", "watch_schedule", "is_current_scope", "inactive", "archive", "dropped_at"])
);
assert.ok(
  !Object.prototype.hasOwnProperty.call(droppedFields, "heartbeat"),
  "dropped trip rows must preserve existing heartbeat links"
);
assert.ok(
  !Object.prototype.hasOwnProperty.call(droppedFields, "watch_schedule"),
  "dropped trip rows must preserve existing watch_schedule links"
);

const preservedLinks = preserveExistingLinkFields(
  { is_current_scope: false },
  { heartbeat: ["recHeartbeat"], watch_schedule: ["recSchedule"] }
);
assert.deepStrictEqual(
  preservedLinks,
  { is_current_scope: false, heartbeat: ["recHeartbeat"], watch_schedule: ["recSchedule"] },
  "watch_trips updates must explicitly preserve existing relationship links"
);

const duplicateArchives = buildDuplicateTripArchiveUpdates(
  [
    { id: "recKeep", fields: { trips_key: "show|date|ring|class|pid|entry", is_current_scope: true } },
    { id: "recDrop", fields: { trips_key: "show|date|ring|class|pid|entry", heartbeat: ["recHeartbeat"], watch_schedule: ["recSchedule"] } },
    { id: "recArchived", fields: { trips_key: "show|date|ring|class|pid|entry", archive: true, watch_schedule: ["recScheduleOld"] } },
  ],
  new Set(["recKeep"]),
  "2026-05-29T21:00:00.000Z",
  "2026-05-29",
  new Set(["heartbeat", "watch_schedule", "is_current_scope", "inactive", "archive", "dropped_at", "run_time", "last_seen_at"])
);

assert.deepStrictEqual(
  duplicateArchives,
  [
    {
      id: "recDrop",
      fields: {
        is_current_scope: false,
        inactive: true,
        archive: true,
        dropped_at: "2026-05-29",
        run_time: "2026-05-29T21:00:00.000Z",
        last_seen_at: "2026-05-29",
        heartbeat: ["recHeartbeat"],
        watch_schedule: ["recSchedule"],
      },
    },
    {
      id: "recArchived",
      fields: {
        is_current_scope: false,
        inactive: true,
        archive: true,
        dropped_at: "2026-05-29",
        run_time: "2026-05-29T21:00:00.000Z",
        last_seen_at: "2026-05-29",
        watch_schedule: ["recScheduleOld"],
      },
    },
  ],
  "duplicate trip keys should archive extra rows without breaking existing relationship links"
);

console.log("trips_daily_trip_key tests passed");
