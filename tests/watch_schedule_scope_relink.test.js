const assert = require("assert");

const {
  classifyWatchScheduleHeartbeatRelink,
  watchScheduleRowScope,
} = require("../lib/watch_schedule_scope_relink");

const appCtx = {
  appShowId: 200000062,
  appSqlDate: "2026-05-28",
  appDowRaw: "Thu",
};

assert.deepStrictEqual(
  watchScheduleRowScope({
    app_show_idv2: 200000061,
    app_sql_datev2: "2026-05-10",
    app_dow_rawv2: "Sun",
  }),
  {
    app_show_id: 200000061,
    app_sql_date: "2026-05-10",
    app_dow_raw: "Sun",
  }
);

assert.strictEqual(
  classifyWatchScheduleHeartbeatRelink(
    {
      app_show_idv2: 200000062,
      app_sql_datev2: "2026-05-28",
      app_dow_rawv2: "Thu",
      heartbeat: ["recOld"],
    },
    appCtx,
    "recNew"
  ).action,
  "link"
);

assert.strictEqual(
  classifyWatchScheduleHeartbeatRelink(
    {
      app_show_idv2: 200000062,
      app_sql_datev2: "2026-05-28",
      app_dow_rawv2: "Thu",
      heartbeat: ["recNew"],
    },
    appCtx,
    "recNew"
  ).action,
  "keep"
);

const mismatchedScheduleDecision = classifyWatchScheduleHeartbeatRelink(
  {
    app_show_idv2: 200000061,
    app_sql_datev2: "2026-05-10",
    app_dow_rawv2: "Sun",
    "app_sql_date (from heartbeat)": ["2026-05-28"],
    heartbeat: ["recCurrentWrong"],
  },
  appCtx,
  "recNew"
);
assert.strictEqual(mismatchedScheduleDecision.action, "clear");
assert.strictEqual(mismatchedScheduleDecision.auto_archive, false);
assert.strictEqual(mismatchedScheduleDecision.deactivate_current_scope, true);

assert.strictEqual(
  classifyWatchScheduleHeartbeatRelink(
    {
      app_show_idv2: 200000061,
      app_sql_datev2: "2026-05-10",
      app_dow_rawv2: "Sun",
      "app_sql_date (from heartbeat)": ["2026-05-10"],
      heartbeat: ["recHistorical"],
    },
    appCtx,
    "recNew"
  ).action,
  "skip"
);

const archivedDecision = classifyWatchScheduleHeartbeatRelink(
    {
      app_show_idv2: 200000062,
      app_sql_datev2: "2026-05-28",
      app_dow_rawv2: "Thu",
      archive: true,
      heartbeat: ["recOldArchived"],
    },
    appCtx,
    "recNew"
);
assert.strictEqual(archivedDecision.action, "clear");
assert.strictEqual(archivedDecision.inactive_reason, "archive");
assert.strictEqual(archivedDecision.auto_archive, true);

const droppedDecision = classifyWatchScheduleHeartbeatRelink(
  {
    app_show_idv2: 200000062,
    app_sql_datev2: "2026-05-28",
    app_dow_rawv2: "Thu",
    dropped_at: "2026-05-17",
    heartbeat: ["recOldDropped"],
  },
  appCtx,
  "recNew"
);
assert.strictEqual(droppedDecision.action, "clear");
assert.strictEqual(droppedDecision.inactive_reason, "dropped");
assert.strictEqual(
  droppedDecision.auto_archive,
  false,
  "dropped_at is a review signal; archive must be checked manually for dropped rows"
);

console.log("watch_schedule_scope_relink tests passed");
