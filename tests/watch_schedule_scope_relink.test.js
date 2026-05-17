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

assert.strictEqual(
  classifyWatchScheduleHeartbeatRelink(
    {
      app_show_idv2: 200000061,
      app_sql_datev2: "2026-05-10",
      app_dow_rawv2: "Sun",
      "app_sql_date (from heartbeat)": ["2026-05-28"],
      heartbeat: ["recCurrentWrong"],
    },
    appCtx,
    "recNew"
  ).action,
  "clear"
);

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

assert.strictEqual(
  classifyWatchScheduleHeartbeatRelink(
    {
      app_show_idv2: 200000062,
      app_sql_datev2: "2026-05-28",
      app_dow_rawv2: "Thu",
      archive: true,
      heartbeat: ["recOldArchived"],
    },
    appCtx,
    "recNew"
  ).action,
  "clear"
);

console.log("watch_schedule_scope_relink tests passed");
