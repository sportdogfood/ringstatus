const assert = require("assert");

const {
  classifyWatchTripsHeartbeatRelink,
  watchTripsRowScope,
} = require("../lib/watch_trips_scope_relink");

const appCtx = {
  appShowId: 200000062,
  appSqlDate: "2026-05-28",
};

assert.deepStrictEqual(
  watchTripsRowScope({
    app_show_idv2: 200000061,
    schedule_show_datev2: "2026-05-10",
  }),
  {
    app_show_id: 200000061,
    app_sql_date: "2026-05-10",
  }
);

assert.strictEqual(
  classifyWatchTripsHeartbeatRelink(
    {
      app_show_idv2: 200000062,
      schedule_show_datev2: "2026-05-28",
      heartbeat: ["recOld"],
    },
    appCtx,
    "recNew"
  ).action,
  "link"
);

assert.strictEqual(
  classifyWatchTripsHeartbeatRelink(
    {
      app_show_idv2: 200000062,
      schedule_show_datev2: "2026-05-28",
      heartbeat: ["recNew"],
    },
    appCtx,
    "recNew"
  ).action,
  "keep"
);

assert.strictEqual(
  classifyWatchTripsHeartbeatRelink(
    {
      app_show_idv2: 200000061,
      schedule_show_datev2: "2026-05-10",
      "app_sql_date (from heartbeat)": ["2026-05-28"],
      heartbeat: ["recCurrentWrong"],
    },
    appCtx,
    "recNew"
  ).action,
  "clear"
);

assert.strictEqual(
  classifyWatchTripsHeartbeatRelink(
    {
      app_show_idv2: 200000061,
      schedule_show_datev2: "2026-05-10",
      "app_sql_date (from heartbeat)": ["2026-05-10"],
      heartbeat: ["recHistorical"],
    },
    appCtx,
    "recNew"
  ).action,
  "skip"
);

console.log("watch_trips_scope_relink tests passed");
