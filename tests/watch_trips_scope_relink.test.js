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

const mismatchedTripDecision = classifyWatchTripsHeartbeatRelink(
  {
    app_show_idv2: 200000061,
    schedule_show_datev2: "2026-05-10",
    "app_sql_date (from heartbeat)": ["2026-05-28"],
    heartbeat: ["recCurrentWrong"],
  },
  appCtx,
  "recNew"
);
assert.strictEqual(mismatchedTripDecision.action, "clear");
assert.strictEqual(mismatchedTripDecision.auto_archive, false);
assert.strictEqual(mismatchedTripDecision.deactivate_current_scope, true);

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

const droppedDecision = classifyWatchTripsHeartbeatRelink(
    {
      app_show_idv2: 200000062,
      schedule_show_datev2: "2026-05-28",
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

const inactiveDecision = classifyWatchTripsHeartbeatRelink(
  {
    app_show_idv2: 200000062,
    schedule_show_datev2: "2026-05-28",
    inactive: true,
    heartbeat: ["recOldInactive"],
  },
  appCtx,
  "recNew"
);
assert.strictEqual(inactiveDecision.action, "clear");
assert.strictEqual(inactiveDecision.inactive_reason, "inactive");
assert.strictEqual(inactiveDecision.auto_archive, true);

console.log("watch_trips_scope_relink tests passed");
