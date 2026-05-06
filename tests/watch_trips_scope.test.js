const assert = require("assert");

const {
  recordMatchesAppScope,
  resolveRecordScopeDate,
  resolveRecordScopeShowId,
} = require("../lib/watch_trips_scope");

const appCtx = {
  app_show_id: 200000061,
  app_sql_date: "2026-05-07",
};

assert.strictEqual(
  resolveRecordScopeShowId({ show_id: 200000061 }),
  200000061
);

assert.strictEqual(
  resolveRecordScopeDate({ schedule_show_datev2: "2026-05-07T00:00:00.000Z" }),
  "2026-05-07"
);

assert.strictEqual(
  recordMatchesAppScope(
    {
      show_id: 200000061,
      schedule_show_datev2: "2026-05-07",
      is_current_scope: true,
      scope_status: "current",
    },
    appCtx
  ),
  true,
  "current show/date row should match"
);

assert.strictEqual(
  recordMatchesAppScope(
    {
      show_id: 200000060,
      schedule_show_datev2: "2026-05-03",
      is_current_scope: false,
      scope_status: "dropped",
    },
    appCtx
  ),
  false,
  "stale dropped row must not be enriched"
);

assert.strictEqual(
  recordMatchesAppScope(
    {
      app_sid: 200000061,
      " scheduled_date": "2026-05-07",
      class_id: null,
      class_number: 770,
      entry_number: 2298,
    },
    appCtx
  ),
  true,
  "class_number fallback rows with null class_id should remain eligible"
);

assert.strictEqual(
  recordMatchesAppScope(
    {
      show_id: 200000061,
      schedule_show_datev2: "2026-05-07",
      is_current_scope: false,
    },
    appCtx
  ),
  false,
  "explicitly non-current rows should not be enriched even if show/date match"
);

console.log("watch_trips_scope tests passed");
