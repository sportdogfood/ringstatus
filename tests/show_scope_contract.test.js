const assert = require("assert");
const fs = require("fs");
const path = require("path");

const {
  buildShowScopeKey,
} = require("../lib/show_scope");

assert.strictEqual(
  buildShowScopeKey({
    customerId: 10002,
    showId: 200000006,
    focusDay: "2026-05-16",
  }),
  "10002|200000006|2026-05-16",
  "show scope key must be customer_id|show_id|focus_day"
);

assert.strictEqual(
  buildShowScopeKey({
    customer_id: "10002",
    show_id: "200000006",
    focus_day: "2026-05-16T00:00:00.000Z",
  }),
  "10002|200000006|2026-05-16",
  "show scope key must accept Airtable-style field names and date values"
);

assert.strictEqual(
  buildShowScopeKey({
    customerId: 10002,
    showId: 200000006,
  }),
  "",
  "show scope key must not be built from incomplete scope values"
);

const tagger = fs.readFileSync(path.resolve(__dirname, "..", "tagger.js"), "utf8");
assert.ok(
  tagger.includes("HEARTBEAT_TARGET_SHOW_RECORD_ID") &&
    tagger.includes("FIELD_SHOW_SCOPE_KEY") &&
    tagger.includes("FIELD_RING_COLLECTION") &&
    tagger.includes("FIELD_LINK_SHOW"),
  "tagger must support record-targeted focused show heartbeat snapshots"
);

assert.ok(
  tagger.includes("modeControlFromTargetDecision") &&
    tagger.includes("heartbeatTargetDecision?.mode_control"),
  "tagger mode control must use the focused show target before legacy shows rows"
);

assert.ok(
  tagger.includes("if (decision || heartbeatTargetAppSqlDate)") &&
    tagger.includes("appSqlDate = decision.focus_day") &&
    !tagger.includes("addDaysSql(decision.focus_day"),
  "focused show focus_day must be the heartbeat app_sql_date; shifted_to_next_day must not add another day"
);

assert.ok(
  tagger.includes("const heartbeatShowId = appCtx.appShowId ?? clock?.showId ?? null;") &&
    tagger.includes("const heartbeatSqlDate = appCtx.appSqlDate || sqlDate;") &&
    tagger.includes("[HEARTBEAT_SHOW_ID]: heartbeatShowId") &&
    tagger.includes("[HEARTBEAT_SQL_DATE]: heartbeatSqlDate"),
  "heartbeat primary show_id/sql_date must come from show.heartbeat focus scope before endpoint clock fallback"
);

const schedulesDaily = fs.readFileSync(path.resolve(__dirname, "..", "schedules_dailyv2.js"), "utf8");
assert.ok(
  schedulesDaily.includes("pickFirst(baseContext.current_app_sql_date, candidateDateFromMode") &&
    schedulesDaily.includes('strOrNull(baseContext.current_app_sql_date_source) || "heartbeat_app_sql_date"') &&
    !schedulesDaily.includes('baseContext.mode === "NIGHT"\n    ? "night_shift"'),
  "schedules_dailyv2 must consume heartbeat app_sql_date instead of recalculating a NIGHT shift"
);

assert.ok(
  schedulesDaily.includes("buildScheduleEndpoint(scope.app_sql_datev2, scope.app_show_idv2, scope.customer_id || CUSTOMER_ID)") &&
    schedulesDaily.includes("buildScheduleEmptyEndpoint(baseHeartbeatContext.app_show_idv2, baseHeartbeatContext.customer_id || CUSTOMER_ID)") &&
    schedulesDaily.includes("const customerId = scope?.customer_id || CUSTOMER_ID"),
  "schedules_dailyv2 schedule endpoints must use heartbeat customer_id before global CUSTOMER_ID fallback"
);

assert.ok(
  schedulesDaily.includes("function existingScheduleRowMatchesScope") &&
    schedulesDaily.includes("if (!existingScheduleRowMatchesScope(row, scope)) continue;") &&
    schedulesDaily.includes("if (boolValue(row?.fields?.inactive) || firstValue(row?.fields?.dropped_at)) continue;"),
  "schedules_dailyv2 must not drop existing watch_schedule rows outside the current schedule scope or rows already dropped"
);

const focusedShowsRunner = fs.readFileSync(path.resolve(__dirname, "..", "run_tagger_task_focused_shows.ps1"), "utf8");
assert.ok(
  focusedShowsRunner.includes("HEARTBEAT_TARGET_SHOW_RECORD_ID") &&
    focusedShowsRunner.includes("run_tagger_task.ps1"),
  "focused show runner must invoke the existing pipeline once per focused show record"
);

console.log("show_scope_contract tests passed");
