const assert = require("assert");
const fs = require("fs");
const path = require("path");

const source = fs.readFileSync(path.resolve(__dirname, "..", "schedules_dailyv2.js"), "utf8");

assert.ok(
  source.includes("SGL_SCHEDULE_FALLBACK_DIRS"),
  "schedules_dailyv2 should allow configured schedule fallback directories"
);

assert.ok(
  !source.includes("sgl_schedule_samples"),
  "schedules_dailyv2 should not use the old tmp/sgl_schedule_samples fallback directory"
);

assert.ok(
  source.includes("early_sgl_payloads") &&
    source.includes("manual_sgl_payloads") &&
    source.includes("schedule-html"),
  "schedules_dailyv2 should use the explicit early/manual payload folder contract"
);

assert.ok(
  source.includes("EARLY_SCHEDULE_PAYLOAD_DIR") &&
    source.includes("MANUAL_SCHEDULE_PAYLOAD_DIR") &&
    source.includes("MANUAL_PEOPLE_PAYLOAD_DIR"),
  "schedules_dailyv2 should name the early/manual schedule and people payload directories"
);

assert.ok(
  source.includes("loadScheduleFallbackPayload"),
  "schedules_dailyv2 should load last-good schedule payloads after soft payload failures"
);

assert.ok(
  source.includes("dated_schedule_fallback"),
  "fallback-backed schedule runs should be identified in run output"
);

assert.ok(
  /catch\s*\(error\)\s*\{[\s\S]+loadScheduleFallbackPayload\(scope\.app_show_idv2,\s*scope\.app_sql_datev2\)/.test(source),
  "dated schedule soft-payload failures should try show/date-specific fallback before failing"
);

assert.ok(
  source.includes("cacheSuccessfulSchedulePayloads") &&
    source.includes("forwardScheduleDates") &&
    source.includes("PREFETCH_FORWARD_SCHEDULES"),
  "successful direct schedule fetches should cache current and forward show-day payloads"
);

assert.ok(
  /schedule_\$\{appSqlDate\}_show_\$\{appShowId\}_\$\{epochSeconds\}\.json/.test(source),
  "schedule cache filenames should be schedule_YYYY-MM-DD_show_SHOWID_EPOCH.json"
);

assert.ok(
  source.includes('TABLE_AUTOMATION_ERRS = process.env.TABLE_AUTOMATION_ERRS || "automation_errs"') &&
    source.includes("createAutomationErr") &&
    source.includes("recordSoftPayloadAudit"),
  "soft/empty SGL payloads should be written to automation_errs"
);

assert.ok(
  source.includes("path=") &&
    source.includes("body_length=") &&
    source.includes("content_length="),
  "automation_errs message should retain the endpoint path and payload size evidence"
);

console.log("schedules_daily_schedule_fallback tests passed");
