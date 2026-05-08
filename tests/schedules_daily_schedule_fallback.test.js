const assert = require("assert");
const fs = require("fs");
const path = require("path");

const source = fs.readFileSync(path.resolve(__dirname, "..", "schedules_dailyv2.js"), "utf8");

assert.ok(
  source.includes("SGL_SCHEDULE_FALLBACK_DIRS"),
  "schedules_dailyv2 should allow configured schedule fallback directories"
);

assert.ok(
  source.includes("tmp\", \"sgl_schedule_samples") ||
    source.includes("tmp\", \"sgl_schedule_samples".replaceAll("\"", "'")),
  "schedules_dailyv2 should include the local sampled schedule cache directory"
);

assert.ok(
  source.includes("C:\\\\actions-runner\\\\ringstatus\\\\manual_sgl_payloads"),
  "schedules_dailyv2 should include the manual SGL payload folder"
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

console.log("schedules_daily_schedule_fallback tests passed");
