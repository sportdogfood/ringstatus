const assert = require("assert");
const fs = require("fs");
const path = require("path");
const {
  applyScheduleHtmlTimeOverlay,
  normalizeHtmlScheduleTimeText,
  parseScheduleHtmlTimeOverlay,
  scheduleHtmlFallbackDirs,
} = require("../schedules_dailyv2");

const source = fs.readFileSync(path.resolve(__dirname, "..", "schedules_dailyv2.js"), "utf8");
const dailyScope = fs.readFileSync(
  path.resolve(__dirname, "..", "docs", "ringstatus_daily_scope_2026-05-09.md"),
  "utf8"
);

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
  source.includes("DEFAULT_SCHEDULE_HTML_FALLBACK_DIRS") &&
    source.includes("manual_schedule_html_missing") &&
    source.includes("manual_schedule_html_lookup"),
  "manual schedule HTML misses should alert with lookup evidence"
);

assert.ok(
  source.includes("expected_filename_shapes") &&
    source.includes("show_id") &&
    source.includes("searched_dirs"),
  "manual schedule HTML lookup evidence should include searched directories and accepted filename shapes"
);

assert.ok(
  dailyScope.includes("Never invent or substitute dates, show IDs, customer IDs, endpoints, field names, or payload values"),
  "daily scope should document the absolute source-input contract"
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
    source.includes("recordSoftPayloadAudit") &&
    source.includes("recordPayloadPingAudit"),
  "SGL payload pings and soft/empty payloads should be written to automation_errs"
);

assert.ok(
  source.includes("path=") &&
    source.includes("body_length=") &&
    source.includes("content_length="),
  "automation_errs message should retain the endpoint path and payload size evidence"
);

assert.ok(
  !source.includes("buildClassesEndpoint") &&
    !source.includes("enrichScheduleRowsWithClassDetails") &&
    !source.includes("class_detail|") &&
    !source.includes("class_endpoint_soft_payloads_nonfatal") &&
    !source.includes("refusing schedule writes after class endpoint soft payloads"),
  "schedules_dailyv2 should not ping /classes/{class_id} while building schedule rows"
);

assert.ok(
  source.includes("classes_endpoint_unreliable_for_schedule_lane") &&
    source.includes("chosen.rows.filter((row) => rowScheduledDateMatchesScope(row, scope))"),
  "schedule rows should come directly from schedule payload/fallback without class endpoint enrichment"
);

assert.equal(normalizeHtmlScheduleTimeText("8:00 AM"), "08:00:00");
assert.equal(normalizeHtmlScheduleTimeText("8:30 AM"), "08:30:00");
assert.equal(normalizeHtmlScheduleTimeText("1:40 PM"), "13:40:00");

const htmlOverlay = parseScheduleHtmlTimeOverlay(`
  <tr class="class_group_row">
    <td><a href="/showgrounds/classes/detail?cid=200024977&amp;sid=200000061&amp;cgid=200023750&amp;ring=1">Open Jumper [701]</a></td>
    <td class="center-align"><a>8:30 AM</a></td>
    <td class="center-align"><a>18</a></td>
  </tr>
`);

assert.equal(htmlOverlay.parsedRows.length, 1);
assert.equal(htmlOverlay.rowsByGroupId.get("200023750").estimated_start_time, "08:30:00");
assert.equal(htmlOverlay.rowsByClassNumber.get("701").estimated_start_time, "08:30:00");

const htmlDirs = scheduleHtmlFallbackDirs();
assert.ok(
  htmlDirs.some((dirPath) => dirPath.endsWith(path.join("manual_sgl_payloads"))),
  "schedule HTML lookup should include manual_sgl_payloads root"
);
assert.ok(
  htmlDirs.some((dirPath) => dirPath.endsWith(path.join("manual_sgl_payloads", "schedule-html"))),
  "schedule HTML lookup should include manual_sgl_payloads/schedule-html"
);

const missingOverlay = applyScheduleHtmlTimeOverlay([], "999999999", "2099-01-01");
assert.equal(missingOverlay.summary.alert, "manual_schedule_html_missing");
assert.ok(missingOverlay.summary.manual_schedule_html_lookup.searched_dirs.length >= 2);
assert.ok(
  missingOverlay.summary.manual_schedule_html_lookup.expected_filename_shapes.includes(
    "schedule_html_2099-01-01_show_999999999_EPOCH.html"
  )
);

console.log("schedules_daily_schedule_fallback tests passed");
