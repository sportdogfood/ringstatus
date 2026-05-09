const assert = require("assert");
const fs = require("fs");
const path = require("path");
const {
  normalizeHtmlScheduleTimeText,
  parseScheduleHtmlTimeOverlay,
} = require("../schedules_dailyv2");

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
    source.includes("const scopedRowsBase = chosen.rows.filter"),
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

console.log("schedules_daily_schedule_fallback tests passed");
