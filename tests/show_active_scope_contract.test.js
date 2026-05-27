const assert = require("assert");
const fs = require("fs");
const path = require("path");

const root = path.resolve(__dirname, "..");
const tagger = fs.readFileSync(path.join(root, "tagger.js"), "utf8");
const defaultShowDateGuard = fs.readFileSync(path.join(root, "lib", "default_show_date_guard.js"), "utf8");
const runnerCommon = fs.readFileSync(path.join(root, "runner_pipeline_common.ps1"), "utf8");
const schedulesDaily = fs.readFileSync(path.join(root, "schedules_dailyv2.js"), "utf8");
const tripsDaily = fs.readFileSync(path.join(root, "trips_dailyv2.js"), "utf8");
const runTaggerTask = fs.readFileSync(path.join(root, "run_tagger_task.ps1"), "utf8");
const focusedRunner = fs.readFileSync(path.join(root, "run_tagger_task_focused_shows.ps1"), "utf8");
const workflow = fs.readFileSync(path.join(root, ".github", "workflows", "ringstatus-pipeline.yml"), "utf8");

assert.ok(
  tagger.includes('const SCOPE_STATUS_NO_ACTIVE_FEEDS = "no-active-feeds";') &&
    tagger.includes("noActiveFeeds: true") &&
    tagger.includes("maybeSet(FIELD_SCOPE_STATUS, appCtx.scopeStatus);"),
  "tagger must keep heartbeat alive and write scope_status=no-active-feeds when show/heartbeat has no active rows"
);

assert.ok(
  tagger.includes('const FIELD_MANUAL_DAY_COUNT = process.env.FIELD_MANUAL_DAY_COUNT || "manual_day_count";') &&
    tagger.includes("manualDayCount") &&
    tagger.includes("manual_day_count: numericFieldOrNull(fields[FIELD_MANUAL_DAY_COUNT])"),
  "tagger must read show.manual_day_count as the manual expected inclusive day count"
);

assert.ok(
  tagger.includes("hasValue(fields[FIELD_SHOW_ID])") &&
    tagger.includes("hasValue(fields[FIELD_CUSTOMER_ID])") &&
    tagger.includes("hasValue(fields[FIELD_SHOW_FOCUS_DAY])") &&
    tagger.includes("isFocusedShowInActiveWindow(fields, nowSqlDate)") &&
    !tagger.includes("hasValue(fields[FIELD_SHOW_START_DATE_BASE]) &&\n      hasValue(fields[FIELD_SHOW_END_DATE_BASE])"),
  "focused show minimum must be show_id, customer_id, and focus_day; start/end/show_name are enrichment fields, but active relinks require today within start_date..end_date when both are present"
);

assert.ok(
  tagger.includes("show_mode_control: showControl?.mode_control || null") &&
    defaultShowDateGuard.includes('mode_source: "show_manual"') &&
    !tagger.includes("shows_mode_control: showControl?.mode_control || null") &&
    !defaultShowDateGuard.includes('mode_source: "shows_manual"'),
  "mode control labels must use show_* naming now that mode_control lives on show"
);

assert.ok(
  !tagger.includes("const lookup = await findShowsMatchAnywhere(appShowId);") &&
    !tagger.includes("table: targetRecord ? TABLE_SHOW_TARGET : TABLE_SHOWS"),
  "tagger must not fall back to legacy shows as a focused source when show/heartbeat is empty"
);

assert.ok(
  runnerCommon.includes("NoActiveFeeds = $true") &&
    runnerCommon.includes("manual_day_count") &&
    runnerCommon.includes("Get-TodaySqlDate") &&
    runnerCommon.includes("Get-TomorrowSqlDate") &&
    runnerCommon.includes("$shiftedToNextDay -and $modeControl -eq 'NIGHT' -and $focusDay -eq $tomorrowSqlDate") &&
    runnerCommon.includes("HEARTBEAT_NO_ACTIVE_FEEDS") &&
    !runnerCommon.includes('throw "No focused show record found in $TableName/$ViewName"'),
  "local runner must continue tagger with HEARTBEAT_NO_ACTIVE_FEEDS when show/heartbeat has zero rows, except an explicit shifted NIGHT focus_day for tomorrow"
);

assert.ok(
  tagger.includes('modeControl === "NIGHT"') &&
    tagger.includes("shiftedToNextDay &&") &&
    tagger.includes("focusDay === tomorrowSqlDate"),
  "tagger must allow a manually shifted NIGHT focused show for tomorrow even when today is before start_date"
);

assert.ok(
  schedulesDaily.includes("fields.is_current_scope = true;") &&
    schedulesDaily.includes('setResolvedField(fields, watchScheduleFieldMeta, "archive", false);') &&
    schedulesDaily.includes('setResolvedField(fields, watchScheduleFieldMeta, "inactive", false);') &&
    schedulesDaily.includes("fields.dropped_at = null;"),
  "schedules_dailyv2 must clear archive/inactive/dropped_at when a schedule row is confirmed in the current feed"
);

assert.ok(
  tripsDaily.includes("async function syncActiveTableLinks") &&
    tripsDaily.includes("summary.error = String(e?.message || e).slice(0, 500);") &&
    tripsDaily.includes("return summary;"),
  "trips_dailyv2 active table link sync must be nonfatal so active_groups access cannot block watch_trips writes"
);

assert.ok(
  focusedRunner.includes("HEARTBEAT_NO_ACTIVE_FEEDS") &&
    focusedRunner.includes("No focused show records found") &&
    focusedRunner.includes("Test-ShowRecordInActiveWindow") &&
    focusedRunner.includes("run_tagger_task.ps1"),
  "focused show runner must write a no-active-feeds heartbeat when no focused show records exist"
);

assert.ok(
  runTaggerTask.includes("Multiple focused show records found") &&
    runTaggerTask.includes("run_tagger_task_focused_shows.ps1"),
  "default local runner must route multiple active show rows to the focused show runner"
);

assert.ok(
  workflow.includes("HEARTBEAT_ACTIVE_SHOWS_JSON") &&
    workflow.includes("HEARTBEAT_NO_ACTIVE_FEEDS=true") &&
    workflow.includes("inActiveWindow") &&
    workflow.includes("for row in active_shows") &&
    !workflow.includes("Expected exactly one focused show row with heartbeat checked"),
  "GitHub workflow must support zero active shows and one pipeline pass per active show"
);

console.log("show_active_scope_contract tests passed");
