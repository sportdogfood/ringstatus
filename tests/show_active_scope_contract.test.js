const assert = require("assert");
const fs = require("fs");
const path = require("path");

const root = path.resolve(__dirname, "..");
const tagger = fs.readFileSync(path.join(root, "tagger.js"), "utf8");
const runnerCommon = fs.readFileSync(path.join(root, "runner_pipeline_common.ps1"), "utf8");
const focusedRunner = fs.readFileSync(path.join(root, "run_tagger_task_focused_shows.ps1"), "utf8");
const workflow = fs.readFileSync(path.join(root, ".github", "workflows", "ringstatus-pipeline.yml"), "utf8");

assert.ok(
  tagger.includes('const SCOPE_STATUS_NO_ACTIVE_FEEDS = "no-active-feeds";') &&
    tagger.includes("noActiveFeeds: true") &&
    tagger.includes("maybeSet(FIELD_SCOPE_STATUS, appCtx.scopeStatus);"),
  "tagger must keep heartbeat alive and write scope_status=no-active-feeds when show/heartbeat has no active rows"
);

assert.ok(
  tagger.includes("hasValue(fields[FIELD_SHOW_ID])") &&
    tagger.includes("hasValue(fields[FIELD_CUSTOMER_ID])") &&
    tagger.includes("hasValue(fields[FIELD_SHOW_FOCUS_DAY])") &&
    !tagger.includes("hasValue(fields[FIELD_SHOW_START_DATE_BASE]) &&\n      hasValue(fields[FIELD_SHOW_END_DATE_BASE])"),
  "focused show minimum must be show_id, customer_id, and focus_day; start/end/show_name are enrichment fields"
);

assert.ok(
  tagger.includes("show_mode_control: showControl?.mode_control || null") &&
    tagger.includes('mode_source: "show_manual"') &&
    !tagger.includes("shows_mode_control: showControl?.mode_control || null") &&
    !tagger.includes('mode_source: "shows_manual"'),
  "mode control labels must use show_* naming now that mode_control lives on show"
);

assert.ok(
  !tagger.includes("const lookup = await findShowsMatchAnywhere(appShowId);") &&
    !tagger.includes("table: targetRecord ? TABLE_SHOW_TARGET : TABLE_SHOWS"),
  "tagger must not fall back to legacy shows as a focused source when show/heartbeat is empty"
);

assert.ok(
  runnerCommon.includes("NoActiveFeeds = $true") &&
    runnerCommon.includes("HEARTBEAT_NO_ACTIVE_FEEDS") &&
    !runnerCommon.includes('throw "No focused show record found in $TableName/$ViewName"'),
  "local runner must continue tagger with HEARTBEAT_NO_ACTIVE_FEEDS when show/heartbeat has zero rows"
);

assert.ok(
  focusedRunner.includes("HEARTBEAT_NO_ACTIVE_FEEDS") &&
    focusedRunner.includes("No focused show records found") &&
    focusedRunner.includes("run_tagger_task.ps1"),
  "focused show runner must write a no-active-feeds heartbeat when no focused show records exist"
);

assert.ok(
  workflow.includes("HEARTBEAT_ACTIVE_SHOWS_JSON") &&
    workflow.includes("HEARTBEAT_NO_ACTIVE_FEEDS=true") &&
    workflow.includes("for row in active_shows") &&
    !workflow.includes("Expected exactly one focused show row with heartbeat checked"),
  "GitHub workflow must support zero active shows and one pipeline pass per active show"
);

console.log("show_active_scope_contract tests passed");
