const assert = require("assert");
const fs = require("fs");
const path = require("path");

const root = path.resolve(__dirname, "..");
const liveGroupsPath = path.join(root, "live_groups_daily.js");
const runTaskPath = path.join(root, "run_tagger_task.ps1");
const workflowPath = path.join(root, ".github", "workflows", "ringstatus-pipeline.yml");

assert.ok(fs.existsSync(liveGroupsPath), "live_groups_daily.js must exist");

const source = fs.readFileSync(liveGroupsPath, "utf8");
const runTask = fs.readFileSync(runTaskPath, "utf8");
const workflow = fs.readFileSync(workflowPath, "utf8");

assert.ok(
  source.includes('TABLE_LIVE_GROUPS = process.env.TABLE_LIVE_GROUPS || "live_groups"'),
  "live_groups_daily must write to live_groups by default"
);

assert.ok(
  source.includes('if (mode !== "DAY")') &&
    source.includes('reason: "mode_not_day"'),
  "live_groups_daily must skip outside DAY mode"
);

assert.ok(
  source.includes("getLiveClassStatus") &&
    source.includes("ListAjax?from_wp_api=true"),
  "live_groups_daily must use the live status gate before ListAjax"
);

assert.ok(
  source.includes("live_focus_day: scope.focus_day") &&
    source.includes("show: [scope.record_id]"),
  "live_groups_daily must link to show and store live_focus_day"
);

assert.ok(
  source.includes("is_cuurent_scope: true") &&
    source.includes("dropped_at: null"),
  "live_groups_daily must mark rows seen in the latest successful payload current and clear dropped_at"
);

assert.ok(
  source.includes("fields.dropped_at = RUN_AT") &&
    source.includes("dropped: droppedUpdates.length"),
  "live_groups_daily must mark same-scope rows missing from a successful payload as dropped"
);

assert.ok(
  source.includes("attachLiveGroupLinks") &&
    source.includes("row.watch_schedule = scheduleIds") &&
    source.includes("row.watch_trips = tripIds"),
  "live_groups_daily must bind watch_schedule and watch_trips links before upsert"
);

assert.ok(
  source.includes('TABLE_WATCH_SCHEDULE = process.env.TABLE_WATCH_SCHEDULE || "watch_schedule"') &&
    source.includes('TABLE_WATCH_TRIPS = process.env.TABLE_WATCH_TRIPS || "watch_trips"'),
  "live_groups_daily must read watch_schedule and watch_trips directly for link binding"
);

assert.ok(
  source.includes("live_groups_status_false") &&
    source.includes("live_groups_no_focus_rows") &&
    source.includes("logAutomationEvent"),
  "live_groups_daily must leave Airtable evidence for false or unmatched live payloads"
);

assert.ok(
  runTask.includes("LIVE_GROUPS_DAILY") &&
    runTask.includes("live_groups_daily.js"),
  "run_tagger_task must include live_groups_daily"
);

assert.ok(
  workflow.includes('run_step "LIVE_GROUPS_DAILY" "live_groups_daily.js" "1"'),
  "GitHub workflow must include live_groups_daily"
);

console.log("live_groups_daily_contract tests passed");
