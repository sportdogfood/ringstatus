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
